"""
Runner principal du module prédictif.
Orchestre : sources → features → prédiction → écriture en DB.
Appelé quotidiennement par le scheduler.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from predictif.config import (
    CACHE_DIR,
    DATA_DIR,
    DATE_DEBUT_HISTORIQUE,
    HORIZONS,
    MODELS_DIR,
    OCCITANIE_DEPTS,
    POINTS_METEO_OCCITANIE,
)
from predictif.features import construire_jeu
from predictif.model import (
    charger_modeles,
    entrainer_et_evaluer,
    predire_zone,
    sauvegarder_modeles,
)
from predictif.sources.hubeau import HubeauHydroSource, HubeauPiezoSource
from predictif.sources.vigeau import VigeauSource
from predictif.sources.weather.open_meteo import OpenMeteoSource

logger = logging.getLogger(__name__)


def _get_weather_source():
    """Sélectionne la source météo selon les variables d'environnement disponibles."""
    if os.getenv("METEOFRANCE_API_KEY"):
        from predictif.sources.weather.meteo_france import MeteoFranceSource
        return MeteoFranceSource()
    source = OpenMeteoSource()
    if source.requires_commercial_license:
        logger.warning(
            "Open-Meteo sans clé commerciale. "
            "Acceptable en développement/backtest, pas en production commerciale. "
            "Configurez OPEN_METEO_API_KEY ou METEOFRANCE_API_KEY."
        )
    return source


async def collecter_donnees(depts: list[str], date_debut: str, date_fin: str) -> dict:
    """Collecte toutes les sources. Retourne un dict {nom_source: SourceResult}."""
    vigeau = VigeauSource(CACHE_DIR)
    hydro = HubeauHydroSource()
    piezo = HubeauPiezoSource()
    meteo = _get_weather_source()

    results = {}
    for src in (vigeau, hydro, piezo):
        logger.info("Collecte : %s…", src.name)
        results[src.name] = await src.fetch(depts, date_debut, date_fin)

    logger.info("Collecte : %s…", meteo.name)
    results[meteo.name] = await meteo.fetch(POINTS_METEO_OCCITANIE, date_debut, date_fin)

    for name, result in results.items():
        if not result.available:
            logger.warning("Source indisponible : %s — %s", name, result.error)

    return results


def sources_manquantes(results: dict) -> list[str]:
    return [name for name, r in results.items() if not r.available]


async def run_pipeline(db_session=None, entrainer: bool = False) -> dict:
    """
    Pipeline complet : collecte → features → prédiction → écriture DB.

    db_session : session SQLAlchemy async (None = pas d'écriture en DB)
    entrainer : True = réentraîner le modèle (à faire rarement, pas quotidiennement)

    Retourne un dict de résumé.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    date_fin = date.today().isoformat()
    results = await collecter_donnees(OCCITANIE_DEPTS, DATE_DEBUT_HISTORIQUE, date_fin)

    manquantes = sources_manquantes(results)
    fiabilite_reduite = bool(manquantes)

    # VigiEau est obligatoire (c'est ce qu'on prédit)
    if not results["vigeau"].available:
        logger.error("VigiEau indisponible — pipeline interrompu")
        return {"statut": "erreur", "raison": "vigeau_indisponible"}

    df_arretes = results["vigeau"].data
    df_hydro = results.get("hubeau_hydro") and results["hubeau_hydro"].data
    df_piezo = results.get("hubeau_piezo") and results["hubeau_piezo"].data
    df_meteo = results.get("open_meteo", results.get("meteo_france"))
    df_meteo = df_meteo.data if df_meteo and df_meteo.available else None

    # Feature engineering
    jeu = construire_jeu(
        df_arretes, df_hydro, df_piezo, df_meteo,
        OCCITANIE_DEPTS,
        fiabilite_reduite=fiabilite_reduite,
        source_manquante=",".join(manquantes) if manquantes else None,
    )

    if jeu.empty:
        return {"statut": "erreur", "raison": "jeu_de_donnees_vide"}

    # Sauvegarde du jeu de données
    try:
        jeu.to_parquet(DATA_DIR / "jeu_donnees.parquet")
    except ImportError:
        jeu.to_csv(DATA_DIR / "jeu_donnees.csv", index=False)

    # Entraînement ou chargement du modèle
    if entrainer:
        rapport, modeles, zones = entrainer_et_evaluer(jeu)
        version = datetime.now(timezone.utc).strftime("%Y-%m-%d_v1")
        artifact_path = sauvegarder_modeles(modeles, zones, MODELS_DIR, version)

        if db_session:
            await _sauvegarder_model_version(db_session, version, rapport, str(artifact_path))
    else:
        # Charge le dernier modèle actif
        pkl_files = sorted(MODELS_DIR.glob("*.pkl"))
        if not pkl_files:
            logger.warning("Aucun modèle sauvegardé — entraînement forcé")
            rapport, modeles, zones = entrainer_et_evaluer(jeu)
            version = datetime.now(timezone.utc).strftime("%Y-%m-%d_v1")
            artifact_path = sauvegarder_modeles(modeles, zones, MODELS_DIR, version)
            if db_session:
                await _sauvegarder_model_version(db_session, version, rapport, str(artifact_path))
        else:
            modeles, zones = charger_modeles(pkl_files[-1])
            version = pkl_files[-1].stem
            rapport = {}

    # Prédictions
    predictions_df = predire_zone(jeu, modeles, zones)

    if db_session and not predictions_df.empty:
        await _sauvegarder_predictions(
            db_session, predictions_df, version, fiabilite_reduite,
            ",".join(manquantes) if manquantes else None,
        )

    return {
        "statut": "ok",
        "date": date.today().isoformat(),
        "n_zones": len(predictions_df),
        "fiabilite_reduite": fiabilite_reduite,
        "sources_manquantes": manquantes,
        "rapport_backtest": rapport,
    }


async def _sauvegarder_model_version(db, version: str, rapport: dict, artifact_path: str):
    """Écrit ou met à jour la ligne ModelVersion en DB."""
    from sqlalchemy import select
    from app.models.prediction import ModelVersion

    async with db.begin_nested():
        result = await db.execute(select(ModelVersion).where(ModelVersion.version == version))
        mv = result.scalar_one_or_none()
        if mv is None:
            mv = ModelVersion(
                version=version,
                algorithm="HistGradientBoostingClassifier",
                trained_at=datetime.now(timezone.utc),
                train_period_start=date(2012, 1, 1),
                train_period_end=date(ANNEE_FIN_ENTRAINEMENT, 12, 31),
                test_period_start=date(ANNEE_FIN_ENTRAINEMENT + 1, 1, 1),
                test_period_end=date.today(),
                backtest_auc_7j=rapport.get("7j", {}).get("auc"),
                backtest_auc_15j=rapport.get("15j", {}).get("auc"),
                backtest_auc_30j=rapport.get("30j", {}).get("auc"),
                backtest_brier_7j=rapport.get("7j", {}).get("brier"),
                backtest_brier_15j=rapport.get("15j", {}).get("brier"),
                backtest_brier_30j=rapport.get("30j", {}).get("brier"),
                backtest_report_json=json.dumps(rapport, default=float),
                is_active=True,
                model_artifact_path=artifact_path,
            )
            db.add(mv)


async def _sauvegarder_predictions(
    db, predictions_df: pd.DataFrame, version: str, fiabilite_reduite: bool, source_manquante: str | None
):
    """Écrit les prédictions du jour en DB."""
    from sqlalchemy import select
    from app.models.prediction import ModelVersion, Prediction, ZoneAlerte

    result = await db.execute(select(ModelVersion).where(ModelVersion.version == version))
    mv = result.scalar_one_or_none()
    if mv is None:
        logger.error("ModelVersion %s introuvable en DB", version)
        return

    today = date.today()
    for _, row in predictions_df.iterrows():
        zone_code = str(row["zone"])
        result = await db.execute(select(ZoneAlerte).where(ZoneAlerte.code == zone_code))
        zone = result.scalar_one_or_none()
        if zone is None:
            logger.debug("Zone %s inconnue en DB — ignorée", zone_code)
            continue

        for h in HORIZONS:
            col = f"proba_{h}j"
            if col not in row or pd.isna(row.get(col)):
                continue
            pred = Prediction(
                model_version_id=mv.id,
                zone_alerte_id=zone.id,
                date_prediction=today,
                horizon_jours=h,
                proba_alerte=float(row[col]),
                niveau_actuel=int(row["niveau_actuel"]),
                fiabilite_reduite=fiabilite_reduite,
                source_manquante=source_manquante,
            )
            db.add(pred)
