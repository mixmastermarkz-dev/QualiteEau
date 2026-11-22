"""
Backtest du modèle prédictif sécheresse — Occitanie.

Lance la collecte des données historiques, l'entraînement et l'évaluation.
Génère BACKTEST_REPORT.md à la racine du projet backend.

Usage (depuis le répertoire backend/) :
    python -m scripts.run_backtest

Critère de validation :
    AUC >= 0.70 sur chaque horizon (7j, 15j, 30j)
    Si un horizon échoue → warning, pas d'erreur fatale (données insuffisantes possibles)

Durée estimée : 10–30 min selon la charge des APIs Hub'Eau et Open-Meteo.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("TESTING", "1")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backtest")

SEUIL_AUC = 0.70
RAPPORT_PATH = Path(__file__).parent.parent / "BACKTEST_REPORT.md"


def _badge(auc: float | None) -> str:
    if auc is None:
        return "⚪ N/D"
    if auc >= SEUIL_AUC:
        return f"✅ {auc:.3f}"
    return f"❌ {auc:.3f} (< {SEUIL_AUC})"


def _ecrire_rapport(rapport: dict, duree_s: float, sources_manquantes: list[str]) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lignes = [
        "# Backtest — Modèle prédictif sécheresse Occitanie",
        "",
        f"> Généré le {now} · durée : {duree_s:.0f}s",
        "",
    ]

    if sources_manquantes:
        lignes += [
            "⚠️ **Sources indisponibles pendant le backtest :** "
            + ", ".join(sources_manquantes),
            "_(Les métriques peuvent être sous-estimées.)_",
            "",
        ]

    # Tableau résumé
    lignes += [
        "## Métriques globales",
        "",
        "| Horizon | AUC ROC | Précision moy. | Brier score | n échantillons test | Taux base |",
        "|---------|---------|----------------|-------------|---------------------|-----------|",
    ]

    validation_ok = True
    for h in [7, 15, 30]:
        k = f"{h}j"
        r = rapport.get(k, {})
        auc = r.get("auc")
        pr  = r.get("precision_moyenne")
        bs  = r.get("brier")
        n   = r.get("n_test", "—")
        tb  = r.get("taux_base")
        if auc is not None and auc < SEUIL_AUC:
            validation_ok = False
        lignes.append(
            f"| **J+{h}** | {_badge(auc)} | "
            f"{'—' if pr is None else f'{pr:.3f}'} | "
            f"{'—' if bs is None else f'{bs:.3f}'} | "
            f"{n} | "
            f"{'—' if tb is None else f'{tb:.1%}'} |"
        )

    lignes += [""]

    # Délais d'anticipation
    r30 = rapport.get("30j", {})
    da = r30.get("delais_anticipation")
    if da:
        lignes += [
            "## Délai d'anticipation (horizon J+30)",
            "",
            f"- Événements de passage en alerte détectés : **{da['n_evenements']}**",
            f"- Événements anticipés (p ≥ 0.5) : **{da['n_anticipes']}**",
            f"- Délai médian d'anticipation : **{da['delai_median_j']} jours**",
            "",
        ]

    # Par département
    par_dept = rapport.get("par_dept", {})
    if par_dept:
        lignes += [
            "## Résultats par département",
            "",
            "| Dept | AUC J+7 | AUC J+15 | AUC J+30 |",
            "|------|---------|----------|----------|",
        ]
        for dept, dr in sorted(par_dept.items()):
            lignes.append(
                f"| **{dept}** | "
                f"{_badge(dr.get('auc_7j'))} | "
                f"{_badge(dr.get('auc_15j'))} | "
                f"{_badge(dr.get('auc_30j'))} |"
            )
        lignes.append("")

    # Verdict
    lignes += [
        "## Verdict",
        "",
        ("✅ **VALIDÉ** — AUC ≥ 0.70 sur tous les horizons. "
         "Le modèle peut être déployé en production."
         if validation_ok else
         "❌ **NON VALIDÉ** — Certains horizons ont AUC < 0.70. "
         "Vérifier la qualité/quantité des données avant déploiement."),
        "",
        "---",
        "",
        "```json",
        json.dumps(rapport, indent=2, ensure_ascii=False, default=float),
        "```",
    ]

    RAPPORT_PATH.write_text("\n".join(lignes), encoding="utf-8")
    logger.info("Rapport écrit : %s", RAPPORT_PATH)


async def main() -> int:
    logger.info("=" * 60)
    logger.info("BACKTEST SÉCHERESSE OCCITANIE")
    logger.info("Collecte des données depuis 2012 — patience requise")
    logger.info("=" * 60)

    t0 = time.time()

    from predictif.runner import collecter_donnees, sources_manquantes
    from predictif.features import construire_jeu
    from predictif.model import entrainer_et_evaluer, sauvegarder_modeles
    from predictif.config import (
        OCCITANIE_DEPTS, POINTS_METEO_OCCITANIE,
        DATE_DEBUT_HISTORIQUE, DATA_DIR, MODELS_DIR,
    )
    from predictif.sources.weather.open_meteo import OpenMeteoSource
    from predictif.sources.hubeau import HubeauHydroSource, HubeauPiezoSource
    from predictif.sources.vigeau import VigeauSource

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    cache_dir = DATA_DIR / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    date_fin = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info("Collecte : VigiEau (arrêtés sécheresse)…")
    vigeau = VigeauSource(cache_dir)
    r_vigeau = await vigeau.fetch(OCCITANIE_DEPTS, DATE_DEBUT_HISTORIQUE, date_fin)
    if not r_vigeau.available:
        logger.error("VigiEau indisponible : %s", r_vigeau.error)
        logger.error("Le backtest nécessite les données VigiEau. Abandon.")
        return 1

    logger.info("  → %d arrêtés chargés", len(r_vigeau.data))

    logger.info("Collecte : Hub'Eau hydrométrie (débits)…")
    hydro = HubeauHydroSource()
    r_hydro = await hydro.fetch(OCCITANIE_DEPTS, DATE_DEBUT_HISTORIQUE, date_fin)
    logger.info("  → %s", f"{len(r_hydro.data):,} observations" if r_hydro.available else f"INDISPONIBLE ({r_hydro.error})")

    logger.info("Collecte : Hub'Eau piézométrie (nappes)…")
    piezo = HubeauPiezoSource()
    r_piezo = await piezo.fetch(OCCITANIE_DEPTS, DATE_DEBUT_HISTORIQUE, date_fin)
    logger.info("  → %s", f"{len(r_piezo.data):,} observations" if r_piezo.available else f"INDISPONIBLE ({r_piezo.error})")

    logger.info("Collecte : Open-Meteo (météo historique)…")
    meteo = OpenMeteoSource()
    r_meteo = await meteo.fetch(POINTS_METEO_OCCITANIE, DATE_DEBUT_HISTORIQUE, date_fin)
    logger.info("  → %s", f"{len(r_meteo.data):,} lignes" if r_meteo.available else f"INDISPONIBLE ({r_meteo.error})")

    manquantes = [
        nom for nom, r in [
            ("hubeau_hydro", r_hydro), ("hubeau_piezo", r_piezo), ("open_meteo", r_meteo)
        ] if not r.available
    ]
    if manquantes:
        logger.warning("Sources absentes : %s — résultats potentiellement dégradés", manquantes)

    logger.info("Construction du jeu de données…")
    jeu = construire_jeu(
        r_vigeau.data,
        r_hydro.data if r_hydro.available else None,
        r_piezo.data if r_piezo.available else None,
        r_meteo.data if r_meteo.available else None,
        OCCITANIE_DEPTS,
        fiabilite_reduite=bool(manquantes),
        source_manquante=",".join(manquantes) if manquantes else None,
    )

    if jeu.empty:
        logger.error("Jeu de données vide après feature engineering. Abandon.")
        return 1

    logger.info("  → jeu : %d lignes × %d colonnes", *jeu.shape)
    logger.info("  → années : %d – %d", jeu['date'].dt.year.min(), jeu['date'].dt.year.max())

    # Sauvegarde du jeu pour analyse ultérieure
    out_parquet = DATA_DIR / "jeu_donnees_backtest.parquet"
    try:
        jeu.to_parquet(out_parquet)
        logger.info("  → jeu sauvegardé : %s", out_parquet)
    except Exception as e:
        logger.warning("Impossible de sauvegarder le parquet : %s", e)

    logger.info("Entraînement et évaluation (train ≤ 2022, test ≥ 2023)…")
    rapport, modeles, zones = entrainer_et_evaluer(jeu)

    if not rapport:
        logger.error("Aucun résultat de backtest produit (données insuffisantes ?).")
        return 1

    # Sauvegarde du modèle
    version = datetime.now(timezone.utc).strftime("%Y-%m-%d_v1")
    artifact_path = sauvegarder_modeles(modeles, zones, MODELS_DIR, version)

    duree = time.time() - t0

    # Rapport
    _ecrire_rapport(rapport, duree, manquantes)

    # Résumé console
    print()
    print("═" * 60)
    print("  RÉSULTATS BACKTEST")
    print("═" * 60)
    for h in [7, 15, 30]:
        r = rapport.get(f"{h}j", {})
        auc = r.get("auc")
        ok = "✅" if (auc is not None and auc >= SEUIL_AUC) else ("❌" if auc is not None else "⚪")
        print(f"  J+{h:2d}  AUC = {auc:.3f if auc else '  N/D'  }  {ok}")
    print(f"\n  Modèle : {artifact_path.name}")
    print(f"  Rapport : {RAPPORT_PATH}")
    print(f"  Durée : {duree:.0f}s")
    print("═" * 60)
    print()

    # Code de sortie : 0 si validé, 2 si AUC insuffisant
    for h in [7, 15, 30]:
        r = rapport.get(f"{h}j", {})
        if r.get("auc") is not None and r["auc"] < SEUIL_AUC:
            logger.warning("AUC J+%dj = %.3f < %.2f — seuil non atteint", h, r["auc"], SEUIL_AUC)
            return 2

    logger.info("Backtest validé ✅")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
