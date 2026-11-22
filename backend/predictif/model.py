"""
Entraînement, évaluation et backtest du modèle prédictif sécheresse.
"""
from __future__ import annotations

import json
import logging
import pickle
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score

from predictif.config import ANNEE_FIN_ENTRAINEMENT, HORIZONS, NIVEAU_CIBLE, SEUIL_PROBA

logger = logging.getLogger(__name__)


def _colonnes_x(jeu: pd.DataFrame) -> list[str]:
    return [c for c in jeu.columns if c not in ("date", "zone") and not c.startswith("y_")]


def _preparer_x(jeu: pd.DataFrame, zones: list[str]) -> pd.DataFrame:
    X = jeu[_colonnes_x(jeu)].copy()
    zone_map = {z: i for i, z in enumerate(zones)}
    X.insert(0, "zone_code", jeu["zone"].map(zone_map).astype(float))
    return X


def _nouveau_modele() -> HistGradientBoostingClassifier:
    # zone_code peut avoir > 255 modalités (limite HistGradient pour categorical_features)
    # → traité comme numérique ordinal, ce qui reste exploitable par le modèle
    return HistGradientBoostingClassifier(
        max_iter=400, learning_rate=0.05, max_leaf_nodes=31, min_samples_leaf=40,
        l2_regularization=1.0, random_state=0,
    )


def _delais_anticipation(jeu: pd.DataFrame, pred_df: pd.DataFrame) -> dict[str, Any]:
    """Calcule le délai médian d'anticipation avant un passage en alerte."""
    p = pred_df.set_index(["zone", "date"])["p"]
    res = []
    for z, g in jeu[jeu["date"].dt.year > ANNEE_FIN_ENTRAINEMENT].groupby("zone"):
        g = g.sort_values("date").set_index("date")["niveau_actuel"]
        bascules = g.index[(g >= NIVEAU_CIBLE) & (g.shift() < NIVEAU_CIBLE)]
        for j in bascules:
            avant = pd.date_range(j - pd.Timedelta(days=max(HORIZONS)), j - pd.Timedelta(days=1))
            pz = p.reindex([(z, d) for d in avant]).to_numpy()
            n = 0
            for v in pz[::-1]:
                if np.isnan(v) or v < SEUIL_PROBA:
                    break
                n += 1
            res.append(n)
    anticipes = [r for r in res if r > 0]
    return {
        "n_evenements": len(res),
        "n_anticipes": len(anticipes),
        "delai_median_j": float(np.median(anticipes)) if anticipes else None,
    }


def entrainer_et_evaluer(jeu: pd.DataFrame) -> dict[str, Any]:
    """
    Entraîne le modèle sur train <= ANNEE_FIN_ENTRAINEMENT, évalue sur test.
    Retourne le rapport de backtest complet.
    """
    zones = sorted(jeu["zone"].unique())
    rapport: dict[str, Any] = {}
    jeu_eval = jeu[jeu["niveau_actuel"] < NIVEAU_CIBLE]
    annee = jeu_eval["date"].dt.year

    modeles: dict[int, HistGradientBoostingClassifier] = {}

    for h in HORIZONS:
        y = jeu_eval[f"y_{h}j"]
        ok = y.notna()
        tr = ok & (annee <= ANNEE_FIN_ENTRAINEMENT)
        te = ok & (annee > ANNEE_FIN_ENTRAINEMENT)

        if y[tr].nunique() < 2 or y[te].nunique() < 2:
            logger.warning("Horizon %dj : pas assez d'événements.", h)
            continue

        m = _nouveau_modele().fit(_preparer_x(jeu_eval[tr], zones), y[tr])
        modeles[h] = m
        p = m.predict_proba(_preparer_x(jeu_eval[te], zones))[:, 1]

        r: dict[str, Any] = {
            "auc": float(roc_auc_score(y[te], p)),
            "precision_moyenne": float(average_precision_score(y[te], p)),
            "brier": float(brier_score_loss(y[te], p)),
            "taux_base": float(y[te].mean()),
            "n_test": int(te.sum()),
            "n_positifs_test": int(y[te].sum()),
        }

        if h == max(HORIZONS):
            pred_df = jeu_eval[te][["zone", "date"]].copy().assign(p=p)
            r["delais_anticipation"] = _delais_anticipation(jeu, pred_df)

        rapport[f"{h}j"] = r
        logger.info(
            "Horizon %2dj | AUC %.3f | précision moy. %.3f (base=%.3f) | Brier %.3f",
            h, r["auc"], r["precision_moyenne"], r["taux_base"], r["brier"],
        )

    # Backtest par département
    if "dep" in jeu_eval.columns:
        rapport["par_dept"] = {}
        for dept, grp in jeu_eval.groupby(jeu_eval["zone"].str[:2] if "dep" not in jeu_eval.columns else jeu_eval["dep"]):
            dept_r: dict[str, Any] = {}
            annee_dept = grp["date"].dt.year
            for h in HORIZONS:
                y = grp[f"y_{h}j"]
                ok = y.notna()
                te = ok & (annee_dept > ANNEE_FIN_ENTRAINEMENT)
                if y[te].nunique() < 2 or te.sum() < 10:
                    continue
                if h not in modeles:
                    continue
                p = modeles[h].predict_proba(_preparer_x(grp[te], zones))[:, 1]
                dept_r[f"auc_{h}j"] = float(roc_auc_score(y[te], p))
                dept_r[f"brier_{h}j"] = float(brier_score_loss(y[te], p))
            if dept_r:
                rapport["par_dept"][str(dept)] = dept_r

    return rapport, modeles, zones


def sauvegarder_modeles(modeles: dict, zones: list[str], models_dir: Path, version: str) -> Path:
    """Sérialise les modèles (un fichier par horizon) + la liste des zones."""
    models_dir.mkdir(parents=True, exist_ok=True)
    artifact = {
        "zones": zones,
        "modeles": {str(h): m for h, m in modeles.items()},
        "version": version,
    }
    path = models_dir / f"{version}.pkl"
    with open(path, "wb") as f:
        pickle.dump(artifact, f)
    logger.info("Modèles sauvegardés : %s", path)
    return path


def charger_modeles(path: Path) -> tuple[dict, list[str]]:
    """Charge les modèles et la liste des zones depuis un fichier .pkl."""
    with open(path, "rb") as f:
        artifact = pickle.load(f)
    return artifact["modeles"], artifact["zones"]


def predire_zone(
    jeu_actuel: pd.DataFrame,
    modeles: dict,
    zones: list[str],
) -> pd.DataFrame:
    """
    Calcule les probabilités pour le dernier jour disponible dans jeu_actuel.
    Retourne un DataFrame avec colonnes [zone, niveau_actuel, proba_7j, proba_15j, proba_30j].
    """
    cle = [c for c in ("q_med", "pluie_30j") if c in jeu_actuel.columns]
    dispo = jeu_actuel.dropna(subset=cle) if cle else jeu_actuel
    if dispo.empty:
        logger.error("Aucune donnée disponible pour la prédiction")
        return pd.DataFrame()

    jour = dispo["date"].max()
    actuel = jeu_actuel[jeu_actuel["date"] == jour].copy()

    for h, m in modeles.items():
        h = int(h)
        col = f"proba_{h}j"
        actuel[col] = m.predict_proba(_preparer_x(actuel, zones))[:, 1]
        actuel.loc[actuel["niveau_actuel"] >= NIVEAU_CIBLE, col] = np.nan

    cols = ["zone", "niveau_actuel"] + [f"proba_{h}j" for h in HORIZONS if f"proba_{h}j" in actuel.columns]
    return actuel[cols].copy()
