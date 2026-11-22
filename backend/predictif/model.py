"""
Entraînement, évaluation et backtest du modèle prédictif sécheresse.
"""
from __future__ import annotations

import hashlib
import json
import logging
import pickle
import stat
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.calibration import calibration_curve
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score

from predictif.config import (
    ANNEE_FIN_ENTRAINEMENT,
    ANNEE_VALIDATION,
    FRAICHEUR_MAX_JOURS_GLOBAL,
    HORIZONS,
    MARGE_JOURS_BLOCS,
    NIVEAU_CIBLE,
    SEUIL_PROBA,
)

logger = logging.getLogger(__name__)


# Colonnes présentes dans le jeu mais qui ne sont pas des variables du modèle :
#   - zone, date : identifiants
#   - dep : chaîne de caractères, non exploitable en ordinal
#   - fiabilite_reduite : métadonnée de sortie, pas une variable d'entrée
#   - zone_code : ordinal incohérent entre Propluvia et VigiEau (supprimé Phase 3)
_NON_FEATURES = frozenset({"date", "zone", "dep", "fiabilite_reduite"})


def _colonnes_x(jeu: pd.DataFrame) -> list[str]:
    return [c for c in jeu.columns if c not in _NON_FEATURES and not c.startswith("y_")]


def _preparer_x(jeu: pd.DataFrame, zones: list[str]) -> pd.DataFrame:
    # zones conservé en signature pour compatibilité des appelants mais non utilisé.
    # zone_code ordinal retiré : incohérent entre référentiels Propluvia et VigiEau.
    return jeu[_colonnes_x(jeu)].copy()


def _nouveau_modele() -> HistGradientBoostingClassifier:
    # zone_code peut avoir > 255 modalités (limite HistGradient pour categorical_features)
    # → traité comme numérique ordinal, ce qui reste exploitable par le modèle
    return HistGradientBoostingClassifier(
        max_iter=400, learning_rate=0.05, max_leaf_nodes=31, min_samples_leaf=40,
        l2_regularization=1.0, random_state=0,
    )


# Variables utilisées par le modèle de référence « arrêtés seuls »
# (backtest_honnete.py — aucune source externe, uniquement les arrêtés VigiEau)
_FEATURES_ARRETES_SEULS = [
    "niveau_actuel",
    "jours_meme_niveau",
    "niveau_max_365j",
    "clim_zone",
    "dep_nb_alerte",
    "dep_une_alerte",
    "type_sou",
    "jour_sin",
    "jour_cos",
]


def _metriques_scalaires(y: pd.Series, p: np.ndarray) -> dict[str, Any]:
    """AUC, précision moyenne, Brier, taux de base."""
    if y.nunique() < 2 or len(y) == 0:
        return {}
    return {
        "auc": float(roc_auc_score(y, p)),
        "precision_moyenne": float(average_precision_score(y, p)),
        "brier": float(brier_score_loss(y, p)),
        "taux_base": float(y.mean()),
        "n_obs": int(len(y)),
        "n_positifs": int(y.sum()),
    }


def _metriques_seuil(y: pd.Series, p: np.ndarray, seuil: float) -> dict[str, Any]:
    """Métriques binaires au seuil fixé (pour le calcul des fausses alertes)."""
    pos = p >= seuil
    vp = int((pos & (y.values == 1)).sum())
    fp = int((pos & (y.values == 0)).sum())
    fn = int(((~pos) & (y.values == 1)).sum())
    n_alarmes = vp + fp
    return {
        "seuil": float(seuil),
        "n_alarmes": n_alarmes,
        "taux_fausse_alerte": float(fp / n_alarmes) if n_alarmes > 0 else float("nan"),
        "rappel": float(vp / (vp + fn)) if (vp + fn) > 0 else 0.0,
    }


def _calibration_stats(y: pd.Series, p: np.ndarray) -> dict[str, Any]:
    """Statistiques de calibration : ECE et courbe de fiabilité (10 bins)."""
    try:
        frac_pos, mean_pred = calibration_curve(y, p, n_bins=10, strategy="quantile")
        ece = float(np.mean(np.abs(frac_pos - mean_pred)))
        return {
            "ece": ece,
            "bins_prob_pred": [round(float(x), 4) for x in mean_pred],
            "bins_prob_reel": [round(float(x), 4) for x in frac_pos],
        }
    except ValueError:
        return {}


def _analyser_evenements(
    jeu: pd.DataFrame,
    pred_index: pd.DataFrame,
    h: int,
    col_proba: str,
) -> dict[str, Any]:
    """
    Analyse par événement : pour chaque entrée en alerte dans la période de test,
    calcule l'anticipation et distingue « première zone du département » vs autres.

    pred_index : DataFrame indexé sur (zone, date) avec colonne col_proba.
    """
    debut_test = pred_index.index.get_level_values("date").min()
    jeu_post = jeu[jeu["date"] >= debut_test].sort_values(["zone", "date"])

    res_premières: list[int] = []   # délais pour zones premières du dépt
    res_autres: list[int] = []       # délais pour zones déjà suivies

    for zone, grp in jeu_post.groupby("zone"):
        g = grp.set_index("date")
        niv = g["niveau_actuel"]
        dep_nb = g["dep_nb_alerte"] if "dep_nb_alerte" in g.columns else pd.Series(np.nan, index=g.index)

        bascules = niv.index[(niv >= NIVEAU_CIBLE) & (niv.shift() < NIVEAU_CIBLE)]
        for t in bascules:
            premiere = not pd.isna(dep_nb.get(t, np.nan)) and dep_nb.get(t, np.nan) == 0
            avant = pd.date_range(t - pd.Timedelta(days=h), t - pd.Timedelta(days=1))
            probas = pred_index.loc[
                pred_index.index.isin([(zone, d) for d in avant]), col_proba
            ].values if hasattr(pred_index, "loc") else np.array([])

            # Délai consécutif ≥ seuil depuis la bascule en remontant
            delai = 0
            for v in probas[::-1]:
                if np.isnan(v) or float(v) < SEUIL_PROBA:
                    break
                delai += 1

            bucket = res_premières if premiere else res_autres
            bucket.append(delai)

    def _stats_bucket(vals: list[int]) -> dict[str, Any]:
        if not vals:
            return {"n_evenements": 0}
        annonces = [v for v in vals if v > 0]
        return {
            "n_evenements": len(vals),
            "n_annonces": len(annonces),
            "taux_annonce": round(len(annonces) / len(vals), 3),
            "delai_median_j": round(float(np.median(annonces)), 1) if annonces else None,
            "delai_q25_j": round(float(np.percentile(annonces, 25)), 1) if annonces else None,
            "delai_q75_j": round(float(np.percentile(annonces, 75)), 1) if annonces else None,
        }

    return {
        "toutes_zones": _stats_bucket(res_premières + res_autres),
        "premiere_zone_dept": _stats_bucket(res_premières),
        "dept_deja_en_alerte": _stats_bucket(res_autres),
    }


def entrainer_et_evaluer(jeu: pd.DataFrame) -> tuple[dict[str, Any], dict, list]:
    """
    Entraîne et évalue trois modèles côte à côte sur la même période de test.

    Découpage (Phase 4) :
      train      : date ≤ 31/12/ANNEE_FIN_ENTRAINEMENT − MARGE_JOURS_BLOCS
      validation : ANNEE_VALIDATION (calibration isotonique uniquement)
      test       : date ≥ 01/01/(ANNEE_VALIDATION+1) + MARGE_JOURS_BLOCS

    Trois modèles :
      (a) climatologie de zone — score = clim_zone, pas de ML
      (b) arrêtés seuls       — features minimales (backtest_honnete.py)
      (c) modèle complet      — arrêtés + débits + nappes + météo

    Les modèles (b) et (c) sont calibrés par régression isotonique sur la validation.
    La calibration n'est JAMAIS ajustée sur le test.

    Retourne (rapport, modeles_complets_par_horizon, zones).
    """
    zones = sorted(jeu["zone"].unique())
    jeu_actif = jeu[jeu["niveau_actuel"] < NIVEAU_CIBLE].copy()

    # ── Découpage temporel ────────────────────────────────────────────────────
    cutoff_train = pd.Timestamp(f"{ANNEE_FIN_ENTRAINEMENT}-12-31") - pd.Timedelta(days=MARGE_JOURS_BLOCS)
    debut_valid = pd.Timestamp(f"{ANNEE_VALIDATION}-01-01")
    fin_valid = pd.Timestamp(f"{ANNEE_VALIDATION}-12-31")
    debut_test = pd.Timestamp(f"{ANNEE_VALIDATION + 1}-01-01") + pd.Timedelta(days=MARGE_JOURS_BLOCS)

    m_tr = jeu_actif["date"] <= cutoff_train
    m_va = (jeu_actif["date"] >= debut_valid) & (jeu_actif["date"] <= fin_valid)
    m_te = jeu_actif["date"] >= debut_test

    rapport: dict[str, Any] = {
        "protocole": {
            "train": f"jusqu'au {cutoff_train.date()}",
            "validation": f"{debut_valid.date()} → {fin_valid.date()}",
            "test": f"depuis {debut_test.date()}",
            "n_obs_train": int(m_tr.sum()),
            "n_obs_valid": int(m_va.sum()),
            "n_obs_test": int(m_te.sum()),
        }
    }

    # ── Sous-ensembles de features ────────────────────────────────────────────
    cols_arr = [c for c in _FEATURES_ARRETES_SEULS if c in jeu_actif.columns]
    cols_all = _colonnes_x(jeu_actif)

    modeles_complets: dict[int, HistGradientBoostingClassifier] = {}

    for h in HORIZONS:
        cible = f"y_{h}j"
        y = jeu_actif[cible]
        ok = y.notna()

        idx_tr = ok & m_tr
        idx_va = ok & m_va
        idx_te = ok & m_te

        if y[idx_tr].nunique() < 2 or y[idx_te].nunique() < 2:
            logger.warning("Horizon %dj : pas assez d'événements.", h)
            continue

        y_tr, y_va, y_te = y[idx_tr], y[idx_va], y[idx_te]

        # ── Modèle (a) : climatologie — pas de ML ─────────────────────────
        p_clim = jeu_actif.loc[idx_te, "clim_zone"].fillna(0).values \
            if "clim_zone" in jeu_actif.columns else np.zeros(idx_te.sum())

        # ── Modèle (b) : arrêtés seuls ────────────────────────────────────
        m_arr = _nouveau_modele().fit(jeu_actif.loc[idx_tr, cols_arr], y_tr)
        p_arr_va = m_arr.predict_proba(jeu_actif.loc[idx_va, cols_arr])[:, 1]
        p_arr_te = m_arr.predict_proba(jeu_actif.loc[idx_te, cols_arr])[:, 1]
        cal_arr = IsotonicRegression(out_of_bounds="clip").fit(p_arr_va, y_va)
        p_arr_cal = cal_arr.transform(p_arr_te)

        # ── Modèle (c) : complet ──────────────────────────────────────────
        m_all = _nouveau_modele().fit(jeu_actif.loc[idx_tr, cols_all], y_tr)
        p_all_va = m_all.predict_proba(jeu_actif.loc[idx_va, cols_all])[:, 1]
        p_all_te = m_all.predict_proba(jeu_actif.loc[idx_te, cols_all])[:, 1]
        cal_all = IsotonicRegression(out_of_bounds="clip").fit(p_all_va, y_va)
        p_all_cal = cal_all.transform(p_all_te)

        modeles_complets[h] = m_all

        rapport[f"{h}j"] = {
            "climatologie": _metriques_scalaires(y_te, p_clim),
            "arretes_seuls": _metriques_scalaires(y_te, p_arr_cal),
            "complet": _metriques_scalaires(y_te, p_all_cal),
            "fausses_alertes": {
                "arretes_seuls": _metriques_seuil(y_te, p_arr_cal, SEUIL_PROBA),
                "complet": _metriques_seuil(y_te, p_all_cal, SEUIL_PROBA),
            },
            "calibration": {
                "arretes_seuls": _calibration_stats(y_te, p_arr_cal),
                "complet": _calibration_stats(y_te, p_all_cal),
            },
        }

        # ── Analyse par événement (horizon le plus long uniquement) ───────
        if h == max(HORIZONS):
            meta_te = jeu_actif.loc[idx_te, ["zone", "date"]].copy()
            meta_te = meta_te.assign(p_arr=p_arr_cal, p_all=p_all_cal)
            pred_idx = meta_te.set_index(["zone", "date"])

            rapport[f"{h}j"]["par_evenement"] = {
                "arretes_seuls": _analyser_evenements(jeu, pred_idx, h, "p_arr"),
                "complet": _analyser_evenements(jeu, pred_idx, h, "p_all"),
            }

        logger.info(
            "Horizon %2dj | AUC clim=%.3f arr=%.3f complet=%.3f (base=%.3f)",
            h,
            rapport[f"{h}j"]["climatologie"].get("auc", float("nan")),
            rapport[f"{h}j"]["arretes_seuls"].get("auc", float("nan")),
            rapport[f"{h}j"]["complet"].get("auc", float("nan")),
            rapport[f"{h}j"]["complet"].get("taux_base", float("nan")),
        )

    # ── Résultats par département ─────────────────────────────────────────────
    if "dep" in jeu_actif.columns:
        rapport["par_dept"] = {}
        for dept, grp in jeu_actif.groupby("dep"):
            dept_r: dict[str, Any] = {}
            m_te_dept = grp["date"] >= debut_test
            for h in HORIZONS:
                y_dept = grp.loc[m_te_dept, f"y_{h}j"].dropna()
                if y_dept.nunique() < 2 or len(y_dept) < 20:
                    continue
                if h not in modeles_complets:
                    continue
                idx_dept_te = grp.index[m_te_dept & grp[f"y_{h}j"].notna()]
                p_dept = modeles_complets[h].predict_proba(
                    jeu_actif.loc[idx_dept_te, cols_all]
                )[:, 1]
                dept_r[f"{h}j"] = _metriques_scalaires(y_dept, p_dept)
            if dept_r:
                rapport["par_dept"][str(dept)] = dept_r

    return rapport, modeles_complets, zones


def sauvegarder_modeles(
    modeles: dict, zones: list[str], models_dir: Path, version: str
) -> tuple[Path, str]:
    """
    Sérialise les modèles + la liste des zones.

    Retourne (chemin .pkl, sha256_hexdigest).
    Écrit un fichier sidecar {version}.pkl.sha256 et passe le .pkl en lecture seule
    pour l'application, afin d'empêcher le remplacement silencieux du fichier.
    """
    models_dir.mkdir(parents=True, exist_ok=True)
    artifact = {
        "zones": zones,
        "modeles": {str(h): m for h, m in modeles.items()},
        "version": version,
    }
    path = models_dir / f"{version}.pkl"
    with open(path, "wb") as f:
        pickle.dump(artifact, f)

    sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    (path.parent / (path.name + ".sha256")).write_text(sha256 + "\n", encoding="ascii")

    # Lecture seule pour l'application (owner=r, group=r, others=-)
    path.chmod(stat.S_IRUSR | stat.S_IRGRP)

    logger.info("Modèles sauvegardés : %s (sha256=%s…)", path, sha256[:12])
    return path, sha256


def charger_modeles(
    path: Path, expected_sha256: str | None = None
) -> tuple[dict, list[str]]:
    """
    Charge les modèles depuis un fichier .pkl.

    Vérifie l'empreinte SHA-256 avant pickle.load :
    - en priorité, depuis le paramètre expected_sha256 (fourni par la DB) ;
    - sinon, depuis le fichier sidecar {path}.sha256 ;
    - si aucun des deux n'est disponible, charge quand même en journalisant un
      avertissement (compatibilité avec les anciens fichiers sans sidecar).

    Lève ValueError si l'empreinte ne correspond pas.
    """
    sidecar = path.parent / (path.name + ".sha256")
    attendu = expected_sha256 or (sidecar.read_text(encoding="ascii").strip() if sidecar.exists() else None)

    if attendu:
        reel = hashlib.sha256(path.read_bytes()).hexdigest()
        if reel != attendu:
            logger.error(
                "Empreinte SHA-256 incorrecte pour %s "
                "(attendu %s…, obtenu %s…) — chargement refusé.",
                path.name, attendu[:12], reel[:12],
            )
            raise ValueError(
                f"Empreinte SHA-256 incorrecte pour {path.name}. Chargement refusé."
            )
    else:
        logger.warning(
            "Aucune empreinte SHA-256 disponible pour %s — chargement sans vérification d'intégrité.",
            path.name,
        )

    with open(path, "rb") as f:
        artifact = pickle.load(f)
    return artifact["modeles"], artifact["zones"]


def predire_zone(
    jeu_actuel: pd.DataFrame,
    modeles: dict,
    zones: list[str],
    date_reference: date | None = None,
) -> pd.DataFrame:
    """
    Calcule les probabilités pour le dernier jour disponible dans jeu_actuel.
    Retourne un DataFrame avec colonnes [zone, niveau_actuel, proba_7j, proba_15j, proba_30j].

    Vérifie la fraîcheur des données : si la dernière date disponible est plus
    ancienne que FRAICHEUR_MAX_JOURS_GLOBAL jours par rapport à date_reference
    (défaut : aujourd'hui), retourne un DataFrame vide et journalise une erreur.
    """
    if date_reference is None:
        date_reference = date.today()

    cle = [c for c in ("q_med", "pluie_30j") if c in jeu_actuel.columns]
    dispo = jeu_actuel.dropna(subset=cle) if cle else jeu_actuel
    if dispo.empty:
        logger.error("Aucune donnée disponible pour la prédiction")
        return pd.DataFrame()

    jour = dispo["date"].max()

    # Contrôle de fraîcheur (S4)
    age_jours = (pd.Timestamp(date_reference) - jour).days
    if age_jours > FRAICHEUR_MAX_JOURS_GLOBAL:
        logger.error(
            "Données trop anciennes pour prévoir : %d jours (maximum autorisé : %d). "
            "Dernière donnée disponible : %s. Prédiction refusée.",
            age_jours,
            FRAICHEUR_MAX_JOURS_GLOBAL,
            jour.date() if hasattr(jour, "date") else jour,
        )
        return pd.DataFrame()
    actuel = jeu_actuel[jeu_actuel["date"] == jour].copy()

    for h, m in modeles.items():
        h = int(h)
        col = f"proba_{h}j"
        actuel[col] = m.predict_proba(_preparer_x(actuel, zones))[:, 1]
        actuel.loc[actuel["niveau_actuel"] >= NIVEAU_CIBLE, col] = np.nan

    cols = ["zone", "niveau_actuel"] + [f"proba_{h}j" for h in HORIZONS if f"proba_{h}j" in actuel.columns]
    return actuel[cols].copy()
