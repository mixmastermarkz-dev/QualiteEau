"""
Feature engineering pour le modèle prédictif sécheresse.
Adapté du prototype secheresse_34.py pour l'Occitanie (13 départements).
"""
from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

from predictif.config import ANNEE_FIN_ENTRAINEMENT, HORIZONS, NIVEAU_CIBLE, NB_STATIONS_DETAILLEES

logger = logging.getLogger(__name__)


def anomalie_saisonniere(serie: pd.Series, log: bool = False) -> pd.Series:
    """
    Écart standardisé à la médiane du même jour de l'année.
    Calculé uniquement sur la période d'entraînement pour éviter le data leakage.
    """
    s = serie.copy()
    if log:
        s = np.log1p(s.clip(lower=0))
    s7 = s.rolling(7, min_periods=3).mean()
    ref = s7[s7.index.year <= ANNEE_FIN_ENTRAINEMENT]
    if ref.notna().sum() < 3 * 365:
        return pd.Series(np.nan, index=s.index)
    clim = ref.groupby(ref.index.dayofyear)
    med = clim.median().reindex(range(1, 367)).interpolate(limit_direction="both")
    ec = clim.std().reindex(range(1, 367)).interpolate(limit_direction="both").replace(0, np.nan)
    doy = s7.index.dayofyear
    return (s7 - med.loc[doy].values) / ec.loc[doy].values


def features_series(
    df_source: pd.DataFrame,
    col_entite: str,
    col_date: str,
    col_val: str,
    prefixe: str,
    log: bool,
    idx: pd.DatetimeIndex,
) -> pd.DataFrame:
    """
    Calcule les features agrégées à partir d'un DataFrame de séries temporelles.
    col_entite : colonne identifiant la station/piézomètre
    """
    anomalies: dict[str, pd.Series] = {}
    for entite, grp in df_source.groupby(col_entite):
        s = grp.set_index(col_date)[col_val].reindex(idx).interpolate(limit=10)
        an = anomalie_saisonniere(s, log=log)
        if an.notna().mean() > 0.5:
            anomalies[str(entite)] = an

    if not anomalies:
        logger.warning("features_series(%s) : aucune série exploitable", prefixe)
        return pd.DataFrame(index=idx)

    m = pd.DataFrame(anomalies)
    out = pd.DataFrame(index=idx)
    out[f"{prefixe}_med"] = m.median(axis=1)
    out[f"{prefixe}_q10"] = m.quantile(0.10, axis=1)
    out[f"{prefixe}_part_sec"] = (m < -1).sum(axis=1) / m.notna().sum(axis=1)
    for k in (7, 14, 30):
        out[f"{prefixe}_med_tend{k}"] = out[f"{prefixe}_med"].diff(k)
    meilleures = m.notna().mean().sort_values(ascending=False).index[:NB_STATIONS_DETAILLEES]
    for c in meilleures:
        out[f"{prefixe}_{c}"] = m[c]
    return out


def features_meteo(df_meteo: pd.DataFrame, idx: pd.DatetimeIndex) -> pd.DataFrame:
    """Features météo agrégées sur toutes les stations météo de l'Occitanie."""
    if df_meteo is None or df_meteo.empty:
        logger.warning("features_meteo : données absentes")
        return pd.DataFrame(index=idx)

    df_meteo = df_meteo.copy()
    df_meteo["time"] = pd.to_datetime(df_meteo["time"])
    m = df_meteo.groupby("time")[["precipitation_sum", "et0_fao_evapotranspiration", "temperature_2m_max"]].mean()
    m = m.reindex(idx).ffill(limit=7)

    p = m["precipitation_sum"]
    e = m["et0_fao_evapotranspiration"]
    t = m["temperature_2m_max"]

    out = pd.DataFrame(index=idx)
    for k in (15, 30, 60, 90, 180, 365):
        out[f"pluie_{k}j"] = p.rolling(k, min_periods=int(k * .8)).sum()
        out[f"bilan_{k}j"] = (p - e).rolling(k, min_periods=int(k * .8)).sum()
    out["tmax_7j"] = t.rolling(7).mean()

    for c in ["pluie_30j", "pluie_90j", "bilan_90j", "pluie_365j"]:
        if c not in out:
            continue
        doy = idx.dayofyear
        ref = out[c][idx.year <= ANNEE_FIN_ENTRAINEMENT]
        med = (ref.groupby(ref.index.dayofyear).median()
               .reindex(range(1, 367)).interpolate(limit_direction="both"))
        out[f"{c}_anom"] = out[c] - med.loc[doy].values

    out["jours_sans_pluie"] = (p < 1).astype(int).groupby((p >= 1).cumsum()).cumsum()
    return out


def construire_jeu(
    df_arretes: pd.DataFrame,
    df_hydro: pd.DataFrame | None,
    df_piezo: pd.DataFrame | None,
    df_meteo: pd.DataFrame | None,
    depts: list[str],
    fiabilite_reduite: bool = False,
    source_manquante: str | None = None,
) -> pd.DataFrame:
    """
    Construit le jeu de données complet (features + targets) pour l'entraînement ou la prédiction.

    fiabilite_reduite : True si au moins une source est absente
    source_manquante : nom de la source absente (pour la transparence)
    """
    from predictif.sources.vigeau import _sans_accents

    # --- Niveaux journaliers par zone (depuis les arrêtés)
    df_arretes = df_arretes.copy()
    if df_arretes["dep"].notna().any():
        df_arretes = df_arretes[df_arretes["dep"].isin(depts)]

    aujourd_hui = pd.Timestamp.today().normalize()
    idx = pd.date_range("2012-01-01", aujourd_hui, freq="D")

    # Tableau date x zone du niveau maximal en vigueur
    lignes = []
    for r in df_arretes.itertuples():
        fin = r.fin if pd.notna(r.fin) else min(r.debut + pd.Timedelta(days=120), aujourd_hui)
        fin = min(fin, r.debut + pd.Timedelta(days=366))
        for d in pd.date_range(r.debut.normalize(), fin.normalize(), freq="D"):
            lignes.append((d, r.zone, r.niveau))
    if not lignes:
        logger.error("Aucun arrêté après filtrage département")
        return pd.DataFrame()

    j = pd.DataFrame(lignes, columns=["date", "zone", "niveau"])
    niv = j.groupby(["date", "zone"])["niveau"].max().unstack("zone").reindex(idx).fillna(0)
    for z in niv.columns:
        premiere = niv.index[niv[z] > 0].min()
        if pd.notna(premiere):
            niv.loc[niv.index < pd.Timestamp(premiere.year, 1, 1), z] = np.nan

    logger.info("Zones d'alerte : %d | période %s → %s", niv.shape[1], idx.min().date(), idx.max().date())

    # --- Features communes
    parts = []
    if df_hydro is not None and not df_hydro.empty:
        parts.append(features_series(df_hydro, "code_station", "date", "valeur", "q", True, idx))
    if df_piezo is not None and not df_piezo.empty:
        parts.append(features_series(df_piezo, "code_bss", "date", "valeur", "nappe", False, idx))
    parts.append(features_meteo(df_meteo, idx) if df_meteo is not None else pd.DataFrame(index=idx))

    commun = pd.concat(parts, axis=1) if parts else pd.DataFrame(index=idx)
    commun["dep_niveau_max"] = niv.max(axis=1)
    commun["dep_nb_zones_alerte"] = (niv >= NIVEAU_CIBLE).sum(axis=1)
    commun["jour_sin"] = np.sin(2 * np.pi * idx.dayofyear / 365.25)
    commun["jour_cos"] = np.cos(2 * np.pi * idx.dayofyear / 365.25)
    commun["fiabilite_reduite"] = int(fiabilite_reduite)

    # --- Features par zone + targets
    result_parts = []
    for z in niv.columns:
        s = niv[z]
        d = pd.DataFrame({"zone": z, "niveau_actuel": s}, index=idx)
        chg = (s != s.shift()).cumsum()
        d["jours_meme_niveau"] = s.groupby(chg).cumcount()
        d["niveau_max_365j"] = s.rolling(365, min_periods=1).max()
        for h in HORIZONS:
            futur = s[::-1].rolling(h, min_periods=h).max()[::-1].shift(-1)
            d[f"y_{h}j"] = (futur >= NIVEAU_CIBLE).astype(float).where(futur.notna())
        result_parts.append(d.join(commun))

    jeu = pd.concat(result_parts).rename_axis("date").reset_index()
    return jeu[jeu["niveau_actuel"].notna()]
