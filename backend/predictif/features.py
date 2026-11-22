"""
Feature engineering pour le modèle prédictif sécheresse.

Corrections Phase 3 (audit 2026-11-22) :
  1. Une zone n'existe dans le jeu qu'à partir de son premier arrêté (plus de remplissage
     depuis le 1er janvier → fuite de présence supprimée).
  2. ffill(limit=10) remplace interpolate(limit=10) : aucune valeur future.
  3. Délais de disponibilité appliqués à chaque source (config.DELAIS_DISPONIBILITE).
  4. Climatologie de zone calculée uniquement sur les années passées (clim_zone).
  5. Variables départementales par zone (dep_nb_alerte, dep_une_alerte, type_sou).
  6. zone_code (ordinal) et fiabilite_reduite retirés des features du modèle.
  7. Colonne dep ajoutée au jeu pour le backtest par département.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from predictif.config import (
    ANNEE_FIN_ENTRAINEMENT,
    DELAIS_DISPONIBILITE,
    HORIZONS,
    NIVEAU_CIBLE,
    NB_STATIONS_DETAILLEES,
)

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _appliquer_delai(
    df: pd.DataFrame | None,
    col_date: str,
    delai_jours: int,
) -> pd.DataFrame | None:
    """
    Décale les dates d'un DataFrame de +delai_jours pour simuler le délai
    de publication d'une source. Les features calculées à la date t utilisent
    alors des données disponibles à t, soit des mesures de t-delai_jours.
    """
    if df is None or df.empty or delai_jours == 0:
        return df
    df = df.copy()
    df[col_date] = pd.to_datetime(df[col_date]) + pd.Timedelta(days=delai_jours)
    return df


def _climatologie_zone(s: pd.Series, idx: pd.DatetimeIndex) -> pd.Series:
    """
    Probabilité historique d'alerte par jour de l'année pour une zone,
    calculée **uniquement sur les années strictement passées** (rolling expanding).

    Méthode (backtest_honnete.py) :
      1. Pour chaque (année, jour_de_l'année) : moyenne des jours en alerte
      2. Lissage saisonnier ±15 j (rolling 31)
      3. Moyenne cumulée des années passées uniquement : expanding().shift(1)

    Retourne NaN pour les premières années (pas assez d'historique).
    """
    a = (s >= NIVEAU_CIBLE).astype(float).where(s.notna())
    doy = pd.Series(idx.dayofyear, index=idx)
    yr = pd.Series(idx.year, index=idx)

    tab = a.groupby([yr, doy]).mean().unstack()
    if tab.empty or tab.shape[0] < 2:
        return pd.Series(np.nan, index=idx)

    # Lissage saisonnier ±15 j
    tab = tab.T.rolling(31, center=True, min_periods=1).mean().T

    # Années strictement passées
    cum = tab.expanding().mean().shift(1)

    clim = cum.stack().reindex(list(zip(yr, doy)))
    clim.index = idx
    return clim


def features_locales_zone(
    zone: str,
    dep: str | None,
    df_hydro: pd.DataFrame | None,
    df_piezo: pd.DataFrame | None,
    idx: pd.DatetimeIndex,
) -> pd.DataFrame:
    """
    Variables locales propres à la zone : stations de référence ou stations proches.

    Approche cible (Phase 3, implémentation partielle) :
      1. Chercher les stations dans data/referentiel/seuils/ (valeurs `verifie`)
         associées à cette zone ou à son cours d'eau de référence.
      2. Si absent, utiliser les stations Hub'Eau du même département.
      3. Garder les variables régionales en plus, pas à la place.

    État actuel : le référentiel seuils (data/referentiel/seuils/) n'existe pas encore.
    Les géométries VigiEau ne sont pas chargées en mémoire.
    → Retourne un DataFrame vide. Les variables régionales restent actives.
    TODO : implémenter une fois data/referentiel/seuils/ disponible.
    """
    # Fallback : stations du même département (approximation en attendant les géométries)
    # Pour une implémentation rigoureuse, il faudrait :
    # - Charger les géométries VigiEau (zones_alerte.geojson) pour le polygone de la zone
    # - Trouver les stations Hub'Eau dans ou à proximité du polygone (< 50 km)
    # - Priorité aux stations marquées verifie dans le référentiel des seuils
    if dep is None or (df_hydro is None and df_piezo is None):
        return pd.DataFrame(index=idx)

    out_parts = []

    if df_hydro is not None and not df_hydro.empty and "dep" in df_hydro.columns:
        h_dep = df_hydro[df_hydro["dep"] == dep]
        if not h_dep.empty:
            feat = features_series(h_dep, "code_station", "date", "valeur", f"ql_{dep}", True, idx)
            out_parts.append(feat)

    if df_piezo is not None and not df_piezo.empty and "dep" in df_piezo.columns:
        p_dep = df_piezo[df_piezo["dep"] == dep]
        if not p_dep.empty:
            feat = features_series(p_dep, "code_bss", "date", "valeur", f"nappel_{dep}", False, idx)
            out_parts.append(feat)

    if out_parts:
        return pd.concat(out_parts, axis=1)
    return pd.DataFrame(index=idx)


# ── Anomalie et features agrégées ─────────────────────────────────────────────

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
    Utilise ffill(limit=10) : jamais de valeur future (Phase 3, item 2).
    """
    anomalies: dict[str, pd.Series] = {}
    for entite, grp in df_source.groupby(col_entite):
        # ffill : uniquement vers l'avant — aucune contamination future
        s = grp.set_index(col_date)[col_val].reindex(idx).ffill(limit=10)
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


# ── Jeu de données principal ──────────────────────────────────────────────────

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

    Paramètres
    ----------
    fiabilite_reduite : True si au moins une source est absente — stocké comme
        colonne de métadonnées, **non utilisé comme variable du modèle** (Phase 3).
    source_manquante  : noms des sources absentes (pour la transparence).
    """
    # ── 1. Filtrage département ────────────────────────────────────────────────
    df_arretes = df_arretes.copy()
    if df_arretes["dep"].notna().any():
        df_arretes = df_arretes[df_arretes["dep"].isin(depts)]

    # Mappings stables par zone (dep, type)
    zone_dep: dict[str, str] = df_arretes.groupby("zone")["dep"].first().to_dict()
    zone_type: dict[str, str] = df_arretes.groupby("zone")["type"].first().to_dict()

    # Premier arrêté connu par zone (pour supprimer les lignes avant l'existence de la zone)
    premier_arrete: dict[str, pd.Timestamp] = df_arretes.groupby("zone")["debut"].min().to_dict()

    # ── 2. Niveaux journaliers (date × zone) ─────────────────────────────────
    aujourd_hui = pd.Timestamp.today().normalize()
    idx = pd.date_range("2012-01-01", aujourd_hui, freq="D")

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

    # Phase 3, item 1 : une zone n'existe qu'à partir de son premier arrêté réel
    # (NE PAS utiliser le 1er janvier de l'année → c'était la source de la fuite)
    for z in niv.columns:
        premiere = premier_arrete.get(z)
        if premiere is not None and pd.notna(premiere):
            niv.loc[niv.index < pd.Timestamp(premiere), z] = np.nan

    logger.info(
        "Zones d'alerte : %d | période %s → %s",
        niv.shape[1], idx.min().date(), idx.max().date(),
    )

    # ── 3. Contexte départemental (autres zones du même dept en alerte) ───────
    alerte = (niv >= NIVEAU_CIBLE)

    dep_alerte_total: dict[str, pd.Series] = {}
    for dep in set(zone_dep.values()):
        zones_du_dep = [z for z in niv.columns if zone_dep.get(z) == dep]
        if zones_du_dep:
            dep_alerte_total[dep] = alerte[zones_du_dep].sum(axis=1)

    # ── 4. Délais de disponibilité (Phase 3, item 3) ─────────────────────────
    df_hydro_s = _appliquer_delai(df_hydro, "date", DELAIS_DISPONIBILITE["hubeau_hydro"])
    df_piezo_s = _appliquer_delai(df_piezo, "date", DELAIS_DISPONIBILITE["hubeau_piezo"])
    df_meteo_s = _appliquer_delai(df_meteo, "time", DELAIS_DISPONIBILITE["meteo"])

    # ── 5. Features communes (régionales) ────────────────────────────────────
    parts = []
    if df_hydro_s is not None and not df_hydro_s.empty:
        parts.append(features_series(df_hydro_s, "code_station", "date", "valeur", "q", True, idx))
    if df_piezo_s is not None and not df_piezo_s.empty:
        parts.append(features_series(df_piezo_s, "code_bss", "date", "valeur", "nappe", False, idx))
    parts.append(features_meteo(df_meteo_s, idx) if df_meteo_s is not None else pd.DataFrame(index=idx))

    commun = pd.concat(parts, axis=1) if parts else pd.DataFrame(index=idx)
    commun["jour_sin"] = np.sin(2 * np.pi * idx.dayofyear / 365.25)
    commun["jour_cos"] = np.cos(2 * np.pi * idx.dayofyear / 365.25)
    # Note : fiabilite_reduite n'est plus dans commun (retiré des features, item 6)

    # ── 6. Features par zone + targets ───────────────────────────────────────
    result_parts = []
    for z in niv.columns:
        s = niv[z]
        dep = zone_dep.get(z)
        typ = zone_type.get(z)

        d = pd.DataFrame({
            "zone": z,
            "dep": dep,                     # item 7 : colonne dep dans le jeu
            "niveau_actuel": s,
        }, index=idx)

        # Historique de la zone
        chg = (s != s.shift()).cumsum()
        d["jours_meme_niveau"] = s.groupby(chg).cumcount()
        d["niveau_max_365j"] = s.rolling(365, min_periods=1).max()

        # Type de ressource (binaire, stable — pas ordinal)
        d["type_sou"] = int(typ == "SOU") if pd.notna(typ) else 0

        # Climatologie de zone (item 5 / item 4 du prompt)
        d["clim_zone"] = _climatologie_zone(s, idx)

        # Contexte départemental (autres zones du même département en alerte)
        if dep in dep_alerte_total:
            dep_total = dep_alerte_total[dep]
            d["dep_nb_alerte"] = dep_total - alerte[z].astype(int)
            d["dep_une_alerte"] = (d["dep_nb_alerte"] > 0).astype(int)
        else:
            d["dep_nb_alerte"] = 0
            d["dep_une_alerte"] = 0

        # Targets
        for h in HORIZONS:
            futur = s[::-1].rolling(h, min_periods=h).max()[::-1].shift(-1)
            d[f"y_{h}j"] = (futur >= NIVEAU_CIBLE).astype(float).where(futur.notna())

        # Métadonnées (non utilisées comme features du modèle, item 6)
        d["fiabilite_reduite"] = int(fiabilite_reduite)

        result_parts.append(d)

    # Concat d'abord (léger), join commun une seule fois
    jeu = pd.concat(result_parts).rename_axis("date").reset_index()
    jeu = jeu[jeu["niveau_actuel"].notna()]
    jeu = jeu.join(commun, on="date")
    return jeu
