#!/usr/bin/env python3
"""
Prototype : anticiper les arrêtés préfectoraux sécheresse dans l'Hérault (34).

Sources (toutes ouvertes, sans clé) :
  - Arrêtés VigiEau / Propluvia (data.gouv.fr)   -> ce qu'on veut prédire
  - Hub'Eau Hydrométrie (débits moyens journaliers QmnJ)
  - Hub'Eau Piézométrie (niveaux des nappes, BRGM)
  - Open-Meteo archive (pluie, évapotranspiration, température)

Usage :
  pip install pandas numpy scikit-learn requests
  python secheresse_34.py telecharger   # ~10-20 min la première fois (cache local)
  python secheresse_34.py inspecter     # vérifie les colonnes des fichiers d'arrêtés
  python secheresse_34.py entrainer     # construit le jeu de données, entraîne, évalue
  python secheresse_34.py predire       # probabilités d'arrêté à 7/15/30 jours par zone
  python secheresse_34.py tout          # les quatre à la suite
"""
from __future__ import annotations

import io
import json
import sys
import time
import unicodedata
import zipfile
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
DEPT = "34"
DEPTS_VOISINS = ["30", "11", "12", "81"]   # Gard, Aude, Aveyron, Tarn : leurs décisions pèsent
DEBUT = "2012-01-01"
ANNEE_FIN_ENTRAINEMENT = 2022             # entraînement <= 2022, test >= 2023
HORIZONS = [7, 15, 30]                    # jours
NIVEAU_CIBLE = 2                          # 1 vigilance, 2 alerte, 3 alerte renforcée, 4 crise
SEUIL_PROBA = 0.5                         # pour le calcul du délai d'anticipation

# Laisser vide = toutes les stations en service du département.
# Mieux : y mettre les stations de référence de l'arrêté-cadre (codes Hub'Eau).
STATIONS_HYDRO: list[str] = []
PIEZOMETRES: list[str] = []
NB_STATIONS_DETAILLEES = 25               # stations gardées comme variables individuelles

# Points météo répartis sur le département (lat, lon)
POINTS_METEO = {
    "montpellier": (43.61, 3.88),
    "beziers": (43.34, 3.21),
    "lodeve": (43.73, 3.32),
    "st_pons": (43.49, 2.77),
    "ganges": (43.93, 3.71),
}

DOSSIER = Path("donnees_34")
CACHE = DOSSIER / "cache"
HUBEAU = "https://hubeau.eaufrance.fr/api"
DATAGOUV_DATASET = "https://www.data.gouv.fr/api/1/datasets/donnee-secheresse-vigieau/"

SESSION = requests.Session()
SESSION.headers["User-Agent"] = "prototype-secheresse-34/0.1"


# --------------------------------------------------------------------------- #
# Utilitaires
# --------------------------------------------------------------------------- #
def sans_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(c for c in s if not unicodedata.combining(c)).lower().strip()


def get_json(url: str, params: dict | None = None, essais: int = 4) -> dict:
    for i in range(essais):
        try:
            r = SESSION.get(url, params=params, timeout=90)
            if r.status_code in (200, 206):
                return r.json()
            if r.status_code == 404:
                return {}
            print(f"  HTTP {r.status_code} sur {r.url[:120]}")
        except requests.RequestException as e:
            print(f"  erreur réseau ({e.__class__.__name__}), nouvel essai…")
        time.sleep(2 * (i + 1))
    raise RuntimeError(f"Échec après {essais} essais : {url}")


def hubeau_pages(url: str, params: dict) -> list[dict]:
    """Suit la pagination Hub'Eau (champ 'next')."""
    out, page = [], get_json(url, params)
    while page:
        out.extend(page.get("data", []))
        nxt = page.get("next")
        if not nxt:
            break
        page = get_json(nxt)
    return out


def niveau_numerique(v) -> float:
    if pd.isna(v):
        return np.nan
    s = sans_accents(v).replace("_", " ")
    if s.isdigit():
        return float(s)
    if "crise" in s:
        return 4
    if "renforc" in s:
        return 3
    if "alerte" in s:
        return 2
    if "vigilance" in s:
        return 1
    if "absence" in s or "aucun" in s or "normal" in s:
        return 0
    return np.nan


def trouver_colonne(cols, inclut, exclut=()) -> str | None:
    for c in cols:
        cn = sans_accents(c)
        if all(m in cn for m in inclut) and not any(m in cn for m in exclut):
            return c
    return None


# --------------------------------------------------------------------------- #
# 1. Téléchargements
# --------------------------------------------------------------------------- #
def telecharger_arretes():
    dest = CACHE / "arretes"
    dest.mkdir(parents=True, exist_ok=True)
    meta = get_json(DATAGOUV_DATASET)
    ressources = meta.get("resources", [])
    print(f"VigiEau : {len(ressources)} ressources sur data.gouv.fr")
    inventaire = []
    for r in ressources:
        titre, url, fmt = r.get("title", ""), r.get("url", ""), (r.get("format") or "").lower()
        t = sans_accents(titre)
        inventaire.append({"titre": titre, "format": fmt, "url": url})
        if "arr" not in t or fmt not in ("csv", "zip"):
            continue
        nom = dest / (sans_accents(titre).replace(" ", "_").replace("/", "-")[:80] + "." + fmt)
        print(f"  ↓ {titre}")
        contenu = SESSION.get(url, timeout=300).content
        if fmt == "zip":
            with zipfile.ZipFile(io.BytesIO(contenu)) as z:
                for n in z.namelist():
                    if n.lower().endswith(".csv"):
                        (dest / (nom.stem + "__" + Path(n).name)).write_bytes(z.read(n))
        else:
            nom.write_bytes(contenu)
    pd.DataFrame(inventaire).to_csv(CACHE / "inventaire_vigieau.csv", index=False)


def telecharger_hydro():
    dest = CACHE / "hydro"
    dest.mkdir(parents=True, exist_ok=True)
    stations = hubeau_pages(f"{HUBEAU}/v2/hydrometrie/referentiel/stations",
                            {"code_departement": DEPT, "en_service": "true", "size": 1000})
    df = pd.DataFrame(stations)
    df.to_csv(CACHE / "stations_hydro.csv", index=False)
    codes = STATIONS_HYDRO or df["code_station"].dropna().unique().tolist()
    print(f"Hydrométrie : {len(codes)} stations")
    for code in codes:
        f = dest / f"{code}.csv"
        if f.exists():
            continue
        obs = hubeau_pages(f"{HUBEAU}/v2/hydrometrie/obs_elab",
                           {"code_entite": code, "grandeur_hydro_elab": "QmnJ",
                            "date_debut_obs_elab": DEBUT, "size": 20000,
                            "fields": "date_obs_elab,resultat_obs_elab"})
        pd.DataFrame(obs).to_csv(f, index=False)
        print(f"  {code} : {len(obs)} jours")


def telecharger_piezo():
    dest = CACHE / "piezo"
    dest.mkdir(parents=True, exist_ok=True)
    stations = hubeau_pages(f"{HUBEAU}/v1/niveaux_nappes/stations",
                            {"code_departement": DEPT, "size": 5000})
    df = pd.DataFrame(stations)
    df.to_csv(CACHE / "stations_piezo.csv", index=False)
    if PIEZOMETRES:
        codes = PIEZOMETRES
    else:
        fin = pd.to_datetime(df.get("date_fin_mesure"), errors="coerce")
        deb = pd.to_datetime(df.get("date_debut_mesure"), errors="coerce")
        ok = (fin > pd.Timestamp.today() - pd.Timedelta(days=90)) & (deb < pd.Timestamp("2016-01-01"))
        codes = df.loc[ok, "code_bss"].dropna().unique().tolist()
    print(f"Piézométrie : {len(codes)} piézomètres actifs avec historique")
    for code in codes:
        f = dest / f"{code.replace('/', '_')}.csv"
        if f.exists():
            continue
        obs = hubeau_pages(f"{HUBEAU}/v1/niveaux_nappes/chroniques",
                           {"code_bss": code, "date_debut_mesure": DEBUT, "size": 20000,
                            "fields": "date_mesure,niveau_nappe_eau"})
        pd.DataFrame(obs).to_csv(f, index=False)
        print(f"  {code} : {len(obs)} mesures")


def telecharger_meteo():
    dest = CACHE / "meteo"
    dest.mkdir(parents=True, exist_ok=True)
    fin = (date.today() - timedelta(days=2)).isoformat()
    for nom, (lat, lon) in POINTS_METEO.items():
        d = get_json("https://archive-api.open-meteo.com/v1/archive", {
            "latitude": lat, "longitude": lon, "start_date": DEBUT, "end_date": fin,
            "daily": "precipitation_sum,et0_fao_evapotranspiration,temperature_2m_max",
            "timezone": "Europe/Paris"})
        pd.DataFrame(d["daily"]).to_csv(dest / f"{nom}.csv", index=False)
        print(f"Météo {nom} : {len(d['daily']['time'])} jours")


def telecharger():
    for etape in (telecharger_arretes, telecharger_hydro, telecharger_piezo, telecharger_meteo):
        etape()


# --------------------------------------------------------------------------- #
# 2. Arrêtés -> niveau d'alerte journalier par zone
# --------------------------------------------------------------------------- #
def lire_arretes(verbeux=False) -> pd.DataFrame:
    morceaux = []
    for f in sorted((CACHE / "arretes").glob("*.csv")):
        df = None
        for sep in (",", ";"):
            try:
                d = pd.read_csv(f, sep=sep, dtype=str, low_memory=False)
                if d.shape[1] > 3:
                    df = d
                    break
            except Exception:
                pass
        if df is None:
            continue
        cols = list(df.columns)
        c = {
            "debut": trouver_colonne(cols, ["debut"], ["cadre"]),
            "fin": trouver_colonne(cols, ["fin"], ["cadre", "fichier"]),
            "niveau": trouver_colonne(cols, ["niveau"]) or trouver_colonne(cols, ["gravite"]),
            "dep": trouver_colonne(cols, ["dep"], ["date"]),
            "zone": trouver_colonne(cols, ["nom", "zone"]) or trouver_colonne(cols, ["zone"]),
        }
        if verbeux:
            print(f"\n{f.name} : {len(df)} lignes\n  colonnes : {cols}\n  détectées : {c}")
        if not (c["debut"] and c["niveau"] and c["zone"]):
            continue
        out = pd.DataFrame({
            "debut": pd.to_datetime(df[c["debut"]], errors="coerce", dayfirst=False),
            "fin": pd.to_datetime(df[c["fin"]], errors="coerce") if c["fin"] else pd.NaT,
            "niveau": df[c["niveau"]].map(niveau_numerique),
            "dep": df[c["dep"]].astype(str).str.extract(r"(\d{1,3}|2A|2B)")[0].str.zfill(2)
                   if c["dep"] else None,
            "zone": df[c["zone"]].map(sans_accents),
        })
        morceaux.append(out)
    if not morceaux:
        sys.exit("Aucun fichier d'arrêtés exploitable : lancez 'inspecter' et ajustez trouver_colonne().")
    a = pd.concat(morceaux, ignore_index=True).dropna(subset=["debut", "niveau", "zone"])
    if a["dep"].isna().all():
        print("⚠ Pas de colonne département trouvée : filtrage impossible, toutes zones gardées.")
    return a.drop_duplicates()


def niveaux_journaliers(a: pd.DataFrame, deps: list[str]) -> pd.DataFrame:
    """Retourne un tableau date x zone du niveau maximal en vigueur."""
    a = a[a["dep"].isin(deps)] if a["dep"].notna().any() else a
    aujourd_hui = pd.Timestamp.today().normalize()
    idx = pd.date_range(DEBUT, aujourd_hui, freq="D")
    if a.empty:
        return pd.DataFrame(index=idx)
    lignes = []
    for r in a.itertuples():
        fin = r.fin if pd.notna(r.fin) else min(r.debut + pd.Timedelta(days=120), aujourd_hui)
        fin = min(fin, r.debut + pd.Timedelta(days=366))
        for d in pd.date_range(r.debut.normalize(), fin.normalize(), freq="D"):
            lignes.append((d, r.zone, r.niveau))
    j = pd.DataFrame(lignes, columns=["date", "zone", "niveau"])
    j = j.groupby(["date", "zone"])["niveau"].max().unstack("zone")
    j = j.reindex(idx).fillna(0)
    # une zone n'existe qu'à partir du 1er janvier de sa première apparition
    for z in j.columns:
        premiere = j.index[j[z] > 0].min()
        if pd.notna(premiere):
            j.loc[j.index < pd.Timestamp(premiere.year, 1, 1), z] = np.nan
    return j


# --------------------------------------------------------------------------- #
# 3. Variables explicatives (département entier)
# --------------------------------------------------------------------------- #
def anomalie_saisonniere(serie: pd.Series, log=False) -> pd.Series:
    """Écart à la médiane du même jour de l'année, calculée sur la période d'entraînement
    seulement (évite que l'avenir fuie dans les variables)."""
    s = serie.copy()
    if log:
        s = np.log1p(s.clip(lower=0))
    s7 = s.rolling(7, min_periods=3).mean()
    doy = s7.index.dayofyear
    ref = s7[s7.index.year <= ANNEE_FIN_ENTRAINEMENT]
    if ref.notna().sum() < 3 * 365:
        return pd.Series(np.nan, index=s.index)
    clim = ref.groupby(ref.index.dayofyear)
    med = clim.median().reindex(range(1, 367)).interpolate(limit_direction="both")
    ec = clim.std().reindex(range(1, 367)).interpolate(limit_direction="both").replace(0, np.nan)
    return (s7 - med.loc[doy].values) / ec.loc[doy].values


def variables_series(dossier: Path, col_date: str, col_val: str, prefixe: str, log: bool,
                     idx: pd.DatetimeIndex) -> pd.DataFrame:
    anomalies = {}
    for f in sorted(dossier.glob("*.csv")):
        try:
            d = pd.read_csv(f)
        except Exception:
            continue
        if d.empty or col_date not in d or col_val not in d:
            continue
        s = (d.assign(date=pd.to_datetime(d[col_date]).dt.tz_localize(None).dt.normalize())
               .groupby("date")[col_val].mean().reindex(idx))
        s = s.interpolate(limit=10)
        an = anomalie_saisonniere(s, log=log)
        if an.notna().mean() > 0.5:
            anomalies[f.stem] = an
    if not anomalies:
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


def variables_meteo(idx: pd.DatetimeIndex) -> pd.DataFrame:
    tables = []
    for f in sorted((CACHE / "meteo").glob("*.csv")):
        d = pd.read_csv(f, parse_dates=["time"]).set_index("time")
        tables.append(d)
    if not tables:
        return pd.DataFrame(index=idx)
    m = pd.concat(tables).groupby(level=0).mean().reindex(idx).ffill(limit=7)
    p, e, t = m["precipitation_sum"], m["et0_fao_evapotranspiration"], m["temperature_2m_max"]
    out = pd.DataFrame(index=idx)
    for k in (15, 30, 60, 90, 180, 365):
        out[f"pluie_{k}j"] = p.rolling(k, min_periods=int(k * .8)).sum()
        out[f"bilan_{k}j"] = (p - e).rolling(k, min_periods=int(k * .8)).sum()
    out["tmax_7j"] = t.rolling(7).mean()
    for c in ["pluie_30j", "pluie_90j", "bilan_90j", "pluie_365j"]:
        doy = idx.dayofyear
        ref = out[c][idx.year <= ANNEE_FIN_ENTRAINEMENT]
        med = ref.groupby(ref.index.dayofyear).median().reindex(range(1, 367)).interpolate(limit_direction="both")
        out[f"{c}_anom"] = out[c] - med.loc[doy].values
    out["jours_sans_pluie"] = (p < 1).astype(int).groupby((p >= 1).cumsum()).cumsum()
    return out


def construire_jeu():
    a = lire_arretes()
    niv = niveaux_journaliers(a, [DEPT])
    idx = niv.index
    print(f"Zones d'alerte du {DEPT} : {niv.shape[1]}  |  période {idx.min().date()} → {idx.max().date()}")

    commun = pd.concat([
        variables_series(CACHE / "hydro", "date_obs_elab", "resultat_obs_elab", "q", True, idx),
        variables_series(CACHE / "piezo", "date_mesure", "niveau_nappe_eau", "nappe", False, idx),
        variables_meteo(idx),
    ], axis=1)
    commun["dep_niveau_max"] = niv.max(axis=1)
    commun["dep_nb_zones_alerte"] = (niv >= NIVEAU_CIBLE).sum(axis=1)
    voisins = niveaux_journaliers(a, DEPTS_VOISINS) if a["dep"].notna().any() else pd.DataFrame(index=idx)
    commun["voisins_niveau_max"] = voisins.max(axis=1).fillna(0) if voisins.shape[1] else 0
    commun["voisins_nb_zones_alerte"] = (voisins >= NIVEAU_CIBLE).sum(axis=1) if voisins.shape[1] else 0
    commun["jour_sin"] = np.sin(2 * np.pi * idx.dayofyear / 365.25)
    commun["jour_cos"] = np.cos(2 * np.pi * idx.dayofyear / 365.25)

    lignes = []
    for z in niv.columns:
        s = niv[z]
        d = pd.DataFrame({"zone": z, "niveau_actuel": s}, index=idx)
        chg = (s != s.shift()).cumsum()
        d["jours_meme_niveau"] = s.groupby(chg).cumcount()
        d["niveau_max_365j"] = s.rolling(365, min_periods=1).max()
        for h in HORIZONS:
            futur = s[::-1].rolling(h, min_periods=h).max()[::-1].shift(-1)
            d[f"y_{h}j"] = (futur >= NIVEAU_CIBLE).astype(float).where(futur.notna())
        lignes.append(d.join(commun))
    jeu = pd.concat(lignes).rename_axis("date").reset_index()
    jeu = jeu[jeu["niveau_actuel"].notna()]
    jeu.to_parquet(DOSSIER / "jeu_donnees.parquet") if _parquet_ok() else jeu.to_csv(DOSSIER / "jeu_donnees.csv", index=False)
    return jeu


def _parquet_ok():
    try:
        import pyarrow  # noqa: F401
        return True
    except ImportError:
        return False


# --------------------------------------------------------------------------- #
# 4. Modèle
# --------------------------------------------------------------------------- #
def colonnes_x(jeu):
    return [c for c in jeu.columns if c not in ("date", "zone") and not c.startswith("y_")]


def preparer_x(jeu, zones):
    X = jeu[colonnes_x(jeu)].copy()
    X.insert(0, "zone_code", jeu["zone"].map({z: i for i, z in enumerate(zones)}).astype(float))
    return X


def nouveau_modele():
    from sklearn.ensemble import HistGradientBoostingClassifier
    return HistGradientBoostingClassifier(
        max_iter=400, learning_rate=0.05, max_leaf_nodes=31, min_samples_leaf=40,
        l2_regularization=1.0, categorical_features=[0], random_state=0)


def entrainer(jeu=None):
    from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score
    jeu = construire_jeu() if jeu is None else jeu
    zones = sorted(jeu["zone"].unique())
    rapport = {}
    jeu_eval = jeu[jeu["niveau_actuel"] < NIVEAU_CIBLE]   # on prédit le *déclenchement*
    annee = jeu_eval["date"].dt.year
    for h in HORIZONS:
        y = jeu_eval[f"y_{h}j"]
        ok = y.notna()
        tr, te = ok & (annee <= ANNEE_FIN_ENTRAINEMENT), ok & (annee > ANNEE_FIN_ENTRAINEMENT)
        if y[tr].nunique() < 2 or y[te].nunique() < 2:
            print(f"Horizon {h} j : pas assez d'événements pour évaluer.")
            continue
        m = nouveau_modele().fit(preparer_x(jeu_eval[tr], zones), y[tr])
        p = m.predict_proba(preparer_x(jeu_eval[te], zones))[:, 1]
        r = {"auc": roc_auc_score(y[te], p), "precision_moyenne": average_precision_score(y[te], p),
             "brier": brier_score_loss(y[te], p), "taux_base": float(y[te].mean()),
             "n_test": int(te.sum())}
        if h == max(HORIZONS):
            r["delais_anticipation"] = delais(jeu, jeu_eval[te][["zone", "date"]].assign(p=p))
        rapport[f"{h}j"] = r
        print(f"Horizon {h:>2} j | AUC {r['auc']:.3f} | précision moy. {r['precision_moyenne']:.3f} "
              f"(hasard = {r['taux_base']:.3f}) | Brier {r['brier']:.3f}")
    if f"{max(HORIZONS)}j" in rapport:
        dl = rapport[f"{max(HORIZONS)}j"]["delais_anticipation"]
        print(f"\nDéclenchements testés : {dl['n_evenements']} | anticipés : {dl['n_anticipes']} | "
              f"délai médian : {dl['delai_median_j']} j")
    (DOSSIER / "rapport_evaluation.json").write_text(json.dumps(rapport, indent=2, ensure_ascii=False, default=float))
    return jeu


def delais(jeu: pd.DataFrame, pred: pd.DataFrame) -> dict:
    """Pour chaque passage en alerte (niveau < cible la veille, >= cible le jour J) pendant la
    période de test : depuis combien de jours la probabilité à 30 j était-elle au-dessus de
    SEUIL_PROBA sans interruption ? 0 = pas anticipé."""
    p = pred.set_index(["zone", "date"])["p"]
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
    return {"n_evenements": len(res), "n_anticipes": len(anticipes),
            "delai_median_j": float(np.median(anticipes)) if anticipes else None}


def predire(jeu=None):
    if jeu is None:
        f = DOSSIER / "jeu_donnees.parquet"
        jeu = pd.read_parquet(f) if f.exists() else pd.read_csv(DOSSIER / "jeu_donnees.csv", parse_dates=["date"])
    zones = sorted(jeu["zone"].unique())
    # dernier jour où les variables hydrologiques sont disponibles
    cle = [c for c in ("q_med", "pluie_30j") if c in jeu]
    dispo = jeu.dropna(subset=cle) if cle else jeu
    jour = dispo["date"].max()
    actuel = jeu[jeu["date"] == jour].copy()
    base = jeu[jeu["niveau_actuel"] < NIVEAU_CIBLE]
    for h in HORIZONS:
        y = base[f"y_{h}j"]
        m = nouveau_modele().fit(preparer_x(base[y.notna()], zones), y[y.notna()])
        actuel[f"proba_{h}j"] = m.predict_proba(preparer_x(actuel, zones))[:, 1]
        actuel.loc[actuel["niveau_actuel"] >= NIVEAU_CIBLE, f"proba_{h}j"] = np.nan
    cols = ["zone", "niveau_actuel"] + [f"proba_{h}j" for h in HORIZONS]
    res = actuel[cols].sort_values(f"proba_{max(HORIZONS)}j", ascending=False)
    res.to_csv(DOSSIER / f"predictions_{jour.date()}.csv", index=False)
    print(f"\nSituation au {jour.date()} — probabilité de passer au moins en ALERTE :")
    noms = {0: "aucun", 1: "vigilance", 2: "alerte", 3: "alerte renf.", 4: "crise"}
    aff = res.assign(niveau_actuel=res["niveau_actuel"].map(noms))
    for h in HORIZONS:
        aff[f"proba_{h}j"] = aff[f"proba_{h}j"].map(lambda v: "déjà" if pd.isna(v) else f"{v:.0%}")
    print(aff.to_string(index=False))
    return res


# --------------------------------------------------------------------------- #
def main():
    DOSSIER.mkdir(exist_ok=True)
    cmd = sys.argv[1] if len(sys.argv) > 1 else "tout"
    if cmd == "telecharger":
        telecharger()
    elif cmd == "inspecter":
        lire_arretes(verbeux=True)
    elif cmd == "entrainer":
        entrainer()
    elif cmd == "predire":
        predire()
    elif cmd == "tout":
        telecharger()
        predire(entrainer())
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
