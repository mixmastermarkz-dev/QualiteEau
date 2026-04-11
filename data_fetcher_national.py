#!/usr/bin/env python3
"""
data_fetcher_national.py — Récupération nationale des données environnementales.
Écrit data/dept/XX.json et data/index.json.
⚠️  Ne touche JAMAIS à full_data.json ni aux fichiers du pipeline existant.

Usage :
  python data_fetcher_national.py --depts 34,30,31,13,75  # depts spécifiques
  python data_fetcher_national.py --batch 1               # batch 1/4 (≈ depts 01-24)
  python data_fetcher_national.py --batch 2               # batch 2/4 (≈ depts 25-48)
  python data_fetcher_national.py --batch 3               # batch 3/4 (≈ depts 49-72)
  python data_fetcher_national.py --batch 4               # batch 4/4 (≈ depts 73-95)
  python data_fetcher_national.py --generate-index-only   # régénère data/index.json
  python data_fetcher_national.py --outdir /tmp/out ...   # répertoire de sortie (CI)

Optimisations vs version précédente :
  - Constantes et utilitaires centralisés dans shared.py (DRY)
  - get_json avec retry (2 tentatives) importé depuis shared.py
  - Nappes fetchées en parallèle par station (ThreadPoolExecutor)
  - Depts traités en parallèle au sein d'un batch (ThreadPoolExecutor)
  - Guard pagination : détecte si Hub'Eau a tronqué les résultats (count > size)
  - Cache WFS Atmo Occitanie thread-safe (verrou)
"""
import os
import json
import argparse
import threading
from datetime import datetime, timedelta
from urllib.parse import quote
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

from shared import (
    PARAMS_POTABLE, PARAMS_RIVIERE,
    QUAL_LABELS, QUAL_COLORS, SUB_PARAMS, OCCITANIE_DEPTS,
    POL_LABELS, POL_COLORS, POLLEN_TAXA,
    get_json, get_color, calc_score, score_color, score_label,
    parse_result, encode_bss, extract_nom_cours_eau,
)

WFS_NATIONAL_AIR = (
    "https://data.atmo-france.org/geoserver/ind/ows"
    "?service=WFS&version=2.0.0&request=GetFeature"
    "&TypeNames=ind:ind_atmo&outputFormat=application%2Fjson"
    "&sortBy=date_ech+D"
)

BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
DATA_DEPT_DIR = os.path.join(BASE_DIR, "data", "dept")
INDEX_PATH    = os.path.join(BASE_DIR, "data", "index.json")

# ---------------------------------------------------------------------------
# RÉFÉRENTIEL — 96 DÉPARTEMENTS MÉTROPOLITAINS
# ---------------------------------------------------------------------------
DEPARTEMENTS = {
    "01": {"nom": "Ain",                        "slug": "ain",                       "region": "Auvergne-Rhône-Alpes"},
    "02": {"nom": "Aisne",                      "slug": "aisne",                     "region": "Hauts-de-France"},
    "03": {"nom": "Allier",                     "slug": "allier",                    "region": "Auvergne-Rhône-Alpes"},
    "04": {"nom": "Alpes-de-Haute-Provence",    "slug": "alpes-de-haute-provence",   "region": "Provence-Alpes-Côte d'Azur"},
    "05": {"nom": "Hautes-Alpes",               "slug": "hautes-alpes",              "region": "Provence-Alpes-Côte d'Azur"},
    "06": {"nom": "Alpes-Maritimes",            "slug": "alpes-maritimes",           "region": "Provence-Alpes-Côte d'Azur"},
    "07": {"nom": "Ardèche",                    "slug": "ardeche",                   "region": "Auvergne-Rhône-Alpes"},
    "08": {"nom": "Ardennes",                   "slug": "ardennes",                  "region": "Grand Est"},
    "09": {"nom": "Ariège",                     "slug": "ariege",                    "region": "Occitanie"},
    "10": {"nom": "Aube",                       "slug": "aube",                      "region": "Grand Est"},
    "11": {"nom": "Aude",                       "slug": "aude",                      "region": "Occitanie"},
    "12": {"nom": "Aveyron",                    "slug": "aveyron",                   "region": "Occitanie"},
    "13": {"nom": "Bouches-du-Rhône",           "slug": "bouches-du-rhone",          "region": "Provence-Alpes-Côte d'Azur"},
    "14": {"nom": "Calvados",                   "slug": "calvados",                  "region": "Normandie"},
    "15": {"nom": "Cantal",                     "slug": "cantal",                    "region": "Auvergne-Rhône-Alpes"},
    "16": {"nom": "Charente",                   "slug": "charente",                  "region": "Nouvelle-Aquitaine"},
    "17": {"nom": "Charente-Maritime",          "slug": "charente-maritime",         "region": "Nouvelle-Aquitaine"},
    "18": {"nom": "Cher",                       "slug": "cher",                      "region": "Centre-Val de Loire"},
    "19": {"nom": "Corrèze",                    "slug": "correze",                   "region": "Nouvelle-Aquitaine"},
    "2A": {"nom": "Corse-du-Sud",               "slug": "corse-du-sud",              "region": "Corse"},
    "2B": {"nom": "Haute-Corse",                "slug": "haute-corse",               "region": "Corse"},
    "21": {"nom": "Côte-d'Or",                  "slug": "cote-d-or",                 "region": "Bourgogne-Franche-Comté"},
    "22": {"nom": "Côtes-d'Armor",              "slug": "cotes-d-armor",             "region": "Bretagne"},
    "23": {"nom": "Creuse",                     "slug": "creuse",                    "region": "Nouvelle-Aquitaine"},
    "24": {"nom": "Dordogne",                   "slug": "dordogne",                  "region": "Nouvelle-Aquitaine"},
    "25": {"nom": "Doubs",                      "slug": "doubs",                     "region": "Bourgogne-Franche-Comté"},
    "26": {"nom": "Drôme",                      "slug": "drome",                     "region": "Auvergne-Rhône-Alpes"},
    "27": {"nom": "Eure",                       "slug": "eure",                      "region": "Normandie"},
    "28": {"nom": "Eure-et-Loir",               "slug": "eure-et-loir",              "region": "Centre-Val de Loire"},
    "29": {"nom": "Finistère",                  "slug": "finistere",                 "region": "Bretagne"},
    "30": {"nom": "Gard",                       "slug": "gard",                      "region": "Occitanie"},
    "31": {"nom": "Haute-Garonne",              "slug": "haute-garonne",             "region": "Occitanie"},
    "32": {"nom": "Gers",                       "slug": "gers",                      "region": "Occitanie"},
    "33": {"nom": "Gironde",                    "slug": "gironde",                   "region": "Nouvelle-Aquitaine"},
    "34": {"nom": "Hérault",                    "slug": "herault",                   "region": "Occitanie"},
    "35": {"nom": "Ille-et-Vilaine",            "slug": "ille-et-vilaine",           "region": "Bretagne"},
    "36": {"nom": "Indre",                      "slug": "indre",                     "region": "Centre-Val de Loire"},
    "37": {"nom": "Indre-et-Loire",             "slug": "indre-et-loire",            "region": "Centre-Val de Loire"},
    "38": {"nom": "Isère",                      "slug": "isere",                     "region": "Auvergne-Rhône-Alpes"},
    "39": {"nom": "Jura",                       "slug": "jura",                      "region": "Bourgogne-Franche-Comté"},
    "40": {"nom": "Landes",                     "slug": "landes",                    "region": "Nouvelle-Aquitaine"},
    "41": {"nom": "Loir-et-Cher",               "slug": "loir-et-cher",              "region": "Centre-Val de Loire"},
    "42": {"nom": "Loire",                      "slug": "loire",                     "region": "Auvergne-Rhône-Alpes"},
    "43": {"nom": "Haute-Loire",                "slug": "haute-loire",               "region": "Auvergne-Rhône-Alpes"},
    "44": {"nom": "Loire-Atlantique",           "slug": "loire-atlantique",          "region": "Pays de la Loire"},
    "45": {"nom": "Loiret",                     "slug": "loiret",                    "region": "Centre-Val de Loire"},
    "46": {"nom": "Lot",                        "slug": "lot",                       "region": "Occitanie"},
    "47": {"nom": "Lot-et-Garonne",             "slug": "lot-et-garonne",            "region": "Nouvelle-Aquitaine"},
    "48": {"nom": "Lozère",                     "slug": "lozere",                    "region": "Occitanie"},
    "49": {"nom": "Maine-et-Loire",             "slug": "maine-et-loire",            "region": "Pays de la Loire"},
    "50": {"nom": "Manche",                     "slug": "manche",                    "region": "Normandie"},
    "51": {"nom": "Marne",                      "slug": "marne",                     "region": "Grand Est"},
    "52": {"nom": "Haute-Marne",                "slug": "haute-marne",               "region": "Grand Est"},
    "53": {"nom": "Mayenne",                    "slug": "mayenne",                   "region": "Pays de la Loire"},
    "54": {"nom": "Meurthe-et-Moselle",         "slug": "meurthe-et-moselle",        "region": "Grand Est"},
    "55": {"nom": "Meuse",                      "slug": "meuse",                     "region": "Grand Est"},
    "56": {"nom": "Morbihan",                   "slug": "morbihan",                  "region": "Bretagne"},
    "57": {"nom": "Moselle",                    "slug": "moselle",                   "region": "Grand Est"},
    "58": {"nom": "Nièvre",                     "slug": "nievre",                    "region": "Bourgogne-Franche-Comté"},
    "59": {"nom": "Nord",                       "slug": "nord",                      "region": "Hauts-de-France"},
    "60": {"nom": "Oise",                       "slug": "oise",                      "region": "Hauts-de-France"},
    "61": {"nom": "Orne",                       "slug": "orne",                      "region": "Normandie"},
    "62": {"nom": "Pas-de-Calais",              "slug": "pas-de-calais",             "region": "Hauts-de-France"},
    "63": {"nom": "Puy-de-Dôme",               "slug": "puy-de-dome",               "region": "Auvergne-Rhône-Alpes"},
    "64": {"nom": "Pyrénées-Atlantiques",       "slug": "pyrenees-atlantiques",      "region": "Nouvelle-Aquitaine"},
    "65": {"nom": "Hautes-Pyrénées",            "slug": "hautes-pyrenees",           "region": "Occitanie"},
    "66": {"nom": "Pyrénées-Orientales",        "slug": "pyrenees-orientales",       "region": "Occitanie"},
    "67": {"nom": "Bas-Rhin",                   "slug": "bas-rhin",                  "region": "Grand Est"},
    "68": {"nom": "Haut-Rhin",                  "slug": "haut-rhin",                 "region": "Grand Est"},
    "69": {"nom": "Rhône",                      "slug": "rhone",                     "region": "Auvergne-Rhône-Alpes"},
    "70": {"nom": "Haute-Saône",                "slug": "haute-saone",               "region": "Bourgogne-Franche-Comté"},
    "71": {"nom": "Saône-et-Loire",             "slug": "saone-et-loire",            "region": "Bourgogne-Franche-Comté"},
    "72": {"nom": "Sarthe",                     "slug": "sarthe",                    "region": "Pays de la Loire"},
    "73": {"nom": "Savoie",                     "slug": "savoie",                    "region": "Auvergne-Rhône-Alpes"},
    "74": {"nom": "Haute-Savoie",               "slug": "haute-savoie",              "region": "Auvergne-Rhône-Alpes"},
    "75": {"nom": "Paris",                      "slug": "paris",                     "region": "Île-de-France"},
    "76": {"nom": "Seine-Maritime",             "slug": "seine-maritime",            "region": "Normandie"},
    "77": {"nom": "Seine-et-Marne",             "slug": "seine-et-marne",            "region": "Île-de-France"},
    "78": {"nom": "Yvelines",                   "slug": "yvelines",                  "region": "Île-de-France"},
    "79": {"nom": "Deux-Sèvres",                "slug": "deux-sevres",               "region": "Nouvelle-Aquitaine"},
    "80": {"nom": "Somme",                      "slug": "somme",                     "region": "Hauts-de-France"},
    "81": {"nom": "Tarn",                       "slug": "tarn",                      "region": "Occitanie"},
    "82": {"nom": "Tarn-et-Garonne",            "slug": "tarn-et-garonne",           "region": "Occitanie"},
    "83": {"nom": "Var",                        "slug": "var",                       "region": "Provence-Alpes-Côte d'Azur"},
    "84": {"nom": "Vaucluse",                   "slug": "vaucluse",                  "region": "Provence-Alpes-Côte d'Azur"},
    "85": {"nom": "Vendée",                     "slug": "vendee",                    "region": "Pays de la Loire"},
    "86": {"nom": "Vienne",                     "slug": "vienne",                    "region": "Nouvelle-Aquitaine"},
    "87": {"nom": "Haute-Vienne",               "slug": "haute-vienne",              "region": "Nouvelle-Aquitaine"},
    "88": {"nom": "Vosges",                     "slug": "vosges",                    "region": "Grand Est"},
    "89": {"nom": "Yonne",                      "slug": "yonne",                     "region": "Bourgogne-Franche-Comté"},
    "90": {"nom": "Territoire de Belfort",      "slug": "territoire-de-belfort",     "region": "Bourgogne-Franche-Comté"},
    "91": {"nom": "Essonne",                    "slug": "essonne",                   "region": "Île-de-France"},
    "92": {"nom": "Hauts-de-Seine",             "slug": "hauts-de-seine",            "region": "Île-de-France"},
    "93": {"nom": "Seine-Saint-Denis",          "slug": "seine-saint-denis",         "region": "Île-de-France"},
    "94": {"nom": "Val-de-Marne",               "slug": "val-de-marne",              "region": "Île-de-France"},
    "95": {"nom": "Val-d'Oise",                 "slug": "val-d-oise",                "region": "Île-de-France"},
}

_ALL_CODES = sorted(DEPARTEMENTS.keys())
_N  = len(_ALL_CODES)   # 96
_BS = _N // 4           # 24
BATCHES = {
    1: _ALL_CODES[0:_BS],
    2: _ALL_CODES[_BS:2*_BS],
    3: _ALL_CODES[2*_BS:3*_BS],
    4: _ALL_CODES[3*_BS:],
}



# ---------------------------------------------------------------------------
# 1. EAU POTABLE — 1 appel par paramètre au niveau département
# ---------------------------------------------------------------------------

def fetch_potable(dept_code: str, today: datetime) -> list:
    """
    Eau potable : 11 appels API (1 par paramètre) au lieu de N_communes × 12.
    Guard pagination : avertit si Hub'Eau a tronqué les résultats.
    """
    print(f"  Eau potable dept {dept_code}…")
    PAGE_SIZE   = 20000
    par_commune: dict = {}

    for p_code, conf in PARAMS_POTABLE.items():
        url_q = (f"https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis"
                 f"?code_departement={dept_code}&code_parametre={p_code}"
                 f"&size={PAGE_SIZE}&sort=desc")
        d_q = get_json(url_q)
        if not d_q or not d_q.get("data"):
            continue

        # Guard pagination
        total_count = d_q.get("count", 0)
        if total_count > PAGE_SIZE:
            print(f"    [WARN] dept {dept_code} param {p_code} : {total_count} résultats > {PAGE_SIZE} — données tronquées")

        p_name        = conf["name"]
        seen_communes: set = set()

        for record in d_q["data"]:
            insee = record.get("code_commune_insee") or record.get("code_commune")
            nom   = record.get("nom_commune", "")
            if not insee or not nom or insee in seen_communes:
                continue
            seen_communes.add(insee)

            val_num, display, color, date_str = parse_result(record, conf)
            if display is None:
                continue

            if insee not in par_commune:
                par_commune[insee] = {
                    "nom":        nom,
                    "conclusion": record.get("conclusion_conformite_prelevement", "N/A"),
                    "parametres": {},
                }
            par_commune[insee]["parametres"].setdefault(p_name, {
                "valeur": display,
                "unite":  record.get("libelle_unite", conf["unite"]),
                "color":  color,
                "date":   date_str,
            })

    potable = []
    for insee, data in par_commune.items():
        if not data["parametres"]:
            continue
        score = calc_score(data["parametres"])
        potable.append({
            "nom":         data["nom"],
            "dept":        dept_code,
            "insee":       insee,
            "restric":     "N.D.",
            "origine":     "Nappe ou captage local",
            "conclusion":  data["conclusion"],
            "parametres":  data["parametres"],
            "score":       score,
            "score_color": score_color(score),
            "score_label": score_label(score),
        })

    potable.sort(key=lambda c: c["nom"])
    print(f"    → {len(potable)} communes avec données")
    return potable

# ---------------------------------------------------------------------------
# 2. RIVIÈRES
# ---------------------------------------------------------------------------

def fetch_rivieres(dept_code: str, today: datetime) -> list:
    print(f"  Rivières dept {dept_code}…")
    two_years_ago = (today - timedelta(days=730)).strftime("%Y-%m-%d")
    codes_str     = ",".join(PARAMS_RIVIERE.keys())

    url = (f"https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/analyse_pc"
           f"?code_departement={dept_code}&code_parametre={codes_str}"
           f"&date_debut_prelevement={two_years_ago}&size=20000&sort=desc")
    d_all = get_json(url, timeout=90)

    par_station: dict = {}
    if d_all:
        for p in d_all.get("data", []):
            cs = p.get("code_station")
            if not cs:
                continue
            if cs not in par_station:
                par_station[cs] = {
                    "nom":      p.get("libelle_station", cs),
                    "dept":     dept_code,
                    "lat":      p.get("latitude"),
                    "lon":      p.get("longitude"),
                    "analyses": [],
                }
            par_station[cs]["analyses"].append(p)

    rivieres = []
    for cs, info in par_station.items():
        parametres, historique = {}, {}
        for p in info["analyses"]:
            p_code = str(p.get("code_parametre", ""))
            if p_code not in PARAMS_RIVIERE:
                continue
            conf   = PARAMS_RIVIERE[p_code]
            p_name = conf["name"]
            val    = p.get("resultat")
            date_s = p.get("date_prelevement", "").split("T")[0]
            if val is None:
                continue
            color = get_color(val, conf)
            historique.setdefault(p_name, [])
            if len(historique[p_name]) < 5:
                historique[p_name].append({"date": date_s, "valeur": val, "color": color})
            if p_name not in parametres:
                parametres[p_name] = {
                    "valeur": round(val, 3), "unite": conf["unite"],
                    "color": color, "date": date_s, "realtime": False,
                }

        if not parametres:
            continue
        score = calc_score(parametres)
        rivieres.append({
            "nom":           info["nom"],
            "dept":          dept_code,
            "lat":           info["lat"],
            "lon":           info["lon"],
            "nom_cours_eau": extract_nom_cours_eau(info["nom"]),
            "parametres":    parametres,
            "historique":    historique,
            "score":         score,
            "score_color":   score_color(score),
            "score_label":   score_label(score),
        })

    print(f"    → {len(rivieres)} stations rivières")
    return rivieres

# ---------------------------------------------------------------------------
# 3. NAPPES (parallèle par station)
# ---------------------------------------------------------------------------

def _fetch_nappe_station(s: dict, one_year_ago: str):
    """Récupère données d'une station nappe. Thread-safe."""
    bss  = encode_bss(s["code_bss"])
    d_tr = get_json(f"https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques_tr"
                    f"?code_bss={bss}&size=168&sort=desc")
    if not d_tr or not d_tr.get("data"):
        return None
    current = d_tr["data"][0]["niveau_eau_ngf"]
    history = [m["niveau_eau_ngf"] for i, m in enumerate(d_tr["data"]) if i % 12 == 0][::-1]
    d_old   = get_json(f"https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques"
                       f"?code_bss={bss}&date_debut_mesure={one_year_ago}&size=1&sort=asc")
    year_ago = d_old["data"][0]["niveau_nappe_eau"] if d_old and d_old.get("data") else None
    return {
        "nom":      s["nom_commune"],
        "dept":     s.get("code_departement", ""),
        "code_bss": s["code_bss"],
        "current":  current,
        "history":  history,
        "year_ago": year_ago,
        "lat":      s.get("y"),
        "lng":      s.get("x"),
        "color":    "#3b82f6" if (year_ago and current > year_ago) else "#ef4444",
    }


def fetch_nappes(dept_code: str, today: datetime) -> list:
    print(f"  Nappes dept {dept_code}…")
    one_year_ago = (today - timedelta(days=365)).strftime("%Y-%m-%d")

    d_nap = get_json(f"https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations"
                     f"?code_departement={dept_code}&format=json&size=500")
    if not d_nap or not d_nap.get("data"):
        return []

    active = [s for s in d_nap["data"]
              if not s.get("date_fin_mesure") or s["date_fin_mesure"] >= "2024-01-01"]

    nappes = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(_fetch_nappe_station, s, one_year_ago) for s in active]
        for future in as_completed(futures):
            r = future.result()
            if r:
                nappes.append(r)

    print(f"    → {len(nappes)} stations nappes")
    return nappes

# ---------------------------------------------------------------------------
# 4. QUALITÉ DE L'AIR (WFS national Atmo France — tous les départements)
# ---------------------------------------------------------------------------

def fetch_air(dept_code: str) -> list:
    """Fetch ATMO index via le WFS national Atmo France (aucune auth requise).
    Couvre toute la France métropolitaine — remplace le WFS Occitanie limité.
    """
    prefix = dept_code.upper()
    cql = f"code_zone LIKE '{prefix}%'"
    url = f"{WFS_NATIONAL_AIR}&CQL_FILTER={quote(cql)}&count=10"

    print(f"  Air dept {dept_code} (Atmo France WFS national)…")
    d = get_json(url)
    if not d or "features" not in d:
        return []

    # Garder l'enregistrement le plus récent par code_zone
    latest: dict = {}
    for f in d.get("features", []):
        p  = f.get("properties", {})
        cz = p.get("code_zone", "")
        if not cz:
            continue
        if cz not in latest or p.get("date_ech", "") > latest[cz].get("date_ech", ""):
            latest[cz] = p

    results = []
    for cz, p in latest.items():
        qual = int(p.get("code_qual") or 0)
        if qual == 0:
            continue
        results.append({
            "zone":   p.get("lib_zone") or cz,
            "dept":   dept_code,
            "date":   p.get("date_ech", ""),
            "indice": qual,
            "label":  p.get("lib_qual") or QUAL_LABELS.get(qual, "N.C."),
            "color":  p.get("coul_qual") or QUAL_COLORS.get(qual, "#94a3b8"),
            "source": p.get("source", ""),
            "polluants": {
                label: {
                    "indice": int(p.get(f"code_{key}") or 0),
                    "label":  QUAL_LABELS.get(int(p.get(f"code_{key}") or 0), "N.C."),
                    "color":  QUAL_COLORS.get(int(p.get(f"code_{key}") or 0), "#94a3b8"),
                }
                for key, label in SUB_PARAMS
            },
        })

    print(f"    → {len(results)} zones air")
    return results[:5]  # max 5 zones par département

# ---------------------------------------------------------------------------
# 5. POLLEN (Atmo Occitanie — uniquement pour les depts Occitanie)
# ---------------------------------------------------------------------------

def fetch_pollen(dept_code: str) -> list:
    if dept_code not in OCCITANIE_DEPTS:
        return []

    print(f"  Pollen dept {dept_code}…")
    url = (f"https://services9.arcgis.com/7Sr9Ek9c1QTKmbwr/arcgis/rest/services"
           f"/Indice_Pollens_sur_la_region_Occitanie/FeatureServer/0/query"
           f"?where=code_zone+%3D+{int(dept_code)}&outFields=*"
           f"&orderByFields=date_ech+DESC&resultRecordCount=1&f=json")
    d = get_json(url)
    if not d or not d.get("features"):
        return []

    results = []
    for f in d["features"]:
        a       = f.get("attributes", {})
        date_ms = a.get("date_ech")
        date_s  = (datetime.fromtimestamp(date_ms / 1000, tz=ZoneInfo("Europe/Paris"))
                   .strftime("%Y-%m-%d")) if date_ms else ""
        indice_g = a.get("indice") or 0
        taxa = {
            name: {
                "indice": a.get(code) or 0,
                "label":  POL_LABELS.get(a.get(code) or 0, "N.C."),
                "color":  POL_COLORS.get(a.get(code) or 0, "#94a3b8"),
            }
            for code, name in POLLEN_TAXA.items()
        }
        results.append({
            "dept":          dept_code,
            "lib_zone":      a.get("lib_zone", f"Département {dept_code}"),
            "date":          date_s,
            "indice_global": indice_g,
            "label_global":  POL_LABELS.get(indice_g, "N.C."),
            "color_global":  POL_COLORS.get(indice_g, "#94a3b8"),
            "taxa":          taxa,
        })
    print(f"    → {len(results)} enregistrement pollen")
    return results

# ---------------------------------------------------------------------------
# TRAITEMENT D'UN DÉPARTEMENT
# ---------------------------------------------------------------------------

def process_dept(dept_code: str, today: datetime, outdir: str) -> dict:
    """Traite un département complet et écrit XX.json dans outdir."""
    info = DEPARTEMENTS.get(dept_code, {})
    print(f"\n{'='*60}")
    print(f"Département {dept_code} — {info.get('nom', '?')} ({info.get('region', '?')})")
    print(f"{'='*60}")

    potable  = fetch_potable(dept_code, today)
    rivieres = fetch_rivieres(dept_code, today)
    nappes   = fetch_nappes(dept_code, today)
    air      = fetch_air(dept_code)
    pollen   = fetch_pollen(dept_code)

    scores    = [c["score"] for c in potable if c.get("score") is not None]
    score_eau = round(sum(scores) / len(scores)) if scores else None

    dept_data = {
        "dept":        dept_code,
        "nom":         info.get("nom", dept_code),
        "slug":        info.get("slug", dept_code),
        "region":      info.get("region", ""),
        "updated":     today.strftime("%d/%m/%Y à %H:%M"),
        "score_eau":   score_eau,
        "nb_communes": len(potable),
        "potable":     potable,
        "rivieres":    rivieres,
        "nappes":      nappes,
        "air":         air,
        "pollen":      pollen,
    }

    os.makedirs(outdir, exist_ok=True)
    out_path = os.path.join(outdir, f"{dept_code}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(dept_data, f, indent=2, ensure_ascii=False)

    print(f"  ✓ {out_path} — {len(potable)} communes · {len(rivieres)} rivières · "
          f"{len(nappes)} nappes · {len(air)} zones air · {len(pollen)} pollen")
    return dept_data

# ---------------------------------------------------------------------------
# GÉNÉRATION DE data/index.json
# ---------------------------------------------------------------------------

def generate_index(dept_dir: str = None):
    """Lit tous les data/dept/XX.json et génère data/index.json."""
    if dept_dir is None:
        dept_dir = DATA_DEPT_DIR

    if not os.path.isdir(dept_dir):
        print("Aucun répertoire data/dept/ trouvé — index.json non généré.")
        return

    index = {}
    for fname in sorted(os.listdir(dept_dir)):
        if not fname.endswith(".json"):
            continue
        dept_code = fname.replace(".json", "")
        try:
            with open(os.path.join(dept_dir, fname), encoding="utf-8") as f:
                d = json.load(f)
        except Exception:
            continue

        info = DEPARTEMENTS.get(dept_code, {})
        index[dept_code] = {
            "nom":         d.get("nom") or info.get("nom", dept_code),
            "slug":        d.get("slug") or info.get("slug", dept_code),
            "region":      d.get("region") or info.get("region", ""),
            "score_eau":   d.get("score_eau"),
            "nb_communes": d.get("nb_communes", 0),
            "updated":     d.get("updated", ""),
        }

    os.makedirs(os.path.dirname(INDEX_PATH), exist_ok=True)
    payload = {
        "updated":      datetime.now(tz=ZoneInfo("Europe/Paris")).strftime("%d/%m/%Y à %H:%M"),
        "nb_depts":     len(index),
        "departements": index,
    }
    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"\ndata/index.json généré — {len(index)} départements")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fetcher national Mon-Environnement.fr")
    group  = parser.add_mutually_exclusive_group()
    group.add_argument("--depts",  help="Codes séparés par virgule (ex: 34,30,31,13,75)")
    group.add_argument("--batch",  type=int, choices=[1, 2, 3, 4],
                       help="Numéro de batch 1–4 pour les jobs parallèles GitHub Actions")
    group.add_argument("--generate-index-only", action="store_true",
                       help="Régénère uniquement data/index.json depuis data/dept/*.json")
    parser.add_argument("--outdir", default=DATA_DEPT_DIR,
                        help="Répertoire de sortie des JSON depts (défaut: data/dept/)")
    args = parser.parse_args()

    if args.generate_index_only:
        generate_index(DATA_DEPT_DIR)
        return

    if args.depts:
        depts = [d.strip().upper() if d.strip().upper() in ("2A", "2B")
                 else d.strip().zfill(2)
                 for d in args.depts.split(",")]
    elif args.batch:
        depts = BATCHES[args.batch]
    else:
        depts = _ALL_CODES

    today  = datetime.now(tz=ZoneInfo("Europe/Paris"))
    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    print(f"data_fetcher_national.py — {today.strftime('%d/%m/%Y à %H:%M')}")
    print(f"Depts à traiter ({len(depts)}) : {depts}")
    print(f"Répertoire de sortie : {outdir}")

    # Traitement des depts en parallèle (max 6 simultanés pour éviter le rate-limiting)
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(process_dept, code, today, outdir): code
                   for code in depts if code in DEPARTEMENTS}
        for future in as_completed(futures):
            code = futures[future]
            try:
                future.result()
            except Exception as exc:
                print(f"[ERROR] Dept {code} : {exc}")

    if outdir == DATA_DEPT_DIR:
        generate_index(DATA_DEPT_DIR)

    print(f"\nTerminé — {len(depts)} département(s) traité(s).")


if __name__ == "__main__":
    main()
