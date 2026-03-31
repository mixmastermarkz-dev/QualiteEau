#!/usr/bin/env python3
"""
data_fetcher_national.py — Récupération nationale des données environnementales
Écriture dans data/dept/XX.json et data/index.json
⚠️  Ne touche JAMAIS à full_data.json ni aux fichiers du pipeline existant.

Usage :
  python data_fetcher_national.py --depts 34,30,31,13,75  # depts spécifiques
  python data_fetcher_national.py --batch 1               # batch 1/4 (≈ depts 01-24)
  python data_fetcher_national.py --batch 2               # batch 2/4 (≈ depts 25-48)
  python data_fetcher_national.py --batch 3               # batch 3/4 (≈ depts 49-72)
  python data_fetcher_national.py --batch 4               # batch 4/4 (≈ depts 73-95)
  python data_fetcher_national.py --generate-index-only   # régénère data/index.json
  python data_fetcher_national.py --outdir /tmp/out ...   # répertoire de sortie (CI)
"""
import urllib.request
import urllib.parse
import json
import os
import argparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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

# Départements de la région Occitanie — seule région couverte par Atmo Occitanie
OCCITANIE_DEPTS = {"09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"}

# Zones EPCI suivies par Atmo Occitanie (IQA air)
# Phase 2 : étendre à tous les depts Occitanie + remplacer par Atmo Data national
AIR_ZONES = {
    # Hérault (34)
    "243400017": {"nom": "Montpellier Métropole",        "dept": "34"},
    "243400769": {"nom": "Béziers Méditerranée",          "dept": "34"},
    "200066355": {"nom": "Bassin de Thau (Sète)",         "dept": "34"},
    "243400470": {"nom": "Pays de l'Or (Mauguio)",        "dept": "34"},
    "243400819": {"nom": "Hérault-Méditerranée (Agde)",   "dept": "34"},
    "200017341": {"nom": "Lodévois et Larzac",            "dept": "34"},
    "243400520": {"nom": "Pays de Lunel",                 "dept": "34"},
    "243400694": {"nom": "Vallée de l'Hérault (Gignac)",  "dept": "34"},
    # Gard (30)
    "243000643": {"nom": "Nîmes Métropole",               "dept": "30"},
    "200066918": {"nom": "Alès Agglomération",            "dept": "30"},
    "200034692": {"nom": "Gard Rhodanien (Bagnols)",      "dept": "30"},
    "243000585": {"nom": "Beaucaire Terre d'Argence",     "dept": "30"},
    "200034379": {"nom": "Pays d'Uzès",                   "dept": "30"},
    "243000296": {"nom": "Pays de Sommières",             "dept": "30"},
    # Haute-Garonne (31) — Phase 2 : compléter depuis le WFS Atmo Occitanie
    "243100518": {"nom": "Toulouse Métropole",            "dept": "31"},
}

# Paramètres eau potable (identiques à data_fetcher.py)
PARAMS_POTABLE = {
    "1340": {"name": "Nitrates",      "unite": "mg/L",     "warning": 25,  "danger": 50,   "mode": "max"},
    "1302": {"name": "pH",            "unite": "unité pH", "w_low": 7.0, "w_high": 8.5, "d_low": 6.5, "d_high": 9.0, "mode": "range"},
    "1399": {"name": "Chlore",        "unite": "mg/L",     "warning": 1.0, "danger": 5.0,  "mode": "max"},
    "1449": {"name": "Bactériologie", "unite": "nb/100mL", "danger": 0,                    "mode": "zero"},
    "1345": {"name": "Calcaire (TH)", "unite": "°f",       "warning": 30,  "danger": 50,   "mode": "max"},
    "1384": {"name": "Aluminium",     "unite": "µg/L",     "warning": 150, "danger": 200,  "mode": "max"},
    "1382": {"name": "Plomb",         "unite": "µg/L",     "warning": 5,   "danger": 10,   "mode": "max"},
    "6276": {"name": "Pesticides",    "unite": "µg/L",     "warning": 0.1, "danger": 0.5,  "mode": "max"},
    "2036": {"name": "THM",           "unite": "µg/L",     "warning": 50,  "danger": 100,  "mode": "max"},
    "1311": {"name": "Conductivité",  "unite": "µS/cm",    "warning": 800, "danger": 1100, "mode": "max"},
    "8111": {"name": "PFAS",          "unite": "µg/L",     "warning": 0.05,"danger": 0.10, "mode": "max"},
}

# Paramètres rivières (identiques à data_fetcher.py)
PARAMS_RIVIERE = {
    "1301": {"name": "Temp. eau",    "unite": "°C",   "warning": 20,  "danger": 25,  "mode": "max"},
    "1340": {"name": "Nitrates",     "unite": "mg/L", "warning": 10,  "danger": 25,  "mode": "max"},
    "1350": {"name": "Phosphore",    "unite": "mg/L", "warning": 0.2, "danger": 0.5, "mode": "max"},
    "1302": {"name": "pH",           "unite": "pH",   "w_low": 6.5, "w_high": 8.5, "d_low": 6.0, "d_high": 9.0, "mode": "range"},
    "1311": {"name": "Conductivité", "unite": "µS/cm","warning": 500, "danger": 900, "mode": "max"},
    "1312": {"name": "O2 saturation","unite": "%",    "warning": 70,  "danger": 50,  "mode": "min"},
    "1303": {"name": "MES",          "unite": "mg/L", "warning": 25,  "danger": 50,  "mode": "max"},
    "1841": {"name": "Ammonium",     "unite": "mg/L", "warning": 0.5, "danger": 2.0, "mode": "max"},
}

QUAL_LABELS = {1:"Bon", 2:"Moyen", 3:"Dégradé", 4:"Mauvais", 5:"Très mauvais", 6:"Extrêmement mauvais", 0:"N.C."}
QUAL_COLORS = {1:"#10b981", 2:"#eab308", 3:"#f59e0b", 4:"#f97316", 5:"#ef4444", 6:"#7c3aed", 0:"#94a3b8"}
POL_LABELS  = {0:"N.C.", 1:"Très faible", 2:"Faible", 3:"Moyen", 4:"Élevé", 5:"Très élevé", 6:"Extrêmement élevé"}
POL_COLORS  = {0:"#94a3b8", 1:"#10b981", 2:"#a3e635", 3:"#f59e0b", 4:"#f97316", 5:"#ef4444", 6:"#7c3aed"}
SUB_PARAMS  = [("no2","NO₂"), ("o3","O₃"), ("pm10","PM10"), ("pm25","PM2.5"), ("so2","SO₂")]
POLLEN_TAXA = {
    "GRAMINEE":"Graminées", "OLEA":"Olivier", "CUPRESSA":"Cyprès", "PLATANUS":"Platane",
    "AMBROSIA":"Ambroisie", "FRAXINUS":"Frêne", "QUERCUS":"Chêne", "URTICACE":"Urticacées",
    "BETULA":"Bouleau",     "ARTEMISI":"Armoise",
}

# Cache partagé pour le WFS Atmo Occitanie (1 seul appel pour tous les depts Occitanie)
_wfs_cache = None

# Batches pour les 4 jobs parallèles
_ALL_CODES = sorted(DEPARTEMENTS.keys())
_N         = len(_ALL_CODES)  # 96
_BS        = _N // 4          # 24
BATCHES = {
    1: _ALL_CODES[0:_BS],
    2: _ALL_CODES[_BS:2*_BS],
    3: _ALL_CODES[2*_BS:3*_BS],
    4: _ALL_CODES[3*_BS:],
}

# ---------------------------------------------------------------------------
# UTILITAIRES (identiques à data_fetcher.py)
# ---------------------------------------------------------------------------

def get_json(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"    [WARN] {url[:80]}... → {e}")
        return None

def get_color(val, conf):
    if val is None:
        return "#94a3b8"
    mode = conf["mode"]
    if mode == "max":
        if val > conf.get("danger", float("inf")): return "#ef4444"
        if val > conf.get("warning", float("inf")): return "#f59e0b"
    elif mode == "min":
        if val < conf["danger"]: return "#ef4444"
        if val < conf["warning"]: return "#f59e0b"
    elif mode == "zero":
        if val > conf["danger"]: return "#ef4444"
    elif mode == "range":
        if val < conf["d_low"] or val > conf["d_high"]: return "#ef4444"
        if val < conf["w_low"] or val > conf["w_high"]: return "#f59e0b"
    return "#10b981"

def calc_score(parametres):
    total, count = 0, 0
    for p in parametres.values():
        c = p.get("color", "#94a3b8")
        if c == "#94a3b8": continue
        if c == "#10b981": total += 100
        elif c == "#f59e0b": total += 50
        count += 1
    return round(total / count) if count else None

def score_color(s):
    if s is None: return "#94a3b8"
    if s >= 80: return "#10b981"
    if s >= 50: return "#f59e0b"
    return "#ef4444"

def score_label(s):
    if s is None: return "N.C."
    if s >= 80: return "Bonne"
    if s >= 50: return "Moyenne"
    return "Mauvaise"

# ---------------------------------------------------------------------------
# 1. EAU POTABLE
# ---------------------------------------------------------------------------

def discover_communes(dept_code):
    """
    Découvre les communes du département via Hub'Eau communes_udi.
    Retourne un dict {code_insee: nom_commune}.
    """
    url = (f"https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/communes_udi"
           f"?code_departement={dept_code}&size=500")
    d = get_json(url)
    if not d or not d.get("data"):
        return {}
    communes = {}
    for c in d["data"]:
        # Les noms de champs Hub'Eau communes_udi — à vérifier si l'API évolue
        code = (c.get("code_commune_insee")
                or c.get("code_commune")
                or c.get("code_insee_commune", ""))
        nom  = (c.get("nom_commune")
                or c.get("libelle_commune", ""))
        if code and nom and code not in communes:
            communes[code] = nom
    return communes

def fetch_potable(dept_code, today):
    """
    Eau potable via Hub'Eau — découverte dynamique des communes.
    Approche : communes_udi → par commune × par paramètre (même logique que data_fetcher.py).
    Phase 2 : optimiser avec requête dept-niveau si l'API le supporte.
    """
    print(f"  Eau potable dept {dept_code}...")
    communes = discover_communes(dept_code)
    if not communes:
        print(f"    Aucune commune trouvée pour le dept {dept_code}")
        return []

    print(f"    {len(communes)} communes découvertes")
    potable = []

    for insee, nom in communes.items():
        # Restriction sécheresse VigiEau
        restric = "Vigilance"
        d_res = get_json(
            f"https://api.vigieau.gouv.fr/api/zones?commune={insee}&profil=particulier",
            timeout=10
        )
        if d_res:
            lvls = [z.get("niveauGravite", "") for z in d_res]
            if "crise" in lvls:              restric = "Crise"
            elif "alerte_renforcee" in lvls: restric = "Alerte Renforcée"
            elif "alerte" in lvls:           restric = "Alerte"

        # Analyses ARS — une requête par paramètre pour obtenir la valeur la plus récente
        conclusion     = "N/A"
        parametres     = {}
        got_conclusion = False

        for p_code, conf in PARAMS_POTABLE.items():
            url_q = (f"https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis"
                     f"?code_commune={insee}&code_parametre={p_code}&size=100&sort=desc")
            d_q = get_json(url_q)
            if not d_q or not d_q.get("data"):
                continue

            if not got_conclusion:
                conclusion = d_q["data"][0].get("conclusion_conformite_prelevement", "N/A")
                got_conclusion = True

            p_name = conf["name"]
            for p in d_q["data"]:
                val_num   = p.get("resultat_numerique")
                val_alpha = p.get("resultat_alphanumerique")
                date_str  = p.get("date_prelevement", "").split("T")[0]

                if val_num is None and (val_alpha is None or str(val_alpha).strip() in ("", "N.M.", "N.R.")):
                    continue

                val_for_color = val_num
                if val_for_color is None and val_alpha and "<" in str(val_alpha):
                    val_for_color = 0.0
                color   = get_color(val_for_color, conf)
                display = val_alpha if val_alpha and ("<" in str(val_alpha) or ">" in str(val_alpha)) else val_num

                if p_name not in parametres:
                    parametres[p_name] = {
                        "valeur": display,
                        "unite":  p.get("libelle_unite", conf["unite"]),
                        "color":  color,
                        "date":   date_str,
                    }
                    break  # valeur la plus récente trouvée

        if not parametres:
            continue  # commune sans aucune donnée récente = ignorée

        score = calc_score(parametres)
        potable.append({
            "nom":         nom,
            "dept":        dept_code,
            "insee":       insee,
            "restric":     restric,
            "origine":     "Nappe ou captage local",
            "conclusion":  conclusion,
            "parametres":  parametres,
            "score":       score,
            "score_color": score_color(score),
            "score_label": score_label(score),
        })

    potable.sort(key=lambda c: c["nom"])
    print(f"    → {len(potable)} communes avec données")
    return potable

# ---------------------------------------------------------------------------
# 2. QUALITÉ RIVIÈRES
# ---------------------------------------------------------------------------

def fetch_rivieres(dept_code, today):
    """Rivières via Hub'Eau — même logique que data_fetcher.py, paramétré par dept."""
    print(f"  Rivières dept {dept_code}...")
    two_years_ago = (today - timedelta(days=730)).strftime("%Y-%m-%d")
    codes_str     = ",".join(PARAMS_RIVIERE.keys())

    url = (f"https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/analyse_pc"
           f"?code_departement={dept_code}&code_parametre={codes_str}"
           f"&date_debut_prelevement={two_years_ago}&size=20000&sort=desc")
    d_all = get_json(url, timeout=90)

    par_station = {}
    if d_all:
        for p in d_all.get("data", []):
            cs = p.get("code_station")
            if not cs: continue
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
        parametres = {}
        historique = {}
        for p in info["analyses"]:
            p_code = str(p.get("code_parametre", ""))
            if p_code not in PARAMS_RIVIERE: continue
            conf   = PARAMS_RIVIERE[p_code]
            p_name = conf["name"]
            val    = p.get("resultat")
            date_s = p.get("date_prelevement", "").split("T")[0]
            if val is None: continue
            color  = get_color(val, conf)
            if p_name not in historique:
                historique[p_name] = []
            if len(historique[p_name]) < 5:
                historique[p_name].append({"date": date_s, "valeur": val, "color": color})
            if p_name not in parametres:
                parametres[p_name] = {
                    "valeur":   round(val, 3),
                    "unite":    conf["unite"],
                    "color":    color,
                    "date":     date_s,
                    "realtime": False,
                }

        if not parametres: continue
        score = calc_score(parametres)
        rivieres.append({
            "nom":         info["nom"],
            "dept":        dept_code,
            "lat":         info["lat"],
            "lon":         info["lon"],
            "parametres":  parametres,
            "historique":  historique,
            "score":       score,
            "score_color": score_color(score),
            "score_label": score_label(score),
        })

    print(f"    → {len(rivieres)} stations rivières")
    return rivieres

# ---------------------------------------------------------------------------
# 3. NAPPES PHRÉATIQUES
# ---------------------------------------------------------------------------

def fetch_nappes(dept_code, today):
    """Nappes via Hub'Eau BRGM — même logique que data_fetcher.py, paramétré par dept."""
    print(f"  Nappes dept {dept_code}...")
    one_year_ago = (today - timedelta(days=365)).strftime("%Y-%m-%d")

    d_nap = get_json(
        f"https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations"
        f"?code_departement={dept_code}&format=json&size=500"
    )
    if not d_nap or not d_nap.get("data"):
        return []

    active = [s for s in d_nap["data"]
              if not s.get("date_fin_mesure") or s["date_fin_mesure"] >= "2024-01-01"]

    nappes = []
    for s in active:
        bss  = urllib.parse.quote(s["code_bss"], safe="")
        d_tr = get_json(
            f"https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques_tr"
            f"?code_bss={bss}&size=168&sort=desc"
        )
        if not d_tr or not d_tr.get("data"): continue
        current = d_tr["data"][0]["niveau_eau_ngf"]
        history = [m["niveau_eau_ngf"] for i, m in enumerate(d_tr["data"]) if i % 12 == 0][::-1]
        d_old = get_json(
            f"https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques"
            f"?code_bss={bss}&date_debut_mesure={one_year_ago}&size=1&sort=asc"
        )
        year_ago = d_old["data"][0]["niveau_nappe_eau"] if d_old and d_old.get("data") else None
        nappes.append({
            "nom":      s["nom_commune"],
            "dept":     dept_code,
            "code_bss": s["code_bss"],
            "current":  current,
            "history":  history,
            "year_ago": year_ago,
            "lat":      s.get("y"),
            "lng":      s.get("x"),
            "color":    "#3b82f6" if (year_ago and current > year_ago) else "#ef4444",
        })

    print(f"    → {len(nappes)} stations nappes")
    return nappes

# ---------------------------------------------------------------------------
# 4. QUALITÉ DE L'AIR (Atmo Occitanie — Occitanie seulement)
# Phase 2 : remplacer par Atmo Data national (admindata.atmo-france.org)
# ---------------------------------------------------------------------------

def _get_wfs_occitanie():
    """Charge le WFS Atmo Occitanie une seule fois (cache module-level)."""
    global _wfs_cache
    if _wfs_cache is not None:
        return _wfs_cache
    url = ("https://dservices9.arcgis.com/7Sr9Ek9c1QTKmbwr/arcgis/services/ind_occitanie/WFSServer"
           "?service=WFS&version=2.0.0&request=GetFeature"
           "&typeName=ind_occitanie:ind_occitanie&outputFormat=GEOJSON&count=500")
    _wfs_cache = get_json(url)
    return _wfs_cache

def fetch_air(dept_code):
    """
    Qualité de l'air pour un département.
    Couverture actuelle : depts Occitanie uniquement (Atmo Occitanie).
    Autres depts : retourne [] — sera complété en Phase 2 avec Atmo Data national.
    """
    if dept_code not in OCCITANIE_DEPTS:
        return []

    dept_zones = {k: v for k, v in AIR_ZONES.items() if v["dept"] == dept_code}
    if not dept_zones:
        return []

    print(f"  Air dept {dept_code} (Atmo Occitanie)...")
    d_air = _get_wfs_occitanie()
    if not d_air or "features" not in d_air:
        return []

    latest = {}
    for f in d_air["features"]:
        p  = f.get("properties", {})
        cz = p.get("code_zone")
        if cz not in dept_zones: continue
        if cz not in latest or p.get("date_ech", "") > latest[cz].get("date_ech", ""):
            latest[cz] = p

    air_results = []
    for cz, p in latest.items():
        qual = p.get("code_qual") or 0
        air_results.append({
            "zone":    dept_zones[cz]["nom"],
            "dept":    dept_code,
            "date":    p.get("date_ech", ""),
            "indice":  qual,
            "label":   p.get("lib_qual") or QUAL_LABELS.get(qual, "N.C."),
            "color":   p.get("coul_qual") or QUAL_COLORS.get(qual, "#94a3b8"),
            "polluants": {
                label: {
                    "indice": p.get(f"code_{key}") or 0,
                    "label":  QUAL_LABELS.get(p.get(f"code_{key}") or 0, "N.C."),
                    "color":  QUAL_COLORS.get(p.get(f"code_{key}") or 0, "#94a3b8"),
                }
                for key, label in SUB_PARAMS
            },
        })
    print(f"    → {len(air_results)} zones air")
    return air_results

# ---------------------------------------------------------------------------
# 5. POLLEN (Atmo Occitanie — Occitanie seulement)
# ---------------------------------------------------------------------------

def fetch_pollen(dept_code):
    """
    Pollen pour un département.
    Couverture actuelle : depts Occitanie uniquement.
    """
    if dept_code not in OCCITANIE_DEPTS:
        return []

    print(f"  Pollen dept {dept_code}...")
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

def process_dept(dept_code, today, outdir):
    """Traite un département et écrit data/dept/XX.json (ou outdir/XX.json)."""
    info = DEPARTEMENTS.get(dept_code, {})
    print(f"\n{'='*60}")
    print(f"Département {dept_code} — {info.get('nom', '?')} ({info.get('region', '?')})")
    print(f"{'='*60}")

    potable   = fetch_potable(dept_code, today)
    rivieres  = fetch_rivieres(dept_code, today)
    nappes    = fetch_nappes(dept_code, today)
    air       = fetch_air(dept_code)
    pollen    = fetch_pollen(dept_code)

    scores     = [c["score"] for c in potable if c.get("score") is not None]
    score_eau  = round(sum(scores) / len(scores)) if scores else None

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

    print(f"  ✓ Écrit → {out_path}")
    print(f"    {len(potable)} communes · {len(rivieres)} rivières · "
          f"{len(nappes)} nappes · {len(air)} zones air · {len(pollen)} pollen")
    return dept_data

# ---------------------------------------------------------------------------
# GÉNÉRATION DE data/index.json
# ---------------------------------------------------------------------------

def generate_index(dept_dir=None):
    """
    Lit tous les data/dept/XX.json et génère data/index.json avec les scores agrégés.
    Appelé en fin de run ou via --generate-index-only.
    """
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
    group.add_argument("--depts", help="Codes séparés par virgule (ex: 34,30,31,13,75)")
    group.add_argument("--batch", type=int, choices=[1, 2, 3, 4],
                       help="Numéro de batch 1-4 pour les jobs parallèles GitHub Actions")
    group.add_argument("--generate-index-only", action="store_true",
                       help="Régénère uniquement data/index.json depuis data/dept/*.json")
    parser.add_argument("--outdir", default=DATA_DEPT_DIR,
                        help="Répertoire de sortie des JSON depts (défaut: data/dept/)")
    args = parser.parse_args()

    if args.generate_index_only:
        generate_index(DATA_DEPT_DIR)
        return

    # Détermination des depts à traiter
    if args.depts:
        depts = [d.strip().upper() if d.strip().upper() in ("2A", "2B")
                 else d.strip().zfill(2)
                 for d in args.depts.split(",")]
    elif args.batch:
        depts = BATCHES[args.batch]
    else:
        depts = _ALL_CODES  # tous les 96 depts

    today  = datetime.now(tz=ZoneInfo("Europe/Paris"))
    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    print(f"data_fetcher_national.py — {today.strftime('%d/%m/%Y à %H:%M')}")
    print(f"Depts à traiter : {depts}")
    print(f"Répertoire de sortie : {outdir}")

    for dept_code in depts:
        if dept_code not in DEPARTEMENTS:
            print(f"[SKIP] Code inconnu : {dept_code}")
            continue
        try:
            process_dept(dept_code, today, outdir)
        except Exception as e:
            print(f"[ERROR] Dept {dept_code} : {e}")

    # Mise à jour de data/index.json (seulement si on écrit dans le répertoire principal)
    if outdir == DATA_DEPT_DIR:
        generate_index(DATA_DEPT_DIR)

    print(f"\nTerminé — {len(depts)} département(s) traité(s).")

if __name__ == "__main__":
    main()
