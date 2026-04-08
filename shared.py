"""
shared.py — Constantes et utilitaires partagés entre tous les scripts Python.
Source unique de vérité : toute modification ici se propage automatiquement.
"""
import re
import json
import urllib.request
import urllib.parse

# ---------------------------------------------------------------------------
# PARAMÈTRES EAU POTABLE (ARS / Hub'Eau)
# ---------------------------------------------------------------------------
PARAMS_POTABLE = {
    "1340": {"name": "Nitrates",      "unite": "mg/L",     "warning": 25,   "danger": 50,   "mode": "max"},
    "1302": {"name": "pH",            "unite": "unité pH", "w_low": 7.0,  "w_high": 8.5, "d_low": 6.5, "d_high": 9.0, "mode": "range"},
    "1399": {"name": "Chlore",        "unite": "mg/L",     "warning": 1.0,  "danger": 5.0,  "mode": "max"},
    "1449": {"name": "Bactériologie", "unite": "nb/100mL", "danger": 0,                     "mode": "zero"},
    "1345": {"name": "Calcaire (TH)", "unite": "°f",       "warning": 30,   "danger": 50,   "mode": "max"},
    "1384": {"name": "Aluminium",     "unite": "µg/L",     "warning": 150,  "danger": 200,  "mode": "max"},
    "1382": {"name": "Plomb",         "unite": "µg/L",     "warning": 5,    "danger": 10,   "mode": "max"},
    "6276": {"name": "Pesticides",    "unite": "µg/L",     "warning": 0.1,  "danger": 0.5,  "mode": "max"},
    "2036": {"name": "THM",           "unite": "µg/L",     "warning": 50,   "danger": 100,  "mode": "max"},
    "1311": {"name": "Conductivité",  "unite": "µS/cm",    "warning": 800,  "danger": 1100, "mode": "max"},
    "8111": {"name": "PFAS",          "unite": "µg/L",     "warning": 0.05, "danger": 0.10, "mode": "max"},
}

# ---------------------------------------------------------------------------
# PARAMÈTRES RIVIÈRES (Hub'Eau / OFB / Agence de l'Eau)
# ---------------------------------------------------------------------------
PARAMS_RIVIERE = {
    "1301": {"name": "Temp. eau",     "unite": "°C",    "warning": 20,  "danger": 25,  "mode": "max"},
    "1340": {"name": "Nitrates",      "unite": "mg/L",  "warning": 10,  "danger": 25,  "mode": "max"},
    "1350": {"name": "Phosphore",     "unite": "mg/L",  "warning": 0.2, "danger": 0.5, "mode": "max"},
    "1302": {"name": "pH",            "unite": "pH",    "w_low": 6.5, "w_high": 8.5, "d_low": 6.0, "d_high": 9.0, "mode": "range"},
    "1311": {"name": "Conductivité",  "unite": "µS/cm", "warning": 500, "danger": 900, "mode": "max"},
    "1312": {"name": "O2 saturation", "unite": "%",     "warning": 70,  "danger": 50,  "mode": "min"},
    "1303": {"name": "MES",           "unite": "mg/L",  "warning": 25,  "danger": 50,  "mode": "max"},
    "1841": {"name": "Ammonium",      "unite": "mg/L",  "warning": 0.5, "danger": 2.0, "mode": "max"},
}

CONF_TEMP = {"warning": 20, "danger": 25, "mode": "max"}

# ---------------------------------------------------------------------------
# QUALITÉ DE L'AIR — Atmo Occitanie
# ---------------------------------------------------------------------------
QUAL_LABELS = {
    0: "N.C.", 1: "Bon", 2: "Moyen", 3: "Dégradé",
    4: "Mauvais", 5: "Très mauvais", 6: "Extrêmement mauvais",
}
QUAL_COLORS = {
    0: "#94a3b8", 1: "#10b981", 2: "#eab308", 3: "#f59e0b",
    4: "#f97316", 5: "#ef4444", 6: "#7c3aed",
}
SUB_PARAMS = [("no2", "NO₂"), ("o3", "O₃"), ("pm10", "PM10"), ("pm25", "PM2.5"), ("so2", "SO₂")]

AIR_ZONES = {
    # Hérault (34)
    "243400017": {"nom": "Montpellier Métropole",         "dept": "34"},
    "243400769": {"nom": "Béziers Méditerranée",           "dept": "34"},
    "200066355": {"nom": "Bassin de Thau (Sète)",          "dept": "34"},
    "243400470": {"nom": "Pays de l'Or (Mauguio)",         "dept": "34"},
    "243400819": {"nom": "Hérault-Méditerranée (Agde)",    "dept": "34"},
    "200017341": {"nom": "Lodévois et Larzac",              "dept": "34"},
    "243400520": {"nom": "Pays de Lunel",                   "dept": "34"},
    "243400694": {"nom": "Vallée de l'Hérault (Gignac)",   "dept": "34"},
    # Gard (30)
    "243000643": {"nom": "Nîmes Métropole",                "dept": "30"},
    "200066918": {"nom": "Alès Agglomération",             "dept": "30"},
    "200034692": {"nom": "Gard Rhodanien (Bagnols)",       "dept": "30"},
    "243000585": {"nom": "Beaucaire Terre d'Argence",      "dept": "30"},
    "200034379": {"nom": "Pays d'Uzès",                    "dept": "30"},
    "243000296": {"nom": "Pays de Sommières",               "dept": "30"},
    # Haute-Garonne (31)
    "243100518": {"nom": "Toulouse Métropole",             "dept": "31"},
}

# Départements couverts par Atmo Occitanie
OCCITANIE_DEPTS = {"09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"}

# ---------------------------------------------------------------------------
# POLLEN — Atmo Occitanie
# ---------------------------------------------------------------------------
POLLEN_TAXA = {
    "GRAMINEE": "Graminées", "OLEA":     "Olivier",
    "CUPRESSA": "Cyprès",    "PLATANUS": "Platane",
    "AMBROSIA": "Ambroisie", "FRAXINUS": "Frêne",
    "QUERCUS":  "Chêne",     "URTICACE": "Urticacées",
    "BETULA":   "Bouleau",   "ARTEMISI": "Armoise",
}
POL_LABELS = {
    0: "N.C.", 1: "Très faible", 2: "Faible", 3: "Moyen",
    4: "Élevé", 5: "Très élevé", 6: "Extrêmement élevé",
}
POL_COLORS = {
    0: "#94a3b8", 1: "#10b981", 2: "#a3e635", 3: "#f59e0b",
    4: "#f97316", 5: "#ef4444", 6: "#7c3aed",
}

# ---------------------------------------------------------------------------
# COULEURS SÉMANTIQUES
# ---------------------------------------------------------------------------
COLOR_OK      = "#10b981"   # vert
COLOR_WARN    = "#f59e0b"   # orange
COLOR_DANGER  = "#ef4444"   # rouge
COLOR_NEUTRAL = "#94a3b8"   # gris (pas de données)

# ---------------------------------------------------------------------------
# RÉSEAU HTTP — get_json avec retry
# ---------------------------------------------------------------------------

def get_json(url: str, timeout: int = 20, retries: int = 2):
    """
    GET JSON avec retry automatique.
    Retourne le dict parsé ou None si toutes les tentatives échouent.
    """
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except Exception as exc:
            if attempt < retries:
                print(f"    [RETRY {attempt + 1}/{retries}] {url[:80]}… → {exc}")
            else:
                print(f"    [ÉCHEC] {url[:80]}… → {exc}")
    return None

# ---------------------------------------------------------------------------
# ÉVALUATION DE LA QUALITÉ
# ---------------------------------------------------------------------------

def get_color(val, conf: dict) -> str:
    """Retourne la couleur sémantique pour une valeur et sa configuration de seuil."""
    if val is None:
        return COLOR_NEUTRAL
    mode = conf["mode"]
    if mode == "max":
        if val > conf.get("danger", float("inf")):  return COLOR_DANGER
        if val > conf.get("warning", float("inf")): return COLOR_WARN
    elif mode == "min":
        if val < conf["danger"]:  return COLOR_DANGER
        if val < conf["warning"]: return COLOR_WARN
    elif mode == "zero":
        if val > conf["danger"]:  return COLOR_DANGER
    elif mode == "range":
        if val < conf["d_low"] or val > conf["d_high"]: return COLOR_DANGER
        if val < conf["w_low"] or val > conf["w_high"]: return COLOR_WARN
    return COLOR_OK

def get_temp_color(val) -> str:
    return get_color(val, CONF_TEMP)

def calc_score(parametres: dict):
    """
    Score de 0 à 100 basé sur les couleurs des paramètres.
    Vert = 100 pts, Orange = 50 pts, Rouge = 0 pts, Gris = ignoré.
    """
    score_map = {COLOR_OK: 100, COLOR_WARN: 50, COLOR_DANGER: 0}
    values = [
        score_map[p["color"]]
        for p in parametres.values()
        if p.get("color", COLOR_NEUTRAL) in score_map
    ]
    return round(sum(values) / len(values)) if values else None

def score_color(s) -> str:
    if s is None:  return COLOR_NEUTRAL
    if s >= 80:    return COLOR_OK
    if s >= 50:    return COLOR_WARN
    return COLOR_DANGER

def score_label(s) -> str:
    if s is None:  return "N.C."
    if s >= 80:    return "Bonne"
    if s >= 50:    return "Moyenne"
    return "Mauvaise"

def score_style(score) -> tuple:
    """Retourne (couleur_hex, label_long) pour l'affichage HTML."""
    if score is None:  return COLOR_NEUTRAL, "Données insuffisantes"
    if score >= 80:    return COLOR_OK,      "Bonne qualité"
    if score >= 50:    return COLOR_WARN,    "Qualité moyenne"
    return COLOR_DANGER, "Mauvaise qualité"

# ---------------------------------------------------------------------------
# EXTRACTION DU NOM DE COURS D'EAU
# ---------------------------------------------------------------------------

def extract_nom_cours_eau(libelle: str) -> str:
    """
    Extrait le nom du cours d'eau depuis le libellé d'une station Hub'Eau.
    Ex: 'LEZ A LATTES 2'                    → 'LEZ'
        'VISTRE DE LA FONTAINE A NIMES'     → 'VISTRE DE LA FONTAINE'
        'GRABIEUX A ST-MARTIN-DE-VALGALGUES'→ 'GRABIEUX'
    """
    m = re.match(r'^(.*?)\s+(?:A|AU|AUX|SUR|DANS|EN)\s+\S', libelle.strip().upper())
    return m.group(1).strip() if m else libelle.strip().upper()

# ---------------------------------------------------------------------------
# UTILITAIRES VALEUR / AFFICHAGE
# ---------------------------------------------------------------------------

def parse_result(record: dict, conf: dict) -> tuple:
    """
    Extrait (val_num, val_display, color, date) depuis un enregistrement Hub'Eau.
    Retourne (None, None, None, None) si la valeur est absente ou vide.
    """
    val_num   = record.get("resultat_numerique")
    val_alpha = record.get("resultat_alphanumerique")
    date_str  = record.get("date_prelevement", "").split("T")[0]

    if val_num is None and (val_alpha is None or str(val_alpha).strip() in ("", "N.M.", "N.R.")):
        return None, None, None, None

    val_for_color = val_num
    if val_for_color is None and val_alpha and "<" in str(val_alpha):
        val_for_color = 0.0

    color   = get_color(val_for_color, conf)
    display = val_alpha if val_alpha and ("<" in str(val_alpha) or ">" in str(val_alpha)) else val_num
    return val_num, display, color, date_str

def encode_bss(code_bss: str) -> str:
    """Encode un code BSS pour utilisation dans une URL Hub'Eau."""
    return urllib.parse.quote(code_bss, safe="")
