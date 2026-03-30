import urllib.request
import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Chemin absolu vers le répertoire du script (pour le cron OVH)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

COMMUNES = {
    # ── HÉRAULT (34) ──────────────────────────────────────────────────────────
    "34172": "Montpellier",            # ~298 000
    "34032": "Béziers",                # ~77 000
    "34301": "Sète",                   # ~43 000
    "34003": "Agde",                   # ~28 000
    "34145": "Lunel",                  # ~27 000
    "34108": "Frontignan",             # ~24 000
    "34299": "Sérignan",               # ~8 500
    "34270": "Saint-Jean-de-Védas",   # ~13 000
    "34057": "Castelnau-le-Lez",       # ~21 000
    "34154": "Mauguio",                # ~17 000
    "34129": "Lattes",                 # ~16 000
    "34116": "Grabels",                # ~9 000
    "34113": "Gigean",                 # ~6 300
    "34114": "Gignac",                 # ~6 700
    "34082": "Le Crès",                # ~9 200
    "34077": "Clapiers",               # ~6 000
    "34079": "Clermont-l'Hérault",    # ~9 500
    "34123": "Juvignac",               # ~8 000
    "34202": "Pignan",                 # ~8 300
    "34255": "Saint-Gély-du-Fesc",    # ~9 000
    "34150": "Marseillan",             # ~8 000
    "34197": "Pézenas",               # ~7 600  ← ex-34199 selon agent
    "34023": "Balaruc-les-Bains",      # ~7 000
    "34088": "Cournonterral",          # ~7 400
    "34217": "Prades-le-Lez",         # ~6 200
    "34213": "Poussan",                # ~6 800
    "34153": "Mèze",                   # ~12 000
    "34151": "Marsillargues",          # ~6 800
    "34198": "Pérols",                 # ~8 000
    "34332": "Vias",                   # ~6 000
    "34192": "Palavas-les-Flots",     # ~6 100
    "34058": "Castries",               # ~6 900
    "34095": "Fabrègues",              # ~5 000
    "34120": "Jacou",                  # ~6 000
    "34022": "Baillargues",            # ~6 000
    "34327": "Vendargues",             # ~5 000
    "34101": "Florensac",              # ~5 100
    "34142": "Lodève",                 # ~7 500
    "34247": "Saint-Clément-de-Rivière", # ~5 100
    "34240": "Saint-Aunès",           # ~4 500
    "34183": "Nissan-lez-Enserune",   # ~4 100
    "34146": "Lunel-Viel",             # ~4 500
    "34169": "Montferrier-sur-Lez",   # ~4 100
    "34162": "Montagnac",              # ~4 300
    "34324": "Valras-Plage",           # ~4 300
    "34148": "Maraussan",              # ~4 700
    "34051": "Canet",                  # ~3 600
    "34052": "Capestang",              # ~3 500
    "34028": "Bédarieux",              # ~5 800
    "34037": "Boujan-sur-Libron",      # ~3 600
    "34031": "Bessan",                 # ~5 800
    "34111": "Ganges",                 # ~3 700
    "34165": "Montbazin",              # ~2 900
    "34166": "Montblanc",              # ~2 900
    "34159": "Mireval",                # ~3 300
    "34276": "Saint-Mathieu-de-Tréviers", # ~4 900
    "34259": "Saint-Georges-d'Orques", # ~5 700
    "34337": "Villeneuve-lès-Maguelone", # ~6 000
    "34336": "Villeneuve-lès-Béziers", # ~4 000

    # ── GARD (30) ─────────────────────────────────────────────────────────────
    "30189": "Nîmes",                  # ~148 000
    "30007": "Alès",                   # ~40 000
    "30028": "Bagnols-sur-Cèze",       # ~18 000
    "30032": "Beaucaire",              # ~16 000
    "30258": "Saint-Gilles",           # ~14 000
    "30351": "Villeneuve-lès-Avignon", # ~13 000
    "30341": "Vauvert",                # ~11 000
    "30202": "Pont-Saint-Esprit",      # ~10 000
    "30334": "Uzès",                   # ~9 000
    "30156": "Marguerittes",           # ~8 000
    "30011": "Les Angles",             # ~8 900
    "30034": "Bellegarde",             # ~8 000
    "30133": "Le Grau-du-Roi",         # ~8 400
    "30003": "Aigues-Mortes",          # ~9 000
    "30217": "Rochefort-du-Gard",      # ~5 500
    "30047": "Bouillargues",           # ~6 100
    "30243": "Saint-Christol-lez-Alès", # ~7 400
    "30259": "Saint-Hilaire-de-Brethmas", # ~5 000
    "30169": "Milhaud",                # ~6 300
    "30062": "Calvisson",              # ~6 500
    "30125": "Garons",                 # ~5 400
    "30155": "Manduel",                # ~6 000
    "30344": "Vergèze",                # ~5 800
    "30006": "Aimargues",              # ~5 700
    "30321": "Sommières",              # ~5 100
    "30132": "La Grand-Combe",         # ~4 800
    "30082": "Clarensac",              # ~4 400
    "30060": "Caissargues",            # ~4 100
    "30221": "Roquemaure",             # ~5 500
    "30012": "Aramon",                 # ~4 100
    "30141": "Laudun-l'Ardoise",      # ~5 000
    "30255": "Saint-Geniès-de-Malgoirès", # ~3 100
    "30333": "Uchaud",                 # ~4 000
    "30211": "Redessan",               # ~4 000
    "30123": "Gallargues-le-Montueux", # ~3 600
    "30263": "Saint-Hippolyte-du-Fort", # ~3 600
    "30227": "Saint-Ambroix",         # ~3 000
    "30210": "Quissac",                # ~3 500
    "30179": "Montfrin",               # ~3 000
    "30212": "Remoulins",              # ~2 300
    "30305": "Salindres",              # ~3 000
    "30350": "Le Vigan",               # ~3 500
    "30276": "Saint-Laurent-des-Arbres", # ~3 000
}

ORIGINES = {
    "Montpellier": "Source du Lez", "Nîmes": "Vistrenque / Rhône",
    "Béziers": "Orb / Sables Astiens", "Sète": "Rhône / Pliocène",
    "Agde": "Hérault / Sables Astiens", "Alès": "Gardon",
    "Bagnols-sur-Cèze": "Rhône", "Beaucaire": "Rhône", "Pont-Saint-Esprit": "Rhône"
}

# Paramètres eau potable (source ARS via Hub'Eau)
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

# Paramètres analyses labo rivières (source Hub'Eau / OFB / Agence de l'Eau)
PARAMS_RIVIERE = {
    "1301": {"name": "Temp. eau",   "unite": "°C",    "warning": 20,  "danger": 25,  "mode": "max"},
    "1340": {"name": "Nitrates",    "unite": "mg/L",  "warning": 10,  "danger": 25,  "mode": "max"},
    "1350": {"name": "Phosphore",   "unite": "mg/L",  "warning": 0.2, "danger": 0.5, "mode": "max"},
    "1302": {"name": "pH",          "unite": "pH",    "w_low": 6.5, "w_high": 8.5, "d_low": 6.0, "d_high": 9.0, "mode": "range"},
    "1311": {"name": "Conductivité","unite": "µS/cm", "warning": 500, "danger": 900, "mode": "max"},
    "1312": {"name": "O2 saturation","unite": "%",    "warning": 70,  "danger": 50,  "mode": "min"},
    "1303": {"name": "MES",         "unite": "mg/L",  "warning": 25,  "danger": 50,  "mode": "max"},
    "1841": {"name": "Ammonium",    "unite": "mg/L",  "warning": 0.5, "danger": 2.0, "mode": "max"},
}

# Seuils température pour coloration
CONF_TEMP = {"warning": 20, "danger": 25, "mode": "max"}

# ---------------------------------------------------------------------------
# UTILITAIRES
# ---------------------------------------------------------------------------

def get_json(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except:
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

def get_temp_color(val):
    return get_color(val, CONF_TEMP)

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

def dept_from_coords(lat, lon):
    """Estime le département (34/30) à partir des coordonnées GPS."""
    if lat is None or lon is None:
        return "34"
    # Distance aux centroïdes approximatifs des deux départements
    d34 = (lat - 43.55) ** 2 + (lon - 3.35) ** 2  # centre Hérault
    d30 = (lat - 43.95) ** 2 + (lon - 4.20) ** 2  # centre Gard
    return "30" if d30 < d34 else "34"

def get_temperature_meteo(lat, lon):
    """Température air et eau de surface via Open-Meteo / Météo-France."""
    url = (f"https://api.open-meteo.com/v1/meteofrance"
           f"?latitude={lat}&longitude={lon}"
           f"&current=temperature_2m,soil_temperature_0cm"
           f"&timezone=Europe%2FParis")
    d = get_json(url)
    if not d or "current" not in d:
        return None, None
    air  = d["current"].get("temperature_2m")
    soil = d["current"].get("soil_temperature_0cm")  # proxy température eau
    return air, soil

# ---------------------------------------------------------------------------
# SCRIPT PRINCIPAL
# ---------------------------------------------------------------------------

def run_all():
    today = datetime.now(tz=ZoneInfo("Europe/Paris"))
    ten_years_ago = (today - timedelta(days=3650)).strftime("%Y-%m-%d")
    one_year_ago  = (today - timedelta(days=365)).strftime("%Y-%m-%d")

    # -----------------------------------------------------------------------
    # 1. EAU POTABLE — source : ARS via Hub'Eau
    # -----------------------------------------------------------------------
    print("Eau potable (ARS / Hub'Eau)...")
    potable = []

    for insee, nom in COMMUNES.items():
        # Niveau de restriction VigiEau
        restric = "Vigilance"
        d_res = get_json(f"https://api.vigieau.gouv.fr/api/zones?commune={insee}&profil=particulier")
        if d_res:
            lvls = [z.get("niveauGravite", "") for z in d_res]
            if "crise" in lvls:             restric = "Crise"
            elif "alerte_renforcee" in lvls: restric = "Alerte Renforcée"
            elif "alerte" in lvls:           restric = "Alerte"

        # Analyses ARS — on fait une requête par paramètre pour garantir
        # de trouver la valeur la plus récente non-nulle pour chacun.
        codes_potable = list(PARAMS_POTABLE.keys())
        conclusion  = "N/A"
        parametres  = {}
        historique  = {}
        got_conclusion = False

        for p_code in codes_potable:
            url_q = (f"https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis"
                     f"?code_commune={insee}&code_parametre={p_code}&size=200&sort=desc")
            d_q = get_json(url_q)
            if not d_q or not d_q.get("data"):
                continue

            if not got_conclusion:
                conclusion = d_q["data"][0].get("conclusion_conformite_prelevement", "N/A")
                got_conclusion = True

            conf   = PARAMS_POTABLE[p_code]
            p_name = conf["name"]

            for p in d_q["data"]:
                val_num   = p.get("resultat_numerique")
                val_alpha = p.get("resultat_alphanumerique")
                date_str  = p.get("date_prelevement", "").split("T")[0]

                # Ignorer les enregistrements sans aucune valeur
                if val_num is None and (val_alpha is None or str(val_alpha).strip() in ("", "N.M.", "N.R.")):
                    continue

                val_for_color = val_num
                if val_for_color is None and val_alpha and "<" in str(val_alpha):
                    val_for_color = 0.0
                color = get_color(val_for_color, conf)
                display = val_alpha if val_alpha and ("<" in str(val_alpha) or ">" in str(val_alpha)) else val_num

                # Première valeur valide = la plus récente
                if p_name not in parametres:
                    parametres[p_name] = {
                        "valeur": display,
                        "unite":  p.get("libelle_unite", conf["unite"]),
                        "color":  color,
                        "date":   date_str
                    }

                if p_name not in historique:
                    historique[p_name] = []
                if len(historique[p_name]) < 5:
                    historique[p_name].append({"date": date_str, "valeur": val_num, "color": color})

                # Arrêter dès qu'on a la valeur courante + 5 points historiques
                if len(historique.get(p_name, [])) >= 5:
                    break

        score = calc_score(parametres)
        potable.append({
            "nom": nom, "dept": insee[:2],
            "restric": restric,
            "origine": ORIGINES.get(nom, "Nappe ou captage local"),
            "conclusion": conclusion,
            "parametres": parametres,
            "historique": historique,
            "score": score,
            "score_color": score_color(score),
            "score_label": score_label(score)
        })
        print(f"  {nom} — {len(parametres)} params — score {score}")

    # -----------------------------------------------------------------------
    # 2. QUALITÉ RIVIÈRES — source : Hub'Eau / Agence de l'Eau + Météo-France
    #    Approche directe : analyse_pc par département, groupement par station
    # -----------------------------------------------------------------------
    print("Qualité rivières (Hub'Eau analyse_pc + Météo-France)...")
    rivieres    = []
    codes_str   = ",".join(PARAMS_RIVIERE.keys())
    stations_vues = set()

    two_years_ago = (today - timedelta(days=730)).strftime("%Y-%m-%d")
    # Filtre sur 2 ans : réduit de 147k à ~13k enregistrements, toutes stations couvertes
    url_analyses = (f"https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/analyse_pc"
                    f"?code_departement=34,30&code_parametre={codes_str}"
                    f"&date_debut_prelevement={two_years_ago}&size=20000&sort=desc")
    d_all = get_json(url_analyses, timeout=90)

    # Grouper par station (code_station)
    par_station = {}
    if d_all:
        for p in d_all.get("data", []):
            cs = p.get("code_station")
            if not cs: continue
            if cs not in par_station:
                lat = p.get("latitude")
                lon = p.get("longitude")
                par_station[cs] = {
                    "nom":     p.get("libelle_station", cs),
                    "dept":    dept_from_coords(lat, lon),
                    "lat":     lat,
                    "lon":     lon,
                    "analyses": []
                }
            par_station[cs]["analyses"].append(p)

    for cs, info in par_station.items():
        parametres = {}
        historique = {}

        for p in info["analyses"]:
            p_code   = str(p.get("code_parametre", ""))
            if p_code not in PARAMS_RIVIERE: continue
            conf     = PARAMS_RIVIERE[p_code]
            p_name   = conf["name"]
            val      = p.get("resultat")
            date_str = p.get("date_prelevement", "").split("T")[0]
            if val is None: continue

            color = get_color(val, conf)

            if p_name not in historique:
                historique[p_name] = []
            if len(historique[p_name]) < 5:
                historique[p_name].append({"date": date_str, "valeur": val, "color": color})

            # Valeur courante = la plus récente (données déjà triées desc)
            if p_name not in parametres:
                parametres[p_name] = {
                    "valeur":   round(val, 3),
                    "unite":    conf["unite"],
                    "color":    color,
                    "date":     date_str,
                    "realtime": False
                }

        if not parametres: continue

        # Température air — Météo-France via Open-Meteo (temps réel)
        lat, lon = info["lat"], info["lon"]
        if lat and lon:
            air, _ = get_temperature_meteo(lat, lon)
            if air is not None:
                parametres["Temp. air"] = {
                    "valeur": round(air, 1), "unite": "°C",
                    "color":  get_temp_color(air),
                    "date":   today.strftime("%Y-%m-%d"), "realtime": True
                }

        score = calc_score({k: v for k, v in parametres.items() if not v.get("realtime")})
        rivieres.append({
            "nom":         info["nom"],
            "commune":     info["nom"],
            "dept":        info["dept"],
            "parametres":  parametres,
            "historique":  historique,
            "score":       score,
            "score_color": score_color(score),
            "score_label": score_label(score)
        })
        print(f"  {info['nom']} ({info['dept']}) — {len(parametres)} params — score {score}")

    # -----------------------------------------------------------------------
    # 3. NAPPES — source : BRGM via Hub'Eau
    # -----------------------------------------------------------------------
    print("Nappes (BRGM / Hub'Eau)...")
    nappe_results = []

    d_nap = get_json("https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations"
                     "?code_departement=34,30&format=json&size=500")
    if d_nap:
        # Inclure les stations sans date de fin (encore actives) OU avec date fin récente
        active = [s for s in d_nap["data"]
                  if not s.get("date_fin_mesure") or s["date_fin_mesure"] >= "2024-01-01"]
        for s in active:
            bss = s["code_bss"].replace("/", "%2F")
            d_tr = get_json(f"https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques_tr"
                            f"?code_bss={bss}&size=168&sort=desc")
            if not d_tr or not d_tr.get("data"): continue
            current = d_tr["data"][0]["niveau_eau_ngf"]
            history = [m["niveau_eau_ngf"] for i, m in enumerate(d_tr["data"]) if i % 12 == 0][::-1]
            d_old = get_json(f"https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques"
                             f"?code_bss={bss}&date_debut_mesure={one_year_ago}&size=1&sort=asc")
            year_ago = d_old["data"][0]["niveau_nappe_eau"] if d_old and d_old.get("data") else None
            nappe_results.append({
                "nom": s["nom_commune"], "dept": s["code_departement"],
                "code_bss": s["code_bss"],
                "current": current, "history": history, "year_ago": year_ago,
                "lat": s["y"], "lng": s["x"],
                "color": "#3b82f6" if (year_ago and current > year_ago) else "#ef4444"
            })
            print(f"  {s['nom_commune']} — {current:.2f} m NGF")

    # -----------------------------------------------------------------------
    # 4. QUALITÉ DE L'AIR — source : Atmo Occitanie (ArcGIS WFS, sans auth)
    # -----------------------------------------------------------------------
    print("Qualité de l'air (Atmo Occitanie)...")

    # Zones EPCI couvertes avec leur département
    AIR_ZONES = {
        # Hérault (34)
        "243400017": {"nom": "Montpellier Métropole",       "dept": "34"},
        "243400769": {"nom": "Béziers Méditerranée",         "dept": "34"},
        "200066355": {"nom": "Bassin de Thau (Sète)",        "dept": "34"},
        "243400470": {"nom": "Pays de l'Or (Mauguio)",      "dept": "34"},
        "243400819": {"nom": "Hérault-Méditerranée (Agde)", "dept": "34"},
        "200017341": {"nom": "Lodévois et Larzac",           "dept": "34"},
        "243400520": {"nom": "Pays de Lunel",                "dept": "34"},
        "243400694": {"nom": "Vallée de l'Hérault (Gignac)","dept": "34"},
        # Gard (30)
        "243000643": {"nom": "Nîmes Métropole",              "dept": "30"},
        "200066918": {"nom": "Alès Agglomération",           "dept": "30"},
        "200034692": {"nom": "Gard Rhodanien (Bagnols)",     "dept": "30"},
        "243000585": {"nom": "Beaucaire Terre d'Argence",   "dept": "30"},
        "200034379": {"nom": "Pays d'Uzès",                 "dept": "30"},
        "243000296": {"nom": "Pays de Sommières",            "dept": "30"},
    }
    QUAL_LABELS = {1:"Bon", 2:"Moyen", 3:"Dégradé", 4:"Mauvais", 5:"Très mauvais", 6:"Extrêmement mauvais", 0:"N.C."}
    QUAL_COLORS = {1:"#10b981", 2:"#eab308", 3:"#f59e0b", 4:"#f97316", 5:"#ef4444", 6:"#7c3aed", 0:"#94a3b8"}
    SUB_PARAMS  = [("no2","NO₂"), ("o3","O₃"), ("pm10","PM10"), ("pm25","PM2.5"), ("so2","SO₂")]

    air_results = []
    wfs_url = ("https://dservices9.arcgis.com/7Sr9Ek9c1QTKmbwr/arcgis/services/ind_occitanie/WFSServer"
               "?service=WFS&version=2.0.0&request=GetFeature"
               "&typeName=ind_occitanie:ind_occitanie&outputFormat=GEOJSON&count=500")
    d_air = get_json(wfs_url)

    if d_air and "features" in d_air:
        latest = {}  # code_zone → feature le plus récent
        for f in d_air["features"]:
            p  = f.get("properties", {})
            cz = p.get("code_zone")
            if cz not in AIR_ZONES: continue
            # Garder la ligne la plus récente par zone
            if cz not in latest or p.get("date_ech","") > latest[cz].get("date_ech",""):
                latest[cz] = p

        for cz, p in latest.items():
            qual = p.get("code_qual") or 0
            air_results.append({
                "zone":    AIR_ZONES[cz]["nom"],
                "dept":    AIR_ZONES[cz]["dept"],
                "date":    p.get("date_ech", ""),
                "indice":  qual,
                "label":   p.get("lib_qual") or QUAL_LABELS.get(qual, "N.C."),
                "color":   p.get("coul_qual") or QUAL_COLORS.get(qual, "#94a3b8"),
                "polluants": {
                    label: {"indice": p.get(f"code_{key}") or 0,
                            "label":  QUAL_LABELS.get(p.get(f"code_{key}") or 0, "N.C."),
                            "color":  QUAL_COLORS.get(p.get(f"code_{key}") or 0, "#94a3b8")}
                    for key, label in SUB_PARAMS
                }
            })
    print(f"  {len(air_results)} zones air")

    # -----------------------------------------------------------------------
    # 5. POLLEN — source : Atmo Occitanie (ArcGIS FeatureServer, sans auth)
    # -----------------------------------------------------------------------
    print("Pollen (Atmo Occitanie)...")

    POLLEN_TAXA = {
        "GRAMINEE": "Graminées",  "OLEA":     "Olivier",
        "CUPRESSA": "Cyprès",     "PLATANUS": "Platane",
        "AMBROSIA": "Ambroisie",  "FRAXINUS": "Frêne",
        "QUERCUS":  "Chêne",      "URTICACE": "Urticacées",
        "BETULA":   "Bouleau",    "ARTEMISI": "Armoise",
    }
    POL_LABELS = {0:"N.C.", 1:"Très faible", 2:"Faible", 3:"Moyen", 4:"Élevé", 5:"Très élevé", 6:"Extrêmement élevé"}
    POL_COLORS = {0:"#94a3b8", 1:"#10b981", 2:"#a3e635", 3:"#f59e0b", 4:"#f97316", 5:"#ef4444", 6:"#7c3aed"}

    pollen_url = ("https://services9.arcgis.com/7Sr9Ek9c1QTKmbwr/arcgis/rest/services"
                  "/Indice_Pollens_sur_la_region_Occitanie/FeatureServer/0/query"
                  "?where=code_zone+IN+(30,34)&outFields=*&orderByFields=date_ech+DESC"
                  "&resultRecordCount=10&f=json")
    d_pol = get_json(pollen_url)

    pollen_results = []
    if d_pol and "features" in d_pol:
        seen = set()
        for f in d_pol["features"]:
            a    = f.get("attributes", {})
            dept = str(a.get("code_zone", ""))
            if dept in seen: continue
            seen.add(dept)
            date_ms  = a.get("date_ech")
            date_str = datetime.fromtimestamp(date_ms / 1000).strftime("%Y-%m-%d") if date_ms else ""
            indice_g = a.get("indice") or 0
            taxa = {
                name: {"indice": a.get(code) or 0,
                        "label":  POL_LABELS.get(a.get(code) or 0, "N.C."),
                        "color":  POL_COLORS.get(a.get(code) or 0, "#94a3b8")}
                for code, name in POLLEN_TAXA.items()
            }
            pollen_results.append({
                "dept":          dept,
                "lib_zone":      a.get("lib_zone", f"Département {dept}"),
                "date":          date_str,
                "indice_global": indice_g,
                "label_global":  POL_LABELS.get(indice_g, "N.C."),
                "color_global":  POL_COLORS.get(indice_g, "#94a3b8"),
                "taxa":          taxa
            })
    print(f"  {len(pollen_results)} départements pollen")

    # -----------------------------------------------------------------------
    # 6. PESTICIDES DANS L'AIR — source : Atmo Occitanie (ArcGIS FeatureServer)
    # -----------------------------------------------------------------------
    print("Pesticides dans l'air (Atmo Occitanie)...")
    pesticide_url = (
        "https://services9.arcgis.com/7Sr9Ek9c1QTKmbwr/arcgis/rest/services"
        "/Concentrations_hebdomadaires_Pesticides_sur_la_Region_Occitanie_vue"
        "/FeatureServer/0/query"
        "?where=1%3D1"
        "&outFields=nom_site,molecule,famille,valeur,unite,date_debut,campagne,x_l93,y_l93"
        "&orderByFields=date_debut+DESC"
        "&resultRecordCount=8000"
        "&f=json"
    )
    d_pest = get_json(pesticide_url, timeout=60)
    pesticide_results = []
    if d_pest and "features" in d_pest:
        BBOX_X = (700000, 920000)
        BBOX_Y = (6230000, 6430000)
        def in_bbox(attrs):
            x = attrs.get("x_l93") or 0
            y = attrs.get("y_l93") or 0
            return BBOX_X[0] <= x <= BBOX_X[1] and BBOX_Y[0] <= y <= BBOX_Y[1]
        local_features = [f["attributes"] for f in d_pest["features"] if in_bbox(f.get("attributes", {}))]
        if not local_features:
            local_features = [f["attributes"] for f in d_pest["features"]]
        mol_latest = {}
        for a in local_features:
            mol = a.get("molecule", "")
            val = a.get("valeur")
            if not mol or val is None or val < 0:
                continue
            if mol not in mol_latest:
                mol_latest[mol] = a
        detected = [(mol, a) for mol, a in mol_latest.items() if a["valeur"] > 0]
        detected.sort(key=lambda x: x[1]["valeur"], reverse=True)
        for mol, a in detected:
            ts = a.get("date_debut")
            mol_date = datetime.fromtimestamp(ts / 1000, tz=ZoneInfo("Europe/Paris")).strftime("%d/%m/%Y") if ts else ""
            pesticide_results.append({
                "molecule": mol,
                "famille":  a.get("famille", ""),
                "valeur":   round(a["valeur"], 4),
                "unite":    a.get("unite", "ng/m³"),
                "site":     a.get("nom_site", ""),
                "date":     mol_date,
                "campagne": a.get("campagne", ""),
            })
    print(f"  {len(pesticide_results)} molécules détectées")

    # -----------------------------------------------------------------------
    # SAUVEGARDE
    # -----------------------------------------------------------------------
    final_data = {
        "nappes":     nappe_results,
        "potable":    potable,
        "rivieres":   rivieres,
        "air":        air_results,
        "pollen":     pollen_results,
        "pesticides": pesticide_results,
        "updated":    today.strftime("%d/%m/%Y à %H:%M")
    }
    out = os.path.join(BASE_DIR, "full_data.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(final_data, f, indent=2, ensure_ascii=False)
    print(f"\nfull_data.json généré — {len(nappe_results)} nappes, "
          f"{len(potable)} communes, {len(rivieres)} rivières, "
          f"{len(air_results)} zones air, {len(pollen_results)} depts pollen, "
          f"{len(pesticide_results)} pesticides.")

if __name__ == "__main__":
    run_all()
