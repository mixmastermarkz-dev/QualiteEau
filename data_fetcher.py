"""
data_fetcher.py — Collecte quotidienne des données environnementales (34 & 30).
Écrit full_data.json à la racine du projet.

Optimisations vs version précédente :
  - Constantes et utilitaires centralisés dans shared.py (DRY)
  - Communes et nappes fetchées en parallèle (ThreadPoolExecutor)
  - Encodage BSS unifié via shared.encode_bss()
"""
import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

from shared import (
    PARAMS_POTABLE, PARAMS_RIVIERE, CONF_TEMP,
    QUAL_LABELS, QUAL_COLORS, SUB_PARAMS, AIR_ZONES,
    POL_LABELS, POL_COLORS, POLLEN_TAXA,
    get_json, get_color, get_temp_color, calc_score,
    score_color, score_label, extract_nom_cours_eau,
    parse_result, encode_bss,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# COMMUNES SUIVIES (Hérault 34 + Gard 30)
# ---------------------------------------------------------------------------
COMMUNES = {
    # ── HÉRAULT (34) ─────────────────────────────────────────────────────
    "34172": "Montpellier",
    "34032": "Béziers",
    "34301": "Sète",
    "34003": "Agde",
    "34145": "Lunel",
    "34108": "Frontignan",
    "34299": "Sérignan",
    "34270": "Saint-Jean-de-Védas",
    "34057": "Castelnau-le-Lez",
    "34154": "Mauguio",
    "34129": "Lattes",
    "34116": "Grabels",
    "34113": "Gigean",
    "34114": "Gignac",
    "34082": "Le Crès",
    "34077": "Clapiers",
    "34079": "Clermont-l'Hérault",
    "34123": "Juvignac",
    "34202": "Pignan",
    "34255": "Saint-Gély-du-Fesc",
    "34150": "Marseillan",
    "34197": "Pézenas",
    "34023": "Balaruc-les-Bains",
    "34088": "Cournonterral",
    "34217": "Prades-le-Lez",
    "34213": "Poussan",
    "34153": "Mèze",
    "34151": "Marsillargues",
    "34198": "Pérols",
    "34332": "Vias",
    "34192": "Palavas-les-Flots",
    "34058": "Castries",
    "34095": "Fabrègues",
    "34120": "Jacou",
    "34022": "Baillargues",
    "34327": "Vendargues",
    "34101": "Florensac",
    "34142": "Lodève",
    "34247": "Saint-Clément-de-Rivière",
    "34240": "Saint-Aunès",
    "34183": "Nissan-lez-Enserune",
    "34146": "Lunel-Viel",
    "34169": "Montferrier-sur-Lez",
    "34162": "Montagnac",
    "34324": "Valras-Plage",
    "34148": "Maraussan",
    "34051": "Canet",
    "34052": "Capestang",
    "34028": "Bédarieux",
    "34037": "Boujan-sur-Libron",
    "34031": "Bessan",
    "34111": "Ganges",
    "34165": "Montbazin",
    "34166": "Montblanc",
    "34159": "Mireval",
    "34276": "Saint-Mathieu-de-Tréviers",
    "34259": "Saint-Georges-d'Orques",
    "34337": "Villeneuve-lès-Maguelone",
    "34336": "Villeneuve-lès-Béziers",
    # ── GARD (30) ────────────────────────────────────────────────────────
    "30189": "Nîmes",
    "30007": "Alès",
    "30028": "Bagnols-sur-Cèze",
    "30032": "Beaucaire",
    "30258": "Saint-Gilles",
    "30351": "Villeneuve-lès-Avignon",
    "30341": "Vauvert",
    "30202": "Pont-Saint-Esprit",
    "30334": "Uzès",
    "30156": "Marguerittes",
    "30011": "Les Angles",
    "30034": "Bellegarde",
    "30133": "Le Grau-du-Roi",
    "30003": "Aigues-Mortes",
    "30217": "Rochefort-du-Gard",
    "30047": "Bouillargues",
    "30243": "Saint-Christol-lez-Alès",
    "30259": "Saint-Hilaire-de-Brethmas",
    "30169": "Milhaud",
    "30062": "Calvisson",
    "30125": "Garons",
    "30155": "Manduel",
    "30344": "Vergèze",
    "30006": "Aimargues",
    "30321": "Sommières",
    "30132": "La Grand-Combe",
    "30082": "Clarensac",
    "30060": "Caissargues",
    "30221": "Roquemaure",
    "30012": "Aramon",
    "30141": "Laudun-l'Ardoise",
    "30255": "Saint-Geniès-de-Malgoirès",
    "30333": "Uchaud",
    "30211": "Redessan",
    "30123": "Gallargues-le-Montueux",
    "30263": "Saint-Hippolyte-du-Fort",
    "30227": "Saint-Ambroix",
    "30210": "Quissac",
    "30179": "Montfrin",
    "30212": "Remoulins",
    "30305": "Salindres",
    "30350": "Le Vigan",
    "30276": "Saint-Laurent-des-Arbres",
}

ORIGINES = {
    "Montpellier":    "Source du Lez",
    "Nîmes":          "Vistrenque / Rhône",
    "Béziers":        "Orb / Sables Astiens",
    "Sète":           "Rhône / Pliocène",
    "Agde":           "Hérault / Sables Astiens",
    "Alès":           "Gardon",
    "Bagnols-sur-Cèze": "Rhône",
    "Beaucaire":      "Rhône",
    "Pont-Saint-Esprit": "Rhône",
}

# ---------------------------------------------------------------------------
# UTILITAIRE LOCAL
# ---------------------------------------------------------------------------

def dept_from_coords(lat, lon) -> str:
    """Estime le département (34 ou 30) à partir des coordonnées GPS."""
    if lat is None or lon is None:
        return "34"
    d34 = (lat - 43.55) ** 2 + (lon - 3.35) ** 2
    d30 = (lat - 43.95) ** 2 + (lon - 4.20) ** 2
    return "30" if d30 < d34 else "34"

def get_temperature_meteo(lat, lon):
    """Température air via Open-Meteo / Météo-France (temps réel)."""
    url = (f"https://api.open-meteo.com/v1/meteofrance"
           f"?latitude={lat}&longitude={lon}"
           f"&current=temperature_2m,soil_temperature_0cm"
           f"&timezone=Europe%2FParis")
    d = get_json(url)
    if not d or "current" not in d:
        return None, None
    return d["current"].get("temperature_2m"), d["current"].get("soil_temperature_0cm")

# ---------------------------------------------------------------------------
# SECTION 1 — EAU POTABLE (parallèle par commune)
# ---------------------------------------------------------------------------

def _fetch_commune(insee: str, nom: str) -> dict:
    """
    Récupère toutes les données eau potable pour une commune.
    Conçu pour ThreadPoolExecutor — indépendant et thread-safe.
    """
    # Restriction sécheresse VigiEau
    restric = "Vigilance"
    d_res = get_json(
        f"https://api.vigieau.gouv.fr/api/zones?commune={insee}&profil=particulier",
        timeout=10,
    )
    if d_res:
        lvls = [z.get("niveauGravite", "") for z in d_res]
        if "crise" in lvls:              restric = "Crise"
        elif "alerte_renforcee" in lvls: restric = "Alerte Renforcée"
        elif "alerte" in lvls:           restric = "Alerte"

    conclusion, parametres, historique = "N/A", {}, {}
    got_conclusion = False

    for p_code, conf in PARAMS_POTABLE.items():
        url_q = (f"https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis"
                 f"?code_commune={insee}&code_parametre={p_code}&size=200&sort=desc")
        d_q = get_json(url_q)
        if not d_q or not d_q.get("data"):
            continue

        if not got_conclusion:
            conclusion = d_q["data"][0].get("conclusion_conformite_prelevement", "N/A")
            got_conclusion = True

        p_name = conf["name"]
        for record in d_q["data"]:
            val_num, display, color, date_str = parse_result(record, conf)
            if display is None:
                continue
            if p_name not in parametres:
                parametres[p_name] = {
                    "valeur": display,
                    "unite":  record.get("libelle_unite", conf["unite"]),
                    "color":  color,
                    "date":   date_str,
                }
            historique.setdefault(p_name, [])
            if len(historique[p_name]) < 5:
                historique[p_name].append({"date": date_str, "valeur": val_num, "color": color})
            if len(historique[p_name]) >= 5:
                break

    score = calc_score(parametres)
    return {
        "nom":        nom,
        "dept":       insee[:2],
        "restric":    restric,
        "origine":    ORIGINES.get(nom, "Nappe ou captage local"),
        "conclusion": conclusion,
        "parametres": parametres,
        "historique": historique,
        "score":      score,
        "score_color": score_color(score),
        "score_label": score_label(score),
    }


def fetch_potable() -> list:
    print(f"Eau potable (ARS / Hub'Eau) — {len(COMMUNES)} communes en parallèle…")
    results = []
    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = {pool.submit(_fetch_commune, insee, nom): nom
                   for insee, nom in COMMUNES.items()}
        for future in as_completed(futures):
            nom = futures[future]
            try:
                r = future.result()
                results.append(r)
                print(f"  ✓ {nom} — {len(r['parametres'])} params — score {r['score']}")
            except Exception as exc:
                print(f"  [ERREUR] {nom}: {exc}")
    results.sort(key=lambda c: c["nom"])
    return results

# ---------------------------------------------------------------------------
# SECTION 2 — RIVIÈRES (appel unique département + enrichissement météo)
# ---------------------------------------------------------------------------

def fetch_rivieres(today: datetime) -> list:
    print("Qualité rivières (Hub'Eau analyse_pc + Météo-France)…")
    two_years_ago = (today - timedelta(days=730)).strftime("%Y-%m-%d")
    codes_str     = ",".join(PARAMS_RIVIERE.keys())

    url = (f"https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/analyse_pc"
           f"?code_departement=34,30&code_parametre={codes_str}"
           f"&date_debut_prelevement={two_years_ago}&size=20000&sort=desc")
    d_all = get_json(url, timeout=90)

    par_station = {}
    if d_all:
        for p in d_all.get("data", []):
            cs = p.get("code_station")
            if not cs:
                continue
            if cs not in par_station:
                lat = p.get("latitude")
                lon = p.get("longitude")
                par_station[cs] = {
                    "nom":      p.get("libelle_station", cs),
                    "dept":     dept_from_coords(lat, lon),
                    "lat":      lat,
                    "lon":      lon,
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

        lat, lon = info["lat"], info["lon"]
        if lat and lon:
            air, _ = get_temperature_meteo(lat, lon)
            if air is not None:
                parametres["Temp. air"] = {
                    "valeur": round(air, 1), "unite": "°C",
                    "color": get_temp_color(air),
                    "date": today.strftime("%Y-%m-%d"), "realtime": True,
                }

        score = calc_score({k: v for k, v in parametres.items() if not v.get("realtime")})
        rivieres.append({
            "nom":           info["nom"],
            "commune":       info["nom"],
            "dept":          info["dept"],
            "lat":           info["lat"],
            "lon":           info["lon"],
            "nom_cours_eau": extract_nom_cours_eau(info["nom"]),
            "parametres":    parametres,
            "historique":    historique,
            "score":         score,
            "score_color":   score_color(score),
            "score_label":   score_label(score),
        })
        print(f"  {info['nom']} ({info['dept']}) — {len(parametres)} params — score {score}")

    return rivieres

# ---------------------------------------------------------------------------
# SECTION 3 — NAPPES (parallèle par station)
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

    d_old    = get_json(f"https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques"
                        f"?code_bss={bss}&date_debut_mesure={one_year_ago}&size=1&sort=asc")
    year_ago = d_old["data"][0]["niveau_nappe_eau"] if d_old and d_old.get("data") else None

    return {
        "nom":      s["nom_commune"],
        "dept":     s["code_departement"],
        "code_bss": s["code_bss"],
        "current":  current,
        "history":  history,
        "year_ago": year_ago,
        "lat":      s.get("y"),
        "lng":      s.get("x"),
        "color":    "#3b82f6" if (year_ago and current > year_ago) else "#ef4444",
    }


def fetch_nappes(one_year_ago: str) -> list:
    print("Nappes (BRGM / Hub'Eau)…")
    d_nap = get_json("https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations"
                     "?code_departement=34,30&format=json&size=500")
    if not d_nap or not d_nap.get("data"):
        return []

    active = [s for s in d_nap["data"]
              if not s.get("date_fin_mesure") or s["date_fin_mesure"] >= "2024-01-01"]

    results = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(_fetch_nappe_station, s, one_year_ago) for s in active]
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
                print(f"  {r['nom']} — {r['current']:.2f} m NGF")

    results.sort(key=lambda x: x["nom"])
    return results

# ---------------------------------------------------------------------------
# SECTION 4 — AIR (Atmo Occitanie)
# ---------------------------------------------------------------------------

def fetch_air() -> list:
    print("Qualité de l'air (Atmo Occitanie)…")
    wfs_url = ("https://dservices9.arcgis.com/7Sr9Ek9c1QTKmbwr/arcgis/services/ind_occitanie/WFSServer"
               "?service=WFS&version=2.0.0&request=GetFeature"
               "&typeName=ind_occitanie:ind_occitanie&outputFormat=GEOJSON&count=500")
    d_air = get_json(wfs_url)
    if not d_air or "features" not in d_air:
        return []

    latest = {}
    for f in d_air["features"]:
        p  = f.get("properties", {})
        cz = p.get("code_zone")
        if cz not in AIR_ZONES:
            continue
        if cz not in latest or p.get("date_ech", "") > latest[cz].get("date_ech", ""):
            latest[cz] = p

    results = []
    for cz, p in latest.items():
        qual = p.get("code_qual") or 0
        results.append({
            "zone":   AIR_ZONES[cz]["nom"],
            "dept":   AIR_ZONES[cz]["dept"],
            "date":   p.get("date_ech", ""),
            "indice": qual,
            "label":  p.get("lib_qual") or QUAL_LABELS.get(qual, "N.C."),
            "color":  p.get("coul_qual") or QUAL_COLORS.get(qual, "#94a3b8"),
            "polluants": {
                label: {
                    "indice": p.get(f"code_{key}") or 0,
                    "label":  QUAL_LABELS.get(p.get(f"code_{key}") or 0, "N.C."),
                    "color":  QUAL_COLORS.get(p.get(f"code_{key}") or 0, "#94a3b8"),
                }
                for key, label in SUB_PARAMS
            },
        })
    print(f"  {len(results)} zones air")
    return results

# ---------------------------------------------------------------------------
# SECTION 5 — POLLEN (Atmo Occitanie)
# ---------------------------------------------------------------------------

def fetch_pollen(today: datetime) -> list:
    print("Pollen (Atmo Occitanie)…")
    url = ("https://services9.arcgis.com/7Sr9Ek9c1QTKmbwr/arcgis/rest/services"
           "/Indice_Pollens_sur_la_region_Occitanie/FeatureServer/0/query"
           "?where=code_zone+IN+(30,34)&outFields=*&orderByFields=date_ech+DESC"
           "&resultRecordCount=10&f=json")
    d = get_json(url)
    if not d or "features" not in d:
        return []

    results, seen = [], set()
    for f in d["features"]:
        a    = f.get("attributes", {})
        dept = str(a.get("code_zone", ""))
        if dept in seen:
            continue
        seen.add(dept)
        date_ms  = a.get("date_ech")
        date_str = (datetime.fromtimestamp(date_ms / 1000, tz=ZoneInfo("Europe/Paris"))
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
            "dept":          dept,
            "lib_zone":      a.get("lib_zone", f"Département {dept}"),
            "date":          date_str,
            "indice_global": indice_g,
            "label_global":  POL_LABELS.get(indice_g, "N.C."),
            "color_global":  POL_COLORS.get(indice_g, "#94a3b8"),
            "taxa":          taxa,
        })
    print(f"  {len(results)} départements pollen")
    return results

# ---------------------------------------------------------------------------
# SECTION 6 — PESTICIDES DANS L'AIR (Atmo Occitanie)
# ---------------------------------------------------------------------------

def fetch_pesticides(today: datetime) -> list:
    print("Pesticides dans l'air (Atmo Occitanie)…")
    url = (
        "https://services9.arcgis.com/7Sr9Ek9c1QTKmbwr/arcgis/rest/services"
        "/Concentrations_hebdomadaires_Pesticides_sur_la_Region_Occitanie_vue"
        "/FeatureServer/0/query"
        "?where=1%3D1"
        "&outFields=nom_site,molecule,famille,valeur,unite,date_debut,campagne,x_l93,y_l93"
        "&orderByFields=date_debut+DESC"
        "&resultRecordCount=8000&f=json"
    )
    d = get_json(url, timeout=60)
    if not d or "features" not in d:
        return []

    BBOX_X = (700000, 920000)
    BBOX_Y = (6230000, 6430000)

    def in_bbox(attrs):
        x = attrs.get("x_l93") or 0
        y = attrs.get("y_l93") or 0
        return BBOX_X[0] <= x <= BBOX_X[1] and BBOX_Y[0] <= y <= BBOX_Y[1]

    all_attrs   = [f["attributes"] for f in d["features"]]
    local_attrs = [a for a in all_attrs if in_bbox(a)] or all_attrs

    mol_latest = {}
    for a in local_attrs:
        mol = a.get("molecule", "")
        val = a.get("valeur")
        if mol and val is not None and val >= 0 and mol not in mol_latest:
            mol_latest[mol] = a

    results = []
    for mol, a in sorted(mol_latest.items(), key=lambda x: x[1]["valeur"], reverse=True):
        if a["valeur"] <= 0:
            continue
        ts = a.get("date_debut")
        mol_date = (datetime.fromtimestamp(ts / 1000, tz=ZoneInfo("Europe/Paris"))
                    .strftime("%d/%m/%Y")) if ts else ""
        results.append({
            "molecule": mol,
            "famille":  a.get("famille", ""),
            "valeur":   round(a["valeur"], 4),
            "unite":    a.get("unite", "ng/m³"),
            "site":     a.get("nom_site", ""),
            "date":     mol_date,
            "campagne": a.get("campagne", ""),
        })
    print(f"  {len(results)} molécules détectées")
    return results

# ---------------------------------------------------------------------------
# POINT D'ENTRÉE
# ---------------------------------------------------------------------------

def run_all():
    today        = datetime.now(tz=ZoneInfo("Europe/Paris"))
    one_year_ago = (today - timedelta(days=365)).strftime("%Y-%m-%d")
    out_path     = os.path.join(BASE_DIR, "full_data.json")

    potable    = fetch_potable()
    rivieres   = fetch_rivieres(today)
    nappes     = fetch_nappes(one_year_ago)
    air        = fetch_air()
    pollen     = fetch_pollen(today)
    pesticides = fetch_pesticides(today)

    # Garde-fou : ne pas écraser les rivières si le fetch a retourné 0 résultats
    if len(rivieres) == 0:
        try:
            with open(out_path, encoding="utf-8") as f:
                prev = json.load(f)
            if prev.get("rivieres"):
                print(f"  [GARDE-FOU] 0 rivières — conservation des {len(prev['rivieres'])} stations précédentes.")
                rivieres = prev["rivieres"]
        except Exception:
            pass

    final_data = {
        "nappes":     nappes,
        "potable":    potable,
        "rivieres":   rivieres,
        "air":        air,
        "pollen":     pollen,
        "pesticides": pesticides,
        "updated":    today.strftime("%d/%m/%Y à %H:%M"),
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(final_data, f, indent=2, ensure_ascii=False)

    print(f"\nfull_data.json généré — {len(nappes)} nappes, {len(potable)} communes, "
          f"{len(rivieres)} rivières, {len(air)} zones air, "
          f"{len(pollen)} depts pollen, {len(pesticides)} pesticides.")


if __name__ == "__main__":
    run_all()
