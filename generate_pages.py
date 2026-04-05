#!/usr/bin/env python3
"""
generate_pages.py — Génère une page HTML statique par commune (eau potable)
à partir de full_data.json, ainsi qu'un sitemap.xml mis à jour.
"""
import json
import os
import re
import math
import unicodedata
from datetime import date

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_URL  = "https://www.mon-environnement.fr"
TODAY     = date.today().strftime("%Y-%m-%d")   # pour sitemap (norme ISO)
TODAY_FR  = date.today().strftime("%d/%m/%Y")   # pour affichage
YEAR      = date.today().year

# ---------------------------------------------------------------------------
# COORDONNÉES GPS (lat, lon) — toutes les communes du 34 et du 30
# ---------------------------------------------------------------------------
GPS = {
    # Hérault (34)
    "Montpellier":                 (43.6108,  3.8767),
    "Béziers":                     (43.3442,  3.2156),
    "Sète":                        (43.4013,  3.6900),
    "Agde":                        (43.3108,  3.4741),
    "Lunel":                       (43.6747,  4.1353),
    "Frontignan":                  (43.4476,  3.7556),
    "Sérignan":                    (43.2803,  3.2853),
    "Saint-Jean-de-Védas":         (43.5739,  3.8072),
    "Castelnau-le-Lez":            (43.6344,  3.9286),
    "Mauguio":                     (43.6169,  4.0072),
    "Lattes":                      (43.5697,  3.8956),
    "Grabels":                     (43.6503,  3.8264),
    "Gigean":                      (43.4936,  3.7156),
    "Gignac":                      (43.6519,  3.5522),
    "Le Crès":                     (43.6358,  3.9500),
    "Clapiers":                    (43.6617,  3.8836),
    "Clermont-l'Hérault":          (43.6281,  3.4317),
    "Juvignac":                    (43.6169,  3.8233),
    "Pignan":                      (43.5742,  3.7964),
    "Saint-Gély-du-Fesc":          (43.7000,  3.8333),
    "Marseillan":                  (43.3594,  3.5361),
    "Pézenas":                     (43.4619,  3.4228),
    "Balaruc-les-Bains":           (43.4456,  3.6794),
    "Cournonterral":               (43.5344,  3.7522),
    "Prades-le-Lez":               (43.7064,  3.8669),
    "Poussan":                     (43.4917,  3.6583),
    "Mèze":                        (43.4378,  3.6022),
    "Marsillargues":               (43.6694,  4.1867),
    "Pérols":                      (43.5756,  3.9494),
    "Vias":                        (43.3183,  3.4119),
    "Palavas-les-Flots":           (43.5272,  3.9278),
    "Castries":                    (43.6817,  3.9983),
    "Fabrègues":                   (43.5528,  3.7714),
    "Jacou":                       (43.6553,  3.9167),
    "Baillargues":                 (43.6458,  4.0233),
    "Vendargues":                  (43.6381,  3.9817),
    "Florensac":                   (43.3861,  3.4628),
    "Lodève":                      (43.7317,  3.3181),
    "Saint-Clément-de-Rivière":    (43.7100,  3.8300),
    "Saint-Aunès":                 (43.6383,  3.9783),
    "Nissan-lez-Enserune":         (43.2894,  3.1522),
    "Lunel-Viel":                  (43.6819,  4.1156),
    "Montferrier-sur-Lez":         (43.6897,  3.8817),
    "Montagnac":                   (43.4756,  3.4867),
    "Valras-Plage":                (43.2467,  3.2922),
    "Maraussan":                   (43.3764,  3.1583),
    "Canet":                       (43.3703,  3.3686),
    "Capestang":                   (43.3317,  3.0406),
    "Bédarieux":                   (43.6156,  3.1556),
    "Boujan-sur-Libron":           (43.3581,  3.2361),
    "Bessan":                      (43.3569,  3.4375),
    "Ganges":                      (43.9347,  3.7083),
    "Montbazin":                   (43.5194,  3.7194),
    "Montblanc":                   (43.3958,  3.3700),
    "Mireval":                     (43.5028,  3.8283),
    "Saint-Mathieu-de-Tréviers":   (43.7669,  3.8761),
    "Saint-Georges-d'Orques":      (43.5814,  3.7639),
    "Villeneuve-lès-Maguelone":    (43.5325,  3.8742),
    "Villeneuve-lès-Béziers":      (43.3206,  3.2469),
    # Gard (30)
    "Nîmes":                       (43.8367,  4.3600),
    "Alès":                        (44.1258,  4.0825),
    "Bagnols-sur-Cèze":            (44.1600,  4.6186),
    "Beaucaire":                   (43.8078,  4.6428),
    "Saint-Gilles":                (43.6772,  4.4333),
    "Villeneuve-lès-Avignon":      (43.9608,  4.7900),
    "Vauvert":                     (43.6942,  4.2736),
    "Pont-Saint-Esprit":           (44.2558,  4.6506),
    "Uzès":                        (44.0125,  4.4194),
    "Marguerittes":                (43.8633,  4.4297),
    "Les Angles":                  (43.9675,  4.7736),
    "Bellegarde":                  (43.7386,  4.5208),
    "Le Grau-du-Roi":              (43.5356,  4.1339),
    "Aigues-Mortes":               (43.5678,  4.1917),
    "Rochefort-du-Gard":           (43.9567,  4.7014),
    "Bouillargues":                (43.7919,  4.3756),
    "Saint-Christol-lez-Alès":     (44.0722,  4.1053),
    "Saint-Hilaire-de-Brethmas":   (44.1072,  4.0953),
    "Milhaud":                     (43.7742,  4.2947),
    "Calvisson":                   (43.7750,  4.2144),
    "Garons":                      (43.7764,  4.3656),
    "Manduel":                     (43.7819,  4.3139),
    "Vergèze":                     (43.7297,  4.2806),
    "Aimargues":                   (43.6894,  4.2136),
    "Sommières":                   (43.7836,  4.0883),
    "La Grand-Combe":              (44.2256,  4.0228),
    "Clarensac":                   (43.8278,  4.2239),
    "Caissargues":                 (43.7661,  4.4033),
    "Roquemaure":                  (44.0456,  4.7014),
    "Aramon":                      (43.8925,  4.7161),
    "Laudun-l'Ardoise":            (44.1006,  4.6700),
    "Saint-Geniès-de-Malgoirès":   (43.9364,  4.2583),
    "Uchaud":                      (43.7606,  4.2803),
    "Redessan":                    (43.8089,  4.3797),
    "Gallargues-le-Montueux":      (43.7267,  4.1714),
    "Saint-Hippolyte-du-Fort":     (43.9694,  3.8597),
    "Saint-Ambroix":               (44.2619,  4.1894),
    "Quissac":                     (43.9094,  4.0025),
    "Montfrin":                    (43.8806,  4.5825),
    "Remoulins":                   (43.9336,  4.5617),
    "Salindres":                   (44.1661,  4.1528),
    "Le Vigan":                    (43.9928,  3.6094),
    "Saint-Laurent-des-Arbres":    (44.0875,  4.7292),
}

# ---------------------------------------------------------------------------
# UTILITAIRES
# ---------------------------------------------------------------------------

def slugify(name: str) -> str:
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "-", name)
    return name.strip("-")


def haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def nearest_communes(nom: str, all_communes: list, n: int = 5) -> list:
    """Retourne les n communes les plus proches géographiquement (hors self)."""
    if nom not in GPS:
        return [c for c in all_communes if c["nom"] != nom][:n]
    lat0, lon0 = GPS[nom]
    distances = []
    for c in all_communes:
        if c["nom"] == nom:
            continue
        cname = c["nom"]
        if cname in GPS:
            lat1, lon1 = GPS[cname]
            distances.append((haversine(lat0, lon0, lat1, lon1), c))
    distances.sort(key=lambda x: x[0])
    return [c for _, c in distances[:n]]


# ---------------------------------------------------------------------------
# STYLES
# ---------------------------------------------------------------------------

RESTRIC_STYLE = {
    "Vigilance": ("bg-green-100 text-green-700",   "Vigilance"),
    "Alerte":    ("bg-yellow-100 text-yellow-700",  "Alerte"),
    "Renforcée": ("bg-orange-100 text-orange-700",  "Alerte renforcée"),
    "Crise":     ("bg-red-100 text-red-700",         "Crise"),
}

COLOR_TO_DOT = {
    "#10b981": "bg-emerald-500",
    "#f59e0b": "bg-amber-400",
    "#ef4444": "bg-red-500",
}


def score_style(score):
    if score is None:
        return "#94a3b8", "Données insuffisantes"
    if score >= 80:
        return "#10b981", "Bonne qualité"
    if score >= 50:
        return "#f59e0b", "Qualité moyenne"
    return "#ef4444", "Mauvaise qualité"


# ---------------------------------------------------------------------------
# COMPOSANTS HTML
# ---------------------------------------------------------------------------

def iso_to_fr(d: str) -> str:
    """Convertit YYYY-MM-DD en DD/MM/YYYY, laisse intact si autre format."""
    if d and d != "—" and len(d) == 10 and d[4] == "-":
        return d[8:10] + "/" + d[5:7] + "/" + d[0:4]
    return d or "—"


def render_param_row(name, p):
    val    = p.get("valeur")
    unite  = p.get("unite", "")
    color  = p.get("color", "#94a3b8")
    d      = iso_to_fr(p.get("date", "—"))
    dot    = COLOR_TO_DOT.get(color, "bg-slate-300")
    val_str = str(val) if val is not None else "—"
    return (
        f'<tr class="border-b border-slate-100 hover:bg-slate-50 transition-colors">'
        f'<td class="py-3 px-4 font-bold text-slate-700 text-sm">{name}</td>'
        f'<td class="py-3 px-4 text-sm text-slate-900 font-black">{val_str}'
        f' <span class="text-slate-400 font-normal text-xs">{unite}</span></td>'
        f'<td class="py-3 px-4 text-xs text-slate-400">{d}</td>'
        f'<td class="py-3 px-4"><span class="inline-block w-3 h-3 rounded-full {dot}"></span></td>'
        f'</tr>'
    )


def render_neighbor_card(c, slug_map):
    nom   = c["nom"]
    slug  = slug_map.get(nom, slugify(nom))
    score = c.get("score")
    sc, _ = score_style(score)
    score_str  = f"{score}/100" if score is not None else "—"
    dept_label = "Hérault (34)" if c.get("dept") == "34" else "Gard (30)"
    return (
        f'<a href="/eau-potable/{slug}/" '
        f'class="block p-4 bg-white rounded-2xl border border-slate-100 '
        f'hover:border-sky-400 hover:shadow-md transition-all group">'
        f'<div class="flex justify-between items-start mb-1">'
        f'<span class="font-black text-slate-800 group-hover:text-sky-600 '
        f'transition-colors text-sm leading-tight">{nom}</span>'
        f'<span class="text-xs font-black rounded-full px-2 py-0.5 ml-2 flex-shrink-0" '
        f'style="background:{sc}22;color:{sc}">{score_str}</span>'
        f'</div>'
        f'<span class="text-[10px] font-bold text-slate-400 uppercase tracking-wide">{dept_label}</span>'
        f'</a>'
    )


def build_json_ld(commune, slug):
    nom      = commune["nom"]
    dept     = commune.get("dept", "")
    dept_name = "Hérault" if dept == "34" else "Gard"
    score    = commune.get("score", "N/A")
    params   = commune.get("parametres", {})

    variables = []
    for pname, pdata in params.items():
        val = pdata.get("valeur")
        if val is not None:
            variables.append({
                "@type":    "PropertyValue",
                "name":     pname,
                "value":    val,
                "unitText": pdata.get("unite", ""),
            })

    ld = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": f"Qualité de l'eau potable à {nom} — {YEAR}",
        "description": (
            f"Données de qualité de l'eau potable pour la commune de {nom} "
            f"({dept_name}, {dept}). Score qualité : {score}/100. "
            f"Source : ARS via Hub'Eau."
        ),
        "url": f"{BASE_URL}/eau-potable/{slug}/",
        "dateModified": TODAY,
        "inLanguage": "fr-FR",
        "license": "https://www.etalab.gouv.fr/licence-ouverte-open-licence",
        "creator": {
            "@type": "Organization",
            "name":  "Qualité Air et Eau 34/30",
            "url":   BASE_URL,
        },
        "publisher": {
            "@type": "Organization",
            "name":  "Hub'Eau — BRGM / OFB / ARS",
            "url":   "https://hubeau.eaufrance.fr",
        },
        "spatialCoverage": {
            "@type":         "Place",
            "name":          nom,
            "addressRegion": dept_name,
            "addressCountry":"FR",
        },
        "variableMeasured": variables,
    }
    return json.dumps(ld, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# TEMPLATE PAGE COMMUNE
# ---------------------------------------------------------------------------

def build_page(commune, neighbors_html, json_ld, slug):
    nom   = commune["nom"]
    dept  = commune.get("dept", "")
    dept_label  = "Hérault (34)" if dept == "34" else "Gard (30)"
    score       = commune.get("score")
    score_color, score_label = score_style(score)
    score_str   = str(score) if score is not None else "—"
    conclusion  = commune.get("conclusion") or "Aucune conclusion disponible."
    origine     = commune.get("origine") or ""
    restric     = commune.get("restric") or ""
    restric_cls, restric_lbl = RESTRIC_STYLE.get(restric, ("bg-slate-100 text-slate-500", restric or "Aucune alerte"))

    params = commune.get("parametres", {})
    if params:
        rows_html = "".join(render_param_row(n, p) for n, p in params.items())
    else:
        rows_html = (
            '<tr><td colspan="4" class="py-6 text-center text-slate-400 text-sm">'
            "Aucune donnée de paramètre disponible.</td></tr>"
        )

    origine_block = ""
    if origine:
        origine_block = (
            '<div class="bg-white rounded-2xl border border-slate-200 p-4">'
            '<p class="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Source d\'eau</p>'
            f'<p class="font-black text-slate-800">{origine}</p>'
            '</div>'
        )

    meta_desc = (
        f"Qualité de l'eau potable à {nom} ({dept_label}) : score {score_str}/100. "
        f"Nitrates, pH, chlore, bactériologie et 8 autres paramètres. "
        f"Données officielles ARS via Hub'Eau, mises à jour chaque matin."
    )

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Qualité de l'eau potable à {nom} — {YEAR} | Mon-Environnement.fr</title>
    <meta name="description" content="{meta_desc}">
    <meta name="robots" content="index, follow">
    <link rel="canonical" href="{BASE_URL}/eau-potable/{slug}/">
    <meta property="og:type" content="website">
    <meta property="og:url" content="{BASE_URL}/eau-potable/{slug}/">
    <meta property="og:title" content="Qualité de l'eau potable à {nom} — {YEAR}">
    <meta property="og:description" content="{meta_desc}">
    <meta property="og:locale" content="fr_FR">
    <meta name="twitter:card" content="summary">
    <meta name="twitter:title" content="Qualité de l'eau potable à {nom} — {YEAR}">
    <meta name="twitter:description" content="{meta_desc}">
    <script type="application/ld+json">
{json_ld}
    </script>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        .score-badge {{
            width: 88px; height: 88px; border-radius: 50%;
            display: flex; flex-direction: column;
            align-items: center; justify-content: center;
            color: white; flex-shrink: 0;
        }}
    </style>
</head>
<body class="bg-slate-100 min-h-screen font-sans antialiased text-slate-900 flex flex-col">

<!-- HEADER -->
<header class="bg-white border-b border-slate-200 shadow-sm">
    <div class="max-w-4xl mx-auto px-4 py-4 flex items-center justify-between gap-4">
        <a href="/" class="flex items-center gap-3 group">
            <span class="text-slate-400 group-hover:text-sky-500 transition-colors text-lg font-black">←</span>
            <div>
                <span class="font-black text-slate-700 uppercase tracking-tight text-sm group-hover:text-sky-600 transition-colors">Qualité Air et Eau 34/30</span>
                <p class="text-[10px] text-slate-400 italic mt-0.5">📡 Données mises à jour quotidiennement à 11h</p>
            </div>
        </a>
        <span class="text-xs font-bold text-slate-400 uppercase tracking-widest hidden md:block">{dept_label}</span>
    </div>
</header>

<!-- MAIN -->
<main class="max-w-4xl mx-auto px-4 py-8 flex-grow w-full space-y-6">

    <!-- H1 + SCORE -->
    <div class="bg-white rounded-3xl shadow-sm border border-slate-200 p-8 flex flex-col md:flex-row gap-6 items-start md:items-center">
        <div class="flex-grow">
            <p class="text-xs font-black text-slate-400 uppercase tracking-widest mb-2">{dept_label} · Eau Potable</p>
            <h1 class="text-3xl md:text-4xl font-black leading-tight">
                Qualité de l'eau potable à <span class="text-sky-600">{nom}</span> — {YEAR}
            </h1>
            <p class="text-sm text-slate-500 mt-3 leading-relaxed max-w-2xl">{conclusion}</p>
        </div>
        <div class="flex-shrink-0 text-center">
            <div class="score-badge mx-auto" style="background:{score_color}">
                <span class="text-3xl font-black leading-none">{score_str}</span>
                <span class="text-xs opacity-80 mt-0.5">/100</span>
            </div>
            <p class="text-xs font-black uppercase tracking-wide mt-2 text-slate-500">{score_label}</p>
        </div>
    </div>

    <!-- INFO BAR -->
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div class="bg-white rounded-2xl border border-slate-200 p-4">
            <p class="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Département</p>
            <p class="font-black text-slate-800 text-sm">{dept_label}</p>
        </div>
        {origine_block}
        <div class="bg-white rounded-2xl border border-slate-200 p-4">
            <p class="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Alerte sécheresse</p>
            <span class="inline-block text-xs font-black px-3 py-1 rounded-full {restric_cls}">{restric_lbl}</span>
        </div>
        <div class="bg-white rounded-2xl border border-slate-200 p-4">
            <p class="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Mis à jour</p>
            <p class="font-black text-slate-800 text-sm">{TODAY_FR}</p>
        </div>
    </div>

    <!-- PARAMETRES -->
    <div class="bg-white rounded-3xl shadow-sm border border-slate-200 overflow-hidden">
        <div class="px-6 py-4 border-b border-slate-100">
            <h2 class="font-black text-slate-800 uppercase tracking-tight">Paramètres analysés</h2>
            <p class="text-xs text-slate-400 mt-0.5">Source : ARS via Hub'Eau · Contrôles sanitaires officiels</p>
        </div>
        <div class="overflow-x-auto">
            <table class="w-full">
                <thead>
                    <tr class="bg-slate-50">
                        <th class="py-2 px-4 text-left text-[10px] font-black text-slate-400 uppercase tracking-widest">Paramètre</th>
                        <th class="py-2 px-4 text-left text-[10px] font-black text-slate-400 uppercase tracking-widest">Valeur</th>
                        <th class="py-2 px-4 text-left text-[10px] font-black text-slate-400 uppercase tracking-widest">Date mesure</th>
                        <th class="py-2 px-4 text-left text-[10px] font-black text-slate-400 uppercase tracking-widest">Conformité</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>
        <p class="text-[10px] text-slate-400 px-6 py-3 flex items-center gap-4">
            <span class="flex items-center gap-1"><span class="inline-block w-2.5 h-2.5 rounded-full bg-emerald-500"></span> Conforme</span>
            <span class="flex items-center gap-1"><span class="inline-block w-2.5 h-2.5 rounded-full bg-amber-400"></span> Vigilance</span>
            <span class="flex items-center gap-1"><span class="inline-block w-2.5 h-2.5 rounded-full bg-red-500"></span> Dépassement</span>
        </p>
    </div>

    <!-- COMMUNES VOISINES -->
    <section class="bg-white rounded-3xl shadow-sm border border-slate-200 p-6">
        <h2 class="font-black text-slate-800 uppercase tracking-tight mb-1">Communes voisines</h2>
        <p class="text-xs text-slate-400 mb-4">Qualité de l'eau potable dans les communes les plus proches</p>
        <div class="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3">
            {neighbors_html}
        </div>
    </section>

    <!-- RETOUR ACCUEIL -->
    <div class="text-center pb-4">
        <a href="/" class="inline-flex items-center gap-2 text-sky-600 font-black uppercase text-sm tracking-wider hover:text-sky-800 transition-colors">
            ← Retour au tableau de bord complet
        </a>
    </div>

</main>

<!-- FOOTER -->
<footer class="bg-slate-900 text-white py-10 mt-4">
    <div class="max-w-4xl mx-auto px-4 text-center">
        <p class="font-black uppercase text-lg mb-2">QUALITÉ <span class="text-sky-400">AIR ET EAU</span></p>
        <p class="text-slate-400 text-sm">Surveillance indépendante · Données officielles ARS, BRGM, Hub'Eau</p>
        <p class="text-slate-400 text-sm">Hérault (34) &amp; Gard (30) · Communes de plus de 1 000 habitants</p>
        <div class="mt-4 flex justify-center gap-6 text-xs text-slate-500">
            <a href="/contact.html" class="hover:text-white transition-colors underline">Contact</a>
            <a href="/mentions-legales.html" class="hover:text-white transition-colors underline">Mentions légales</a>
            <a href="/politique-confidentialite.html" class="hover:text-white transition-colors underline">Confidentialité</a>
        </div>
        <p class="text-slate-600 text-xs mt-4">© {YEAR} · <a href="/" class="hover:text-white transition-colors">Mon-Environnement.fr</a></p>
    </div>
</footer>

</body>
</html>"""


# ---------------------------------------------------------------------------
# SITEMAP
# ---------------------------------------------------------------------------

def generate_sitemap(commune_slugs: list) -> str:
    # Pages statiques permanentes
    static_pages = [
        (f"{BASE_URL}/",                           TODAY, "daily",   "1.0"),
        (f"{BASE_URL}/eau-potable/",               TODAY, "monthly", "0.7"),
        (f"{BASE_URL}/contact.html",                          TODAY, "yearly",  "0.3"),
        (f"{BASE_URL}/mentions-legales.html",               TODAY, "yearly",  "0.3"),
        (f"{BASE_URL}/politique-confidentialite.html",      TODAY, "yearly",  "0.3"),
    ]
    urls = []
    for loc, lastmod, freq, pri in static_pages:
        urls.append(
            f"  <url>\n"
            f"    <loc>{loc}</loc>\n"
            f"    <lastmod>{lastmod}</lastmod>\n"
            f"    <changefreq>{freq}</changefreq>\n"
            f"    <priority>{pri}</priority>\n"
            f"  </url>"
        )
    for slug in sorted(commune_slugs):
        urls.append(
            f"  <url>\n"
            f"    <loc>{BASE_URL}/eau-potable/{slug}/</loc>\n"
            f"    <lastmod>{TODAY}</lastmod>\n"
            f"    <changefreq>daily</changefreq>\n"
            f"    <priority>0.8</priority>\n"
            f"  </url>"
        )
    body = "\n".join(urls)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{body}\n"
        "</urlset>\n"
    )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    json_path = os.path.join(BASE_DIR, "full_data.json")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    communes = data.get("potable", [])
    if not communes:
        print("Aucune donnée potable dans full_data.json — rien à générer.")
        return

    # Correspondance nom → slug pour les liens internes
    slug_map = {c["nom"]: slugify(c["nom"]) for c in communes}

    out_dir = os.path.join(BASE_DIR, "eau-potable")
    os.makedirs(out_dir, exist_ok=True)

    generated = []
    for commune in communes:
        nom  = commune["nom"]
        slug = slug_map[nom]

        neighbors     = nearest_communes(nom, communes, n=5)
        neighbors_html = "\n            ".join(render_neighbor_card(c, slug_map) for c in neighbors)
        json_ld       = build_json_ld(commune, slug)
        html          = build_page(commune, neighbors_html, json_ld, slug)

        page_dir = os.path.join(out_dir, slug)
        os.makedirs(page_dir, exist_ok=True)
        with open(os.path.join(page_dir, "index.html"), "w", encoding="utf-8") as f:
            f.write(html)

        generated.append(slug)
        print(f"  OK /eau-potable/{slug}/")

    # Sitemap mis à jour
    sitemap = generate_sitemap(generated)
    with open(os.path.join(BASE_DIR, "sitemap.xml"), "w", encoding="utf-8") as f:
        f.write(sitemap)

    print(f"\nSitemap mis à jour — {len(generated) + 5} URLs ({len(generated)} communes + 5 pages statiques)")
    print(f"Pages générées    — {len(generated)} communes")


if __name__ == "__main__":
    main()
