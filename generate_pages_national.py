#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
"""
generate_pages_national.py — Génère une page HTML par commune (eau potable)
pour toutes les communes ≥5 000 hab couvertes par Hub'Eau (France entière).

Sources :
  data/referentiel/communes.json   — GPS + slug + nom propre (geo.api.gouv.fr)
  data/dept/XX.json                — données Hub'Eau (data_fetcher_national.py)

Usage : python generate_pages_national.py
"""
import json
import os
import re
import math
import unicodedata
from datetime import date
from pathlib import Path

from shared import score_style

BASE_DIR  = Path(__file__).parent
DATA_DIR  = BASE_DIR / "data"
DEPT_DIR  = DATA_DIR / "dept"
REF_FILE  = DATA_DIR / "referentiel" / "communes.json"
OUT_DIR   = BASE_DIR / "eau-potable"

BASE_URL  = "https://www.mon-environnement.fr"
TODAY     = date.today().strftime("%Y-%m-%d")
TODAY_FR  = date.today().strftime("%d/%m/%Y")
YEAR      = date.today().year


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
    dphi   = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    return R * 2 * math.asin(math.sqrt(a))


def nearest_communes(commune: dict, all_communes: list, n: int = 5) -> list:
    """Retourne les n communes les plus proches géographiquement (hors self)."""
    lat0 = commune.get("lat")
    lon0 = commune.get("lon")
    if lat0 is None or lon0 is None:
        return [c for c in all_communes if c["slug"] != commune["slug"]][:n]
    distances = []
    for c in all_communes:
        if c["slug"] == commune["slug"]:
            continue
        if c.get("lat") is None:
            continue
        dist = haversine(lat0, lon0, c["lat"], c["lon"])
        distances.append((dist, c))
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


# ---------------------------------------------------------------------------
# COMPOSANTS HTML
# ---------------------------------------------------------------------------

def iso_to_fr(d: str) -> str:
    if d and d != "—" and len(d) == 10 and d[4] == "-":
        return d[8:10] + "/" + d[5:7] + "/" + d[0:4]
    return d or "—"


def render_param_row(name, p):
    val     = p.get("valeur")
    unite   = p.get("unite", "")
    color   = p.get("color", "#94a3b8")
    d       = iso_to_fr(p.get("date", "—"))
    dot     = COLOR_TO_DOT.get(color, "bg-slate-300")
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


def render_neighbor_card(c: dict) -> str:
    nom        = c["nom"]
    slug       = c["slug"]
    score      = c.get("score")
    sc, _      = score_style(score)
    score_str  = f"{score}/100" if score is not None else "—"
    dept_label = c.get("dept_label", c.get("dept", ""))
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


def build_json_ld(commune: dict, slug: str) -> str:
    nom       = commune["nom"]
    dept      = commune.get("dept", "")
    dept_nom  = commune.get("dept_nom", dept)
    region    = commune.get("region", "")
    score     = commune.get("score", "N/A")
    params    = commune.get("parametres", {})

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
            f"({dept_nom}, {dept}). Score qualité : {score}/100. "
            f"Source : ARS via Hub'Eau."
        ),
        "url": f"{BASE_URL}/eau-potable/{slug}/",
        "dateModified": TODAY,
        "inLanguage": "fr-FR",
        "license": "https://www.etalab.gouv.fr/licence-ouverte-open-licence",
        "creator": {
            "@type": "Organization",
            "name":  "Mon-Environnement.fr",
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
            "addressRegion": dept_nom,
            "addressCountry":"FR",
        },
        "variableMeasured": variables,
    }
    return json.dumps(ld, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# TEMPLATE PAGE COMMUNE
# ---------------------------------------------------------------------------

def build_page(commune: dict, neighbors_html: str, json_ld: str, slug: str) -> str:
    nom         = commune["nom"]
    dept        = commune.get("dept", "")
    dept_nom    = commune.get("dept_nom", dept)
    dept_label  = commune.get("dept_label", f"{dept_nom} ({dept})")
    score       = commune.get("score")
    score_color, score_label = score_style(score)
    score_str   = str(score) if score is not None else "—"
    conclusion  = commune.get("conclusion") or "Aucune conclusion disponible."
    origine     = commune.get("origine") or ""
    restric     = commune.get("restric") or ""
    restric_cls, restric_lbl = RESTRIC_STYLE.get(
        restric, ("bg-slate-100 text-slate-500", restric or "Aucune alerte")
    )

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
        f"Nitrates, pH, chlore, bactériologie et autres paramètres. "
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
                <span class="font-black text-slate-700 uppercase tracking-tight text-sm group-hover:text-sky-600 transition-colors">Mon-Environnement.fr</span>
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
        <p class="font-black uppercase text-lg mb-2">MON-<span class="text-sky-400">ENVIRONNEMENT</span>.FR</p>
        <p class="text-slate-400 text-sm">Surveillance indépendante · Données officielles ARS, BRGM, Hub'Eau</p>
        <p class="text-slate-400 text-sm">France entière · Communes de plus de 5 000 habitants</p>
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
    static_pages = [
        (f"{BASE_URL}/",                                    TODAY, "daily",   "1.0"),
        (f"{BASE_URL}/eau-potable/",                        TODAY, "monthly", "0.7"),
        (f"{BASE_URL}/contact.html",                        TODAY, "yearly",  "0.3"),
        (f"{BASE_URL}/mentions-legales.html",               TODAY, "yearly",  "0.3"),
        (f"{BASE_URL}/politique-confidentialite.html",      TODAY, "yearly",  "0.3"),
    ]
    urls = []
    for loc, lastmod, freq, pri in static_pages:
        urls.append(
            f"  <url>\n    <loc>{loc}</loc>\n    <lastmod>{lastmod}</lastmod>\n"
            f"    <changefreq>{freq}</changefreq>\n    <priority>{pri}</priority>\n  </url>"
        )
    for slug in sorted(commune_slugs):
        urls.append(
            f"  <url>\n    <loc>{BASE_URL}/eau-potable/{slug}/</loc>\n"
            f"    <lastmod>{TODAY}</lastmod>\n    <changefreq>daily</changefreq>\n"
            f"    <priority>0.8</priority>\n  </url>"
        )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(urls) + "\n</urlset>\n"
    )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    # 1. Charger le référentiel → GPS par code INSEE
    print(f"Chargement du référentiel communes...")
    with open(REF_FILE, encoding="utf-8") as f:
        referentiel = json.load(f)

    ref_by_code = {}
    for r in referentiel:
        coords = r["centre"]["coordinates"]  # [lon, lat]
        ref_by_code[r["code"]] = {
            "nom":  r["nom"],
            "slug": r["slug"],
            "lat":  coords[1],
            "lon":  coords[0],
        }
    print(f"  {len(ref_by_code)} communes dans le référentiel (≥5 000 hab)")

    # 2. Charger tous les fichiers dept/*.json et fusionner avec référentiel
    dept_files = sorted(DEPT_DIR.glob("*.json"))
    print(f"\nChargement des données Hub'Eau ({len(dept_files)} départements)...")

    all_communes = []   # liste de dicts enrichis (potable + GPS + dept_label)
    dept_stats   = {}   # dept → nb communes générées

    for dept_file in dept_files:
        with open(dept_file, encoding="utf-8") as f:
            dept_data = json.load(f)

        dept_code = dept_data.get("dept", dept_file.stem)
        dept_nom  = dept_data.get("nom", dept_code)
        dept_region = dept_data.get("region", "")
        dept_label  = f"{dept_nom} ({dept_code})"
        potable     = dept_data.get("potable", [])

        count = 0
        for commune in potable:
            insee = commune.get("insee", "")
            if insee not in ref_by_code:
                continue  # commune <5 000 hab ou hors référentiel

            ref = ref_by_code[insee]
            merged = dict(commune)          # copie des données potable
            merged["nom"]        = ref["nom"]    # nom proprement casé
            merged["slug"]       = ref["slug"]
            merged["lat"]        = ref["lat"]
            merged["lon"]        = ref["lon"]
            merged["dept_nom"]   = dept_nom
            merged["dept_label"] = dept_label
            merged["region"]     = dept_region
            all_communes.append(merged)
            count += 1

        if count:
            dept_stats[dept_code] = count

    print(f"  {len(all_communes)} communes à générer ({len(dept_stats)} départements couverts)")

    if not all_communes:
        print("Aucune commune trouvée — vérifie les fichiers data/dept/ et data/referentiel/communes.json")
        return

    # 3. Générer les pages
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    generated = []
    errors    = []

    print(f"\nGénération des pages...")
    for i, commune in enumerate(all_communes, 1):
        nom  = commune["nom"]
        slug = commune["slug"]
        try:
            neighbors      = nearest_communes(commune, all_communes, n=5)
            neighbors_html = "\n            ".join(render_neighbor_card(c) for c in neighbors)
            json_ld        = build_json_ld(commune, slug)
            html           = build_page(commune, neighbors_html, json_ld, slug)

            page_dir = OUT_DIR / slug
            page_dir.mkdir(parents=True, exist_ok=True)
            (page_dir / "index.html").write_text(html, encoding="utf-8")
            generated.append(slug)

            if i % 100 == 0 or i == len(all_communes):
                print(f"  {i}/{len(all_communes)} pages générées...")
        except Exception as e:
            errors.append((nom, str(e)))
            print(f"  ERREUR {nom}: {e}")

    # 4. Sitemap
    sitemap = generate_sitemap(generated)
    sitemap_path = BASE_DIR / "sitemap.xml"
    sitemap_path.write_text(sitemap, encoding="utf-8")

    print(f"\nTerminé :")
    print(f"  Pages générées   : {len(generated)}")
    print(f"  Erreurs          : {len(errors)}")
    print(f"  Sitemap          : {sitemap_path} ({len(generated) + 5} URLs)")
    if errors:
        print(f"\nCommunes en erreur :")
        for nom, err in errors:
            print(f"  {nom}: {err}")


if __name__ == "__main__":
    main()
