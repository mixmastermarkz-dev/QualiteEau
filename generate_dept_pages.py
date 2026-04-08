#!/usr/bin/env python3
"""
generate_dept_pages.py — Génère une page HTML par département
à partir des fichiers data/dept/XX.json.
Met à jour sitemap.xml avec les URLs département.

Usage:
  python generate_dept_pages.py                # génère toutes les pages disponibles
  python generate_dept_pages.py --depts 82,34  # depts spécifiques
"""
import json
import os
import re
import argparse
from datetime import date

from shared import score_style

BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
BASE_URL      = "https://www.mon-environnement.fr"
TODAY         = date.today().strftime("%Y-%m-%d")
TODAY_FR      = date.today().strftime("%d/%m/%Y")
YEAR          = date.today().year

DATA_DEPT_DIR = os.path.join(BASE_DIR, "data", "dept")
OUT_DIR       = os.path.join(BASE_DIR, "departement")
SITEMAP_PATH  = os.path.join(BASE_DIR, "sitemap.xml")

COLOR_TO_DOT = {
    "#10b981": "bg-emerald-500",
    "#f59e0b": "bg-amber-400",
    "#ef4444": "bg-red-500",
}

QUAL_COLORS_TW = {
    "#10b981": "text-emerald-600 bg-emerald-50",
    "#a3e635": "text-lime-600 bg-lime-50",
    "#eab308": "text-yellow-600 bg-yellow-50",
    "#f59e0b": "text-amber-600 bg-amber-50",
    "#f97316": "text-orange-600 bg-orange-50",
    "#ef4444": "text-red-600 bg-red-50",
    "#7c3aed": "text-purple-700 bg-purple-50",
    "#94a3b8": "text-slate-500 bg-slate-100",
}

POL_COLORS_TW = QUAL_COLORS_TW


# ---------------------------------------------------------------------------
# UTILITAIRES
# ---------------------------------------------------------------------------

def iso_to_fr(d):
    if d and d != "—" and len(d) == 10 and d[4] == "-":
        return d[8:10] + "/" + d[5:7] + "/" + d[0:4]
    return d or "—"


# score_style importé depuis shared.py


def tw_for_color(color, mapping=None):
    if mapping is None:
        mapping = QUAL_COLORS_TW
    return mapping.get(color, "text-slate-500 bg-slate-100")


# ---------------------------------------------------------------------------
# RENDU SECTIONS
# ---------------------------------------------------------------------------

def render_param_row(name, p):
    val   = p.get("valeur")
    unite = p.get("unite", "")
    color = p.get("color", "#94a3b8")
    d     = iso_to_fr(p.get("date", "—"))
    dot   = COLOR_TO_DOT.get(color, "bg-slate-300")
    val_str = str(val) if val is not None else "—"
    return (
        f'<tr class="border-b border-slate-100 hover:bg-slate-50">'
        f'<td class="py-2 px-3 text-xs font-bold text-slate-700">{name}</td>'
        f'<td class="py-2 px-3 text-xs font-black text-slate-900">{val_str}'
        f'<span class="text-slate-400 font-normal ml-0.5">{unite}</span></td>'
        f'<td class="py-2 px-3 text-xs text-slate-400">{d}</td>'
        f'<td class="py-2 px-3"><span class="inline-block w-2.5 h-2.5 rounded-full {dot}"></span></td>'
        f'</tr>'
    )


def render_commune_card(c):
    nom   = c.get("nom", "?")
    score = c.get("score")
    sc, sl = score_style(score)
    score_str = str(score) if score is not None else "—"
    params_html = ""
    params = c.get("parametres", {})
    # Affiche les 3 premiers paramètres qui ont une valeur
    shown = 0
    for pname, p in params.items():
        if shown >= 3:
            break
        val = p.get("valeur")
        if val is None:
            continue
        dot = COLOR_TO_DOT.get(p.get("color", ""), "bg-slate-300")
        params_html += (
            f'<span class="flex items-center gap-1 text-[10px] text-slate-500">'
            f'<span class="w-2 h-2 rounded-full {dot} flex-shrink-0"></span>'
            f'{pname}</span>'
        )
        shown += 1
    return (
        f'<div class="bg-white rounded-2xl border border-slate-200 p-4 flex flex-col gap-2">'
        f'<div class="flex items-start justify-between gap-2">'
        f'<span class="font-black text-slate-800 text-sm leading-tight">{nom}</span>'
        f'<span class="flex-shrink-0 text-xs font-black rounded-full px-2 py-0.5 text-white" '
        f'style="background:{sc}">{score_str}/100</span>'
        f'</div>'
        f'<div class="flex flex-wrap gap-x-3 gap-y-1">{params_html}</div>'
        f'</div>'
    )


def render_riviere_card(r):
    nom   = r.get("nom", "?")
    score = r.get("score")
    sc, sl = score_style(score)
    score_str = str(score) if score is not None else "—"
    params = r.get("parametres", {})
    rows = "".join(render_param_row(n, p) for n, p in list(params.items())[:5])
    return (
        f'<div class="bg-white rounded-2xl border border-slate-200 overflow-hidden">'
        f'<div class="px-4 py-3 border-b border-slate-100 flex items-center justify-between gap-3">'
        f'<span class="font-black text-slate-800 text-sm leading-tight">{nom}</span>'
        f'<span class="flex-shrink-0 text-xs font-black rounded-full px-2 py-0.5 text-white" '
        f'style="background:{sc}">{score_str}/100</span>'
        f'</div>'
        f'<table class="w-full"><tbody>{rows}</tbody></table>'
        f'</div>'
    )


def render_nappe_card(n):
    nom   = n.get("nom_station") or n.get("nom", "?")
    dept  = n.get("dept", "")
    niv   = n.get("niveau_m")
    date_ = iso_to_fr(n.get("date", ""))
    tendance = n.get("tendance", "")
    color = n.get("color", "#94a3b8")
    dot   = COLOR_TO_DOT.get(color, "bg-slate-300")
    niv_str = f"{niv} m" if niv is not None else "N.C."
    t_icon = {"hausse": "↑", "baisse": "↓", "stable": "→"}.get(tendance, "")
    return (
        f'<div class="bg-white rounded-2xl border border-slate-200 p-4">'
        f'<div class="flex items-start justify-between gap-2 mb-2">'
        f'<span class="font-bold text-slate-800 text-sm leading-tight">{nom}</span>'
        f'<span class="inline-block w-2.5 h-2.5 rounded-full {dot} flex-shrink-0 mt-1"></span>'
        f'</div>'
        f'<div class="flex gap-4 text-xs text-slate-600">'
        f'<span>Niveau : <strong class="text-slate-900">{niv_str}</strong></span>'
        f'<span class="text-slate-400">{date_}</span>'
        f'{("<span class=\"font-bold text-sky-600\">" + t_icon + " " + tendance.capitalize() + "</span>") if tendance else ""}'
        f'</div>'
        f'</div>'
    )


def render_air_section(air):
    if not air:
        return '<p class="text-slate-400 text-sm">Données air non disponibles pour ce département.</p>'
    cards = []
    for z in air:
        nom    = z.get("nom_zone", "?")
        iq     = z.get("indice_qualite") or 0
        lbl    = z.get("label_qualite", "N.C.")
        color  = z.get("color_qualite", "#94a3b8")
        date_  = iso_to_fr(z.get("date", ""))
        tw     = tw_for_color(color)
        pols   = z.get("polluants", {})
        pol_html = ""
        for p_name, p_data in pols.items():
            pl = p_data.get("label", "N.C.")
            pc = p_data.get("color", "#94a3b8")
            pt = tw_for_color(pc)
            pol_html += (
                f'<span class="inline-flex items-center gap-1 text-[10px] font-bold px-1.5 py-0.5 rounded-lg {pt}">'
                f'{p_name} : {pl}</span>'
            )
        cards.append(
            f'<div class="bg-white rounded-2xl border border-slate-200 p-4">'
            f'<div class="flex items-center justify-between gap-2 mb-2">'
            f'<span class="font-black text-slate-800 text-sm">{nom}</span>'
            f'<span class="text-xs font-black px-2 py-0.5 rounded-full {tw}">{lbl} ({iq})</span>'
            f'</div>'
            f'<div class="flex flex-wrap gap-1.5">{pol_html}</div>'
            f'<p class="text-[10px] text-slate-400 mt-2">Date : {date_}</p>'
            f'</div>'
        )
    return '<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">' + "".join(cards) + '</div>'


def render_pollen_section(pollen):
    if not pollen:
        return '<p class="text-slate-400 text-sm">Données pollen non disponibles pour ce département.</p>'
    p = pollen[0]
    date_  = iso_to_fr(p.get("date", ""))
    zone   = p.get("lib_zone", "")
    ig     = p.get("indice_global") or 0
    lbl_g  = p.get("label_global", "N.C.")
    color_g = p.get("color_global", "#94a3b8")
    tw_g   = tw_for_color(color_g, POL_COLORS_TW)
    taxa   = p.get("taxa", {})
    rows = ""
    for taxon, t_data in taxa.items():
        lbl  = t_data.get("label", "N.C.")
        c    = t_data.get("color", "#94a3b8")
        tw   = tw_for_color(c, POL_COLORS_TW)
        idx  = t_data.get("indice", 0)
        rows += (
            f'<tr class="border-b border-slate-100">'
            f'<td class="py-2 px-4 text-sm text-slate-700 font-bold">{taxon}</td>'
            f'<td class="py-2 px-4"><span class="text-xs font-black px-2 py-0.5 rounded-full {tw}">{lbl}</span></td>'
            f'<td class="py-2 px-4 text-xs text-slate-400">{idx}</td>'
            f'</tr>'
        )
    return (
        f'<div class="bg-white rounded-2xl border border-slate-200 overflow-hidden">'
        f'<div class="px-4 py-3 border-b border-slate-100 flex items-center justify-between">'
        f'<div>'
        f'<p class="font-black text-slate-800">{zone}</p>'
        f'<p class="text-xs text-slate-400">Semaine du {date_}</p>'
        f'</div>'
        f'<span class="text-sm font-black px-3 py-1 rounded-full {tw_g}">{lbl_g} (indice {ig})</span>'
        f'</div>'
        f'<table class="w-full">'
        f'<thead><tr class="bg-slate-50">'
        f'<th class="py-2 px-4 text-left text-[10px] font-black text-slate-400 uppercase tracking-widest">Taxon</th>'
        f'<th class="py-2 px-4 text-left text-[10px] font-black text-slate-400 uppercase tracking-widest">Niveau</th>'
        f'<th class="py-2 px-4 text-left text-[10px] font-black text-slate-400 uppercase tracking-widest">Indice</th>'
        f'</tr></thead>'
        f'<tbody>{rows}</tbody>'
        f'</table>'
        f'</div>'
    )


# ---------------------------------------------------------------------------
# PAGE HTML
# ---------------------------------------------------------------------------

def build_dept_page(data):
    dept        = data.get("dept", "??")
    nom         = data.get("nom", f"Département {dept}")
    slug        = data.get("slug", dept)
    region      = data.get("region", "")
    updated     = data.get("updated", TODAY_FR)
    score_eau   = data.get("score_eau")
    nb_communes = data.get("nb_communes", 0)
    potable     = data.get("potable", [])
    rivieres    = data.get("rivieres", [])
    nappes      = data.get("nappes", [])
    air         = data.get("air", [])
    pollen      = data.get("pollen", [])

    sc, sl = score_style(score_eau)
    score_str = str(score_eau) if score_eau is not None else "—"

    meta_desc = (
        f"Qualité de l'eau et de l'air en {nom} ({dept}) : score eau potable {score_str}/100. "
        f"{nb_communes} communes suivies. Rivières, nappes phréatiques. "
        f"Données officielles ARS, Hub'Eau, Atmo, mises à jour quotidiennement."
    )

    # Sections données
    communes_html = ""
    if potable:
        cards = "\n".join(render_commune_card(c) for c in potable)
        communes_html = f'<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">{cards}</div>'
    else:
        communes_html = '<p class="text-slate-400 text-sm">Aucune donnée eau potable disponible pour ce département.</p>'

    rivieres_html = ""
    if rivieres:
        rivieres_html = "\n".join(render_riviere_card(r) for r in rivieres[:20])
        rivieres_html = f'<div class="space-y-3">{rivieres_html}</div>'
    else:
        rivieres_html = '<p class="text-slate-400 text-sm">Aucune donnée rivière disponible.</p>'

    nappes_html = ""
    if nappes:
        cards = "\n".join(render_nappe_card(n) for n in nappes[:20])
        nappes_html = f'<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">{cards}</div>'
    else:
        nappes_html = '<p class="text-slate-400 text-sm">Aucune donnée nappe disponible.</p>'

    air_html    = render_air_section(air)
    pollen_html = render_pollen_section(pollen)

    # Section air+pollen (masquée si vide pour les depts hors Occitanie)
    air_section = ""
    if air or pollen:
        air_section = f"""
    <!-- AIR -->
    <section class="bg-white rounded-3xl shadow-sm border border-slate-200 overflow-hidden">
        <div class="px-6 py-4 border-b border-slate-100">
            <h2 class="font-black text-slate-800 uppercase tracking-tight">Qualité de l'air</h2>
            <p class="text-xs text-slate-400 mt-0.5">Source : Atmo Occitanie</p>
        </div>
        <div class="p-6">{air_html}</div>
    </section>

    <!-- POLLEN -->
    <section class="bg-white rounded-3xl shadow-sm border border-slate-200 overflow-hidden">
        <div class="px-6 py-4 border-b border-slate-100">
            <h2 class="font-black text-slate-800 uppercase tracking-tight">Pollen</h2>
            <p class="text-xs text-slate-400 mt-0.5">Source : Atmo Occitanie</p>
        </div>
        <div class="p-6">{pollen_html}</div>
    </section>"""

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Qualité de l'eau et de l'air — {nom} ({dept}) — {YEAR} | Mon-Environnement.fr</title>
    <meta name="description" content="{meta_desc}">
    <meta name="robots" content="index, follow">
    <link rel="canonical" href="{BASE_URL}/departement/{slug}/">
    <meta property="og:type" content="website">
    <meta property="og:url" content="{BASE_URL}/departement/{slug}/">
    <meta property="og:title" content="Qualité eau et air — {nom} ({dept}) — {YEAR}">
    <meta property="og:description" content="{meta_desc}">
    <meta property="og:locale" content="fr_FR">
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🌿</text></svg>">
    <script type="application/ld+json">
{{
  "@context": "https://schema.org",
  "@type": "Dataset",
  "name": "Qualité environnementale — {nom} ({dept}) — {YEAR}",
  "description": "{meta_desc}",
  "url": "{BASE_URL}/departement/{slug}/",
  "dateModified": "{TODAY}",
  "inLanguage": "fr-FR",
  "license": "https://www.etalab.gouv.fr/licence-ouverte-open-licence",
  "spatialCoverage": {{"@type": "Place", "name": "{nom}", "addressRegion": "{region}", "addressCountry": "FR"}}
}}
    </script>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        #side-drawer {{ transform: translateX(-100%); transition: transform 0.28s cubic-bezier(.4,0,.2,1); }}
        #side-drawer.open {{ transform: translateX(0); }}
        #menu-overlay {{ opacity: 0; pointer-events: none; transition: opacity 0.28s ease; }}
        #menu-overlay.open {{ opacity: 1; pointer-events: auto; }}
        .score-badge {{ width:80px;height:80px;border-radius:50%;display:flex;flex-direction:column;align-items:center;justify-content:center;color:white;flex-shrink:0; }}
    </style>
</head>
<body class="bg-slate-100 min-h-screen font-sans antialiased text-slate-900 flex flex-col">

<!-- OVERLAY -->
<div id="menu-overlay" class="fixed inset-0 bg-black/40 z-[200]" onclick="closeMenu()"></div>

<!-- SIDE DRAWER -->
<nav id="side-drawer" class="fixed top-0 left-0 h-full w-72 bg-white shadow-2xl z-[201] flex flex-col">
    <div class="p-5 border-b border-slate-100 flex justify-between items-center">
        <span class="font-black uppercase text-slate-800 tracking-tight text-sm">Mon-Environnement.fr</span>
        <button onclick="closeMenu()" class="w-9 h-9 flex items-center justify-center rounded-xl text-slate-400 hover:text-slate-700 hover:bg-slate-100 text-xl leading-none">&times;</button>
    </div>
    <div class="flex-grow overflow-y-auto p-3 space-y-1">
        <a href="/"                      class="flex items-center px-4 py-4 rounded-2xl font-bold text-slate-700 hover:bg-slate-50 text-sm min-h-[48px]">🏠 Accueil</a>
        <a href="/departement/"          class="flex items-center px-4 py-4 rounded-2xl font-bold text-sky-600 bg-sky-50 text-sm min-h-[48px]">📍 Tous les départements</a>
        <a href="/eau-potable/"          class="flex items-center px-4 py-4 rounded-2xl font-bold text-slate-700 hover:bg-slate-50 text-sm min-h-[48px]">🏘️ Communes (34/30)</a>
        <a href="/contact.html"          class="flex items-center px-4 py-4 rounded-2xl font-bold text-slate-700 hover:bg-slate-50 text-sm min-h-[48px]">✉️ Contact</a>
        <a href="/mentions-legales.html" class="flex items-center px-4 py-4 rounded-2xl font-bold text-slate-700 hover:bg-slate-50 text-sm min-h-[48px]">📄 Mentions légales</a>
    </div>
</nav>

<!-- HEADER -->
<header class="bg-white border-b border-slate-200 shadow-sm">
    <div class="max-w-5xl mx-auto px-4 py-4 flex items-center justify-between gap-4">
        <div class="flex items-center gap-3">
            <button onclick="toggleMenu()" class="flex items-center justify-center w-11 h-11 rounded-2xl bg-slate-100 hover:bg-slate-200 transition-colors" aria-label="Menu">
                <svg class="w-5 h-5 text-slate-700" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M4 6h16M4 12h16M4 18h16"/>
                </svg>
            </button>
            <div>
                <a href="/" class="font-black text-slate-700 uppercase tracking-tight text-sm hover:text-sky-600 transition-colors">Mon-Environnement.fr</a>
                <p class="text-[10px] text-slate-400 italic mt-0.5">📡 Données mises à jour quotidiennement à 11h</p>
            </div>
        </div>
        <a href="/departement/" class="text-xs font-bold text-slate-400 hover:text-sky-500 transition-colors hidden md:block">← Tous les départements</a>
    </div>
</header>

<!-- MAIN -->
<main class="max-w-5xl mx-auto px-4 py-8 flex-grow w-full space-y-6">

    <!-- BREADCRUMB -->
    <nav class="text-xs text-slate-400 flex items-center gap-1.5">
        <a href="/" class="hover:text-sky-500">Accueil</a>
        <span>›</span>
        <a href="/departement/" class="hover:text-sky-500">Départements</a>
        <span>›</span>
        <span class="text-slate-600 font-bold">{nom} ({dept})</span>
    </nav>

    <!-- HERO -->
    <div class="bg-white rounded-3xl shadow-sm border border-slate-200 p-8 flex flex-col md:flex-row gap-6 items-start md:items-center">
        <div class="flex-grow">
            <p class="text-xs font-black text-slate-400 uppercase tracking-widest mb-2">{region} · Département {dept}</p>
            <h1 class="text-3xl md:text-4xl font-black leading-tight">
                <span class="text-sky-600">{nom}</span>
            </h1>
            <div class="flex flex-wrap gap-4 mt-3 text-sm text-slate-500">
                <span><strong class="text-slate-800">{nb_communes}</strong> communes suivies</span>
                <span><strong class="text-slate-800">{len(rivieres)}</strong> stations rivières</span>
                <span><strong class="text-slate-800">{len(nappes)}</strong> stations nappes</span>
            </div>
            <p class="text-[10px] text-slate-400 mt-2">Mis à jour le {updated}</p>
        </div>
        <div class="flex-shrink-0 text-center">
            <div class="score-badge mx-auto" style="background:{sc}">
                <span class="text-2xl font-black leading-none">{score_str}</span>
                <span class="text-xs opacity-80 mt-0.5">/100</span>
            </div>
            <p class="text-xs font-black uppercase tracking-wide mt-2 text-slate-500">{sl}</p>
            <p class="text-[10px] text-slate-400">Eau potable</p>
        </div>
    </div>

    <!-- EAU POTABLE -->
    <section class="bg-white rounded-3xl shadow-sm border border-slate-200 overflow-hidden">
        <div class="px-6 py-4 border-b border-slate-100 flex items-center justify-between">
            <div>
                <h2 class="font-black text-slate-800 uppercase tracking-tight">Eau potable</h2>
                <p class="text-xs text-slate-400 mt-0.5">Source : ARS via Hub'Eau · {nb_communes} communes</p>
            </div>
            <div class="flex items-center gap-3 text-[10px] font-bold text-slate-400">
                <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-full bg-emerald-500"></span> ≥80</span>
                <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-full bg-amber-400"></span> 50-79</span>
                <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-full bg-red-500"></span> &lt;50</span>
            </div>
        </div>
        <div class="p-6">{communes_html}</div>
    </section>

    <!-- RIVIÈRES -->
    <section class="bg-white rounded-3xl shadow-sm border border-slate-200 overflow-hidden">
        <div class="px-6 py-4 border-b border-slate-100">
            <h2 class="font-black text-slate-800 uppercase tracking-tight">Rivières</h2>
            <p class="text-xs text-slate-400 mt-0.5">Source : Hub'Eau qualité_rivieres · {len(rivieres)} stations</p>
        </div>
        <div class="p-6">{rivieres_html}</div>
    </section>

    <!-- NAPPES -->
    <section class="bg-white rounded-3xl shadow-sm border border-slate-200 overflow-hidden">
        <div class="px-6 py-4 border-b border-slate-100">
            <h2 class="font-black text-slate-800 uppercase tracking-tight">Nappes phréatiques</h2>
            <p class="text-xs text-slate-400 mt-0.5">Source : Hub'Eau niveaux_nappes · {len(nappes)} stations</p>
        </div>
        <div class="p-6">{nappes_html}</div>
    </section>
{air_section}
    <!-- RETOUR -->
    <div class="text-center pb-4">
        <a href="/departement/" class="inline-flex items-center gap-2 text-sky-600 font-black uppercase text-sm tracking-wider hover:text-sky-800 transition-colors">
            ← Tous les départements
        </a>
    </div>

</main>

<!-- FOOTER -->
<footer class="bg-slate-900 text-white py-10 mt-4">
    <div class="max-w-5xl mx-auto px-4 text-center">
        <p class="font-black uppercase text-lg mb-2">MON-<span class="text-sky-400">ENVIRONNEMENT</span>.FR</p>
        <p class="text-slate-400 text-sm">Surveillance indépendante · Données officielles ARS, BRGM, Hub'Eau, Atmo</p>
        <p class="text-slate-400 text-sm mt-1">{nom} ({dept}) · {region}</p>
        <p class="text-slate-600 text-xs mt-4">© {YEAR} · <a href="/" class="hover:text-white transition-colors">mon-environnement.fr</a></p>
    </div>
</footer>

<script>
function toggleMenu() {{
    document.getElementById('side-drawer').classList.toggle('open');
    document.getElementById('menu-overlay').classList.toggle('open');
}}
function closeMenu() {{
    document.getElementById('side-drawer').classList.remove('open');
    document.getElementById('menu-overlay').classList.remove('open');
}}
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# SITEMAP
# ---------------------------------------------------------------------------

def update_sitemap(dept_slugs):
    """Ajoute les URLs département au sitemap existant (ou crée si absent)."""
    dept_entries = []
    for slug in sorted(dept_slugs):
        dept_entries.append(
            f"  <url>\n"
            f"    <loc>{BASE_URL}/departement/{slug}/</loc>\n"
            f"    <lastmod>{TODAY}</lastmod>\n"
            f"    <changefreq>daily</changefreq>\n"
            f"    <priority>0.9</priority>\n"
            f"  </url>"
        )

    if not os.path.exists(SITEMAP_PATH):
        # Crée un sitemap minimal
        body = "\n".join(dept_entries)
        content = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
            f"{body}\n"
            "</urlset>\n"
        )
        with open(SITEMAP_PATH, "w", encoding="utf-8") as f:
            f.write(content)
        return

    with open(SITEMAP_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    # Supprime les entrées /departement/ existantes pour les remplacer
    content = re.sub(
        r'\s*<url>\s*<loc>[^<]*/departement/[^<]*</loc>.*?</url>',
        '',
        content,
        flags=re.DOTALL,
    )

    # Insère avant </urlset>
    insertion = "\n" + "\n".join(dept_entries)
    content = content.replace("</urlset>", insertion + "\n</urlset>")

    with open(SITEMAP_PATH, "w", encoding="utf-8") as f:
        f.write(content)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--depts", default="", help="Codes depts séparés par virgule")
    args = parser.parse_args()

    if not os.path.isdir(DATA_DEPT_DIR):
        print(f"Répertoire {DATA_DEPT_DIR} introuvable — aucune page générée.")
        return

    # Liste des fichiers JSON disponibles
    available = {
        f.replace(".json", ""): os.path.join(DATA_DEPT_DIR, f)
        for f in sorted(os.listdir(DATA_DEPT_DIR))
        if f.endswith(".json")
    }

    if args.depts:
        targets = {d.strip().upper() for d in args.depts.split(",")}
        # Normalise 34 → "34", 9 → "09"
        targets = {d.zfill(2) for d in targets}
        available = {k: v for k, v in available.items() if k in targets}

    if not available:
        print("Aucun fichier data/dept/*.json trouvé — rien à générer.")
        return

    generated_slugs = []
    for dept_code, json_path in available.items():
        try:
            with open(json_path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"  [ERREUR] {json_path}: {e}")
            continue

        slug = data.get("slug") or dept_code
        html = build_dept_page(data)

        page_dir = os.path.join(OUT_DIR, slug)
        os.makedirs(page_dir, exist_ok=True)
        with open(os.path.join(page_dir, "index.html"), "w", encoding="utf-8") as f:
            f.write(html)

        generated_slugs.append(slug)
        nom = data.get("nom", dept_code)
        nb  = data.get("nb_communes", 0)
        sc  = data.get("score_eau", "N.C.")
        print(f"  OK /departement/{slug}/ — {nom} ({dept_code}) · {nb} communes · score {sc}")

    if generated_slugs:
        update_sitemap(generated_slugs)
        print(f"\nPages générées : {len(generated_slugs)}")
        print(f"Sitemap mis à jour : {SITEMAP_PATH}")


if __name__ == "__main__":
    main()
