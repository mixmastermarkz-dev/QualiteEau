#!/usr/bin/env python3
"""
generate_national.py — Générateur de pages pour toutes les villes de France

Lit :
  - data/referentiel/communes.json   (36 000 communes INSEE, généré par fetch_communes.py)
  - data/dept/XX.json                (données par département, générées par data_fetcher_national.py)

Génère :
  - france/{slug}/index.html         (page FREE : données minimales, indexable)
  - france/{slug}/detail/index.html  (page PAID : toutes les données, gatée par CF Worker)

Usage :
  python v2/generate/generate_national.py
  python v2/generate/generate_national.py --dept 34,30   # test pilote
  python v2/generate/generate_national.py --free-only    # uniquement les pages free

Dépendances : stdlib uniquement
"""

import argparse
import json
import os
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
COMMUNES_FILE = ROOT / "data" / "referentiel" / "communes.json"
DEPT_DATA_DIR = ROOT / "data" / "dept"
OUT_DIR = ROOT / "france"


# ── Helpers ─────────────────────────────────────────────────────────────────

def slugify(name: str) -> str:
    """Convertit un nom de commune en slug URL."""
    s = unicodedata.normalize("NFD", name.lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.replace("'", "-").replace(" ", "-").replace("/", "-")
    s = "".join(c for c in s if c.isalnum() or c == "-")
    while "--" in s:
        s = s.replace("--", "-")
    return s.strip("-")


def load_dept_data(dept_code: str) -> dict:
    """Charge le JSON de département si disponible."""
    path = DEPT_DATA_DIR / f"{dept_code}.json"
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


# ── Extraction FREE (données minimales) ─────────────────────────────────────

def extract_free(commune: dict, dept_data: dict) -> dict:
    """
    Extrait uniquement les données du tier FREE pour une commune.
    Score global + label seulement, pas de détail paramètres.
    """
    nom = commune["nom"]
    code_dept = commune["codeDepartement"]
    free = {
        "nom": nom,
        "dept": code_dept,
        "tier": "free",
    }

    # Eau potable — score global + conclusion courte
    potable = dept_data.get("potable", [])
    rec = next((p for p in potable if _norm(p.get("nom", "")) == _norm(nom)), None)
    if rec:
        free["eau"] = {
            "score": rec.get("score"),
            "label": rec.get("score_label"),
            "color": rec.get("score_color"),
            "conclusion": (rec.get("conclusion") or "")[:120],  # tronqué
            "date": _latest_date(rec.get("parametres", {})),
        }

    # Air — indice global + label
    air = dept_data.get("air", [])
    if air:
        zone = air[0]  # premier enregistrement du dept
        free["air"] = {
            "indice": zone.get("indice"),
            "label": zone.get("label"),
            "color": zone.get("color"),
            "date": zone.get("date"),
        }

    # Pollen — indice global dept
    pollen = dept_data.get("pollen", [])
    rec_p = next((p for p in pollen if p.get("dept") == code_dept), None)
    if rec_p:
        free["pollen"] = {
            "indice": rec_p.get("indice_global"),
            "label": rec_p.get("label_global"),
            "color": rec_p.get("color_global"),
            "date": rec_p.get("date"),
        }

    # Nappes — niveau + tendance (sans historique)
    nappes = dept_data.get("nappes", [])
    nappe = next((n for n in nappes if _dept_match(n.get("dept", ""), code_dept)), None)
    if nappe:
        diff = round(nappe.get("current", 0) - nappe.get("year_ago", 0), 2)
        free["nappes"] = {
            "current": nappe.get("current"),
            "trend": "hausse" if diff > 0.2 else "baisse" if diff < -0.2 else "stable",
        }

    return free


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFD", s.lower())
    return "".join(c for c in s if unicodedata.category(c) != "Mn").strip()


def _dept_match(dept_str: str, code: str) -> bool:
    return dept_str.lstrip("0") == code.lstrip("0")


def _latest_date(parametres: dict) -> str:
    dates = [p.get("date", "") for p in parametres.values() if p.get("date")]
    return max(dates) if dates else ""


# ── Génération HTML ──────────────────────────────────────────────────────────

def render_free_page(commune: dict, data: dict) -> str:
    """Génère la page HTML free d'une commune."""
    nom = commune["nom"]
    dept_name = commune.get("nomDepartement", commune["codeDepartement"])
    slug = slugify(nom)
    lat = commune.get("centre", {}).get("coordinates", [None, None])[1]
    lon = commune.get("centre", {}).get("coordinates", [None, None])[0]

    eau = data.get("eau", {})
    air = data.get("air", {})
    pollen = data.get("pollen", {})

    eau_score = eau.get("score", "—")
    eau_label = eau.get("label", "")
    eau_color = eau.get("color", "#94a3b8")
    air_indice = air.get("indice", "—")
    air_label = air.get("label", "")
    air_color = air.get("color", "#94a3b8")
    pollen_indice = pollen.get("indice", "—")
    pollen_label = pollen.get("label", "")

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Environnement à {nom} ({dept_name}) — Air, Eau, Pollen | Mon-Environnement.fr</title>
<meta name="description" content="Qualité de l'air, eau potable et risque pollinique à {nom} ({dept_name}). Indice ATMO {air_indice}/6, eau {eau_score}/100. Données officielles.">
<meta name="robots" content="index, follow">
<link rel="canonical" href="https://www.mon-environnement.fr/france/{slug}/">
<meta property="og:title" content="Environnement à {nom} — Mon-Environnement.fr">
<meta property="og:description" content="Air : {air_label} ({air_indice}/6) · Eau : {eau_label} ({eau_score}/100) · Pollen : {pollen_label}">
<meta property="og:type" content="website">
<meta name="theme-color" content="#14b8a6">
<script type="application/ld+json">{{
  "@context": "https://schema.org",
  "@type": "Dataset",
  "name": "Données environnementales — {nom}",
  "description": "Qualité de l''air, eau potable, nappes phréatiques et pollen à {nom}",
  "spatialCoverage": {{
    "@type": "Place",
    "name": "{nom}, {dept_name}",
    "geo": {{"@type": "GeoCoordinates","latitude": {lat},"longitude": {lon}}}
  }},
  "publisher": {{"@type": "Organization","name": "Mon-Environnement.fr","url": "https://www.mon-environnement.fr"}}
}}</script>
<!-- TODO: intégrer le template CSS/JS de la page nationale -->
</head>
<body>
  <h1>Environnement à {nom}</h1>
  <p>Qualité de l'air : <strong style="color:{air_color}">{air_indice}/6 — {air_label}</strong></p>
  <p>Eau potable : <strong style="color:{eau_color}">{eau_score}/100 — {eau_label}</strong></p>
  <p>Risque pollinique : <strong>{pollen_indice}/5 — {pollen_label}</strong></p>
  <a href="/france/{slug}/detail/">Voir toutes les données détaillées →</a>
  <p><small>Données libres — sources officielles ARS, Atmo, BRGM, Hub'Eau</small></p>
</body>
</html>"""


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Génère les pages nationales")
    parser.add_argument("--dept", help="Codes depts à traiter (ex: 34,30,31)")
    parser.add_argument("--free-only", action="store_true", help="Générer seulement les pages free")
    args = parser.parse_args()

    if not COMMUNES_FILE.exists():
        print(f"❌ {COMMUNES_FILE} introuvable. Lancer d'abord fetch_communes.py", file=sys.stderr)
        sys.exit(1)

    with open(COMMUNES_FILE, encoding="utf-8") as f:
        communes = json.load(f)

    # Filtrer par département si spécifié
    if args.dept:
        target_depts = set(d.strip().zfill(2) for d in args.dept.split(","))
        communes = [c for c in communes if c.get("codeDepartement", "").zfill(2) in target_depts]

    print(f"→ {len(communes)} communes à traiter")

    dept_cache: dict[str, dict] = {}
    generated = 0
    skipped = 0

    for commune in communes:
        code_dept = commune.get("codeDepartement", "")
        nom = commune.get("nom", "")
        if not nom or not code_dept:
            skipped += 1
            continue

        # Charger les données du département (cache)
        if code_dept not in dept_cache:
            dept_cache[code_dept] = load_dept_data(code_dept)
        dept_data = dept_cache[code_dept]

        slug = slugify(nom)
        free_data = extract_free(commune, dept_data)

        # Page FREE
        free_dir = OUT_DIR / slug
        free_dir.mkdir(parents=True, exist_ok=True)
        (free_dir / "index.html").write_text(
            render_free_page(commune, free_data), encoding="utf-8"
        )

        # Page PAID (placeholder — à compléter avec le template détaillé)
        if not args.free_only:
            detail_dir = OUT_DIR / slug / "detail"
            detail_dir.mkdir(parents=True, exist_ok=True)
            # TODO: render_paid_page(commune, dept_data)
            # Pour l'instant, page placeholder
            (detail_dir / "index.html").write_text(
                f"<!-- PAID: {nom} — données complètes à générer -->", encoding="utf-8"
            )

        generated += 1
        if generated % 500 == 0:
            print(f"  {generated}/{len(communes)} pages générées…")

    print(f"✅ {generated} communes générées, {skipped} ignorées")


if __name__ == "__main__":
    main()
