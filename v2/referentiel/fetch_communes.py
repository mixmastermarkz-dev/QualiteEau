#!/usr/bin/env python3
"""
fetch_communes.py — Télécharge le référentiel national des communes françaises
depuis l'API Géo officielle du gouvernement.

Génère : data/referentiel/communes.json
  Environ 34 000 communes continentales + DOM (hors collectivités d'outre-mer)

Usage : python v2/referentiel/fetch_communes.py
        python v2/referentiel/fetch_communes.py --min-pop 500   # filtrer par population

Source : https://geo.api.gouv.fr/communes
Licence : Licence Ouverte 2.0 (Etalab)
"""

import argparse
import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
OUT_FILE = ROOT / "data" / "referentiel" / "communes.json"

GEO_API_URL = (
    "https://geo.api.gouv.fr/communes"
    "?fields=nom,code,codeDepartement,codeRegion,centre,population,codesPostaux"
    "&format=json"
    "&geometry=centre"
)


def fetch_communes(min_pop: int = 0) -> list[dict]:
    print(f"Téléchargement depuis {GEO_API_URL} …")
    req = urllib.request.Request(GEO_API_URL, headers={"Accept": "application/json"})

    with urllib.request.urlopen(req, timeout=60) as resp:
        communes = json.loads(resp.read().decode())

    print(f"→ {len(communes)} communes reçues")

    if min_pop > 0:
        communes = [c for c in communes if (c.get("population") or 0) >= min_pop]
        print(f"→ {len(communes)} communes après filtre pop ≥ {min_pop}")

    # Normaliser les clés et ajouter le slug
    result = []
    for c in communes:
        slug = _slugify(c.get("nom", ""))
        result.append({
            "nom": c.get("nom", ""),
            "slug": slug,
            "code": c.get("code", ""),          # code INSEE 5 chiffres
            "codeDepartement": c.get("codeDepartement", ""),
            "codeRegion": c.get("codeRegion", ""),
            "codesPostaux": c.get("codesPostaux", []),
            "population": c.get("population", 0),
            "centre": c.get("centre", {}),       # GeoJSON Point {type, coordinates: [lon, lat]}
        })

    # Trier par département puis par nom
    result.sort(key=lambda x: (x["codeDepartement"], x["nom"]))
    return result


def _slugify(name: str) -> str:
    import unicodedata
    s = unicodedata.normalize("NFD", name.lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.replace("'", "-").replace(" ", "-").replace("/", "-")
    s = "".join(c for c in s if c.isalnum() or c == "-")
    while "--" in s:
        s = s.replace("--", "-")
    return s.strip("-")


def main():
    parser = argparse.ArgumentParser(description="Télécharge le référentiel communes")
    parser.add_argument("--min-pop", type=int, default=0,
                        help="Population minimale (0 = toutes les communes)")
    args = parser.parse_args()

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    try:
        communes = fetch_communes(args.min_pop)
    except Exception as e:
        print(f"❌ Erreur lors du téléchargement : {e}", file=sys.stderr)
        sys.exit(1)

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(communes, f, ensure_ascii=False, separators=(",", ":"))

    size_mb = OUT_FILE.stat().st_size / 1_048_576
    print(f"✅ {len(communes)} communes → {OUT_FILE} ({size_mb:.1f} Mo)")
    print(f"   Exemple : {communes[0]['nom']} ({communes[0]['codeDepartement']})")


if __name__ == "__main__":
    main()
