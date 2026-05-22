#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
"""
validate_national.py — Valide que Hub'Eau retourne des données
pour un échantillon de communes dans des régions variées.

Usage : python v2/referentiel/validate_national.py
"""
import json
import urllib.request
import urllib.parse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT      = Path(__file__).parent.parent.parent
REF_FILE  = ROOT / "data" / "referentiel" / "communes.json"

# 20 villes test — 1 par grande région, diversité géographique
SAMPLE = [
    {"nom": "Montpellier",    "code": "34172", "dept": "34"},
    {"nom": "Marseille",      "code": "13055", "dept": "13"},
    {"nom": "Lyon",           "code": "69123", "dept": "69"},
    {"nom": "Toulouse",       "code": "31555", "dept": "31"},
    {"nom": "Bordeaux",       "code": "33063", "dept": "33"},
    {"nom": "Nantes",         "code": "44109", "dept": "44"},
    {"nom": "Strasbourg",     "code": "67482", "dept": "67"},
    {"nom": "Lille",          "code": "59350", "dept": "59"},
    {"nom": "Rennes",         "code": "35238", "dept": "35"},
    {"nom": "Grenoble",       "code": "38185", "dept": "38"},
    {"nom": "Rouen",          "code": "76540", "dept": "76"},
    {"nom": "Toulon",         "code": "83137", "dept": "83"},
    {"nom": "Clermont-Fd",    "code": "63113", "dept": "63"},
    {"nom": "Dijon",          "code": "21231", "dept": "21"},
    {"nom": "Angers",         "code": "49007", "dept": "49"},
    {"nom": "Reims",          "code": "51454", "dept": "51"},
    {"nom": "Le Havre",       "code": "76351", "dept": "76"},
    {"nom": "Amiens",         "code": "80021", "dept": "80"},
    {"nom": "Limoges",        "code": "87085", "dept": "87"},
    {"nom": "Brest",          "code": "29019", "dept": "29"},
]

HUBEAU_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis"


def test_commune(c: dict) -> dict:
    params = {
        "code_commune": c["code"],
        "fields": "code_commune,nom_commune,code_parametre,resultat_numerique,date_prelevement",
        "size": 1,
        "sort": "desc",
    }
    url = HUBEAU_URL + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        count = data.get("count", 0)
        sample = data.get("data", [])
        last_date = sample[0].get("date_prelevement", "?") if sample else "—"
        return {
            "nom": c["nom"],
            "dept": c["dept"],
            "code": c["code"],
            "count": count,
            "last_date": last_date,
            "ok": count > 0,
        }
    except Exception as e:
        return {"nom": c["nom"], "dept": c["dept"], "code": c["code"],
                "count": 0, "last_date": "—", "ok": False, "error": str(e)}


def main():
    print(f"Validation Hub'Eau sur {len(SAMPLE)} communes test...\n")

    results = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(test_commune, c): c for c in SAMPLE}
        for fut in as_completed(futures):
            results.append(fut.result())

    results.sort(key=lambda x: x["dept"])

    ok  = [r for r in results if r["ok"]]
    nok = [r for r in results if not r["ok"]]

    print(f"{'Commune':<20} {'Dept':<6} {'Code INSEE':<12} {'Résultats':<12} {'Dernier prélèvement'}")
    print("─" * 72)
    for r in results:
        status = "✅" if r["ok"] else "❌"
        err    = f"  [{r.get('error','')}]" if not r["ok"] else ""
        print(f"{status} {r['nom']:<18} {r['dept']:<6} {r['code']:<12} {r['count']:<12} {r['last_date']}{err}")

    print()
    print(f"Résultat : {len(ok)}/{len(results)} communes avec données Hub'Eau")
    if nok:
        print(f"Sans données : {', '.join(r['nom'] for r in nok)}")

    # Vérifier aussi que le référentiel est lisible
    if REF_FILE.exists():
        with open(REF_FILE, encoding="utf-8") as f:
            ref = json.load(f)
        print(f"\nRéférentiel : {len(ref)} communes chargées depuis {REF_FILE}")
        depts = len({c["codeDepartement"] for c in ref})
        print(f"             {depts} départements couverts")
        print(f"             Pop min={min(c['population'] for c in ref):,}  "
              f"Pop max={max(c['population'] for c in ref):,}")
    else:
        print(f"\n⚠️  Référentiel introuvable : {REF_FILE}")
        print("   Lance d'abord : python v2/referentiel/fetch_communes.py --min-pop 5000")


if __name__ == "__main__":
    main()
