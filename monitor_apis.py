#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
monitor_apis.py — Vérifie la disponibilité de toutes les APIs du projet.

Usage :
  python monitor_apis.py               # affiche le rapport, exit 1 si KO
  python monitor_apis.py --json        # sortie JSON (pour CI)
  python monitor_apis.py --github-issue # crée un GitHub Issue si KO (CI)

Variables d'environnement pour l'alerte email (optionnel) :
  ALERT_EMAIL_TO     destinataire (ex: admin@example.com)
  ALERT_SMTP_HOST    serveur SMTP (ex: smtp.gmail.com)
  ALERT_SMTP_PORT    port SMTP (ex: 587)
  ALERT_SMTP_USER    utilisateur SMTP
  ALERT_SMTP_PASS    mot de passe SMTP
  GITHUB_TOKEN       token GitHub pour créer des Issues
  GITHUB_REPOSITORY  propriétaire/dépôt (ex: utilisateur/mon-environnement)
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import json
import time
import urllib.request
import urllib.error
import os
import argparse
import smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from zoneinfo import ZoneInfo

PARIS = ZoneInfo("Europe/Paris")

# ---------------------------------------------------------------------------
# LISTE DES APIs À SURVEILLER
# ---------------------------------------------------------------------------

APIS = [
    {
        "name": "Hub'Eau — Eau potable v1",
        "url":  "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis?code_departement=34&size=1",
        "type": "hubeau",
        "critical": True,
        "note": "Si v1 arrêtée, migrer vers v2",
    },
    {
        "name": "Hub'Eau — Rivières v2",
        "url":  "https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/analyse_pc?code_departement=34&size=1",
        "type": "hubeau",
        "critical": True,
        "note": "",
    },
    {
        "name": "Hub'Eau — Nappes stations v1",
        "url":  "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations?code_departement=34&size=1",
        "type": "hubeau",
        "critical": True,
        "note": "v2 existe aussi : api/v2/niveaux_nappes/stations",
    },
    {
        "name": "Hub'Eau — Nappes chroniques temps-réel v1",
        "url":  "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques_tr?code_bss=BSS001CKGX&size=1",
        "type": "hubeau",
        "critical": True,
        "note": "",
    },
    {
        "name": "Hub'Eau — Nappes chroniques historiques v1",
        "url":  "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques?code_bss=BSS001CKGX&size=1",
        "type": "hubeau",
        "critical": True,
        "note": "",
    },
    {
        "name": "VigiEau — Restrictions sécheresse",
        "url":  "https://api.vigieau.gouv.fr/api/zones?commune=34172&profil=particulier",
        "type": "json",
        "critical": False,
        "note": "",
    },
    {
        "name": "Open-Meteo — Météo",
        "url":  "https://api.open-meteo.com/v1/meteofrance?latitude=43.6&longitude=3.9&current=temperature_2m&timezone=Europe%2FParis",
        "type": "json",
        "critical": False,
        "note": "",
    },
    {
        "name": "Atmo Occitanie — Qualité de l'air (WFS)",
        "url":  "https://dservices9.arcgis.com/7Sr9Ek9c1QTKmbwr/arcgis/services/ind_occitanie/WFSServer?service=WFS&version=2.0.0&request=GetFeature&typeName=ind_occitanie:ind_occitanie&outputFormat=GEOJSON&count=1",
        "type": "json",
        "critical": True,
        "note": "",
    },
    {
        "name": "Atmo France — Qualité de l'air national (WFS)",
        "url":  "https://data.atmo-france.org/geoserver/ind/ows?service=WFS&version=2.0.0&request=GetFeature&TypeNames=ind:ind_atmo&outputFormat=application%2Fjson&count=1",
        "type": "json",
        "critical": True,
        "note": "",
    },
    {
        "name": "Atmo Occitanie — Pollen (FeatureServer)",
        "url":  "https://services9.arcgis.com/7Sr9Ek9c1QTKmbwr/arcgis/rest/services/Indice_Pollens_sur_la_region_Occitanie/FeatureServer/0/query?where=code_zone+IN+(34)&outFields=*&resultRecordCount=1&f=json",
        "type": "json",
        "critical": False,
        "note": "",
    },
    {
        "name": "Atmo Occitanie — Pesticides (FeatureServer)",
        "url":  "https://services9.arcgis.com/7Sr9Ek9c1QTKmbwr/arcgis/rest/services/Concentrations_hebdomadaires_Pesticides_sur_la_Region_Occitanie_vue/FeatureServer/0/query?where=1%3D1&outFields=nom_site&resultRecordCount=1&f=json",
        "type": "json",
        "critical": False,
        "note": "",
    },
]

# ---------------------------------------------------------------------------
# VÉRIFICATION D'UN ENDPOINT
# ---------------------------------------------------------------------------

def check_api(api: dict, timeout: int = 15) -> dict:
    """Teste un endpoint et retourne un dict de résultat."""
    url   = api["url"]
    t0    = time.time()
    result = {
        "name":     api["name"],
        "url":      url,
        "critical": api["critical"],
        "note":     api.get("note", ""),
        "http":     None,
        "elapsed":  None,
        "ok":       False,
        "cause":    "",
    }
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MonEnvironnement-Monitor/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            result["http"] = r.status
            raw = r.read().decode(errors="replace")
        result["elapsed"] = round(time.time() - t0, 2)

        if raw.lstrip().startswith("<"):
            result["ok"]    = False
            result["cause"] = "Réponse HTML (page de maintenance) au lieu de JSON"
            return result

        data = json.loads(raw)
        # Vérifications spécifiques par type
        if api.get("type") == "hubeau":
            if isinstance(data, dict) and "count" in data:
                result["ok"] = True
            else:
                result["ok"]    = False
                result["cause"] = f"Champ 'count' absent — format inattendu"
        else:
            result["ok"] = True

    except urllib.error.HTTPError as e:
        result["http"]    = e.code
        result["elapsed"] = round(time.time() - t0, 2)
        result["cause"]   = f"HTTP {e.code}"
    except urllib.error.URLError as e:
        result["elapsed"] = round(time.time() - t0, 2)
        result["cause"]   = f"Réseau : {e.reason}"
    except json.JSONDecodeError as e:
        result["elapsed"] = round(time.time() - t0, 2)
        result["cause"]   = f"JSON invalide : {e}"
    except Exception as e:
        result["elapsed"] = round(time.time() - t0, 2)
        result["cause"]   = str(e)

    return result

# ---------------------------------------------------------------------------
# RAPPORT
# ---------------------------------------------------------------------------

def run_checks(timeout: int = 15) -> list:
    results = []
    for api in APIS:
        print(f"  Test : {api['name']}…", flush=True)
        r = check_api(api, timeout=timeout)
        status = "✅ OK" if r["ok"] else "❌ KO"
        print(f"    {status} | HTTP {r['http']} | {r['elapsed']}s"
              + (f" | {r['cause']}" if r["cause"] else ""), flush=True)
        results.append(r)
    return results


def build_report(results: list) -> str:
    now_str = datetime.now(PARIS).strftime("%d/%m/%Y à %H:%M")
    ko      = [r for r in results if not r["ok"]]
    ok      = [r for r in results if r["ok"]]
    crit_ko = [r for r in ko if r["critical"]]

    lines = [
        f"# Rapport de monitoring — {now_str}",
        f"",
        f"**{len(ok)}/{len(results)} APIs opérationnelles**"
        + (f" — ⚠ {len(crit_ko)} critique(s) KO" if crit_ko else " — ✅ Tout va bien"),
        f"",
        f"## Tableau de statut",
        f"",
        f"| API | HTTP | Temps | Statut | Cause |",
        f"|-----|------|-------|--------|-------|",
    ]
    for r in results:
        icon   = "✅" if r["ok"] else ("🔴" if r["critical"] else "🟡")
        status = "OK" if r["ok"] else "KO"
        http   = str(r["http"]) if r["http"] else "—"
        t      = f"{r['elapsed']}s" if r["elapsed"] is not None else "—"
        cause  = r["cause"] or "—"
        lines.append(f"| {icon} {r['name']} | {http} | {t} | {status} | {cause} |")

    if ko:
        lines += ["", "## APIs en échec", ""]
        for r in ko:
            crit_tag = " **(critique)**" if r["critical"] else ""
            lines.append(f"### ❌ {r['name']}{crit_tag}")
            lines.append(f"- URL : `{r['url']}`")
            lines.append(f"- HTTP : {r['http']}")
            lines.append(f"- Cause : {r['cause']}")
            if r["note"]:
                lines.append(f"- Note : {r['note']}")
            lines.append("")

    return "\n".join(lines)

# ---------------------------------------------------------------------------
# ALERTE EMAIL
# ---------------------------------------------------------------------------

def send_email_alert(report: str, ko_list: list):
    to_addr   = os.environ.get("ALERT_EMAIL_TO")
    smtp_host = os.environ.get("ALERT_SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("ALERT_SMTP_PORT", "587"))
    smtp_user = os.environ.get("ALERT_SMTP_USER")
    smtp_pass = os.environ.get("ALERT_SMTP_PASS")

    if not all([to_addr, smtp_user, smtp_pass]):
        print("  [EMAIL] Variables ALERT_EMAIL_TO / ALERT_SMTP_USER / ALERT_SMTP_PASS non configurées — alerte email ignorée.")
        return

    crit_ko = [r for r in ko_list if r["critical"]]
    subject = f"[mon-environnement] ⚠ {len(ko_list)} API(s) en panne" \
              + (" — CRITIQUE" if crit_ko else "")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = smtp_user
    msg["To"]      = to_addr

    body_text = report
    body_html = f"<pre style='font-family:monospace;font-size:13px'>{report}</pre>"

    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    msg.attach(MIMEText(body_html, "html",  "utf-8"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as s:
            s.ehlo()
            s.starttls()
            s.login(smtp_user, smtp_pass)
            s.sendmail(smtp_user, to_addr, msg.as_string())
        print(f"  [EMAIL] Alerte envoyée à {to_addr}")
    except Exception as e:
        print(f"  [EMAIL] Échec de l'envoi : {e}")

# ---------------------------------------------------------------------------
# ALERTE GITHUB ISSUE
# ---------------------------------------------------------------------------

def create_github_issue(report: str, ko_list: list):
    token = os.environ.get("GITHUB_TOKEN")
    repo  = os.environ.get("GITHUB_REPOSITORY")

    if not token or not repo:
        print("  [GITHUB] GITHUB_TOKEN ou GITHUB_REPOSITORY non définis — issue ignorée.")
        return

    crit_ko = [r for r in ko_list if r["critical"]]
    title   = f"[Monitoring] {len(ko_list)} API(s) en panne" \
              + (" — CRITIQUE" if crit_ko else "")
    body    = report + "\n\n---\n*Généré automatiquement par monitor_apis.py*"

    payload = json.dumps({
        "title":  title,
        "body":   body,
        "labels": ["monitoring", "bug"],
    }).encode()

    api_url = f"https://api.github.com/repos/{repo}/issues"
    req = urllib.request.Request(
        api_url,
        data=payload,
        headers={
            "Authorization": f"token {token}",
            "Content-Type":  "application/json",
            "User-Agent":    "MonEnvironnement-Monitor/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        print(f"  [GITHUB] Issue créée : {data.get('html_url', '?')}")
    except Exception as e:
        print(f"  [GITHUB] Échec création issue : {e}")

# ---------------------------------------------------------------------------
# POINT D'ENTRÉE
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Monitoring APIs mon-environnement.fr")
    parser.add_argument("--json",          action="store_true", help="Sortie JSON sur stdout")
    parser.add_argument("--github-issue",  action="store_true", help="Crée une GitHub Issue si des APIs sont KO")
    parser.add_argument("--email",         action="store_true", help="Envoie un email si des APIs sont KO")
    parser.add_argument("--timeout",       type=int, default=15, help="Timeout par requête (secondes)")
    parser.add_argument("--fail-on-critical", action="store_true", default=True,
                        help="Exit 1 seulement si une API critique est KO (défaut)")
    parser.add_argument("--fail-on-any",   action="store_true", help="Exit 1 si n'importe quelle API est KO")
    args = parser.parse_args()

    print(f"\n=== Monitoring APIs — {datetime.now(PARIS).strftime('%d/%m/%Y à %H:%M')} ===\n")
    results  = run_checks(timeout=args.timeout)
    ko_list  = [r for r in results if not r["ok"]]
    crit_ko  = [r for r in ko_list if r["critical"]]
    report   = build_report(results)

    print("\n" + report)

    if args.json:
        print(json.dumps({"results": results, "ko_count": len(ko_list),
                          "critical_ko_count": len(crit_ko)}, indent=2, ensure_ascii=False))

    if ko_list:
        if args.email:
            send_email_alert(report, ko_list)
        if args.github_issue:
            create_github_issue(report, ko_list)

    # Code de sortie
    if args.fail_on_any and ko_list:
        sys.exit(1)
    if crit_ko:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
