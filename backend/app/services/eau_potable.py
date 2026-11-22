"""
Service eau potable — Hub'Eau SISE-Eaux.

Paramètres réels disponibles via /api/v1/qualite_eau_potable/resultats_dis :

    1340  Nitrates (mg/L)         limite : 50 mg/L
    1302  pH                      référence : 6.5–9.0
    1295  Turbidité (NFU)         limite : 1 NFU
    1399  Chlore total (mg/L)     référence : 5 mg/L
    1449  E. coli (n/100mL)       limite : 0
    1447  Bact. coliforme (n/100mL) limite : 0
    1370  Aluminium (µg/L)        limite : 200 µg/L
    1337  Chlorures (mg/L)        référence : 250 mg/L
    1303  Conductivité (µS/cm)    référence : 2500 µS/cm

PFAS : pas de code SANDRE disponible dans Hub'Eau (état nov. 2026).
Le suivi est extrait du champ conclusion_conformite_prelevement.
Lien officiel : https://resultats.eaux.sante.gouv.fr/
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_BASE = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable"
_TIMEOUT = httpx.Timeout(15.0)
_UA = {"User-Agent": "mon-environnement.fr/1.0 (contact@mon-environnement.fr)"}

# Paramètres à récupérer (codes SANDRE validés)
_PARAMS_EAU = {
    "1340": {"name": "Nitrates",      "unite": "mg/L",   "limite": 50.0,  "ref": None},
    "1302": {"name": "pH",            "unite": "pH",     "limite": None,  "ref": (6.5, 9.0)},
    "1295": {"name": "Turbidité",     "unite": "NFU",    "limite": 1.0,   "ref": None},
    "1399": {"name": "Chlore total",  "unite": "mg/L",   "limite": 5.0,   "ref": None},
    "1449": {"name": "E. coli",       "unite": "n/100mL","limite": 0.0,   "ref": None},
    "1370": {"name": "Aluminium",     "unite": "µg/L",   "limite": 200.0, "ref": None},
    "1337": {"name": "Chlorures",     "unite": "mg/L",   "limite": None,  "ref": 250.0},
    "1303": {"name": "Conductivité",  "unite": "µS/cm",  "limite": None,  "ref": 2500.0},
}

# Expressions pour détecter les PFAS dans les conclusions
_RE_PFAS_DEPASSEMENT = re.compile(
    r"(PFAS|pfas|perfluoro).{0,120}(supérieure?|dépasse|non.{0,10}conforme|restrict)",
    re.IGNORECASE | re.DOTALL,
)
_RE_PFAS_CONFORME = re.compile(
    r"(PFAS|pfas|perfluoro).{0,80}(conforme|inférieure?)",
    re.IGNORECASE | re.DOTALL,
)
_RE_PFAS_VAL = re.compile(
    r"(\d+[,\.]\d+)\s*µg/[lL]",
    re.IGNORECASE,
)


def _parse_pfas_conclusion(conclusion: str) -> dict:
    """
    Extrait l'information PFAS d'un texte de conclusion de conformité.
    Retourne un dict avec statut, valeur si mentionnée, et texte brut.
    """
    if not conclusion:
        return {"statut": "non_mesure", "texte": None, "valeur": None}

    text_upper = conclusion.upper()

    if "PFAS" not in text_upper and "PERFLUORO" not in text_upper:
        return {"statut": "non_mentionne", "texte": None, "valeur": None}

    # Cherche la valeur numérique si mentionnée
    valeur = None
    m_val = _RE_PFAS_VAL.search(conclusion)
    if m_val:
        try:
            valeur = float(m_val.group(1).replace(",", "."))
        except ValueError:
            pass

    if _RE_PFAS_DEPASSEMENT.search(conclusion):
        return {
            "statut": "non_conforme",
            "texte": conclusion[:300],
            "valeur": valeur,
        }
    if _RE_PFAS_CONFORME.search(conclusion):
        return {
            "statut": "conforme",
            "texte": conclusion[:300],
            "valeur": valeur,
        }

    return {
        "statut": "mentionne",
        "texte": conclusion[:300],
        "valeur": valeur,
    }


async def get_eau_commune(code_commune: str) -> dict:
    """
    Récupère les derniers résultats d'analyses d'eau potable pour une commune.

    Retourne :
        commune_nom, date_derniere_analyse,
        conformite_bacterio, conformite_chimique,
        parametres: {code: {name, valeur, unite, conforme}},
        pfas: {statut, texte, valeur},
        source_url
    """
    # On demande les 200 derniers résultats triés par date desc
    # pour couvrir tous les paramètres du dernier prélèvement
    since = (datetime.now(timezone.utc) - timedelta(days=730)).strftime("%Y-%m-%d")

    params = {
        "code_commune": code_commune,
        "date_min_prelevement": since,
        "size": 200,
        "sort": "desc",
        "fields": (
            "code_parametre,libelle_parametre,resultat_numerique,"
            "resultat_alphanumerique,libelle_unite,date_prelevement,"
            "conclusion_conformite_prelevement,"
            "conformite_limites_bact_prelevement,"
            "conformite_limites_pc_prelevement,"
            "nom_commune,nom_distributeur"
        ),
    }

    try:
        async with httpx.AsyncClient(headers=_UA, timeout=_TIMEOUT) as client:
            r = await client.get(f"{_BASE}/resultats_dis", params=params)
            r.raise_for_status()
            data = r.json().get("data", [])
    except Exception as exc:
        logger.warning("Hub'Eau eau potable %s : %s", code_commune, exc)
        return _empty_result(code_commune)

    if not data:
        return _empty_result(code_commune)

    # Tri par date desc — on prend le prélèvement le plus récent
    latest = sorted(data, key=lambda x: x.get("date_prelevement", ""), reverse=True)
    ref = latest[0]

    commune_nom  = ref.get("nom_commune", "").title()
    distributeur = ref.get("nom_distributeur", "")
    date_str     = (ref.get("date_prelevement") or "")[:10]  # YYYY-MM-DD
    conf_bacterio = ref.get("conformite_limites_bact_prelevement", "")
    conf_chimique  = ref.get("conformite_limites_pc_prelevement", "")
    conclusion    = ref.get("conclusion_conformite_prelevement", "")

    # Collecte des résultats par paramètre (dernier en date)
    seen: dict[str, dict] = {}
    pfas_data = {"statut": "non_mentionne", "texte": None, "valeur": None}

    for row in latest:
        code  = row.get("code_parametre", "")
        if code in seen:
            continue

        if code in _PARAMS_EAU:
            meta = _PARAMS_EAU[code]
            valeur_num = row.get("resultat_numerique")
            valeur_txt = row.get("resultat_alphanumerique", "")

            # Conformité numérique simple
            conforme = None
            if valeur_num is not None:
                limite = meta.get("limite")
                ref_range = meta.get("ref")
                if limite is not None:
                    if code == "1449":  # E. coli : tolérance zéro
                        conforme = valeur_num == 0
                    else:
                        conforme = valeur_num <= limite
                elif ref_range is not None:
                    if isinstance(ref_range, tuple):
                        conforme = ref_range[0] <= valeur_num <= ref_range[1]

            seen[code] = {
                "name":     meta["name"],
                "valeur":   valeur_txt or (str(valeur_num) if valeur_num is not None else "—"),
                "valeur_num": valeur_num,
                "unite":    meta["unite"],
                "conforme": conforme,
            }

        # Extraction PFAS depuis la conclusion
        if pfas_data["statut"] in ("non_mentionne",) and conclusion:
            pfas_candidate = _parse_pfas_conclusion(conclusion)
            if pfas_candidate["statut"] not in ("non_mesure", "non_mentionne"):
                pfas_data = pfas_candidate

    # Dernière tentative PFAS sur toutes les conclusions
    if pfas_data["statut"] == "non_mentionne":
        for row in latest[:50]:
            c = row.get("conclusion_conformite_prelevement", "")
            pfas_candidate = _parse_pfas_conclusion(c)
            if pfas_candidate["statut"] not in ("non_mesure", "non_mentionne"):
                pfas_data = pfas_candidate
                break

    return {
        "disponible": True,
        "commune_nom": commune_nom,
        "distributeur": distributeur,
        "date_derniere_analyse": date_str,
        "conformite_bacterio": "Conforme" if conf_bacterio == "C" else ("Non conforme" if conf_bacterio == "D" else "—"),
        "conformite_chimique":  "Conforme" if conf_chimique  == "C" else ("Non conforme" if conf_chimique  == "D" else "—"),
        "conforme_global": conf_bacterio == "C" and conf_chimique == "C",
        "parametres": seen,
        "pfas": pfas_data,
        "source_url": f"https://resultats.eaux.sante.gouv.fr/#qualite_eau_commune/{code_commune}",
    }


def _empty_result(code_commune: str) -> dict:
    return {
        "disponible": False,
        "commune_nom": "",
        "distributeur": "",
        "date_derniere_analyse": None,
        "conformite_bacterio": "—",
        "conformite_chimique": "—",
        "conforme_global": None,
        "parametres": {},
        "pfas": {"statut": "non_mesure", "texte": None, "valeur": None},
        "source_url": f"https://resultats.eaux.sante.gouv.fr/#qualite_eau_commune/{code_commune}",
    }
