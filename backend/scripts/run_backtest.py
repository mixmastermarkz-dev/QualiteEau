"""
Backtest du modèle prédictif sécheresse — Occitanie.

Lance la collecte des données historiques, l'entraînement et l'évaluation
sur trois modèles (climatologie, arrêtés seuls, complet).
Génère BACKTEST_REPORT.md à la racine du répertoire backend/.

Usage (depuis le répertoire backend/) :
    python -m scripts.run_backtest

Le rapport généré doit être validé par le propriétaire du projet avant
toute mise en production. La sauvegarde du modèle est bloquée tant que
validated_by_owner = False dans la base.

Durée estimée : 10–30 min selon la charge des APIs Hub'Eau et Open-Meteo.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("TESTING", "1")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backtest")

SEUIL_AUC_COMPLET = 0.70
SEUIL_AUC_CLIM    = 0.65   # seuil plus bas pour la climatologie seule
RAPPORT_PATH = Path(__file__).parent.parent / "BACKTEST_REPORT.md"


# ── Helpers de formatage ───────────────────────────────────────────────────────

def _fmt_auc(auc: float | None, seuil: float = SEUIL_AUC_COMPLET) -> str:
    if auc is None:
        return "⚪ N/D"
    if auc >= seuil:
        return f"✅ {auc:.3f}"
    return f"❌ {auc:.3f}"

def _fmt_f(val: float | None, fmt: str = ".3f") -> str:
    return "—" if val is None else format(val, fmt)

def _fmt_pct(val: float | None) -> str:
    return "—" if val is None else f"{val:.1%}"


# ── Écriture du rapport ────────────────────────────────────────────────────────

def _ecrire_rapport(
    rapport: dict[str, Any],
    duree_s: float,
    sources_manquantes: list[str],
) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    prot = rapport.get("protocole", {})

    lignes: list[str] = [
        "# Backtest — Modèle prédictif sécheresse Occitanie",
        "",
        f"> Généré le {now} · durée : {duree_s:.0f}s",
        "> **Statut : À VALIDER PAR LE PROPRIÉTAIRE**",
        "> Aucune mise en production avant validation explicite du propriétaire du projet.",
        "",
    ]

    if sources_manquantes:
        lignes += [
            "⚠️ **Sources indisponibles pendant le backtest :** "
            + ", ".join(sources_manquantes),
            "_(Les métriques peuvent être sous-estimées sur les modèles utilisant ces sources.)_",
            "",
        ]

    # ── Protocole ──────────────────────────────────────────────────────────────
    lignes += [
        "## Protocole d'évaluation",
        "",
        "| Bloc | Période | Observations |",
        "|------|---------|--------------|",
        f"| **Entraînement** | {prot.get('train', '—')} | {prot.get('n_obs_train', '—'):,} |"
        if isinstance(prot.get('n_obs_train'), int) else
        f"| **Entraînement** | {prot.get('train', '—')} | — |",
        f"| **Validation** (calibration) | {prot.get('validation', '—')} | {prot.get('n_obs_valid', '—'):,} |"
        if isinstance(prot.get('n_obs_valid'), int) else
        f"| **Validation** (calibration) | {prot.get('validation', '—')} | — |",
        f"| **Test** (hors-échantillon) | {prot.get('test', '—')} | {prot.get('n_obs_test', '—'):,} |"
        if isinstance(prot.get('n_obs_test'), int) else
        f"| **Test** (hors-échantillon) | {prot.get('test', '—')} | — |",
        "",
        "Les modèles (b) et (c) sont calibrés par régression isotonique sur la validation uniquement.",
        "La calibration n'est jamais ajustée sur le test.",
        "",
    ]

    # ── Comparaison 3 modèles par horizon ──────────────────────────────────────
    lignes += [
        "## Comparaison des trois modèles par horizon",
        "",
        "Trois modèles sont comparés sur la même période de test :",
        "",
        "- **(a) Climatologie** : score saisonnier historique de la zone (aucun ML, aucune source externe)",
        "- **(b) Arrêtés seuls** : ML entraîné uniquement sur les arrêtés VigiEau (sans hydro/piézométrie/météo)",
        "- **(c) Modèle complet** : arrêtés + débits Hub'Eau + nappes + météo Open-Meteo",
        "",
    ]

    validation_ok = True
    for h in [7, 15, 30]:
        k = f"{h}j"
        r = rapport.get(k, {})
        clim  = r.get("climatologie", {})
        arr   = r.get("arretes_seuls", {})
        comp  = r.get("complet", {})

        auc_comp = comp.get("auc")
        if auc_comp is not None and auc_comp < SEUIL_AUC_COMPLET:
            validation_ok = False

        lignes += [
            f"### Horizon J+{h}",
            "",
            "| Modèle | AUC ROC | Prec. moy. | Brier | Taux base | N test |",
            "|--------|---------|------------|-------|-----------|--------|",
            f"| (a) Climatologie | {_fmt_auc(clim.get('auc'), SEUIL_AUC_CLIM)} | "
            f"{_fmt_f(clim.get('precision_moyenne'))} | "
            f"{_fmt_f(clim.get('brier'))} | "
            f"{_fmt_pct(clim.get('taux_base'))} | "
            f"{clim.get('n_obs', '—')} |",
            f"| (b) Arrêtés seuls | {_fmt_auc(arr.get('auc'))} | "
            f"{_fmt_f(arr.get('precision_moyenne'))} | "
            f"{_fmt_f(arr.get('brier'))} | "
            f"{_fmt_pct(arr.get('taux_base'))} | "
            f"{arr.get('n_obs', '—')} |",
            f"| **(c) Modèle complet** | **{_fmt_auc(auc_comp)}** | "
            f"**{_fmt_f(comp.get('precision_moyenne'))}** | "
            f"**{_fmt_f(comp.get('brier'))}** | "
            f"{_fmt_pct(comp.get('taux_base'))} | "
            f"{comp.get('n_obs', '—')} |",
            "",
        ]

        # Fausses alertes
        fa = r.get("fausses_alertes", {})
        fa_arr  = fa.get("arretes_seuls", {})
        fa_comp = fa.get("complet", {})
        if fa_arr or fa_comp:
            lignes += [
                f"**Fausses alertes au seuil p ≥ {fa_arr.get('seuil', 0.5):.2f}** :",
                "",
                "| Modèle | Nb alarmes | Taux fausse alerte | Rappel |",
                "|--------|-----------|-------------------|--------|",
                f"| (b) Arrêtés seuls | {fa_arr.get('n_alarmes', '—')} | "
                f"{_fmt_pct(fa_arr.get('taux_fausse_alerte'))} | "
                f"{_fmt_pct(fa_arr.get('rappel'))} |",
                f"| (c) Modèle complet | {fa_comp.get('n_alarmes', '—')} | "
                f"{_fmt_pct(fa_comp.get('taux_fausse_alerte'))} | "
                f"{_fmt_pct(fa_comp.get('rappel'))} |",
                "",
            ]

        # Calibration
        cal = r.get("calibration", {})
        cal_arr  = cal.get("arretes_seuls", {})
        cal_comp = cal.get("complet", {})
        ece_arr  = cal_arr.get("ece")
        ece_comp = cal_comp.get("ece")
        if ece_arr is not None or ece_comp is not None:
            lignes += [
                f"**Calibration (ECE, 10 bins quantiles)** :",
                f"- (b) Arrêtés seuls : ECE = {_fmt_f(ece_arr)}",
                f"- (c) Modèle complet : ECE = {_fmt_f(ece_comp)}",
                "",
            ]

    # ── Analyse par événement (J+30) ──────────────────────────────────────────
    r30 = rapport.get("30j", {})
    pe = r30.get("par_evenement", {})
    if pe:
        pe_arr  = pe.get("arretes_seuls", {})
        pe_comp = pe.get("complet", {})

        lignes += [
            "## Analyse par événement — Horizon J+30",
            "",
            "Pour chaque passage en alerte (niveau ≥ 2) dans la période de test,",
            "on mesure si le modèle l'a annoncé et combien de jours à l'avance.",
            "",
            "Les événements sont distingués selon le contexte départemental :",
            "- **Première zone du dépt** : aucune autre zone du département n'est déjà en alerte",
            "- **Département déjà en alerte** : au moins une zone du département est déjà en alerte",
            "",
            "| Catégorie | Modèle | Événements | Annoncés | Taux d'annonce | Délai médian |",
            "|-----------|--------|-----------|----------|----------------|-------------|",
        ]

        def _ligne_evenement(label: str, modele: str, stats: dict[str, Any]) -> str:
            n  = stats.get("n_evenements", "—")
            an = stats.get("n_annonces", "—")
            ta = _fmt_pct(stats.get("taux_annonce"))
            dm = stats.get("delai_median_j")
            return (
                f"| {label} | {modele} | {n} | {an} | {ta} | "
                f"{'—' if dm is None else f'{dm:.1f} j'} |"
            )

        for label, stats_arr, stats_comp in [
            ("Toutes zones",           pe_arr.get("toutes_zones", {}),           pe_comp.get("toutes_zones", {})),
            ("Première zone du dépt",  pe_arr.get("premiere_zone_dept", {}),     pe_comp.get("premiere_zone_dept", {})),
            ("Dépt déjà en alerte",    pe_arr.get("dept_deja_en_alerte", {}),    pe_comp.get("dept_deja_en_alerte", {})),
        ]:
            lignes.append(_ligne_evenement(label, "(b) Arrêtés seuls", stats_arr))
            lignes.append(_ligne_evenement(label, "(c) Modèle complet", stats_comp))

        lignes += [""]

    # ── Résultats par département ──────────────────────────────────────────────
    par_dept = rapport.get("par_dept", {})
    if par_dept:
        lignes += [
            "## Résultats par département (modèle complet, hors-échantillon)",
            "",
            "| Dept | AUC J+7 | AUC J+15 | AUC J+30 | Taux base 30j |",
            "|------|---------|----------|----------|---------------|",
        ]
        NOMS_DEPTS = {
            "09": "Ariège", "11": "Aude", "12": "Aveyron", "30": "Gard",
            "31": "Haute-Garonne", "32": "Gers", "34": "Hérault", "46": "Lot",
            "48": "Lozère", "65": "Hautes-Pyrénées", "66": "Pyrénées-Orientales",
            "81": "Tarn", "82": "Tarn-et-Garonne",
        }
        for dept, dr in sorted(par_dept.items()):
            nom = NOMS_DEPTS.get(dept, dept)
            auc7  = dr.get("7j",  {}).get("auc")
            auc15 = dr.get("15j", {}).get("auc")
            auc30 = dr.get("30j", {}).get("auc")
            tb30  = dr.get("30j", {}).get("taux_base")
            lignes.append(
                f"| **{dept}** — {nom} | "
                f"{_fmt_auc(auc7)} | "
                f"{_fmt_auc(auc15)} | "
                f"{_fmt_auc(auc30)} | "
                f"{_fmt_pct(tb30)} |"
            )
        lignes += [""]

    # ── Conclusion factuelle ───────────────────────────────────────────────────
    lignes += [
        "## Conclusion factuelle",
        "",
        "### Le modèle complet fait-il nettement mieux que « arrêtés seuls » ?",
        "",
    ]

    # Comparer sur les premières zones du département (cas le plus difficile)
    pe_arr_prem  = pe.get("arretes_seuls",  {}).get("premiere_zone_dept", {}) if pe else {}
    pe_comp_prem = pe.get("complet",        {}).get("premiere_zone_dept", {}) if pe else {}
    r30_arr  = r30.get("arretes_seuls", {})
    r30_comp = r30.get("complet", {})

    auc_arr_30  = r30_arr.get("auc")
    auc_comp_30 = r30_comp.get("auc")

    if auc_arr_30 is not None and auc_comp_30 is not None:
        delta_auc = auc_comp_30 - auc_arr_30
        if abs(delta_auc) < 0.02:
            verdict_sources = (
                "Le modèle complet n'améliore pas significativement le modèle arrêtés seuls "
                f"(ΔAUC J+30 = {delta_auc:+.3f}). "
                "Les sources externes (hydro, piézométrie, météo) n'apportent pas de gain mesurable "
                "sur cette période de test. Cela peut indiquer : données hydro/piézométrie insuffisantes, "
                "rupture de série Propluvia→VigiEau, ou modèle déjà saturé par les arrêtés."
            )
        elif delta_auc >= 0.02:
            verdict_sources = (
                f"Le modèle complet améliore significativement le modèle arrêtés seuls "
                f"(ΔAUC J+30 = {delta_auc:+.3f}). "
                "Les sources externes (hydro, piézométrie, météo) apportent un gain mesurable."
            )
        else:
            verdict_sources = (
                f"Le modèle complet est moins performant que le modèle arrêtés seuls "
                f"(ΔAUC J+30 = {delta_auc:+.3f}). "
                "Vérifier la cohérence des sources externes et l'absence de biais d'imputation."
            )
        lignes += [verdict_sources, ""]

    ta_arr  = pe_arr_prem.get("taux_annonce")
    ta_comp = pe_comp_prem.get("taux_annonce")
    if ta_arr is not None and ta_comp is not None:
        lignes += [
            f"Sur les **premières zones du département** (cas le plus difficile) :",
            f"- (b) Arrêtés seuls : taux d'annonce = {ta_arr:.1%}, "
            f"délai médian = {_fmt_f(pe_arr_prem.get('delai_median_j'), '.1f')} j",
            f"- (c) Modèle complet : taux d'annonce = {ta_comp:.1%}, "
            f"délai médian = {_fmt_f(pe_comp_prem.get('delai_median_j'), '.1f')} j",
            "",
        ]

    # ── Verdict de validation ──────────────────────────────────────────────────
    lignes += [
        "## Verdict",
        "",
        f"**Seuil de validation : AUC ≥ {SEUIL_AUC_COMPLET} sur chaque horizon (modèle complet).**",
        "",
        ("✅ **Critère AUC atteint** — le rapport peut être soumis au propriétaire pour validation."
         if validation_ok else
         f"❌ **Critère AUC non atteint** (< {SEUIL_AUC_COMPLET}) — "
         "vérifier la qualité des données avant de soumettre."),
        "",
        "**Ce rapport est à l'état brut. Il doit être relu et validé par le propriétaire**",
        "**du projet avant toute mise en production. Ne pas modifier les chiffres.**",
        "",
        "---",
        "",
    ]

    # ── Validation et avertissements ──────────────────────────────────────────
    lignes += [
        "## Validation requise avant production",
        "",
        "- [ ] AUC ≥ 0.70 sur les 3 horizons — modèle complet",
        "- [ ] Modèle complet nettement meilleur que arrêtés seuls sur les premières zones du département",
        "- [ ] Calibration acceptable (ECE < 0.05)",
        "- [ ] Résultats vérifiés et validés par le propriétaire du projet",
        "- [ ] Mention légale affichée sur toutes les interfaces utilisateur",
        "",
        "---",
        "",
    ]

    # ── Section historique ────────────────────────────────────────────────────
    lignes += [
        "## Historique : résultat invalidé du 2026-11-22",
        "",
        "> ❌ **INVALIDÉ** — les chiffres ci-dessous proviennent d'une version avec fuite de données.",
        "> Ils ne doivent pas être utilisés. Conservés ici à titre d'archive de l'audit.",
        "",
        "**Cause racine** : le parser VigiEau lisait chaque liste JSON de zones comme un seul",
        "nom de zone (ex. `['76_34_0004', '76_34_0017', ...]` = 1 zone au lieu de 19).",
        "Résultat : 1 486 « zones » fictives, chacune = 1 arrêté entier.",
        "La présence même d'une zone dans le jeu signalait qu'un arrêté existait cette année-là.",
        "Une règle triviale « zone récemment apparue × haute saison » atteignait AUC 0.941.",
        "",
        "| Horizon | AUC (invalidé) | Note |",
        "|---------|---------------|------|",
        "| J+7  | ~~0.972~~ | fuite de données |",
        "| J+15 | ~~0.973~~ | fuite de données |",
        "| J+30 | ~~0.975~~ | fuite de données |",
        "",
        "---",
        "",
    ]

    # ── Avertissements sources ────────────────────────────────────────────────
    lignes += [
        "## Avertissements",
        "",
        "### Licence Open-Meteo",
        "L'API archive gratuite d'Open-Meteo (météo historique) est **réservée à un usage non commercial**.",
        "En production commerciale : obtenir une clé payante Open-Meteo ou implémenter `MeteoFranceSource`.",
        "",
        "### Rupture de référentiel de zones (Propluvia → VigiEau)",
        "Les zones 2012–2022 (Propluvia) et 2023– (VigiEau) utilisent des codes différents.",
        "Le modèle ne peut pas exploiter l'historique spécifique d'une zone Propluvia pour prédire",
        "une zone VigiEau. Il s'appuie sur les caractéristiques transférables : saisonnalité,",
        "niveaux des zones voisines, variables hydro/météo régionales.",
        "",
        "### Données Hub'Eau",
        "Hub'Eau a connu une maintenance en mai–novembre 2026. Les données hydrologiques",
        "peuvent être incomplètes pour cette période.",
        "",
    ]

    # ── JSON brut ─────────────────────────────────────────────────────────────
    lignes += [
        "## Données brutes (JSON)",
        "",
        "```json",
        json.dumps(rapport, indent=2, ensure_ascii=False, default=float),
        "```",
    ]

    RAPPORT_PATH.write_text("\n".join(lignes), encoding="utf-8")
    logger.info("Rapport écrit : %s", RAPPORT_PATH)


# ── Entrée principale ──────────────────────────────────────────────────────────

async def main() -> int:
    logger.info("=" * 60)
    logger.info("BACKTEST SÉCHERESSE OCCITANIE — Phase 4")
    logger.info("3 modèles · train≤2021 · valid=2022 · test≥2023")
    logger.info("Collecte des données depuis 2012 — patience requise")
    logger.info("=" * 60)

    t0 = time.time()

    from predictif.features import construire_jeu
    from predictif.model import entrainer_et_evaluer, sauvegarder_modeles
    from predictif.config import (
        OCCITANIE_DEPTS, POINTS_METEO_OCCITANIE,
        DATE_DEBUT_HISTORIQUE, DATA_DIR, MODELS_DIR,
    )
    from predictif.sources.weather.open_meteo import OpenMeteoSource
    from predictif.sources.hubeau import HubeauHydroSource, HubeauPiezoSource
    from predictif.sources.vigeau import VigeauSource

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    cache_dir = DATA_DIR / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    date_fin = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info("Collecte : VigiEau (arrêtés sécheresse)…")
    vigeau = VigeauSource(cache_dir)
    r_vigeau = await vigeau.fetch(OCCITANIE_DEPTS, DATE_DEBUT_HISTORIQUE, date_fin)
    if not r_vigeau.available:
        logger.error("VigiEau indisponible : %s", r_vigeau.error)
        logger.error("Le backtest nécessite les données VigiEau. Abandon.")
        return 1

    logger.info("  → %d arrêtés chargés", len(r_vigeau.data))

    logger.info("Collecte : Hub'Eau hydrométrie (débits)…")
    hydro = HubeauHydroSource()
    r_hydro = await hydro.fetch(OCCITANIE_DEPTS, DATE_DEBUT_HISTORIQUE, date_fin)
    logger.info(
        "  → %s",
        f"{len(r_hydro.data):,} observations" if r_hydro.available else f"INDISPONIBLE ({r_hydro.error})",
    )

    logger.info("Collecte : Hub'Eau piézométrie (nappes)…")
    piezo = HubeauPiezoSource()
    r_piezo = await piezo.fetch(OCCITANIE_DEPTS, DATE_DEBUT_HISTORIQUE, date_fin)
    logger.info(
        "  → %s",
        f"{len(r_piezo.data):,} observations" if r_piezo.available else f"INDISPONIBLE ({r_piezo.error})",
    )

    logger.info("Collecte : Open-Meteo (météo historique)…")
    meteo = OpenMeteoSource()
    r_meteo = await meteo.fetch(POINTS_METEO_OCCITANIE, DATE_DEBUT_HISTORIQUE, date_fin)
    logger.info(
        "  → %s",
        f"{len(r_meteo.data):,} lignes" if r_meteo.available else f"INDISPONIBLE ({r_meteo.error})",
    )

    manquantes = [
        nom for nom, r in [
            ("hubeau_hydro", r_hydro),
            ("hubeau_piezo", r_piezo),
            ("open_meteo",   r_meteo),
        ] if not r.available
    ]
    if manquantes:
        logger.warning(
            "Sources absentes : %s — résultats des modèles utilisant ces sources potentiellement dégradés",
            manquantes,
        )

    logger.info("Construction du jeu de données…")
    jeu = construire_jeu(
        r_vigeau.data,
        r_hydro.data  if r_hydro.available  else None,
        r_piezo.data  if r_piezo.available  else None,
        r_meteo.data  if r_meteo.available  else None,
        OCCITANIE_DEPTS,
        fiabilite_reduite=bool(manquantes),
        source_manquante=",".join(manquantes) if manquantes else None,
    )

    if jeu.empty:
        logger.error("Jeu de données vide après feature engineering. Abandon.")
        return 1

    logger.info(
        "  → jeu : %d lignes × %d colonnes, années %d–%d",
        *jeu.shape, jeu["date"].dt.year.min(), jeu["date"].dt.year.max(),
    )

    # Sauvegarde du jeu pour analyse ultérieure / reproductibilité
    out_parquet = DATA_DIR / "jeu_donnees_backtest.parquet"
    try:
        jeu.to_parquet(out_parquet)
        logger.info("  → jeu sauvegardé : %s", out_parquet)
    except Exception as exc:
        logger.warning("Impossible de sauvegarder le parquet : %s", exc)

    logger.info(
        "Entraînement et évaluation (train≤%d, valid=%d, test≥%d)…",
        2021, 2022, 2023,
    )
    rapport, modeles, zones = entrainer_et_evaluer(jeu)

    if not rapport:
        logger.error("Aucun résultat de backtest produit (données insuffisantes ?).")
        return 1

    # Sauvegarde du modèle complet (non validé → validated_by_owner = False en DB)
    version = datetime.now(timezone.utc).strftime("%Y-%m-%d_v1")
    artifact_path, sha256 = sauvegarder_modeles(modeles, zones, MODELS_DIR, version)
    logger.info("SHA-256 : %s", sha256[:16] + "…")

    duree = time.time() - t0
    _ecrire_rapport(rapport, duree, manquantes)

    # ── Résumé console ────────────────────────────────────────────────────────
    print()
    print("═" * 70)
    print("  RÉSULTATS BACKTEST — MODÈLE PRÉDICTIF SÉCHERESSE OCCITANIE")
    print("═" * 70)
    print(f"  {'Horizon':<8}  {'Clim (a)':<12}  {'Arrêtés (b)':<14}  {'Complet (c)':<14}")
    print(f"  {'':─<8}  {'':─<12}  {'':─<14}  {'':─<14}")
    for h in [7, 15, 30]:
        r = rapport.get(f"{h}j", {})
        auc_c = r.get("climatologie",  {}).get("auc")
        auc_a = r.get("arretes_seuls", {}).get("auc")
        auc_x = r.get("complet",       {}).get("auc")
        ok_c = "✅" if (auc_c is not None and auc_c >= SEUIL_AUC_CLIM)   else ("❌" if auc_c is not None else "⚪")
        ok_a = "✅" if (auc_a is not None and auc_a >= SEUIL_AUC_COMPLET) else ("❌" if auc_a is not None else "⚪")
        ok_x = "✅" if (auc_x is not None and auc_x >= SEUIL_AUC_COMPLET) else ("❌" if auc_x is not None else "⚪")
        fc = f"{auc_c:.3f}" if auc_c is not None else " N/D "
        fa = f"{auc_a:.3f}" if auc_a is not None else " N/D "
        fx = f"{auc_x:.3f}" if auc_x is not None else " N/D "
        print(f"  J+{h:<6}  {ok_c} {fc:<9}  {ok_a} {fa:<11}  {ok_x} {fx}")

    print()
    print(f"  Modèle sauvegardé  : {artifact_path.name}")
    print(f"  Rapport            : {RAPPORT_PATH.name}")
    print(f"  Durée              : {duree:.0f}s")
    print()
    print("  ⚠️  VALIDATION OBLIGATOIRE PAR LE PROPRIÉTAIRE AVANT PRODUCTION")
    print("═" * 70)
    print()

    # Code de sortie : 0 si AUC complet atteint sur tous les horizons, 2 sinon
    for h in [7, 15, 30]:
        r = rapport.get(f"{h}j", {})
        auc = r.get("complet", {}).get("auc")
        if auc is not None and auc < SEUIL_AUC_COMPLET:
            logger.warning(
                "AUC modèle complet J+%dj = %.3f < %.2f — seuil non atteint",
                h, auc, SEUIL_AUC_COMPLET,
            )
            return 2

    logger.info("Critère AUC atteint sur tous les horizons ✅")
    logger.info("Soumettre BACKTEST_REPORT.md au propriétaire pour validation.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
