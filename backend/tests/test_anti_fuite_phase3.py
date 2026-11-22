"""
Tests anti-fuite automatisés — Phase 3 (audit 2026-11-22).

Obligatoires dans la CI. Toute régression sur ces tests indique une fuite
de données potentielle dans le jeu d'entraînement.

Tests :
  T1 — Aucune ligne pour une zone avant son premier arrêté
  T2 — Test de perturbation : modifier des données après t ne change pas les
       features à t (aucune contamination future)
  T3 — La règle naïve « zone vient d'apparaître × saison » doit avoir une AUC
       < 0,75 sur le jeu de test. Si ≥ 0,75, une fuite de présence subsiste.
"""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest
from sklearn.metrics import roc_auc_score

from predictif.config import ANNEE_FIN_ENTRAINEMENT, NIVEAU_CIBLE
from predictif.features import construire_jeu


# ── Fixture : jeu minimal reproductible ──────────────────────────────────────

def _arretes_minimaux() -> pd.DataFrame:
    """
    Crée un DataFrame d'arrêtés minimal couvrant 3 zones sur 10 années.

    Zone A (11_TEST_A) : première restriction en 2016, annuelle en été
    Zone B (11_TEST_B) : première restriction en 2018, moins fréquente
    Zone C (11_TEST_C) : première restriction en 2021, récente
    """
    rows = []
    for annee in range(2016, 2024):
        rows.append({
            "arrete": f"AP11-{annee}-A",
            "zone": "11_TEST_A", "dep": "11",
            "date_signature": pd.Timestamp(f"{annee}-06-28"),
            "debut": pd.Timestamp(f"{annee}-07-01"),
            "fin": pd.Timestamp(f"{annee}-09-30"),
            "nom": "Zone A", "type": "SUP", "niveau": 2.0,
        })
    for annee in range(2018, 2023, 2):
        rows.append({
            "arrete": f"AP11-{annee}-B",
            "zone": "11_TEST_B", "dep": "11",
            "date_signature": pd.Timestamp(f"{annee}-07-10"),
            "debut": pd.Timestamp(f"{annee}-07-15"),
            "fin": pd.Timestamp(f"{annee}-08-31"),
            "nom": "Zone B", "type": "SUP", "niveau": 3.0,
        })
    for annee in range(2021, 2024):
        rows.append({
            "arrete": f"AP11-{annee}-C",
            "zone": "11_TEST_C", "dep": "11",
            "date_signature": pd.Timestamp(f"{annee}-08-01"),
            "debut": pd.Timestamp(f"{annee}-08-05"),
            "fin": pd.Timestamp(f"{annee}-10-15"),
            "nom": "Zone C", "type": "SOU", "niveau": 2.0,
        })
    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def jeu_et_arretes():
    """Construit le jeu complet une seule fois (lourd en CPU)."""
    df_arretes = _arretes_minimaux()
    jeu = construire_jeu(
        df_arretes=df_arretes,
        df_hydro=None,
        df_piezo=None,
        df_meteo=None,
        depts=["11"],
    )
    return {"arretes": df_arretes, "jeu": jeu}


# ── T1 : Pas de ligne avant le premier arrêté ─────────────────────────────────

def test_t1_aucune_ligne_avant_premier_arrete(jeu_et_arretes: dict) -> None:
    """
    Pour chaque zone, la première date du jeu doit être ≥ à la date de début
    du premier arrêté de cette zone.
    Vérifie que la fuite de présence (remplissage depuis le 1er janvier) est supprimée.
    """
    df_arretes = jeu_et_arretes["arretes"]
    jeu = jeu_et_arretes["jeu"]

    premiers = df_arretes.groupby("zone")["debut"].min()

    for zone, grp in jeu.groupby("zone"):
        if zone not in premiers.index:
            continue
        premier_arrete = primers = premiers[zone]
        date_min_jeu = grp["date"].min()
        assert date_min_jeu >= pd.Timestamp(premier_arrete), (
            f"Zone {zone} : date minimale dans le jeu {date_min_jeu.date()} "
            f"< premier arrêté {premier_arrete.date()}. "
            "La fuite de présence n'est pas supprimée."
        )


def test_t1_zone_c_absente_avant_2021(jeu_et_arretes: dict) -> None:
    """Zone C apparaît en 2021 : aucune ligne avant le 2021-08-05."""
    jeu = jeu_et_arretes["jeu"]
    zone_c = jeu[jeu["zone"] == "11_TEST_C"]
    assert not zone_c.empty, "Zone C devrait être dans le jeu"
    min_date = zone_c["date"].min()
    assert min_date >= pd.Timestamp("2021-08-05"), (
        f"Zone C présente dans le jeu dès {min_date.date()} (attendu ≥ 2021-08-05)"
    )


# ── T2 : Test de perturbation (pas de contamination future) ──────────────────

def test_t2_modification_futur_ne_change_pas_features_passe() -> None:
    """
    Pour une date t, les features calculées par construire_jeu ne doivent pas
    changer si on modifie les données strictement après t.

    Méthode : construire deux jeux identiques sauf que le second a un arrêté
    supplémentaire loin dans le futur. Les features à une date passée t doivent
    être identiques dans les deux jeux.
    """
    df_base = _arretes_minimaux()

    # Jeu de référence (sans arrêté futur)
    jeu_base = construire_jeu(df_base, None, None, None, ["11"])

    # Jeu perturbé : ajout d'un arrêté 2 ans après la dernière date du jeu de base
    df_perturbe = pd.concat([df_base, pd.DataFrame([{
        "arrete": "AP11-FUTUR",
        "zone": "11_TEST_A", "dep": "11",
        "date_signature": pd.Timestamp("2030-01-01"),
        "debut": pd.Timestamp("2030-01-02"),
        "fin": pd.Timestamp("2030-04-30"),
        "nom": "Zone A", "type": "SUP", "niveau": 4.0,
    }])], ignore_index=True)
    jeu_perturbe = construire_jeu(df_perturbe, None, None, None, ["11"])

    # Date de test : 2022-07-15 (bien avant l'arrêté futur)
    t = pd.Timestamp("2022-07-15")
    cols_features = ["niveau_actuel", "jours_meme_niveau", "niveau_max_365j",
                     "clim_zone", "dep_nb_alerte", "dep_une_alerte"]

    base_t = jeu_base[
        (jeu_base["zone"] == "11_TEST_A") & (jeu_base["date"] == t)
    ][cols_features]
    perturbe_t = jeu_perturbe[
        (jeu_perturbe["zone"] == "11_TEST_A") & (jeu_perturbe["date"] == t)
    ][cols_features]

    if base_t.empty or perturbe_t.empty:
        pytest.skip("Date de test absente du jeu — élargir les fixtures")

    pd.testing.assert_frame_equal(
        base_t.reset_index(drop=True),
        perturbe_t.reset_index(drop=True),
        check_names=False,
        obj="features à t=2022-07-15 après perturbation future",
    )


# ── T3 : AUC de la règle naïve "zone vient d'apparaître × saison" < 0,75 ──────

def test_t3_auc_regle_naive_inferieure_075(jeu_et_arretes: dict) -> None:
    """
    La règle naïve « zone récemment apparue × haute saison » ne doit pas
    dépasser AUC 0,75 sur le jeu de test (années > ANNEE_FIN_ENTRAINEMENT).

    Si elle le dépasse, la fuite de présence (zone=arrêté) subsiste dans le jeu.

    Score naïf :
      - 1 si la zone est dans sa 1re année d'existence ET on est en été (juin–sept)
      - 0 sinon
    """
    jeu = jeu_et_arretes["jeu"].copy()

    # Première date connue de chaque zone dans le jeu (= premier arrêté après Phase 3)
    zone_premiere = jeu.groupby("zone")["date"].min()
    jeu["jours_depuis_premiere"] = (
        jeu["date"] - jeu["zone"].map(zone_premiere)
    ).dt.days

    # Score naïf : zone récente (< 365 j) × été (juin=6, sept=9)
    saison_ete = jeu["date"].dt.month.between(6, 9).astype(float)
    zone_recente = (jeu["jours_depuis_premiere"] < 365).astype(float)
    jeu["score_naif"] = zone_recente * saison_ete

    # Jeu de test uniquement
    te = jeu[jeu["date"].dt.year > ANNEE_FIN_ENTRAINEMENT].copy()
    cible = "y_30j"
    ok = te[cible].notna() & te["score_naif"].notna()
    te = te[ok]

    if te[cible].nunique() < 2 or len(te) < 100:
        pytest.skip("Pas assez d'événements pour calculer l'AUC — élargir les fixtures")

    auc = roc_auc_score(te[cible], te["score_naif"])
    assert auc < 0.75, (
        f"La règle naïve « zone récente × saison » obtient AUC = {auc:.3f} ≥ 0,75. "
        "Une fuite de présence subsiste dans le jeu de données. "
        "Vérifier que les zones n'existent pas dans le jeu avant leur premier arrêté."
    )


# ── T4 : Colonne dep présente dans le jeu ────────────────────────────────────

def test_t4_colonne_dep_presente(jeu_et_arretes: dict) -> None:
    """La colonne dep doit être présente dans le jeu (nécessaire pour le backtest par dpt)."""
    jeu = jeu_et_arretes["jeu"]
    assert "dep" in jeu.columns, "La colonne 'dep' est absente du jeu"
    assert jeu["dep"].notna().any(), "La colonne 'dep' est entièrement NaN"


# ── T5 : fiabilite_reduite absent des features du modèle ─────────────────────

def test_t5_fiabilite_reduite_non_feature() -> None:
    """fiabilite_reduite doit être dans le jeu comme métadonnée mais pas dans les colonnes X."""
    from predictif.model import _colonnes_x, _NON_FEATURES
    assert "fiabilite_reduite" in _NON_FEATURES, (
        "fiabilite_reduite doit être dans _NON_FEATURES de model.py"
    )

    # Simuler un jeu avec cette colonne
    jeu_factice = pd.DataFrame({
        "niveau_actuel": [1.0],
        "jours_meme_niveau": [5],
        "fiabilite_reduite": [1],
        "dep": ["34"],
        "zone": ["76_34_0001"],
        "y_30j": [0.0],
    })
    cols = _colonnes_x(jeu_factice)
    assert "fiabilite_reduite" not in cols, (
        "fiabilite_reduite ne doit pas apparaître dans les colonnes X du modèle"
    )
    assert "dep" not in cols, (
        "dep ne doit pas apparaître dans les colonnes X du modèle"
    )
