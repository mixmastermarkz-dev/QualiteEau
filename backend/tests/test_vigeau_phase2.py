"""
Tests Phase 2 — Lecture correcte des arrêtés VigiEau.

Vérifie :
  - Éclater les listes JSON : une ligne par (arrêté, zone)
  - L'arrêté DDTM34-2023-04-13770 → exactement 19 lignes avec les bons niveaux
  - Aucune valeur de `zone` ne contient `[` ou `"`
  - Un niveau inconnu lève ValueError (pas de devinette silencieuse)
  - Déduplication sur (arrete, zone) entre fichiers
"""
from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pandas as pd
import pytest

from predictif.sources.vigeau import _lire_csv_arrete, _NIVEAUX_GRAVITE


# ── Données de fixture ────────────────────────────────────────────────────────

# Représentation fidèle de l'arrêté DDTM34-2023-04-13770 (Hérault, 19 zones).
# Niveaux réels observés dans l'audit : 10 vigilance, 5 alerte, 3 alerte_renforcee, 1 crise.
_ZONES_CODES = [
    "76_34_0004", "76_34_0017", "76_34_0021", "76_34_0023", "76_34_0027",
    "76_34_0031", "76_34_0036", "76_34_0038", "76_34_0042", "76_34_0045",
    "76_34_0048", "76_34_0052", "76_34_0055", "76_34_0059", "76_34_0063",
    "76_34_0067", "76_34_0071", "76_34_0074", "76_34_0078",
]  # 19 codes

_ZONES_NOMS = [f"Zone Hérault {i + 1}" for i in range(19)]

_ZONES_NIVEAUX = (
    ["vigilance"] * 10
    + ["alerte"] * 5
    + ["alerte_renforcee"] * 3
    + ["crise"] * 1
)  # 10 + 5 + 3 + 1 = 19

_ZONES_TYPES = ["SUP"] * 15 + ["SOU"] * 3 + ["AEP"] * 1


def _csv_vigieau(arretes: list[dict]) -> str:
    """
    Génère un CSV VigiEau minimal via le module csv pour un quoting correct.
    Les colonnes zones_alerte.* contiennent des JSON arrays ; le module csv
    les encadre de guillemets et échappe les guillemets internes.
    """
    import csv
    import io as _io

    out = _io.StringIO()
    writer = csv.writer(out)
    writer.writerow([
        "numero", "departement", "date_debut", "date_fin", "date_signature",
        "zones_alerte.code", "zones_alerte.nom",
        "zones_alerte.niveau_gravite", "zones_alerte.type",
    ])
    for a in arretes:
        writer.writerow([
            a["numero"],
            a["dep"],
            a["debut"],
            a.get("fin", ""),
            a.get("signature", ""),
            json.dumps(a["codes"], ensure_ascii=False),
            json.dumps(a["noms"], ensure_ascii=False),
            json.dumps(a["niveaux"], ensure_ascii=False),
            json.dumps(a["types"], ensure_ascii=False),
        ])
    return out.getvalue()


@pytest.fixture
def fixture_ddtm34(tmp_path: Path) -> Path:
    """CSV avec l'arrêté DDTM34-2023-04-13770 (19 zones)."""
    contenu = _csv_vigieau([{
        "numero": "DDTM34-2023-04-13770",
        "dep": "34",
        "debut": "2023-04-01",
        "fin": "2023-10-15",
        "signature": "2023-03-29",
        "codes": _ZONES_CODES,
        "noms": _ZONES_NOMS,
        "niveaux": _ZONES_NIVEAUX,
        "types": _ZONES_TYPES,
    }])
    p = tmp_path / "arrete_34_2023.csv"
    p.write_text(contenu, encoding="utf-8")
    return p


@pytest.fixture
def fixture_deux_arretes(tmp_path: Path) -> tuple[Path, Path]:
    """Deux CSV avec le même arrêté (simule général + annuel) pour tester la dédup."""
    donnees = [{
        "numero": "DDTM34-2023-04-13770",
        "dep": "34",
        "debut": "2023-04-01",
        "fin": "",
        "signature": "2023-03-29",
        "codes": _ZONES_CODES[:3],
        "noms": _ZONES_NOMS[:3],
        "niveaux": _ZONES_NIVEAUX[:3],
        "types": _ZONES_TYPES[:3],
    }]
    p1 = tmp_path / "arretes_general.csv"
    p2 = tmp_path / "arretes_2023.csv"
    p1.write_text(_csv_vigieau(donnees), encoding="utf-8")
    p2.write_text(_csv_vigieau(donnees), encoding="utf-8")
    return p1, p2


# ── Tests de parsing ──────────────────────────────────────────────────────────

def test_ddtm34_donne_19_lignes(fixture_ddtm34: Path) -> None:
    """L'arrêté DDTM34-2023-04-13770 doit produire exactement 19 lignes."""
    df = _lire_csv_arrete(fixture_ddtm34)
    assert df is not None, "Le CSV doit être parseable"
    subset = df[df["arrete"] == "DDTM34-2023-04-13770"]
    assert len(subset) == 19, (
        f"Attendu 19 lignes pour DDTM34-2023-04-13770, obtenu {len(subset)}"
    )


def test_ddtm34_niveaux_corrects(fixture_ddtm34: Path) -> None:
    """Les niveaux de l'arrêté doivent être correctement mappés (1, 2, 3, 4)."""
    df = _lire_csv_arrete(fixture_ddtm34)
    assert df is not None
    subset = df[df["arrete"] == "DDTM34-2023-04-13770"]
    niveaux = sorted(subset["niveau"].astype(int).tolist())
    assert niveaux.count(1) == 10, f"Attendu 10 vigilance (1), obtenu {niveaux.count(1)}"
    assert niveaux.count(2) == 5,  f"Attendu 5 alerte (2), obtenu {niveaux.count(2)}"
    assert niveaux.count(3) == 3,  f"Attendu 3 alerte_renforcee (3), obtenu {niveaux.count(3)}"
    assert niveaux.count(4) == 1,  f"Attendu 1 crise (4), obtenu {niveaux.count(4)}"


def test_zone_code_pas_de_crochet_ni_guillemet(fixture_ddtm34: Path) -> None:
    """Aucune valeur de `zone` ne doit contenir '[' ou '\"' (non-régression fuite JSON)."""
    df = _lire_csv_arrete(fixture_ddtm34)
    assert df is not None
    for zone in df["zone"]:
        assert "[" not in str(zone), f"Zone contient '[' : {zone!r}"
        assert '"' not in str(zone), f"Zone contient '\"' : {zone!r}"


def test_schema_colonnes(fixture_ddtm34: Path) -> None:
    """Le DataFrame doit contenir les colonnes du schéma cible."""
    df = _lire_csv_arrete(fixture_ddtm34)
    assert df is not None
    attendues = {"arrete", "dep", "date_signature", "debut", "fin", "zone", "nom", "type", "niveau"}
    assert attendues.issubset(set(df.columns)), (
        f"Colonnes manquantes : {attendues - set(df.columns)}"
    )


def test_niveau_inconnu_leve_valueerror(tmp_path: Path) -> None:
    """Un niveau inconnu dans les données doit lever ValueError, pas être ignoré silencieusement."""
    contenu = _csv_vigieau([{
        "numero": "TEST-001",
        "dep": "34",
        "debut": "2023-05-01",
        "fin": "",
        "signature": "2023-04-28",
        "codes": ["76_34_0001"],
        "noms": ["Zone test"],
        "niveaux": ["restriction_temporaire"],  # valeur inconnue
        "types": ["SUP"],
    }])
    p = tmp_path / "niveau_inconnu.csv"
    p.write_text(contenu, encoding="utf-8")

    with pytest.raises(ValueError, match="Niveaux inconnus"):
        _lire_csv_arrete(p)


def test_format_inconnu_retourne_none(tmp_path: Path) -> None:
    """Un CSV sans colonnes zones_alerte.* (ancien format) doit retourner None, pas planter."""
    # Format Propluvia pré-2023 : colonnes différentes, pas de zones_alerte.*
    ancien_format = textwrap.dedent("""\
        code_arrete,departement,date_debut,date_fin,niveau,zone_alerte
        AP34-2019-001,34,2019-07-01,2019-09-30,alerte,Bassin du Lez
        AP34-2019-002,34,2019-07-01,2019-09-30,crise,Littoral héraultais
    """)
    p = tmp_path / "propluvia_2019.csv"
    p.write_text(ancien_format, encoding="utf-8")

    result = _lire_csv_arrete(p)
    assert result is None, "Un fichier de format inconnu doit retourner None"


def test_date_signature_presente(fixture_ddtm34: Path) -> None:
    """La date de signature doit être parsée (sert pour les délais de disponibilité)."""
    df = _lire_csv_arrete(fixture_ddtm34)
    assert df is not None
    subset = df[df["arrete"] == "DDTM34-2023-04-13770"]
    assert subset["date_signature"].notna().all(), "date_signature ne doit pas être NaT"
    assert str(subset["date_signature"].iloc[0].date()) == "2023-03-29"


# ── Test de déduplication ─────────────────────────────────────────────────────

def test_deduplication_arrete_zone(fixture_deux_arretes: tuple[Path, Path], tmp_path: Path) -> None:
    """
    Deux CSV contenant le même arrêté (fichier général + fichier annuel) doivent
    produire autant de lignes qu'un seul fichier, pas le double.
    """
    from predictif.sources.vigeau import VigeauSource

    p1, p2 = fixture_deux_arretes
    source = VigeauSource(tmp_path / "cache_dedup")
    source._cache_dir.mkdir(parents=True, exist_ok=True)

    # Copier les deux fichiers dans le cache
    import shutil
    shutil.copy(p1, source._cache_dir / p1.name)
    shutil.copy(p2, source._cache_dir / p2.name)

    df = source._lire_tous(depts=["34"])
    assert df is not None
    n = len(df[df["arrete"] == "DDTM34-2023-04-13770"])
    assert n == 3, (
        f"Après dédup, l'arrêté doit apparaître 3 fois (3 zones), pas {n}"
    )
