"""
Source VigiEau : arrêtés préfectoraux sécheresse depuis data.gouv.fr.

Format VigiEau (depuis ~2023) :
  - 1 ligne CSV = 1 arrêté
  - Les colonnes `zones_alerte.code`, `.nom`, `.niveau_gravite`, `.type`
    contiennent des **listes JSON** (une entrée par zone couverte par l'arrêté)
  - Cette fonction les éclate : 1 ligne de sortie = 1 (arrêté, zone)

Schéma de sortie :
  arrete, dep, date_signature, debut, fin, zone, nom, type, niveau

Niveau (entier 1–4) :
  vigilance=1  alerte=2  alerte_renforcée=3  crise=4
  Toute valeur inconnue lève ValueError (pas de devinette silencieuse).
"""
from __future__ import annotations

import io
import json
import logging
import re
import unicodedata
import zipfile
from pathlib import Path

import httpx
import pandas as pd

from predictif.sources.base import DataSource, SourceResult

logger = logging.getLogger(__name__)

DATAGOUV_VIGIEAU = "https://www.data.gouv.fr/api/1/datasets/donnee-secheresse-vigieau/"
_HEADERS = {"User-Agent": "mon-environnement.fr/predictif-1.0 (contact@mon-environnement.fr)"}

# Mapping strict : valeur normalisée → entier 1–4.
# Pour ajouter un synonyme, ajoutez-le ici et documentez la source.
_NIVEAUX_GRAVITE: dict[str, int] = {
    "vigilance": 1,
    "alerte": 2,
    "alerte_renforcee": 3,   # virgule sous-score (VigiEau API)
    "crise": 4,
}


# ── Utilitaires ───────────────────────────────────────────────────────────────

def _sans_accents(s: str) -> str:
    """Normalise une chaîne : minuscules, sans accents."""
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(c for c in s if not unicodedata.combining(c)).lower().strip()


def _trouver_colonne(cols: list[str], inclut: list[str], exclut: tuple[str, ...] = ()) -> str | None:
    """Heuristique : première colonne dont le nom normalisé contient tous les mots d'`inclut`."""
    for c in cols:
        cn = _sans_accents(c)
        if all(m in cn for m in inclut) and not any(m in cn for m in exclut):
            return c
    return None


def _trouver_colonne_exacte(cols: list[str], candidates: list[str]) -> str | None:
    """Retourne la première colonne dont le nom normalisé correspond exactement à un candidat."""
    normalises = [_sans_accents(x) for x in candidates]
    for c in cols:
        if _sans_accents(c) in normalises:
            return c
    return None


def _parse_liste_json(valeur) -> list:
    """Parse une cellule contenant une liste JSON. Retourne [] si vide/NaN."""
    if pd.isna(valeur):
        return []
    try:
        x = json.loads(str(valeur))
        return x if isinstance(x, list) else [x]
    except (json.JSONDecodeError, TypeError, ValueError):
        # Repli : traiter la valeur comme un élément unique
        return [str(valeur)]


def _normaliser_niveau(valeur: str) -> str:
    """Normalise un niveau : minuscules, sans accents, espaces → underscore."""
    n = _sans_accents(str(valeur))
    n = re.sub(r"[\s\-]+", "_", n).strip("_")
    return n


# ── Parsing CSV ───────────────────────────────────────────────────────────────

def _lire_csv_arrete(path: Path) -> pd.DataFrame | None:
    """
    Lit un CSV VigiEau. Éclate les listes JSON : une ligne par (arrêté, zone).

    Retourne None si le fichier n'a pas le bon format (ignoré silencieusement).
    Lève ValueError si un niveau inconnu est rencontré (pas de devinette).

    Schéma de sortie :
      arrete | dep | date_signature | debut | fin | zone | nom | type | niveau
    """
    # Lecture brute
    df = None
    for sep in (",", ";"):
        try:
            d = pd.read_csv(path, sep=sep, dtype=str, low_memory=False)
            if d.shape[1] > 3:
                df = d
                break
        except Exception:
            pass
    if df is None or df.empty:
        return None

    cols = list(df.columns)

    # Colonnes obligatoires du format VigiEau (zones_alerte.*)
    col_code = _trouver_colonne_exacte(cols, ["zones_alerte.code"])
    col_niveau = _trouver_colonne_exacte(cols, [
        "zones_alerte.niveau_gravite",
        "zones_alerte.niveaugravite",
    ])

    if not col_code or not col_niveau:
        # Format non VigiEau (ex. Propluvia pré-2023) ou fichier cadre → ignorer
        logger.debug(
            "CSV %s : colonnes zones_alerte.code / .niveau_gravite absentes — format inconnu, ignoré",
            path.name,
        )
        return None

    # Colonnes optionnelles
    col_nom = _trouver_colonne_exacte(cols, ["zones_alerte.nom"])
    col_type = _trouver_colonne_exacte(cols, ["zones_alerte.type"])

    # Métadonnées de l'arrêté
    col_arrete = _trouver_colonne_exacte(cols, ["numero", "numero_arrete", "id", "id_arrete"])
    col_dep = _trouver_colonne_exacte(cols, [
        "departement", "code_departement", "num_departement", "numero_departement",
    ])
    col_debut = _trouver_colonne_exacte(cols, ["date_debut"])
    col_fin = _trouver_colonne_exacte(cols, ["date_fin"])
    col_signature = _trouver_colonne_exacte(cols, ["date_signature"])

    if not col_debut:
        logger.error("CSV %s : colonne date_debut introuvable. Colonnes : %s", path.name, cols)
        raise ValueError(f"Colonne date_debut introuvable dans {path.name}. Colonnes : {cols}")

    # Éclater les listes JSON
    lignes: list[dict] = []
    niveaux_inconnus: set[str] = set()

    for _, row in df.iterrows():
        codes = _parse_liste_json(row.get(col_code, ""))
        noms = _parse_liste_json(row.get(col_nom, "")) if col_nom else []
        niveaux_bruts = _parse_liste_json(row.get(col_niveau, ""))
        types = _parse_liste_json(row.get(col_type, "")) if col_type else []

        # Département : extraire les chiffres (ou 2A/2B)
        dep_brut = str(row.get(col_dep, "") or "")
        m_dep = re.search(r"(\d{1,3}|2[Aa]|2[Bb])", dep_brut)
        dep = m_dep.group(1).zfill(2)[:3] if m_dep else None

        arrete = row.get(col_arrete) if col_arrete else None
        debut_brut = row.get(col_debut)
        fin_brut = row.get(col_fin) if col_fin else None
        signature_brut = row.get(col_signature) if col_signature else None

        for i, code in enumerate(codes):
            if not code or pd.isna(code):
                continue

            if i >= len(niveaux_bruts):
                continue  # pas de niveau correspondant à ce code

            niv_brut = str(niveaux_bruts[i]).strip()
            niv_norme = _normaliser_niveau(niv_brut)

            if niv_norme not in _NIVEAUX_GRAVITE:
                niveaux_inconnus.add(niv_brut)
                continue  # collecte l'inconnu, lèvera après la boucle

            lignes.append({
                "arrete": arrete,
                "dep": dep,
                "date_signature": signature_brut,
                "debut": debut_brut,
                "fin": fin_brut,
                "zone": str(code),
                "nom": noms[i] if i < len(noms) else None,
                "type": types[i] if i < len(types) else None,
                "niveau": float(_NIVEAUX_GRAVITE[niv_norme]),
            })

    if niveaux_inconnus:
        raise ValueError(
            f"Niveaux inconnus dans {path.name} : {sorted(niveaux_inconnus)}. "
            f"Valeurs attendues : {list(_NIVEAUX_GRAVITE.keys())}. "
            "Ajoutez la correspondance dans _NIVEAUX_GRAVITE de vigeau.py."
        )

    if not lignes:
        return None

    out = pd.DataFrame(lignes)
    out["debut"] = pd.to_datetime(out["debut"], errors="coerce", dayfirst=False)
    out["fin"] = pd.to_datetime(out["fin"], errors="coerce")
    out["date_signature"] = pd.to_datetime(out["date_signature"], errors="coerce")

    return out.dropna(subset=["debut", "niveau", "zone"])


# ── Source VigiEau ────────────────────────────────────────────────────────────

class VigeauSource(DataSource):
    """
    Arrêtés préfectoraux VigiEau téléchargés depuis data.gouv.fr.
    Met en cache les CSV dans cache_dir pour éviter les re-téléchargements.
    """

    def __init__(self, cache_dir: Path):
        self._cache_dir = cache_dir / "arretes"
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    @property
    def name(self) -> str:
        return "vigeau"

    async def fetch(self, depts: list[str], date_debut: str, date_fin: str) -> SourceResult:
        try:
            async with httpx.AsyncClient(headers=_HEADERS, timeout=30.0) as client:
                r = await client.get(DATAGOUV_VIGIEAU)
                r.raise_for_status()
                meta = r.json()

            ressources = meta.get("resources", [])
            downloaded = 0
            for res in ressources:
                titre = res.get("title", "")
                url = res.get("url", "")
                fmt = (res.get("format") or "").lower()
                t = _sans_accents(titre)
                u = url.lower()

                # On ne veut QUE les CSV d'arrêtés — exclure GeoJSON, shapefiles, zones géo
                if fmt not in ("csv", "zip"):
                    continue
                if "arr" not in t:
                    continue
                if any(x in u for x in ("geojson", "shapefile", "shp", "gpkg", "zones_geo", "pmtiles", ".pbf", "mvt")):
                    logger.info("VigiEau : fichier géographique ignoré : %s", url[:80])
                    continue
                if any(x in t for x in ("geojson", "shapefile", "geographi", "cartograph")):
                    logger.info("VigiEau : ressource géographique ignorée : %s", titre[:60])
                    continue

                nom = self._cache_dir / (_sans_accents(titre).replace(" ", "_").replace("/", "-")[:80] + "." + fmt)
                if nom.exists():
                    continue

                async with httpx.AsyncClient(headers=_HEADERS, timeout=300.0) as client:
                    r2 = await client.get(url)
                    contenu = r2.content

                if fmt == "zip":
                    with zipfile.ZipFile(io.BytesIO(contenu)) as z:
                        for n in z.namelist():
                            if n.lower().endswith(".csv"):
                                dest = self._cache_dir / (nom.stem + "__" + Path(n).name)
                                dest.write_bytes(z.read(n))
                else:
                    nom.write_bytes(contenu)
                downloaded += 1

            logger.info("VigiEau : %d nouveaux fichiers téléchargés", downloaded)
            df = self._lire_tous(depts)
            if df is None or df.empty:
                return SourceResult(data=None, available=False, error="Aucune donnée VigiEau parseable", source_name=self.name)
            return SourceResult(data=df, available=True, source_name=self.name)

        except Exception as e:
            logger.error("VigiEau fetch error : %s", e)
            return SourceResult(data=None, available=False, error=str(e), source_name=self.name)

    def _lire_tous(self, depts: list[str]) -> pd.DataFrame | None:
        """
        Lit tous les CSV du cache. Déduplique sur (arrete, zone) pour éliminer
        les doublons entre le fichier général et les fichiers annuels.
        """
        morceaux = []
        errors = []

        for f in sorted(self._cache_dir.glob("*.csv")):
            if "cadre" in f.name.lower():
                logger.debug("VigiEau : fichier cadre ignoré : %s", f.name)
                continue
            try:
                df = _lire_csv_arrete(f)
                if df is not None and not df.empty:
                    morceaux.append(df)
            except ValueError as e:
                errors.append(str(e))

        if errors:
            raise ValueError("Erreurs de schéma VigiEau :\n" + "\n".join(errors))

        if not morceaux:
            return None

        a = pd.concat(morceaux, ignore_index=True)

        # Dédupliquer : un arrêté ne couvre une zone qu'une seule fois
        # (clé : arrete + zone ; le fichier général et les fichiers annuels se recoupent)
        n_avant = len(a)
        a = a.drop_duplicates(subset=["arrete", "zone"])
        n_doublons = n_avant - len(a)
        if n_doublons:
            logger.info("VigiEau : %d doublons supprimés (arrete, zone)", n_doublons)

        if depts and a["dep"].notna().any():
            a = a[a["dep"].isin(depts)]

        logger.info(
            "VigiEau : %d lignes (arrêté, zone) | %d zones distinctes | %d arrêtés",
            len(a), a["zone"].nunique(), a["arrete"].nunique(),
        )
        return a
