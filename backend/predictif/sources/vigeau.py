"""
Source VigiEau : arrêtés préfectoraux sécheresse depuis data.gouv.fr.

Valide le schéma des CSV et lève une erreur explicite si les colonnes changent.
"""
from __future__ import annotations

import io
import logging
import unicodedata
import zipfile
from pathlib import Path

import httpx
import pandas as pd

from predictif.sources.base import DataSource, SourceResult

logger = logging.getLogger(__name__)

DATAGOUV_VIGIEAU = "https://www.data.gouv.fr/api/1/datasets/donnee-secheresse-vigieau/"
_HEADERS = {"User-Agent": "mon-environnement.fr/predictif-1.0 (contact@mon-environnement.fr)"}

# Colonnes minimales attendues dans les CSV VigiEau (après normalisation)
# La détection se fait par heuristique (see _trouver_colonne), mais ces noms
# sont vérifiés comme résultat final.
COLONNES_REQUISES = {"debut", "niveau", "zone"}


def _sans_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(c for c in s if not unicodedata.combining(c)).lower().strip()


def _trouver_colonne(cols: list[str], inclut: list[str], exclut: tuple[str, ...] = ()) -> str | None:
    for c in cols:
        cn = _sans_accents(c)
        if all(m in cn for m in inclut) and not any(m in cn for m in exclut):
            return c
    return None


def _niveau_numerique(v) -> float:
    import numpy as np
    if pd.isna(v):
        return np.nan
    s = _sans_accents(v).replace("_", " ")
    if s.isdigit():
        return float(s)
    if "crise" in s:
        return 4.0
    if "renforc" in s:
        return 3.0
    if "alerte" in s:
        return 2.0
    if "vigilance" in s:
        return 1.0
    if any(w in s for w in ("absence", "aucun", "normal", "pas de")):
        return 0.0
    return np.nan


def _lire_csv_arrete(path: Path) -> pd.DataFrame | None:
    """Lit un CSV d'arrêté, détecte les colonnes, retourne None si invalide."""
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
    c = {
        "debut": _trouver_colonne(cols, ["debut"], ("cadre",)),
        "fin": _trouver_colonne(cols, ["fin"], ("cadre", "fichier")),
        "niveau": (_trouver_colonne(cols, ["niveau"], ("aep",))
                   or _trouver_colonne(cols, ["gravite"], ("aep",))),
        "dep": _trouver_colonne(cols, ["dep"], ("date",)),
        "zone": _trouver_colonne(cols, ["nom", "zone"]) or _trouver_colonne(cols, ["zone"]),
    }

    manquantes = {k for k in COLONNES_REQUISES if not c.get(k)}
    if manquantes:
        logger.error(
            "CSV VigiEau %s : colonnes manquantes %s. "
            "Colonnes détectées : %s. Colonnes disponibles : %s",
            path.name, manquantes, c, cols,
        )
        raise ValueError(
            f"Schéma VigiEau inattendu dans {path.name} : colonnes {manquantes} introuvables. "
            f"Colonnes disponibles : {cols}. "
            "Mettez à jour _trouver_colonne() dans vigeau.py."
        )

    out = pd.DataFrame({
        "debut": pd.to_datetime(df[c["debut"]], errors="coerce", dayfirst=False),
        "fin": pd.to_datetime(df[c["fin"]], errors="coerce") if c["fin"] else pd.NaT,
        "niveau": df[c["niveau"]].map(_niveau_numerique),
        "dep": (df[c["dep"]].astype(str).str.extract(r"(\d{1,3}|2A|2B)")[0].str.zfill(2)
                if c["dep"] else None),
        "zone": df[c["zone"]].map(_sans_accents),
    })
    return out.dropna(subset=["debut", "niveau", "zone"])


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
                # Exclure explicitement les fichiers géographiques (peuvent faire > 500 Mo)
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
        morceaux = []
        errors = []
        for f in sorted(self._cache_dir.glob("*.csv")):
            # Les fichiers "cadre" sont des métadonnées administratives sans colonne niveau
            if "cadre" in f.name.lower():
                logger.debug("VigiEau : fichier cadre ignoré (pas de niveaux) : %s", f.name)
                continue
            try:
                df = _lire_csv_arrete(f)
                if df is not None and not df.empty:
                    morceaux.append(df)
            except ValueError as e:
                errors.append(str(e))

        if errors:
            # Schéma inattendu : on plante clairement
            raise ValueError("Erreurs de schéma VigiEau :\n" + "\n".join(errors))

        if not morceaux:
            return None

        a = pd.concat(morceaux, ignore_index=True).drop_duplicates()
        if depts and a["dep"].notna().any():
            a = a[a["dep"].isin(depts)]
        return a
