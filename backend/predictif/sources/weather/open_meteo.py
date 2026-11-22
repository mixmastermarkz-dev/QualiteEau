"""
Source météo Open-Meteo.

AVERTISSEMENT LICENCE :
  L'API archive gratuite d'Open-Meteo (archive-api.open-meteo.com) est réservée
  à un USAGE NON COMMERCIAL.
  Pour un usage commercial, une clé payante est OBLIGATOIRE.
  Référence : https://open-meteo.com/en/terms

  Ce module vérifie la variable d'environnement OPEN_METEO_API_KEY.
  Si elle est absente, il ajoute un avertissement dans les logs et le rapport.
  En production commerciale, refusez le démarrage si la clé est absente.
"""
from __future__ import annotations

import logging
import os

import httpx
import pandas as pd

from predictif.sources.base import SourceResult, WeatherSource

logger = logging.getLogger(__name__)

_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
_HEADERS = {"User-Agent": "mon-environnement.fr/predictif-1.0 (contact@mon-environnement.fr)"}


class OpenMeteoSource(WeatherSource):
    """
    Open-Meteo archive.
    - Sans clé : usage non commercial uniquement.
    - Avec OPEN_METEO_API_KEY : usage commercial autorisé.
    """

    def __init__(self):
        self._api_key = os.getenv("OPEN_METEO_API_KEY", "")
        if not self._api_key:
            logger.warning(
                "OPEN_METEO_API_KEY absent. Open-Meteo archive = usage NON COMMERCIAL uniquement. "
                "En production commerciale, obtenez une clé sur https://open-meteo.com/en/pricing"
            )

    @property
    def name(self) -> str:
        return "open_meteo"

    @property
    def requires_commercial_license(self) -> bool:
        return not bool(self._api_key)  # True = avertissement, pas de clé

    async def fetch(
        self,
        points: dict[str, tuple[float, float]],
        date_debut: str,
        date_fin: str,
    ) -> SourceResult:
        tables = []
        try:
            async with httpx.AsyncClient(headers=_HEADERS, timeout=60.0) as client:
                for nom, (lat, lon) in points.items():
                    params = {
                        "latitude": lat,
                        "longitude": lon,
                        "start_date": date_debut,
                        "end_date": date_fin,
                        "daily": "precipitation_sum,et0_fao_evapotranspiration,temperature_2m_max",
                        "timezone": "Europe/Paris",
                    }
                    if self._api_key:
                        params["apikey"] = self._api_key
                    r = await client.get(_ARCHIVE_URL, params=params)
                    if r.status_code != 200:
                        logger.warning("Open-Meteo HTTP %s pour %s", r.status_code, nom)
                        continue
                    d = r.json()
                    if "daily" not in d:
                        continue
                    df = pd.DataFrame(d["daily"])
                    df["station"] = nom
                    tables.append(df)

            if not tables:
                return SourceResult(data=None, available=False, error="Aucune donnée météo Open-Meteo", source_name=self.name)

            df = pd.concat(tables)
            df["time"] = pd.to_datetime(df["time"])
            return SourceResult(data=df, available=True, source_name=self.name)

        except Exception as e:
            logger.error("Open-Meteo error : %s", e)
            return SourceResult(data=None, available=False, error=str(e), source_name=self.name)
