"""
Source météo Météo-France (données ouvertes).

Implémentation partielle — à compléter quand les données ouvertes
Météo-France seront accessibles via l'API publique.

API : https://portail-api.meteofrance.fr/web/en/api/DonneesPubliquesObservation
Nécessite : METEOFRANCE_API_KEY (gratuite, inscription requise)

Utilisé comme alternative commerciale à Open-Meteo.
"""
from __future__ import annotations

import logging
import os

from predictif.sources.base import SourceResult, WeatherSource

logger = logging.getLogger(__name__)


class MeteoFranceSource(WeatherSource):
    """
    Source météo Météo-France (données ouvertes).
    À utiliser en production commerciale à la place d'Open-Meteo (sans clé).
    """

    def __init__(self):
        self._api_key = os.getenv("METEOFRANCE_API_KEY", "")
        if not self._api_key:
            logger.warning("METEOFRANCE_API_KEY absent — MeteoFranceSource non fonctionnel.")

    @property
    def name(self) -> str:
        return "meteo_france"

    @property
    def requires_commercial_license(self) -> bool:
        return False  # données ouvertes

    async def fetch(self, points, date_debut, date_fin) -> SourceResult:
        # TODO : implémenter l'appel à l'API Météo-France observations
        # https://portail-api.meteofrance.fr/web/en/api/DonneesPubliquesObservation
        logger.warning("MeteoFranceSource non implémenté — utilisez OpenMeteoSource pour l'instant.")
        return SourceResult(
            data=None,
            available=False,
            error="MeteoFranceSource non encore implémenté",
            source_name=self.name,
        )
