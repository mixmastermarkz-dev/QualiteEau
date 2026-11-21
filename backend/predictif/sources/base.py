"""Classes de base pour les sources de données."""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
import pandas as pd


@dataclass
class SourceResult:
    """Résultat d'une source de données."""
    data: pd.DataFrame | None
    available: bool
    error: str | None = None
    source_name: str = ""


class DataSource(ABC):
    """Interface commune pour toutes les sources de données."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Nom de la source (pour les logs et la détection de fiabilité réduite)."""

    @abstractmethod
    async def fetch(self, depts: list[str], date_debut: str, date_fin: str) -> SourceResult:
        """
        Télécharge les données pour les départements donnés.
        Ne lève jamais d'exception : retourne SourceResult(available=False, error=...) en cas d'erreur.
        """


class WeatherSource(ABC):
    """Interface pour les sources météo (Open-Meteo ou Météo-France)."""

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def requires_commercial_license(self) -> bool:
        """True si l'utilisation commerciale nécessite une clé payante."""

    @abstractmethod
    async def fetch(
        self,
        points: dict[str, tuple[float, float]],
        date_debut: str,
        date_fin: str,
    ) -> SourceResult: ...
