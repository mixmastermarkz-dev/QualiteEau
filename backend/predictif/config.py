"""
Configuration du module prédictif — Occitanie.
Toutes les constantes ici, jamais dans la logique métier.
"""
from __future__ import annotations
from pathlib import Path

# Périmètre Occitanie
OCCITANIE_DEPTS = ["09","11","12","30","31","32","34","46","48","65","66","81","82"]

# Horizons de prédiction (jours)
HORIZONS = [7, 15, 30]

# Niveau cible (passage en alerte = niveau >= 2)
NIVEAU_CIBLE = 2

# Période d'entraînement / de test
ANNEE_FIN_ENTRAINEMENT = 2022
DATE_DEBUT_HISTORIQUE = "2012-01-01"

# Seuil de probabilité pour le calcul du délai d'anticipation
SEUIL_PROBA = 0.5

# Nombre de stations gardées comme variables individuelles
NB_STATIONS_DETAILLEES = 25

# Points météo représentatifs de l'Occitanie (lat, lon)
POINTS_METEO_OCCITANIE = {
    "montpellier": (43.61, 3.88),
    "toulouse": (43.60, 1.44),
    "nimes": (43.84, 4.36),
    "carcassonne": (43.21, 2.35),
    "perpignan": (42.69, 2.89),
    "rodez": (44.35, 2.57),
    "mende": (44.52, 3.50),
    "foix": (42.97, 1.61),
    "tarbes": (43.23, 0.07),
    "montauban": (44.02, 1.35),
    "albi": (43.93, 2.15),
    "cahors": (44.45, 1.44),
    "beziers": (43.34, 3.21),
}

# Dossier de données (relatif à la racine backend/)
DATA_DIR = Path(__file__).parent.parent / "data" / "predictif"
CACHE_DIR = DATA_DIR / "cache"
MODELS_DIR = DATA_DIR / "models"

# URLs des APIs
HUBEAU_BASE = "https://hubeau.eaufrance.fr/api"
DATAGOUV_VIGIEAU = "https://www.data.gouv.fr/api/1/datasets/donnee-secheresse-vigieau/"

# Délai de grâce pour les alertes e-mail (éviter le spam)
ALERTE_COOLDOWN_HEURES = 24
