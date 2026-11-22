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

# Découpage temporel (Phase 4)
#   train      : jusqu'à ANNEE_FIN_ENTRAINEMENT (inclus), moins MARGE_JOURS_BLOCS de tampon
#   validation : ANNEE_VALIDATION (réglages + calibration isotonique — jamais vu en test)
#   test       : à partir de ANNEE_VALIDATION + 1, plus MARGE_JOURS_BLOCS de tampon
ANNEE_FIN_ENTRAINEMENT = 2021
ANNEE_VALIDATION = 2022
MARGE_JOURS_BLOCS = 30          # jours de tampon entre blocs pour éviter le biais de look-ahead
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

# ── Délais de disponibilité des sources ──────────────────────────────────────
# Chaque source publie ses données avec un certain délai après la date de mesure.
# En appliquant ces décalages dans construire_jeu, le backtest simule correctement
# ce que le modèle aurait connu au moment de la prédiction.
#
# Délais documentés (sources : pages API et expérience opérationnelle) :
#   hubeau_hydro  : Hub'Eau hydro publie les débits avec ~1 j de délai.
#   hubeau_piezo  : Hub'Eau piezo variable (souvent 3–14 j ; 7 j = conservateur).
#   meteo         : Open-Meteo ERA5-Land ~2 j ; archive Météo-France similaire.
#   vigeau        : Arrêtés disponibles proche de la date de signature (délai = 0).
#
# À affiner si les délais observés changent (ex. après une maintenance Hub'Eau).
DELAIS_DISPONIBILITE: dict[str, int] = {
    "hubeau_hydro": 1,
    "hubeau_piezo": 7,
    "meteo": 2,
    "vigeau": 0,
}

# ── Fraîcheur des données ──────────────────────────────────────────────────────
# Délais maximaux (en jours) au-delà desquels les données d'une source sont
# considérées trop anciennes. Ces délais tiennent compte du délai de publication
# réel de chaque API (à documenter et affiner par expérience).
#
#  vigeau      : arrêtés — source obligatoire ; pas de prédiction si dépassé
#  hubeau_hydro: débits Hub'Eau — délai réel ~J+1 ; sans prédiction fiabilité réduite
#  hubeau_piezo: piézométrie Hub'Eau — délai variable, souvent plusieurs jours
#  meteo       : archive météo — délai ~J+2 (Open-Meteo / Météo-France archive)
FRAICHEUR_MAX_JOURS: dict[str, int] = {
    "vigeau": 3,
    "hubeau_hydro": 7,
    "hubeau_piezo": 14,
    "meteo": 3,
}

# Délai global : si la dernière donnée disponible dans le jeu est plus ancienne
# que ce seuil (en jours), predire_zone refuse toute prédiction — quelle que
# soit la source concernée. Couvre le cas Hub'Eau figé depuis mai 2026.
FRAICHEUR_MAX_JOURS_GLOBAL = 90
