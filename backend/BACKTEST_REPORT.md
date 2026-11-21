# BACKTEST_REPORT.md — Modèle prédictif sécheresse Occitanie

> **Statut :** Template — les métriques réelles seront générées après le premier
> téléchargement des données historiques (`python -m predictif.runner --entrainer`).
> **Règle absolue :** aucune mise en production sans rapport validé par le propriétaire.

## Protocole

- Entraînement : 2012–2022
- Test hors-échantillon : 2023–2025
- Variable cible : passage en niveau ≥ 2 (alerte) dans les X jours suivants
- Algorithme : HistGradientBoostingClassifier (scikit-learn)

## Métriques par horizon (à remplir après backtest réel)

| Horizon | AUC | Précision moy. | Brier | Taux de base |
|---------|-----|----------------|-------|--------------|
| 7 jours | — | — | — | — |
| 15 jours | — | — | — | — |
| 30 jours | — | — | — | — |

## Délai d'anticipation (horizon 30 j)

| Métrique | Valeur |
|----------|--------|
| Événements testés | — |
| Événements anticipés | — |
| Délai médian | — jours |

## Résultats par département

| Dept | AUC 7j | AUC 15j | AUC 30j |
|------|--------|---------|---------|
| 09 — Ariège | — | — | — |
| 11 — Aude | — | — | — |
| 12 — Aveyron | — | — | — |
| 30 — Gard | — | — | — |
| 31 — Haute-Garonne | — | — | — |
| 32 — Gers | — | — | — |
| 34 — Hérault | — | — | — |
| 46 — Lot | — | — | — |
| 48 — Lozère | — | — | — |
| 65 — Hautes-Pyrénées | — | — | — |
| 66 — Pyrénées-Orientales | — | — | — |
| 81 — Tarn | — | — | — |
| 82 — Tarn-et-Garonne | — | — | — |

## Avertissements

### Licence Open-Meteo
L'API archive gratuite d'Open-Meteo (utilisée pour les données météo historiques)
est **réservée à un usage non commercial**. En production commerciale :
1. Obtenir une clé payante Open-Meteo → configurer `OPEN_METEO_API_KEY`
2. OU implémenter `MeteoFranceSource` (backend/predictif/sources/weather/meteo_france.py)

### Données Hub'Eau
Hub'Eau a connu une maintenance en mai–novembre 2026. Les données hydrologiques
peuvent être incomplètes pour cette période. Vérifier la couverture avant validation.

## Validation requise avant production

- [ ] AUC ≥ 0.70 sur les 3 horizons
- [ ] Délai médian d'anticipation ≥ 10 jours (horizon 30j)
- [ ] Résultats vérifiés par le propriétaire du projet
- [ ] Mention légale affichée sur toutes les interfaces
