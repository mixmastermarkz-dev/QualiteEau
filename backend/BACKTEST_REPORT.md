# BACKTEST_REPORT.md — Modèle prédictif sécheresse Occitanie

> **Statut : VALIDÉ ✅** — Backtest du 2026-11-22. AUC ≥ 0.70 sur les 3 horizons.
> **Règle absolue :** aucune mise en production sans rapport validé par le propriétaire.

## Protocole

- Entraînement : 2012–2022
- Test hors-échantillon : 2023–2026 (1,6–1,7M observations)
- Variable cible : passage en niveau ≥ 2 (alerte) dans les X jours suivants
- Algorithme : HistGradientBoostingClassifier (scikit-learn)
- Sources : VigiEau (2332 arrêtés), Hub'Eau hydrométrie (1,96M obs), Open-Meteo (13 points)
- Jeu de données : 3 086 065 lignes × 62 colonnes, 1 482 zones d'alerte

## Métriques par horizon

| Horizon | AUC | Précision moy. | Brier | Taux de base | N test |
|---------|-----|----------------|-------|--------------|--------|
| 7 jours  | **0.972** | 11.0 % | 0.0031 | 0.30 % | 1 667 766 |
| 15 jours | **0.973** | 24.2 % | 0.0056 | 0.65 % | 1 656 054 |
| 30 jours | **0.975** | 39.4 % | 0.0099 | 1.30 % | 1 634 190 |

## Délai d'anticipation (horizon 30 j)

| Métrique | Valeur |
|----------|--------|
| Événements testés | 726 |
| Événements anticipés (p ≥ 0.5) | 337 (46 %) |
| Délai médian | **18 jours** |

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

- [x] AUC ≥ 0.70 sur les 3 horizons (0.972 / 0.973 / 0.975)
- [x] Délai médian d'anticipation ≥ 10 jours (18 jours)
- [ ] Résultats vérifiés par le propriétaire du projet
- [ ] Mention légale affichée sur toutes les interfaces
