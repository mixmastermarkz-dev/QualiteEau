# BACKTEST_REPORT.md — Modèle prédictif sécheresse Occitanie

> **Statut : À VALIDER PAR LE PROPRIÉTAIRE ⚠️**
> Ce rapport est généré automatiquement par `scripts/run_backtest.py`.
> **Aucune mise en production avant validation explicite du propriétaire du projet.**
> Ne pas modifier les chiffres sans relancer le script.

---

## Protocole d'évaluation (Phase 4)

| Bloc | Période | Rôle |
|------|---------|------|
| **Entraînement** | jusqu'au 2021-12-01 (−30 j marge) | apprentissage des 3 modèles |
| **Validation** | 2022 complet | calibration isotonique uniquement |
| **Test** (hors-échantillon) | depuis le 2023-02-01 (+30 j marge) | évaluation finale |

Les modèles (b) et (c) sont calibrés par régression isotonique sur la validation.
La calibration n'est **jamais** ajustée sur le test (absence de fuite de calibration).

---

## Comparaison des trois modèles

Trois modèles sont comparés sur la **même période de test** :

- **(a) Climatologie** : score saisonnier historique de la zone (aucun ML, aucune source externe)
- **(b) Arrêtés seuls** : ML entraîné uniquement sur les arrêtés VigiEau
- **(c) Modèle complet** : arrêtés + débits Hub'Eau + nappes + météo Open-Meteo

> Les tableaux ci-dessous seront complétés à l'issue du prochain `python -m scripts.run_backtest`.
> Les valeurs indiquées sont des placeholders.

### Horizon J+7

| Modèle | AUC ROC | Prec. moy. | Brier | Taux base | N test |
|--------|---------|------------|-------|-----------|--------|
| (a) Climatologie | — | — | — | — | — |
| (b) Arrêtés seuls | — | — | — | — | — |
| **(c) Modèle complet** | **—** | **—** | **—** | — | — |

**Fausses alertes au seuil p ≥ 0.50** :

| Modèle | Nb alarmes | Taux fausse alerte | Rappel |
|--------|-----------|-------------------|--------|
| (b) Arrêtés seuls | — | — | — |
| (c) Modèle complet | — | — | — |

### Horizon J+15

| Modèle | AUC ROC | Prec. moy. | Brier | Taux base | N test |
|--------|---------|------------|-------|-----------|--------|
| (a) Climatologie | — | — | — | — | — |
| (b) Arrêtés seuls | — | — | — | — | — |
| **(c) Modèle complet** | **—** | **—** | **—** | — | — |

### Horizon J+30

| Modèle | AUC ROC | Prec. moy. | Brier | Taux base | N test |
|--------|---------|------------|-------|-----------|--------|
| (a) Climatologie | — | — | — | — | — |
| (b) Arrêtés seuls | — | — | — | — | — |
| **(c) Modèle complet** | **—** | **—** | **—** | — | — |

---

## Analyse par événement — Horizon J+30

Pour chaque passage en alerte (niveau ≥ 2) dans la période de test,
mesure si le modèle l'a annoncé et combien de jours à l'avance.

Les événements sont distingués selon le contexte départemental :
- **Première zone du dépt** : aucune autre zone du département n'est déjà en alerte → cas le plus difficile
- **Département déjà en alerte** : au moins une zone est déjà en alerte → cas plus facile

| Catégorie | Modèle | Événements | Annoncés | Taux d'annonce | Délai médian |
|-----------|--------|-----------|----------|----------------|-------------|
| Toutes zones | (b) Arrêtés seuls | — | — | — | — |
| Toutes zones | (c) Modèle complet | — | — | — | — |
| **Première zone du dépt** | (b) Arrêtés seuls | — | — | — | — |
| **Première zone du dépt** | (c) Modèle complet | — | — | — | — |
| Dépt déjà en alerte | (b) Arrêtés seuls | — | — | — | — |
| Dépt déjà en alerte | (c) Modèle complet | — | — | — | — |

---

## Résultats par département (modèle complet, hors-échantillon)

| Dept | AUC J+7 | AUC J+15 | AUC J+30 | Taux base 30j |
|------|---------|----------|----------|---------------|
| **09** — Ariège | — | — | — | — |
| **11** — Aude | — | — | — | — |
| **12** — Aveyron | — | — | — | — |
| **30** — Gard | — | — | — | — |
| **31** — Haute-Garonne | — | — | — | — |
| **32** — Gers | — | — | — | — |
| **34** — Hérault | — | — | — | — |
| **46** — Lot | — | — | — | — |
| **48** — Lozère | — | — | — | — |
| **65** — Hautes-Pyrénées | — | — | — | — |
| **66** — Pyrénées-Orientales | — | — | — | — |
| **81** — Tarn | — | — | — | — |
| **82** — Tarn-et-Garonne | — | — | — | — |

---

## Conclusion factuelle

> À compléter après le premier backtest complet.

La question centrale est : **le modèle complet fait-il nettement mieux que « arrêtés seuls »
sur les premières zones du département** (cas le plus difficile, le seul où les sources
externes comme les débits ou la météo peuvent apporter une information précoce) ?

Si ΔAUC J+30 (complet − arrêtés seuls) < 0.02 sur les premières zones du département,
les sources externes n'apportent pas de gain mesurable sur cette période de test —
ce qui peut s'expliquer par la rupture de référentiel Propluvia→VigiEau (2023)
ou par l'insuffisance des données Hub'Eau sur la période de test.

---

## Validation requise avant production

- [ ] AUC ≥ 0.70 sur les 3 horizons — modèle complet hors-échantillon
- [ ] Modèle complet nettement meilleur que arrêtés seuls sur les premières zones du département
- [ ] Calibration acceptable (ECE < 0.05 après calibration isotonique)
- [ ] Résultats vérifiés et validés par le propriétaire du projet
- [ ] Mention légale affichée sur toutes les interfaces utilisateur

---

## Changement de référentiel de zones : Propluvia → VigiEau (2023)

Avant 2023, les arrêtés étaient publiés sur **Propluvia** (DGPR/BRGM).
Depuis début 2023, le référentiel officiel est **VigiEau** avec un nouveau système de codes.

| Période | Source | Format | Codes de zones |
|---------|--------|--------|---------------|
| 2012–2022 | Propluvia | 1 ligne = 1 zone | Noms libres (ex. `Bassin du Lez soutenu amont`) |
| 2023– | VigiEau | 1 ligne = 1 arrêté, listes JSON | Codes normalisés (ex. `76_34_0004`) |

**Conséquences** :
- Le modèle ne peut pas exploiter l'historique spécifique d'une zone Propluvia pour une zone VigiEau.
- La variable `zone_code` (ordinal) a été **supprimée** des features (Phase 3) car incohérente entre référentiels.
- Une zone n'entre dans le jeu qu'à partir de son **premier arrêté** (pas rétroactivement depuis le 1er janvier).

---

## Historique : résultat invalidé du 2026-11-22

> ❌ **INVALIDÉ** — les chiffres ci-dessous proviennent d'une version avec fuite de données.
> Ils ne doivent pas être utilisés. Conservés ici à titre d'archive de l'audit.

**Cause racine** : le parser VigiEau lisait chaque liste JSON de zones comme un seul nom
de zone (ex. `['76_34_0004', '76_34_0017', ...]` = 1 zone au lieu de 19).
Résultat : 1 486 « zones » fictives, chacune = 1 arrêté entier.
La présence même d'une zone dans le jeu signalait qu'un arrêté existait cette année-là.
Une règle triviale « zone récemment apparue × haute saison » atteignait AUC 0.941.

| Horizon | AUC (invalidé) | Note |
|---------|---------------|------|
| J+7  | ~~0.972~~ | fuite de données |
| J+15 | ~~0.973~~ | fuite de données |
| J+30 | ~~0.975~~ | fuite de données |

---

## Avertissements

### Licence Open-Meteo
L'API archive gratuite d'Open-Meteo (météo historique) est **réservée à un usage non commercial**.
En production commerciale : obtenir une clé payante Open-Meteo ou implémenter `MeteoFranceSource`.

### Données Hub'Eau
Hub'Eau a connu une maintenance en mai–novembre 2026. Les données hydrologiques
peuvent être incomplètes pour cette période. Vérifier la couverture avant validation.
