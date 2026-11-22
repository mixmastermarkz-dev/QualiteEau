# Rapport final — Correction du modèle prédictif sécheresse Occitanie

**Date :** 2026-11-22 — Fin du plan de correction en 5 phases
**Branche :** `feature/predictif-abonnements`

---

## 1. Résumé (10 lignes)

Le backtest « AUC 0,97, validé » du commit `abd2a57c1` était **faux** : chaque arrêté VigiEau était lu comme une seule zone fictive, créant une fuite de présence triviale.
Corrigé, le modèle de référence (arrêtés seuls, sans débit ni météo) obtient AUC ≈ 0,92 sur 2023–2026 — chiffre flatteur sur un problème très déséquilibré.
Ce qui compte vraiment : sur les **premières restrictions d'un département** (286 cas, les seuls où la prévision a de la valeur), seulement **29 %** sont annoncées 30 jours avant au seuil 0,5, avec **71 % de fausses alertes**.
Le modèle sait surtout qu'une zone suit ses voisines une fois que le département est déjà en alerte — information que VigiEau et la presse communiquent déjà gratuitement.
La **question décisive reste ouverte** : les débits Hub'Eau et la météo à l'échelle de chaque zone améliorent-ils nettement ce modèle de référence sur les premières restrictions ?
La réponse sera dans `BACKTEST_REPORT.md` après le premier lancement de `python -m scripts.run_backtest`.
Si le gain (ΔAUC, Δtaux d'annonce sur premières zones) est < 2 points, les sources externes n'apportent rien de vendable.
En l'état, **le produit commercial « prévision sécheresse » n'est pas justifié** ; la **jauge de distance au seuil** (niveau actuel vs seuil préfectoral) reste le produit à proposer.
Les 4 failles de sécurité critiques ou élevées (S1–S4) sont **corrigées** et couvertes par des tests automatisés.
Le code est prêt pour un premier backtest réel ; aucune mise en production avant validation par le propriétaire.

---

## 2. Critères de validation (audit § 6) — état au 2026-11-22

Les seuils chiffrés de référence (audit § 6, dernière ligne) :
> **50 % des premières restrictions annoncées 7+ jours avant, au plus 50 % de fausses alertes.**

| # | Critère | Résultat | Statut |
|---|---------|----------|--------|
| 1 | **Aucune fuite** — tests automatisés (T1–T5) passent en CI | Tests T1–T5 créés et passent (Phase 3) | ✅ |
| 2 | **Comparaison obligatoire** au modèle arrêtés seuls et à la climatologie, sur la même période de test | Trois modèles côte à côte dans `entrainer_et_evaluer` et `BACKTEST_REPORT.md` (Phase 4) | ✅ |
| 3a | **Premières restrictions annoncées ≥ 50 %** au moins 7 jours avant (seuil 0,5) | Modèle ref (arrêtés seuls) : **29 %** à J+30 _(audit backtest_honnete.py)_ | ❌ |
| 3b | **Fausses alertes ≤ 50 %** au seuil utilisé | Modèle ref (arrêtés seuls) : **71 %** à J+30 _(audit)_ | ❌ |
| 3c | Délai médian d'anticipation documenté | 18 jours (sur données avec fuite — **non fiable**) ; à mesurer après backtest corrigé | ⚠️ à remesurer |
| 4 | **Amélioration nette** du modèle complet vs arrêtés seuls sur premières restrictions | Pas encore mesuré sur modèle corrigé — dépend du backtest complet | ⏳ |
| 5 | **Calibration** vérifiée (courbe de fiabilité, ECE) | Infrastructure en place (IsotonicRegression sur validation 2022, `calibration_curve`) | ⏳ chiffres après backtest |
| 6 | **Résultats par département** — pas de vente là où le modèle n'est pas meilleur que la référence | Infrastructure en place ; tableau à remplir après backtest | ⏳ |

**Statuts :** ✅ acquis · ❌ non atteint · ⚠️ à vérifier · ⏳ en attente du backtest complet

---

## 3. Recommandation : ne pas commercialiser de « prévision » en l'état

Les critères 3a et 3b (audit § 6) ne sont **pas atteints** avec le modèle de référence (arrêtés seuls) :

- Seulement **29 %** des premières restrictions de département sont annoncées, contre 50 % requis.
- **71 %** des alarmes seraient fausses, contre 50 % maximum requis.

Le modèle anticipe bien les zones qui suivent leurs voisines (département déjà en alerte), mais c'est ce que VigiEau affiche déjà publiquement. Ce qu'un abonné paierait, c'est l'anticipation de la **première restriction** du département — et c'est précisément là que le modèle échoue.

### Si le modèle complet (avec débits + nappes + météo) n'améliore pas nettement le modèle arrêtés seuls

**Recommandation : ne pas vendre de produit « prévision ».**

Proposer à la place la **jauge de distance au seuil** :
- Afficher le niveau actuel (en pourcentage du seuil préfectoral) pour chaque zone.
- Transparent, vérifiable, basé sur des données officielles en temps réel.
- Honnête : pas de probabilité inventée, pas de fausse promesse.
- Commercialisable dès maintenant, sans attendre un modèle ML qui atteigne les critères.

Cette approche est décrite dans le prompt offre v2, phase 4.

### Si le modèle complet améliore nettement le modèle arrêtés seuls (ΔAUC ≥ 2 pts, Δtaux d'annonce ≥ 10 pts sur premières zones)

Soumettre `BACKTEST_REPORT.md` au propriétaire avec les chiffres réels.
Fixer les seuils de vente département par département (critère 6).
Afficher **en permanence** la mention :
> « Estimation non officielle — modèle prédictif expérimental. Seuls les arrêtés préfectoraux publiés font foi. »

---

## 4. Travaux réalisés (phases 1–4)

### Phase 1 — Sécurité (4 failles corrigées)

| Réf | Gravité | Correction | Tests |
|-----|---------|-----------|-------|
| S1 | Critique | `build_site_public.py` (liste blanche), `HiddenFilesMiddleware`, ports `127.0.0.1` uniquement | `test_securite_phase1.py` (8 tests) |
| S2 | Élevée | SHA-256 sidecar + vérification avant `pickle.load`, `validated_by_owner` en DB, migration Alembic `d3e4f5a6b7c8` | id. |
| S3 | Élevée | Entraînement automatique supprimé de `runner.py` | id. |
| S4 | Élevée | `FRAICHEUR_MAX_JOURS_GLOBAL = 90`, `predire_zone` refuse si données > 90 j | id. |

### Phase 2 — Lecture correcte des arrêtés VigiEau

- `vigeau.py` réécrit : explosion des listes JSON, 1 ligne par (arrêté, zone), niveaux stricts (ValueError si inconnu), déduplication sur `(arrete, zone)`.
- Tests : `test_vigeau_phase2.py` (8 tests, dont fixture DDTM34-2023-04-13770 → 19 lignes).

### Phase 3 — Jeu sans fuite

- Zone n'existe qu'à partir de son **premier arrêté** (plus de remplissage depuis le 1er janvier).
- `ffill(limit=10)` remplace `interpolate(limit=10)`.
- `DELAIS_DISPONIBILITE` par source dans `config.py`.
- Climatologie de zone sur **années passées uniquement** (`expanding().shift(1)`).
- `zone_code` et `fiabilite_reduite` retirés des features du modèle.
- Colonne `dep` ajoutée au jeu.
- Tests : `test_anti_fuite_phase3.py` (5 tests T1–T5).

### Phase 4 — Évaluation honnête

- Découpage : train ≤ 2021-12-01 (−30 j), validation = 2022, test ≥ 2023-02-01 (+30 j).
- 3 modèles côte à côte : (a) climatologie, (b) arrêtés seuls, (c) complet.
- Calibration isotonique sur validation uniquement.
- Analyse par événement : taux d'annonce, délai médian, première zone du département vs département déjà en alerte.
- Fausses alertes au seuil SEUIL_PROBA.
- `BACKTEST_REPORT.md` avec statut « À VALIDER PAR LE PROPRIÉTAIRE » et section historique « résultat invalidé 2026-11-22 ».
- `scripts/run_backtest.py` réécrit pour produire le rapport complet.

---

## 5. Prochaine étape obligatoire

```bash
# Depuis backend/ sur la machine avec accès aux APIs
python -m scripts.run_backtest
```

Puis lire `BACKTEST_REPORT.md` et décider :
- Modèle complet nettement meilleur sur premières restrictions ? → soumettre pour validation, puis critère 4 ✅.
- Pas d'amélioration nette ? → abandonner le produit « prévision », déployer la jauge de distance au seuil.

**Aucune mise en production avant que le propriétaire ait validé `BACKTEST_REPORT.md`.**

---

## 6. Fichiers modifiés ou créés (plan de correction)

| Fichier | Phase | Action |
|---------|-------|--------|
| `docker-compose.yml` | 1-S1 | Ports `127.0.0.1`, volume `./build/site` |
| `scripts/build_site_public.py` | 1-S1 | Nouveau — liste blanche site public |
| `backend/app/main.py` | 1-S1 | `HiddenFilesMiddleware` |
| `backend/app/models/prediction.py` | 1-S2 | `model_sha256`, `validated_by_owner` |
| `backend/migrations/versions/d3e4f5a6b7c8_…py` | 1-S2 | Migration Alembic |
| `backend/predictif/config.py` | 1,3,4 | `FRAICHEUR_MAX_JOURS*`, `DELAIS_DISPONIBILITE`, `ANNEE_VALIDATION`, `MARGE_JOURS_BLOCS` |
| `backend/predictif/model.py` | 1,3,4 | SHA-256, `_NON_FEATURES`, 3 modèles, calibration, analyse événements |
| `backend/predictif/runner.py` | 1 | Pas d'entraînement auto, `validated_by_owner` |
| `backend/predictif/sources/vigeau.py` | 2 | Réécriture complète — explosion JSON |
| `backend/predictif/features.py` | 3 | `ffill`, délais, climatologie, `dep` |
| `backend/scripts/run_backtest.py` | 4 | Réécriture — 3 modèles, rapport Phase 4 |
| `backend/BACKTEST_REPORT.md` | 4 | Mise à jour — statut à valider, historique invalidé |
| `backend/tests/test_securite_phase1.py` | 1 | Nouveau — 8 tests |
| `backend/tests/test_vigeau_phase2.py` | 2 | Nouveau — 8 tests |
| `backend/tests/test_anti_fuite_phase3.py` | 3 | Nouveau — 5 tests |
| `backend/data/predictif/_invalide_2026-11-22/` | — | Ancien `.pkl` et `.parquet` déplacés |
