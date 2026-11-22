# Audit du modèle prédictif sécheresse (22/11/2026)

> **Conclusion :** le résultat « AUC 0,97, backtest validé » du commit `abd2a57c1` est **faux**. Il vient d'une erreur de lecture des données VigiEau, qui crée une fuite d'information. Le modèle sauvegardé (`data/predictif/models/2026-11-22_v1.pkl`) **ne doit pas être utilisé**. L'audit relève aussi une **faille de sécurité critique** dans `docker-compose.yml`, à corriger avant tout déploiement.

Scripts de l'audit : `Claude outputs/audit_modele/`. Ils sont reproductibles à partir du cache `data/predictif/cache/arretes/`.

---

## 1. La cause : chaque « zone » était en réalité un arrêté entier

Dans les CSV VigiEau, **une ligne = un arrêté**, et les colonnes `zones_alerte.*` contiennent des **listes JSON**. Par exemple, l'arrêté DDTM34-2023-04-13770 couvre 19 zones :

| Colonne | Contenu |
|---|---|
| `zones_alerte.code` | `["76_34_0004","76_34_0017", …]` (19 codes) |
| `zones_alerte.nom` | `["Axe Lez soutenu…","Bassin versant du Thoré amont", …]` |
| `zones_alerte.niveau_gravite` | `["vigilance","vigilance", …, "alerte", …]` |

`vigeau.py` (`_lire_csv_arrete`) lit la liste entière comme **un seul nom de zone**. `_niveau_numerique` cherche ensuite « crise », puis « renforc », puis « alerte » dans le texte de la liste, et donne à cet ensemble le niveau de la zone la plus grave.

Conséquences :
- **1 486 « zones »** pour l'Occitanie, alors qu'il y en a **846 vraies**. Chaque « zone » est en fait un arrêté.
- **96 %** de ces « zones » n'apparaissent qu'une seule année.
- `construire_jeu` fait commencer l'historique de chaque « zone » au 1er janvier de l'année de son premier arrêté. **La simple présence d'une ligne révèle donc qu'un arrêté va tomber cette année.**
- Les arrêtés sont présents en double (fichier général et fichiers annuels) : 1 901 doublons.

## 2. La preuve chiffrée (période de test 2023–2026, même code que le projet)

| Modèle | AUC J+7 | AUC J+30 | Préc. moy. J+30 |
|---|---|---|---|
| Rapport d'origine (débits + nappes + météo + arrêtés) | 0,972 | 0,975 | 0,394 |
| **Même code, avec les arrêtés seuls** (aucun débit, aucune nappe, aucune météo) | **0,974** | **0,976** | 0,357 |
| Même code sans les 2 variables d'historique (`jours_meme_niveau`, `niveau_max_365j`) | 0,702 | 0,705 | 0,024 |
| **Règle sans apprentissage :** « la zone vient d'apparaître dans le jeu » × saison | **0,941** | **0,951** | 0,285 |

Les débits et la météo n'apportent rien au score. Une règle triviale, qui n'exploite que le défaut de construction, atteint déjà 0,95.

## 3. La vraie performance, une fois la fuite corrigée (arrêtés seuls)

Reconstruction correcte :
- une ligne par (arrêté, zone), avec le code officiel `zones_alerte.code` ;
- une zone n'existe dans le jeu qu'**à partir de son premier arrêté** ;
- la climatologie de chaque zone est calculée **uniquement sur les années passées** ;
- **30 jours de marge** séparent l'entraînement du test.

| Horizon | AUC | Préc. moy. | Taux de base |
|---|---|---|---|
| J+7 | 0,925 | 0,094 | 0,9 % |
| J+15 | 0,924 | 0,148 | 1,9 % |
| J+30 | 0,925 | 0,266 | 3,6 % |

L'AUC reste flatteuse : elle compte des millions de jours d'hiver sans enjeu. **Les mesures qui comptent sont celles par événement** (1 121 entrées en alerte entre 2023 et 2026) :

| Seuil | Entrées en alerte annoncées dans les 30 j avant | … quand le département n'avait encore **aucune** zone en alerte (286 cas) | Fausses alertes |
|---|---|---|---|
| 0,3 | 69 % | **45 %** | **74 %** |
| 0,5 | 48 % | **29 %** | **71 %** |

Lecture : le modèle sait surtout annoncer qu'une zone va suivre ses voisines, une fois que la préfecture a déjà commencé à restreindre. Ça, VigiEau et la presse le disent déjà. **Il anticipe mal la première restriction d'un département, qui est pourtant ce que les clients paieraient**, et 7 alertes sur 10 seraient fausses.

**Reste à mesurer**, avec le réseau (Hub'Eau et Open-Meteo) : est-ce que les débits, les nappes et la météo, **à l'échelle de la zone**, améliorent nettement ce modèle de référence ? C'est la seule question qui décide si un produit « prévision » est viable.

## 4. Autres défauts du modèle

1. **Variables régionales et non locales.** Débit et nappe sont résumés par la médiane de toutes les stations d'Occitanie, la météo par la moyenne de 13 villes. Aucune variable ne décrit la zone elle-même. Il faut utiliser les stations de référence de chaque zone (référentiel des seuils, phase 3 du prompt offre v2).
2. **Petite fuite par interpolation.** `features_series` utilise `interpolate(limit=10)`, qui remplit un trou avec une valeur future. Il faut un `ffill`.
3. **Délais de publication ignorés.** Le backtest suppose que tout est connu le jour J. En réalité : débits Hub'Eau autour de J+1, piézométrie souvent plusieurs jours après, archive météo plusieurs jours après, arrêtés signés avant leur entrée en vigueur. Il faut décaler chaque source de son délai réel.
4. **`fiabilite_reduite` utilisée comme variable.** Elle vaut toujours 0 à l'entraînement et 1 en production quand une source manque, donc hors de ce que le modèle a appris. Elle doit être une **information affichée**, pas une variable du modèle.
5. **`zone_code` en numéro ordinal.** Le modèle mémorise des identifiants de zones qui, en plus, changent entre 2012-2022 et 2023+. À retirer, ou à remplacer par des attributs stables : type de ressource, département, climatologie passée.
6. **Le backtest par département ne tourne jamais.** Le jeu n'a pas de colonne `dep` (bug dans `entrainer_et_evaluer`). Le tableau du rapport est vide.
7. **Aucune référence de comparaison.** Une AUC seule, sur des données déséquilibrées et saisonnières, ne prouve rien. Il faut toujours comparer au modèle « arrêtés seuls » et à la climatologie de la zone.
8. **Pas de calibration.** Une probabilité de 0,6 doit vouloir dire environ 60 %. Il faut une courbe de fiabilité, et une calibration sur une année de validation distincte du test.
9. **Fin d'arrêté inconnue.** Elle est fixée arbitrairement à 120 jours. C'est rare (0,1 % des cas) mais à documenter.

## 5. Sécurité

| # | Gravité | Problème | Correction |
|---|---|---|---|
| S1 | **Critique** | `docker-compose.yml` monte **toute la racine du dépôt** (`.:/app/site:ro`), et `main.py` la sert sur `/` avec `StaticFiles`. Sont alors téléchargeables : `/backend/.env` et `/.env` (secrets JWT, Stripe, base de données ; les deux fichiers existent), `/backend/data/predictif/models/*.pkl`, le jeu de données, et probablement `/.git/`. | Servir **uniquement** un dossier de publication construit exprès (liste blanche : `index.html`, `eau-potable/`, `departement/`, `tarifs/`, `assets/`, pages légales…). Refuser les fichiers cachés. En développement, n'exposer les ports **8000 et 5432** que sur `127.0.0.1` (aujourd'hui `"8000:8000"` et `"5432:5432"`, donc joignables depuis tout le réseau local). Ajouter un test qui vérifie que `/backend/.env`, `/.env`, `/.git/config` et `/*.pkl` renvoient 404. |
| S2 | Élevée | `pickle.load` sur le dernier `.pkl` trouvé par ordre alphabétique : quiconque peut écrire dans `models/` exécute du code sur le serveur. Aucun lien avec la validation. | Enregistrer l'empreinte SHA-256 dans `model_version` et la vérifier avant de charger. Ne charger que la version `is_active=True` **validée par le propriétaire**. Fichiers en lecture seule pour l'application. |
| S3 | Élevée | Entraînement automatique en production si aucun modèle n'existe (`runner.py`) : un modèle non validé peut être servi sans que personne le sache. | Pas d'entraînement automatique en production. Sans modèle validé, pas de prévision. |
| S4 | Élevée | `predire_zone` prend le dernier jour où `q_med` et `pluie_30j` existent. Comme Hub'Eau est figé depuis mai 2026, une « prévision du jour » peut reposer en silence sur des données vieilles de plusieurs mois. | Contrôle de fraîcheur pour chaque source. Au-delà du délai, pas de prévision ou une alerte « fiabilité réduite » visible, avec la date des données. |
| S5 | Moyenne | Le rapport affichait « VALIDÉ » sur la seule base d'une AUC ≥ 0,70. | Statut passé à « INVALIDÉ ». Nouveaux critères de validation (section 6). |

## 6. Critères de validation proposés (à valider par le propriétaire)

Avant de vendre une prévision :
1. Aucune fuite : tests automatisés (section 3 du prompt de correction).
2. Comparaison obligatoire au modèle « arrêtés seuls » et à la climatologie de la zone, sur la même période de test.
3. Mesures par événement au seuil réellement utilisé : **premières restrictions du département annoncées au moins 7 jours avant** ; **part de fausses alertes** ; délai médian d'anticipation.
4. Amélioration **nette** par rapport au modèle « arrêtés seuls » sur les premières restrictions du département. Sinon, l'ajout des débits et de la météo n'apporte rien, et on ne vend pas de « prévision ».
5. Calibration vérifiée (courbe de fiabilité).
6. Résultats par département. Pas de vente dans un département où le modèle n'est pas meilleur que la référence.

**Seuils chiffrés : à fixer par le propriétaire.** Une base raisonnable : au moins 50 % des premières restrictions annoncées 7 jours avant, avec au plus 50 % de fausses alertes. Si ce n'est pas atteint, la **jauge de distance au seuil** (phase 4 du prompt offre v2) reste le produit à vendre : transparente et vérifiable, elle ne promet pas une prévision.
