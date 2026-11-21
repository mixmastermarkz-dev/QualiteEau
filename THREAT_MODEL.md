# THREAT_MODEL.md — mon-environnement.fr

> **Date :** 2026-11-21
> **Méthode :** STRIDE (Spoofing, Tampering, Repudiation, Information Disclosure, Denial of Service, Elevation of Privilege)
> **Périmètre :** architecture cible (Phase 3+) incluant le backend FastAPI, l'espace payant `/app/`, et le module prédictif

---

## 1. Actifs à protéger

| Actif | Valeur | Impact d'une compromission |
|-------|--------|--------------------------|
| **Comptes et sessions** | Accès aux données payantes et aux paramètres utilisateur | Usurpation d'identité, fuite de données personnelles, accès non autorisé aux prédictions |
| **Paiement** | Flux financier, abonnements Stripe | Fraude, annulation illégitime d'abonnements, accès payant gratuit |
| **Prédictions payantes** | Données différenciantes, vendues en B2B | Concurrence déloyale, perte de valeur commerciale, accès sans paiement |
| **Données personnelles** | Emails, noms, rôles des agents de collectivités | Violation RGPD, risque juridique, perte de confiance |
| **Serveur VPS** | Infrastructure du backend et de la base de données | Arrêt du service, vol de toutes les données, ransomware |
| **Tokens d'API (formule C)** | Clés d'accès à l'API premium | Consommation hors quota, accès non facturé aux prédictions |
| **Réputation** | Crédibilité des prévisions sécheresse | Une fausse prévision présentée comme officielle expose à des poursuites et détruit la confiance |

---

## 2. Profils d'attaquants

| Attaquant | Motivation | Niveau technique | Accès initial |
|-----------|-----------|-----------------|---------------|
| **Utilisateur gratuit** | Obtenir l'accès payant sans payer | Faible à moyen | Compte gratuit, frontend public |
| **Abonné formule A** | Obtenir les droits de la formule C | Moyen | Session authentifiée formule A |
| **Concurrent** | Aspirer les données prédictives | Moyen à élevé | Aucun (scraping externe) |
| **Bot / scraper** | Consommer l'API gratuitement, spam | Faible | Réseau public |
| **Attaquant supply chain CI** | Compromettre le déploiement, injecter du code | Élevé | GitHub Actions (fork PR, tag poisoning) |
| **Attaquant externe générique** | Exploitation de vulnérabilités web | Variable | Réseau public |

---

## 3. Analyse STRIDE

### 3.1 Spoofing (usurpation d'identité)

| Menace | Vecteur | Actif ciblé | Probabilité | Impact | Contre-mesure |
|--------|---------|------------|-------------|--------|---------------|
| **S1** Fabrication de JWT | Valeur de repli `JWT_SECRET` connue (dans l'historique git) | Accès payant | ÉLEVÉE | CRITIQUE | Supprimer le repli, `JWT_SECRET` obligatoire au démarrage, archiver `v2/stripe/` |
| **S2** Vol de cookie de session | XSS sur le site, réseau non sécurisé, navigateur compromis | Comptes et sessions | MOYENNE | ÉLEVÉ | Cookie `__Host-session` avec `HttpOnly; Secure; SameSite=Lax`, CSP stricte, rotation après login |
| **S3** Réutilisation de lien magique | Interception d'email, shoulder surfing | Compte utilisateur | FAIBLE | ÉLEVÉ | Lien à usage unique, valable 15 min, hash stocké en base |
| **S4** Usurpation d'événement Stripe | Fausse requête POST au webhook | Paiement | FAIBLE (signature requise) | ÉLEVÉ | `stripe.Webhook.construct_event` avec tolérance de timestamp, idempotence |
| **S5** Confusion d'algorithme JWT (`alg: none`) | Token forgé avec `alg: none` | Accès payant | FAIBLE (implémentation actuelle) | CRITIQUE | Valider `alg` dans le header JWT avant toute vérification |

### 3.2 Tampering (altération)

| Menace | Vecteur | Actif ciblé | Probabilité | Impact | Contre-mesure |
|--------|---------|------------|-------------|--------|---------------|
| **T1** Injection SQL | Requêtes construites par concaténation | Base de données | FAIBLE (si ORM utilisé) | CRITIQUE | SQLAlchemy uniquement, Pydantic en mode strict, jamais de SQL par concaténation |
| **T2** Modification de `plan` dans la requête | Paramètre client `plan=c` sur un abonnement A | Accès payant, paiement | ÉLEVÉE (sans contrôle serveur) | ÉLEVÉ | Droits déduits uniquement du `price_id` Stripe côté serveur, jamais du client |
| **T3** Falsification du `price_id` lors du checkout | Paramètre client ou interception | Paiement | ÉLEVÉE (sans liste blanche) | ÉLEVÉ | Sessions Checkout créées côté serveur avec `price_id` issu d'une liste blanche config |
| **T4** Injection dans les prédictions | Données corrompues en entrée (Hub'Eau, VigiEau) | Réputation | MOYENNE (sources tierces) | ÉLEVÉ | Validation de schéma sur chaque source, marquage "fiabilité réduite" si donnée absente |
| **T5** Modification du code via supply chain CI | Tag Action GitHub modifié | Serveur, code, secrets | FAIBLE | CRITIQUE | Épingler toutes les Actions par SHA, Dependabot pour les mises à jour |
| **T6** Injection XSS → modification du DOM | Script CDN compromis (Tailwind, Leaflet, unpkg) | Utilisateurs, sessions | MOYENNE | ÉLEVÉ | SRI sur les scripts CDN, CSP stricte avec nonces |

### 3.3 Repudiation (répudiation)

| Menace | Vecteur | Actif ciblé | Probabilité | Impact | Contre-mesure |
|--------|---------|------------|-------------|--------|---------------|
| **R1** Contestation de paiement / accès | Utilisateur nie avoir souscrit ou résilié | Paiement, litiges | MOYENNE | MOYEN | Journal d'audit non modifiable : toute action sensible loggée (who, what, when, from_ip), factures Stripe conservées |
| **R2** Administrateur nie une action | Action admin non tracée | Données clients, abonnements | FAIBLE | ÉLEVÉ | Journal d'audit avec signature, impersonation loggée |
| **R3** Prévision fausse attribuée comme officielle | Affichage sans mention légale | Réputation | MOYENNE | TRÈS ÉLEVÉ | Mention obligatoire partout : "Estimation non officielle — modèle prédictif expérimental. Seuls les arrêtés préfectoraux font foi." |

### 3.4 Information Disclosure (divulgation d'information)

| Menace | Vecteur | Actif ciblé | Probabilité | Impact | Contre-mesure |
|--------|---------|------------|-------------|--------|---------------|
| **I1** Fuite des prédictions payantes via JSON statique | Données engagées dans `data/` ou GitHub Pages | Prédictions payantes | ÉLEVÉE (si mal configuré) | ÉLEVÉ | Seul un indicateur grossier (faible/modéré/élevé) dans les JSON publics, prédictions détaillées uniquement via l'API backend authentifiée |
| **I2** Enumération des comptes via messages d'erreur | Message différent selon que l'email existe ou non | Données personnelles | ÉLEVÉE | MOYEN | Message d'erreur identique : "Si un compte existe pour cet email, un lien vous a été envoyé" |
| **I3** Fuite de données personnelles via IDOR | Accès aux ressources d'une autre organisation en changeant un ID | Données personnelles, prédictions | ÉLEVÉE (sans contrôle) | TRÈS ÉLEVÉ | Toutes les requêtes DB filtrées par `org_id`, Row-Level Security PostgreSQL, tests IDOR automatisés |
| **I4** Logs contenant des données personnelles ou secrets | Journalisation excessive en prod | Données personnelles, secrets | MOYENNE | ÉLEVÉ | Aucune donnée personnelle ni secret dans les logs, niveau `INFO` uniquement en prod |
| **I5** Fuite via Swagger / OpenAPI | Endpoint de documentation exposé en prod | Tokens, structure API | ÉLEVÉE (si non désactivé) | MOYEN | Swagger et ReDoc désactivés en production |
| **I6** SSRF vers ressources internes | API externe malveillante ou paramètre URL contrôlé | Serveur, réseau interne | FAIBLE | ÉLEVÉ | Liste blanche des domaines externes autorisés (Hub'Eau, VigiEau, Stripe, Brevo, Open-Meteo), timeout strict |
| **I7** Scraping des données prédictives | Crawl massif de l'API publique | Prédictions, infrastructure | ÉLEVÉE | MOYEN | Rate limiting `slowapi`, clé API pour accès détaillé, quota par clé |

### 3.5 Denial of Service (déni de service)

| Menace | Vecteur | Actif ciblé | Probabilité | Impact | Contre-mesure |
|--------|---------|------------|-------------|--------|---------------|
| **D1** Flood de l'API publique / auth | Bots, concurrent malveillant | Serveur, disponibilité | ÉLEVÉE | MOYEN | Rate limiting progressif (`slowapi`), Cloudflare Bot Fight Mode + WAF |
| **D2** Flood du webhook Stripe | Requêtes POST massives | Backend | FAIBLE | MOYEN | Vérification de signature avant tout traitement ; Cloudflare WAF en amont |
| **D3** Génération de liens magiques en masse | Flood de `/auth/magic-link` | Serveur email (quota Brevo), disponibilité | ÉLEVÉE | MOYEN | Rate limiting strict sur le endpoint magic-link (par IP et par email), CAPTCHA si nécessaire |
| **D4** Épuisement des connexions DB | Requêtes lentes intentionnelles | Base de données | FAIBLE | ÉLEVÉ | Timeout sur toutes les requêtes, pool de connexions limité, requêtes paramétrées |
| **D5** Occupation du quota Open-Meteo | Scraping des prédictions sans compte | Quota API | MOYENNE | MOYEN | Cache des appels météo (ne pas interroger l'API à chaque requête), rate limiting API interne |

### 3.6 Elevation of Privilege (élévation de privilèges)

| Menace | Vecteur | Actif ciblé | Probabilité | Impact | Contre-mesure |
|--------|---------|------------|-------------|--------|---------------|
| **E1** Abonné A accédant aux fonctions C | Appel direct d'un endpoint `/api/v1/risque/` sans vérification de plan | Prédictions payantes | ÉLEVÉE (sans contrôle) | ÉLEVÉ | Chaque route déclare la formule minimale requise ; refus par défaut ; test IDOR A→C dans la CI |
| **E2** Utilisateur accédant aux ressources d'un autre `org_id` | Modification de l'ID dans la requête | Données multi-tenant | ÉLEVÉE (sans contrôle) | TRÈS ÉLEVÉ | Filtre `org_id` sur **toutes** les requêtes DB, Row-Level Security PostgreSQL |
| **E3** Utilisateur role `user` effectuant des actions `admin` | Appel direct d'un endpoint `/admin/` | Gestion des abonnements, comptes | ÉLEVÉE (sans contrôle) | ÉLEVÉ | Vérification du rôle sur chaque route protégée, refus par défaut |
| **E4** Contournement du TOTP admin | Brute-force du code TOTP | Compte admin, super-admin | FAIBLE | CRITIQUE | Blocage après 5 tentatives, délai exponentiel, alerte email |
| **E5** Compromission de la CI → push sur master | Workflow avec `contents: write`, Actions par tag mutable | Infrastructure, code | FAIBLE | TRÈS ÉLEVÉ | SHA sur les Actions, permissions minimales, protection de branche `master` (required reviews) |
| **E6** Clé API formule C utilisée au-delà du quota | Partage ou fuite de clé | Prédictions, quota | MOYENNE | MOYEN | Quota par clé, révocation immédiate possible, préfixe visible pour identification |

---

## 4. Flux de données sensibles et points de confiance

```
[Navigateur utilisateur]
        │ HTTPS uniquement
        │ Cookie __Host-session (HttpOnly, Secure, SameSite=Lax)
        ▼
[Cloudflare] ── WAF, rate limiting, Bot Fight Mode
        │ Cloudflare Tunnel (aucun port ouvert sur le VPS)
        ▼
[FastAPI / VPS OVH France]
        │ Validation Pydantic stricte sur toutes les entrées
        │ Vérification session en base, org_id sur toutes les requêtes
        │ CSP stricte, HSTS, X-Frame-Options, etc.
        ▼
[PostgreSQL] ── Row-Level Security, filtre org_id
        │
        ├── [Stripe] ── Webhook signé, idempotence, liste blanche price_id
        ├── [Brevo SMTP] ── Emails transactionnels uniquement, lien magique hashé
        └── [Hub'Eau / VigiEau / Open-Meteo] ── Liste blanche SSRF, timeout
```

**Frontières de confiance :**
- **Jamais faire confiance au client** pour le plan, le `price_id`, l'`org_id` ou le rôle.
- **Faire confiance à Stripe** uniquement après vérification de signature du webhook.
- **Ne jamais faire confiance aux sources externes** (Hub'Eau, VigiEau) pour le contenu structurel : valider le schéma à chaque ingestion.

---

## 5. Décision architecturale à valider : `/app/` vs sous-domaine

Le PLAN.md et les décisions validées indiquent `www.mon-environnement.fr/app/` sur le même domaine. Cette architecture nécessite de faire passer le DNS par Cloudflare pour aiguiller `/app/*` vers le VPS, puisque GitHub Pages ne peut pas router un sous-chemin vers un autre serveur.

### Option A : `www.mon-environnement.fr/app/` (décision validée)

**Via Cloudflare Tunnel + Worker de routage**

| Avantages | Inconvénients |
|-----------|---------------|
| Même domaine → cookies partagés sur tout le domaine | DNS **doit** passer par Cloudflare (orange cloud) → perte du CNAME direct GitHub Pages |
| Meilleur SEO (tout sur le même domaine) | Cloudflare devient un SPOF pour le site public |
| Pas de CORS entre site public et backend | Si Cloudflare tombe, tout tombe (site statique inclus) |
| Cloudflare Tunnel : aucun port ouvert sur le VPS | Complexité de routage (Worker Cloudflare ou Page Rule) |
| Bot Fight Mode, WAF, DDoS protection sur tout le trafic | Nécessite `SameSite=Lax` (pas `Strict`) pour les cookies inter-pages |

**Recommandation de mise en œuvre :** Cloudflare Tunnel (préféré à l'ouverture de ports). Le Worker de routage intercepte les requêtes vers `/app/*` et les redirige vers le Tunnel. GitHub Pages continue de servir le reste.

### Option B : `app.mon-environnement.fr` (sous-domaine)

| Avantages | Inconvénients |
|-----------|---------------|
| Séparation claire : GitHub Pages reste sur `www`, VPS sur `app` | CORS nécessaire entre `www` et `app` |
| Pas de Cloudflare obligatoire pour le site public | Cookies non partagés entre `www` et `app` (sécurité +, UX -) |
| Déploiement indépendant backend et frontend | Deux noms de domaine à gérer dans les mentions légales |
| SSL dédié pour le sous-domaine backend | Légèrement plus complexe pour l'utilisateur (URL différente) |

### Recommandation

**L'Option A (décision déjà validée) est la bonne** dans un contexte d'accès de grande consommation, à condition que :
1. Cloudflare Tunnel soit utilisé (pas d'ouverture de port).
2. Le routage soit fait via un Cloudflare Worker simple (pas de Page Rule, plus de contrôle).
3. La règle HSTS `includeSubDomains` soit vérifiée pour ne pas exposer de sous-domaines non HTTPS.

**→ Confirmes-tu le choix de `/app/` via Cloudflare Tunnel ? (Décision déjà validée, je vérifie avant de structurer le runbook de déploiement.)**

---

## 6. Questions ouvertes (PLAN.md)

Ces questions **ne bloquent pas la Phase 1**, mais doivent être résolues avant les phases suivantes indiquées :

| Réf. | Question | Bloquante à partir de |
|------|----------|-----------------------|
| **Q2** | Structure juridique (SIRET, forme, TVA) — nécessaire pour les mentions légales, les factures et le contrat Stripe live | Phase 3 (checkout Stripe live) |
| **Q3** | Email transactionnel : Brevo confirmé ou alternative (Mailjet, Postmark) ? — impact sur l'intégration SMTP du lien magique et des alertes | Phase 3 (lien magique) |

---

*Prochaine étape : Phase 2 — sécurisation de la CI et corrections de l'existant. En attente de ta validation.*
