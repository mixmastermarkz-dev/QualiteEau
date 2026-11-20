# PLAN.md — mon-environnement.fr

> **Statut :** Rédigé le 19/11/2026 — en attente de validation avant tout développement.
> Ce document décrit l'état actuel du projet, l'architecture cible et le plan d'exécution par étapes.

---

## 1. ÉTAT ACTUEL

### 1.1 Stack technique

| Couche | Technologie | Notes |
|--------|-------------|-------|
| Génération des pages | Python 3.11 (stdlib seule) | Aucune dépendance externe, `urllib.request` |
| Frontend | HTML5 + CSS3 + Vanilla JS | Tailwind CSS (CDN), Leaflet.js 1.9.4 (CDN) |
| Hébergement | GitHub Pages | CDN mondial, HTTPS automatique, zéro coût serveur |
| DNS / registrar | OVHcloud | CNAME → github.io |
| CI/CD | GitHub Actions | 5 workflows : update_data, update_national, ci, monitor_apis, deploy-worker |
| Edge (optionnel) | Cloudflare Worker (`workers/auth-gate/`) | Middleware JWT — déployé mais non actif |
| Monétisation | Google AdSense (ca-pub-6656990941754174) | Passif |
| Base de données | **Aucune** | Données en JSON (fichiers statiques) |
| Auth / comptes | **Aucune** | Site entièrement public |
| Paiement | **Aucun** (stub Stripe dans `v2/stripe/`) | Non intégré |

### 1.2 Architecture de génération des pages

```
APIs publiques (Hub'Eau, Atmo, VigiEau, Open-Meteo)
        │  quotidien 09h30–10h00 UTC (GitHub Actions)
        ▼
data_fetcher.py                 data_fetcher_national.py
(Hérault 34 + Gard 30)         (96 depts, 4 jobs parallèles)
        │                               │
        ▼                               ▼
full_data.json (1,9 Mo)         data/dept/XX.json (96 fichiers)
        │                               │
        ▼                               ▼
generate_pages.py               generate_dept_pages.py
~2 178 communes générées        97 pages département
eau-potable/{slug}/index.html   departement/{slug}/index.html
        │                               │
        └───────────────┬───────────────┘
                        ▼
                sitemap.xml (13 287 URLs)
                        │
                git commit + push → GitHub Pages
```

**Point critique :** `index.html` (page d'accueil) est un SPA JavaScript pur. Sans exécution JS, le contenu n'est pas visible. Les pages communes (`eau-potable/*/`) sont elles statiquement générées avec contenu HTML complet.

### 1.3 Sources de données

| Source | Usage | Version API | État (19/11/2026) |
|--------|-------|-------------|-------------------|
| Hub'Eau (BRGM/OFB/ARS) | Eau potable, rivières, nappes | v1 (potable, nappes), v2 (rivières) | **KO — maintenance BRGM** (retourne HTML) |
| Atmo Occitanie (ArcGIS) | Qualité de l'air, pollen, pesticides | WFS 2.0 / FeatureServer | OK |
| Atmo France (GeoServer) | Qualité de l'air national | WFS 2.0 | OK (lent : ~14 s) |
| VigiEau (MTECT) | Restrictions eau / sécheresse | REST JSON | OK |
| Open-Meteo | Température air en temps réel | v1 | OK |
| RNSA (via Atmo Occitanie) | Pollen (taxa) | FeatureServer | OK |

**Couverture actuelle :**
- Eau potable : France entière (via Hub'Eau, 96 depts)
- Qualité de l'air : Occitanie uniquement + national via Atmo France (sans détail communal)
- Pollen : Occitanie uniquement
- Rivières / nappes : France entière (via Hub'Eau)
- Restrictions eau : France entière (VigiEau)

### 1.4 Pages existantes

| Type | Nombre | Génération | SEO |
|------|--------|------------|-----|
| `eau-potable/{slug}/` | ~2 178 | Statique Python | ✅ HTML complet, Schema.org Dataset |
| `departement/{slug}/` | 97 | Statique Python | ✅ HTML complet |
| `index.html` | 1 | SPA client-only | ⚠ Contenu invisible sans JS |
| `contact.html` | 1 | Manuel | ✅ |
| `mentions-legales.html` | 1 | Manuel | ✅ |
| `politique-confidentialite.html` | 1 | Manuel | ✅ |

### 1.5 Défauts identifiés

**Fonctionnels :**
- `index.html` : "Chargement…" / "—" sans JS ; pas de fallback noscript
- 11 depts DOM-TOM (971-988) dans CITIES sans fichier JSON correspondant
- `get_json()` ne détectait pas les réponses HTML de maintenance (corrigé le 19/11/2026)
- Guard-fous incomplets : seules les rivières étaient protégées (corrigé le 19/11/2026)
- Données Hub'Eau gelées depuis mai 2026 (maintenance en cours)

**SEO / conformité :**
- Mention "100% Officiel" inexacte (formulation à corriger)
- Pas de page "Sources et méthodologie"
- RNSA absent du pied de page
- Popular cities = uniquement Occitanie sur un site présenté comme national
- Pas de balise `<noscript>` ni de rendu serveur pour `index.html`

**Légal / RGPD :**
- Éditeur individuel (Nicolas Berrux) — cohérent pour l'instant, à vérifier si structure juridique change avec l'offre payante
- Pas de bandeau cookies conforme (AdSense = cookies publicitaires = consentement obligatoire)
- Politique de confidentialité à mettre à jour pour l'offre collectivité (données personnelles des agents)

**Sécurité (site actuel, risque faible car statique) :**
- Aucun secret commité (✅)
- Aucune authentification ni donnée personnelle (✅ pour l'instant)
- CSP absente sur GitHub Pages (limitation de la plateforme)
- AdSense : JavaScript tiers sans intégrité SRI

---

## 2. ARCHITECTURE CIBLE

### 2.1 Vue d'ensemble

```
┌─────────────────────────────────────────────────────────────────┐
│                     COUCHE PUBLIQUE                             │
│                                                                 │
│  GitHub Pages / Cloudflare Pages                               │
│  ├─ index.html (SPA + fallback noscript)                       │
│  ├─ eau-potable/{slug}/ (HTML statique généré)                 │
│  ├─ departement/{slug}/ (HTML statique généré)                 │
│  └─ sources-et-methodologie.html, collectivites.html           │
└──────────────────────────────┬──────────────────────────────────┘
                               │ fetch JSON data
┌──────────────────────────────▼──────────────────────────────────┐
│                     COUCHE API / BACKEND                        │
│                                                                 │
│  FastAPI (Python 3.11+)  ─── PostgreSQL 16                     │
│  ├─ /api/v1/commune/{insee}  (données publiques)               │
│  ├─ /api/v1/restrictions/    (VigiEau enrichi)                 │
│  ├─ /api/v1/risque/{insee}   (indice prédictif, payant)        │
│  ├─ /auth/*                  (JWT, sessions collectivité)      │
│  ├─ /collectivite/*          (espace payant)                   │
│  └─ /admin/*                 (back-office super-admin)         │
│                                                                 │
│  Tâches planifiées (APScheduler ou Celery + Redis)             │
│  ├─ Fetch quotidien Hub'Eau, Atmo, VigiEau                     │
│  ├─ Import historique arrêtés sécheresse                       │
│  └─ Calcul indice prédictif                                    │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│                     SERVICES TIERS                              │
│  Stripe (paiements)  │  SMTP transactionnel (Brevo/Mailjet)    │
│  Hub'Eau / Atmo / VigiEau / BRGM / data.gouv.fr               │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Choix techniques proposés — à valider

#### Backend

**Proposition : FastAPI (Python) + PostgreSQL**

Justification :
- Python déjà utilisé pour tout le pipeline de données → pas de changement de langue
- FastAPI : moderne, async, typage strict, documentation OpenAPI auto-générée
- Idéal pour exposer le pipeline ML (scikit-learn / xgboost) directement
- PostgreSQL : standard, robuste, extensions utiles (TimescaleDB pour séries temporelles si besoin)
- Alternative rejetée : Django — plus de magie, overhead plus important pour une API ; Next.js — introduit Node.js, rupture de stack

**ORM : SQLAlchemy 2.x (async) + Alembic (migrations)**

#### Frontend (espace connecté)

**Proposition : Jinja2 (templates serveur) + HTMX + Alpine.js**

Justification :
- Le back-office et l'espace collectivité ne nécessitent pas un SPA complet
- Jinja2 + HTMX : dynamisme côté client sans build step, sans Node.js, cohérent avec FastAPI
- Alpine.js : réactivité légère (menus, formulaires), aucun build
- Résultat : stack 100% Python, maintenable seul
- Alternative rejetée : React/Next.js — over-engineering pour un usage B2G (business-to-government) avec une équipe d'une personne

**Site public :** Conserver la génération statique Python actuelle (elle fonctionne bien pour le SEO)

#### Hébergement

**Proposition : VPS OVHcloud (France, ISO 27001) — à valider**

Justification :
- Données potentiellement sensibles (agents de collectivités) → hébergement en France recommandé
- OVHcloud : déjà fournisseur DNS, facturation en euros, certifié SecNumCloud eligible
- VPS Start/Pro (~6-15 €/mois) ou offre dédiée selon charge
- Alternative : Scaleway (également France, RGPD natif)
- Alternative rejetée : Render/Railway — américains, CLOUD ACT applicable, moins adapté pour des clients publics français

**Site public :** Rester sur GitHub Pages (gratuit, CDN mondial, aucune raison de migrer)

**Base de données PostgreSQL :** Hébergée sur le même VPS (ou Supabase France si mutualisé)

#### Email transactionnel

**Proposition : Brevo (ex-Sendinblue, siège France) — à valider**

Justification : RGPD natif, siège France, plan gratuit généreux (300 emails/jour), API simple

#### Paiement

**Proposition : Stripe** (déjà mentionné dans v2/stripe/)

Justification :
- Standard de l'industrie, documentation excellente, webhooks fiables
- Support natif paiement par virement SEPA (important pour collectivités)
- Génération de factures via Stripe Billing
- **Mais :** pour les bons de commande/mandats administratifs, prévoir un flow manuel complémentaire

---

## 3. PLAN D'EXÉCUTION PAR ÉTAPES

### Étape 0 — Fondations backend (prérequis pour étapes 2-5)

**Durée estimée :** 1-2 semaines
**Objectif :** Mettre en place l'infrastructure backend sans casser le site actuel.

Tâches :
- [ ] Créer le projet FastAPI dans `backend/`
- [ ] Configurer PostgreSQL, SQLAlchemy, Alembic
- [ ] Variables d'environnement (`.env.example` sans valeurs, `.env` dans `.gitignore`)
- [ ] Environnements : `dev` (local), `staging` (VPS test), `prod` (VPS prod)
- [ ] Docker Compose pour le développement local (Python + PostgreSQL + Redis)
- [ ] CI : lint (Ruff), tests (pytest), scan secrets (gitleaks), Semgrep

**Question à valider :** VPS OVHcloud ou autre hébergeur ?

---

### Étape 1 — Correctifs du site actuel

**Durée estimée :** 1 semaine
**Peut démarrer sans le backend.**

#### 1.1 Rendu et SEO (index.html)
- [ ] Fallback `<noscript>` sur `index.html` : liste des villes populaires avec liens directs vers `/eau-potable/{slug}/`
- [ ] Statistiques dans le HTML (pas seulement JS) : "2 178 communes · 96 départements · 6 sources officielles"
- [ ] `<title>` et `<meta description>` sur index.html optimisés (national, pas seulement Occitanie)
- [ ] Popular cities : remplacer par une sélection nationale (Paris, Lyon, Marseille, Toulouse, Montpellier, Bordeaux, Nantes, Strasbourg, Lille, Rennes)

#### 1.2 Cohérence périmètre national
- [ ] Supprimer la mention "Occitanie" des textes génériques d'`index.html`
- [ ] Afficher la couverture réelle par module (eau : France entière ; air : Occitanie + données nationales sans détail communal ; pollen : Occitanie)
- [ ] Créer des fichiers JSON placeholder pour DOM-TOM (971-988) pour éviter le fallback sur `full_data.json`

#### 1.3 Contenus et sources
- [ ] Remplacer "100% Officiel" par "Données issues de sources publiques officielles (ARS, BRGM, Atmo, VigiEau)"
- [ ] Ajouter RNSA dans le pied de page et dans les mentions légales
- [ ] Créer `sources-et-methodologie.html` : tableau des sources, fréquence de mise à jour, liens officiels, explication des scores
- [ ] Mettre à jour `sitemap.xml` avec la nouvelle page

#### 1.4 Légal et RGPD
- [ ] Bandeau de consentement aux cookies (AdSense = consentement requis RGPD) — solution légère sans framework externe (banner HTML/CSS/JS natif avec `localStorage`)
- [ ] Mettre à jour `politique-confidentialite.html` : ajouter section sur le futur espace collectivité
- [ ] Vérifier les mentions légales (éditeur, hébergeur, SIRET si structure juridique)

#### 1.5 Accessibilité (WCAG AA — critique)
- [ ] Audit : attributs `alt` sur toutes les images/icônes SVG
- [ ] Labels explicites sur les `<input>` de recherche
- [ ] Ordre de focus logique au clavier
- [ ] Contrastes couleurs : vérifier les badges de score (vert/orange/rouge sur blanc)
- [ ] `lang="fr"` sur `<html>` ✅ (déjà présent — à vérifier)
- [ ] `aria-label` sur les graphiques canvas (sparklines nappes)
- [ ] `role="status"` sur le div "Chargement des données…"

#### 1.6 Performance (Lighthouse)
- [ ] Différer le chargement de Leaflet.js (non critique au premier rendu)
- [ ] Préconnexion aux CDN : `<link rel="preconnect" href="https://cdn.tailwindcss.com">`
- [ ] Compression gzip/brotli (GitHub Pages la fournit nativement)
- [ ] Image Open Graph : optimiser og:image (servir depuis un CDN ou générer en base64)

**État d'erreur propre (déjà commencé, à finaliser) :**
- [ ] Bannière de fraîcheur des données ✅ (faite le 19/11/2026)
- [ ] Message d'erreur par module si données absentes ✅ (commencé)

---

### Étape 2 — Module restrictions d'eau (France entière)

**Prérequis :** Étape 0 (backend), ou variante légère (JSON statique enrichi) si backend non encore déployé.
**Durée estimée :** 2 semaines.

#### 2.1 Affichage VigiEau sur les fiches communes
VigiEau est **déjà appelé** dans `data_fetcher.py` (champ `restric` dans les données potable). Il s'agit d'enrichir l'affichage :
- [ ] Afficher : zone d'alerte, niveau (vigilance/alerte/alerte renforcée/crise), type d'eau (AEP, SUP, SOU), profil (particulier/collectivité/exploitation)
- [ ] Lien vers le PDF de l'arrêté préfectoral (disponible via l'API VigiEau)
- [ ] Message légal : "Seules les publications légales des préfectures font foi"

#### 2.2 Historique des arrêtés (data.gouv.fr)
- [ ] Importer les fichiers CSV annuels "Arrêtés YYYY" et "Zones d'alerte" depuis data.gouv.fr
- [ ] Schéma DB : `arrete_secheresse(id, annee, dept, zone_alerte, date_debut, date_fin, niveau, type_eau)` + `zone_alerte(id, code, nom, geometry)`
- [ ] Tâche planifiée : vérification hebdomadaire des nouveaux fichiers sur data.gouv.fr

#### 2.3 Frise historique
- [ ] Par commune : calculer les jours sous restriction par an (2015-2026), niveau maximal atteint
- [ ] Affichage : mini-calendrier annuel coloré (type "GitHub contributions heatmap")

#### 2.4 Cache et résilience
- [ ] Cache Redis (ou cache fichier JSON simple si Redis non disponible) sur les appels VigiEau
- [ ] Tâche quotidienne à 06h00 UTC (avant le monitoring de 07h00)
- [ ] Fallback : si VigiEau indisponible, afficher la dernière donnée cachée avec sa date

---

### Étape 3 — Indice prédictif de risque

**Prérequis :** Étape 2 (historique arrêtés en base).
**Durée estimée :** 3-4 semaines (data science incluse).

#### 3.1 Pipeline de données (par zone d'alerte)
Croisement de 5 signaux :
1. **Tendance piézométrique** : Hub'Eau `niveaux_nappes/chroniques` → variation sur 30/90 jours
2. **Débit des cours d'eau** : Hub'Eau `v2/hydrometrie/observations_tr` (API fonctionnelle, différente de la maintenance BRGM)
3. **Précipitations** : Open-Meteo `forecast` ou `archive` (données historiques gratuites)
4. **Saisonnalité** : mois de l'année, percentile de la période
5. **Historique arrêtés** : combien de fois cette zone a-t-elle été en restriction ce mois-ci les années précédentes

#### 3.2 Modèle
- [ ] v1 : règles heuristiques (baseline interprétable, pas de ML)
  - Ex. : si nappe < P20 ET débit < P20 ET mois dans [mai-septembre] → risque élevé
- [ ] v2 : régression logistique ordonnée (4 classes : aucun/vigilance/alerte/crise)
- [ ] v3 (optionnel) : Gradient Boosting (XGBoost) si données suffisantes
- **Priorité calibration :** Occitanie (données disponibles, terrain connu)
- **Sortie :** probabilité de passage à chaque niveau à 2, 4 et 8 semaines + top-3 facteurs explicatifs (SHAP)

#### 3.3 Backtest obligatoire avant mise en production
- [ ] Période : 2015-2025, hors-échantillon (train sur 2015-2022, test sur 2023-2025)
- [ ] Métriques : précision, rappel, F1 par niveau, Brier score
- [ ] Rapport `BACKTEST_REPORT.md` versionné avec le modèle
- **Règle :** aucune mise en production sans rapport de backtest validé

#### 3.4 Versionnement des modèles
- [ ] Table DB : `model_version(id, version, algorithm, trained_at, backtest_precision, backtest_recall, is_active)`
- [ ] Table DB : `prediction(id, model_version_id, zone_alerte_id, date_pred, horizon_weeks, proba_vigilance, proba_alerte, proba_renforcee, proba_crise, top_factors_json)`
- [ ] Comparaison prédiction vs arrêté réel (rétrospectivement)

#### 3.5 Interface
- [ ] Mention visible : "Estimation non officielle – modèle prédictif expérimental"
- [ ] Indicateur de confiance (si données source incomplètes → fiabilité réduite)

---

### Étape 4 — Espace collectivité (payant)

**Prérequis :** Étapes 0, 2, 3.
**Durée estimée :** 4-6 semaines.

#### 4.1 Modèle de données

```
organisation (id, nom, siret, type, nb_sieges, plan, stripe_customer_id, created_at, suspended_at)
user (id, org_id, email, role[admin|user], hashed_pw, totp_secret, last_login, deactivated_at)
abonnement (id, org_id, stripe_subscription_id, type[stripe|bon_de_commande], statut, date_debut, date_fin)
commune_suivi (org_id, code_insee, alert_seuil_niveau)
pilote (id, org_id, date_debut, date_fin, converted_at)
alerte_envoyee (id, org_id, user_id, commune_insee, niveau, canal[email|sms], sent_at)
facture (id, org_id, stripe_invoice_id, montant_ht, tva, statut, pdf_url, created_at)
```

#### 4.2 Authentification et sécurité
- [ ] Argon2id pour les mots de passe
- [ ] TOTP (MFA) obligatoire pour admin collectivité, proposé aux utilisateurs
- [ ] Rate limiting : 5 tentatives de connexion → blocage progressif 1min, 5min, 15min
- [ ] Sessions : cookie HttpOnly, Secure, SameSite=Strict, rotation après login
- [ ] Invitation par jeton signé (durée 72h, usage unique)

#### 4.3 Tableau de bord collectivité
- [ ] Communes et zones suivies, carte de France des niveaux actuels
- [ ] Indice de risque par commune (sortie modèle étape 3)
- [ ] Comparaison année en cours vs N-1, N-2
- [ ] Alertes configurables par niveau seuil et canal (email, SMS via Brevo)

#### 4.4 Rapports et exports
- [ ] PDF mensuel (via WeasyPrint ou pdfkit) : tableau de bord, prévisions, historique
- [ ] Kit communication : modèles de messages aux habitants par niveau (Markdown éditable)

#### 4.5 Paiements
- [ ] Stripe Checkout pour l'abonnement en ligne
- [ ] Flow manuel : bon de commande → abonnement créé par admin → virement SEPA
- [ ] Génération de factures conformes (numérotation séquentielle, mentions légales, SIRET, TVA)
- [ ] Webhooks Stripe signés (vérification `stripe.Webhook.construct_event`)

#### 4.6 Pages publiques
- [ ] `/collectivites` : présentation de l'offre, formulaire de demande de démo
- [ ] Comptes pilotes (30 jours gratuits, convertibles en abonnement)

---

### Étape 5 — Back-office super-admin

**Prérequis :** Étape 4.
**Durée estimée :** 3-4 semaines.

#### 5.1 Accès et sécurité
- [ ] Route `/admin/` isolée, MFA obligatoire, session courte (2h), nouvelle auth pour actions sensibles
- [ ] Restriction par IP (liste blanche configurable via `.env`)
- [ ] Journal d'audit : chaque action loggée (who, what, when, from_ip), non modifiable

#### 5.2 Tableau de bord
- MRR, clients actifs, pilotes en cours / arrivant à échéance, alertes envoyées, état des sources

#### 5.3 Gestion
- Collectivités (CRUD + suspend + suppr. logique), utilisateurs (CRUD + forcer reset MFA), abonnements, factures, pilotes, CRM démo

#### 5.4 Impersonation
- [ ] Mode "voir en tant que" (lecture seule), journalisé, **bannière visible dans l'interface impersonnée**

#### 5.5 Supervision des données
- [ ] Date de dernière mise à jour par source, erreurs d'import, relance manuelle

#### 5.6 Modèle prédictif
- [ ] Version en production, derniers scores de backtest, comparaison prédictions vs arrêtés

#### 5.7 Contenus
- [ ] Éditeur Markdown pour pages publiques (FAQ, méthodologie, page Collectivités) et modèles d'email

#### 5.8 Exports CSV
- [ ] Clients, utilisateurs, factures

#### 5.9 Script création super-admin
- [ ] `python backend/scripts/create_superadmin.py` — interactif, hache le mot de passe, aucun compte par défaut

---

### Étape 6 — Sécurité (transversale)

**À faire en parallèle de chaque étape.**

#### Audit initial (Étape 1)
- [ ] `SECURITY_AUDIT.md` : secrets dans le code/historique git (gitleaks), dépendances vulnérables (pip-audit / Safety), configuration GitHub Pages (CSP impossible → documenter la limitation)
- [ ] `.github/dependabot.yml` : mises à jour automatiques des dépendances Python + Actions

#### CI/CD sécurité (Étape 0)
- [ ] gitleaks (scan secrets dans les commits)
- [ ] Semgrep (analyse statique OWASP)
- [ ] pip-audit (dépendances vulnérables)
- [ ] Tests IDOR automatisés dès l'étape 4

#### Hardening backend (Étape 0+)
- [ ] CSP stricte sur les routes FastAPI (nonces, pas d'unsafe-inline)
- [ ] Headers : HSTS, X-Content-Type-Options, Referrer-Policy, Permissions-Policy, X-Frame-Options (frame-ancestors 'none')
- [ ] CORS restreint à l'origine du site public
- [ ] Rate limiting : `slowapi` sur les routes API publiques et auth
- [ ] SSRF : liste blanche des domaines externes autorisés
- [ ] Validation stricte avec Pydantic v2 sur toutes les entrées

#### Pré-production (avant ouverture payante)
- [ ] Scan OWASP ZAP en préproduction (automatisé dans CI)
- [ ] **Rappel :** commander un pentest externe avant la mise en production de l'offre payante

#### Documents
- [ ] `SECURITY.md` : procédure de signalement des vulnérabilités (Responsible Disclosure)
- [ ] `/.well-known/security.txt`

---

## 4. QUESTIONS À VALIDER AVANT DE DÉMARRER

Ces décisions sont structurantes ou difficiles à revenir en arrière. Je n'écris pas une ligne de code avant ta réponse sur chacune.

### Q1 — Hébergement backend
> **Proposition :** VPS OVHcloud France (Sandbox ou Start, ~6-15 €/mois), hébergement des données en France, certifiable SecNumCloud.
> **Alternative :** Scaleway Paris, Hetzner Falkenstein (Allemagne, RGPD conforme mais hors France).
> **Contrainte :** GitHub Pages reste pour le site public (pas de changement).
>
> **→ Quelle option ? As-tu déjà un VPS ou une préférence d'hébergeur ?**

### Q2 — Structure juridique
> **Actuellement :** Nicolas Berrux, éditeur individuel.
> **Avec l'offre payante :** facturer des collectivités nécessite probablement une structure (auto-entrepreneur, SASU, SAS…) avec un SIRET et une TVA.
>
> **→ As-tu déjà une structure juridique ou est-ce à prévoir ? (Impact sur les mentions légales, les factures, le contrat Stripe.)**

### Q3 — Email transactionnel
> **Proposition :** Brevo (siège France, RGPD natif, 300 emails/jour gratuits).
> **Alternative :** Mailjet (France), Postmark (US), AWS SES (US).
>
> **→ Brevo convient-il ? As-tu déjà un compte ou une préférence ?**

### Q4 — Domaine de l'espace connecté
> Options :
> - `app.mon-environnement.fr` (sous-domaine → séparation claire, SSL dédié)
> - `www.mon-environnement.fr/app/` (même domaine → cookies partagés, plus simple)
> - `admin.mon-environnement.fr` (back-office séparé)
>
> **→ Quelle structure de domaines souhaites-tu ?**

### Q5 — SMS (optionnel pour les alertes collectivité)
> **Si alertes SMS :** Brevo SMS, Twilio, ou OVHcloud SMS.
> **Si non :** email uniquement dans un premier temps.
>
> **→ SMS dès l'ouverture, ou post-MVP ?**

### Q6 — Tarification de l'offre collectivité
> Le back-office permet de gérer les formules et tarifs dynamiquement.
> **Mais le modèle de données (nombre de sièges, fonctionnalités par plan) doit être décidé dès l'étape 4.**
> Exemple de structure simple :
> - Pilote (30 jours gratuit, 1-5 communes, email uniquement)
> - Starter (X €/mois, 5 communes, 3 utilisateurs, email)
> - Pro (Y €/mois, communes illimitées, 10 utilisateurs, email + SMS, PDF)
>
> **→ As-tu une idée des niveaux de plans et de la fourchette de prix ? (Peut être affiné, mais besoin d'une base pour modéliser la DB.)**

### Q7 — Périmètre initial du prédictif (Étape 3)
> **Proposition :** Calibration sur Occitanie uniquement (zones d'alerte connues, terrain familier), puis extension nationale.
> **Alternative :** France entière dès le départ (plus de données, moins de biais régional, mais moins de maîtrise du contexte local).
>
> **→ Occitanie first, ou national d'emblée ?**

### Q8 — Priorité d'exécution
> L'ordre proposé est : 0 → 1 → 2 → 3 → 4 → 5 → 6 (en parallèle).
> **Alternative possible :** Commencer par l'étape 1 (correctifs, aucun backend) pendant que tu valides les choix d'infra pour l'étape 0.
>
> **→ Veux-tu que je commence par l'étape 1 (zero backend, déployable immédiatement) pendant la validation des questions ci-dessus ?**

---

## 5. RISQUES ET DÉPENDANCES

| Risque | Probabilité | Impact | Mitigation |
|--------|-------------|--------|------------|
| Hub'Eau maintenance prolongée | Élevée (en cours) | Élevé | Guard-fous actifs, monitoring quotidien, bannière UI |
| Modèle prédictif insuffisamment précis | Moyenne | Élevé | Backtest obligatoire, règle "pas de prod sans rapport" |
| Collectivités exigent hébergement souverain | Moyenne | Moyen | OVHcloud France (SecNumCloud eligible) dès l'étape 0 |
| Fraude / abus de l'espace collectivité | Faible | Élevé | MFA obligatoire, rate limiting, isolation multi-tenant |
| Données personnelles fuites (étape 4) | Faible | Très élevé | Chiffrement au repos, audit IDOR systématique |
| Dépendance Stripe pour encaissement | Élevée | Moyen | Flow de secours virement SEPA + bon de commande manuel |
| Charge technique pour une seule personne | Élevée | Élevé | Avancer une étape à la fois, MVP par étape |

---

## 6. FEUILLE DE ROUTE CONDENSÉE

```
Sem 1-2  │ Étape 1 (correctifs site actuel — aucun backend requis)
         │ Étape 6 : audit initial + CI sécurité
         │
Sem 3-4  │ Étape 0 (fondations backend — VPS + FastAPI + PostgreSQL)
         │
Sem 5-6  │ Étape 2 (restrictions eau — VigiEau enrichi + historique)
         │
Sem 7-10 │ Étape 3 (indice prédictif — pipeline + modèle + backtest)
         │
Sem 11-16│ Étape 4 (espace collectivité — auth + dashboard + paiements)
         │
Sem 17-20│ Étape 5 (back-office super-admin)
         │
         ▼ Pentest externe → mise en production de l'offre payante
```

---

*Document mis à jour automatiquement — toute modification doit être commitée avec un message clair.*
