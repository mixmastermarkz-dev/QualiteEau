# SECURITY_AUDIT.md — mon-environnement.fr

> **Date :** 2026-11-21
> **Branche :** feature/predictif-abonnements
> **Périmètre :** audit lecture seule, aucune modification de code
> **Méthode :** revue manuelle + `git grep` sur tout l'historique accessible
> **Priorité :** CRITIQUE > HAUTE > MOYENNE > FAIBLE > INFO

---

## 1. Secrets dans le code et l'historique Git

### 1.1 Valeur de repli JWT_SECRET — CRITIQUE

**Fichier :** `v2/stripe/webhook.py` ligne 142
**Présent dans :** tout l'historique visible (au moins 10 commits, jusqu'au premier commit accessible)

```python
secret = os.environ.get("JWT_SECRET", "dev-secret-change-in-production")
```

**Impact :** si `JWT_SECRET` n'est pas définie sur le serveur de production, n'importe qui connaissant le code source (repo public ou futur fork) peut fabriquer un JWT valide signé avec cette valeur et obtenir un accès payant. Cette valeur est **définitivement dans l'historique Git** ; même si le code est corrigé, elle reste accessible via `git log`.

**Action requise (Phase 2) :**
- Remplacer le repli par une erreur explicite au démarrage (`raise RuntimeError`).
- Archiver `v2/stripe/` dans `legacy/` avec une note de sécurité.
- **Ne pas réécrire l'historique** (risque de confusion sur un dépôt partagé) mais documenter que cette valeur de repli est compromise et ne doit jamais être utilisée.

### 1.2 ID AdSense dans index.html — INFO

`ca-pub-6656990941754174` est présent en clair dans `index.html`. C'est attendu (côté client, public), mais il doit être protégé contre le détournement de compte AdSense (activation du système de liste blanche des domaines autorisés dans la console AdSense).

### 1.3 Aucune clé Stripe live ou de vrai secret trouvé

Aucune clé `sk_live_*`, `pk_live_*`, `sk_test_*` ni secret SMTP/API tiers dans le code ou l'historique scanné. Bon point.

---

## 2. Dépendances vulnérables

### 2.1 Dépendances Python — pas de fichier requirements.txt ni requirements pinned

Le projet de génération n'a aucune dépendance externe (stdlib uniquement). Mais :

| Dépendance | Fichier | Version épinglée | Risque |
|-----------|---------|-----------------|--------|
| `stripe` | `v2/stripe/webhook.py` | Non (`try: import stripe`) | MOYENNE — version inconnue, potentiellement obsolète |
| `pandas` | `backend/predictif/prototype/secheresse_34.py` | Non (`import pandas as pd`) | MOYENNE |
| `numpy` | idem | Non | MOYENNE |
| `scikit-learn` | idem | Non | MOYENNE |
| `requests` | idem | Non | MOYENNE |

**Conséquence :** `pip install` sans version épinglée peut introduire une version avec une CVE connue. `pip-audit` est absent de la CI.

**Action requise (Phase 2) :** créer `requirements.txt` (ou `pyproject.toml`) avec versions épinglées pour chaque composant ; ajouter `pip-audit` à la CI.

### 2.2 Dépendances JavaScript (CDN) — HAUTE

| Script | URL | SRI | Risque |
|--------|-----|-----|--------|
| Tailwind CSS | `https://cdn.tailwindcss.com` | Absent | HAUTE — CDN dynamique, impossible de fixer un hash SRI |
| Leaflet CSS | `https://unpkg.com/leaflet@1.9.4/dist/leaflet.css` | Absent | MOYENNE |
| Leaflet JS | `https://unpkg.com/leaflet@1.9.4/dist/leaflet.js` | Absent | HAUTE |
| Google Fonts | `https://fonts.googleapis.com/css2?family=Inter…` | Absent | FAIBLE (CSS seul) |
| AdSense | `https://pagead2.googlesyndication.com/…/adsbygoogle.js` | Absent | HAUTE (tiers pub) |

Un attaquant compromettant unpkg.com ou cdn.tailwindcss.com pourrait injecter du code JavaScript arbitraire sur toutes les pages.

**Action requise (Phase 5) :** remplacer le CDN Tailwind par du CSS compilé (`tailwindcss` CLI) ; ajouter des attributs `integrity=` sur Leaflet et les autres scripts versionnés ; AdSense ne peut pas avoir de SRI (fichier dynamique).

---

## 3. Workflows GitHub Actions

### 3.1 Actions référencées par tag, pas par SHA — HAUTE

Toutes les actions utilisées sont référencées par tag mutable (`@v4`, `@v5`, `@v3`). Un tag peut être modifié pour pointer vers un commit malveillant (attaque supply chain) sans aucun avertissement.

| Workflow | Action | Tag actuel | SHA requis |
|----------|--------|-----------|------------|
| Tous | `actions/checkout` | `@v4` | Doit être épinglé |
| Tous | `actions/setup-python` | `@v5` | Doit être épinglé |
| `update_national.yml` | `actions/upload-artifact` | `@v4` | Doit être épinglé |
| `update_national.yml` | `actions/download-artifact` | `@v4` | Doit être épinglé |
| `deploy-worker.yml` | `cloudflare/wrangler-action` | `@v3` | Doit être épinglé |

**Action requise (Phase 2) :** remplacer chaque `@vN` par son SHA complet (avec le tag en commentaire).

### 3.2 Permissions non restreintes dans ci.yml — HAUTE

`ci.yml` n'a pas de section `permissions:`. Il hérite des permissions par défaut du dépôt. Si le dépôt est configuré avec `contents: write` par défaut, un PR de fork déclenche un workflow avec accès en écriture au repo.

**Action requise (Phase 2) :** ajouter `permissions: contents: read` au niveau du workflow `ci.yml`, et `permissions: {}` (aucune) au niveau de chaque job qui ne nécessite pas d'écriture.

### 3.3 Workflow deploy-worker.yml déclenché sur master — MOYENNE

`deploy-worker.yml` se déclenche sur tout push vers `master` sur le chemin `workers/auth-gate/**`. Il expose `CLOUDFLARE_API_TOKEN`. Tant que le Worker n'est pas actif et le DNS ne passe pas par Cloudflare, le risque est contenu, mais tout commit accidentel dans `workers/auth-gate/` depuis master déclencherait un déploiement.

**Action requise (Phase 2) :** ajouter `permissions: {}` et restreindre à `workflow_dispatch` uniquement tant que le Worker reste en développement.

### 3.4 Secrets SMTP exposés dans monitor_apis.yml — MOYENNE

`monitor_apis.yml` injecte `ALERT_SMTP_PASS` et autres secrets SMTP dans un job. Ces secrets ne sont pas exposés à des workflows de fork (déclenchement par schedule uniquement), mais ils sont présents dans les logs en cas d'erreur d'impression accidentelle.

**Action requise :** s'assurer que `monitor_apis.py` ne journalise jamais les variables d'environnement.

### 3.5 Absences dans la CI — HAUTE

Les outils de sécurité suivants sont **absents** de la CI :
- `gitleaks` : détection de secrets dans les commits
- `pip-audit` : vulnérabilités dans les dépendances Python
- `semgrep` (règles OWASP + Python)
- `bandit` : analyse statique de sécurité Python
- `pytest` : aucun test automatisé

---

## 4. Failles dans v2/stripe/webhook.py

Toutes les failles listées dans le PROMPT sont confirmées :

### 4.1 Cookie posé dans la réponse à Stripe — CRITIQUE (fonctionnel)

`_handle_checkout()` appelle `req_handler._respond(200, ..., {"Set-Cookie": cookie})`. Cette réponse est envoyée à **Stripe** (le serveur webhook), pas au navigateur de l'utilisateur. Le cookie ne sera jamais posé dans le navigateur du client. L'authentification est **totalement brisée** : aucun utilisateur ne peut se connecter via ce flux.

### 4.2 Plan codé en dur — HAUTE

```python
"plan": "premium",
```

Peu importe la formule souscrite (A, B, C), tout le monde reçoit `plan=premium`. Aucune distinction entre formules possible.

### 4.3 Vérification de signature sans tolérance horodatage — HAUTE

La fonction `_verify_stripe_signature()` vérifie la signature mais **n'applique pas la tolérance de 300 secondes** recommandée par Stripe sur le timestamp `t`. Un événement capturé et rejoué des heures plus tard serait accepté si la signature est valide.

### 4.4 Absence d'idempotence — HAUTE

Aucune table de déduplication des événements. Si Stripe renvoie un événement (ce qu'il fait en cas de non-réponse 2xx dans les délais), il est traité deux fois.

### 4.5 JWT_SECRET avec valeur de repli — CRITIQUE

Voir §1.1. Valeur de repli `"dev-secret-change-in-production"` présente dans tout l'historique git. Si la variable n'est pas définie en prod, le secret est connu publiquement.

---

## 5. Failles dans workers/auth-gate/

### 5.1 Routes protégées inexistantes — HAUTE

```javascript
const isPremium = path.startsWith('/france/') && path.includes('/detail');
```

Ces URLs n'existent pas dans l'architecture actuelle (les pages communes sont sous `/eau-potable/{slug}/`). Le Worker ne protège donc rien en pratique.

### 5.2 Vérification d'un seul plan — MOYENNE

```javascript
if (payload.plan !== 'premium') { return redirectToPremium(url, 'plan'); }
```

Avec 3 formules A/B/C, cette logique doit gérer plusieurs niveaux de droits par route.

### 5.3 Worker non actif — INFO

Le DNS ne passe pas par Cloudflare ; le Worker n'intercepte aucune requête. Le risque est actuellement nul mais le code ne doit pas être considéré comme sécurisé pour un déploiement futur.

### 5.4 Absence de vérification de l'algorithme JWT — HAUTE

`verifyJWT()` utilise `crypto.subtle.importKey` avec `{ name: 'HMAC', hash: 'SHA-256' }` sans valider l'en-tête JWT. Un attaquant pourrait tenter de soumettre un token avec `"alg": "none"` ou `"alg": "RS256"` (avec sa propre clé publique) si l'implémentation était différente. L'implémentation actuelle est relativement sûre grâce à l'usage de `crypto.subtle.verify`, mais la valeur de `alg` dans le header n'est jamais contrôlée.

---

## 6. En-têtes HTTP actuels

GitHub Pages ne permet pas de configurer des en-têtes HTTP personnalisés (limitation de la plateforme).

| En-tête | Présent | Source | Remarque |
|---------|---------|--------|----------|
| `Strict-Transport-Security` | Oui | GitHub Pages | Fourni automatiquement |
| `X-Content-Type-Options: nosniff` | Oui | GitHub Pages | Fourni automatiquement |
| `Content-Security-Policy` | **Non** | — | Impossible sur GitHub Pages |
| `Referrer-Policy` | **Non** | — | Absent |
| `Permissions-Policy` | **Non** | — | Absent |
| `X-Frame-Options` / `frame-ancestors` | **Non** | — | Absent → risque de clickjacking théorique |
| `Cross-Origin-Opener-Policy` | **Non** | — | Absent |

**Conséquence :** sans CSP, toute injection XSS (via un script tiers compromis ou une faille future) peut exécuter du JS arbitraire sans restriction. Cette limitation est **inhérente à GitHub Pages** et ne peut être corrigée qu'en faisant passer le trafic par un reverse proxy (Cloudflare Worker ou équivalent).

**Action (Phase 3 / backend) :** les routes sous `/app/` (backend FastAPI) devront impérativement servir tous les en-têtes listés. Pour le site statique, envisager un Cloudflare Worker de réécriture d'en-têtes (faible coût).

---

## 7. JavaScript tiers et bandeau cookies

### 7.1 AdSense chargé avant consentement — HAUTE (non conforme RGPD/CNIL)

Dans `index.html`, ligne 39 :
```html
<script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-6656990941754174" crossorigin="anonymous"></script>
```

Ce `<script>` est dans le `<head>`, **avant tout consentement**. Les tentatives de mise en pause via `window['adsbygoogle']={paused:true}` (lignes 694 et 701) ne suffisent pas : le script AdSense est déjà téléchargé et peut déposer des cookies avant que le code de pause s'exécute.

**Exigence CNIL :** les cookies publicitaires nécessitent un consentement préalable explicite (opt-in). Le bouton "Refuser" doit être aussi visible que "Accepter".

**Action requise (Phase 5) :** charger AdSense uniquement après consentement explicite, via injection dynamique du `<script>` (pas d'ajout dans le HTML initial).

### 7.2 Absence de SRI sur les scripts CDN

Voir §2.2. Aucun attribut `integrity=` sur les scripts CDN.

---

## 8. Prototype secheresse_34.py — Licence Open-Meteo

**Fichier :** `backend/predictif/prototype/secheresse_34.py`

Open-Meteo API gratuite est **réservée à l'usage non commercial** (licence CC BY 4.0). Un service payant comme l'offre B/C utilisant les données Open-Meteo sans abonnement commercial violerait les conditions d'utilisation.

**Action requise (Phase 4) :** isoler la source météo derrière une interface (voir section §4 du PROMPT), implémenter deux backends (Open-Meteo commercial avec clé, et Météo-France données ouvertes).

---

## 9. Fichier .gitignore — état

Le `.gitignore` couvre correctement :
- `.env` ✅
- `.dev.vars` ✅
- `workers/auth-gate/.dev.vars` ✅
- `donnees_34/` ✅ (données prédictives payantes)

**Lacune :** `*.pem` (certificats/clés privées) n'est pas dans le `.gitignore`. À ajouter avant tout déploiement HTTPS manuel.

---

## 10. Résumé des priorités

| Priorité | Élément | Phase |
|----------|---------|-------|
| 🔴 CRITIQUE | `JWT_SECRET` avec repli dans le code et l'historique | Phase 2 |
| 🔴 CRITIQUE | Cookie de session posé dans la réponse à Stripe (auth brisée) | Phase 2/3 |
| 🟠 HAUTE | Absence de gitleaks, pip-audit, semgrep, bandit dans la CI | Phase 2 |
| 🟠 HAUTE | Actions GitHub Actions référencées par tag mutable | Phase 2 |
| 🟠 HAUTE | Permissions `ci.yml` non restreintes | Phase 2 |
| 🟠 HAUTE | Absence de SRI sur scripts CDN tiers | Phase 5 |
| 🟠 HAUTE | AdSense chargé avant consentement (non conforme RGPD/CNIL) | Phase 5 |
| 🟠 HAUTE | Absence de vérification d'alg dans le Worker JWT | Phase 2 |
| 🟠 HAUTE | Rejoue d'événements Stripe possible (pas de tolérance timestamp) | Phase 2/3 |
| 🟠 HAUTE | Dépendances Python non épinglées | Phase 2 |
| 🟡 MOYENNE | Routes protégées par le Worker inexistantes | Phase 2 |
| 🟡 MOYENNE | Plan codé en dur dans le webhook Stripe | Phase 3 |
| 🟡 MOYENNE | Absence d'idempotence webhook Stripe | Phase 3 |
| 🟡 MOYENNE | `*.pem` absent du `.gitignore` | Phase 2 |
| 🟡 MOYENNE | Licence Open-Meteo non commerciale | Phase 4 |
| 🔵 FAIBLE | Workflow deploy-worker déclenché sur master | Phase 2 |
| ⚪ INFO | ID AdSense public (intentionnel) | — |
| ⚪ INFO | Worker non actif (DNS ne passe pas par Cloudflare) | — |

---

## 11. Limitations de cet audit

- Pas de scan `gitleaks` outillé (non installé) : revue manuelle sur mots-clés `secret`, `key`, `token`, `password`.
- Pas de `pip-audit` : aucune dépendance externe dans le code de production actuel — à relancer dès l'ajout du backend.
- Pas de scan DAST (pages statiques sans serveur local accessible).
- Historique Git limité aux commits visibles localement (clone complet recommandé pour un audit exhaustif).
