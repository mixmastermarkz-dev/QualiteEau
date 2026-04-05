# Audit AdSense / SEO — Mon-Environnement.fr
*Réalisé le 05/04/2026*

---

## Ce qui a été créé / modifié

| Fichier | Action | Détail |
|---|---|---|
| `politique-confidentialite.html` | **Créé** | Page dédiée RGPD avec sections : données collectées, cookies, Google AdSense, CartoDB, GitHub Pages, droits RGPD, lien CNIL |
| `generate_pages.py` | **Modifié** | Titre des 102 pages eau-potable : `nbrx.fr` → `Mon-Environnement.fr` |
| `generate_pages.py` | **Modifié** | Footer des 102 pages : ajout liens Contact / Mentions légales / Confidentialité |
| `generate_pages.py` | **Modifié** | Sitemap : ajout de `politique-confidentialite.html` dans les pages statiques (107 URLs total) |
| `eau-potable/*/index.html` | **Regénérés** (×102) | Titre et footer corrigés |
| `mentions-legales.html` | **Modifié** | Footer : ajout lien "Confidentialité" · Lien interne "politique de confidentialité" → URL dédiée |
| `contact.html` | **Modifié** | Footer : ajout lien "Confidentialité" |
| `index.html` | **Modifié** | Bandeau cookies + footer : bouton modal → lien `<a href="/politique-confidentialite.html">` |
| `sitemap.xml` | **Regénéré** | 107 URLs incluant `politique-confidentialite.html` |

---

## Ce qui était déjà conforme

| Point | Statut |
|---|---|
| `robots.txt` — Googlebot et AdsBot-Google autorisés | ✅ |
| `<meta charset="UTF-8">` sur toutes les pages | ✅ |
| `<meta name="viewport">` sur toutes les pages | ✅ |
| `<meta name="robots" content="index, follow">` (pas de noindex) | ✅ |
| `<meta name="description">` unique et remplie | ✅ |
| `<title>` unique par page | ✅ (sauf nbrx.fr — corrigé) |
| Balises Open Graph et Twitter Card | ✅ |
| Balises `<link rel="canonical">` | ✅ |
| Données structurées JSON-LD (WebSite + Dataset) | ✅ |
| Script AdSense avec vrai `ca-pub-` dans `index.html` | ✅ |
| `mentions-legales.html` conforme LCEN | ✅ |
| `contact.html` avec email réel | ✅ |
| Bandeau cookies avec gestion Accepter / Refuser | ✅ |
| `sitemap.xml` référencé dans `robots.txt` | ✅ |

---

## Points à compléter manuellement

1. **Google Search Console** — Soumettre (ou re-soumettre) le sitemap après ce push :
   `https://www.mon-environnement.fr/sitemap.xml`

2. **Pages département** — Le dossier `departement/` est vide car le workflow `update_national.yml` n'a pas encore tourné avec succès.
   → Déclencher manuellement depuis GitHub Actions > "Mise à jour nationale des données" > "Run workflow"
   → Une fois généré, les pages seront automatiquement ajoutées au sitemap par `generate_dept_pages.py`

3. **Compte AdSense** — Vérifier que `mon-environnement.fr` est bien validé (pas `nbrx.fr`) dans la console AdSense.
   Si le site était référencé sous `nbrx.fr`, mettre à jour l'URL du site dans AdSense > Sites.

---

## Actions restantes hors code

- [ ] Soumettre sitemap.xml dans Google Search Console
- [ ] Déclencher le workflow national manuellement (pages département)
- [ ] Vérifier l'URL du site dans la console Google AdSense
