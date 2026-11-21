#!/usr/bin/env python3
"""
generate_tarifs.py — Génère tarifs/index.html
Page de présentation des formules d'abonnement Mon-Environnement.fr
"""
import os
from datetime import date

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
YEAR = date.today().year
TAILWIND_SRI = "sha256-9vMjhXEJhnMA4eZu2RkP4gbUPIzq4+zL9+KIIOHJj4A="


def _cookie_banner_html() -> str:
    return """<!-- BANDEAU COOKIES CNIL -->
<div id="cookie-banner" style="display:none" role="dialog" aria-label="Consentement cookies" aria-modal="true"
     class="fixed bottom-0 left-0 right-0 z-50 bg-white border-t border-slate-200 shadow-2xl p-4 md:p-6">
  <div class="max-w-4xl mx-auto flex flex-col md:flex-row items-start md:items-center gap-4">
    <div class="flex-1 text-sm text-slate-700">
      <p class="font-bold mb-1">Ce site utilise des cookies publicitaires (Google AdSense)</p>
      <p>En cliquant « Accepter », vous autorisez le dépôt de cookies pour la personnalisation des publicités.
         Vous pouvez refuser sans impact sur votre navigation.
         <a href="/politique-confidentialite.html" class="underline text-sky-600">En savoir plus</a>.</p>
    </div>
    <div class="flex gap-3 flex-shrink-0">
      <button id="cookie-refuse" onclick="cookieRefuse()"
              class="px-5 py-2.5 rounded-xl border-2 border-slate-300 text-sm font-bold text-slate-700 hover:border-slate-500 transition-colors">
        Refuser
      </button>
      <button id="cookie-accept" onclick="cookieAccept()"
              class="px-5 py-2.5 rounded-xl bg-sky-500 text-white text-sm font-bold hover:bg-sky-600 transition-colors">
        Accepter
      </button>
    </div>
  </div>
</div>
<script>
(function(){
  var CONSENT_KEY = 'cookie_consent';
  var CONSENT_VERSION = '1';
  function getConsent(){ return localStorage.getItem(CONSENT_KEY); }
  function loadAdsense(){
    if(document.querySelector('script[data-adsense]')) return;
    var s = document.createElement('script');
    s.async = true;
    s.setAttribute('data-adsense','1');
    s.src = 'https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-6656990941754174';
    s.crossOrigin = 'anonymous';
    document.head.appendChild(s);
  }
  window.cookieAccept = function(){
    localStorage.setItem(CONSENT_KEY, 'accepted:' + CONSENT_VERSION);
    document.getElementById('cookie-banner').style.display = 'none';
    loadAdsense();
  };
  window.cookieRefuse = function(){
    localStorage.setItem(CONSENT_KEY, 'refused:' + CONSENT_VERSION);
    document.getElementById('cookie-banner').style.display = 'none';
  };
  var c = getConsent();
  if(!c){ document.getElementById('cookie-banner').style.display = ''; }
  else if(c.startsWith('accepted')){ loadAdsense(); }
})();
</script>"""


def build_tarifs_page() -> str:
    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Prévision sécheresse — Choisissez votre formule | Mon-Environnement.fr</title>
    <meta name="description" content="Abonnez-vous pour accéder aux prévisions de sécheresse sur vos communes en Occitanie. 3 formules : Essentiel, Pro, Ultimate. Modèle prédictif expérimental.">
    <meta name="robots" content="index, follow">
    <link rel="canonical" href="https://www.mon-environnement.fr/tarifs/">
    <meta property="og:type" content="website">
    <meta property="og:url" content="https://www.mon-environnement.fr/tarifs/">
    <meta property="og:title" content="Prévision sécheresse — Formules d'abonnement">
    <meta property="og:description" content="Accédez aux prévisions de sécheresse sur vos zones d'alerte en Occitanie. Alertes e-mail, historique CSV, API à clé.">
    <meta property="og:locale" content="fr_FR">
    <!-- AdSense chargé après consentement cookies (voir cookie-banner en bas de page) -->
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🌿</text></svg>">
    <style>
        body {{ font-family: system-ui, sans-serif; }}
        .plan-card {{ transition: transform 0.15s, box-shadow 0.15s; }}
        .plan-card:hover {{ transform: translateY(-2px); box-shadow: 0 8px 32px rgba(0,0,0,0.10); }}
    </style>
</head>
<body class="bg-slate-50 min-h-screen text-slate-900 flex flex-col">

<noscript>
<style>body{{ font-family: system-ui, sans-serif; }}</style>
</noscript>

<!-- HEADER -->
<header class="bg-white border-b border-slate-200 shadow-sm">
    <div class="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between gap-4">
        <a href="/" class="flex items-center gap-3 group">
            <span class="text-slate-400 group-hover:text-sky-500 transition-colors text-lg font-black">←</span>
            <span class="font-black text-slate-700 uppercase tracking-tight text-sm group-hover:text-sky-600 transition-colors">Mon-Environnement.fr</span>
        </a>
        <span class="text-xs font-bold text-slate-400 uppercase tracking-widest hidden md:block">Formules d'abonnement</span>
    </div>
</header>

<!-- HERO -->
<main class="flex-grow">
<section class="max-w-6xl mx-auto px-4 py-12 text-center">
    <p class="text-xs font-black text-sky-600 uppercase tracking-widest mb-3">Prévisions sécheresse — Occitanie</p>
    <h1 class="text-4xl md:text-5xl font-black text-slate-900 leading-tight mb-4">
        Prévision sécheresse —<br class="hidden md:block"> Choisissez votre formule
    </h1>
    <p class="text-slate-500 text-lg max-w-2xl mx-auto">
        Anticipez les restrictions d'eau sur vos zones d'alerte. Alertes e-mail, horizons prédictifs jusqu'à 30 jours, historique CSV.
    </p>
    <p class="mt-4 inline-block bg-amber-50 border border-amber-200 text-amber-800 text-xs font-bold px-4 py-2 rounded-full">
        ⚠️ Estimation non officielle — modèle prédictif expérimental. Seuls les arrêtés préfectoraux publiés font foi.
    </p>
</section>

<!-- GRILLE DES PLANS -->
<section class="max-w-6xl mx-auto px-4 pb-16">
    <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">

        <!-- PLAN GRATUIT -->
        <div class="plan-card bg-white rounded-3xl border border-slate-200 p-6 flex flex-col">
            <div class="mb-6">
                <p class="text-xs font-black text-slate-400 uppercase tracking-widest mb-1">Gratuit</p>
                <p class="text-3xl font-black text-slate-900">0 €</p>
                <p class="text-slate-400 text-sm mt-1">Toujours gratuit</p>
            </div>
            <ul class="space-y-3 flex-grow text-sm text-slate-700">
                <li class="flex items-start gap-2"><span class="text-green-500 font-black mt-0.5">✓</span><span><strong>Périmètre :</strong> 1 commune (indicateur grossier)</span></li>
                <li class="flex items-start gap-2"><span class="text-slate-300 font-black mt-0.5">—</span><span class="text-slate-400">Horizons prédictifs</span></li>
                <li class="flex items-start gap-2"><span class="text-slate-300 font-black mt-0.5">—</span><span class="text-slate-400">Facteurs explicatifs</span></li>
                <li class="flex items-start gap-2"><span class="text-slate-300 font-black mt-0.5">—</span><span class="text-slate-400">Alertes e-mail</span></li>
                <li class="flex items-start gap-2"><span class="text-slate-300 font-black mt-0.5">—</span><span class="text-slate-400">Historique &amp; CSV</span></li>
                <li class="flex items-start gap-2"><span class="text-slate-300 font-black mt-0.5">—</span><span class="text-slate-400">API à clé</span></li>
                <li class="flex items-start gap-2"><span class="text-slate-300 font-black mt-0.5">—</span><span class="text-slate-400">Rapport PDF mensuel</span></li>
                <li class="flex items-start gap-2"><span class="text-green-500 font-black mt-0.5">✓</span><span><strong>Utilisateurs :</strong> 1</span></li>
            </ul>
            <div class="mt-6">
                <span class="block w-full text-center px-4 py-3 rounded-xl border-2 border-slate-200 text-sm font-bold text-slate-500 cursor-default">
                    Accès actuel
                </span>
            </div>
        </div>

        <!-- PLAN A — ESSENTIEL -->
        <div class="plan-card bg-white rounded-3xl border-2 border-sky-200 p-6 flex flex-col">
            <div class="mb-6">
                <p class="text-xs font-black text-sky-600 uppercase tracking-widest mb-1">A — Essentiel</p>
                <p class="text-3xl font-black text-slate-900">4,90 €<span class="text-base font-bold text-slate-400">/mois</span></p>
                <p class="text-slate-400 text-sm mt-1">Sans engagement</p>
            </div>
            <ul class="space-y-3 flex-grow text-sm text-slate-700">
                <li class="flex items-start gap-2"><span class="text-green-500 font-black mt-0.5">✓</span><span><strong>Périmètre :</strong> 1 commune</span></li>
                <li class="flex items-start gap-2"><span class="text-green-500 font-black mt-0.5">✓</span><span><strong>Horizons :</strong> 15 jours</span></li>
                <li class="flex items-start gap-2"><span class="text-slate-300 font-black mt-0.5">—</span><span class="text-slate-400">Facteurs explicatifs</span></li>
                <li class="flex items-start gap-2"><span class="text-green-500 font-black mt-0.5">✓</span><span><strong>Alertes e-mail</strong> à seuil fixe</span></li>
                <li class="flex items-start gap-2"><span class="text-slate-300 font-black mt-0.5">—</span><span class="text-slate-400">Historique &amp; CSV</span></li>
                <li class="flex items-start gap-2"><span class="text-slate-300 font-black mt-0.5">—</span><span class="text-slate-400">API à clé</span></li>
                <li class="flex items-start gap-2"><span class="text-slate-300 font-black mt-0.5">—</span><span class="text-slate-400">Rapport PDF mensuel</span></li>
                <li class="flex items-start gap-2"><span class="text-green-500 font-black mt-0.5">✓</span><span><strong>Utilisateurs :</strong> 1</span></li>
            </ul>
            <div class="mt-6">
                <a href="/app/checkout?plan=a"
                   class="block w-full text-center px-4 py-3 rounded-xl bg-sky-500 hover:bg-sky-600 text-white text-sm font-bold transition-colors">
                    Choisir Essentiel →
                </a>
            </div>
        </div>

        <!-- PLAN B — PRO -->
        <div class="plan-card bg-gradient-to-b from-sky-50 to-white rounded-3xl border-2 border-sky-400 p-6 flex flex-col relative">
            <div class="absolute -top-3 left-1/2 -translate-x-1/2">
                <span class="bg-sky-500 text-white text-[10px] font-black uppercase tracking-widest px-3 py-1 rounded-full">Populaire</span>
            </div>
            <div class="mb-6">
                <p class="text-xs font-black text-sky-600 uppercase tracking-widest mb-1">B — Pro</p>
                <p class="text-3xl font-black text-slate-900">39 €<span class="text-base font-bold text-slate-400">/mois</span></p>
                <p class="text-slate-400 text-sm mt-1">Sans engagement</p>
            </div>
            <ul class="space-y-3 flex-grow text-sm text-slate-700">
                <li class="flex items-start gap-2"><span class="text-green-500 font-black mt-0.5">✓</span><span><strong>Périmètre :</strong> 10 zones d'alerte</span></li>
                <li class="flex items-start gap-2"><span class="text-green-500 font-black mt-0.5">✓</span><span><strong>Horizons :</strong> 7, 15, 30 jours</span></li>
                <li class="flex items-start gap-2"><span class="text-slate-300 font-black mt-0.5">—</span><span class="text-slate-400">Facteurs explicatifs</span></li>
                <li class="flex items-start gap-2"><span class="text-green-500 font-black mt-0.5">✓</span><span><strong>Alertes e-mail</strong> seuil réglable</span></li>
                <li class="flex items-start gap-2"><span class="text-green-500 font-black mt-0.5">✓</span><span><strong>Historique &amp; export CSV</strong></span></li>
                <li class="flex items-start gap-2"><span class="text-slate-300 font-black mt-0.5">—</span><span class="text-slate-400">API à clé</span></li>
                <li class="flex items-start gap-2"><span class="text-slate-300 font-black mt-0.5">—</span><span class="text-slate-400">Rapport PDF mensuel</span></li>
                <li class="flex items-start gap-2"><span class="text-green-500 font-black mt-0.5">✓</span><span><strong>Utilisateurs :</strong> 1</span></li>
            </ul>
            <div class="mt-6">
                <a href="/app/checkout?plan=b"
                   class="block w-full text-center px-4 py-3 rounded-xl bg-sky-600 hover:bg-sky-700 text-white text-sm font-bold transition-colors">
                    Choisir Pro →
                </a>
            </div>
        </div>

        <!-- PLAN C — ULTIMATE -->
        <div class="plan-card bg-gradient-to-b from-slate-900 to-slate-800 rounded-3xl border border-slate-700 p-6 flex flex-col text-white">
            <div class="mb-6">
                <p class="text-xs font-black text-sky-400 uppercase tracking-widest mb-1">C — Ultimate</p>
                <p class="text-3xl font-black">199 €<span class="text-base font-bold text-slate-400">/mois</span></p>
                <p class="text-slate-400 text-sm mt-1">ou sur devis</p>
            </div>
            <ul class="space-y-3 flex-grow text-sm text-slate-300">
                <li class="flex items-start gap-2"><span class="text-green-400 font-black mt-0.5">✓</span><span><strong class="text-white">Périmètre :</strong> Toute l'Occitanie</span></li>
                <li class="flex items-start gap-2"><span class="text-green-400 font-black mt-0.5">✓</span><span><strong class="text-white">Horizons :</strong> 7, 15, 30 jours</span></li>
                <li class="flex items-start gap-2"><span class="text-green-400 font-black mt-0.5">✓</span><span><strong class="text-white">Facteurs explicatifs</strong> Top-3</span></li>
                <li class="flex items-start gap-2"><span class="text-green-400 font-black mt-0.5">✓</span><span><strong class="text-white">Alertes e-mail</strong> seuil réglable, multi-destinataires</span></li>
                <li class="flex items-start gap-2"><span class="text-green-400 font-black mt-0.5">✓</span><span><strong class="text-white">Historique &amp; export CSV</strong></span></li>
                <li class="flex items-start gap-2"><span class="text-green-400 font-black mt-0.5">✓</span><span><strong class="text-white">API à clé</strong></span></li>
                <li class="flex items-start gap-2"><span class="text-green-400 font-black mt-0.5">✓</span><span><strong class="text-white">Rapport PDF mensuel</strong></span></li>
                <li class="flex items-start gap-2"><span class="text-green-400 font-black mt-0.5">✓</span><span><strong class="text-white">Utilisateurs :</strong> jusqu'à 10</span></li>
                <li class="flex items-start gap-2"><span class="text-sky-400 font-black mt-0.5">★</span><span><strong class="text-white">Pilote gratuit</strong> 30 jours</span></li>
            </ul>
            <div class="mt-6 space-y-2">
                <a href="/app/checkout?plan=c"
                   class="block w-full text-center px-4 py-3 rounded-xl bg-sky-500 hover:bg-sky-400 text-white text-sm font-bold transition-colors">
                    Choisir Ultimate →
                </a>
                <a href="/contact.html"
                   class="block w-full text-center px-4 py-2.5 rounded-xl border border-slate-600 hover:border-slate-400 text-slate-300 text-xs font-bold transition-colors">
                    ou sur devis — contactez-nous
                </a>
            </div>
        </div>

    </div>
</section>

<!-- MENTION LÉGALE -->
<section class="max-w-4xl mx-auto px-4 pb-10 text-center">
    <div class="bg-amber-50 border border-amber-200 rounded-2xl p-4">
        <p class="text-xs text-amber-800 font-bold">
            ⚠️ Estimation non officielle — modèle prédictif expérimental.<br>
            Seuls les arrêtés préfectoraux publiés font foi. Les prévisions ne se substituent pas aux informations officielles de la préfecture.
        </p>
    </div>
</section>

<!-- FAQ -->
<section class="max-w-3xl mx-auto px-4 pb-16">
    <h2 class="text-2xl font-black text-slate-900 text-center mb-8">Questions fréquentes</h2>
    <div class="space-y-4">

        <details class="bg-white rounded-2xl border border-slate-200 p-5 group" open>
            <summary class="font-bold text-slate-800 cursor-pointer list-none flex items-center justify-between gap-3">
                <span>Les prévisions sont-elles officielles ?</span>
                <span class="text-slate-400 group-open:rotate-180 transition-transform">▼</span>
            </summary>
            <p class="mt-3 text-sm text-slate-600 leading-relaxed">
                Non. Ce service est indépendant et expérimental. Les prévisions sont produites par un modèle statistique à partir de données publiques (VigiEau, Hub'Eau, Météo-France). Elles ne remplacent en aucun cas les arrêtés préfectoraux. Pour toute décision officielle, référez-vous aux publications de votre préfecture de département.
            </p>
        </details>

        <details class="bg-white rounded-2xl border border-slate-200 p-5 group">
            <summary class="font-bold text-slate-800 cursor-pointer list-none flex items-center justify-between gap-3">
                <span>Comment fonctionne le modèle ?</span>
                <span class="text-slate-400 group-open:rotate-180 transition-transform">▼</span>
            </summary>
            <p class="mt-3 text-sm text-slate-600 leading-relaxed">
                Le modèle croise les historiques de restrictions (VigiEau), les niveaux des nappes phréatiques (BRGM), les débits des cours d'eau et les données pluviométriques. Un algorithme de classification entraîné sur les années 2012–2023 prédit le niveau d'alerte probable sur votre zone pour les 7, 15 et 30 prochains jours. La précision est indicative : les événements climatiques exceptionnels peuvent être sous-estimés.
            </p>
        </details>

        <details class="bg-white rounded-2xl border border-slate-200 p-5 group">
            <summary class="font-bold text-slate-800 cursor-pointer list-none flex items-center justify-between gap-3">
                <span>Puis-je changer ou résilier ma formule ?</span>
                <span class="text-slate-400 group-open:rotate-180 transition-transform">▼</span>
            </summary>
            <p class="mt-3 text-sm text-slate-600 leading-relaxed">
                Oui, sans engagement. Vous pouvez changer de formule ou résilier à tout moment depuis votre espace abonné. La résiliation prend effet à la fin de la période déjà payée. Aucun frais de résiliation n'est appliqué.
            </p>
        </details>

        <details class="bg-white rounded-2xl border border-slate-200 p-5 group">
            <summary class="font-bold text-slate-800 cursor-pointer list-none flex items-center justify-between gap-3">
                <span>Les données personnelles sont-elles protégées ?</span>
                <span class="text-slate-400 group-open:rotate-180 transition-transform">▼</span>
            </summary>
            <p class="mt-3 text-sm text-slate-600 leading-relaxed">
                Oui. Seules votre adresse e-mail et vos préférences de zones sont collectées. Ces données ne sont ni vendues ni partagées avec des tiers. Elles sont hébergées en France. Vous pouvez demander leur suppression à tout moment via la page <a href="/contact.html" class="underline text-sky-600">contact</a>. Conformément au RGPD, vous disposez d'un droit d'accès, de rectification et d'effacement.
            </p>
        </details>

        <details class="bg-white rounded-2xl border border-slate-200 p-5 group">
            <summary class="font-bold text-slate-800 cursor-pointer list-none flex items-center justify-between gap-3">
                <span>La formule C est-elle disponible par bon de commande ?</span>
                <span class="text-slate-400 group-open:rotate-180 transition-transform">▼</span>
            </summary>
            <p class="mt-3 text-sm text-slate-600 leading-relaxed">
                Oui. Pour les collectivités, associations et entreprises qui ont besoin d'une facturation via bon de commande ou d'un contrat cadre, nous proposons la formule C sur devis. <a href="/contact.html" class="underline text-sky-600">Contactez-nous</a> avec votre SIRET et vos besoins : nous vous répondons sous 48 h ouvrées.
            </p>
        </details>

    </div>
</section>

</main>

<!-- FOOTER -->
<footer class="bg-slate-900 text-white py-10 mt-4">
    <div class="max-w-4xl mx-auto px-4 text-center">
        <p class="font-black uppercase text-lg mb-2">MON-<span class="text-sky-400">ENVIRONNEMENT</span>.FR</p>
        <p class="text-slate-400 text-sm">Surveillance indépendante · Données officielles ARS, BRGM, Hub'Eau</p>
        <p class="text-slate-400 text-sm mt-1 italic">Estimation non officielle — modèle prédictif expérimental. Seuls les arrêtés préfectoraux publiés font foi.</p>
        <div class="mt-4 flex justify-center gap-6 text-xs text-slate-500">
            <a href="/contact.html" class="hover:text-white transition-colors underline">Contact</a>
            <a href="/mentions-legales.html" class="hover:text-white transition-colors underline">Mentions légales</a>
            <a href="/politique-confidentialite.html" class="hover:text-white transition-colors underline">Confidentialité</a>
        </div>
        <p class="text-slate-600 text-xs mt-4">© {YEAR} · <a href="/" class="hover:text-white transition-colors">Mon-Environnement.fr</a></p>
    </div>
</footer>

{_cookie_banner_html()}
</body>
</html>"""


def main():
    out_dir = os.path.join(BASE_DIR, "tarifs")
    os.makedirs(out_dir, exist_ok=True)
    html = build_tarifs_page()
    out_path = os.path.join(out_dir, "index.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  OK tarifs/index.html ({len(html)} caractères)")


if __name__ == "__main__":
    main()
