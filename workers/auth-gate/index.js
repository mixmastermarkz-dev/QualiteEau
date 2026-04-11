/**
 * Cloudflare Worker — Gate Premium mon-environnement.fr
 *
 * Intercepte toutes les requêtes vers /france/*/detail/*
 * Vérifie le JWT HMAC-SHA256 dans le cookie __me_token
 * Si valide → passe la requête
 * Si absent/invalide/expiré → redirige vers /premium/
 *
 * Variables d'environnement Workers à configurer dans wrangler.toml :
 *   JWT_SECRET = secret HMAC partagé avec le webhook Stripe (Vercel/Netlify)
 *
 * Déploiement :
 *   cd workers/auth-gate && npx wrangler deploy
 */

const PREMIUM_PATHS = ['/france/', '/detail/'];
const PREMIUM_REDIRECT = '/premium/?ref=gate';
const COOKIE_NAME = '__me_token';
const TOKEN_MAX_AGE_DAYS = 32; // légèrement > 30 jours pour couvrir les délais de facturation

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // ── Ne gate que les URLs /france/xxx/detail/ ──────────────────────────
    const isPremium = path.startsWith('/france/') && path.includes('/detail');
    if (!isPremium) {
      return fetch(request); // pass-through
    }

    // ── Lire le cookie JWT ────────────────────────────────────────────────
    const cookieHeader = request.headers.get('Cookie') || '';
    const token = parseCookie(cookieHeader, COOKIE_NAME);

    if (!token) {
      return redirectToPremium(url);
    }

    // ── Vérifier le JWT ───────────────────────────────────────────────────
    const payload = await verifyJWT(token, env.JWT_SECRET);
    if (!payload) {
      return redirectToPremium(url);
    }

    // ── Vérifier l'expiration ─────────────────────────────────────────────
    const now = Math.floor(Date.now() / 1000);
    if (payload.exp && payload.exp < now) {
      return redirectToPremium(url, 'expired');
    }

    // ── Vérifier le plan ──────────────────────────────────────────────────
    if (payload.plan !== 'premium') {
      return redirectToPremium(url, 'plan');
    }

    // ── Accès autorisé ────────────────────────────────────────────────────
    return fetch(request);
  }
};

// ── Helpers ────────────────────────────────────────────────────────────────

function parseCookie(cookieHeader, name) {
  const match = cookieHeader.match(new RegExp(`(?:^|;\\s*)${name}=([^;]+)`));
  return match ? decodeURIComponent(match[1]) : null;
}

function redirectToPremium(url, reason = '') {
  const dest = new URL(PREMIUM_REDIRECT, url.origin);
  dest.searchParams.set('next', url.pathname);
  if (reason) dest.searchParams.set('reason', reason);
  return Response.redirect(dest.toString(), 302);
}

/**
 * Vérifie un JWT de format : base64url(header).base64url(payload).base64url(signature)
 * Signature = HMAC-SHA256(secret, header + '.' + payload)
 * Retourne le payload décodé ou null si invalide.
 */
async function verifyJWT(token, secret) {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) return null;

    const [headerB64, payloadB64, sigB64] = parts;

    // Vérifier la signature
    const encoder = new TextEncoder();
    const keyData = encoder.encode(secret);
    const key = await crypto.subtle.importKey(
      'raw', keyData,
      { name: 'HMAC', hash: 'SHA-256' },
      false, ['verify']
    );

    const signatureBytes = base64UrlDecode(sigB64);
    const dataBytes = encoder.encode(`${headerB64}.${payloadB64}`);
    const valid = await crypto.subtle.verify('HMAC', key, signatureBytes, dataBytes);

    if (!valid) return null;

    // Décoder le payload
    const payload = JSON.parse(new TextDecoder().decode(base64UrlDecode(payloadB64)));
    return payload;
  } catch {
    return null;
  }
}

function base64UrlDecode(str) {
  // Convertit base64url → base64 standard
  const b64 = str.replace(/-/g, '+').replace(/_/g, '/').padEnd(
    str.length + (4 - str.length % 4) % 4, '='
  );
  const binary = atob(b64);
  return Uint8Array.from(binary, c => c.charCodeAt(0));
}
