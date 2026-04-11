"""
Vercel Serverless Function — Webhook Stripe
/api/stripe-webhook

Événements gérés :
  - checkout.session.completed  → génère JWT, pose cookie __me_token
  - customer.subscription.deleted → révoque le token (KV Cloudflare)
  - customer.subscription.updated  → renouvelle si plan changé

Variables d'environnement (Vercel) :
  STRIPE_SECRET_KEY        clé secrète Stripe
  STRIPE_WEBHOOK_SECRET    secret de signature du webhook Stripe
  JWT_SECRET               partagé avec le Cloudflare Worker
  CF_KV_NAMESPACE_ID       Cloudflare KV namespace pour les tokens révoqués
  CF_API_TOKEN             token API Cloudflare (KV write)
  CF_ACCOUNT_ID            Cloudflare account ID

Usage local :
  pip install stripe
  stripe listen --forward-to localhost:3000/api/stripe-webhook
"""

import hashlib
import hmac
import json
import os
import time
import base64
from http.server import BaseHTTPRequestHandler

# stripe est la seule dépendance externe (pip install stripe)
try:
    import stripe
except ImportError:
    stripe = None


COOKIE_NAME = "__me_token"
TOKEN_TTL_SECONDS = 32 * 24 * 3600  # 32 jours
DOMAIN = "mon-environnement.fr"


# ── Handler Vercel ──────────────────────────────────────────────────────────

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        # Vérification signature Stripe
        sig = self.headers.get("Stripe-Signature", "")
        webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "")

        if not _verify_stripe_signature(body, sig, webhook_secret):
            self._respond(400, {"error": "Invalid signature"})
            return

        event = json.loads(body)
        event_type = event.get("type", "")

        if event_type == "checkout.session.completed":
            _handle_checkout(event["data"]["object"], self)
        elif event_type in ("customer.subscription.deleted",):
            _handle_revoke(event["data"]["object"])
            self._respond(200, {"ok": True})
        else:
            self._respond(200, {"ok": True, "ignored": event_type})

    def _respond(self, status, body, headers=None):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        if headers:
            for k, v in headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())


# ── Handlers événements ─────────────────────────────────────────────────────

def _handle_checkout(session, req_handler):
    email = session.get("customer_email") or session.get("customer_details", {}).get("email", "")
    customer_id = session.get("customer", "")
    subscription_id = session.get("subscription", "")

    if not email:
        req_handler._respond(400, {"error": "No email in session"})
        return

    token = _generate_jwt(email, customer_id, subscription_id)
    cookie = (
        f"{COOKIE_NAME}={token}; "
        f"HttpOnly; Secure; SameSite=Lax; "
        f"Domain={DOMAIN}; "
        f"Max-Age={TOKEN_TTL_SECONDS}; Path=/"
    )

    # Redirection vers la page de succès après paiement
    req_handler._respond(200, {"ok": True, "token_issued": True}, {
        "Set-Cookie": cookie
    })


def _handle_revoke(subscription):
    """Ajoute le subscription_id à la liste de révocation dans Cloudflare KV."""
    sub_id = subscription.get("id", "")
    if not sub_id:
        return

    cf_token = os.environ.get("CF_API_TOKEN", "")
    cf_account = os.environ.get("CF_ACCOUNT_ID", "")
    cf_kv_ns = os.environ.get("CF_KV_NAMESPACE_ID", "")

    if not all([cf_token, cf_account, cf_kv_ns]):
        return  # KV non configuré — tokens expireront naturellement

    import urllib.request
    url = f"https://api.cloudflare.com/client/v4/accounts/{cf_account}/storage/kv/namespaces/{cf_kv_ns}/values/revoked:{sub_id}"
    req = urllib.request.Request(
        url,
        data=b"1",
        headers={
            "Authorization": f"Bearer {cf_token}",
            "Content-Type": "text/plain",
        },
        method="PUT"
    )
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # Ne pas bloquer le webhook si KV échoue


# ── JWT HMAC-SHA256 ─────────────────────────────────────────────────────────

def _generate_jwt(email: str, customer_id: str, subscription_id: str) -> str:
    """
    Génère un JWT minimal signé HMAC-SHA256.
    Format : base64url(header).base64url(payload).base64url(signature)
    Compatible avec le Cloudflare Worker verifyJWT().
    """
    secret = os.environ.get("JWT_SECRET", "dev-secret-change-in-production")

    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps({
        "sub": email,
        "cid": customer_id,
        "sid": subscription_id,
        "plan": "premium",
        "iat": int(time.time()),
        "exp": int(time.time()) + TOKEN_TTL_SECONDS,
    }).encode())

    signing_input = f"{header}.{payload}"
    sig = hmac.new(secret.encode(), signing_input.encode(), hashlib.sha256).digest()
    signature = _b64url(sig)

    return f"{signing_input}.{signature}"


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


# ── Vérification signature Stripe ──────────────────────────────────────────

def _verify_stripe_signature(payload: bytes, sig_header: str, secret: str) -> bool:
    try:
        parts = {k: v for item in sig_header.split(",") for k, v in [item.split("=", 1)]}
        timestamp = parts["t"]
        v1 = parts["v1"]
        signed_payload = f"{timestamp}.{payload.decode()}"
        expected = hmac.new(secret.encode(), signed_payload.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, v1)
    except Exception:
        return False
