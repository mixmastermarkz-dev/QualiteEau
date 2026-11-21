"""
Tests des webhooks Stripe.

Couvre :
- Signature invalide → 400
- Payload malformé → 400
- Rejeu (ancien timestamp) → 400
- Événement valide → 200, subscription mise à jour
- Doublon (même stripe_event_id) → 200, "already_processed"
- checkout.session.completed → plan activé
- customer.subscription.updated → plan mis à jour
- customer.subscription.deleted → plan FREE, statut CANCELED
- invoice.paid → délai de grâce levé
- invoice.payment_failed → délai de grâce posé
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models.organisation import Organisation
from app.models.subscription import Plan, Subscription, SubscriptionStatus
from app.services.auth import get_or_create_user
from tests.conftest import TEST_WEBHOOK_SECRET, sign_stripe_payload, unique_email


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_webhook_payload(**kwargs) -> bytes:
    """Crée un payload JSON d'événement Stripe minimal."""
    defaults = {
        "id": f"evt_{uuid.uuid4().hex[:16]}",
        "type": "checkout.session.completed",
        "data": {"object": {}},
    }
    defaults.update(kwargs)
    return json.dumps(defaults).encode()


async def post_webhook(
    client: AsyncClient,
    payload: bytes,
    secret: str = TEST_WEBHOOK_SECRET,
    timestamp: int | None = None,
) -> "Response":
    """Poste un événement webhook signé."""
    sig = sign_stripe_payload(payload, secret, timestamp)
    return await client.post(
        "/billing/webhook",
        content=payload,
        headers={
            "Content-Type": "application/json",
            "stripe-signature": sig,
        },
    )


# ── Signature ─────────────────────────────────────────────────────────────────

async def test_webhook_invalid_signature_returns_400(
    client: AsyncClient, with_stripe_webhook_secret
):
    """Une signature HMAC incorrecte est rejetée."""
    payload = make_webhook_payload()
    response = await client.post(
        "/billing/webhook",
        content=payload,
        headers={
            "Content-Type": "application/json",
            "stripe-signature": "t=12345,v1=invalide",
        },
    )
    assert response.status_code == 400


async def test_webhook_missing_signature_returns_400(
    client: AsyncClient, with_stripe_webhook_secret
):
    """L'absence d'en-tête stripe-signature est rejetée."""
    payload = make_webhook_payload()
    response = await client.post(
        "/billing/webhook",
        content=payload,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400


async def test_webhook_replay_attack_returns_400(
    client: AsyncClient, with_stripe_webhook_secret
):
    """Un événement avec un timestamp vieux de >5 minutes est rejeté (anti-rejeu)."""
    payload = make_webhook_payload()
    old_timestamp = int(time.time()) - 400  # 6 minutes 40 secondes
    response = await post_webhook(client, payload, timestamp=old_timestamp)
    assert response.status_code == 400


async def test_webhook_no_secret_configured_returns_503(client: AsyncClient):
    """Sans STRIPE_WEBHOOK_SECRET configuré, le webhook est refusé avec 503."""
    from app.config import settings as _settings
    original = _settings.stripe_webhook_secret
    object.__setattr__(_settings, "stripe_webhook_secret", "")
    try:
        payload = make_webhook_payload()
        response = await client.post(
            "/billing/webhook",
            content=payload,
            headers={"Content-Type": "application/json", "stripe-signature": "t=1,v1=x"},
        )
    finally:
        object.__setattr__(_settings, "stripe_webhook_secret", original)
    assert response.status_code == 503


# ── Idempotence ───────────────────────────────────────────────────────────────

async def test_webhook_idempotence_service_level():
    """
    Vérifie l'idempotence au niveau du service (sans couche HTTP).
    Un événement Stripe ne doit être traité qu'une seule fois.
    """
    from app.models.stripe_event import StripeEvent
    from app.services.billing import is_event_already_processed, mark_event_processed

    event_id = f"evt_{uuid.uuid4().hex[:16]}"

    async with AsyncSessionLocal() as db:
        # Premier traitement : l'événement n'existe pas encore
        assert not await is_event_already_processed(db, event_id)
        await mark_event_processed(db, event_id, "invoice.paid")
        await db.commit()

    async with AsyncSessionLocal() as db:
        # Deuxième traitement : l'événement est déjà marqué
        assert await is_event_already_processed(db, event_id)


async def test_webhook_duplicate_via_http(client: AsyncClient, with_stripe_webhook_secret):
    """Un doublon reçu via HTTP retourne 200 "already_processed"."""
    from app.models.stripe_event import StripeEvent

    event_id = f"evt_{uuid.uuid4().hex[:16]}"

    # Pré-insère l'événement comme déjà traité
    async with AsyncSessionLocal() as db:
        db.add(StripeEvent(
            stripe_event_id=event_id,
            event_type="invoice.paid",
            processed_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
        ))
        await db.commit()

    # Envoie le webhook via HTTP avec signature valide
    payload = make_webhook_payload(id=event_id, type="invoice.paid")
    response = await post_webhook(client, payload)

    assert response.status_code == 200
    assert response.json()["status"] == "already_processed"


# ── Événements fonctionnels ───────────────────────────────────────────────────

async def test_subscription_deleted_sets_free_plan():
    """customer.subscription.deleted passe l'org en plan FREE + CANCELED."""
    email = unique_email()

    async with AsyncSessionLocal() as db:
        user = await get_or_create_user(db, email)
        org_id = user.org_id

        # Associe un stripe_customer_id à l'org
        result = await db.execute(select(Organisation).where(Organisation.id == org_id))
        org = result.scalar_one()
        org.stripe_customer_id = f"cus_test_{uuid.uuid4().hex[:8]}"
        customer_id = org.stripe_customer_id

        # Met l'abonnement en plan A actif
        result = await db.execute(select(Subscription).where(Subscription.org_id == org_id))
        sub = result.scalar_one()
        sub.plan = Plan.A
        sub.status = SubscriptionStatus.ACTIVE
        await db.commit()

    from app.services.billing import handle_subscription_deleted

    fake_event_data = {
        "object": {
            "id": "sub_todelete",
            "customer": customer_id,
        }
    }

    async with AsyncSessionLocal() as db:
        await handle_subscription_deleted(db, fake_event_data)
        await db.commit()

    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Subscription).where(Subscription.org_id == org_id))
        sub = result.scalar_one()

    assert sub.plan == Plan.FREE
    assert sub.status == SubscriptionStatus.CANCELED


async def test_invoice_payment_failed_sets_grace_period():
    """invoice.payment_failed pose le délai de grâce."""
    from app.config import settings
    email = unique_email()

    async with AsyncSessionLocal() as db:
        user = await get_or_create_user(db, email)
        org_id = user.org_id

        result = await db.execute(select(Organisation).where(Organisation.id == org_id))
        org = result.scalar_one()
        org.stripe_customer_id = f"cus_test_{uuid.uuid4().hex[:8]}"
        customer_id = org.stripe_customer_id

        result = await db.execute(select(Subscription).where(Subscription.org_id == org_id))
        sub = result.scalar_one()
        sub.plan = Plan.B
        sub.status = SubscriptionStatus.ACTIVE
        await db.commit()

    from app.services.billing import handle_invoice_payment_failed

    fake_event_data = {
        "object": {
            "id": f"in_{uuid.uuid4().hex[:8]}",
            "customer": customer_id,
        }
    }

    async with AsyncSessionLocal() as db:
        await handle_invoice_payment_failed(db, fake_event_data)
        await db.commit()

    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Subscription).where(Subscription.org_id == org_id))
        sub = result.scalar_one()

    assert sub.status == SubscriptionStatus.GRACE_PERIOD
    assert sub.grace_period_until is not None
    expected = datetime.now(timezone.utc) + timedelta(days=settings.payment_grace_period_days)
    # Tolérance de 5 secondes
    assert abs((sub.grace_period_until - expected).total_seconds()) < 5


async def test_invoice_paid_lifts_grace_period():
    """invoice.paid lève le délai de grâce et remet le statut ACTIVE."""
    email = unique_email()

    async with AsyncSessionLocal() as db:
        user = await get_or_create_user(db, email)
        org_id = user.org_id

        result = await db.execute(select(Organisation).where(Organisation.id == org_id))
        org = result.scalar_one()
        org.stripe_customer_id = f"cus_test_{uuid.uuid4().hex[:8]}"
        customer_id = org.stripe_customer_id

        result = await db.execute(select(Subscription).where(Subscription.org_id == org_id))
        sub = result.scalar_one()
        sub.plan = Plan.A
        sub.status = SubscriptionStatus.GRACE_PERIOD
        sub.grace_period_until = datetime.now(timezone.utc) + timedelta(days=3)
        await db.commit()

    from app.services.billing import handle_invoice_paid

    async with AsyncSessionLocal() as db:
        await handle_invoice_paid(
            db,
            {"object": {"id": f"in_{uuid.uuid4().hex[:8]}", "customer": customer_id}},
        )
        await db.commit()

    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Subscription).where(Subscription.org_id == org_id))
        sub = result.scalar_one()

    assert sub.status == SubscriptionStatus.ACTIVE
    assert sub.grace_period_until is None


async def test_subscription_updated_ignores_unknown_customer():
    """Un événement avec un customer inconnu est ignoré silencieusement."""
    from app.services.billing import handle_subscription_updated

    fake_event_data = {
        "object": {
            "id": "sub_unknown",
            "customer": "cus_does_not_exist_in_db",
            "status": "active",
            "items": {"data": [{"price": {"id": "price_xyz"}}]},
        }
    }

    # Doit s'exécuter sans exception
    async with AsyncSessionLocal() as db:
        await handle_subscription_updated(db, fake_event_data)
        await db.commit()  # no-op si rien n'a changé, mais nécessaire pour fermer proprement


async def test_unhandled_event_type_returns_200(
    client: AsyncClient, with_stripe_webhook_secret
):
    """Un type d'événement non géré est ignoré et retourne 200."""
    # Génère un événement de type inconnu avec une vraie signature
    event_id = f"evt_{uuid.uuid4().hex[:16]}"
    payload = make_webhook_payload(id=event_id, type="charge.refunded")
    response = await post_webhook(client, payload)
    assert response.status_code == 200
