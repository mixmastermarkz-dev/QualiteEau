"""
Service de facturation Stripe.

Responsabilités :
- Créer/récupérer un client Stripe pour une organisation.
- Créer une session Checkout avec un price_id validé côté serveur.
- Traiter les événements webhook Stripe (idempotents).
- Mettre à jour le plan et l'état de l'abonnement en base.

Règles critiques :
- Le plan est TOUJOURS déduit du stripe_price_id via settings.price_plan_map.
  Il n'est JAMAIS fourni ni accepté depuis le client.
- Chaque événement est traité au plus une fois (table stripe_events).
- stripe.Webhook.construct_event vérifie la signature ET l'horodatage
  (tolérance par défaut = 300 s).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import stripe
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.organisation import Organisation
from app.models.stripe_event import StripeEvent
from app.models.subscription import Plan, Subscription, SubscriptionStatus
from app.models.user import User
from app.services.audit import log_action

logger = logging.getLogger(__name__)


# ── Client Stripe ─────────────────────────────────────────────────────────────

def init_stripe() -> None:
    """À appeler une seule fois au démarrage de l'application."""
    if settings.stripe_secret_key:
        stripe.api_key = settings.stripe_secret_key
    else:
        logger.warning(
            "[STRIPE] STRIPE_SECRET_KEY non définie — paiement désactivé en dev."
        )


# ── Client Stripe par organisation ───────────────────────────────────────────

async def get_or_create_stripe_customer(
    db: AsyncSession, user: User, org: Organisation
) -> str:
    """
    Retourne le stripe_customer_id de l'organisation, en créant un client
    Stripe si nécessaire.
    """
    if org.stripe_customer_id:
        return org.stripe_customer_id

    customer = stripe.Customer.create(
        email=user.email,
        metadata={"org_id": org.id},
    )
    org.stripe_customer_id = customer.id
    await db.flush()

    await log_action(
        db,
        action="stripe_customer_created",
        actor_user_id=user.id,
        org_id=org.id,
        target_type="stripe_customer",
        target_id=customer.id,
    )
    return customer.id


# ── Checkout ──────────────────────────────────────────────────────────────────

async def create_checkout_session(
    db: AsyncSession,
    user: User,
    org: Organisation,
    plan_value: str,
) -> str:
    """
    Crée une session Stripe Checkout et retourne son URL.

    Le price_id est déterminé côté serveur depuis plan_value (A/B/C).
    La valeur provenant du client n'est JAMAIS utilisée directement.
    """
    price_id = settings.get_price_id_for_plan(plan_value)
    if not price_id:
        raise ValueError(f"Formule inconnue ou non configurée : {plan_value!r}")

    customer_id = await get_or_create_stripe_customer(db, user, org)

    session = stripe.checkout.Session.create(
        payment_method_types=["card"],
        line_items=[{"price": price_id, "quantity": 1}],
        mode="subscription",
        customer=customer_id,
        client_reference_id=org.id,
        metadata={"org_id": org.id, "user_id": user.id, "plan": plan_value.upper()},
        success_url=(
            f"{settings.frontend_origin}/app/billing/success"
            "?session_id={CHECKOUT_SESSION_ID}"
        ),
        cancel_url=f"{settings.frontend_origin}/app/billing/annulation",
        # Essai gratuit 30 jours pour la formule C
        **(
            {"subscription_data": {"trial_period_days": 30}}
            if plan_value.upper() == "C"
            else {}
        ),
    )

    await log_action(
        db,
        action="checkout_session_created",
        actor_user_id=user.id,
        org_id=org.id,
        metadata={"plan": plan_value.upper(), "stripe_session_id": session.id},
    )

    return session.url


async def retrieve_checkout_session(session_id: str) -> dict[str, Any]:
    """
    Récupère l'état d'une session Checkout depuis l'API Stripe.
    Ne fait JAMAIS confiance aux paramètres URL — toujours vérifier côté serveur.
    """
    session = stripe.checkout.Session.retrieve(
        session_id, expand=["subscription"]
    )
    return {
        "payment_status": session.payment_status,
        "subscription_id": session.subscription.id if session.subscription else None,
        "status": session.status,
    }


# ── Portail client ────────────────────────────────────────────────────────────

async def create_portal_session(org: Organisation) -> str:
    """Crée une session Stripe Billing Portal et retourne son URL."""
    if not org.stripe_customer_id:
        raise ValueError("Cette organisation n'a pas encore de client Stripe.")

    portal = stripe.billing_portal.Session.create(
        customer=org.stripe_customer_id,
        return_url=f"{settings.frontend_origin}/app/billing",
    )
    return portal.url


# ── Idempotence ───────────────────────────────────────────────────────────────

async def is_event_already_processed(db: AsyncSession, stripe_event_id: str) -> bool:
    result = await db.execute(
        select(StripeEvent).where(StripeEvent.stripe_event_id == stripe_event_id)
    )
    return result.scalar_one_or_none() is not None


async def mark_event_processed(
    db: AsyncSession, stripe_event_id: str, event_type: str
) -> None:
    db.add(
        StripeEvent(
            stripe_event_id=stripe_event_id,
            event_type=event_type,
            processed_at=datetime.now(timezone.utc),
        )
    )
    await db.flush()


# ── Mise à jour de l'abonnement ───────────────────────────────────────────────

async def _get_or_create_subscription(
    db: AsyncSession, org_id: str
) -> Subscription:
    result = await db.execute(
        select(Subscription).where(Subscription.org_id == org_id)
    )
    sub = result.scalar_one_or_none()
    if sub is None:
        sub = Subscription(org_id=org_id)
        db.add(sub)
        await db.flush()
    return sub


async def _get_org_by_stripe_customer(
    db: AsyncSession, customer_id: str
) -> Organisation | None:
    result = await db.execute(
        select(Organisation).where(Organisation.stripe_customer_id == customer_id)
    )
    return result.scalar_one_or_none()


def _plan_from_stripe_subscription(stripe_sub: Any) -> Plan:
    """Déduit le Plan depuis le premier price_id de l'abonnement Stripe."""
    try:
        price_id = stripe_sub["items"]["data"][0]["price"]["id"]
        return settings.price_plan_map.get(price_id, Plan.FREE)
    except (KeyError, IndexError, TypeError):
        return Plan.FREE


# ── Gestionnaires d'événements ────────────────────────────────────────────────

async def handle_checkout_completed(db: AsyncSession, event_data: dict) -> None:
    """checkout.session.completed — activation de l'abonnement."""
    session_obj = event_data["object"]
    org_id: str = session_obj.get("client_reference_id") or (
        session_obj.get("metadata") or {}
    ).get("org_id", "")

    if not org_id:
        logger.error("[STRIPE] checkout.session.completed sans org_id — ignoré.")
        return

    stripe_sub_id: str | None = session_obj.get("subscription")
    customer_id: str | None = session_obj.get("customer")

    # Mise à jour du stripe_customer_id sur l'organisation si absent
    if customer_id:
        result = await db.execute(
            select(Organisation).where(Organisation.id == org_id)
        )
        org = result.scalar_one_or_none()
        if org and not org.stripe_customer_id:
            org.stripe_customer_id = customer_id

    # Récupération des détails de l'abonnement Stripe pour le plan
    plan = Plan.FREE
    if stripe_sub_id:
        stripe_sub = stripe.Subscription.retrieve(
            stripe_sub_id, expand=["items.data.price"]
        )
        plan = _plan_from_stripe_subscription(stripe_sub)

    sub = await _get_or_create_subscription(db, org_id)
    sub.plan = plan
    sub.status = SubscriptionStatus.ACTIVE
    sub.stripe_subscription_id = stripe_sub_id
    sub.grace_period_until = None

    await log_action(
        db,
        action="subscription_activated",
        org_id=org_id,
        metadata={"plan": plan.value, "stripe_subscription_id": stripe_sub_id},
    )
    logger.info("[STRIPE] Abonnement activé org=%s plan=%s", org_id, plan.value)


async def handle_subscription_updated(db: AsyncSession, event_data: dict) -> None:
    """customer.subscription.created / updated."""
    stripe_sub = event_data["object"]
    customer_id: str = stripe_sub.get("customer", "")

    org = await _get_org_by_stripe_customer(db, customer_id)
    if not org:
        logger.warning(
            "[STRIPE] subscription_updated : customer %s introuvable.", customer_id
        )
        return

    plan = _plan_from_stripe_subscription(stripe_sub)
    stripe_status = stripe_sub.get("status", "")

    status_map = {
        "active": SubscriptionStatus.ACTIVE,
        "trialing": SubscriptionStatus.TRIALING,
        "past_due": SubscriptionStatus.PAST_DUE,
        "canceled": SubscriptionStatus.CANCELED,
        "incomplete": SubscriptionStatus.INCOMPLETE,
        "incomplete_expired": SubscriptionStatus.CANCELED,
    }
    new_status = status_map.get(stripe_status, SubscriptionStatus.INCOMPLETE)

    sub = await _get_or_create_subscription(db, org.id)
    old_plan = sub.plan.value

    sub.plan = plan
    sub.status = new_status
    sub.stripe_subscription_id = stripe_sub.get("id")

    if stripe_sub.get("trial_end"):
        sub.trial_ends_at = datetime.fromtimestamp(
            stripe_sub["trial_end"], tz=timezone.utc
        )

    await log_action(
        db,
        action="subscription_updated",
        org_id=org.id,
        metadata={
            "plan_before": old_plan,
            "plan_after": plan.value,
            "stripe_status": stripe_status,
        },
    )
    logger.info(
        "[STRIPE] Abonnement mis à jour org=%s plan=%s→%s status=%s",
        org.id, old_plan, plan.value, stripe_status,
    )


async def handle_subscription_deleted(db: AsyncSession, event_data: dict) -> None:
    """customer.subscription.deleted."""
    stripe_sub = event_data["object"]
    customer_id: str = stripe_sub.get("customer", "")

    org = await _get_org_by_stripe_customer(db, customer_id)
    if not org:
        return

    sub = await _get_or_create_subscription(db, org.id)
    sub.status = SubscriptionStatus.CANCELED
    sub.plan = Plan.FREE
    sub.grace_period_until = None

    await log_action(
        db,
        action="subscription_canceled",
        org_id=org.id,
        metadata={"stripe_subscription_id": stripe_sub.get("id")},
    )
    logger.info("[STRIPE] Abonnement annulé org=%s", org.id)


async def handle_invoice_paid(db: AsyncSession, event_data: dict) -> None:
    """invoice.paid — confirme le paiement et lève le délai de grâce."""
    invoice = event_data["object"]
    customer_id: str = invoice.get("customer", "")

    org = await _get_org_by_stripe_customer(db, customer_id)
    if not org:
        return

    sub = await _get_or_create_subscription(db, org.id)
    if sub.status in (SubscriptionStatus.PAST_DUE, SubscriptionStatus.GRACE_PERIOD):
        sub.status = SubscriptionStatus.ACTIVE
        sub.grace_period_until = None

        await log_action(
            db,
            action="invoice_paid_grace_lifted",
            org_id=org.id,
            metadata={"invoice_id": invoice.get("id")},
        )


async def handle_invoice_payment_failed(db: AsyncSession, event_data: dict) -> None:
    """invoice.payment_failed — passe en délai de grâce configurable."""
    invoice = event_data["object"]
    customer_id: str = invoice.get("customer", "")

    org = await _get_org_by_stripe_customer(db, customer_id)
    if not org:
        return

    sub = await _get_or_create_subscription(db, org.id)
    grace_until = datetime.now(timezone.utc) + timedelta(
        days=settings.payment_grace_period_days
    )
    sub.status = SubscriptionStatus.GRACE_PERIOD
    sub.grace_period_until = grace_until

    await log_action(
        db,
        action="invoice_payment_failed",
        org_id=org.id,
        metadata={
            "invoice_id": invoice.get("id"),
            "grace_period_until": grace_until.isoformat(),
        },
    )
    logger.warning(
        "[STRIPE] Paiement échoué org=%s — grâce jusqu'au %s",
        org.id, grace_until.date(),
    )


# ── Dispatcher principal ──────────────────────────────────────────────────────

_HANDLERS = {
    "checkout.session.completed": handle_checkout_completed,
    "customer.subscription.created": handle_subscription_updated,
    "customer.subscription.updated": handle_subscription_updated,
    "customer.subscription.deleted": handle_subscription_deleted,
    "invoice.paid": handle_invoice_paid,
    "invoice.payment_failed": handle_invoice_payment_failed,
}


async def dispatch_stripe_event(db: AsyncSession, event: stripe.Event) -> None:
    """
    Dispatche un événement Stripe vers le bon gestionnaire.
    Les événements inconnus sont ignorés silencieusement (pas de retry Stripe).
    """
    handler = _HANDLERS.get(event.type)
    if handler is None:
        logger.debug("[STRIPE] Événement non géré : %s", event.type)
        return

    await handler(db, event.data)
