"""
Routes de facturation Stripe.

POST /billing/webhook        — webhook Stripe (auth par signature, pas par cookie)
POST /billing/checkout       — crée une session Checkout
GET  /billing/checkout/success — vérifie l'état d'une session Checkout
POST /billing/portal         — crée une session du portail client Stripe
GET  /billing/subscription   — état de l'abonnement courant

Règles de sécurité :
- Le webhook lit le corps brut AVANT tout parsing JSON (requis pour la signature).
- La signature Stripe est vérifiée avec stripe.Webhook.construct_event
  (tolérance d'horodatage par défaut = 300 s — protège contre le rejeu).
- Chaque événement est idempotent (table stripe_events).
- Le price_id est toujours choisi côté serveur depuis la config.
- La page de succès récupère l'état depuis l'API Stripe, jamais depuis l'URL.
"""

from __future__ import annotations

import logging

import stripe
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.dependencies import get_current_user
from app.models.organisation import Organisation
from app.models.subscription import Plan, Subscription
from app.models.user import User
from app.schemas.billing import (
    CheckoutRequest,
    CheckoutResponse,
    CheckoutSuccessResponse,
    PortalResponse,
    SubscriptionStatusResponse,
)
from app.services.billing import (
    create_checkout_session,
    create_portal_session,
    dispatch_stripe_event,
    is_event_already_processed,
    mark_event_processed,
    retrieve_checkout_session,
)
from app.services.csrf import verify_csrf

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/billing", tags=["facturation"])


# ── Helper ────────────────────────────────────────────────────────────────────

async def _get_org(db: AsyncSession, org_id: str) -> Organisation:
    result = await db.execute(
        select(Organisation).where(Organisation.id == org_id)
    )
    org = result.scalar_one_or_none()
    if org is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organisation introuvable.")
    return org


# ── Webhook Stripe ────────────────────────────────────────────────────────────

@router.post(
    "/webhook",
    summary="Webhook Stripe — NE PAS APPELER MANUELLEMENT",
    include_in_schema=False,  # Masqué du Swagger
)
async def stripe_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Point d'entrée des événements Stripe.

    1. Lit le corps brut (obligatoire pour la vérification de signature).
    2. Vérifie la signature HMAC avec stripe-signature + stripe_webhook_secret.
    3. Vérifie l'idempotence (stripe_event_id déjà traité → ignore).
    4. Dispatche vers le gestionnaire approprié.
    5. Retourne toujours 200 pour les événements non gérés (évite les retries inutiles).
    """
    if not settings.stripe_webhook_secret:
        logger.error("[STRIPE] STRIPE_WEBHOOK_SECRET non configurée — webhook rejeté.")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook non configuré.",
        )

    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, settings.stripe_webhook_secret
            # La tolérance d'horodatage par défaut (300 s) protège contre le rejeu
        )
    except stripe.error.SignatureVerificationError:
        logger.warning("[STRIPE] Signature webhook invalide.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Signature invalide.",
        )
    except Exception as exc:
        logger.error("[STRIPE] Erreur parsing webhook : %s", exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Payload invalide.",
        )

    # Idempotence
    if await is_event_already_processed(db, event.id):
        logger.info("[STRIPE] Événement déjà traité : %s", event.id)
        return {"status": "already_processed"}

    await mark_event_processed(db, event.id, event.type)
    await dispatch_stripe_event(db, event)

    return {"status": "ok"}


# ── Checkout ──────────────────────────────────────────────────────────────────

@router.post(
    "/checkout",
    response_model=CheckoutResponse,
    summary="Crée une session Checkout Stripe",
    dependencies=[Depends(verify_csrf)],
)
async def create_checkout(
    request: Request,
    body: CheckoutRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> CheckoutResponse:
    """
    Crée une session Stripe Checkout et retourne l'URL de redirection.
    Le price_id est choisi côté serveur depuis la formule demandée.
    """
    if not settings.stripe_secret_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Paiement non disponible en développement sans STRIPE_SECRET_KEY.",
        )

    org = await _get_org(db, current_user.org_id)

    try:
        checkout_url = await create_checkout_session(db, current_user, org, body.plan)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    return CheckoutResponse(checkout_url=checkout_url)


@router.get(
    "/checkout/success",
    response_model=CheckoutSuccessResponse,
    summary="Vérifie l'état d'une session Checkout (depuis l'API Stripe)",
)
async def checkout_success(
    session_id: str,
    current_user: User = Depends(get_current_user),
) -> CheckoutSuccessResponse:
    """
    Appelé par le frontend après le retour de Stripe.
    Vérifie l'état via l'API Stripe — ne fait JAMAIS confiance aux paramètres URL.
    """
    if not settings.stripe_secret_key:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail="Paiement non disponible en dev.")
    try:
        result = await retrieve_checkout_session(session_id)
    except stripe.error.StripeError as exc:
        logger.warning("[STRIPE] retrieve_checkout_session error : %s", exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Session introuvable.")

    return CheckoutSuccessResponse(**result)


# ── Portail client ────────────────────────────────────────────────────────────

@router.post(
    "/portal",
    response_model=PortalResponse,
    summary="Crée une session Stripe Billing Portal",
    dependencies=[Depends(verify_csrf)],
)
async def billing_portal(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> PortalResponse:
    """
    Redirige l'utilisateur vers le portail Stripe pour gérer son abonnement.
    """
    if not settings.stripe_secret_key:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail="Paiement non disponible en dev.")

    org = await _get_org(db, current_user.org_id)

    try:
        portal_url = await create_portal_session(org)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    return PortalResponse(portal_url=portal_url)


# ── État de l'abonnement ──────────────────────────────────────────────────────

@router.get(
    "/subscription",
    response_model=SubscriptionStatusResponse,
    summary="État de l'abonnement courant",
)
async def get_subscription_status(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> SubscriptionStatusResponse:
    result = await db.execute(
        select(Subscription).where(Subscription.org_id == current_user.org_id)
    )
    sub = result.scalar_one_or_none()

    if sub is None:
        return SubscriptionStatusResponse(
            plan=Plan.FREE.value,
            status="active",
            grace_period_until=None,
            trial_ends_at=None,
        )

    return SubscriptionStatusResponse(
        plan=sub.plan.value,
        status=sub.status.value,
        grace_period_until=(
            sub.grace_period_until.isoformat() if sub.grace_period_until else None
        ),
        trial_ends_at=(
            sub.trial_ends_at.isoformat() if sub.trial_ends_at else None
        ),
    )
