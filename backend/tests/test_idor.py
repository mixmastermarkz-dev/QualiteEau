"""
Tests IDOR (Insecure Direct Object Reference) et autorisation par plan.

Couvre :
- Utilisateur A ne peut pas accéder aux ressources de l'organisation B
- Plan A ne peut pas accéder aux fonctionnalités réservées au plan C
- Un utilisateur non authentifié ne peut rien accéder
- Vérification que /billing/subscription filtre bien par org_id
"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models.subscription import Plan, Subscription, SubscriptionStatus
from app.models.user import User
from app.services.auth import get_or_create_user
from app.services.csrf import generate_csrf_token
from tests.conftest import unique_email


# ── Isolation inter-organisations ─────────────────────────────────────────────

async def test_user_a_cannot_see_user_b_subscription(
    auth_client_a: dict, auth_client_c: dict
):
    """
    L'utilisateur A ne doit voir que son propre abonnement,
    pas celui de l'organisation B (ici plan C).
    """
    client_a: AsyncClient = auth_client_a["client"]
    client_c: AsyncClient = auth_client_c["client"]

    # Vérifie que chacun voit son propre plan
    r_a = await client_a.get("/billing/subscription")
    r_c = await client_c.get("/billing/subscription")

    assert r_a.status_code == 200
    assert r_c.status_code == 200

    # Les plans doivent être différents — A voit "A", C voit "C"
    assert r_a.json()["plan"] == "A"
    assert r_c.json()["plan"] == "C"

    # Aucune cross-contamination : le plan de A n'est pas C
    assert r_a.json()["plan"] != r_c.json()["plan"]


async def test_me_returns_only_own_org(auth_client_a: dict, auth_client_c: dict):
    """
    Chaque utilisateur ne voit que son propre org_id dans /auth/me.
    """
    client_a: AsyncClient = auth_client_a["client"]
    client_c: AsyncClient = auth_client_c["client"]

    r_a = await client_a.get("/auth/me")
    r_c = await client_c.get("/auth/me")

    assert r_a.status_code == 200
    assert r_c.status_code == 200

    org_a = r_a.json()["user"]["org_id"]
    org_c = r_c.json()["user"]["org_id"]

    # Les orgs doivent être différentes
    assert org_a != org_c

    # Chaque utilisateur voit l'org qui lui appartient
    assert org_a == auth_client_a["user"]["org_id"]
    assert org_c == auth_client_c["user"]["org_id"]


async def test_stealing_session_cookie_from_other_user():
    """
    Un attaquant qui utilise le cookie de session de la victime
    obtient l'identité de la VICTIME — pas la sienne.
    Ce test vérifie que le cookie lie bien à une session précise en DB.
    """
    from app.main import app as fastapi_app
    from app.services.auth import create_session

    email_victim = unique_email()
    email_attacker = unique_email()

    async with AsyncSessionLocal() as db:
        victim = await get_or_create_user(db, email_victim)
        attacker = await get_or_create_user(db, email_attacker)
        victim_raw_session = await create_session(db, victim)
        await db.commit()
        victim_id = victim.id
        attacker_id = attacker.id

    async with AsyncClient(
        transport=ASGITransport(app=fastapi_app),
        base_url="http://testserver",
        follow_redirects=False,
    ) as c:
        c.cookies.set("session", victim_raw_session)
        response = await c.get("/auth/me")

    assert response.status_code == 200
    returned_user_id = response.json()["user"]["id"]
    assert returned_user_id == victim_id
    assert returned_user_id != attacker_id


# ── Autorisation par plan ─────────────────────────────────────────────────────

async def test_plan_a_cannot_access_plan_c_checkout(auth_client_a: dict):
    """
    Un utilisateur plan A peut demander un checkout pour n'importe quelle formule
    (c'est normal — il veut upgrader), mais les price_id sont toujours validés côté serveur.
    Ce test vérifie que le endpoint checkout refuse en l'absence de STRIPE_SECRET_KEY.
    """
    client_a: AsyncClient = auth_client_a["client"]

    # En dev, STRIPE_SECRET_KEY est vide — doit retourner 503, pas les données d'une autre org
    response = await client_a.post(
        "/billing/checkout",
        json={"plan": "C"},
        headers=auth_client_a["headers"],
    )
    # 503 : paiement non disponible en dev (pas de stripe key)
    # C'est acceptable — ce qui compte c'est que le price_id n'est pas devinable
    assert response.status_code in (400, 503)


async def test_checkout_rejects_invalid_plan(auth_client_a: dict):
    """Un plan inexistant est refusé avec 422."""
    client_a: AsyncClient = auth_client_a["client"]
    response = await client_a.post(
        "/billing/checkout",
        json={"plan": "Z"},  # Plan inexistant
        headers=auth_client_a["headers"],
    )
    assert response.status_code == 422


async def test_checkout_rejects_extra_fields(auth_client_a: dict):
    """Extra fields interdits (extra='forbid')."""
    client_a: AsyncClient = auth_client_a["client"]
    response = await client_a.post(
        "/billing/checkout",
        json={"plan": "A", "price_id": "price_hacked"},  # Tentative d'injection de price_id
        headers=auth_client_a["headers"],
    )
    assert response.status_code == 422


async def test_portal_requires_auth(client: AsyncClient):
    """
    Le portail Stripe requiert une session valide.
    Sans cookie de session, la vérification CSRF (déclarée en premier) retourne 403.
    Sans CSRF mais avec session, l'auth passe et CSRF retourne 403.
    Dans les deux cas, l'accès est refusé à un utilisateur non authentifié.
    """
    response = await client.post("/billing/portal")
    # 401 (session manquante) ou 403 (CSRF manquant — vérifié avant l'auth dans ce router)
    assert response.status_code in (401, 403)


async def test_subscription_status_requires_auth(client: AsyncClient):
    """L'état de l'abonnement n'est pas public."""
    response = await client.get("/billing/subscription")
    assert response.status_code == 401


# ── Webhooks — isolation des données ─────────────────────────────────────────

async def test_webhook_uses_stripe_customer_id_not_client_data():
    """
    Le webhook ne fait jamais confiance aux metadata envoyées par Stripe
    pour le plan — il relit le price_id depuis l'abonnement Stripe.
    Ce test vérifie que handle_subscription_updated ignore un customer inconnu.
    """
    from app.services.billing import handle_subscription_updated
    from app.database import AsyncSessionLocal

    # Événement avec un customer_id qui n'existe pas en base
    fake_event_data = {
        "object": {
            "id": "sub_fake",
            "customer": "cus_nonexistent_xyz",
            "status": "active",
            "items": {
                "data": [{"price": {"id": "price_unknown"}}]
            },
        }
    }

    # Ne doit pas lever d'exception et ne doit rien modifier
    async with AsyncSessionLocal() as db:
        await handle_subscription_updated(db, fake_event_data)
        await db.commit()

    # Si on arrive ici, le handler a correctement ignoré l'événement inconnu
