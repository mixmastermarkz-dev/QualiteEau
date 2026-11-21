"""
Fixtures partagées pour tous les tests.

Stratégie d'isolation :
- Chaque test utilise des adresses e-mail uniques (UUID) pour éviter les collisions.
- Les sessions de test ne font pas de rollback global : les tests s'appuient sur
  des données isolées par UUID.
- Le client httpx conserve les cookies entre les requêtes d'un même test.
"""

from __future__ import annotations

# IMPORTANT : doit être défini AVANT tout import de app.database
# pour que l'engine soit créé avec NullPool dès le départ.
import os
os.environ.setdefault("TESTING", "1")

import hashlib
import hmac
import json
import time
import uuid

import pytest
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator

import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.main import app
from app.models.organisation import Organisation
from app.models.session import Session
from app.models.subscription import Plan, Subscription, SubscriptionStatus
from app.models.user import User, UserRole
from app.services.auth import create_magic_token, create_session, get_or_create_user
from app.services.csrf import generate_csrf_token


# ── Helpers ───────────────────────────────────────────────────────────────────

def unique_email() -> str:
    """Génère une adresse e-mail unique pour éviter les collisions entre tests."""
    return f"test_{uuid.uuid4().hex[:8]}@test.example"


def sign_stripe_payload(payload: bytes, secret: str, timestamp: int | None = None) -> str:
    """
    Génère un en-tête stripe-signature valide pour les tests webhook.
    Réplique l'algorithme HMAC-SHA256 de Stripe.
    """
    ts = timestamp if timestamp is not None else int(time.time())
    signed = f"{ts}.{payload.decode()}"
    sig = hmac.new(secret.encode(), signed.encode(), hashlib.sha256).hexdigest()
    return f"t={ts},v1={sig}"


TEST_WEBHOOK_SECRET = "whsec_test_secret_for_pytest_only"


@pytest.fixture
def with_stripe_webhook_secret():
    """
    Active temporairement un webhook secret de test.
    Utilise object.__setattr__ pour contourner la validation Pydantic v2.
    """
    from app.config import settings as _settings
    original = _settings.stripe_webhook_secret
    object.__setattr__(_settings, "stripe_webhook_secret", TEST_WEBHOOK_SECRET)
    yield TEST_WEBHOOK_SECRET
    object.__setattr__(_settings, "stripe_webhook_secret", original)


# ── Fixtures de base ──────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def db() -> AsyncGenerator[AsyncSession, None]:
    """Session de base de données pour manipulation directe dans les tests."""
    async with AsyncSessionLocal() as session:
        yield session


@pytest_asyncio.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    """Client HTTP sans session (anonyme)."""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
        follow_redirects=False,
    ) as ac:
        yield ac


# ── Fixtures utilisateurs ─────────────────────────────────────────────────────

async def _create_user_with_session(plan: Plan = Plan.FREE) -> dict:
    """
    Crée un utilisateur, son organisation, son abonnement et une session valide.
    Retourne un dict avec toutes les infos nécessaires aux tests.
    """
    email = unique_email()

    async with AsyncSessionLocal() as db:
        user = await get_or_create_user(db, email)
        raw_token = await create_magic_token(db, user)

        # Forcer le plan demandé
        result = await db.execute(
            select(Subscription).where(Subscription.org_id == user.org_id)
        )
        sub = result.scalar_one_or_none()
        if sub:
            sub.plan = plan
            if plan != Plan.FREE:
                sub.status = SubscriptionStatus.ACTIVE

        await db.commit()
        user_id = user.id
        org_id = user.org_id

    return {
        "email": email,
        "user_id": user_id,
        "org_id": org_id,
        "magic_token": raw_token,
        "plan": plan,
    }


@pytest_asyncio.fixture
async def user_free() -> dict:
    """Utilisateur avec plan FREE."""
    return await _create_user_with_session(Plan.FREE)


@pytest_asyncio.fixture
async def user_a() -> dict:
    """Utilisateur avec plan A (Essentiel)."""
    return await _create_user_with_session(Plan.A)


@pytest_asyncio.fixture
async def user_c() -> dict:
    """Utilisateur avec plan C (Ultimate)."""
    return await _create_user_with_session(Plan.C)


@pytest_asyncio.fixture
async def auth_client(client: AsyncClient, user_free: dict) -> dict:
    """
    Client HTTP avec une session valide (plan FREE).
    Retourne {"client": ..., "user": ..., "csrf_token": ..., "headers": ...}
    """
    response = await client.get(f"/auth/verify?token={user_free['magic_token']}")
    assert response.status_code == 302, f"Vérification échouée : {response.status_code}"

    csrf_token = client.cookies.get("csrf_token", "")
    return {
        "client": client,
        "user": user_free,
        "csrf_token": csrf_token,
        "headers": {"X-CSRF-Token": csrf_token},
    }


@pytest_asyncio.fixture
async def auth_client_a(client: AsyncClient, user_a: dict) -> dict:
    """Client authentifié avec plan A."""
    # Nouveau client pour éviter les conflits de cookies avec auth_client
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
        follow_redirects=False,
    ) as new_client:
        response = await new_client.get(f"/auth/verify?token={user_a['magic_token']}")
        assert response.status_code == 302
        csrf = new_client.cookies.get("csrf_token", "")
        yield {"client": new_client, "user": user_a, "csrf_token": csrf,
               "headers": {"X-CSRF-Token": csrf}}


@pytest_asyncio.fixture
async def auth_client_c(client: AsyncClient, user_c: dict) -> dict:
    """Client authentifié avec plan C."""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
        follow_redirects=False,
    ) as new_client:
        response = await new_client.get(f"/auth/verify?token={user_c['magic_token']}")
        assert response.status_code == 302
        csrf = new_client.cookies.get("csrf_token", "")
        yield {"client": new_client, "user": user_c, "csrf_token": csrf,
               "headers": {"X-CSRF-Token": csrf}}
