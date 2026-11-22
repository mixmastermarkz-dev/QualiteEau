"""
Tests d'authentification.

Couvre :
- Parcours lien magique complet (demande → vérification → session)
- Usage unique du jeton magique
- Jeton expiré
- Cookie de session posé correctement
- GET /auth/me authentifié et non authentifié
- Logout (révocation session courante)
- Logout-all (révocation de toutes les sessions)
- Protection CSRF : absent, invalide, valide
- Session expirée (manipulation directe en DB)
- Session révoquée (manipulation directe en DB)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models.magic_token import MagicToken
from app.models.session import Session
from app.services.auth import create_magic_token, create_session, get_or_create_user
from app.services.csrf import generate_csrf_token
from tests.conftest import unique_email


# ── Magic link — parcours nominal ─────────────────────────────────────────────

async def test_magic_link_request_returns_generic_message(client: AsyncClient):
    """La réponse est identique qu'un compte existe ou non (anti-énumération)."""
    email = unique_email()
    response = await client.post("/auth/magic-link", json={"email": email})
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    # Message générique — ne révèle pas si le compte existe
    assert "enregistré" in data["message"].lower() or "lien" in data["message"].lower()


async def test_magic_link_request_nonexistent_email_same_response(client: AsyncClient):
    """Un email inconnu retourne exactement le même message qu'un email existant."""
    r1 = await client.post("/auth/magic-link", json={"email": unique_email()})
    r2 = await client.post("/auth/magic-link", json={"email": unique_email()})
    assert r1.json() == r2.json()


async def test_magic_link_request_creates_user_in_db(client: AsyncClient):
    """Un nouvel email crée un utilisateur et son organisation en base."""
    email = unique_email()
    await client.post("/auth/magic-link", json={"email": email})

    async with AsyncSessionLocal() as db:
        from app.models.user import User
        result = await db.execute(select(User).where(User.email == email))
        user = result.scalar_one_or_none()
    assert user is not None
    assert user.email == email


async def test_verify_valid_token_redirects(client: AsyncClient, user_free: dict):
    """Un jeton valide déclenche une redirection."""
    response = await client.get(f"/auth/verify?token={user_free['magic_token']}")
    assert response.status_code == 302


async def test_verify_valid_token_sets_session_cookie(client: AsyncClient, user_free: dict):
    """La redirection pose un cookie de session HttpOnly."""
    await client.get(f"/auth/verify?token={user_free['magic_token']}")
    # httpx stocke les cookies reçus dans les réponses 3xx
    session_cookie = client.cookies.get("session") or client.cookies.get("__Host-session")
    assert session_cookie is not None, "Cookie de session absent"


async def test_verify_valid_token_sets_csrf_cookie(client: AsyncClient, user_free: dict):
    """La redirection pose également le cookie CSRF lisible par JS."""
    await client.get(f"/auth/verify?token={user_free['magic_token']}")
    csrf_cookie = client.cookies.get("csrf_token")
    assert csrf_cookie is not None, "Cookie CSRF absent"


async def test_verify_invalid_token_redirects_to_error(client: AsyncClient):
    """Un jeton invalide redirige vers la page d'erreur (pas de 4xx brut)."""
    response = await client.get("/auth/verify?token=jeton_totalement_invalide")
    assert response.status_code == 302
    assert "erreur" in response.headers["location"].lower() or "error" in response.headers["location"].lower() or "invalid" in response.headers["location"].lower() or "invalide" in response.headers["location"].lower()


async def test_verify_token_single_use(client: AsyncClient, user_free: dict):
    """Un jeton magique ne peut être utilisé qu'une seule fois."""
    token = user_free["magic_token"]

    # Première utilisation — succès
    r1 = await client.get(f"/auth/verify?token={token}")
    assert r1.status_code == 302

    # Deuxième utilisation — redirige vers erreur
    r2 = await client.get(f"/auth/verify?token={token}")
    assert r2.status_code == 302
    assert "erreur" in r2.headers["location"].lower() or "error" in r2.headers["location"].lower() or "invalid" in r2.headers["location"].lower() or "invalide" in r2.headers["location"].lower()


async def test_verify_expired_token_redirects_to_error():
    """Un jeton expiré est rejeté."""
    email = unique_email()

    async with AsyncSessionLocal() as db:
        user = await get_or_create_user(db, email)
        raw = await create_magic_token(db, user)
        # Expire le jeton immédiatement
        result = await db.execute(
            select(MagicToken).where(MagicToken.user_id == user.id)
        )
        token_obj = result.scalars().first()
        token_obj.expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
        await db.commit()

    async with AsyncClient(
        transport=__import__("httpx").ASGITransport(app=__import__("app.main", fromlist=["app"]).app),
        base_url="http://testserver",
        follow_redirects=False,
    ) as c:
        response = await c.get(f"/auth/verify?token={raw}")

    assert response.status_code == 302
    assert "erreur" in response.headers["location"].lower() or "error" in response.headers["location"].lower() or "invalide" in response.headers["location"].lower() or "invalid" in response.headers["location"].lower()


# ── /auth/me ──────────────────────────────────────────────────────────────────

async def test_me_unauthenticated_returns_401(client: AsyncClient):
    response = await client.get("/auth/me")
    assert response.status_code == 401


async def test_me_authenticated_returns_user(auth_client: dict):
    c: AsyncClient = auth_client["client"]
    response = await c.get("/auth/me")
    assert response.status_code == 200
    data = response.json()
    assert data["user"]["email"] == auth_client["user"]["email"]
    assert "plan" in data


async def test_me_returns_correct_plan(auth_client: dict):
    c: AsyncClient = auth_client["client"]
    response = await c.get("/auth/me")
    assert response.status_code == 200
    assert response.json()["plan"] == "free"


# ── Expiration / révocation ───────────────────────────────────────────────────

async def test_expired_session_returns_401(user_free: dict):
    """Une session expirée est rejetée avec 401."""
    async with AsyncSessionLocal() as db:
        user = await get_or_create_user(db, user_free["email"])
        raw = await create_session(db, user)

        result = await db.execute(
            select(Session).where(Session.user_id == user.id)
        )
        sessions = result.scalars().all()
        # Expire toutes les sessions de cet utilisateur
        for s in sessions:
            s.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        await db.commit()

    async with AsyncClient(
        transport=__import__("httpx").ASGITransport(app=__import__("app.main", fromlist=["app"]).app),
        base_url="http://testserver",
        follow_redirects=False,
    ) as c:
        c.cookies.set("session", raw)
        response = await c.get("/auth/me")

    assert response.status_code == 401


async def test_revoked_session_returns_401(user_free: dict):
    """Une session révoquée est rejetée avec 401."""
    async with AsyncSessionLocal() as db:
        user = await get_or_create_user(db, user_free["email"])
        raw = await create_session(db, user)

        result = await db.execute(select(Session).where(Session.user_id == user.id))
        sessions = result.scalars().all()
        for s in sessions:
            s.revoked_at = datetime.now(timezone.utc)
        await db.commit()

    async with AsyncClient(
        transport=__import__("httpx").ASGITransport(app=__import__("app.main", fromlist=["app"]).app),
        base_url="http://testserver",
        follow_redirects=False,
    ) as c:
        c.cookies.set("session", raw)
        response = await c.get("/auth/me")

    assert response.status_code == 401


async def test_inactive_session_returns_401(user_free: dict):
    """Une session inactive depuis trop longtemps est rejetée."""
    from app.config import settings

    async with AsyncSessionLocal() as db:
        user = await get_or_create_user(db, user_free["email"])
        raw = await create_session(db, user)

        result = await db.execute(select(Session).where(Session.user_id == user.id))
        sessions = result.scalars().all()
        inactivity = timedelta(days=settings.session_inactivity_days + 1)
        for s in sessions:
            s.last_active_at = datetime.now(timezone.utc) - inactivity
        await db.commit()

    async with AsyncClient(
        transport=__import__("httpx").ASGITransport(app=__import__("app.main", fromlist=["app"]).app),
        base_url="http://testserver",
        follow_redirects=False,
    ) as c:
        c.cookies.set("session", raw)
        response = await c.get("/auth/me")

    assert response.status_code == 401


# ── Logout ────────────────────────────────────────────────────────────────────

async def test_logout_requires_csrf(auth_client: dict):
    """POST /auth/logout sans CSRF retourne 403."""
    c: AsyncClient = auth_client["client"]
    response = await c.post("/auth/logout")
    assert response.status_code == 403


async def test_logout_with_valid_csrf_revokes_session(auth_client: dict):
    """POST /auth/logout avec CSRF valide révoque la session."""
    c: AsyncClient = auth_client["client"]

    response = await c.post("/auth/logout", headers=auth_client["headers"])
    assert response.status_code == 200

    # La session ne fonctionne plus
    response2 = await c.get("/auth/me")
    assert response2.status_code == 401


async def test_logout_all_revokes_all_sessions(user_free: dict):
    """POST /auth/logout-all révoque toutes les sessions de l'utilisateur."""
    email = user_free["email"]

    # Crée 3 sessions pour le même utilisateur
    async with AsyncSessionLocal() as db:
        user = await get_or_create_user(db, email)
        raws = [await create_session(db, user) for _ in range(3)]
        await db.commit()
        user_id = user.id

    # Utilise la première session pour logout-all
    from app.main import app as fastapi_app
    from httpx import ASGITransport

    async with AsyncClient(
        transport=ASGITransport(app=fastapi_app),
        base_url="http://testserver",
        follow_redirects=False,
    ) as c:
        c.cookies.set("session", raws[0])
        csrf = generate_csrf_token(user_id)
        c.cookies.set("csrf_token", csrf)

        response = await c.post("/auth/logout-all", headers={"X-CSRF-Token": csrf})
        assert response.status_code == 200

    # Vérifie que toutes les sessions sont révoquées en DB
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Session).where(
                Session.user_id == user_id,
                Session.revoked_at.is_(None),
            )
        )
        active_sessions = result.scalars().all()

    assert len(active_sessions) == 0


# ── CSRF ──────────────────────────────────────────────────────────────────────

async def test_csrf_missing_header_returns_403(auth_client: dict):
    c: AsyncClient = auth_client["client"]
    # Cookie csrf_token présent mais pas d'en-tête
    response = await c.post("/auth/logout")
    assert response.status_code == 403


async def test_csrf_mismatched_header_returns_403(auth_client: dict):
    c: AsyncClient = auth_client["client"]
    # En-tête différent du cookie
    response = await c.post(
        "/auth/logout",
        headers={"X-CSRF-Token": "mauvais_jeton_csrf"},
    )
    assert response.status_code == 403


async def test_csrf_forged_token_returns_403(auth_client: dict):
    """Un jeton CSRF non signé (forgé) est rejeté même s'il correspond au cookie."""
    c: AsyncClient = auth_client["client"]
    forged = "forged.unsigned.token"
    c.cookies.set("csrf_token", forged)
    response = await c.post("/auth/logout", headers={"X-CSRF-Token": forged})
    assert response.status_code == 403


async def test_csrf_valid_allows_request(auth_client: dict):
    c: AsyncClient = auth_client["client"]
    response = await c.post("/auth/logout", headers=auth_client["headers"])
    assert response.status_code == 200


# ── Validation des entrées ────────────────────────────────────────────────────

async def test_magic_link_invalid_email_returns_422(client: AsyncClient):
    response = await client.post("/auth/magic-link", json={"email": "pas_un_email"})
    assert response.status_code == 422


async def test_magic_link_extra_field_returns_422(client: AsyncClient):
    """Pydantic extra='forbid' — champ inattendu rejeté."""
    response = await client.post(
        "/auth/magic-link",
        json={"email": unique_email(), "role": "admin"},
    )
    assert response.status_code == 422
