"""
Service d'authentification.

Règles de sécurité respectées :
- Jeton magique : seul le hash SHA-256 est stocké.
- Session : seul le hash SHA-256 du jeton cookie est stocké.
- Nouvel identifiant de session à chaque connexion (fixation impossible).
- Messages d'erreur identiques qu'un compte existe ou non (pas d'énumération).
- Un compte inexistant crée silencieusement un utilisateur + org + abonnement FREE.
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.magic_token import MagicToken
from app.models.organisation import Organisation
from app.models.session import Session
from app.models.subscription import Plan, Subscription, SubscriptionStatus
from app.models.user import User, UserRole


# ── Helpers ──────────────────────────────────────────────────────────────────

def hash_token(raw: str) -> str:
    """Hash SHA-256 d'un jeton brut — seule valeur persistée en base."""
    return hashlib.sha256(raw.encode()).hexdigest()


def generate_raw_token() -> str:
    """Génère un jeton brut cryptographiquement sûr (256 bits)."""
    return secrets.token_urlsafe(32)


# ── Utilisateurs ─────────────────────────────────────────────────────────────

async def get_or_create_user(db: AsyncSession, email: str) -> User:
    """
    Retourne l'utilisateur existant ou en crée un nouveau.

    À la création : organisation individuelle + abonnement FREE + rôle ADMIN.
    La réponse REST est identique dans les deux cas (pas d'énumération des comptes).
    """
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()

    if user is not None:
        return user

    # Nouvel utilisateur — organisation dédiée
    org = Organisation(name=email.split("@")[0])
    db.add(org)
    await db.flush()

    user = User(
        org_id=org.id,
        email=email,
        role=UserRole.ADMIN,  # Premier utilisateur = admin de sa propre org
    )
    db.add(user)

    # Abonnement FREE par défaut
    sub = Subscription(
        org_id=org.id,
        plan=Plan.FREE,
        status=SubscriptionStatus.ACTIVE,
    )
    db.add(sub)
    await db.flush()

    return user


# ── Lien magique ──────────────────────────────────────────────────────────────

async def create_magic_token(db: AsyncSession, user: User) -> str:
    """
    Génère un jeton de lien magique.
    Retourne le jeton BRUT à envoyer par e-mail — jamais stocké ni logué.
    """
    raw = generate_raw_token()
    now = datetime.now(timezone.utc)

    token = MagicToken(
        user_id=user.id,
        token_hash=hash_token(raw),
        expires_at=now + timedelta(minutes=settings.magic_link_ttl_minutes),
        created_at=now,
    )
    db.add(token)
    await db.flush()

    return raw


async def consume_magic_token(db: AsyncSession, raw: str) -> Optional[User]:
    """
    Vérifie et consomme un jeton magique (usage unique).

    Retourne l'utilisateur si tout est valide, None sinon.
    La raison de l'échec n'est jamais exposée à l'appelant.
    """
    token_hash = hash_token(raw)
    now = datetime.now(timezone.utc)

    result = await db.execute(
        select(MagicToken).where(MagicToken.token_hash == token_hash)
    )
    magic_token = result.scalar_one_or_none()

    # Vérifications dans l'ordre — même timing pour chaque cas
    if magic_token is None or magic_token.used_at is not None or magic_token.expires_at < now:
        return None

    # Consommation immédiate (usage unique)
    magic_token.used_at = now

    result = await db.execute(
        select(User).where(User.id == magic_token.user_id, User.is_active.is_(True))
    )
    return result.scalar_one_or_none()


# ── Sessions ──────────────────────────────────────────────────────────────────

async def create_session(
    db: AsyncSession,
    user: User,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> str:
    """
    Crée une session côté serveur.
    Retourne le jeton BRUT à placer dans le cookie HttpOnly.
    """
    raw = generate_raw_token()
    now = datetime.now(timezone.utc)

    session = Session(
        user_id=user.id,
        token_hash=hash_token(raw),
        created_at=now,
        last_active_at=now,
        expires_at=now + timedelta(days=settings.session_duration_days),
        ip_address=ip_address,
        user_agent=(user_agent or "")[:255],
    )
    db.add(session)
    await db.flush()

    return raw


async def get_valid_session(db: AsyncSession, raw: str) -> Optional[Session]:
    """Retrouve une session valide et non révoquée depuis son jeton brut."""
    token_hash = hash_token(raw)
    now = datetime.now(timezone.utc)

    result = await db.execute(
        select(Session).where(
            Session.token_hash == token_hash,
            Session.revoked_at.is_(None),
            Session.expires_at > now,
        )
    )
    session = result.scalar_one_or_none()
    if session is None:
        return None

    # Vérification du délai d'inactivité
    inactivity_deadline = session.last_active_at + timedelta(
        days=settings.session_inactivity_days
    )
    if inactivity_deadline < now:
        session.revoked_at = now
        return None

    return session


async def touch_session(db: AsyncSession, session: Session) -> None:
    """Met à jour last_active_at (à appeler sur chaque requête authentifiée)."""
    session.last_active_at = datetime.now(timezone.utc)


async def revoke_session(db: AsyncSession, session: Session) -> None:
    """Révoque une session (déconnexion)."""
    session.revoked_at = datetime.now(timezone.utc)


async def revoke_all_sessions(db: AsyncSession, user: User) -> None:
    """Révoque toutes les sessions actives d'un utilisateur (déconnexion globale)."""
    now = datetime.now(timezone.utc)
    result = await db.execute(
        select(Session).where(
            Session.user_id == user.id,
            Session.revoked_at.is_(None),
            Session.expires_at > now,
        )
    )
    for session in result.scalars().all():
        session.revoked_at = now
