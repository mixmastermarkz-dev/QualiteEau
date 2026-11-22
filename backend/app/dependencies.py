"""
Dépendances FastAPI réutilisables.

get_current_user   — extrait et valide la session depuis le cookie.
require_plan       — vérifie que l'organisation a le plan minimum requis.
"""

from __future__ import annotations

from fastapi import Cookie, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models.session import Session
from app.models.subscription import Plan, Subscription
from app.models.user import User
from app.services.auth import get_valid_session, touch_session


# Ordre des plans pour les comparaisons hiérarchiques
_PLAN_ORDER = {Plan.FREE: 0, Plan.A: 1, Plan.B: 2, Plan.C: 3}


async def get_current_user(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> User:
    """
    Lit le cookie de session, valide la session et retourne l'utilisateur.
    Lève 401 si la session est absente, expirée ou révoquée.
    """
    raw_token: str | None = request.cookies.get(settings.session_cookie_name)

    if not raw_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentification requise.",
        )

    session = await get_valid_session(db, raw_token)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expirée ou invalide.",
        )

    # Mise à jour silencieuse de l'activité
    await touch_session(db, session)

    result = await db.execute(
        select(User).where(User.id == session.user_id, User.is_active.is_(True))
    )
    user = result.scalar_one_or_none()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Compte désactivé.",
        )

    return user


async def get_current_session(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> Session:
    """Retourne l'objet Session actif (utile pour logout)."""
    raw_token: str | None = request.cookies.get(settings.session_cookie_name)
    if not raw_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentification requise.")

    session = await get_valid_session(db, raw_token)
    if session is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expirée ou invalide.")

    return session


def require_plan(minimum: Plan):
    """
    Fabrique une dépendance qui vérifie que l'org de l'utilisateur
    a au moins le plan `minimum`.

    Usage : Depends(require_plan(Plan.B))
    """
    async def _check(
        current_user: User = Depends(get_current_user),
        db: AsyncSession = Depends(get_db),
    ) -> User:
        result = await db.execute(
            select(Subscription).where(
                Subscription.org_id == current_user.org_id
            )
        )
        sub = result.scalar_one_or_none()

        current_plan = sub.plan if sub else Plan.FREE
        if _PLAN_ORDER.get(current_plan, 0) < _PLAN_ORDER[minimum]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Cette fonctionnalité requiert le plan {minimum.value} ou supérieur.",
            )
        return current_user

    return _check
