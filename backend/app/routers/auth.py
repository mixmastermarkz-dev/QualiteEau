"""
Routes d'authentification.

POST /auth/magic-link  — demande un lien magique (rate limited)
GET  /auth/verify      — vérifie le jeton, ouvre la session
POST /auth/logout      — révoque la session courante
POST /auth/logout-all  — révoque toutes les sessions de l'utilisateur
GET  /auth/me          — infos utilisateur courant
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.dependencies import get_current_session, get_current_user
from app.models.session import Session
from app.models.subscription import Plan, Subscription
from app.models.user import User
from app.schemas.auth import MagicLinkRequest, MagicLinkResponse, SessionOut, UserOut
from app.services.auth import (
    consume_magic_token,
    create_magic_token,
    create_session,
    get_or_create_user,
    revoke_all_sessions,
    revoke_session,
)
from app.services.csrf import generate_csrf_token, verify_csrf
from app.services.email import send_magic_link

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["authentification"])
# Note : les limites par route (5/min, 10/min) seront appliquées via un middleware
# dédié une fois que la compatibilité slowapi/FastAPI sera confirmée.
# Le global 200/min de main.py protège en attendant.


# ── Helpers cookie ────────────────────────────────────────────────────────────

def _set_session_cookie(response: Response, raw_token: str) -> None:
    """
    Pose le cookie de session.
    - Production  : __Host-session, Secure=True, SameSite=Lax, Path=/, pas de Domain
    - Développement : session, Secure=False
    """
    response.set_cookie(
        key=settings.session_cookie_name,
        value=raw_token,
        httponly=True,
        secure=settings.is_production,
        samesite="lax",
        path="/",
        max_age=settings.session_duration_days * 86400,
        # Pas d'attribut domain → le navigateur restreint au domaine exact
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(
        key=settings.session_cookie_name,
        path="/",
        secure=settings.is_production,
        samesite="lax",
    )


def _set_csrf_cookie(response: Response, user_id: str) -> None:
    """
    Cookie CSRF : lisible par JavaScript (pas HttpOnly), Secure en prod.
    Le frontend doit le lire et le renvoyer dans X-CSRF-Token.
    """
    csrf_token = generate_csrf_token(user_id)
    response.set_cookie(
        key="csrf_token",
        value=csrf_token,
        httponly=False,   # Lisible par JS — c'est intentionnel pour le double-submit
        secure=settings.is_production,
        samesite="lax",
        path="/",
        max_age=settings.session_duration_days * 86400,
    )


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post(
    "/magic-link",
    response_model=MagicLinkResponse,
    summary="Demande un lien magique de connexion",
)
async def request_magic_link(
    request: Request,
    body: MagicLinkRequest,
    db: AsyncSession = Depends(get_db),
) -> MagicLinkResponse:
    """
    Envoie un lien magique à l'adresse e-mail fournie.

    Réponse identique qu'un compte existe ou non (protection contre l'énumération).
    Rate limit : 5 requêtes / minute / IP (géré par slowapi dans main.py).
    """
    user = await get_or_create_user(db, body.email)
    raw_token = await create_magic_token(db, user)

    verify_url = str(request.base_url) + f"auth/verify?token={raw_token}"
    await send_magic_link(body.email, verify_url)

    return MagicLinkResponse()


@router.get(
    "/verify",
    summary="Vérifie le jeton magique et ouvre une session",
    response_class=RedirectResponse,
)
async def verify_magic_link(
    token: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """
    Appelé depuis le lien e-mail.
    - Vérifie le jeton (hash + TTL + usage unique).
    - Crée une session côté serveur.
    - Pose les cookies session + CSRF.
    - Redirige vers le frontend.
    """
    user = await consume_magic_token(db, token)
    if user is None:
        # Redirection vers une page d'erreur plutôt qu'une 4xx brute
        return RedirectResponse(
            url=settings.frontend_origin + "/app/login?error=lien_invalide",
            status_code=status.HTTP_302_FOUND,
        )

    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent", "")
    raw_session = await create_session(db, user, ip_address=ip, user_agent=ua)

    response = RedirectResponse(
        url=settings.frontend_origin + "/app/",
        status_code=status.HTTP_302_FOUND,
    )
    _set_session_cookie(response, raw_session)
    _set_csrf_cookie(response, user.id)

    return response


@router.post(
    "/logout",
    summary="Déconnexion (révocation de la session courante)",
    dependencies=[Depends(verify_csrf)],
)
async def logout(
    response: Response,
    db: AsyncSession = Depends(get_db),
    current_session: Session = Depends(get_current_session),
) -> dict:
    await revoke_session(db, current_session)
    _clear_session_cookie(response)
    response.delete_cookie("csrf_token", path="/")
    return {"message": "Déconnecté."}


@router.post(
    "/logout-all",
    summary="Déconnexion de tous les appareils",
    dependencies=[Depends(verify_csrf)],
)
async def logout_all(
    response: Response,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    await revoke_all_sessions(db, current_user)
    _clear_session_cookie(response)
    response.delete_cookie("csrf_token", path="/")
    return {"message": "Toutes les sessions ont été révoquées."}


@router.get(
    "/me",
    response_model=SessionOut,
    summary="Informations sur l'utilisateur connecté",
)
async def get_me(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> SessionOut:
    result = await db.execute(
        select(Subscription).where(Subscription.org_id == current_user.org_id)
    )
    sub = result.scalar_one_or_none()
    plan = sub.plan.value if sub else Plan.FREE.value

    return SessionOut(
        user=UserOut.model_validate(current_user),
        plan=plan,
    )
