"""
Routes HTML /app/ — pages serveur-side avec Jinja2.

GET  /app/           → tableau de bord (redirige vers /app/login si non connecté)
GET  /app/login      → page de connexion
POST /app/login      → envoie le lien magique → redirige vers /app/verify-sent
GET  /app/verify-sent→ "vérifiez votre e-mail"
GET  /app/checkout   → page de choix de formule
GET  /app/account    → page de compte
POST /app/logout     → déconnexion → redirige vers /app/login

Sécurité :
- Les données protégées sont résolues côté serveur. Seul le plan de l'utilisateur
  détermine ce qui est rendu — les `??` ne cachent pas de vraies valeurs, ils
  remplacent littéralement les données que le moteur de template n'injecte pas.
- CSRF : token lu depuis le cookie et injecté dans le contexte de chaque page.
  Les formulaires POST le soumettent via un champ caché.
- Redirect-to-login (302) au lieu de 401 JSON pour toutes les pages HTML.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Form, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models.subscription import Plan, Subscription
from app.models.user import User
from app.services.auth import (
    create_magic_token,
    create_session,
    get_or_create_user,
    get_valid_session,
    revoke_session,
    touch_session,
)
from app.services.csrf import generate_csrf_token, verify_csrf_value
from app.services.eau_potable import get_eau_commune
from app.services.email import send_magic_link

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/app", tags=["pages"])
templates = Jinja2Templates(directory="app/templates")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _set_session_cookie(response, raw_token: str) -> None:
    response.set_cookie(
        key=settings.session_cookie_name,
        value=raw_token,
        httponly=True,
        secure=settings.is_production,
        samesite="lax",
        path="/",
        max_age=settings.session_duration_days * 86400,
    )


def _clear_session_cookie(response) -> None:
    response.delete_cookie(key=settings.session_cookie_name, path="/",
                           secure=settings.is_production, samesite="lax")


def _get_csrf_token(request: Request, user_id: str = "") -> str:
    """Lit le cookie csrf_token existant ou en génère un temporaire."""
    return request.cookies.get("csrf_token") or generate_csrf_token(user_id or "anon")


async def _resolve_user_plan(
    db: AsyncSession, raw_token: Optional[str]
) -> tuple[Optional[User], str]:
    """
    Retourne (user, plan_value) depuis le cookie brut.
    Si la session est absente/expirée, retourne (None, 'free').
    """
    if not raw_token:
        return None, Plan.FREE.value

    session = await get_valid_session(db, raw_token)
    if session is None:
        return None, Plan.FREE.value

    await touch_session(db, session)

    result = await db.execute(
        select(User).where(User.id == session.user_id, User.is_active.is_(True))
    )
    user = result.scalar_one_or_none()
    if user is None:
        return None, Plan.FREE.value

    sub_result = await db.execute(
        select(Subscription).where(Subscription.org_id == user.org_id)
    )
    sub = sub_result.scalar_one_or_none()
    plan = sub.plan.value if sub else Plan.FREE.value

    return user, plan


def _mock_drought_data(plan: str) -> dict:
    """
    Données sécheresse de démonstration.
    Les champs non autorisés ne sont PAS inclus — le template affiche '??'.
    """
    has_a = plan in ("A", "B", "C")
    has_b = plan in ("B", "C")

    data = {
        "niveau_actuel": "Alerte renforcée",
        "code_couleur": "eleve",
    }
    if has_a:
        data["prev_7j"] = 72
        data["prev_7j_niveau"] = "eleve"
    else:
        data["prev_7j"] = None
        data["prev_7j_niveau"] = None
    if has_b:
        data["prev_15j"] = 68
        data["prev_30j"] = 54
    else:
        data["prev_15j"] = None
        data["prev_30j"] = None
    return data


def _mock_air_data(plan: str) -> dict:
    has_a = plan in ("A", "B", "C")
    has_b = plan in ("B", "C")

    data = {
        "atmo": 4,
        "atmo_label": "Bon",
        "zone_nom": "Montpellier Méditerranée",
    }
    if has_a:
        data["pm25"] = 8.3
        data["pm10"] = 14.1
    if has_b:
        data["no2"] = 22.4
        data["o3"] = 61.7
    return data



# ── Pages HTML ────────────────────────────────────────────────────────────────

@router.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    commune: str = "34172",  # Montpellier par défaut
    db: AsyncSession = Depends(get_db),
):
    raw_token = request.cookies.get(settings.session_cookie_name)
    user, plan = await _resolve_user_plan(db, raw_token)

    if user is None:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    csrf = _get_csrf_token(request, user.id)

    # Données eau potable réelles depuis Hub'Eau
    eau_data = await get_eau_commune(commune)

    return templates.TemplateResponse(
        "app/dashboard.html",
        {
            "request": request,
            "current_user": user,
            "email": user.email,
            "plan": plan,
            "csrf_token": csrf,
            "drought": _mock_drought_data(plan),
            "air": _mock_air_data(plan),
            "eau": eau_data,
            "eau_commune_recherchee": commune,
            "api_key": None,
        },
    )


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = ""):
    # Si déjà connecté, redirige vers le dashboard
    raw_token = request.cookies.get(settings.session_cookie_name)
    if raw_token:
        from app.database import get_db as _get_db
        # Vérification légère sans DB (simple présence du cookie)
        pass  # La redirection est gérée par le dashboard

    csrf = _get_csrf_token(request)
    return templates.TemplateResponse(
        "app/login.html",
        {"request": request, "current_user": None, "csrf_token": csrf,
         "error": error, "prefill_email": ""},
    )


@router.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    email: str = Form(...),
    csrf_token: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    # Vérification CSRF
    stored = request.cookies.get("csrf_token") or ""
    if not verify_csrf_value(stored, csrf_token):
        csrf = _get_csrf_token(request)
        return templates.TemplateResponse(
            "app/login.html",
            {"request": request, "current_user": None, "csrf_token": csrf,
             "error": "csrf", "prefill_email": email},
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )

    email = email.strip().lower()
    user = await get_or_create_user(db, email)
    raw_token = await create_magic_token(db, user)

    base = str(request.base_url).rstrip("/")
    verify_url = f"{base}/auth/verify?token={raw_token}"
    dev_link = verify_url if settings.is_development else None

    # En production, on envoie l'e-mail
    if not settings.is_development:
        await send_magic_link(email, verify_url)
    else:
        logger.info("[DEV] Magic link pour %s : %s", email, verify_url)

    csrf = _get_csrf_token(request)
    return templates.TemplateResponse(
        "app/verify_sent.html",
        {"request": request, "current_user": None, "csrf_token": csrf,
         "email": email, "dev_link": dev_link},
    )


@router.get("/verify-sent", response_class=HTMLResponse)
async def verify_sent(request: Request, email: str = ""):
    csrf = _get_csrf_token(request)
    return templates.TemplateResponse(
        "app/verify_sent.html",
        {"request": request, "current_user": None, "csrf_token": csrf,
         "email": email, "dev_link": None},
    )


@router.get("/checkout", response_class=HTMLResponse)
async def checkout_page(
    request: Request,
    plan: str = "",
    db: AsyncSession = Depends(get_db),
):
    raw_token = request.cookies.get(settings.session_cookie_name)
    user, current_plan = await _resolve_user_plan(db, raw_token)

    if user is None:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    csrf = _get_csrf_token(request, user.id)
    return templates.TemplateResponse(
        "app/checkout.html",
        {
            "request": request,
            "current_user": user,
            "email": user.email,
            "plan": current_plan,
            "current_plan": current_plan,
            "selected_plan": plan.lower(),
            "csrf_token": csrf,
        },
    )


@router.get("/account", response_class=HTMLResponse)
async def account_page(request: Request, db: AsyncSession = Depends(get_db)):
    raw_token = request.cookies.get(settings.session_cookie_name)
    user, plan = await _resolve_user_plan(db, raw_token)

    if user is None:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    # Données abonnement
    sub_result = await db.execute(
        select(Subscription).where(Subscription.org_id == user.org_id)
    )
    sub = sub_result.scalar_one_or_none()
    sub_status = sub.status.value if sub else "active"
    grace_until = None
    if sub and sub.grace_period_until:
        grace_until = sub.grace_period_until.strftime("%d/%m/%Y")

    csrf = _get_csrf_token(request, user.id)
    return templates.TemplateResponse(
        "app/account.html",
        {
            "request": request,
            "current_user": user,
            "email": user.email,
            "plan": plan,
            "sub_status": sub_status,
            "grace_until": grace_until,
            "api_key": None,
            "csrf_token": csrf,
        },
    )


@router.post("/logout")
async def logout(
    request: Request,
    csrf_token: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    # Vérification CSRF
    stored = request.cookies.get("csrf_token") or ""
    if not verify_csrf_value(stored, csrf_token):
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    raw_token = request.cookies.get(settings.session_cookie_name)
    if raw_token:
        session = await get_valid_session(db, raw_token)
        if session:
            await revoke_session(db, session)

    response = RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)
    _clear_session_cookie(response)
    response.delete_cookie("csrf_token", path="/")
    return response
