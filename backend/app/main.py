"""
Point d'entrée FastAPI — mon-environnement.fr backend
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from app.config import settings
from app.database import engine, Base
import app.models  # noqa: F401 — enregistre tous les modèles dans Base.metadata
from app.routers import auth as auth_router
from app.routers import billing as billing_router
from app.services.billing import init_stripe


# ── Rate limiter ──────────────────────────────────────────────────────────────

limiter = Limiter(key_func=get_remote_address, default_limits=["200/minute"])


# ── Cycle de vie ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.is_production:
        settings.assert_production_secrets()

    # Initialisation du client Stripe (clé API)
    init_stripe()

    # Créer les tables si elles n'existent pas (dev uniquement — prod utilise Alembic)
    if settings.is_development:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    yield

    await engine.dispose()


# ── Application ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="mon-environnement.fr API",
    version="0.1.0",
    docs_url="/docs" if not settings.is_production else None,
    redoc_url="/redoc" if not settings.is_production else None,
    openapi_url="/openapi.json" if not settings.is_production else None,
    lifespan=lifespan,
)


# ── Middleware ────────────────────────────────────────────────────────────────

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = (
            "geolocation=(), microphone=(), camera=()"
        )
        if settings.is_production:
            response.headers["Strict-Transport-Security"] = (
                "max-age=63072000; includeSubDomains; preload"
            )
        return response


app.add_middleware(SecurityHeadersMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_origin],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Content-Type", "X-CSRF-Token"],
)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["système"])
async def health():
    """Point de santé — vérifié par le monitoring."""
    return {"status": "ok", "environment": settings.environment}


app.include_router(auth_router.router)
app.include_router(billing_router.router)


# ── Rate limit par route ──────────────────────────────────────────────────────
# Les limites spécifiques sont déclarées sur les routes via @limiter.limit(...)
# directement dans les routers. Le défaut global est 200/minute/IP.
