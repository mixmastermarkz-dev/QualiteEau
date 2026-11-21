"""
Protection CSRF — double soumission signée (Double Submit Cookie).

Fonctionnement :
1. À la connexion, le serveur génère un jeton CSRF signé avec itsdangerous
   et le place dans un cookie LISIBLE (pas HttpOnly) nommé "csrf_token".
2. Le frontend JavaScript lit ce cookie et l'envoie dans l'en-tête X-CSRF-Token
   sur chaque requête qui modifie des données (POST, PUT, DELETE, PATCH).
3. Le serveur vérifie que le header correspond au cookie ET que la signature
   est valide (empêche la falsification du jeton).

Sécurité : un attaquant CSRF ne peut pas lire le cookie depuis une autre origine
(politique Same-Origin), et ne peut pas forger la signature sans SESSION_SECRET.
"""

from __future__ import annotations

import logging

from fastapi import Cookie, Header, HTTPException, status
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from app.config import settings

logger = logging.getLogger(__name__)

# Durée de validité d'un jeton CSRF (1 heure)
_CSRF_MAX_AGE = 3600


def _get_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(settings.session_secret, salt="csrf")


def generate_csrf_token(user_id: str) -> str:
    """Génère un jeton CSRF signé lié à l'identifiant utilisateur."""
    return _get_serializer().dumps(user_id)


def _verify_csrf_token(token: str) -> bool:
    """Vérifie la signature et l'expiration d'un jeton CSRF."""
    try:
        _get_serializer().loads(token, max_age=_CSRF_MAX_AGE)
        return True
    except (BadSignature, SignatureExpired):
        return False


async def verify_csrf(
    x_csrf_token: str | None = Header(default=None, alias="X-CSRF-Token"),
    csrf_token: str | None = Cookie(default=None),
) -> None:
    """
    Dépendance FastAPI à déclarer sur toutes les routes mutantes (POST/PUT/DELETE).

    Vérifie :
    - L'en-tête X-CSRF-Token est présent.
    - Il correspond au cookie csrf_token (double soumission).
    - Le jeton est signé valide et non expiré.
    """
    if not x_csrf_token or not csrf_token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Jeton CSRF manquant.",
        )
    if x_csrf_token != csrf_token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Jeton CSRF invalide.",
        )
    if not _verify_csrf_token(x_csrf_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Jeton CSRF invalide.",
        )
