"""
Import centralisé de tous les modèles.

Nécessaire pour qu'Alembic détecte toutes les tables lors de l'autogenerate.
"""

from app.models.organisation import Organisation
from app.models.user import User, UserRole
from app.models.magic_token import MagicToken
from app.models.session import Session
from app.models.subscription import Subscription, Plan, SubscriptionStatus
from app.models.stripe_event import StripeEvent
from app.models.api_key import ApiKey
from app.models.audit_log import AuditLog
from app.models.prediction import ZoneAlerte, ArreteSecheresse, ModelVersion, Prediction

__all__ = [
    "Organisation",
    "User",
    "UserRole",
    "MagicToken",
    "Session",
    "Subscription",
    "Plan",
    "SubscriptionStatus",
    "StripeEvent",
    "ApiKey",
    "AuditLog",
    "ZoneAlerte",
    "ArreteSecheresse",
    "ModelVersion",
    "Prediction",
]
