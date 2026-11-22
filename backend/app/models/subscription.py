"""
Abonnement Stripe d'une organisation.

Règle critique : le plan (A/B/C) est TOUJOURS déduit du stripe_price_id
via la table de correspondance dans config.py. Il n'est JAMAIS fourni
ni accepté depuis le client.

Le champ `plan` ici est un cache mis à jour uniquement via webhook Stripe.
"""

from __future__ import annotations

import enum
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Enum, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models._base import TimestampMixin, new_uuid

if TYPE_CHECKING:
    from app.models.organisation import Organisation


class Plan(str, enum.Enum):
    FREE = "free"
    A = "A"   # Essentiel — 4,90 €/mois
    B = "B"   # Pro — 39 €/mois
    C = "C"   # Ultimate — 199 €/mois


class SubscriptionStatus(str, enum.Enum):
    TRIALING = "trialing"
    ACTIVE = "active"
    PAST_DUE = "past_due"       # Paiement échoué, délai de grâce en cours
    GRACE_PERIOD = "grace_period"  # Délai de grâce configurable avant suspension
    CANCELED = "canceled"
    INCOMPLETE = "incomplete"   # Checkout pas encore complété


class Subscription(Base, TimestampMixin):
    __tablename__ = "subscriptions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    org_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("organisations.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,   # 1 abonnement par organisation
        index=True,
    )

    # Plan — cache calculé à partir de stripe_price_id via config.PRICE_PLAN_MAP
    # Mis à jour UNIQUEMENT via webhook Stripe (jamais depuis une requête client)
    plan: Mapped[Plan] = mapped_column(
        Enum(Plan, name="plan_enum"), nullable=False, default=Plan.FREE
    )

    status: Mapped[SubscriptionStatus] = mapped_column(
        Enum(SubscriptionStatus, name="subscription_status"),
        nullable=False,
        default=SubscriptionStatus.INCOMPLETE,
    )

    # Identifiants Stripe — jamais nuls une fois l'abonnement activé
    stripe_subscription_id: Mapped[str | None] = mapped_column(
        String(255), unique=True, nullable=True, index=True
    )
    stripe_price_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Fin du délai de grâce après un échec de paiement (configurable)
    grace_period_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Fin de l'essai gratuit (formule C uniquement — 30 jours)
    trial_ends_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Circuit manuel (formule C, paiement par bon de commande)
    # Journalisé dans AuditLog
    is_manual: Mapped[bool] = mapped_column(default=False, nullable=False)

    # Relations
    organisation: Mapped["Organisation"] = relationship(
        "Organisation", back_populates="subscription", lazy="raise"
    )

    @property
    def is_active(self) -> bool:
        """Vrai si l'accès payant est accordé (actif ou délai de grâce)."""
        from datetime import timezone
        now = datetime.now(timezone.utc)
        if self.status in (SubscriptionStatus.ACTIVE, SubscriptionStatus.TRIALING):
            return True
        if self.status == SubscriptionStatus.GRACE_PERIOD:
            return self.grace_period_until is not None and self.grace_period_until > now
        return False
