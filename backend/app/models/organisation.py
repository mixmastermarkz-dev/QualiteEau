"""
Organisation — unité d'isolation multi-tenant.

Chaque abonné (particulier, entreprise, collectivité) appartient à une organisation.
Toutes les requêtes en base sont filtrées par org_id.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models._base import TimestampMixin, new_uuid

if TYPE_CHECKING:
    from app.models.user import User
    from app.models.subscription import Subscription
    from app.models.api_key import ApiKey


class Organisation(Base, TimestampMixin):
    __tablename__ = "organisations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    # Stripe — rempli lors de la création du client Stripe, jamais par le frontend
    stripe_customer_id: Mapped[str | None] = mapped_column(
        String(255), unique=True, nullable=True, index=True
    )

    # Relations
    users: Mapped[list["User"]] = relationship(
        "User", back_populates="organisation", lazy="raise"
    )
    subscription: Mapped["Subscription | None"] = relationship(
        "Subscription", back_populates="organisation", uselist=False, lazy="raise"
    )
    api_keys: Mapped[list["ApiKey"]] = relationship(
        "ApiKey", back_populates="organisation", lazy="raise"
    )
