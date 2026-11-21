"""
Événements Stripe traités — garantit l'idempotence.

Avant tout traitement d'un webhook, on vérifie que stripe_event_id
n'existe pas déjà. Si oui, on ignore silencieusement.
"""

from datetime import datetime

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base
from app.models._base import new_uuid


class StripeEvent(Base):
    __tablename__ = "stripe_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)

    # Identifiant Stripe (ex : "evt_1ABC...") — unique, utilisé pour la déduplication
    stripe_event_id: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False, index=True
    )
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
