"""
Journal d'audit non modifiable.

Actions tracées : connexion, déconnexion, changement de formule,
action d'admin, création/révocation de clé API, paiement, accès refusé.

Règles :
- Pas de données personnelles ni de secrets dans les champs.
- Pas de UPDATE ni DELETE sur cette table (enforcement applicatif).
- actor_user_id peut être null pour les actions système (webhooks Stripe, tâches cron).
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base
from app.models._base import new_uuid


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)

    # Auteur — null pour les actions système
    actor_user_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # Organisation concernée (null pour les actions système transverses)
    org_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("organisations.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    action: Mapped[str] = mapped_column(String(100), nullable=False, index=True)

    # Type et identifiant de la ressource ciblée (ex : "subscription", "api_key")
    target_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    target_id: Mapped[str | None] = mapped_column(String(36), nullable=True)

    # Contexte libre — JSON encodé manuellement, sans PII ni secret
    # Exemple : '{"plan_before": "A", "plan_after": "B"}'
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    # IP hashée (SHA-256) — permet de détecter une anomalie sans stocker l'IP brute
    ip_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
