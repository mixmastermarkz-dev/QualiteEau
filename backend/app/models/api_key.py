"""
Clé d'API (formule C uniquement).

Règles de sécurité :
- Seul le hash SHA-256 de la clé est stocké.
- Le préfixe (8 caractères) est visible pour que l'utilisateur identifie sa clé.
- Format de la clé brute : "menv_{prefix}_{secret_urlsafe_32bytes}"
- La clé brute n'est affichée qu'une seule fois à la création.
- Quota par clé, révocation individuelle.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models._base import TimestampMixin, new_uuid

if TYPE_CHECKING:
    from app.models.organisation import Organisation


class ApiKey(Base, TimestampMixin):
    __tablename__ = "api_keys"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    org_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("organisations.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Label donné par l'utilisateur (ex : "script-cron-serveur")
    name: Mapped[str] = mapped_column(String(100), nullable=False)

    # Préfixe visible (8 chars) — affiché dans l'UI pour identifier la clé
    key_prefix: Mapped[str] = mapped_column(String(8), nullable=False)

    # Hash SHA-256 de la clé brute complète — jamais la clé elle-même
    key_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)

    # Quota quotidien (0 = illimité)
    quota_per_day: Mapped[int] = mapped_column(Integer, nullable=False, default=1000)
    requests_today: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    quota_reset_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relations
    organisation: Mapped["Organisation"] = relationship(
        "Organisation", back_populates="api_keys", lazy="raise"
    )

    @property
    def is_active(self) -> bool:
        return self.revoked_at is None
