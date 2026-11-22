"""
Jeton de lien magique.

Règles de sécurité :
- Seul le hash SHA-256 est stocké en base.
- Usage unique : used_at est défini à la consommation.
- Validité : 15 minutes.
- Le jeton brut est envoyé par e-mail uniquement, jamais logué.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models._base import new_uuid

if TYPE_CHECKING:
    from app.models.user import User


class MagicToken(Base):
    __tablename__ = "magic_tokens"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Seul le hash est persisté — le jeton brut n'est JAMAIS stocké
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)

    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Relations
    user: Mapped["User"] = relationship("User", back_populates="magic_tokens", lazy="raise")
