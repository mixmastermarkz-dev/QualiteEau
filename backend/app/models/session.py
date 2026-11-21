"""
Session côté serveur.

Règles de sécurité :
- L'identifiant brut de session est dans le cookie uniquement.
- Seul son hash SHA-256 est stocké en base.
- Nouvel identifiant à chaque connexion (fixation de session impossible).
- Expiration absolue + expiration par inactivité.
- Révocable individuellement ou en masse (déconnexion de tous les appareils).
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models._base import new_uuid

if TYPE_CHECKING:
    from app.models.user import User


class Session(Base):
    __tablename__ = "sessions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Hash SHA-256 du jeton brut contenu dans le cookie — jamais le jeton lui-même
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_active_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Expiration absolue (ex : 30 jours même si actif)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Contexte d'audit — pas de donnée personnelle sensible
    ip_address: Mapped[str | None] = mapped_column(String(45), nullable=True)   # IPv6 max 45 chars
    user_agent: Mapped[str | None] = mapped_column(String(255), nullable=True)  # tronqué

    # Si le TOTP a été vérifié pour cette session (requis pour admins C)
    totp_verified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Relations
    user: Mapped["User"] = relationship("User", back_populates="sessions", lazy="raise")

    @property
    def is_valid(self) -> bool:
        from datetime import timezone
        now = datetime.now(timezone.utc)
        return self.revoked_at is None and self.expires_at > now
