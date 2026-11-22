"""
Utilisateur.

Authentification : lien magique (prioritaire) ou mot de passe Argon2id.
TOTP obligatoire pour les admins de formule C et le super-admin.
"""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Enum, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models._base import TimestampMixin, new_uuid

if TYPE_CHECKING:
    from app.models.organisation import Organisation
    from app.models.session import Session
    from app.models.magic_token import MagicToken


class UserRole(str, enum.Enum):
    ADMIN = "admin"   # Peut gérer les membres et la facturation de son org
    USER = "user"     # Accès lecture dans son org
    SUPERADMIN = "superadmin"  # Accès total — 1 seul compte, non exposé via API


class User(Base, TimestampMixin):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    org_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("organisations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)

    # Mot de passe — nullable pour les utilisateurs magic-link uniquement
    password_hash: Mapped[str | None] = mapped_column(String(512), nullable=True)

    role: Mapped[UserRole] = mapped_column(
        Enum(UserRole, name="user_role"), nullable=False, default=UserRole.USER
    )

    # TOTP — obligatoire pour ADMIN/SUPERADMIN en plan C
    # Le secret est stocké chiffré (chiffrement applicatif, clé dans SESSION_SECRET)
    totp_secret_enc: Mapped[str | None] = mapped_column(String(512), nullable=True)
    totp_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # Relations
    organisation: Mapped["Organisation"] = relationship(
        "Organisation", back_populates="users", lazy="raise"
    )
    sessions: Mapped[list["Session"]] = relationship(
        "Session", back_populates="user", lazy="raise"
    )
    magic_tokens: Mapped[list["MagicToken"]] = relationship(
        "MagicToken", back_populates="user", lazy="raise"
    )
