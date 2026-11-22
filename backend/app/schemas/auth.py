"""
Schémas Pydantic pour les routes d'authentification.

Mode strict : tout champ inattendu est rejeté.
L'email est normalisé (minuscules, strip) à la validation.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, EmailStr, field_validator


class MagicLinkRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    email: EmailStr

    @field_validator("email", mode="before")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        return v.strip().lower()


class MagicLinkResponse(BaseModel):
    """
    Réponse identique qu'un compte existe ou non — pas d'énumération.
    """
    message: str = (
        "Si cet email est enregistré, vous recevrez un lien de connexion."
    )


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    email: str
    role: str
    org_id: str


class SessionOut(BaseModel):
    user: UserOut
    plan: str
