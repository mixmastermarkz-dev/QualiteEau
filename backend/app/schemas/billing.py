"""
Schémas Pydantic pour les routes de facturation.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, field_validator


class CheckoutRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    plan: str  # "A", "B" ou "C" uniquement

    @field_validator("plan")
    @classmethod
    def validate_plan(cls, v: str) -> str:
        normalized = v.strip().upper()
        if normalized not in ("A", "B", "C"):
            raise ValueError("La formule doit être A, B ou C.")
        return normalized


class CheckoutResponse(BaseModel):
    checkout_url: str


class CheckoutSuccessResponse(BaseModel):
    payment_status: str
    subscription_id: str | None
    status: str


class PortalResponse(BaseModel):
    portal_url: str


class SubscriptionStatusResponse(BaseModel):
    plan: str
    status: str
    grace_period_until: str | None
    trial_ends_at: str | None
