"""
Routes API publiques et payantes pour le module prédictif.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies import get_current_user, require_plan
from app.models.prediction import ModelVersion, Prediction, ZoneAlerte
from app.models.subscription import Plan
from app.models.user import User

router = APIRouter(prefix="/api/v1", tags=["predictif"])

# Indicateur grossier : 3 niveaux de risque public
def _proba_to_risque_grossier(proba_30j: float) -> str:
    if proba_30j < 0.33:
        return "faible"
    if proba_30j < 0.66:
        return "modere"
    return "eleve"


@router.get("/risque/{code_zone}")
async def get_risque_zone(
    code_zone: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Risque sécheresse grossier pour une zone (public, sans compte).
    Retourne uniquement faible/modéré/élevé et la mention légale.
    """
    result = await db.execute(
        select(ZoneAlerte).where(ZoneAlerte.code == code_zone)
    )
    zone = result.scalar_one_or_none()
    if zone is None:
        raise HTTPException(404, "Zone d'alerte inconnue")

    # Prédiction à 30j du modèle actif
    result = await db.execute(
        select(Prediction)
        .join(ModelVersion)
        .where(
            ModelVersion.is_active == True,
            Prediction.zone_alerte_id == zone.id,
            Prediction.horizon_jours == 30,
        )
        .order_by(Prediction.date_prediction.desc())
        .limit(1)
    )
    pred = result.scalar_one_or_none()

    if pred is None:
        return {
            "zone": code_zone,
            "risque": "inconnu",
            "mention_legale": "Estimation non officielle — modèle prédictif expérimental. Seuls les arrêtés préfectoraux publiés font foi.",
            "fiabilite_reduite": False,
        }

    return {
        "zone": code_zone,
        "risque": _proba_to_risque_grossier(pred.proba_alerte),
        "date_prediction": pred.date_prediction.isoformat(),
        "mention_legale": "Estimation non officielle — modèle prédictif expérimental. Seuls les arrêtés préfectoraux publiés font foi.",
        "fiabilite_reduite": pred.fiabilite_reduite,
    }


@router.get("/risque/{code_zone}/detail")
async def get_risque_zone_detail(
    code_zone: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_plan(Plan.A)),
):
    """
    Prédictions détaillées à 7/15/30 jours (formule A et supérieur).
    """
    result = await db.execute(
        select(ZoneAlerte).where(ZoneAlerte.code == code_zone)
    )
    zone = result.scalar_one_or_none()
    if zone is None:
        raise HTTPException(404, "Zone d'alerte inconnue")

    result = await db.execute(
        select(Prediction)
        .join(ModelVersion)
        .where(
            ModelVersion.is_active == True,
            Prediction.zone_alerte_id == zone.id,
        )
        .order_by(Prediction.date_prediction.desc(), Prediction.horizon_jours)
        .limit(3)
    )
    preds = result.scalars().all()

    return {
        "zone": code_zone,
        "predictions": [
            {
                "horizon_jours": p.horizon_jours,
                "proba_alerte": round(p.proba_alerte, 3),
                "niveau_actuel": p.niveau_actuel,
                "date_prediction": p.date_prediction.isoformat(),
                "fiabilite_reduite": p.fiabilite_reduite,
                "source_manquante": p.source_manquante,
            }
            for p in preds
        ],
        "mention_legale": "Estimation non officielle — modèle prédictif expérimental. Seuls les arrêtés préfectoraux publiés font foi.",
    }
