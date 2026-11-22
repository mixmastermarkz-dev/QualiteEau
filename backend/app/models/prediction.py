"""
Modèles DB pour le module prédictif sécheresse Occitanie.

ZoneAlerte       — zones d'alerte VigiEau
ArreteSecheresse — historique des arrêtés préfectoraux
ModelVersion     — versions entraînées du modèle ML
Prediction       — prédictions journalières par zone et horizon
"""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base
from app.models._base import TimestampMixin, new_uuid


class ZoneAlerte(Base, TimestampMixin):
    __tablename__ = "zones_alerte"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    code: Mapped[str] = mapped_column(String(64), unique=True)  # identifiant VigiEau
    nom: Mapped[str] = mapped_column(String(256))
    departement: Mapped[str] = mapped_column(String(3))  # "34", "09", etc.
    type_eau: Mapped[str] = mapped_column(String(32))  # SUP, SOU, AEP
    geometry_wkt: Mapped[str | None] = mapped_column(Text)  # WKT simple, nullable


class ArreteSecheresse(Base, TimestampMixin):
    __tablename__ = "arretes_secheresse"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    zone_alerte_id: Mapped[str] = mapped_column(
        ForeignKey("zones_alerte.id"), index=True
    )
    date_debut: Mapped[date] = mapped_column(Date)
    date_fin: Mapped[date | None] = mapped_column(Date, nullable=True)
    niveau: Mapped[int] = mapped_column(
        SmallInteger
    )  # 1 vigilance, 2 alerte, 3 renforcée, 4 crise
    source_fichier: Mapped[str | None] = mapped_column(
        String(256)
    )  # nom du CSV d'origine


class ModelVersion(Base, TimestampMixin):
    __tablename__ = "model_versions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    version: Mapped[str] = mapped_column(String(32), unique=True)  # ex. "2026-11-21_v1"
    algorithm: Mapped[str] = mapped_column(String(64))  # ex. "HistGradientBoosting"
    trained_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    train_period_start: Mapped[date] = mapped_column(Date)
    train_period_end: Mapped[date] = mapped_column(Date)
    test_period_start: Mapped[date] = mapped_column(Date)
    test_period_end: Mapped[date] = mapped_column(Date)
    backtest_auc_7j: Mapped[float | None] = mapped_column(Float)
    backtest_auc_15j: Mapped[float | None] = mapped_column(Float)
    backtest_auc_30j: Mapped[float | None] = mapped_column(Float)
    backtest_brier_7j: Mapped[float | None] = mapped_column(Float)
    backtest_brier_15j: Mapped[float | None] = mapped_column(Float)
    backtest_brier_30j: Mapped[float | None] = mapped_column(Float)
    backtest_report_json: Mapped[str | None] = mapped_column(Text)  # JSON complet
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)
    model_artifact_path: Mapped[str | None] = mapped_column(
        String(512)
    )  # chemin .pkl
    model_sha256: Mapped[str | None] = mapped_column(
        String(64)
    )  # empreinte SHA-256 du .pkl, vérifiée avant pickle.load
    validated_by_owner: Mapped[bool] = mapped_column(
        Boolean, default=False
    )  # le propriétaire doit valider avant tout déploiement en production


class Prediction(Base, TimestampMixin):
    __tablename__ = "predictions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    model_version_id: Mapped[str] = mapped_column(
        ForeignKey("model_versions.id"), index=True
    )
    zone_alerte_id: Mapped[str] = mapped_column(
        ForeignKey("zones_alerte.id"), index=True
    )
    date_prediction: Mapped[date] = mapped_column(
        Date, index=True
    )  # date à laquelle on prédit
    horizon_jours: Mapped[int] = mapped_column(SmallInteger)  # 7, 15, ou 30
    proba_alerte: Mapped[float] = mapped_column(Float)  # P(niveau >= 2)
    niveau_actuel: Mapped[int] = mapped_column(
        SmallInteger
    )  # niveau au moment de la prédiction
    fiabilite_reduite: Mapped[bool] = mapped_column(
        Boolean, default=False
    )  # source manquante
    source_manquante: Mapped[str | None] = mapped_column(
        String(256)
    )  # ex. "hubeau_piezo"
    top_factors_json: Mapped[str | None] = mapped_column(
        Text
    )  # JSON {"facteur": importance}
    niveau_reel: Mapped[int | None] = mapped_column(
        SmallInteger, nullable=True
    )  # rempli après

    __table_args__ = (
        UniqueConstraint(
            "model_version_id",
            "zone_alerte_id",
            "date_prediction",
            "horizon_jours",
        ),
    )
