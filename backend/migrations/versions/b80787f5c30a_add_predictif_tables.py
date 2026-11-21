"""add_predictif_tables

Revision ID: b80787f5c30a
Revises: c42dec8242ab
Create Date: 2026-11-21 16:49:12.447413

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b80787f5c30a'
down_revision: Union[str, None] = 'c42dec8242ab'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "zones_alerte",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("code", sa.String(64), nullable=False, unique=True),
        sa.Column("nom", sa.String(256), nullable=False),
        sa.Column("departement", sa.String(3), nullable=False),
        sa.Column("type_eau", sa.String(32), nullable=False),
        sa.Column("geometry_wkt", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        if_not_exists=True,
    )

    op.create_table(
        "arretes_secheresse",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("zone_alerte_id", sa.String(36), sa.ForeignKey("zones_alerte.id"), nullable=False, index=True),
        sa.Column("date_debut", sa.Date, nullable=False),
        sa.Column("date_fin", sa.Date, nullable=True),
        sa.Column("niveau", sa.SmallInteger, nullable=False),
        sa.Column("source_fichier", sa.String(256), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        if_not_exists=True,
    )

    op.create_table(
        "model_versions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("version", sa.String(32), nullable=False, unique=True),
        sa.Column("algorithm", sa.String(64), nullable=False),
        sa.Column("trained_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("train_period_start", sa.Date, nullable=False),
        sa.Column("train_period_end", sa.Date, nullable=False),
        sa.Column("test_period_start", sa.Date, nullable=False),
        sa.Column("test_period_end", sa.Date, nullable=False),
        sa.Column("backtest_auc_7j", sa.Float, nullable=True),
        sa.Column("backtest_auc_15j", sa.Float, nullable=True),
        sa.Column("backtest_auc_30j", sa.Float, nullable=True),
        sa.Column("backtest_brier_7j", sa.Float, nullable=True),
        sa.Column("backtest_brier_15j", sa.Float, nullable=True),
        sa.Column("backtest_brier_30j", sa.Float, nullable=True),
        sa.Column("backtest_report_json", sa.Text, nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, default=False),
        sa.Column("model_artifact_path", sa.String(512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        if_not_exists=True,
    )

    op.create_table(
        "predictions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("model_version_id", sa.String(36), sa.ForeignKey("model_versions.id"), nullable=False, index=True),
        sa.Column("zone_alerte_id", sa.String(36), sa.ForeignKey("zones_alerte.id"), nullable=False, index=True),
        sa.Column("date_prediction", sa.Date, nullable=False, index=True),
        sa.Column("horizon_jours", sa.SmallInteger, nullable=False),
        sa.Column("proba_alerte", sa.Float, nullable=False),
        sa.Column("niveau_actuel", sa.SmallInteger, nullable=False),
        sa.Column("fiabilite_reduite", sa.Boolean, nullable=False, default=False),
        sa.Column("source_manquante", sa.String(256), nullable=True),
        sa.Column("top_factors_json", sa.Text, nullable=True),
        sa.Column("niveau_reel", sa.SmallInteger, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("model_version_id", "zone_alerte_id", "date_prediction", "horizon_jours"),
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_table("predictions")
    op.drop_table("model_versions")
    op.drop_table("arretes_secheresse")
    op.drop_table("zones_alerte")
