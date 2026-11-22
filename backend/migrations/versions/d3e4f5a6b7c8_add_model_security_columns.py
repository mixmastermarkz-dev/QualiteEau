"""add_model_security_columns

Révision S2 de l'audit sécurité du 2026-11-22 :
- model_sha256 : empreinte SHA-256 du fichier .pkl, calculée à la sauvegarde
  et vérifiée avant pickle.load.
- validated_by_owner : le propriétaire du service doit valider explicitement
  un modèle avant qu'il soit servi en production. Sans validation, aucune
  prédiction n'est émise.

Revision ID: d3e4f5a6b7c8
Revises: b80787f5c30a
Create Date: 2026-11-22 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d3e4f5a6b7c8"
down_revision: Union[str, None] = "b80787f5c30a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "model_versions",
        sa.Column("model_sha256", sa.String(64), nullable=True),
    )
    op.add_column(
        "model_versions",
        sa.Column(
            "validated_by_owner",
            sa.Boolean,
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("model_versions", "validated_by_owner")
    op.drop_column("model_versions", "model_sha256")
