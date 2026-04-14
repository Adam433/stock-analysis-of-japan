"""add backtest run rps definition version

Revision ID: 20260415_0014
Revises: 20260415_0013
Create Date: 2026-04-15 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260415_0014"
down_revision = "20260415_0013"
branch_labels = None
depends_on = None
legacy_definition_version = "legacy-unrecorded-pre-rps-versioning"


def upgrade() -> None:
    with op.batch_alter_table("backtest_runs") as batch_op:
        batch_op.add_column(sa.Column("rps_definition_version", sa.String(length=64), nullable=True))
    op.execute(
        sa.text(
            "UPDATE backtest_runs SET rps_definition_version = :version WHERE rps_definition_version IS NULL"
        ).bindparams(version=legacy_definition_version)
    )


def downgrade() -> None:
    with op.batch_alter_table("backtest_runs") as batch_op:
        batch_op.drop_column("rps_definition_version")
