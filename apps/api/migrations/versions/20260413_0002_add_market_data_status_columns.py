"""add market data status columns

Revision ID: 20260413_0002
Revises: 20260413_0001
Create Date: 2026-04-13 22:45:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260413_0002"
down_revision = "20260413_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("market_data_daily") as batch_op:
        batch_op.add_column(
            sa.Column("data_source", sa.String(length=32), nullable=False, server_default="unknown")
        )
        batch_op.add_column(
            sa.Column("data_status", sa.String(length=16), nullable=False, server_default="complete")
        )


def downgrade() -> None:
    with op.batch_alter_table("market_data_daily") as batch_op:
        batch_op.drop_column("data_status")
        batch_op.drop_column("data_source")
