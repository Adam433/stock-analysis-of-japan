"""add fundamental cash flow fields

Revision ID: 20260504_0026
Revises: 20260503_0025
Create Date: 2026-05-04
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260504_0026"
down_revision = "20260503_0025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "fundamentals_annual",
        sa.Column("operating_cash_flow", sa.Numeric(20, 2), nullable=True),
    )
    op.add_column(
        "fundamentals_annual",
        sa.Column("free_cash_flow", sa.Numeric(20, 2), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("fundamentals_annual", "free_cash_flow")
    op.drop_column("fundamentals_annual", "operating_cash_flow")
