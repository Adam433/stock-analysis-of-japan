"""add fundamental valuation input fields

Revision ID: 20260504_0027
Revises: 20260504_0026
Create Date: 2026-05-04
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260504_0027"
down_revision = "20260504_0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "fundamentals_annual",
        sa.Column("diluted_eps", sa.Numeric(18, 6), nullable=True),
    )
    op.add_column(
        "fundamentals_annual",
        sa.Column("stockholders_equity", sa.Numeric(20, 2), nullable=True),
    )
    op.add_column(
        "fundamentals_annual",
        sa.Column("weighted_average_diluted_shares", sa.Numeric(20, 2), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("fundamentals_annual", "weighted_average_diluted_shares")
    op.drop_column("fundamentals_annual", "stockholders_equity")
    op.drop_column("fundamentals_annual", "diluted_eps")
