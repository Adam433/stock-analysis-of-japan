"""add US refresh universe filter metadata

Revision ID: 20260428_0022
Revises: 20260417_0021
Create Date: 2026-04-28 00:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260428_0022"
down_revision = "20260417_0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_checks = {
        constraint["name"]
        for constraint in inspector.get_check_constraints("market_data_refresh_runs")
    }
    with op.batch_alter_table("market_data_refresh_runs") as batch_op:
        if "market_data_refresh_runs_universe_filter" in existing_checks:
            batch_op.drop_constraint(
                "market_data_refresh_runs_universe_filter",
                type_="check",
            )
        batch_op.create_check_constraint(
            "market_data_refresh_runs_universe_filter",
            "universe_filter IN ('explicit_symbols', 'tse_common_stock', 'us_common_stock')",
        )


def downgrade() -> None:
    with op.batch_alter_table("market_data_refresh_runs") as batch_op:
        batch_op.drop_constraint(
            "market_data_refresh_runs_universe_filter",
            type_="check",
        )
        batch_op.create_check_constraint(
            "market_data_refresh_runs_universe_filter",
            "universe_filter IN ('explicit_symbols', 'tse_common_stock')",
        )
