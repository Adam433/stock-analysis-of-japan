"""add refresh universe filter metadata

Revision ID: 20260415_0012
Revises: 20260414_0011
Create Date: 2026-04-15 00:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260415_0012"
down_revision = "20260414_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_columns = {
        column["name"] for column in inspector.get_columns("market_data_refresh_runs")
    }
    existing_checks = {
        constraint["name"] for constraint in inspector.get_check_constraints("market_data_refresh_runs")
    }

    if "universe_filter" not in existing_columns:
        op.add_column(
            "market_data_refresh_runs",
            sa.Column(
                "universe_filter",
                sa.String(length=32),
                nullable=False,
                server_default="explicit_symbols",
            ),
        )
    if "market_data_refresh_runs_universe_filter" not in existing_checks:
        with op.batch_alter_table("market_data_refresh_runs") as batch_op:
            batch_op.create_check_constraint(
                "market_data_refresh_runs_universe_filter",
                "universe_filter IN ('explicit_symbols', 'tse_common_stock')",
            )


def downgrade() -> None:
    with op.batch_alter_table("market_data_refresh_runs") as batch_op:
        batch_op.drop_constraint(
            "market_data_refresh_runs_universe_filter",
            type_="check",
        )
        batch_op.drop_column("universe_filter")
