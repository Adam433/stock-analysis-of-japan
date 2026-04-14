"""add refresh universe scope metadata

Revision ID: 20260414_0011
Revises: 20260414_0010
Create Date: 2026-04-14 23:59:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260414_0011"
down_revision = "20260414_0010"
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

    if "universe_scope" not in existing_columns:
        op.add_column(
            "market_data_refresh_runs",
            sa.Column(
                "universe_scope",
                sa.String(length=24),
                nullable=False,
                server_default="symbol_list",
            ),
        )
    if "requested_symbol_count" not in existing_columns:
        op.add_column(
            "market_data_refresh_runs",
            sa.Column(
                "requested_symbol_count",
                sa.Integer(),
                nullable=False,
                server_default="0",
            ),
        )
    op.execute("UPDATE market_data_refresh_runs SET requested_symbol_count = 0")
    if "market_data_refresh_runs_universe_scope" not in existing_checks:
        with op.batch_alter_table("market_data_refresh_runs") as batch_op:
            batch_op.create_check_constraint(
                "market_data_refresh_runs_universe_scope",
                "universe_scope IN ('symbol_list', 'full_universe')",
            )


def downgrade() -> None:
    with op.batch_alter_table("market_data_refresh_runs") as batch_op:
        batch_op.drop_constraint(
            "market_data_refresh_runs_universe_scope",
            type_="check",
        )
        batch_op.drop_column("requested_symbol_count")
        batch_op.drop_column("universe_scope")
