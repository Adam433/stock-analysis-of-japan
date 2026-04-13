"""add screen runs and results

Revision ID: 20260414_0006
Revises: 20260414_0005
Create Date: 2026-04-14 01:20:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260414_0006"
down_revision = "20260414_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "screen_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("strategy_configuration_id", sa.Integer(), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("total_candidates", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("qualified_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="completed"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["strategy_configuration_id"],
            ["strategy_configurations.id"],
            name="fk_screen_runs_strategy_configuration_id_strategy_configurations",
            ondelete="RESTRICT",
        ),
    )
    op.create_index("ix_screen_runs_trade_date", "screen_runs", ["trade_date"], unique=False)
    op.create_index(
        "ix_screen_runs_strategy_configuration_id",
        "screen_runs",
        ["strategy_configuration_id"],
        unique=False,
    )

    op.create_table(
        "screen_run_results",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("screen_run_id", sa.Integer(), nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("passed", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("rps_50", sa.Numeric(5, 2), nullable=True),
        sa.Column("rps_120", sa.Numeric(5, 2), nullable=True),
        sa.Column("rps_250", sa.Numeric(5, 2), nullable=True),
        sa.Column("best_rps_value", sa.Numeric(5, 2), nullable=True),
        sa.Column("rps_threshold", sa.Integer(), nullable=False),
        sa.Column("high_proximity_ratio", sa.Numeric(18, 6), nullable=True),
        sa.Column("high_proximity_threshold_pct", sa.Numeric(5, 2), nullable=False),
        sa.Column("max_drawdown_from_high_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("rps_condition_passed", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("high_proximity_condition_passed", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_screen_run_results_instrument_id_instruments",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["screen_run_id"],
            ["screen_runs.id"],
            name="fk_screen_run_results_screen_run_id_screen_runs",
            ondelete="CASCADE",
        ),
    )
    op.create_index("ix_screen_run_results_trade_date", "screen_run_results", ["trade_date"], unique=False)
    op.create_index("ix_screen_run_results_screen_run_id", "screen_run_results", ["screen_run_id"], unique=False)
    op.create_index("ix_screen_run_results_instrument_id", "screen_run_results", ["instrument_id"], unique=False)
