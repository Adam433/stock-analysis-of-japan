"""add backtest runs

Revision ID: 20260414_0009
Revises: 20260414_0008
Create Date: 2026-04-14 03:35:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260414_0009"
down_revision = "20260414_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "backtest_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("strategy_configuration_id", sa.Integer(), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="running"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("status IN ('running', 'completed', 'failed')", name="ck_backtest_runs_backtest_runs_status"),
        sa.ForeignKeyConstraint(
            ["strategy_configuration_id"],
            ["strategy_configurations.id"],
            name="fk_backtest_runs_strategy_configuration_id_strategy_configurations",
            ondelete="RESTRICT",
        ),
    )
    op.create_index("ix_backtest_runs_strategy_configuration_id", "backtest_runs", ["strategy_configuration_id"], unique=False)
    op.create_index("ix_backtest_runs_start_date", "backtest_runs", ["start_date"], unique=False)
    op.create_index("ix_backtest_runs_end_date", "backtest_runs", ["end_date"], unique=False)
