"""add backtest run summary fields

Revision ID: 20260414_0010
Revises: 20260414_0009
Create Date: 2026-04-14 04:05:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260414_0010"
down_revision = "20260414_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("backtest_runs") as batch_op:
        batch_op.add_column(sa.Column("trade_dates_evaluated", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("total_candidates_evaluated", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("qualifying_observations", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("unique_qualified_instruments", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("first_qualified_trade_date", sa.Date(), nullable=True))
        batch_op.add_column(sa.Column("last_qualified_trade_date", sa.Date(), nullable=True))
        batch_op.add_column(sa.Column("result_checksum", sa.String(length=64), nullable=True))
