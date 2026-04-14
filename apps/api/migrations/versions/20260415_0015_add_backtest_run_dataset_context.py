"""add backtest run dataset context

Revision ID: 20260415_0015
Revises: 20260415_0014
Create Date: 2026-04-15 20:02:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260415_0015"
down_revision = "20260415_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("backtest_runs", sa.Column("dataset_trade_date_start", sa.Date(), nullable=True))
    op.add_column("backtest_runs", sa.Column("dataset_trade_date_end", sa.Date(), nullable=True))
    op.add_column("backtest_runs", sa.Column("dataset_checksum", sa.String(length=64), nullable=True))


def downgrade() -> None:
    op.drop_column("backtest_runs", "dataset_checksum")
    op.drop_column("backtest_runs", "dataset_trade_date_end")
    op.drop_column("backtest_runs", "dataset_trade_date_start")
