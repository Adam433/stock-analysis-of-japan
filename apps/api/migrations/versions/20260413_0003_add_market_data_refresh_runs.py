"""add market data refresh runs

Revision ID: 20260413_0003
Revises: 20260413_0002
Create Date: 2026-04-13 23:20:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

REFRESH_RUN_STATUS_VALUES = ("running", "succeeded", "partial", "failed")


revision = "20260413_0003"
down_revision = "20260413_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_data_refresh_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("requested_symbols", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("latest_trade_date", sa.Date(), nullable=True),
        sa.Column("rows_processed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rows_inserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rows_updated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("partial_rows", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unavailable_rows", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            f"status IN {REFRESH_RUN_STATUS_VALUES}",
            name="market_data_refresh_runs_status",
        ),
    )


def downgrade() -> None:
    op.drop_table("market_data_refresh_runs")
