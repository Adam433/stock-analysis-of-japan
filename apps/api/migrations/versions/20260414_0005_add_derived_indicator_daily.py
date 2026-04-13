"""add derived indicator daily

Revision ID: 20260414_0005
Revises: 20260414_0004
Create Date: 2026-04-14 00:55:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260414_0005"
down_revision = "20260414_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "derived_indicator_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("rps_50", sa.Numeric(5, 2), nullable=True),
        sa.Column("rps_120", sa.Numeric(5, 2), nullable=True),
        sa.Column("rps_250", sa.Numeric(5, 2), nullable=True),
        sa.Column("fifty_two_week_high", sa.Numeric(18, 6), nullable=True),
        sa.Column("high_proximity_ratio", sa.Numeric(18, 6), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_derived_indicator_daily_instrument_id_instruments",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "instrument_id",
            "trade_date",
            name="uq_derived_indicator_daily_instrument_id_trade_date",
        ),
    )
    op.create_index(
        "ix_derived_indicator_daily_trade_date",
        "derived_indicator_daily",
        ["trade_date"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_derived_indicator_daily_trade_date", table_name="derived_indicator_daily")
    op.drop_table("derived_indicator_daily")
