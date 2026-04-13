"""initial market data baseline

Revision ID: 20260413_0001
Revises:
Create Date: 2026-04-13 22:15:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260413_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "instruments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("exchange", sa.String(length=16), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.Column("currency", sa.String(length=8), nullable=False, server_default="JPY"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("symbol", "exchange", name="uq_instruments_symbol_exchange"),
    )
    op.create_index("ix_instruments_symbol", "instruments", ["symbol"], unique=False)

    op.create_table(
        "market_data_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("open", sa.Numeric(18, 6), nullable=True),
        sa.Column("high", sa.Numeric(18, 6), nullable=True),
        sa.Column("low", sa.Numeric(18, 6), nullable=True),
        sa.Column("close", sa.Numeric(18, 6), nullable=True),
        sa.Column("adj_close", sa.Numeric(18, 6), nullable=True),
        sa.Column("volume", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_market_data_daily_instrument_id_instruments",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "instrument_id",
            "trade_date",
            name="uq_market_data_daily_instrument_id_trade_date",
        ),
    )
    op.create_index(
        "ix_market_data_daily_trade_date",
        "market_data_daily",
        ["trade_date"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_market_data_daily_trade_date", table_name="market_data_daily")
    op.drop_table("market_data_daily")
    op.drop_index("ix_instruments_symbol", table_name="instruments")
    op.drop_table("instruments")
