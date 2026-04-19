"""add fundamentals annual

Revision ID: 20260417_0021
Revises: 20260417_0020
Create Date: 2026-04-17 17:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260417_0021"
down_revision = "20260417_0020"
branch_labels = None
depends_on = None

FUNDAMENTALS_DATA_STATUS_CONSTRAINT = "data_status IN ('complete', 'partial', 'missing')"


def upgrade() -> None:
    op.create_table(
        "fundamentals_annual",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("fiscal_year_end_date", sa.Date(), nullable=False),
        sa.Column("fiscal_year_label", sa.String(length=32), nullable=False),
        sa.Column("net_income", sa.Numeric(20, 2), nullable=True),
        sa.Column("net_income_currency", sa.String(length=8), nullable=False, server_default="JPY"),
        sa.Column("pe", sa.Numeric(18, 4), nullable=True),
        sa.Column("pb", sa.Numeric(18, 4), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("source_as_of_date", sa.Date(), nullable=False),
        sa.Column("data_status", sa.String(length=16), nullable=False, server_default="complete"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            FUNDAMENTALS_DATA_STATUS_CONSTRAINT,
            name="fundamentals_annual_data_status",
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_fundamentals_annual_instrument_id_instruments",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "instrument_id",
            "fiscal_year_end_date",
            name="uq_fundamentals_annual_instrument_id_fiscal_year_end_date",
        ),
    )
    op.create_index(
        "ix_fundamentals_annual_instrument_id",
        "fundamentals_annual",
        ["instrument_id"],
        unique=False,
    )
    op.create_index(
        "ix_fundamentals_annual_fiscal_year_end_date",
        "fundamentals_annual",
        ["fiscal_year_end_date"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_fundamentals_annual_fiscal_year_end_date", table_name="fundamentals_annual")
    op.drop_index("ix_fundamentals_annual_instrument_id", table_name="fundamentals_annual")
    op.drop_table("fundamentals_annual")
