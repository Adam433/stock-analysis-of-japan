"""add watchlist entries

Revision ID: 20260414_0007
Revises: 20260414_0006
Create Date: 2026-04-14 02:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260414_0007"
down_revision = "20260414_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "watchlist_entries",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_watchlist_entries_instrument_id_instruments",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("instrument_id", name="uq_watchlist_entries_instrument_id"),
    )
    op.create_index("ix_watchlist_entries_instrument_id", "watchlist_entries", ["instrument_id"], unique=False)
