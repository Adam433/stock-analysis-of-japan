"""add watchlist context fields

Revision ID: 20260414_0008
Revises: 20260414_0007
Create Date: 2026-04-14 02:55:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260414_0008"
down_revision = "20260414_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("watchlist_entries") as batch_op:
        batch_op.add_column(sa.Column("note", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("observation_reason", sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column("added_date", sa.Date(), nullable=True))
    op.execute(sa.text("UPDATE watchlist_entries SET added_date = date(created_at) WHERE added_date IS NULL"))
    with op.batch_alter_table("watchlist_entries") as batch_op:
        batch_op.alter_column("added_date", existing_type=sa.Date(), nullable=False)
