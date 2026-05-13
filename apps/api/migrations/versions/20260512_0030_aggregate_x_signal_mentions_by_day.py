"""aggregate x signal mentions by day

Revision ID: 20260512_0030
Revises: 20260512_0029
Create Date: 2026-05-12
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260512_0030"
down_revision = "20260512_0029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("x_signal_mentions", sa.Column("mention_date", sa.Date(), nullable=True))
    op.add_column(
        "x_signal_mentions",
        sa.Column("mention_count", sa.Integer(), nullable=False, server_default="1"),
    )
    op.add_column("x_signal_mentions", sa.Column("source_post_ids_json", sa.Text(), nullable=True))
    op.execute("UPDATE x_signal_mentions SET mention_date = date(mentioned_at) WHERE mention_date IS NULL")
    op.create_index("ix_x_signal_mentions_mention_date", "x_signal_mentions", ["mention_date"], unique=False)
    op.create_index(
        "ix_x_signal_mentions_author_symbol_date",
        "x_signal_mentions",
        ["author_id", "symbol", "mention_date"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_x_signal_mentions_author_symbol_date", table_name="x_signal_mentions")
    op.drop_index("ix_x_signal_mentions_mention_date", table_name="x_signal_mentions")
    op.drop_column("x_signal_mentions", "source_post_ids_json")
    op.drop_column("x_signal_mentions", "mention_count")
    op.drop_column("x_signal_mentions", "mention_date")
