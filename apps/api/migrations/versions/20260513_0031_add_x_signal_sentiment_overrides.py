"""add x signal sentiment override fields

Revision ID: 20260513_0031
Revises: 20260512_0030
Create Date: 2026-05-13
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260513_0031"
down_revision = "20260512_0030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("x_signal_mentions", sa.Column("llm_sentiment", sa.String(length=16), nullable=True))
    op.add_column("x_signal_mentions", sa.Column("manual_sentiment", sa.String(length=16), nullable=True))
    op.add_column(
        "x_signal_mentions",
        sa.Column("sentiment_source", sa.String(length=16), nullable=False, server_default="extraction"),
    )
    op.execute(
        """
        UPDATE x_signal_mentions
        SET llm_sentiment = sentiment,
            sentiment_source = 'llm'
        WHERE analysis_source NOT IN ('extraction-v1', 'heuristic-v1')
        """
    )
    op.execute(
        """
        UPDATE x_signal_mentions
        SET sentiment_source = 'extraction'
        WHERE analysis_source IN ('extraction-v1', 'heuristic-v1')
        """
    )


def downgrade() -> None:
    op.drop_column("x_signal_mentions", "sentiment_source")
    op.drop_column("x_signal_mentions", "manual_sentiment")
    op.drop_column("x_signal_mentions", "llm_sentiment")
