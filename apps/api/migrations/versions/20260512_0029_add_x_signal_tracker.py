"""add x signal tracker tables

Revision ID: 20260512_0029
Revises: 20260511_0028
Create Date: 2026-05-12
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260512_0029"
down_revision = "20260511_0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "x_signal_authors",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("handle", sa.String(length=64), nullable=False),
        sa.Column("display_name", sa.String(length=128), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("tracking_status", sa.String(length=16), nullable=False, server_default="active"),
        sa.Column("last_fetch_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_analyzed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("tracking_status IN ('active', 'paused')", name="x_signal_authors_tracking_status"),
        sa.UniqueConstraint("handle", name="uq_x_signal_authors_handle"),
    )
    op.create_index("ix_x_signal_authors_handle", "x_signal_authors", ["handle"], unique=False)

    op.create_table(
        "x_signal_fetch_requests",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("author_id", sa.Integer(), nullable=False),
        sa.Column("lookback_value", sa.Integer(), nullable=False),
        sa.Column("lookback_unit", sa.String(length=16), nullable=False),
        sa.Column("requested_from", sa.Date(), nullable=False),
        sa.Column("requested_to", sa.Date(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending_chrome_capture"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "status IN ('pending_chrome_capture', 'imported', 'failed')",
            name="x_signal_fetch_requests_status",
        ),
        sa.CheckConstraint(
            "lookback_unit IN ('months', 'days')",
            name="x_signal_fetch_requests_lookback_unit",
        ),
        sa.ForeignKeyConstraint(
            ["author_id"],
            ["x_signal_authors.id"],
            name="fk_x_signal_fetch_requests_author_id_x_signal_authors",
            ondelete="CASCADE",
        ),
    )
    op.create_index("ix_x_signal_fetch_requests_author_id", "x_signal_fetch_requests", ["author_id"], unique=False)
    op.create_index(
        "ix_x_signal_fetch_requests_requested_from",
        "x_signal_fetch_requests",
        ["requested_from"],
        unique=False,
    )
    op.create_index(
        "ix_x_signal_fetch_requests_requested_to",
        "x_signal_fetch_requests",
        ["requested_to"],
        unique=False,
    )

    op.create_table(
        "x_signal_posts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("author_id", sa.Integer(), nullable=False),
        sa.Column("external_post_id", sa.String(length=128), nullable=True),
        sa.Column("posted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("raw_payload_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["author_id"],
            ["x_signal_authors.id"],
            name="fk_x_signal_posts_author_id_x_signal_authors",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("author_id", "content_hash", name="uq_x_signal_posts_author_content_hash"),
    )
    op.create_index("ix_x_signal_posts_author_id", "x_signal_posts", ["author_id"], unique=False)
    op.create_index("ix_x_signal_posts_external_post_id", "x_signal_posts", ["external_post_id"], unique=False)
    op.create_index("ix_x_signal_posts_posted_at", "x_signal_posts", ["posted_at"], unique=False)
    op.create_index("ix_x_signal_posts_content_hash", "x_signal_posts", ["content_hash"], unique=False)

    op.create_table(
        "x_signal_mentions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("author_id", sa.Integer(), nullable=False),
        sa.Column("post_id", sa.Integer(), nullable=True),
        sa.Column("instrument_id", sa.Integer(), nullable=True),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("exchange", sa.String(length=16), nullable=True),
        sa.Column("company_name", sa.String(length=255), nullable=True),
        sa.Column("mention_kind", sa.String(length=24), nullable=False),
        sa.Column("sector_label", sa.String(length=128), nullable=True),
        sa.Column("sentiment", sa.String(length=16), nullable=False, server_default="unknown"),
        sa.Column("confidence", sa.Numeric(5, 4), nullable=True),
        sa.Column("mentioned_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("is_sector_proxy", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("proxy_reason", sa.Text(), nullable=True),
        sa.Column("source_text_excerpt", sa.Text(), nullable=True),
        sa.Column("analysis_source", sa.String(length=64), nullable=False, server_default="heuristic-v1"),
        sa.Column("analysis_note", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "mention_kind IN ('stock', 'sector_proxy')",
            name="x_signal_mentions_kind",
        ),
        sa.CheckConstraint(
            "sentiment IN ('bullish', 'bearish', 'neutral', 'unknown')",
            name="x_signal_mentions_sentiment",
        ),
        sa.ForeignKeyConstraint(
            ["author_id"],
            ["x_signal_authors.id"],
            name="fk_x_signal_mentions_author_id_x_signal_authors",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["post_id"],
            ["x_signal_posts.id"],
            name="fk_x_signal_mentions_post_id_x_signal_posts",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_x_signal_mentions_instrument_id_instruments",
            ondelete="SET NULL",
        ),
    )
    op.create_index("ix_x_signal_mentions_author_id", "x_signal_mentions", ["author_id"], unique=False)
    op.create_index("ix_x_signal_mentions_post_id", "x_signal_mentions", ["post_id"], unique=False)
    op.create_index("ix_x_signal_mentions_instrument_id", "x_signal_mentions", ["instrument_id"], unique=False)
    op.create_index("ix_x_signal_mentions_symbol", "x_signal_mentions", ["symbol"], unique=False)
    op.create_index("ix_x_signal_mentions_mentioned_at", "x_signal_mentions", ["mentioned_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_x_signal_mentions_mentioned_at", table_name="x_signal_mentions")
    op.drop_index("ix_x_signal_mentions_symbol", table_name="x_signal_mentions")
    op.drop_index("ix_x_signal_mentions_instrument_id", table_name="x_signal_mentions")
    op.drop_index("ix_x_signal_mentions_post_id", table_name="x_signal_mentions")
    op.drop_index("ix_x_signal_mentions_author_id", table_name="x_signal_mentions")
    op.drop_table("x_signal_mentions")

    op.drop_index("ix_x_signal_posts_content_hash", table_name="x_signal_posts")
    op.drop_index("ix_x_signal_posts_posted_at", table_name="x_signal_posts")
    op.drop_index("ix_x_signal_posts_external_post_id", table_name="x_signal_posts")
    op.drop_index("ix_x_signal_posts_author_id", table_name="x_signal_posts")
    op.drop_table("x_signal_posts")

    op.drop_index("ix_x_signal_fetch_requests_requested_to", table_name="x_signal_fetch_requests")
    op.drop_index("ix_x_signal_fetch_requests_requested_from", table_name="x_signal_fetch_requests")
    op.drop_index("ix_x_signal_fetch_requests_author_id", table_name="x_signal_fetch_requests")
    op.drop_table("x_signal_fetch_requests")

    op.drop_index("ix_x_signal_authors_handle", table_name="x_signal_authors")
    op.drop_table("x_signal_authors")
