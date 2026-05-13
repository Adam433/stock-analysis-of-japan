from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Boolean, CheckConstraint, Date, DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from stockanalyse_api.db.base import Base, TimestampMixin

X_SIGNAL_AUTHOR_STATUS_VALUES = ("active", "paused")
X_SIGNAL_FETCH_STATUS_VALUES = (
    "pending_chrome_capture",
    "imported",
    "failed",
)
X_SIGNAL_LOOKBACK_UNIT_VALUES = ("months", "days")
X_SIGNAL_MENTION_KIND_VALUES = ("stock", "sector_proxy")
X_SIGNAL_SENTIMENT_VALUES = ("bullish", "bearish", "neutral", "unknown")
X_SIGNAL_SENTIMENT_SOURCE_VALUES = ("extraction", "llm", "manual")


class XSignalAuthor(TimestampMixin, Base):
    __tablename__ = "x_signal_authors"
    __table_args__ = (
        UniqueConstraint("handle", name="uq_x_signal_authors_handle"),
        CheckConstraint(
            f"tracking_status IN {X_SIGNAL_AUTHOR_STATUS_VALUES}",
            name="x_signal_authors_tracking_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    handle: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    display_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    tracking_status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", server_default="active")
    last_fetch_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_analyzed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    posts = relationship(
        "XSignalPost",
        back_populates="author",
        cascade="all, delete-orphan",
    )
    mentions = relationship(
        "XSignalMention",
        back_populates="author",
        cascade="all, delete-orphan",
    )


class XSignalFetchRequest(TimestampMixin, Base):
    __tablename__ = "x_signal_fetch_requests"
    __table_args__ = (
        CheckConstraint(
            f"status IN {X_SIGNAL_FETCH_STATUS_VALUES}",
            name="x_signal_fetch_requests_status",
        ),
        CheckConstraint(
            f"lookback_unit IN {X_SIGNAL_LOOKBACK_UNIT_VALUES}",
            name="x_signal_fetch_requests_lookback_unit",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    author_id: Mapped[int] = mapped_column(
        ForeignKey("x_signal_authors.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    lookback_value: Mapped[int] = mapped_column(Integer, nullable=False)
    lookback_unit: Mapped[str] = mapped_column(String(16), nullable=False)
    requested_from: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    requested_to: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="pending_chrome_capture",
        server_default="pending_chrome_capture",
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class XSignalPost(TimestampMixin, Base):
    __tablename__ = "x_signal_posts"
    __table_args__ = (
        UniqueConstraint("author_id", "content_hash", name="uq_x_signal_posts_author_content_hash"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    author_id: Mapped[int] = mapped_column(
        ForeignKey("x_signal_authors.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    external_post_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    posted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    author = relationship("XSignalAuthor", back_populates="posts")
    mentions = relationship(
        "XSignalMention",
        back_populates="post",
        cascade="all, delete-orphan",
    )


class XSignalMention(TimestampMixin, Base):
    __tablename__ = "x_signal_mentions"
    __table_args__ = (
        CheckConstraint(
            f"mention_kind IN {X_SIGNAL_MENTION_KIND_VALUES}",
            name="x_signal_mentions_kind",
        ),
        CheckConstraint(
            f"sentiment IN {X_SIGNAL_SENTIMENT_VALUES}",
            name="x_signal_mentions_sentiment",
        ),
        CheckConstraint(
            f"sentiment_source IN {X_SIGNAL_SENTIMENT_SOURCE_VALUES}",
            name="x_signal_mentions_sentiment_source",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    author_id: Mapped[int] = mapped_column(
        ForeignKey("x_signal_authors.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    post_id: Mapped[int | None] = mapped_column(
        ForeignKey("x_signal_posts.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    instrument_id: Mapped[int | None] = mapped_column(
        ForeignKey("instruments.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    exchange: Mapped[str | None] = mapped_column(String(16), nullable=True)
    company_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    mention_kind: Mapped[str] = mapped_column(String(24), nullable=False)
    sector_label: Mapped[str | None] = mapped_column(String(128), nullable=True)
    sentiment: Mapped[str] = mapped_column(String(16), nullable=False, default="unknown", server_default="unknown")
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4), nullable=True)
    mention_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    mention_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    mentioned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    is_sector_proxy: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    proxy_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_text_excerpt: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_post_ids_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    analysis_source: Mapped[str] = mapped_column(String(64), nullable=False, default="heuristic-v1", server_default="heuristic-v1")
    analysis_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    llm_sentiment: Mapped[str | None] = mapped_column(String(16), nullable=True)
    manual_sentiment: Mapped[str | None] = mapped_column(String(16), nullable=True)
    sentiment_source: Mapped[str] = mapped_column(String(16), nullable=False, default="extraction", server_default="extraction")

    author = relationship("XSignalAuthor", back_populates="mentions")
    post = relationship("XSignalPost", back_populates="mentions")
