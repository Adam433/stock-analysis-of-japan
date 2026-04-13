from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import CheckConstraint, Date, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from stockanalyse_api.db.base import Base, TimestampMixin

REFRESH_RUN_STATUS_VALUES = ("running", "succeeded", "partial", "failed")


class MarketDataRefreshRun(TimestampMixin, Base):
    __tablename__ = "market_data_refresh_runs"
    __table_args__ = (
        CheckConstraint(
            f"status IN {REFRESH_RUN_STATUS_VALUES}",
            name="market_data_refresh_runs_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    requested_symbols: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    latest_trade_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    rows_processed: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    rows_inserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    rows_updated: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    partial_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    unavailable_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
