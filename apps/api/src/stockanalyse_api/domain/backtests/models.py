from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import CheckConstraint, Date, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from stockanalyse_api.db.base import Base, TimestampMixin

BACKTEST_RUN_STATUS_VALUES = ("running", "completed", "failed")


class BacktestRun(TimestampMixin, Base):
    __tablename__ = "backtest_runs"
    __table_args__ = (
        CheckConstraint(
            f"status IN {BACKTEST_RUN_STATUS_VALUES}",
            name="backtest_runs_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    strategy_configuration_id: Mapped[int] = mapped_column(
        ForeignKey("strategy_configurations.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    start_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    end_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    rps_definition_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    dataset_trade_date_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    dataset_trade_date_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    dataset_checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running", server_default="running")
    trade_dates_evaluated: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    total_candidates_evaluated: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    qualifying_observations: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    unique_qualified_instruments: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    first_qualified_trade_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    last_qualified_trade_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    result_checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
