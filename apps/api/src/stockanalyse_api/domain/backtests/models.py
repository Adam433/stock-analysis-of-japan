from __future__ import annotations

from datetime import date, datetime

from decimal import Decimal

from sqlalchemy import CheckConstraint, Date, DateTime, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from stockanalyse_api.db.base import Base, TimestampMixin

BACKTEST_RUN_STATUS_VALUES = ("running", "completed", "failed", "failed-recoverable")
BACKTEST_LIFECYCLE_VALUES = ("portfolio_return", "legacy_condition_hit")


class BacktestRun(TimestampMixin, Base):
    __tablename__ = "backtest_runs"
    __table_args__ = (
        CheckConstraint(
            f"status IN {BACKTEST_RUN_STATUS_VALUES}",
            name="backtest_runs_status",
        ),
        CheckConstraint(
            f"backtest_lifecycle IN {BACKTEST_LIFECYCLE_VALUES}",
            name="backtest_runs_lifecycle",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    strategy_configuration_id: Mapped[int] = mapped_column(
        ForeignKey("strategy_configurations.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    source_screen_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("screen_runs.id", ondelete="RESTRICT"),
        nullable=True,
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
    effective_holding_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    effective_stop_loss_pct: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), nullable=True)
    effective_portfolio_cap: Mapped[int | None] = mapped_column(Integer, nullable=True)
    effective_entry_deferral_window_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    backtest_lifecycle: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running", server_default="running")
    trade_dates_evaluated: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    total_candidates_evaluated: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    qualifying_observations: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    unique_qualified_instruments: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    first_qualified_trade_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    last_qualified_trade_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    result_checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
