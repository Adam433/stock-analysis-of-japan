from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from stockanalyse_api.db.base import Base, TimestampMixin


class StrategyConfiguration(TimestampMixin, Base):
    __tablename__ = "strategy_configurations"

    id: Mapped[int] = mapped_column(primary_key=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    rps_threshold: Mapped[int] = mapped_column(Integer, nullable=False)
    selected_rps_windows: Mapped[str] = mapped_column(String(64), nullable=False, default="50,120,250")
    min_rps_lines_required: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    high_proximity_threshold_pct: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="1")


class ScreenRun(TimestampMixin, Base):
    __tablename__ = "screen_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    strategy_configuration_id: Mapped[int] = mapped_column(
        ForeignKey("strategy_configurations.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    rps_definition_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    total_candidates: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    qualified_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="completed", server_default="completed")


class ScreenRunResult(TimestampMixin, Base):
    __tablename__ = "screen_run_results"

    id: Mapped[int] = mapped_column(primary_key=True)
    screen_run_id: Mapped[int] = mapped_column(
        ForeignKey("screen_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    rps_50: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    rps_120: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    rps_250: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    best_rps_value: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    rps_threshold: Mapped[int] = mapped_column(Integer, nullable=False)
    high_proximity_ratio: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    high_proximity_threshold_pct: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    max_drawdown_from_high_pct: Mapped[Decimal | None] = mapped_column(Numeric(6, 2), nullable=True)
    rps_condition_passed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    high_proximity_condition_passed: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="0",
    )
