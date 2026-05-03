from __future__ import annotations

from datetime import date, datetime

from decimal import Decimal

from sqlalchemy import Boolean, CheckConstraint, Date, DateTime, ForeignKey, Index, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from stockanalyse_api.db.base import Base, TimestampMixin

BACKTEST_RUN_STATUS_VALUES = (
    "running",
    "completed",
    "failed",
    "failed-recoverable",
    "failed-data-insufficient",
)
BACKTEST_LIFECYCLE_VALUES = ("portfolio_return", "legacy_condition_hit")
OPTIMIZATION_RUN_STATUS_VALUES = (
    "running",
    "completed",
    "failed",
    "cancel_requested",
    "cancelled",
)
OPTIMIZATION_RESULT_STATUS_VALUES = ("completed", "failed")
CUP_HANDLE_MATERIALIZATION_STATUS_VALUES = ("running", "completed", "failed")
PORTFOLIO_RETURN_PROVENANCE_CONSTRAINT = (
    "(backtest_lifecycle = 'legacy_condition_hit') OR "
    "(backtest_lifecycle = 'portfolio_return' AND source_screen_run_id IS NOT NULL AND rps_definition_version IS NULL)"
)


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
        CheckConstraint(
            PORTFOLIO_RETURN_PROVENANCE_CONSTRAINT,
            name="backtest_runs_portfolio_return_provenance",
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
    ranking_policy_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    excluded_securities_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    portfolio_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    position_count_after_exclusions: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cumulative_return: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    equity_curve_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    per_security_returns_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    backtest_lifecycle: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running", server_default="running")
    trade_dates_evaluated: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    total_candidates_evaluated: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    qualifying_observations: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    unique_qualified_instruments: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    first_qualified_trade_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    last_qualified_trade_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    result_checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class OptimizationRun(TimestampMixin, Base):
    __tablename__ = "optimization_runs"
    __table_args__ = (
        CheckConstraint(
            f"status IN {OPTIMIZATION_RUN_STATUS_VALUES}",
            name="optimization_runs_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    market: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    train_start_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    train_end_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    validation_start_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    validation_end_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    objective: Mapped[str] = mapped_column(String(64), nullable=False, default="score", server_default="score")
    parameter_space_json: Mapped[str] = mapped_column(Text, nullable=False)
    parameter_sets_json: Mapped[str] = mapped_column(Text, nullable=False)
    data_snapshot_json: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running", server_default="running")
    total_parameter_sets: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    completed_parameter_sets: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    failed_parameter_sets: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    best_result_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "optimization_results.id",
            ondelete="SET NULL",
            use_alter=True,
            name="fk_optimization_runs_best_result_id_optimization_results",
        ),
        nullable=True,
        index=True,
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class OptimizationResult(TimestampMixin, Base):
    __tablename__ = "optimization_results"
    __table_args__ = (
        CheckConstraint(
            f"status IN {OPTIMIZATION_RESULT_STATUS_VALUES}",
            name="optimization_results_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    optimization_run_id: Mapped[int] = mapped_column(
        ForeignKey("optimization_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    parameter_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    parameters_json: Mapped[str] = mapped_column(Text, nullable=False)
    train_metrics_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    validation_metrics_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    score: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True, index=True)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="completed", server_default="completed")
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class OptimizationResultDetailCache(TimestampMixin, Base):
    __tablename__ = "optimization_result_detail_cache"

    id: Mapped[int] = mapped_column(primary_key=True)
    optimization_result_id: Mapped[int] = mapped_column(
        ForeignKey("optimization_results.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    max_trades_returned: Mapped[int] = mapped_column(Integer, nullable=False)
    train_result_json: Mapped[str] = mapped_column(Text, nullable=False)
    validation_result_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class StrategyPreset(TimestampMixin, Base):
    __tablename__ = "strategy_presets"

    id: Mapped[int] = mapped_column(primary_key=True)
    market: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    parameters_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    parameters_json: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    source_optimization_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("optimization_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    source_optimization_result_id: Mapped[int | None] = mapped_column(
        ForeignKey("optimization_results.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )


class CupHandleMaterializationRun(TimestampMixin, Base):
    __tablename__ = "cup_handle_materialization_runs"
    __table_args__ = (
        CheckConstraint(
            f"status IN {CUP_HANDLE_MATERIALIZATION_STATUS_VALUES}",
            name="cup_handle_materialization_runs_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    market: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running", server_default="running")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_start_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source_end_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    latest_market_data_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    generation_bounds_json: Mapped[str] = mapped_column(Text, nullable=False)
    feature_windows_json: Mapped[str] = mapped_column(Text, nullable=False)
    detector_version: Mapped[str] = mapped_column(String(64), nullable=False)
    events_created: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    symbols_processed: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class CupHandlePatternEvent(TimestampMixin, Base):
    __tablename__ = "cup_handle_pattern_events"
    __table_args__ = (
        Index(
            "ix_cup_handle_pattern_events_market_breakout_date_instrument",
            "market",
            "breakout_date",
            "instrument_id",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    market: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    materialization_run_id: Mapped[int] = mapped_column(
        ForeignKey("cup_handle_materialization_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    symbol_snapshot: Mapped[str] = mapped_column(String(32), nullable=False)
    breakout_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    left_lip_date: Mapped[date] = mapped_column(Date, nullable=False)
    cup_bottom_date: Mapped[date] = mapped_column(Date, nullable=False)
    right_lip_date: Mapped[date] = mapped_column(Date, nullable=False)
    handle_low_date: Mapped[date] = mapped_column(Date, nullable=False)
    cup_duration: Mapped[int] = mapped_column(Integer, nullable=False)
    handle_duration: Mapped[int] = mapped_column(Integer, nullable=False)
    total_duration: Mapped[int] = mapped_column(Integer, nullable=False)
    cup_depth_pct: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    handle_depth_pct: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    right_lip_delta_pct: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    handle_low_position_pct: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    handle_depth_to_cup_depth_pct: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    handle_high_above_lip_pct: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    bottom_dwell_days_zone_20: Mapped[int] = mapped_column(Integer, nullable=False)
    bottom_dwell_days_zone_35: Mapped[int] = mapped_column(Integer, nullable=False)
    bottom_span_pct_zone_20: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    bottom_span_pct_zone_35: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    left_side_duration_pct: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    right_side_duration_pct: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    prior_uptrend_pct_60: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), nullable=True)
    prior_uptrend_pct_90: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), nullable=True)
    prior_uptrend_pct_120: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), nullable=True)
    prior_uptrend_pct_180: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), nullable=True)
    breakout_volume_ratio_20: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    breakout_volume_ratio_50: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    breakout_volume_ratio_60: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    breakout_close_over_resistance_pct: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    data_start_date: Mapped[date] = mapped_column(Date, nullable=False)
    data_end_date: Mapped[date] = mapped_column(Date, nullable=False)
    detector_version: Mapped[str] = mapped_column(String(64), nullable=False)
