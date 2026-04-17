from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation

from sqlalchemy import desc, select

from stockanalyse_api.domain.backtests.models import BacktestRun
from stockanalyse_api.domain.screens.models import ScreenRun, StrategyConfiguration
from stockanalyse_api.services.backtesting import BacktestRunSummary, serialize_backtest_run
from stockanalyse_api.services.portfolio_backtest_defaults import (
    MVP_ENTRY_DEFERRAL_WINDOW_DAYS,
    MVP_HOLDING_DAYS,
    MVP_PORTFOLIO_CAP,
    MVP_STOP_LOSS_PCT,
)
from stockanalyse_api.services.rps_semantics import APPROVED_RPS_DEFINITION_VERSION

DEBOUNCE_WINDOW_SECONDS = 5


def dispatch_portfolio_return_backtest_execution(session, run: BacktestRun) -> None:
    """Execution handoff stub for Story 5.2."""


def _coerce_decimal(value: Decimal | float | int | str | None, *, field_name: str) -> Decimal:
    if value is None:
        raise ValueError(f"{field_name} is required.")

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be a decimal number.") from exc


def _normalize_launch_parameters(
    *,
    holding_days: int | None,
    stop_loss_pct: Decimal | float | int | str | None,
    portfolio_cap: int | None,
    entry_deferral_window_days: int | None,
) -> tuple[int, Decimal, int, int]:
    normalized_holding_days = MVP_HOLDING_DAYS if holding_days is None else int(holding_days)
    normalized_stop_loss_pct = MVP_STOP_LOSS_PCT if stop_loss_pct is None else _coerce_decimal(
        stop_loss_pct,
        field_name="stop_loss_pct",
    )
    normalized_portfolio_cap = MVP_PORTFOLIO_CAP if portfolio_cap is None else int(portfolio_cap)
    normalized_entry_deferral_window_days = (
        MVP_ENTRY_DEFERRAL_WINDOW_DAYS
        if entry_deferral_window_days is None
        else int(entry_deferral_window_days)
    )

    if normalized_holding_days < 1:
        raise ValueError("holding_days must be an integer greater than or equal to 1.")
    if normalized_stop_loss_pct <= Decimal("-1") or normalized_stop_loss_pct >= Decimal("0"):
        raise ValueError("stop_loss_pct must be greater than -1 and less than 0.")
    if normalized_portfolio_cap < 1:
        raise ValueError("portfolio_cap must be an integer greater than or equal to 1.")
    if normalized_entry_deferral_window_days < 1:
        raise ValueError("entry_deferral_window_days must be an integer greater than or equal to 1.")

    return (
        normalized_holding_days,
        normalized_stop_loss_pct.quantize(Decimal("0.0001")),
        normalized_portfolio_cap,
        normalized_entry_deferral_window_days,
    )


def _load_screen_run(session, *, screen_run_id: int) -> ScreenRun:
    screen_run = session.get(ScreenRun, screen_run_id)
    if screen_run is None:
        raise LookupError("Screen run not found.")
    if screen_run.status != "completed":
        raise ValueError("Only completed screen runs can launch a portfolio-return backtest.")
    return screen_run


def _load_strategy_configuration(session, *, strategy_configuration_id: int) -> StrategyConfiguration:
    configuration = session.get(StrategyConfiguration, strategy_configuration_id)
    if configuration is None:
        raise ValueError("Strategy configuration not found for screen run.")
    return configuration


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _find_latest_matching_run(
    session,
    *,
    screen_run_id: int,
    holding_days: int,
    stop_loss_pct: Decimal,
    portfolio_cap: int,
    entry_deferral_window_days: int,
) -> BacktestRun | None:
    return session.execute(
        select(BacktestRun)
        .where(
            BacktestRun.backtest_lifecycle == "portfolio_return",
            BacktestRun.source_screen_run_id == screen_run_id,
            BacktestRun.effective_holding_days == holding_days,
            BacktestRun.effective_stop_loss_pct == stop_loss_pct,
            BacktestRun.effective_portfolio_cap == portfolio_cap,
            BacktestRun.effective_entry_deferral_window_days == entry_deferral_window_days,
        )
        .order_by(desc(BacktestRun.started_at), desc(BacktestRun.id))
        .limit(1)
    ).scalar_one_or_none()


def _prepare_run_for_launch(
    run: BacktestRun,
    *,
    screen_run: ScreenRun,
    holding_days: int,
    stop_loss_pct: Decimal,
    portfolio_cap: int,
    entry_deferral_window_days: int,
) -> None:
    now = datetime.now(UTC)
    run.strategy_configuration_id = screen_run.strategy_configuration_id
    run.source_screen_run_id = screen_run.id
    run.start_date = screen_run.trade_date
    run.end_date = screen_run.trade_date
    run.started_at = now
    run.completed_at = None
    run.rps_definition_version = screen_run.rps_definition_version or APPROVED_RPS_DEFINITION_VERSION
    run.backtest_lifecycle = "portfolio_return"
    run.status = "running"
    run.dataset_trade_date_start = None
    run.dataset_trade_date_end = None
    run.dataset_checksum = None
    run.trade_dates_evaluated = 0
    run.total_candidates_evaluated = 0
    run.qualifying_observations = 0
    run.unique_qualified_instruments = 0
    run.first_qualified_trade_date = None
    run.last_qualified_trade_date = None
    run.result_checksum = None
    run.error_message = None
    run.effective_holding_days = holding_days
    run.effective_stop_loss_pct = stop_loss_pct
    run.effective_portfolio_cap = portfolio_cap
    run.effective_entry_deferral_window_days = entry_deferral_window_days


def _persist_failed_recoverable(session, run: BacktestRun, *, message: str) -> None:
    run.status = "failed-recoverable"
    run.error_message = message
    run.completed_at = datetime.now(UTC)
    session.commit()
    session.refresh(run)


def launch_portfolio_return_backtest(
    session,
    *,
    screen_run_id: int,
    holding_days: int | None = None,
    stop_loss_pct: Decimal | float | int | str | None = None,
    portfolio_cap: int | None = None,
    entry_deferral_window_days: int | None = None,
) -> BacktestRunSummary:
    (
        normalized_holding_days,
        normalized_stop_loss_pct,
        normalized_portfolio_cap,
        normalized_entry_deferral_window_days,
    ) = _normalize_launch_parameters(
        holding_days=holding_days,
        stop_loss_pct=stop_loss_pct,
        portfolio_cap=portfolio_cap,
        entry_deferral_window_days=entry_deferral_window_days,
    )
    screen_run = _load_screen_run(session, screen_run_id=screen_run_id)
    configuration = _load_strategy_configuration(
        session,
        strategy_configuration_id=screen_run.strategy_configuration_id,
    )
    latest_matching_run = _find_latest_matching_run(
        session,
        screen_run_id=screen_run_id,
        holding_days=normalized_holding_days,
        stop_loss_pct=normalized_stop_loss_pct,
        portfolio_cap=normalized_portfolio_cap,
        entry_deferral_window_days=normalized_entry_deferral_window_days,
    )
    now = datetime.now(UTC)
    debounce_threshold = now - timedelta(seconds=DEBOUNCE_WINDOW_SECONDS)

    if latest_matching_run is not None and latest_matching_run.status == "failed-recoverable":
        run = latest_matching_run
    elif latest_matching_run is not None and _as_utc(latest_matching_run.started_at) >= debounce_threshold:
        return serialize_backtest_run(latest_matching_run, configuration)
    else:
        run = BacktestRun(
            strategy_configuration_id=screen_run.strategy_configuration_id,
            source_screen_run_id=screen_run.id,
            start_date=screen_run.trade_date,
            end_date=screen_run.trade_date,
            started_at=now,
            completed_at=None,
            rps_definition_version=screen_run.rps_definition_version or APPROVED_RPS_DEFINITION_VERSION,
            backtest_lifecycle="portfolio_return",
            status="running",
            error_message=None,
            effective_holding_days=normalized_holding_days,
            effective_stop_loss_pct=normalized_stop_loss_pct,
            effective_portfolio_cap=normalized_portfolio_cap,
            effective_entry_deferral_window_days=normalized_entry_deferral_window_days,
        )
        session.add(run)

    _prepare_run_for_launch(
        run,
        screen_run=screen_run,
        holding_days=normalized_holding_days,
        stop_loss_pct=normalized_stop_loss_pct,
        portfolio_cap=normalized_portfolio_cap,
        entry_deferral_window_days=normalized_entry_deferral_window_days,
    )
    session.commit()
    session.refresh(run)

    try:
        dispatch_portfolio_return_backtest_execution(session, run)
    except Exception as exc:
        _persist_failed_recoverable(session, run, message=str(exc))
        return serialize_backtest_run(run, configuration)

    session.refresh(run)
    return serialize_backtest_run(run, configuration)
