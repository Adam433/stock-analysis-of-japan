from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from hashlib import sha256

from sqlalchemy import select

from stockanalyse_api.domain.backtests.models import BacktestRun
from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.screens.models import StrategyConfiguration
from stockanalyse_api.services.rps_semantics import (
    APPROVED_RPS_DEFINITION_VERSION,
    normalize_rps_definition_version,
)
from stockanalyse_api.services.screening import evaluate_indicator_snapshot
from stockanalyse_api.services.strategy_config import get_active_strategy_configuration


@dataclass(slots=True)
class BacktestRunSummary:
    id: int
    strategy_configuration_id: int
    status: str
    start_date: str
    end_date: str
    started_at: str
    completed_at: str | None
    rps_definition_version: str | None
    dataset_trade_date_start: str | None
    dataset_trade_date_end: str | None
    dataset_checksum: str | None
    error_message: str | None
    result_summary: dict[str, object]
    parameter_set: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _serialize(run: BacktestRun, configuration: StrategyConfiguration) -> BacktestRunSummary:
    return BacktestRunSummary(
        id=run.id,
        strategy_configuration_id=run.strategy_configuration_id,
        status=run.status,
        start_date=run.start_date.isoformat(),
        end_date=run.end_date.isoformat(),
        started_at=run.started_at.isoformat(),
        completed_at=run.completed_at.isoformat() if run.completed_at is not None else None,
        rps_definition_version=normalize_rps_definition_version(run.rps_definition_version),
        dataset_trade_date_start=run.dataset_trade_date_start.isoformat() if run.dataset_trade_date_start is not None else None,
        dataset_trade_date_end=run.dataset_trade_date_end.isoformat() if run.dataset_trade_date_end is not None else None,
        dataset_checksum=run.dataset_checksum,
        error_message=run.error_message,
        result_summary={
            "trade_dates_evaluated": run.trade_dates_evaluated,
            "total_candidates_evaluated": run.total_candidates_evaluated,
            "qualifying_observations": run.qualifying_observations,
            "unique_qualified_instruments": run.unique_qualified_instruments,
            "first_qualified_trade_date": (
                run.first_qualified_trade_date.isoformat() if run.first_qualified_trade_date is not None else None
            ),
            "last_qualified_trade_date": (
                run.last_qualified_trade_date.isoformat() if run.last_qualified_trade_date is not None else None
            ),
            "result_checksum": run.result_checksum,
        },
        parameter_set={
            "id": configuration.id,
            "version": configuration.version,
            "rps_threshold": configuration.rps_threshold,
            "selected_rps_windows": [int(part) for part in configuration.selected_rps_windows.split(",") if part],
            "min_rps_lines_required": configuration.min_rps_lines_required,
            "high_proximity_threshold_pct": f"{configuration.high_proximity_threshold_pct:.2f}",
        },
    )


def launch_backtest_run(session, *, start_date: date, end_date: date) -> BacktestRunSummary:
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date.")

    configuration_snapshot = get_active_strategy_configuration(session)
    configuration = session.get(StrategyConfiguration, configuration_snapshot.id)
    if configuration is None:
        raise ValueError(f"StrategyConfiguration with id={configuration_snapshot.id} not found.")

    run = BacktestRun(
        strategy_configuration_id=configuration.id,
        start_date=start_date,
        end_date=end_date,
        started_at=datetime.now(UTC),
        completed_at=None,
        rps_definition_version=APPROVED_RPS_DEFINITION_VERSION,
        status="running",
        error_message=None,
    )
    session.add(run)
    session.commit()
    session.refresh(run)

    return _serialize(run, configuration)


def get_backtest_run(session, run_id: int) -> BacktestRunSummary | None:
    run = session.get(BacktestRun, run_id)
    if run is None:
        return None

    configuration = session.get(StrategyConfiguration, run.strategy_configuration_id)
    if configuration is None:
        return None
    return _serialize(run, configuration)


def get_latest_backtest_run(session) -> BacktestRunSummary | None:
    run_id = session.execute(
        select(BacktestRun.id).order_by(BacktestRun.started_at.desc(), BacktestRun.id.desc()).limit(1)
    ).scalar_one_or_none()
    if run_id is None:
        return None
    return get_backtest_run(session, run_id)


def list_backtest_runs(session, *, limit: int = 50, offset: int = 0) -> list[BacktestRunSummary]:
    rows = session.execute(
        select(BacktestRun, StrategyConfiguration)
        .join(StrategyConfiguration, StrategyConfiguration.id == BacktestRun.strategy_configuration_id)
        .order_by(BacktestRun.started_at.desc(), BacktestRun.id.desc())
        .limit(limit)
        .offset(offset)
    ).all()

    return [_serialize(run, configuration) for run, configuration in rows]


def execute_backtest_run(session, run_id: int) -> BacktestRunSummary:
    run = session.get(BacktestRun, run_id)
    if run is None:
        raise LookupError("Backtest run not found.")

    configuration = session.get(StrategyConfiguration, run.strategy_configuration_id)
    if configuration is None:
        raise ValueError("Strategy configuration not found for backtest run.")

    indicators = session.execute(
        select(DerivedIndicatorDaily, Instrument)
        .join(Instrument, Instrument.id == DerivedIndicatorDaily.instrument_id)
        .where(
            DerivedIndicatorDaily.trade_date >= run.start_date,
            DerivedIndicatorDaily.trade_date <= run.end_date,
        )
        .order_by(DerivedIndicatorDaily.trade_date.asc(), Instrument.symbol.asc())
    ).all()

    if not indicators:
        run.status = "failed"
        run.error_message = "No derived indicator facts are available for the requested backtest range."
        run.completed_at = datetime.now(UTC)
        session.commit()
        raise ValueError(run.error_message)

    trade_dates: set[date] = set()
    unique_qualified_instruments: set[int] = set()
    qualifying_trade_dates: list[date] = []
    qualifying_tuples: list[str] = []
    dataset_tuples: list[str] = []
    total_candidates_evaluated = 0
    qualifying_observations = 0

    for indicator_row, instrument in indicators:
        trade_dates.add(indicator_row.trade_date)
        total_candidates_evaluated += 1
        dataset_tuples.append(
            ":".join(
                [
                    indicator_row.trade_date.isoformat(),
                    str(instrument.id),
                    instrument.symbol,
                    instrument.exchange,
                    f"{indicator_row.rps_50:.2f}" if indicator_row.rps_50 is not None else "null",
                    f"{indicator_row.rps_120:.2f}" if indicator_row.rps_120 is not None else "null",
                    f"{indicator_row.rps_250:.2f}" if indicator_row.rps_250 is not None else "null",
                    (
                        f"{indicator_row.high_proximity_ratio:.6f}"
                        if indicator_row.high_proximity_ratio is not None
                        else "null"
                    ),
                ]
            )
        )
        evaluation = evaluate_indicator_snapshot(indicator_row, configuration)
        if evaluation["passed"]:
            qualifying_observations += 1
            unique_qualified_instruments.add(instrument.id)
            qualifying_trade_dates.append(indicator_row.trade_date)
            qualifying_tuples.append(
                f"{indicator_row.trade_date.isoformat()}:{instrument.symbol}:{evaluation['best_rps_value']}:{evaluation['high_proximity_ratio']}"
            )

    checksum = sha256("|".join(qualifying_tuples).encode("utf-8")).hexdigest()
    dataset_checksum = sha256("|".join(dataset_tuples).encode("utf-8")).hexdigest()
    run.trade_dates_evaluated = len(trade_dates)
    run.total_candidates_evaluated = total_candidates_evaluated
    run.qualifying_observations = qualifying_observations
    run.unique_qualified_instruments = len(unique_qualified_instruments)
    run.first_qualified_trade_date = min(qualifying_trade_dates) if qualifying_trade_dates else None
    run.last_qualified_trade_date = max(qualifying_trade_dates) if qualifying_trade_dates else None
    run.dataset_trade_date_start = min(trade_dates) if trade_dates else None
    run.dataset_trade_date_end = max(trade_dates) if trade_dates else None
    run.dataset_checksum = dataset_checksum
    run.result_checksum = checksum
    run.status = "completed"
    run.error_message = None
    run.completed_at = datetime.now(UTC)
    session.commit()
    session.refresh(run)

    return _serialize(run, configuration)
