from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime

from sqlalchemy import select

from stockanalyse_api.domain.backtests.models import BacktestRun
from stockanalyse_api.domain.screens.models import StrategyConfiguration
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
    error_message: str | None
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
        error_message=run.error_message,
        parameter_set={
            "id": configuration.id,
            "version": configuration.version,
            "rps_threshold": configuration.rps_threshold,
            "high_proximity_threshold_pct": f"{configuration.high_proximity_threshold_pct:.2f}",
        },
    )


def launch_backtest_run(session, *, start_date: date, end_date: date) -> BacktestRunSummary:
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date.")

    configuration_snapshot = get_active_strategy_configuration(session)
    configuration = session.get(StrategyConfiguration, configuration_snapshot.id)
    assert configuration is not None

    run = BacktestRun(
        strategy_configuration_id=configuration.id,
        start_date=start_date,
        end_date=end_date,
        started_at=datetime.now(UTC),
        completed_at=None,
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
