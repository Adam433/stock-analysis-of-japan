from __future__ import annotations

from sqlalchemy import select

from stockanalyse_api.domain.backtests.models import BacktestRun
from stockanalyse_api.domain.screens.models import ScreenRun, StrategyConfiguration
from stockanalyse_api.services.rps_semantics import normalize_rps_definition_version
from stockanalyse_api.services.strategy_config import _deserialize_selected_rps_windows

SOURCE_SCREEN_RUN_UNAVAILABLE_ERROR = "source_screen_run_unavailable"


class SourceScreenRunUnavailableError(LookupError):
    """Raised when a portfolio-return run can no longer resolve its source screen run."""

    def __init__(self, backtest_run_id: int):
        super().__init__(SOURCE_SCREEN_RUN_UNAVAILABLE_ERROR)
        self.backtest_run_id = backtest_run_id


def resolve_screen_run_or_unavailable(session, run_id: int) -> dict[str, object] | None:
    run = session.get(BacktestRun, run_id)
    if run is None:
        raise LookupError("Backtest run not found.")
    if run.backtest_lifecycle != "portfolio_return":
        raise ValueError("Only portfolio_return runs support this endpoint.")
    if run.source_screen_run_id is None:
        return None

    row = session.execute(
        select(ScreenRun, StrategyConfiguration)
        .join(StrategyConfiguration, StrategyConfiguration.id == ScreenRun.strategy_configuration_id)
        .where(ScreenRun.id == run.source_screen_run_id)
    ).one_or_none()
    if row is None:
        return None

    screen_run, configuration = row
    if screen_run.status != "completed":
        return None

    parameter_snapshot = {
        "id": configuration.id,
        "version": configuration.version,
        "rps_threshold": configuration.rps_threshold,
        "high_proximity_threshold_pct": f"{configuration.high_proximity_threshold_pct:.2f}",
        "selected_rps_windows": _deserialize_selected_rps_windows(configuration.selected_rps_windows),
        "min_rps_lines_required": configuration.min_rps_lines_required,
        "is_active": configuration.is_active,
    }
    semantics_snapshot = {
        "source_screen_run_id": screen_run.id,
        "strategy_configuration_id": configuration.id,
        "strategy_configuration_version": configuration.version,
        "rps_definition_version": normalize_rps_definition_version(screen_run.rps_definition_version),
        "rps_threshold": configuration.rps_threshold,
        "high_proximity_threshold_pct": f"{configuration.high_proximity_threshold_pct:.2f}",
        "selected_rps_windows": parameter_snapshot["selected_rps_windows"],
        "min_rps_lines_required": configuration.min_rps_lines_required,
    }

    return {
        "backtest_run": {
            "id": run.id,
            "status": run.status,
            "backtest_lifecycle": run.backtest_lifecycle,
            "source_screen_run_id": run.source_screen_run_id,
            "rps_definition_version": run.rps_definition_version,
        },
        "source_screen_run": {
            "id": screen_run.id,
            "trade_date": screen_run.trade_date.isoformat(),
            "status": screen_run.status,
            "strategy_configuration_id": screen_run.strategy_configuration_id,
            "strategy_configuration_version": configuration.version,
            "rps_definition_version": normalize_rps_definition_version(screen_run.rps_definition_version),
        },
        "parameter_snapshot": parameter_snapshot,
        "dataset_version": {
            "trade_date_start": run.dataset_trade_date_start.isoformat() if run.dataset_trade_date_start is not None else None,
            "trade_date_end": run.dataset_trade_date_end.isoformat() if run.dataset_trade_date_end is not None else None,
            "checksum": run.dataset_checksum,
        },
        "semantics_snapshot": semantics_snapshot,
    }


def resolve_semantics_via_source_screen_run(session, run_id: int) -> dict[str, object]:
    traceability = resolve_screen_run_or_unavailable(session, run_id)
    if traceability is None:
        raise SourceScreenRunUnavailableError(run_id)
    return traceability
