from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import select

from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.screens.models import ScreenRun, ScreenRunResult, StrategyConfiguration


@dataclass(slots=True)
class ScreenRunSummary:
    id: int
    strategy_configuration_id: int
    trade_date: str
    executed_at: str
    total_candidates: int
    qualified_count: int
    status: str
    parameter_set: dict[str, object]
    qualified_results: list[dict[str, object]]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _quantize(value: Decimal, pattern: str) -> Decimal:
    return value.quantize(Decimal(pattern), rounding=ROUND_HALF_UP)


def _latest_trade_date(session) -> datetime.date | None:
    return session.execute(select(DerivedIndicatorDaily.trade_date).order_by(DerivedIndicatorDaily.trade_date.desc()).limit(1)).scalar_one_or_none()


def _active_configuration(session) -> StrategyConfiguration:
    configuration = session.execute(
        select(StrategyConfiguration)
        .where(StrategyConfiguration.is_active.is_(True))
        .order_by(StrategyConfiguration.version.desc(), StrategyConfiguration.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    if configuration is None:
        raise ValueError("No active strategy configuration is available.")
    return configuration


def execute_screen_run(session) -> ScreenRunSummary:
    configuration = _active_configuration(session)
    trade_date = _latest_trade_date(session)
    if trade_date is None:
        raise ValueError("No derived indicator facts are available for screening.")

    indicators = session.execute(
        select(DerivedIndicatorDaily, Instrument)
        .join(Instrument, Instrument.id == DerivedIndicatorDaily.instrument_id)
        .where(DerivedIndicatorDaily.trade_date == trade_date)
        .order_by(Instrument.symbol.asc())
    ).all()
    if not indicators:
        raise ValueError("No derived indicator facts are available for the latest trade date.")

    screen_run = ScreenRun(
        strategy_configuration_id=configuration.id,
        trade_date=trade_date,
        executed_at=datetime.now(UTC),
        total_candidates=len(indicators),
        qualified_count=0,
        status="completed",
    )
    session.add(screen_run)
    session.flush()

    qualified_results: list[dict[str, object]] = []
    qualified_count = 0
    proximity_limit = Decimal("1") - (configuration.high_proximity_threshold_pct / Decimal("100"))

    for indicator_row, instrument in indicators:
        rps_values = [value for value in (indicator_row.rps_50, indicator_row.rps_120, indicator_row.rps_250) if value is not None]
        best_rps_value = max(rps_values) if rps_values else None
        rps_condition_passed = best_rps_value is not None and best_rps_value >= configuration.rps_threshold

        high_proximity_ratio = indicator_row.high_proximity_ratio
        high_proximity_condition_passed = (
            high_proximity_ratio is not None and high_proximity_ratio >= proximity_limit
        )
        max_drawdown_from_high_pct = None
        if high_proximity_ratio is not None:
            max_drawdown_from_high_pct = _quantize((Decimal("1") - high_proximity_ratio) * Decimal("100"), "0.01")

        passed = rps_condition_passed and high_proximity_condition_passed
        if passed:
            qualified_count += 1

        result = ScreenRunResult(
            screen_run_id=screen_run.id,
            instrument_id=instrument.id,
            trade_date=trade_date,
            passed=passed,
            rps_50=indicator_row.rps_50,
            rps_120=indicator_row.rps_120,
            rps_250=indicator_row.rps_250,
            best_rps_value=best_rps_value,
            rps_threshold=configuration.rps_threshold,
            high_proximity_ratio=high_proximity_ratio,
            high_proximity_threshold_pct=configuration.high_proximity_threshold_pct,
            max_drawdown_from_high_pct=max_drawdown_from_high_pct,
            rps_condition_passed=rps_condition_passed,
            high_proximity_condition_passed=high_proximity_condition_passed,
        )
        session.add(result)

        if passed:
            qualified_results.append(
                {
                    "instrument_id": instrument.id,
                    "symbol": instrument.symbol,
                    "exchange": instrument.exchange,
                    "trade_date": trade_date.isoformat(),
                    "best_rps_value": f"{best_rps_value:.2f}" if best_rps_value is not None else None,
                    "rps_threshold": configuration.rps_threshold,
                    "high_proximity_ratio": f"{high_proximity_ratio:.6f}" if high_proximity_ratio is not None else None,
                    "high_proximity_threshold_pct": f"{configuration.high_proximity_threshold_pct:.2f}",
                    "max_drawdown_from_high_pct": f"{max_drawdown_from_high_pct:.2f}" if max_drawdown_from_high_pct is not None else None,
                    "rps_condition_passed": rps_condition_passed,
                    "high_proximity_condition_passed": high_proximity_condition_passed,
                }
            )

    screen_run.qualified_count = qualified_count
    session.commit()

    return ScreenRunSummary(
        id=screen_run.id,
        strategy_configuration_id=configuration.id,
        trade_date=trade_date.isoformat(),
        executed_at=screen_run.executed_at.isoformat(),
        total_candidates=screen_run.total_candidates,
        qualified_count=screen_run.qualified_count,
        status=screen_run.status,
        parameter_set={
            "id": configuration.id,
            "version": configuration.version,
            "rps_threshold": configuration.rps_threshold,
            "high_proximity_threshold_pct": f"{configuration.high_proximity_threshold_pct:.2f}",
        },
        qualified_results=qualified_results,
    )


def get_screen_run(session, screen_run_id: int) -> ScreenRunSummary | None:
    screen_run = session.get(ScreenRun, screen_run_id)
    if screen_run is None:
        return None

    configuration = session.get(StrategyConfiguration, screen_run.strategy_configuration_id)
    results = session.execute(
        select(ScreenRunResult, Instrument)
        .join(Instrument, Instrument.id == ScreenRunResult.instrument_id)
        .where(ScreenRunResult.screen_run_id == screen_run_id, ScreenRunResult.passed.is_(True))
        .order_by(Instrument.symbol.asc())
    ).all()

    qualified_results = [
        {
            "instrument_id": instrument.id,
            "symbol": instrument.symbol,
            "exchange": instrument.exchange,
            "trade_date": result.trade_date.isoformat(),
            "best_rps_value": f"{result.best_rps_value:.2f}" if result.best_rps_value is not None else None,
            "rps_threshold": result.rps_threshold,
            "high_proximity_ratio": f"{result.high_proximity_ratio:.6f}" if result.high_proximity_ratio is not None else None,
            "high_proximity_threshold_pct": f"{result.high_proximity_threshold_pct:.2f}",
            "max_drawdown_from_high_pct": f"{result.max_drawdown_from_high_pct:.2f}" if result.max_drawdown_from_high_pct is not None else None,
            "rps_condition_passed": result.rps_condition_passed,
            "high_proximity_condition_passed": result.high_proximity_condition_passed,
        }
        for result, instrument in results
    ]

    return ScreenRunSummary(
        id=screen_run.id,
        strategy_configuration_id=screen_run.strategy_configuration_id,
        trade_date=screen_run.trade_date.isoformat(),
        executed_at=screen_run.executed_at.isoformat(),
        total_candidates=screen_run.total_candidates,
        qualified_count=screen_run.qualified_count,
        status=screen_run.status,
        parameter_set={
            "id": configuration.id,
            "version": configuration.version,
            "rps_threshold": configuration.rps_threshold,
            "high_proximity_threshold_pct": f"{configuration.high_proximity_threshold_pct:.2f}",
        },
        qualified_results=qualified_results,
    )


def get_latest_screen_run(session) -> ScreenRunSummary | None:
    screen_run_id = session.execute(
        select(ScreenRun.id).order_by(ScreenRun.executed_at.desc(), ScreenRun.id.desc()).limit(1)
    ).scalar_one_or_none()
    if screen_run_id is None:
        return None
    return get_screen_run(session, screen_run_id)
