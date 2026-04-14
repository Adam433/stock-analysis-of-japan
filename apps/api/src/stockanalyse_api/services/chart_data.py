from __future__ import annotations

from dataclasses import asdict, dataclass

from sqlalchemy import select

from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.domain.screens.models import ScreenRun, ScreenRunResult, StrategyConfiguration


@dataclass(slots=True)
class StockDetailPayload:
    instrument: dict[str, object]
    screen_run: dict[str, object]
    rule_breakdown: dict[str, object]
    latest_indicator_snapshot: dict[str, object]
    candlesticks: list[dict[str, object]]
    indicator_history: list[dict[str, object]]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def get_stock_detail_payload(session, *, instrument_id: int, screen_run_id: int) -> StockDetailPayload | None:
    instrument = session.get(Instrument, instrument_id)
    screen_run = session.get(ScreenRun, screen_run_id)
    if instrument is None or screen_run is None:
        return None

    result = session.execute(
        select(ScreenRunResult)
        .where(
            ScreenRunResult.screen_run_id == screen_run_id,
            ScreenRunResult.instrument_id == instrument_id,
        )
        .limit(1)
    ).scalar_one_or_none()
    if result is None:
        return None

    configuration = session.get(StrategyConfiguration, screen_run.strategy_configuration_id)
    indicator_row = session.execute(
        select(DerivedIndicatorDaily)
        .where(
            DerivedIndicatorDaily.instrument_id == instrument_id,
            DerivedIndicatorDaily.trade_date == screen_run.trade_date,
        )
        .limit(1)
    ).scalar_one_or_none()

    candle_rows = session.execute(
        select(MarketDataDaily)
        .where(
            MarketDataDaily.instrument_id == instrument_id,
            MarketDataDaily.trade_date <= screen_run.trade_date,
        )
        .order_by(MarketDataDaily.trade_date.desc())
        .limit(120)
    ).scalars().all()
    candle_rows.reverse()

    candlesticks = [
        {
            "trade_date": candle.trade_date.isoformat(),
            "open": f"{candle.open:.6f}" if candle.open is not None else None,
            "high": f"{candle.high:.6f}" if candle.high is not None else None,
            "low": f"{candle.low:.6f}" if candle.low is not None else None,
            "close": f"{candle.close:.6f}" if candle.close is not None else None,
            "adj_close": f"{candle.adj_close:.6f}" if candle.adj_close is not None else None,
            "volume": candle.volume,
            "data_status": candle.data_status,
        }
        for candle in candle_rows
    ]

    indicator_rows = session.execute(
        select(DerivedIndicatorDaily)
        .where(
            DerivedIndicatorDaily.instrument_id == instrument_id,
            DerivedIndicatorDaily.trade_date >= candle_rows[0].trade_date if candle_rows else screen_run.trade_date,
            DerivedIndicatorDaily.trade_date <= screen_run.trade_date,
        )
        .order_by(DerivedIndicatorDaily.trade_date.asc())
    ).scalars().all()

    indicator_history = [
        {
            "trade_date": row.trade_date.isoformat(),
            "rps_50": f"{row.rps_50:.2f}" if row.rps_50 is not None else None,
            "rps_120": f"{row.rps_120:.2f}" if row.rps_120 is not None else None,
            "rps_250": f"{row.rps_250:.2f}" if row.rps_250 is not None else None,
            "high_proximity_ratio": (
                f"{row.high_proximity_ratio:.6f}" if row.high_proximity_ratio is not None else None
            ),
        }
        for row in indicator_rows
    ]

    latest_indicator_snapshot = {
        "trade_date": screen_run.trade_date.isoformat(),
        "rps_50": f"{indicator_row.rps_50:.2f}" if indicator_row and indicator_row.rps_50 is not None else None,
        "rps_120": f"{indicator_row.rps_120:.2f}" if indicator_row and indicator_row.rps_120 is not None else None,
        "rps_250": f"{indicator_row.rps_250:.2f}" if indicator_row and indicator_row.rps_250 is not None else None,
        "fifty_two_week_high": (
            f"{indicator_row.fifty_two_week_high:.6f}"
            if indicator_row and indicator_row.fifty_two_week_high is not None
            else None
        ),
        "high_proximity_ratio": (
            f"{indicator_row.high_proximity_ratio:.6f}"
            if indicator_row and indicator_row.high_proximity_ratio is not None
            else None
        ),
    }

    return StockDetailPayload(
        instrument={
            "id": instrument.id,
            "symbol": instrument.symbol,
            "exchange": instrument.exchange,
            "name": instrument.name,
            "currency": instrument.currency,
        },
        screen_run={
            "id": screen_run.id,
            "trade_date": screen_run.trade_date.isoformat(),
            "executed_at": screen_run.executed_at.isoformat(),
            "status": screen_run.status,
            "strategy_configuration_version": configuration.version if configuration is not None else None,
        },
        rule_breakdown={
            "passed": result.passed,
            "rps_condition": {
                "passed": result.rps_condition_passed,
                "best_rps_value": f"{result.best_rps_value:.2f}" if result.best_rps_value is not None else None,
                "threshold": result.rps_threshold,
                "rps_50": f"{result.rps_50:.2f}" if result.rps_50 is not None else None,
                "rps_120": f"{result.rps_120:.2f}" if result.rps_120 is not None else None,
                "rps_250": f"{result.rps_250:.2f}" if result.rps_250 is not None else None,
            },
            "high_proximity_condition": {
                "passed": result.high_proximity_condition_passed,
                "high_proximity_ratio": (
                    f"{result.high_proximity_ratio:.6f}" if result.high_proximity_ratio is not None else None
                ),
                "threshold_pct": f"{result.high_proximity_threshold_pct:.2f}",
                "max_drawdown_from_high_pct": (
                    f"{result.max_drawdown_from_high_pct:.2f}"
                    if result.max_drawdown_from_high_pct is not None
                    else None
                ),
            },
        },
        latest_indicator_snapshot=latest_indicator_snapshot,
        candlesticks=candlesticks,
        indicator_history=indicator_history,
    )
