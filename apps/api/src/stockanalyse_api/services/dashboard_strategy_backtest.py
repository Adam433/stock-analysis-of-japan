from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from statistics import median
from typing import Callable


class BacktestCancelledError(Exception):
    """Raised when a should_cancel callback signals to abort an in-progress backtest."""

from sqlalchemy import distinct, select

from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.dashboard import CupHandleParams
from stockanalyse_api.services.dashboard import FundamentalGrowthParams
from stockanalyse_api.services.dashboard import _market_exchanges
from stockanalyse_api.services.dashboard import normalize_market
from stockanalyse_api.services.dashboard import preload_screen_candidate_cache
from stockanalyse_api.services.dashboard import screen_universe
from stockanalyse_api.services.market_data_adjustments import adjusted_close
from stockanalyse_api.services.market_data_adjustments import adjusted_ohlc
from stockanalyse_api.services.market_data_adjustments import adjusted_open
from stockanalyse_api.services.market_data_adjustments import is_complete_market_row

RATIO_PATTERN = Decimal("0.000001")
MAX_TRADE_LOOKAHEAD_ROWS = 5000


@dataclass(slots=True)
class CupHandleRpsTrade:
    signal_date: str
    instrument_id: int
    symbol: str
    entry_date: str
    entry_price: str
    exit_date: str
    exit_price: str
    exit_reason: str
    realized_return: str
    rps_score: str | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class CupHandleRpsSignalDay:
    signal_date: str
    hit_count: int
    selected_count: int
    completed_trades: int
    average_return: str | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class CupHandleRpsBacktestResult:
    start_date: str
    end_date: str
    signal_dates_evaluated: int
    total_candidates_evaluated: int
    qualifying_observations: int
    selected_trades: int
    completed_trades: int
    excluded_trades: int
    cumulative_average_return: str | None
    average_trade_return: str | None
    median_trade_return: str | None
    win_rate: str | None
    best_trade_return: str | None
    worst_trade_return: str | None
    stop_loss_trades: int
    stop_loss_trigger_ratio: str | None
    take_profit_trades: int
    take_profit_trigger_ratio: str | None
    rps_exit_trades: int
    rps_exit_trigger_ratio: str | None
    max_consecutive_losses: int
    signal_days: list[dict[str, object]]
    trades: list[dict[str, object]]
    excluded: list[dict[str, object]]
    parameters: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _quantize_ratio(value: Decimal) -> Decimal:
    return value.quantize(RATIO_PATTERN, rounding=ROUND_HALF_UP)


def _format_ratio(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{_quantize_ratio(value):.6f}"


def _hit_rps_score(hit: dict[str, object], selected_windows: list[int]) -> Decimal | None:
    values: list[Decimal] = []
    for window in selected_windows:
        raw_value = hit.get(f"rps_{window}")
        if raw_value is None:
            continue
        values.append(Decimal(str(raw_value)))
    if not values:
        return None
    return max(values)


def _max_consecutive_losses(trade_returns: list[Decimal]) -> int:
    current = 0
    maximum = 0
    for trade_return in trade_returns:
        if trade_return < 0:
            current += 1
            maximum = max(maximum, current)
        else:
            current = 0
    return maximum


def _load_trade_dates(
    session,
    *,
    start_date: date,
    end_date: date,
    market: str | None,
) -> list[date]:
    exchanges = _market_exchanges(market)
    has_market_instrument = session.execute(
        select(Instrument.id).where(Instrument.exchange.in_(exchanges)).limit(1)
    ).scalar_one_or_none()
    if has_market_instrument is None:
        return []
    return list(
        session.execute(
            select(distinct(DerivedIndicatorDaily.trade_date))
            .join(Instrument, Instrument.id == DerivedIndicatorDaily.instrument_id)
            .where(
                DerivedIndicatorDaily.trade_date >= start_date,
                DerivedIndicatorDaily.trade_date <= end_date,
                Instrument.exchange.in_(exchanges),
            )
            .order_by(DerivedIndicatorDaily.trade_date.asc())
        ).scalars()
    )


def _load_future_rows(
    session,
    *,
    instrument_id: int,
    signal_date: date,
    limit: int,
) -> list[MarketDataDaily]:
    return list(
        session.execute(
            select(MarketDataDaily)
            .where(
                MarketDataDaily.instrument_id == instrument_id,
                MarketDataDaily.trade_date > signal_date,
            )
            .order_by(MarketDataDaily.trade_date.asc())
            .limit(limit)
        ).scalars()
    )


def _valid_open(row: MarketDataDaily | None) -> Decimal | None:
    if row is None or not is_complete_market_row(row):
        return None
    return adjusted_open(row)


def _mark_price(row: MarketDataDaily, fallback: Decimal) -> Decimal:
    if not is_complete_market_row(row):
        return fallback
    return adjusted_close(row) or fallback


def _valid_close(row: MarketDataDaily | None) -> Decimal | None:
    if row is None or not is_complete_market_row(row):
        return None
    return adjusted_close(row)


def _stop_loss_exit_price(
    row: MarketDataDaily,
    *,
    entry_price: Decimal,
    stop_loss_pct: Decimal,
) -> Decimal | None:
    if not is_complete_market_row(row):
        return None
    ohlc = adjusted_ohlc(row)
    if ohlc.low is None:
        return None
    stop_price = entry_price * (Decimal("1") + stop_loss_pct)
    if ohlc.open is not None and ohlc.open <= stop_price:
        return ohlc.open
    if ohlc.low <= stop_price:
        return stop_price
    return None


def _indicator_rps_score(row: DerivedIndicatorDaily | None, selected_windows: list[int]) -> Decimal | None:
    if row is None:
        return None
    values: list[Decimal] = []
    for window in selected_windows:
        raw_value = getattr(row, f"rps_{window}", None)
        if raw_value is not None:
            values.append(Decimal(str(raw_value)))
    if not values:
        return None
    return max(values)


def _load_future_indicator_map(
    session,
    *,
    instrument_id: int,
    signal_date: date,
    limit: int,
) -> dict[date, DerivedIndicatorDaily]:
    if limit < 1:
        return {}
    rows = session.execute(
        select(DerivedIndicatorDaily)
        .where(
            DerivedIndicatorDaily.instrument_id == instrument_id,
            DerivedIndicatorDaily.trade_date > signal_date,
        )
        .order_by(DerivedIndicatorDaily.trade_date.asc())
        .limit(limit)
    ).scalars()
    return {row.trade_date: row for row in rows}


def _screen_cache_key(
    *,
    signal_date: date,
    market: str,
    use_rps: bool,
    rps_threshold: int,
    selected_rps_windows: list[int],
    min_rps_windows_passing: int,
    use_cup_handle: bool,
    cup_handle_params: CupHandleParams,
    fundamental_growth_params: FundamentalGrowthParams | None,
) -> str:
    payload = {
        "signal_date": signal_date.isoformat(),
        "market": market,
        "use_rps": use_rps,
        "rps_threshold": rps_threshold,
        "selected_rps_windows": selected_rps_windows,
        "min_rps_windows_passing": min_rps_windows_passing,
        "use_cup_handle": use_cup_handle,
        "cup_handle_params": cup_handle_params.to_dict(),
        "fundamental_growth_params": (
            fundamental_growth_params.to_dict()
            if fundamental_growth_params is not None
            else None
        ),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _simulate_trade(
    session,
    *,
    signal_date: date,
    hit: dict[str, object],
    selected_windows: list[int],
    holding_days: int | None,
    stop_loss_pct: Decimal,
    take_profit_pct: Decimal | None = None,
    rps_exit_threshold: int | None = None,
    entry_delay_days: int = 0,
    entry_deferral_window_days: int = 5,
    future_rows_cache: dict[tuple[object, ...], list[MarketDataDaily]] | None = None,
    future_indicator_cache: dict[tuple[object, ...], dict[date, DerivedIndicatorDaily]] | None = None,
) -> CupHandleRpsTrade | dict[str, object]:
    instrument_id = int(hit["instrument_id"])
    symbol = str(hit["symbol"])
    future_row_limit = (
        MAX_TRADE_LOOKAHEAD_ROWS
        if holding_days is None
        else entry_delay_days + entry_deferral_window_days + holding_days + 40
    )
    future_rows_cache_key = (
        "future_rows",
        instrument_id,
        signal_date.isoformat(),
        future_row_limit,
    )
    cached_rows = (
        future_rows_cache.get(future_rows_cache_key)
        if future_rows_cache is not None
        else None
    )
    if cached_rows is None:
        rows = _load_future_rows(
            session,
            instrument_id=instrument_id,
            signal_date=signal_date,
            limit=future_row_limit,
        )
        if future_rows_cache is not None:
            future_rows_cache[future_rows_cache_key] = rows
    else:
        rows = cached_rows
    if len(rows) < entry_delay_days + entry_deferral_window_days:
        return {
            "signal_date": signal_date.isoformat(),
            "instrument_id": instrument_id,
            "symbol": symbol,
            "reason": "data_insufficient_for_entry",
        }

    entry_index: int | None = None
    entry_price: Decimal | None = None
    entry_window_start = entry_delay_days
    entry_window_end = entry_window_start + entry_deferral_window_days
    for offset, row in enumerate(rows[entry_window_start:entry_window_end]):
        candidate_open = _valid_open(row)
        if candidate_open is not None:
            entry_index = entry_window_start + offset
            entry_price = candidate_open
            break

    if entry_index is None or entry_price is None:
        return {
            "signal_date": signal_date.isoformat(),
            "instrument_id": instrument_id,
            "symbol": symbol,
            "reason": "no_valid_open_in_entry_window",
        }

    entry_row = rows[entry_index]
    last_mark_price = entry_price
    indicator_by_date: dict[date, DerivedIndicatorDaily] = {}
    if rps_exit_threshold is not None:
        future_indicator_cache_key = (
            "future_indicator_map",
            instrument_id,
            signal_date.isoformat(),
            len(rows),
        )
        cached_indicator_map = (
            future_indicator_cache.get(future_indicator_cache_key)
            if future_indicator_cache is not None
            else None
        )
        if cached_indicator_map is None:
            indicator_by_date = _load_future_indicator_map(
                session,
                instrument_id=instrument_id,
                signal_date=signal_date,
                limit=len(rows),
            )
            if future_indicator_cache is not None:
                future_indicator_cache[future_indicator_cache_key] = indicator_by_date
        else:
            indicator_by_date = cached_indicator_map
    trigger_index: int | None = None
    trigger_reason = "holding_period_elapsed"
    immediate_exit_row: MarketDataDaily | None = None
    immediate_exit_price: Decimal | None = None
    held_trading_days = 0
    for index in range(entry_index, len(rows)):
        row = rows[index]
        last_mark_price = _mark_price(row, last_mark_price)
        held_trading_days += 1
        stop_exit_price = _stop_loss_exit_price(
            row,
            entry_price=entry_price,
            stop_loss_pct=stop_loss_pct,
        )
        if stop_exit_price is not None:
            trigger_index = index
            trigger_reason = "stop_loss"
            immediate_exit_row = row
            immediate_exit_price = stop_exit_price
            break
        mark_return = _quantize_ratio((last_mark_price / entry_price) - Decimal("1"))
        if take_profit_pct is not None and mark_return >= take_profit_pct:
            trigger_index = index
            trigger_reason = "take_profit"
            break
        rps_score = _indicator_rps_score(indicator_by_date.get(row.trade_date), selected_windows)
        if (
            rps_exit_threshold is not None
            and rps_score is not None
            and rps_score < Decimal(rps_exit_threshold)
        ):
            trigger_index = index
            trigger_reason = "rps_exit"
            break
        if holding_days is not None and held_trading_days >= holding_days:
            trigger_index = index
            break

    if trigger_index is None:
        if holding_days is None:
            for row in reversed(rows[entry_index:]):
                candidate_close = _valid_close(row)
                if candidate_close is not None:
                    realized_return = _quantize_ratio((candidate_close / entry_price) - Decimal("1"))
                    rps_score = _hit_rps_score(hit, selected_windows)
                    return CupHandleRpsTrade(
                        signal_date=signal_date.isoformat(),
                        instrument_id=instrument_id,
                        symbol=symbol,
                        entry_date=entry_row.trade_date.isoformat(),
                        entry_price=f"{entry_price:.6f}",
                        exit_date=row.trade_date.isoformat(),
                        exit_price=f"{candidate_close:.6f}",
                        exit_reason="data_end_mark",
                        realized_return=f"{realized_return:.6f}",
                        rps_score=f"{rps_score:.2f}" if rps_score is not None else None,
                    )
        return {
            "signal_date": signal_date.isoformat(),
            "instrument_id": instrument_id,
            "symbol": symbol,
            "reason": "data_insufficient_for_holding_period",
        }

    exit_row: MarketDataDaily | None = None
    exit_price: Decimal | None = None
    if immediate_exit_row is not None and immediate_exit_price is not None:
        exit_row = immediate_exit_row
        exit_price = immediate_exit_price
    else:
        for row in rows[trigger_index + 1 :]:
            candidate_open = _valid_open(row)
            if candidate_open is not None:
                exit_row = row
                exit_price = candidate_open
                break

    if exit_row is None or exit_price is None:
        return {
            "signal_date": signal_date.isoformat(),
            "instrument_id": instrument_id,
            "symbol": symbol,
            "reason": "data_insufficient_for_exit",
        }

    realized_return = _quantize_ratio((exit_price / entry_price) - Decimal("1"))
    rps_score = _hit_rps_score(hit, selected_windows)
    return CupHandleRpsTrade(
        signal_date=signal_date.isoformat(),
        instrument_id=instrument_id,
        symbol=symbol,
        entry_date=entry_row.trade_date.isoformat(),
        entry_price=f"{entry_price:.6f}",
        exit_date=exit_row.trade_date.isoformat(),
        exit_price=f"{exit_price:.6f}",
        exit_reason=trigger_reason,
        realized_return=f"{realized_return:.6f}",
        rps_score=f"{rps_score:.2f}" if rps_score is not None else None,
    )


def _trade_cache_key(
    *,
    signal_date: date,
    hit: dict[str, object],
    selected_windows: list[int],
    holding_days: int | None,
    stop_loss_pct: Decimal,
    take_profit_pct: Decimal | None,
    rps_exit_threshold: int | None,
    entry_delay_days: int,
    entry_deferral_window_days: int,
) -> tuple[object, ...]:
    return (
        "trade_simulation",
        signal_date.isoformat(),
        int(hit["instrument_id"]),
        str(hit["symbol"]),
        tuple(selected_windows),
        str(hit.get("rps_50")),
        str(hit.get("rps_120")),
        str(hit.get("rps_250")),
        holding_days,
        str(stop_loss_pct),
        str(take_profit_pct),
        rps_exit_threshold,
        entry_delay_days,
        entry_deferral_window_days,
    )


def _simulate_trade_with_cache(
    session,
    *,
    signal_date: date,
    hit: dict[str, object],
    selected_windows: list[int],
    holding_days: int | None,
    stop_loss_pct: Decimal,
    take_profit_pct: Decimal | None = None,
    rps_exit_threshold: int | None = None,
    entry_delay_days: int = 0,
    entry_deferral_window_days: int = 5,
    trade_cache: dict[tuple[object, ...], CupHandleRpsTrade | dict[str, object]] | None = None,
    future_rows_cache: dict[tuple[object, ...], list[MarketDataDaily]] | None = None,
    future_indicator_cache: dict[tuple[object, ...], dict[date, DerivedIndicatorDaily]] | None = None,
) -> CupHandleRpsTrade | dict[str, object]:
    if trade_cache is None:
        return _simulate_trade(
            session,
            signal_date=signal_date,
            hit=hit,
            selected_windows=selected_windows,
            holding_days=holding_days,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            rps_exit_threshold=rps_exit_threshold,
            entry_delay_days=entry_delay_days,
            entry_deferral_window_days=entry_deferral_window_days,
            future_rows_cache=future_rows_cache,
            future_indicator_cache=future_indicator_cache,
        )
    cache_key = _trade_cache_key(
        signal_date=signal_date,
        hit=hit,
        selected_windows=selected_windows,
        holding_days=holding_days,
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
        rps_exit_threshold=rps_exit_threshold,
        entry_delay_days=entry_delay_days,
        entry_deferral_window_days=entry_deferral_window_days,
    )
    cached = trade_cache.get(cache_key)
    if cached is not None:
        return cached
    trade_or_exclusion = _simulate_trade(
        session,
        signal_date=signal_date,
        hit=hit,
        selected_windows=selected_windows,
        holding_days=holding_days,
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
        rps_exit_threshold=rps_exit_threshold,
        entry_delay_days=entry_delay_days,
        entry_deferral_window_days=entry_deferral_window_days,
        future_rows_cache=future_rows_cache,
        future_indicator_cache=future_indicator_cache,
    )
    trade_cache[cache_key] = trade_or_exclusion
    return trade_or_exclusion


def run_cup_handle_rps_backtest(
    session,
    *,
    start_date: date,
    end_date: date,
    rps_threshold: int,
    selected_rps_windows: list[int],
    cup_handle_params: CupHandleParams,
    use_rps: bool = True,
    min_rps_windows_passing: int = 1,
    use_cup_handle: bool = True,
    fundamental_growth_params: FundamentalGrowthParams | None = None,
    market: str | None = None,
    holding_days: int | None = 130,
    stop_loss_pct: Decimal = Decimal("-0.08"),
    take_profit_pct: Decimal | None = None,
    rps_exit_threshold: int | None = None,
    portfolio_cap: int = 10,
    position_weight_pct: Decimal = Decimal("0.10"),
    allow_reentry_while_open: bool = False,
    entry_delay_days: int = 0,
    entry_deferral_window_days: int = 5,
    max_trades_returned: int = 300,
    screen_cache: dict[str, dict[str, object]] | None = None,
    screen_candidate_cache: dict[tuple[object, ...], dict[str, object]] | None = None,
    fundamental_growth_cache: dict[tuple[object, ...], object] | None = None,
    cup_event_cache: dict[tuple[object, ...], dict[int, list[object]] | None] | None = None,
    trade_cache: dict[tuple[object, ...], CupHandleRpsTrade | dict[str, object]] | None = None,
    trade_dates_cache: dict[tuple[object, ...], list[date]] | None = None,
    future_rows_cache: dict[tuple[object, ...], list[MarketDataDaily]] | None = None,
    future_indicator_cache: dict[tuple[object, ...], dict[date, DerivedIndicatorDaily]] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> CupHandleRpsBacktestResult:
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date.")
    if holding_days is not None and holding_days < 1:
        raise ValueError("holding_days must be greater than or equal to 1.")
    if stop_loss_pct <= Decimal("-1") or stop_loss_pct >= Decimal("0"):
        raise ValueError("stop_loss_pct must be greater than -1 and less than 0.")
    if take_profit_pct is not None and take_profit_pct <= Decimal("0"):
        raise ValueError("take_profit_pct must be greater than 0 when provided.")
    if rps_exit_threshold is not None and not 0 <= rps_exit_threshold <= 100:
        raise ValueError("rps_exit_threshold must be between 0 and 100 when provided.")
    if portfolio_cap < 1:
        raise ValueError("portfolio_cap must be greater than or equal to 1.")
    if position_weight_pct <= Decimal("0") or position_weight_pct > Decimal("1"):
        raise ValueError("position_weight_pct must be greater than 0 and less than or equal to 1.")
    if entry_delay_days < 0:
        raise ValueError("entry_delay_days must be greater than or equal to 0.")
    if entry_deferral_window_days < 1:
        raise ValueError("entry_deferral_window_days must be greater than or equal to 1.")
    if max_trades_returned < 0:
        raise ValueError("max_trades_returned must be greater than or equal to 0.")

    resolved_market = normalize_market(market)
    trade_dates_cache_key = (
        "trade_dates",
        resolved_market,
        start_date.isoformat(),
        end_date.isoformat(),
    )
    cached_trade_dates = (
        trade_dates_cache.get(trade_dates_cache_key)
        if trade_dates_cache is not None
        else None
    )
    if cached_trade_dates is None:
        trade_dates = _load_trade_dates(
            session,
            start_date=start_date,
            end_date=end_date,
            market=resolved_market,
        )
        if trade_dates_cache is not None:
            trade_dates_cache[trade_dates_cache_key] = trade_dates
    else:
        trade_dates = cached_trade_dates
    if use_rps:
        preload_screen_candidate_cache(
            session,
            market=resolved_market,
            trade_dates=trade_dates,
            use_rps=use_rps,
            rps_threshold=rps_threshold,
            selected_rps_windows=selected_rps_windows,
            min_rps_windows_passing=min_rps_windows_passing,
            candidate_cache=screen_candidate_cache,
        )
    signal_days: list[CupHandleRpsSignalDay] = []
    completed_trades: list[CupHandleRpsTrade] = []
    excluded: list[dict[str, object]] = []
    total_candidates_evaluated = 0
    qualifying_observations = 0
    selected_trade_count = 0
    open_positions: list[tuple[str, date]] = []

    for index, signal_date in enumerate(trade_dates):
        if should_cancel is not None and index % 5 == 0 and should_cancel():
            raise BacktestCancelledError()
        open_positions = [
            (symbol, exit_date) for symbol, exit_date in open_positions if exit_date > signal_date
        ]
        cache_key = _screen_cache_key(
            signal_date=signal_date,
            market=resolved_market,
            use_rps=use_rps,
            rps_threshold=rps_threshold,
            selected_rps_windows=selected_rps_windows,
            min_rps_windows_passing=min_rps_windows_passing,
            use_cup_handle=use_cup_handle,
            cup_handle_params=cup_handle_params,
            fundamental_growth_params=fundamental_growth_params,
        )
        screen_result = screen_cache.get(cache_key) if screen_cache is not None else None
        if screen_result is None:
            screen_result = screen_universe(
                session,
                use_rps=use_rps,
                rps_threshold=rps_threshold,
                selected_rps_windows=selected_rps_windows,
                min_rps_windows_passing=min_rps_windows_passing,
                use_cup_handle=use_cup_handle,
                cup_handle_params=cup_handle_params,
                fundamental_growth_params=fundamental_growth_params,
                trade_date=signal_date,
                market=resolved_market,
                candidate_cache=screen_candidate_cache,
                fundamental_growth_cache=fundamental_growth_cache,
                cup_event_cache=cup_event_cache,
                cup_event_cache_start_date=start_date,
                cup_event_cache_end_date=end_date,
            )
            if screen_cache is not None:
                screen_cache[cache_key] = screen_result
        hits = list(screen_result["hits"])
        total_candidates_evaluated += int(screen_result["total_evaluated"])
        qualifying_observations += len(hits)

        if use_rps:
            hits.sort(
                key=lambda hit: (
                    -(_hit_rps_score(hit, selected_rps_windows) or Decimal("-1")),
                    str(hit["symbol"]),
                )
            )
        else:
            hits.sort(key=lambda hit: str(hit["symbol"]))
        available_slots = max(portfolio_cap - len(open_positions), 0)
        open_symbols = {symbol for symbol, _ in open_positions}
        selected_hits: list[dict[str, object]] = []
        for hit in hits:
            if len(selected_hits) >= available_slots:
                break
            symbol = str(hit["symbol"])
            if not allow_reentry_while_open and symbol in open_symbols:
                continue
            selected_hits.append(hit)
        selected_trade_count += len(selected_hits)
        day_returns: list[Decimal] = []
        day_completed = 0
        for hit in selected_hits:
            trade_or_exclusion = _simulate_trade_with_cache(
                session,
                signal_date=signal_date,
                hit=hit,
                selected_windows=selected_rps_windows,
                holding_days=holding_days,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
                rps_exit_threshold=rps_exit_threshold,
                entry_delay_days=entry_delay_days,
                entry_deferral_window_days=entry_deferral_window_days,
                trade_cache=trade_cache,
                future_rows_cache=future_rows_cache,
                future_indicator_cache=future_indicator_cache,
            )
            if isinstance(trade_or_exclusion, CupHandleRpsTrade):
                completed_trades.append(trade_or_exclusion)
                day_returns.append(Decimal(trade_or_exclusion.realized_return))
                day_completed += 1
                open_positions.append(
                    (trade_or_exclusion.symbol, date.fromisoformat(trade_or_exclusion.exit_date))
                )
            else:
                excluded.append(trade_or_exclusion)

        average_day_return = (
            sum(day_returns, Decimal("0")) / Decimal(len(day_returns)) if day_returns else None
        )
        signal_days.append(
            CupHandleRpsSignalDay(
                signal_date=signal_date.isoformat(),
                hit_count=len(hits),
                selected_count=len(selected_hits),
                completed_trades=day_completed,
                average_return=_format_ratio(average_day_return),
            )
        )

    trade_returns = [Decimal(trade.realized_return) for trade in completed_trades]
    average_trade_return = (
        sum(trade_returns, Decimal("0")) / Decimal(len(trade_returns)) if trade_returns else None
    )
    median_trade_return = Decimal(str(median(trade_returns))) if trade_returns else None
    win_rate = (
        Decimal(sum(1 for value in trade_returns if value > 0)) / Decimal(len(trade_returns))
        if trade_returns
        else None
    )
    stop_loss_trades = sum(1 for trade in completed_trades if trade.exit_reason == "stop_loss")
    stop_loss_trigger_ratio = (
        Decimal(stop_loss_trades) / Decimal(len(completed_trades)) if completed_trades else None
    )
    take_profit_trades = sum(1 for trade in completed_trades if trade.exit_reason == "take_profit")
    take_profit_trigger_ratio = (
        Decimal(take_profit_trades) / Decimal(len(completed_trades)) if completed_trades else None
    )
    rps_exit_trades = sum(1 for trade in completed_trades if trade.exit_reason == "rps_exit")
    rps_exit_trigger_ratio = (
        Decimal(rps_exit_trades) / Decimal(len(completed_trades)) if completed_trades else None
    )

    return CupHandleRpsBacktestResult(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        signal_dates_evaluated=len(trade_dates),
        total_candidates_evaluated=total_candidates_evaluated,
        qualifying_observations=qualifying_observations,
        selected_trades=selected_trade_count,
        completed_trades=len(completed_trades),
        excluded_trades=len(excluded),
        cumulative_average_return=_format_ratio(average_trade_return),
        average_trade_return=_format_ratio(average_trade_return),
        median_trade_return=_format_ratio(median_trade_return),
        win_rate=_format_ratio(win_rate),
        best_trade_return=_format_ratio(max(trade_returns)) if trade_returns else None,
        worst_trade_return=_format_ratio(min(trade_returns)) if trade_returns else None,
        stop_loss_trades=stop_loss_trades,
        stop_loss_trigger_ratio=_format_ratio(stop_loss_trigger_ratio),
        take_profit_trades=take_profit_trades,
        take_profit_trigger_ratio=_format_ratio(take_profit_trigger_ratio),
        rps_exit_trades=rps_exit_trades,
        rps_exit_trigger_ratio=_format_ratio(rps_exit_trigger_ratio),
        max_consecutive_losses=_max_consecutive_losses(trade_returns),
        signal_days=[signal_day.to_dict() for signal_day in signal_days if signal_day.hit_count],
        trades=[trade.to_dict() for trade in completed_trades[:max_trades_returned]],
        excluded=excluded[:max_trades_returned],
        parameters={
            "rps_threshold": rps_threshold,
            "use_rps": use_rps,
            "selected_rps_windows": selected_rps_windows,
            "min_rps_windows_passing": min_rps_windows_passing,
            "use_cup_handle": use_cup_handle,
            "market": resolved_market,
            "cup_handle_params": cup_handle_params.to_dict(),
            "fundamental_growth_params": (
                fundamental_growth_params.to_dict()
                if fundamental_growth_params is not None
                else None
            ),
            "holding_days": holding_days,
            "stop_loss_pct": f"{stop_loss_pct:.4f}",
            "take_profit_pct": f"{take_profit_pct:.4f}" if take_profit_pct is not None else None,
            "rps_exit_threshold": rps_exit_threshold,
            "portfolio_cap": portfolio_cap,
            "position_weight_pct": f"{position_weight_pct:.4f}",
            "allow_reentry_while_open": allow_reentry_while_open,
            "entry_delay_days": entry_delay_days,
            "entry_deferral_window_days": entry_deferral_window_days,
            "max_trades_returned": max_trades_returned,
        },
    )
