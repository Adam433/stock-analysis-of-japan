from __future__ import annotations

import json
import math
from bisect import bisect_right
from dataclasses import asdict, dataclass, replace
from datetime import date, timedelta
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
MONEY_PATTERN = Decimal("0.01")
MAX_TRADE_LOOKAHEAD_ROWS = 5000
DEFAULT_MARKET_FILTER_PARAMS: dict[str, object] = {
    "enabled": False,
    "symbol": "SPY",
    "require_price_above_sma": True,
    "price_sma_days": 200,
    "require_fast_sma_above_slow_sma": False,
    "fast_sma_days": 50,
    "slow_sma_days": 200,
}
DEFAULT_RELATIVE_STRENGTH_PARAMS: dict[str, object] = {
    "enabled": False,
    "symbol": "SPY",
    "lookback_days": 120,
    "min_excess_return_pct": "0",
}
DEFAULT_CASH_FALLBACK_PARAMS: dict[str, object] = {
    "enabled": False,
    "symbol": "SPY",
}


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
    rps_score_date: str | None = None
    rps_detail: str | None = None
    cup_handle_breakout_date: str | None = None
    fundamental_growth_summary: str | None = None
    fundamental_growth_latest_yoy_pct: str | None = None
    fundamental_operating_cash_flow_latest_yoy_pct: str | None = None
    exit_rps_score: str | None = None
    exit_rps_score_date: str | None = None
    exit_rps_detail: str | None = None
    invested_cash: str | None = None
    exit_cash: str | None = None
    realized_profit: str | None = None

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
    spy_trade_benchmark_count: int
    spy_average_trade_benchmark_return: str | None
    spy_average_trade_excess_return: str | None
    spy_median_trade_excess_return: str | None
    spy_excess_trade_win_rate: str | None
    best_trade_return: str | None
    worst_trade_return: str | None
    stop_loss_trades: int
    stop_loss_trigger_ratio: str | None
    take_profit_trades: int
    take_profit_trigger_ratio: str | None
    rps_exit_trades: int
    rps_exit_trigger_ratio: str | None
    max_consecutive_losses: int
    initial_capital: str
    position_size_amount: str
    final_capital: str
    total_profit: str
    account_total_return: str | None
    account_annualized_return: str | None
    account_max_drawdown: str | None
    account_peak_capital: str
    account_final_date: str
    account_equity_curve: list[dict[str, object]]
    account_yearly_returns: dict[str, object]
    signal_days: list[dict[str, object]]
    trades: list[dict[str, object]]
    excluded: list[dict[str, object]]
    parameters: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class _OpenAccountPosition:
    instrument_id: int
    symbol: str
    entry_date: date
    entry_price: Decimal
    exit_date: date
    invested_cash: Decimal
    exit_cash: Decimal


def _quantize_ratio(value: Decimal) -> Decimal:
    return value.quantize(RATIO_PATTERN, rounding=ROUND_HALF_UP)


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_PATTERN, rounding=ROUND_HALF_UP)


def _format_ratio(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{_quantize_ratio(value):.6f}"


def _format_money(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{_quantize_money(value):.2f}"


def _annualize_return(
    total_return: Decimal,
    *,
    start_date: date,
    end_date: date,
) -> Decimal | None:
    elapsed_days = max((end_date - start_date).days, 0)
    if elapsed_days == 0:
        return None
    base = Decimal("1") + total_return
    if base <= Decimal("0"):
        return None
    annualized = math.pow(float(base), 365.25 / elapsed_days) - 1.0
    return Decimal(str(annualized)).quantize(RATIO_PATTERN)


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


def _format_rps_score(value: Decimal | None) -> str | None:
    return f"{value:.2f}" if value is not None else None


def _hit_rps_values(hit: dict[str, object], selected_windows: list[int]) -> list[tuple[int, Decimal]]:
    values: list[tuple[int, Decimal]] = []
    for window in selected_windows:
        raw_value = hit.get(f"rps_{window}")
        if raw_value is None:
            continue
        values.append((window, Decimal(str(raw_value))))
    return values


def _format_rps_detail(values: list[tuple[int, Decimal]]) -> str | None:
    if not values:
        return None
    return " / ".join(f"RPS{window} {value:.2f}" for window, value in values)


def _hit_rps_detail(hit: dict[str, object], selected_windows: list[int]) -> str | None:
    return _format_rps_detail(_hit_rps_values(hit, selected_windows))


def _fundamental_growth_summary(hit: dict[str, object]) -> str | None:
    status = str(hit.get("fundamental_growth_status") or "")
    if status == "not_required":
        return "未启用"
    if not status:
        return None
    details: list[str] = []
    years = hit.get("fundamental_growth_years")
    growth_count = hit.get("fundamental_growth_count")
    latest_year = hit.get("fundamental_growth_latest_year")
    try:
        comparable_years = max(int(years) - 1, 0) if years is not None else None
    except (TypeError, ValueError):
        comparable_years = None
    latest_yoy = hit.get("fundamental_growth_latest_yoy_pct")
    headline = (
        f"最近净利润同比 {latest_yoy}%"
        if latest_yoy is not None
        else "最近净利润同比 —"
    )
    if growth_count is not None and comparable_years is not None:
        details.append(f"净利润增长 {growth_count}/{comparable_years} 年")
    ocf_yoy = hit.get("fundamental_operating_cash_flow_latest_yoy_pct")
    if ocf_yoy is not None:
        details.append(f"经营现金流同比 {ocf_yoy}%")
    if latest_year:
        details.append(f"最新财年 {latest_year}")
    if details:
        return f"{headline}（{'，'.join(details)}）"
    return headline


def _entry_context_fields(
    *,
    signal_date: date,
    hit: dict[str, object],
    selected_windows: list[int],
) -> dict[str, str | None]:
    rps_score = _hit_rps_score(hit, selected_windows)
    return {
        "rps_score": _format_rps_score(rps_score),
        "rps_score_date": signal_date.isoformat(),
        "rps_detail": _hit_rps_detail(hit, selected_windows),
        "cup_handle_breakout_date": (
            str(hit["cup_handle_breakout_date"])
            if hit.get("cup_handle_breakout_date") is not None
            else None
        ),
        "fundamental_growth_summary": _fundamental_growth_summary(hit),
        "fundamental_growth_latest_yoy_pct": (
            str(hit["fundamental_growth_latest_yoy_pct"])
            if hit.get("fundamental_growth_latest_yoy_pct") is not None
            else None
        ),
        "fundamental_operating_cash_flow_latest_yoy_pct": (
            str(hit["fundamental_operating_cash_flow_latest_yoy_pct"])
            if hit.get("fundamental_operating_cash_flow_latest_yoy_pct") is not None
            else None
        ),
    }


def _copy_trade_with_entry_context(
    trade: CupHandleRpsTrade | dict[str, object],
    *,
    signal_date: date,
    hit: dict[str, object],
    selected_windows: list[int],
) -> CupHandleRpsTrade | dict[str, object]:
    if not isinstance(trade, CupHandleRpsTrade):
        return trade
    return replace(
        trade,
        **_entry_context_fields(signal_date=signal_date, hit=hit, selected_windows=selected_windows),
    )


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


def _normalize_market_filter_params(params: dict[str, object] | None) -> dict[str, object]:
    merged = dict(DEFAULT_MARKET_FILTER_PARAMS)
    merged.update(params or {})
    enabled = bool(merged.get("enabled", False))
    symbol = str(merged.get("symbol") or "SPY").strip().upper()
    if enabled and not symbol:
        raise ValueError("market_filter_params.symbol is required when market filter is enabled.")
    price_sma_days = int(merged.get("price_sma_days") or 200)
    fast_sma_days = int(merged.get("fast_sma_days") or 50)
    slow_sma_days = int(merged.get("slow_sma_days") or 200)
    for field_name, value in {
        "price_sma_days": price_sma_days,
        "fast_sma_days": fast_sma_days,
        "slow_sma_days": slow_sma_days,
    }.items():
        if value < 2:
            raise ValueError(f"market_filter_params.{field_name} must be at least 2.")
    return {
        "enabled": enabled,
        "symbol": symbol,
        "require_price_above_sma": bool(merged.get("require_price_above_sma", True)),
        "price_sma_days": price_sma_days,
        "require_fast_sma_above_slow_sma": bool(
            merged.get("require_fast_sma_above_slow_sma", False)
        ),
        "fast_sma_days": fast_sma_days,
        "slow_sma_days": slow_sma_days,
    }


def _normalize_relative_strength_params(params: dict[str, object] | None) -> dict[str, object]:
    merged = dict(DEFAULT_RELATIVE_STRENGTH_PARAMS)
    merged.update(params or {})
    enabled = bool(merged.get("enabled", False))
    symbol = str(merged.get("symbol") or "SPY").strip().upper()
    if enabled and not symbol:
        raise ValueError("relative_strength_params.symbol is required when enabled.")
    lookback_days = int(merged.get("lookback_days") or 120)
    if lookback_days < 2:
        raise ValueError("relative_strength_params.lookback_days must be at least 2.")
    min_excess_return_pct = Decimal(str(merged.get("min_excess_return_pct", "0")))
    return {
        "enabled": enabled,
        "symbol": symbol,
        "lookback_days": lookback_days,
        "min_excess_return_pct": f"{min_excess_return_pct:.4f}",
    }


def _normalize_cash_fallback_params(params: dict[str, object] | None) -> dict[str, object]:
    merged = dict(DEFAULT_CASH_FALLBACK_PARAMS)
    merged.update(params or {})
    enabled = bool(merged.get("enabled", False))
    symbol = str(merged.get("symbol") or "SPY").strip().upper()
    if enabled and not symbol:
        raise ValueError("cash_fallback_params.symbol is required when enabled.")
    return {
        "enabled": enabled,
        "symbol": symbol,
    }


def _resolve_symbol_instrument_id(
    session,
    *,
    symbol: str,
    market: str | None,
) -> int | None:
    exchanges = _market_exchanges(market)
    return session.execute(
        select(Instrument.id)
        .where(
            Instrument.symbol == symbol.upper(),
            Instrument.exchange.in_(exchanges),
        )
        .order_by(Instrument.id.asc())
        .limit(1)
    ).scalar_one_or_none()


def _market_filter_cache_key(
    *,
    params: dict[str, object],
    start_date: date,
    end_date: date,
) -> tuple[object, ...]:
    return (
        "market_filter",
        start_date.isoformat(),
        end_date.isoformat(),
        params["enabled"],
        params["symbol"],
        params["require_price_above_sma"],
        params["price_sma_days"],
        params["require_fast_sma_above_slow_sma"],
        params["fast_sma_days"],
        params["slow_sma_days"],
    )


def _load_market_filter_allowed_dates(
    session,
    *,
    start_date: date,
    end_date: date,
    params: dict[str, object],
    market_filter_cache: dict[tuple[object, ...], set[date]] | None = None,
) -> set[date]:
    cache_key = _market_filter_cache_key(
        params=params,
        start_date=start_date,
        end_date=end_date,
    )
    cached = market_filter_cache.get(cache_key) if market_filter_cache is not None else None
    if cached is not None:
        return cached

    symbol = str(params["symbol"])
    rows = list(
        session.execute(
            select(MarketDataDaily)
            .join(Instrument, Instrument.id == MarketDataDaily.instrument_id)
            .where(
                Instrument.symbol == symbol,
                MarketDataDaily.trade_date <= end_date,
            )
            .order_by(MarketDataDaily.trade_date.asc())
        ).scalars()
    )
    if not rows:
        raise ValueError(f"Market filter symbol {symbol} has no market data.")

    require_price_above_sma = bool(params["require_price_above_sma"])
    price_sma_days = int(params["price_sma_days"])
    require_fast_sma_above_slow_sma = bool(params["require_fast_sma_above_slow_sma"])
    fast_sma_days = int(params["fast_sma_days"])
    slow_sma_days = int(params["slow_sma_days"])
    close_values: list[Decimal] = []
    allowed_dates: set[date] = set()
    for row in rows:
        if not is_complete_market_row(row):
            continue
        close = adjusted_close(row)
        if close is None:
            continue
        close_values.append(close)
        passes = True
        if require_price_above_sma:
            if len(close_values) < price_sma_days:
                passes = False
            else:
                price_sma = sum(close_values[-price_sma_days:], Decimal("0")) / Decimal(
                    price_sma_days
                )
                passes = close > price_sma
        if passes and require_fast_sma_above_slow_sma:
            if len(close_values) < max(fast_sma_days, slow_sma_days):
                passes = False
            else:
                fast_sma = sum(close_values[-fast_sma_days:], Decimal("0")) / Decimal(
                    fast_sma_days
                )
                slow_sma = sum(close_values[-slow_sma_days:], Decimal("0")) / Decimal(
                    slow_sma_days
                )
                passes = fast_sma > slow_sma
        if passes and start_date <= row.trade_date <= end_date:
            allowed_dates.add(row.trade_date)

    if market_filter_cache is not None:
        market_filter_cache[cache_key] = allowed_dates
    return allowed_dates


def _trade_with_account_values(
    trade: CupHandleRpsTrade,
    *,
    invested_cash: Decimal,
) -> CupHandleRpsTrade:
    realized_return = Decimal(trade.realized_return)
    exit_cash = invested_cash * (Decimal("1") + realized_return)
    realized_profit = exit_cash - invested_cash
    return CupHandleRpsTrade(
        signal_date=trade.signal_date,
        instrument_id=trade.instrument_id,
        symbol=trade.symbol,
        entry_date=trade.entry_date,
        entry_price=trade.entry_price,
        exit_date=trade.exit_date,
        exit_price=trade.exit_price,
        exit_reason=trade.exit_reason,
        realized_return=trade.realized_return,
        rps_score=trade.rps_score,
        invested_cash=_format_money(invested_cash),
        exit_cash=_format_money(exit_cash),
        realized_profit=_format_money(realized_profit),
    )


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
    max_exit_date: date | None = None,
) -> list[MarketDataDaily]:
    predicates = [
        MarketDataDaily.instrument_id == instrument_id,
        MarketDataDaily.trade_date > signal_date,
    ]
    if max_exit_date is not None:
        predicates.append(MarketDataDaily.trade_date <= max_exit_date)
    return list(
        session.execute(
            select(MarketDataDaily)
            .where(*predicates)
            .order_by(MarketDataDaily.trade_date.asc())
            .limit(limit)
        ).scalars()
    )


def _load_mark_price_series(
    session,
    *,
    instrument_id: int,
    start_date: date,
    end_date: date,
) -> tuple[list[date], list[Decimal]]:
    rows = session.execute(
        select(MarketDataDaily)
        .where(
            MarketDataDaily.instrument_id == instrument_id,
            MarketDataDaily.trade_date >= start_date,
            MarketDataDaily.trade_date <= end_date,
        )
        .order_by(MarketDataDaily.trade_date.asc())
    ).scalars()
    dates: list[date] = []
    closes: list[Decimal] = []
    for row in rows:
        if not is_complete_market_row(row):
            continue
        close = adjusted_close(row)
        if close is None or close <= Decimal("0"):
            continue
        dates.append(row.trade_date)
        closes.append(close)
    return dates, closes


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


def _indicator_rps_values(
    row: DerivedIndicatorDaily | None,
    selected_windows: list[int],
) -> list[tuple[int, Decimal]]:
    if row is None:
        return []
    values: list[tuple[int, Decimal]] = []
    for window in selected_windows:
        raw_value = getattr(row, f"rps_{window}", None)
        if raw_value is not None:
            values.append((window, Decimal(str(raw_value))))
    return values


def _indicator_rps_detail(
    row: DerivedIndicatorDaily | None,
    selected_windows: list[int],
) -> str | None:
    return _format_rps_detail(_indicator_rps_values(row, selected_windows))


def _load_future_indicator_map(
    session,
    *,
    instrument_id: int,
    signal_date: date,
    limit: int,
    max_exit_date: date | None = None,
) -> dict[date, DerivedIndicatorDaily]:
    if limit < 1:
        return {}
    predicates = [
        DerivedIndicatorDaily.instrument_id == instrument_id,
        DerivedIndicatorDaily.trade_date > signal_date,
    ]
    if max_exit_date is not None:
        predicates.append(DerivedIndicatorDaily.trade_date <= max_exit_date)
    rows = session.execute(
        select(DerivedIndicatorDaily)
        .where(*predicates)
        .order_by(DerivedIndicatorDaily.trade_date.asc())
        .limit(limit)
    ).scalars()
    return {row.trade_date: row for row in rows}


def _load_relative_strength_metrics(
    session,
    *,
    instrument_ids: list[int],
    signal_date: date,
    market: str | None,
    params: dict[str, object],
    relative_strength_cache: dict[tuple[object, ...], dict[int, dict[str, object]]] | None,
) -> dict[int, dict[str, object]]:
    if not instrument_ids:
        return {}
    lookback_days = int(params["lookback_days"])
    benchmark_symbol = str(params["symbol"])
    cache_key = (
        "relative_strength",
        benchmark_symbol,
        signal_date.isoformat(),
        lookback_days,
        str(params["min_excess_return_pct"]),
        tuple(sorted(set(instrument_ids))),
    )
    cached = relative_strength_cache.get(cache_key) if relative_strength_cache is not None else None
    if cached is not None:
        return cached

    benchmark_instrument_id = _resolve_symbol_instrument_id(
        session,
        symbol=benchmark_symbol,
        market=market,
    )
    if benchmark_instrument_id is None:
        raise ValueError(f"Relative strength symbol {benchmark_symbol} has no instrument.")

    all_instrument_ids = sorted(set(instrument_ids + [benchmark_instrument_id]))
    start_bound = signal_date - timedelta(days=max(lookback_days * 3, lookback_days + 30))
    rows = session.execute(
        select(MarketDataDaily)
        .where(
            MarketDataDaily.instrument_id.in_(all_instrument_ids),
            MarketDataDaily.trade_date >= start_bound,
            MarketDataDaily.trade_date <= signal_date,
        )
        .order_by(MarketDataDaily.instrument_id.asc(), MarketDataDaily.trade_date.asc())
    ).scalars()
    prices_by_instrument: dict[int, list[tuple[date, Decimal]]] = {}
    for row in rows:
        if not is_complete_market_row(row):
            continue
        close = adjusted_close(row)
        if close is None or close <= Decimal("0"):
            continue
        prices_by_instrument.setdefault(row.instrument_id, []).append((row.trade_date, close))

    def lookback_return(instrument_id: int) -> Decimal | None:
        prices = prices_by_instrument.get(instrument_id, [])
        if len(prices) <= lookback_days:
            return None
        current_price = prices[-1][1]
        prior_price = prices[-lookback_days - 1][1]
        if prior_price <= Decimal("0"):
            return None
        return (current_price / prior_price) - Decimal("1")

    benchmark_return = lookback_return(benchmark_instrument_id)
    min_excess_return = Decimal(str(params["min_excess_return_pct"]))
    metrics: dict[int, dict[str, object]] = {}
    for instrument_id in instrument_ids:
        stock_return = lookback_return(instrument_id)
        excess_return = (
            stock_return - benchmark_return
            if stock_return is not None and benchmark_return is not None
            else None
        )
        metrics[instrument_id] = {
            "symbol": benchmark_symbol,
            "lookback_days": lookback_days,
            "stock_return": _format_ratio(stock_return),
            "benchmark_return": _format_ratio(benchmark_return),
            "excess_return": _format_ratio(excess_return),
            "passed": (
                excess_return is not None
                and excess_return >= min_excess_return
            ),
        }
    if relative_strength_cache is not None:
        relative_strength_cache[cache_key] = metrics
    return metrics


def _filter_hits_by_relative_strength(
    session,
    *,
    hits: list[dict[str, object]],
    signal_date: date,
    market: str | None,
    params: dict[str, object],
    relative_strength_cache: dict[tuple[object, ...], dict[int, dict[str, object]]] | None,
) -> list[dict[str, object]]:
    if not params.get("enabled") or not hits:
        return hits
    metrics_by_instrument = _load_relative_strength_metrics(
        session,
        instrument_ids=[int(hit["instrument_id"]) for hit in hits],
        signal_date=signal_date,
        market=market,
        params=params,
        relative_strength_cache=relative_strength_cache,
    )
    filtered_hits: list[dict[str, object]] = []
    for hit in hits:
        metric = metrics_by_instrument.get(int(hit["instrument_id"]))
        if not metric or not metric["passed"]:
            continue
        filtered_hit = dict(hit)
        filtered_hit["relative_strength"] = metric
        filtered_hits.append(filtered_hit)
    return filtered_hits


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
    max_hits: int | None = None,
    exclude_symbols: set[str] | frozenset[str] | None = None,
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
        "max_hits": max_hits,
        "exclude_symbols": sorted(exclude_symbols or []),
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
    max_exit_date: date | None = None,
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
        max_exit_date.isoformat() if max_exit_date is not None else None,
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
            max_exit_date=max_exit_date,
        )
        if future_rows_cache is not None:
            future_rows_cache[future_rows_cache_key] = rows
    else:
        rows = cached_rows
    if len(rows) <= entry_delay_days:
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
    entry_context = _entry_context_fields(
        signal_date=signal_date,
        hit=hit,
        selected_windows=selected_windows,
    )

    def mark_to_latest_close(exit_reason: str) -> CupHandleRpsTrade | None:
        for row in reversed(rows[entry_index:]):
            candidate_close = _valid_close(row)
            if candidate_close is None:
                continue
            realized_return = _quantize_ratio((candidate_close / entry_price) - Decimal("1"))
            return CupHandleRpsTrade(
                signal_date=signal_date.isoformat(),
                instrument_id=instrument_id,
                symbol=symbol,
                entry_date=entry_row.trade_date.isoformat(),
                entry_price=f"{entry_price:.6f}",
                exit_date=row.trade_date.isoformat(),
                exit_price=f"{candidate_close:.6f}",
                exit_reason=exit_reason,
                realized_return=f"{realized_return:.6f}",
                **entry_context,
            )
        return None

    indicator_by_date: dict[date, DerivedIndicatorDaily] = {}
    if rps_exit_threshold is not None:
        future_indicator_cache_key = (
            "future_indicator_map",
            instrument_id,
            signal_date.isoformat(),
            len(rows),
            max_exit_date.isoformat() if max_exit_date is not None else None,
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
                max_exit_date=max_exit_date,
            )
            if future_indicator_cache is not None:
                future_indicator_cache[future_indicator_cache_key] = indicator_by_date
        else:
            indicator_by_date = cached_indicator_map
    trigger_index: int | None = None
    trigger_reason = "holding_period_elapsed"
    immediate_exit_row: MarketDataDaily | None = None
    immediate_exit_price: Decimal | None = None
    trigger_rps_score: Decimal | None = None
    trigger_rps_score_date: date | None = None
    trigger_rps_detail: str | None = None
    held_trading_days = 0
    for index in range(entry_index, len(rows)):
        row = rows[index]
        last_mark_price = _mark_price(row, last_mark_price)
        held_trading_days += 1
        indicator_row = indicator_by_date.get(row.trade_date)
        current_rps_score = _indicator_rps_score(indicator_row, selected_windows)
        current_rps_detail = _indicator_rps_detail(indicator_row, selected_windows)
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
            trigger_rps_score = current_rps_score
            trigger_rps_score_date = row.trade_date if current_rps_score is not None else None
            trigger_rps_detail = current_rps_detail
            break
        mark_return = _quantize_ratio((last_mark_price / entry_price) - Decimal("1"))
        if take_profit_pct is not None and mark_return >= take_profit_pct:
            trigger_index = index
            trigger_reason = "take_profit"
            trigger_rps_score = current_rps_score
            trigger_rps_score_date = row.trade_date if current_rps_score is not None else None
            trigger_rps_detail = current_rps_detail
            break
        if (
            rps_exit_threshold is not None
            and current_rps_score is not None
            and current_rps_score < Decimal(rps_exit_threshold)
        ):
            trigger_index = index
            trigger_reason = "rps_exit"
            trigger_rps_score = current_rps_score
            trigger_rps_score_date = row.trade_date
            trigger_rps_detail = current_rps_detail
            break
        if holding_days is not None and held_trading_days >= holding_days:
            trigger_index = index
            trigger_rps_score = current_rps_score
            trigger_rps_score_date = row.trade_date if current_rps_score is not None else None
            trigger_rps_detail = current_rps_detail
            break

    if trigger_index is None:
        if max_exit_date is not None:
            window_mark_trade = mark_to_latest_close("window_end_mark")
            if window_mark_trade is not None:
                return window_mark_trade
        if holding_days is None:
            data_end_mark_trade = mark_to_latest_close("data_end_mark")
            if data_end_mark_trade is not None:
                return data_end_mark_trade
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
        if max_exit_date is not None:
            window_mark_trade = mark_to_latest_close("window_end_mark")
            if window_mark_trade is not None:
                return window_mark_trade
        return {
            "signal_date": signal_date.isoformat(),
            "instrument_id": instrument_id,
            "symbol": symbol,
            "reason": "data_insufficient_for_exit",
        }

    realized_return = _quantize_ratio((exit_price / entry_price) - Decimal("1"))
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
        exit_rps_score=_format_rps_score(trigger_rps_score),
        exit_rps_score_date=(
            trigger_rps_score_date.isoformat() if trigger_rps_score_date is not None else None
        ),
        exit_rps_detail=trigger_rps_detail,
        **entry_context,
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
    max_exit_date: date | None,
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
        max_exit_date.isoformat() if max_exit_date is not None else None,
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
    max_exit_date: date | None = None,
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
            max_exit_date=max_exit_date,
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
        max_exit_date=max_exit_date,
    )
    cached = trade_cache.get(cache_key)
    if cached is not None:
        return _copy_trade_with_entry_context(
            cached,
            signal_date=signal_date,
            hit=hit,
            selected_windows=selected_windows,
        )
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
        max_exit_date=max_exit_date,
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
    initial_capital: Decimal = Decimal("100000"),
    position_size_amount: Decimal | None = None,
    allow_reentry_while_open: bool = False,
    market_filter_params: dict[str, object] | None = None,
    relative_strength_params: dict[str, object] | None = None,
    cash_fallback_params: dict[str, object] | None = None,
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
    market_filter_cache: dict[tuple[object, ...], set[date]] | None = None,
    relative_strength_cache: dict[tuple[object, ...], dict[int, dict[str, object]]] | None = None,
    should_cancel: Callable[[], bool] | None = None,
    preload_screen_candidates: bool = True,
    prefer_broad_candidate_cache: bool = False,
    max_broad_candidate_cache_dates: int | None = None,
    execution_limited_screen: bool = False,
    force_exit_at_end: bool = False,
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
    if initial_capital <= Decimal("0"):
        raise ValueError("initial_capital must be greater than 0.")
    resolved_position_size_amount = (
        position_size_amount
        if position_size_amount is not None
        else initial_capital * position_weight_pct
    )
    if resolved_position_size_amount <= Decimal("0"):
        raise ValueError("position_size_amount must be greater than 0 when provided.")
    if resolved_position_size_amount > initial_capital:
        raise ValueError("position_size_amount cannot exceed initial_capital.")
    initial_capital = _quantize_money(initial_capital)
    resolved_position_size_amount = _quantize_money(resolved_position_size_amount)
    if entry_delay_days < 0:
        raise ValueError("entry_delay_days must be greater than or equal to 0.")
    if entry_deferral_window_days < 1:
        raise ValueError("entry_deferral_window_days must be greater than or equal to 1.")
    if max_trades_returned < 0:
        raise ValueError("max_trades_returned must be greater than or equal to 0.")

    resolved_market = normalize_market(market)
    normalized_market_filter_params = _normalize_market_filter_params(market_filter_params)
    normalized_relative_strength_params = _normalize_relative_strength_params(
        relative_strength_params
    )
    normalized_cash_fallback_params = _normalize_cash_fallback_params(cash_fallback_params)
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
    market_filter_allowed_dates: set[date] | None = None
    if normalized_market_filter_params["enabled"]:
        market_filter_allowed_dates = _load_market_filter_allowed_dates(
            session,
            start_date=start_date,
            end_date=end_date,
            params=normalized_market_filter_params,
            market_filter_cache=market_filter_cache,
        )
    if use_rps and preload_screen_candidates:
        preload_screen_candidate_cache(
            session,
            market=resolved_market,
            trade_dates=trade_dates,
            use_rps=use_rps,
            rps_threshold=rps_threshold,
            selected_rps_windows=selected_rps_windows,
            min_rps_windows_passing=min_rps_windows_passing,
            candidate_cache=screen_candidate_cache,
            prefer_broad_candidate_cache=prefer_broad_candidate_cache,
            max_broad_candidate_cache_dates=max_broad_candidate_cache_dates,
        )
    signal_days: list[CupHandleRpsSignalDay] = []
    completed_trades: list[CupHandleRpsTrade] = []
    excluded: list[dict[str, object]] = []
    total_candidates_evaluated = 0
    qualifying_observations = 0
    selected_trade_count = 0
    open_positions: list[_OpenAccountPosition] = []
    account_cash = initial_capital
    account_peak_capital = initial_capital
    account_max_drawdown = Decimal("0")
    account_equity_curve: list[dict[str, object]] = []
    account_year_start_equity: dict[str, Decimal] = {}
    account_year_latest_equity: dict[str, Decimal] = {}
    account_final_date = start_date
    mark_price_series_cache: dict[int, tuple[date, list[date], list[Decimal]]] = {}
    cash_fallback_instrument_id: int | None = None
    cash_fallback_last_close: Decimal | None = None

    def latest_mark_close(
        *,
        instrument_id: int,
        point_date: date,
    ) -> Decimal | None:
        if session is None:
            return None
        cached_series = mark_price_series_cache.get(instrument_id)
        if cached_series is None or point_date > cached_series[0]:
            if len(mark_price_series_cache) >= 512:
                mark_price_series_cache.pop(next(iter(mark_price_series_cache)))
            loaded_end_date = max(end_date, point_date)
            dates, closes = _load_mark_price_series(
                session,
                instrument_id=instrument_id,
                start_date=start_date,
                end_date=loaded_end_date,
            )
            mark_price_series_cache[instrument_id] = (loaded_end_date, dates, closes)
        else:
            _, dates, closes = cached_series
        index = bisect_right(dates, point_date) - 1
        if index < 0:
            return None
        return closes[index]

    if normalized_cash_fallback_params["enabled"]:
        cash_fallback_symbol = str(normalized_cash_fallback_params["symbol"])
        cash_fallback_instrument_id = _resolve_symbol_instrument_id(
            session,
            symbol=cash_fallback_symbol,
            market=resolved_market,
        )
        if cash_fallback_instrument_id is None:
            raise ValueError(f"Cash fallback symbol {cash_fallback_symbol} has no instrument.")
        cash_fallback_last_close = latest_mark_close(
            instrument_id=cash_fallback_instrument_id,
            point_date=start_date,
        )

    def apply_cash_fallback_return(point_date: date) -> bool:
        nonlocal account_cash, cash_fallback_last_close
        if (
            not normalized_cash_fallback_params["enabled"]
            or cash_fallback_instrument_id is None
            or account_cash <= Decimal("0")
        ):
            return False
        mark_close = latest_mark_close(
            instrument_id=cash_fallback_instrument_id,
            point_date=point_date,
        )
        if mark_close is None:
            return False
        if cash_fallback_last_close is None:
            cash_fallback_last_close = mark_close
            return False
        if mark_close == cash_fallback_last_close:
            return False
        account_cash = account_cash * (mark_close / cash_fallback_last_close)
        cash_fallback_last_close = mark_close
        return True

    def position_market_value(position: _OpenAccountPosition, point_date: date) -> Decimal:
        if point_date < position.entry_date or position.entry_price <= Decimal("0"):
            return position.invested_cash
        mark_close = latest_mark_close(
            instrument_id=position.instrument_id,
            point_date=point_date,
        )
        if mark_close is None:
            return position.invested_cash
        return position.invested_cash * (mark_close / position.entry_price)

    def invested_cash_total() -> Decimal:
        return sum((position.invested_cash for position in open_positions), Decimal("0"))

    def open_positions_market_value(point_date: date) -> Decimal:
        return sum(
            (position_market_value(position, point_date) for position in open_positions),
            Decimal("0"),
        )

    def append_account_point(point_date: date, event: str) -> None:
        nonlocal account_peak_capital, account_max_drawdown, account_final_date
        invested_market_value = open_positions_market_value(point_date)
        equity = account_cash + invested_market_value
        if equity > account_peak_capital:
            account_peak_capital = equity
        drawdown = (
            (equity / account_peak_capital) - Decimal("1")
            if account_peak_capital > Decimal("0")
            else Decimal("0")
        )
        if drawdown < account_max_drawdown:
            account_max_drawdown = drawdown
        year = point_date.isoformat()[:4]
        account_year_start_equity.setdefault(year, equity)
        account_year_latest_equity[year] = equity
        account_final_date = max(account_final_date, point_date)
        account_equity_curve.append(
            {
                "signal_date": point_date.isoformat(),
                "date": point_date.isoformat(),
                "event": event,
                "equity": _format_ratio(equity / initial_capital),
                "capital": _format_money(equity),
                "cash": _format_money(account_cash),
                "invested_cash": _format_money(invested_cash_total()),
                "invested_market_value": _format_money(invested_market_value),
                "cash_fallback_symbol": (
                    normalized_cash_fallback_params["symbol"]
                    if normalized_cash_fallback_params["enabled"]
                    else None
                ),
                "drawdown": _format_ratio(drawdown),
            }
        )

    append_account_point(start_date, "start")

    for index, signal_date in enumerate(trade_dates):
        if should_cancel is not None and index % 5 == 0 and should_cancel():
            raise BacktestCancelledError()
        cash_fallback_marked = apply_cash_fallback_return(signal_date)
        settled_positions: list[_OpenAccountPosition] = []
        remaining_positions: list[_OpenAccountPosition] = []
        for position in open_positions:
            if position.exit_date <= signal_date:
                account_cash += position.exit_cash
                settled_positions.append(position)
            else:
                remaining_positions.append(position)
        open_positions = remaining_positions
        if settled_positions:
            append_account_point(signal_date, "settlement")
        if market_filter_allowed_dates is not None and signal_date not in market_filter_allowed_dates:
            if (open_positions or cash_fallback_marked) and not settled_positions:
                append_account_point(signal_date, "mark")
            continue
        available_slots = max(portfolio_cap - len(open_positions), 0)
        if execution_limited_screen and available_slots <= 0:
            if (open_positions or cash_fallback_marked) and not settled_positions:
                append_account_point(signal_date, "mark")
            continue
        open_symbols = {position.symbol for position in open_positions}
        relative_strength_enabled = bool(normalized_relative_strength_params["enabled"])
        screen_max_hits = (
            None
            if relative_strength_enabled
            else available_slots if execution_limited_screen else None
        )
        screen_exclude_symbols = open_symbols if execution_limited_screen else None
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
            max_hits=screen_max_hits,
            exclude_symbols=screen_exclude_symbols,
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
                max_hits=screen_max_hits,
                exclude_symbols=screen_exclude_symbols,
                prefer_broad_candidate_cache=prefer_broad_candidate_cache,
                max_broad_candidate_cache_dates=max_broad_candidate_cache_dates,
            )
            if screen_cache is not None:
                screen_cache[cache_key] = screen_result
        hits = list(screen_result["hits"])
        hits = _filter_hits_by_relative_strength(
            session,
            hits=hits,
            signal_date=signal_date,
            market=resolved_market,
            params=normalized_relative_strength_params,
            relative_strength_cache=relative_strength_cache,
        )
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
        day_returns: list[Decimal] = []
        day_completed = 0
        day_selected = 0
        for hit in hits:
            if day_selected >= available_slots:
                break
            symbol = str(hit["symbol"])
            if not allow_reentry_while_open and symbol in open_symbols:
                continue
            if account_cash < resolved_position_size_amount:
                excluded.append(
                    {
                        "signal_date": signal_date.isoformat(),
                        "instrument_id": int(hit["instrument_id"]),
                        "symbol": symbol,
                        "reason": "cash_insufficient",
                        "available_cash": _format_money(account_cash),
                        "required_cash": _format_money(resolved_position_size_amount),
                    }
                )
                break
            day_selected += 1
            selected_trade_count += 1
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
                max_exit_date=end_date if force_exit_at_end else None,
                trade_cache=trade_cache,
                future_rows_cache=future_rows_cache,
                future_indicator_cache=future_indicator_cache,
            )
            if isinstance(trade_or_exclusion, CupHandleRpsTrade):
                account_trade = _trade_with_account_values(
                    trade_or_exclusion,
                    invested_cash=resolved_position_size_amount,
                )
                completed_trades.append(account_trade)
                day_returns.append(Decimal(account_trade.realized_return))
                day_completed += 1
                account_cash -= resolved_position_size_amount
                exit_cash = resolved_position_size_amount * (
                    Decimal("1") + Decimal(account_trade.realized_return)
                )
                open_positions.append(
                    _OpenAccountPosition(
                        instrument_id=account_trade.instrument_id,
                        symbol=account_trade.symbol,
                        entry_date=date.fromisoformat(account_trade.entry_date),
                        entry_price=Decimal(account_trade.entry_price),
                        exit_date=date.fromisoformat(account_trade.exit_date),
                        invested_cash=resolved_position_size_amount,
                        exit_cash=_quantize_money(exit_cash),
                    )
                )
                open_symbols.add(account_trade.symbol)
            else:
                excluded.append(trade_or_exclusion)
        if day_selected:
            append_account_point(signal_date, "signal")
        elif (open_positions or cash_fallback_marked) and not settled_positions:
            append_account_point(signal_date, "mark")

        average_day_return = (
            sum(day_returns, Decimal("0")) / Decimal(len(day_returns)) if day_returns else None
        )
        signal_days.append(
            CupHandleRpsSignalDay(
                signal_date=signal_date.isoformat(),
                hit_count=len(hits),
                selected_count=day_selected,
                completed_trades=day_completed,
                average_return=_format_ratio(average_day_return),
            )
        )

    for position in sorted(list(open_positions), key=lambda item: item.exit_date):
        apply_cash_fallback_return(position.exit_date)
        account_cash += position.exit_cash
        open_positions = [item for item in open_positions if item is not position]
        append_account_point(position.exit_date, "final_settlement")
    window_end_cash_fallback_marked = apply_cash_fallback_return(end_date)
    if force_exit_at_end and (account_final_date < end_date or window_end_cash_fallback_marked):
        append_account_point(end_date, "window_end")

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
    spy_trade_benchmark_returns: list[Decimal] = []
    spy_trade_excess_returns: list[Decimal] = []
    spy_instrument_id = (
        _resolve_symbol_instrument_id(
            session,
            symbol="SPY",
            market=resolved_market,
        )
        if hasattr(session, "execute")
        else None
    )
    if spy_instrument_id is not None:
        for trade in completed_trades:
            spy_entry_close = latest_mark_close(
                instrument_id=spy_instrument_id,
                point_date=date.fromisoformat(trade.entry_date),
            )
            spy_exit_close = latest_mark_close(
                instrument_id=spy_instrument_id,
                point_date=date.fromisoformat(trade.exit_date),
            )
            if (
                spy_entry_close is None
                or spy_entry_close <= Decimal("0")
                or spy_exit_close is None
                or spy_exit_close <= Decimal("0")
            ):
                continue
            spy_trade_return = (spy_exit_close / spy_entry_close) - Decimal("1")
            trade_return = Decimal(trade.realized_return)
            spy_trade_benchmark_returns.append(spy_trade_return)
            spy_trade_excess_returns.append(trade_return - spy_trade_return)
    spy_trade_excess_win_rate = (
        Decimal(sum(1 for value in spy_trade_excess_returns if value > 0))
        / Decimal(len(spy_trade_excess_returns))
        if spy_trade_excess_returns
        else None
    )
    final_capital = account_cash
    total_profit = final_capital - initial_capital
    account_total_return = (
        (final_capital / initial_capital) - Decimal("1")
        if initial_capital > Decimal("0")
        else None
    )
    account_annualized_return = (
        _annualize_return(
            account_total_return,
            start_date=start_date,
            end_date=account_final_date,
        )
        if account_total_return is not None
        else None
    )
    account_yearly_returns = {
        year: _format_ratio((account_year_latest_equity[year] / start_equity) - Decimal("1"))
        for year, start_equity in sorted(account_year_start_equity.items())
        if start_equity > Decimal("0") and year in account_year_latest_equity
    }

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
        spy_trade_benchmark_count=len(spy_trade_excess_returns),
        spy_average_trade_benchmark_return=_format_ratio(
            sum(spy_trade_benchmark_returns, Decimal("0"))
            / Decimal(len(spy_trade_benchmark_returns))
            if spy_trade_benchmark_returns
            else None
        ),
        spy_average_trade_excess_return=_format_ratio(
            sum(spy_trade_excess_returns, Decimal("0")) / Decimal(len(spy_trade_excess_returns))
            if spy_trade_excess_returns
            else None
        ),
        spy_median_trade_excess_return=(
            _format_ratio(Decimal(str(median(spy_trade_excess_returns))))
            if spy_trade_excess_returns
            else None
        ),
        spy_excess_trade_win_rate=_format_ratio(spy_trade_excess_win_rate),
        best_trade_return=_format_ratio(max(trade_returns)) if trade_returns else None,
        worst_trade_return=_format_ratio(min(trade_returns)) if trade_returns else None,
        stop_loss_trades=stop_loss_trades,
        stop_loss_trigger_ratio=_format_ratio(stop_loss_trigger_ratio),
        take_profit_trades=take_profit_trades,
        take_profit_trigger_ratio=_format_ratio(take_profit_trigger_ratio),
        rps_exit_trades=rps_exit_trades,
        rps_exit_trigger_ratio=_format_ratio(rps_exit_trigger_ratio),
        max_consecutive_losses=_max_consecutive_losses(trade_returns),
        initial_capital=_format_money(initial_capital) or "0.00",
        position_size_amount=_format_money(resolved_position_size_amount) or "0.00",
        final_capital=_format_money(final_capital) or "0.00",
        total_profit=_format_money(total_profit) or "0.00",
        account_total_return=_format_ratio(account_total_return),
        account_annualized_return=_format_ratio(account_annualized_return),
        account_max_drawdown=_format_ratio(account_max_drawdown if account_equity_curve else None),
        account_peak_capital=_format_money(account_peak_capital) or "0.00",
        account_final_date=account_final_date.isoformat(),
        account_equity_curve=account_equity_curve,
        account_yearly_returns=account_yearly_returns,
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
            "initial_capital": _format_money(initial_capital),
            "position_size_amount": _format_money(resolved_position_size_amount),
            "allow_reentry_while_open": allow_reentry_while_open,
            "market_filter_params": normalized_market_filter_params,
            "relative_strength_params": normalized_relative_strength_params,
            "cash_fallback_params": normalized_cash_fallback_params,
            "entry_delay_days": entry_delay_days,
            "entry_deferral_window_days": entry_deferral_window_days,
            "max_trades_returned": max_trades_returned,
            "force_exit_at_end": force_exit_at_end,
        },
    )
