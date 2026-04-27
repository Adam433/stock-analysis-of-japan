from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from statistics import median

from sqlalchemy import distinct, select

from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.dashboard import CupHandleParams, screen_universe
from stockanalyse_api.services.market_data_adjustments import adjusted_close
from stockanalyse_api.services.market_data_adjustments import adjusted_open
from stockanalyse_api.services.market_data_adjustments import is_complete_market_row

RATIO_PATTERN = Decimal("0.000001")


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


def _load_trade_dates(session, *, start_date: date, end_date: date) -> list[date]:
    return list(
        session.execute(
            select(distinct(DerivedIndicatorDaily.trade_date))
            .where(
                DerivedIndicatorDaily.trade_date >= start_date,
                DerivedIndicatorDaily.trade_date <= end_date,
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


def _simulate_trade(
    session,
    *,
    signal_date: date,
    hit: dict[str, object],
    selected_windows: list[int],
    holding_days: int,
    stop_loss_pct: Decimal,
    entry_deferral_window_days: int,
) -> CupHandleRpsTrade | dict[str, object]:
    instrument_id = int(hit["instrument_id"])
    symbol = str(hit["symbol"])
    rows = _load_future_rows(
        session,
        instrument_id=instrument_id,
        signal_date=signal_date,
        limit=entry_deferral_window_days + holding_days + 40,
    )
    if len(rows) < entry_deferral_window_days:
        return {
            "signal_date": signal_date.isoformat(),
            "instrument_id": instrument_id,
            "symbol": symbol,
            "reason": "data_insufficient_for_entry",
        }

    entry_index: int | None = None
    entry_price: Decimal | None = None
    for index, row in enumerate(rows[:entry_deferral_window_days]):
        candidate_open = _valid_open(row)
        if candidate_open is not None:
            entry_index = index
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
    trigger_index: int | None = None
    trigger_reason = "holding_period_elapsed"
    held_trading_days = 0
    for index in range(entry_index, len(rows)):
        row = rows[index]
        last_mark_price = _mark_price(row, last_mark_price)
        held_trading_days += 1
        if _quantize_ratio((last_mark_price / entry_price) - Decimal("1")) <= stop_loss_pct:
            trigger_index = index
            trigger_reason = "stop_loss"
            break
        if held_trading_days >= holding_days:
            trigger_index = index
            break

    if trigger_index is None:
        return {
            "signal_date": signal_date.isoformat(),
            "instrument_id": instrument_id,
            "symbol": symbol,
            "reason": "data_insufficient_for_holding_period",
        }

    exit_row: MarketDataDaily | None = None
    exit_price: Decimal | None = None
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


def run_cup_handle_rps_backtest(
    session,
    *,
    start_date: date,
    end_date: date,
    rps_threshold: int,
    selected_rps_windows: list[int],
    cup_handle_params: CupHandleParams,
    holding_days: int = 120,
    stop_loss_pct: Decimal = Decimal("-0.08"),
    portfolio_cap: int = 20,
    entry_deferral_window_days: int = 5,
    max_trades_returned: int = 300,
) -> CupHandleRpsBacktestResult:
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date.")
    if holding_days < 1:
        raise ValueError("holding_days must be greater than or equal to 1.")
    if stop_loss_pct <= Decimal("-1") or stop_loss_pct >= Decimal("0"):
        raise ValueError("stop_loss_pct must be greater than -1 and less than 0.")
    if portfolio_cap < 1:
        raise ValueError("portfolio_cap must be greater than or equal to 1.")
    if entry_deferral_window_days < 1:
        raise ValueError("entry_deferral_window_days must be greater than or equal to 1.")
    if max_trades_returned < 0:
        raise ValueError("max_trades_returned must be greater than or equal to 0.")

    trade_dates = _load_trade_dates(session, start_date=start_date, end_date=end_date)
    signal_days: list[CupHandleRpsSignalDay] = []
    completed_trades: list[CupHandleRpsTrade] = []
    excluded: list[dict[str, object]] = []
    total_candidates_evaluated = 0
    qualifying_observations = 0
    selected_trade_count = 0

    for signal_date in trade_dates:
        screen_result = screen_universe(
            session,
            use_rps=True,
            rps_threshold=rps_threshold,
            selected_rps_windows=selected_rps_windows,
            use_cup_handle=True,
            cup_handle_params=cup_handle_params,
            trade_date=signal_date,
        )
        hits = list(screen_result["hits"])
        total_candidates_evaluated += int(screen_result["total_evaluated"])
        qualifying_observations += len(hits)

        hits.sort(
            key=lambda hit: (
                -(_hit_rps_score(hit, selected_rps_windows) or Decimal("-1")),
                str(hit["symbol"]),
            )
        )
        selected_hits = hits[:portfolio_cap]
        selected_trade_count += len(selected_hits)
        day_returns: list[Decimal] = []
        day_completed = 0
        for hit in selected_hits:
            trade_or_exclusion = _simulate_trade(
                session,
                signal_date=signal_date,
                hit=hit,
                selected_windows=selected_rps_windows,
                holding_days=holding_days,
                stop_loss_pct=stop_loss_pct,
                entry_deferral_window_days=entry_deferral_window_days,
            )
            if isinstance(trade_or_exclusion, CupHandleRpsTrade):
                completed_trades.append(trade_or_exclusion)
                day_returns.append(Decimal(trade_or_exclusion.realized_return))
                day_completed += 1
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
        signal_days=[signal_day.to_dict() for signal_day in signal_days if signal_day.hit_count],
        trades=[trade.to_dict() for trade in completed_trades[:max_trades_returned]],
        excluded=excluded[:max_trades_returned],
        parameters={
            "rps_threshold": rps_threshold,
            "selected_rps_windows": selected_rps_windows,
            "cup_handle_params": cup_handle_params.to_dict(),
            "holding_days": holding_days,
            "stop_loss_pct": f"{stop_loss_pct:.4f}",
            "portfolio_cap": portfolio_cap,
            "entry_deferral_window_days": entry_deferral_window_days,
            "max_trades_returned": max_trades_returned,
        },
    )
