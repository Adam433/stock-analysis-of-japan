from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

from sqlalchemy import distinct, func, select

from stockanalyse_api.config.settings import (
    get_local_csv_raw_dir,
    get_tse_common_stock_symbols_path,
)
from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily

APPROVED_RPS_WINDOWS = (50, 120, 250)
DEFAULT_RPS_THRESHOLD = 90
DEFAULT_CUP_LOOKBACK_DAYS = 250
DEFAULT_CHART_WINDOW_DAYS = 250

# O'Neil cup-with-handle parameters (conservative defaults).
CUP_MIN_DEPTH_PCT = Decimal("12")
CUP_MAX_DEPTH_PCT = Decimal("35")
CUP_MIN_DURATION = 25
CUP_MAX_DURATION = 130
HANDLE_MIN_DURATION = 5
HANDLE_MAX_DURATION = 30
HANDLE_MAX_PULLBACK_PCT = Decimal("15")
HANDLE_MIN_PULLBACK_PCT = Decimal("3")
RIGHT_LIP_MAX_DELTA_PCT = Decimal("5")


@dataclass(slots=True)
class OverviewSnapshot:
    universe_size: int
    csv_pool_size: int
    instruments_in_db: int
    instruments_with_market_data: int
    instruments_updated_to_latest: int
    instruments_with_indicators_at_latest: int
    latest_trade_date: str | None
    latest_indicator_date: str | None
    coverage_ratio: float

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class ScreenHit:
    instrument_id: int
    symbol: str
    exchange: str
    name: str | None
    trade_date: str
    rps_50: str | None
    rps_120: str | None
    rps_250: str | None
    rps_passed: bool
    cup_handle_passed: bool
    cup_handle_breakout_date: str | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def get_overview(session) -> OverviewSnapshot:
    universe_size = _count_universe_manifest()
    csv_pool_size = _count_local_csv_pool()

    instruments_in_db = session.execute(select(func.count(Instrument.id))).scalar_one() or 0
    instruments_with_market_data = session.execute(
        select(func.count(distinct(MarketDataDaily.instrument_id)))
    ).scalar_one() or 0
    latest_trade_date = session.execute(select(func.max(MarketDataDaily.trade_date))).scalar_one()
    latest_indicator_date = session.execute(
        select(func.max(DerivedIndicatorDaily.trade_date))
    ).scalar_one()

    updated_to_latest = 0
    indicators_at_latest = 0
    if latest_trade_date is not None:
        updated_to_latest = session.execute(
            select(func.count(distinct(MarketDataDaily.instrument_id))).where(
                MarketDataDaily.trade_date == latest_trade_date
            )
        ).scalar_one() or 0
    if latest_indicator_date is not None:
        indicators_at_latest = session.execute(
            select(func.count(distinct(DerivedIndicatorDaily.instrument_id))).where(
                DerivedIndicatorDaily.trade_date == latest_indicator_date
            )
        ).scalar_one() or 0

    base_for_ratio = max(instruments_with_market_data, 1)
    coverage = updated_to_latest / base_for_ratio if instruments_with_market_data else 0.0

    return OverviewSnapshot(
        universe_size=universe_size,
        csv_pool_size=csv_pool_size,
        instruments_in_db=instruments_in_db,
        instruments_with_market_data=instruments_with_market_data,
        instruments_updated_to_latest=updated_to_latest,
        instruments_with_indicators_at_latest=indicators_at_latest,
        latest_trade_date=latest_trade_date.isoformat() if latest_trade_date else None,
        latest_indicator_date=latest_indicator_date.isoformat() if latest_indicator_date else None,
        coverage_ratio=round(coverage, 4),
    )


def _count_universe_manifest() -> int:
    path = get_tse_common_stock_symbols_path()
    if not path.exists():
        return 0
    return sum(
        1
        for raw in path.read_text(encoding="utf-8").splitlines()
        if raw.strip() and not raw.strip().startswith("#")
    )


def _count_local_csv_pool() -> int:
    csv_dir = get_local_csv_raw_dir()
    if not csv_dir.exists():
        return 0
    return sum(1 for entry in csv_dir.iterdir() if entry.suffix.lower() == ".csv")


def list_local_csv_symbols() -> list[str]:
    csv_dir = get_local_csv_raw_dir()
    if not csv_dir.exists():
        return []
    return sorted(entry.stem for entry in csv_dir.iterdir() if entry.suffix.lower() == ".csv")


def _normalize_rps_windows(values: list[int]) -> list[int]:
    if not values:
        return []
    cleaned = sorted({v for v in values if v in APPROVED_RPS_WINDOWS})
    return cleaned


def _resolve_target_trade_date(session, requested: date | None) -> date | None:
    if requested is not None:
        return requested
    return session.execute(select(func.max(DerivedIndicatorDaily.trade_date))).scalar_one()


def screen_universe(
    session,
    *,
    use_rps: bool,
    rps_threshold: int,
    selected_rps_windows: list[int],
    use_cup_handle: bool,
    trade_date: date | None = None,
) -> dict[str, object]:
    target_date = _resolve_target_trade_date(session, trade_date)
    if target_date is None:
        return {
            "trade_date": None,
            "criteria": {
                "use_rps": use_rps,
                "rps_threshold": rps_threshold,
                "selected_rps_windows": _normalize_rps_windows(selected_rps_windows),
                "use_cup_handle": use_cup_handle,
            },
            "total_evaluated": 0,
            "hits": [],
        }

    selected_windows = _normalize_rps_windows(selected_rps_windows)
    if use_rps and not selected_windows:
        # User chose RPS but no window — treat as no-op AND condition that fails everything.
        return {
            "trade_date": target_date.isoformat(),
            "criteria": {
                "use_rps": use_rps,
                "rps_threshold": rps_threshold,
                "selected_rps_windows": [],
                "use_cup_handle": use_cup_handle,
            },
            "total_evaluated": 0,
            "hits": [],
        }

    rows = session.execute(
        select(DerivedIndicatorDaily, Instrument)
        .join(Instrument, Instrument.id == DerivedIndicatorDaily.instrument_id)
        .where(DerivedIndicatorDaily.trade_date == target_date)
        .order_by(Instrument.symbol.asc())
    ).all()

    threshold_decimal = Decimal(rps_threshold)
    hits: list[ScreenHit] = []

    for indicator_row, instrument in rows:
        rps_value_by_window = {
            50: indicator_row.rps_50,
            120: indicator_row.rps_120,
            250: indicator_row.rps_250,
        }

        if use_rps:
            rps_passed = all(
                rps_value_by_window[w] is not None and rps_value_by_window[w] >= threshold_decimal
                for w in selected_windows
            )
        else:
            rps_passed = True

        if use_rps and not rps_passed:
            continue

        cup_breakout_date: date | None = None
        if use_cup_handle:
            candles = _load_candles(
                session,
                instrument_id=instrument.id,
                cutoff=target_date,
                limit=DEFAULT_CUP_LOOKBACK_DAYS,
            )
            cup_pattern = _detect_cup_handle_pattern(candles)
            if cup_pattern is None:
                continue
            cup_breakout_date = cup_pattern["breakout_date"]
            cup_handle_passed = True
        else:
            cup_handle_passed = False

        hits.append(
            ScreenHit(
                instrument_id=instrument.id,
                symbol=instrument.symbol,
                exchange=instrument.exchange,
                name=instrument.name,
                trade_date=target_date.isoformat(),
                rps_50=_format_decimal(indicator_row.rps_50, "0.01"),
                rps_120=_format_decimal(indicator_row.rps_120, "0.01"),
                rps_250=_format_decimal(indicator_row.rps_250, "0.01"),
                rps_passed=rps_passed,
                cup_handle_passed=cup_handle_passed,
                cup_handle_breakout_date=(
                    cup_breakout_date.isoformat() if cup_breakout_date else None
                ),
            )
        )

    return {
        "trade_date": target_date.isoformat(),
        "criteria": {
            "use_rps": use_rps,
            "rps_threshold": rps_threshold,
            "selected_rps_windows": selected_windows,
            "use_cup_handle": use_cup_handle,
        },
        "total_evaluated": len(rows),
        "hits": [hit.to_dict() for hit in hits],
    }


def get_chart_with_markers(
    session,
    *,
    instrument_id: int,
    use_rps: bool,
    rps_threshold: int,
    selected_rps_windows: list[int],
    use_cup_handle: bool,
    trade_date: date | None = None,
    window_days: int = DEFAULT_CHART_WINDOW_DAYS,
) -> dict[str, object] | None:
    instrument = session.get(Instrument, instrument_id)
    if instrument is None:
        return None

    target_date = _resolve_target_trade_date(session, trade_date)
    if target_date is None:
        return None

    candles = _load_candles(
        session,
        instrument_id=instrument_id,
        cutoff=target_date,
        limit=window_days,
    )
    if not candles:
        return None

    earliest = candles[0].trade_date
    indicator_rows = session.execute(
        select(DerivedIndicatorDaily)
        .where(
            DerivedIndicatorDaily.instrument_id == instrument_id,
            DerivedIndicatorDaily.trade_date >= earliest,
            DerivedIndicatorDaily.trade_date <= target_date,
        )
        .order_by(DerivedIndicatorDaily.trade_date.asc())
    ).scalars().all()

    selected_windows = _normalize_rps_windows(selected_rps_windows)
    threshold_decimal = Decimal(rps_threshold)

    rps_marker_dates: list[date] = []
    if use_rps and selected_windows:
        for row in indicator_rows:
            rps_values = {50: row.rps_50, 120: row.rps_120, 250: row.rps_250}
            passes = all(
                rps_values[w] is not None and rps_values[w] >= threshold_decimal
                for w in selected_windows
            )
            if passes:
                rps_marker_dates.append(row.trade_date)

    cup_breakout_date: date | None = None
    cup_pattern_meta: dict[str, object] | None = None
    if use_cup_handle:
        cup_pattern = _detect_cup_handle_pattern(candles)
        if cup_pattern is not None:
            cup_breakout_date = cup_pattern["breakout_date"]
            cup_pattern_meta = {
                "left_lip_date": cup_pattern["left_lip_date"].isoformat(),
                "cup_bottom_date": cup_pattern["cup_bottom_date"].isoformat(),
                "right_lip_date": cup_pattern["right_lip_date"].isoformat(),
                "handle_low_date": cup_pattern["handle_low_date"].isoformat(),
                "breakout_date": cup_pattern["breakout_date"].isoformat(),
                "cup_depth_pct": str(cup_pattern["cup_depth_pct"]),
                "handle_depth_pct": str(cup_pattern["handle_depth_pct"]),
            }

    return {
        "instrument": {
            "id": instrument.id,
            "symbol": instrument.symbol,
            "exchange": instrument.exchange,
            "name": instrument.name,
        },
        "trade_date": target_date.isoformat(),
        "candles": [
            {
                "trade_date": c.trade_date.isoformat(),
                "open": _to_float(c.open),
                "high": _to_float(c.high),
                "low": _to_float(c.low),
                "close": _to_float(c.close),
                "volume": c.volume,
            }
            for c in candles
        ],
        "rps_marker_dates": [d.isoformat() for d in rps_marker_dates],
        "cup_handle_breakout_date": (
            cup_breakout_date.isoformat() if cup_breakout_date else None
        ),
        "cup_handle_pattern": cup_pattern_meta,
    }


def _load_candles(session, *, instrument_id: int, cutoff: date, limit: int) -> list[MarketDataDaily]:
    rows = session.execute(
        select(MarketDataDaily)
        .where(
            MarketDataDaily.instrument_id == instrument_id,
            MarketDataDaily.trade_date <= cutoff,
        )
        .order_by(MarketDataDaily.trade_date.desc())
        .limit(limit)
    ).scalars().all()
    rows.reverse()
    return rows


def _detect_cup_handle_pattern(candles: list[MarketDataDaily]) -> dict[str, object] | None:
    """Detect a recent O'Neil cup-with-handle pattern.

    Returns a dict describing the pattern, or None if no pattern ends within
    the supplied candle window. Only the most recent valid pattern is returned.
    """
    if len(candles) < CUP_MIN_DURATION + HANDLE_MIN_DURATION:
        return None

    closes = [c.close for c in candles]
    highs = [c.high if c.high is not None else c.close for c in candles]
    lows = [c.low if c.low is not None else c.close for c in candles]
    if any(v is None for v in closes):
        return None

    n = len(candles)
    # Search back to ~last 30 trading days for a breakout day.
    breakout_search_start = max(n - 30, 0)
    for breakout_idx in range(n - 1, breakout_search_start - 1, -1):
        result = _try_pattern_ending_at(candles, highs, lows, closes, breakout_idx)
        if result is not None:
            return result
    return None


def _try_pattern_ending_at(
    candles: list[MarketDataDaily],
    highs: list,
    lows: list,
    closes: list,
    breakout_idx: int,
) -> dict[str, object] | None:
    # Handle window: HANDLE_MIN_DURATION..HANDLE_MAX_DURATION before breakout_idx.
    for handle_len in range(HANDLE_MIN_DURATION, HANDLE_MAX_DURATION + 1):
        right_lip_idx = breakout_idx - handle_len
        if right_lip_idx < CUP_MIN_DURATION:
            break
        right_lip_high = highs[right_lip_idx]
        if right_lip_high is None or right_lip_high <= 0:
            continue
        # Breakout requires close above right lip.
        if closes[breakout_idx] is None or closes[breakout_idx] <= right_lip_high:
            continue
        handle_segment_lows = lows[right_lip_idx + 1 : breakout_idx + 1]
        if not handle_segment_lows or any(v is None for v in handle_segment_lows):
            continue
        handle_low = min(handle_segment_lows)
        handle_low_offset = handle_segment_lows.index(handle_low)
        handle_pullback_pct = (Decimal(right_lip_high - handle_low) / Decimal(right_lip_high)) * Decimal("100")
        if handle_pullback_pct < HANDLE_MIN_PULLBACK_PCT or handle_pullback_pct > HANDLE_MAX_PULLBACK_PCT:
            continue

        # Cup window: precedes the right lip; depth from peak down to bottom and recovery to right lip.
        for cup_len in range(CUP_MIN_DURATION, CUP_MAX_DURATION + 1):
            left_lip_idx = right_lip_idx - cup_len
            if left_lip_idx < 0:
                break
            cup_segment_highs = highs[left_lip_idx : right_lip_idx + 1]
            cup_segment_lows = lows[left_lip_idx : right_lip_idx + 1]
            if any(v is None for v in cup_segment_highs) or any(v is None for v in cup_segment_lows):
                continue
            left_lip_high = highs[left_lip_idx]
            cup_bottom = min(cup_segment_lows)
            cup_bottom_offset = cup_segment_lows.index(cup_bottom)
            cup_bottom_idx = left_lip_idx + cup_bottom_offset

            # The bottom should sit roughly in the middle, not adjacent to the lips.
            if cup_bottom_offset < 5 or (cup_len - cup_bottom_offset) < 5:
                continue

            cup_depth_pct = (Decimal(left_lip_high - cup_bottom) / Decimal(left_lip_high)) * Decimal("100")
            if cup_depth_pct < CUP_MIN_DEPTH_PCT or cup_depth_pct > CUP_MAX_DEPTH_PCT:
                continue

            # Right lip should approximately equal left lip (within tolerance).
            lip_delta_pct = abs(Decimal(left_lip_high - right_lip_high) / Decimal(left_lip_high)) * Decimal("100")
            if lip_delta_pct > RIGHT_LIP_MAX_DELTA_PCT:
                continue

            # The cup interior should not exceed lips (rounded shape, no spike above either lip).
            interior_high = max(cup_segment_highs[1:-1]) if cup_len >= 2 else cup_bottom
            lip_baseline = max(left_lip_high, right_lip_high)
            if interior_high > lip_baseline * 1.02:
                continue

            return {
                "left_lip_date": candles[left_lip_idx].trade_date,
                "cup_bottom_date": candles[cup_bottom_idx].trade_date,
                "right_lip_date": candles[right_lip_idx].trade_date,
                "handle_low_date": candles[right_lip_idx + 1 + handle_low_offset].trade_date,
                "breakout_date": candles[breakout_idx].trade_date,
                "cup_depth_pct": cup_depth_pct.quantize(Decimal("0.01")),
                "handle_depth_pct": handle_pullback_pct.quantize(Decimal("0.01")),
            }
    return None


def _to_float(value) -> float | None:
    if value is None:
        return None
    return float(value)


def _format_decimal(value, pattern: str) -> str | None:
    if value is None:
        return None
    return f"{value:.2f}" if pattern == "0.01" else str(value)
