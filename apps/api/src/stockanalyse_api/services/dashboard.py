from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import distinct, func, select

from stockanalyse_api.config.settings import (
    get_local_csv_raw_dir,
)
from stockanalyse_api.domain.fundamentals.models import FundamentalsAnnual
from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.market_data_adjustments import adjusted_ohlc
from stockanalyse_api.services.market_data_adjustments import has_extreme_price_gap
from stockanalyse_api.services.market_data_adjustments import is_complete_market_row

APPROVED_RPS_WINDOWS = (50, 120, 250)
DEFAULT_RPS_THRESHOLD = 90
DEFAULT_CUP_LOOKBACK_DAYS = 520
DEFAULT_CHART_WINDOW_DAYS = 250
MARKET_EXCHANGES = {
    "jp": ("TSE",),
    "us": ("US",),
}

# O'Neil cup-with-handle parameters (conservative defaults).
CUP_MIN_DEPTH_PCT = Decimal("12")
CUP_MAX_DEPTH_PCT = Decimal("33")
CUP_MIN_DURATION = 60
CUP_MAX_DURATION = 220
HANDLE_MIN_DURATION = 5
HANDLE_MAX_DURATION = 40
HANDLE_MAX_PULLBACK_PCT = Decimal("12")
HANDLE_MIN_PULLBACK_PCT = Decimal("3")
RIGHT_LIP_MAX_DELTA_PCT = Decimal("5")
CUP_HANDLE_MIN_TOTAL_DURATION = 120
CUP_HANDLE_MAX_TOTAL_DURATION = 260
CUP_HANDLE_BREAKOUT_LOOKBACK_DAYS = 30
PRIOR_UPTREND_LOOKBACK_DAYS = 120
MIN_PRIOR_UPTREND_PCT = Decimal("30")
MIN_HANDLE_LOW_POSITION_PCT = Decimal("66")
MAX_HANDLE_DEPTH_TO_CUP_DEPTH_PCT = Decimal("35")
MAX_HANDLE_HIGH_ABOVE_LIP_PCT = Decimal("2")
MIN_BOTTOM_DWELL_DAYS = 5
BOTTOM_ZONE_PCT = Decimal("20")
MIN_BOTTOM_SPAN_PCT = Decimal("10")
MIN_CUP_SIDE_DURATION_PCT = Decimal("20")
BREAKOUT_VOLUME_AVG_DAYS = 50
MIN_BREAKOUT_VOLUME_MULTIPLIER = Decimal("1.4")


@dataclass(frozen=True, slots=True)
class CupHandleParams:
    min_cup_duration: int = CUP_MIN_DURATION
    max_cup_duration: int = CUP_MAX_DURATION
    min_handle_duration: int = HANDLE_MIN_DURATION
    max_handle_duration: int = HANDLE_MAX_DURATION
    min_total_duration: int = CUP_HANDLE_MIN_TOTAL_DURATION
    max_total_duration: int = CUP_HANDLE_MAX_TOTAL_DURATION
    min_cup_depth_pct: Decimal = CUP_MIN_DEPTH_PCT
    max_cup_depth_pct: Decimal = CUP_MAX_DEPTH_PCT
    min_handle_pullback_pct: Decimal = HANDLE_MIN_PULLBACK_PCT
    max_handle_pullback_pct: Decimal = HANDLE_MAX_PULLBACK_PCT
    max_right_lip_delta_pct: Decimal = RIGHT_LIP_MAX_DELTA_PCT
    require_prior_uptrend: bool = True
    prior_uptrend_lookback_days: int = PRIOR_UPTREND_LOOKBACK_DAYS
    min_prior_uptrend_pct: Decimal = MIN_PRIOR_UPTREND_PCT
    min_handle_low_position_pct: Decimal = MIN_HANDLE_LOW_POSITION_PCT
    max_handle_depth_to_cup_depth_pct: Decimal = MAX_HANDLE_DEPTH_TO_CUP_DEPTH_PCT
    max_handle_high_above_lip_pct: Decimal = MAX_HANDLE_HIGH_ABOVE_LIP_PCT
    min_bottom_dwell_days: int = MIN_BOTTOM_DWELL_DAYS
    bottom_zone_pct: Decimal = BOTTOM_ZONE_PCT
    min_bottom_span_pct: Decimal = MIN_BOTTOM_SPAN_PCT
    min_cup_side_duration_pct: Decimal = MIN_CUP_SIDE_DURATION_PCT
    require_breakout_volume: bool = False
    breakout_volume_avg_days: int = BREAKOUT_VOLUME_AVG_DAYS
    min_breakout_volume_multiplier: Decimal = MIN_BREAKOUT_VOLUME_MULTIPLIER
    breakout_lookback_days: int = CUP_HANDLE_BREAKOUT_LOOKBACK_DAYS
    lookback_days: int = DEFAULT_CUP_LOOKBACK_DAYS

    @property
    def effective_lookback_days(self) -> int:
        prior_window = self.prior_uptrend_lookback_days if self.require_prior_uptrend else 0
        return max(
            self.lookback_days,
            prior_window + self.max_total_duration + self.breakout_lookback_days + 5,
        )

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        for key, value in payload.items():
            if isinstance(value, Decimal):
                payload[key] = str(value)
        return payload


DEFAULT_CUP_HANDLE_PARAMS = CupHandleParams()


@dataclass(frozen=True, slots=True)
class FundamentalGrowthParams:
    enabled: bool = False
    min_years: int = 3
    min_growth_count: int | None = None
    min_yoy_growth_pct: Decimal = Decimal("0")
    require_positive_net_income: bool = True
    reporting_lag_days: int = 120

    @property
    def effective_min_growth_count(self) -> int:
        return self.min_growth_count if self.min_growth_count is not None else self.min_years - 1

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        for key, value in payload.items():
            if isinstance(value, Decimal):
                payload[key] = str(value)
        payload["effective_min_growth_count"] = self.effective_min_growth_count
        return payload


DEFAULT_FUNDAMENTAL_GROWTH_PARAMS = FundamentalGrowthParams()


def normalize_market(value: str | None) -> str:
    market = (value or "jp").lower()
    if market not in MARKET_EXCHANGES:
        raise ValueError("market must be jp or us.")
    return market


def _market_exchanges(market: str | None) -> tuple[str, ...]:
    return MARKET_EXCHANGES[normalize_market(market)]


@dataclass(slots=True)
class OverviewSnapshot:
    csv_pool_size: int
    instruments_in_db: int
    instruments_with_market_data: int
    fundamentals_rows: int
    instruments_with_fundamentals: int
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
    rps_pass_count: int
    cup_handle_passed: bool
    cup_handle_breakout_date: str | None
    fundamental_growth_passed: bool
    fundamental_growth_years: int | None
    fundamental_growth_count: int | None
    fundamental_growth_latest_year: str | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class _PatternCandle:
    trade_date: date
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    adj_close: Decimal | None = None
    volume: int | None = None
    data_status: str = "complete"


@dataclass(frozen=True, slots=True)
class _CupCandidate:
    left_lip_idx: int
    cup_bottom_idx: int
    cup_depth_pct: float


def get_overview(session, *, market: str | None = None) -> OverviewSnapshot:
    exchanges = _market_exchanges(market)
    csv_pool_size = _count_local_csv_pool()

    instruments_in_db = session.execute(
        select(func.count(Instrument.id)).where(Instrument.exchange.in_(exchanges))
    ).scalar_one() or 0
    if not instruments_in_db:
        return OverviewSnapshot(
            csv_pool_size=csv_pool_size,
            instruments_in_db=0,
            instruments_with_market_data=0,
            fundamentals_rows=0,
            instruments_with_fundamentals=0,
            instruments_updated_to_latest=0,
            instruments_with_indicators_at_latest=0,
            latest_trade_date=None,
            latest_indicator_date=None,
            coverage_ratio=0.0,
        )

    has_market_data = (
        select(MarketDataDaily.id)
        .where(MarketDataDaily.instrument_id == Instrument.id)
        .exists()
    )
    instrument_id_query = select(Instrument.id).where(Instrument.exchange.in_(exchanges))
    instruments_with_market_data = session.execute(
        select(func.count(Instrument.id)).where(
            Instrument.exchange.in_(exchanges),
            has_market_data,
        )
    ).scalar_one() or 0
    fundamentals_rows = session.execute(
        select(func.count(FundamentalsAnnual.id)).where(
            FundamentalsAnnual.instrument_id.in_(instrument_id_query)
        )
    ).scalar_one() or 0
    instruments_with_fundamentals = session.execute(
        select(func.count(distinct(FundamentalsAnnual.instrument_id))).where(
            FundamentalsAnnual.instrument_id.in_(instrument_id_query),
            FundamentalsAnnual.net_income.is_not(None),
        )
    ).scalar_one() or 0
    latest_trade_date = session.execute(
        select(MarketDataDaily.trade_date)
        .join(Instrument, Instrument.id == MarketDataDaily.instrument_id)
        .where(Instrument.exchange.in_(exchanges))
        .order_by(MarketDataDaily.trade_date.desc())
        .limit(1)
    ).scalar_one_or_none()
    latest_indicator_date = session.execute(
        select(DerivedIndicatorDaily.trade_date)
        .join(Instrument, Instrument.id == DerivedIndicatorDaily.instrument_id)
        .where(Instrument.exchange.in_(exchanges))
        .order_by(DerivedIndicatorDaily.trade_date.desc())
        .limit(1)
    ).scalar_one_or_none()

    updated_to_latest = 0
    indicators_at_latest = 0
    if latest_trade_date is not None:
        updated_to_latest = session.execute(
            select(func.count(distinct(MarketDataDaily.instrument_id)))
            .join(Instrument, Instrument.id == MarketDataDaily.instrument_id)
            .where(
                Instrument.exchange.in_(exchanges),
                MarketDataDaily.trade_date == latest_trade_date,
            )
        ).scalar_one() or 0
    if latest_indicator_date is not None:
        indicators_at_latest = session.execute(
            select(func.count(distinct(DerivedIndicatorDaily.instrument_id)))
            .join(Instrument, Instrument.id == DerivedIndicatorDaily.instrument_id)
            .where(
                Instrument.exchange.in_(exchanges),
                DerivedIndicatorDaily.trade_date == latest_indicator_date,
            )
        ).scalar_one() or 0

    base_for_ratio = max(instruments_with_market_data, 1)
    coverage = updated_to_latest / base_for_ratio if instruments_with_market_data else 0.0

    return OverviewSnapshot(
        csv_pool_size=csv_pool_size,
        instruments_in_db=instruments_in_db,
        instruments_with_market_data=instruments_with_market_data,
        fundamentals_rows=fundamentals_rows,
        instruments_with_fundamentals=instruments_with_fundamentals,
        instruments_updated_to_latest=updated_to_latest,
        instruments_with_indicators_at_latest=indicators_at_latest,
        latest_trade_date=latest_trade_date.isoformat() if latest_trade_date else None,
        latest_indicator_date=latest_indicator_date.isoformat() if latest_indicator_date else None,
        coverage_ratio=round(coverage, 4),
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


def _normalize_cup_handle_params(params: CupHandleParams | None = None) -> CupHandleParams:
    params = params or DEFAULT_CUP_HANDLE_PARAMS
    checks = (
        ("min_cup_duration", params.min_cup_duration),
        ("max_cup_duration", params.max_cup_duration),
        ("min_handle_duration", params.min_handle_duration),
        ("max_handle_duration", params.max_handle_duration),
        ("min_total_duration", params.min_total_duration),
        ("max_total_duration", params.max_total_duration),
        ("prior_uptrend_lookback_days", params.prior_uptrend_lookback_days),
        ("min_bottom_dwell_days", params.min_bottom_dwell_days),
        ("breakout_volume_avg_days", params.breakout_volume_avg_days),
        ("breakout_lookback_days", params.breakout_lookback_days),
        ("lookback_days", params.lookback_days),
    )
    for name, value in checks:
        if value <= 0:
            raise ValueError(f"{name} must be greater than 0.")

    if params.min_cup_duration > params.max_cup_duration:
        raise ValueError("min_cup_duration must be less than or equal to max_cup_duration.")
    if params.min_handle_duration > params.max_handle_duration:
        raise ValueError(
            "min_handle_duration must be less than or equal to max_handle_duration."
        )
    if params.min_total_duration > params.max_total_duration:
        raise ValueError(
            "min_total_duration must be less than or equal to max_total_duration."
        )
    if params.max_total_duration < params.min_cup_duration + params.min_handle_duration:
        raise ValueError(
            "max_total_duration is too small for the selected cup and handle durations."
        )
    if params.min_total_duration > params.max_cup_duration + params.max_handle_duration:
        raise ValueError(
            "min_total_duration is too large for the selected cup and handle durations."
        )

    percent_pairs = (
        ("min_cup_depth_pct", params.min_cup_depth_pct),
        ("max_cup_depth_pct", params.max_cup_depth_pct),
        ("min_handle_pullback_pct", params.min_handle_pullback_pct),
        ("max_handle_pullback_pct", params.max_handle_pullback_pct),
        ("max_right_lip_delta_pct", params.max_right_lip_delta_pct),
        ("min_prior_uptrend_pct", params.min_prior_uptrend_pct),
        ("min_handle_low_position_pct", params.min_handle_low_position_pct),
        ("max_handle_depth_to_cup_depth_pct", params.max_handle_depth_to_cup_depth_pct),
        ("max_handle_high_above_lip_pct", params.max_handle_high_above_lip_pct),
        ("bottom_zone_pct", params.bottom_zone_pct),
        ("min_bottom_span_pct", params.min_bottom_span_pct),
        ("min_cup_side_duration_pct", params.min_cup_side_duration_pct),
        ("min_breakout_volume_multiplier", params.min_breakout_volume_multiplier),
    )
    for name, value in percent_pairs:
        if value < 0:
            raise ValueError(f"{name} must be greater than or equal to 0.")
    if params.min_cup_depth_pct > params.max_cup_depth_pct:
        raise ValueError("min_cup_depth_pct must be less than or equal to max_cup_depth_pct.")
    if params.min_handle_pullback_pct > params.max_handle_pullback_pct:
        raise ValueError(
            "min_handle_pullback_pct must be less than or equal to max_handle_pullback_pct."
        )

    return params


def _normalize_rps_windows(values: list[int]) -> list[int]:
    if not values:
        return []
    cleaned = sorted({v for v in values if v in APPROVED_RPS_WINDOWS})
    return cleaned


def _normalize_min_rps_windows_passing(value: int, selected_windows: list[int]) -> int:
    if value < 1:
        raise ValueError("min_rps_windows_passing must be greater than or equal to 1.")
    if selected_windows and value > len(selected_windows):
        raise ValueError("min_rps_windows_passing cannot exceed selected RPS window count.")
    return value


def _normalize_fundamental_growth_params(
    params: FundamentalGrowthParams | None = None,
) -> FundamentalGrowthParams:
    params = params or DEFAULT_FUNDAMENTAL_GROWTH_PARAMS
    if params.min_years < 2:
        raise ValueError("fundamental min_years must be greater than or equal to 2.")
    if params.reporting_lag_days < 0:
        raise ValueError("fundamental reporting_lag_days must be greater than or equal to 0.")
    if params.min_yoy_growth_pct < Decimal("-100"):
        raise ValueError("fundamental min_yoy_growth_pct must be greater than or equal to -100.")
    if params.effective_min_growth_count < 1:
        raise ValueError("fundamental min_growth_count must be greater than or equal to 1.")
    if params.effective_min_growth_count > params.min_years - 1:
        raise ValueError("fundamental min_growth_count cannot exceed min_years - 1.")
    return params


def _resolve_target_trade_date(
    session,
    requested: date | None,
    *,
    market: str | None = None,
) -> date | None:
    if requested is not None:
        return requested
    exchanges = _market_exchanges(market)
    has_market_instrument = session.execute(
        select(Instrument.id).where(Instrument.exchange.in_(exchanges)).limit(1)
    ).scalar_one_or_none()
    if has_market_instrument is None:
        return None
    return session.execute(
        select(DerivedIndicatorDaily.trade_date)
        .join(Instrument, Instrument.id == DerivedIndicatorDaily.instrument_id)
        .where(Instrument.exchange.in_(exchanges))
        .order_by(DerivedIndicatorDaily.trade_date.desc())
        .limit(1)
    ).scalar_one_or_none()


def screen_universe(
    session,
    *,
    use_rps: bool,
    rps_threshold: int,
    selected_rps_windows: list[int],
    min_rps_windows_passing: int = 1,
    use_cup_handle: bool = False,
    cup_handle_params: CupHandleParams | None = None,
    fundamental_growth_params: FundamentalGrowthParams | None = None,
    trade_date: date | None = None,
    market: str | None = None,
) -> dict[str, object]:
    resolved_market = normalize_market(market)
    exchanges = _market_exchanges(resolved_market)
    resolved_cup_params = (
        _normalize_cup_handle_params(cup_handle_params) if use_cup_handle else DEFAULT_CUP_HANDLE_PARAMS
    )
    resolved_fundamental_params = _normalize_fundamental_growth_params(
        fundamental_growth_params
    )
    target_date = _resolve_target_trade_date(session, trade_date, market=resolved_market)
    if target_date is None:
        return {
            "trade_date": None,
            "criteria": {
                "use_rps": use_rps,
                "rps_threshold": rps_threshold,
                "selected_rps_windows": _normalize_rps_windows(selected_rps_windows),
                "min_rps_windows_passing": min_rps_windows_passing,
                "use_cup_handle": use_cup_handle,
                "cup_handle_params": resolved_cup_params.to_dict(),
                "fundamental_growth_params": resolved_fundamental_params.to_dict(),
                "market": resolved_market,
            },
            "total_evaluated": 0,
            "hits": [],
        }

    selected_windows = _normalize_rps_windows(selected_rps_windows)
    resolved_min_rps_passing = _normalize_min_rps_windows_passing(
        min_rps_windows_passing,
        selected_windows,
    )
    if use_rps and not selected_windows:
        # User chose RPS but no window — treat as no-op AND condition that fails everything.
        return {
            "trade_date": target_date.isoformat(),
            "criteria": {
                "use_rps": use_rps,
                "rps_threshold": rps_threshold,
                "selected_rps_windows": [],
                "min_rps_windows_passing": resolved_min_rps_passing,
                "use_cup_handle": use_cup_handle,
                "cup_handle_params": resolved_cup_params.to_dict(),
                "fundamental_growth_params": resolved_fundamental_params.to_dict(),
                "market": resolved_market,
            },
            "total_evaluated": 0,
            "hits": [],
        }

    rows = session.execute(
        select(DerivedIndicatorDaily, Instrument)
        .join(Instrument, Instrument.id == DerivedIndicatorDaily.instrument_id)
        .where(
            DerivedIndicatorDaily.trade_date == target_date,
            Instrument.exchange.in_(exchanges),
        )
        .order_by(Instrument.symbol.asc())
    ).all()

    threshold_decimal = Decimal(rps_threshold)
    candidates: list[tuple[DerivedIndicatorDaily, Instrument, bool, int]] = []

    for indicator_row, instrument in rows:
        rps_value_by_window = {
            50: indicator_row.rps_50,
            120: indicator_row.rps_120,
            250: indicator_row.rps_250,
        }

        rps_pass_count = sum(
            1
            for window in selected_windows
            if rps_value_by_window[window] is not None
            and rps_value_by_window[window] >= threshold_decimal
        )
        if use_rps:
            rps_passed = rps_pass_count >= resolved_min_rps_passing
        else:
            rps_passed = True

        if use_rps and not rps_passed:
            continue

        candidates.append((indicator_row, instrument, rps_passed, rps_pass_count))

    candles_by_instrument: dict[int, list[_PatternCandle]] = {}
    if use_cup_handle and candidates:
        candles_by_instrument = _load_candles_by_instrument(
            session,
            instrument_ids=[instrument.id for _, instrument, _, _ in candidates],
            cutoff=target_date,
            limit=resolved_cup_params.effective_lookback_days,
        )

    hits: list[ScreenHit] = []
    for indicator_row, instrument, rps_passed, rps_pass_count in candidates:
        cup_breakout_date: date | None = None
        if use_cup_handle:
            candles = candles_by_instrument.get(instrument.id, [])
            cup_pattern = _detect_cup_handle_pattern(candles, resolved_cup_params)
            if cup_pattern is None:
                continue
            cup_breakout_date = cup_pattern["breakout_date"]
            cup_handle_passed = True
        else:
            cup_handle_passed = False

        fundamental_meta = _evaluate_fundamental_growth(
            session,
            instrument_id=instrument.id,
            signal_date=target_date,
            params=resolved_fundamental_params,
        )
        if resolved_fundamental_params.enabled and not fundamental_meta["passed"]:
            continue

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
                rps_pass_count=rps_pass_count,
                cup_handle_passed=cup_handle_passed,
                cup_handle_breakout_date=(
                    cup_breakout_date.isoformat() if cup_breakout_date else None
                ),
                fundamental_growth_passed=bool(fundamental_meta["passed"]),
                fundamental_growth_years=fundamental_meta["available_years"],
                fundamental_growth_count=fundamental_meta["growth_count"],
                fundamental_growth_latest_year=fundamental_meta["latest_fiscal_year"],
            )
        )

    return {
        "trade_date": target_date.isoformat(),
        "criteria": {
            "use_rps": use_rps,
            "rps_threshold": rps_threshold,
            "selected_rps_windows": selected_windows,
            "min_rps_windows_passing": resolved_min_rps_passing,
            "use_cup_handle": use_cup_handle,
            "cup_handle_params": resolved_cup_params.to_dict(),
            "fundamental_growth_params": resolved_fundamental_params.to_dict(),
            "market": resolved_market,
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
    min_rps_windows_passing: int = 1,
    cup_handle_params: CupHandleParams | None = None,
    trade_date: date | None = None,
    window_days: int = DEFAULT_CHART_WINDOW_DAYS,
    market: str | None = None,
) -> dict[str, object] | None:
    resolved_cup_params = (
        _normalize_cup_handle_params(cup_handle_params) if use_cup_handle else DEFAULT_CUP_HANDLE_PARAMS
    )
    instrument = session.get(Instrument, instrument_id)
    if instrument is None:
        return None

    chart_market = market
    if chart_market is None:
        chart_market = "us" if instrument.exchange == "US" else "jp"
    target_date = _resolve_target_trade_date(session, trade_date, market=chart_market)
    if target_date is None:
        return None

    candles = _load_candles(
        session,
        instrument_id=instrument_id,
        cutoff=target_date,
        limit=max(window_days, resolved_cup_params.effective_lookback_days)
        if use_cup_handle
        else window_days,
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
    resolved_min_rps_passing = _normalize_min_rps_windows_passing(
        min_rps_windows_passing,
        selected_windows,
    )
    threshold_decimal = Decimal(rps_threshold)

    rps_marker_dates: list[date] = []
    if use_rps and selected_windows:
        for row in indicator_rows:
            rps_values = {50: row.rps_50, 120: row.rps_120, 250: row.rps_250}
            pass_count = sum(
                1
                for window in selected_windows
                if rps_values[window] is not None and rps_values[window] >= threshold_decimal
            )
            if pass_count >= resolved_min_rps_passing:
                rps_marker_dates.append(row.trade_date)

    cup_breakout_date: date | None = None
    cup_pattern_meta: dict[str, object] | None = None
    if use_cup_handle:
        cup_pattern = _detect_cup_handle_pattern(candles, resolved_cup_params)
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
                "cup_duration": cup_pattern["cup_duration"],
                "handle_duration": cup_pattern["handle_duration"],
                "total_duration": cup_pattern["total_duration"],
                "handle_position_pct": str(cup_pattern["handle_position_pct"]),
                "handle_depth_to_cup_depth_pct": str(
                    cup_pattern["handle_depth_to_cup_depth_pct"]
                ),
                "prior_uptrend_pct": (
                    str(cup_pattern["prior_uptrend_pct"])
                    if cup_pattern["prior_uptrend_pct"] is not None
                    else None
                ),
                "breakout_volume_ratio": (
                    str(cup_pattern["breakout_volume_ratio"])
                    if cup_pattern["breakout_volume_ratio"] is not None
                    else None
                ),
            }

    return {
        "instrument": {
            "id": instrument.id,
            "symbol": instrument.symbol,
            "exchange": instrument.exchange,
            "name": instrument.name,
        },
        "trade_date": target_date.isoformat(),
        "candles": [_serialize_adjusted_candle(c) for c in candles if is_complete_market_row(c)],
        "rps_marker_dates": [d.isoformat() for d in rps_marker_dates],
        "cup_handle_breakout_date": (
            cup_breakout_date.isoformat() if cup_breakout_date else None
        ),
        "cup_handle_pattern": cup_pattern_meta,
        "cup_handle_params": resolved_cup_params.to_dict(),
    }


def _serialize_adjusted_candle(candle: MarketDataDaily) -> dict[str, object]:
    adjusted = adjusted_ohlc(candle)
    return {
        "trade_date": candle.trade_date.isoformat(),
        "open": _to_float(adjusted.open),
        "high": _to_float(adjusted.high),
        "low": _to_float(adjusted.low),
        "close": _to_float(adjusted.close),
        "volume": candle.volume,
        "data_status": candle.data_status,
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


def _load_candles_by_instrument(
    session,
    *,
    instrument_ids: list[int],
    cutoff: date,
    limit: int,
    chunk_size: int = 500,
) -> dict[int, list[_PatternCandle]]:
    if not instrument_ids:
        return {}

    candles_by_instrument: dict[int, list[_PatternCandle]] = defaultdict(list)
    lookback_start = cutoff - timedelta(days=max(limit * 3, 750))
    for offset in range(0, len(instrument_ids), chunk_size):
        chunk = instrument_ids[offset : offset + chunk_size]
        rows = session.execute(
            select(
                MarketDataDaily.instrument_id,
                MarketDataDaily.trade_date,
                MarketDataDaily.high,
                MarketDataDaily.low,
                MarketDataDaily.close,
                MarketDataDaily.adj_close,
                MarketDataDaily.volume,
                MarketDataDaily.data_status,
            )
            .where(
                MarketDataDaily.instrument_id.in_(chunk),
                MarketDataDaily.trade_date >= lookback_start,
                MarketDataDaily.trade_date <= cutoff,
            )
            .order_by(MarketDataDaily.instrument_id.asc(), MarketDataDaily.trade_date.asc())
        ).all()
        for row in rows:
            candles_by_instrument[row.instrument_id].append(
                _PatternCandle(
                    trade_date=row.trade_date,
                    high=row.high,
                    low=row.low,
                    close=row.close,
                    adj_close=row.adj_close,
                    volume=row.volume,
                    data_status=row.data_status,
                )
            )
        for instrument_id in chunk:
            candles = candles_by_instrument.get(instrument_id)
            if candles and len(candles) > limit:
                candles_by_instrument[instrument_id] = candles[-limit:]

    for instrument_id in instrument_ids:
        candles = candles_by_instrument.get(instrument_id, [])
        if len(candles) >= limit:
            continue
        rows = _load_candles(
            session,
            instrument_id=instrument_id,
            cutoff=cutoff,
            limit=limit,
        )
        if rows:
            candles_by_instrument[instrument_id] = [
                _PatternCandle(
                    trade_date=row.trade_date,
                    high=row.high,
                    low=row.low,
                    close=row.close,
                    adj_close=row.adj_close,
                    volume=row.volume,
                    data_status=row.data_status,
                )
                for row in rows
            ]

    return dict(candles_by_instrument)


def _evaluate_fundamental_growth(
    session,
    *,
    instrument_id: int,
    signal_date: date,
    params: FundamentalGrowthParams,
) -> dict[str, object]:
    if not params.enabled:
        return {
            "passed": True,
            "available_years": None,
            "growth_count": None,
            "latest_fiscal_year": None,
        }

    available_cutoff = signal_date - timedelta(days=params.reporting_lag_days)
    rows = list(
        session.execute(
            select(FundamentalsAnnual)
            .where(
                FundamentalsAnnual.instrument_id == instrument_id,
                FundamentalsAnnual.fiscal_year_end_date <= available_cutoff,
                FundamentalsAnnual.net_income.is_not(None),
                FundamentalsAnnual.data_status != "missing",
            )
            .order_by(FundamentalsAnnual.fiscal_year_end_date.desc())
            .limit(params.min_years)
        ).scalars()
    )
    rows.reverse()
    if len(rows) < params.min_years:
        return {
            "passed": False,
            "available_years": len(rows),
            "growth_count": None,
            "latest_fiscal_year": rows[-1].fiscal_year_label if rows else None,
        }

    net_income_values = [row.net_income for row in rows]
    if params.require_positive_net_income and any(
        value is None or value <= 0 for value in net_income_values
    ):
        return {
            "passed": False,
            "available_years": len(rows),
            "growth_count": 0,
            "latest_fiscal_year": rows[-1].fiscal_year_label,
        }

    growth_multiplier = Decimal("1") + (params.min_yoy_growth_pct / Decimal("100"))
    growth_count = 0
    for previous, current in zip(net_income_values, net_income_values[1:]):
        if previous is None or current is None:
            continue
        if previous > 0:
            passed_growth = current >= previous * growth_multiplier
        else:
            passed_growth = current > previous
        if passed_growth:
            growth_count += 1

    return {
        "passed": growth_count >= params.effective_min_growth_count,
        "available_years": len(rows),
        "growth_count": growth_count,
        "latest_fiscal_year": rows[-1].fiscal_year_label,
    }


def _detect_cup_handle_pattern(
    candles: list[MarketDataDaily] | list[_PatternCandle],
    params: CupHandleParams | None = None,
) -> dict[str, object] | None:
    """Detect a recent O'Neil cup-with-handle pattern.

    Returns a dict describing the pattern, or None if no pattern ends within
    the supplied candle window. Only the most recent valid pattern is returned.
    """
    params = _normalize_cup_handle_params(params)
    min_required = max(
        params.min_cup_duration + params.min_handle_duration,
        params.min_total_duration,
    )
    usable_candles: list[MarketDataDaily] | list[_PatternCandle] = []
    closes: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    volumes: list[int | None] = []
    for candle in candles:
        if not is_complete_market_row(candle):
            continue
        adjusted = adjusted_ohlc(candle)
        if adjusted.close is None:
            continue
        if closes and has_extreme_price_gap(closes[-1], adjusted.close):
            usable_candles = []
            closes = []
            highs = []
            lows = []
            volumes = []
        close = float(adjusted.close)
        usable_candles.append(candle)
        closes.append(close)
        highs.append(float(adjusted.high) if adjusted.high is not None else close)
        lows.append(float(adjusted.low) if adjusted.low is not None else close)
        volumes.append(getattr(candle, "volume", None))

    if len(usable_candles) < min_required + 1:
        return None

    n = len(usable_candles)
    breakout_search_start = max(n - params.breakout_lookback_days, 0)
    right_lip_indices = {
        breakout_idx - handle_len
        for breakout_idx in range(n - 1, breakout_search_start - 1, -1)
        for handle_len in range(params.min_handle_duration, params.max_handle_duration + 1)
        if breakout_idx - handle_len >= params.min_cup_duration
    }
    cup_candidates = _find_cup_candidates_by_right_lip(
        highs,
        lows,
        closes,
        right_lip_indices,
        params,
    )
    for breakout_idx in range(n - 1, breakout_search_start - 1, -1):
        result = _try_pattern_ending_at(
            usable_candles,
            highs,
            lows,
            closes,
            volumes,
            cup_candidates,
            params,
            breakout_idx,
        )
        if result is not None:
            return result
    return None


def _has_prior_uptrend(
    closes: list[float],
    *,
    left_lip_idx: int,
    left_lip_high: float,
    params: CupHandleParams,
) -> bool:
    if not params.require_prior_uptrend:
        return True
    start_idx = max(0, left_lip_idx - params.prior_uptrend_lookback_days)
    prior_closes = [value for value in closes[start_idx:left_lip_idx] if value > 0]
    if not prior_closes or left_lip_high <= 0:
        return False
    prior_low = min(prior_closes)
    prior_uptrend_pct = ((left_lip_high / prior_low) - 1) * 100
    return prior_uptrend_pct >= float(params.min_prior_uptrend_pct)


def _prior_uptrend_pct(
    closes: list[float],
    *,
    left_lip_idx: int,
    left_lip_high: float,
    params: CupHandleParams,
) -> float | None:
    start_idx = max(0, left_lip_idx - params.prior_uptrend_lookback_days)
    prior_closes = [value for value in closes[start_idx:left_lip_idx] if value > 0]
    if not prior_closes or left_lip_high <= 0:
        return None
    return ((left_lip_high / min(prior_closes)) - 1) * 100


def _has_rounded_bottom(
    lows: list[float],
    *,
    left_lip_idx: int,
    right_lip_idx: int,
    cup_bottom_low: float,
    cup_depth_abs: float,
    params: CupHandleParams,
) -> bool:
    if cup_depth_abs <= 0:
        return False
    cup_len = right_lip_idx - left_lip_idx
    bottom_threshold = cup_bottom_low + (cup_depth_abs * float(params.bottom_zone_pct) / 100)
    bottom_zone_indices = [
        idx
        for idx in range(left_lip_idx, right_lip_idx + 1)
        if lows[idx] <= bottom_threshold
    ]
    if len(bottom_zone_indices) < params.min_bottom_dwell_days:
        return False
    bottom_span = bottom_zone_indices[-1] - bottom_zone_indices[0] + 1
    return bottom_span >= max(1, int(cup_len * float(params.min_bottom_span_pct) / 100))


def _find_cup_candidates_by_right_lip(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    right_lip_indices: set[int],
    params: CupHandleParams,
) -> dict[int, list[_CupCandidate]]:
    params = _normalize_cup_handle_params(params)
    candidates: dict[int, list[_CupCandidate]] = defaultdict(list)
    n = len(highs)
    min_depth_pct = float(params.min_cup_depth_pct)
    max_depth_pct = float(params.max_cup_depth_pct)
    max_lip_delta_pct = float(params.max_right_lip_delta_pct)
    for right_lip_idx in sorted(right_lip_indices):
        if right_lip_idx >= n:
            continue
        right_lip_high = highs[right_lip_idx]
        if right_lip_high <= 0:
            continue

        min_low = float("inf")
        min_low_idx = right_lip_idx
        interior_high = float("-inf")
        min_left_idx = max(0, right_lip_idx - params.max_cup_duration)
        for left_lip_idx in range(right_lip_idx, min_left_idx - 1, -1):
            low = lows[left_lip_idx]
            if low < min_low:
                min_low = low
                min_low_idx = left_lip_idx

            if left_lip_idx + 1 <= right_lip_idx - 1:
                interior_high = max(interior_high, highs[left_lip_idx + 1])

            cup_len = right_lip_idx - left_lip_idx
            if cup_len < params.min_cup_duration:
                continue

            left_lip_high = highs[left_lip_idx]
            if left_lip_high <= 0:
                continue

            cup_bottom_offset = min_low_idx - left_lip_idx
            min_side_duration = max(5, int(cup_len * float(params.min_cup_side_duration_pct) / 100))
            if (
                cup_bottom_offset < min_side_duration
                or (cup_len - cup_bottom_offset) < min_side_duration
            ):
                continue

            cup_depth_pct = ((left_lip_high - min_low) / left_lip_high) * 100
            if cup_depth_pct < min_depth_pct or cup_depth_pct > max_depth_pct:
                continue

            if not _has_prior_uptrend(
                closes,
                left_lip_idx=left_lip_idx,
                left_lip_high=left_lip_high,
                params=params,
            ):
                continue
            if not _has_rounded_bottom(
                lows,
                left_lip_idx=left_lip_idx,
                right_lip_idx=right_lip_idx,
                cup_bottom_low=min_low,
                cup_depth_abs=left_lip_high - min_low,
                params=params,
            ):
                continue

            lip_delta_pct = abs((left_lip_high - right_lip_high) / left_lip_high) * 100
            if lip_delta_pct > max_lip_delta_pct:
                continue

            lip_baseline = max(left_lip_high, right_lip_high)
            if interior_high > lip_baseline * 1.02:
                continue

            candidates[right_lip_idx].append(
                _CupCandidate(
                    left_lip_idx=left_lip_idx,
                    cup_bottom_idx=min_low_idx,
                    cup_depth_pct=cup_depth_pct,
                )
            )

    return candidates


def _try_pattern_ending_at(
    candles: list[MarketDataDaily] | list[_PatternCandle],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[int | None],
    cup_candidates: dict[int, list[_CupCandidate]],
    params: CupHandleParams,
    breakout_idx: int,
) -> dict[str, object] | None:
    min_handle_pullback_pct = float(params.min_handle_pullback_pct)
    max_handle_pullback_pct = float(params.max_handle_pullback_pct)
    # Handle window: configured min..max duration before breakout_idx.
    for handle_len in range(params.min_handle_duration, params.max_handle_duration + 1):
        right_lip_idx = breakout_idx - handle_len
        if right_lip_idx < params.min_cup_duration:
            break
        right_lip_candidates = cup_candidates.get(right_lip_idx)
        if not right_lip_candidates:
            continue

        right_lip_high = highs[right_lip_idx]
        if right_lip_high <= 0:
            continue

        handle_segment_lows = lows[right_lip_idx + 1 : breakout_idx]
        if not handle_segment_lows:
            continue
        handle_segment_highs = highs[right_lip_idx + 1 : breakout_idx]
        handle_high = max(handle_segment_highs) if handle_segment_highs else right_lip_high
        if handle_high > right_lip_high * (
            1 + float(params.max_handle_high_above_lip_pct) / 100
        ):
            continue

        # Breakout should clear both the cup lip and the handle's local resistance.
        breakout_resistance = max(right_lip_high, handle_high)
        if closes[breakout_idx] <= breakout_resistance:
            continue
        breakout_volume_ratio = _breakout_volume_ratio(
            volumes,
            breakout_idx=breakout_idx,
            avg_days=params.breakout_volume_avg_days,
        )
        if params.require_breakout_volume and (
            breakout_volume_ratio is None
            or breakout_volume_ratio < float(params.min_breakout_volume_multiplier)
        ):
            continue

        handle_low = min(handle_segment_lows)
        handle_low_offset = handle_segment_lows.index(handle_low)
        handle_pullback_pct = ((right_lip_high - handle_low) / right_lip_high) * 100
        if (
            handle_pullback_pct < min_handle_pullback_pct
            or handle_pullback_pct > max_handle_pullback_pct
        ):
            continue

        for cup_candidate in right_lip_candidates:
            cup_duration = right_lip_idx - cup_candidate.left_lip_idx
            total_duration = breakout_idx - cup_candidate.left_lip_idx
            if (
                total_duration < params.min_total_duration
                or total_duration > params.max_total_duration
            ):
                continue
            cup_bottom_low = lows[cup_candidate.cup_bottom_idx]
            cup_depth_abs = highs[cup_candidate.left_lip_idx] - cup_bottom_low
            if cup_depth_abs <= 0:
                continue
            handle_position_pct = ((handle_low - cup_bottom_low) / cup_depth_abs) * 100
            if handle_position_pct < float(params.min_handle_low_position_pct):
                continue
            handle_depth_to_cup_depth_pct = (
                (right_lip_high - handle_low) / cup_depth_abs
            ) * 100
            if handle_depth_to_cup_depth_pct > float(
                params.max_handle_depth_to_cup_depth_pct
            ):
                continue
            prior_uptrend_pct = _prior_uptrend_pct(
                closes,
                left_lip_idx=cup_candidate.left_lip_idx,
                left_lip_high=highs[cup_candidate.left_lip_idx],
                params=params,
            )

            return {
                "left_lip_date": candles[cup_candidate.left_lip_idx].trade_date,
                "cup_bottom_date": candles[cup_candidate.cup_bottom_idx].trade_date,
                "right_lip_date": candles[right_lip_idx].trade_date,
                "handle_low_date": candles[right_lip_idx + 1 + handle_low_offset].trade_date,
                "breakout_date": candles[breakout_idx].trade_date,
                "cup_depth_pct": Decimal(str(cup_candidate.cup_depth_pct)).quantize(
                    Decimal("0.01")
                ),
                "handle_depth_pct": Decimal(str(handle_pullback_pct)).quantize(
                    Decimal("0.01")
                ),
                "cup_duration": cup_duration,
                "handle_duration": handle_len,
                "total_duration": total_duration,
                "handle_position_pct": Decimal(str(handle_position_pct)).quantize(
                    Decimal("0.01")
                ),
                "handle_depth_to_cup_depth_pct": Decimal(
                    str(handle_depth_to_cup_depth_pct)
                ).quantize(Decimal("0.01")),
                "prior_uptrend_pct": (
                    Decimal(str(prior_uptrend_pct)).quantize(Decimal("0.01"))
                    if prior_uptrend_pct is not None
                    else None
                ),
                "breakout_volume_ratio": (
                    Decimal(str(breakout_volume_ratio)).quantize(Decimal("0.01"))
                    if breakout_volume_ratio is not None
                    else None
                ),
            }
    return None


def _breakout_volume_ratio(
    volumes: list[int | None],
    *,
    breakout_idx: int,
    avg_days: int,
) -> float | None:
    breakout_volume = volumes[breakout_idx]
    if breakout_volume is None or breakout_volume <= 0:
        return None
    start_idx = max(0, breakout_idx - avg_days)
    prior_volumes = [
        volume
        for volume in volumes[start_idx:breakout_idx]
        if volume is not None and volume > 0
    ]
    if not prior_volumes:
        return None
    return breakout_volume / (sum(prior_volumes) / len(prior_volumes))


def _to_float(value) -> float | None:
    if value is None:
        return None
    return float(value)


def _format_decimal(value, pattern: str) -> str | None:
    if value is None:
        return None
    return f"{value:.2f}" if pattern == "0.01" else str(value)
