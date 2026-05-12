from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from decimal import Decimal
from math import ceil
from pathlib import Path

from sqlalchemy import case, distinct, func, literal, select

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
from stockanalyse_api.services.cup_handle_materialization import (
    filter_materialized_cup_handle_event_pool,
    load_materialized_cup_handle_event_pool,
    load_materialized_cup_handle_matches,
    select_latest_materialized_cup_handle_matches,
)

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
CUP_HANDLE_PRIOR_UPTREND_FEATURE_WINDOWS = (60, 90, 120, 180)
CUP_HANDLE_BREAKOUT_VOLUME_FEATURE_WINDOWS = (20, 50, 60)
CUP_HANDLE_BOTTOM_FEATURE_ZONES = (20, 35)


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
    max_pe: Decimal | None = None
    max_pb: Decimal | None = None
    require_positive_operating_cash_flow: bool = False
    require_positive_free_cash_flow: bool = False
    min_operating_cash_flow_growth_count: int | None = None
    min_operating_cash_flow_yoy_growth_pct: Decimal = Decimal("0")

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


@dataclass(frozen=True, slots=True)
class _FundamentalAnnualSnapshot:
    fiscal_year_end_date: date
    fiscal_year_label: str
    net_income: Decimal | None
    net_income_currency: str = "USD"
    source_as_of_date: date | None = None
    operating_cash_flow: Decimal | None = None
    free_cash_flow: Decimal | None = None
    diluted_eps: Decimal | None = None
    stockholders_equity: Decimal | None = None
    weighted_average_diluted_shares: Decimal | None = None
    pe: Decimal | None = None
    pb: Decimal | None = None


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
    fundamental_growth_status: str
    fundamental_growth_years: int | None
    fundamental_growth_count: int | None
    fundamental_growth_latest_year: str | None
    fundamental_growth_latest_yoy_pct: str | None
    fundamental_operating_cash_flow_latest_yoy_pct: str | None

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


def _screen_candidate_cache_key(
    *,
    market: str,
    trade_date: date,
    use_rps: bool,
    rps_threshold: int,
    selected_windows: list[int],
    min_rps_windows_passing: int,
) -> tuple[object, ...]:
    return (
        "screen_candidates",
        market,
        trade_date.isoformat(),
        bool(use_rps),
        int(rps_threshold),
        tuple(selected_windows),
        int(min_rps_windows_passing),
    )


def _screen_broad_candidate_cache_key(
    *,
    market: str,
    trade_date: date,
) -> tuple[object, ...]:
    return (
        "screen_broad_candidates",
        market,
        trade_date.isoformat(),
    )


def _broad_candidate_cache_date_count(
    candidate_cache: dict[tuple[object, ...], dict[str, object]],
    *,
    market: str,
) -> int:
    return sum(
        1
        for key in candidate_cache
        if (
            isinstance(key, tuple)
            and len(key) >= 2
            and key[0] == "screen_broad_candidates"
            and key[1] == market
        )
    )


def _rps_pass_expression(
    *,
    threshold: Decimal,
    selected_windows: list[int],
):
    rps_pass_expression = literal(0)
    for window in selected_windows:
        rps_column = getattr(DerivedIndicatorDaily, f"rps_{window}")
        rps_pass_expression = rps_pass_expression + case(
            (rps_column >= threshold, 1),
            else_=0,
        )
    return rps_pass_expression


def _screen_candidate_payload(
    *,
    instrument_id: int,
    symbol: str,
    exchange: str,
    name: str | None,
    rps_50,
    rps_120,
    rps_250,
    use_rps: bool,
    threshold_decimal: Decimal,
    selected_windows: list[int],
    min_rps_windows_passing: int,
) -> dict[str, object]:
    rps_value_by_window = {
        50: rps_50,
        120: rps_120,
        250: rps_250,
    }

    rps_pass_count = sum(
        1
        for window in selected_windows
        if rps_value_by_window[window] is not None
        and rps_value_by_window[window] >= threshold_decimal
    )
    rps_passed = rps_pass_count >= min_rps_windows_passing if use_rps else True

    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": exchange,
        "name": name,
        "rps_50": rps_50,
        "rps_120": rps_120,
        "rps_250": rps_250,
        "rps_passed": rps_passed,
        "rps_pass_count": rps_pass_count,
    }


def _candidate_with_rps_evaluation(
    candidate: dict[str, object],
    *,
    use_rps: bool,
    threshold_decimal: Decimal,
    selected_windows: list[int],
    min_rps_windows_passing: int,
) -> dict[str, object]:
    return _screen_candidate_payload(
        instrument_id=int(candidate["instrument_id"]),
        symbol=str(candidate["symbol"]),
        exchange=str(candidate["exchange"]),
        name=candidate.get("name"),  # type: ignore[arg-type]
        rps_50=candidate.get("rps_50"),
        rps_120=candidate.get("rps_120"),
        rps_250=candidate.get("rps_250"),
        use_rps=use_rps,
        threshold_decimal=threshold_decimal,
        selected_windows=selected_windows,
        min_rps_windows_passing=min_rps_windows_passing,
    )


def _filter_broad_screen_candidates(
    candidates: list[dict[str, object]],
    *,
    use_rps: bool,
    threshold_decimal: Decimal,
    selected_windows: list[int],
    min_rps_windows_passing: int,
) -> list[dict[str, object]]:
    filtered: list[dict[str, object]] = []
    for candidate in candidates:
        evaluated = _candidate_with_rps_evaluation(
            candidate,
            use_rps=use_rps,
            threshold_decimal=threshold_decimal,
            selected_windows=selected_windows,
            min_rps_windows_passing=min_rps_windows_passing,
        )
        if use_rps and not evaluated["rps_passed"]:
            continue
        filtered.append(evaluated)
    return filtered


def _candidate_rps_score(
    candidate: dict[str, object],
    selected_windows: list[int],
) -> Decimal | None:
    values: list[Decimal] = []
    for window in selected_windows:
        raw_value = candidate.get(f"rps_{window}")
        if raw_value is None:
            continue
        values.append(Decimal(str(raw_value)))
    return max(values) if values else None


def preload_screen_candidate_cache(
    session,
    *,
    market: str | None,
    trade_dates: list[date],
    use_rps: bool,
    rps_threshold: int,
    selected_rps_windows: list[int],
    min_rps_windows_passing: int = 1,
    candidate_cache: dict[tuple[object, ...], dict[str, object]] | None,
    prefer_broad_candidate_cache: bool = False,
    max_broad_candidate_cache_dates: int | None = None,
) -> None:
    if candidate_cache is None or not trade_dates:
        return
    resolved_market = normalize_market(market)
    selected_windows = _normalize_rps_windows(selected_rps_windows)
    resolved_min_rps_passing = _normalize_min_rps_windows_passing(
        min_rps_windows_passing,
        selected_windows,
    )
    if use_rps and not selected_windows:
        return

    if prefer_broad_candidate_cache:
        _preload_broad_screen_candidate_cache(
            session,
            market=resolved_market,
            trade_dates=trade_dates,
            candidate_cache=candidate_cache,
            max_broad_candidate_cache_dates=max_broad_candidate_cache_dates,
        )
        return

    missing_dates = [
        trade_date
        for trade_date in trade_dates
        if _screen_candidate_cache_key(
            market=resolved_market,
            trade_date=trade_date,
            use_rps=use_rps,
            rps_threshold=rps_threshold,
            selected_windows=selected_windows,
            min_rps_windows_passing=resolved_min_rps_passing,
        )
        not in candidate_cache
    ]
    if not missing_dates:
        return

    exchanges = _market_exchanges(resolved_market)
    start_date = min(missing_dates)
    end_date = max(missing_dates)
    base_filters = (
        DerivedIndicatorDaily.trade_date >= start_date,
        DerivedIndicatorDaily.trade_date <= end_date,
        Instrument.exchange.in_(exchanges),
    )
    counts_by_date = {
        row.trade_date: int(row.total_evaluated)
        for row in session.execute(
            select(
                DerivedIndicatorDaily.trade_date,
                func.count(DerivedIndicatorDaily.instrument_id).label("total_evaluated"),
            )
            .join(Instrument, Instrument.id == DerivedIndicatorDaily.instrument_id)
            .where(*base_filters)
            .group_by(DerivedIndicatorDaily.trade_date)
        ).all()
    }

    query = (
        select(
            DerivedIndicatorDaily.trade_date,
            DerivedIndicatorDaily.instrument_id,
            DerivedIndicatorDaily.rps_50,
            DerivedIndicatorDaily.rps_120,
            DerivedIndicatorDaily.rps_250,
            Instrument.symbol,
            Instrument.exchange,
            Instrument.name,
        )
        .join(Instrument, Instrument.id == DerivedIndicatorDaily.instrument_id)
        .where(*base_filters)
        .order_by(DerivedIndicatorDaily.trade_date.asc(), Instrument.symbol.asc())
    )

    threshold_decimal = Decimal(rps_threshold)
    if use_rps:
        query = query.where(
            _rps_pass_expression(
                threshold=threshold_decimal,
                selected_windows=selected_windows,
            )
            >= resolved_min_rps_passing
        )

    candidates_by_date: dict[date, list[dict[str, object]]] = defaultdict(list)
    for row in session.execute(query):
        candidates_by_date[row.trade_date].append(
            _screen_candidate_payload(
                instrument_id=row.instrument_id,
                symbol=row.symbol,
                exchange=row.exchange,
                name=row.name,
                rps_50=row.rps_50,
                rps_120=row.rps_120,
                rps_250=row.rps_250,
                use_rps=use_rps,
                threshold_decimal=threshold_decimal,
                selected_windows=selected_windows,
                min_rps_windows_passing=resolved_min_rps_passing,
            )
        )

    for trade_date in missing_dates:
        cache_key = _screen_candidate_cache_key(
            market=resolved_market,
            trade_date=trade_date,
            use_rps=use_rps,
            rps_threshold=rps_threshold,
            selected_windows=selected_windows,
            min_rps_windows_passing=resolved_min_rps_passing,
        )
        candidate_cache[cache_key] = {
            "total_evaluated": counts_by_date.get(trade_date, 0),
            "candidates": candidates_by_date.get(trade_date, []),
        }


def _preload_broad_screen_candidate_cache(
    session,
    *,
    market: str,
    trade_dates: list[date],
    candidate_cache: dict[tuple[object, ...], dict[str, object]] | None,
    max_broad_candidate_cache_dates: int | None = None,
) -> None:
    if candidate_cache is None or not trade_dates:
        return
    missing_dates = [
        trade_date
        for trade_date in trade_dates
        if _screen_broad_candidate_cache_key(market=market, trade_date=trade_date)
        not in candidate_cache
    ]
    if not missing_dates:
        return
    if max_broad_candidate_cache_dates is not None:
        remaining = max_broad_candidate_cache_dates - _broad_candidate_cache_date_count(
            candidate_cache,
            market=market,
        )
        if remaining <= 0:
            return
        missing_dates = missing_dates[:remaining]

    exchanges = _market_exchanges(market)
    start_date = min(missing_dates)
    end_date = max(missing_dates)
    rows = session.execute(
        select(
            DerivedIndicatorDaily.trade_date,
            DerivedIndicatorDaily.instrument_id,
            DerivedIndicatorDaily.rps_50,
            DerivedIndicatorDaily.rps_120,
            DerivedIndicatorDaily.rps_250,
            Instrument.symbol,
            Instrument.exchange,
            Instrument.name,
        )
        .join(Instrument, Instrument.id == DerivedIndicatorDaily.instrument_id)
        .where(
            DerivedIndicatorDaily.trade_date >= start_date,
            DerivedIndicatorDaily.trade_date <= end_date,
            Instrument.exchange.in_(exchanges),
        )
        .order_by(DerivedIndicatorDaily.trade_date.asc(), Instrument.symbol.asc())
    )
    candidates_by_date: dict[date, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        candidates_by_date[row.trade_date].append(
            _screen_candidate_payload(
                instrument_id=row.instrument_id,
                symbol=row.symbol,
                exchange=row.exchange,
                name=row.name,
                rps_50=row.rps_50,
                rps_120=row.rps_120,
                rps_250=row.rps_250,
                use_rps=False,
                threshold_decimal=Decimal("0"),
                selected_windows=[],
                min_rps_windows_passing=1,
            )
        )

    for trade_date in missing_dates:
        candidates = candidates_by_date.get(trade_date, [])
        candidate_cache[
            _screen_broad_candidate_cache_key(market=market, trade_date=trade_date)
        ] = {
            "total_evaluated": len(candidates),
            "candidates": candidates,
        }


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
    if params.max_pe is not None and params.max_pe <= 0:
        raise ValueError("fundamental max_pe must be greater than 0 when provided.")
    if params.max_pb is not None and params.max_pb <= 0:
        raise ValueError("fundamental max_pb must be greater than 0 when provided.")
    if params.min_operating_cash_flow_yoy_growth_pct < Decimal("-100"):
        raise ValueError(
            "fundamental min_operating_cash_flow_yoy_growth_pct must be greater than or equal to -100."
        )
    if params.min_operating_cash_flow_growth_count is not None:
        if params.min_operating_cash_flow_growth_count < 1:
            raise ValueError(
                "fundamental min_operating_cash_flow_growth_count must be greater than or equal to 1."
            )
        if params.min_operating_cash_flow_growth_count > params.min_years - 1:
            raise ValueError(
                "fundamental min_operating_cash_flow_growth_count cannot exceed min_years - 1."
            )
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
    candidate_cache: dict[tuple[object, ...], dict[str, object]] | None = None,
    fundamental_growth_cache: dict[tuple[object, ...], object] | None = None,
    cup_event_cache: dict[tuple[object, ...], dict[int, list[object]] | None] | None = None,
    cup_event_cache_start_date: date | None = None,
    cup_event_cache_end_date: date | None = None,
    max_hits: int | None = None,
    exclude_symbols: set[str] | frozenset[str] | None = None,
    prefer_broad_candidate_cache: bool = False,
    max_broad_candidate_cache_dates: int | None = None,
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

    candidate_cache_key = _screen_candidate_cache_key(
        market=resolved_market,
        trade_date=target_date,
        use_rps=use_rps,
        rps_threshold=rps_threshold,
        selected_windows=selected_windows,
        min_rps_windows_passing=resolved_min_rps_passing,
    )
    cached_candidates = None
    threshold_decimal = Decimal(rps_threshold)
    if candidate_cache is not None and prefer_broad_candidate_cache:
        broad_candidate_cache_key = _screen_broad_candidate_cache_key(
            market=resolved_market,
            trade_date=target_date,
        )
        broad_candidates = candidate_cache.get(broad_candidate_cache_key)
        if broad_candidates is None:
            _preload_broad_screen_candidate_cache(
                session,
                market=resolved_market,
                trade_dates=[target_date],
                candidate_cache=candidate_cache,
                max_broad_candidate_cache_dates=max_broad_candidate_cache_dates,
            )
            broad_candidates = candidate_cache.get(broad_candidate_cache_key)
        if broad_candidates is not None:
            cached_candidates = {
                "total_evaluated": broad_candidates["total_evaluated"],
                "candidates": _filter_broad_screen_candidates(
                    list(broad_candidates["candidates"]),  # type: ignore[arg-type]
                    use_rps=use_rps,
                    threshold_decimal=threshold_decimal,
                    selected_windows=selected_windows,
                    min_rps_windows_passing=resolved_min_rps_passing,
                ),
            }
    if cached_candidates is None:
        cached_candidates = (
            candidate_cache.get(candidate_cache_key) if candidate_cache is not None else None
        )
    if cached_candidates is None:
        base_filters = (
            DerivedIndicatorDaily.trade_date == target_date,
            Instrument.exchange.in_(exchanges),
        )
        total_evaluated = session.execute(
            select(func.count(DerivedIndicatorDaily.instrument_id))
            .join(Instrument, Instrument.id == DerivedIndicatorDaily.instrument_id)
            .where(*base_filters)
        ).scalar_one()
        query = (
            select(DerivedIndicatorDaily, Instrument)
            .join(Instrument, Instrument.id == DerivedIndicatorDaily.instrument_id)
            .where(*base_filters)
            .order_by(Instrument.symbol.asc())
        )

        if use_rps:
            query = query.where(
                _rps_pass_expression(
                    threshold=threshold_decimal,
                    selected_windows=selected_windows,
                )
                >= resolved_min_rps_passing
            )

        rows = session.execute(query).all()
        candidates: list[dict[str, object]] = []

        for indicator_row, instrument in rows:
            candidates.append(
                _screen_candidate_payload(
                    instrument_id=instrument.id,
                    symbol=instrument.symbol,
                    exchange=instrument.exchange,
                    name=instrument.name,
                    rps_50=indicator_row.rps_50,
                    rps_120=indicator_row.rps_120,
                    rps_250=indicator_row.rps_250,
                    use_rps=use_rps,
                    threshold_decimal=threshold_decimal,
                    selected_windows=selected_windows,
                    min_rps_windows_passing=resolved_min_rps_passing,
                )
            )
        cached_candidates = {
            "total_evaluated": total_evaluated,
            "candidates": candidates,
        }
        if candidate_cache is not None:
            candidate_cache[candidate_cache_key] = cached_candidates

    total_evaluated = int(cached_candidates["total_evaluated"])
    candidates = list(cached_candidates["candidates"])  # type: ignore[arg-type]
    excluded_symbols = set(exclude_symbols or ())
    if excluded_symbols:
        candidates = [
            candidate
            for candidate in candidates
            if str(candidate["symbol"]) not in excluded_symbols
        ]
    if use_rps:
        candidates.sort(
            key=lambda candidate: (
                -(_candidate_rps_score(candidate, selected_windows) or Decimal("-1")),
                str(candidate["symbol"]),
            )
        )
    else:
        candidates.sort(key=lambda candidate: str(candidate["symbol"]))
    candidate_instrument_ids = [
        int(candidate["instrument_id"]) for candidate in candidates
    ]
    hit_limit = max_hits if max_hits is not None and max_hits > 0 else None
    limited_hit_scan = hit_limit is not None

    cup_handle_source = "disabled"
    materialized_cup_events = None
    candles_by_instrument: dict[int, list[_PatternCandle]] = {}
    if use_cup_handle and candidates:
        if (
            cup_event_cache is not None
            and cup_event_cache_start_date is not None
            and cup_event_cache_end_date is not None
        ):
            cup_event_cache_key = (
                "materialized_cup_event_pool",
                resolved_market,
                cup_event_cache_start_date.isoformat(),
                cup_event_cache_end_date.isoformat(),
                int(resolved_cup_params.breakout_lookback_days),
            )
            cached_event_pool = cup_event_cache.get(cup_event_cache_key, ...)
            if cached_event_pool is ...:
                cached_event_pool = load_materialized_cup_handle_event_pool(
                    session,
                    market=resolved_market,
                    start_date=cup_event_cache_start_date,
                    end_date=cup_event_cache_end_date,
                    params=resolved_cup_params,
                )
                cup_event_cache[cup_event_cache_key] = cached_event_pool
            filtered_event_pool = cached_event_pool
            if cached_event_pool is not None:
                filtered_cup_event_cache_key = (
                    "filtered_materialized_cup_event_pool",
                    resolved_market,
                    cup_event_cache_start_date.isoformat(),
                    cup_event_cache_end_date.isoformat(),
                    int(resolved_cup_params.breakout_lookback_days),
                    tuple(
                        (key, str(value))
                        for key, value in resolved_cup_params.to_dict().items()
                    ),
                )
                cached_filtered_event_pool = cup_event_cache.get(
                    filtered_cup_event_cache_key,
                    ...,
                )
                if cached_filtered_event_pool is ...:
                    cached_filtered_event_pool = filter_materialized_cup_handle_event_pool(
                        cached_event_pool,  # type: ignore[arg-type]
                        params=resolved_cup_params,
                    )
                    cup_event_cache[filtered_cup_event_cache_key] = cached_filtered_event_pool
                filtered_event_pool = cached_filtered_event_pool
            materialized_cup_events = (
                select_latest_materialized_cup_handle_matches(
                    filtered_event_pool,  # type: ignore[arg-type]
                    signal_date=target_date,
                    instrument_ids=candidate_instrument_ids,
                    params=resolved_cup_params,
                    assume_params_matched=True,
                )
                if filtered_event_pool is not None
                else None
            )
        else:
            materialized_cup_events = load_materialized_cup_handle_matches(
                session,
                market=resolved_market,
                signal_date=target_date,
                instrument_ids=candidate_instrument_ids,
                params=resolved_cup_params,
            )
        if materialized_cup_events is None:
            cup_handle_source = "runtime_scan"
            candles_by_instrument = _load_candles_by_instrument(
                session,
                instrument_ids=candidate_instrument_ids,
                cutoff=target_date,
                limit=resolved_cup_params.effective_lookback_days,
            )
        else:
            cup_handle_source = "materialized"

    fundamental_rows_by_instrument: dict[int, list[_FundamentalAnnualSnapshot]] = {}
    fundamental_market_price_by_instrument: dict[int, Decimal] = {}
    if (
        resolved_fundamental_params.enabled
        and candidate_instrument_ids
        and fundamental_growth_cache is not None
        and not limited_hit_scan
    ):
        fundamental_rows_by_instrument = _load_fundamental_rows_by_instrument(
            session,
            instrument_ids=candidate_instrument_ids,
            cache=fundamental_growth_cache,
        )
    if (
        resolved_fundamental_params.enabled
        and candidate_instrument_ids
        and (
            resolved_fundamental_params.max_pe is not None
            or resolved_fundamental_params.max_pb is not None
        )
        and not limited_hit_scan
    ):
        fundamental_market_price_by_instrument = _load_market_close_by_instrument(
            session,
            instrument_ids=candidate_instrument_ids,
            trade_date=target_date,
            cache=fundamental_growth_cache,
        )

    def fundamental_rows_for(instrument_id: int) -> list[_FundamentalAnnualSnapshot] | None:
        if instrument_id in fundamental_rows_by_instrument:
            return fundamental_rows_by_instrument[instrument_id]
        if fundamental_growth_cache is None:
            return None
        rows_by_id = _load_fundamental_rows_by_instrument(
            session,
            instrument_ids=[instrument_id],
            cache=fundamental_growth_cache,
        )
        return rows_by_id.get(instrument_id, [])

    def market_price_for(instrument_id: int) -> Decimal | None:
        if (
            resolved_fundamental_params.max_pe is None
            and resolved_fundamental_params.max_pb is None
        ):
            return None
        if instrument_id in fundamental_market_price_by_instrument:
            return fundamental_market_price_by_instrument[instrument_id]
        loaded = _load_market_close_by_instrument(
            session,
            instrument_ids=[instrument_id],
            trade_date=target_date,
            cache=fundamental_growth_cache,
        )
        return loaded.get(instrument_id)

    hits: list[ScreenHit] = []
    fundamental_status_counts: dict[str, int] = defaultdict(int)
    for candidate in candidates:
        instrument_id = int(candidate["instrument_id"])
        cup_breakout_date: date | None = None
        if use_cup_handle:
            if materialized_cup_events is not None:
                cup_event = materialized_cup_events.get(instrument_id)
                if cup_event is None:
                    continue
                cup_breakout_date = cup_event.breakout_date
            else:
                candles = candles_by_instrument.get(instrument_id, [])
                cup_pattern = _detect_cup_handle_pattern(candles, resolved_cup_params)
                if cup_pattern is None:
                    continue
                cup_breakout_date = cup_pattern["breakout_date"]
            cup_handle_passed = True
        else:
            cup_handle_passed = False

        fundamental_cache_key = (
            "fundamental_growth",
            instrument_id,
            target_date.isoformat(),
            bool(resolved_fundamental_params.enabled),
            int(resolved_fundamental_params.min_years),
            int(resolved_fundamental_params.effective_min_growth_count),
            str(resolved_fundamental_params.min_yoy_growth_pct),
            bool(resolved_fundamental_params.require_positive_net_income),
            int(resolved_fundamental_params.reporting_lag_days),
            str(resolved_fundamental_params.max_pe),
            str(resolved_fundamental_params.max_pb),
            bool(resolved_fundamental_params.require_positive_operating_cash_flow),
            bool(resolved_fundamental_params.require_positive_free_cash_flow),
            (
                int(resolved_fundamental_params.min_operating_cash_flow_growth_count)
                if resolved_fundamental_params.min_operating_cash_flow_growth_count is not None
                else None
            ),
            str(resolved_fundamental_params.min_operating_cash_flow_yoy_growth_pct),
        )
        cached_fundamental_meta = (
            fundamental_growth_cache.get(fundamental_cache_key)
            if fundamental_growth_cache is not None
            else None
        )
        fundamental_meta = (
            cached_fundamental_meta if isinstance(cached_fundamental_meta, dict) else None
        )
        if fundamental_meta is None:
            if not resolved_fundamental_params.enabled:
                fundamental_meta = _evaluate_fundamental_growth_from_rows(
                    [],
                    signal_date=target_date,
                    params=resolved_fundamental_params,
                )
            else:
                market_price = market_price_for(instrument_id)
                fundamental_rows = fundamental_rows_for(instrument_id)
                if fundamental_rows is not None:
                    fundamental_meta = _evaluate_fundamental_growth_from_rows(
                        fundamental_rows,
                        signal_date=target_date,
                        params=resolved_fundamental_params,
                        market_price=market_price,
                    )
                else:
                    fundamental_meta = _evaluate_fundamental_growth(
                        session,
                        instrument_id=instrument_id,
                        signal_date=target_date,
                        params=resolved_fundamental_params,
                        market_price=market_price,
                    )
            if fundamental_growth_cache is not None:
                fundamental_growth_cache[fundamental_cache_key] = fundamental_meta
        fundamental_status_counts[str(fundamental_meta["status"])] += 1
        if resolved_fundamental_params.enabled and not fundamental_meta["passed"]:
            continue

        hits.append(
            ScreenHit(
                instrument_id=instrument_id,
                symbol=str(candidate["symbol"]),
                exchange=str(candidate["exchange"]),
                name=candidate["name"],  # type: ignore[arg-type]
                trade_date=target_date.isoformat(),
                rps_50=_format_decimal(candidate["rps_50"], "0.01"),
                rps_120=_format_decimal(candidate["rps_120"], "0.01"),
                rps_250=_format_decimal(candidate["rps_250"], "0.01"),
                rps_passed=bool(candidate["rps_passed"]),
                rps_pass_count=int(candidate["rps_pass_count"]),
                cup_handle_passed=cup_handle_passed,
                cup_handle_breakout_date=(
                    cup_breakout_date.isoformat() if cup_breakout_date else None
                ),
                fundamental_growth_passed=bool(fundamental_meta["passed"]),
                fundamental_growth_status=str(fundamental_meta["status"]),
                fundamental_growth_years=fundamental_meta["available_years"],
                fundamental_growth_count=fundamental_meta["growth_count"],
                fundamental_growth_latest_year=fundamental_meta["latest_fiscal_year"],
                fundamental_growth_latest_yoy_pct=_format_decimal(
                    fundamental_meta.get("latest_net_income_yoy_pct"),
                    "0.01",
                ),
                fundamental_operating_cash_flow_latest_yoy_pct=_format_decimal(
                    fundamental_meta.get("latest_operating_cash_flow_yoy_pct"),
                    "0.01",
                ),
            )
        )
        if hit_limit is not None and len(hits) >= hit_limit:
            break

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
        "total_evaluated": total_evaluated,
        "diagnostics": {
            "fundamental_growth_status_counts": dict(fundamental_status_counts),
            "cup_handle_source": cup_handle_source,
        },
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


def _load_fundamental_rows_by_instrument(
    session,
    *,
    instrument_ids: list[int],
    cache: dict[tuple[object, ...], object],
    chunk_size: int = 500,
) -> dict[int, list[_FundamentalAnnualSnapshot]]:
    rows_by_instrument: dict[int, list[_FundamentalAnnualSnapshot]] = {}
    missing_ids: list[int] = []
    for instrument_id in dict.fromkeys(instrument_ids):
        cache_key = ("fundamental_growth_rows", instrument_id)
        cached_rows = cache.get(cache_key)
        if isinstance(cached_rows, list):
            rows_by_instrument[instrument_id] = cached_rows
        else:
            missing_ids.append(instrument_id)

    for offset in range(0, len(missing_ids), chunk_size):
        chunk = missing_ids[offset : offset + chunk_size]
        loaded: dict[int, list[_FundamentalAnnualSnapshot]] = {
            instrument_id: [] for instrument_id in chunk
        }
        rows = session.execute(
            select(
                FundamentalsAnnual.instrument_id,
                FundamentalsAnnual.fiscal_year_end_date,
                FundamentalsAnnual.fiscal_year_label,
                FundamentalsAnnual.net_income,
                FundamentalsAnnual.net_income_currency,
                FundamentalsAnnual.source_as_of_date,
                FundamentalsAnnual.operating_cash_flow,
                FundamentalsAnnual.free_cash_flow,
                FundamentalsAnnual.diluted_eps,
                FundamentalsAnnual.stockholders_equity,
                FundamentalsAnnual.weighted_average_diluted_shares,
                FundamentalsAnnual.pe,
                FundamentalsAnnual.pb,
            )
            .where(
                FundamentalsAnnual.instrument_id.in_(chunk),
                FundamentalsAnnual.net_income.is_not(None),
                FundamentalsAnnual.data_status != "missing",
            )
            .order_by(
                FundamentalsAnnual.instrument_id.asc(),
                FundamentalsAnnual.fiscal_year_end_date.asc(),
            )
        ).all()
        for row in rows:
            loaded[row.instrument_id].append(
                _FundamentalAnnualSnapshot(
                    fiscal_year_end_date=row.fiscal_year_end_date,
                    fiscal_year_label=row.fiscal_year_label,
                    net_income=row.net_income,
                    net_income_currency=row.net_income_currency,
                    source_as_of_date=row.source_as_of_date,
                    operating_cash_flow=row.operating_cash_flow,
                    free_cash_flow=row.free_cash_flow,
                    diluted_eps=row.diluted_eps,
                    stockholders_equity=row.stockholders_equity,
                    weighted_average_diluted_shares=row.weighted_average_diluted_shares,
                    pe=row.pe,
                    pb=row.pb,
                )
            )
        for instrument_id, snapshots in loaded.items():
            cache[("fundamental_growth_rows", instrument_id)] = snapshots
            rows_by_instrument[instrument_id] = snapshots

    return rows_by_instrument


def _load_market_close_by_instrument(
    session,
    *,
    instrument_ids: list[int],
    trade_date: date,
    cache: dict[tuple[object, ...], object] | None = None,
    chunk_size: int = 500,
) -> dict[int, Decimal]:
    prices_by_instrument: dict[int, Decimal] = {}
    missing_ids: list[int] = []
    for instrument_id in dict.fromkeys(instrument_ids):
        cache_key = ("fundamental_market_close", instrument_id, trade_date.isoformat())
        cached_price = cache.get(cache_key) if cache is not None else None
        if isinstance(cached_price, Decimal):
            prices_by_instrument[instrument_id] = cached_price
        elif cached_price is False:
            continue
        else:
            missing_ids.append(instrument_id)

    for offset in range(0, len(missing_ids), chunk_size):
        chunk = missing_ids[offset : offset + chunk_size]
        loaded = session.execute(
            select(
                MarketDataDaily.instrument_id,
                MarketDataDaily.close,
                MarketDataDaily.adj_close,
            )
            .where(
                MarketDataDaily.instrument_id.in_(chunk),
                MarketDataDaily.trade_date == trade_date,
            )
        ).all()
        loaded_by_id: dict[int, Decimal] = {}
        for row in loaded:
            price = row.close if row.close is not None else row.adj_close
            if price is not None and price > 0:
                loaded_by_id[row.instrument_id] = price
                prices_by_instrument[row.instrument_id] = price
        if cache is not None:
            for instrument_id in chunk:
                cache[("fundamental_market_close", instrument_id, trade_date.isoformat())] = (
                    loaded_by_id.get(instrument_id) or False
                )

    return prices_by_instrument


def _evaluate_fundamental_growth_from_rows(
    rows: list[_FundamentalAnnualSnapshot],
    *,
    signal_date: date,
    params: FundamentalGrowthParams,
    market_price: Decimal | None = None,
) -> dict[str, object]:
    if not params.enabled:
        return {
            "passed": True,
            "status": "not_required",
            "available_years": None,
            "growth_count": None,
            "latest_fiscal_year": None,
        }

    available_cutoff = signal_date - timedelta(days=params.reporting_lag_days)
    available_rows = [
        row
        for row in rows
        if row.fiscal_year_end_date <= available_cutoff
        and (row.source_as_of_date is None or row.source_as_of_date <= signal_date)
    ][-params.min_years :]
    return _evaluate_fundamental_growth_from_available_rows(
        available_rows,
        signal_date=signal_date,
        market_price=market_price,
        params=params,
    )


def _latest_yoy_pct(values: list[Decimal | None]) -> Decimal | None:
    if len(values) < 2:
        return None
    previous = values[-2]
    current = values[-1]
    if previous is None or current is None or previous <= 0:
        return None
    return ((current / previous) - Decimal("1")) * Decimal("100")


def _evaluate_fundamental_growth_from_available_rows(
    rows: list[_FundamentalAnnualSnapshot],
    *,
    signal_date: date,
    params: FundamentalGrowthParams,
    market_price: Decimal | None = None,
) -> dict[str, object]:
    if len(rows) < params.min_years:
        return {
            "passed": False,
            "status": "missing" if not rows else "insufficient_history",
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
            "status": "not_positive",
            "available_years": len(rows),
            "growth_count": 0,
            "latest_fiscal_year": rows[-1].fiscal_year_label,
        }

    latest_row = rows[-1]
    if params.require_positive_operating_cash_flow:
        operating_cash_flow_values = [row.operating_cash_flow for row in rows]
        if any(value is None for value in operating_cash_flow_values):
            return {
                "passed": False,
                "status": "cash_flow_missing",
                "available_years": len(rows),
                "growth_count": None,
                "latest_fiscal_year": latest_row.fiscal_year_label,
            }
        if any(value <= 0 for value in operating_cash_flow_values if value is not None):
            return {
                "passed": False,
                "status": "cash_flow_not_positive",
                "available_years": len(rows),
                "growth_count": None,
                "latest_fiscal_year": latest_row.fiscal_year_label,
            }
    if params.require_positive_free_cash_flow:
        free_cash_flow_values = [row.free_cash_flow for row in rows]
        if any(value is None for value in free_cash_flow_values):
            return {
                "passed": False,
                "status": "cash_flow_missing",
                "available_years": len(rows),
                "growth_count": None,
                "latest_fiscal_year": latest_row.fiscal_year_label,
            }
        if any(value <= 0 for value in free_cash_flow_values if value is not None):
            return {
                "passed": False,
                "status": "cash_flow_not_positive",
                "available_years": len(rows),
                "growth_count": None,
                "latest_fiscal_year": latest_row.fiscal_year_label,
            }
    if params.min_operating_cash_flow_growth_count is not None:
        operating_cash_flow_values = [row.operating_cash_flow for row in rows]
        if any(value is None for value in operating_cash_flow_values):
            return {
                "passed": False,
                "status": "cash_flow_missing",
                "available_years": len(rows),
                "growth_count": None,
                "latest_fiscal_year": latest_row.fiscal_year_label,
            }
        cash_flow_growth_multiplier = Decimal("1") + (
            params.min_operating_cash_flow_yoy_growth_pct / Decimal("100")
        )
        cash_flow_growth_count = 0
        for previous, current in zip(
            operating_cash_flow_values,
            operating_cash_flow_values[1:],
        ):
            if previous is None or current is None:
                continue
            if previous > 0:
                passed_growth = current >= previous * cash_flow_growth_multiplier
            else:
                passed_growth = current > previous
            if passed_growth:
                cash_flow_growth_count += 1
        if cash_flow_growth_count < params.min_operating_cash_flow_growth_count:
            return {
                "passed": False,
                "status": "cash_flow_growth_failed",
                "available_years": len(rows),
                "growth_count": cash_flow_growth_count,
                "latest_fiscal_year": latest_row.fiscal_year_label,
            }
    if params.max_pe is not None:
        pe = _point_in_time_pe(
            latest_row,
            market_price=market_price,
            signal_date=signal_date,
        )
        if pe is None:
            return {
                "passed": False,
                "status": "valuation_missing",
                "available_years": len(rows),
                "growth_count": None,
                "latest_fiscal_year": latest_row.fiscal_year_label,
            }
        if pe <= 0 or pe > params.max_pe:
            return {
                "passed": False,
                "status": "valuation_failed",
                "available_years": len(rows),
                "growth_count": None,
                "latest_fiscal_year": latest_row.fiscal_year_label,
            }
    if params.max_pb is not None:
        pb = _point_in_time_pb(
            latest_row,
            market_price=market_price,
            signal_date=signal_date,
        )
        if pb is None:
            return {
                "passed": False,
                "status": "valuation_missing",
                "available_years": len(rows),
                "growth_count": None,
                "latest_fiscal_year": latest_row.fiscal_year_label,
            }
        if pb <= 0 or pb > params.max_pb:
            return {
                "passed": False,
                "status": "valuation_failed",
                "available_years": len(rows),
                "growth_count": None,
                "latest_fiscal_year": latest_row.fiscal_year_label,
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
        "status": "passed" if growth_count >= params.effective_min_growth_count else "growth_failed",
        "available_years": len(rows),
        "growth_count": growth_count,
        "latest_fiscal_year": rows[-1].fiscal_year_label,
        "latest_net_income_yoy_pct": _latest_yoy_pct(net_income_values),
        "latest_operating_cash_flow_yoy_pct": _latest_yoy_pct(
            [row.operating_cash_flow for row in rows]
        ),
    }


def _is_usd_financial_row(row: _FundamentalAnnualSnapshot) -> bool:
    return (row.net_income_currency or "").upper() == "USD"


def _current_valuation_is_available(
    row: _FundamentalAnnualSnapshot,
    *,
    signal_date: date,
) -> bool:
    return row.source_as_of_date is not None and row.source_as_of_date <= signal_date


def _point_in_time_pe(
    row: _FundamentalAnnualSnapshot,
    *,
    market_price: Decimal | None,
    signal_date: date,
) -> Decimal | None:
    if market_price is not None and market_price > 0 and _is_usd_financial_row(row):
        if row.diluted_eps is not None:
            return market_price / row.diluted_eps if row.diluted_eps > 0 else Decimal("-1")
        if (
            row.weighted_average_diluted_shares is not None
            and row.weighted_average_diluted_shares > 0
            and row.net_income is not None
        ):
            return (
                (market_price * row.weighted_average_diluted_shares) / row.net_income
                if row.net_income > 0
                else Decimal("-1")
            )
    if row.pe is not None and _current_valuation_is_available(row, signal_date=signal_date):
        return row.pe
    return None


def _point_in_time_pb(
    row: _FundamentalAnnualSnapshot,
    *,
    market_price: Decimal | None,
    signal_date: date,
) -> Decimal | None:
    if (
        market_price is not None
        and market_price > 0
        and _is_usd_financial_row(row)
        and row.weighted_average_diluted_shares is not None
        and row.weighted_average_diluted_shares > 0
        and row.stockholders_equity is not None
    ):
        return (
            (market_price * row.weighted_average_diluted_shares) / row.stockholders_equity
            if row.stockholders_equity > 0
            else Decimal("-1")
        )
    if row.pb is not None and _current_valuation_is_available(row, signal_date=signal_date):
        return row.pb
    return None


def _evaluate_fundamental_growth(
    session,
    *,
    instrument_id: int,
    signal_date: date,
    params: FundamentalGrowthParams,
    market_price: Decimal | None = None,
) -> dict[str, object]:
    if not params.enabled:
        return {
            "passed": True,
            "status": "not_required",
            "available_years": None,
            "growth_count": None,
            "latest_fiscal_year": None,
        }

    available_cutoff = signal_date - timedelta(days=params.reporting_lag_days)
    rows = [
        _FundamentalAnnualSnapshot(
            fiscal_year_end_date=row.fiscal_year_end_date,
            fiscal_year_label=row.fiscal_year_label,
            net_income=row.net_income,
            net_income_currency=row.net_income_currency,
            source_as_of_date=row.source_as_of_date,
            operating_cash_flow=row.operating_cash_flow,
            free_cash_flow=row.free_cash_flow,
            diluted_eps=row.diluted_eps,
            stockholders_equity=row.stockholders_equity,
            weighted_average_diluted_shares=row.weighted_average_diluted_shares,
            pe=row.pe,
            pb=row.pb,
        )
        for row in session.execute(
            select(
                FundamentalsAnnual.fiscal_year_end_date,
                FundamentalsAnnual.fiscal_year_label,
                FundamentalsAnnual.net_income,
                FundamentalsAnnual.net_income_currency,
                FundamentalsAnnual.source_as_of_date,
                FundamentalsAnnual.operating_cash_flow,
                FundamentalsAnnual.free_cash_flow,
                FundamentalsAnnual.diluted_eps,
                FundamentalsAnnual.stockholders_equity,
                FundamentalsAnnual.weighted_average_diluted_shares,
                FundamentalsAnnual.pe,
                FundamentalsAnnual.pb,
            )
            .where(
                FundamentalsAnnual.instrument_id == instrument_id,
                FundamentalsAnnual.fiscal_year_end_date <= available_cutoff,
                FundamentalsAnnual.source_as_of_date <= signal_date,
                FundamentalsAnnual.net_income.is_not(None),
                FundamentalsAnnual.data_status != "missing",
            )
            .order_by(FundamentalsAnnual.fiscal_year_end_date.desc())
            .limit(params.min_years)
        ).all()
    ]
    rows.reverse()
    return _evaluate_fundamental_growth_from_available_rows(
        rows,
        signal_date=signal_date,
        market_price=market_price,
        params=params,
    )


def _format_meta_pct(value: object) -> str | None:
    if value is None:
        return None
    return _format_decimal(Decimal(str(value)), "0.01")


def _fundamental_growth_summary_from_meta(meta: dict[str, object]) -> str | None:
    status = str(meta.get("status") or "")
    if status == "not_required":
        return "未启用"
    latest_yoy = _format_meta_pct(meta.get("latest_net_income_yoy_pct"))
    ocf_yoy = _format_meta_pct(meta.get("latest_operating_cash_flow_yoy_pct"))
    details: list[str] = []
    years = meta.get("available_years")
    growth_count = meta.get("growth_count")
    try:
        comparable_years = max(int(years) - 1, 0) if years is not None else None
    except (TypeError, ValueError):
        comparable_years = None
    if latest_yoy is not None:
        headline = f"最近净利润同比 {latest_yoy}%"
    else:
        headline = "最近净利润同比 —"
    if growth_count is not None and comparable_years is not None:
        details.append(f"净利润增长 {growth_count}/{comparable_years} 年")
    if ocf_yoy is not None:
        details.append(f"经营现金流同比 {ocf_yoy}%")
    latest_year = meta.get("latest_fiscal_year")
    if latest_year:
        details.append(f"最新财年 {latest_year}")
    if details:
        return f"{headline}（{'，'.join(details)}）"
    return headline


def fundamental_growth_context_for_signal(
    session,
    *,
    instrument_id: int,
    signal_date: date,
    params: FundamentalGrowthParams,
) -> dict[str, str | None]:
    market_price: Decimal | None = None
    if params.max_pe is not None or params.max_pb is not None:
        market_price = _load_market_close_by_instrument(
            session,
            instrument_ids=[instrument_id],
            trade_date=signal_date,
            cache={},
        ).get(instrument_id)
    meta = _evaluate_fundamental_growth(
        session,
        instrument_id=instrument_id,
        signal_date=signal_date,
        params=params,
        market_price=market_price,
    )
    return {
        "fundamental_growth_summary": _fundamental_growth_summary_from_meta(meta),
        "fundamental_growth_latest_yoy_pct": _format_meta_pct(
            meta.get("latest_net_income_yoy_pct")
        ),
        "fundamental_operating_cash_flow_latest_yoy_pct": _format_meta_pct(
            meta.get("latest_operating_cash_flow_yoy_pct")
        ),
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
    prepared = _prepare_cup_handle_series(candles)
    usable_candles, closes, highs, lows, volumes = prepared
    min_required = max(
        params.min_cup_duration + params.min_handle_duration,
        params.min_total_duration,
    )
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


def detect_cup_handle_patterns(
    candles: list[MarketDataDaily] | list[_PatternCandle],
    params: CupHandleParams | None = None,
) -> list[dict[str, object]]:
    """Detect all valid cup-with-handle breakout events in a candle series."""
    params = _normalize_cup_handle_params(params)
    prepared = _prepare_cup_handle_series(candles)
    usable_candles, closes, highs, lows, volumes = prepared
    min_required = max(
        params.min_cup_duration + params.min_handle_duration,
        params.min_total_duration,
    )
    if len(usable_candles) < min_required + 1:
        return []

    n = len(usable_candles)
    right_lip_indices = {
        breakout_idx - handle_len
        for breakout_idx in range(min_required, n)
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
    events: list[dict[str, object]] = []
    for breakout_idx in range(min_required, n):
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
            events.append(result)
    return events


def _prepare_cup_handle_series(
    candles: list[MarketDataDaily] | list[_PatternCandle],
) -> tuple[
    list[MarketDataDaily] | list[_PatternCandle],
    list[float],
    list[float],
    list[float],
    list[int | None],
]:
    usable_candles: list[MarketDataDaily | _PatternCandle] = []
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
    return usable_candles, closes, highs, lows, volumes


def _has_prior_uptrend(
    closes: list[float],
    *,
    left_lip_idx: int,
    left_lip_high: float,
    params: CupHandleParams,
) -> bool:
    if not params.require_prior_uptrend:
        return True
    prior_uptrend_pct = _prior_uptrend_pct_for_window(
        closes,
        left_lip_idx=left_lip_idx,
        left_lip_high=left_lip_high,
        lookback_days=params.prior_uptrend_lookback_days,
    )
    if prior_uptrend_pct is None:
        return False
    return prior_uptrend_pct >= float(params.min_prior_uptrend_pct)


def _prior_uptrend_pct(
    closes: list[float],
    *,
    left_lip_idx: int,
    left_lip_high: float,
    params: CupHandleParams,
) -> float | None:
    return _prior_uptrend_pct_for_window(
        closes,
        left_lip_idx=left_lip_idx,
        left_lip_high=left_lip_high,
        lookback_days=params.prior_uptrend_lookback_days,
    )


def _prior_uptrend_pct_for_window(
    closes: list[float],
    *,
    left_lip_idx: int,
    left_lip_high: float,
    lookback_days: int,
) -> float | None:
    start_idx = max(0, left_lip_idx - lookback_days)
    prior_closes = [value for value in closes[start_idx:left_lip_idx] if value > 0]
    if not prior_closes or left_lip_high <= 0:
        return None
    return ((left_lip_high / min(prior_closes)) - 1) * 100


def _bottom_zone_stats(
    lows: list[float],
    *,
    left_lip_idx: int,
    right_lip_idx: int,
    cup_bottom_low: float,
    cup_depth_abs: float,
    bottom_zone_pct: float,
) -> tuple[int, float]:
    if cup_depth_abs <= 0:
        return 0, 0.0
    cup_len = max(right_lip_idx - left_lip_idx, 1)
    bottom_threshold = cup_bottom_low + (cup_depth_abs * bottom_zone_pct / 100)
    first_idx: int | None = None
    last_idx: int | None = None
    dwell_days = 0
    for idx in range(left_lip_idx, right_lip_idx + 1):
        if lows[idx] <= bottom_threshold:
            dwell_days += 1
            if first_idx is None:
                first_idx = idx
            last_idx = idx
    if first_idx is None or last_idx is None:
        return 0, 0.0
    bottom_span = last_idx - first_idx + 1
    return dwell_days, (bottom_span / cup_len) * 100


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
    cup_len = max(right_lip_idx - left_lip_idx, 1)
    bottom_threshold = cup_bottom_low + (cup_depth_abs * float(params.bottom_zone_pct) / 100)
    min_span_days = max(1, ceil(cup_len * float(params.min_bottom_span_pct) / 100))
    first_idx: int | None = None
    dwell_days = 0
    for idx in range(left_lip_idx, right_lip_idx + 1):
        if lows[idx] > bottom_threshold:
            continue
        dwell_days += 1
        if first_idx is None:
            first_idx = idx
        if dwell_days >= params.min_bottom_dwell_days and idx - first_idx + 1 >= min_span_days:
            return True
    return False


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
                next_interior_high = highs[left_lip_idx + 1]
                if next_interior_high > interior_high:
                    interior_high = next_interior_high

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

            lip_delta_pct = abs((left_lip_high - right_lip_high) / left_lip_high) * 100
            if lip_delta_pct > max_lip_delta_pct:
                continue

            lip_baseline = left_lip_high if left_lip_high >= right_lip_high else right_lip_high
            if interior_high > lip_baseline * 1.02:
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
            left_lip_high = highs[cup_candidate.left_lip_idx]
            lip_delta_pct = abs((left_lip_high - right_lip_high) / left_lip_high) * 100
            handle_high_above_lip_pct = max(
                ((handle_high / right_lip_high) - 1) * 100,
                0,
            )
            left_side_duration_pct = (
                (cup_candidate.cup_bottom_idx - cup_candidate.left_lip_idx)
                / max(cup_duration, 1)
            ) * 100
            right_side_duration_pct = (
                (right_lip_idx - cup_candidate.cup_bottom_idx) / max(cup_duration, 1)
            ) * 100
            bottom_feature_stats = {
                zone: _bottom_zone_stats(
                    lows,
                    left_lip_idx=cup_candidate.left_lip_idx,
                    right_lip_idx=right_lip_idx,
                    cup_bottom_low=cup_bottom_low,
                    cup_depth_abs=cup_depth_abs,
                    bottom_zone_pct=float(zone),
                )
                for zone in CUP_HANDLE_BOTTOM_FEATURE_ZONES
            }
            prior_uptrend_by_window = {
                window: _prior_uptrend_pct_for_window(
                    closes,
                    left_lip_idx=cup_candidate.left_lip_idx,
                    left_lip_high=left_lip_high,
                    lookback_days=window,
                )
                for window in CUP_HANDLE_PRIOR_UPTREND_FEATURE_WINDOWS
            }
            breakout_volume_by_window = {
                window: _breakout_volume_ratio(
                    volumes,
                    breakout_idx=breakout_idx,
                    avg_days=window,
                )
                for window in CUP_HANDLE_BREAKOUT_VOLUME_FEATURE_WINDOWS
            }
            breakout_close_over_resistance_pct = (
                (closes[breakout_idx] / breakout_resistance) - 1
            ) * 100

            return {
                "left_lip_date": candles[cup_candidate.left_lip_idx].trade_date,
                "cup_bottom_date": candles[cup_candidate.cup_bottom_idx].trade_date,
                "right_lip_date": candles[right_lip_idx].trade_date,
                "handle_low_date": candles[right_lip_idx + 1 + handle_low_offset].trade_date,
                "breakout_date": candles[breakout_idx].trade_date,
                "_left_lip_idx": cup_candidate.left_lip_idx,
                "_cup_bottom_idx": cup_candidate.cup_bottom_idx,
                "_right_lip_idx": right_lip_idx,
                "_handle_low_idx": right_lip_idx + 1 + handle_low_offset,
                "_breakout_idx": breakout_idx,
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
                "right_lip_delta_pct": Decimal(str(lip_delta_pct)).quantize(
                    Decimal("0.01")
                ),
                "handle_high_above_lip_pct": Decimal(str(handle_high_above_lip_pct)).quantize(
                    Decimal("0.01")
                ),
                "left_side_duration_pct": Decimal(str(left_side_duration_pct)).quantize(
                    Decimal("0.01")
                ),
                "right_side_duration_pct": Decimal(str(right_side_duration_pct)).quantize(
                    Decimal("0.01")
                ),
                "bottom_dwell_days_by_zone": {
                    zone: dwell_days for zone, (dwell_days, _) in bottom_feature_stats.items()
                },
                "bottom_span_pct_by_zone": {
                    zone: Decimal(str(span_pct)).quantize(Decimal("0.01"))
                    for zone, (_, span_pct) in bottom_feature_stats.items()
                },
                "prior_uptrend_pct": (
                    Decimal(str(prior_uptrend_pct)).quantize(Decimal("0.01"))
                    if prior_uptrend_pct is not None
                    else None
                ),
                "prior_uptrend_pct_by_window": {
                    window: (
                        Decimal(str(value)).quantize(Decimal("0.01"))
                        if value is not None
                        else None
                    )
                    for window, value in prior_uptrend_by_window.items()
                },
                "breakout_volume_ratio": (
                    Decimal(str(breakout_volume_ratio)).quantize(Decimal("0.01"))
                    if breakout_volume_ratio is not None
                    else None
                ),
                "breakout_volume_ratio_by_window": {
                    window: (
                        Decimal(str(value)).quantize(Decimal("0.01"))
                        if value is not None
                        else None
                    )
                    for window, value in breakout_volume_by_window.items()
                },
                "breakout_close_over_resistance_pct": Decimal(
                    str(breakout_close_over_resistance_pct)
                ).quantize(Decimal("0.01")),
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
