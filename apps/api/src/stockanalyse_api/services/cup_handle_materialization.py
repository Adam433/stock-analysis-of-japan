from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from math import ceil
from decimal import Decimal
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.exc import OperationalError

from stockanalyse_api.domain.backtests.models import (
    CupHandleMaterializationRun,
    CupHandlePatternEvent,
)
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily

CUP_HANDLE_DETECTOR_VERSION = "cup_handle_candidate_v1"
CUP_HANDLE_PRIOR_UPTREND_FEATURE_WINDOWS = (60, 90, 120, 180)
CUP_HANDLE_BREAKOUT_VOLUME_FEATURE_WINDOWS = (20, 50, 60)
CUP_HANDLE_BOTTOM_FEATURE_ZONES = (20, 35)
MARKET_EXCHANGES = {
    "jp": ("TSE",),
    "us": ("US",),
}

CANDIDATE_GENERATION_BOUNDS: dict[str, object] = {
    "lookback_days": 750,
    "min_cup_duration": 35,
    "max_cup_duration": 330,
    "min_handle_duration": 3,
    "max_handle_duration": 90,
    "min_total_duration": 50,
    "max_total_duration": 420,
    "min_cup_depth_pct": "5",
    "max_cup_depth_pct": "60",
    "min_handle_pullback_pct": "1",
    "max_handle_pullback_pct": "35",
    "max_right_lip_delta_pct": "15",
    "require_prior_uptrend": False,
    "prior_uptrend_lookback_days": list(CUP_HANDLE_PRIOR_UPTREND_FEATURE_WINDOWS),
    "min_prior_uptrend_pct": None,
    "min_handle_low_position_pct": "40",
    "max_handle_depth_to_cup_depth_pct": "80",
    "max_handle_high_above_lip_pct": "8",
    "min_bottom_dwell_days": 2,
    "bottom_zone_pct": "35",
    "min_bottom_span_pct": "5",
    "min_cup_side_duration_pct": "10",
    "require_breakout_volume": False,
    "breakout_volume_avg_days": list(CUP_HANDLE_BREAKOUT_VOLUME_FEATURE_WINDOWS),
    "min_breakout_volume_multiplier": None,
    "breakout_lookback_days": 60,
}


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime, Decimal)):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable.")


def dump_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default)


def load_json(value: str | None, *, default: Any = None) -> Any:
    if value is None:
        return default
    return json.loads(value)


def normalize_market(value: str | None) -> str:
    market = (value or "jp").lower()
    if market not in MARKET_EXCHANGES:
        raise ValueError("market must be jp or us.")
    return market


def _decimal(value: object) -> Decimal:
    return Decimal(str(value))


def _quantized_decimal(value: object | None, pattern: str = "0.0001") -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value)).quantize(Decimal(pattern))


def _bounds_payload() -> str:
    return dump_json(CANDIDATE_GENERATION_BOUNDS)


def _feature_windows_payload() -> str:
    return dump_json(
        {
            "prior_uptrend": list(CUP_HANDLE_PRIOR_UPTREND_FEATURE_WINDOWS),
            "breakout_volume": list(CUP_HANDLE_BREAKOUT_VOLUME_FEATURE_WINDOWS),
            "bottom_zones": list(CUP_HANDLE_BOTTOM_FEATURE_ZONES),
        }
    )


def _candidate_generation_params():
    from stockanalyse_api.services.dashboard import CupHandleParams

    return CupHandleParams(
        min_cup_duration=35,
        max_cup_duration=330,
        min_handle_duration=3,
        max_handle_duration=90,
        min_total_duration=50,
        max_total_duration=420,
        min_cup_depth_pct=Decimal("5"),
        max_cup_depth_pct=Decimal("60"),
        min_handle_pullback_pct=Decimal("1"),
        max_handle_pullback_pct=Decimal("35"),
        max_right_lip_delta_pct=Decimal("15"),
        require_prior_uptrend=False,
        prior_uptrend_lookback_days=120,
        min_prior_uptrend_pct=Decimal("0"),
        min_handle_low_position_pct=Decimal("40"),
        max_handle_depth_to_cup_depth_pct=Decimal("80"),
        max_handle_high_above_lip_pct=Decimal("8"),
        min_bottom_dwell_days=2,
        bottom_zone_pct=Decimal("35"),
        min_bottom_span_pct=Decimal("5"),
        min_cup_side_duration_pct=Decimal("10"),
        require_breakout_volume=False,
        breakout_volume_avg_days=50,
        min_breakout_volume_multiplier=Decimal("0"),
        breakout_lookback_days=60,
        lookback_days=750,
    )


def cup_handle_params_covered_by_materialization(
    params,
    *,
    generation_bounds: dict[str, object],
) -> bool:
    if int(params.lookback_days) > int(generation_bounds["lookback_days"]):
        return False
    if int(params.breakout_lookback_days) > int(generation_bounds["breakout_lookback_days"]):
        return False

    range_checks = (
        ("cup_duration", params.min_cup_duration, params.max_cup_duration),
        ("handle_duration", params.min_handle_duration, params.max_handle_duration),
        ("total_duration", params.min_total_duration, params.max_total_duration),
    )
    for prefix, minimum, maximum in range_checks:
        if int(minimum) < int(generation_bounds[f"min_{prefix}"]):
            return False
        if int(maximum) > int(generation_bounds[f"max_{prefix}"]):
            return False

    decimal_range_checks = (
        ("cup_depth_pct", params.min_cup_depth_pct, params.max_cup_depth_pct),
        ("handle_pullback_pct", params.min_handle_pullback_pct, params.max_handle_pullback_pct),
    )
    for prefix, minimum, maximum in decimal_range_checks:
        if _decimal(minimum) < _decimal(generation_bounds[f"min_{prefix}"]):
            return False
        if _decimal(maximum) > _decimal(generation_bounds[f"max_{prefix}"]):
            return False

    if _decimal(params.max_right_lip_delta_pct) > _decimal(generation_bounds["max_right_lip_delta_pct"]):
        return False
    if _decimal(params.min_handle_low_position_pct) < _decimal(
        generation_bounds["min_handle_low_position_pct"]
    ):
        return False
    if _decimal(params.max_handle_depth_to_cup_depth_pct) > _decimal(
        generation_bounds["max_handle_depth_to_cup_depth_pct"]
    ):
        return False
    if _decimal(params.max_handle_high_above_lip_pct) > _decimal(
        generation_bounds["max_handle_high_above_lip_pct"]
    ):
        return False
    if _decimal(params.min_cup_side_duration_pct) < _decimal(
        generation_bounds["min_cup_side_duration_pct"]
    ):
        return False

    if int(Decimal(str(params.bottom_zone_pct))) not in CUP_HANDLE_BOTTOM_FEATURE_ZONES:
        return False
    if params.require_prior_uptrend and int(params.prior_uptrend_lookback_days) not in (
        CUP_HANDLE_PRIOR_UPTREND_FEATURE_WINDOWS
    ):
        return False
    if params.require_breakout_volume and int(params.breakout_volume_avg_days) not in (
        CUP_HANDLE_BREAKOUT_VOLUME_FEATURE_WINDOWS
    ):
        return False
    return True


def find_covering_materialization_run(
    session,
    *,
    market: str,
    signal_date: date,
    params,
) -> CupHandleMaterializationRun | None:
    resolved_market = normalize_market(market)
    effective_lookback_days = int(
        getattr(params, "effective_lookback_days", getattr(params, "lookback_days", 0))
    )
    # The dashboard runtime path interprets lookback as a candle count, not a
    # calendar-day count. Use a conservative calendar multiplier so a completed
    # materialization run is only reused when it has enough trading-history rows.
    required_calendar_days = ceil(effective_lookback_days * 8 / 5)
    required_source_start = signal_date - timedelta(days=required_calendar_days)
    candidate_runs = list(
        session.execute(
            select(CupHandleMaterializationRun)
            .where(
                CupHandleMaterializationRun.market == resolved_market,
                CupHandleMaterializationRun.status == "completed",
                CupHandleMaterializationRun.source_start_date <= required_source_start,
                CupHandleMaterializationRun.source_end_date >= signal_date,
                CupHandleMaterializationRun.detector_version == CUP_HANDLE_DETECTOR_VERSION,
            )
            .order_by(CupHandleMaterializationRun.completed_at.desc())
        ).scalars()
    )
    for run in candidate_runs:
        generation_bounds = load_json(run.generation_bounds_json, default={})
        if isinstance(generation_bounds, dict) and cup_handle_params_covered_by_materialization(
            params,
            generation_bounds=generation_bounds,
        ):
            return run
    return None


def load_materialized_cup_handle_matches(
    session,
    *,
    market: str,
    signal_date: date,
    instrument_ids: list[int],
    params,
) -> dict[int, CupHandlePatternEvent] | None:
    if not instrument_ids:
        return {}
    try:
        materialization_run = find_covering_materialization_run(
            session,
            market=market,
            signal_date=signal_date,
            params=params,
        )
    except OperationalError as exc:
        if "cup_handle_materialization_runs" in str(exc) or "cup_handle_pattern_events" in str(exc):
            return None
        raise
    if materialization_run is None:
        return None

    earliest_breakout_date = signal_date - timedelta(days=int(params.breakout_lookback_days) * 3)
    query: Select[tuple[CupHandlePatternEvent]] = select(CupHandlePatternEvent).where(
        CupHandlePatternEvent.materialization_run_id == materialization_run.id,
        CupHandlePatternEvent.market == normalize_market(market),
        CupHandlePatternEvent.instrument_id.in_(instrument_ids),
        CupHandlePatternEvent.breakout_date >= earliest_breakout_date,
        CupHandlePatternEvent.breakout_date <= signal_date,
        CupHandlePatternEvent.cup_duration >= int(params.min_cup_duration),
        CupHandlePatternEvent.cup_duration <= int(params.max_cup_duration),
        CupHandlePatternEvent.handle_duration >= int(params.min_handle_duration),
        CupHandlePatternEvent.handle_duration <= int(params.max_handle_duration),
        CupHandlePatternEvent.total_duration >= int(params.min_total_duration),
        CupHandlePatternEvent.total_duration <= int(params.max_total_duration),
        CupHandlePatternEvent.cup_depth_pct >= _decimal(params.min_cup_depth_pct),
        CupHandlePatternEvent.cup_depth_pct <= _decimal(params.max_cup_depth_pct),
        CupHandlePatternEvent.handle_depth_pct >= _decimal(params.min_handle_pullback_pct),
        CupHandlePatternEvent.handle_depth_pct <= _decimal(params.max_handle_pullback_pct),
        CupHandlePatternEvent.right_lip_delta_pct <= _decimal(params.max_right_lip_delta_pct),
        CupHandlePatternEvent.handle_low_position_pct >= _decimal(params.min_handle_low_position_pct),
        CupHandlePatternEvent.handle_depth_to_cup_depth_pct
        <= _decimal(params.max_handle_depth_to_cup_depth_pct),
        CupHandlePatternEvent.handle_high_above_lip_pct <= _decimal(params.max_handle_high_above_lip_pct),
    )

    bottom_zone = int(Decimal(str(params.bottom_zone_pct)))
    if bottom_zone == 20:
        query = query.where(
            CupHandlePatternEvent.bottom_dwell_days_zone_20 >= int(params.min_bottom_dwell_days),
            CupHandlePatternEvent.bottom_span_pct_zone_20 >= _decimal(params.min_bottom_span_pct),
        )
    elif bottom_zone == 35:
        query = query.where(
            CupHandlePatternEvent.bottom_dwell_days_zone_35 >= int(params.min_bottom_dwell_days),
            CupHandlePatternEvent.bottom_span_pct_zone_35 >= _decimal(params.min_bottom_span_pct),
        )
    else:
        return None

    if params.require_prior_uptrend:
        prior_column = getattr(
            CupHandlePatternEvent,
            f"prior_uptrend_pct_{int(params.prior_uptrend_lookback_days)}",
            None,
        )
        if prior_column is None:
            return None
        query = query.where(
            prior_column.is_not(None),
            prior_column >= _decimal(params.min_prior_uptrend_pct),
        )

    if params.require_breakout_volume:
        volume_column = getattr(
            CupHandlePatternEvent,
            f"breakout_volume_ratio_{int(params.breakout_volume_avg_days)}",
            None,
        )
        if volume_column is None:
            return None
        query = query.where(
            volume_column.is_not(None),
            volume_column >= _decimal(params.min_breakout_volume_multiplier),
        )

    try:
        events = list(
            session.execute(
                query.order_by(
                    CupHandlePatternEvent.instrument_id.asc(),
                    CupHandlePatternEvent.breakout_date.desc(),
                    CupHandlePatternEvent.id.desc(),
                )
            ).scalars()
        )
    except OperationalError as exc:
        if "cup_handle_pattern_events" in str(exc):
            return None
        raise
    event_by_instrument: dict[int, CupHandlePatternEvent] = {}
    for event in events:
        event_by_instrument.setdefault(event.instrument_id, event)
    return event_by_instrument


def materialize_cup_handle_candidates(
    session,
    *,
    market: str = "us",
    source_start_date: date | None = None,
    source_end_date: date | None = None,
    commit_every: int = 100,
) -> CupHandleMaterializationRun:
    resolved_market = normalize_market(market)
    exchanges = MARKET_EXCHANGES[resolved_market]
    latest_market_date = session.execute(
        select(func.max(MarketDataDaily.trade_date))
        .join(Instrument, Instrument.id == MarketDataDaily.instrument_id)
        .where(Instrument.exchange.in_(exchanges))
    ).scalar_one_or_none()
    start_date = source_start_date or session.execute(
        select(func.min(MarketDataDaily.trade_date))
        .join(Instrument, Instrument.id == MarketDataDaily.instrument_id)
        .where(Instrument.exchange.in_(exchanges))
    ).scalar_one_or_none()
    end_date = source_end_date or latest_market_date
    if start_date is None or end_date is None:
        raise ValueError("No market data is available for cup-handle materialization.")
    if start_date > end_date:
        raise ValueError("source_start_date must be on or before source_end_date.")

    run = CupHandleMaterializationRun(
        market=resolved_market,
        status="running",
        started_at=datetime.now(UTC),
        source_start_date=start_date,
        source_end_date=end_date,
        latest_market_data_date=latest_market_date,
        generation_bounds_json=_bounds_payload(),
        feature_windows_json=_feature_windows_payload(),
        detector_version=CUP_HANDLE_DETECTOR_VERSION,
    )
    session.add(run)
    session.commit()
    session.refresh(run)

    try:
        from stockanalyse_api.services.dashboard import detect_cup_handle_patterns

        params = _candidate_generation_params()
        instruments = list(
            session.execute(
                select(Instrument)
                .where(Instrument.exchange.in_(exchanges))
                .order_by(Instrument.symbol.asc())
            ).scalars()
        )
        for instrument in instruments:
            rows = list(
                session.execute(
                    select(MarketDataDaily)
                    .where(
                        MarketDataDaily.instrument_id == instrument.id,
                        MarketDataDaily.trade_date >= start_date,
                        MarketDataDaily.trade_date <= end_date,
                    )
                    .order_by(MarketDataDaily.trade_date.asc())
                ).scalars()
            )
            if rows:
                for pattern in detect_cup_handle_patterns(rows, params):
                    event = _event_from_pattern(
                        pattern,
                        run=run,
                        market=resolved_market,
                        instrument=instrument,
                        data_start_date=rows[0].trade_date,
                        data_end_date=rows[-1].trade_date,
                    )
                    session.add(event)
                    run.events_created += 1
            run.symbols_processed += 1
            if commit_every > 0 and run.symbols_processed % commit_every == 0:
                session.commit()

        run.status = "completed"
        run.completed_at = datetime.now(UTC)
        session.commit()
        session.refresh(run)
        return run
    except Exception as exc:
        session.rollback()
        failed_run = session.get(CupHandleMaterializationRun, run.id)
        if failed_run is None:
            raise
        failed_run.status = "failed"
        failed_run.completed_at = datetime.now(UTC)
        failed_run.error_message = f"{type(exc).__name__}: {exc}"
        session.commit()
        session.refresh(failed_run)
        return failed_run


def _event_from_pattern(
    pattern: dict[str, object],
    *,
    run: CupHandleMaterializationRun,
    market: str,
    instrument: Instrument,
    data_start_date: date,
    data_end_date: date,
) -> CupHandlePatternEvent:
    prior_by_window = pattern.get("prior_uptrend_pct_by_window")
    volume_by_window = pattern.get("breakout_volume_ratio_by_window")
    bottom_dwell_by_zone = pattern.get("bottom_dwell_days_by_zone")
    bottom_span_by_zone = pattern.get("bottom_span_pct_by_zone")
    if not isinstance(prior_by_window, dict):
        prior_by_window = {}
    if not isinstance(volume_by_window, dict):
        volume_by_window = {}
    if not isinstance(bottom_dwell_by_zone, dict):
        bottom_dwell_by_zone = {}
    if not isinstance(bottom_span_by_zone, dict):
        bottom_span_by_zone = {}

    return CupHandlePatternEvent(
        market=market,
        materialization_run_id=run.id,
        instrument_id=instrument.id,
        symbol_snapshot=instrument.symbol,
        breakout_date=pattern["breakout_date"],  # type: ignore[arg-type]
        left_lip_date=pattern["left_lip_date"],  # type: ignore[arg-type]
        cup_bottom_date=pattern["cup_bottom_date"],  # type: ignore[arg-type]
        right_lip_date=pattern["right_lip_date"],  # type: ignore[arg-type]
        handle_low_date=pattern["handle_low_date"],  # type: ignore[arg-type]
        cup_duration=int(pattern["cup_duration"]),
        handle_duration=int(pattern["handle_duration"]),
        total_duration=int(pattern["total_duration"]),
        cup_depth_pct=_quantized_decimal(pattern["cup_depth_pct"]) or Decimal("0"),
        handle_depth_pct=_quantized_decimal(pattern["handle_depth_pct"]) or Decimal("0"),
        right_lip_delta_pct=_quantized_decimal(pattern["right_lip_delta_pct"]) or Decimal("0"),
        handle_low_position_pct=_quantized_decimal(pattern["handle_position_pct"]) or Decimal("0"),
        handle_depth_to_cup_depth_pct=_quantized_decimal(
            pattern["handle_depth_to_cup_depth_pct"]
        )
        or Decimal("0"),
        handle_high_above_lip_pct=_quantized_decimal(pattern["handle_high_above_lip_pct"])
        or Decimal("0"),
        bottom_dwell_days_zone_20=int(bottom_dwell_by_zone.get(20, 0)),
        bottom_dwell_days_zone_35=int(bottom_dwell_by_zone.get(35, 0)),
        bottom_span_pct_zone_20=_quantized_decimal(bottom_span_by_zone.get(20)) or Decimal("0"),
        bottom_span_pct_zone_35=_quantized_decimal(bottom_span_by_zone.get(35)) or Decimal("0"),
        left_side_duration_pct=_quantized_decimal(pattern["left_side_duration_pct"]) or Decimal("0"),
        right_side_duration_pct=_quantized_decimal(pattern["right_side_duration_pct"]) or Decimal("0"),
        prior_uptrend_pct_60=_quantized_decimal(prior_by_window.get(60)),
        prior_uptrend_pct_90=_quantized_decimal(prior_by_window.get(90)),
        prior_uptrend_pct_120=_quantized_decimal(prior_by_window.get(120)),
        prior_uptrend_pct_180=_quantized_decimal(prior_by_window.get(180)),
        breakout_volume_ratio_20=_quantized_decimal(volume_by_window.get(20)),
        breakout_volume_ratio_50=_quantized_decimal(volume_by_window.get(50)),
        breakout_volume_ratio_60=_quantized_decimal(volume_by_window.get(60)),
        breakout_close_over_resistance_pct=_quantized_decimal(
            pattern["breakout_close_over_resistance_pct"]
        )
        or Decimal("0"),
        data_start_date=data_start_date,
        data_end_date=data_end_date,
        detector_version=CUP_HANDLE_DETECTOR_VERSION,
    )
