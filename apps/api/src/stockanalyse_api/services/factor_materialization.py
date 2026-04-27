from __future__ import annotations

from collections import deque
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import os

from sqlalchemy import select

# Ensure SQLAlchemy relationship targets are registered for direct service usage.
import stockanalyse_api.domain.fundamentals.models  # noqa: F401
import stockanalyse_api.domain.instruments.models  # noqa: F401
from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.market_data_adjustments import adjusted_close
from stockanalyse_api.services.market_data_adjustments import has_extreme_price_gap
from stockanalyse_api.services.market_data_adjustments import is_complete_market_row

RPS_LOOKBACKS = (50, 120, 250)
ONE_HUNDRED = Decimal("100")
MAX_HISTORY_WINDOW = 252
DEFAULT_MATERIALIZE_COMMIT_EVERY_DATES = int(
    os.environ.get("STOCKANALYSE_MATERIALIZE_COMMIT_EVERY_DATES", "5")
)


@dataclass(slots=True)
class PricePoint:
    instrument_id: int
    trade_date: date
    price: Decimal


def _quantize(value: Decimal, pattern: str) -> Decimal:
    return value.quantize(Decimal(pattern), rounding=ROUND_HALF_UP)


def _resolve_price(row: MarketDataDaily) -> Decimal | None:
    if not is_complete_market_row(row):
        return None
    return adjusted_close(row)


def _percentile_scores(
    points_by_instrument: dict[int, Decimal],
) -> dict[int, Decimal]:
    ranked = sorted(
        points_by_instrument.items(),
        key=lambda item: (item[1], item[0]),
    )
    if not ranked:
        return {}
    if len(ranked) == 1:
        instrument_id, _ = ranked[0]
        return {instrument_id: ONE_HUNDRED}

    scores: dict[int, Decimal] = {}
    denominator = Decimal(len(ranked) - 1)
    for rank_index, (instrument_id, _) in enumerate(ranked):
        percentile = Decimal(rank_index) / denominator * ONE_HUNDRED
        scores[instrument_id] = _quantize(percentile, "0.01")
    return scores


def _append_price_point(history: deque[PricePoint], point: PricePoint) -> None:
    if history and has_extreme_price_gap(history[-1].price, point.price):
        history.clear()
    history.append(point)


def _preload_price_history(session, since_date: date) -> dict[int, deque[PricePoint]]:
    price_history: dict[int, deque[PricePoint]] = defaultdict(
        lambda: deque(maxlen=MAX_HISTORY_WINDOW)
    )
    instrument_ids = session.execute(
        select(MarketDataDaily.instrument_id)
        .where(
            MarketDataDaily.trade_date < since_date,
            MarketDataDaily.data_status == "complete",
            MarketDataDaily.volume > 0,
            MarketDataDaily.close.is_not(None),
        )
        .group_by(MarketDataDaily.instrument_id)
        .order_by(MarketDataDaily.instrument_id.asc())
    ).scalars().all()
    for instrument_id in instrument_ids:
        rows = session.execute(
            select(MarketDataDaily)
            .where(
                MarketDataDaily.instrument_id == instrument_id,
                MarketDataDaily.trade_date < since_date,
                MarketDataDaily.data_status == "complete",
                MarketDataDaily.volume > 0,
                MarketDataDaily.close.is_not(None),
            )
            .order_by(MarketDataDaily.trade_date.desc())
            .limit(MAX_HISTORY_WINDOW)
        ).scalars().all()
        for row in reversed(rows):
            price = _resolve_price(row)
            if price is None:
                continue
            _append_price_point(
                price_history[instrument_id],
                PricePoint(
                    instrument_id=row.instrument_id,
                    trade_date=row.trade_date,
                    price=price,
                ),
            )
    return price_history


def materialize_derived_indicator_facts(
    session,
    *,
    commit_every_dates: int = DEFAULT_MATERIALIZE_COMMIT_EVERY_DATES,
    progress_callback: Callable[[dict[str, object]], None] | None = None,
    since_date: date | None = None,
) -> dict[str, int]:
    """Materialize RPS / 52-week-high facts.

    When ``since_date`` is supplied, market rows older than ``since_date`` are
    still walked to warm the per-instrument 252-day price history (so that
    RPS_250 has the right lookback), but no derived rows are written for those
    earlier trade dates. This lets dashboards refresh the recent slice without
    paying the cost of the full backfill.
    """
    price_history: dict[int, deque[PricePoint]] = (
        _preload_price_history(session, since_date)
        if since_date is not None
        else defaultdict(lambda: deque(maxlen=MAX_HISTORY_WINDOW))
    )
    current_trade_date: date | None = None
    current_date_facts: dict[int, dict[str, Decimal | None]] = {}
    current_date_returns: dict[int, dict[int, Decimal]] = {
        lookback: {} for lookback in RPS_LOOKBACKS
    }
    inserted = 0
    updated = 0
    processed_trade_dates = 0
    scanned_trade_dates = 0

    def flush_trade_date(trade_date: date | None) -> None:
        nonlocal inserted, updated, processed_trade_dates, scanned_trade_dates, current_date_facts, current_date_returns
        if trade_date is None:
            return

        scanned_trade_dates += 1
        if since_date is not None and trade_date < since_date:
            if progress_callback is not None:
                progress_callback(
                    {
                        "trade_date": trade_date.isoformat(),
                        "scanned_trade_dates": scanned_trade_dates,
                        "processed_trade_dates": processed_trade_dates,
                        "inserted": inserted,
                        "updated": updated,
                        "warming_up": True,
                    }
                )
            current_date_facts = {}
            current_date_returns = {lookback: {} for lookback in RPS_LOOKBACKS}
            return

        existing_rows = {
            row.instrument_id: row
            for row in session.execute(
                select(DerivedIndicatorDaily).where(DerivedIndicatorDaily.trade_date == trade_date)
            ).scalars()
        }

        if not current_date_facts:
            for row in existing_rows.values():
                session.delete(row)
            processed_trade_dates += 1
            current_date_returns = {lookback: {} for lookback in RPS_LOOKBACKS}
            return

        for lookback, returns_by_instrument in current_date_returns.items():
            percentile_scores = _percentile_scores(returns_by_instrument)
            field_name = f"rps_{lookback}"
            for instrument_id, score in percentile_scores.items():
                current_date_facts[instrument_id][field_name] = score

        for instrument_id, row in existing_rows.items():
            if instrument_id not in current_date_facts:
                session.delete(row)

        for instrument_id, facts in current_date_facts.items():
            row = existing_rows.get(instrument_id)
            if row is None:
                row = DerivedIndicatorDaily(
                    instrument_id=instrument_id,
                    trade_date=trade_date,
                )
                session.add(row)
                inserted += 1
            else:
                updated += 1

            row.rps_50 = facts["rps_50"]
            row.rps_120 = facts["rps_120"]
            row.rps_250 = facts["rps_250"]
            row.fifty_two_week_high = facts["fifty_two_week_high"]
            row.high_proximity_ratio = facts["high_proximity_ratio"]

        processed_trade_dates += 1
        if processed_trade_dates % commit_every_dates == 0:
            session.commit()
            if progress_callback is not None:
                progress_callback(
                    {
                        "trade_date": trade_date.isoformat(),
                        "scanned_trade_dates": scanned_trade_dates,
                        "processed_trade_dates": processed_trade_dates,
                        "inserted": inserted,
                        "updated": updated,
                        "warming_up": False,
                    }
                )

        current_date_facts = {}
        current_date_returns = {lookback: {} for lookback in RPS_LOOKBACKS}

    trade_dates_query = (
        select(MarketDataDaily.trade_date)
        .group_by(MarketDataDaily.trade_date)
        .order_by(MarketDataDaily.trade_date.asc())
    )
    if since_date is not None:
        trade_dates_query = trade_dates_query.where(MarketDataDaily.trade_date >= since_date)
    trade_dates = session.execute(trade_dates_query).scalars().all()

    for trade_date in trade_dates:
        current_trade_date = trade_date
        market_rows = session.execute(
            select(MarketDataDaily)
            .where(
                MarketDataDaily.trade_date == trade_date,
                MarketDataDaily.data_status == "complete",
                MarketDataDaily.volume > 0,
                MarketDataDaily.close.is_not(None),
            )
            .order_by(MarketDataDaily.instrument_id.asc())
        ).scalars().all()

        for market_row in market_rows:
            price = _resolve_price(market_row)
            if price is None:
                continue

            history = price_history[market_row.instrument_id]
            _append_price_point(
                history,
                PricePoint(
                    instrument_id=market_row.instrument_id,
                    trade_date=market_row.trade_date,
                    price=price,
                ),
            )
            history_list = list(history)
            current_index = len(history_list) - 1

            fifty_two_week_high = max(point.price for point in history_list)
            high_proximity_ratio = _quantize(price / fifty_two_week_high, "0.000001")

            facts = {
                "rps_50": None,
                "rps_120": None,
                "rps_250": None,
                "fifty_two_week_high": fifty_two_week_high,
                "high_proximity_ratio": high_proximity_ratio,
            }

            for lookback in RPS_LOOKBACKS:
                prior_index = current_index - lookback
                if prior_index >= 0:
                    prior_price = history_list[prior_index].price
                    relative_strength = (price / prior_price) - Decimal("1")
                    current_date_returns[lookback][market_row.instrument_id] = relative_strength

            current_date_facts[market_row.instrument_id] = facts

        flush_trade_date(current_trade_date)

    session.commit()
    if progress_callback is not None and current_trade_date is not None:
        progress_callback(
            {
                "trade_date": current_trade_date.isoformat(),
                "scanned_trade_dates": scanned_trade_dates,
                "processed_trade_dates": processed_trade_dates,
                "inserted": inserted,
                "updated": updated,
                "warming_up": False,
                "final": True,
            }
        )
    return {
        "inserted": inserted,
        "updated": updated,
        "processed_trade_dates": processed_trade_dates,
        "scanned_trade_dates": scanned_trade_dates,
    }
