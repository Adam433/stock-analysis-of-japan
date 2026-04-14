from __future__ import annotations

from collections import deque
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import os

from sqlalchemy import select

from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.market_data.models import MarketDataDaily

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
    return row.adj_close or row.close


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


def materialize_derived_indicator_facts(
    session,
    *,
    commit_every_dates: int = DEFAULT_MATERIALIZE_COMMIT_EVERY_DATES,
) -> dict[str, int]:
    price_history: dict[int, deque[PricePoint]] = defaultdict(
        lambda: deque(maxlen=MAX_HISTORY_WINDOW)
    )
    current_trade_date: date | None = None
    current_date_facts: dict[int, dict[str, Decimal | None]] = {}
    current_date_returns: dict[int, dict[int, Decimal]] = {
        lookback: {} for lookback in RPS_LOOKBACKS
    }
    inserted = 0
    updated = 0
    processed_trade_dates = 0

    def flush_trade_date(trade_date: date | None) -> None:
        nonlocal inserted, updated, processed_trade_dates, current_date_facts, current_date_returns
        if trade_date is None or not current_date_facts:
            return

        for lookback, returns_by_instrument in current_date_returns.items():
            percentile_scores = _percentile_scores(returns_by_instrument)
            field_name = f"rps_{lookback}"
            for instrument_id, score in percentile_scores.items():
                current_date_facts[instrument_id][field_name] = score

        existing_rows = {
            row.instrument_id: row
            for row in session.execute(
                select(DerivedIndicatorDaily).where(DerivedIndicatorDaily.trade_date == trade_date)
            ).scalars()
        }

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

        current_date_facts = {}
        current_date_returns = {lookback: {} for lookback in RPS_LOOKBACKS}

    market_rows = session.execute(
        select(MarketDataDaily)
        .order_by(MarketDataDaily.trade_date.asc(), MarketDataDaily.instrument_id.asc())
    ).scalars().yield_per(10_000)

    for market_row in market_rows:
        if current_trade_date is None:
            current_trade_date = market_row.trade_date
        elif market_row.trade_date != current_trade_date:
            flush_trade_date(current_trade_date)
            current_trade_date = market_row.trade_date

        price = _resolve_price(market_row)
        if price is None or market_row.data_status == "unavailable":
            continue

        history = price_history[market_row.instrument_id]
        history.append(
            PricePoint(
                instrument_id=market_row.instrument_id,
                trade_date=market_row.trade_date,
                price=price,
            )
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
    return {"inserted": inserted, "updated": updated}
