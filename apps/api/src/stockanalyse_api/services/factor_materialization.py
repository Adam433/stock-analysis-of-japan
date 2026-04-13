from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import select

from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.market_data.models import MarketDataDaily

RPS_LOOKBACKS = (50, 120, 250)
ONE_HUNDRED = Decimal("100")


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


def materialize_derived_indicator_facts(session) -> dict[str, int]:
    market_rows = session.execute(
        select(MarketDataDaily)
        .order_by(MarketDataDaily.trade_date.asc(), MarketDataDaily.instrument_id.asc())
    ).scalars().all()

    rows_by_key: dict[tuple[int, date], DerivedIndicatorDaily] = {
        (row.instrument_id, row.trade_date): row
        for row in session.execute(select(DerivedIndicatorDaily)).scalars().all()
    }

    price_history: dict[int, list[PricePoint]] = defaultdict(list)
    facts_by_key: dict[tuple[int, date], dict[str, Decimal | None]] = {}
    returns_by_lookback_date: dict[tuple[int, date], dict[int, Decimal]] = defaultdict(dict)

    for market_row in market_rows:
        price = _resolve_price(market_row)
        if price is not None and market_row.data_status != "unavailable":
            history = price_history[market_row.instrument_id]
            history.append(
                PricePoint(
                    instrument_id=market_row.instrument_id,
                    trade_date=market_row.trade_date,
                    price=price,
                )
            )
            current_index = len(history) - 1

            fifty_two_week_window = history[max(0, current_index - 251) : current_index + 1]
            fifty_two_week_high = max(point.price for point in fifty_two_week_window)
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
                    prior_price = history[prior_index].price
                    relative_strength = (price / prior_price) - Decimal("1")
                    returns_by_lookback_date[(lookback, market_row.trade_date)][market_row.instrument_id] = (
                        relative_strength
                    )

            facts_by_key[(market_row.instrument_id, market_row.trade_date)] = facts

    for (lookback, trade_date), returns_by_instrument in returns_by_lookback_date.items():
        percentile_scores = _percentile_scores(returns_by_instrument)
        field_name = f"rps_{lookback}"
        for instrument_id, score in percentile_scores.items():
            facts_by_key[(instrument_id, trade_date)][field_name] = score

    inserted = 0
    updated = 0
    for (instrument_id, trade_date), facts in facts_by_key.items():
        row = rows_by_key.get((instrument_id, trade_date))
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

    session.commit()
    return {"inserted": inserted, "updated": updated}
