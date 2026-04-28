from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class AdjustedOhlc:
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None


MAX_ADJUSTMENT_FACTOR = Decimal("20")
MAX_PRICE_GAP_RATIO = Decimal("20")


def is_complete_market_row(row: object) -> bool:
    if (getattr(row, "data_status", "complete") or "complete") != "complete":
        return False
    if not _has_positive_volume(getattr(row, "volume", None)):
        return False
    return all(
        _to_decimal(getattr(row, field, None)) is not None
        for field in ("high", "low", "close")
    )


def adjusted_ohlc(row: object) -> AdjustedOhlc:
    raw_close = _to_decimal(getattr(row, "close", None))
    adj_close = _to_decimal(getattr(row, "adj_close", None))
    factor = _adjustment_factor(raw_close, adj_close)

    return AdjustedOhlc(
        open=_scale_price(getattr(row, "open", None), factor),
        high=_scale_price(getattr(row, "high", None), factor),
        low=_scale_price(getattr(row, "low", None), factor),
        close=adj_close if factor is not None else raw_close,
    )


def adjusted_open(row: object) -> Decimal | None:
    return adjusted_ohlc(row).open


def adjusted_close(row: object) -> Decimal | None:
    return adjusted_ohlc(row).close


def has_extreme_price_gap(
    previous_price: object,
    current_price: object,
    *,
    max_ratio: Decimal = MAX_PRICE_GAP_RATIO,
) -> bool:
    previous = _to_decimal(previous_price)
    current = _to_decimal(current_price)
    if previous is None or current is None or previous <= 0 or current <= 0:
        return False
    ratio = max(previous / current, current / previous)
    return ratio >= max_ratio


def _adjustment_factor(
    raw_close: Decimal | None,
    adj_close: Decimal | None,
) -> Decimal | None:
    if raw_close is None or raw_close <= 0 or adj_close is None:
        return None
    factor = adj_close / raw_close
    if factor <= 0:
        return None
    # Long dividend-adjusted histories can legitimately have very small
    # positive factors; only reject factors that would amplify raw prices.
    if factor > MAX_ADJUSTMENT_FACTOR:
        return None
    return factor


def _scale_price(value: object, factor: Decimal | None) -> Decimal | None:
    price = _to_decimal(value)
    if price is None:
        return None
    return price * factor if factor is not None else price


def _has_positive_volume(value: object) -> bool:
    if value is None:
        return False
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return False


def _to_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
