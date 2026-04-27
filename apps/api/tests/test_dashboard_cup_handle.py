from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from stockanalyse_api.services.dashboard import (
    CupHandleParams,
    _detect_cup_handle_pattern,
)


@dataclass(frozen=True, slots=True)
class Candle:
    trade_date: date
    high: Decimal
    low: Decimal
    close: Decimal
    adj_close: Decimal | None = None
    volume: int | None = 1000
    data_status: str = "complete"


def _d(value: float) -> Decimal:
    return Decimal(str(round(value, 4)))


def _make_cup_handle(
    cup_duration: int,
    handle_duration: int,
    *,
    handle_low_price: float = 92,
) -> list[Candle]:
    start = date(2025, 1, 1)
    bottom_idx = cup_duration // 2
    prior_duration = 60
    candles: list[Candle] = []

    for idx in range(prior_duration):
        price = 62 + 38 * (idx / max(prior_duration - 1, 1))
        candles.append(
            Candle(
                trade_date=start + timedelta(days=idx),
                high=_d(price),
                low=_d(price),
                close=_d(price),
            )
        )

    for idx in range(cup_duration + 1):
        if idx <= bottom_idx:
            price = 100 - 25 * (idx / bottom_idx)
        else:
            price = 75 + 25 * ((idx - bottom_idx) / (cup_duration - bottom_idx))
        candles.append(
            Candle(
                trade_date=start + timedelta(days=prior_duration + idx),
                high=_d(price),
                low=_d(price),
                close=_d(price),
            )
        )

    mid_handle = max(handle_duration // 2, 1)
    for offset in range(1, handle_duration):
        if offset <= mid_handle:
            price = 100 - (100 - handle_low_price) * (offset / mid_handle)
        else:
            price = handle_low_price + 4 * (
                (offset - mid_handle) / max(handle_duration - mid_handle - 1, 1)
            )
        idx = prior_duration + cup_duration + offset
        candles.append(
            Candle(
                trade_date=start + timedelta(days=idx),
                high=_d(price),
                low=_d(price),
                close=_d(price),
            )
        )

    breakout_idx = prior_duration + cup_duration + handle_duration
    candles.append(
        Candle(
            trade_date=start + timedelta(days=breakout_idx),
            high=Decimal("106"),
            low=Decimal("101"),
            close=Decimal("105"),
        )
    )
    return candles


class DashboardCupHandleTests(unittest.TestCase):
    def test_default_cup_handle_rejects_short_total_duration(self) -> None:
        candles = _make_cup_handle(cup_duration=80, handle_duration=10)
        relaxed_params = CupHandleParams(
            min_cup_duration=60,
            max_cup_duration=120,
            min_handle_duration=5,
            max_handle_duration=20,
            min_total_duration=30,
            max_total_duration=120,
            lookback_days=140,
        )

        self.assertIsNotNone(_detect_cup_handle_pattern(candles, relaxed_params))
        self.assertIsNone(_detect_cup_handle_pattern(candles))

    def test_default_cup_handle_accepts_longer_total_duration(self) -> None:
        candles = _make_cup_handle(cup_duration=130, handle_duration=10)

        pattern = _detect_cup_handle_pattern(candles)

        self.assertIsNotNone(pattern)
        assert pattern is not None
        self.assertGreaterEqual(pattern["total_duration"], 120)

    def test_default_cup_handle_rejects_deep_handle(self) -> None:
        candles = _make_cup_handle(
            cup_duration=130,
            handle_duration=10,
            handle_low_price=86,
        )
        relaxed_params = CupHandleParams(
            max_handle_pullback_pct=Decimal("15"),
            min_handle_low_position_pct=Decimal("40"),
            max_handle_depth_to_cup_depth_pct=Decimal("60"),
        )

        self.assertIsNone(_detect_cup_handle_pattern(candles))
        self.assertIsNotNone(_detect_cup_handle_pattern(candles, relaxed_params))

    def test_cup_handle_detection_ignores_partial_split_boundary_rows(self) -> None:
        candles = _make_cup_handle(cup_duration=130, handle_duration=10)
        noisy_partial_row = Candle(
            trade_date=date(2026, 4, 10),
            high=Decimal("13680"),
            low=Decimal("13520"),
            close=Decimal("13610"),
            adj_close=None,
            data_status="partial",
        )

        pattern = _detect_cup_handle_pattern([noisy_partial_row, *candles])

        self.assertIsNotNone(pattern)


if __name__ == "__main__":
    unittest.main()
