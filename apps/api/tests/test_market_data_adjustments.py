from __future__ import annotations

import unittest
from dataclasses import dataclass
from decimal import Decimal

from stockanalyse_api.services.market_data_adjustments import adjusted_ohlc
from stockanalyse_api.services.market_data_adjustments import has_extreme_price_gap
from stockanalyse_api.services.market_data_adjustments import is_complete_market_row


@dataclass(slots=True)
class Row:
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    adj_close: Decimal | None
    volume: int | None = 100
    data_status: str = "complete"


class MarketDataAdjustmentTests(unittest.TestCase):
    def test_adjusted_ohlc_scales_prices_by_adjusted_close_factor(self) -> None:
        adjusted = adjusted_ohlc(
            Row(
                open=Decimal("100"),
                high=Decimal("110"),
                low=Decimal("90"),
                close=Decimal("100"),
                adj_close=Decimal("25"),
            )
        )

        self.assertEqual(adjusted.open, Decimal("25"))
        self.assertEqual(adjusted.high, Decimal("27.5"))
        self.assertEqual(adjusted.low, Decimal("22.5"))
        self.assertEqual(adjusted.close, Decimal("25"))

    def test_partial_rows_are_not_complete_for_analysis(self) -> None:
        self.assertFalse(
            is_complete_market_row(
                Row(
                    open=Decimal("100"),
                    high=Decimal("110"),
                    low=Decimal("90"),
                    close=Decimal("100"),
                    adj_close=None,
                    data_status="partial",
                )
            )
        )

    def test_zero_volume_rows_are_not_complete_for_analysis(self) -> None:
        self.assertFalse(
            is_complete_market_row(
                Row(
                    open=Decimal("100"),
                    high=Decimal("110"),
                    low=Decimal("90"),
                    close=Decimal("100"),
                    adj_close=Decimal("100"),
                    volume=0,
                )
            )
        )

    def test_adjusted_ohlc_ignores_implausible_adjustment_factor(self) -> None:
        adjusted = adjusted_ohlc(
            Row(
                open=Decimal("20"),
                high=Decimal("22"),
                low=Decimal("19"),
                close=Decimal("20"),
                adj_close=Decimal("400000000"),
            )
        )

        self.assertEqual(adjusted.open, Decimal("20"))
        self.assertEqual(adjusted.high, Decimal("22"))
        self.assertEqual(adjusted.low, Decimal("19"))
        self.assertEqual(adjusted.close, Decimal("20"))

    def test_detects_extreme_price_gap(self) -> None:
        self.assertTrue(has_extreme_price_gap(Decimal("700"), Decimal("1")))
        self.assertFalse(has_extreme_price_gap(Decimal("100"), Decimal("105")))


if __name__ == "__main__":
    unittest.main()
