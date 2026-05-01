from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal
from urllib.error import HTTPError

from stockanalyse_api.services.ingestion.provider_models import ProviderFundamentalsAnnual
from stockanalyse_api.services.ingestion.providers.fallback_fundamentals_provider import (
    FallbackFundamentalsProvider,
)


class _Provider:
    def __init__(self, rows: list[ProviderFundamentalsAnnual] | Exception) -> None:
        self.rows = rows
        self.calls: list[tuple[str, str]] = []

    def fetch_annual_fundamentals(
        self,
        symbol: str,
        *,
        exchange: str = "US",
    ) -> list[ProviderFundamentalsAnnual]:
        self.calls.append((symbol, exchange))
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows


class FallbackFundamentalsProviderTests(unittest.TestCase):
    def test_returns_first_non_empty_provider_rows(self) -> None:
        first = _Provider([])
        second_rows = [
            ProviderFundamentalsAnnual(
                symbol="CNI",
                exchange="US",
                fiscal_year_end_date=date(2025, 12, 31),
                fiscal_year_label="FY2025",
                net_income=Decimal("4720000000"),
                net_income_currency="CAD",
                source="yahoo_finance_fundamentals_us",
                source_as_of_date=date(2026, 5, 1),
                data_status="partial",
            )
        ]
        second = _Provider(second_rows)
        provider = FallbackFundamentalsProvider((first, second))

        rows = provider.fetch_annual_fundamentals("CNI", exchange="US")

        self.assertEqual(rows, second_rows)
        self.assertEqual(first.calls, [("CNI", "US")])
        self.assertEqual(second.calls, [("CNI", "US")])

    def test_continues_when_provider_raises(self) -> None:
        first = _Provider(RuntimeError("unavailable"))
        second_rows = [
            ProviderFundamentalsAnnual(
                symbol="AAPL",
                exchange="US",
                fiscal_year_end_date=date(2025, 9, 30),
                fiscal_year_label="FY2025",
                net_income=Decimal("100"),
            )
        ]
        provider = FallbackFundamentalsProvider((first, _Provider(second_rows)))

        rows = provider.fetch_annual_fundamentals("AAPL", exchange="US")

        self.assertEqual(rows, second_rows)

    def test_disables_provider_after_rate_limit_error(self) -> None:
        limited = _Provider(HTTPError("https://example.test", 429, "Too Many Requests", {}, None))
        fallback = _Provider([])
        provider = FallbackFundamentalsProvider((limited, fallback))

        provider.fetch_annual_fundamentals("ACAA", exchange="US")
        provider.fetch_annual_fundamentals("AGBK", exchange="US")

        self.assertEqual(limited.calls, [("ACAA", "US")])
        self.assertEqual(fallback.calls, [("ACAA", "US"), ("AGBK", "US")])


if __name__ == "__main__":
    unittest.main()
