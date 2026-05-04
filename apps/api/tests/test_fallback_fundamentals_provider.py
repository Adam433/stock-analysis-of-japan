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

    def test_merges_later_provider_valuation_into_primary_rows(self) -> None:
        sec_rows = [
            ProviderFundamentalsAnnual(
                symbol="AAPL",
                exchange="US",
                fiscal_year_end_date=date(2024, 9, 30),
                fiscal_year_label="FY2024",
                net_income=Decimal("93736000000"),
                net_income_currency="USD",
                source="sec_companyfacts",
                source_as_of_date=date(2025, 10, 31),
                data_status="partial",
            )
        ]
        yahoo_rows = [
            ProviderFundamentalsAnnual(
                symbol="AAPL",
                exchange="US",
                fiscal_year_end_date=date(2024, 9, 30),
                fiscal_year_label="FY2024",
                net_income=Decimal("93736000000"),
                net_income_currency="USD",
                operating_cash_flow=Decimal("118254000000"),
                diluted_eps=Decimal("6.08"),
                stockholders_equity=Decimal("56950000000"),
                weighted_average_diluted_shares=Decimal("15408095000"),
                pe=Decimal("29.5"),
                pb=Decimal("43.2"),
                source="yahoo_finance_fundamentals_us",
                source_as_of_date=date(2026, 5, 4),
                data_status="complete",
            )
        ]
        sec = _Provider(sec_rows)
        yahoo = _Provider(yahoo_rows)
        provider = FallbackFundamentalsProvider((sec, yahoo))

        rows = provider.fetch_annual_fundamentals("AAPL", exchange="US")

        self.assertEqual(rows, sec_rows)
        self.assertEqual(rows[-1].source, "sec_companyfacts")
        self.assertEqual(rows[-1].pe, Decimal("29.5"))
        self.assertEqual(rows[-1].pb, Decimal("43.2"))
        self.assertEqual(rows[-1].operating_cash_flow, Decimal("118254000000"))
        self.assertEqual(rows[-1].diluted_eps, Decimal("6.08"))
        self.assertEqual(rows[-1].stockholders_equity, Decimal("56950000000"))
        self.assertEqual(rows[-1].weighted_average_diluted_shares, Decimal("15408095000"))
        self.assertEqual(rows[-1].data_status, "complete")
        self.assertEqual(sec.calls, [("AAPL", "US")])
        self.assertEqual(yahoo.calls, [("AAPL", "US")])

    def test_cash_flow_without_valuation_does_not_skip_valuation_provider(self) -> None:
        sec_rows = [
            ProviderFundamentalsAnnual(
                symbol="MSFT",
                exchange="US",
                fiscal_year_end_date=date(2025, 6, 30),
                fiscal_year_label="FY2025",
                net_income=Decimal("101832000000"),
                operating_cash_flow=Decimal("136162000000"),
                free_cash_flow=Decimal("71611000000"),
                source="sec_companyfacts",
                source_as_of_date=date(2025, 7, 30),
                data_status="partial",
            )
        ]
        yahoo_rows = [
            ProviderFundamentalsAnnual(
                symbol="MSFT",
                exchange="US",
                fiscal_year_end_date=date(2025, 6, 30),
                fiscal_year_label="FY2025",
                net_income=Decimal("101832000000"),
                pe=Decimal("36.2"),
                pb=Decimal("11.8"),
                source="yahoo_finance_fundamentals_us",
                source_as_of_date=date(2026, 5, 4),
                data_status="complete",
            )
        ]
        sec = _Provider(sec_rows)
        yahoo = _Provider(yahoo_rows)
        provider = FallbackFundamentalsProvider((sec, yahoo))

        rows = provider.fetch_annual_fundamentals("MSFT", exchange="US")

        self.assertEqual(rows[-1].operating_cash_flow, Decimal("136162000000"))
        self.assertEqual(rows[-1].free_cash_flow, Decimal("71611000000"))
        self.assertEqual(rows[-1].pe, Decimal("36.2"))
        self.assertEqual(rows[-1].pb, Decimal("11.8"))
        self.assertEqual(sec.calls, [("MSFT", "US")])
        self.assertEqual(yahoo.calls, [("MSFT", "US")])

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
