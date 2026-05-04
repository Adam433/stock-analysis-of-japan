from __future__ import annotations

import unittest
from datetime import UTC, datetime
from decimal import Decimal

from stockanalyse_api.services.ingestion.providers.yahoo_finance_fundamentals_provider import (
    YahooFinanceFundamentalsProvider,
)


def _timestamp(year: int, month: int, day: int) -> int:
    return int(datetime(year, month, day, tzinfo=UTC).timestamp())


class YahooFinanceFundamentalsProviderTests(unittest.TestCase):
    def test_parse_quote_summary_payload_applies_current_multiples_to_latest_fiscal_year(self) -> None:
        provider = YahooFinanceFundamentalsProvider()
        payload = {
            "quoteSummary": {
                "result": [
                    {
                        "incomeStatementHistory": {
                            "incomeStatementHistory": [
                                {
                                    "endDate": {"raw": _timestamp(2023, 3, 31)},
                                    "netIncome": {"raw": 1000},
                                },
                                {
                                    "endDate": {"raw": _timestamp(2024, 3, 31)},
                                    "netIncome": {"raw": 1200},
                                },
                            ]
                        },
                        "summaryDetail": {
                            "trailingPE": {"raw": 10.1},
                        },
                        "defaultKeyStatistics": {
                            "priceToBook": {"raw": 1.11},
                        },
                        "cashflowStatementHistory": {
                            "cashflowStatements": [
                                {
                                    "endDate": {"raw": _timestamp(2024, 3, 31)},
                                    "totalCashFromOperatingActivities": {"raw": 1500},
                                    "capitalExpenditures": {"raw": -200},
                                }
                            ]
                        },
                    }
                ],
                "error": None,
            }
        }

        rows = provider._parse_quote_summary_payload("7203.T", "TSE", payload)

        self.assertEqual([row.fiscal_year_label for row in rows], ["FY2023", "FY2024"])
        self.assertEqual(rows[0].data_status, "partial")
        self.assertIsNone(rows[0].pe)
        self.assertIsNone(rows[0].pb)
        self.assertEqual(rows[1].data_status, "complete")
        self.assertEqual(rows[1].pe, Decimal("10.1"))
        self.assertEqual(rows[1].pb, Decimal("1.11"))
        self.assertEqual(rows[1].operating_cash_flow, Decimal("1500"))
        self.assertEqual(rows[1].free_cash_flow, Decimal("1300"))

    def test_parse_quote_summary_payload_uses_financial_currency_when_available(self) -> None:
        provider = YahooFinanceFundamentalsProvider(default_currency="USD")
        payload = {
            "quoteSummary": {
                "result": [
                    {
                        "incomeStatementHistory": {
                            "incomeStatementHistory": [
                                {
                                    "endDate": {"raw": _timestamp(2025, 12, 31)},
                                    "netIncome": {"raw": 4720000000},
                                }
                            ]
                        },
                        "price": {
                            "financialCurrency": "CAD",
                            "currency": "USD",
                        },
                    }
                ],
                "error": None,
            }
        }

        rows = provider._parse_quote_summary_payload("CNI", "US", payload)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].net_income_currency, "CAD")
        self.assertEqual(rows[0].source, "yahoo_finance_fundamentals")


if __name__ == "__main__":
    unittest.main()
