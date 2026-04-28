from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from stockanalyse_api.services.ingestion.providers.sec_companyfacts_fundamentals_provider import (
    SecCompanyFactsFundamentalsProvider,
)


class SecCompanyFactsFundamentalsProviderTests(unittest.TestCase):
    def test_parse_companyfacts_keeps_annual_net_income_rows(self) -> None:
        provider = SecCompanyFactsFundamentalsProvider()
        payload = {
            "facts": {
                "us-gaap": {
                    "NetIncomeLoss": {
                        "units": {
                            "USD": [
                                {
                                    "form": "10-Q",
                                    "fp": "Q1",
                                    "end": "2024-03-30",
                                    "filed": "2024-05-03",
                                    "fy": 2024,
                                    "val": 100,
                                },
                                {
                                    "form": "10-K",
                                    "fp": "FY",
                                    "end": "2024-09-28",
                                    "filed": "2024-11-01",
                                    "fy": 2024,
                                    "val": 93736000000,
                                },
                                {
                                    "form": "10-K",
                                    "fp": "FY",
                                    "end": "2024-09-28",
                                    "filed": "2024-11-04",
                                    "fy": 2024,
                                    "val": 93740000000,
                                },
                            ]
                        }
                    }
                }
            }
        }

        rows = provider._parse_companyfacts("AAPL", "US", payload)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].fiscal_year_label, "FY2024")
        self.assertEqual(rows[0].fiscal_year_end_date, date(2024, 9, 28))
        self.assertEqual(rows[0].net_income, Decimal("93740000000"))
        self.assertEqual(rows[0].source_as_of_date, date(2024, 11, 4))
        self.assertEqual(rows[0].data_status, "partial")

    def test_parse_companyfacts_labels_restated_comparative_rows_by_period_end(self) -> None:
        provider = SecCompanyFactsFundamentalsProvider()
        payload = {
            "facts": {
                "us-gaap": {
                    "NetIncomeLoss": {
                        "units": {
                            "USD": [
                                {
                                    "form": "10-K",
                                    "fp": "FY",
                                    "end": "2023-10-31",
                                    "filed": "2023-12-20",
                                    "fy": 2023,
                                    "val": 1240000000,
                                },
                                {
                                    "form": "10-K",
                                    "fp": "FY",
                                    "end": "2023-10-31",
                                    "filed": "2025-12-22",
                                    "fy": 2025,
                                    "val": 1241000000,
                                },
                            ]
                        }
                    }
                }
            }
        }

        rows = provider._parse_companyfacts("A", "US", payload)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].fiscal_year_label, "FY2023")
        self.assertEqual(rows[0].fiscal_year_end_date, date(2023, 10, 31))
        self.assertEqual(rows[0].net_income, Decimal("1241000000"))
        self.assertEqual(rows[0].source_as_of_date, date(2025, 12, 22))

    def test_parse_companyfacts_supports_ifrs_annual_reports_and_non_usd_currency(self) -> None:
        provider = SecCompanyFactsFundamentalsProvider()
        payload = {
            "facts": {
                "ifrs-full": {
                    "ProfitLossAttributableToOwnersOfParent": {
                        "units": {
                            "CAD": [
                                {
                                    "form": "40-F",
                                    "fp": "FY",
                                    "start": "2023-04-01",
                                    "end": "2024-03-31",
                                    "filed": "2024-06-20",
                                    "val": -69326000,
                                },
                                {
                                    "form": "40-F",
                                    "fp": "FY",
                                    "start": "2024-04-01",
                                    "end": "2024-06-30",
                                    "filed": "2025-06-18",
                                    "val": 3754000,
                                },
                                {
                                    "form": "40-F",
                                    "fp": "FY",
                                    "start": "2024-04-01",
                                    "end": "2025-03-31",
                                    "filed": "2025-06-18",
                                    "val": 1591000,
                                },
                            ]
                        }
                    }
                }
            }
        }

        rows = provider._parse_companyfacts("ACB", "US", payload)

        self.assertEqual(len(rows), 2)
        self.assertEqual([row.fiscal_year_label for row in rows], ["FY2024", "FY2025"])
        self.assertEqual(rows[0].net_income_currency, "CAD")
        self.assertEqual(rows[1].net_income, Decimal("1591000"))

    def test_parse_companyfacts_supports_us_gaap_20_f_annual_reports(self) -> None:
        provider = SecCompanyFactsFundamentalsProvider()
        payload = {
            "facts": {
                "us-gaap": {
                    "NetIncomeLoss": {
                        "units": {
                            "USD": [
                                {
                                    "form": "6-K",
                                    "fp": "Q2",
                                    "start": "2025-01-01",
                                    "end": "2025-06-30",
                                    "filed": "2025-07-30",
                                    "val": 1902069000,
                                },
                                {
                                    "form": "20-F",
                                    "fp": "FY",
                                    "start": "2025-01-01",
                                    "end": "2025-12-31",
                                    "filed": "2026-02-12",
                                    "val": 3750612000,
                                },
                            ]
                        }
                    }
                }
            }
        }

        rows = provider._parse_companyfacts("AER", "US", payload)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].fiscal_year_label, "FY2025")
        self.assertEqual(rows[0].net_income, Decimal("3750612000"))


if __name__ == "__main__":
    unittest.main()
