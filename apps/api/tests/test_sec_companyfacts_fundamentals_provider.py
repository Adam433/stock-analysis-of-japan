from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from stockanalyse_api.services.ingestion.providers.sec_companyfacts_fundamentals_provider import (
    SecCompanyFactsFundamentalsProvider,
)


class SecCompanyFactsFundamentalsProviderTests(unittest.TestCase):
    def test_add_exchange_ticker_mapping_fills_symbols_missing_from_company_mapping(self) -> None:
        mapping: dict[str, int] = {}
        SecCompanyFactsFundamentalsProvider._add_company_ticker_mapping(
            mapping,
            {"0": {"ticker": "AAPL", "cik_str": 320193}},
        )
        SecCompanyFactsFundamentalsProvider._add_exchange_ticker_mapping(
            mapping,
            {
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [
                    [1759186, "Coeptis Therapeutics Holdings, Inc.", "ZSQR", "Nasdaq"],
                    [999999, "Apple Duplicate", "AAPL", "Nasdaq"],
                ],
            },
        )

        self.assertEqual(mapping["AAPL"], 320193)
        self.assertEqual(mapping["ZSQR"], 1759186)

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
                    },
                    "NetCashProvidedByUsedInOperatingActivities": {
                        "units": {
                            "USD": [
                                {
                                    "form": "10-K",
                                    "fp": "FY",
                                    "end": "2024-09-28",
                                    "filed": "2024-11-04",
                                    "fy": 2024,
                                    "val": 118254000000,
                                }
                            ]
                        }
                    },
                    "PaymentsToAcquirePropertyPlantAndEquipment": {
                        "units": {
                            "USD": [
                                {
                                    "form": "10-K",
                                    "fp": "FY",
                                    "end": "2024-09-28",
                                    "filed": "2024-11-04",
                                    "fy": 2024,
                                    "val": 9447000000,
                                }
                            ]
                        }
                    },
                    "EarningsPerShareDiluted": {
                        "units": {
                            "USD/shares": [
                                {
                                    "form": "10-K",
                                    "fp": "FY",
                                    "end": "2024-09-28",
                                    "filed": "2024-11-04",
                                    "fy": 2024,
                                    "val": 6.08,
                                }
                            ]
                        }
                    },
                    "StockholdersEquity": {
                        "units": {
                            "USD": [
                                {
                                    "form": "10-K",
                                    "fp": "FY",
                                    "end": "2024-09-28",
                                    "filed": "2024-11-04",
                                    "fy": 2024,
                                    "val": 56950000000,
                                }
                            ]
                        }
                    },
                    "WeightedAverageNumberOfDilutedSharesOutstanding": {
                        "units": {
                            "shares": [
                                {
                                    "form": "10-K",
                                    "fp": "FY",
                                    "end": "2024-09-28",
                                    "filed": "2024-11-04",
                                    "fy": 2024,
                                    "val": 15408095000,
                                }
                            ]
                        }
                    },
                }
            }
        }

        rows = provider._parse_companyfacts("AAPL", "US", payload)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].fiscal_year_label, "FY2024")
        self.assertEqual(rows[0].fiscal_year_end_date, date(2024, 9, 28))
        self.assertEqual(rows[0].net_income, Decimal("93740000000"))
        self.assertEqual(rows[0].operating_cash_flow, Decimal("118254000000"))
        self.assertEqual(rows[0].free_cash_flow, Decimal("108807000000"))
        self.assertEqual(rows[0].diluted_eps, Decimal("6.08"))
        self.assertEqual(rows[0].stockholders_equity, Decimal("56950000000"))
        self.assertEqual(rows[0].weighted_average_diluted_shares, Decimal("15408095000"))
        self.assertEqual(rows[0].source_as_of_date, date(2024, 11, 4))
        self.assertEqual(rows[0].data_status, "partial")

    def test_parse_companyfacts_prefers_primary_fiscal_year_over_later_comparative_rows(self) -> None:
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
        self.assertEqual(rows[0].net_income, Decimal("1240000000"))
        self.assertEqual(rows[0].source_as_of_date, date(2023, 12, 20))

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

    def test_parse_companyfacts_falls_back_to_annual_form_without_fp_when_full_year(self) -> None:
        provider = SecCompanyFactsFundamentalsProvider()
        payload = {
            "facts": {
                "ifrs-full": {
                    "ProfitLoss": {
                        "units": {
                            "CAD": [
                                {
                                    "form": "40-F",
                                    "fp": None,
                                    "start": "2025-01-01",
                                    "end": "2025-12-31",
                                    "filed": "2026-03-31",
                                    "val": -19722261,
                                }
                            ]
                        }
                    }
                }
            }
        }

        rows = provider._parse_companyfacts("AEC", "US", payload)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].fiscal_year_label, "FY2025")
        self.assertEqual(rows[0].net_income, Decimal("-19722261"))
        self.assertEqual(rows[0].net_income_currency, "CAD")

    def test_parse_companyfacts_falls_back_to_full_year_6_k_when_no_annual_report_exists(self) -> None:
        provider = SecCompanyFactsFundamentalsProvider()
        payload = {
            "facts": {
                "us-gaap": {
                    "NetIncomeLoss": {
                        "units": {
                            "CAD": [
                                {
                                    "form": "6-K",
                                    "fp": "FY",
                                    "start": "2025-01-01",
                                    "end": "2025-12-31",
                                    "filed": "2026-02-04",
                                    "val": 4720000000,
                                }
                            ]
                        }
                    }
                }
            }
        }

        rows = provider._parse_companyfacts("CNI", "US", payload)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].fiscal_year_label, "FY2025")
        self.assertEqual(rows[0].net_income, Decimal("4720000000"))
        self.assertEqual(rows[0].net_income_currency, "CAD")

    def test_parse_companyfacts_ignores_short_supplemental_periods(self) -> None:
        provider = SecCompanyFactsFundamentalsProvider()
        payload = {
            "facts": {
                "ifrs-full": {
                    "ProfitLoss": {
                        "units": {
                            "USD": [
                                {
                                    "form": "6-K",
                                    "fp": None,
                                    "start": "2023-06-22",
                                    "end": "2023-12-31",
                                    "filed": "2026-02-20",
                                    "val": -11431,
                                }
                            ]
                        }
                    }
                }
            }
        }

        rows = provider._parse_companyfacts("KWM", "US", payload)

        self.assertEqual(rows, [])

    def test_parse_companyfacts_falls_back_to_registration_statement_full_year(self) -> None:
        provider = SecCompanyFactsFundamentalsProvider()
        payload = {
            "facts": {
                "us-gaap": {
                    "NetIncomeLoss": {
                        "units": {
                            "USD": [
                                {
                                    "form": "S-1",
                                    "fp": None,
                                    "start": "2024-07-01",
                                    "end": "2025-06-30",
                                    "filed": "2026-04-17",
                                    "val": -14065948,
                                }
                            ]
                        }
                    }
                }
            }
        }

        rows = provider._parse_companyfacts("CAST", "US", payload)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].fiscal_year_label, "FY2025")
        self.assertEqual(rows[0].net_income, Decimal("-14065948"))


if __name__ == "__main__":
    unittest.main()
