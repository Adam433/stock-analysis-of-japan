from __future__ import annotations

import unittest
from pathlib import Path

from stockanalyse_api.services.ingestion.providers.registry import (
    BACKEND_ONLY_BOUNDARY,
    MVP_MARKET_SCOPE,
    build_ingestion_provider,
)


class SecurityBoundaryTests(unittest.TestCase):
    def test_ingestion_provider_registry_enforces_backend_only_boundary(self) -> None:
        provider = build_ingestion_provider(
            "static_fixture",
            fixture_path=Path("tests/fixtures/japan_equity_eod_fixture.json"),
        )

        self.assertEqual(provider.credential_boundary, BACKEND_ONLY_BOUNDARY)
        self.assertEqual(provider.market_scope, MVP_MARKET_SCOPE)

    def test_local_csv_directory_provider_stays_within_backend_only_boundary(self) -> None:
        provider = build_ingestion_provider(
            "local_csv_directory",
            csv_dir=Path("data/archive/local_seed_csv"),
            symbols_file=Path("data/tse_common_stock_symbols.txt"),
        )

        self.assertEqual(provider.credential_boundary, BACKEND_ONLY_BOUNDARY)
        self.assertEqual(provider.market_scope, MVP_MARKET_SCOPE)

    def test_yahoo_finance_chart_provider_stays_within_backend_only_boundary(self) -> None:
        provider = build_ingestion_provider(
            "yahoo_finance_chart",
            symbols_file=Path("data/tse_common_stock_symbols.txt"),
        )

        self.assertEqual(provider.credential_boundary, BACKEND_ONLY_BOUNDARY)
        self.assertEqual(provider.market_scope, MVP_MARKET_SCOPE)

    def test_yahoo_finance_chart_us_provider_stays_within_backend_only_boundary(self) -> None:
        provider = build_ingestion_provider(
            "yahoo_finance_chart_us",
            symbols_file=Path("data/us_stock_symbols.txt"),
        )

        self.assertEqual(provider.credential_boundary, BACKEND_ONLY_BOUNDARY)
        self.assertEqual(provider.market_scope, "us_equities_eod")


if __name__ == "__main__":
    unittest.main()
