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


if __name__ == "__main__":
    unittest.main()
