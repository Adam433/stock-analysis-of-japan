from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar


class EodMarketDataProvider(Protocol):
    provider_name: str
    market_scope: str
    credential_boundary: str

    def fetch_daily_bars(self, symbols: list[str]) -> Iterable[ProviderDailyBar]:
        """Return raw provider daily bars for the requested symbols."""
