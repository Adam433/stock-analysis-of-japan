from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import Protocol

from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar
from stockanalyse_api.services.ingestion.provider_models import ProviderInstrument


class EodMarketDataProvider(Protocol):
    provider_name: str
    market_scope: str
    credential_boundary: str

    def list_supported_instruments(self) -> list[ProviderInstrument]:
        """Return the provider-supported instrument universe for full refresh runs."""

    def fetch_daily_bars(
        self,
        symbols: list[str],
        *,
        start_after_by_symbol: dict[str, date] | None = None,
    ) -> Iterable[ProviderDailyBar]:
        """Return raw provider daily bars for the requested symbols."""
