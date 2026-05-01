from __future__ import annotations

from collections.abc import Sequence
from urllib.error import HTTPError

from stockanalyse_api.services.ingestion.provider_models import ProviderFundamentalsAnnual


class FallbackFundamentalsProvider:
    provider_name = "fallback_fundamentals"
    market_scope = "us_equities_fundamentals"
    credential_boundary = "backend_only"

    def __init__(
        self,
        providers: Sequence[object],
        *,
        provider_name: str = "fallback_fundamentals",
        market_scope: str = "us_equities_fundamentals",
    ) -> None:
        if not providers:
            raise ValueError("At least one fundamentals provider is required.")
        self.providers = tuple(providers)
        self.provider_name = provider_name
        self.market_scope = market_scope
        self._disabled_provider_ids: set[int] = set()

    def fetch_annual_fundamentals(
        self,
        symbol: str,
        *,
        exchange: str = "US",
    ) -> list[ProviderFundamentalsAnnual]:
        for provider in self.providers:
            provider_id = id(provider)
            if provider_id in self._disabled_provider_ids:
                continue
            try:
                rows = provider.fetch_annual_fundamentals(symbol, exchange=exchange)
            except HTTPError as exc:
                if exc.code in {401, 403, 429}:
                    self._disabled_provider_ids.add(provider_id)
                continue
            except Exception:
                continue
            if rows:
                return rows
        return []
