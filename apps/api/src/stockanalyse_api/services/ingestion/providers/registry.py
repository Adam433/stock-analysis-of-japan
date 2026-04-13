from __future__ import annotations

from pathlib import Path

from stockanalyse_api.services.ingestion.providers.static_provider import StaticFixtureProvider

MVP_MARKET_SCOPE = "jp_equities_eod"
BACKEND_ONLY_BOUNDARY = "backend_only"


def build_ingestion_provider(provider_name: str, *, fixture_path: Path | None = None):
    if provider_name == "static_fixture":
        if fixture_path is None:
            raise ValueError("fixture_path is required for the static_fixture provider.")
        provider = StaticFixtureProvider(fixture_path)
    else:
        raise ValueError(f"Unsupported provider: {provider_name}")

    if getattr(provider, "credential_boundary", None) != BACKEND_ONLY_BOUNDARY:
        raise ValueError("Provider credential boundary must remain backend_only.")
    if getattr(provider, "market_scope", None) != MVP_MARKET_SCOPE:
        raise ValueError("Provider market scope must remain Japan-equity EOD for MVP.")
    return provider
