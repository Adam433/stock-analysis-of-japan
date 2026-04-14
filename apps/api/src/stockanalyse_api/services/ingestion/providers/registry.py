from __future__ import annotations

from pathlib import Path

from stockanalyse_api.services.ingestion.providers.local_csv_directory_provider import (
    LocalCsvDirectoryProvider,
)
from stockanalyse_api.services.ingestion.providers.static_provider import StaticFixtureProvider
from stockanalyse_api.services.ingestion.providers.yahoo_finance_chart_provider import (
    YahooFinanceChartProvider,
)

MVP_MARKET_SCOPE = "jp_equities_eod"
BACKEND_ONLY_BOUNDARY = "backend_only"


def build_ingestion_provider(
    provider_name: str,
    *,
    fixture_path: Path | None = None,
    csv_dir: Path | None = None,
    symbols_file: Path | None = None,
):
    if provider_name == "static_fixture":
        if fixture_path is None:
            raise ValueError("fixture_path is required for the static_fixture provider.")
        provider = StaticFixtureProvider(fixture_path)
    elif provider_name == "local_csv_directory":
        if csv_dir is None:
            raise ValueError("csv_dir is required for the local_csv_directory provider.")
        provider = LocalCsvDirectoryProvider(csv_dir, symbols_file=symbols_file)
    elif provider_name == "yahoo_finance_chart":
        if symbols_file is None:
            raise ValueError("symbols_file is required for the yahoo_finance_chart provider.")
        provider = YahooFinanceChartProvider(symbols_file=symbols_file)
    else:
        raise ValueError(f"Unsupported provider: {provider_name}")

    if getattr(provider, "credential_boundary", None) != BACKEND_ONLY_BOUNDARY:
        raise ValueError("Provider credential boundary must remain backend_only.")
    if getattr(provider, "market_scope", None) != MVP_MARKET_SCOPE:
        raise ValueError("Provider market scope must remain Japan-equity EOD for MVP.")
    return provider
