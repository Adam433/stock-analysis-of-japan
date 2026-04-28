from __future__ import annotations

from pathlib import Path

from stockanalyse_api.services.ingestion.providers.alpha_vantage_daily_adjusted_provider import (
    AlphaVantageDailyAdjustedProvider,
)
from stockanalyse_api.services.ingestion.providers.local_csv_directory_provider import (
    LocalCsvDirectoryProvider,
)
from stockanalyse_api.services.ingestion.providers.sec_companyfacts_fundamentals_provider import (
    SecCompanyFactsFundamentalsProvider,
)
from stockanalyse_api.services.ingestion.providers.static_provider import StaticFixtureProvider
from stockanalyse_api.services.ingestion.providers.yahoo_finance_chart_provider import (
    YahooFinanceChartProvider,
)
from stockanalyse_api.services.ingestion.providers.yahoo_finance_fundamentals_provider import (
    YahooFinanceFundamentalsProvider,
)

ALLOWED_MARKET_SCOPES = {
    "jp_equities_eod",
    "us_equities_eod",
    "us_equities_fundamentals",
}
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
    elif provider_name == "yahoo_finance_chart_us":
        if symbols_file is None:
            raise ValueError("symbols_file is required for the yahoo_finance_chart_us provider.")
        provider = YahooFinanceChartProvider(
            symbols_file=symbols_file,
            provider_name="yahoo_finance_chart_us",
            market_scope="us_equities_eod",
            exchange="US",
            currency="USD",
        )
    elif provider_name == "alpha_vantage_daily_adjusted":
        if symbols_file is None:
            raise ValueError("symbols_file is required for the alpha_vantage_daily_adjusted provider.")
        provider = AlphaVantageDailyAdjustedProvider(symbols_file=symbols_file)
    elif provider_name == "yahoo_finance_fundamentals":
        provider = YahooFinanceFundamentalsProvider()
    elif provider_name == "sec_companyfacts":
        provider = SecCompanyFactsFundamentalsProvider()
    else:
        raise ValueError(f"Unsupported provider: {provider_name}")

    if getattr(provider, "credential_boundary", None) != BACKEND_ONLY_BOUNDARY:
        raise ValueError("Provider credential boundary must remain backend_only.")
    if getattr(provider, "market_scope", None) not in ALLOWED_MARKET_SCOPES:
        raise ValueError("Provider market scope is not enabled.")
    return provider
