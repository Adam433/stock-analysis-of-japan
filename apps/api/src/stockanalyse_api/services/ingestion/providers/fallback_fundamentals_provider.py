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
        best_rows: list[ProviderFundamentalsAnnual] | None = None
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
                best_rows = (
                    rows
                    if best_rows is None
                    else _merge_supplemental_fields(best_rows, rows)
                )
                if _has_latest_valuation(best_rows):
                    return best_rows
        return best_rows or []


def _has_latest_valuation(rows: list[ProviderFundamentalsAnnual]) -> bool:
    if not rows:
        return False
    latest = rows[-1]
    return (
        latest.pe is not None
        or latest.pb is not None
        or latest.diluted_eps is not None
        or (
            latest.stockholders_equity is not None
            and latest.weighted_average_diluted_shares is not None
        )
    )


def _merge_supplemental_fields(
    primary_rows: list[ProviderFundamentalsAnnual],
    supplemental_rows: list[ProviderFundamentalsAnnual],
) -> list[ProviderFundamentalsAnnual]:
    supplemental_field_rows = [
        row
        for row in supplemental_rows
        if any(
            value is not None
            for value in (
                row.pe,
                row.pb,
                row.operating_cash_flow,
                row.free_cash_flow,
                row.diluted_eps,
                row.stockholders_equity,
                row.weighted_average_diluted_shares,
            )
        )
    ]
    if not primary_rows or not supplemental_field_rows:
        return primary_rows

    valuation_row = supplemental_field_rows[-1]
    target = next(
        (
            row
            for row in reversed(primary_rows)
            if row.fiscal_year_end_date == valuation_row.fiscal_year_end_date
        ),
        primary_rows[-1],
    )
    if target.pe is None:
        target.pe = valuation_row.pe
    if target.pb is None:
        target.pb = valuation_row.pb
    if target.operating_cash_flow is None:
        target.operating_cash_flow = valuation_row.operating_cash_flow
    if target.free_cash_flow is None:
        target.free_cash_flow = valuation_row.free_cash_flow
    if target.diluted_eps is None:
        target.diluted_eps = valuation_row.diluted_eps
    if target.stockholders_equity is None:
        target.stockholders_equity = valuation_row.stockholders_equity
    if target.weighted_average_diluted_shares is None:
        target.weighted_average_diluted_shares = valuation_row.weighted_average_diluted_shares
    if target.net_income is not None and target.pe is not None and target.pb is not None:
        target.data_status = "complete"
    return primary_rows
