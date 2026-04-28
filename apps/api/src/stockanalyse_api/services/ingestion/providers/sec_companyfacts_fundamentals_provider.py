from __future__ import annotations

import json
import os
import ssl
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from urllib.parse import quote
from urllib.request import Request, urlopen

try:
    import certifi
except ImportError:  # pragma: no cover - certifi is optional at runtime.
    certifi = None

from stockanalyse_api.services.ingestion.provider_models import ProviderFundamentalsAnnual

NET_INCOME_TAGS = (
    "NetIncomeLoss",
    "ProfitLoss",
    "NetIncomeLossAvailableToCommonStockholdersBasic",
)
IFRS_NET_INCOME_TAGS = (
    "ProfitLossAttributableToOwnersOfParent",
    "ProfitLossAttributableToOrdinaryEquityHoldersOfParentEntity",
    "ProfitLoss",
)
ANNUAL_FORMS = {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}
MIN_ANNUAL_DURATION_DAYS = 300


class SecCompanyFactsFundamentalsProvider:
    provider_name = "sec_companyfacts"
    market_scope = "us_equities_fundamentals"
    credential_boundary = "backend_only"

    def __init__(
        self,
        *,
        timeout_seconds: int = 30,
        user_agent: str | None = None,
        min_request_interval_seconds: float | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent or os.environ.get(
            "STOCKANALYSE_SEC_USER_AGENT",
            "stockAnalyse/0.1 contact=local@example.com",
        )
        self.min_request_interval_seconds = (
            min_request_interval_seconds
            if min_request_interval_seconds is not None
            else float(os.environ.get("STOCKANALYSE_SEC_MIN_REQUEST_INTERVAL_SECONDS", "0.12"))
        )
        self._last_request_at: float | None = None
        self._ticker_to_cik: dict[str, int] | None = None

    def fetch_annual_fundamentals(
        self,
        symbol: str,
        *,
        exchange: str = "US",
    ) -> list[ProviderFundamentalsAnnual]:
        cik = self._resolve_cik(symbol)
        if cik is None:
            return []
        payload = self._fetch_companyfacts(cik)
        return self._parse_companyfacts(symbol, exchange, payload)

    def _resolve_cik(self, symbol: str) -> int | None:
        if self._ticker_to_cik is None:
            payload = self._fetch_json("https://www.sec.gov/files/company_tickers.json")
            mapping: dict[str, int] = {}
            if isinstance(payload, dict):
                for row in payload.values():
                    if not isinstance(row, dict):
                        continue
                    ticker = row.get("ticker")
                    cik = row.get("cik_str")
                    if ticker and cik is not None:
                        mapping[str(ticker).upper()] = int(cik)
            self._ticker_to_cik = mapping
        return self._ticker_to_cik.get(symbol.replace(".", "-").upper())

    def _fetch_companyfacts(self, cik: int) -> dict[str, object]:
        return self._fetch_json(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
        )

    def _fetch_json(self, url: str) -> dict[str, object]:
        self._throttle_request()
        request = Request(
            quote(url, safe=":/?=&."),
            headers={
                "User-Agent": self.user_agent,
                "Accept": "application/json",
            },
        )
        ssl_context = (
            ssl.create_default_context(cafile=certifi.where())
            if certifi is not None
            else ssl.create_default_context()
        )
        with urlopen(request, timeout=self.timeout_seconds, context=ssl_context) as response:
            return json.loads(response.read().decode("utf-8"))

    def _throttle_request(self) -> None:
        if self.min_request_interval_seconds <= 0:
            return
        now = time.monotonic()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            if elapsed < self.min_request_interval_seconds:
                time.sleep(self.min_request_interval_seconds - elapsed)
        self._last_request_at = time.monotonic()

    def _parse_companyfacts(
        self,
        symbol: str,
        exchange: str,
        payload: dict[str, object],
    ) -> list[ProviderFundamentalsAnnual]:
        facts = payload.get("facts")
        if not isinstance(facts, dict):
            return []

        candidates: list[list[ProviderFundamentalsAnnual]] = []
        for taxonomy_name, tags in (
            ("us-gaap", NET_INCOME_TAGS),
            ("ifrs-full", IFRS_NET_INCOME_TAGS),
        ):
            taxonomy = facts.get(taxonomy_name) if isinstance(facts, dict) else None
            if not isinstance(taxonomy, dict):
                continue
            for tag in tags:
                fact = taxonomy.get(tag)
                units = fact.get("units") if isinstance(fact, dict) else None
                if not isinstance(units, dict):
                    continue
                for currency, unit_rows in self._iter_currency_unit_rows(units):
                    parsed = self._parse_net_income_rows(
                        symbol,
                        exchange,
                        unit_rows,
                        currency=currency,
                    )
                    if parsed:
                        candidates.append(parsed[-10:])
        if not candidates:
            return []
        return max(
            candidates,
            key=lambda rows: (rows[-1].fiscal_year_end_date, len(rows)),
        )

    def _parse_net_income_rows(
        self,
        symbol: str,
        exchange: str,
        rows: list[object],
        *,
        currency: str = "USD",
    ) -> list[ProviderFundamentalsAnnual]:
        by_fiscal_year_end: dict[date, ProviderFundamentalsAnnual] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("form") not in ANNUAL_FORMS or row.get("fp") != "FY":
                continue
            fiscal_year_end = self._read_date(row.get("end"))
            fiscal_year_start = self._read_date(row.get("start"))
            filed_date = self._read_date(row.get("filed")) or date.today()
            value = row.get("val")
            if fiscal_year_end is None or value is None:
                continue
            if not self._is_annual_duration(fiscal_year_start, fiscal_year_end):
                continue
            existing = by_fiscal_year_end.get(fiscal_year_end)
            if existing is not None and existing.source_as_of_date >= filed_date:
                continue
            by_fiscal_year_end[fiscal_year_end] = ProviderFundamentalsAnnual(
                symbol=symbol,
                exchange=exchange,
                fiscal_year_end_date=fiscal_year_end,
                fiscal_year_label=f"FY{fiscal_year_end.year}",
                net_income=Decimal(str(value)),
                net_income_currency=currency,
                source=self.provider_name,
                source_as_of_date=filed_date,
                data_status="partial",
            )

        return sorted(by_fiscal_year_end.values(), key=lambda item: item.fiscal_year_end_date)

    @staticmethod
    def _iter_currency_unit_rows(units: dict[str, object]) -> list[tuple[str, list[object]]]:
        rows_by_currency = [
            (currency, rows)
            for currency, rows in units.items()
            if isinstance(currency, str)
            and len(currency) == 3
            and currency.isalpha()
            and isinstance(rows, list)
        ]
        return sorted(rows_by_currency, key=lambda item: (item[0] != "USD", item[0]))

    @staticmethod
    def _is_annual_duration(start: date | None, end: date) -> bool:
        if start is None:
            return True
        return (end - start).days >= MIN_ANNUAL_DURATION_DAYS

    @staticmethod
    def _read_date(value: object) -> date | None:
        if not value:
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, UTC).date()
        return date.fromisoformat(str(value))
