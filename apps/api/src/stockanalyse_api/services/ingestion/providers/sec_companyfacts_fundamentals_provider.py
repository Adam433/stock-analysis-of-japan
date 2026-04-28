from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from decimal import Decimal
from urllib.parse import quote
from urllib.request import Request, urlopen

from stockanalyse_api.services.ingestion.provider_models import ProviderFundamentalsAnnual

NET_INCOME_TAGS = (
    "NetIncomeLoss",
    "ProfitLoss",
    "NetIncomeLossAvailableToCommonStockholdersBasic",
)


class SecCompanyFactsFundamentalsProvider:
    provider_name = "sec_companyfacts"
    market_scope = "us_equities_fundamentals"
    credential_boundary = "backend_only"

    def __init__(self, *, timeout_seconds: int = 30, user_agent: str | None = None) -> None:
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent or os.environ.get(
            "STOCKANALYSE_SEC_USER_AGENT",
            "stockAnalyse/0.1 contact=local@example.com",
        )
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
        request = Request(
            quote(url, safe=":/?=&."),
            headers={
                "User-Agent": self.user_agent,
                "Accept": "application/json",
            },
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))

    def _parse_companyfacts(
        self,
        symbol: str,
        exchange: str,
        payload: dict[str, object],
    ) -> list[ProviderFundamentalsAnnual]:
        facts = payload.get("facts")
        us_gaap = facts.get("us-gaap") if isinstance(facts, dict) else None
        if not isinstance(us_gaap, dict):
            return []

        for tag in NET_INCOME_TAGS:
            fact = us_gaap.get(tag)
            units = fact.get("units") if isinstance(fact, dict) else None
            usd_rows = units.get("USD") if isinstance(units, dict) else None
            if isinstance(usd_rows, list):
                parsed = self._parse_net_income_rows(symbol, exchange, usd_rows)
                if parsed:
                    return parsed[-10:]
        return []

    def _parse_net_income_rows(
        self,
        symbol: str,
        exchange: str,
        rows: list[object],
    ) -> list[ProviderFundamentalsAnnual]:
        by_fiscal_year_end: dict[date, ProviderFundamentalsAnnual] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("form") != "10-K" or row.get("fp") != "FY":
                continue
            fiscal_year_end = self._read_date(row.get("end"))
            filed_date = self._read_date(row.get("filed")) or date.today()
            value = row.get("val")
            fiscal_year = row.get("fy")
            if fiscal_year_end is None or value is None:
                continue
            existing = by_fiscal_year_end.get(fiscal_year_end)
            if existing is not None and existing.source_as_of_date >= filed_date:
                continue
            by_fiscal_year_end[fiscal_year_end] = ProviderFundamentalsAnnual(
                symbol=symbol,
                exchange=exchange,
                fiscal_year_end_date=fiscal_year_end,
                fiscal_year_label=f"FY{fiscal_year or fiscal_year_end.year}",
                net_income=Decimal(str(value)),
                net_income_currency="USD",
                source=self.provider_name,
                source_as_of_date=filed_date,
                data_status="partial",
            )

        return sorted(by_fiscal_year_end.values(), key=lambda item: item.fiscal_year_end_date)

    @staticmethod
    def _read_date(value: object) -> date | None:
        if not value:
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, UTC).date()
        return date.fromisoformat(str(value))
