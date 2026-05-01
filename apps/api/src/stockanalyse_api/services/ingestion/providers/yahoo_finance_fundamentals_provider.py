from __future__ import annotations

import json
import os
import ssl
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from http.cookiejar import CookieJar
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import HTTPSHandler, HTTPCookieProcessor, Request, build_opener

from stockanalyse_api.services.ingestion.provider_models import ProviderFundamentalsAnnual

try:
    import certifi
except ModuleNotFoundError:  # pragma: no cover - fallback for bare system interpreters
    certifi = None


class YahooFinanceFundamentalsProvider:
    provider_name = "yahoo_finance_fundamentals"
    market_scope = "jp_equities_eod"
    credential_boundary = "backend_only"

    def __init__(
        self,
        *,
        timeout_seconds: int = 30,
        provider_name: str = "yahoo_finance_fundamentals",
        market_scope: str = "jp_equities_eod",
        default_currency: str = "JPY",
        min_request_interval_seconds: float | None = None,
        modules: tuple[str, ...] | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.provider_name = provider_name
        self.market_scope = market_scope
        self.default_currency = default_currency
        self.modules = modules or (
            "incomeStatementHistory",
            "summaryDetail",
            "defaultKeyStatistics",
            "price",
        )
        self.min_request_interval_seconds = (
            min_request_interval_seconds
            if min_request_interval_seconds is not None
            else float(
                os.environ.get(
                    "STOCKANALYSE_YAHOO_FUNDAMENTALS_MIN_REQUEST_INTERVAL_SECONDS",
                    "0.35",
                )
            )
        )
        self._last_request_at: float | None = None
        self._cookie_jar = CookieJar()
        self._opener = self._build_opener()
        self._crumb: str | None = None

    def fetch_annual_fundamentals(self, symbol: str, *, exchange: str = "TSE") -> list[ProviderFundamentalsAnnual]:
        payload = self._fetch_quote_summary_payload(symbol)
        return self._parse_quote_summary_payload(symbol, exchange, payload)

    def _fetch_quote_summary_payload(self, symbol: str) -> dict[str, object]:
        for attempt in range(2):
            try:
                crumb = self._get_crumb()
                url = (
                    f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{quote(symbol)}"
                    f"?modules={','.join(self.modules)}&crumb={quote(crumb, safe='')}"
                )
                return self._fetch_json(url)
            except HTTPError as exc:
                if attempt == 0 and exc.code in {401, 403, 429}:
                    self._reset_session()
                    continue
                raise
        return {}

    def _fetch_json(self, url: str) -> dict[str, object]:
        request = Request(
            url,
            headers={
                "User-Agent": self._user_agent(),
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        self._throttle_request()
        with self._opener.open(request, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))

    def _get_crumb(self) -> str:
        if self._crumb:
            return self._crumb
        try:
            self._prime_cookie()
        except HTTPError as exc:
            if exc.code != 404:
                raise
        self._crumb = self._fetch_text("https://query1.finance.yahoo.com/v1/test/getcrumb").strip()
        if not self._crumb:
            raise RuntimeError("Yahoo Finance crumb response was empty.")
        return self._crumb

    def _fetch_text(self, url: str) -> str:
        request = Request(
            url,
            headers={
                "User-Agent": self._user_agent(),
                "Accept": "application/json,text/plain,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        self._throttle_request()
        with self._opener.open(request, timeout=self.timeout_seconds) as response:
            return response.read().decode("utf-8")

    def _prime_cookie(self) -> None:
        self._fetch_text("https://fc.yahoo.com")

    def _reset_session(self) -> None:
        self._cookie_jar.clear()
        self._opener = self._build_opener()
        self._crumb = None

    def _build_opener(self):
        ssl_context = (
            ssl.create_default_context(cafile=certifi.where())
            if certifi is not None
            else ssl.create_default_context()
        )
        return build_opener(
            HTTPCookieProcessor(self._cookie_jar),
            HTTPSHandler(context=ssl_context),
        )

    def _throttle_request(self) -> None:
        if self.min_request_interval_seconds <= 0:
            return
        now = time.monotonic()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            if elapsed < self.min_request_interval_seconds:
                time.sleep(self.min_request_interval_seconds - elapsed)
        self._last_request_at = time.monotonic()

    @staticmethod
    def _user_agent() -> str:
        return (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36 stockAnalyse/0.1"
        )

    def _parse_quote_summary_payload(
        self,
        symbol: str,
        exchange: str,
        payload: dict[str, object],
    ) -> list[ProviderFundamentalsAnnual]:
        summary = payload.get("quoteSummary") if isinstance(payload, dict) else None
        if not isinstance(summary, dict):
            return []

        error = summary.get("error")
        if error:
            return []

        results = summary.get("result")
        if not isinstance(results, list) or not results:
            return []

        first_result = results[0] if isinstance(results[0], dict) else {}
        income_section = first_result.get("incomeStatementHistory")
        income_rows = income_section.get("incomeStatementHistory") if isinstance(income_section, dict) else []
        summary_detail = first_result.get("summaryDetail")
        if not isinstance(summary_detail, dict):
            summary_detail = {}
        statistics = first_result.get("defaultKeyStatistics")
        if not isinstance(statistics, dict):
            statistics = {}
        price = first_result.get("price") if isinstance(first_result, dict) else {}
        currency = (
            self._read_string(price, "financialCurrency")
            or self._read_string(price, "currency")
            or self.default_currency
        )

        current_pe = self._read_decimal(summary_detail, "trailingPE")
        current_pb = self._read_decimal(statistics, "priceToBook")
        source_as_of_date = date.today()

        parsed_rows: list[ProviderFundamentalsAnnual] = []
        for row in income_rows if isinstance(income_rows, list) else []:
            if not isinstance(row, dict):
                continue

            fiscal_year_end = self._read_date(row.get("endDate"))
            if fiscal_year_end is None:
                continue

            net_income = self._read_decimal(row, "netIncome")
            parsed_rows.append(
                ProviderFundamentalsAnnual(
                    symbol=symbol,
                    exchange=exchange,
                    fiscal_year_end_date=fiscal_year_end,
                    fiscal_year_label=f"FY{fiscal_year_end.year}",
                    net_income=net_income,
                    net_income_currency=currency,
                    pe=None,
                    pb=None,
                    source=self.provider_name,
                    source_as_of_date=source_as_of_date,
                    data_status=self._resolve_data_status(net_income, None, None),
                )
            )

        parsed_rows.sort(key=lambda row: row.fiscal_year_end_date)
        if parsed_rows:
            latest_row = parsed_rows[-1]
            latest_row.pb = current_pb
            if latest_row.net_income is not None and latest_row.net_income >= 0:
                latest_row.pe = current_pe
            latest_row.data_status = self._resolve_data_status(
                latest_row.net_income,
                latest_row.pe,
                latest_row.pb,
            )
        return parsed_rows[-5:]

    @staticmethod
    def _read_decimal(payload: dict[str, object], field_name: str) -> Decimal | None:
        field = payload.get(field_name)
        if not isinstance(field, dict):
            return None
        raw = field.get("raw")
        if raw is None:
            return None
        return Decimal(str(raw))

    @staticmethod
    def _read_string(payload: object, field_name: str) -> str | None:
        if not isinstance(payload, dict):
            return None
        field = payload.get(field_name)
        if isinstance(field, str) and field:
            return field
        if not isinstance(field, dict):
            return None
        raw = field.get("raw") or field.get("fmt") or field.get("longFmt")
        if isinstance(raw, str) and raw:
            return raw
        return None

    @staticmethod
    def _read_date(value: object) -> date | None:
        if not isinstance(value, dict):
            return None
        raw = value.get("raw")
        if raw is None:
            return None
        return datetime.fromtimestamp(int(raw), UTC).date()

    @staticmethod
    def _resolve_data_status(net_income: Decimal | None, pe: Decimal | None, pb: Decimal | None) -> str:
        if net_income is None:
            return "missing"
        if pe is None or pb is None:
            return "partial"
        return "complete"
