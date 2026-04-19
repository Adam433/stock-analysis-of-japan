from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
import ssl
from urllib.parse import quote
from urllib.request import Request, urlopen

from stockanalyse_api.services.ingestion.provider_models import ProviderFundamentalsAnnual

try:
    import certifi
except ModuleNotFoundError:  # pragma: no cover - fallback for bare system interpreters
    certifi = None


class YahooFinanceFundamentalsProvider:
    provider_name = "yahoo_finance_fundamentals"
    market_scope = "jp_equities_eod"
    credential_boundary = "backend_only"
    modules = ("incomeStatementHistory", "summaryDetail", "defaultKeyStatistics")

    def __init__(self, *, timeout_seconds: int = 30) -> None:
        self.timeout_seconds = timeout_seconds

    def fetch_annual_fundamentals(self, symbol: str, *, exchange: str = "TSE") -> list[ProviderFundamentalsAnnual]:
        payload = self._fetch_quote_summary_payload(symbol)
        return self._parse_quote_summary_payload(symbol, exchange, payload)

    def _fetch_quote_summary_payload(self, symbol: str) -> dict[str, object]:
        url = (
            f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{quote(symbol)}"
            f"?modules={','.join(self.modules)}"
        )
        request = Request(
            url,
            headers={
                "User-Agent": "stockAnalyse/0.1 (+https://localhost)",
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
        summary_detail = first_result.get("summaryDetail") if isinstance(first_result, dict) else {}
        statistics = first_result.get("defaultKeyStatistics") if isinstance(first_result, dict) else {}

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
                    net_income_currency="JPY",
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
