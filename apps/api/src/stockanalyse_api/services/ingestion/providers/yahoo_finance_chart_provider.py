from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
import ssl
from urllib.parse import quote
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar
from stockanalyse_api.services.ingestion.provider_models import ProviderInstrument

try:
    import certifi
except ModuleNotFoundError:  # pragma: no cover - fallback for bare system interpreters
    certifi = None


DEFAULT_HISTORY_START = date(2000, 1, 1)


class YahooFinanceChartProvider:
    provider_name = "yahoo_finance_chart"
    market_scope = "jp_equities_eod"
    credential_boundary = "backend_only"

    def __init__(
        self,
        *,
        symbols_file: Path,
        timeout_seconds: int = 30,
        history_start: date = DEFAULT_HISTORY_START,
    ) -> None:
        self.symbols_file = symbols_file
        self.timeout_seconds = timeout_seconds
        self.history_start = history_start

    def list_supported_instruments(self) -> list[ProviderInstrument]:
        instruments: list[ProviderInstrument] = []
        for symbol in self._load_supported_symbols():
            instruments.append(
                ProviderInstrument(
                    symbol=symbol,
                    exchange="TSE",
                    currency="JPY",
                    instrument_type="common_stock",
                )
            )
        return instruments

    def fetch_daily_bars(
        self,
        symbols: list[str],
        *,
        start_after_by_symbol: dict[str, date] | None = None,
    ) -> Iterable[ProviderDailyBar]:
        for symbol in symbols:
            start_after = (start_after_by_symbol or {}).get(symbol)
            try:
                payload = self._fetch_chart_payload(symbol, start_after=start_after)
            except HTTPError as exc:
                if exc.code in {400, 404}:
                    continue
                raise
            yield from self._parse_chart_payload(
                symbol,
                payload,
                start_after=start_after,
            )

    def _load_supported_symbols(self) -> list[str]:
        symbols: list[str] = []
        for raw_line in self.symbols_file.read_text(encoding="utf-8").splitlines():
            symbol = raw_line.strip()
            if not symbol or symbol.startswith("#"):
                continue
            symbols.append(symbol)
        return symbols

    def _fetch_chart_payload(self, symbol: str, *, start_after: date | None) -> dict:
        period1_date = (
            max(self.history_start, start_after + timedelta(days=1))
            if start_after is not None
            else self.history_start
        )
        period2_date = date.today() + timedelta(days=1)
        period1 = self._to_unix_timestamp(period1_date)
        period2 = self._to_unix_timestamp(period2_date)
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(symbol)}"
            f"?interval=1d&includeAdjustedClose=true&events=div,splits&period1={period1}&period2={period2}"
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

    def _parse_chart_payload(
        self,
        symbol: str,
        payload: dict,
        *,
        start_after: date | None,
    ) -> Iterable[ProviderDailyBar]:
        chart = payload.get("chart") or {}
        error = chart.get("error")
        if error:
            return

        result = chart.get("result") or []
        if not result:
            return

        first_result = result[0]
        timestamps = first_result.get("timestamp") or []
        indicators = first_result.get("indicators") or {}
        quotes = indicators.get("quote") or [{}]
        quote_row = quotes[0] if quotes else {}
        adjclose_rows = indicators.get("adjclose") or [{}]
        adjclose_row = adjclose_rows[0] if adjclose_rows else {}
        meta = first_result.get("meta") or {}

        opens = quote_row.get("open") or []
        highs = quote_row.get("high") or []
        lows = quote_row.get("low") or []
        closes = quote_row.get("close") or []
        volumes = quote_row.get("volume") or []
        adjcloses = adjclose_row.get("adjclose") or []

        for index, timestamp in enumerate(timestamps):
            trade_date = datetime.fromtimestamp(timestamp, UTC).date()
            if start_after is not None and trade_date <= start_after:
                continue

            close = self._value_at(closes, index)
            if close is None:
                continue

            yield ProviderDailyBar(
                symbol=symbol,
                exchange="TSE",
                trade_date=trade_date,
                open=self._to_decimal(self._value_at(opens, index)),
                high=self._to_decimal(self._value_at(highs, index)),
                low=self._to_decimal(self._value_at(lows, index)),
                close=self._to_decimal(close),
                adj_close=self._to_decimal(self._value_at(adjcloses, index) or close),
                volume=self._to_int(self._value_at(volumes, index)),
                data_source=self.provider_name,
                instrument_name=meta.get("longName") or meta.get("shortName"),
                currency=(meta.get("currency") or "JPY"),
            )

    @staticmethod
    def _value_at(values: list, index: int):
        return values[index] if index < len(values) else None

    @staticmethod
    def _to_decimal(value) -> Decimal | None:
        if value is None:
            return None
        return Decimal(str(value))

    @staticmethod
    def _to_int(value) -> int | None:
        if value is None:
            return None
        return int(value)

    @staticmethod
    def _to_unix_timestamp(day: date) -> int:
        return int(datetime.combine(day, time.min, tzinfo=UTC).timestamp())
