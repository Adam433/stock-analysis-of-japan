from __future__ import annotations

import json
import logging
import os
import time as time_module
from collections.abc import Iterable
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
import ssl
from urllib.parse import quote
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar
from stockanalyse_api.services.ingestion.provider_models import ProviderInstrument

try:
    import certifi
except ModuleNotFoundError:  # pragma: no cover - fallback for bare system interpreters
    certifi = None


DEFAULT_HISTORY_START = date(2000, 1, 1)
TRANSIENT_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}

logger = logging.getLogger(__name__)


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
        provider_name: str = "yahoo_finance_chart",
        market_scope: str = "jp_equities_eod",
        exchange: str = "TSE",
        currency: str = "JPY",
        request_interval_seconds: float | None = None,
        max_retries: int | None = None,
    ) -> None:
        self.symbols_file = symbols_file
        self.timeout_seconds = timeout_seconds
        self.history_start = history_start
        self.provider_name = provider_name
        self.market_scope = market_scope
        self.exchange = exchange
        self.currency = currency
        self.request_interval_seconds = (
            request_interval_seconds
            if request_interval_seconds is not None
            else float(os.environ.get("STOCKANALYSE_YAHOO_REQUEST_INTERVAL_SECONDS", "0.2"))
        )
        self.max_retries = (
            max_retries
            if max_retries is not None
            else int(os.environ.get("STOCKANALYSE_YAHOO_MAX_RETRIES", "3"))
        )
        self._last_request_at: float | None = None

    def list_supported_instruments(self) -> list[ProviderInstrument]:
        instruments: list[ProviderInstrument] = []
        for symbol in self._load_supported_symbols():
            instruments.append(
                ProviderInstrument(
                    symbol=symbol,
                    exchange=self.exchange,
                    currency=self.currency,
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
                if exc.code in {400, 404} or exc.code in TRANSIENT_HTTP_STATUS_CODES:
                    logger.warning("Skipping Yahoo chart symbol %s after HTTP %s.", symbol, exc.code)
                    continue
                raise
            except (URLError, TimeoutError) as exc:
                logger.warning("Skipping Yahoo chart symbol %s after request failure: %s", symbol, exc)
                continue
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
        self._throttle()
        period1_date = (
            max(self.history_start, start_after + timedelta(days=1))
            if start_after is not None
            else self.history_start
        )
        period2_date = date.today() + timedelta(days=1)
        period1 = self._to_unix_timestamp(period1_date)
        period2 = self._to_unix_timestamp(period2_date)
        yahoo_symbol = self._to_yahoo_symbol(symbol)
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(yahoo_symbol)}"
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
        for attempt in range(self.max_retries + 1):
            try:
                with urlopen(request, timeout=self.timeout_seconds, context=ssl_context) as response:
                    return json.loads(response.read().decode("utf-8"))
            except HTTPError as exc:
                if exc.code in {400, 404}:
                    raise
                if exc.code not in TRANSIENT_HTTP_STATUS_CODES or attempt >= self.max_retries:
                    raise
                self._sleep_before_retry(attempt)
            except (URLError, TimeoutError):
                if attempt >= self.max_retries:
                    raise
                self._sleep_before_retry(attempt)

        raise RuntimeError(f"Yahoo chart request failed for {symbol}.")

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
                exchange=self.exchange,
                trade_date=trade_date,
                open=self._to_decimal(self._value_at(opens, index)),
                high=self._to_decimal(self._value_at(highs, index)),
                low=self._to_decimal(self._value_at(lows, index)),
                close=self._to_decimal(close),
                adj_close=self._to_decimal(self._value_at(adjcloses, index) or close),
                volume=self._to_int(self._value_at(volumes, index)),
                data_source=self.provider_name,
                instrument_name=meta.get("longName") or meta.get("shortName"),
                currency=(meta.get("currency") or self.currency),
            )

    def _throttle(self) -> None:
        if self.request_interval_seconds <= 0:
            return
        now = time_module.monotonic()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            remaining = self.request_interval_seconds - elapsed
            if remaining > 0:
                time_module.sleep(remaining)
        self._last_request_at = time_module.monotonic()

    @staticmethod
    def _sleep_before_retry(attempt: int) -> None:
        time_module.sleep(min(30.0, 2.0 ** attempt))

    def _to_yahoo_symbol(self, symbol: str) -> str:
        if self.exchange == "US":
            return symbol.replace(".", "-")
        return symbol

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
