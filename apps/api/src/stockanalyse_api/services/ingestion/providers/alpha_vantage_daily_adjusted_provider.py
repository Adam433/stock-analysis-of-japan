from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable
from datetime import date
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar
from stockanalyse_api.services.ingestion.provider_models import ProviderInstrument


class AlphaVantageDailyAdjustedProvider:
    provider_name = "alpha_vantage_daily_adjusted"
    market_scope = "us_equities_eod"
    credential_boundary = "backend_only"

    def __init__(
        self,
        *,
        symbols_file: Path,
        api_key: str | None = None,
        timeout_seconds: int = 30,
        request_interval_seconds: float | None = None,
    ) -> None:
        self.symbols_file = symbols_file
        self.api_key = api_key or os.environ.get("ALPHAVANTAGE_API_KEY")
        self.timeout_seconds = timeout_seconds
        self.request_interval_seconds = (
            request_interval_seconds
            if request_interval_seconds is not None
            else float(os.environ.get("ALPHAVANTAGE_REQUEST_INTERVAL_SECONDS", "12.5"))
        )
        self._last_request_at: float | None = None
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY is required for US market data refresh.")

    def list_supported_instruments(self) -> list[ProviderInstrument]:
        return [
            ProviderInstrument(
                symbol=symbol,
                exchange="US",
                currency="USD",
                instrument_type="common_stock",
            )
            for symbol in self._load_supported_symbols()
        ]

    def fetch_daily_bars(
        self,
        symbols: list[str],
        *,
        start_after_by_symbol: dict[str, date] | None = None,
    ) -> Iterable[ProviderDailyBar]:
        for symbol in symbols:
            payload = self._fetch_payload(symbol)
            yield from self._parse_payload(
                symbol,
                payload,
                start_after=(start_after_by_symbol or {}).get(symbol),
            )

    def _load_supported_symbols(self) -> list[str]:
        symbols: list[str] = []
        for raw_line in self.symbols_file.read_text(encoding="utf-8").splitlines():
            symbol = raw_line.strip()
            if not symbol or symbol.startswith("#"):
                continue
            symbols.append(symbol)
        return symbols

    def _fetch_payload(self, symbol: str) -> dict[str, object]:
        self._throttle()
        query = urlencode(
            {
                "function": "TIME_SERIES_DAILY_ADJUSTED",
                "symbol": symbol,
                "outputsize": "full",
                "apikey": self.api_key,
            }
        )
        request = Request(
            f"https://www.alphavantage.co/query?{query}",
            headers={
                "User-Agent": "stockAnalyse/0.1 (+https://localhost)",
                "Accept": "application/json",
            },
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))

    def _throttle(self) -> None:
        if self.request_interval_seconds <= 0:
            return
        now = time.monotonic()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            remaining = self.request_interval_seconds - elapsed
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_at = time.monotonic()

    def _parse_payload(
        self,
        symbol: str,
        payload: dict[str, object],
        *,
        start_after: date | None,
    ) -> Iterable[ProviderDailyBar]:
        if "Error Message" in payload:
            return
        if "Note" in payload or "Information" in payload:
            raise RuntimeError(str(payload.get("Note") or payload.get("Information")))

        metadata = payload.get("Meta Data")
        series = payload.get("Time Series (Daily)")
        if not isinstance(series, dict):
            return
        name = symbol
        if isinstance(metadata, dict):
            name = str(metadata.get("2. Symbol") or symbol)

        for raw_date, row in sorted(series.items()):
            if not isinstance(row, dict):
                continue
            trade_date = date.fromisoformat(raw_date)
            if start_after is not None and trade_date <= start_after:
                continue
            close = self._read_decimal(row, "4. close")
            if close is None:
                continue
            adjusted_close = self._read_decimal(row, "5. adjusted close") or close
            yield ProviderDailyBar(
                symbol=symbol,
                exchange="US",
                trade_date=trade_date,
                open=self._read_decimal(row, "1. open"),
                high=self._read_decimal(row, "2. high"),
                low=self._read_decimal(row, "3. low"),
                close=close,
                adj_close=adjusted_close,
                volume=self._read_int(row, "6. volume"),
                data_source=self.provider_name,
                instrument_name=name,
                currency="USD",
            )

    @staticmethod
    def _read_decimal(row: dict[str, object], field: str) -> Decimal | None:
        raw = row.get(field)
        if raw is None:
            return None
        return Decimal(str(raw))

    @staticmethod
    def _read_int(row: dict[str, object], field: str) -> int | None:
        raw = row.get(field)
        if raw is None:
            return None
        return int(raw)
