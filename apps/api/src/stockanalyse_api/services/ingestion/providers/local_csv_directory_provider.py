from __future__ import annotations

import csv
from datetime import date
from decimal import Decimal
from pathlib import Path
from collections.abc import Iterable

from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar
from stockanalyse_api.services.ingestion.provider_models import ProviderInstrument


class LocalCsvDirectoryProvider:
    provider_name = "local_csv_directory"
    market_scope = "jp_equities_eod"
    credential_boundary = "backend_only"

    def __init__(self, csv_dir: Path, symbols_file: Path | None = None) -> None:
        self.csv_dir = csv_dir
        self.symbols_file = symbols_file

    def list_supported_instruments(self) -> list[ProviderInstrument]:
        if self.symbols_file is None:
            raise ValueError("symbols_file is required to enumerate the TSE common-stock universe.")

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
            csv_path = self.csv_dir / f"{symbol}.csv"
            if not csv_path.exists():
                continue
            start_after = (start_after_by_symbol or {}).get(symbol)

            with csv_path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    trade_date = date.fromisoformat(row["Date"])
                    if start_after is not None and trade_date <= start_after:
                        continue
                    yield ProviderDailyBar(
                        symbol=symbol,
                        exchange="TSE",
                        trade_date=trade_date,
                        open=self._parse_decimal(row.get("Open")),
                        high=self._parse_decimal(row.get("High")),
                        low=self._parse_decimal(row.get("Low")),
                        close=self._parse_decimal(row.get("Close")),
                        adj_close=self._parse_decimal(row.get("Adj Close")),
                        volume=self._parse_int(row.get("Volume")),
                        data_source=self.provider_name,
                        instrument_name=None,
                        currency="JPY",
                    )

    def _load_supported_symbols(self) -> list[str]:
        assert self.symbols_file is not None

        symbols: list[str] = []
        for raw_line in self.symbols_file.read_text(encoding="utf-8").splitlines():
            symbol = raw_line.strip()
            if not symbol or symbol.startswith("#"):
                continue
            symbols.append(symbol)
        return symbols

    @staticmethod
    def _parse_decimal(value: str | None) -> Decimal | None:
        if value is None or value == "":
            return None
        return Decimal(value)

    @staticmethod
    def _parse_int(value: str | None) -> int | None:
        if value is None or value == "":
            return None
        return int(value)
