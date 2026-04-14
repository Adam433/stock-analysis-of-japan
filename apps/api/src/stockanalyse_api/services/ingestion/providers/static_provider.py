from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar
from stockanalyse_api.services.ingestion.provider_models import ProviderInstrument


class StaticFixtureProvider:
    provider_name = "static_fixture"
    market_scope = "jp_equities_eod"
    credential_boundary = "backend_only"

    def __init__(self, fixture_path: Path) -> None:
        self.fixture_path = fixture_path

    def list_supported_instruments(self) -> list[ProviderInstrument]:
        payload = json.loads(self.fixture_path.read_text())
        instruments_by_symbol: dict[tuple[str, str], ProviderInstrument] = {}

        for row in payload["daily_bars"]:
            key = (row["symbol"], row["exchange"])
            instruments_by_symbol.setdefault(
                key,
                ProviderInstrument(
                    symbol=row["symbol"],
                    exchange=row["exchange"],
                    name=row.get("instrument_name"),
                    currency=row.get("currency", "JPY"),
                    instrument_type=row.get("instrument_type", "common_stock"),
                ),
            )

        return sorted(
            instruments_by_symbol.values(),
            key=lambda instrument: (instrument.exchange, instrument.symbol),
        )

    def fetch_daily_bars(
        self,
        symbols: list[str],
        *,
        start_after_by_symbol: dict[str, date] | None = None,
    ) -> list[ProviderDailyBar]:
        payload = json.loads(self.fixture_path.read_text())
        requested = set(symbols)
        bars: list[ProviderDailyBar] = []

        for row in payload["daily_bars"]:
            if row["symbol"] not in requested:
                continue
            trade_date = date.fromisoformat(row["trade_date"])
            start_after = (start_after_by_symbol or {}).get(row["symbol"])
            if start_after is not None and trade_date <= start_after:
                continue
            bars.append(
                ProviderDailyBar(
                    symbol=row["symbol"],
                    exchange=row["exchange"],
                    trade_date=trade_date,
                    open=Decimal(row["open"]) if row.get("open") is not None else None,
                    high=Decimal(row["high"]) if row.get("high") is not None else None,
                    low=Decimal(row["low"]) if row.get("low") is not None else None,
                    close=Decimal(row["close"]) if row.get("close") is not None else None,
                    adj_close=Decimal(row["adj_close"]) if row.get("adj_close") is not None else None,
                    volume=row.get("volume"),
                    data_status=row.get("data_status"),
                    data_source=self.provider_name,
                    instrument_name=row.get("instrument_name"),
                    currency=row.get("currency", "JPY"),
                )
            )

        return bars
