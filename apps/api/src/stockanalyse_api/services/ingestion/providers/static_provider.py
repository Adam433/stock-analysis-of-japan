from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar


class StaticFixtureProvider:
    provider_name = "static_fixture"

    def __init__(self, fixture_path: Path) -> None:
        self.fixture_path = fixture_path

    def fetch_daily_bars(self, symbols: list[str]) -> list[ProviderDailyBar]:
        payload = json.loads(self.fixture_path.read_text())
        requested = set(symbols)
        bars: list[ProviderDailyBar] = []

        for row in payload["daily_bars"]:
            if row["symbol"] not in requested:
                continue
            bars.append(
                ProviderDailyBar(
                    symbol=row["symbol"],
                    exchange=row["exchange"],
                    trade_date=date.fromisoformat(row["trade_date"]),
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
