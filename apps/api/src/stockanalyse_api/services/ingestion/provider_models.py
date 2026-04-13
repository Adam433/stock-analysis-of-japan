from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal


@dataclass(slots=True)
class ProviderInstrument:
    symbol: str
    exchange: str
    name: str | None = None
    currency: str = "JPY"
    is_active: bool = True


@dataclass(slots=True)
class ProviderDailyBar:
    symbol: str
    exchange: str
    trade_date: date
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    adj_close: Decimal | None = None
    volume: int | None = None
    data_status: str | None = None
    data_source: str = "unknown"
    instrument_name: str | None = None
    currency: str = "JPY"
    metadata: dict[str, str] = field(default_factory=dict)
