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
    instrument_type: str = "common_stock"


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


@dataclass(slots=True)
class ProviderFundamentalsAnnual:
    symbol: str
    exchange: str
    fiscal_year_end_date: date
    fiscal_year_label: str
    net_income: Decimal | None = None
    net_income_currency: str = "JPY"
    operating_cash_flow: Decimal | None = None
    free_cash_flow: Decimal | None = None
    diluted_eps: Decimal | None = None
    stockholders_equity: Decimal | None = None
    weighted_average_diluted_shares: Decimal | None = None
    pe: Decimal | None = None
    pb: Decimal | None = None
    source: str = "unknown"
    source_as_of_date: date | None = None
    data_status: str = "complete"
