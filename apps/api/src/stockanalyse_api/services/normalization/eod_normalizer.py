from __future__ import annotations

from dataclasses import dataclass

from stockanalyse_api.domain.market_data.models import DATA_STATUS_VALUES
from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar, ProviderInstrument


VALID_DATA_STATUSES = set(DATA_STATUS_VALUES)


@dataclass(slots=True)
class NormalizedEodRecord:
    instrument: ProviderInstrument
    bar: ProviderDailyBar


def resolve_data_status(bar: ProviderDailyBar) -> str:
    if bar.data_status in VALID_DATA_STATUSES:
        return bar.data_status

    price_fields = [bar.open, bar.high, bar.low, bar.close, bar.adj_close]
    populated = sum(value is not None for value in price_fields)
    if populated == 0:
        return "unavailable"
    if populated < len(price_fields):
        return "partial"
    return "complete"


def normalize_daily_bar(bar: ProviderDailyBar) -> NormalizedEodRecord:
    instrument = ProviderInstrument(
        symbol=bar.symbol,
        exchange=bar.exchange,
        name=bar.instrument_name,
        currency=bar.currency,
    )
    normalized_bar = ProviderDailyBar(
        symbol=bar.symbol,
        exchange=bar.exchange,
        trade_date=bar.trade_date,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        adj_close=bar.adj_close,
        volume=bar.volume,
        data_status=resolve_data_status(bar),
        data_source=bar.data_source,
        instrument_name=bar.instrument_name,
        currency=bar.currency,
        metadata=bar.metadata,
    )
    return NormalizedEodRecord(instrument=instrument, bar=normalized_bar)
