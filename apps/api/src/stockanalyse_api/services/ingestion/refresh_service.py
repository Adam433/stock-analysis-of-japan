from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import select

from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar
from stockanalyse_api.services.ingestion.providers.base import EodMarketDataProvider
from stockanalyse_api.services.normalization.eod_normalizer import normalize_daily_bar


def _get_or_create_instrument(session, bar: ProviderDailyBar) -> Instrument:
    instrument = session.execute(
        select(Instrument).where(
            Instrument.symbol == bar.symbol,
            Instrument.exchange == bar.exchange,
        )
    ).scalar_one_or_none()

    if instrument is None:
        instrument = Instrument(
            symbol=bar.symbol,
            exchange=bar.exchange,
            name=bar.instrument_name,
            currency=bar.currency,
        )
        session.add(instrument)
        session.flush()
        return instrument

    instrument.name = bar.instrument_name or instrument.name
    instrument.currency = bar.currency or instrument.currency
    return instrument


def refresh_market_data(session, provider: EodMarketDataProvider, symbols: list[str]) -> dict[str, int]:
    inserted = 0
    updated = 0
    rows_by_key: dict[tuple[int, object], MarketDataDaily] = {}

    for raw_bar in provider.fetch_daily_bars(symbols):
        normalized = normalize_daily_bar(raw_bar)
        instrument = _get_or_create_instrument(session, normalized.bar)
        row_key = (instrument.id, normalized.bar.trade_date)

        row = rows_by_key.get(row_key)
        if row is None:
            row = session.execute(
                select(MarketDataDaily).where(
                    MarketDataDaily.instrument_id == instrument.id,
                    MarketDataDaily.trade_date == normalized.bar.trade_date,
                )
            ).scalar_one_or_none()

            if row is None:
                row = MarketDataDaily(
                    instrument_id=instrument.id,
                    trade_date=normalized.bar.trade_date,
                )
                session.add(row)
                inserted += 1
            else:
                updated += 1

            rows_by_key[row_key] = row
        else:
            updated += 1

        row.open = normalized.bar.open
        row.high = normalized.bar.high
        row.low = normalized.bar.low
        row.close = normalized.bar.close
        row.adj_close = normalized.bar.adj_close
        row.volume = normalized.bar.volume
        row.data_source = normalized.bar.data_source
        row.data_status = normalized.bar.data_status or "complete"

    session.commit()
    return {"inserted": inserted, "updated": updated}
