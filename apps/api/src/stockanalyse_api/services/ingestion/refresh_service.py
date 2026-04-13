from __future__ import annotations

from datetime import date
from datetime import UTC, datetime

from sqlalchemy import select

from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.domain.operations.models import MarketDataRefreshRun
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


def refresh_market_data(session, provider: EodMarketDataProvider, symbols: list[str]) -> dict[str, int | str | None]:
    inserted = 0
    updated = 0
    processed = 0
    partial_rows = 0
    unavailable_rows = 0
    latest_trade_date: date | None = None
    rows_by_key: dict[tuple[int, date], MarketDataDaily] = {}

    for raw_bar in provider.fetch_daily_bars(symbols):
        processed += 1
        normalized = normalize_daily_bar(raw_bar)
        instrument = _get_or_create_instrument(session, normalized.bar)
        row_key = (instrument.id, normalized.bar.trade_date)
        if latest_trade_date is None or normalized.bar.trade_date > latest_trade_date:
            latest_trade_date = normalized.bar.trade_date

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
        if row.data_status == "partial":
            partial_rows += 1
        elif row.data_status == "unavailable":
            unavailable_rows += 1

    session.commit()
    return {
        "processed": processed,
        "inserted": inserted,
        "updated": updated,
        "partial_rows": partial_rows,
        "unavailable_rows": unavailable_rows,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
    }


def execute_market_data_refresh(
    session,
    provider: EodMarketDataProvider,
    symbols: list[str],
) -> dict[str, int | str | None]:
    provider_name = getattr(provider, "provider_name", provider.__class__.__name__)
    refresh_run = MarketDataRefreshRun(
        provider=provider_name,
        requested_symbols=",".join(symbols),
        status="running",
        started_at=datetime.now(UTC),
    )
    session.add(refresh_run)
    session.commit()

    try:
        result = refresh_market_data(session, provider, symbols)
        refresh_run = session.get(MarketDataRefreshRun, refresh_run.id)
        refresh_run.status = (
            "partial" if result["partial_rows"] or result["unavailable_rows"] else "succeeded"
        )
        refresh_run.completed_at = datetime.now(UTC)
        refresh_run.latest_trade_date = (
            datetime.fromisoformat(result["latest_trade_date"]).date()
            if result["latest_trade_date"] is not None
            else None
        )
        refresh_run.rows_processed = result["processed"]
        refresh_run.rows_inserted = result["inserted"]
        refresh_run.rows_updated = result["updated"]
        refresh_run.partial_rows = result["partial_rows"]
        refresh_run.unavailable_rows = result["unavailable_rows"]
        refresh_run.error_message = None
        session.commit()
        return result
    except Exception as exc:
        session.rollback()
        refresh_run = session.get(MarketDataRefreshRun, refresh_run.id)
        refresh_run.status = "failed"
        refresh_run.completed_at = datetime.now(UTC)
        refresh_run.error_message = str(exc)
        session.commit()
        raise
