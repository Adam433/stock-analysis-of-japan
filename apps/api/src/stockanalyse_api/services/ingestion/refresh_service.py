from __future__ import annotations

from datetime import date, timedelta
from datetime import UTC, datetime
import inspect
import os

from sqlalchemy import func, select

# Ensure SQLAlchemy relationship targets are registered for direct job usage.
import stockanalyse_api.domain.fundamentals.models  # noqa: F401
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.domain.operations.models import MarketDataRefreshRun
from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar
from stockanalyse_api.services.ingestion.provider_models import ProviderInstrument
from stockanalyse_api.services.ingestion.providers.base import EodMarketDataProvider
from stockanalyse_api.services.normalization.eod_normalizer import normalize_daily_bar


DEFAULT_UNIVERSE_FILTER = "tse_common_stock"
DEFAULT_REFRESH_COMMIT_EVERY = int(os.environ.get("STOCKANALYSE_REFRESH_COMMIT_EVERY", "500"))
DEFAULT_REFRESH_OVERLAP_DAYS = int(os.environ.get("STOCKANALYSE_REFRESH_OVERLAP_DAYS", "30"))


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


def refresh_market_data(
    session,
    provider: EodMarketDataProvider,
    symbols: list[str],
    *,
    commit_every: int = DEFAULT_REFRESH_COMMIT_EVERY,
    overlap_days: int = DEFAULT_REFRESH_OVERLAP_DAYS,
) -> dict[str, int | str | None]:
    inserted = 0
    updated = 0
    processed = 0
    latest_trade_date: date | None = None
    rows_by_key: dict[tuple[int, date], MarketDataDaily] = {}
    final_status_by_key: dict[tuple[str, str, date], str] = {}

    def flush_batch() -> None:
        nonlocal rows_by_key
        if not rows_by_key:
            return
        session.commit()
        rows_by_key = {}

    latest_stored_dates = _apply_refresh_overlap(
        _load_latest_trade_dates_by_symbol(
            session,
            symbols,
            exchange=_provider_exchange(provider),
        ),
        overlap_days=overlap_days,
    )

    for raw_bar in _fetch_provider_daily_bars(
        provider,
        symbols,
        start_after_by_symbol=latest_stored_dates,
    ):
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
        final_status_by_key[(normalized.bar.symbol, normalized.bar.exchange, normalized.bar.trade_date)] = row.data_status

        if processed % commit_every == 0:
            flush_batch()

    flush_batch()
    partial_rows = sum(1 for status in final_status_by_key.values() if status == "partial")
    unavailable_rows = sum(1 for status in final_status_by_key.values() if status == "unavailable")
    return {
        "processed": processed,
        "inserted": inserted,
        "updated": updated,
        "partial_rows": partial_rows,
        "unavailable_rows": unavailable_rows,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
    }


def _provider_exchange(provider: EodMarketDataProvider) -> str | None:
    market_scope = getattr(provider, "market_scope", "")
    if market_scope.startswith("jp_"):
        return "TSE"
    if market_scope.startswith("us_"):
        return "US"
    return None


def _load_latest_trade_dates_by_symbol(
    session,
    symbols: list[str],
    *,
    exchange: str | None = None,
) -> dict[str, date]:
    if not symbols:
        return {}

    query = (
        select(
            Instrument.symbol,
            func.max(MarketDataDaily.trade_date),
        )
        .join(MarketDataDaily, MarketDataDaily.instrument_id == Instrument.id)
        .where(Instrument.symbol.in_(symbols))
        .group_by(Instrument.symbol)
    )
    if exchange is not None:
        query = query.where(Instrument.exchange == exchange)
    rows = session.execute(query).all()
    return {symbol: latest_trade_date for symbol, latest_trade_date in rows if latest_trade_date is not None}


def _apply_refresh_overlap(
    latest_dates: dict[str, date],
    *,
    overlap_days: int,
) -> dict[str, date]:
    if overlap_days <= 0:
        return latest_dates
    overlap_delta = timedelta(days=overlap_days)
    return {
        symbol: latest_trade_date - overlap_delta
        for symbol, latest_trade_date in latest_dates.items()
    }


def _fetch_provider_daily_bars(
    provider: EodMarketDataProvider,
    symbols: list[str],
    *,
    start_after_by_symbol: dict[str, date],
):
    fetch_daily_bars = provider.fetch_daily_bars
    signature = inspect.signature(fetch_daily_bars)
    if "start_after_by_symbol" in signature.parameters:
        return fetch_daily_bars(
            symbols,
            start_after_by_symbol=start_after_by_symbol,
        )
    return fetch_daily_bars(symbols)


def _filter_supported_instruments(
    instruments: list[ProviderInstrument],
    *,
    universe_filter: str,
) -> list[ProviderInstrument]:
    if universe_filter == "tse_common_stock":
        return [
            instrument
            for instrument in instruments
            if instrument.exchange == "TSE"
            and instrument.instrument_type == "common_stock"
            and instrument.is_active
        ]

    if universe_filter == "us_common_stock":
        return [
            instrument
            for instrument in instruments
            if instrument.exchange == "US"
            and instrument.instrument_type == "common_stock"
            and instrument.is_active
        ]

    if universe_filter == "explicit_symbols":
        return instruments

    raise ValueError(f"Unsupported universe filter: {universe_filter}")


def resolve_refresh_symbols(
    provider: EodMarketDataProvider,
    *,
    symbols: list[str] | None = None,
    all_supported: bool = False,
    universe_filter: str = DEFAULT_UNIVERSE_FILTER,
) -> tuple[list[str], str, str]:
    if all_supported:
        supported_instruments = provider.list_supported_instruments()
        filtered_instruments = _filter_supported_instruments(
            supported_instruments,
            universe_filter=universe_filter,
        )
        resolved_symbols = [instrument.symbol for instrument in filtered_instruments]
        return resolved_symbols, "full_universe", universe_filter

    if not symbols:
        raise ValueError("symbols are required unless all_supported is enabled.")

    return symbols, "symbol_list", "explicit_symbols"


def execute_market_data_refresh(
    session,
    provider: EodMarketDataProvider,
    symbols: list[str] | None = None,
    *,
    all_supported: bool = False,
    universe_filter: str = DEFAULT_UNIVERSE_FILTER,
    commit_every: int = DEFAULT_REFRESH_COMMIT_EVERY,
) -> dict[str, int | str | None]:
    provider_name = getattr(provider, "provider_name", provider.__class__.__name__)
    resolved_symbols, universe_scope, resolved_universe_filter = resolve_refresh_symbols(
        provider,
        symbols=symbols,
        all_supported=all_supported,
        universe_filter=universe_filter,
    )
    if not resolved_symbols:
        raise ValueError("Refresh run resolved to an empty symbol universe.")
    refresh_run = MarketDataRefreshRun(
        provider=provider_name,
        universe_scope=universe_scope,
        universe_filter=resolved_universe_filter,
        requested_symbol_count=len(resolved_symbols),
        requested_symbols="" if universe_scope == "full_universe" else ",".join(resolved_symbols),
        status="running",
        started_at=datetime.now(UTC),
    )
    session.add(refresh_run)
    session.commit()

    try:
        result = refresh_market_data(
            session,
            provider,
            resolved_symbols,
            commit_every=commit_every,
        )
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
