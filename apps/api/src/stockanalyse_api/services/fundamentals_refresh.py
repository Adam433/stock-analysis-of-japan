from __future__ import annotations

from datetime import date

from sqlalchemy import select

from stockanalyse_api.domain.fundamentals.models import FundamentalsAnnual
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.services.ingestion.provider_models import ProviderFundamentalsAnnual
from stockanalyse_api.services.ingestion.providers.registry import build_ingestion_provider

DEFAULT_FUNDAMENTALS_PROVIDER = "yahoo_finance_fundamentals"


def refresh_instrument_fundamentals(session, *, instrument_id: int, provider=None) -> bool:
    instrument = session.get(Instrument, instrument_id)
    if instrument is None:
        raise ValueError("Instrument not found.")

    provider = provider or build_ingestion_provider(DEFAULT_FUNDAMENTALS_PROVIDER)
    existing_rows = session.execute(
        select(FundamentalsAnnual)
        .where(FundamentalsAnnual.instrument_id == instrument_id)
        .order_by(FundamentalsAnnual.fiscal_year_end_date.asc())
    ).scalars().all()

    try:
        provider_rows = provider.fetch_annual_fundamentals(instrument.symbol, exchange=instrument.exchange)
    except Exception:
        session.rollback()
        if existing_rows:
            refreshed_on = date.today()
            for row in existing_rows:
                row.source_as_of_date = refreshed_on
            session.commit()
        return False

    rows_by_end_date = {row.fiscal_year_end_date: row for row in existing_rows}
    touched_rows: list[FundamentalsAnnual] = []
    for provider_row in provider_rows[-5:]:
        row = rows_by_end_date.get(provider_row.fiscal_year_end_date)
        if row is None:
            row = FundamentalsAnnual(
                instrument_id=instrument_id,
                fiscal_year_end_date=provider_row.fiscal_year_end_date,
                fiscal_year_label=provider_row.fiscal_year_label,
                net_income=provider_row.net_income,
                net_income_currency=provider_row.net_income_currency,
                pe=provider_row.pe,
                pb=provider_row.pb,
                source=provider_row.source,
                source_as_of_date=provider_row.source_as_of_date or date.today(),
                data_status=provider_row.data_status,
            )
            session.add(row)
        else:
            _apply_provider_row(row, provider_row)
        touched_rows.append(row)

    if not touched_rows and existing_rows:
        refreshed_on = date.today()
        for row in existing_rows:
            row.source_as_of_date = refreshed_on

    session.commit()
    return bool(touched_rows)


def _apply_provider_row(row: FundamentalsAnnual, provider_row: ProviderFundamentalsAnnual) -> None:
    row.fiscal_year_label = provider_row.fiscal_year_label
    row.net_income = provider_row.net_income
    row.net_income_currency = provider_row.net_income_currency
    row.pe = provider_row.pe
    row.pb = provider_row.pb
    row.source = provider_row.source
    row.source_as_of_date = provider_row.source_as_of_date or date.today()
    row.data_status = provider_row.data_status
