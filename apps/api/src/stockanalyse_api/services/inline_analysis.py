from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from stockanalyse_api.domain.fundamentals.models import FundamentalsAnnual
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.screens.models import ScreenRun, ScreenRunResult
from stockanalyse_api.services.chart_data import (
    collect_candlesticks,
    resolve_latest_stock_detail_screen_run_id,
    serialize_candlestick_rows,
)
from stockanalyse_api.services.fundamentals_refresh import refresh_instrument_fundamentals

DEFAULT_INLINE_ANALYSIS_CANDLE_WINDOW = 252
DEFAULT_INLINE_ANALYSIS_FISCAL_YEAR_LIMIT = 5
FUNDAMENTALS_REFRESH_MAX_AGE_DAYS = 7


@dataclass(slots=True)
class FiscalYearValuationPayload:
    fiscal_year_label: str
    fiscal_year_end_month: int
    net_income: str | None
    net_income_currency: str
    pe: str | None
    pb: str | None
    data_status: str


@dataclass(slots=True)
class InlineAnalysisPayload:
    instrument: dict[str, object]
    screen_run_ref: dict[str, object]
    candlesticks: list[dict[str, object]]
    candlestick_window_days_available: int
    valuation_by_fiscal_year: list[dict[str, object]]
    generated_at: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def get_inline_analysis_payload(
    session,
    *,
    instrument_id: int,
    screen_run_id: int | None = None,
    candle_window_days: int = DEFAULT_INLINE_ANALYSIS_CANDLE_WINDOW,
    fiscal_year_limit: int = DEFAULT_INLINE_ANALYSIS_FISCAL_YEAR_LIMIT,
    provider=None,
) -> InlineAnalysisPayload | None:
    instrument = session.get(Instrument, instrument_id)
    resolved_screen_run_id = screen_run_id or resolve_latest_stock_detail_screen_run_id(
        session,
        instrument_id=instrument_id,
    )
    if instrument is None or resolved_screen_run_id is None:
        return None

    screen_run = session.get(ScreenRun, resolved_screen_run_id)
    if screen_run is None:
        return None

    result = session.execute(
        select(ScreenRunResult)
        .where(
            ScreenRunResult.screen_run_id == resolved_screen_run_id,
            ScreenRunResult.instrument_id == instrument_id,
        )
        .limit(1)
    ).scalar_one_or_none()
    if result is None:
        return None

    candle_rows = collect_candlesticks(
        session,
        instrument_id=instrument_id,
        trade_date_cutoff=screen_run.trade_date,
        limit=candle_window_days,
    )
    fundamentals_rows = _load_recent_fundamentals(
        session,
        instrument_id=instrument_id,
        fiscal_year_limit=fiscal_year_limit,
        provider=provider,
    )

    valuation_payload = [
        asdict(
            FiscalYearValuationPayload(
                fiscal_year_label=row.fiscal_year_label,
                fiscal_year_end_month=row.fiscal_year_end_date.month,
                net_income=f"{row.net_income:.2f}" if row.net_income is not None else None,
                net_income_currency=row.net_income_currency,
                pe=f"{row.pe:.4f}" if row.pe is not None else None,
                pb=f"{row.pb:.4f}" if row.pb is not None else None,
                data_status=row.data_status,
            )
        )
        for row in fundamentals_rows
    ]

    return InlineAnalysisPayload(
        instrument={
            "id": instrument.id,
            "symbol": instrument.symbol,
            "exchange": instrument.exchange,
            "name": instrument.name,
            "currency": instrument.currency,
        },
        screen_run_ref={
            "id": screen_run.id,
            "trade_date": screen_run.trade_date.isoformat(),
        },
        candlesticks=serialize_candlestick_rows(candle_rows),
        candlestick_window_days_available=len(candle_rows),
        valuation_by_fiscal_year=valuation_payload,
        generated_at=datetime.now(UTC).isoformat(),
    )


def _load_recent_fundamentals(session, *, instrument_id: int, fiscal_year_limit: int, provider=None) -> list[FundamentalsAnnual]:
    rows = _query_fundamentals(session, instrument_id=instrument_id, fiscal_year_limit=fiscal_year_limit)
    if _should_refresh_fundamentals(rows):
        refresh_instrument_fundamentals(session, instrument_id=instrument_id, provider=provider)
        rows = _query_fundamentals(session, instrument_id=instrument_id, fiscal_year_limit=fiscal_year_limit)
    return rows


def _query_fundamentals(session, *, instrument_id: int, fiscal_year_limit: int) -> list[FundamentalsAnnual]:
    rows = session.execute(
        select(FundamentalsAnnual)
        .where(FundamentalsAnnual.instrument_id == instrument_id)
        .order_by(FundamentalsAnnual.fiscal_year_end_date.desc())
        .limit(fiscal_year_limit)
    ).scalars().all()
    rows.reverse()
    return rows


def _should_refresh_fundamentals(rows: list[FundamentalsAnnual]) -> bool:
    if not rows:
        return True
    latest_as_of = max(row.source_as_of_date for row in rows)
    return latest_as_of <= datetime.now(UTC).date() - timedelta(days=FUNDAMENTALS_REFRESH_MAX_AGE_DAYS)
