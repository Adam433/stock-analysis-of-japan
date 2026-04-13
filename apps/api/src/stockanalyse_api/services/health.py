from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime

from sqlalchemy import func, select

from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.domain.operations.models import MarketDataRefreshRun


@dataclass(slots=True)
class MarketDataHealthSnapshot:
    freshness_state: str
    latest_trade_date: str | None
    age_in_days: int | None
    coverage_status: str
    total_instruments: int
    partial_rows: int
    unavailable_rows: int
    last_refresh: dict[str, object] | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _classify_freshness(latest_trade_date: date | None, today: date) -> tuple[str, int | None]:
    if latest_trade_date is None:
        return "missing", None

    age_in_days = max((today - latest_trade_date).days, 0)
    if age_in_days <= 3:
        return "fresh", age_in_days
    return "stale", age_in_days


def get_market_data_health(session, today: date | None = None) -> MarketDataHealthSnapshot:
    snapshot_date = today or datetime.now(UTC).date()
    latest_trade_date = session.execute(select(func.max(MarketDataDaily.trade_date))).scalar_one()

    partial_rows, unavailable_rows = session.execute(
        select(
            func.count().filter(MarketDataDaily.data_status == "partial"),
            func.count().filter(MarketDataDaily.data_status == "unavailable"),
        )
    ).one()
    total_instruments = session.execute(select(func.count(func.distinct(MarketDataDaily.instrument_id)))).scalar_one()

    last_refresh = session.execute(
        select(MarketDataRefreshRun)
        .order_by(MarketDataRefreshRun.started_at.desc(), MarketDataRefreshRun.id.desc())
        .limit(1)
    ).scalar_one_or_none()

    freshness_state, age_in_days = _classify_freshness(latest_trade_date, snapshot_date)
    coverage_status = "complete"

    refresh_payload: dict[str, object] | None = None
    if last_refresh is not None:
        refresh_payload = {
            "status": last_refresh.status,
            "provider": last_refresh.provider,
            "started_at": last_refresh.started_at.isoformat(),
            "completed_at": last_refresh.completed_at.isoformat() if last_refresh.completed_at else None,
            "rows_processed": last_refresh.rows_processed,
            "rows_inserted": last_refresh.rows_inserted,
            "rows_updated": last_refresh.rows_updated,
            "partial_rows": last_refresh.partial_rows,
            "unavailable_rows": last_refresh.unavailable_rows,
            "latest_trade_date": last_refresh.latest_trade_date.isoformat() if last_refresh.latest_trade_date else None,
            "error_message": last_refresh.error_message,
            "requested_symbols": [symbol for symbol in last_refresh.requested_symbols.split(",") if symbol],
        }

        if last_refresh.status == "failed":
            coverage_status = "failed"
        elif partial_rows or unavailable_rows:
            coverage_status = "partial"
        elif last_refresh.status == "partial" or last_refresh.partial_rows or last_refresh.unavailable_rows:
            coverage_status = "partial"
    elif latest_trade_date is None:
        coverage_status = "missing"

    return MarketDataHealthSnapshot(
        freshness_state=freshness_state,
        latest_trade_date=latest_trade_date.isoformat() if latest_trade_date else None,
        age_in_days=age_in_days,
        coverage_status=coverage_status,
        total_instruments=total_instruments,
        partial_rows=partial_rows,
        unavailable_rows=unavailable_rows,
        last_refresh=refresh_payload,
    )
