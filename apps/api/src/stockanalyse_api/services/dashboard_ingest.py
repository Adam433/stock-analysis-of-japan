from __future__ import annotations

import logging
import threading
import traceback
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from sqlalchemy import select

from stockanalyse_api.config.settings import (
    get_auto_refresh_commit_every,
    get_auto_refresh_csv_dir,
    get_auto_refresh_fixture_path,
    get_auto_refresh_provider,
    get_auto_refresh_symbols_file,
)
from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.services.factor_materialization import materialize_derived_indicator_facts
from stockanalyse_api.services.ingestion.providers.registry import (
    build_ingestion_provider,
)
from stockanalyse_api.services.ingestion.refresh_service import DEFAULT_UNIVERSE_FILTER
from stockanalyse_api.services.ingestion.refresh_service import execute_market_data_refresh

logger = logging.getLogger(__name__)

DEFAULT_DASHBOARD_MATERIALIZE_SINCE_DAYS = 30


@dataclass(slots=True)
class IngestJobState:
    status: str = "idle"  # idle | running | completed | failed
    job_kind: str | None = None  # refresh | materialize | combined
    phase: str | None = None  # refresh | materialize
    started_at: str | None = None
    finished_at: str | None = None
    refresh_provider: str | None = None
    universe_count: int = 0
    refresh_processed: int = 0
    refresh_inserted: int = 0
    refresh_updated: int = 0
    refresh_latest_trade_date: str | None = None
    materialize_since_date: str | None = None
    materialize_scanned_dates: int = 0
    materialize_scan_total_dates: int = 0
    materialize_processed_dates: int = 0
    materialize_total_dates: int = 0
    materialize_latest_trade_date: str | None = None
    materialize_inserted: int = 0
    materialize_updated: int = 0
    error: str | None = None
    log_tail: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


_state_lock = threading.Lock()
_job_state = IngestJobState()
_worker_thread: threading.Thread | None = None


def get_job_state() -> dict[str, object]:
    with _state_lock:
        return _job_state.to_dict()


def _append_log(line: str) -> None:
    with _state_lock:
        _job_state.log_tail.append(line)
        # keep tail bounded
        if len(_job_state.log_tail) > 50:
            _job_state.log_tail = _job_state.log_tail[-50:]


def _set_state(**updates) -> None:
    with _state_lock:
        for key, value in updates.items():
            setattr(_job_state, key, value)


def trigger_update_and_materialize(
    *,
    materialize_since_days: int | None = DEFAULT_DASHBOARD_MATERIALIZE_SINCE_DAYS,
    skip_refresh: bool = False,
    skip_materialize: bool = False,
) -> dict[str, object]:
    global _worker_thread
    with _state_lock:
        if skip_refresh and skip_materialize:
            return {"started": False, "reason": "no_work", "state": _job_state.to_dict()}
        if _job_state.status == "running":
            return {"started": False, "reason": "already_running", "state": _job_state.to_dict()}
        job_kind = "combined"
        if skip_materialize:
            job_kind = "refresh"
        elif skip_refresh:
            job_kind = "materialize"
        # Reset state for a fresh run.
        globals()["_job_state"] = IngestJobState(
            status="running",
            job_kind=job_kind,
            phase="refresh" if not skip_refresh else "materialize",
            started_at=datetime.now(UTC).isoformat(),
        )

    _worker_thread = threading.Thread(
        target=_run_pipeline,
        name="dashboard-update-materialize",
        kwargs={
            "materialize_since_days": materialize_since_days,
            "skip_refresh": skip_refresh,
            "skip_materialize": skip_materialize,
        },
        daemon=True,
    )
    _worker_thread.start()
    return {"started": True, "state": get_job_state()}


def _run_pipeline(
    *,
    materialize_since_days: int | None = None,
    skip_refresh: bool = False,
    skip_materialize: bool = False,
) -> None:
    try:
        if not skip_refresh:
            _set_state(phase="refresh")
            provider_name = get_auto_refresh_provider()
            provider = build_ingestion_provider(
                provider_name,
                fixture_path=get_auto_refresh_fixture_path(),
                csv_dir=get_auto_refresh_csv_dir(),
                symbols_file=get_auto_refresh_symbols_file(),
            )
            with SessionLocal() as session:
                symbols = _load_existing_tse_symbols(session)
                _set_state(refresh_provider=provider_name, universe_count=len(symbols))
                _append_log(f"refresh universe resolved: {len(symbols)} existing TSE symbols")
                result = execute_market_data_refresh(
                    session,
                    provider,
                    symbols,
                    all_supported=False,
                    universe_filter=DEFAULT_UNIVERSE_FILTER,
                    commit_every=get_auto_refresh_commit_every(),
                )
            _set_state(
                refresh_processed=int(result.get("processed", result.get("rows_processed", 0)) or 0),
                refresh_inserted=int(result.get("inserted", result.get("rows_inserted", 0)) or 0),
                refresh_updated=int(result.get("updated", result.get("rows_updated", 0)) or 0),
                refresh_latest_trade_date=str(result.get("latest_trade_date") or "") or None,
            )
            _append_log(
                "refresh done: processed={} inserted={} updated={} latest={}".format(
                    _job_state.refresh_processed,
                    _job_state.refresh_inserted,
                    _job_state.refresh_updated,
                    _job_state.refresh_latest_trade_date,
                )
            )
        else:
            _append_log("refresh skipped (skip_refresh=True)")

        if skip_materialize:
            _append_log("materialization skipped (skip_materialize=True)")
            _set_state(
                status="completed",
                phase=None,
                finished_at=datetime.now(UTC).isoformat(),
            )
            return

        _set_state(phase="materialize")

        def report_progress(payload: dict[str, object]) -> None:
            _set_state(
                materialize_scanned_dates=int(payload.get("scanned_trade_dates", 0) or 0),
                materialize_processed_dates=int(payload.get("processed_trade_dates", 0) or 0),
                materialize_latest_trade_date=str(payload.get("trade_date") or "") or None,
                materialize_inserted=int(payload.get("inserted", 0) or 0),
                materialize_updated=int(payload.get("updated", 0) or 0),
            )

        with SessionLocal() as session:
            from sqlalchemy import func, select
            from stockanalyse_api.domain.market_data.models import MarketDataDaily

            total_dates = (
                session.execute(select(func.count(func.distinct(MarketDataDaily.trade_date)))).scalar_one()
                or 0
            )
            since_date = None
            if materialize_since_days is not None:
                # Pick the date `materialize_since_days` distinct trading days back
                # from the most-recent broadly-covered trade_date (skips orphan
                # seed rows that would otherwise inflate the window).
                recent_dates = session.execute(
                    select(MarketDataDaily.trade_date)
                    .group_by(MarketDataDaily.trade_date)
                    .having(func.count() > 100)
                    .order_by(MarketDataDaily.trade_date.desc())
                    .limit(materialize_since_days + 1)
                ).scalars().all()
                if recent_dates:
                    since_date = recent_dates[-1]
            target_dates = _count_materialize_target_dates(
                session,
                since_date=since_date,
            )
            _set_state(
                materialize_scan_total_dates=target_dates if since_date is not None else total_dates,
                materialize_total_dates=target_dates,
                materialize_since_date=since_date.isoformat() if since_date else None,
            )

            mat_result = materialize_derived_indicator_facts(
                session,
                progress_callback=report_progress,
                commit_every_dates=1,
                since_date=since_date,
            )

        _set_state(
            materialize_inserted=int(mat_result.get("inserted", 0) or 0),
            materialize_updated=int(mat_result.get("updated", 0) or 0),
        )
        _append_log(
            "materialize done: inserted={} updated={}".format(
                _job_state.materialize_inserted,
                _job_state.materialize_updated,
            )
        )

        _set_state(
            status="completed",
            phase=None,
            finished_at=datetime.now(UTC).isoformat(),
        )
    except Exception as exc:
        logger.exception("Dashboard update/materialization pipeline failed")
        _set_state(
            status="failed",
            phase=None,
            finished_at=datetime.now(UTC).isoformat(),
            error=f"{type(exc).__name__}: {exc}",
        )
        _append_log("ERROR " + traceback.format_exc().splitlines()[-1])


def _load_existing_tse_symbols(session) -> list[str]:
    symbols = session.execute(
        select(Instrument.symbol)
        .where(Instrument.exchange == "TSE")
        .order_by(Instrument.symbol.asc())
    ).scalars().all()
    if not symbols:
        raise ValueError("No TSE instruments are available in the database to refresh.")
    return list(symbols)


def _count_materialize_target_dates(session, *, since_date) -> int:
    from sqlalchemy import func, select
    from stockanalyse_api.domain.market_data.models import MarketDataDaily

    query = select(func.count(func.distinct(MarketDataDaily.trade_date)))
    if since_date is not None:
        query = query.where(MarketDataDaily.trade_date >= since_date)
    return int(session.execute(query).scalar_one() or 0)
