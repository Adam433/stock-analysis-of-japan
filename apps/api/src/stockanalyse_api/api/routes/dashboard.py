from __future__ import annotations

from datetime import date
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.dashboard import (
    APPROVED_RPS_WINDOWS,
    DEFAULT_CHART_WINDOW_DAYS,
    DEFAULT_RPS_THRESHOLD,
    get_chart_with_markers,
    get_overview,
    screen_universe,
)
from stockanalyse_api.services.dashboard_ingest import (
    DEFAULT_DASHBOARD_MATERIALIZE_SINCE_DAYS,
    get_job_state,
    trigger_update_and_materialize,
)

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

_INDEX_HTML_PATH = Path(__file__).resolve().parent.parent / "templates" / "dashboard.html"


class ScreenRequest(BaseModel):
    use_rps: bool = False
    rps_threshold: int = Field(default=DEFAULT_RPS_THRESHOLD, ge=0, le=100)
    selected_rps_windows: list[int] = Field(default_factory=lambda: list(APPROVED_RPS_WINDOWS))
    use_cup_handle: bool = False
    trade_date: date | None = None


class ChartRequest(BaseModel):
    use_rps: bool = False
    rps_threshold: int = Field(default=DEFAULT_RPS_THRESHOLD, ge=0, le=100)
    selected_rps_windows: list[int] = Field(default_factory=lambda: list(APPROVED_RPS_WINDOWS))
    use_cup_handle: bool = False
    trade_date: date | None = None
    window_days: int = Field(default=DEFAULT_CHART_WINDOW_DAYS, ge=30, le=750)


@router.get("", response_class=HTMLResponse, include_in_schema=False)
@router.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard_index() -> HTMLResponse:
    html = _INDEX_HTML_PATH.read_text(encoding="utf-8")
    return HTMLResponse(content=html)


@router.get("/api/overview")
def read_overview() -> dict[str, object]:
    with SessionLocal() as session:
        return {"overview": get_overview(session).to_dict()}


@router.post("/api/screen")
def post_screen(payload: ScreenRequest) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            return screen_universe(
                session,
                use_rps=payload.use_rps,
                rps_threshold=payload.rps_threshold,
                selected_rps_windows=payload.selected_rps_windows,
                use_cup_handle=payload.use_cup_handle,
                trade_date=payload.trade_date,
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


class IngestRequest(BaseModel):
    materialize_since_days: int | None = DEFAULT_DASHBOARD_MATERIALIZE_SINCE_DAYS
    skip_refresh: bool = False


@router.post("/api/update")
def post_update_and_materialize(payload: IngestRequest | None = None) -> dict[str, object]:
    payload = payload or IngestRequest()
    return trigger_update_and_materialize(
        materialize_since_days=payload.materialize_since_days,
        skip_refresh=payload.skip_refresh,
    )


@router.get("/api/update/status")
def read_update_status() -> dict[str, object]:
    return {"state": get_job_state()}


@router.post("/api/chart/{instrument_id}")
def post_chart(instrument_id: int, payload: ChartRequest) -> dict[str, object]:
    with SessionLocal() as session:
        result = get_chart_with_markers(
            session,
            instrument_id=instrument_id,
            use_rps=payload.use_rps,
            rps_threshold=payload.rps_threshold,
            selected_rps_windows=payload.selected_rps_windows,
            use_cup_handle=payload.use_cup_handle,
            trade_date=payload.trade_date,
            window_days=payload.window_days,
        )
    if result is None:
        raise HTTPException(status_code=404, detail="Instrument or candles not found.")
    return result
