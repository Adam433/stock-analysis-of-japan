from __future__ import annotations

from fastapi import APIRouter, HTTPException

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.chart_data import get_stock_detail_payload
from stockanalyse_api.services.inline_analysis import get_inline_analysis_payload

router = APIRouter(prefix="/stocks", tags=["stocks"])


@router.get("/{instrument_id}/detail")
def read_stock_detail(instrument_id: int, screen_run_id: int | None = None) -> dict[str, object]:
    with SessionLocal() as session:
        payload = get_stock_detail_payload(
            session,
            instrument_id=instrument_id,
            screen_run_id=screen_run_id,
        )

    if payload is None:
        raise HTTPException(status_code=404, detail="Stock detail payload not found for the given instrument context.")

    return {"stock_detail": payload.to_dict()}


@router.get("/{instrument_id}/inline-analysis")
def read_stock_inline_analysis(
    instrument_id: int,
    screen_run_id: int | None = None,
) -> dict[str, object]:
    with SessionLocal() as session:
        payload = get_inline_analysis_payload(
            session,
            instrument_id=instrument_id,
            screen_run_id=screen_run_id,
        )

    if payload is None:
        raise HTTPException(status_code=404, detail="Inline analysis payload not found for the given instrument context.")

    return {"inline_analysis": payload.to_dict()}
