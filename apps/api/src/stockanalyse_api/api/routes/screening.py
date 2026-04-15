from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.screening import (
    execute_screen_run,
    get_latest_screen_run,
    get_screen_run,
    list_available_screening_trade_dates,
)

router = APIRouter(prefix="/screen", tags=["screen"])


class ScreenRunCreatePayload(BaseModel):
    trade_date: date | None = None


@router.post("/runs")
def create_screen_run(payload: ScreenRunCreatePayload | None = None) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            return {
                "screen_run": execute_screen_run(
                    session,
                    trade_date=payload.trade_date if payload is not None else None,
                ).to_dict()
            }
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/trade-dates")
def read_available_screening_trade_dates() -> dict[str, object]:
    with SessionLocal() as session:
        return {"trade_dates": [item.to_dict() for item in list_available_screening_trade_dates(session)]}


@router.get("/runs/latest")
def read_latest_screen_run() -> dict[str, object]:
    with SessionLocal() as session:
        summary = get_latest_screen_run(session)

    return {"screen_run": summary.to_dict() if summary is not None else None}


@router.get("/runs/{screen_run_id}")
def read_screen_run(screen_run_id: int) -> dict[str, object]:
    with SessionLocal() as session:
        summary = get_screen_run(session, screen_run_id)

    if summary is None:
        raise HTTPException(status_code=404, detail="Screen run not found.")

    return {"screen_run": summary.to_dict()}
