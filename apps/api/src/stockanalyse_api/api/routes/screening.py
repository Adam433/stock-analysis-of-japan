from __future__ import annotations

from fastapi import APIRouter, HTTPException

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.screening import execute_screen_run, get_latest_screen_run, get_screen_run

router = APIRouter(prefix="/screen", tags=["screen"])


@router.post("/runs")
def create_screen_run() -> dict[str, object]:
    try:
        with SessionLocal() as session:
            return {"screen_run": execute_screen_run(session).to_dict()}
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


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
