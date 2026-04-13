from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.backtesting import get_backtest_run, get_latest_backtest_run, launch_backtest_run

router = APIRouter(prefix="/backtests", tags=["backtests"])


class BacktestRunCreateRequest(BaseModel):
    start_date: date
    end_date: date


@router.post("/runs")
def create_backtest_run(payload: BacktestRunCreateRequest) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            run = launch_backtest_run(session, start_date=payload.start_date, end_date=payload.end_date)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return {"backtest_run": run.to_dict()}


@router.get("/runs/latest")
def read_latest_backtest_run() -> dict[str, object]:
    with SessionLocal() as session:
        run = get_latest_backtest_run(session)
    return {"backtest_run": run.to_dict() if run is not None else None}


@router.get("/runs/{run_id}")
def read_backtest_run(run_id: int) -> dict[str, object]:
    with SessionLocal() as session:
        run = get_backtest_run(session, run_id)

    if run is None:
        raise HTTPException(status_code=404, detail="Backtest run not found.")

    return {"backtest_run": run.to_dict()}
