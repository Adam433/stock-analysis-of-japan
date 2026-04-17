from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.backtesting import (
    execute_backtest_run,
    get_backtest_run,
    get_latest_backtest_run,
    list_backtest_runs,
    launch_backtest_run,
)
from stockanalyse_api.services.portfolio_backtest import launch_portfolio_return_backtest
from stockanalyse_api.services.portfolio_backtest_defaults import get_portfolio_backtest_defaults

router = APIRouter(prefix="/backtests", tags=["backtests"])


class BacktestRunCreateRequest(BaseModel):
    start_date: date
    end_date: date


class PortfolioReturnBacktestRunCreateRequest(BaseModel):
    screen_run_id: int
    holding_days: int | None = None
    stop_loss_pct: float | None = None
    portfolio_cap: int | None = None
    entry_deferral_window_days: int | None = None


@router.get("/defaults")
def read_portfolio_return_backtest_defaults() -> dict[str, object]:
    return {"defaults": get_portfolio_backtest_defaults()}


@router.post("/runs")
def create_backtest_run(payload: BacktestRunCreateRequest) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            run = launch_backtest_run(session, start_date=payload.start_date, end_date=payload.end_date)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return {"backtest_run": run.to_dict()}


@router.post("/portfolio-return/runs")
def create_portfolio_return_backtest_run(
    payload: PortfolioReturnBacktestRunCreateRequest,
) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            run = launch_portfolio_return_backtest(
                session,
                screen_run_id=payload.screen_run_id,
                holding_days=payload.holding_days,
                stop_loss_pct=payload.stop_loss_pct,
                portfolio_cap=payload.portfolio_cap,
                entry_deferral_window_days=payload.entry_deferral_window_days,
            )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return {"backtest_run": run.to_dict()}


@router.get("/runs/latest")
def read_latest_backtest_run() -> dict[str, object]:
    with SessionLocal() as session:
        run = get_latest_backtest_run(session)
    return {"backtest_run": run.to_dict() if run is not None else None}


@router.get("/runs")
def read_backtest_runs(limit: int = 50, offset: int = 0) -> dict[str, object]:
    with SessionLocal() as session:
        runs = list_backtest_runs(session, limit=limit, offset=offset)
    return {"backtest_runs": [run.to_dict() for run in runs]}


@router.get("/runs/{run_id}")
def read_backtest_run(run_id: int) -> dict[str, object]:
    with SessionLocal() as session:
        run = get_backtest_run(session, run_id)

    if run is None:
        raise HTTPException(status_code=404, detail="Backtest run not found.")

    return {"backtest_run": run.to_dict()}


@router.post("/runs/{run_id}/execute")
def run_backtest(run_id: int) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            run = execute_backtest_run(session, run_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return {"backtest_run": run.to_dict()}
