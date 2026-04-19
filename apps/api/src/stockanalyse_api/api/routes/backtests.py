from __future__ import annotations

from datetime import date
from decimal import Decimal

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.screening import get_screen_run
from stockanalyse_api.services.backtesting import (
    execute_backtest_run,
    get_backtest_run,
    get_latest_backtest_run,
    list_backtest_runs,
    launch_backtest_run,
)
from stockanalyse_api.services.portfolio_backtest import launch_portfolio_return_backtest
from stockanalyse_api.services.portfolio_backtest_defaults import get_portfolio_backtest_defaults
from stockanalyse_api.services.portfolio_backtest_metrics import (
    calculate_max_drawdown,
    calculate_win_rate,
)
from stockanalyse_api.services.portfolio_backtest_traceability import (
    SourceScreenRunUnavailableError,
    SOURCE_SCREEN_RUN_UNAVAILABLE_ERROR,
    resolve_screen_run_or_unavailable,
    resolve_semantics_via_source_screen_run,
)

router = APIRouter(prefix="/backtests", tags=["backtests"])


class BacktestRunCreateRequest(BaseModel):
    start_date: date
    end_date: date


class PortfolioReturnBacktestRunCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    screen_run_id: int
    holding_days: int | None = None
    stop_loss_pct: float | None = None
    portfolio_cap: int | None = None
    entry_deferral_window_days: int | None = None


def _require_portfolio_return_completed_run(session, run_id: int):
    run = get_backtest_run(session, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Backtest run not found.")
    if run.backtest_lifecycle != "portfolio_return":
        raise HTTPException(status_code=422, detail="Only portfolio_return runs support this endpoint.")
    if run.status != "completed":
        raise HTTPException(status_code=422, detail="Only completed portfolio_return runs support this endpoint.")
    return run


def _format_ratio(value: Decimal) -> str:
    return f"{value:.6f}"


def _serialize_source_screen_run(session, screen_run_id: int | None) -> dict[str, object] | None:
    if screen_run_id is None:
        return None

    screen_run = get_screen_run(session, screen_run_id)
    if screen_run is None:
        return None

    return {
        "id": screen_run.id,
        "trade_date": screen_run.trade_date,
        "strategy_configuration_version": screen_run.parameter_set["version"],
        "status": screen_run.status,
    }


def _build_aligned_equity_curve(equity_curve: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "days_since_entry": index,
            "equity": point["equity"],
        }
        for index, point in enumerate(equity_curve)
    ]


def _resolve_traceability_or_raise(session, run_id: int) -> dict[str, object]:
    try:
        return resolve_semantics_via_source_screen_run(session, run_id)
    except SourceScreenRunUnavailableError as exc:
        raise
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _build_source_screen_run_unavailable_response(*, backtest_run_id: int) -> JSONResponse:
    return JSONResponse(
        status_code=410,
        content={
            "error": SOURCE_SCREEN_RUN_UNAVAILABLE_ERROR,
            "error_code": SOURCE_SCREEN_RUN_UNAVAILABLE_ERROR,
            "backtest_run_id": backtest_run_id,
        },
    )


def _serialize_portfolio_return_result(session, run_id: int) -> dict[str, object]:
    run = _require_portfolio_return_completed_run(session, run_id)
    if resolve_screen_run_or_unavailable(session, run_id) is None:
        raise SourceScreenRunUnavailableError(run.id)
    win_rate = calculate_win_rate(run.per_security_returns)
    max_drawdown = calculate_max_drawdown(run.equity_curve)

    return {
        "run": run.to_dict(),
        "cumulative_return": run.cumulative_return,
        "win_rate": _format_ratio(win_rate),
        "max_drawdown": _format_ratio(max_drawdown),
        "equity_curve": run.equity_curve,
        "per_security_returns": run.per_security_returns,
        "source_screen_run": _serialize_source_screen_run(session, run.source_screen_run_id),
    }


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


@router.get("/portfolio-return/runs/{run_id}/result")
def read_portfolio_return_backtest_result(run_id: int) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            result = _serialize_portfolio_return_result(session, run_id)
    except SourceScreenRunUnavailableError as exc:
        return _build_source_screen_run_unavailable_response(backtest_run_id=exc.backtest_run_id)
    return {"result": result}


@router.get("/portfolio-return/runs/{run_id}/trace")
def read_portfolio_return_backtest_trace(run_id: int) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            trace = _resolve_traceability_or_raise(session, run_id)
    except SourceScreenRunUnavailableError as exc:
        return _build_source_screen_run_unavailable_response(backtest_run_id=exc.backtest_run_id)
    return {"trace": trace}


@router.get("/portfolio-return/runs/{run_id}/semantics-snapshot")
def read_portfolio_return_backtest_semantics_snapshot(run_id: int) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            trace = _resolve_traceability_or_raise(session, run_id)
    except SourceScreenRunUnavailableError as exc:
        return _build_source_screen_run_unavailable_response(backtest_run_id=exc.backtest_run_id)
    return {"semantics_snapshot": trace["semantics_snapshot"]}


@router.get("/portfolio-return/runs/compare")
def compare_portfolio_return_backtest_runs(ids: str) -> dict[str, object]:
    try:
        run_ids = []
        seen_run_ids: set[int] = set()
        for raw_id in ids.split(","):
            trimmed_id = raw_id.strip()
            if not trimmed_id:
                continue
            run_id = int(trimmed_id)
            if run_id < 1:
                raise ValueError
            if run_id not in seen_run_ids:
                seen_run_ids.add(run_id)
                run_ids.append(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="ids must be a comma-separated list of positive integers.") from exc

    if len(run_ids) < 2:
        raise HTTPException(status_code=422, detail="At least two run ids are required for comparison.")

    try:
        with SessionLocal() as session:
            runs = []
            for run_id in run_ids:
                result = _serialize_portfolio_return_result(session, run_id)
                run = result["run"]
                semantics_snapshot = _resolve_traceability_or_raise(session, run_id)["semantics_snapshot"]
                runs.append(
                    {
                        **result,
                        "compare_dimensions": {
                            "holding_days": run["effective_holding_days"],
                            "stop_loss_pct": run["effective_stop_loss_pct"],
                            "portfolio_cap": run["effective_portfolio_cap"],
                            "source_screen_run_id": run["source_screen_run_id"],
                            "source_trade_date": (
                                result["source_screen_run"]["trade_date"] if result["source_screen_run"] is not None else None
                            ),
                            "strategy_configuration_version": (
                                result["source_screen_run"]["strategy_configuration_version"]
                                if result["source_screen_run"] is not None
                                else None
                            ),
                            "rps_definition_version": semantics_snapshot["rps_definition_version"],
                        },
                        "aligned_equity_curve": _build_aligned_equity_curve(result["equity_curve"]),
                    }
                )
    except SourceScreenRunUnavailableError as exc:
        return _build_source_screen_run_unavailable_response(backtest_run_id=exc.backtest_run_id)

    return {"runs": runs}


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
