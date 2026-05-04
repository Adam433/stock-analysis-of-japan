from __future__ import annotations

import os
import json
import math
import random
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import fields
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from itertools import product
from typing import Any

from sqlalchemy import func, select

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.backtests.models import (
    OptimizationResult,
    OptimizationResultDetailCache,
    OptimizationRun,
)
from stockanalyse_api.services.dashboard import (
    DEFAULT_CUP_HANDLE_PARAMS,
    DEFAULT_FUNDAMENTAL_GROWTH_PARAMS,
    CupHandleParams,
    FundamentalGrowthParams,
    get_overview,
    normalize_market,
)
from stockanalyse_api.services.dashboard_strategy_backtest import (
    BacktestCancelledError,
    run_cup_handle_rps_backtest,
)

DEFAULT_OPTIMIZATION_OBJECTIVE = "score"
DEFAULT_MAX_PARAMETER_SETS = 1000
DEFAULT_OPTIMIZATION_PARALLEL_GROUP_SIZE = 1
DEFAULT_OPTIMIZATION_DETAIL_CACHE_TRADES = 300
MAX_AUTO_OPTIMIZATION_WORKERS = 6
DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS = FundamentalGrowthParams(
    enabled=True,
    min_years=3,
    min_growth_count=2,
    min_yoy_growth_pct=Decimal("0"),
    require_positive_net_income=True,
    reporting_lag_days=120,
    max_pe=Decimal("60"),
    max_pb=Decimal("15"),
    require_positive_operating_cash_flow=True,
    require_positive_free_cash_flow=False,
    min_operating_cash_flow_growth_count=1,
    min_operating_cash_flow_yoy_growth_pct=Decimal("0"),
)
RATIO_PATTERN = Decimal("0.000001")
SUPPORTED_OPTIMIZATION_OBJECTIVES = {
    "score",
    "average_annualized_return",
    "robust_annualized_return",
    "annualized_return",
    "max_drawdown",
    "return_drawdown_ratio",
    "win_rate",
    "total_return",
}
SUPPORTED_SEARCH_MODES = {"grid", "random"}

_WORKER_SCREEN_CACHE: dict[str, dict[str, object]] = {}
_WORKER_SCREEN_CANDIDATE_CACHE: dict[tuple[object, ...], dict[str, object]] = {}
_WORKER_FUNDAMENTAL_GROWTH_CACHE: dict[tuple[object, ...], object] = {}
_WORKER_CUP_EVENT_CACHE: dict[tuple[object, ...], dict[int, list[object]] | None] = {}
_WORKER_TRADE_CACHE: dict[tuple[object, ...], object] = {}
_WORKER_TRADE_DATES_CACHE: dict[tuple[object, ...], list[date]] = {}
_WORKER_FUTURE_ROWS_CACHE: dict[tuple[object, ...], object] = {}
_WORKER_FUTURE_INDICATOR_CACHE: dict[tuple[object, ...], object] = {}


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime, Decimal)):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable.")


def dump_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default)


def load_json(value: str | None, *, default: Any = None) -> Any:
    if value is None:
        return default
    return json.loads(value)


def stable_parameter_hash(parameters: dict[str, object]) -> str:
    return sha256(dump_json(parameters).encode("utf-8")).hexdigest()


def _as_list(value: object, *, default: list[object]) -> list[object]:
    if value is None:
        return default
    if isinstance(value, list):
        return value
    return [value]


def _optimization_metadata(
    *,
    search_mode: str,
    random_seed: int | None,
    max_workers: int | str | None = None,
) -> dict[str, object]:
    metadata: dict[str, object] = {"search_mode": search_mode}
    if random_seed is not None:
        metadata["random_seed"] = random_seed
    metadata["max_workers"] = max_workers if max_workers is not None else "auto"
    return metadata


def _split_parameter_space_metadata(
    parameter_space: dict[str, object] | None,
) -> tuple[dict[str, object], dict[str, object]]:
    raw_space = dict(parameter_space or {})
    metadata = raw_space.pop("_optimization", {})
    if not isinstance(metadata, dict):
        metadata = {}
    return raw_space, metadata


def _coerce_configured_max_workers(value: object) -> int | str | None:
    if value is None or value == "":
        return None
    if isinstance(value, str) and value.strip().lower() == "auto":
        return "auto"
    workers = int(value)
    if workers < 1:
        raise ValueError("max_workers must be at least 1 or auto.")
    return workers


def _configured_max_workers_from_metadata(metadata: dict[str, object]) -> int | str | None:
    return _coerce_configured_max_workers(metadata.get("max_workers", "auto"))


def _parameter_axes(parameter_space: dict[str, object]) -> list[tuple[str, list[object]]]:
    return [
        ("use_rps", _as_list(parameter_space.get("use_rps"), default=[True])),
        ("rps_threshold", _as_list(parameter_space.get("rps_threshold"), default=[90])),
        (
            "selected_rps_windows",
            _as_list(parameter_space.get("selected_rps_windows"), default=[[50, 120, 250]]),
        ),
        (
            "min_rps_windows_passing",
            _as_list(parameter_space.get("min_rps_windows_passing"), default=[1]),
        ),
        ("use_cup_handle", _as_list(parameter_space.get("use_cup_handle"), default=[True])),
        ("cup_handle_params", _as_list(parameter_space.get("cup_handle_params"), default=[{}])),
        (
            "fundamental_growth_params",
            _as_list(
                parameter_space.get("fundamental_growth_params"),
                default=[DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS.to_dict()],
            ),
        ),
        ("holding_days", _as_list(parameter_space.get("holding_days"), default=[130])),
        ("stop_loss_pct", _as_list(parameter_space.get("stop_loss_pct"), default=["-0.08"])),
        ("take_profit_pct", _as_list(parameter_space.get("take_profit_pct"), default=[None])),
        ("rps_exit_threshold", _as_list(parameter_space.get("rps_exit_threshold"), default=[None])),
        ("portfolio_cap", _as_list(parameter_space.get("portfolio_cap"), default=[10])),
        (
            "position_weight_pct",
            _as_list(parameter_space.get("position_weight_pct"), default=["0.10"]),
        ),
        (
            "allow_reentry_while_open",
            _as_list(parameter_space.get("allow_reentry_while_open"), default=[False]),
        ),
        ("entry_delay_days", _as_list(parameter_space.get("entry_delay_days"), default=[0])),
        (
            "entry_deferral_window_days",
            _as_list(parameter_space.get("entry_deferral_window_days"), default=[5]),
        ),
    ]


def _normalize_raw_parameter_values(
    names: list[str],
    values: tuple[object, ...],
) -> dict[str, object]:
    return _normalize_parameter_set(dict(zip(names, values, strict=True)))


def _coerce_decimal(value: object, *, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be a decimal number.") from exc


def _coerce_optional_decimal(value: object, *, field_name: str) -> Decimal | None:
    if value is None or value == "":
        return None
    return _coerce_decimal(value, field_name=field_name)


def _coerce_optional_int(value: object, *, field_name: str) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _dataclass_payload(default_obj: object, overrides: dict[str, object] | None) -> dict[str, object]:
    payload = {field.name: getattr(default_obj, field.name) for field in fields(default_obj)}
    payload.update(overrides or {})
    return payload


def _cup_handle_params_from_payload(payload: dict[str, object] | None) -> CupHandleParams:
    merged = _dataclass_payload(DEFAULT_CUP_HANDLE_PARAMS, payload)
    decimal_fields = {
        field.name
        for field in fields(CupHandleParams)
        if isinstance(getattr(DEFAULT_CUP_HANDLE_PARAMS, field.name), Decimal)
    }
    return CupHandleParams(
        **{
            key: _coerce_decimal(value, field_name=key) if key in decimal_fields else value
            for key, value in merged.items()
        }
    )


def _fundamental_params_from_payload(
    payload: dict[str, object] | None,
    *,
    default_params: FundamentalGrowthParams = DEFAULT_FUNDAMENTAL_GROWTH_PARAMS,
    force_enabled: bool = False,
) -> FundamentalGrowthParams:
    merged = _dataclass_payload(default_params, payload)
    if force_enabled:
        merged["enabled"] = True
    return FundamentalGrowthParams(
        enabled=bool(merged["enabled"]),
        min_years=int(merged["min_years"]),
        min_growth_count=(
            None if merged.get("min_growth_count") is None else int(merged["min_growth_count"])
        ),
        min_yoy_growth_pct=_coerce_decimal(
            merged["min_yoy_growth_pct"],
            field_name="min_yoy_growth_pct",
        ),
        require_positive_net_income=bool(merged["require_positive_net_income"]),
        reporting_lag_days=int(merged["reporting_lag_days"]),
        max_pe=(
            None
            if merged.get("max_pe") is None
            else _coerce_decimal(merged["max_pe"], field_name="max_pe")
        ),
        max_pb=(
            None
            if merged.get("max_pb") is None
            else _coerce_decimal(merged["max_pb"], field_name="max_pb")
        ),
        require_positive_operating_cash_flow=bool(
            merged["require_positive_operating_cash_flow"]
        ),
        require_positive_free_cash_flow=bool(merged["require_positive_free_cash_flow"]),
        min_operating_cash_flow_growth_count=(
            None
            if merged.get("min_operating_cash_flow_growth_count") is None
            else int(merged["min_operating_cash_flow_growth_count"])
        ),
        min_operating_cash_flow_yoy_growth_pct=_coerce_decimal(
            merged["min_operating_cash_flow_yoy_growth_pct"],
            field_name="min_operating_cash_flow_yoy_growth_pct",
        ),
    )


def _normalize_parameter_set(parameters: dict[str, object]) -> dict[str, object]:
    use_rps = bool(parameters.get("use_rps", True))
    use_cup_handle = bool(parameters.get("use_cup_handle", True))
    cup_params = _cup_handle_params_from_payload(
        parameters.get("cup_handle_params")
        if use_cup_handle and isinstance(parameters.get("cup_handle_params"), dict)
        else {}
    )
    fundamental_params = _fundamental_params_from_payload(
        parameters.get("fundamental_growth_params")
        if isinstance(parameters.get("fundamental_growth_params"), dict)
        else {},
        default_params=DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS,
        force_enabled=True,
    )
    if not fundamental_params.enabled:
        fundamental_params = DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS
    selected_windows = [int(window) for window in parameters.get("selected_rps_windows", [50, 120, 250])]  # type: ignore[arg-type]
    selected_windows = sorted(set(selected_windows), key=selected_windows.index)
    take_profit_pct = _coerce_optional_decimal(
        parameters.get("take_profit_pct"),
        field_name="take_profit_pct",
    )
    if take_profit_pct is not None and take_profit_pct <= Decimal("0"):
        raise ValueError("take_profit_pct must be greater than 0 when provided.")
    rps_exit_threshold = _coerce_optional_int(
        parameters.get("rps_exit_threshold"),
        field_name="rps_exit_threshold",
    )
    if rps_exit_threshold is not None and not 0 <= rps_exit_threshold <= 100:
        raise ValueError("rps_exit_threshold must be between 0 and 100 when provided.")
    if not use_rps:
        rps_exit_threshold = None
        selected_windows = [50, 120, 250]
    holding_days = _coerce_optional_int(
        parameters.get("holding_days", 130),
        field_name="holding_days",
    )
    if holding_days is not None and holding_days < 1:
        raise ValueError("holding_days must be greater than or equal to 1.")
    position_weight_pct = _coerce_decimal(
        parameters.get("position_weight_pct", "0.10"),
        field_name="position_weight_pct",
    )
    if position_weight_pct <= Decimal("0") or position_weight_pct > Decimal("1"):
        raise ValueError("position_weight_pct must be greater than 0 and less than or equal to 1.")
    return {
        "use_rps": use_rps,
        "rps_threshold": int(parameters.get("rps_threshold", 90)) if use_rps else 0,
        "selected_rps_windows": selected_windows,
        "min_rps_windows_passing": int(parameters.get("min_rps_windows_passing", 1)) if use_rps else 1,
        "use_cup_handle": use_cup_handle,
        "cup_handle_params": cup_params.to_dict(),
        "fundamental_growth_params": fundamental_params.to_dict(),
        "holding_days": holding_days,
        "stop_loss_pct": f"{_coerce_decimal(parameters.get('stop_loss_pct', '-0.08'), field_name='stop_loss_pct'):.4f}",
        "take_profit_pct": f"{take_profit_pct:.4f}" if take_profit_pct is not None else None,
        "rps_exit_threshold": rps_exit_threshold,
        "portfolio_cap": int(parameters.get("portfolio_cap", 10)),
        "position_weight_pct": f"{position_weight_pct:.4f}",
        "allow_reentry_while_open": bool(parameters.get("allow_reentry_while_open", False)),
        "entry_delay_days": int(parameters.get("entry_delay_days", 0)),
        "entry_deferral_window_days": int(parameters.get("entry_deferral_window_days", 5)),
    }


def build_parameter_sets(
    parameter_space: dict[str, object] | None,
    *,
    max_parameter_sets: int = DEFAULT_MAX_PARAMETER_SETS,
    search_mode: str = "grid",
    random_seed: int | None = None,
) -> list[dict[str, object]]:
    parameter_space, metadata = _split_parameter_space_metadata(parameter_space)
    if metadata.get("search_mode"):
        search_mode = str(metadata["search_mode"])
    if metadata.get("random_seed") is not None:
        random_seed = int(metadata["random_seed"])
    if search_mode not in SUPPORTED_SEARCH_MODES:
        raise ValueError("search_mode must be one of: " + ", ".join(sorted(SUPPORTED_SEARCH_MODES)) + ".")
    if max_parameter_sets < 1:
        raise ValueError("max_parameter_sets must be at least 1.")
    axes = _parameter_axes(parameter_space)
    axis_names = [name for name, _ in axes]
    axis_values = [values for _, values in axes]
    parameter_sets: list[dict[str, object]] = []
    seen_hashes: set[str] = set()

    def append_combination(combination: tuple[object, ...]) -> None:
        normalized = _normalize_raw_parameter_values(axis_names, combination)
        parameter_hash = stable_parameter_hash(normalized)
        if parameter_hash in seen_hashes:
            return
        seen_hashes.add(parameter_hash)
        parameter_sets.append(normalized)

    if search_mode == "grid":
        for combination in product(*axis_values):
            append_combination(combination)
            if len(parameter_sets) > max_parameter_sets:
                raise ValueError(
                    f"Parameter space expands to more than {max_parameter_sets} combinations."
                )
        return parameter_sets

    population_size = math.prod(len(values) for values in axis_values)
    if population_size <= max_parameter_sets:
        for combination in product(*axis_values):
            append_combination(combination)
        rng = random.Random(random_seed)
        rng.shuffle(parameter_sets)
        return parameter_sets

    rng = random.Random(random_seed)
    max_attempts = max(max_parameter_sets * 20, 1000)
    attempts = 0
    while len(parameter_sets) < max_parameter_sets and attempts < max_attempts:
        attempts += 1
        append_combination(tuple(rng.choice(values) for values in axis_values))
    if not parameter_sets:
        raise ValueError("Parameter space produced no combinations.")
    return parameter_sets


def _data_snapshot(session, *, market: str, require_data_ready: bool) -> dict[str, object]:
    overview = get_overview(session, market=market).to_dict()
    if require_data_ready:
        if int(overview["instruments_with_market_data"]) == 0:
            raise ValueError("No market data is available for optimization.")
        if int(overview["instruments_with_indicators_at_latest"]) == 0:
            raise ValueError("No derived indicator data is available for optimization.")
    return {
        "market": market,
        "overview": overview,
        "captured_at": datetime.now(UTC).isoformat(),
    }


def _isoformat_utc(value):
    if value is None:
        return None
    if getattr(value, "tzinfo", None) is None:
        value = value.replace(tzinfo=UTC)
    return value.isoformat()


def serialize_optimization_run(run: OptimizationRun) -> dict[str, object]:
    parameter_space, metadata = _split_parameter_space_metadata(
        load_json(run.parameter_space_json, default={})
    )
    return {
        "id": run.id,
        "market": run.market,
        "train_start_date": run.train_start_date.isoformat(),
        "train_end_date": run.train_end_date.isoformat(),
        "validation_start_date": (
            run.validation_start_date.isoformat() if run.validation_start_date else None
        ),
        "validation_end_date": (
            run.validation_end_date.isoformat() if run.validation_end_date else None
        ),
        "objective": run.objective,
        "status": run.status,
        "total_parameter_sets": run.total_parameter_sets,
        "completed_parameter_sets": run.completed_parameter_sets,
        "failed_parameter_sets": run.failed_parameter_sets,
        "best_result_id": run.best_result_id,
        "started_at": _isoformat_utc(run.started_at),
        "completed_at": _isoformat_utc(run.completed_at),
        "error_message": run.error_message,
        "search_mode": metadata.get("search_mode", "grid"),
        "random_seed": metadata.get("random_seed"),
        "max_workers": metadata.get("max_workers", "auto"),
        "parameter_space": parameter_space,
        "parameter_sets": load_json(run.parameter_sets_json, default=[]),
        "data_snapshot": load_json(run.data_snapshot_json, default={}),
    }


def serialize_optimization_result(result: OptimizationResult) -> dict[str, object]:
    return {
        "id": result.id,
        "optimization_run_id": result.optimization_run_id,
        "parameter_hash": result.parameter_hash,
        "parameters": load_json(result.parameters_json, default={}),
        "train_metrics": load_json(result.train_metrics_json, default=None),
        "validation_metrics": load_json(result.validation_metrics_json, default=None),
        "score": f"{result.score:.6f}" if result.score is not None else None,
        "rank": result.rank,
        "status": result.status,
        "failure_reason": result.failure_reason,
        "completed_at": result.completed_at.isoformat() if result.completed_at else None,
    }


def create_optimization_run(
    session,
    *,
    market: str = "us",
    train_start_date: date,
    train_end_date: date,
    validation_start_date: date | None = None,
    validation_end_date: date | None = None,
    parameter_space: dict[str, object] | None = None,
    objective: str = DEFAULT_OPTIMIZATION_OBJECTIVE,
    max_parameter_sets: int = DEFAULT_MAX_PARAMETER_SETS,
    search_mode: str = "grid",
    random_seed: int | None = None,
    max_workers: int | str | None = None,
    require_data_ready: bool = True,
) -> OptimizationRun:
    resolved_market = normalize_market(market)
    if train_start_date > train_end_date:
        raise ValueError("train_start_date must be on or before train_end_date.")
    if (validation_start_date is None) != (validation_end_date is None):
        raise ValueError("validation_start_date and validation_end_date must be provided together.")
    if validation_start_date and validation_end_date and validation_start_date > validation_end_date:
        raise ValueError("validation_start_date must be on or before validation_end_date.")
    if objective not in SUPPORTED_OPTIMIZATION_OBJECTIVES:
        raise ValueError(
            "objective must be one of: "
            + ", ".join(sorted(SUPPORTED_OPTIMIZATION_OBJECTIVES))
            + "."
        )
    if search_mode not in SUPPORTED_SEARCH_MODES:
        raise ValueError(
            "search_mode must be one of: "
            + ", ".join(sorted(SUPPORTED_SEARCH_MODES))
            + "."
        )
    configured_max_workers = _coerce_configured_max_workers(max_workers)

    raw_parameter_space, _ = _split_parameter_space_metadata(parameter_space)
    stored_parameter_space = {
        **raw_parameter_space,
        "_optimization": _optimization_metadata(
            search_mode=search_mode,
            random_seed=random_seed,
            max_workers=configured_max_workers,
        ),
    }
    parameter_sets = build_parameter_sets(
        raw_parameter_space,
        max_parameter_sets=max_parameter_sets,
        search_mode=search_mode,
        random_seed=random_seed,
    )
    if not parameter_sets:
        raise ValueError("Parameter space produced no combinations.")

    run = OptimizationRun(
        market=resolved_market,
        train_start_date=train_start_date,
        train_end_date=train_end_date,
        validation_start_date=validation_start_date,
        validation_end_date=validation_end_date,
        objective=objective,
        parameter_space_json=dump_json(stored_parameter_space),
        parameter_sets_json=dump_json(parameter_sets),
        data_snapshot_json=dump_json(
            _data_snapshot(session, market=resolved_market, require_data_ready=require_data_ready)
        ),
        status="running",
        total_parameter_sets=len(parameter_sets),
        completed_parameter_sets=0,
        failed_parameter_sets=0,
        started_at=datetime.now(UTC),
    )
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def get_optimization_run(session, run_id: int) -> OptimizationRun | None:
    return session.get(OptimizationRun, run_id)


def get_optimization_result(session, result_id: int) -> OptimizationResult | None:
    return session.get(OptimizationResult, result_id)


def get_latest_optimization_run(session, *, market: str = "us") -> OptimizationRun | None:
    resolved_market = normalize_market(market)
    return session.execute(
        select(OptimizationRun)
        .where(OptimizationRun.market == resolved_market)
        .order_by(OptimizationRun.started_at.desc(), OptimizationRun.id.desc())
        .limit(1)
    ).scalar_one_or_none()


def list_optimization_results(
    session,
    *,
    run_id: int,
    limit: int = 100,
    offset: int = 0,
) -> list[OptimizationResult]:
    return list(
        session.execute(
            select(OptimizationResult)
            .where(OptimizationResult.optimization_run_id == run_id)
            .order_by(OptimizationResult.rank.asc().nulls_last(), OptimizationResult.id.asc())
            .limit(limit)
            .offset(offset)
        ).scalars()
    )


def count_optimization_results(session, *, run_id: int) -> int:
    return int(
        session.execute(
            select(func.count(OptimizationResult.id)).where(
                OptimizationResult.optimization_run_id == run_id
            )
        ).scalar_one()
    )


def delete_optimization_run(session, run_id: int) -> None:
    run = session.get(OptimizationRun, run_id)
    if run is None:
        raise LookupError("Optimization run not found.")
    if run.status in {"running", "cancel_requested"}:
        raise ValueError("Cannot delete a running optimization run; cancel it first.")
    result_ids = select(OptimizationResult.id).where(
        OptimizationResult.optimization_run_id == run_id
    )
    session.execute(
        OptimizationResultDetailCache.__table__.delete().where(
            OptimizationResultDetailCache.optimization_result_id.in_(result_ids)
        )
    )
    session.execute(
        OptimizationResult.__table__.delete().where(
            OptimizationResult.optimization_run_id == run_id
        )
    )
    session.delete(run)
    session.commit()


def delete_optimization_result(session, result_id: int) -> None:
    result = session.get(OptimizationResult, result_id)
    if result is None:
        raise LookupError("Optimization result not found.")
    run = session.get(OptimizationRun, result.optimization_run_id)
    session.execute(
        OptimizationResultDetailCache.__table__.delete().where(
            OptimizationResultDetailCache.optimization_result_id == result_id
        )
    )
    session.delete(result)
    if run is not None and run.best_result_id == result_id:
        run.best_result_id = None
    session.commit()


def _decimal_metric(metrics: dict[str, object], key: str, default: str = "0") -> Decimal:
    value = metrics.get(key)
    if value is None:
        return Decimal(default)
    return _coerce_decimal(value, field_name=key)


def _format_metric(value: Decimal | None) -> str | None:
    return f"{value.quantize(RATIO_PATTERN):.6f}" if value is not None else None


def _annualize_return(total_return: Decimal, *, start_date: object, end_date: object) -> Decimal | None:
    if start_date is None or end_date is None:
        return None
    try:
        start = date.fromisoformat(str(start_date))
        end = date.fromisoformat(str(end_date))
    except ValueError:
        return None
    elapsed_days = max((end - start).days, 0)
    if elapsed_days == 0:
        return None
    base = Decimal("1") + total_return
    if base <= Decimal("0"):
        return None
    annualized = math.pow(float(base), 365.25 / elapsed_days) - 1.0
    return Decimal(str(annualized)).quantize(RATIO_PATTERN)


def _portfolio_metrics_from_signal_days(result: dict[str, object]) -> dict[str, object]:
    signal_days = result.get("signal_days")
    if not isinstance(signal_days, list):
        signal_days = []
    parameters = result.get("parameters")
    position_weight_pct = Decimal("1")
    if isinstance(parameters, dict) and parameters.get("position_weight_pct") is not None:
        position_weight_pct = _coerce_decimal(
            parameters["position_weight_pct"],
            field_name="position_weight_pct",
        )

    equity = Decimal("1")
    peak = Decimal("1")
    max_drawdown = Decimal("0")
    usable_return_count = 0
    equity_curve: list[dict[str, object]] = []
    yearly_equity: dict[str, Decimal] = {}
    for row in signal_days:
        if not isinstance(row, dict) or row.get("average_return") is None:
            continue
        signal_date = str(row.get("signal_date") or "")
        if not signal_date:
            continue
        raw_day_return = _coerce_decimal(row["average_return"], field_name="signal_day.average_return")
        completed_trades = int(row.get("completed_trades", 0) or 0)
        exposure = (
            min(position_weight_pct * Decimal(completed_trades), Decimal("1"))
            if completed_trades
            else Decimal("1")
        )
        day_return = raw_day_return * exposure
        equity *= Decimal("1") + day_return
        year = signal_date[:4]
        yearly_equity[year] = yearly_equity.get(year, Decimal("1")) * (Decimal("1") + day_return)
        usable_return_count += 1
        if equity > peak:
            peak = equity
        drawdown = (equity / peak) - Decimal("1") if peak > Decimal("0") else Decimal("0")
        if drawdown < max_drawdown:
            max_drawdown = drawdown
        equity_curve.append(
            {
                "signal_date": signal_date,
                "equity": _format_metric(equity),
                "drawdown": _format_metric(drawdown),
                "day_return": _format_metric(day_return),
            }
        )

    total_return = equity - Decimal("1") if usable_return_count else None
    annualized_return = (
        _annualize_return(
            total_return,
            start_date=result.get("start_date"),
            end_date=result.get("end_date"),
        )
        if total_return is not None
        else None
    )
    return_drawdown_ratio = None
    if annualized_return is not None and max_drawdown < Decimal("0"):
        return_drawdown_ratio = annualized_return / abs(max_drawdown)

    return {
        "total_return": _format_metric(total_return),
        "annualized_return": _format_metric(annualized_return),
        "average_annualized_return": _format_metric(annualized_return),
        "max_drawdown": _format_metric(max_drawdown if usable_return_count else None),
        "return_drawdown_ratio": _format_metric(return_drawdown_ratio),
        "signal_day_return_count": usable_return_count,
        "equity_curve": equity_curve,
        "yearly_returns": {
            year: _format_metric(year_equity - Decimal("1"))
            for year, year_equity in sorted(yearly_equity.items())
        },
    }


def _extract_metrics(result: dict[str, object]) -> dict[str, object]:
    completed = int(result.get("completed_trades", 0) or 0)
    average_return = _decimal_metric(result, "average_trade_return")
    win_rate = _decimal_metric(result, "win_rate")
    worst_trade = _decimal_metric(result, "worst_trade_return")
    sample_penalty = Decimal("0")
    if completed < 10:
        sample_penalty = Decimal("0.20")
    elif completed < 50:
        sample_penalty = Decimal("0.08")
    metrics = {
        "signal_dates_evaluated": int(result.get("signal_dates_evaluated", 0) or 0),
        "total_candidates_evaluated": int(result.get("total_candidates_evaluated", 0) or 0),
        "qualifying_observations": int(result.get("qualifying_observations", 0) or 0),
        "selected_trades": int(result.get("selected_trades", 0) or 0),
        "completed_trades": completed,
        "average_trade_return": f"{average_return:.6f}",
        "median_trade_return": result.get("median_trade_return"),
        "win_rate": f"{win_rate:.6f}",
        "best_trade_return": result.get("best_trade_return"),
        "worst_trade_return": f"{worst_trade:.6f}",
        "stop_loss_trades": int(result.get("stop_loss_trades", 0) or 0),
        "stop_loss_trigger_ratio": result.get("stop_loss_trigger_ratio") or "0.000000",
        "take_profit_trades": int(result.get("take_profit_trades", 0) or 0),
        "take_profit_trigger_ratio": result.get("take_profit_trigger_ratio") or "0.000000",
        "rps_exit_trades": int(result.get("rps_exit_trades", 0) or 0),
        "rps_exit_trigger_ratio": result.get("rps_exit_trigger_ratio") or "0.000000",
        "max_consecutive_losses": int(result.get("max_consecutive_losses", 0) or 0),
        "sample_penalty": f"{sample_penalty:.6f}",
    }
    metrics.update(_portfolio_metrics_from_signal_days(result))
    return metrics


def _composite_score_metrics(metrics: dict[str, object]) -> Decimal:
    completed = Decimal(int(metrics.get("completed_trades", 0) or 0))
    average_return = _decimal_metric(metrics, "average_trade_return")
    win_rate = _decimal_metric(metrics, "win_rate")
    worst_trade = _decimal_metric(metrics, "worst_trade_return")
    sample_penalty = _decimal_metric(metrics, "sample_penalty")
    annualized_return = _decimal_metric(metrics, "annualized_return")
    max_drawdown = _decimal_metric(metrics, "max_drawdown")
    return_drawdown_ratio = _decimal_metric(metrics, "return_drawdown_ratio")
    stop_loss_trigger_ratio = _decimal_metric(metrics, "stop_loss_trigger_ratio")
    max_consecutive_losses = Decimal(int(metrics.get("max_consecutive_losses", 0) or 0))
    sample_bonus = min(completed / Decimal("200"), Decimal("0.15"))
    risk_adjusted_bonus = min(return_drawdown_ratio * Decimal("0.02"), Decimal("0.10"))
    loss_streak_penalty = min(max_consecutive_losses / Decimal("100"), Decimal("0.10"))
    score = (
        average_return
        + (annualized_return * Decimal("0.15"))
        + (win_rate * Decimal("0.10"))
        + risk_adjusted_bonus
        + sample_bonus
        - abs(worst_trade)
        - (abs(max_drawdown) * Decimal("0.15"))
        - (stop_loss_trigger_ratio * Decimal("0.05"))
        - loss_streak_penalty
        - sample_penalty
    )
    return score.quantize(RATIO_PATTERN)


def _score_metrics(
    metrics: dict[str, object],
    *,
    objective: str = DEFAULT_OPTIMIZATION_OBJECTIVE,
) -> Decimal:
    if objective == "score":
        return _composite_score_metrics(metrics)
    if objective == "average_annualized_return":
        return _decimal_metric(
            metrics,
            "average_annualized_return",
            default=str(metrics.get("annualized_return") or "0"),
        ).quantize(RATIO_PATTERN)
    if objective == "robust_annualized_return":
        return _decimal_metric(metrics, "annualized_return").quantize(RATIO_PATTERN)
    if objective == "annualized_return":
        return _decimal_metric(metrics, "annualized_return").quantize(RATIO_PATTERN)
    if objective == "max_drawdown":
        return _decimal_metric(metrics, "max_drawdown").quantize(RATIO_PATTERN)
    if objective == "return_drawdown_ratio":
        return _decimal_metric(metrics, "return_drawdown_ratio").quantize(RATIO_PATTERN)
    if objective == "win_rate":
        return _decimal_metric(metrics, "win_rate").quantize(RATIO_PATTERN)
    if objective == "total_return":
        return _decimal_metric(metrics, "total_return").quantize(RATIO_PATTERN)
    raise ValueError(f"Unsupported optimization objective: {objective}.")


def _score_metric_pair(
    train_metrics: dict[str, object],
    validation_metrics: dict[str, object] | None,
    *,
    objective: str = DEFAULT_OPTIMIZATION_OBJECTIVE,
) -> Decimal:
    if objective != "robust_annualized_return":
        return _score_metrics(validation_metrics or train_metrics, objective=objective)
    train_annualized = _decimal_metric(train_metrics, "annualized_return")
    validation_annualized = (
        _decimal_metric(validation_metrics, "annualized_return")
        if validation_metrics is not None
        else train_annualized
    )
    train_drawdown = abs(_decimal_metric(train_metrics, "max_drawdown"))
    validation_drawdown = (
        abs(_decimal_metric(validation_metrics, "max_drawdown"))
        if validation_metrics is not None
        else train_drawdown
    )
    train_completed = Decimal(int(train_metrics.get("completed_trades", 0) or 0))
    validation_completed = (
        Decimal(int(validation_metrics.get("completed_trades", 0) or 0))
        if validation_metrics is not None
        else train_completed
    )
    consistency_floor = min(train_annualized, validation_annualized)
    average_return = (train_annualized + validation_annualized) / Decimal("2")
    positive_gap = abs(train_annualized - validation_annualized)
    drawdown_penalty = max(train_drawdown, validation_drawdown) * Decimal("0.15")
    gap_penalty = positive_gap * Decimal("0.35")
    sample_penalty = Decimal("0")
    if min(train_completed, validation_completed) < Decimal("50"):
        sample_penalty = Decimal("0.08")
    elif min(train_completed, validation_completed) < Decimal("100"):
        sample_penalty = Decimal("0.03")
    score = (
        consistency_floor * Decimal("0.70")
        + average_return * Decimal("0.30")
        - gap_penalty
        - drawdown_penalty
        - sample_penalty
    )
    return score.quantize(RATIO_PATTERN)


def _attach_average_annualized_return(
    train_metrics: dict[str, object],
    validation_metrics: dict[str, object] | None,
) -> None:
    values = [
        _decimal_metric(metrics, "annualized_return")
        for metrics in (train_metrics, validation_metrics)
        if isinstance(metrics, dict) and metrics.get("annualized_return") is not None
    ]
    if not values:
        return
    average = sum(values, Decimal("0")) / Decimal(len(values))
    target = validation_metrics if validation_metrics is not None else train_metrics
    target["average_annualized_return"] = _format_metric(average)


def _run_backtest_once(
    session,
    *,
    start_date: date,
    end_date: date,
    market: str,
    parameters: dict[str, object],
    max_trades_returned: int = 0,
    screen_cache: dict[str, dict[str, object]] | None = None,
    screen_candidate_cache: dict[tuple[object, ...], dict[str, object]] | None = None,
    fundamental_growth_cache: dict[tuple[object, ...], object] | None = None,
    cup_event_cache: dict[tuple[object, ...], dict[int, list[object]] | None] | None = None,
    trade_cache: dict[tuple[object, ...], object] | None = None,
    trade_dates_cache: dict[tuple[object, ...], list[date]] | None = None,
    future_rows_cache: dict[tuple[object, ...], object] | None = None,
    future_indicator_cache: dict[tuple[object, ...], object] | None = None,
    should_cancel=None,
) -> dict[str, object]:
    result = run_cup_handle_rps_backtest(
        session,
        start_date=start_date,
        end_date=end_date,
        use_rps=bool(parameters.get("use_rps", True)),
        rps_threshold=int(parameters["rps_threshold"]),
        selected_rps_windows=list(parameters["selected_rps_windows"]),  # type: ignore[arg-type]
        min_rps_windows_passing=int(parameters["min_rps_windows_passing"]),
        use_cup_handle=bool(parameters.get("use_cup_handle", True)),
        cup_handle_params=_cup_handle_params_from_payload(parameters["cup_handle_params"]),  # type: ignore[arg-type]
        fundamental_growth_params=_fundamental_params_from_payload(parameters["fundamental_growth_params"]),  # type: ignore[arg-type]
        market=market,
        holding_days=_coerce_optional_int(
            parameters.get("holding_days", 130),
            field_name="holding_days",
        ),
        stop_loss_pct=_coerce_decimal(parameters["stop_loss_pct"], field_name="stop_loss_pct"),
        take_profit_pct=_coerce_optional_decimal(
            parameters.get("take_profit_pct"),
            field_name="take_profit_pct",
        ),
        rps_exit_threshold=_coerce_optional_int(
            parameters.get("rps_exit_threshold"),
            field_name="rps_exit_threshold",
        ),
        portfolio_cap=int(parameters.get("portfolio_cap", 10)),
        position_weight_pct=_coerce_decimal(
            parameters.get("position_weight_pct", "0.10"),
            field_name="position_weight_pct",
        ),
        allow_reentry_while_open=bool(parameters.get("allow_reentry_while_open", False)),
        entry_delay_days=int(parameters.get("entry_delay_days", 0)),
        entry_deferral_window_days=int(parameters.get("entry_deferral_window_days", 5)),
        max_trades_returned=max_trades_returned,
        screen_cache=screen_cache,
        screen_candidate_cache=screen_candidate_cache,
        fundamental_growth_cache=fundamental_growth_cache,
        cup_event_cache=cup_event_cache,
        trade_cache=trade_cache,
        trade_dates_cache=trade_dates_cache,
        future_rows_cache=future_rows_cache,  # type: ignore[arg-type]
        future_indicator_cache=future_indicator_cache,  # type: ignore[arg-type]
        should_cancel=should_cancel,
    )
    return result.to_dict()


def _evaluate_parameter_set(
    session,
    *,
    start_date: date,
    end_date: date,
    validation_start_date: date | None,
    validation_end_date: date | None,
    market: str,
    objective: str,
    parameters: dict[str, object],
    screen_cache: dict[str, dict[str, object]] | None = None,
    screen_candidate_cache: dict[tuple[object, ...], dict[str, object]] | None = None,
    fundamental_growth_cache: dict[tuple[object, ...], object] | None = None,
    cup_event_cache: dict[tuple[object, ...], dict[int, list[object]] | None] | None = None,
    trade_cache: dict[tuple[object, ...], object] | None = None,
    trade_dates_cache: dict[tuple[object, ...], list[date]] | None = None,
    future_rows_cache: dict[tuple[object, ...], object] | None = None,
    future_indicator_cache: dict[tuple[object, ...], object] | None = None,
    should_cancel=None,
) -> dict[str, object]:
    try:
        train_result = _run_backtest_once(
            session,
            start_date=start_date,
            end_date=end_date,
            market=market,
            parameters=parameters,
            max_trades_returned=DEFAULT_OPTIMIZATION_DETAIL_CACHE_TRADES,
            screen_cache=screen_cache,
            screen_candidate_cache=screen_candidate_cache,
            fundamental_growth_cache=fundamental_growth_cache,
            cup_event_cache=cup_event_cache,
            trade_cache=trade_cache,
            trade_dates_cache=trade_dates_cache,
            future_rows_cache=future_rows_cache,
            future_indicator_cache=future_indicator_cache,
            should_cancel=should_cancel,
        )
        train_metrics = _extract_metrics(train_result)
        validation_result = None
        validation_metrics = None
        score_source = train_metrics
        if validation_start_date is not None and validation_end_date is not None:
            validation_result = _run_backtest_once(
                session,
                start_date=validation_start_date,
                end_date=validation_end_date,
                market=market,
                parameters=parameters,
                max_trades_returned=DEFAULT_OPTIMIZATION_DETAIL_CACHE_TRADES,
                screen_cache=screen_cache,
                screen_candidate_cache=screen_candidate_cache,
                fundamental_growth_cache=fundamental_growth_cache,
                cup_event_cache=cup_event_cache,
                trade_cache=trade_cache,
                trade_dates_cache=trade_dates_cache,
                future_rows_cache=future_rows_cache,
                future_indicator_cache=future_indicator_cache,
                should_cancel=should_cancel,
            )
            validation_metrics = _extract_metrics(validation_result)
            score_source = validation_metrics
        _attach_average_annualized_return(train_metrics, validation_metrics)
        score = _score_metric_pair(
            train_metrics,
            validation_metrics,
            objective=objective,
        )
        return {
            "parameters": parameters,
            "train_metrics": train_metrics,
            "validation_metrics": validation_metrics,
            "train_result": train_result,
            "validation_result": validation_result,
            "score": score,
            "status": "completed",
            "failure_reason": None,
        }
    except BacktestCancelledError:
        raise
    except Exception as exc:
        return {
            "parameters": parameters,
            "train_metrics": None,
            "validation_metrics": None,
            "score": None,
            "status": "failed",
            "failure_reason": f"{type(exc).__name__}: {exc}",
        }


def _register_worker_domain_models() -> None:
    import stockanalyse_api.domain.backtests.models  # noqa: F401
    import stockanalyse_api.domain.fundamentals.models  # noqa: F401
    import stockanalyse_api.domain.indicators.models  # noqa: F401
    import stockanalyse_api.domain.instruments.models  # noqa: F401
    import stockanalyse_api.domain.market_data.models  # noqa: F401
    import stockanalyse_api.domain.screens.models  # noqa: F401


def _evaluate_parameter_set_group_worker(payload: dict[str, object]) -> list[dict[str, object]]:
    _register_worker_domain_models()
    parameter_sets = payload["parameter_sets"]
    if not isinstance(parameter_sets, list):
        raise ValueError("Worker payload parameter_sets must be a list.")
    results: list[dict[str, object]] = []
    with SessionLocal() as session:
        for parameters in parameter_sets:
            if not isinstance(parameters, dict):
                raise ValueError("Worker payload parameters must be dictionaries.")
            results.append(
                _evaluate_parameter_set(
                    session,
                    start_date=payload["start_date"],  # type: ignore[arg-type]
                    end_date=payload["end_date"],  # type: ignore[arg-type]
                    validation_start_date=payload.get("validation_start_date"),  # type: ignore[arg-type]
                    validation_end_date=payload.get("validation_end_date"),  # type: ignore[arg-type]
                    market=str(payload["market"]),
                    objective=str(payload["objective"]),
                    parameters=parameters,
                    screen_cache=_WORKER_SCREEN_CACHE,
                    screen_candidate_cache=_WORKER_SCREEN_CANDIDATE_CACHE,
                    fundamental_growth_cache=_WORKER_FUNDAMENTAL_GROWTH_CACHE,
                    cup_event_cache=_WORKER_CUP_EVENT_CACHE,
                    trade_cache=_WORKER_TRADE_CACHE,
                    trade_dates_cache=_WORKER_TRADE_DATES_CACHE,
                    future_rows_cache=_WORKER_FUTURE_ROWS_CACHE,
                    future_indicator_cache=_WORKER_FUTURE_INDICATOR_CACHE,
                    should_cancel=None,
                )
            )
    return results


def _parameter_screen_signature(parameters: dict[str, object]) -> str:
    return dump_json(
        {
            "rps_threshold": parameters.get("rps_threshold"),
            "use_rps": parameters.get("use_rps"),
            "selected_rps_windows": parameters.get("selected_rps_windows"),
            "min_rps_windows_passing": parameters.get("min_rps_windows_passing"),
            "use_cup_handle": parameters.get("use_cup_handle"),
        }
    )


def _parallel_parameter_groups(
    parameter_sets: list[dict[str, object]],
    *,
    group_size: int = DEFAULT_OPTIMIZATION_PARALLEL_GROUP_SIZE,
    target_group_count: int | None = None,
) -> list[list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for parameters in parameter_sets:
        grouped.setdefault(_parameter_screen_signature(parameters), []).append(parameters)
    groups: list[list[dict[str, object]]] = []
    chunk_size = max(int(group_size), 1)
    if target_group_count is not None and len(grouped) < target_group_count:
        target_chunk_size = max(1, math.ceil(len(parameter_sets) / max(target_group_count, 1)))
        chunk_size = min(chunk_size, target_chunk_size)
    for group in grouped.values():
        for index in range(0, len(group), chunk_size):
            groups.append(group[index : index + chunk_size])
    return groups


def _session_uses_in_memory_database(session) -> bool:
    bind = session.get_bind()
    url = getattr(bind, "url", None)
    if url is None:
        return False
    return url.get_backend_name() == "sqlite" and (url.database in {None, "", ":memory:"})


def _auto_optimization_worker_count() -> int:
    configured = os.environ.get("STOCKANALYSE_OPTIMIZATION_MAX_WORKERS")
    if configured:
        coerced = _coerce_configured_max_workers(configured)
        if isinstance(coerced, int):
            return coerced
    cpu_count = os.cpu_count() or 1
    return max(1, min(MAX_AUTO_OPTIMIZATION_WORKERS, max(cpu_count - 2, 1)))


def _resolve_optimization_max_workers(
    session,
    *,
    configured_max_workers: int | str | None,
    total_parameter_sets: int,
) -> int:
    if total_parameter_sets <= 1 or _session_uses_in_memory_database(session):
        return 1
    if configured_max_workers in {None, "auto"}:
        workers = _auto_optimization_worker_count()
    else:
        workers = int(configured_max_workers)
    return max(1, min(workers, total_parameter_sets))


def _evaluation_failure(parameters: dict[str, object], exc: BaseException) -> dict[str, object]:
    return {
        "parameters": parameters,
        "train_metrics": None,
        "validation_metrics": None,
        "score": None,
        "status": "failed",
        "failure_reason": f"{type(exc).__name__}: {exc}",
    }


def _terminate_process_pool(
    executor: ProcessPoolExecutor,
    pending: dict[Future[list[dict[str, object]]], list[dict[str, object]]],
) -> None:
    for future in pending:
        future.cancel()
    processes = getattr(executor, "_processes", None)
    if isinstance(processes, dict):
        for process in list(processes.values()):
            if getattr(process, "is_alive", lambda: False)():
                process.terminate()
    shutdown = getattr(executor, "shutdown", None)
    if shutdown is not None:
        shutdown(wait=False, cancel_futures=True)


def _persist_evaluation_result(
    session,
    *,
    run: OptimizationRun,
    evaluation: dict[str, object],
) -> None:
    parameters = evaluation["parameters"]
    if not isinstance(parameters, dict):
        raise ValueError("Optimization evaluation parameters are invalid.")
    status = str(evaluation["status"])
    persisted = _persist_result(
        session,
        run=run,
        parameters=parameters,
        train_metrics=evaluation.get("train_metrics"),  # type: ignore[arg-type]
        validation_metrics=evaluation.get("validation_metrics"),  # type: ignore[arg-type]
        score=evaluation.get("score"),  # type: ignore[arg-type]
        status=status,
        failure_reason=(
            str(evaluation["failure_reason"])
            if evaluation.get("failure_reason") is not None
            else None
        ),
    )
    if status == "completed":
        train_result = evaluation.get("train_result")
        validation_result = evaluation.get("validation_result")
        if isinstance(train_result, dict):
            _store_detail_cache(
                session,
                result_id=persisted.id,
                max_trades_returned=DEFAULT_OPTIMIZATION_DETAIL_CACHE_TRADES,
                train_result=train_result,
                validation_result=(
                    validation_result if isinstance(validation_result, dict) else None
                ),
            )
        run.completed_parameter_sets += 1
    else:
        run.failed_parameter_sets += 1


def _entry_reason(parameters: dict[str, object]) -> str:
    parts = [
        (
            f"RPS 达到 {parameters.get('rps_threshold', '—')}"
            if parameters.get("use_rps", True)
            else "未启用 RPS 过滤"
        ),
        "杯柄突破" if parameters.get("use_cup_handle", True) else "未启用杯柄过滤",
    ]
    fundamentals = parameters.get("fundamental_growth_params")
    if isinstance(fundamentals, dict) and fundamentals.get("enabled"):
        parts.append("财务增长通过")
    return "；".join(parts)


def _exit_reason_label(reason: object) -> str:
    labels = {
        "stop_loss": "固定止损触发",
        "rps_exit": "RPS 跌破退出阈值",
        "take_profit": "固定止盈触发",
        "holding_period_elapsed": "持有期结束",
        "data_end_mark": "数据末尾按收盘价估值",
    }
    return labels.get(str(reason), str(reason or "未知"))


def _annotate_trades(
    trades: list[dict[str, object]],
    *,
    period: str,
    parameters: dict[str, object],
) -> list[dict[str, object]]:
    entry_reason = _entry_reason(parameters)
    return [
        {
            **trade,
            "period": period,
            "entry_reason": entry_reason,
            "exit_reason_label": _exit_reason_label(trade.get("exit_reason")),
            "rps_exit_threshold": parameters.get("rps_exit_threshold"),
        }
        for trade in trades
    ]


def _result_from_metrics(
    metrics: dict[str, object] | None,
    *,
    start_date: date,
    end_date: date,
    parameters: dict[str, object],
) -> dict[str, object]:
    result = dict(metrics or {})
    result.setdefault("start_date", start_date.isoformat())
    result.setdefault("end_date", end_date.isoformat())
    result.setdefault("parameters", parameters)
    result["trades"] = []
    return result


def _load_detail_cache(
    session,
    *,
    result_id: int,
    max_trades_returned: int,
) -> tuple[dict[str, object], dict[str, object] | None] | None:
    cache = session.execute(
        select(OptimizationResultDetailCache).where(
            OptimizationResultDetailCache.optimization_result_id == result_id
        )
    ).scalar_one_or_none()
    if cache is None or cache.max_trades_returned < max_trades_returned:
        return None
    return (
        load_json(cache.train_result_json, default={}),
        load_json(cache.validation_result_json, default=None),
    )


def _store_detail_cache(
    session,
    *,
    result_id: int,
    max_trades_returned: int,
    train_result: dict[str, object],
    validation_result: dict[str, object] | None,
) -> None:
    cache = session.execute(
        select(OptimizationResultDetailCache).where(
            OptimizationResultDetailCache.optimization_result_id == result_id
        )
    ).scalar_one_or_none()
    if cache is None:
        cache = OptimizationResultDetailCache(
            optimization_result_id=result_id,
            max_trades_returned=max_trades_returned,
            train_result_json=dump_json(train_result),
            validation_result_json=(
                dump_json(validation_result) if validation_result is not None else None
            ),
            generated_at=datetime.now(UTC),
        )
        session.add(cache)
        return
    if cache.max_trades_returned <= max_trades_returned:
        cache.max_trades_returned = max_trades_returned
        cache.train_result_json = dump_json(train_result)
        cache.validation_result_json = (
            dump_json(validation_result) if validation_result is not None else None
        )
        cache.generated_at = datetime.now(UTC)


def build_optimization_result_detail(
    session,
    *,
    result_id: int,
    max_trades_returned: int = 1000,
) -> dict[str, object]:
    result = get_optimization_result(session, result_id)
    if result is None:
        raise LookupError("Optimization result not found.")
    run = get_optimization_run(session, result.optimization_run_id)
    if run is None:
        raise LookupError("Optimization run not found.")

    parameters = load_json(result.parameters_json, default={})
    if not isinstance(parameters, dict):
        raise ValueError("Optimization result parameters are invalid.")

    cached = _load_detail_cache(
        session,
        result_id=result.id,
        max_trades_returned=max_trades_returned,
    )
    if cached is not None:
        train_result, validation_result = cached
    else:
        train_metrics = load_json(result.train_metrics_json, default=None)
        validation_metrics = load_json(result.validation_metrics_json, default=None)
        train_completed = (
            int(train_metrics.get("completed_trades", 0) or 0)
            if isinstance(train_metrics, dict)
            else 0
        )
        validation_completed = (
            int(validation_metrics.get("completed_trades", 0) or 0)
            if isinstance(validation_metrics, dict)
            else 0
        )
        if train_completed == 0 and validation_completed == 0:
            train_result = _result_from_metrics(
                train_metrics if isinstance(train_metrics, dict) else None,
                start_date=run.train_start_date,
                end_date=run.train_end_date,
                parameters=parameters,
            )
            validation_result = (
                _result_from_metrics(
                    validation_metrics if isinstance(validation_metrics, dict) else None,
                    start_date=run.validation_start_date,
                    end_date=run.validation_end_date,
                    parameters=parameters,
                )
                if run.validation_start_date is not None and run.validation_end_date is not None
                else None
            )
        else:
            screen_cache: dict[str, dict[str, object]] = {}
            screen_candidate_cache: dict[tuple[object, ...], dict[str, object]] = {}
            fundamental_growth_cache: dict[tuple[object, ...], object] = {}
            cup_event_cache: dict[tuple[object, ...], dict[int, list[object]] | None] = {}
            trade_cache: dict[tuple[object, ...], object] = {}
            trade_dates_cache: dict[tuple[object, ...], list[date]] = {}
            future_rows_cache: dict[tuple[object, ...], object] = {}
            future_indicator_cache: dict[tuple[object, ...], object] = {}
            train_result = _run_backtest_once(
                session,
                start_date=run.train_start_date,
                end_date=run.train_end_date,
                market=run.market,
                parameters=parameters,
                max_trades_returned=max_trades_returned,
                screen_cache=screen_cache,
                screen_candidate_cache=screen_candidate_cache,
                fundamental_growth_cache=fundamental_growth_cache,
                cup_event_cache=cup_event_cache,
                trade_cache=trade_cache,
                trade_dates_cache=trade_dates_cache,
                future_rows_cache=future_rows_cache,
                future_indicator_cache=future_indicator_cache,
            )
            validation_result = None
            if run.validation_start_date is not None and run.validation_end_date is not None:
                validation_result = _run_backtest_once(
                    session,
                    start_date=run.validation_start_date,
                    end_date=run.validation_end_date,
                    market=run.market,
                    parameters=parameters,
                    max_trades_returned=max_trades_returned,
                    screen_cache=screen_cache,
                    screen_candidate_cache=screen_candidate_cache,
                    fundamental_growth_cache=fundamental_growth_cache,
                    cup_event_cache=cup_event_cache,
                    trade_cache=trade_cache,
                    trade_dates_cache=trade_dates_cache,
                    future_rows_cache=future_rows_cache,
                    future_indicator_cache=future_indicator_cache,
                )
        _store_detail_cache(
            session,
            result_id=result.id,
            max_trades_returned=max_trades_returned,
            train_result=train_result,
            validation_result=validation_result,
        )
        session.commit()

    train_trades = _annotate_trades(
        train_result.get("trades", []),  # type: ignore[arg-type]
        period="train",
        parameters=parameters,
    )
    validation_trades = (
        _annotate_trades(
            validation_result.get("trades", []),  # type: ignore[union-attr,arg-type]
            period="validation",
            parameters=parameters,
        )
        if validation_result is not None
        else []
    )

    return {
        "optimization_run": serialize_optimization_run(run),
        "optimization_result": serialize_optimization_result(result),
        "parameters": parameters,
        "train": {
            "summary": {key: value for key, value in train_result.items() if key != "trades"},
            "trades": train_trades,
        },
        "validation": (
            {
                "summary": {
                    key: value for key, value in validation_result.items() if key != "trades"
                },
                "trades": validation_trades,
            }
            if validation_result is not None
            else None
        ),
    }


def _persist_result(
    session,
    *,
    run: OptimizationRun,
    parameters: dict[str, object],
    train_metrics: dict[str, object] | None,
    validation_metrics: dict[str, object] | None,
    score: Decimal | None,
    status: str,
    failure_reason: str | None = None,
) -> OptimizationResult:
    result = OptimizationResult(
        optimization_run_id=run.id,
        parameter_hash=stable_parameter_hash(parameters),
        parameters_json=dump_json(parameters),
        train_metrics_json=dump_json(train_metrics) if train_metrics is not None else None,
        validation_metrics_json=(
            dump_json(validation_metrics) if validation_metrics is not None else None
        ),
        score=score,
        status=status,
        failure_reason=failure_reason,
        completed_at=datetime.now(UTC),
    )
    session.add(result)
    session.flush()
    return result


def _rank_results(session, run: OptimizationRun) -> None:
    completed_results = list(
        session.execute(
            select(OptimizationResult)
            .where(
                OptimizationResult.optimization_run_id == run.id,
                OptimizationResult.status == "completed",
                OptimizationResult.score.is_not(None),
            )
        ).scalars()
    )

    train_ranks: dict[int, int] = {}
    validation_ranks: dict[int, int] = {}
    metric_payloads: dict[int, tuple[dict[str, object] | None, dict[str, object] | None]] = {}
    train_scores: list[tuple[Decimal, str, OptimizationResult]] = []
    validation_scores: list[tuple[Decimal, str, OptimizationResult]] = []
    for result in completed_results:
        train_metrics = load_json(result.train_metrics_json, default=None)
        validation_metrics = load_json(result.validation_metrics_json, default=None)
        if isinstance(train_metrics, dict):
            train_score = _score_metrics(train_metrics, objective=run.objective)
            train_metrics["train_objective_score"] = _format_metric(train_score)
            train_scores.append((train_score, result.parameter_hash, result))
        else:
            train_metrics = None
        if isinstance(validation_metrics, dict):
            validation_score = _score_metrics(validation_metrics, objective=run.objective)
            validation_metrics["validation_objective_score"] = _format_metric(validation_score)
            validation_scores.append((validation_score, result.parameter_hash, result))
        else:
            validation_metrics = None
        metric_payloads[result.id] = (train_metrics, validation_metrics)

    train_scores.sort(key=lambda item: (-item[0], item[1]))
    for rank, (_, _, result) in enumerate(train_scores, start=1):
        train_ranks[result.id] = rank

    validation_scores.sort(key=lambda item: (-item[0], item[1]))
    for rank, (_, _, result) in enumerate(validation_scores, start=1):
        validation_ranks[result.id] = rank

    for result in completed_results:
        train_metrics, validation_metrics = metric_payloads[result.id]
        if train_metrics is not None:
            train_metrics["train_rank"] = train_ranks.get(result.id)
            result.train_metrics_json = dump_json(train_metrics)
        if validation_metrics is not None:
            validation_rank = validation_ranks.get(result.id)
            validation_metrics["validation_rank"] = validation_rank
            if result.id in train_ranks and validation_rank is not None:
                validation_metrics["train_validation_rank_gap"] = validation_rank - train_ranks[result.id]
            result.validation_metrics_json = dump_json(validation_metrics)

    completed_results.sort(
        key=lambda result: (
            -(result.score or Decimal("-999999")),
            result.parameter_hash,
        )
    )
    for index, result in enumerate(completed_results, start=1):
        result.rank = index
    run.best_result_id = completed_results[0].id if completed_results else None


def execute_optimization_run(session, run_id: int) -> OptimizationRun:
    run = session.get(OptimizationRun, run_id)
    if run is None:
        raise LookupError("Optimization run not found.")
    if run.status not in {"running", "cancel_requested"}:
        return run

    parameter_sets = load_json(run.parameter_sets_json, default=[])
    if not isinstance(parameter_sets, list):
        parameter_sets = []
    parameter_sets = [parameters for parameters in parameter_sets if isinstance(parameters, dict)]
    _, metadata = _split_parameter_space_metadata(load_json(run.parameter_space_json, default={}))
    configured_max_workers = _configured_max_workers_from_metadata(metadata)
    max_workers = _resolve_optimization_max_workers(
        session,
        configured_max_workers=configured_max_workers,
        total_parameter_sets=len(parameter_sets),
    )
    screen_cache: dict[str, dict[str, object]] = {}
    screen_candidate_cache: dict[tuple[object, ...], dict[str, object]] = {}
    fundamental_growth_cache: dict[tuple[object, ...], object] = {}
    cup_event_cache: dict[tuple[object, ...], dict[int, list[object]] | None] = {}
    trade_cache: dict[tuple[object, ...], object] = {}
    trade_dates_cache: dict[tuple[object, ...], list[date]] = {}
    future_rows_cache: dict[tuple[object, ...], object] = {}
    future_indicator_cache: dict[tuple[object, ...], object] = {}

    cancel_state = {"checked_at": 0.0, "cached": False}

    def should_cancel() -> bool:
        now = time.monotonic()
        if now - cancel_state["checked_at"] < 0.5:
            return cancel_state["cached"]
        cancel_state["checked_at"] = now
        try:
            session.refresh(run)
        except Exception:
            return cancel_state["cached"]
        cancel_state["cached"] = run.status == "cancel_requested"
        return cancel_state["cached"]

    def finalize_cancel() -> OptimizationRun:
        _rank_results(session, run)
        run.status = "cancelled"
        run.completed_at = datetime.now(UTC)
        session.commit()
        session.refresh(run)
        return run

    def execute_serial() -> OptimizationRun | None:
        for parameters in parameter_sets:
            session.refresh(run)
            if run.status == "cancel_requested":
                return finalize_cancel()
            try:
                evaluation = _evaluate_parameter_set(
                    session,
                    start_date=run.train_start_date,
                    end_date=run.train_end_date,
                    validation_start_date=run.validation_start_date,
                    validation_end_date=run.validation_end_date,
                    market=run.market,
                    objective=run.objective,
                    parameters=parameters,
                    screen_cache=screen_cache,
                    screen_candidate_cache=screen_candidate_cache,
                    fundamental_growth_cache=fundamental_growth_cache,
                    cup_event_cache=cup_event_cache,
                    trade_cache=trade_cache,
                    trade_dates_cache=trade_dates_cache,
                    future_rows_cache=future_rows_cache,
                    future_indicator_cache=future_indicator_cache,
                    should_cancel=should_cancel,
                )
                _persist_evaluation_result(session, run=run, evaluation=evaluation)
            except BacktestCancelledError:
                session.rollback()
                return finalize_cancel()
            session.commit()
        return None

    def worker_payload(group: list[dict[str, object]]) -> dict[str, object]:
        return {
            "start_date": run.train_start_date,
            "end_date": run.train_end_date,
            "validation_start_date": run.validation_start_date,
            "validation_end_date": run.validation_end_date,
            "market": run.market,
            "objective": run.objective,
            "parameter_sets": group,
        }

    def execute_parallel() -> OptimizationRun | None:
        groups = _parallel_parameter_groups(
            parameter_sets,
            target_group_count=max_workers,
        )
        if not groups:
            return None
        pending: dict[Future[list[dict[str, object]]], list[dict[str, object]]] = {}
        group_index = 0

        def submit_next(executor: ProcessPoolExecutor) -> None:
            nonlocal group_index
            if group_index >= len(groups):
                return
            group = groups[group_index]
            group_index += 1
            pending[executor.submit(_evaluate_parameter_set_group_worker, worker_payload(group))] = group

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for _ in range(min(max_workers, len(groups))):
                submit_next(executor)
            while pending:
                done, _ = wait(pending.keys(), timeout=0.5, return_when=FIRST_COMPLETED)
                if not done:
                    session.refresh(run)
                    if run.status == "cancel_requested":
                        _terminate_process_pool(executor, pending)
                        return finalize_cancel()
                    continue

                for future in done:
                    group = pending.pop(future)
                    try:
                        evaluations = future.result()
                    except Exception as exc:
                        evaluations = [_evaluation_failure(parameters, exc) for parameters in group]
                    for evaluation in evaluations:
                        _persist_evaluation_result(session, run=run, evaluation=evaluation)
                    session.commit()
                    session.refresh(run)
                    if run.status == "cancel_requested":
                        _terminate_process_pool(executor, pending)
                        return finalize_cancel()
                    submit_next(executor)
            _terminate_process_pool(executor, pending)
        return None

    try:
        cancelled_run = execute_serial() if max_workers == 1 else execute_parallel()
        if cancelled_run is not None:
            return cancelled_run

        _rank_results(session, run)
        run.status = "completed" if run.completed_parameter_sets else "failed"
        run.completed_at = datetime.now(UTC)
        if run.status == "failed":
            run.error_message = "All parameter sets failed."
        session.commit()
        session.refresh(run)
        return run
    except BacktestCancelledError:
        session.rollback()
        return finalize_cancel()
    except Exception as exc:
        run.status = "failed"
        run.error_message = f"{type(exc).__name__}: {exc}"
        run.completed_at = datetime.now(UTC)
        session.commit()
        session.refresh(run)
        return run


def dispatch_optimization_run_execution(run_id: int) -> None:
    def run_worker() -> None:
        with SessionLocal() as session:
            execute_optimization_run(session, run_id)

    thread = threading.Thread(
        target=run_worker,
        name=f"optimization-run-{run_id}",
        daemon=True,
    )
    thread.start()


def cancel_optimization_run(session, run_id: int) -> OptimizationRun:
    run = session.get(OptimizationRun, run_id)
    if run is None:
        raise LookupError("Optimization run not found.")
    if run.status == "running":
        run.status = "cancel_requested"
    session.commit()
    session.refresh(run)
    return run
