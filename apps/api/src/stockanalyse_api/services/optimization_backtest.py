from __future__ import annotations

import json
import math
import random
import threading
from dataclasses import fields
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from itertools import product
from typing import Any

from sqlalchemy import select

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.backtests.models import OptimizationResult, OptimizationRun
from stockanalyse_api.services.dashboard import (
    DEFAULT_CUP_HANDLE_PARAMS,
    DEFAULT_FUNDAMENTAL_GROWTH_PARAMS,
    CupHandleParams,
    FundamentalGrowthParams,
    get_overview,
    normalize_market,
)
from stockanalyse_api.services.dashboard_strategy_backtest import run_cup_handle_rps_backtest

DEFAULT_OPTIMIZATION_OBJECTIVE = "score"
DEFAULT_MAX_PARAMETER_SETS = 1000
RATIO_PATTERN = Decimal("0.000001")
SUPPORTED_OPTIMIZATION_OBJECTIVES = {
    "score",
    "average_annualized_return",
    "annualized_return",
    "max_drawdown",
    "return_drawdown_ratio",
    "win_rate",
    "total_return",
}
SUPPORTED_SEARCH_MODES = {"grid", "random"}


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
) -> dict[str, object]:
    metadata: dict[str, object] = {"search_mode": search_mode}
    if random_seed is not None:
        metadata["random_seed"] = random_seed
    return metadata


def _split_parameter_space_metadata(
    parameter_space: dict[str, object] | None,
) -> tuple[dict[str, object], dict[str, object]]:
    raw_space = dict(parameter_space or {})
    metadata = raw_space.pop("_optimization", {})
    if not isinstance(metadata, dict):
        metadata = {}
    return raw_space, metadata


def _parameter_axes(parameter_space: dict[str, object]) -> list[tuple[str, list[object]]]:
    return [
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
            _as_list(parameter_space.get("fundamental_growth_params"), default=[{}]),
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


def _fundamental_params_from_payload(payload: dict[str, object] | None) -> FundamentalGrowthParams:
    merged = _dataclass_payload(DEFAULT_FUNDAMENTAL_GROWTH_PARAMS, payload)
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
    )


def _normalize_parameter_set(parameters: dict[str, object]) -> dict[str, object]:
    cup_params = _cup_handle_params_from_payload(parameters.get("cup_handle_params") if isinstance(parameters.get("cup_handle_params"), dict) else {})
    fundamental_params = _fundamental_params_from_payload(
        parameters.get("fundamental_growth_params")
        if isinstance(parameters.get("fundamental_growth_params"), dict)
        else {}
    )
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
        "rps_threshold": int(parameters.get("rps_threshold", 90)),
        "selected_rps_windows": selected_windows,
        "min_rps_windows_passing": int(parameters.get("min_rps_windows_passing", 1)),
        "use_cup_handle": bool(parameters.get("use_cup_handle", True)),
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
        "started_at": run.started_at.isoformat(),
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "error_message": run.error_message,
        "search_mode": metadata.get("search_mode", "grid"),
        "random_seed": metadata.get("random_seed"),
        "parameter_space": parameter_space,
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

    raw_parameter_space, _ = _split_parameter_space_metadata(parameter_space)
    stored_parameter_space = {
        **raw_parameter_space,
        "_optimization": _optimization_metadata(
            search_mode=search_mode,
            random_seed=random_seed,
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


def delete_optimization_run(session, run_id: int) -> None:
    run = session.get(OptimizationRun, run_id)
    if run is None:
        raise LookupError("Optimization run not found.")
    if run.status in {"running", "cancel_requested"}:
        raise ValueError("Cannot delete a running optimization run; cancel it first.")
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
) -> dict[str, object]:
    result = run_cup_handle_rps_backtest(
        session,
        start_date=start_date,
        end_date=end_date,
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
    )
    return result.to_dict()


def _entry_reason(parameters: dict[str, object]) -> str:
    parts = [
        f"RPS 达到 {parameters.get('rps_threshold', '—')}",
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

    screen_cache: dict[str, dict[str, object]] = {}
    train_result = _run_backtest_once(
        session,
        start_date=run.train_start_date,
        end_date=run.train_end_date,
        market=run.market,
        parameters=parameters,
        max_trades_returned=max_trades_returned,
        screen_cache=screen_cache,
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
        )

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
) -> None:
    session.add(
        OptimizationResult(
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
    )


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
    screen_cache: dict[str, dict[str, object]] = {}
    try:
        for parameters in parameter_sets:
            session.refresh(run)
            if run.status == "cancel_requested":
                _rank_results(session, run)
                run.status = "cancelled"
                run.completed_at = datetime.now(UTC)
                session.commit()
                session.refresh(run)
                return run
            try:
                train_result = _run_backtest_once(
                    session,
                    start_date=run.train_start_date,
                    end_date=run.train_end_date,
                    market=run.market,
                    parameters=parameters,
                    screen_cache=screen_cache,
                )
                train_metrics = _extract_metrics(train_result)
                validation_metrics = None
                score_source = train_metrics
                if run.validation_start_date is not None and run.validation_end_date is not None:
                    validation_result = _run_backtest_once(
                        session,
                        start_date=run.validation_start_date,
                        end_date=run.validation_end_date,
                        market=run.market,
                        parameters=parameters,
                        screen_cache=screen_cache,
                    )
                    validation_metrics = _extract_metrics(validation_result)
                    score_source = validation_metrics
                _attach_average_annualized_return(train_metrics, validation_metrics)
                score = _score_metrics(score_source, objective=run.objective)
                _persist_result(
                    session,
                    run=run,
                    parameters=parameters,
                    train_metrics=train_metrics,
                    validation_metrics=validation_metrics,
                    score=score,
                    status="completed",
                )
                run.completed_parameter_sets += 1
            except Exception as exc:
                _persist_result(
                    session,
                    run=run,
                    parameters=parameters,
                    train_metrics=None,
                    validation_metrics=None,
                    score=None,
                    status="failed",
                    failure_reason=f"{type(exc).__name__}: {exc}",
                )
                run.failed_parameter_sets += 1
            session.commit()

        _rank_results(session, run)
        run.status = "completed" if run.completed_parameter_sets else "failed"
        run.completed_at = datetime.now(UTC)
        if run.status == "failed":
            run.error_message = "All parameter sets failed."
        session.commit()
        session.refresh(run)
        return run
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
