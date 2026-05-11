from __future__ import annotations

import os
from copy import deepcopy
import math
import random
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from datetime import UTC, date, datetime
from decimal import Decimal
from itertools import product
from typing import Any

from sqlalchemy import func, select

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.backtests.models import (
    OptimizationResult,
    OptimizationResultDetailCache,
    OptimizationRun,
)
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.dashboard import (
    _market_exchanges,
    get_overview,
    normalize_market,
)
from stockanalyse_api.services.dashboard_strategy_backtest import (
    BacktestCancelledError,
    DEFAULT_CASH_FALLBACK_PARAMS,
    DEFAULT_MARKET_FILTER_PARAMS,
    DEFAULT_RELATIVE_STRENGTH_PARAMS,
    run_cup_handle_rps_backtest,
)
from stockanalyse_api.services.market_data_adjustments import adjusted_close
from stockanalyse_api.services.market_data_adjustments import is_complete_market_row
from stockanalyse_api.services.strategy_parameters import (
    DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS,
    _coerce_decimal,
    _coerce_optional_decimal,
    _coerce_optional_int,
    _cup_handle_params_from_payload,
    _fundamental_params_from_payload,
    dump_json,
    load_json,
    normalize_strategy_parameter_set,
    stable_parameter_hash,
)

DEFAULT_OPTIMIZATION_OBJECTIVE = "score"
DEFAULT_MAX_PARAMETER_SETS = 1000
DEFAULT_OPTIMIZATION_PARALLEL_GROUP_SIZE = 1
DEFAULT_OPTIMIZATION_DETAIL_CACHE_TRADES = 300
DEFAULT_BENCHMARK_SYMBOLS = ("SPY", "QQQ")
BACKTEST_SEMANTICS_VERSION = "optimization_strict_window_mtm_v1"
MAX_AUTO_OPTIMIZATION_WORKERS = 3
DEFAULT_OPTIMIZATION_MAX_TASKS_PER_CHILD = 24
DEFAULT_WORKER_CACHE_MAX_ENTRIES = 4096
DEFAULT_WORKER_FUNDAMENTAL_CACHE_MAX_ENTRIES = 20000
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
    "spy_alpha",
}
SUPPORTED_SEARCH_MODES = {"grid", "random"}


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None or raw_value == "":
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return max(minimum, value)


class _BoundedDict(dict):
    def __init__(self, *, max_entries: int) -> None:
        super().__init__()
        self.max_entries = max(1, max_entries)

    def __setitem__(self, key, value) -> None:  # type: ignore[no-untyped-def]
        if key not in self and len(self) >= self.max_entries:
            self.pop(next(iter(self)))
        super().__setitem__(key, value)


_WORKER_CACHE_MAX_ENTRIES = _env_int(
    "STOCKANALYSE_OPTIMIZATION_WORKER_CACHE_MAX_ENTRIES",
    DEFAULT_WORKER_CACHE_MAX_ENTRIES,
    minimum=128,
)
_WORKER_FUNDAMENTAL_CACHE_MAX_ENTRIES = _env_int(
    "STOCKANALYSE_OPTIMIZATION_WORKER_FUNDAMENTAL_CACHE_MAX_ENTRIES",
    DEFAULT_WORKER_FUNDAMENTAL_CACHE_MAX_ENTRIES,
    minimum=1024,
)

_WORKER_SCREEN_CACHE: dict[str, dict[str, object]] = _BoundedDict(
    max_entries=_WORKER_CACHE_MAX_ENTRIES,
)
_WORKER_SCREEN_CANDIDATE_CACHE: dict[tuple[object, ...], dict[str, object]] = _BoundedDict(
    max_entries=_WORKER_CACHE_MAX_ENTRIES,
)
_WORKER_FUNDAMENTAL_GROWTH_CACHE: dict[tuple[object, ...], object] = _BoundedDict(
    max_entries=_WORKER_FUNDAMENTAL_CACHE_MAX_ENTRIES,
)
_WORKER_CUP_EVENT_CACHE: dict[tuple[object, ...], dict[int, list[object]] | None] = _BoundedDict(
    max_entries=_WORKER_CACHE_MAX_ENTRIES,
)
_WORKER_TRADE_CACHE: dict[tuple[object, ...], object] = _BoundedDict(
    max_entries=_WORKER_CACHE_MAX_ENTRIES,
)
_WORKER_TRADE_DATES_CACHE: dict[tuple[object, ...], list[date]] = _BoundedDict(
    max_entries=128,
)
_WORKER_FUTURE_ROWS_CACHE: dict[tuple[object, ...], object] = _BoundedDict(
    max_entries=_WORKER_CACHE_MAX_ENTRIES,
)
_WORKER_FUTURE_INDICATOR_CACHE: dict[tuple[object, ...], object] = _BoundedDict(
    max_entries=_WORKER_CACHE_MAX_ENTRIES,
)
_WORKER_MARKET_FILTER_CACHE: dict[tuple[object, ...], set[date]] = _BoundedDict(
    max_entries=128,
)
_WORKER_RELATIVE_STRENGTH_CACHE: dict[
    tuple[object, ...],
    dict[int, dict[str, object]],
] = _BoundedDict(max_entries=_WORKER_CACHE_MAX_ENTRIES)
_WORKER_BENCHMARK_CACHE: dict[tuple[object, ...], dict[str, object]] = _BoundedDict(
    max_entries=128,
)


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


def _configured_max_tasks_per_child_from_metadata(
    metadata: dict[str, object],
) -> int | None:
    raw_value = metadata.get("max_tasks_per_child")
    if raw_value in {None, "", "auto"}:
        resolved = _env_int(
            "STOCKANALYSE_OPTIMIZATION_MAX_TASKS_PER_CHILD",
            DEFAULT_OPTIMIZATION_MAX_TASKS_PER_CHILD,
            minimum=0,
        )
    else:
        resolved = int(raw_value)
        if resolved < 0:
            raise ValueError("max_tasks_per_child must be greater than or equal to 0.")
    return resolved if resolved > 0 else None


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
        ("initial_capital", _as_list(parameter_space.get("initial_capital"), default=["100000"])),
        (
            "position_size_amount",
            _as_list(parameter_space.get("position_size_amount"), default=[None]),
        ),
        (
            "allow_reentry_while_open",
            _as_list(parameter_space.get("allow_reentry_while_open"), default=[False]),
        ),
        (
            "market_filter_params",
            _as_list(
                parameter_space.get("market_filter_params"),
                default=[DEFAULT_MARKET_FILTER_PARAMS],
            ),
        ),
        (
            "relative_strength_params",
            _as_list(
                parameter_space.get("relative_strength_params"),
                default=[DEFAULT_RELATIVE_STRENGTH_PARAMS],
            ),
        ),
        (
            "cash_fallback_params",
            _as_list(
                parameter_space.get("cash_fallback_params"),
                default=[DEFAULT_CASH_FALLBACK_PARAMS],
            ),
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


def _normalize_parameter_set(parameters: dict[str, object]) -> dict[str, object]:
    return normalize_strategy_parameter_set(parameters)


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


def serialize_optimization_run(
    run: OptimizationRun,
    *,
    include_parameter_sets: bool = True,
) -> dict[str, object]:
    parameter_space, metadata = _split_parameter_space_metadata(
        load_json(run.parameter_space_json, default={})
    )
    payload = {
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
        "data_snapshot": load_json(run.data_snapshot_json, default={}),
    }
    parameter_sets: list[object] = []
    if include_parameter_sets or run.status == "running":
        loaded_parameter_sets = load_json(run.parameter_sets_json, default=[])
        parameter_sets = loaded_parameter_sets if isinstance(loaded_parameter_sets, list) else []
    if include_parameter_sets:
        payload["parameter_sets"] = parameter_sets
    if run.status == "running":
        current_index = int(run.completed_parameter_sets or 0) + int(run.failed_parameter_sets or 0)
        payload["current_parameter_set"] = (
            parameter_sets[current_index]
            if 0 <= current_index < len(parameter_sets)
            else None
        )
    return payload


def _compact_optimization_metrics(metrics: object) -> object:
    if not isinstance(metrics, dict):
        return metrics
    return {
        key: value
        for key, value in metrics.items()
        if key not in {"equity_curve", "yearly_returns"}
    }


_OPTIMIZATION_METRIC_SUMMARY_KEYS = {
    "annualized_return",
    "average_annualized_return",
    "average_trade_return",
    "completed_trades",
    "final_capital",
    "initial_capital",
    "max_drawdown",
    "return_drawdown_ratio",
    "rps_exit_trigger_ratio",
    "sample_penalty",
    "selected_trades",
    "signal_dates_evaluated",
    "spy_average_trade_excess_return",
    "spy_excess_annualized_return",
    "spy_excess_total_return",
    "spy_excess_trade_win_rate",
    "spy_max_drawdown_improvement",
    "stop_loss_trigger_ratio",
    "total_profit",
    "total_return",
    "train_objective_score",
    "train_rank",
    "validation_rank",
    "validation_rank_delta",
    "win_rate",
}


def _summarize_optimization_metrics(metrics: object) -> object:
    if not isinstance(metrics, dict):
        return metrics
    return {
        key: value
        for key, value in metrics.items()
        if key in _OPTIMIZATION_METRIC_SUMMARY_KEYS
    }


def serialize_optimization_result(
    result: OptimizationResult,
    *,
    include_metric_series: bool = True,
    metrics_summary_only: bool = False,
) -> dict[str, object]:
    train_metrics = load_json(result.train_metrics_json, default=None)
    validation_metrics = load_json(result.validation_metrics_json, default=None)
    if metrics_summary_only:
        train_metrics = _summarize_optimization_metrics(train_metrics)
        validation_metrics = _summarize_optimization_metrics(validation_metrics)
    if not include_metric_series:
        train_metrics = _compact_optimization_metrics(train_metrics)
        validation_metrics = _compact_optimization_metrics(validation_metrics)
    return {
        "id": result.id,
        "optimization_run_id": result.optimization_run_id,
        "parameter_hash": result.parameter_hash,
        "parameters": load_json(result.parameters_json, default={}),
        "train_metrics": train_metrics,
        "validation_metrics": validation_metrics,
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


def _single_parameter_space_from_parameters(parameters: dict[str, object]) -> dict[str, object]:
    normalized = _normalize_parameter_set(parameters)
    return {
        axis_name: [deepcopy(normalized[axis_name])]
        for axis_name, _ in _parameter_axes({})
        if axis_name in normalized
    }


def create_optimization_rerun_from_result(
    session,
    *,
    result_id: int,
    max_workers: int | str | None = 1,
    require_data_ready: bool = True,
) -> OptimizationRun:
    result = get_optimization_result(session, result_id)
    if result is None:
        raise LookupError("Optimization result not found.")
    source_run = get_optimization_run(session, result.optimization_run_id)
    if source_run is None:
        raise LookupError("Source optimization run not found.")
    parameters = load_json(result.parameters_json, default={})
    if not isinstance(parameters, dict):
        raise ValueError("Optimization result parameters are invalid.")
    return create_optimization_run(
        session,
        market=source_run.market,
        train_start_date=source_run.train_start_date,
        train_end_date=source_run.train_end_date,
        validation_start_date=source_run.validation_start_date,
        validation_end_date=source_run.validation_end_date,
        parameter_space=_single_parameter_space_from_parameters(parameters),
        objective=source_run.objective,
        max_parameter_sets=1,
        search_mode="grid",
        random_seed=None,
        max_workers=max_workers,
        require_data_ready=require_data_ready,
    )


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


def _optional_decimal_metric(metrics: dict[str, object] | None, key: str) -> Decimal | None:
    if metrics is None:
        return None
    value = metrics.get(key)
    if value is None:
        return None
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


def _max_drawdown_from_equity(equity_values: list[Decimal]) -> Decimal | None:
    if not equity_values:
        return None
    peak = equity_values[0]
    max_drawdown = Decimal("0")
    for equity in equity_values:
        if equity > peak:
            peak = equity
        if peak <= Decimal("0"):
            continue
        drawdown = (equity / peak) - Decimal("1")
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _benchmark_cache_key(
    *,
    market: str,
    start_date: date,
    end_date: date,
    symbols: tuple[str, ...],
) -> tuple[object, ...]:
    return (
        "benchmark_metrics",
        normalize_market(market),
        start_date.isoformat(),
        end_date.isoformat(),
        tuple(symbol.upper() for symbol in symbols),
    )


def _load_benchmark_metrics(
    session,
    *,
    market: str,
    start_date: date,
    end_date: date,
    symbols: tuple[str, ...] = DEFAULT_BENCHMARK_SYMBOLS,
    benchmark_cache: dict[tuple[object, ...], dict[str, object]] | None = None,
) -> dict[str, object]:
    cache_key = _benchmark_cache_key(
        market=market,
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
    )
    cached = benchmark_cache.get(cache_key) if benchmark_cache is not None else None
    if cached is not None:
        return cached

    resolved_market = normalize_market(market)
    exchanges = _market_exchanges(resolved_market)
    benchmarks: dict[str, object] = {}
    for symbol in symbols:
        rows = list(
            session.execute(
                select(MarketDataDaily)
                .join(Instrument, Instrument.id == MarketDataDaily.instrument_id)
                .where(
                    Instrument.symbol == symbol.upper(),
                    Instrument.exchange.in_(exchanges),
                    MarketDataDaily.trade_date >= start_date,
                    MarketDataDaily.trade_date <= end_date,
                )
                .order_by(MarketDataDaily.trade_date.asc())
            ).scalars()
        )
        price_points: list[tuple[date, Decimal]] = []
        for row in rows:
            if not is_complete_market_row(row):
                continue
            close = adjusted_close(row)
            if close is None or close <= Decimal("0"):
                continue
            price_points.append((row.trade_date, close))
        if len(price_points) < 2:
            benchmarks[symbol.upper()] = {
                "symbol": symbol.upper(),
                "status": "insufficient_data",
                "requested_start_date": start_date.isoformat(),
                "requested_end_date": end_date.isoformat(),
                "data_points": len(price_points),
                "total_return": None,
                "annualized_return": None,
                "max_drawdown": None,
                "return_drawdown_ratio": None,
            }
            continue

        first_date, first_price = price_points[0]
        last_date, last_price = price_points[-1]
        total_return = (last_price / first_price) - Decimal("1")
        annualized_return = _annualize_return(
            total_return,
            start_date=first_date.isoformat(),
            end_date=last_date.isoformat(),
        )
        equity_values = [price / first_price for _, price in price_points]
        max_drawdown = _max_drawdown_from_equity(equity_values)
        return_drawdown_ratio = (
            annualized_return / abs(max_drawdown)
            if annualized_return is not None
            and max_drawdown is not None
            and max_drawdown < Decimal("0")
            else None
        )
        benchmarks[symbol.upper()] = {
            "symbol": symbol.upper(),
            "status": "ok",
            "start_date": first_date.isoformat(),
            "end_date": last_date.isoformat(),
            "requested_start_date": start_date.isoformat(),
            "requested_end_date": end_date.isoformat(),
            "data_points": len(price_points),
            "total_return": _format_metric(total_return),
            "annualized_return": _format_metric(annualized_return),
            "max_drawdown": _format_metric(max_drawdown),
            "return_drawdown_ratio": _format_metric(return_drawdown_ratio),
        }

    if benchmark_cache is not None:
        benchmark_cache[cache_key] = benchmarks
    return benchmarks


def _benchmark_status(benchmarks: dict[str, object]) -> str:
    statuses = [
        str(benchmark.get("status"))
        for benchmark in benchmarks.values()
        if isinstance(benchmark, dict)
    ]
    if statuses and all(status == "ok" for status in statuses):
        return "complete"
    if any(status == "ok" for status in statuses):
        return "partial"
    return "missing"


def _benchmark_relative_metrics(
    metrics: dict[str, object],
    benchmarks: dict[str, object],
) -> dict[str, object]:
    strategy_total_return = _optional_decimal_metric(metrics, "total_return")
    strategy_annualized_return = _optional_decimal_metric(metrics, "annualized_return")
    strategy_max_drawdown = _optional_decimal_metric(metrics, "max_drawdown")
    relative: dict[str, object] = {}
    for symbol, benchmark in benchmarks.items():
        if not isinstance(benchmark, dict):
            continue
        benchmark_total_return = _optional_decimal_metric(benchmark, "total_return")
        benchmark_annualized_return = _optional_decimal_metric(benchmark, "annualized_return")
        benchmark_max_drawdown = _optional_decimal_metric(benchmark, "max_drawdown")
        relative[symbol] = {
            "excess_total_return": _format_metric(
                strategy_total_return - benchmark_total_return
                if strategy_total_return is not None and benchmark_total_return is not None
                else None
            ),
            "excess_annualized_return": _format_metric(
                strategy_annualized_return - benchmark_annualized_return
                if strategy_annualized_return is not None
                and benchmark_annualized_return is not None
                else None
            ),
            "max_drawdown_improvement": _format_metric(
                strategy_max_drawdown - benchmark_max_drawdown
                if strategy_max_drawdown is not None and benchmark_max_drawdown is not None
                else None
            ),
        }
    return relative


def _attach_primary_benchmark_metrics(metrics: dict[str, object]) -> None:
    relative = metrics.get("benchmark_relative")
    if not isinstance(relative, dict):
        return
    spy_relative = relative.get("SPY")
    if not isinstance(spy_relative, dict):
        return
    metrics["spy_excess_total_return"] = spy_relative.get("excess_total_return")
    metrics["spy_excess_annualized_return"] = spy_relative.get("excess_annualized_return")
    metrics["spy_max_drawdown_improvement"] = spy_relative.get("max_drawdown_improvement")


def _portfolio_metrics_from_signal_days(result: dict[str, object]) -> dict[str, object]:
    if result.get("account_total_return") is not None:
        account_annualized_return = (
            _coerce_decimal(
                result.get("account_annualized_return") or "0",
                field_name="account_annualized_return",
            )
            if result.get("account_annualized_return") is not None
            else None
        )
        account_max_drawdown = (
            _coerce_decimal(
                result.get("account_max_drawdown") or "0",
                field_name="account_max_drawdown",
            )
            if result.get("account_max_drawdown") is not None
            else None
        )
        return_drawdown_ratio = (
            account_annualized_return / abs(account_max_drawdown)
            if account_annualized_return is not None
            and account_max_drawdown is not None
            and account_max_drawdown < Decimal("0")
            else None
        )
        return {
            "total_return": result.get("account_total_return"),
            "annualized_return": result.get("account_annualized_return"),
            "average_annualized_return": result.get("account_annualized_return"),
            "max_drawdown": result.get("account_max_drawdown"),
            "return_drawdown_ratio": _format_metric(return_drawdown_ratio),
            "signal_day_return_count": len(result.get("account_equity_curve") or []),
            "equity_curve": result.get("account_equity_curve") or [],
            "yearly_returns": result.get("account_yearly_returns") or {},
            "initial_capital": result.get("initial_capital"),
            "position_size_amount": result.get("position_size_amount"),
            "final_capital": result.get("final_capital"),
            "total_profit": result.get("total_profit"),
        }
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


def _extract_metrics(
    result: dict[str, object],
    *,
    benchmark_metrics: dict[str, object] | None = None,
) -> dict[str, object]:
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
        "spy_trade_benchmark_count": int(result.get("spy_trade_benchmark_count", 0) or 0),
        "spy_average_trade_benchmark_return": result.get("spy_average_trade_benchmark_return"),
        "spy_average_trade_excess_return": result.get("spy_average_trade_excess_return"),
        "spy_median_trade_excess_return": result.get("spy_median_trade_excess_return"),
        "spy_excess_trade_win_rate": result.get("spy_excess_trade_win_rate"),
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
        "backtest_semantics_version": BACKTEST_SEMANTICS_VERSION,
    }
    metrics.update(_portfolio_metrics_from_signal_days(result))
    if benchmark_metrics is not None:
        metrics["benchmarks"] = benchmark_metrics
        metrics["benchmark_status"] = _benchmark_status(benchmark_metrics)
        metrics["benchmark_relative"] = _benchmark_relative_metrics(metrics, benchmark_metrics)
        _attach_primary_benchmark_metrics(metrics)
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
    if objective == "spy_alpha":
        return _alpha_score_metrics(metrics)
    raise ValueError(f"Unsupported optimization objective: {objective}.")


def _alpha_score_metrics(metrics: dict[str, object]) -> Decimal:
    trade_excess = _optional_decimal_metric(metrics, "spy_average_trade_excess_return")
    if trade_excess is not None:
        completed = Decimal(int(metrics.get("completed_trades", 0) or 0))
        trade_benchmark_count = Decimal(int(metrics.get("spy_trade_benchmark_count", 0) or 0))
        excess_win_rate = _optional_decimal_metric(metrics, "spy_excess_trade_win_rate") or Decimal("0")
        annualized_return = _optional_decimal_metric(metrics, "annualized_return") or Decimal("0")
        max_drawdown = abs(_optional_decimal_metric(metrics, "max_drawdown") or Decimal("0"))
        sample_penalty = _alpha_trade_sample_penalty(
            train_completed=completed,
            validation_completed=trade_benchmark_count,
        )
        score = (
            trade_excess * Decimal("0.75")
            + excess_win_rate * Decimal("0.08")
            + annualized_return * Decimal("0.08")
            - max_drawdown * Decimal("0.08")
            - sample_penalty
        )
        return score.quantize(RATIO_PATTERN)

    if metrics.get("benchmark_status") == "missing":
        return Decimal("-1.000000")
    excess_annualized = _optional_decimal_metric(metrics, "spy_excess_annualized_return")
    excess_total = _optional_decimal_metric(metrics, "spy_excess_total_return")
    drawdown_improvement = _optional_decimal_metric(metrics, "spy_max_drawdown_improvement")
    if excess_annualized is None:
        return Decimal("-1.000000")
    completed = Decimal(int(metrics.get("completed_trades", 0) or 0))
    sample_penalty = _robust_sample_penalty(
        train_completed=completed,
        validation_completed=completed,
    )
    score = (
        excess_annualized * Decimal("0.70")
        + (excess_total or Decimal("0")) * Decimal("0.15")
        + (drawdown_improvement or Decimal("0")) * Decimal("0.20")
        - sample_penalty
    )
    return score.quantize(RATIO_PATTERN)


def _alpha_trade_sample_penalty(
    *,
    train_completed: Decimal,
    validation_completed: Decimal,
) -> Decimal:
    min_completed = min(train_completed, validation_completed)
    if min_completed < Decimal("3"):
        return Decimal("0.18")
    if min_completed < Decimal("5"):
        return Decimal("0.10")
    if min_completed < Decimal("10"):
        return Decimal("0.05")
    if min_completed < Decimal("20"):
        return Decimal("0.02")
    return Decimal("0")


def _robust_sample_penalty(
    *,
    train_completed: Decimal,
    validation_completed: Decimal,
) -> Decimal:
    min_completed = min(train_completed, validation_completed)
    if min_completed < Decimal("5"):
        return Decimal("0.25")
    if min_completed < Decimal("10"):
        return Decimal("0.12")
    if min_completed < Decimal("20"):
        return Decimal("0.06")
    if min_completed < Decimal("40"):
        return Decimal("0.02")
    return Decimal("0")


def _score_metric_pair(
    train_metrics: dict[str, object],
    validation_metrics: dict[str, object] | None,
    *,
    objective: str = DEFAULT_OPTIMIZATION_OBJECTIVE,
) -> Decimal:
    if objective == "spy_alpha":
        return _score_alpha_metric_pair(train_metrics, validation_metrics)
    if objective != "robust_annualized_return":
        return _score_metrics(validation_metrics or train_metrics, objective=objective)
    train_annualized_value = _optional_decimal_metric(train_metrics, "annualized_return")
    validation_annualized_value = (
        _optional_decimal_metric(validation_metrics, "annualized_return")
        if validation_metrics is not None
        else train_annualized_value
    )
    train_annualized = train_annualized_value or Decimal("0")
    validation_annualized = validation_annualized_value or Decimal("0")
    train_drawdown = abs(_optional_decimal_metric(train_metrics, "max_drawdown") or Decimal("0"))
    validation_drawdown = (
        abs(_optional_decimal_metric(validation_metrics, "max_drawdown") or Decimal("0"))
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
    annualized_gap = abs(train_annualized - validation_annualized)
    largest_drawdown = max(train_drawdown, validation_drawdown)
    drawdown_penalty = largest_drawdown * Decimal("0.08")
    gap_penalty = annualized_gap * Decimal("0.06")
    sample_penalty = _robust_sample_penalty(
        train_completed=train_completed,
        validation_completed=validation_completed,
    )
    missing_penalty = Decimal("0")
    if train_annualized_value is None:
        missing_penalty += Decimal("0.18")
    if validation_metrics is not None and validation_annualized_value is None:
        missing_penalty += Decimal("0.18")
    negative_train_penalty = abs(min(train_annualized, Decimal("0"))) * Decimal("0.25")
    if train_annualized < Decimal("-0.03"):
        negative_train_penalty += abs(train_annualized - Decimal("-0.03")) * Decimal("1.20")
    risk_adjusted_bonus = Decimal("0")
    if validation_annualized > Decimal("0"):
        drawdown_floor = max(validation_drawdown, Decimal("0.03"))
        risk_adjusted_bonus = min(
            Decimal("0.18"),
            (validation_annualized / drawdown_floor) * Decimal("0.04"),
        )
    score = (
        validation_annualized * Decimal("0.55")
        + consistency_floor * Decimal("0.20")
        + average_return * Decimal("0.15")
        + risk_adjusted_bonus
        - gap_penalty
        - drawdown_penalty
        - sample_penalty
        - negative_train_penalty
        - missing_penalty
    )
    return score.quantize(RATIO_PATTERN)


def _score_alpha_metric_pair(
    train_metrics: dict[str, object],
    validation_metrics: dict[str, object] | None,
) -> Decimal:
    train_trade_excess_value = _optional_decimal_metric(
        train_metrics,
        "spy_average_trade_excess_return",
    )
    validation_trade_excess_value = (
        _optional_decimal_metric(validation_metrics, "spy_average_trade_excess_return")
        if validation_metrics is not None
        else train_trade_excess_value
    )
    if train_trade_excess_value is not None and validation_trade_excess_value is not None:
        train_excess = train_trade_excess_value
        validation_excess = validation_trade_excess_value
        train_completed = Decimal(int(train_metrics.get("completed_trades", 0) or 0))
        validation_completed = (
            Decimal(int(validation_metrics.get("completed_trades", 0) or 0))
            if validation_metrics is not None
            else train_completed
        )
        train_benchmark_count = Decimal(int(train_metrics.get("spy_trade_benchmark_count", 0) or 0))
        validation_benchmark_count = (
            Decimal(int(validation_metrics.get("spy_trade_benchmark_count", 0) or 0))
            if validation_metrics is not None
            else train_benchmark_count
        )
        validation_win_rate = (
            _optional_decimal_metric(validation_metrics, "spy_excess_trade_win_rate")
            if validation_metrics is not None
            else _optional_decimal_metric(train_metrics, "spy_excess_trade_win_rate")
        ) or Decimal("0")
        validation_annualized = (
            _optional_decimal_metric(validation_metrics, "annualized_return")
            if validation_metrics is not None
            else _optional_decimal_metric(train_metrics, "annualized_return")
        ) or Decimal("0")
        validation_drawdown = abs(
            (
                _optional_decimal_metric(validation_metrics, "max_drawdown")
                if validation_metrics is not None
                else _optional_decimal_metric(train_metrics, "max_drawdown")
            )
            or Decimal("0")
        )
        consistency_floor = min(train_excess, validation_excess)
        average_excess = (train_excess + validation_excess) / Decimal("2")
        excess_gap = abs(train_excess - validation_excess)
        sample_penalty = _alpha_trade_sample_penalty(
            train_completed=min(train_completed, train_benchmark_count),
            validation_completed=min(validation_completed, validation_benchmark_count),
        )
        negative_train_penalty = abs(min(train_excess, Decimal("0"))) * Decimal("0.35")
        score = (
            validation_excess * Decimal("0.60")
            + consistency_floor * Decimal("0.20")
            + average_excess * Decimal("0.10")
            + validation_win_rate * Decimal("0.08")
            + validation_annualized * Decimal("0.08")
            - validation_drawdown * Decimal("0.08")
            - excess_gap * Decimal("0.08")
            - sample_penalty
            - negative_train_penalty
        )
        return score.quantize(RATIO_PATTERN)

    target_metrics = validation_metrics or train_metrics
    if target_metrics.get("benchmark_status") == "missing":
        return Decimal("-1.000000")
    train_excess_value = _optional_decimal_metric(
        train_metrics,
        "spy_excess_annualized_return",
    )
    validation_excess_value = (
        _optional_decimal_metric(validation_metrics, "spy_excess_annualized_return")
        if validation_metrics is not None
        else train_excess_value
    )
    if train_excess_value is None or validation_excess_value is None:
        return Decimal("-1.000000")
    train_excess = train_excess_value
    validation_excess = validation_excess_value
    validation_total_excess = (
        _optional_decimal_metric(validation_metrics, "spy_excess_total_return")
        if validation_metrics is not None
        else _optional_decimal_metric(train_metrics, "spy_excess_total_return")
    ) or Decimal("0")
    validation_drawdown_improvement = (
        _optional_decimal_metric(validation_metrics, "spy_max_drawdown_improvement")
        if validation_metrics is not None
        else _optional_decimal_metric(train_metrics, "spy_max_drawdown_improvement")
    ) or Decimal("0")
    train_completed = Decimal(int(train_metrics.get("completed_trades", 0) or 0))
    validation_completed = (
        Decimal(int(validation_metrics.get("completed_trades", 0) or 0))
        if validation_metrics is not None
        else train_completed
    )
    consistency_floor = min(train_excess, validation_excess)
    average_excess = (train_excess + validation_excess) / Decimal("2")
    excess_gap = abs(train_excess - validation_excess)
    sample_penalty = _robust_sample_penalty(
        train_completed=train_completed,
        validation_completed=validation_completed,
    )
    benchmark_penalty = Decimal("0")
    if train_metrics.get("benchmark_status") != "complete":
        benchmark_penalty += Decimal("0.08")
    if validation_metrics is not None and validation_metrics.get("benchmark_status") != "complete":
        benchmark_penalty += Decimal("0.08")
    negative_train_penalty = abs(min(train_excess, Decimal("0"))) * Decimal("0.25")
    score = (
        validation_excess * Decimal("0.60")
        + consistency_floor * Decimal("0.20")
        + average_excess * Decimal("0.10")
        + validation_total_excess * Decimal("0.08")
        + validation_drawdown_improvement * Decimal("0.15")
        - excess_gap * Decimal("0.08")
        - sample_penalty
        - benchmark_penalty
        - negative_train_penalty
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
    market_filter_cache: dict[tuple[object, ...], set[date]] | None = None,
    relative_strength_cache: dict[tuple[object, ...], dict[int, dict[str, object]]] | None = None,
    prefer_broad_candidate_cache: bool = False,
    max_broad_candidate_cache_dates: int | None = None,
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
        initial_capital=_coerce_decimal(
            parameters.get("initial_capital", "100000"),
            field_name="initial_capital",
        ),
        position_size_amount=_coerce_optional_decimal(
            parameters.get("position_size_amount"),
            field_name="position_size_amount",
        ),
        allow_reentry_while_open=bool(parameters.get("allow_reentry_while_open", False)),
        market_filter_params=(
            parameters.get("market_filter_params")
            if isinstance(parameters.get("market_filter_params"), dict)
            else DEFAULT_MARKET_FILTER_PARAMS
        ),
        relative_strength_params=(
            parameters.get("relative_strength_params")
            if isinstance(parameters.get("relative_strength_params"), dict)
            else DEFAULT_RELATIVE_STRENGTH_PARAMS
        ),
        cash_fallback_params=(
            parameters.get("cash_fallback_params")
            if isinstance(parameters.get("cash_fallback_params"), dict)
            else DEFAULT_CASH_FALLBACK_PARAMS
        ),
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
        market_filter_cache=market_filter_cache,
        relative_strength_cache=relative_strength_cache,
        should_cancel=should_cancel,
        preload_screen_candidates=False,
        prefer_broad_candidate_cache=prefer_broad_candidate_cache,
        max_broad_candidate_cache_dates=max_broad_candidate_cache_dates,
        execution_limited_screen=True,
        force_exit_at_end=True,
    )
    payload = result.to_dict()
    payload["backtest_semantics_version"] = BACKTEST_SEMANTICS_VERSION
    return payload


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
    market_filter_cache: dict[tuple[object, ...], set[date]] | None = None,
    relative_strength_cache: dict[tuple[object, ...], dict[int, dict[str, object]]] | None = None,
    benchmark_cache: dict[tuple[object, ...], dict[str, object]] | None = None,
    prefer_broad_candidate_cache: bool = False,
    max_broad_candidate_cache_dates: int | None = None,
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
            market_filter_cache=market_filter_cache,
            relative_strength_cache=relative_strength_cache,
            prefer_broad_candidate_cache=prefer_broad_candidate_cache,
            max_broad_candidate_cache_dates=max_broad_candidate_cache_dates,
            should_cancel=should_cancel,
        )
        train_benchmarks = _load_benchmark_metrics(
            session,
            market=market,
            start_date=start_date,
            end_date=end_date,
            benchmark_cache=benchmark_cache,
        )
        train_metrics = _extract_metrics(
            train_result,
            benchmark_metrics=train_benchmarks,
        )
        validation_result = None
        validation_metrics = None
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
                market_filter_cache=market_filter_cache,
                relative_strength_cache=relative_strength_cache,
                prefer_broad_candidate_cache=prefer_broad_candidate_cache,
                max_broad_candidate_cache_dates=max_broad_candidate_cache_dates,
                should_cancel=should_cancel,
            )
            validation_benchmarks = _load_benchmark_metrics(
                session,
                market=market,
                start_date=validation_start_date,
                end_date=validation_end_date,
                benchmark_cache=benchmark_cache,
            )
            validation_metrics = _extract_metrics(
                validation_result,
                benchmark_metrics=validation_benchmarks,
            )
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


def _has_spy_benchmark(metrics: dict[str, object] | None) -> bool:
    if metrics is None:
        return False
    benchmarks = metrics.get("benchmarks")
    if not isinstance(benchmarks, dict):
        return False
    spy = benchmarks.get("SPY")
    return isinstance(spy, dict) and spy.get("status") == "ok"


def evaluate_strategy_parameter_set(
    session,
    *,
    start_date: date,
    end_date: date,
    validation_start_date: date | None = None,
    validation_end_date: date | None = None,
    market: str,
    objective: str,
    parameters: dict[str, object],
    require_complete_benchmark: bool = False,
    screen_cache: dict[str, dict[str, object]] | None = None,
    screen_candidate_cache: dict[tuple[object, ...], dict[str, object]] | None = None,
    fundamental_growth_cache: dict[tuple[object, ...], object] | None = None,
    cup_event_cache: dict[tuple[object, ...], dict[int, list[object]] | None] | None = None,
    trade_cache: dict[tuple[object, ...], object] | None = None,
    trade_dates_cache: dict[tuple[object, ...], list[date]] | None = None,
    future_rows_cache: dict[tuple[object, ...], object] | None = None,
    future_indicator_cache: dict[tuple[object, ...], object] | None = None,
    market_filter_cache: dict[tuple[object, ...], set[date]] | None = None,
    relative_strength_cache: dict[tuple[object, ...], dict[int, dict[str, object]]] | None = None,
    benchmark_cache: dict[tuple[object, ...], dict[str, object]] | None = None,
    prefer_broad_candidate_cache: bool = False,
    max_broad_candidate_cache_dates: int | None = None,
    should_cancel=None,
) -> dict[str, object]:
    normalized_parameters = _normalize_parameter_set(parameters)
    evaluation = _evaluate_parameter_set(
        session,
        start_date=start_date,
        end_date=end_date,
        validation_start_date=validation_start_date,
        validation_end_date=validation_end_date,
        market=market,
        objective=objective,
        parameters=normalized_parameters,
        screen_cache=screen_cache,
        screen_candidate_cache=screen_candidate_cache,
        fundamental_growth_cache=fundamental_growth_cache,
        cup_event_cache=cup_event_cache,
        trade_cache=trade_cache,
        trade_dates_cache=trade_dates_cache,
        future_rows_cache=future_rows_cache,
        future_indicator_cache=future_indicator_cache,
        market_filter_cache=market_filter_cache,
        relative_strength_cache=relative_strength_cache,
        benchmark_cache=benchmark_cache,
        prefer_broad_candidate_cache=prefer_broad_candidate_cache,
        max_broad_candidate_cache_dates=max_broad_candidate_cache_dates,
        should_cancel=should_cancel,
    )
    if (
        require_complete_benchmark
        and objective == "spy_alpha"
        and evaluation.get("status") == "completed"
        and (
            not _has_spy_benchmark(evaluation.get("train_metrics"))  # type: ignore[arg-type]
            or (
                validation_start_date is not None
                and validation_end_date is not None
                and not _has_spy_benchmark(evaluation.get("validation_metrics"))  # type: ignore[arg-type]
            )
        )
    ):
        evaluation = {
            **evaluation,
            "score": None,
            "status": "failed",
            "failure_reason": "BenchmarkDataUnavailable: SPY benchmark metrics are required for spy_alpha.",
        }
    return evaluation


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
                    market_filter_cache=_WORKER_MARKET_FILTER_CACHE,
                    relative_strength_cache=_WORKER_RELATIVE_STRENGTH_CACHE,
                    benchmark_cache=_WORKER_BENCHMARK_CACHE,
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
        "window_end_mark": "窗口结束按收盘价估值",
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
    detail_unavailable_reason: str | None = None,
) -> dict[str, object]:
    result = dict(metrics or {})
    result.setdefault("start_date", start_date.isoformat())
    result.setdefault("end_date", end_date.isoformat())
    result.setdefault("parameters", parameters)
    result["detail_source"] = "metrics_only"
    if detail_unavailable_reason is not None:
        result["detail_unavailable_reason"] = detail_unavailable_reason
    result["trades"] = []
    return result


def _metrics_semantics_version(metrics: dict[str, object] | None) -> str | None:
    if not isinstance(metrics, dict):
        return None
    value = metrics.get("backtest_semantics_version")
    return str(value) if value is not None else None


def _result_semantics_version(result: dict[str, object] | None) -> str | None:
    if not isinstance(result, dict):
        return None
    value = result.get("backtest_semantics_version")
    if value is None:
        parameters = result.get("parameters")
        if isinstance(parameters, dict):
            value = parameters.get("backtest_semantics_version")
    return str(value) if value is not None else None


def _metric_pair_uses_current_semantics(
    train_metrics: dict[str, object] | None,
    validation_metrics: dict[str, object] | None,
) -> bool:
    if _metrics_semantics_version(train_metrics) != BACKTEST_SEMANTICS_VERSION:
        return False
    if validation_metrics is None:
        return True
    return _metrics_semantics_version(validation_metrics) == BACKTEST_SEMANTICS_VERSION


def _detail_cache_matches_metrics(
    *,
    train_result: dict[str, object],
    validation_result: dict[str, object] | None,
    train_metrics: dict[str, object] | None,
    validation_metrics: dict[str, object] | None,
) -> bool:
    expected_train_version = _metrics_semantics_version(train_metrics)
    if _result_semantics_version(train_result) != expected_train_version:
        return False
    if validation_metrics is None:
        return validation_result is None
    if validation_result is None:
        return False
    return _result_semantics_version(validation_result) == _metrics_semantics_version(
        validation_metrics
    )


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

    train_metrics = load_json(result.train_metrics_json, default=None)
    validation_metrics = load_json(result.validation_metrics_json, default=None)
    cached = _load_detail_cache(
        session,
        result_id=result.id,
        max_trades_returned=max_trades_returned,
    )
    if cached is not None and _detail_cache_matches_metrics(
        train_result=cached[0],
        validation_result=cached[1],
        train_metrics=train_metrics if isinstance(train_metrics, dict) else None,
        validation_metrics=validation_metrics if isinstance(validation_metrics, dict) else None,
    ):
        train_result, validation_result = cached
    else:
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
        metrics_use_current_semantics = _metric_pair_uses_current_semantics(
            train_metrics if isinstance(train_metrics, dict) else None,
            validation_metrics if isinstance(validation_metrics, dict) else None,
        )
        if not metrics_use_current_semantics:
            train_result = _result_from_metrics(
                train_metrics if isinstance(train_metrics, dict) else None,
                start_date=run.train_start_date,
                end_date=run.train_end_date,
                parameters=parameters,
                detail_unavailable_reason="legacy_semantics_not_recomputed",
            )
            validation_result = (
                _result_from_metrics(
                    validation_metrics if isinstance(validation_metrics, dict) else None,
                    start_date=run.validation_start_date,
                    end_date=run.validation_end_date,
                    parameters=parameters,
                    detail_unavailable_reason="legacy_semantics_not_recomputed",
                )
                if run.validation_start_date is not None and run.validation_end_date is not None
                else None
            )
        elif train_completed == 0 and validation_completed == 0:
            train_result = _result_from_metrics(
                train_metrics if isinstance(train_metrics, dict) else None,
                start_date=run.train_start_date,
                end_date=run.train_end_date,
                parameters=parameters,
                detail_unavailable_reason="no_completed_trades",
            )
            validation_result = (
                _result_from_metrics(
                    validation_metrics if isinstance(validation_metrics, dict) else None,
                    start_date=run.validation_start_date,
                    end_date=run.validation_end_date,
                    parameters=parameters,
                    detail_unavailable_reason="no_completed_trades",
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
            market_filter_cache: dict[tuple[object, ...], set[date]] = {}
            relative_strength_cache: dict[tuple[object, ...], dict[int, dict[str, object]]] = {}
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
                market_filter_cache=market_filter_cache,
                relative_strength_cache=relative_strength_cache,
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
                    market_filter_cache=market_filter_cache,
                    relative_strength_cache=relative_strength_cache,
                )
        if _metric_pair_uses_current_semantics(
            train_result if isinstance(train_result, dict) else None,
            validation_result if isinstance(validation_result, dict) else None,
        ):
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
    max_tasks_per_child = _configured_max_tasks_per_child_from_metadata(metadata)
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
    market_filter_cache: dict[tuple[object, ...], set[date]] = {}
    relative_strength_cache: dict[tuple[object, ...], dict[int, dict[str, object]]] = {}
    benchmark_cache: dict[tuple[object, ...], dict[str, object]] = {}

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
                    market_filter_cache=market_filter_cache,
                    relative_strength_cache=relative_strength_cache,
                    benchmark_cache=benchmark_cache,
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

        executor_kwargs: dict[str, object] = {"max_workers": max_workers}
        if max_tasks_per_child is not None:
            executor_kwargs["max_tasks_per_child"] = max_tasks_per_child
        with ProcessPoolExecutor(**executor_kwargs) as executor:
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
