from __future__ import annotations

import copy
import os
import resource
import random
import subprocess
import sys
import threading
import time
from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy import select

from stockanalyse_api.domain.backtests.models import GaEvent, GaIndividual, GaRun
from stockanalyse_api.services.optimization_backtest import evaluate_strategy_parameter_set
from stockanalyse_api.services.strategy_parameters import (
    DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS,
    STRATEGY_PARAMETER_SCHEMA_VERSION,
    dump_json,
    load_json,
    normalize_strategy_parameter_set,
    stable_parameter_hash,
)

DEFAULT_GA_OBJECTIVE = "spy_alpha"
DEFAULT_GA_POPULATION_SIZE = 12
DEFAULT_GA_MAX_GENERATIONS = 4
DEFAULT_GA_ELITE_COUNT = 2
DEFAULT_GA_MUTATION_RATE = Decimal("0.20")
DEFAULT_GA_STAGNATION_PATIENCE = 2
DEFAULT_GA_MAX_BROAD_CANDIDATE_CACHE_DATES = 260
DEFAULT_GA_DETAIL_TRADES_RETURNED = 300

DEFAULT_GA_GENE_SPACE: dict[str, list[object]] = {
    "rps_threshold": [70, 75, 80, 85],
    "selected_rps_windows": [[120, 250], [50, 120, 250]],
    "min_rps_windows_passing": [1, 2],
    "rps_exit_threshold": [None, 60, 65, 70, 75],
    "stop_loss_pct": ["-0.06", "-0.08", "-0.10"],
    "use_cup_handle": [False],
    "portfolio_cap": [10, 20],
    "position_weight_pct": ["0.05", "0.10"],
    "fundamental_growth_params": [DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS.to_dict()],
    "market_filter_params": [
        {"enabled": False},
        {
            "enabled": True,
            "symbol": "SPY",
            "require_price_above_sma": True,
            "price_sma_days": 200,
            "require_fast_sma_above_slow_sma": False,
            "fast_sma_days": 50,
            "slow_sma_days": 200,
        },
        {
            "enabled": True,
            "symbol": "SPY",
            "require_price_above_sma": True,
            "price_sma_days": 200,
            "require_fast_sma_above_slow_sma": True,
            "fast_sma_days": 50,
            "slow_sma_days": 200,
        },
    ],
    "relative_strength_params": [
        {"enabled": False},
        {
            "enabled": True,
            "symbol": "SPY",
            "lookback_days": 120,
            "min_excess_return_pct": "0",
        },
        {
            "enabled": True,
            "symbol": "SPY",
            "lookback_days": 250,
            "min_excess_return_pct": "0",
        },
    ],
}

DEFAULT_GA_INITIAL_POPULATION: list[dict[str, object]] = [
    {
        "candidate_name": "quality_light_no_valuation",
        "rps_threshold": 80,
        "selected_rps_windows": [120, 250],
        "min_rps_windows_passing": 1,
        "rps_exit_threshold": 75,
        "stop_loss_pct": "-0.08",
        "portfolio_cap": 20,
        "position_weight_pct": "0.05",
        "use_cup_handle": False,
        "fundamental_growth_params": DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS.to_dict(),
    },
    {
        "candidate_name": "growth_ocf",
        "rps_threshold": 80,
        "selected_rps_windows": [50, 120, 250],
        "min_rps_windows_passing": 2,
        "rps_exit_threshold": 75,
        "stop_loss_pct": "-0.08",
        "portfolio_cap": 20,
        "position_weight_pct": "0.05",
        "use_cup_handle": False,
        "fundamental_growth_params": DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS.to_dict(),
    },
    {
        "candidate_name": "value_quality_spy_strength",
        "rps_threshold": 70,
        "selected_rps_windows": [120, 250],
        "min_rps_windows_passing": 1,
        "rps_exit_threshold": 65,
        "stop_loss_pct": "-0.08",
        "portfolio_cap": 20,
        "position_weight_pct": "0.05",
        "use_cup_handle": False,
        "fundamental_growth_params": DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS.to_dict(),
        "market_filter_params": {
            "enabled": True,
            "symbol": "SPY",
            "require_price_above_sma": True,
            "price_sma_days": 200,
            "require_fast_sma_above_slow_sma": False,
            "fast_sma_days": 50,
            "slow_sma_days": 200,
        },
        "relative_strength_params": {
            "enabled": True,
            "symbol": "SPY",
            "lookback_days": 120,
            "min_excess_return_pct": "0",
        },
    },
]

DEFAULT_GA_FITNESS_CONFIG: dict[str, object] = {
    "elite_count": DEFAULT_GA_ELITE_COUNT,
    "mutation_rate": str(DEFAULT_GA_MUTATION_RATE),
    "stagnation_patience": DEFAULT_GA_STAGNATION_PATIENCE,
    "require_complete_benchmark": True,
    "require_all_windows": True,
    "reuse_parameter_window_evaluations": True,
    "prefer_broad_candidate_cache": True,
    "max_broad_candidate_cache_dates": DEFAULT_GA_MAX_BROAD_CANDIDATE_CACHE_DATES,
}

_ACTIVE_GA_RUN_IDS: set[int] = set()
_ACTIVE_GA_RUN_IDS_LOCK = threading.Lock()


def _as_list(value: object) -> list[object]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _normalize_gene_space(gene_space: dict[str, object] | None) -> dict[str, list[object]]:
    source = gene_space or DEFAULT_GA_GENE_SPACE
    normalized = {key: _as_list(value) for key, value in source.items()}
    if not normalized:
        raise ValueError("gene_space must not be empty.")
    empty_axes = [key for key, values in normalized.items() if not values]
    if empty_axes:
        raise ValueError("gene_space contains empty axes: " + ", ".join(sorted(empty_axes)))
    return normalized


def _candidate_from_payload(payload: dict[str, object]) -> dict[str, object]:
    clean_payload = {key: value for key, value in payload.items() if key != "candidate_name"}
    return normalize_strategy_parameter_set(clean_payload)


def _is_invalid_rps_exit_order_error(exc: ValueError) -> bool:
    return "rps_exit_threshold must be lower than rps_threshold" in str(exc)


def _random_candidate(gene_space: dict[str, list[object]], rng: random.Random) -> dict[str, object]:
    max_attempts = max(_finite_gene_space_size(gene_space) * 2, 100)
    attempts = 0
    while attempts < max_attempts:
        attempts += 1
        try:
            return _candidate_from_payload(
                {key: copy.deepcopy(rng.choice(values)) for key, values in gene_space.items()}
            )
        except ValueError as exc:
            if _is_invalid_rps_exit_order_error(exc):
                continue
            raise
    raise ValueError("gene_space cannot produce a valid RPS entry/exit candidate.")


def _maybe_mutate(
    parameters: dict[str, object],
    *,
    gene_space: dict[str, list[object]],
    rng: random.Random,
) -> tuple[dict[str, object], dict[str, object] | None]:
    try:
        return _mutate(parameters, gene_space=gene_space, rng=rng)
    except ValueError as exc:
        if _is_invalid_rps_exit_order_error(exc):
            return parameters, None
        raise


def _finite_gene_space_size(gene_space: dict[str, list[object]]) -> int:
    size = 1
    for values in gene_space.values():
        size *= len(values)
    return size


def _seed_population(
    *,
    gene_space: dict[str, list[object]],
    initial_population: list[dict[str, object]],
    population_size: int,
    rng: random.Random,
) -> list[dict[str, object]]:
    population: list[dict[str, object]] = []
    seen: set[str] = set()

    def append(parameters: dict[str, object]) -> None:
        parameter_hash = stable_parameter_hash(parameters)
        if parameter_hash in seen:
            return
        seen.add(parameter_hash)
        population.append(parameters)

    for payload in initial_population:
        append(_candidate_from_payload(payload))
        if len(population) >= population_size:
            return population

    max_attempts = max(population_size * 20, _finite_gene_space_size(gene_space) * 2)
    attempts = 0
    while len(population) < population_size and attempts < max_attempts:
        attempts += 1
        append(_random_candidate(gene_space, rng))
    if len(population) < population_size:
        raise ValueError("gene_space cannot produce enough unique candidates for population_size.")
    return population


def _score_value(value: object) -> Decimal:
    if value is None:
        return Decimal("-999999")
    return Decimal(str(value))


def _parse_date(value: object, *, field_name: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date.") from exc


def _optional_date(value: object, *, field_name: str) -> date | None:
    if value in {None, ""}:
        return None
    return _parse_date(value, field_name=field_name)


def _evaluation_windows_for_run(
    run: GaRun,
    fitness_config: dict[str, object],
) -> list[dict[str, object]]:
    raw_windows = fitness_config.get("evaluation_windows")
    if isinstance(raw_windows, list) and raw_windows:
        windows: list[dict[str, object]] = []
        for index, raw_window in enumerate(raw_windows, start=1):
            if not isinstance(raw_window, dict):
                raise ValueError("evaluation_windows entries must be dictionaries.")
            windows.append(
                {
                    "name": str(raw_window.get("name") or f"window_{index}"),
                    "train_start_date": _parse_date(
                        raw_window.get("train_start_date"),
                        field_name=f"evaluation_windows[{index}].train_start_date",
                    ),
                    "train_end_date": _parse_date(
                        raw_window.get("train_end_date"),
                        field_name=f"evaluation_windows[{index}].train_end_date",
                    ),
                    "validation_start_date": _optional_date(
                        raw_window.get("validation_start_date"),
                        field_name=f"evaluation_windows[{index}].validation_start_date",
                    ),
                    "validation_end_date": _optional_date(
                        raw_window.get("validation_end_date"),
                        field_name=f"evaluation_windows[{index}].validation_end_date",
                    ),
                }
            )
        return windows
    return [
        {
            "name": "primary",
            "train_start_date": run.train_start_date,
            "train_end_date": run.train_end_date,
            "validation_start_date": run.validation_start_date,
            "validation_end_date": run.validation_end_date,
        }
    ]


def _window_cache_key(
    *,
    run: GaRun,
    parameters: dict[str, object],
    window: dict[str, object],
    require_complete_benchmark: bool,
) -> tuple[object, ...]:
    return (
        "ga_window_evaluation",
        run.market,
        run.objective,
        stable_parameter_hash(parameters),
        window["train_start_date"].isoformat(),
        window["train_end_date"].isoformat(),
        (
            window["validation_start_date"].isoformat()
            if window["validation_start_date"] is not None
            else None
        ),
        (
            window["validation_end_date"].isoformat()
            if window["validation_end_date"] is not None
            else None
        ),
        require_complete_benchmark,
    )


def _new_ga_cache_stats() -> dict[str, object]:
    return {
        "window_cache_hits": 0,
        "window_cache_misses": 0,
        "window_evaluation_seconds": 0.0,
        "individual_seconds": [],
    }


def _increment_cache_stat(stats: dict[str, object], key: str, amount: int = 1) -> None:
    stats[key] = int(stats.get(key, 0) or 0) + amount


def _add_seconds_stat(stats: dict[str, object], key: str, seconds: float) -> None:
    stats[key] = float(stats.get(key, 0.0) or 0.0) + seconds


def _record_individual_seconds(stats: dict[str, object], seconds: float) -> None:
    values = stats.setdefault("individual_seconds", [])
    if isinstance(values, list):
        values.append(seconds)


def _cache_stats_baseline(stats: dict[str, object]) -> dict[str, object]:
    individual_seconds = stats.get("individual_seconds")
    if not isinstance(individual_seconds, list):
        individual_seconds = []
    return {
        "window_cache_hits": int(stats.get("window_cache_hits", 0) or 0),
        "window_cache_misses": int(stats.get("window_cache_misses", 0) or 0),
        "window_evaluation_seconds": float(stats.get("window_evaluation_seconds", 0.0) or 0.0),
        "individual_count": len(individual_seconds),
        "individual_seconds_total": sum(float(value) for value in individual_seconds),
    }


def _cache_stats_delta(
    stats: dict[str, object],
    baseline: dict[str, object],
) -> dict[str, object]:
    individual_seconds = stats.get("individual_seconds")
    if not isinstance(individual_seconds, list):
        individual_seconds = []
    individual_count = len(individual_seconds) - int(baseline.get("individual_count", 0) or 0)
    individual_seconds_total = sum(float(value) for value in individual_seconds) - float(
        baseline.get("individual_seconds_total", 0.0) or 0.0
    )
    return {
        "window_cache_hits": int(stats.get("window_cache_hits", 0) or 0)
        - int(baseline.get("window_cache_hits", 0) or 0),
        "window_cache_misses": int(stats.get("window_cache_misses", 0) or 0)
        - int(baseline.get("window_cache_misses", 0) or 0),
        "window_evaluation_seconds": _format_seconds(
            float(stats.get("window_evaluation_seconds", 0.0) or 0.0)
            - float(baseline.get("window_evaluation_seconds", 0.0) or 0.0)
        ),
        "average_individual_seconds": _format_seconds(
            individual_seconds_total / individual_count if individual_count else 0.0
        ),
    }


def _format_seconds(seconds: float) -> str:
    return f"{seconds:.3f}"


def _max_rss_snapshot() -> int | None:
    try:
        return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except (OSError, AttributeError):
        return None


def _compact_ga_metrics(metrics: object) -> object:
    if not isinstance(metrics, dict):
        return metrics
    return {
        key: copy.deepcopy(value)
        for key, value in metrics.items()
        if key != "equity_curve"
    }


def _compact_ga_result_detail(result: object) -> dict[str, object] | None:
    if not isinstance(result, dict):
        return None
    summary_keys = (
        "start_date",
        "end_date",
        "initial_capital",
        "final_capital",
        "total_profit",
        "total_return",
        "annualized_return",
        "average_annualized_return",
        "completed_trades",
        "selected_trades",
        "win_rate",
        "max_drawdown",
        "position_size_amount",
        "benchmark_status",
        "spy_excess_annualized_return",
        "spy_excess_total_return",
        "detail_source",
        "detail_unavailable_reason",
    )
    compacted = {
        key: copy.deepcopy(result[key])
        for key in summary_keys
        if key in result
    }
    trades = result.get("trades")
    if isinstance(trades, list):
        compacted["trades"] = copy.deepcopy(trades[:DEFAULT_GA_DETAIL_TRADES_RETURNED])
        compacted["trades_returned"] = min(len(trades), DEFAULT_GA_DETAIL_TRADES_RETURNED)
        compacted["total_trades_available"] = len(trades)
    else:
        compacted["trades"] = []
        compacted["trades_returned"] = 0
        compacted["total_trades_available"] = 0
    return compacted


def _compact_ga_window_summary(summary: dict[str, object]) -> dict[str, object]:
    compacted = dict(summary)
    compacted["train_metrics"] = _compact_ga_metrics(compacted.get("train_metrics"))
    compacted["validation_metrics"] = _compact_ga_metrics(compacted.get("validation_metrics"))
    compacted["train_result"] = _compact_ga_result_detail(compacted.get("train_result"))
    compacted["validation_result"] = _compact_ga_result_detail(compacted.get("validation_result"))
    return compacted


def _compact_ga_evaluation(evaluation: dict[str, object]) -> dict[str, object]:
    compacted: dict[str, object] = {
        "parameters": copy.deepcopy(evaluation.get("parameters")),
        "train_metrics": _compact_ga_metrics(evaluation.get("train_metrics")),
        "validation_metrics": _compact_ga_metrics(evaluation.get("validation_metrics")),
        "train_result": _compact_ga_result_detail(evaluation.get("train_result")),
        "validation_result": _compact_ga_result_detail(evaluation.get("validation_result")),
        "aggregate_metrics": copy.deepcopy(evaluation.get("aggregate_metrics")),
        "score": evaluation.get("score"),
        "status": evaluation.get("status"),
        "failure_reason": evaluation.get("failure_reason"),
    }
    windows = evaluation.get("window_evaluations")
    if isinstance(windows, list):
        compacted["window_evaluations"] = [
            _compact_ga_window_summary(window)
            if isinstance(window, dict)
            else copy.deepcopy(window)
            for window in windows
        ]
    return compacted


def _metric_decimal(metrics: dict[str, object] | None, key: str, default: str = "0") -> Decimal:
    if metrics is None:
        return Decimal(default)
    value = metrics.get(key)
    if value is None:
        return Decimal(default)
    return Decimal(str(value))


def _metric_int(metrics: dict[str, object] | None, key: str) -> int:
    if metrics is None:
        return 0
    return int(metrics.get(key, 0) or 0)


def _primary_window_metrics(evaluation: dict[str, object]) -> dict[str, object] | None:
    validation_metrics = evaluation.get("validation_metrics")
    if isinstance(validation_metrics, dict):
        return validation_metrics
    train_metrics = evaluation.get("train_metrics")
    if isinstance(train_metrics, dict):
        return train_metrics
    return None


def _window_sample_penalty(min_completed_trades: int) -> Decimal:
    if min_completed_trades < 5:
        return Decimal("0.250000")
    if min_completed_trades < 10:
        return Decimal("0.120000")
    if min_completed_trades < 20:
        return Decimal("0.050000")
    return Decimal("0")


def _yearly_stability_penalty(metrics_by_window: list[dict[str, object] | None]) -> Decimal:
    penalty = Decimal("0")
    for metrics in metrics_by_window:
        if not isinstance(metrics, dict):
            penalty += Decimal("0.030000")
            continue
        yearly_returns = metrics.get("yearly_returns")
        if not isinstance(yearly_returns, dict) or len(yearly_returns) < 2:
            continue
        absolute_returns = [abs(Decimal(str(value))) for value in yearly_returns.values()]
        total = sum(absolute_returns, Decimal("0"))
        if total > 0 and max(absolute_returns) / total > Decimal("0.75"):
            penalty += Decimal("0.050000")
    return penalty


def _aggregate_window_evaluations(
    *,
    parameters: dict[str, object],
    windows: list[dict[str, object]],
    evaluations: list[dict[str, object]],
    require_all_windows: bool,
) -> dict[str, object]:
    completed_scores: list[Decimal] = []
    metrics_by_window: list[dict[str, object] | None] = []
    window_summaries: list[dict[str, object]] = []
    failed_reasons: list[str] = []
    completed_trade_counts: list[int] = []
    drawdowns: list[Decimal] = []

    for window, evaluation in zip(windows, evaluations, strict=True):
        score = evaluation.get("score")
        status = str(evaluation.get("status") or "failed")
        metrics = _primary_window_metrics(evaluation)
        metrics_by_window.append(metrics)
        completed_trades = _metric_int(metrics, "completed_trades")
        completed_trade_counts.append(completed_trades)
        drawdowns.append(abs(_metric_decimal(metrics, "max_drawdown", default="0")))
        if status == "completed" and score is not None:
            completed_scores.append(_score_value(score))
        else:
            failed_reasons.append(
                f"{window['name']}: {evaluation.get('failure_reason') or 'missing score'}"
            )
        window_summaries.append(
            {
                "name": window["name"],
                "train_start_date": window["train_start_date"].isoformat(),
                "train_end_date": window["train_end_date"].isoformat(),
                "validation_start_date": (
                    window["validation_start_date"].isoformat()
                    if window["validation_start_date"] is not None
                    else None
                ),
                "validation_end_date": (
                    window["validation_end_date"].isoformat()
                    if window["validation_end_date"] is not None
                    else None
                ),
                "status": status,
                "score": str(score) if score is not None else None,
                "completed_trades": completed_trades,
                "failure_reason": evaluation.get("failure_reason"),
                "train_metrics": _compact_ga_metrics(evaluation.get("train_metrics")),
                "validation_metrics": _compact_ga_metrics(evaluation.get("validation_metrics")),
                "train_result": _compact_ga_result_detail(evaluation.get("train_result")),
                "validation_result": _compact_ga_result_detail(evaluation.get("validation_result")),
            }
        )

    completed_window_count = len(completed_scores)
    if not completed_scores or (require_all_windows and completed_window_count != len(windows)):
        return {
            "parameters": parameters,
            "train_metrics": None,
            "validation_metrics": None,
            "train_result": None,
            "validation_result": None,
            "window_evaluations": window_summaries,
            "aggregate_metrics": {
                "window_count": len(windows),
                "completed_window_count": completed_window_count,
                "failed_window_count": len(windows) - completed_window_count,
            },
            "score": None,
            "status": "failed",
            "failure_reason": "; ".join(failed_reasons) or "No completed evaluation windows.",
        }

    average_score = sum(completed_scores, Decimal("0")) / Decimal(len(completed_scores))
    minimum_score = min(completed_scores)
    maximum_score = max(completed_scores)
    score_dispersion = maximum_score - minimum_score
    min_completed_trades = min(completed_trade_counts) if completed_trade_counts else 0
    avg_completed_trades = (
        sum(completed_trade_counts, 0) / len(completed_trade_counts)
        if completed_trade_counts
        else 0
    )
    sample_penalty = _window_sample_penalty(min_completed_trades)
    max_drawdown = max(drawdowns) if drawdowns else Decimal("0")
    drawdown_penalty = max_drawdown * Decimal("0.050000")
    yearly_penalty = _yearly_stability_penalty(metrics_by_window)
    aggregate_score = (
        average_score * Decimal("0.550000")
        + minimum_score * Decimal("0.350000")
        - score_dispersion * Decimal("0.100000")
        - sample_penalty
        - drawdown_penalty
        - yearly_penalty
    ).quantize(Decimal("0.000001"))
    aggregate_metrics = {
        "window_count": len(windows),
        "completed_window_count": completed_window_count,
        "failed_window_count": len(windows) - completed_window_count,
        "average_window_score": f"{average_score:.6f}",
        "minimum_window_score": f"{minimum_score:.6f}",
        "maximum_window_score": f"{maximum_score:.6f}",
        "score_dispersion": f"{score_dispersion:.6f}",
        "minimum_completed_trades": min_completed_trades,
        "average_completed_trades": f"{avg_completed_trades:.2f}",
        "max_drawdown": f"{max_drawdown:.6f}",
        "sample_penalty": f"{sample_penalty:.6f}",
        "drawdown_penalty": f"{drawdown_penalty:.6f}",
        "yearly_stability_penalty": f"{yearly_penalty:.6f}",
    }
    return {
        "parameters": parameters,
        "train_metrics": aggregate_metrics,
        "validation_metrics": None,
        "train_result": None,
        "validation_result": None,
        "window_evaluations": window_summaries,
        "aggregate_metrics": aggregate_metrics,
        "score": aggregate_score,
        "status": "completed",
        "failure_reason": None,
    }


def _ranked_individuals(individuals: list[GaIndividual]) -> list[GaIndividual]:
    return sorted(individuals, key=lambda item: (_score_value(item.fitness), item.id), reverse=True)


def _cross_over(
    parent_a: dict[str, object],
    parent_b: dict[str, object],
    *,
    rng: random.Random,
) -> dict[str, object]:
    child: dict[str, object] = {}
    for key in sorted(set(parent_a) | set(parent_b)):
        if key == "strategy_schema_version":
            continue
        source = parent_a if rng.random() < 0.5 else parent_b
        fallback = parent_b if source is parent_a else parent_a
        child[key] = copy.deepcopy(source.get(key, fallback.get(key)))
    return _candidate_from_payload(child)


def _mutate(
    parameters: dict[str, object],
    *,
    gene_space: dict[str, list[object]],
    rng: random.Random,
) -> tuple[dict[str, object], dict[str, object] | None]:
    if not gene_space:
        return parameters, None
    key = rng.choice(list(gene_space))
    value = copy.deepcopy(rng.choice(gene_space[key]))
    mutated = dict(parameters)
    mutated[key] = value
    return _candidate_from_payload(mutated), {"field": key, "value": value}


def _next_generation(
    *,
    current_individuals: list[GaIndividual],
    gene_space: dict[str, list[object]],
    population_size: int,
    elite_count: int,
    mutation_rate: Decimal,
    rng: random.Random,
) -> list[dict[str, object]]:
    ranked = _ranked_individuals(current_individuals)
    elites = ranked[: max(1, min(elite_count, len(ranked)))]
    next_population: list[dict[str, object]] = []
    seen: set[str] = set()

    def append(
        parameters: dict[str, object],
        *,
        parent_a_id: int | None,
        parent_b_id: int | None,
        mutation: dict[str, object] | None,
    ) -> None:
        parameter_hash = stable_parameter_hash(parameters)
        if parameter_hash in seen:
            return
        seen.add(parameter_hash)
        next_population.append(
            {
                "parameters": parameters,
                "parent_a_id": parent_a_id,
                "parent_b_id": parent_b_id,
                "mutation": mutation,
            }
        )

    for elite in elites:
        append(
            load_json(elite.parameters_json, default={}),
            parent_a_id=elite.id,
            parent_b_id=None,
            mutation=None,
        )

    attempts = 0
    max_attempts = max(population_size * 20, _finite_gene_space_size(gene_space) * 2)
    while len(next_population) < population_size and attempts < max_attempts:
        attempts += 1
        parent_a = rng.choice(elites)
        parent_b = rng.choice(elites)
        try:
            child = _cross_over(
                load_json(parent_a.parameters_json, default={}),
                load_json(parent_b.parameters_json, default={}),
                rng=rng,
            )
        except ValueError as exc:
            if _is_invalid_rps_exit_order_error(exc):
                continue
            raise
        mutation = None
        if rng.random() < float(mutation_rate):
            child, mutation = _maybe_mutate(child, gene_space=gene_space, rng=rng)
        append(child, parent_a_id=parent_a.id, parent_b_id=parent_b.id, mutation=mutation)
        if len(seen) >= _finite_gene_space_size(gene_space):
            break
    attempts = 0
    while len(next_population) < population_size and attempts < max_attempts:
        attempts += 1
        child = _random_candidate(gene_space, rng)
        append(child, parent_a_id=None, parent_b_id=None, mutation={"source": "random_fill"})
    if len(next_population) < population_size:
        raise ValueError("gene_space cannot produce enough unique candidates for next generation.")
    return next_population


def create_ga_run(
    session,
    *,
    market: str = "us",
    train_start_date: date,
    train_end_date: date,
    validation_start_date: date | None = None,
    validation_end_date: date | None = None,
    holdout_start_date: date | None = None,
    holdout_end_date: date | None = None,
    objective: str = DEFAULT_GA_OBJECTIVE,
    gene_space: dict[str, object] | None = None,
    fitness_config: dict[str, object] | None = None,
    initial_population: list[dict[str, object]] | None = None,
    population_size: int = DEFAULT_GA_POPULATION_SIZE,
    max_generations: int = DEFAULT_GA_MAX_GENERATIONS,
    random_seed: int | None = None,
) -> GaRun:
    if population_size < 2:
        raise ValueError("population_size must be at least 2.")
    if max_generations < 1:
        raise ValueError("max_generations must be at least 1.")
    normalized_gene_space = _normalize_gene_space(gene_space)
    resolved_fitness_config = {**DEFAULT_GA_FITNESS_CONFIG, **(fitness_config or {})}
    resolved_initial_population = initial_population or DEFAULT_GA_INITIAL_POPULATION
    resolved_random_seed = (
        random_seed
        if random_seed is not None
        else random.SystemRandom().randrange(0, 2_147_483_647)
    )
    run = GaRun(
        market=market,
        train_start_date=train_start_date,
        train_end_date=train_end_date,
        validation_start_date=validation_start_date,
        validation_end_date=validation_end_date,
        holdout_start_date=holdout_start_date,
        holdout_end_date=holdout_end_date,
        objective=objective,
        strategy_schema_version=STRATEGY_PARAMETER_SCHEMA_VERSION,
        population_size=population_size,
        max_generations=max_generations,
        random_seed=resolved_random_seed,
        gene_space_json=dump_json(normalized_gene_space),
        fitness_config_json=dump_json(resolved_fitness_config),
        initial_population_json=dump_json(resolved_initial_population),
        total_generations=max_generations,
        total_individuals=population_size * max_generations,
        started_at=datetime.now(UTC),
    )
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def get_ga_run(session, run_id: int) -> GaRun | None:
    return session.get(GaRun, run_id)


def get_ga_individual(session, individual_id: int) -> GaIndividual | None:
    return session.get(GaIndividual, individual_id)


def list_ga_runs(session, *, market: str = "us", limit: int = 50) -> list[GaRun]:
    return list(
        session.execute(
            select(GaRun)
            .where(GaRun.market == market)
            .order_by(GaRun.started_at.desc(), GaRun.id.desc())
            .limit(limit)
        ).scalars()
    )


def list_ga_individuals(
    session,
    *,
    run_id: int,
    limit: int = 100,
    offset: int = 0,
) -> list[GaIndividual]:
    return list(
        session.execute(
            select(GaIndividual)
            .where(GaIndividual.ga_run_id == run_id)
            .order_by(
                GaIndividual.generation.asc(),
                GaIndividual.individual_index.asc(),
                GaIndividual.id.asc(),
            )
            .limit(limit)
            .offset(offset)
        ).scalars()
    )


def list_ga_events(
    session,
    *,
    run_id: int,
    limit: int = 200,
    offset: int = 0,
) -> list[GaEvent]:
    return list(
        session.execute(
            select(GaEvent)
            .where(GaEvent.ga_run_id == run_id)
            .order_by(GaEvent.id.asc())
            .limit(limit)
            .offset(offset)
        ).scalars()
    )


def serialize_ga_run(run: GaRun) -> dict[str, object]:
    return {
        "id": run.id,
        "market": run.market,
        "train_start_date": run.train_start_date.isoformat(),
        "train_end_date": run.train_end_date.isoformat(),
        "validation_start_date": run.validation_start_date.isoformat() if run.validation_start_date else None,
        "validation_end_date": run.validation_end_date.isoformat() if run.validation_end_date else None,
        "holdout_start_date": run.holdout_start_date.isoformat() if run.holdout_start_date else None,
        "holdout_end_date": run.holdout_end_date.isoformat() if run.holdout_end_date else None,
        "objective": run.objective,
        "strategy_schema_version": run.strategy_schema_version,
        "population_size": run.population_size,
        "max_generations": run.max_generations,
        "random_seed": run.random_seed,
        "status": run.status,
        "total_generations": run.total_generations,
        "completed_generations": run.completed_generations,
        "total_individuals": run.total_individuals,
        "completed_individuals": run.completed_individuals,
        "failed_individuals": run.failed_individuals,
        "best_individual_id": run.best_individual_id,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "error_message": run.error_message,
    }


def serialize_ga_individual(
    individual: GaIndividual,
    *,
    include_evaluation: bool = False,
) -> dict[str, object]:
    payload = {
        "id": individual.id,
        "ga_run_id": individual.ga_run_id,
        "generation": individual.generation,
        "individual_index": individual.individual_index,
        "parameter_hash": individual.parameter_hash,
        "parameters": load_json(individual.parameters_json, default={}),
        "fitness": f"{individual.fitness:.6f}" if individual.fitness is not None else None,
        "metrics": load_json(individual.metrics_json, default=None),
        "source_optimization_result_id": individual.source_optimization_result_id,
        "parent_a_id": individual.parent_a_id,
        "parent_b_id": individual.parent_b_id,
        "mutation": load_json(individual.mutation_json, default=None),
        "status": individual.status,
        "failure_reason": individual.failure_reason,
        "completed_at": individual.completed_at.isoformat() if individual.completed_at else None,
        "created_at": individual.created_at.isoformat() if individual.created_at else None,
        "updated_at": individual.updated_at.isoformat() if individual.updated_at else None,
    }
    if include_evaluation:
        payload["evaluation"] = load_json(individual.evaluation_json, default=None)
    return payload


def serialize_ga_event(event: GaEvent) -> dict[str, object]:
    return {
        "id": event.id,
        "ga_run_id": event.ga_run_id,
        "generation": event.generation,
        "event_type": event.event_type,
        "event": load_json(event.event_json, default={}),
        "created_at": event.created_at.isoformat() if event.created_at else None,
    }


def _log_ga_event(
    session,
    *,
    run_id: int,
    generation: int | None,
    event_type: str,
    event: dict[str, object],
) -> None:
    session.add(
        GaEvent(
            ga_run_id=run_id,
            generation=generation,
            event_type=event_type,
            event_json=dump_json(event),
        )
    )


def _persist_individual(
    session,
    *,
    run: GaRun,
    generation: int,
    individual_index: int,
    candidate: dict[str, object],
    evaluation: dict[str, object],
) -> GaIndividual:
    parameters = candidate["parameters"]
    if not isinstance(parameters, dict):
        raise ValueError("GA candidate parameters must be a dictionary.")
    compact_evaluation = _compact_ga_evaluation(evaluation)
    status = str(evaluation.get("status") or "failed")
    score = evaluation.get("score")
    individual = GaIndividual(
        ga_run_id=run.id,
        generation=generation,
        individual_index=individual_index,
        parameter_hash=stable_parameter_hash(parameters),
        parameters_json=dump_json(parameters),
        fitness=_score_value(score) if score is not None else None,
        metrics_json=dump_json(
            {
                "train": compact_evaluation.get("train_metrics"),
                "validation": compact_evaluation.get("validation_metrics"),
                "aggregate": compact_evaluation.get("aggregate_metrics"),
                "windows": compact_evaluation.get("window_evaluations"),
            }
        ),
        evaluation_json=dump_json(compact_evaluation),
        parent_a_id=candidate.get("parent_a_id"),  # type: ignore[arg-type]
        parent_b_id=candidate.get("parent_b_id"),  # type: ignore[arg-type]
        mutation_json=dump_json(candidate.get("mutation")) if candidate.get("mutation") is not None else None,
        status=status,
        failure_reason=(
            str(evaluation["failure_reason"])
            if evaluation.get("failure_reason") is not None
            else None
        ),
        completed_at=datetime.now(UTC),
    )
    session.add(individual)
    session.flush()
    return individual


def _evaluate_candidate(
    session,
    *,
    run: GaRun,
    parameters: dict[str, object],
    evaluation_windows: list[dict[str, object]],
    require_complete_benchmark: bool,
    require_all_windows: bool,
    reuse_parameter_window_evaluations: bool,
    prefer_broad_candidate_cache: bool,
    max_broad_candidate_cache_dates: int | None,
    evaluation_cache: dict[tuple[object, ...], dict[str, object]],
    cache_stats: dict[str, object],
    screen_cache: dict[str, dict[str, object]],
    screen_candidate_cache: dict[tuple[object, ...], dict[str, object]],
    fundamental_growth_cache: dict[tuple[object, ...], object],
    cup_event_cache: dict[tuple[object, ...], dict[int, list[object]] | None],
    trade_cache: dict[tuple[object, ...], object],
    trade_dates_cache: dict[tuple[object, ...], list[date]],
    future_rows_cache: dict[tuple[object, ...], object],
    future_indicator_cache: dict[tuple[object, ...], object],
    market_filter_cache: dict[tuple[object, ...], set[date]],
    relative_strength_cache: dict[tuple[object, ...], dict[int, dict[str, object]]],
    benchmark_cache: dict[tuple[object, ...], dict[str, object]],
) -> dict[str, object]:
    candidate_started_at = time.perf_counter()
    evaluations: list[dict[str, object]] = []
    for window in evaluation_windows:
        cache_key = _window_cache_key(
            run=run,
            parameters=parameters,
            window=window,
            require_complete_benchmark=require_complete_benchmark,
        )
        cached = evaluation_cache.get(cache_key) if reuse_parameter_window_evaluations else None
        if cached is not None:
            _increment_cache_stat(cache_stats, "window_cache_hits")
            evaluations.append(copy.deepcopy(cached))
            continue
        _increment_cache_stat(cache_stats, "window_cache_misses")
        window_started_at = time.perf_counter()
        evaluation = evaluate_strategy_parameter_set(
            session,
            start_date=window["train_start_date"],
            end_date=window["train_end_date"],
            validation_start_date=window["validation_start_date"],
            validation_end_date=window["validation_end_date"],
            market=run.market,
            objective=run.objective,
            parameters=parameters,
            require_complete_benchmark=require_complete_benchmark,
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
        )
        _add_seconds_stat(cache_stats, "window_evaluation_seconds", time.perf_counter() - window_started_at)
        compact_evaluation = _compact_ga_evaluation(evaluation)
        if reuse_parameter_window_evaluations:
            evaluation_cache[cache_key] = copy.deepcopy(compact_evaluation)
        evaluations.append(compact_evaluation)
    _record_individual_seconds(cache_stats, time.perf_counter() - candidate_started_at)
    if len(evaluation_windows) == 1:
        evaluation = evaluations[0]
        evaluation["window_evaluations"] = [
            {
                "name": evaluation_windows[0]["name"],
                "train_start_date": evaluation_windows[0]["train_start_date"].isoformat(),
                "train_end_date": evaluation_windows[0]["train_end_date"].isoformat(),
                "validation_start_date": (
                    evaluation_windows[0]["validation_start_date"].isoformat()
                    if evaluation_windows[0]["validation_start_date"] is not None
                    else None
                ),
                "validation_end_date": (
                    evaluation_windows[0]["validation_end_date"].isoformat()
                    if evaluation_windows[0]["validation_end_date"] is not None
                    else None
                ),
                "status": evaluation.get("status"),
                "score": str(evaluation.get("score")) if evaluation.get("score") is not None else None,
                "failure_reason": evaluation.get("failure_reason"),
            }
        ]
        return evaluation
    return _aggregate_window_evaluations(
        parameters=parameters,
        windows=evaluation_windows,
        evaluations=evaluations,
        require_all_windows=require_all_windows,
    )


def _generation_parameter_counts(individuals: list[GaIndividual]) -> dict[str, dict[str, int]]:
    tracked_fields = (
        "rps_threshold",
        "selected_rps_windows",
        "min_rps_windows_passing",
        "rps_exit_threshold",
        "stop_loss_pct",
        "portfolio_cap",
        "use_cup_handle",
    )
    counts: dict[str, dict[str, int]] = {field: {} for field in tracked_fields}
    for individual in individuals:
        parameters = load_json(individual.parameters_json, default={})
        if not isinstance(parameters, dict):
            continue
        for field in tracked_fields:
            key = dump_json(parameters.get(field))
            counts[field][key] = counts[field].get(key, 0) + 1
    return counts


def _generation_summary_event(
    *,
    generation: int,
    individuals: list[GaIndividual],
    generation_best: GaIndividual | None,
    stale_generations: int,
    evaluation_windows: list[dict[str, object]],
    performance: dict[str, object],
) -> dict[str, object]:
    fitness_values = [
        _score_value(individual.fitness)
        for individual in individuals
        if individual.fitness is not None
    ]
    average_fitness = (
        sum(fitness_values, Decimal("0")) / Decimal(len(fitness_values))
        if fitness_values
        else None
    )
    failed_count = sum(1 for individual in individuals if individual.status != "completed")
    return {
        "generation": generation,
        "evaluated": len(individuals),
        "evaluation_window_count": len(evaluation_windows),
        "best_individual_id": generation_best.id if generation_best else None,
        "best_fitness": str(generation_best.fitness) if generation_best else None,
        "average_fitness": f"{average_fitness:.6f}" if average_fitness is not None else None,
        "unique_parameter_hashes": len({individual.parameter_hash for individual in individuals}),
        "failed_count": failed_count,
        "failure_rate": f"{(Decimal(failed_count) / Decimal(len(individuals))):.6f}"
        if individuals
        else None,
        "parameter_value_counts": _generation_parameter_counts(individuals),
        "performance": performance,
        "stale_generations": stale_generations,
    }


def _candidate_parameter_hash(candidate: dict[str, object]) -> str:
    parameters = candidate.get("parameters")
    if not isinstance(parameters, dict):
        raise ValueError("GA candidate parameters must be a dictionary.")
    return stable_parameter_hash(parameters)


def _existing_individuals_by_generation(
    session,
    *,
    run: GaRun,
) -> dict[int, dict[int, GaIndividual]]:
    by_generation: dict[int, dict[int, GaIndividual]] = {}
    individuals = (
        session.execute(
            select(GaIndividual)
            .where(GaIndividual.ga_run_id == run.id)
            .order_by(
                GaIndividual.generation.asc(),
                GaIndividual.individual_index.asc(),
                GaIndividual.id.asc(),
            )
        )
        .scalars()
        .all()
    )
    for individual in individuals:
        if individual.generation < 0 or individual.generation >= run.max_generations:
            raise ValueError(
                f"GA run #{run.id} has persisted individual outside generation range: "
                f"generation={individual.generation}."
            )
        if individual.individual_index < 0 or individual.individual_index >= run.population_size:
            raise ValueError(
                f"GA run #{run.id} has persisted individual outside population range: "
                f"generation={individual.generation}, index={individual.individual_index}."
            )
        generation = by_generation.setdefault(individual.generation, {})
        if individual.individual_index in generation:
            raise ValueError(
                f"GA run #{run.id} has duplicate persisted individuals for "
                f"generation={individual.generation}, index={individual.individual_index}."
            )
        generation[individual.individual_index] = individual
    return by_generation


def _generation_summary_generations(session, *, run_id: int) -> set[int]:
    rows = (
        session.execute(
            select(GaEvent.generation)
            .where(GaEvent.ga_run_id == run_id)
            .where(GaEvent.event_type == "generation_summary")
            .where(GaEvent.generation.is_not(None))
        )
        .scalars()
        .all()
    )
    return {int(generation) for generation in rows if generation is not None}


def _sync_ga_run_progress_from_existing(
    run: GaRun,
    existing_by_generation: dict[int, dict[int, GaIndividual]],
) -> None:
    individuals = [
        individual
        for generation in existing_by_generation.values()
        for individual in generation.values()
    ]
    run.completed_individuals = sum(
        1 for individual in individuals if individual.status == "completed"
    )
    run.failed_individuals = sum(1 for individual in individuals if individual.status != "completed")
    completed_generations = 0
    for generation in range(run.max_generations):
        if len(existing_by_generation.get(generation, {})) < run.population_size:
            break
        completed_generations = generation + 1
    run.completed_generations = completed_generations


def _candidate_matches_existing(
    *,
    candidate: dict[str, object],
    existing: GaIndividual,
) -> tuple[bool, str]:
    expected_hash = _candidate_parameter_hash(candidate)
    return existing.parameter_hash == expected_hash, expected_hash


def _resume_random_fill_candidate(
    *,
    gene_space: dict[str, list[object]],
    seen_hashes: set[str],
    rng: random.Random,
) -> dict[str, object]:
    max_attempts = max(len(seen_hashes) * 20, _finite_gene_space_size(gene_space) * 2)
    attempts = 0
    while attempts < max_attempts:
        attempts += 1
        parameters = _random_candidate(gene_space, rng)
        parameter_hash = stable_parameter_hash(parameters)
        if parameter_hash not in seen_hashes:
            return {
                "parameters": parameters,
                "parent_a_id": None,
                "parent_b_id": None,
                "mutation": {"source": "resume_random_fill"},
            }
    raise ValueError("gene_space cannot produce enough unique candidates to resume generation.")


def _evaluate_holdout(
    session,
    *,
    run: GaRun,
    best_individual: GaIndividual | None,
    require_complete_benchmark: bool,
    prefer_broad_candidate_cache: bool,
    max_broad_candidate_cache_dates: int | None,
    screen_cache: dict[str, dict[str, object]],
    screen_candidate_cache: dict[tuple[object, ...], dict[str, object]],
    fundamental_growth_cache: dict[tuple[object, ...], object],
    cup_event_cache: dict[tuple[object, ...], dict[int, list[object]] | None],
    trade_cache: dict[tuple[object, ...], object],
    trade_dates_cache: dict[tuple[object, ...], list[date]],
    future_rows_cache: dict[tuple[object, ...], object],
    future_indicator_cache: dict[tuple[object, ...], object],
    market_filter_cache: dict[tuple[object, ...], set[date]],
    relative_strength_cache: dict[tuple[object, ...], dict[int, dict[str, object]]],
    benchmark_cache: dict[tuple[object, ...], dict[str, object]],
) -> None:
    if run.holdout_start_date is None or run.holdout_end_date is None or best_individual is None:
        return
    parameters = load_json(best_individual.parameters_json, default={})
    if not isinstance(parameters, dict):
        return
    holdout_evaluation = evaluate_strategy_parameter_set(
        session,
        start_date=run.holdout_start_date,
        end_date=run.holdout_end_date,
        validation_start_date=None,
        validation_end_date=None,
        market=run.market,
        objective=run.objective,
        parameters=parameters,
        require_complete_benchmark=require_complete_benchmark,
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
    )
    compact_holdout_evaluation = _compact_ga_evaluation(holdout_evaluation)
    existing_evaluation = load_json(best_individual.evaluation_json, default={})
    if not isinstance(existing_evaluation, dict):
        existing_evaluation = {}
    existing_evaluation["holdout_evaluation"] = compact_holdout_evaluation
    best_individual.evaluation_json = dump_json(existing_evaluation)
    _log_ga_event(
        session,
        run_id=run.id,
        generation=None,
        event_type="holdout_evaluation",
        event={
            "best_individual_id": best_individual.id,
            "holdout_start_date": run.holdout_start_date.isoformat(),
            "holdout_end_date": run.holdout_end_date.isoformat(),
            "status": compact_holdout_evaluation.get("status"),
            "score": (
                str(compact_holdout_evaluation.get("score"))
                if compact_holdout_evaluation.get("score") is not None
                else None
            ),
            "failure_reason": compact_holdout_evaluation.get("failure_reason"),
        },
    )


def execute_ga_run(session, run_id: int) -> GaRun:
    run = session.get(GaRun, run_id)
    if run is None:
        raise LookupError("GA run not found.")
    if run.status not in {"running", "cancel_requested"}:
        return run

    existing_by_generation = _existing_individuals_by_generation(session, run=run)
    existing_count = sum(len(generation) for generation in existing_by_generation.values())
    if existing_count and run.random_seed is None:
        raise ValueError("GA run cannot resume safely without a persisted random_seed.")

    rng = random.Random(run.random_seed)
    gene_space = _normalize_gene_space(load_json(run.gene_space_json, default={}))
    fitness_config = {**DEFAULT_GA_FITNESS_CONFIG, **load_json(run.fitness_config_json, default={})}
    elite_count = int(fitness_config.get("elite_count", DEFAULT_GA_ELITE_COUNT))
    mutation_rate = Decimal(str(fitness_config.get("mutation_rate", DEFAULT_GA_MUTATION_RATE)))
    stagnation_patience = int(
        fitness_config.get("stagnation_patience", DEFAULT_GA_STAGNATION_PATIENCE)
    )
    require_complete_benchmark = bool(fitness_config.get("require_complete_benchmark", True))
    require_all_windows = bool(fitness_config.get("require_all_windows", True))
    reuse_parameter_window_evaluations = bool(
        fitness_config.get("reuse_parameter_window_evaluations", True)
    )
    prefer_broad_candidate_cache = bool(fitness_config.get("prefer_broad_candidate_cache", True))
    raw_max_broad_candidate_cache_dates = fitness_config.get(
        "max_broad_candidate_cache_dates",
        DEFAULT_GA_MAX_BROAD_CANDIDATE_CACHE_DATES,
    )
    max_broad_candidate_cache_dates = (
        None
        if raw_max_broad_candidate_cache_dates in {None, ""}
        else int(raw_max_broad_candidate_cache_dates)
    )
    evaluation_windows = _evaluation_windows_for_run(run, fitness_config)
    initial_population = load_json(run.initial_population_json, default=[])
    if not isinstance(initial_population, list):
        initial_population = []
    population = [
        {"parameters": parameters, "parent_a_id": None, "parent_b_id": None, "mutation": None}
        for parameters in _seed_population(
            gene_space=gene_space,
            initial_population=initial_population,  # type: ignore[arg-type]
            population_size=run.population_size,
            rng=rng,
        )
    ]
    summary_generations = _generation_summary_generations(session, run_id=run.id)
    if existing_count:
        _sync_ga_run_progress_from_existing(run, existing_by_generation)
        _log_ga_event(
            session,
            run_id=run.id,
            generation=None,
            event_type="run_resumed",
            event={
                "restored_individuals": existing_count,
                "completed_individuals": run.completed_individuals,
                "failed_individuals": run.failed_individuals,
                "completed_generations": run.completed_generations,
            },
        )
        session.commit()

    best_score: Decimal | None = None
    stale_generations = 0
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
    evaluation_cache: dict[tuple[object, ...], dict[str, object]] = {}
    cache_stats = _new_ga_cache_stats()

    try:
        for generation in range(run.max_generations):
            generation_started_at = time.perf_counter()
            generation_cache_baseline = _cache_stats_baseline(cache_stats)
            session.refresh(run)
            if run.status == "cancel_requested":
                run.status = "cancelled"
                run.completed_at = datetime.now(UTC)
                session.commit()
                return run
            generation_individuals: list[GaIndividual] = []
            existing_generation = existing_by_generation.setdefault(generation, {})
            seen_generation_hashes = {
                individual.parameter_hash for individual in existing_generation.values()
            }
            for index, candidate in enumerate(population):
                existing_individual = existing_generation.get(index)
                if existing_individual is not None:
                    matches_existing, expected_hash = _candidate_matches_existing(
                        candidate=candidate,
                        existing=existing_individual,
                    )
                    if not matches_existing:
                        _log_ga_event(
                            session,
                            run_id=run.id,
                            generation=generation,
                            event_type="resume_candidate_mismatch",
                            event={
                                "generation": generation,
                                "individual_index": index,
                                "individual_id": existing_individual.id,
                                "persisted_parameter_hash": existing_individual.parameter_hash,
                                "reconstructed_parameter_hash": expected_hash,
                                "resolution": "used_persisted_individual",
                            },
                        )
                    generation_individuals.append(existing_individual)
                    continue

                session.refresh(run)
                if run.status == "cancel_requested":
                    run.status = "cancelled"
                    run.completed_at = datetime.now(UTC)
                    session.commit()
                    return run

                candidate_hash = _candidate_parameter_hash(candidate)
                if candidate_hash in seen_generation_hashes:
                    candidate = _resume_random_fill_candidate(
                        gene_space=gene_space,
                        seen_hashes=seen_generation_hashes,
                        rng=rng,
                    )
                    candidate_hash = _candidate_parameter_hash(candidate)
                    _log_ga_event(
                        session,
                        run_id=run.id,
                        generation=generation,
                        event_type="resume_duplicate_candidate_replaced",
                        event={
                            "generation": generation,
                            "individual_index": index,
                            "replacement_parameter_hash": candidate_hash,
                        },
                    )

                individual_cache_baseline = _cache_stats_baseline(cache_stats)
                evaluation = _evaluate_candidate(
                    session,
                    run=run,
                    evaluation_windows=evaluation_windows,
                    parameters=candidate["parameters"],  # type: ignore[arg-type]
                    require_complete_benchmark=require_complete_benchmark,
                    require_all_windows=require_all_windows,
                    reuse_parameter_window_evaluations=reuse_parameter_window_evaluations,
                    prefer_broad_candidate_cache=prefer_broad_candidate_cache,
                    max_broad_candidate_cache_dates=max_broad_candidate_cache_dates,
                    evaluation_cache=evaluation_cache,
                    cache_stats=cache_stats,
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
                )
                individual = _persist_individual(
                    session,
                    run=run,
                    generation=generation,
                    individual_index=index,
                    candidate=candidate,
                    evaluation=evaluation,
                )
                generation_individuals.append(individual)
                existing_generation[index] = individual
                seen_generation_hashes.add(individual.parameter_hash)
                if individual.status == "completed":
                    run.completed_individuals += 1
                else:
                    run.failed_individuals += 1
                _log_ga_event(
                    session,
                    run_id=run.id,
                    generation=generation,
                    event_type="individual_evaluated",
                    event={
                        "generation": generation,
                        "individual_index": index,
                        "individual_id": individual.id,
                        "parameter_hash": individual.parameter_hash,
                        "status": individual.status,
                        "fitness": str(individual.fitness) if individual.fitness is not None else None,
                        "completed_individuals": run.completed_individuals,
                        "failed_individuals": run.failed_individuals,
                        "performance": _cache_stats_delta(cache_stats, individual_cache_baseline),
                    },
                )
                session.commit()

            if len(generation_individuals) < run.population_size:
                raise ValueError(
                    f"GA run #{run.id} generation {generation} was not fully reconstructed."
                )

            ranked = _ranked_individuals(generation_individuals)
            generation_best = ranked[0] if ranked else None
            if generation_best is not None and generation_best.fitness is not None:
                if best_score is None or generation_best.fitness > best_score:
                    best_score = generation_best.fitness
                    run.best_individual_id = generation_best.id
                    stale_generations = 0
                else:
                    stale_generations += 1
            else:
                stale_generations += 1

            run.completed_generations = generation + 1
            if generation not in summary_generations:
                _log_ga_event(
                    session,
                    run_id=run.id,
                    generation=generation,
                    event_type="generation_summary",
                    event=_generation_summary_event(
                        generation=generation,
                        individuals=generation_individuals,
                        generation_best=generation_best,
                        stale_generations=stale_generations,
                        evaluation_windows=evaluation_windows,
                        performance={
                            **_cache_stats_delta(cache_stats, generation_cache_baseline),
                            "generation_seconds": _format_seconds(
                                time.perf_counter() - generation_started_at
                            ),
                            "cache_sizes": {
                                "ga_window_evaluations": len(evaluation_cache),
                                "screen": len(screen_cache),
                                "screen_candidates": len(screen_candidate_cache),
                                "fundamentals": len(fundamental_growth_cache),
                                "cup_events": len(cup_event_cache),
                                "trades": len(trade_cache),
                                "trade_dates": len(trade_dates_cache),
                                "future_rows": len(future_rows_cache),
                                "future_indicators": len(future_indicator_cache),
                                "market_filter": len(market_filter_cache),
                                "relative_strength": len(relative_strength_cache),
                                "benchmarks": len(benchmark_cache),
                            },
                            "max_rss": _max_rss_snapshot(),
                        },
                    ),
                )
                summary_generations.add(generation)
            session.commit()
            if stale_generations >= stagnation_patience:
                break
            if generation < run.max_generations - 1:
                population = _next_generation(
                    current_individuals=generation_individuals,
                    gene_space=gene_space,
                    population_size=run.population_size,
                    elite_count=elite_count,
                    mutation_rate=mutation_rate,
                    rng=rng,
                )

        best_individual = (
            session.get(GaIndividual, run.best_individual_id)
            if run.best_individual_id is not None
            else None
        )
        _evaluate_holdout(
            session,
            run=run,
            best_individual=best_individual,
            require_complete_benchmark=require_complete_benchmark,
            prefer_broad_candidate_cache=prefer_broad_candidate_cache,
            max_broad_candidate_cache_dates=max_broad_candidate_cache_dates,
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
        )
        run.status = "completed"
        run.completed_at = datetime.now(UTC)
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


def dispatch_ga_run_execution(run_id: int) -> None:
    with _ACTIVE_GA_RUN_IDS_LOCK:
        if run_id in _ACTIVE_GA_RUN_IDS:
            return
        _ACTIVE_GA_RUN_IDS.add(run_id)

    def run_worker() -> None:
        try:
            log_dir = os.path.join(os.getcwd(), "logs")
            os.makedirs(log_dir, exist_ok=True)
            log_path = os.path.join(log_dir, f"ga_run_{run_id}.log")
            env = os.environ.copy()
            with open(log_path, "a", buffering=1) as log_file:
                log_file.write(f"\n--- dispatch ga run #{run_id} ---\n")
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "stockanalyse_api.jobs.execute_ga_run",
                        str(run_id),
                    ],
                    cwd=os.getcwd(),
                    env=env,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                process.wait()
        finally:
            with _ACTIVE_GA_RUN_IDS_LOCK:
                _ACTIVE_GA_RUN_IDS.discard(run_id)

    thread = threading.Thread(
        target=run_worker,
        name=f"ga-run-{run_id}",
        daemon=True,
    )
    thread.start()


def _is_ga_run_active(run_id: int) -> bool:
    with _ACTIVE_GA_RUN_IDS_LOCK:
        return run_id in _ACTIVE_GA_RUN_IDS


def resume_ga_run(session, run_id: int) -> GaRun:
    run = session.get(GaRun, run_id)
    if run is None:
        raise LookupError("GA run not found.")
    if run.status == "completed":
        raise ValueError("Completed GA runs cannot be resumed.")
    run.status = "running"
    run.completed_at = None
    run.error_message = None
    session.commit()
    session.refresh(run)
    return run


def cancel_ga_run(session, run_id: int) -> GaRun:
    run = session.get(GaRun, run_id)
    if run is None:
        raise LookupError("GA run not found.")
    if run.status == "running":
        if _is_ga_run_active(run_id):
            run.status = "cancel_requested"
        else:
            run.status = "cancelled"
            run.completed_at = datetime.now(UTC)
    elif run.status == "cancel_requested" and not _is_ga_run_active(run_id):
        run.status = "cancelled"
        run.completed_at = datetime.now(UTC)
    session.commit()
    session.refresh(run)
    return run
