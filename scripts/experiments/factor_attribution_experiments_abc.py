from __future__ import annotations

import os
from collections import Counter
from datetime import UTC, date, datetime
from typing import Iterable

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.backtests.models import OptimizationRun
from stockanalyse_api.services.dashboard import normalize_market
from stockanalyse_api.services.optimization_backtest import (
    _data_snapshot,
    _normalize_parameter_set,
    dump_json,
    execute_optimization_run,
    stable_parameter_hash,
)

from scripts.experiments.finance_rps_cup_reference_optimization import (
    FUNDAMENTAL_MODES,
    LOOSE_CUP_VARIANTS,
    STRICT_CUP_VARIANTS,
)


MAX_WORKERS = int(os.environ.get("STOCKANALYSE_FACTOR_ABC_WORKERS", "2"))
MAX_TASKS_PER_CHILD = int(os.environ.get("STOCKANALYSE_FACTOR_ABC_MAX_TASKS_PER_CHILD", "24"))
REQUESTED_EXPERIMENTS = [
    item.strip().upper()
    for item in os.environ.get("STOCKANALYSE_FACTOR_ABC_EXPERIMENTS", "A,B,C").split(",")
    if item.strip()
]

TRAIN_START = date(2018, 1, 1)
TRAIN_END = date(2022, 12, 31)
VALIDATION_START = date(2023, 1, 1)
VALIDATION_END = date(2026, 4, 30)


def _fundamental_payload(name: str) -> dict[str, object]:
    return next(payload for item_name, payload in FUNDAMENTAL_MODES if item_name == name)


def _cup_payload(name: str) -> tuple[bool, dict[str, object]]:
    if name == "none":
        return False, {}
    for item_name, payload in [*LOOSE_CUP_VARIANTS, *STRICT_CUP_VARIANTS]:
        if item_name == name:
            return True, payload
    raise ValueError(f"Unknown cup variant: {name}")


def _base_parameters(
    *,
    finance_name: str,
    rps_threshold: int,
    selected_windows: list[int],
    cup_name: str,
    rps_exit_threshold: int,
    holding_days: int | None = None,
    min_rps_windows_passing: int = 1,
) -> dict[str, object]:
    use_cup, cup_payload = _cup_payload(cup_name)
    return {
        "use_rps": True,
        "rps_threshold": rps_threshold,
        "selected_rps_windows": selected_windows,
        "min_rps_windows_passing": min_rps_windows_passing,
        "use_cup_handle": use_cup,
        "cup_handle_params": cup_payload,
        "fundamental_growth_params": _fundamental_payload(finance_name),
        "holding_days": holding_days,
        "stop_loss_pct": "-0.08",
        "take_profit_pct": None,
        "rps_exit_threshold": rps_exit_threshold,
        "portfolio_cap": 20,
        "position_weight_pct": "0.0500",
        "initial_capital": "100000.00",
        "position_size_amount": "5000.00",
        "allow_reentry_while_open": False,
        "market_filter_params": {"enabled": False},
        "entry_delay_days": 0,
        "entry_deferral_window_days": 5,
    }


def _dedupe(rows: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    parameter_sets: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in rows:
        normalized = _normalize_parameter_set(raw)
        parameter_hash = stable_parameter_hash(normalized)
        if parameter_hash in seen:
            continue
        seen.add(parameter_hash)
        parameter_sets.append(normalized)
    return parameter_sets


def _tag_counts(parameter_sets: list[dict[str, object]]) -> Counter[str]:
    tags: Counter[str] = Counter()
    for params in parameter_sets:
        finance_name = _finance_name(params["fundamental_growth_params"])  # type: ignore[arg-type]
        cup_name = _cup_name(
            bool(params["use_cup_handle"]),
            params["cup_handle_params"],  # type: ignore[arg-type]
        )
        tags[f"finance:{finance_name}"] += 1
        tags[f"rps:{params['rps_threshold']}"] += 1
        tags[f"windows:{'+'.join(str(item) for item in params['selected_rps_windows'])}"] += 1
        tags[f"cup:{cup_name}"] += 1
        tags[f"rps_exit:{params['rps_exit_threshold']}"] += 1
        tags[f"holding:{params['holding_days'] if params['holding_days'] is not None else 'none'}"] += 1
        tags[f"min_rps_windows_passing:{params['min_rps_windows_passing']}"] += 1
    return tags


def _finance_name(payload: dict[str, object]) -> str:
    normalized = _normalize_parameter_set({"fundamental_growth_params": payload})[
        "fundamental_growth_params"
    ]
    for name, candidate in FUNDAMENTAL_MODES:
        candidate_normalized = _normalize_parameter_set({"fundamental_growth_params": candidate})[
            "fundamental_growth_params"
        ]
        if candidate_normalized == normalized:
            return name
    return "unknown"


def _cup_name(use_cup_handle: bool, payload: dict[str, object]) -> str:
    if not use_cup_handle:
        return "none"
    normalized = _normalize_parameter_set(
        {"use_cup_handle": True, "cup_handle_params": payload}
    )["cup_handle_params"]
    for name, candidate in [*LOOSE_CUP_VARIANTS, *STRICT_CUP_VARIANTS]:
        candidate_normalized = _normalize_parameter_set(
            {"use_cup_handle": True, "cup_handle_params": candidate}
        )["cup_handle_params"]
        if candidate_normalized == normalized:
            return name
    return "unknown"


def build_experiment_a() -> tuple[str, str, list[dict[str, object]], dict[str, object]]:
    rows = []
    for finance_name in ["quality_light_no_valuation", "quality_ocf"]:
        for rps_threshold in [70, 80, 85]:
            for selected_windows in [[50, 120], [120, 250], [50, 120, 250]]:
                for cup_name in ["none", "loose_no_prior", "base_volume_confirm"]:
                    for rps_exit_threshold in [75, 80]:
                        rows.append(
                            _base_parameters(
                                finance_name=finance_name,
                                rps_threshold=rps_threshold,
                                selected_windows=selected_windows,
                                cup_name=cup_name,
                                rps_exit_threshold=rps_exit_threshold,
                            )
                        )
    parameter_sets = _dedupe(rows)
    return (
        "factor_attribution_experiment_a_cup",
        "Experiment A: K-line/cup-handle attribution with finance and RPS held in a paired grid.",
        parameter_sets,
        {
            "axis": "cup_handle",
            "finance": ["quality_light_no_valuation", "quality_ocf"],
            "rps_thresholds": [70, 80, 85],
            "windows": ["50+120", "120+250", "50+120+250"],
            "cups": ["none", "loose_no_prior", "base_volume_confirm"],
            "rps_exit_thresholds": [75, 80],
        },
    )


def build_experiment_b() -> tuple[str, str, list[dict[str, object]], dict[str, object]]:
    rows = []
    for finance_name in ["quality_light_no_valuation", "valuation_tight"]:
        for cup_name in ["none", "base_volume_confirm"]:
            for rps_threshold in [70, 80, 85, 90, 95]:
                for selected_windows in [[50, 120], [120, 250], [50, 120, 250]]:
                    for min_passing in [1, 2]:
                        rows.append(
                            _base_parameters(
                                finance_name=finance_name,
                                rps_threshold=rps_threshold,
                                selected_windows=selected_windows,
                                cup_name=cup_name,
                                rps_exit_threshold=80,
                                min_rps_windows_passing=min_passing,
                            )
                        )
    parameter_sets = _dedupe(rows)
    return (
        "factor_attribution_experiment_b_rps",
        "Experiment B: RPS threshold/window/min-passing attribution under light and tight finance.",
        parameter_sets,
        {
            "axis": "rps",
            "finance": ["quality_light_no_valuation", "valuation_tight"],
            "cups": ["none", "base_volume_confirm"],
            "rps_thresholds": [70, 80, 85, 90, 95],
            "windows": ["50+120", "120+250", "50+120+250"],
            "min_rps_windows_passing": [1, 2],
        },
    )


def build_experiment_c() -> tuple[str, str, list[dict[str, object]], dict[str, object]]:
    rows = []
    for finance_name in [
        "quality_light_no_valuation",
        "growth_ocf",
        "valuation_growth_tight",
        "quality_ocf",
        "value_quality",
    ]:
        for rps_threshold in [80, 85]:
            for cup_name in ["none", "base_volume_confirm"]:
                for holding_days in [None, 130]:
                    rows.append(
                        _base_parameters(
                            finance_name=finance_name,
                            rps_threshold=rps_threshold,
                            selected_windows=[50, 120, 250],
                            cup_name=cup_name,
                            rps_exit_threshold=80,
                            holding_days=holding_days,
                        )
                    )
    parameter_sets = _dedupe(rows)
    return (
        "factor_attribution_experiment_c_finance",
        "Experiment C: finance strictness/risk attribution with RPS and K-line controls.",
        parameter_sets,
        {
            "axis": "finance",
            "finance": [
                "quality_light_no_valuation",
                "growth_ocf",
                "valuation_growth_tight",
                "quality_ocf",
                "value_quality",
            ],
            "rps_thresholds": [80, 85],
            "windows": ["50+120+250"],
            "cups": ["none", "base_volume_confirm"],
            "holding_days": ["none", 130],
        },
    )


EXPERIMENT_BUILDERS = {
    "A": build_experiment_a,
    "B": build_experiment_b,
    "C": build_experiment_c,
}


def _create_run(
    *,
    session,
    market: str,
    name: str,
    description: str,
    parameter_sets: list[dict[str, object]],
    design: dict[str, object],
) -> OptimizationRun:
    tags = _tag_counts(parameter_sets)
    full_design = {
        "name": name,
        "description": description,
        "market_filter": "disabled",
        "train": f"{TRAIN_START.isoformat()}..{TRAIN_END.isoformat()}",
        "validation": f"{VALIDATION_START.isoformat()}..{VALIDATION_END.isoformat()}",
        "account": {
            "initial_capital": "100000.00",
            "position_size_amount": "5000.00",
            "portfolio_cap": 20,
        },
        "counts": dict(sorted(tags.items())),
        **design,
    }
    parameter_space = {
        "_optimization": {
            "search_mode": "grid",
            "random_seed": None,
            "max_workers": MAX_WORKERS,
            "max_tasks_per_child": MAX_TASKS_PER_CHILD,
        },
        "_design": full_design,
    }
    run = OptimizationRun(
        market=market,
        train_start_date=TRAIN_START,
        train_end_date=TRAIN_END,
        validation_start_date=VALIDATION_START,
        validation_end_date=VALIDATION_END,
        objective="robust_annualized_return",
        parameter_space_json=dump_json(parameter_space),
        parameter_sets_json=dump_json(parameter_sets),
        data_snapshot_json=dump_json(_data_snapshot(session, market=market, require_data_ready=True)),
        status="running",
        total_parameter_sets=len(parameter_sets),
        completed_parameter_sets=0,
        failed_parameter_sets=0,
        started_at=datetime.now(UTC),
    )
    session.add(run)
    session.commit()
    session.refresh(run)
    print(f"created optimization run #{run.id} {name} with {len(parameter_sets)} parameter sets", flush=True)
    print(f"design counts: {dict(sorted(tags.items()))}", flush=True)
    return run


def main() -> None:
    unknown = [item for item in REQUESTED_EXPERIMENTS if item not in EXPERIMENT_BUILDERS]
    if unknown:
        raise ValueError(f"Unknown experiments: {', '.join(unknown)}")
    market = normalize_market("us")
    with SessionLocal() as session:
        for experiment_key in REQUESTED_EXPERIMENTS:
            name, description, parameter_sets, design = EXPERIMENT_BUILDERS[experiment_key]()
            run = _create_run(
                session=session,
                market=market,
                name=name,
                description=description,
                parameter_sets=parameter_sets,
                design=design,
            )
            completed = execute_optimization_run(session, int(run.id))
            print(
                f"finished optimization run #{completed.id}: "
                f"status={completed.status}, completed={completed.completed_parameter_sets}, "
                f"failed={completed.failed_parameter_sets}, best_result_id={completed.best_result_id}",
                flush=True,
            )


if __name__ == "__main__":
    main()
