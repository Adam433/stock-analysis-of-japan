from __future__ import annotations

import os
from collections import Counter
from datetime import UTC, date, datetime

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
)


MAX_WORKERS = int(os.environ.get("STOCKANALYSE_ROLLING_VALIDATION_WORKERS", "1"))
MAX_TASKS_PER_CHILD = int(
    os.environ.get("STOCKANALYSE_ROLLING_VALIDATION_MAX_TASKS_PER_CHILD", "12")
)

ROLLING_WINDOWS = [
    ("w1_2014_2018_to_2019_2022", date(2014, 1, 1), date(2018, 12, 31), date(2019, 1, 1), date(2022, 12, 31)),
    ("w2_2016_2020_to_2021_2023", date(2016, 1, 1), date(2020, 12, 31), date(2021, 1, 1), date(2023, 12, 31)),
    ("w3_2018_2022_to_2023_2026", date(2018, 1, 1), date(2022, 12, 31), date(2023, 1, 1), date(2026, 4, 30)),
]

CANDIDATES = [
    {
        "name": "return_quality_light_loose",
        "description": "Return-seeking default from confirmation run.",
        "finance": "quality_light_no_valuation",
        "cup": "loose_no_prior",
    },
    {
        "name": "balanced_value_quality_loose",
        "description": "Balanced candidate: close validation return with smaller training drawdown.",
        "finance": "value_quality",
        "cup": "loose_no_prior",
    },
    {
        "name": "growth_ocf_low_drawdown_none",
        "description": "Growth OCF low-drawdown candidate without K-line filter.",
        "finance": "growth_ocf",
        "cup": "none",
    },
    {
        "name": "growth_ocf_loose",
        "description": "Growth OCF observation candidate with the loose cup-handle filter.",
        "finance": "growth_ocf",
        "cup": "loose_no_prior",
    },
]


def _fundamental_payload(name: str) -> dict[str, object]:
    return next(payload for item_name, payload in FUNDAMENTAL_MODES if item_name == name)


def _cup_payload(name: str) -> tuple[bool, dict[str, object]]:
    if name == "none":
        return False, {}
    for item_name, payload in LOOSE_CUP_VARIANTS:
        if item_name == name:
            return True, payload
    raise ValueError(f"Unknown cup variant: {name}")


def _candidate_parameters(candidate: dict[str, str]) -> dict[str, object]:
    use_cup_handle, cup_payload = _cup_payload(candidate["cup"])
    return _normalize_parameter_set(
        {
            "use_rps": True,
            "rps_threshold": 70,
            "selected_rps_windows": [50, 120, 250],
            "min_rps_windows_passing": 1,
            "use_cup_handle": use_cup_handle,
            "cup_handle_params": cup_payload,
            "fundamental_growth_params": _fundamental_payload(candidate["finance"]),
            "holding_days": None,
            "stop_loss_pct": "-0.08",
            "take_profit_pct": None,
            "rps_exit_threshold": 80,
            "portfolio_cap": 20,
            "position_weight_pct": "0.0500",
            "initial_capital": "100000.00",
            "position_size_amount": "5000.00",
            "allow_reentry_while_open": False,
            "market_filter_params": {"enabled": False},
            "entry_delay_days": 0,
            "entry_deferral_window_days": 5,
        }
    )


def build_parameter_sets() -> tuple[list[dict[str, object]], Counter[str]]:
    parameter_sets: list[dict[str, object]] = []
    seen: set[str] = set()
    tags: Counter[str] = Counter()
    for candidate in CANDIDATES:
        normalized = _candidate_parameters(candidate)
        normalized["candidate_name"] = candidate["name"]
        parameter_hash = stable_parameter_hash(normalized)
        if parameter_hash in seen:
            continue
        seen.add(parameter_hash)
        parameter_sets.append(normalized)
        tags[f"candidate:{candidate['name']}"] += 1
        tags[f"finance:{candidate['finance']}"] += 1
        tags[f"cup:{candidate['cup']}"] += 1
    return parameter_sets, tags


def _create_run(
    *,
    session,
    market: str,
    window_name: str,
    train_start: date,
    train_end: date,
    validation_start: date,
    validation_end: date,
    parameter_sets: list[dict[str, object]],
    tags: Counter[str],
) -> OptimizationRun:
    design = {
        "name": "rolling_window_candidate_validation",
        "window": window_name,
        "description": (
            "Rolling-window validation of the current return, balanced, and growth_ocf "
            "observation candidates. Single-worker friendly."
        ),
        "candidates": CANDIDATES,
        "train": f"{train_start.isoformat()}..{train_end.isoformat()}",
        "validation": f"{validation_start.isoformat()}..{validation_end.isoformat()}",
        "market_filter": "disabled",
        "fixed_parameters": {
            "rps_threshold": 70,
            "selected_rps_windows": [50, 120, 250],
            "min_rps_windows_passing": 1,
            "rps_exit_threshold": 80,
            "holding_days": None,
            "stop_loss_pct": "-0.08",
        },
        "account": {
            "initial_capital": "100000.00",
            "position_size_amount": "5000.00",
            "portfolio_cap": 20,
        },
        "counts": dict(sorted(tags.items())),
    }
    parameter_space = {
        "_optimization": {
            "search_mode": "grid",
            "random_seed": None,
            "max_workers": MAX_WORKERS,
            "max_tasks_per_child": MAX_TASKS_PER_CHILD,
        },
        "_design": design,
    }
    run = OptimizationRun(
        market=market,
        train_start_date=train_start,
        train_end_date=train_end,
        validation_start_date=validation_start,
        validation_end_date=validation_end,
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
    print(
        f"created optimization run #{run.id} {window_name} with {len(parameter_sets)} parameter sets",
        flush=True,
    )
    print(f"design counts: {dict(sorted(tags.items()))}", flush=True)
    return run


def main() -> None:
    market = normalize_market("us")
    parameter_sets, tags = build_parameter_sets()
    with SessionLocal() as session:
        for window_name, train_start, train_end, validation_start, validation_end in ROLLING_WINDOWS:
            run = _create_run(
                session=session,
                market=market,
                window_name=window_name,
                train_start=train_start,
                train_end=train_end,
                validation_start=validation_start,
                validation_end=validation_end,
                parameter_sets=parameter_sets,
                tags=tags,
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
