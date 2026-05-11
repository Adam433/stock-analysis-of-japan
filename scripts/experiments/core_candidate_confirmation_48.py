from __future__ import annotations

import os
from collections import Counter
from datetime import UTC, date, datetime

from sqlalchemy import select

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.backtests.models import OptimizationResult, OptimizationRun
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


MAX_WORKERS = int(os.environ.get("STOCKANALYSE_CORE_CONFIRMATION_WORKERS", "2"))
MAX_TASKS_PER_CHILD = int(
    os.environ.get("STOCKANALYSE_CORE_CONFIRMATION_MAX_TASKS_PER_CHILD", "24")
)
RESUME_FROM_RUN_ID = os.environ.get("STOCKANALYSE_CORE_CONFIRMATION_RESUME_RUN_ID")

TRAIN_START = date(2018, 1, 1)
TRAIN_END = date(2022, 12, 31)
VALIDATION_START = date(2023, 1, 1)
VALIDATION_END = date(2026, 4, 30)


def _fundamental_payload(name: str) -> dict[str, object]:
    return next(payload for item_name, payload in FUNDAMENTAL_MODES if item_name == name)


def _cup_payload(name: str) -> tuple[bool, dict[str, object]]:
    if name == "none":
        return False, {}
    for item_name, payload in LOOSE_CUP_VARIANTS:
        if item_name == name:
            return True, payload
    raise ValueError(f"Unknown cup variant: {name}")


def build_parameter_sets() -> tuple[list[dict[str, object]], Counter[str]]:
    rows: list[dict[str, object]] = []
    for finance_name in ["quality_light_no_valuation", "value_quality", "growth_ocf"]:
        for cup_name in ["none", "loose_no_prior"]:
            for rps_threshold in [70, 80]:
                for selected_windows in [[120, 250], [50, 120, 250]]:
                    for rps_exit_threshold in [75, 80]:
                        use_cup_handle, cup_payload = _cup_payload(cup_name)
                        rows.append(
                            {
                                "use_rps": True,
                                "rps_threshold": rps_threshold,
                                "selected_rps_windows": selected_windows,
                                "min_rps_windows_passing": 1,
                                "use_cup_handle": use_cup_handle,
                                "cup_handle_params": cup_payload,
                                "fundamental_growth_params": _fundamental_payload(finance_name),
                                "holding_days": None,
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
                        )
    parameter_sets: list[dict[str, object]] = []
    seen: set[str] = set()
    tags: Counter[str] = Counter()
    for raw in rows:
        normalized = _normalize_parameter_set(raw)
        parameter_hash = stable_parameter_hash(normalized)
        if parameter_hash in seen:
            continue
        seen.add(parameter_hash)
        parameter_sets.append(normalized)
        tags[f"finance:{_finance_name(normalized['fundamental_growth_params'])}"] += 1  # type: ignore[arg-type]
        tags[f"cup:{_cup_name(bool(normalized['use_cup_handle']), normalized['cup_handle_params'])}"] += 1  # type: ignore[arg-type]
        tags[f"rps:{normalized['rps_threshold']}"] += 1
        tags[f"windows:{'+'.join(str(item) for item in normalized['selected_rps_windows'])}"] += 1
        tags[f"rps_exit:{normalized['rps_exit_threshold']}"] += 1
    return parameter_sets, tags


def _finance_name(payload: dict[str, object]) -> str:
    for name, candidate in FUNDAMENTAL_MODES:
        if _normalize_parameter_set({"fundamental_growth_params": candidate})[
            "fundamental_growth_params"
        ] == payload:
            return name
    return "unknown"


def _cup_name(use_cup_handle: bool, payload: dict[str, object]) -> str:
    if not use_cup_handle:
        return "none"
    for name, candidate in LOOSE_CUP_VARIANTS:
        if _normalize_parameter_set({"use_cup_handle": True, "cup_handle_params": candidate})[
            "cup_handle_params"
        ] == payload:
            return name
    return "unknown"


def main() -> None:
    market = normalize_market("us")
    parameter_sets, tags = build_parameter_sets()
    design = {
        "name": "core_candidate_confirmation_48",
        "description": (
            "48-set confirmation grid after factor attribution review. Uses low-frequency "
            "friendly robust scoring and keeps market filter disabled."
        ),
        "finance": ["quality_light_no_valuation", "value_quality", "growth_ocf"],
        "cups": ["none", "loose_no_prior"],
        "rps_thresholds": [70, 80],
        "windows": ["120+250", "50+120+250"],
        "rps_exit_thresholds": [75, 80],
        "holding_days": ["none"],
        "market_filter": "disabled",
        "train": f"{TRAIN_START.isoformat()}..{TRAIN_END.isoformat()}",
        "validation": f"{VALIDATION_START.isoformat()}..{VALIDATION_END.isoformat()}",
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
    with SessionLocal() as session:
        if RESUME_FROM_RUN_ID:
            completed_hashes = {
                row.parameter_hash
                for row in session.execute(
                    select(OptimizationResult).where(
                        OptimizationResult.optimization_run_id == int(RESUME_FROM_RUN_ID),
                        OptimizationResult.status == "completed",
                    )
                ).scalars()
            }
            original_count = len(parameter_sets)
            parameter_sets = [
                parameters
                for parameters in parameter_sets
                if stable_parameter_hash(parameters) not in completed_hashes
            ]
            design["resume"] = {
                "from_run_id": int(RESUME_FROM_RUN_ID),
                "completed_hashes": len(completed_hashes),
                "original_parameter_sets": original_count,
                "remaining_parameter_sets": len(parameter_sets),
            }
            if not parameter_sets:
                print(
                    f"run #{RESUME_FROM_RUN_ID} has no remaining parameter sets",
                    flush=True,
                )
                return
            parameter_space["_design"] = design
        run = OptimizationRun(
            market=market,
            train_start_date=TRAIN_START,
            train_end_date=TRAIN_END,
            validation_start_date=VALIDATION_START,
            validation_end_date=VALIDATION_END,
            objective="robust_annualized_return",
            parameter_space_json=dump_json(parameter_space),
            parameter_sets_json=dump_json(parameter_sets),
            data_snapshot_json=dump_json(
                _data_snapshot(session, market=market, require_data_ready=True)
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
        print(
            f"created optimization run #{run.id} with {len(parameter_sets)} parameter sets",
            flush=True,
        )
        print(f"design counts: {dict(sorted(tags.items()))}", flush=True)
        completed = execute_optimization_run(session, int(run.id))
        print(
            f"finished optimization run #{completed.id}: "
            f"status={completed.status}, completed={completed.completed_parameter_sets}, "
            f"failed={completed.failed_parameter_sets}, best_result_id={completed.best_result_id}",
            flush=True,
        )


if __name__ == "__main__":
    main()
