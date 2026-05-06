from __future__ import annotations

import random
import os
from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal

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


SEED = 20260504
MAX_WORKERS = int(os.environ.get("STOCKANALYSE_REFERENCE_OPTIMIZATION_WORKERS", "2"))
MAX_TASKS_PER_CHILD = int(
    os.environ.get("STOCKANALYSE_REFERENCE_OPTIMIZATION_MAX_TASKS_PER_CHILD", "24")
)
RESUME_FROM_RUN_ID = os.environ.get("STOCKANALYSE_REFERENCE_OPTIMIZATION_RESUME_RUN_ID")
REPEATS_PER_CELL = int(os.environ.get("STOCKANALYSE_REFERENCE_OPTIMIZATION_REPEATS", "1"))
NO_RPS_CONTROL_REPEATS = int(
    os.environ.get("STOCKANALYSE_REFERENCE_OPTIMIZATION_NO_RPS_REPEATS", "1")
)


def _fundamental_mode(
    name: str,
    *,
    min_years: int,
    min_growth_count: int,
    min_yoy_growth_pct: str,
    max_pe: str | None,
    max_pb: str | None,
    require_positive_operating_cash_flow: bool,
    require_positive_free_cash_flow: bool,
    min_operating_cash_flow_growth_count: int | None,
    min_operating_cash_flow_yoy_growth_pct: str = "0",
) -> tuple[str, dict[str, object]]:
    return (
        name,
        {
            "enabled": True,
            "min_years": min_years,
            "min_growth_count": min_growth_count,
            "min_yoy_growth_pct": min_yoy_growth_pct,
            "require_positive_net_income": True,
            "reporting_lag_days": 120,
            "max_pe": max_pe,
            "max_pb": max_pb,
            "require_positive_operating_cash_flow": require_positive_operating_cash_flow,
            "require_positive_free_cash_flow": require_positive_free_cash_flow,
            "min_operating_cash_flow_growth_count": min_operating_cash_flow_growth_count,
            "min_operating_cash_flow_yoy_growth_pct": min_operating_cash_flow_yoy_growth_pct,
        },
    )


FUNDAMENTAL_MODES = [
    _fundamental_mode(
        "quality_light_no_valuation",
        min_years=3,
        min_growth_count=1,
        min_yoy_growth_pct="0",
        max_pe=None,
        max_pb=None,
        require_positive_operating_cash_flow=False,
        require_positive_free_cash_flow=False,
        min_operating_cash_flow_growth_count=None,
    ),
    _fundamental_mode(
        "quality_ocf",
        min_years=3,
        min_growth_count=2,
        min_yoy_growth_pct="0",
        max_pe="60",
        max_pb="15",
        require_positive_operating_cash_flow=True,
        require_positive_free_cash_flow=False,
        min_operating_cash_flow_growth_count=None,
    ),
    _fundamental_mode(
        "growth_ocf",
        min_years=3,
        min_growth_count=2,
        min_yoy_growth_pct="10",
        max_pe="60",
        max_pb="15",
        require_positive_operating_cash_flow=True,
        require_positive_free_cash_flow=False,
        min_operating_cash_flow_growth_count=1,
    ),
    _fundamental_mode(
        "valuation_tight",
        min_years=3,
        min_growth_count=2,
        min_yoy_growth_pct="0",
        max_pe="40",
        max_pb="8",
        require_positive_operating_cash_flow=True,
        require_positive_free_cash_flow=False,
        min_operating_cash_flow_growth_count=1,
    ),
    _fundamental_mode(
        "valuation_growth_tight",
        min_years=3,
        min_growth_count=2,
        min_yoy_growth_pct="5",
        max_pe="40",
        max_pb="8",
        require_positive_operating_cash_flow=True,
        require_positive_free_cash_flow=False,
        min_operating_cash_flow_growth_count=1,
    ),
    _fundamental_mode(
        "cashflow_strict",
        min_years=4,
        min_growth_count=2,
        min_yoy_growth_pct="5",
        max_pe="60",
        max_pb="15",
        require_positive_operating_cash_flow=True,
        require_positive_free_cash_flow=True,
        min_operating_cash_flow_growth_count=2,
    ),
    _fundamental_mode(
        "growth_strict",
        min_years=4,
        min_growth_count=3,
        min_yoy_growth_pct="10",
        max_pe="60",
        max_pb="15",
        require_positive_operating_cash_flow=True,
        require_positive_free_cash_flow=False,
        min_operating_cash_flow_growth_count=2,
        min_operating_cash_flow_yoy_growth_pct="5",
    ),
    _fundamental_mode(
        "value_quality",
        min_years=3,
        min_growth_count=1,
        min_yoy_growth_pct="0",
        max_pe="30",
        max_pb="5",
        require_positive_operating_cash_flow=True,
        require_positive_free_cash_flow=False,
        min_operating_cash_flow_growth_count=1,
    ),
]


def _cup_variant(
    name: str,
    *,
    min_cup_depth_pct: str,
    max_cup_depth_pct: str,
    min_handle_pullback_pct: str,
    max_handle_pullback_pct: str,
    max_right_lip_delta_pct: str,
    require_prior_uptrend: bool,
    min_prior_uptrend_pct: str,
    min_handle_low_position_pct: str,
    max_handle_depth_to_cup_depth_pct: str,
    max_handle_high_above_lip_pct: str,
    require_breakout_volume: bool = False,
    min_breakout_volume_multiplier: str = "1.4",
) -> tuple[str, dict[str, object]]:
    return (
        name,
        {
            "min_cup_duration": 35,
            "max_cup_duration": 330,
            "min_handle_duration": 3,
            "max_handle_duration": 60,
            "min_total_duration": 80,
            "max_total_duration": 420,
            "min_cup_depth_pct": min_cup_depth_pct,
            "max_cup_depth_pct": max_cup_depth_pct,
            "min_handle_pullback_pct": min_handle_pullback_pct,
            "max_handle_pullback_pct": max_handle_pullback_pct,
            "max_right_lip_delta_pct": max_right_lip_delta_pct,
            "require_prior_uptrend": require_prior_uptrend,
            "prior_uptrend_lookback_days": 120,
            "min_prior_uptrend_pct": min_prior_uptrend_pct,
            "min_handle_low_position_pct": min_handle_low_position_pct,
            "max_handle_depth_to_cup_depth_pct": max_handle_depth_to_cup_depth_pct,
            "max_handle_high_above_lip_pct": max_handle_high_above_lip_pct,
            "min_bottom_dwell_days": 2,
            "bottom_zone_pct": "35",
            "min_bottom_span_pct": "5",
            "min_cup_side_duration_pct": "20",
            "require_breakout_volume": require_breakout_volume,
            "breakout_volume_avg_days": 50,
            "min_breakout_volume_multiplier": min_breakout_volume_multiplier,
            "breakout_lookback_days": 60,
            "lookback_days": 750,
        },
    )


LOOSE_CUP_VARIANTS = [
    _cup_variant(
        "loose_no_prior",
        min_cup_depth_pct="5",
        max_cup_depth_pct="55",
        min_handle_pullback_pct="1",
        max_handle_pullback_pct="25",
        max_right_lip_delta_pct="15",
        require_prior_uptrend=False,
        min_prior_uptrend_pct="10",
        min_handle_low_position_pct="40",
        max_handle_depth_to_cup_depth_pct="80",
        max_handle_high_above_lip_pct="8",
    ),
    _cup_variant(
        "loose_prior_10",
        min_cup_depth_pct="5",
        max_cup_depth_pct="55",
        min_handle_pullback_pct="1",
        max_handle_pullback_pct="25",
        max_right_lip_delta_pct="15",
        require_prior_uptrend=True,
        min_prior_uptrend_pct="10",
        min_handle_low_position_pct="40",
        max_handle_depth_to_cup_depth_pct="80",
        max_handle_high_above_lip_pct="8",
    ),
    _cup_variant(
        "broad_mid",
        min_cup_depth_pct="8",
        max_cup_depth_pct="55",
        min_handle_pullback_pct="1",
        max_handle_pullback_pct="20",
        max_right_lip_delta_pct="10",
        require_prior_uptrend=True,
        min_prior_uptrend_pct="20",
        min_handle_low_position_pct="55",
        max_handle_depth_to_cup_depth_pct="60",
        max_handle_high_above_lip_pct="5",
    ),
    _cup_variant(
        "shallow_broad",
        min_cup_depth_pct="5",
        max_cup_depth_pct="45",
        min_handle_pullback_pct="1",
        max_handle_pullback_pct="20",
        max_right_lip_delta_pct="10",
        require_prior_uptrend=False,
        min_prior_uptrend_pct="20",
        min_handle_low_position_pct="55",
        max_handle_depth_to_cup_depth_pct="60",
        max_handle_high_above_lip_pct="5",
    ),
]

STRICT_CUP_VARIANTS = [
    _cup_variant(
        "base_prior_20",
        min_cup_depth_pct="10",
        max_cup_depth_pct="45",
        min_handle_pullback_pct="1",
        max_handle_pullback_pct="12",
        max_right_lip_delta_pct="5",
        require_prior_uptrend=True,
        min_prior_uptrend_pct="20",
        min_handle_low_position_pct="55",
        max_handle_depth_to_cup_depth_pct="35",
        max_handle_high_above_lip_pct="2",
    ),
    _cup_variant(
        "base_no_prior",
        min_cup_depth_pct="10",
        max_cup_depth_pct="45",
        min_handle_pullback_pct="1",
        max_handle_pullback_pct="12",
        max_right_lip_delta_pct="5",
        require_prior_uptrend=False,
        min_prior_uptrend_pct="20",
        min_handle_low_position_pct="55",
        max_handle_depth_to_cup_depth_pct="35",
        max_handle_high_above_lip_pct="2",
    ),
    _cup_variant(
        "oneil_strict",
        min_cup_depth_pct="12",
        max_cup_depth_pct="33",
        min_handle_pullback_pct="3",
        max_handle_pullback_pct="12",
        max_right_lip_delta_pct="5",
        require_prior_uptrend=True,
        min_prior_uptrend_pct="30",
        min_handle_low_position_pct="66",
        max_handle_depth_to_cup_depth_pct="35",
        max_handle_high_above_lip_pct="2",
    ),
    _cup_variant(
        "strict_relaxed_lip",
        min_cup_depth_pct="10",
        max_cup_depth_pct="45",
        min_handle_pullback_pct="2",
        max_handle_pullback_pct="12",
        max_right_lip_delta_pct="10",
        require_prior_uptrend=True,
        min_prior_uptrend_pct="20",
        min_handle_low_position_pct="55",
        max_handle_depth_to_cup_depth_pct="35",
        max_handle_high_above_lip_pct="5",
    ),
    _cup_variant(
        "base_volume_confirm",
        min_cup_depth_pct="10",
        max_cup_depth_pct="45",
        min_handle_pullback_pct="1",
        max_handle_pullback_pct="12",
        max_right_lip_delta_pct="5",
        require_prior_uptrend=True,
        min_prior_uptrend_pct="20",
        min_handle_low_position_pct="55",
        max_handle_depth_to_cup_depth_pct="35",
        max_handle_high_above_lip_pct="2",
        require_breakout_volume=True,
        min_breakout_volume_multiplier="1.2",
    ),
]

PRIMARY_RPS_THRESHOLDS = [70, 80, 85, 90, 95]
RPS_WINDOWS = [[50, 120], [120, 250], [50, 120, 250]]
STOP_LOSSES = ["-0.06", "-0.08", "-0.10", "-0.12"]
HOLDING_DAYS = [None, 130, 260]
RPS_EXITS = [75, 80, 85]
ENTRY_DELAYS = [0, 1, 2]
PRIMARY_CUP_GROUPS = ["none", "loose", "strict"]
NO_RPS_CONTROL_FINANCE_NAMES = {
    "quality_light_no_valuation",
    "valuation_growth_tight",
    "growth_strict",
    "value_quality",
}
NO_RPS_CONTROL_CUP_GROUPS = ["none"]


def _choose_cup(rng: random.Random, group: str) -> tuple[bool, dict[str, object], str]:
    if group == "none":
        return False, {}, "none"
    variants = LOOSE_CUP_VARIANTS if group == "loose" else STRICT_CUP_VARIANTS
    name, payload = rng.choice(variants)
    return True, payload, name


def build_parameter_sets() -> tuple[list[dict[str, object]], Counter[str]]:
    rng = random.Random(SEED)
    parameter_sets: list[dict[str, object]] = []
    seen: set[str] = set()
    tags: Counter[str] = Counter()

    def append_parameter_sets(
        *,
        phase: str,
        finance_name: str,
        finance_payload: dict[str, object],
        use_rps: bool,
        rps_threshold: int,
        cup_group: str,
        repeats: int,
    ) -> None:
        attempts = 0
        accepted = 0
        while accepted < repeats:
            attempts += 1
            if attempts > 200:
                rps_label = rps_threshold if use_rps else "off"
                raise RuntimeError(
                    f"Unable to produce enough unique sets for "
                    f"{phase}/{finance_name}/{rps_label}/{cup_group}."
                )
            use_cup, cup_payload, cup_name = _choose_cup(rng, cup_group)
            selected_windows = rng.choice(RPS_WINDOWS)
            raw = {
                "use_rps": use_rps,
                "rps_threshold": rps_threshold,
                "selected_rps_windows": selected_windows,
                "min_rps_windows_passing": rng.choice(
                    [1, min(2, len(selected_windows))]
                ),
                "use_cup_handle": use_cup,
                "cup_handle_params": cup_payload,
                "fundamental_growth_params": finance_payload,
                "holding_days": rng.choice(HOLDING_DAYS),
                "stop_loss_pct": rng.choice(STOP_LOSSES),
                "take_profit_pct": None,
                "rps_exit_threshold": rng.choice(RPS_EXITS) if use_rps else None,
                "portfolio_cap": 20,
                "position_weight_pct": "0.0500",
                "initial_capital": "100000.00",
                "position_size_amount": "5000.00",
                "allow_reentry_while_open": False,
                "entry_delay_days": rng.choice(ENTRY_DELAYS),
                "entry_deferral_window_days": 5,
            }
            normalized = _normalize_parameter_set(raw)
            parameter_hash = stable_parameter_hash(normalized)
            if parameter_hash in seen:
                continue
            seen.add(parameter_hash)
            parameter_sets.append(normalized)
            tags[f"phase:{phase}"] += 1
            tags[f"finance:{finance_name}"] += 1
            tags[f"rps:{rps_threshold if use_rps else 'off'}"] += 1
            tags[f"cup_group:{cup_group}"] += 1
            tags[f"cup:{cup_name}"] += 1
            accepted += 1

    for finance_name, finance_payload in FUNDAMENTAL_MODES:
        for rps_threshold in PRIMARY_RPS_THRESHOLDS:
            for cup_group in PRIMARY_CUP_GROUPS:
                append_parameter_sets(
                    phase="primary_rps70_floor",
                    finance_name=finance_name,
                    finance_payload=finance_payload,
                    use_rps=True,
                    rps_threshold=rps_threshold,
                    cup_group=cup_group,
                    repeats=REPEATS_PER_CELL,
                )

    for finance_name, finance_payload in FUNDAMENTAL_MODES:
        if finance_name not in NO_RPS_CONTROL_FINANCE_NAMES:
            continue
        for cup_group in NO_RPS_CONTROL_CUP_GROUPS:
            append_parameter_sets(
                phase="no_rps_control",
                finance_name=finance_name,
                finance_payload=finance_payload,
                use_rps=False,
                rps_threshold=0,
                cup_group=cup_group,
                repeats=NO_RPS_CONTROL_REPEATS,
            )

    rng.shuffle(parameter_sets)
    return parameter_sets, tags


def main() -> None:
    market = normalize_market("us")
    parameter_sets, tags = build_parameter_sets()
    design = {
        "name": "finance_core_rps70_floor_reference",
        "balanced_cells": {
            "fundamental_modes": len(FUNDAMENTAL_MODES),
            "rps_thresholds": PRIMARY_RPS_THRESHOLDS,
            "rps_floor": min(PRIMARY_RPS_THRESHOLDS),
            "cup_groups": PRIMARY_CUP_GROUPS,
            "repeats_per_cell": REPEATS_PER_CELL,
        },
        "no_rps_controls": {
            "finance_modes": sorted(NO_RPS_CONTROL_FINANCE_NAMES),
            "cup_groups": NO_RPS_CONTROL_CUP_GROUPS,
            "repeats_per_cell": NO_RPS_CONTROL_REPEATS,
        },
        "train": "2018-01-01..2022-12-31",
        "validation": "2023-01-01..2026-04-30",
        "account": {
            "initial_capital": "100000.00",
            "position_size_amount": "5000.00",
            "portfolio_cap": 20,
        },
        "counts": dict(sorted(tags.items())),
    }
    parameter_space = {
        "_optimization": {
            "search_mode": "random",
            "random_seed": SEED,
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
        run = OptimizationRun(
            market=market,
            train_start_date=date(2018, 1, 1),
            train_end_date=date(2022, 12, 31),
            validation_start_date=date(2023, 1, 1),
            validation_end_date=date(2026, 4, 30),
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
