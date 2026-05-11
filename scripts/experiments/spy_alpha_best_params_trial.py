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


MAX_WORKERS = int(os.environ.get("STOCKANALYSE_SPY_ALPHA_TRIAL_WORKERS", "1"))
MAX_TASKS_PER_CHILD = int(
    os.environ.get("STOCKANALYSE_SPY_ALPHA_TRIAL_MAX_TASKS_PER_CHILD", "6")
)

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


FINANCE_CANDIDATES = [
    "quality_light_no_valuation",
    "value_quality",
]

CUP_CANDIDATE = "loose_no_prior"

MARKET_FILTERS = [
    (
        "none",
        {"enabled": False, "symbol": "SPY"},
    ),
    (
        "spy_200ma",
        {
            "enabled": True,
            "symbol": "SPY",
            "require_price_above_sma": True,
            "price_sma_days": 200,
            "require_fast_sma_above_slow_sma": False,
            "fast_sma_days": 50,
            "slow_sma_days": 200,
        },
    ),
    (
        "spy_50_200ma",
        {
            "enabled": True,
            "symbol": "SPY",
            "require_price_above_sma": True,
            "price_sma_days": 200,
            "require_fast_sma_above_slow_sma": True,
            "fast_sma_days": 50,
            "slow_sma_days": 200,
        },
    ),
]

RELATIVE_STRENGTH_FILTERS = [
    (
        "none",
        {"enabled": False, "symbol": "SPY"},
    ),
    (
        "spy_120d",
        {
            "enabled": True,
            "symbol": "SPY",
            "lookback_days": 120,
            "min_excess_return_pct": "0",
        },
    ),
    (
        "spy_250d",
        {
            "enabled": True,
            "symbol": "SPY",
            "lookback_days": 250,
            "min_excess_return_pct": "0",
        },
    ),
]


def build_parameter_sets() -> tuple[list[dict[str, object]], Counter[str]]:
    use_cup_handle, cup_payload = _cup_payload(CUP_CANDIDATE)
    parameter_sets: list[dict[str, object]] = []
    seen: set[str] = set()
    tags: Counter[str] = Counter()

    for finance_name in FINANCE_CANDIDATES:
        for market_filter_name, market_filter_payload in MARKET_FILTERS:
            for relative_name, relative_payload in RELATIVE_STRENGTH_FILTERS:
                raw = {
                    "use_rps": True,
                    "rps_threshold": 70,
                    "selected_rps_windows": [50, 120, 250],
                    "min_rps_windows_passing": 1,
                    "use_cup_handle": use_cup_handle,
                    "cup_handle_params": cup_payload,
                    "fundamental_growth_params": _fundamental_payload(finance_name),
                    "holding_days": None,
                    "stop_loss_pct": "-0.08",
                    "take_profit_pct": None,
                    "rps_exit_threshold": 80,
                    "portfolio_cap": 20,
                    "position_weight_pct": "0.0500",
                    "initial_capital": "100000.00",
                    "position_size_amount": "5000.00",
                    "allow_reentry_while_open": False,
                    "market_filter_params": market_filter_payload,
                    "relative_strength_params": relative_payload,
                    "cash_fallback_params": {"enabled": False, "symbol": "SPY"},
                    "entry_delay_days": 0,
                    "entry_deferral_window_days": 5,
                }
                normalized = _normalize_parameter_set(raw)
                normalized["candidate_name"] = (
                    f"{finance_name}__{CUP_CANDIDATE}__{market_filter_name}__{relative_name}"
                )
                parameter_hash = stable_parameter_hash(normalized)
                if parameter_hash in seen:
                    continue
                seen.add(parameter_hash)
                parameter_sets.append(normalized)
                tags[f"finance:{finance_name}"] += 1
                tags[f"cup:{CUP_CANDIDATE}"] += 1
                tags[f"market:{market_filter_name}"] += 1
                tags[f"relative_strength:{relative_name}"] += 1
    return parameter_sets, tags


def main() -> None:
    market = normalize_market("us")
    parameter_sets, tags = build_parameter_sets()
    design = {
        "name": "spy_alpha_best_params_trial",
        "description": (
            "Small single-worker trial around the current best return/balanced candidates. "
            "Uses the satellite-alpha scoring lens: SPY remains the core holding, idle cash "
            "is not converted into SPY, and objective=spy_alpha ranks actual trades by "
            "same-holding-period excess return versus SPY."
        ),
        "finance": FINANCE_CANDIDATES,
        "cup": CUP_CANDIDATE,
        "market_filters": [name for name, _ in MARKET_FILTERS],
        "relative_strength_filters": [name for name, _ in RELATIVE_STRENGTH_FILTERS],
        "fixed_parameters": {
            "rps_threshold": 70,
            "selected_rps_windows": [50, 120, 250],
            "min_rps_windows_passing": 1,
            "rps_exit_threshold": 80,
            "holding_days": None,
            "stop_loss_pct": "-0.08",
            "cash_fallback": "disabled",
        },
        "account": {
            "initial_capital": "100000.00",
            "position_size_amount": "5000.00",
            "portfolio_cap": 20,
        },
        "train": f"{TRAIN_START.isoformat()}..{TRAIN_END.isoformat()}",
        "validation": f"{VALIDATION_START.isoformat()}..{VALIDATION_END.isoformat()}",
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
        run = OptimizationRun(
            market=market,
            train_start_date=TRAIN_START,
            train_end_date=TRAIN_END,
            validation_start_date=VALIDATION_START,
            validation_end_date=VALIDATION_END,
            objective="spy_alpha",
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
        print(f"objective=spy_alpha, max_workers={MAX_WORKERS}", flush=True)
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
