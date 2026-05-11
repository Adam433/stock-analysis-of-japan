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
    STRICT_CUP_VARIANTS,
)


MAX_WORKERS = int(os.environ.get("STOCKANALYSE_MARKET_FILTER_EXPERIMENT_WORKERS", "2"))
MAX_TASKS_PER_CHILD = int(
    os.environ.get("STOCKANALYSE_MARKET_FILTER_EXPERIMENT_MAX_TASKS_PER_CHILD", "24")
)
MARKET_FILTER_SYMBOL = os.environ.get("STOCKANALYSE_MARKET_FILTER_SYMBOL", "SPY").upper()


def _fundamental_payloads() -> list[tuple[str, dict[str, object]]]:
    selected = {"quality_light_no_valuation", "quality_ocf"}
    return [(name, payload) for name, payload in FUNDAMENTAL_MODES if name in selected]


def _cup_payloads() -> list[tuple[str, bool, dict[str, object]]]:
    loose_no_prior = next(payload for name, payload in LOOSE_CUP_VARIANTS if name == "loose_no_prior")
    base_volume_confirm = next(
        payload for name, payload in STRICT_CUP_VARIANTS if name == "base_volume_confirm"
    )
    return [
        ("none", False, {}),
        ("loose_no_prior", True, loose_no_prior),
        ("base_volume_confirm", True, base_volume_confirm),
    ]


def _market_filters() -> list[tuple[str, dict[str, object]]]:
    return [
        ("none", {"enabled": False, "symbol": MARKET_FILTER_SYMBOL}),
        (
            "spy_200ma",
            {
                "enabled": True,
                "symbol": MARKET_FILTER_SYMBOL,
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
                "symbol": MARKET_FILTER_SYMBOL,
                "require_price_above_sma": True,
                "price_sma_days": 200,
                "require_fast_sma_above_slow_sma": True,
                "fast_sma_days": 50,
                "slow_sma_days": 200,
            },
        ),
    ]


def build_parameter_sets() -> tuple[list[dict[str, object]], Counter[str]]:
    parameter_sets: list[dict[str, object]] = []
    seen: set[str] = set()
    tags: Counter[str] = Counter()
    for finance_name, finance_payload in _fundamental_payloads():
        for rps_threshold in [80, 85]:
            for selected_windows in [[120, 250], [50, 120, 250]]:
                for cup_name, use_cup, cup_payload in _cup_payloads():
                    for market_filter_name, market_filter_payload in _market_filters():
                        raw = {
                            "use_rps": True,
                            "rps_threshold": rps_threshold,
                            "selected_rps_windows": selected_windows,
                            "min_rps_windows_passing": 1,
                            "use_cup_handle": use_cup,
                            "cup_handle_params": cup_payload,
                            "fundamental_growth_params": finance_payload,
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
                            "entry_delay_days": 0,
                            "entry_deferral_window_days": 5,
                        }
                        normalized = _normalize_parameter_set(raw)
                        parameter_hash = stable_parameter_hash(normalized)
                        if parameter_hash in seen:
                            continue
                        seen.add(parameter_hash)
                        parameter_sets.append(normalized)
                        tags[f"finance:{finance_name}"] += 1
                        tags[f"rps:{rps_threshold}"] += 1
                        tags[f"windows:{'+'.join(str(item) for item in selected_windows)}"] += 1
                        tags[f"cup:{cup_name}"] += 1
                        tags[f"market_filter:{market_filter_name}"] += 1
    return parameter_sets, tags


def main() -> None:
    market = normalize_market("us")
    parameter_sets, tags = build_parameter_sets()
    design = {
        "name": "market_filter_experiment_d",
        "description": "Experiment D: paired market-regime filter test across 24 core strategy cells.",
        "market_filters": ["none", "spy_200ma", "spy_50_200ma"],
        "market_filter_symbol": MARKET_FILTER_SYMBOL,
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
