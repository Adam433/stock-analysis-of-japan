from __future__ import annotations

import os
from datetime import date

from sqlalchemy import select

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.backtests.models import GaEvent, GaIndividual
from stockanalyse_api.services.genetic_optimizer import create_ga_run, execute_ga_run, serialize_ga_run
from stockanalyse_api.services.strategy_parameters import (
    DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS,
    load_json,
)


POPULATION_SIZE = int(os.environ.get("STOCKANALYSE_GA_OVERNIGHT_POPULATION", "12"))
MAX_GENERATIONS = int(os.environ.get("STOCKANALYSE_GA_OVERNIGHT_GENERATIONS", "18"))
RANDOM_SEED = int(os.environ.get("STOCKANALYSE_GA_OVERNIGHT_RANDOM_SEED", "20260512"))

QUALITY_LIGHT = {
    **DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS.to_dict(),
    "max_pe": None,
    "max_pb": None,
    "min_growth_count": 1,
    "effective_min_growth_count": 1,
    "require_positive_operating_cash_flow": False,
    "require_positive_free_cash_flow": False,
    "min_operating_cash_flow_growth_count": None,
}
GROWTH_OCF = DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS.to_dict()
VALUE_QUALITY = {
    **DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS.to_dict(),
    "max_pe": "45",
    "max_pb": "12",
}
STRICT_CASHFLOW = {
    **DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS.to_dict(),
    "max_pe": "60",
    "max_pb": "15",
    "require_positive_free_cash_flow": True,
    "min_operating_cash_flow_growth_count": 2,
}

LOOSE_CUP = {
    "lookback_days": 750,
    "min_cup_depth_pct": "5",
    "max_cup_depth_pct": "55",
    "min_cup_duration": 35,
    "max_cup_duration": 330,
    "min_handle_pullback_pct": "1",
    "max_handle_pullback_pct": "25",
    "min_handle_duration": 3,
    "max_handle_duration": 60,
    "require_prior_uptrend": False,
    "require_breakout_volume": False,
}

EVALUATION_WINDOWS = [
    {
        "name": "wf_2021_2022_to_2023",
        "train_start_date": "2021-01-01",
        "train_end_date": "2022-12-31",
        "validation_start_date": "2023-01-01",
        "validation_end_date": "2023-12-31",
    },
    {
        "name": "wf_2022_2023_to_2024",
        "train_start_date": "2022-01-01",
        "train_end_date": "2023-12-31",
        "validation_start_date": "2024-01-01",
        "validation_end_date": "2024-12-31",
    },
    {
        "name": "wf_2023_2024_to_2025",
        "train_start_date": "2023-01-01",
        "train_end_date": "2024-12-31",
        "validation_start_date": "2025-01-01",
        "validation_end_date": "2025-12-31",
    },
    {
        "name": "wf_2024_2025_to_2026",
        "train_start_date": "2024-01-01",
        "train_end_date": "2025-12-31",
        "validation_start_date": "2026-01-01",
        "validation_end_date": "2026-04-30",
    },
]

GENE_SPACE = {
    "rps_threshold": [70, 75, 80, 85],
    "selected_rps_windows": [[120, 250], [50, 120, 250]],
    "min_rps_windows_passing": [1, 2],
    "rps_exit_threshold": [None, 60, 65, 70, 75],
    "stop_loss_pct": ["-0.06", "-0.08", "-0.10"],
    "holding_days": [None, 130],
    "use_cup_handle": [False, True],
    "cup_handle_params": [LOOSE_CUP],
    "portfolio_cap": [15, 20],
    "position_weight_pct": ["0.05"],
    "fundamental_growth_params": [QUALITY_LIGHT, GROWTH_OCF, VALUE_QUALITY, STRICT_CASHFLOW],
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
        {"enabled": True, "symbol": "SPY", "lookback_days": 120, "min_excess_return_pct": "0"},
        {"enabled": True, "symbol": "SPY", "lookback_days": 250, "min_excess_return_pct": "0"},
    ],
}

INITIAL_POPULATION = [
    {
        "rps_threshold": 70,
        "selected_rps_windows": [120, 250],
        "min_rps_windows_passing": 1,
        "rps_exit_threshold": 65,
        "stop_loss_pct": "-0.08",
        "holding_days": None,
        "use_cup_handle": False,
        "portfolio_cap": 20,
        "position_weight_pct": "0.05",
        "fundamental_growth_params": QUALITY_LIGHT,
    },
    {
        "rps_threshold": 80,
        "selected_rps_windows": [50, 120, 250],
        "min_rps_windows_passing": 2,
        "rps_exit_threshold": 75,
        "stop_loss_pct": "-0.08",
        "holding_days": None,
        "use_cup_handle": False,
        "portfolio_cap": 20,
        "position_weight_pct": "0.05",
        "fundamental_growth_params": GROWTH_OCF,
    },
    {
        "rps_threshold": 75,
        "selected_rps_windows": [120, 250],
        "min_rps_windows_passing": 1,
        "rps_exit_threshold": 70,
        "stop_loss_pct": "-0.08",
        "holding_days": None,
        "use_cup_handle": False,
        "portfolio_cap": 20,
        "position_weight_pct": "0.05",
        "fundamental_growth_params": VALUE_QUALITY,
        "market_filter_params": {"enabled": True, "symbol": "SPY", "require_price_above_sma": True, "price_sma_days": 200},
        "relative_strength_params": {"enabled": True, "symbol": "SPY", "lookback_days": 120, "min_excess_return_pct": "0"},
    },
    {
        "rps_threshold": 75,
        "selected_rps_windows": [50, 120, 250],
        "min_rps_windows_passing": 1,
        "rps_exit_threshold": 70,
        "stop_loss_pct": "-0.06",
        "holding_days": 130,
        "use_cup_handle": True,
        "cup_handle_params": LOOSE_CUP,
        "portfolio_cap": 20,
        "position_weight_pct": "0.05",
        "fundamental_growth_params": STRICT_CASHFLOW,
    },
]


def main() -> None:
    with SessionLocal() as session:
        run = create_ga_run(
            session,
            market="us",
            train_start_date=date(2021, 1, 1),
            train_end_date=date(2026, 4, 30),
            holdout_start_date=date(2026, 3, 1),
            holdout_end_date=date(2026, 4, 30),
            objective="spy_alpha",
            gene_space=GENE_SPACE,
            initial_population=INITIAL_POPULATION,
            fitness_config={
                "evaluation_windows": EVALUATION_WINDOWS,
                "require_complete_benchmark": True,
                "require_all_windows": True,
                "reuse_parameter_window_evaluations": True,
                "prefer_broad_candidate_cache": True,
                "max_broad_candidate_cache_dates": 260,
                "elite_count": 3,
                "mutation_rate": "0.25",
                "stagnation_patience": 8,
            },
            population_size=POPULATION_SIZE,
            max_generations=MAX_GENERATIONS,
            random_seed=RANDOM_SEED,
        )
        print(
            f"created ga overnight run #{run.id}: population={POPULATION_SIZE}, "
            f"generations={MAX_GENERATIONS}, windows={len(EVALUATION_WINDOWS)}",
            flush=True,
        )
        completed = execute_ga_run(session, int(run.id))
        print(serialize_ga_run(completed), flush=True)
        events = session.execute(
            select(GaEvent)
            .where(GaEvent.ga_run_id == completed.id, GaEvent.event_type == "generation_summary")
            .order_by(GaEvent.id.asc())
        ).scalars()
        for event in events:
            print(load_json(event.event_json, default={}), flush=True)
        if completed.best_individual_id is not None:
            best = session.get(GaIndividual, completed.best_individual_id)
            if best is not None:
                print(
                    {
                        "best_individual_id": best.id,
                        "fitness": str(best.fitness) if best.fitness is not None else None,
                        "parameters": load_json(best.parameters_json, default={}),
                        "metrics": load_json(best.metrics_json, default={}),
                    },
                    flush=True,
                )


if __name__ == "__main__":
    main()
