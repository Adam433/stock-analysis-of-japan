from __future__ import annotations

import os
from datetime import date

from sqlalchemy import select

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.backtests.models import GaEvent, GaIndividual
from stockanalyse_api.services.genetic_optimizer import create_ga_run, execute_ga_run, serialize_ga_run
from stockanalyse_api.services.strategy_parameters import load_json


POPULATION_SIZE = int(os.environ.get("STOCKANALYSE_GA_TRIAL_POPULATION", "2"))
MAX_GENERATIONS = int(os.environ.get("STOCKANALYSE_GA_TRIAL_GENERATIONS", "2"))
RANDOM_SEED = int(os.environ.get("STOCKANALYSE_GA_TRIAL_RANDOM_SEED", "20260511"))

EVALUATION_WINDOWS = [
    {
        "name": "wf_2025_sep_oct",
        "train_start_date": "2025-07-01",
        "train_end_date": "2025-08-31",
        "validation_start_date": "2025-09-01",
        "validation_end_date": "2025-10-31",
    },
    {
        "name": "wf_2025_nov_dec",
        "train_start_date": "2025-09-01",
        "train_end_date": "2025-10-31",
        "validation_start_date": "2025-11-01",
        "validation_end_date": "2025-12-31",
    },
    {
        "name": "wf_2026_jan_feb",
        "train_start_date": "2025-11-01",
        "train_end_date": "2025-12-31",
        "validation_start_date": "2026-01-01",
        "validation_end_date": "2026-02-28",
    },
]

GENE_SPACE = {
    "rps_threshold": [70, 80],
    "selected_rps_windows": [[120, 250], [50, 120, 250]],
    "min_rps_windows_passing": [1],
    "rps_exit_threshold": [75, 80],
    "stop_loss_pct": ["-0.08"],
    "use_cup_handle": [False],
    "portfolio_cap": [20],
    "position_weight_pct": ["0.05"],
}

INITIAL_POPULATION = [
    {
        "rps_threshold": 70,
        "selected_rps_windows": [120, 250],
        "min_rps_windows_passing": 1,
        "rps_exit_threshold": 80,
        "stop_loss_pct": "-0.08",
        "use_cup_handle": False,
        "portfolio_cap": 20,
        "position_weight_pct": "0.05",
    },
    {
        "rps_threshold": 80,
        "selected_rps_windows": [120, 250],
        "min_rps_windows_passing": 1,
        "rps_exit_threshold": 80,
        "stop_loss_pct": "-0.08",
        "use_cup_handle": False,
        "portfolio_cap": 20,
        "position_weight_pct": "0.05",
    },
    {
        "rps_threshold": 70,
        "selected_rps_windows": [50, 120, 250],
        "min_rps_windows_passing": 1,
        "rps_exit_threshold": 75,
        "stop_loss_pct": "-0.08",
        "use_cup_handle": False,
        "portfolio_cap": 20,
        "position_weight_pct": "0.05",
    },
    {
        "rps_threshold": 80,
        "selected_rps_windows": [50, 120, 250],
        "min_rps_windows_passing": 1,
        "rps_exit_threshold": 75,
        "stop_loss_pct": "-0.08",
        "use_cup_handle": False,
        "portfolio_cap": 20,
        "position_weight_pct": "0.05",
    },
]


def _print_generation_events(session, ga_run_id: int) -> None:
    events = session.execute(
        select(GaEvent).where(GaEvent.ga_run_id == ga_run_id).order_by(GaEvent.id.asc())
    ).scalars()
    for event in events:
        payload = load_json(event.event_json, default={})
        print(
            {
                "event_type": event.event_type,
                "generation": event.generation,
                "best_fitness": payload.get("best_fitness"),
                "average_fitness": payload.get("average_fitness"),
                "performance": payload.get("performance"),
                "failed_count": payload.get("failed_count"),
                "failure_rate": payload.get("failure_rate"),
            },
            flush=True,
        )


def _print_best_individual(session, best_individual_id: int | None) -> None:
    if best_individual_id is None:
        return
    best = session.get(GaIndividual, best_individual_id)
    if best is None:
        return
    print(
        {
            "best_individual_id": best.id,
            "fitness": str(best.fitness) if best.fitness is not None else None,
            "parameters": load_json(best.parameters_json, default={}),
            "metrics": load_json(best.metrics_json, default={}),
        },
        flush=True,
    )


def main() -> None:
    with SessionLocal() as session:
        run = create_ga_run(
            session,
            market="us",
            train_start_date=date(2025, 7, 1),
            train_end_date=date(2026, 2, 28),
            holdout_start_date=date(2026, 3, 1),
            holdout_end_date=date(2026, 4, 30),
            objective="spy_alpha",
            gene_space=GENE_SPACE,
            initial_population=INITIAL_POPULATION,
            fitness_config={
                "evaluation_windows": EVALUATION_WINDOWS,
                "require_complete_benchmark": True,
                "reuse_parameter_window_evaluations": True,
                "elite_count": min(2, POPULATION_SIZE),
                "mutation_rate": "0.20",
                "stagnation_patience": 2,
            },
            population_size=POPULATION_SIZE,
            max_generations=MAX_GENERATIONS,
            random_seed=RANDOM_SEED,
        )
        print(
            f"created ga walk-forward run #{run.id}: "
            f"population={POPULATION_SIZE}, generations={MAX_GENERATIONS}, "
            f"windows={len(EVALUATION_WINDOWS)}",
            flush=True,
        )
        completed = execute_ga_run(session, int(run.id))
        print(serialize_ga_run(completed), flush=True)
        _print_generation_events(session, int(completed.id))
        _print_best_individual(session, completed.best_individual_id)


if __name__ == "__main__":
    main()
