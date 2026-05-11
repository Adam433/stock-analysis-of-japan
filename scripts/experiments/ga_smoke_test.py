from __future__ import annotations

from datetime import date

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.backtests.models import GaEvent
from stockanalyse_api.services.genetic_optimizer import create_ga_run, execute_ga_run, serialize_ga_run
from stockanalyse_api.services.strategy_parameters import load_json


def main() -> None:
    with SessionLocal() as session:
        run = create_ga_run(
            session,
            market="us",
            train_start_date=date(2026, 1, 1),
            train_end_date=date(2026, 4, 30),
            objective="spy_alpha",
            gene_space={
                "rps_threshold": [70, 80],
                "selected_rps_windows": [[120, 250]],
                "min_rps_windows_passing": [1],
                "rps_exit_threshold": [80],
                "stop_loss_pct": ["-0.08"],
                "use_cup_handle": [False],
                "portfolio_cap": [20],
                "position_weight_pct": ["0.05"],
            },
            initial_population=[
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
            ],
            fitness_config={
                "require_complete_benchmark": True,
                "reuse_parameter_window_evaluations": True,
                "stagnation_patience": 1,
            },
            population_size=2,
            max_generations=1,
            random_seed=20260511,
        )
        print(f"created ga run #{run.id}", flush=True)
        completed = execute_ga_run(session, int(run.id))
        print(serialize_ga_run(completed), flush=True)
        events = (
            session.query(GaEvent)
            .filter(GaEvent.ga_run_id == completed.id)
            .order_by(GaEvent.id.asc())
            .all()
        )
        for event in events:
            print(
                {
                    "event_type": event.event_type,
                    "generation": event.generation,
                    "payload": load_json(event.event_json, default={}),
                },
                flush=True,
            )


if __name__ == "__main__":
    main()
