from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import stockanalyse_api.domain.backtests.models  # noqa: F401
import stockanalyse_api.domain.fundamentals.models  # noqa: F401
import stockanalyse_api.domain.indicators.models  # noqa: F401
import stockanalyse_api.domain.instruments.models  # noqa: F401
import stockanalyse_api.domain.market_data.models  # noqa: F401
import stockanalyse_api.domain.operations.models  # noqa: F401
import stockanalyse_api.domain.screens.models  # noqa: F401
import stockanalyse_api.domain.watchlists.models  # noqa: F401
from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.backtests.models import GaEvent, GaIndividual
from stockanalyse_api.services.genetic_optimizer import (
    create_ga_run,
    execute_ga_run,
    list_ga_events,
    list_ga_individuals,
    list_ga_runs,
    serialize_ga_event,
    serialize_ga_individual,
    serialize_ga_run,
)
from stockanalyse_api.services.strategy_parameters import load_json


class GeneticOptimizerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        Base.metadata.create_all(self.engine)
        self.session_factory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            future=True,
        )

    def tearDown(self) -> None:
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def test_create_ga_run_persists_configuration(self) -> None:
        with self.session_factory() as session:
            run = create_ga_run(
                session,
                train_start_date=date(2020, 1, 1),
                train_end_date=date(2022, 12, 31),
                validation_start_date=date(2023, 1, 1),
                validation_end_date=date(2024, 12, 31),
                population_size=4,
                max_generations=2,
                random_seed=7,
            )
            payload = serialize_ga_run(run)
            runs = list_ga_runs(session)
            run_id = run.id

        self.assertEqual(payload["status"], "running")
        self.assertEqual(payload["total_individuals"], 8)
        self.assertEqual(payload["random_seed"], 7)
        self.assertEqual([item.id for item in runs], [run_id])

    def test_execute_ga_run_evaluates_generations_and_tracks_best(self) -> None:
        def fake_evaluator(*args, **kwargs):
            parameters = kwargs["parameters"]
            score = Decimal("0.200000") if parameters["rps_threshold"] == 80 else Decimal("0.050000")
            return {
                "parameters": parameters,
                "train_metrics": {
                    "completed_trades": 12,
                    "spy_average_trade_excess_return": str(score),
                    "spy_trade_benchmark_count": 12,
                },
                "validation_metrics": None,
                "train_result": {},
                "validation_result": None,
                "score": score,
                "status": "completed",
                "failure_reason": None,
            }

        gene_space = {
            "rps_threshold": [70, 80],
            "selected_rps_windows": [[120, 250]],
            "min_rps_windows_passing": [1],
            "stop_loss_pct": ["-0.08", "-0.10"],
            "use_cup_handle": [False],
        }
        with self.session_factory() as session:
            run = create_ga_run(
                session,
                train_start_date=date(2020, 1, 1),
                train_end_date=date(2022, 12, 31),
                validation_start_date=date(2023, 1, 1),
                validation_end_date=date(2024, 12, 31),
                gene_space=gene_space,
                initial_population=[{"rps_threshold": 70}, {"rps_threshold": 80}],
                fitness_config={
                    "require_complete_benchmark": False,
                    "mutation_rate": "0.50",
                    "stagnation_patience": 5,
                },
                population_size=4,
                max_generations=2,
                random_seed=11,
            )

            with patch(
                "stockanalyse_api.services.genetic_optimizer.evaluate_strategy_parameter_set",
                side_effect=fake_evaluator,
            ):
                completed = execute_ga_run(session, run.id)

            best = session.get(GaIndividual, completed.best_individual_id)
            events = session.execute(select(GaEvent)).scalars().all()
            completed_status = completed.status
            completed_generations = completed.completed_generations
            completed_individuals = completed.completed_individuals
            best_parameters = load_json(best.parameters_json) if best is not None else None
            generation_event_count = sum(
                1 for event in events if event.event_type == "generation_summary"
            )
            individual_event_count = sum(
                1 for event in events if event.event_type == "individual_evaluated"
            )
            listed_individuals = list_ga_individuals(session, run_id=run.id, limit=2)
            listed_events = list_ga_events(session, run_id=run.id, limit=1)
            serialized_individual = serialize_ga_individual(
                listed_individuals[0],
                include_evaluation=True,
            )
            serialized_event = serialize_ga_event(listed_events[0])

        self.assertEqual(completed_status, "completed")
        self.assertEqual(completed_generations, 2)
        self.assertEqual(completed_individuals, 8)
        self.assertIsNotNone(best_parameters)
        assert best_parameters is not None
        self.assertEqual(best_parameters["rps_threshold"], 80)
        self.assertEqual(generation_event_count, 2)
        self.assertEqual(individual_event_count, 8)
        self.assertEqual(len(listed_individuals), 2)
        self.assertIn("evaluation", serialized_individual)
        self.assertEqual(serialized_event["event_type"], "individual_evaluated")

    def test_execute_ga_run_persists_failed_individuals(self) -> None:
        def fake_evaluator(*args, **kwargs):
            return {
                "parameters": kwargs["parameters"],
                "train_metrics": None,
                "validation_metrics": None,
                "train_result": None,
                "validation_result": None,
                "score": None,
                "status": "failed",
                "failure_reason": "SyntheticFailure",
            }

        with self.session_factory() as session:
            run = create_ga_run(
                session,
                train_start_date=date(2020, 1, 1),
                train_end_date=date(2022, 12, 31),
                fitness_config={"require_complete_benchmark": False},
                population_size=2,
                max_generations=1,
                random_seed=3,
            )

            with patch(
                "stockanalyse_api.services.genetic_optimizer.evaluate_strategy_parameter_set",
                side_effect=fake_evaluator,
            ):
                completed = execute_ga_run(session, run.id)

            individuals = session.execute(select(GaIndividual)).scalars().all()
            completed_status = completed.status
            failed_individuals = completed.failed_individuals
            best_individual_id = completed.best_individual_id

        self.assertEqual(completed_status, "completed")
        self.assertEqual(failed_individuals, 2)
        self.assertIsNone(best_individual_id)
        self.assertEqual({item.status for item in individuals}, {"failed"})

    def test_execute_ga_run_aggregates_multiple_evaluation_windows(self) -> None:
        def fake_evaluator(*args, **kwargs):
            parameters = kwargs["parameters"]
            base_score = Decimal("0.200000") if parameters["rps_threshold"] == 80 else Decimal("0.050000")
            return {
                "parameters": parameters,
                "train_metrics": {
                    "completed_trades": 25,
                    "max_drawdown": "-0.080000",
                    "equity_curve": [{"date": "2018-01-02", "equity": "1.010000"}],
                },
                "validation_metrics": {
                    "completed_trades": 25,
                    "max_drawdown": "-0.100000",
                    "equity_curve": [{"date": "2019-01-02", "equity": "1.020000"}],
                    "yearly_returns": {"2021": "0.050000", "2022": "0.060000"},
                },
                "train_result": {"large_detail": [1, 2, 3]},
                "validation_result": {"large_detail": [4, 5, 6]},
                "score": base_score,
                "status": "completed",
                "failure_reason": None,
            }

        windows = [
            {
                "name": "wf_1",
                "train_start_date": "2014-01-01",
                "train_end_date": "2018-12-31",
                "validation_start_date": "2019-01-01",
                "validation_end_date": "2020-12-31",
            },
            {
                "name": "wf_2",
                "train_start_date": "2016-01-01",
                "train_end_date": "2020-12-31",
                "validation_start_date": "2021-01-01",
                "validation_end_date": "2022-12-31",
            },
            {
                "name": "wf_3",
                "train_start_date": "2018-01-01",
                "train_end_date": "2022-12-31",
                "validation_start_date": "2023-01-01",
                "validation_end_date": "2024-12-31",
            },
        ]
        with self.session_factory() as session:
            run = create_ga_run(
                session,
                train_start_date=date(2014, 1, 1),
                train_end_date=date(2022, 12, 31),
                validation_start_date=date(2023, 1, 1),
                validation_end_date=date(2024, 12, 31),
                gene_space={
                    "rps_threshold": [70, 80],
                    "selected_rps_windows": [[120, 250]],
                    "use_cup_handle": [False],
                },
                initial_population=[{"rps_threshold": 70}, {"rps_threshold": 80}],
                fitness_config={
                    "evaluation_windows": windows,
                    "require_complete_benchmark": False,
                },
                population_size=2,
                max_generations=1,
                random_seed=5,
            )

            with patch(
                "stockanalyse_api.services.genetic_optimizer.evaluate_strategy_parameter_set",
                side_effect=fake_evaluator,
            ) as mocked_evaluator:
                completed = execute_ga_run(session, run.id)

            best = session.get(GaIndividual, completed.best_individual_id)
            event = session.execute(
                select(GaEvent).where(GaEvent.event_type == "generation_summary")
            ).scalar_one()
            best_metrics = load_json(best.metrics_json) if best is not None else None
            event_payload = load_json(event.event_json)

        self.assertEqual(mocked_evaluator.call_count, 6)
        self.assertEqual(completed.status, "completed")
        self.assertIsNotNone(best_metrics)
        assert best_metrics is not None
        self.assertEqual(best_metrics["aggregate"]["window_count"], 3)
        self.assertEqual(best_metrics["aggregate"]["completed_window_count"], 3)
        self.assertEqual(len(best_metrics["windows"]), 3)
        self.assertNotIn("equity_curve", best_metrics["windows"][0]["train_metrics"])
        self.assertNotIn("equity_curve", best_metrics["windows"][0]["validation_metrics"])
        self.assertEqual(
            best_metrics["windows"][0]["validation_metrics"]["yearly_returns"],
            {"2021": "0.050000", "2022": "0.060000"},
        )
        self.assertEqual(event_payload["evaluation_window_count"], 3)
        self.assertEqual(load_json(best.parameters_json)["rps_threshold"], 80)

    def test_execute_ga_run_records_holdout_for_best_without_affecting_fitness(self) -> None:
        def fake_evaluator(*args, **kwargs):
            parameters = kwargs["parameters"]
            score = Decimal("0.200000") if parameters["rps_threshold"] == 80 else Decimal("0.050000")
            if kwargs["validation_start_date"] is None:
                score = Decimal("-0.100000")
            return {
                "parameters": parameters,
                "train_metrics": {"completed_trades": 25, "max_drawdown": "-0.080000"},
                "validation_metrics": None,
                "train_result": {},
                "validation_result": None,
                "score": score,
                "status": "completed",
                "failure_reason": None,
            }

        with self.session_factory() as session:
            run = create_ga_run(
                session,
                train_start_date=date(2018, 1, 1),
                train_end_date=date(2022, 12, 31),
                validation_start_date=date(2023, 1, 1),
                validation_end_date=date(2024, 12, 31),
                holdout_start_date=date(2025, 1, 1),
                holdout_end_date=date(2026, 4, 30),
                gene_space={
                    "rps_threshold": [70, 80],
                    "selected_rps_windows": [[120, 250]],
                    "use_cup_handle": [False],
                },
                initial_population=[{"rps_threshold": 70}, {"rps_threshold": 80}],
                fitness_config={"require_complete_benchmark": False},
                population_size=2,
                max_generations=1,
                random_seed=5,
            )

            with patch(
                "stockanalyse_api.services.genetic_optimizer.evaluate_strategy_parameter_set",
                side_effect=fake_evaluator,
            ) as mocked_evaluator:
                completed = execute_ga_run(session, run.id)

            best = session.get(GaIndividual, completed.best_individual_id)
            events = session.execute(select(GaEvent).order_by(GaEvent.id)).scalars().all()
            best_evaluation = load_json(best.evaluation_json) if best is not None else None

        self.assertEqual(mocked_evaluator.call_count, 3)
        self.assertEqual(completed.status, "completed")
        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best.fitness, Decimal("0.200000"))
        self.assertEqual(
            [event.event_type for event in events],
            ["individual_evaluated", "individual_evaluated", "generation_summary", "holdout_evaluation"],
        )
        self.assertIsNotNone(best_evaluation)
        assert best_evaluation is not None
        self.assertEqual(best_evaluation["holdout_evaluation"]["score"], "-0.100000")

    def test_execute_ga_run_reuses_parameter_window_evaluation_cache(self) -> None:
        def fake_evaluator(*args, **kwargs):
            parameters = kwargs["parameters"]
            return {
                "parameters": parameters,
                "train_metrics": {"completed_trades": 25, "max_drawdown": "-0.080000"},
                "validation_metrics": None,
                "train_result": {},
                "validation_result": None,
                "score": Decimal("0.200000") if parameters["rps_threshold"] == 80 else Decimal("0.050000"),
                "status": "completed",
                "failure_reason": None,
            }

        with self.session_factory() as session:
            run = create_ga_run(
                session,
                train_start_date=date(2018, 1, 1),
                train_end_date=date(2022, 12, 31),
                gene_space={
                    "rps_threshold": [70, 80],
                    "selected_rps_windows": [[120, 250]],
                    "use_cup_handle": [False],
                },
                initial_population=[{"rps_threshold": 70}, {"rps_threshold": 80}],
                fitness_config={
                    "require_complete_benchmark": False,
                    "mutation_rate": "0",
                    "elite_count": 2,
                    "stagnation_patience": 5,
                },
                population_size=2,
                max_generations=2,
                random_seed=5,
            )

            with patch(
                "stockanalyse_api.services.genetic_optimizer.evaluate_strategy_parameter_set",
                side_effect=fake_evaluator,
            ) as mocked_evaluator:
                completed = execute_ga_run(session, run.id)

            events = session.execute(
                select(GaEvent)
                .where(GaEvent.event_type == "generation_summary")
                .order_by(GaEvent.id)
            ).scalars().all()
            event_payloads = [load_json(event.event_json) for event in events]

        self.assertEqual(completed.status, "completed")
        self.assertEqual(mocked_evaluator.call_count, 2)
        self.assertEqual(event_payloads[0]["performance"]["window_cache_misses"], 2)
        self.assertEqual(event_payloads[0]["performance"]["window_cache_hits"], 0)
        self.assertEqual(event_payloads[1]["performance"]["window_cache_hits"], 2)
        self.assertEqual(event_payloads[1]["performance"]["window_cache_misses"], 0)
        self.assertEqual(event_payloads[1]["performance"]["cache_sizes"]["ga_window_evaluations"], 2)
