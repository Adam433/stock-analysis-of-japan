from __future__ import annotations

import unittest
from concurrent.futures import Future
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import stockanalyse_api.domain.fundamentals.models  # noqa: F401
import stockanalyse_api.domain.indicators.models  # noqa: F401
import stockanalyse_api.domain.instruments.models  # noqa: F401
import stockanalyse_api.domain.market_data.models  # noqa: F401
import stockanalyse_api.domain.screens.models  # noqa: F401
from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.backtests.models import (
    OptimizationResult,
    OptimizationResultDetailCache,
    StrategyPreset,
)
from stockanalyse_api.services.dashboard import DEFAULT_CUP_HANDLE_PARAMS
from stockanalyse_api.services.dashboard_strategy_backtest import (
    _simulate_trade,
    run_cup_handle_rps_backtest,
)
from stockanalyse_api.services.optimization_backtest import (
    _attach_average_annualized_return,
    _extract_metrics,
    _parallel_parameter_groups,
    _score_metric_pair,
    _score_metrics,
    build_optimization_result_detail,
    build_parameter_sets,
    create_optimization_run,
    dump_json,
    execute_optimization_run,
    list_optimization_results,
    serialize_optimization_result,
    serialize_optimization_run,
    stable_parameter_hash,
)
from stockanalyse_api.services.strategy_presets import (
    activate_strategy_preset,
    delete_strategy_preset,
    duplicate_strategy_preset,
    list_strategy_presets,
    save_strategy_preset,
    update_strategy_preset,
)


class _BacktestResult:
    def __init__(
        self,
        *,
        completed_trades: int,
        average_trade_return: str,
        win_rate: str,
        worst_trade_return: str,
    ) -> None:
        self.completed_trades = completed_trades
        self.average_trade_return = average_trade_return
        self.win_rate = win_rate
        self.worst_trade_return = worst_trade_return

    def to_dict(self) -> dict[str, object]:
        return {
            "signal_dates_evaluated": 10,
            "total_candidates_evaluated": 1000,
            "qualifying_observations": 100,
            "selected_trades": self.completed_trades,
            "completed_trades": self.completed_trades,
            "average_trade_return": self.average_trade_return,
            "median_trade_return": self.average_trade_return,
            "win_rate": self.win_rate,
            "best_trade_return": "0.300000",
            "worst_trade_return": self.worst_trade_return,
        }


def _market_row(trade_date: date, price: str) -> SimpleNamespace:
    value = Decimal(price)
    return SimpleNamespace(
        trade_date=trade_date,
        open=value,
        high=value,
        low=value,
        close=value,
        adj_close=value,
        volume=1000,
        data_status="complete",
    )


def _ohlc_row(
    trade_date: date,
    *,
    open_price: str,
    high: str,
    low: str,
    close: str,
) -> SimpleNamespace:
    return SimpleNamespace(
        trade_date=trade_date,
        open=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        adj_close=Decimal(close),
        volume=1000,
        data_status="complete",
    )


class OptimizationBacktestTests(unittest.TestCase):
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

    def test_parameter_hash_is_stable_for_equivalent_payloads(self) -> None:
        left = {"rps_threshold": 90, "selected_rps_windows": [50, 120]}
        right = {"selected_rps_windows": [50, 120], "rps_threshold": 90}

        self.assertEqual(stable_parameter_hash(left), stable_parameter_hash(right))

    def test_build_parameter_sets_expands_and_deduplicates_grid(self) -> None:
        parameter_sets = build_parameter_sets(
            {
                "rps_threshold": [85, 90],
                "selected_rps_windows": [[50, 120], [50, 120]],
                "holding_days": [60],
            }
        )

        self.assertEqual(len(parameter_sets), 2)
        self.assertEqual({item["rps_threshold"] for item in parameter_sets}, {85, 90})
        self.assertEqual(parameter_sets[0]["selected_rps_windows"], [50, 120])

    def test_build_parameter_sets_requires_fundamentals_by_default(self) -> None:
        parameter_sets = build_parameter_sets({"rps_threshold": [90]})

        fundamentals = parameter_sets[0]["fundamental_growth_params"]

        self.assertTrue(fundamentals["enabled"])
        self.assertEqual(fundamentals["min_years"], 3)
        self.assertEqual(fundamentals["min_growth_count"], 2)
        self.assertTrue(fundamentals["require_positive_net_income"])
        self.assertEqual(fundamentals["max_pe"], "60")
        self.assertEqual(fundamentals["max_pb"], "15")
        self.assertTrue(fundamentals["require_positive_operating_cash_flow"])
        self.assertFalse(fundamentals["require_positive_free_cash_flow"])
        self.assertEqual(fundamentals["min_operating_cash_flow_growth_count"], 1)

    def test_build_parameter_sets_forces_fundamentals_when_payload_disables_it(self) -> None:
        parameter_sets = build_parameter_sets(
            {
                "rps_threshold": [90],
                "fundamental_growth_params": [
                    {
                        "enabled": False,
                        "min_years": 4,
                        "min_growth_count": 1,
                        "min_yoy_growth_pct": "5",
                        "require_positive_net_income": False,
                        "reporting_lag_days": 90,
                    }
                ],
            }
        )

        fundamentals = parameter_sets[0]["fundamental_growth_params"]

        self.assertTrue(fundamentals["enabled"])
        self.assertEqual(fundamentals["min_years"], 4)
        self.assertEqual(fundamentals["min_growth_count"], 1)
        self.assertEqual(fundamentals["min_yoy_growth_pct"], "5")
        self.assertFalse(fundamentals["require_positive_net_income"])
        self.assertEqual(fundamentals["reporting_lag_days"], 90)

    def test_build_parameter_sets_can_disable_rps_for_attribution(self) -> None:
        parameter_sets = build_parameter_sets(
            {
                "use_rps": [False],
                "rps_threshold": [80, 90],
                "selected_rps_windows": [[50, 120], [50, 120, 250]],
                "rps_exit_threshold": [80],
            }
        )

        self.assertEqual(len(parameter_sets), 1)
        self.assertFalse(parameter_sets[0]["use_rps"])
        self.assertEqual(parameter_sets[0]["rps_threshold"], 0)
        self.assertEqual(parameter_sets[0]["selected_rps_windows"], [50, 120, 250])
        self.assertEqual(parameter_sets[0]["min_rps_windows_passing"], 1)
        self.assertIsNone(parameter_sets[0]["rps_exit_threshold"])

    def test_build_parameter_sets_can_disable_cup_handle_for_attribution(self) -> None:
        parameter_sets = build_parameter_sets(
            {
                "use_cup_handle": [False],
                "cup_handle_params": [
                    {"min_cup_depth_pct": 5},
                    {"min_cup_depth_pct": 12},
                ],
            }
        )

        self.assertEqual(len(parameter_sets), 1)
        self.assertFalse(parameter_sets[0]["use_cup_handle"])
        self.assertEqual(
            parameter_sets[0]["cup_handle_params"],
            DEFAULT_CUP_HANDLE_PARAMS.to_dict(),
        )

    def test_build_parameter_sets_preserves_optional_valuation_filters(self) -> None:
        parameter_sets = build_parameter_sets(
            {
                "fundamental_growth_params": [
                    {
                        "enabled": True,
                        "min_years": 3,
                        "min_growth_count": 2,
                        "min_yoy_growth_pct": "10",
                        "max_pe": "30",
                        "max_pb": "8",
                        "require_positive_operating_cash_flow": True,
                        "require_positive_free_cash_flow": True,
                        "min_operating_cash_flow_growth_count": 2,
                        "min_operating_cash_flow_yoy_growth_pct": "5",
                    }
                ],
            }
        )

        fundamentals = parameter_sets[0]["fundamental_growth_params"]

        self.assertEqual(fundamentals["max_pe"], "30")
        self.assertEqual(fundamentals["max_pb"], "8")
        self.assertTrue(fundamentals["require_positive_operating_cash_flow"])
        self.assertTrue(fundamentals["require_positive_free_cash_flow"])
        self.assertEqual(fundamentals["min_operating_cash_flow_growth_count"], 2)
        self.assertEqual(fundamentals["min_operating_cash_flow_yoy_growth_pct"], "5")

    def test_build_parameter_sets_supports_seeded_random_sampling(self) -> None:
        parameter_space = {
            "rps_threshold": [80, 81, 82, 83, 84, 85],
            "holding_days": [60, 100, 130],
            "stop_loss_pct": ["-0.06", "-0.08"],
        }

        first = build_parameter_sets(
            parameter_space,
            max_parameter_sets=5,
            search_mode="random",
            random_seed=7,
        )
        second = build_parameter_sets(
            parameter_space,
            max_parameter_sets=5,
            search_mode="random",
            random_seed=7,
        )

        self.assertEqual(len(first), 5)
        self.assertEqual(
            [stable_parameter_hash(parameters) for parameters in first],
            [stable_parameter_hash(parameters) for parameters in second],
        )

    def test_build_parameter_sets_normalizes_sell_and_portfolio_parameters(self) -> None:
        parameter_sets = build_parameter_sets(
            {
                "take_profit_pct": ["0.25"],
                "rps_exit_threshold": [80],
                "portfolio_cap": [10],
                "position_weight_pct": ["0.10"],
                "initial_capital": ["100000"],
                "position_size_amount": ["10000"],
                "allow_reentry_while_open": [False],
            }
        )

        self.assertEqual(parameter_sets[0]["take_profit_pct"], "0.2500")
        self.assertEqual(parameter_sets[0]["rps_exit_threshold"], 80)
        self.assertEqual(parameter_sets[0]["portfolio_cap"], 10)
        self.assertEqual(parameter_sets[0]["position_weight_pct"], "0.1000")
        self.assertEqual(parameter_sets[0]["initial_capital"], "100000.00")
        self.assertEqual(parameter_sets[0]["position_size_amount"], "10000.00")
        self.assertFalse(parameter_sets[0]["allow_reentry_while_open"])

    def test_build_parameter_sets_normalizes_market_filter_parameters(self) -> None:
        parameter_sets = build_parameter_sets(
            {
                "market_filter_params": [
                    {"enabled": False},
                    {
                        "enabled": True,
                        "symbol": "spy",
                        "require_price_above_sma": True,
                        "price_sma_days": 200,
                        "require_fast_sma_above_slow_sma": True,
                        "fast_sma_days": 50,
                        "slow_sma_days": 200,
                    },
                ],
            }
        )

        self.assertEqual(len(parameter_sets), 2)
        self.assertFalse(parameter_sets[0]["market_filter_params"]["enabled"])
        self.assertTrue(parameter_sets[1]["market_filter_params"]["enabled"])
        self.assertEqual(parameter_sets[1]["market_filter_params"]["symbol"], "SPY")
        self.assertTrue(
            parameter_sets[1]["market_filter_params"]["require_fast_sma_above_slow_sma"]
        )

    def test_create_optimization_run_persists_random_search_metadata(self) -> None:
        with self.session_factory() as session:
            run = create_optimization_run(
                session,
                market="us",
                train_start_date=date(2025, 1, 1),
                train_end_date=date(2025, 12, 31),
                parameter_space={
                    "rps_threshold": [80, 81, 82, 83, 84, 85],
                    "holding_days": [60, 100, 130],
                },
                max_parameter_sets=4,
                search_mode="random",
                random_seed=11,
                require_data_ready=False,
            )
            payload = serialize_optimization_run(run)

        self.assertEqual(run.total_parameter_sets, 4)
        self.assertEqual(payload["search_mode"], "random")
        self.assertEqual(payload["random_seed"], 11)
        self.assertNotIn("_optimization", payload["parameter_space"])

    def test_create_optimization_run_persists_worker_metadata(self) -> None:
        with self.session_factory() as session:
            run = create_optimization_run(
                session,
                market="us",
                train_start_date=date(2025, 1, 1),
                train_end_date=date(2025, 12, 31),
                parameter_space={"rps_threshold": [90]},
                max_workers=4,
                require_data_ready=False,
            )
            payload = serialize_optimization_run(run)

        self.assertEqual(payload["max_workers"], 4)

    def test_execute_optimization_run_persists_ranked_results(self) -> None:
        def fake_backtest(*args, **kwargs):
            if kwargs["rps_threshold"] == 90:
                return _BacktestResult(
                    completed_trades=60,
                    average_trade_return="0.120000",
                    win_rate="0.650000",
                    worst_trade_return="-0.060000",
                )
            return _BacktestResult(
                completed_trades=60,
                average_trade_return="0.040000",
                win_rate="0.520000",
                worst_trade_return="-0.080000",
            )

        with self.session_factory() as session:
            run = create_optimization_run(
                session,
                market="us",
                train_start_date=date(2025, 1, 1),
                train_end_date=date(2025, 12, 31),
                parameter_space={"rps_threshold": [85, 90]},
                require_data_ready=False,
            )
            with patch(
                "stockanalyse_api.services.optimization_backtest.run_cup_handle_rps_backtest",
                side_effect=fake_backtest,
            ):
                completed = execute_optimization_run(session, run.id)
            results = list_optimization_results(session, run_id=run.id)

        self.assertEqual(completed.status, "completed")
        self.assertEqual(completed.completed_parameter_sets, 2)
        self.assertEqual(results[0].rank, 1)
        self.assertEqual(serialize_optimization_result(results[0])["parameters"]["rps_threshold"], 90)
        self.assertEqual(completed.best_result_id, results[0].id)

    def test_execute_optimization_run_parallel_persists_worker_results(self) -> None:
        class FakeProcessPoolExecutor:
            instances: list["FakeProcessPoolExecutor"] = []

            def __init__(
                self,
                *,
                max_workers: int,
                max_tasks_per_child: int | None = None,
            ) -> None:
                self.max_workers = max_workers
                self.max_tasks_per_child = max_tasks_per_child
                self.submitted = 0
                FakeProcessPoolExecutor.instances.append(self)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                return None

            def submit(self, fn, payload):
                self.submitted += 1
                future: Future = Future()
                future.set_result(fn(payload))
                return future

        def fake_group_worker(payload):
            evaluations = []
            for parameters in payload["parameter_sets"]:
                threshold = int(parameters["rps_threshold"])
                if threshold == 85:
                    evaluations.append(
                        {
                            "parameters": parameters,
                            "train_metrics": None,
                            "validation_metrics": None,
                            "score": None,
                            "status": "failed",
                            "failure_reason": "RuntimeError: bad parameter",
                        }
                    )
                    continue
                score = Decimal(threshold) / Decimal("1000")
                evaluations.append(
                    {
                        "parameters": parameters,
                        "train_metrics": {
                            "completed_trades": 60,
                            "average_trade_return": f"{score:.6f}",
                            "win_rate": "0.550000",
                            "worst_trade_return": "-0.070000",
                            "sample_penalty": "0.000000",
                        },
                        "validation_metrics": None,
                        "score": score,
                        "status": "completed",
                        "failure_reason": None,
                    }
                )
            return evaluations

        with self.session_factory() as session:
            run = create_optimization_run(
                session,
                market="us",
                train_start_date=date(2025, 1, 1),
                train_end_date=date(2025, 12, 31),
                parameter_space={"rps_threshold": [85, 90, 95]},
                max_workers=2,
                require_data_ready=False,
            )
            with (
                patch(
                    "stockanalyse_api.services.optimization_backtest._session_uses_in_memory_database",
                    return_value=False,
                ),
                patch(
                    "stockanalyse_api.services.optimization_backtest.ProcessPoolExecutor",
                    FakeProcessPoolExecutor,
                ),
                patch(
                    "stockanalyse_api.services.optimization_backtest._evaluate_parameter_set_group_worker",
                    side_effect=fake_group_worker,
                ),
            ):
                completed = execute_optimization_run(session, run.id)
            results = list_optimization_results(session, run_id=run.id)

        self.assertEqual(FakeProcessPoolExecutor.instances[-1].max_workers, 2)
        self.assertGreaterEqual(FakeProcessPoolExecutor.instances[-1].submitted, 2)
        self.assertEqual(completed.status, "completed")
        self.assertEqual(completed.completed_parameter_sets, 2)
        self.assertEqual(completed.failed_parameter_sets, 1)
        self.assertEqual(results[0].rank, 1)
        self.assertEqual(serialize_optimization_result(results[0])["parameters"]["rps_threshold"], 95)

    def test_parallel_parameter_groups_batch_different_cup_params(self) -> None:
        base_space = {
            "rps_threshold": [85],
            "selected_rps_windows": [[50, 120]],
            "cup_handle_params": [
                {"min_cup_depth_pct": 10, "max_cup_depth_pct": 30},
                {"min_cup_depth_pct": 12, "max_cup_depth_pct": 33},
            ],
            "stop_loss_pct": ["-0.08"],
            "rps_exit_threshold": [80],
        }
        parameter_sets = build_parameter_sets(base_space)

        groups = _parallel_parameter_groups(parameter_sets, group_size=8)

        self.assertEqual(len(parameter_sets), 2)
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]), 2)

    def test_parallel_parameter_groups_batch_different_fundamental_params(self) -> None:
        base_space = {
            "rps_threshold": [85],
            "selected_rps_windows": [[50, 120]],
            "fundamental_growth_params": [
                {"enabled": True, "min_years": 3, "min_growth_count": 1},
                {"enabled": True, "min_years": 3, "min_growth_count": 2},
            ],
            "stop_loss_pct": ["-0.08"],
            "rps_exit_threshold": [80],
        }
        parameter_sets = build_parameter_sets(base_space)

        groups = _parallel_parameter_groups(parameter_sets, group_size=8)

        self.assertEqual(len(parameter_sets), 2)
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]), 2)

    def test_parallel_parameter_groups_split_small_runs_to_fill_workers(self) -> None:
        base_space = {
            "rps_threshold": [85],
            "selected_rps_windows": [[50, 120], [50, 120, 250]],
            "fundamental_growth_params": [
                {"enabled": True, "min_years": 3, "min_growth_count": 1, "min_yoy_growth_pct": "0"},
                {"enabled": True, "min_years": 3, "min_growth_count": 1, "min_yoy_growth_pct": "5"},
                {"enabled": True, "min_years": 3, "min_growth_count": 1, "min_yoy_growth_pct": "10"},
                {"enabled": True, "min_years": 3, "min_growth_count": 1, "min_yoy_growth_pct": "20"},
                {"enabled": True, "min_years": 3, "min_growth_count": 2, "min_yoy_growth_pct": "0"},
                {"enabled": True, "min_years": 3, "min_growth_count": 2, "min_yoy_growth_pct": "5"},
                {"enabled": True, "min_years": 3, "min_growth_count": 2, "min_yoy_growth_pct": "10"},
                {"enabled": True, "min_years": 3, "min_growth_count": 2, "min_yoy_growth_pct": "20"},
            ],
        }
        parameter_sets = build_parameter_sets(base_space)

        groups = _parallel_parameter_groups(
            parameter_sets,
            group_size=8,
            target_group_count=6,
        )

        self.assertEqual(len(parameter_sets), 16)
        self.assertEqual(len(groups), 6)
        self.assertLessEqual(max(len(group) for group in groups), 3)

    def test_execute_optimization_run_isolates_failed_parameter_sets(self) -> None:
        def fake_backtest(*args, **kwargs):
            if kwargs["rps_threshold"] == 85:
                raise RuntimeError("bad parameter")
            return _BacktestResult(
                completed_trades=20,
                average_trade_return="0.050000",
                win_rate="0.550000",
                worst_trade_return="-0.050000",
            )

        with self.session_factory() as session:
            run = create_optimization_run(
                session,
                market="us",
                train_start_date=date(2025, 1, 1),
                train_end_date=date(2025, 12, 31),
                parameter_space={"rps_threshold": [85, 90]},
                require_data_ready=False,
            )
            with patch(
                "stockanalyse_api.services.optimization_backtest.run_cup_handle_rps_backtest",
                side_effect=fake_backtest,
            ):
                completed = execute_optimization_run(session, run.id)
            rows = session.execute(select(OptimizationResult)).scalars().all()

        self.assertEqual(completed.status, "completed")
        self.assertEqual(completed.completed_parameter_sets, 1)
        self.assertEqual(completed.failed_parameter_sets, 1)
        self.assertEqual({row.status for row in rows}, {"completed", "failed"})

    def test_execute_optimization_run_covers_thirty_plus_parameter_sets_with_failures(self) -> None:
        def fake_backtest(*args, **kwargs):
            if kwargs["rps_threshold"] == 82 and kwargs["holding_days"] == 100:
                raise RuntimeError("bad parameter")
            return _BacktestResult(
                completed_trades=60,
                average_trade_return=f"{Decimal(kwargs['rps_threshold']) / Decimal('1000'):.6f}",
                win_rate="0.550000",
                worst_trade_return="-0.070000",
            )

        with self.session_factory() as session:
            run = create_optimization_run(
                session,
                market="us",
                train_start_date=date(2025, 1, 1),
                train_end_date=date(2025, 12, 31),
                parameter_space={
                    "rps_threshold": [80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90],
                    "holding_days": [60, 100, 130],
                },
                require_data_ready=False,
            )
            with patch(
                "stockanalyse_api.services.optimization_backtest.run_cup_handle_rps_backtest",
                side_effect=fake_backtest,
            ):
                completed = execute_optimization_run(session, run.id)
            results = list_optimization_results(session, run_id=run.id, limit=40)

        self.assertEqual(completed.total_parameter_sets, 33)
        self.assertEqual(completed.status, "completed")
        self.assertEqual(completed.completed_parameter_sets, 32)
        self.assertEqual(completed.failed_parameter_sets, 1)
        self.assertEqual(len(results), 33)
        self.assertEqual(results[0].status, "completed")
        self.assertEqual(results[0].rank, 1)
        self.assertEqual(results[-1].status, "failed")

    def test_execute_optimization_run_can_rank_by_selected_objective(self) -> None:
        def fake_backtest(*args, **kwargs):
            if kwargs["rps_threshold"] == 85:
                return _BacktestResult(
                    completed_trades=60,
                    average_trade_return="0.010000",
                    win_rate="0.900000",
                    worst_trade_return="-0.120000",
                )
            return _BacktestResult(
                completed_trades=60,
                average_trade_return="0.120000",
                win_rate="0.400000",
                worst_trade_return="-0.060000",
            )

        with self.session_factory() as session:
            run = create_optimization_run(
                session,
                market="us",
                train_start_date=date(2025, 1, 1),
                train_end_date=date(2025, 12, 31),
                parameter_space={"rps_threshold": [85, 90]},
                objective="win_rate",
                require_data_ready=False,
            )
            with patch(
                "stockanalyse_api.services.optimization_backtest.run_cup_handle_rps_backtest",
                side_effect=fake_backtest,
            ):
                execute_optimization_run(session, run.id)
            results = list_optimization_results(session, run_id=run.id)

        self.assertEqual(results[0].rank, 1)
        self.assertEqual(serialize_optimization_result(results[0])["parameters"]["rps_threshold"], 85)
        self.assertEqual(f"{results[0].score:.6f}", "0.900000")

    def test_execute_optimization_run_records_train_and_validation_ranks(self) -> None:
        def fake_backtest(*args, **kwargs):
            is_train = kwargs["start_date"] == date(2025, 1, 1)
            threshold = kwargs["rps_threshold"]
            if threshold == 85:
                average_return = "0.120000" if is_train else "0.010000"
            else:
                average_return = "0.010000" if is_train else "0.120000"
            return _BacktestResult(
                completed_trades=60,
                average_trade_return=average_return,
                win_rate="0.600000",
                worst_trade_return="-0.060000",
            )

        with self.session_factory() as session:
            run = create_optimization_run(
                session,
                market="us",
                train_start_date=date(2025, 1, 1),
                train_end_date=date(2025, 6, 30),
                validation_start_date=date(2025, 7, 1),
                validation_end_date=date(2025, 12, 31),
                parameter_space={"rps_threshold": [85, 90]},
                require_data_ready=False,
            )
            with patch(
                "stockanalyse_api.services.optimization_backtest.run_cup_handle_rps_backtest",
                side_effect=fake_backtest,
            ):
                execute_optimization_run(session, run.id)
            serialized = [
                serialize_optimization_result(result)
                for result in list_optimization_results(session, run_id=run.id)
            ]

        by_threshold = {row["parameters"]["rps_threshold"]: row for row in serialized}
        self.assertEqual(serialized[0]["parameters"]["rps_threshold"], 90)
        self.assertEqual(by_threshold[85]["train_metrics"]["train_rank"], 1)
        self.assertEqual(by_threshold[85]["validation_metrics"]["validation_rank"], 2)
        self.assertEqual(by_threshold[85]["validation_metrics"]["train_validation_rank_gap"], 1)
        self.assertEqual(by_threshold[90]["train_metrics"]["train_rank"], 2)
        self.assertEqual(by_threshold[90]["validation_metrics"]["validation_rank"], 1)
        self.assertEqual(by_threshold[90]["validation_metrics"]["train_validation_rank_gap"], -1)

    def test_metric_extraction_penalizes_small_samples(self) -> None:
        metrics = _extract_metrics(
            _BacktestResult(
                completed_trades=5,
                average_trade_return="0.100000",
                win_rate="0.600000",
                worst_trade_return="-0.050000",
            ).to_dict()
        )

        self.assertEqual(metrics["sample_penalty"], "0.200000")

    def test_metric_extraction_derives_portfolio_risk_metrics(self) -> None:
        metrics = _extract_metrics(
            {
                "start_date": "2025-01-01",
                "end_date": "2025-12-31",
                "signal_dates_evaluated": 3,
                "total_candidates_evaluated": 100,
                "qualifying_observations": 10,
                "selected_trades": 10,
                "completed_trades": 10,
                "average_trade_return": "0.020000",
                "median_trade_return": "0.010000",
                "win_rate": "0.600000",
                "best_trade_return": "0.150000",
                "worst_trade_return": "-0.080000",
                "stop_loss_trades": 3,
                "stop_loss_trigger_ratio": "0.300000",
                "max_consecutive_losses": 4,
                "signal_days": [
                    {"signal_date": "2025-01-02", "average_return": "0.100000"},
                    {"signal_date": "2025-02-03", "average_return": "-0.050000"},
                    {"signal_date": "2025-03-04", "average_return": "0.020000"},
                ],
            }
        )

        self.assertEqual(metrics["total_return"], "0.065900")
        self.assertEqual(metrics["max_drawdown"], "-0.050000")
        self.assertEqual(metrics["signal_day_return_count"], 3)
        self.assertEqual(metrics["stop_loss_trades"], 3)
        self.assertEqual(metrics["stop_loss_trigger_ratio"], "0.300000")
        self.assertEqual(metrics["max_consecutive_losses"], 4)
        self.assertEqual(len(metrics["equity_curve"]), 3)
        self.assertEqual(metrics["equity_curve"][-1]["equity"], "1.065900")
        self.assertEqual(metrics["yearly_returns"], {"2025": "0.065900"})
        self.assertIsNotNone(metrics["annualized_return"])
        self.assertIsNotNone(metrics["return_drawdown_ratio"])

    def test_metric_extraction_adds_benchmark_relative_metrics(self) -> None:
        metrics = _extract_metrics(
            {
                "start_date": "2025-01-01",
                "end_date": "2025-12-31",
                "completed_trades": 20,
                "average_trade_return": "0.020000",
                "win_rate": "0.600000",
                "worst_trade_return": "-0.080000",
                "account_total_return": "0.150000",
                "account_annualized_return": "0.120000",
                "account_max_drawdown": "-0.080000",
                "account_equity_curve": [],
                "account_yearly_returns": {},
            },
            benchmark_metrics={
                "SPY": {
                    "total_return": "0.100000",
                    "annualized_return": "0.080000",
                    "max_drawdown": "-0.200000",
                }
            },
        )

        self.assertEqual(metrics["benchmarks"]["SPY"]["total_return"], "0.100000")
        self.assertEqual(
            metrics["benchmark_relative"]["SPY"]["excess_total_return"],
            "0.050000",
        )
        self.assertEqual(
            metrics["benchmark_relative"]["SPY"]["excess_annualized_return"],
            "0.040000",
        )
        self.assertEqual(
            metrics["benchmark_relative"]["SPY"]["max_drawdown_improvement"],
            "0.120000",
        )

    def test_score_penalizes_stop_loss_ratio_and_loss_streak(self) -> None:
        base_metrics = {
            "completed_trades": 80,
            "average_trade_return": "0.080000",
            "annualized_return": "0.180000",
            "win_rate": "0.600000",
            "worst_trade_return": "-0.080000",
            "max_drawdown": "-0.120000",
            "return_drawdown_ratio": "1.500000",
            "stop_loss_trigger_ratio": "0.000000",
            "max_consecutive_losses": 0,
            "sample_penalty": "0.000000",
        }
        risky_metrics = dict(base_metrics)
        risky_metrics["stop_loss_trigger_ratio"] = "0.500000"
        risky_metrics["max_consecutive_losses"] = 6

        self.assertLess(_score_metrics(risky_metrics), _score_metrics(base_metrics))

    def test_average_annualized_return_objective_uses_cross_period_average(self) -> None:
        train_metrics = {"annualized_return": "0.300000"}
        validation_metrics = {"annualized_return": "0.100000"}

        _attach_average_annualized_return(train_metrics, validation_metrics)

        self.assertEqual(validation_metrics["average_annualized_return"], "0.200000")
        self.assertEqual(
            _score_metrics(validation_metrics, objective="average_annualized_return"),
            Decimal("0.200000"),
        )

    def test_robust_annualized_return_penalizes_unstable_cross_period_results(self) -> None:
        stable_train = {
            "annualized_return": "0.180000",
            "max_drawdown": "-0.120000",
            "completed_trades": 120,
        }
        stable_validation = {
            "annualized_return": "0.160000",
            "max_drawdown": "-0.110000",
            "completed_trades": 110,
        }
        unstable_train = {
            "annualized_return": "-0.020000",
            "max_drawdown": "-0.180000",
            "completed_trades": 120,
        }
        unstable_validation = {
            "annualized_return": "0.200000",
            "max_drawdown": "-0.150000",
            "completed_trades": 110,
        }

        self.assertGreater(
            _score_metric_pair(
                stable_train,
                stable_validation,
                objective="robust_annualized_return",
            ),
            _score_metric_pair(
                unstable_train,
                unstable_validation,
                objective="robust_annualized_return",
            ),
        )

    def test_robust_annualized_return_keeps_validation_return_directional(self) -> None:
        shared_train = {
            "annualized_return": "0.010000",
            "max_drawdown": "-0.080000",
            "completed_trades": 120,
        }
        lower_validation = {
            "annualized_return": "0.080000",
            "max_drawdown": "-0.120000",
            "completed_trades": 140,
        }
        higher_validation = {
            "annualized_return": "0.220000",
            "max_drawdown": "-0.120000",
            "completed_trades": 140,
        }

        self.assertGreater(
            _score_metric_pair(
                shared_train,
                higher_validation,
                objective="robust_annualized_return",
            ),
            _score_metric_pair(
                shared_train,
                lower_validation,
                objective="robust_annualized_return",
            ),
        )

    def test_robust_annualized_return_rejects_missing_or_tiny_train_sample(self) -> None:
        healthy_train = {
            "annualized_return": "0.010000",
            "max_drawdown": "-0.080000",
            "completed_trades": 120,
        }
        tiny_missing_train = {
            "max_drawdown": "0.000000",
            "completed_trades": 2,
        }
        validation = {
            "annualized_return": "0.250000",
            "max_drawdown": "-0.100000",
            "completed_trades": 150,
        }

        self.assertGreater(
            _score_metric_pair(
                healthy_train,
                validation,
                objective="robust_annualized_return",
            ),
            _score_metric_pair(
                tiny_missing_train,
                validation,
                objective="robust_annualized_return",
            ),
        )

    def test_robust_annualized_return_keeps_viable_low_frequency_sample_competitive(self) -> None:
        high_volume_train = {
            "annualized_return": "0.005000",
            "max_drawdown": "-0.080000",
            "completed_trades": 120,
        }
        high_volume_validation = {
            "annualized_return": "0.100000",
            "max_drawdown": "-0.100000",
            "completed_trades": 180,
        }
        low_frequency_train = {
            "annualized_return": "0.020000",
            "max_drawdown": "-0.080000",
            "completed_trades": 22,
        }
        low_frequency_validation = {
            "annualized_return": "0.160000",
            "max_drawdown": "-0.100000",
            "completed_trades": 55,
        }

        self.assertGreater(
            _score_metric_pair(
                low_frequency_train,
                low_frequency_validation,
                objective="robust_annualized_return",
            ),
            _score_metric_pair(
                high_volume_train,
                high_volume_validation,
                objective="robust_annualized_return",
            ),
        )

    def test_robust_annualized_return_rejects_severely_negative_training(self) -> None:
        viable_train = {
            "annualized_return": "0.050000",
            "max_drawdown": "-0.150000",
            "completed_trades": 120,
        }
        viable_validation = {
            "annualized_return": "0.100000",
            "max_drawdown": "-0.150000",
            "completed_trades": 120,
        }
        failed_train = {
            "annualized_return": "-0.100000",
            "max_drawdown": "-0.200000",
            "completed_trades": 120,
        }
        failed_validation = {
            "annualized_return": "0.300000",
            "max_drawdown": "-0.200000",
            "completed_trades": 120,
        }

        self.assertGreater(
            _score_metric_pair(
                viable_train,
                viable_validation,
                objective="robust_annualized_return",
            ),
            _score_metric_pair(
                failed_train,
                failed_validation,
                objective="robust_annualized_return",
            ),
        )

    def test_create_optimization_run_rejects_unsupported_objective(self) -> None:
        with self.session_factory() as session:
            with self.assertRaisesRegex(ValueError, "objective must be one of"):
                create_optimization_run(
                    session,
                    market="us",
                    train_start_date=date(2025, 1, 1),
                    train_end_date=date(2025, 12, 31),
                    parameter_space={"rps_threshold": [90]},
                    objective="unknown",
                    require_data_ready=False,
                )

    def test_create_optimization_run_rejects_unsupported_search_mode(self) -> None:
        with self.session_factory() as session:
            with self.assertRaisesRegex(ValueError, "search_mode must be one of"):
                create_optimization_run(
                    session,
                    market="us",
                    train_start_date=date(2025, 1, 1),
                    train_end_date=date(2025, 12, 31),
                    parameter_space={"rps_threshold": [90]},
                    search_mode="bayesian",
                    require_data_ready=False,
                )

    def test_backtest_reuses_screen_cache_for_same_selection_parameters(self) -> None:
        cache: dict[str, dict[str, object]] = {}
        screen_payload = {"hits": [], "total_evaluated": 10}

        with (
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest._load_trade_dates",
                return_value=[date(2025, 1, 2), date(2025, 1, 3)],
            ),
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest.screen_universe",
                return_value=screen_payload,
            ) as screen_mock,
        ):
            for _ in range(2):
                run_cup_handle_rps_backtest(
                    None,
                    start_date=date(2025, 1, 1),
                    end_date=date(2025, 1, 31),
                    rps_threshold=90,
                    selected_rps_windows=[50, 120],
                    min_rps_windows_passing=1,
                    cup_handle_params=DEFAULT_CUP_HANDLE_PARAMS,
                    market="us",
                    screen_cache=cache,
                )

        self.assertEqual(screen_mock.call_count, 2)
        self.assertEqual(len(cache), 2)

    def test_backtest_skips_screen_when_portfolio_is_full(self) -> None:
        screen_payload = {
            "hits": [
                {
                    "instrument_id": 1,
                    "symbol": "AAPL",
                    "rps_50": "95",
                    "rps_120": "90",
                    "rps_250": "88",
                }
            ],
            "total_evaluated": 10,
        }
        rows = [_market_row(date(2025, 1, day), "100") for day in range(2, 9)]

        with (
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest._load_trade_dates",
                return_value=[date(2025, 1, 1), date(2025, 1, 2)],
            ),
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest.screen_universe",
                return_value=screen_payload,
            ) as screen_mock,
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest._load_future_rows",
                return_value=rows,
            ),
        ):
            run_cup_handle_rps_backtest(
                None,
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 31),
                rps_threshold=90,
                selected_rps_windows=[50, 120],
                min_rps_windows_passing=1,
                use_cup_handle=False,
                cup_handle_params=DEFAULT_CUP_HANDLE_PARAMS,
                market="us",
                holding_days=5,
                stop_loss_pct=Decimal("-0.50"),
                portfolio_cap=1,
                entry_deferral_window_days=1,
                execution_limited_screen=True,
            )

        self.assertEqual(screen_mock.call_count, 1)
        self.assertEqual(screen_mock.call_args.kwargs["max_hits"], 1)

    def test_backtest_skips_screen_when_market_filter_blocks_date(self) -> None:
        screen_payload = {"hits": [], "total_evaluated": 10}

        with (
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest._load_trade_dates",
                return_value=[date(2025, 1, 1), date(2025, 1, 2)],
            ),
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest._load_market_filter_allowed_dates",
                return_value={date(2025, 1, 2)},
            ) as market_filter_mock,
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest.screen_universe",
                return_value=screen_payload,
            ) as screen_mock,
        ):
            result = run_cup_handle_rps_backtest(
                None,
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 31),
                rps_threshold=90,
                selected_rps_windows=[50, 120],
                min_rps_windows_passing=1,
                use_cup_handle=False,
                cup_handle_params=DEFAULT_CUP_HANDLE_PARAMS,
                market="us",
                market_filter_params={
                    "enabled": True,
                    "symbol": "SPY",
                    "require_price_above_sma": True,
                    "price_sma_days": 200,
                },
            )

        self.assertEqual(market_filter_mock.call_count, 1)
        self.assertEqual(screen_mock.call_count, 1)
        self.assertEqual(result.to_dict()["parameters"]["market_filter_params"]["symbol"], "SPY")

    def test_backtest_reuses_trade_cache_for_identical_selected_trade(self) -> None:
        trade_cache: dict[tuple[object, ...], object] = {}
        screen_payload = {
            "hits": [
                {
                    "instrument_id": 1,
                    "symbol": "AAPL",
                    "rps_50": "95",
                    "rps_120": "90",
                    "rps_250": "88",
                }
            ],
            "total_evaluated": 10,
        }
        rows = [
            _market_row(date(2025, 1, 2), "100"),
            _market_row(date(2025, 1, 3), "110"),
        ]

        with (
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest._load_trade_dates",
                return_value=[date(2025, 1, 1)],
            ),
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest.screen_universe",
                return_value=screen_payload,
            ),
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest._load_future_rows",
                return_value=rows,
            ) as future_rows_mock,
        ):
            for _ in range(2):
                run_cup_handle_rps_backtest(
                    None,
                    start_date=date(2025, 1, 1),
                    end_date=date(2025, 1, 31),
                    rps_threshold=90,
                    selected_rps_windows=[50, 120],
                    min_rps_windows_passing=1,
                    use_cup_handle=False,
                    cup_handle_params=DEFAULT_CUP_HANDLE_PARAMS,
                    market="us",
                    holding_days=1,
                    stop_loss_pct=Decimal("-0.50"),
                    portfolio_cap=1,
                    trade_cache=trade_cache,
                )

        self.assertEqual(future_rows_mock.call_count, 1)
        self.assertEqual(len(trade_cache), 1)

    def test_backtest_tracks_cash_account_with_fixed_position_size(self) -> None:
        screen_payload = {
            "hits": [
                {
                    "instrument_id": 1,
                    "symbol": "AAPL",
                    "rps_50": "95",
                    "rps_120": "90",
                    "rps_250": "88",
                }
            ],
            "total_evaluated": 10,
        }
        rows = [
            _market_row(date(2025, 1, 2), "100"),
            _market_row(date(2025, 1, 3), "110"),
        ]

        with (
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest._load_trade_dates",
                return_value=[date(2025, 1, 1)],
            ),
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest.screen_universe",
                return_value=screen_payload,
            ),
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest._load_future_rows",
                return_value=rows,
            ),
        ):
            result = run_cup_handle_rps_backtest(
                None,
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 31),
                rps_threshold=90,
                selected_rps_windows=[50, 120],
                min_rps_windows_passing=1,
                use_cup_handle=False,
                cup_handle_params=DEFAULT_CUP_HANDLE_PARAMS,
                market="us",
                holding_days=1,
                stop_loss_pct=Decimal("-0.50"),
                portfolio_cap=1,
                initial_capital=Decimal("100000"),
                position_size_amount=Decimal("10000"),
                entry_deferral_window_days=1,
            )

        payload = result.to_dict()
        self.assertEqual(payload["initial_capital"], "100000.00")
        self.assertEqual(payload["position_size_amount"], "10000.00")
        self.assertEqual(payload["final_capital"], "101000.00")
        self.assertEqual(payload["total_profit"], "1000.00")
        self.assertEqual(payload["account_total_return"], "0.010000")
        self.assertEqual(payload["account_final_date"], "2025-01-03")
        self.assertEqual(payload["trades"][0]["invested_cash"], "10000.00")
        self.assertEqual(payload["trades"][0]["exit_cash"], "11000.00")
        self.assertEqual(payload["trades"][0]["realized_profit"], "1000.00")
        metrics = _extract_metrics(payload)
        self.assertEqual(metrics["total_return"], "0.010000")
        self.assertEqual(metrics["final_capital"], "101000.00")

    def test_simulate_trade_respects_entry_delay_days(self) -> None:
        rows = [
            _market_row(date(2025, 1, 2), "100"),
            _market_row(date(2025, 1, 3), "110"),
            _market_row(date(2025, 1, 6), "120"),
            _market_row(date(2025, 1, 7), "132"),
        ]

        with patch(
            "stockanalyse_api.services.dashboard_strategy_backtest._load_future_rows",
            return_value=rows,
        ):
            trade = _simulate_trade(
                None,
                signal_date=date(2025, 1, 1),
                hit={"instrument_id": 1, "symbol": "AAPL", "rps_50": "95"},
                selected_windows=[50],
                holding_days=1,
                stop_loss_pct=Decimal("-0.50"),
                entry_delay_days=2,
                entry_deferral_window_days=1,
            )

        self.assertNotIsInstance(trade, dict)
        assert not isinstance(trade, dict)
        self.assertEqual(trade.entry_date, "2025-01-06")
        self.assertEqual(trade.entry_price, "120.000000")
        self.assertEqual(trade.exit_date, "2025-01-07")
        self.assertEqual(trade.realized_return, "0.100000")

    def test_simulate_trade_exits_on_take_profit_at_next_valid_open(self) -> None:
        rows = [
            _market_row(date(2025, 1, 2), "100"),
            _market_row(date(2025, 1, 3), "130"),
            _market_row(date(2025, 1, 6), "128"),
        ]

        with patch(
            "stockanalyse_api.services.dashboard_strategy_backtest._load_future_rows",
            return_value=rows,
        ):
            trade = _simulate_trade(
                None,
                signal_date=date(2025, 1, 1),
                hit={"instrument_id": 1, "symbol": "AAPL", "rps_50": "95"},
                selected_windows=[50],
                holding_days=10,
                stop_loss_pct=Decimal("-0.50"),
                take_profit_pct=Decimal("0.25"),
                entry_delay_days=0,
                entry_deferral_window_days=1,
            )

        self.assertNotIsInstance(trade, dict)
        assert not isinstance(trade, dict)
        self.assertEqual(trade.exit_reason, "take_profit")
        self.assertEqual(trade.exit_date, "2025-01-06")
        self.assertEqual(trade.realized_return, "0.280000")

    def test_simulate_trade_stop_loss_uses_intraday_low_and_stop_price(self) -> None:
        rows = [
            _ohlc_row(
                date(2025, 1, 2),
                open_price="100",
                high="103",
                low="99",
                close="102",
            ),
            _ohlc_row(
                date(2025, 1, 3),
                open_price="101",
                high="102",
                low="91",
                close="99",
            ),
            _ohlc_row(
                date(2025, 1, 6),
                open_price="100",
                high="100",
                low="98",
                close="99",
            ),
        ]

        with patch(
            "stockanalyse_api.services.dashboard_strategy_backtest._load_future_rows",
            return_value=rows,
        ):
            trade = _simulate_trade(
                None,
                signal_date=date(2025, 1, 1),
                hit={"instrument_id": 1, "symbol": "AAPL", "rps_50": "95"},
                selected_windows=[50],
                holding_days=10,
                stop_loss_pct=Decimal("-0.08"),
                entry_delay_days=0,
                entry_deferral_window_days=1,
            )

        self.assertNotIsInstance(trade, dict)
        assert not isinstance(trade, dict)
        self.assertEqual(trade.exit_reason, "stop_loss")
        self.assertEqual(trade.exit_date, "2025-01-03")
        self.assertEqual(trade.exit_price, "92.000000")
        self.assertEqual(trade.realized_return, "-0.080000")

    def test_simulate_trade_stop_loss_gap_down_uses_open_price(self) -> None:
        rows = [
            _market_row(date(2025, 1, 2), "100"),
            _ohlc_row(
                date(2025, 1, 3),
                open_price="88",
                high="89",
                low="84",
                close="86",
            ),
        ]

        with patch(
            "stockanalyse_api.services.dashboard_strategy_backtest._load_future_rows",
            return_value=rows,
        ):
            trade = _simulate_trade(
                None,
                signal_date=date(2025, 1, 1),
                hit={"instrument_id": 1, "symbol": "AAPL", "rps_50": "95"},
                selected_windows=[50],
                holding_days=10,
                stop_loss_pct=Decimal("-0.08"),
                entry_delay_days=0,
                entry_deferral_window_days=1,
            )

        self.assertNotIsInstance(trade, dict)
        assert not isinstance(trade, dict)
        self.assertEqual(trade.exit_reason, "stop_loss")
        self.assertEqual(trade.exit_date, "2025-01-03")
        self.assertEqual(trade.exit_price, "88.000000")
        self.assertEqual(trade.realized_return, "-0.120000")

    def test_simulate_trade_without_holding_limit_marks_to_latest_close(self) -> None:
        rows = [
            _market_row(date(2025, 1, 2), "100"),
            _market_row(date(2025, 1, 3), "105"),
            _market_row(date(2025, 1, 6), "112"),
        ]

        with patch(
            "stockanalyse_api.services.dashboard_strategy_backtest._load_future_rows",
            return_value=rows,
        ):
            trade = _simulate_trade(
                None,
                signal_date=date(2025, 1, 1),
                hit={"instrument_id": 1, "symbol": "AAPL", "rps_50": "95"},
                selected_windows=[50],
                holding_days=None,
                stop_loss_pct=Decimal("-0.08"),
                entry_delay_days=0,
                entry_deferral_window_days=1,
            )

        self.assertNotIsInstance(trade, dict)
        assert not isinstance(trade, dict)
        self.assertEqual(trade.exit_reason, "data_end_mark")
        self.assertEqual(trade.exit_date, "2025-01-06")
        self.assertEqual(trade.exit_price, "112.000000")
        self.assertEqual(trade.realized_return, "0.120000")

    def test_simulate_trade_with_window_end_marks_to_latest_close(self) -> None:
        rows = [
            _market_row(date(2025, 1, 2), "100"),
            _market_row(date(2025, 1, 3), "105"),
        ]

        with patch(
            "stockanalyse_api.services.dashboard_strategy_backtest._load_future_rows",
            return_value=rows,
        ) as future_rows_mock:
            trade = _simulate_trade(
                None,
                signal_date=date(2025, 1, 1),
                hit={"instrument_id": 1, "symbol": "AAPL", "rps_50": "95"},
                selected_windows=[50],
                holding_days=10,
                stop_loss_pct=Decimal("-0.08"),
                entry_delay_days=0,
                entry_deferral_window_days=1,
                max_exit_date=date(2025, 1, 3),
            )

        self.assertEqual(future_rows_mock.call_args.kwargs["max_exit_date"], date(2025, 1, 3))
        self.assertNotIsInstance(trade, dict)
        assert not isinstance(trade, dict)
        self.assertEqual(trade.exit_reason, "window_end_mark")
        self.assertEqual(trade.exit_date, "2025-01-03")
        self.assertEqual(trade.exit_price, "105.000000")
        self.assertEqual(trade.realized_return, "0.050000")

    def test_simulate_trade_exits_when_rps_falls_below_threshold(self) -> None:
        rows = [
            _market_row(date(2025, 1, 2), "100"),
            _market_row(date(2025, 1, 3), "101"),
            _market_row(date(2025, 1, 6), "102"),
        ]
        indicator_map = {
            date(2025, 1, 3): SimpleNamespace(trade_date=date(2025, 1, 3), rps_50=Decimal("79"))
        }

        with (
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest._load_future_rows",
                return_value=rows,
            ),
            patch(
                "stockanalyse_api.services.dashboard_strategy_backtest._load_future_indicator_map",
                return_value=indicator_map,
            ),
        ):
            trade = _simulate_trade(
                None,
                signal_date=date(2025, 1, 1),
                hit={"instrument_id": 1, "symbol": "AAPL", "rps_50": "95"},
                selected_windows=[50],
                holding_days=10,
                stop_loss_pct=Decimal("-0.50"),
                rps_exit_threshold=80,
                entry_delay_days=0,
                entry_deferral_window_days=1,
            )

        self.assertNotIsInstance(trade, dict)
        assert not isinstance(trade, dict)
        self.assertEqual(trade.exit_reason, "rps_exit")
        self.assertEqual(trade.exit_date, "2025-01-06")

    def test_cancel_requested_run_stops_before_execution(self) -> None:
        with self.session_factory() as session:
            run = create_optimization_run(
                session,
                market="us",
                train_start_date=date(2025, 1, 1),
                train_end_date=date(2025, 12, 31),
                parameter_space={"rps_threshold": [85, 90]},
                require_data_ready=False,
            )
            run.status = "cancel_requested"
            session.commit()

            completed = execute_optimization_run(session, run.id)
            result_count = session.execute(select(OptimizationResult)).scalars().all()

        self.assertEqual(completed.status, "cancelled")
        self.assertEqual(result_count, [])

    def test_cancel_after_partial_completion_keeps_partial_rankings(self) -> None:
        from stockanalyse_api.domain.backtests.models import OptimizationRun

        captured: dict[str, int | None] = {"run_id": None}
        call_count = {"value": 0}

        def fake_backtest(session, *args, **kwargs):
            call_count["value"] += 1
            if captured["run_id"] is not None:
                run = session.get(OptimizationRun, captured["run_id"])
                run.status = "cancel_requested"
            return _BacktestResult(
                completed_trades=60,
                average_trade_return="0.100000",
                win_rate="0.500000",
                worst_trade_return="-0.050000",
            )

        with self.session_factory() as session:
            run = create_optimization_run(
                session,
                market="us",
                train_start_date=date(2025, 1, 1),
                train_end_date=date(2025, 12, 31),
                parameter_space={"rps_threshold": [85, 90, 95]},
                require_data_ready=False,
            )
            captured["run_id"] = run.id
            with patch(
                "stockanalyse_api.services.optimization_backtest.run_cup_handle_rps_backtest",
                side_effect=fake_backtest,
            ):
                completed = execute_optimization_run(session, run.id)
            results = list(session.execute(select(OptimizationResult)).scalars())

        self.assertEqual(completed.status, "cancelled")
        self.assertEqual(call_count["value"], 1)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].rank, 1)
        self.assertEqual(completed.best_result_id, results[0].id)

    def test_optimization_result_detail_returns_annotated_trade_rows(self) -> None:
        def fake_backtest(*args, **kwargs):
            return SimpleNamespace(
                to_dict=lambda: {
                    "start_date": kwargs["start_date"].isoformat(),
                    "end_date": kwargs["end_date"].isoformat(),
                    "completed_trades": 1,
                    "trades": [
                        {
                            "signal_date": kwargs["start_date"].isoformat(),
                            "instrument_id": 1,
                            "symbol": "AAPL",
                            "entry_date": "2026-03-03",
                            "entry_price": "100.000000",
                            "exit_date": "2026-03-10",
                            "exit_price": "92.000000",
                            "exit_reason": "rps_exit",
                            "realized_return": "-0.080000",
                            "rps_score": "88.00",
                        }
                    ],
                }
            )

        with self.session_factory() as session:
            run = create_optimization_run(
                session,
                market="us",
                train_start_date=date(2026, 3, 2),
                train_end_date=date(2026, 3, 13),
                validation_start_date=date(2026, 3, 16),
                validation_end_date=date(2026, 3, 31),
                parameter_space={"rps_threshold": [80], "use_cup_handle": [False]},
                require_data_ready=False,
            )
            parameters = build_parameter_sets(
                {"rps_threshold": [80], "use_cup_handle": [False]}
            )[0]
            result = OptimizationResult(
                optimization_run_id=run.id,
                parameter_hash=stable_parameter_hash(parameters),
                parameters_json=dump_json(parameters),
                train_metrics_json=dump_json({"completed_trades": 1}),
                validation_metrics_json=dump_json({"completed_trades": 1}),
                score=Decimal("0.1"),
                rank=1,
                status="completed",
            )
            session.add(result)
            session.commit()
            result_id = result.id

            with patch(
                "stockanalyse_api.services.optimization_backtest.run_cup_handle_rps_backtest",
                side_effect=fake_backtest,
            ):
                detail = build_optimization_result_detail(session, result_id=result_id)

        validation_trade = detail["validation"]["trades"][0]
        self.assertEqual(validation_trade["symbol"], "AAPL")
        self.assertEqual(
            validation_trade["entry_reason"],
            "RPS 达到 80；未启用杯柄过滤；财务增长通过",
        )
        self.assertEqual(validation_trade["exit_reason_label"], "RPS 跌破退出阈值")
        self.assertEqual(validation_trade["entry_price"], "100.000000")
        self.assertEqual(validation_trade["exit_price"], "92.000000")

    def test_optimization_result_detail_uses_metrics_without_rerun_when_no_trades(self) -> None:
        with self.session_factory() as session:
            run = create_optimization_run(
                session,
                market="us",
                train_start_date=date(2026, 3, 2),
                train_end_date=date(2026, 3, 13),
                validation_start_date=date(2026, 3, 16),
                validation_end_date=date(2026, 3, 31),
                parameter_space={"rps_threshold": [80], "use_cup_handle": [False]},
                require_data_ready=False,
            )
            parameters = build_parameter_sets(
                {"rps_threshold": [80], "use_cup_handle": [False]}
            )[0]
            result = OptimizationResult(
                optimization_run_id=run.id,
                parameter_hash=stable_parameter_hash(parameters),
                parameters_json=dump_json(parameters),
                train_metrics_json=dump_json(
                    {"completed_trades": 0, "annualized_return": "0.000000"}
                ),
                validation_metrics_json=dump_json(
                    {"completed_trades": 0, "annualized_return": "0.000000"}
                ),
                score=Decimal("-0.2"),
                rank=1,
                status="completed",
            )
            session.add(result)
            session.commit()
            result_id = result.id

            with patch(
                "stockanalyse_api.services.optimization_backtest.run_cup_handle_rps_backtest"
            ) as mocked_backtest:
                detail = build_optimization_result_detail(session, result_id=result_id)

            cache = session.execute(
                select(OptimizationResultDetailCache).where(
                    OptimizationResultDetailCache.optimization_result_id == result_id
                )
            ).scalar_one()

        mocked_backtest.assert_not_called()
        self.assertEqual(detail["train"]["trades"], [])
        self.assertEqual(detail["validation"]["trades"], [])
        self.assertEqual(detail["train"]["summary"]["start_date"], "2026-03-02")
        self.assertEqual(cache.max_trades_returned, 1000)

    def test_optimization_result_detail_reuses_cached_trade_rows(self) -> None:
        with self.session_factory() as session:
            run = create_optimization_run(
                session,
                market="us",
                train_start_date=date(2026, 3, 2),
                train_end_date=date(2026, 3, 13),
                validation_start_date=None,
                validation_end_date=None,
                parameter_space={"rps_threshold": [80], "use_cup_handle": [False]},
                require_data_ready=False,
            )
            parameters = build_parameter_sets(
                {"rps_threshold": [80], "use_cup_handle": [False]}
            )[0]
            result = OptimizationResult(
                optimization_run_id=run.id,
                parameter_hash=stable_parameter_hash(parameters),
                parameters_json=dump_json(parameters),
                train_metrics_json=dump_json({"completed_trades": 1}),
                validation_metrics_json=None,
                score=Decimal("0.1"),
                rank=1,
                status="completed",
            )
            session.add(result)
            session.commit()
            result_id = result.id
            session.add(
                OptimizationResultDetailCache(
                    optimization_result_id=result_id,
                    max_trades_returned=1000,
                    train_result_json=dump_json(
                        {
                            "completed_trades": 1,
                            "trades": [
                                {
                                    "signal_date": "2026-03-02",
                                    "symbol": "AAPL",
                                    "exit_reason": "stop_loss",
                                }
                            ],
                        }
                    ),
                    validation_result_json=None,
                    generated_at=datetime.now(UTC),
                )
            )
            session.commit()

            with patch(
                "stockanalyse_api.services.optimization_backtest.run_cup_handle_rps_backtest"
            ) as mocked_backtest:
                detail = build_optimization_result_detail(session, result_id=result_id)

        mocked_backtest.assert_not_called()
        self.assertEqual(detail["train"]["trades"][0]["symbol"], "AAPL")
        self.assertEqual(detail["train"]["trades"][0]["exit_reason_label"], "固定止损触发")

    def test_strategy_presets_can_be_saved_listed_and_activated(self) -> None:
        first_params = {"rps_threshold": 85}
        second_params = {"rps_threshold": 90}

        with self.session_factory() as session:
            first = save_strategy_preset(
                session,
                market="us",
                name="US RPS 85",
                parameters=first_params,
            )
            second = save_strategy_preset(
                session,
                market="us",
                name="US RPS 90",
                parameters=second_params,
            )
            activated = activate_strategy_preset(session, second.id)
            presets = list_strategy_presets(session, market="us")
            rows = session.execute(select(StrategyPreset)).scalars().all()

        self.assertFalse(next(row for row in rows if row.id == first.id).is_active)
        self.assertTrue(activated.is_active)
        self.assertEqual(presets[0].id, second.id)
        self.assertEqual(presets[0].parameters_hash, stable_parameter_hash(second_params))

    def test_strategy_presets_can_be_updated_duplicated_and_deleted(self) -> None:
        with self.session_factory() as session:
            preset = save_strategy_preset(
                session,
                market="us",
                name="US RPS 85",
                parameters={"rps_threshold": 85},
            )
            updated = update_strategy_preset(
                session,
                preset.id,
                name="US RPS 90",
                parameters={"rps_threshold": 90},
                is_active=True,
            )
            copied = duplicate_strategy_preset(session, updated.id, name="US RPS 90 Copy")
            delete_strategy_preset(session, updated.id)
            presets = list_strategy_presets(session, market="us")
            updated_name = updated.name
            updated_hash = updated.parameters_hash
            copied_name = copied.name
            copied_hash = copied.parameters_hash
            copied_id = copied.id

        self.assertEqual(updated_name, "US RPS 90")
        self.assertEqual(updated_hash, stable_parameter_hash({"rps_threshold": 90}))
        self.assertEqual(copied_name, "US RPS 90 Copy")
        self.assertEqual(copied_hash, updated_hash)
        self.assertEqual([preset.id for preset in presets], [copied_id])


if __name__ == "__main__":
    unittest.main()
