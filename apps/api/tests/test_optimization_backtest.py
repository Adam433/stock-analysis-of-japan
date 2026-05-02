from __future__ import annotations

import unittest
from datetime import date
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
from stockanalyse_api.domain.backtests.models import OptimizationResult, StrategyPreset
from stockanalyse_api.services.dashboard import DEFAULT_CUP_HANDLE_PARAMS
from stockanalyse_api.services.dashboard_strategy_backtest import (
    _simulate_trade,
    run_cup_handle_rps_backtest,
)
from stockanalyse_api.services.optimization_backtest import (
    _attach_average_annualized_return,
    _extract_metrics,
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
                "allow_reentry_while_open": [False],
            }
        )

        self.assertEqual(parameter_sets[0]["take_profit_pct"], "0.2500")
        self.assertEqual(parameter_sets[0]["rps_exit_threshold"], 80)
        self.assertEqual(parameter_sets[0]["portfolio_cap"], 10)
        self.assertEqual(parameter_sets[0]["position_weight_pct"], "0.1000")
        self.assertFalse(parameter_sets[0]["allow_reentry_while_open"])

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
        self.assertEqual(validation_trade["entry_reason"], "RPS 达到 80；未启用杯柄过滤")
        self.assertEqual(validation_trade["exit_reason_label"], "RPS 跌破退出阈值")
        self.assertEqual(validation_trade["entry_price"], "100.000000")
        self.assertEqual(validation_trade["exit_price"], "92.000000")

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
