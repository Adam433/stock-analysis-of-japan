from __future__ import annotations

import json
import os
import re
import unittest
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from alembic import command
from alembic.config import Config
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from stockanalyse_api.api.routes.backtests import (
    compare_portfolio_return_backtest_runs,
    read_portfolio_return_backtest_semantics_snapshot,
    read_portfolio_return_backtest_result,
    read_portfolio_return_backtest_trace,
)
from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.backtests.models import BacktestRun
from stockanalyse_api.domain.screens.models import ScreenRun, ScreenRunResult
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.backtesting import (
    execute_backtest_run,
    get_backtest_run,
    get_latest_backtest_run,
    list_backtest_runs,
    launch_backtest_run,
)
from stockanalyse_api.services.factor_materialization import materialize_derived_indicator_facts
from stockanalyse_api.services.portfolio_backtest import launch_portfolio_return_backtest
from stockanalyse_api.services.portfolio_backtest_defaults import (
    MVP_ENTRY_DEFERRAL_WINDOW_DAYS,
    MVP_HOLDING_DAYS,
    MVP_PORTFOLIO_VALUE,
    MVP_PORTFOLIO_CAP,
    MVP_STOP_LOSS_PCT,
)
from stockanalyse_api.services.portfolio_backtest_metrics import (
    calculate_max_drawdown,
    calculate_win_rate,
)
from stockanalyse_api.services.portfolio_backtest_traceability import resolve_semantics_via_source_screen_run
from stockanalyse_api.services.rps_semantics import (
    APPROVED_RPS_DEFINITION_VERSION,
    LEGACY_UNRECORDED_RPS_DEFINITION_VERSION,
)
from stockanalyse_api.services.screening import execute_screen_run, get_screen_run
from stockanalyse_api.services.strategy_config import get_active_strategy_configuration, save_strategy_configuration


class BacktestingTests(unittest.TestCase):
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

    def _build_alembic_config(self, database_url: str) -> Config:
        api_root = Path(__file__).resolve().parents[1]
        config = Config(str(api_root / "alembic.ini"))
        config.set_main_option("script_location", str(api_root / "migrations"))
        config.set_main_option("prepend_sys_path", str(api_root / "src"))
        config.set_main_option("sqlalchemy.url", database_url)
        return config

    def _seed_backtest_context(self) -> None:
        start_date = date(2024, 1, 1)
        with self.session_factory() as session:
            leader = Instrument(symbol="7203", exchange="TSE", name="Leader")
            middle = Instrument(symbol="6758", exchange="TSE", name="Middle")
            laggard = Instrument(symbol="9984", exchange="TSE", name="Laggard")
            session.add_all([leader, middle, laggard])
            session.flush()

            rows: list[MarketDataDaily] = []
            for index in range(320):
                trade_date = start_date + timedelta(days=index)
                leader_close = Decimal("100") + Decimal(index)
                middle_close = Decimal("120") + Decimal(index) / Decimal("3")
                laggard_close = Decimal("140") + Decimal(index) / Decimal("10")
                if index >= 240:
                    middle_close -= Decimal(index - 239) / Decimal("4")
                    laggard_close -= Decimal(index - 239)

                rows.extend(
                    [
                        MarketDataDaily(
                            instrument_id=leader.id,
                            trade_date=trade_date,
                            open=leader_close,
                            high=leader_close,
                            low=leader_close,
                            close=leader_close,
                            adj_close=leader_close,
                            volume=1000 + index,
                            data_status="complete",
                            data_source="test",
                        ),
                        MarketDataDaily(
                            instrument_id=middle.id,
                            trade_date=trade_date,
                            open=middle_close,
                            high=middle_close,
                            low=middle_close,
                            close=middle_close,
                            adj_close=middle_close,
                            volume=1000 + index,
                            data_status="complete",
                            data_source="test",
                        ),
                        MarketDataDaily(
                            instrument_id=laggard.id,
                            trade_date=trade_date,
                            open=laggard_close,
                            high=laggard_close,
                            low=laggard_close,
                            close=laggard_close,
                            adj_close=laggard_close,
                            volume=1000 + index,
                            data_status="complete",
                            data_source="test",
                        ),
                    ]
                )

            session.add_all(rows)
            session.commit()

        with self.session_factory() as session:
            get_active_strategy_configuration(session)
            save_strategy_configuration(
                session,
                rps_threshold=90,
                selected_rps_windows=[50, 120, 250],
                high_proximity_threshold_pct=Decimal("5.00"),
            )
            materialize_derived_indicator_facts(session)

    def _create_completed_screen_run(self) -> int:
        self._seed_backtest_context()

        with self.session_factory() as session:
            screen_run = execute_screen_run(session, trade_date=date(2024, 9, 16))

        return screen_run.id

    def _seed_portfolio_execution_fixture(
        self,
        *,
        screen_trade_date: date,
        candidates: list[dict[str, object]],
        market_data_by_symbol: dict[str, list[dict[str, object]]],
        rps_definition_version: str = APPROVED_RPS_DEFINITION_VERSION,
    ) -> dict[str, object]:
        with self.session_factory() as session:
            configuration = get_active_strategy_configuration(session)
            instruments_by_symbol: dict[str, Instrument] = {}

            for candidate in candidates:
                symbol = str(candidate["symbol"])
                instrument = Instrument(symbol=symbol, exchange="TSE", name=symbol)
                session.add(instrument)
                instruments_by_symbol[symbol] = instrument

            session.flush()

            for symbol, rows in market_data_by_symbol.items():
                instrument = instruments_by_symbol[symbol]
                for row in rows:
                    open_value = row.get("open")
                    close_value = row.get("close", open_value)
                    adj_close_value = row.get("adj_close", close_value)
                    session.add(
                        MarketDataDaily(
                            instrument_id=instrument.id,
                            trade_date=row["trade_date"],
                            open=open_value,
                            high=row.get("high", close_value),
                            low=row.get("low", close_value),
                            close=close_value,
                            adj_close=adj_close_value,
                            volume=row.get("volume", 1000),
                            data_status=row.get("data_status", "complete"),
                            data_source="test",
                        )
                    )

            qualified_count = sum(1 for candidate in candidates if candidate.get("passed", True))
            screen_run = ScreenRun(
                strategy_configuration_id=configuration.id,
                trade_date=screen_trade_date,
                executed_at=datetime.now(UTC),
                rps_definition_version=rps_definition_version,
                total_candidates=len(candidates),
                qualified_count=qualified_count,
                status="completed",
            )
            session.add(screen_run)
            session.flush()

            for candidate in candidates:
                symbol = str(candidate["symbol"])
                instrument = instruments_by_symbol[symbol]
                passed = bool(candidate.get("passed", True))
                best_rps_value = Decimal(str(candidate.get("best_rps_value", "95")))
                session.add(
                    ScreenRunResult(
                        screen_run_id=screen_run.id,
                        instrument_id=instrument.id,
                        trade_date=screen_trade_date,
                        passed=passed,
                        rps_50=best_rps_value,
                        rps_120=best_rps_value,
                        rps_250=best_rps_value,
                        best_rps_value=best_rps_value,
                        rps_threshold=configuration.rps_threshold,
                        high_proximity_ratio=Decimal("0.980000"),
                        high_proximity_threshold_pct=configuration.high_proximity_threshold_pct,
                        max_drawdown_from_high_pct=Decimal("2.00"),
                        rps_condition_passed=passed,
                        high_proximity_condition_passed=passed,
                    )
                )

            session.commit()

            return {
                "screen_run_id": screen_run.id,
                "instrument_ids_by_symbol": {
                    symbol: instrument.id for symbol, instrument in instruments_by_symbol.items()
                },
            }

    def _create_legacy_condition_hit_run(self) -> int:
        with self.session_factory() as session:
            configuration = get_active_strategy_configuration(session)
            run = BacktestRun(
                strategy_configuration_id=configuration.id,
                source_screen_run_id=None,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC),
                rps_definition_version=LEGACY_UNRECORDED_RPS_DEFINITION_VERSION,
                backtest_lifecycle="legacy_condition_hit",
                status="completed",
                trade_dates_evaluated=20,
                total_candidates_evaluated=40,
                qualifying_observations=4,
                unique_qualified_instruments=4,
                result_checksum="legacy-checksum",
            )
            session.add(run)
            session.commit()
            session.refresh(run)
            return run.id

    def test_launch_backtest_run_persists_range_and_parameter_set(self) -> None:
        with self.session_factory() as session:
            run = launch_backtest_run(
                session,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

        self.assertEqual(run.status, "running")
        self.assertEqual(run.backtest_lifecycle, "legacy_condition_hit")
        self.assertEqual(run.start_date, "2024-01-01")
        self.assertEqual(run.end_date, "2024-12-31")
        self.assertEqual(run.rps_definition_version, APPROVED_RPS_DEFINITION_VERSION)
        self.assertEqual(run.parameter_set["version"], 1)
        self.assertEqual(run.parameter_set["selected_rps_windows"], [50, 120, 250])

    def test_launch_backtest_run_rejects_invalid_date_range(self) -> None:
        with self.session_factory() as session:
            with self.assertRaises(ValueError):
                launch_backtest_run(
                    session,
                    start_date=date(2024, 12, 31),
                    end_date=date(2024, 1, 1),
                )

    def test_get_latest_backtest_run_returns_latest_persisted_run(self) -> None:
        with self.session_factory() as session:
            first = launch_backtest_run(
                session,
                start_date=date(2023, 1, 1),
                end_date=date(2023, 6, 30),
            )
            second = launch_backtest_run(
                session,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 6, 30),
            )
            latest = get_latest_backtest_run(session)
            fetched = get_backtest_run(session, second.id)

        self.assertEqual(first.id + 1, second.id)
        self.assertIsNotNone(latest)
        self.assertIsNotNone(fetched)
        assert latest is not None
        assert fetched is not None
        self.assertEqual(latest.id, second.id)
        self.assertEqual(latest.backtest_lifecycle, "legacy_condition_hit")
        self.assertEqual(latest.rps_definition_version, APPROVED_RPS_DEFINITION_VERSION)
        self.assertEqual(fetched.backtest_lifecycle, "legacy_condition_hit")
        self.assertEqual(fetched.rps_definition_version, APPROVED_RPS_DEFINITION_VERSION)
        self.assertEqual(fetched.start_date, "2024-01-01")

    def test_list_backtest_runs_returns_runs_with_latest_first(self) -> None:
        with self.session_factory() as session:
            first = launch_backtest_run(
                session,
                start_date=date(2023, 1, 1),
                end_date=date(2023, 6, 30),
            )
            second = launch_backtest_run(
                session,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 6, 30),
            )
            runs = list_backtest_runs(session)

        self.assertEqual([run.id for run in runs], [second.id, first.id])
        self.assertEqual(runs[0].backtest_lifecycle, "legacy_condition_hit")
        self.assertEqual(runs[1].backtest_lifecycle, "legacy_condition_hit")
        self.assertEqual(runs[0].rps_definition_version, APPROVED_RPS_DEFINITION_VERSION)
        self.assertEqual(runs[1].rps_definition_version, APPROVED_RPS_DEFINITION_VERSION)

    def test_execute_backtest_run_completes_with_reproducible_summary(self) -> None:
        self._seed_backtest_context()

        with self.session_factory() as session:
            first = launch_backtest_run(
                session,
                start_date=date(2024, 9, 1),
                end_date=date(2024, 9, 16),
            )
            second = launch_backtest_run(
                session,
                start_date=date(2024, 9, 1),
                end_date=date(2024, 9, 16),
            )
            first_completed = execute_backtest_run(session, first.id)
            second_completed = execute_backtest_run(session, second.id)

        self.assertEqual(first_completed.status, "completed")
        self.assertEqual(second_completed.status, "completed")
        self.assertEqual(first_completed.backtest_lifecycle, "legacy_condition_hit")
        self.assertEqual(second_completed.backtest_lifecycle, "legacy_condition_hit")
        self.assertGreater(first_completed.result_summary["trade_dates_evaluated"], 0)
        self.assertGreater(first_completed.result_summary["qualifying_observations"], 0)
        self.assertEqual(
            first_completed.result_summary["result_checksum"],
            second_completed.result_summary["result_checksum"],
        )
        self.assertEqual(
            first_completed.result_summary["unique_qualified_instruments"],
            second_completed.result_summary["unique_qualified_instruments"],
        )
        self.assertEqual(first_completed.rps_definition_version, APPROVED_RPS_DEFINITION_VERSION)
        self.assertEqual(second_completed.rps_definition_version, APPROVED_RPS_DEFINITION_VERSION)
        self.assertEqual(first_completed.dataset_trade_date_start, "2024-09-01")
        self.assertEqual(first_completed.dataset_trade_date_end, "2024-09-16")
        self.assertIsNotNone(first_completed.dataset_checksum)
        self.assertEqual(first_completed.dataset_checksum, second_completed.dataset_checksum)

    def test_execute_backtest_run_without_derived_facts_preserves_run_version_context(self) -> None:
        with self.session_factory() as session:
            launched = launch_backtest_run(
                session,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )
            with self.assertRaises(ValueError):
                execute_backtest_run(session, launched.id)
            fetched = get_backtest_run(session, launched.id)

        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched.status, "failed")
        self.assertEqual(fetched.backtest_lifecycle, "legacy_condition_hit")
        self.assertEqual(fetched.rps_definition_version, APPROVED_RPS_DEFINITION_VERSION)
        self.assertIsNone(fetched.dataset_trade_date_start)
        self.assertIsNone(fetched.dataset_trade_date_end)
        self.assertIsNone(fetched.dataset_checksum)

    def test_get_backtest_run_returns_explicit_legacy_marker_when_version_is_missing(self) -> None:
        with self.session_factory() as session:
            launch_backtest_run(
                session,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )
            persisted = session.execute(select(BacktestRun).limit(1)).scalar_one()
            persisted.rps_definition_version = None
            session.commit()
            fetched = get_backtest_run(session, persisted.id)

        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched.backtest_lifecycle, "legacy_condition_hit")
        self.assertEqual(fetched.rps_definition_version, LEGACY_UNRECORDED_RPS_DEFINITION_VERSION)

    def test_single_day_backtest_stays_aligned_with_screening_semantics(self) -> None:
        self._seed_backtest_context()

        with self.session_factory() as session:
            screen_run = execute_screen_run(session, trade_date=date(2024, 9, 16))
            backtest = launch_backtest_run(
                session,
                start_date=date(2024, 9, 16),
                end_date=date(2024, 9, 16),
            )
            completed = execute_backtest_run(session, backtest.id)

        self.assertEqual(screen_run.rps_definition_version, APPROVED_RPS_DEFINITION_VERSION)
        self.assertEqual(completed.rps_definition_version, APPROVED_RPS_DEFINITION_VERSION)
        self.assertEqual(completed.backtest_lifecycle, "legacy_condition_hit")
        self.assertEqual(screen_run.trade_date, "2024-09-16")
        self.assertEqual(completed.dataset_trade_date_start, screen_run.trade_date)
        self.assertEqual(completed.dataset_trade_date_end, screen_run.trade_date)
        self.assertEqual(completed.result_summary["trade_dates_evaluated"], 1)
        self.assertEqual(completed.result_summary["qualifying_observations"], screen_run.qualified_count)

    def test_migration_upgrade_rejects_invalid_portfolio_return_provenance_rows(self) -> None:
        with TemporaryDirectory() as tmpdir:
            database_url = f"sqlite:///{Path(tmpdir) / 'migration.db'}"
            config = self._build_alembic_config(database_url)

            with patch.dict(os.environ, {"STOCKANALYSE_DATABASE_URL": database_url}, clear=False):
                command.upgrade(config, "20260417_0019")

            engine = create_engine(database_url, future=True)
            try:
                with engine.begin() as connection:
                    connection.execute(
                        text(
                            "INSERT INTO strategy_configurations "
                            "(version, rps_threshold, high_proximity_threshold_pct) "
                            "VALUES (1, 90, 5.00)"
                        )
                    )
                    connection.execute(
                        text(
                            "INSERT INTO backtest_runs "
                            "("
                            "strategy_configuration_id, start_date, end_date, started_at, status, "
                            "backtest_lifecycle, rps_definition_version"
                            ") "
                            "VALUES "
                            "(1, '2024-01-01', '2024-01-31', '2024-01-02 09:00:00+00:00', 'completed', "
                            "'portfolio_return', 'rps-v1')"
                        )
                    )

                with patch.dict(os.environ, {"STOCKANALYSE_DATABASE_URL": database_url}, clear=False):
                    with self.assertRaises(RuntimeError):
                        command.upgrade(config, "20260417_0020")
            finally:
                engine.dispose()

    def test_migration_upgrade_backfills_legacy_condition_hit_lifecycle(self) -> None:
        with TemporaryDirectory() as tmpdir:
            database_url = f"sqlite:///{Path(tmpdir) / 'migration.db'}"
            config = self._build_alembic_config(database_url)

            with patch.dict(os.environ, {"STOCKANALYSE_DATABASE_URL": database_url}, clear=False):
                command.upgrade(config, "20260415_0016")

            engine = create_engine(database_url, future=True)
            try:
                with engine.begin() as connection:
                    connection.execute(
                        text(
                            "INSERT INTO strategy_configurations "
                            "(version, rps_threshold, high_proximity_threshold_pct) "
                            "VALUES (1, 90, 5.00)"
                        )
                    )
                    connection.execute(
                        text(
                            "INSERT INTO backtest_runs "
                            "(strategy_configuration_id, start_date, end_date, started_at, status) "
                            "VALUES (1, '2024-01-01', '2024-12-31', '2024-01-02 09:00:00+00:00', 'running')"
                        )
                    )

                with patch.dict(os.environ, {"STOCKANALYSE_DATABASE_URL": database_url}, clear=False):
                    command.upgrade(config, "20260417_0017")

                with engine.connect() as connection:
                    lifecycle = connection.execute(text("SELECT backtest_lifecycle FROM backtest_runs")).scalar_one()
                    null_count = connection.execute(
                        text("SELECT COUNT(*) FROM backtest_runs WHERE backtest_lifecycle IS NULL")
                    ).scalar_one()

                self.assertEqual(lifecycle, "legacy_condition_hit")
                self.assertEqual(null_count, 0)
            finally:
                engine.dispose()

    def test_migration_upgrade_rejects_invalid_lifecycle_value(self) -> None:
        with TemporaryDirectory() as tmpdir:
            database_url = f"sqlite:///{Path(tmpdir) / 'migration.db'}"
            config = self._build_alembic_config(database_url)

            with patch.dict(os.environ, {"STOCKANALYSE_DATABASE_URL": database_url}, clear=False):
                command.upgrade(config, "20260417_0017")

            engine = create_engine(database_url, future=True)
            try:
                with engine.begin() as connection:
                    connection.execute(
                        text(
                            "INSERT INTO strategy_configurations "
                            "(version, rps_threshold, high_proximity_threshold_pct) "
                            "VALUES (1, 90, 5.00)"
                        )
                    )

                with self.assertRaises(IntegrityError):
                    with engine.begin() as connection:
                        connection.execute(
                            text(
                                "INSERT INTO backtest_runs "
                                "("
                                "strategy_configuration_id, start_date, end_date, started_at, status, backtest_lifecycle"
                                ") "
                                "VALUES "
                                "(1, '2024-01-01', '2024-12-31', '2024-01-02 09:00:00+00:00', 'running', 'invalid')"
                            )
                        )
            finally:
                engine.dispose()

    def test_backtest_run_rejects_invalid_lifecycle_value(self) -> None:
        with self.session_factory() as session:
            configuration = get_active_strategy_configuration(session)
            run = BacktestRun(
                strategy_configuration_id=configuration.id,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
                started_at=datetime.now(UTC),
                completed_at=None,
                rps_definition_version=APPROVED_RPS_DEFINITION_VERSION,
                backtest_lifecycle="invalid-lifecycle",
                status="running",
                error_message=None,
            )
            session.add(run)

            with self.assertRaises(IntegrityError):
                session.commit()

            session.rollback()

    def test_backtest_run_rejects_portfolio_return_without_source_screen_run(self) -> None:
        with self.session_factory() as session:
            configuration = get_active_strategy_configuration(session)
            run = BacktestRun(
                strategy_configuration_id=configuration.id,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
                started_at=datetime.now(UTC),
                completed_at=None,
                rps_definition_version=None,
                backtest_lifecycle="portfolio_return",
                status="running",
                error_message=None,
            )
            session.add(run)

            with self.assertRaises(IntegrityError):
                session.commit()

            session.rollback()

    def test_backtest_run_rejects_portfolio_return_with_rps_definition_version(self) -> None:
        with self.session_factory() as session:
            configuration = get_active_strategy_configuration(session)
            screen_run = ScreenRun(
                strategy_configuration_id=configuration.id,
                trade_date=date(2024, 9, 16),
                executed_at=datetime.now(UTC),
                status="completed",
                total_candidates=0,
                qualified_count=0,
                rps_definition_version=APPROVED_RPS_DEFINITION_VERSION,
            )
            session.add(screen_run)
            session.flush()

            run = BacktestRun(
                strategy_configuration_id=configuration.id,
                source_screen_run_id=screen_run.id,
                start_date=date(2024, 9, 16),
                end_date=date(2024, 9, 16),
                started_at=datetime.now(UTC),
                completed_at=None,
                rps_definition_version=APPROVED_RPS_DEFINITION_VERSION,
                backtest_lifecycle="portfolio_return",
                status="running",
                error_message=None,
            )
            session.add(run)

            with self.assertRaises(IntegrityError):
                session.commit()

            session.rollback()

    def test_portfolio_backtest_launch_constructor_does_not_store_screening_definition_fields(self) -> None:
        service_path = Path(__file__).resolve().parents[1] / "src/stockanalyse_api/services/portfolio_backtest.py"
        source = service_path.read_text(encoding="utf-8")
        match = re.search(r"run = BacktestRun\((?P<body>.*?)\n        \)", source, re.DOTALL)

        self.assertIsNotNone(match)
        constructor_body = match.group("body")
        self.assertNotIn("rps_definition_version=", constructor_body)
        self.assertNotIn("rps_threshold=", constructor_body)
        self.assertNotIn("selected_rps_windows=", constructor_body)
        self.assertNotIn("high_proximity_threshold_pct=", constructor_body)

    def test_launch_portfolio_return_backtest_creates_run_from_completed_screen_run(self) -> None:
        screen_run_id = self._create_completed_screen_run()

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(session, screen_run_id=screen_run_id)
            persisted = session.get(BacktestRun, launched.id)

        self.assertIsNotNone(persisted)
        assert persisted is not None
        self.assertEqual(launched.backtest_lifecycle, "portfolio_return")
        self.assertEqual(launched.status, "completed")
        self.assertIsNone(launched.rps_definition_version)
        self.assertEqual(persisted.source_screen_run_id, screen_run_id)
        self.assertIsNone(persisted.rps_definition_version)
        self.assertEqual(persisted.effective_holding_days, MVP_HOLDING_DAYS)
        self.assertEqual(f"{persisted.effective_stop_loss_pct:.4f}", f"{MVP_STOP_LOSS_PCT:.4f}")
        self.assertEqual(persisted.effective_portfolio_cap, MVP_PORTFOLIO_CAP)
        self.assertEqual(
            persisted.effective_entry_deferral_window_days,
            MVP_ENTRY_DEFERRAL_WINDOW_DAYS,
        )

    def test_launch_portfolio_return_backtest_uses_defaults_module_values(self) -> None:
        screen_run_id = self._create_completed_screen_run()

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(session, screen_run_id=screen_run_id)

        self.assertEqual(launched.effective_holding_days, MVP_HOLDING_DAYS)
        self.assertEqual(launched.effective_stop_loss_pct, f"{MVP_STOP_LOSS_PCT:.4f}")
        self.assertEqual(launched.effective_portfolio_cap, MVP_PORTFOLIO_CAP)
        self.assertEqual(
            launched.effective_entry_deferral_window_days,
            MVP_ENTRY_DEFERRAL_WINDOW_DAYS,
        )

    def test_launch_portfolio_return_backtest_rejects_invalid_parameters(self) -> None:
        screen_run_id = self._create_completed_screen_run()

        invalid_payloads = (
            {"holding_days": 0},
            {"stop_loss_pct": Decimal("-1.00")},
            {"stop_loss_pct": Decimal("0")},
            {"portfolio_cap": 0},
            {"entry_deferral_window_days": 0},
        )

        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                with self.session_factory() as session:
                    with self.assertRaises(ValueError):
                        launch_portfolio_return_backtest(session, screen_run_id=screen_run_id, **payload)

    def test_launch_portfolio_return_backtest_rejects_explicit_rps_definition_version(self) -> None:
        screen_run_id = self._create_completed_screen_run()

        with self.session_factory() as session:
            with self.assertRaises(ValueError):
                launch_portfolio_return_backtest(
                    session,
                    screen_run_id=screen_run_id,
                    rps_definition_version="rps-v9-should-not-pass",
                )

    def test_launch_portfolio_return_backtest_deduplicates_same_screen_run_within_window(self) -> None:
        screen_run_id = self._create_completed_screen_run()

        with patch(
            "stockanalyse_api.services.portfolio_backtest.dispatch_portfolio_return_backtest_execution",
            return_value=None,
        ):
            with self.session_factory() as session:
                first = launch_portfolio_return_backtest(session, screen_run_id=screen_run_id)
                second = launch_portfolio_return_backtest(session, screen_run_id=screen_run_id)

        self.assertEqual(first.id, second.id)

    def test_launch_portfolio_return_backtest_creates_new_run_after_recent_completion(self) -> None:
        screen_run_id = self._create_completed_screen_run()

        with self.session_factory() as session:
            first = launch_portfolio_return_backtest(session, screen_run_id=screen_run_id)
            persisted = session.get(BacktestRun, first.id)
            assert persisted is not None
            persisted.status = "completed"
            persisted.completed_at = datetime.now(UTC)
            session.commit()

            second = launch_portfolio_return_backtest(session, screen_run_id=screen_run_id)

        self.assertNotEqual(first.id, second.id)

    def test_launch_portfolio_return_backtest_rejects_non_completed_screen_run(self) -> None:
        with self.session_factory() as session:
            configuration = get_active_strategy_configuration(session)
            screen_run = ScreenRun(
                strategy_configuration_id=configuration.id,
                trade_date=date(2024, 9, 16),
                executed_at=datetime.now(UTC),
                status="running",
                total_candidates=0,
                qualified_count=0,
            )
            session.add(screen_run)
            session.commit()
            session.refresh(screen_run)

            with self.assertRaises(ValueError):
                launch_portfolio_return_backtest(session, screen_run_id=screen_run.id)

    def test_launch_portfolio_return_backtest_marks_failed_recoverable_and_retries_same_record(self) -> None:
        screen_run_id = self._create_completed_screen_run()

        with patch(
            "stockanalyse_api.services.portfolio_backtest.dispatch_portfolio_return_backtest_execution",
            side_effect=[RuntimeError("dispatcher boom"), None],
        ):
            with self.session_factory() as session:
                failed = launch_portfolio_return_backtest(session, screen_run_id=screen_run_id)
                retried = launch_portfolio_return_backtest(session, screen_run_id=screen_run_id)
                persisted = session.get(BacktestRun, failed.id)

        self.assertEqual(failed.status, "failed-recoverable")
        self.assertEqual(failed.id, retried.id)
        self.assertEqual(retried.status, "running")
        self.assertIsNotNone(persisted)
        assert persisted is not None
        self.assertEqual(persisted.status, "running")
        self.assertIsNone(persisted.error_message)

    def test_launch_portfolio_return_backtest_executes_t_plus_one_equal_weight_positions(self) -> None:
        fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[
                {"symbol": "7203", "best_rps_value": "99.00"},
                {"symbol": "6758", "best_rps_value": "97.00"},
            ],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("101"), "adj_close": Decimal("101")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("101"), "close": Decimal("103"), "adj_close": Decimal("103")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("104"), "close": Decimal("104"), "adj_close": Decimal("104")},
                ],
                "6758": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("200"), "close": Decimal("202"), "adj_close": Decimal("202")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("202"), "close": Decimal("205"), "adj_close": Decimal("205")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("210"), "close": Decimal("210"), "adj_close": Decimal("210")},
                ],
            },
        )

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=3,
            )

        self.assertEqual(launched.status, "completed")
        self.assertEqual(launched.backtest_lifecycle, "portfolio_return")
        self.assertEqual(launched.position_count_after_exclusions, 2)
        self.assertEqual(launched.portfolio_value, f"{MVP_PORTFOLIO_VALUE:.6f}")
        self.assertEqual(launched.ranking_policy_id, "rps_desc_ticker_asc_v1")
        self.assertEqual(launched.cumulative_return, "0.045000")
        self.assertEqual(len(launched.per_security_returns), 2)
        self.assertEqual(launched.per_security_returns[0]["entry_date"], "2024-09-17")
        self.assertEqual(launched.per_security_returns[0]["exit_date"], "2024-09-19")
        self.assertEqual(launched.per_security_returns[0]["exit_reason"], "holding_period_elapsed")
        self.assertEqual(launched.per_security_returns[0]["realized_return"], "0.040000")
        self.assertEqual(launched.per_security_returns[1]["realized_return"], "0.050000")
        self.assertEqual(launched.excluded_securities, [])
        self.assertEqual(launched.equity_curve[-1]["equity"], "1.045000")

    def test_launch_portfolio_return_backtest_applies_cap_ranking_and_exclusion_list(self) -> None:
        fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[
                {"symbol": "7203", "best_rps_value": "99.00"},
                {"symbol": "6758", "best_rps_value": "97.00"},
                {"symbol": "6501", "best_rps_value": "97.00"},
            ],
            market_data_by_symbol={
                symbol: [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("100"), "adj_close": Decimal("100")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("100"), "close": Decimal("100"), "adj_close": Decimal("100")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("100"), "close": Decimal("100"), "adj_close": Decimal("100")},
                ]
                for symbol in ("7203", "6758", "6501")
            },
        )

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=2,
                entry_deferral_window_days=2,
            )

        self.assertEqual(launched.status, "completed")
        self.assertEqual(launched.ranking_policy_id, "rps_desc_ticker_asc_v1")
        self.assertEqual(launched.position_count_after_exclusions, 2)
        self.assertEqual(len(launched.excluded_securities), 1)
        self.assertEqual(launched.excluded_securities[0]["symbol"], "6758")
        self.assertEqual(launched.excluded_securities[0]["exclusion_reason"], "cap_overflow")

    def test_launch_portfolio_return_backtest_applies_entry_deferral_and_exclusion_reasons(self) -> None:
        fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[
                {"symbol": "7203", "best_rps_value": "99.00"},
                {"symbol": "6758", "best_rps_value": "98.00"},
                {"symbol": "6501", "best_rps_value": "97.00"},
            ],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": None, "close": Decimal("100"), "adj_close": Decimal("100")},
                    {"trade_date": date(2024, 9, 18), "open": None, "close": Decimal("101"), "adj_close": Decimal("101")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("102"), "close": Decimal("103"), "adj_close": Decimal("103")},
                    {"trade_date": date(2024, 9, 20), "open": Decimal("104"), "close": Decimal("104"), "adj_close": Decimal("104")},
                    {"trade_date": date(2024, 9, 21), "open": Decimal("105"), "close": Decimal("105"), "adj_close": Decimal("105")},
                ],
                "6758": [
                    {"trade_date": date(2024, 9, 17), "open": None, "close": Decimal("200"), "adj_close": Decimal("200")},
                    {"trade_date": date(2024, 9, 18), "open": None, "close": Decimal("201"), "adj_close": Decimal("201")},
                    {"trade_date": date(2024, 9, 19), "open": None, "close": Decimal("202"), "adj_close": Decimal("202")},
                ],
                "6501": [
                    {"trade_date": date(2024, 9, 17), "open": None, "close": None, "adj_close": None, "data_status": "unavailable"},
                    {"trade_date": date(2024, 9, 18), "open": None, "close": None, "adj_close": None, "data_status": "unavailable"},
                    {"trade_date": date(2024, 9, 19), "open": None, "close": None, "adj_close": None, "data_status": "unavailable"},
                ],
            },
        )

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=3,
            )

        self.assertEqual(launched.status, "completed")
        self.assertEqual(launched.position_count_after_exclusions, 1)
        self.assertEqual(launched.per_security_returns[0]["symbol"], "7203")
        self.assertEqual(launched.per_security_returns[0]["entry_date"], "2024-09-19")
        excluded_by_symbol = {
            item["symbol"]: item["exclusion_reason"] for item in launched.excluded_securities
        }
        self.assertEqual(excluded_by_symbol["6758"], "no_valid_open_in_deferral_window")
        self.assertEqual(
            excluded_by_symbol["6501"],
            "suspended_delisted_or_corp_action_in_deferral_window",
        )

    def test_launch_portfolio_return_backtest_handles_stop_loss_gap_down_and_exit_deferral(self) -> None:
        fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[
                {"symbol": "7203", "best_rps_value": "99.00"},
                {"symbol": "6758", "best_rps_value": "98.00"},
            ],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("100"), "adj_close": Decimal("100")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("99"), "close": Decimal("91"), "adj_close": Decimal("91")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("80"), "close": Decimal("81"), "adj_close": Decimal("81")},
                ],
                "6758": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("100"), "adj_close": Decimal("100")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("99"), "close": Decimal("90"), "adj_close": Decimal("90")},
                    {"trade_date": date(2024, 9, 19), "open": None, "close": Decimal("88"), "adj_close": Decimal("88")},
                    {"trade_date": date(2024, 9, 20), "open": Decimal("85"), "close": Decimal("85"), "adj_close": Decimal("85")},
                ],
            },
        )

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=5,
                stop_loss_pct=Decimal("-0.08"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )

        per_security = {item["symbol"]: item for item in launched.per_security_returns}
        self.assertEqual(per_security["7203"]["exit_reason"], "stop_loss")
        self.assertEqual(per_security["7203"]["exit_date"], "2024-09-19")
        self.assertEqual(per_security["7203"]["realized_return"], "-0.200000")
        self.assertEqual(per_security["6758"]["exit_reason"], "stop_loss")
        self.assertEqual(per_security["6758"]["exit_date"], "2024-09-20")
        self.assertEqual(per_security["6758"]["realized_return"], "-0.150000")

    def test_launch_portfolio_return_backtest_returns_empty_portfolio_when_all_candidates_excluded(self) -> None:
        fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[{"symbol": "7203", "best_rps_value": "99.00"}],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": None, "close": Decimal("100"), "adj_close": Decimal("100")},
                    {"trade_date": date(2024, 9, 18), "open": None, "close": Decimal("101"), "adj_close": Decimal("101")},
                ],
            },
        )

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )

        self.assertEqual(launched.status, "completed")
        self.assertEqual(launched.position_count_after_exclusions, 0)
        self.assertEqual(launched.cumulative_return, "0.000000")
        self.assertEqual(launched.per_security_returns, [])
        self.assertEqual(launched.equity_curve, [])
        self.assertEqual(launched.dataset_trade_date_start, "2024-09-17")
        self.assertEqual(launched.dataset_trade_date_end, "2024-09-18")
        self.assertIsNotNone(launched.dataset_checksum)

    def test_launch_portfolio_return_backtest_fails_when_data_is_insufficient(self) -> None:
        fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[{"symbol": "7203", "best_rps_value": "99.00"}],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("101"), "adj_close": Decimal("101")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("101"), "close": Decimal("102"), "adj_close": Decimal("102")},
                ],
            },
        )

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=3,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )

        self.assertEqual(launched.status, "failed-data-insufficient")
        self.assertEqual(launched.error_message, "数据不足以完成持有期")
        self.assertEqual(launched.dataset_trade_date_start, "2024-09-17")
        self.assertEqual(launched.dataset_trade_date_end, "2024-09-18")
        self.assertIsNotNone(launched.dataset_checksum)

    def test_launch_portfolio_return_backtest_fails_when_entry_window_cannot_be_evaluated(self) -> None:
        fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[{"symbol": "7203", "best_rps_value": "99.00"}],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": None, "close": Decimal("100"), "adj_close": Decimal("100")},
                ],
            },
        )

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )

        self.assertEqual(launched.status, "failed-data-insufficient")
        self.assertEqual(launched.error_message, "数据不足以完成持有期")
        self.assertEqual(launched.dataset_trade_date_start, "2024-09-17")
        self.assertEqual(launched.dataset_trade_date_end, "2024-09-17")
        self.assertIsNotNone(launched.dataset_checksum)

    def test_launch_portfolio_return_backtest_is_deterministic_and_surfaces_dataset_changes(self) -> None:
        fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[{"symbol": "7203", "best_rps_value": "99.00"}],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("100"), "adj_close": Decimal("100")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("101"), "close": Decimal("102"), "adj_close": Decimal("102")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("104"), "close": Decimal("104"), "adj_close": Decimal("104")},
                ],
            },
        )

        with self.session_factory() as session:
            first = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )
            second = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )
            row = session.execute(
                select(MarketDataDaily)
                .join(Instrument, Instrument.id == MarketDataDaily.instrument_id)
                .where(Instrument.symbol == "7203", MarketDataDaily.trade_date == date(2024, 9, 19))
            ).scalar_one()
            row.open = Decimal("110")
            session.commit()
            third = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )

        self.assertEqual(first.status, "completed")
        self.assertEqual(second.status, "completed")
        self.assertEqual(first.cumulative_return, second.cumulative_return)
        self.assertEqual(first.dataset_checksum, second.dataset_checksum)
        self.assertEqual(first.result_summary["result_checksum"], second.result_summary["result_checksum"])
        self.assertNotEqual(first.dataset_checksum, third.dataset_checksum)
        self.assertNotEqual(first.cumulative_return, third.cumulative_return)

    def test_portfolio_backtest_service_does_not_define_rebalance_or_reentry_paths(self) -> None:
        service_path = Path(__file__).resolve().parents[1] / "src/stockanalyse_api/services/portfolio_backtest.py"
        source = service_path.read_text(encoding="utf-8")

        self.assertNotIn("rebalance", source)
        self.assertNotIn("re_entry", source)
        self.assertNotIn("add_position", source)

    def test_calculate_win_rate_handles_edge_cases(self) -> None:
        self.assertEqual(calculate_win_rate([]), Decimal("0"))
        self.assertEqual(
            calculate_win_rate(
                [
                    {"realized_return": "0.050000"},
                    {"realized_return": "-0.020000"},
                    {"realized_return": "0.000000"},
                ]
            ),
            Decimal("0.333333"),
        )
        self.assertEqual(
            calculate_win_rate(
                [
                    {"realized_return": "0.010000"},
                    {"realized_return": "0.020000"},
                ]
            ),
            Decimal("1.000000"),
        )

    def test_calculate_max_drawdown_handles_edge_cases(self) -> None:
        self.assertEqual(calculate_max_drawdown([]), Decimal("0"))
        self.assertEqual(
            calculate_max_drawdown([{"trade_date": "2024-09-17", "equity": "1.000000"}]),
            Decimal("0"),
        )
        self.assertEqual(
            calculate_max_drawdown(
                [
                    {"trade_date": "2024-09-17", "equity": "1.000000"},
                    {"trade_date": "2024-09-18", "equity": "1.200000"},
                    {"trade_date": "2024-09-19", "equity": "0.900000"},
                    {"trade_date": "2024-09-20", "equity": "1.100000"},
                ]
            ),
            Decimal("0.250000"),
        )

    def test_result_route_returns_portfolio_metrics_and_source_screen_run(self) -> None:
        fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[
                {"symbol": "7203", "best_rps_value": "99.00"},
                {"symbol": "6758", "best_rps_value": "97.00"},
            ],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("100"), "adj_close": Decimal("100")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("101"), "close": Decimal("104"), "adj_close": Decimal("104")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("104"), "close": Decimal("104"), "adj_close": Decimal("104")},
                ],
                "6758": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("200"), "close": Decimal("200"), "adj_close": Decimal("200")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("198"), "close": Decimal("180"), "adj_close": Decimal("180")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("190"), "close": Decimal("190"), "adj_close": Decimal("190")},
                ],
            },
        )

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )

        with patch("stockanalyse_api.api.routes.backtests.SessionLocal", self.session_factory):
            payload = read_portfolio_return_backtest_result(launched.id)

        result = payload["result"]
        self.assertEqual(result["run"]["id"], launched.id)
        self.assertEqual(result["cumulative_return"], launched.cumulative_return)
        self.assertEqual(result["win_rate"], "0.500000")
        self.assertEqual(result["max_drawdown"], "0.030000")
        self.assertEqual(result["source_screen_run"]["id"], fixture["screen_run_id"])
        self.assertEqual(result["source_screen_run"]["trade_date"], "2024-09-16")
        self.assertEqual(result["source_screen_run"]["strategy_configuration_version"], 1)
        self.assertEqual(len(result["equity_curve"]), 3)
        self.assertEqual(len(result["per_security_returns"]), 2)

    def test_result_route_rejects_legacy_runs(self) -> None:
        with self.session_factory() as session:
            get_active_strategy_configuration(session)
        legacy_run_id = self._create_legacy_condition_hit_run()

        with patch("stockanalyse_api.api.routes.backtests.SessionLocal", self.session_factory):
            with self.assertRaises(HTTPException) as context:
                read_portfolio_return_backtest_result(legacy_run_id)

        self.assertEqual(context.exception.status_code, 422)

    def test_trace_route_returns_source_screen_run_trace_and_null_run_rps_version(self) -> None:
        fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[{"symbol": "7203", "best_rps_value": "99.00"}],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("101"), "adj_close": Decimal("101")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("101"), "close": Decimal("102"), "adj_close": Decimal("102")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("103"), "close": Decimal("103"), "adj_close": Decimal("103")},
                ],
            },
            rps_definition_version="rps-v9-trace",
        )

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )

        with patch("stockanalyse_api.api.routes.backtests.SessionLocal", self.session_factory):
            payload = read_portfolio_return_backtest_trace(launched.id)

        trace = payload["trace"]
        self.assertEqual(trace["backtest_run"]["id"], launched.id)
        self.assertIsNone(trace["backtest_run"]["rps_definition_version"])
        self.assertEqual(trace["source_screen_run"]["id"], fixture["screen_run_id"])
        self.assertEqual(trace["source_screen_run"]["trade_date"], "2024-09-16")
        self.assertEqual(trace["source_screen_run"]["status"], "completed")
        self.assertEqual(trace["source_screen_run"]["strategy_configuration_id"], launched.strategy_configuration_id)
        self.assertEqual(trace["source_screen_run"]["rps_definition_version"], "rps-v9-trace")
        self.assertEqual(trace["parameter_snapshot"]["rps_threshold"], 90)
        self.assertEqual(trace["parameter_snapshot"]["selected_rps_windows"], [50, 120, 250])
        self.assertEqual(trace["parameter_snapshot"]["min_rps_lines_required"], 1)
        self.assertEqual(trace["dataset_version"]["trade_date_start"], "2024-09-17")
        self.assertEqual(trace["dataset_version"]["trade_date_end"], "2024-09-19")
        self.assertIsNotNone(trace["dataset_version"]["checksum"])

    def test_semantics_snapshot_route_matches_source_screen_run_snapshot(self) -> None:
        fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[{"symbol": "7203", "best_rps_value": "99.00"}],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("101"), "adj_close": Decimal("101")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("101"), "close": Decimal("102"), "adj_close": Decimal("102")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("103"), "close": Decimal("103"), "adj_close": Decimal("103")},
                ],
            },
            rps_definition_version="rps-v2-snapshot",
        )

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )
            screen_run = get_screen_run(session, fixture["screen_run_id"])
            traceability = resolve_semantics_via_source_screen_run(session, launched.id)

        with patch("stockanalyse_api.api.routes.backtests.SessionLocal", self.session_factory):
            payload = read_portfolio_return_backtest_semantics_snapshot(launched.id)

        semantics_snapshot = payload["semantics_snapshot"]
        self.assertEqual(semantics_snapshot, traceability["semantics_snapshot"])
        self.assertEqual(semantics_snapshot["source_screen_run_id"], fixture["screen_run_id"])
        self.assertEqual(semantics_snapshot["strategy_configuration_id"], screen_run.strategy_configuration_id)
        self.assertEqual(semantics_snapshot["strategy_configuration_version"], screen_run.parameter_set["version"])
        self.assertEqual(semantics_snapshot["rps_definition_version"], screen_run.rps_definition_version)
        self.assertEqual(semantics_snapshot["rps_threshold"], screen_run.parameter_set["rps_threshold"])
        self.assertEqual(semantics_snapshot["high_proximity_threshold_pct"], screen_run.parameter_set["high_proximity_threshold_pct"])
        self.assertEqual(semantics_snapshot["selected_rps_windows"], screen_run.parameter_set["selected_rps_windows"])
        self.assertEqual(semantics_snapshot["min_rps_lines_required"], 1)

    def test_trace_route_rejects_legacy_lifecycle_runs(self) -> None:
        with self.session_factory() as session:
            get_active_strategy_configuration(session)
        legacy_run_id = self._create_legacy_condition_hit_run()

        with patch("stockanalyse_api.api.routes.backtests.SessionLocal", self.session_factory):
            with self.assertRaises(HTTPException) as context:
                read_portfolio_return_backtest_trace(legacy_run_id)

        self.assertEqual(context.exception.status_code, 422)

    def test_trace_route_rejects_legacy_portfolio_range_runs(self) -> None:
        self._seed_backtest_context()

        with self.session_factory() as session:
            launched = launch_backtest_run(
                session,
                start_date=date(2024, 9, 16),
                end_date=date(2024, 9, 16),
            )
            completed = execute_backtest_run(session, launched.id)

        with patch("stockanalyse_api.api.routes.backtests.SessionLocal", self.session_factory):
            with self.assertRaises(HTTPException) as context:
                read_portfolio_return_backtest_trace(completed.id)

        self.assertEqual(context.exception.status_code, 422)
        self.assertEqual(context.exception.detail, "Only portfolio_return runs support this endpoint.")

    def test_trace_route_returns_gone_when_source_screen_run_is_unavailable(self) -> None:
        fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[{"symbol": "7203", "best_rps_value": "99.00"}],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("101"), "adj_close": Decimal("101")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("101"), "close": Decimal("102"), "adj_close": Decimal("102")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("103"), "close": Decimal("103"), "adj_close": Decimal("103")},
                ],
            },
        )

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )
            session.execute(text("DELETE FROM screen_run_results WHERE screen_run_id = :screen_run_id"), {"screen_run_id": fixture["screen_run_id"]})
            session.execute(text("DELETE FROM screen_runs WHERE id = :screen_run_id"), {"screen_run_id": fixture["screen_run_id"]})
            session.commit()

        with patch("stockanalyse_api.api.routes.backtests.SessionLocal", self.session_factory):
            response = read_portfolio_return_backtest_trace(launched.id)

        self.assertEqual(response.status_code, 410)
        payload = json.loads(response.body)
        self.assertEqual(payload["error"], "source_screen_run_unavailable")
        self.assertEqual(payload["error_code"], "source_screen_run_unavailable")
        self.assertEqual(payload["backtest_run_id"], launched.id)

    def test_result_route_returns_gone_when_source_screen_run_is_unavailable(self) -> None:
        fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[{"symbol": "7203", "best_rps_value": "99.00"}],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("101"), "adj_close": Decimal("101")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("101"), "close": Decimal("102"), "adj_close": Decimal("102")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("103"), "close": Decimal("103"), "adj_close": Decimal("103")},
                ],
            },
        )

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(
                session,
                screen_run_id=fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )
            session.execute(text("DELETE FROM screen_run_results WHERE screen_run_id = :screen_run_id"), {"screen_run_id": fixture["screen_run_id"]})
            session.execute(text("DELETE FROM screen_runs WHERE id = :screen_run_id"), {"screen_run_id": fixture["screen_run_id"]})
            session.commit()

        with patch("stockanalyse_api.api.routes.backtests.SessionLocal", self.session_factory):
            response = read_portfolio_return_backtest_result(launched.id)

        self.assertEqual(response.status_code, 410)
        payload = json.loads(response.body)
        self.assertEqual(payload["error_code"], "source_screen_run_unavailable")
        self.assertEqual(payload["backtest_run_id"], launched.id)

    def test_compare_route_aligns_curves_by_days_since_entry_and_rejects_legacy(self) -> None:
        first_fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[{"symbol": "7203", "best_rps_value": "99.00"}],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("101"), "adj_close": Decimal("101")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("101"), "close": Decimal("102"), "adj_close": Decimal("102")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("103"), "close": Decimal("103"), "adj_close": Decimal("103")},
                ],
            },
        )
        second_fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 20),
            candidates=[{"symbol": "6758", "best_rps_value": "98.00"}],
            market_data_by_symbol={
                "6758": [
                    {"trade_date": date(2024, 9, 23), "open": Decimal("200"), "close": Decimal("201"), "adj_close": Decimal("201")},
                    {"trade_date": date(2024, 9, 24), "open": Decimal("201"), "close": Decimal("202"), "adj_close": Decimal("202")},
                    {"trade_date": date(2024, 9, 25), "open": Decimal("203"), "close": Decimal("203"), "adj_close": Decimal("203")},
                ],
            },
        )

        with self.session_factory() as session:
            first = launch_portfolio_return_backtest(
                session,
                screen_run_id=first_fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )
            second = launch_portfolio_return_backtest(
                session,
                screen_run_id=second_fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )

        with patch("stockanalyse_api.api.routes.backtests.SessionLocal", self.session_factory):
            payload = compare_portfolio_return_backtest_runs(f"{first.id},{second.id}")

        runs = payload["runs"]
        self.assertEqual([point["days_since_entry"] for point in runs[0]["aligned_equity_curve"]], [0, 1, 2])
        self.assertEqual([point["days_since_entry"] for point in runs[1]["aligned_equity_curve"]], [0, 1, 2])
        self.assertEqual(runs[0]["compare_dimensions"]["source_trade_date"], "2024-09-16")
        self.assertEqual(runs[1]["compare_dimensions"]["source_trade_date"], "2024-09-20")
        self.assertEqual(runs[0]["compare_dimensions"]["rps_definition_version"], APPROVED_RPS_DEFINITION_VERSION)
        self.assertEqual(runs[1]["compare_dimensions"]["rps_definition_version"], APPROVED_RPS_DEFINITION_VERSION)

        legacy_run_id = self._create_legacy_condition_hit_run()
        with patch("stockanalyse_api.api.routes.backtests.SessionLocal", self.session_factory):
            with self.assertRaises(HTTPException) as context:
                compare_portfolio_return_backtest_runs(f"{first.id},{legacy_run_id}")

        self.assertEqual(context.exception.status_code, 422)

    def test_compare_route_rejects_legacy_portfolio_range_runs(self) -> None:
        self._seed_backtest_context()

        with self.session_factory() as session:
            screen_run = execute_screen_run(session, trade_date=date(2024, 9, 16))
            traced_run = launch_portfolio_return_backtest(
                session,
                screen_run_id=screen_run.id,
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )
            old_run = launch_backtest_run(
                session,
                start_date=date(2024, 9, 16),
                end_date=date(2024, 9, 16),
            )
            old_completed = execute_backtest_run(session, old_run.id)

        with patch("stockanalyse_api.api.routes.backtests.SessionLocal", self.session_factory):
            with self.assertRaises(HTTPException) as context:
                compare_portfolio_return_backtest_runs(f"{traced_run.id},{old_completed.id}")

        self.assertEqual(context.exception.status_code, 422)
        self.assertEqual(context.exception.detail, "Only portfolio_return runs support this endpoint.")

    def test_compare_route_returns_gone_when_source_screen_run_is_unavailable(self) -> None:
        first_fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[{"symbol": "7203", "best_rps_value": "99.00"}],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("101"), "adj_close": Decimal("101")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("101"), "close": Decimal("102"), "adj_close": Decimal("102")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("103"), "close": Decimal("103"), "adj_close": Decimal("103")},
                ],
            },
        )
        second_fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 20),
            candidates=[{"symbol": "6758", "best_rps_value": "98.00"}],
            market_data_by_symbol={
                "6758": [
                    {"trade_date": date(2024, 9, 23), "open": Decimal("200"), "close": Decimal("201"), "adj_close": Decimal("201")},
                    {"trade_date": date(2024, 9, 24), "open": Decimal("201"), "close": Decimal("202"), "adj_close": Decimal("202")},
                    {"trade_date": date(2024, 9, 25), "open": Decimal("203"), "close": Decimal("203"), "adj_close": Decimal("203")},
                ],
            },
        )

        with self.session_factory() as session:
            first = launch_portfolio_return_backtest(
                session,
                screen_run_id=first_fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )
            second = launch_portfolio_return_backtest(
                session,
                screen_run_id=second_fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )
            session.execute(text("DELETE FROM screen_run_results WHERE screen_run_id = :screen_run_id"), {"screen_run_id": first_fixture["screen_run_id"]})
            session.execute(text("DELETE FROM screen_runs WHERE id = :screen_run_id"), {"screen_run_id": first_fixture["screen_run_id"]})
            session.commit()

        with patch("stockanalyse_api.api.routes.backtests.SessionLocal", self.session_factory):
            response = compare_portfolio_return_backtest_runs(f"{first.id},{second.id}")

        self.assertEqual(response.status_code, 410)
        payload = json.loads(response.body)
        self.assertEqual(payload["error_code"], "source_screen_run_unavailable")
        self.assertEqual(payload["backtest_run_id"], first.id)

    def test_compare_route_includes_resolved_rps_definition_versions_for_each_run(self) -> None:
        first_fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 16),
            candidates=[{"symbol": "7203", "best_rps_value": "99.00"}],
            market_data_by_symbol={
                "7203": [
                    {"trade_date": date(2024, 9, 17), "open": Decimal("100"), "close": Decimal("101"), "adj_close": Decimal("101")},
                    {"trade_date": date(2024, 9, 18), "open": Decimal("101"), "close": Decimal("102"), "adj_close": Decimal("102")},
                    {"trade_date": date(2024, 9, 19), "open": Decimal("103"), "close": Decimal("103"), "adj_close": Decimal("103")},
                ],
            },
            rps_definition_version="rps-v1-compare",
        )
        second_fixture = self._seed_portfolio_execution_fixture(
            screen_trade_date=date(2024, 9, 20),
            candidates=[{"symbol": "6758", "best_rps_value": "98.00"}],
            market_data_by_symbol={
                "6758": [
                    {"trade_date": date(2024, 9, 23), "open": Decimal("200"), "close": Decimal("201"), "adj_close": Decimal("201")},
                    {"trade_date": date(2024, 9, 24), "open": Decimal("201"), "close": Decimal("202"), "adj_close": Decimal("202")},
                    {"trade_date": date(2024, 9, 25), "open": Decimal("203"), "close": Decimal("203"), "adj_close": Decimal("203")},
                ],
            },
            rps_definition_version="rps-v2-compare",
        )

        with self.session_factory() as session:
            first = launch_portfolio_return_backtest(
                session,
                screen_run_id=first_fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )
            second = launch_portfolio_return_backtest(
                session,
                screen_run_id=second_fixture["screen_run_id"],
                holding_days=2,
                stop_loss_pct=Decimal("-0.20"),
                portfolio_cap=5,
                entry_deferral_window_days=2,
            )

        with patch("stockanalyse_api.api.routes.backtests.SessionLocal", self.session_factory):
            payload = compare_portfolio_return_backtest_runs(f"{first.id},{second.id}")

        versions_by_run_id = {
            run["run"]["id"]: run["compare_dimensions"]["rps_definition_version"] for run in payload["runs"]
        }
        self.assertEqual(versions_by_run_id[first.id], "rps-v1-compare")
        self.assertEqual(versions_by_run_id[second.id], "rps-v2-compare")
