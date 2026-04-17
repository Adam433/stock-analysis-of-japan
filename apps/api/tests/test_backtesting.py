from __future__ import annotations

import os
import unittest
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.backtests.models import BacktestRun
from stockanalyse_api.domain.screens.models import ScreenRun
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
    MVP_PORTFOLIO_CAP,
    MVP_STOP_LOSS_PCT,
)
from stockanalyse_api.services.rps_semantics import (
    APPROVED_RPS_DEFINITION_VERSION,
    LEGACY_UNRECORDED_RPS_DEFINITION_VERSION,
)
from stockanalyse_api.services.screening import execute_screen_run
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
            for index in range(260):
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
                            close=leader_close,
                            adj_close=leader_close,
                            data_status="complete",
                            data_source="test",
                        ),
                        MarketDataDaily(
                            instrument_id=middle.id,
                            trade_date=trade_date,
                            close=middle_close,
                            adj_close=middle_close,
                            data_status="complete",
                            data_source="test",
                        ),
                        MarketDataDaily(
                            instrument_id=laggard.id,
                            trade_date=trade_date,
                            close=laggard_close,
                            adj_close=laggard_close,
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

    def test_launch_backtest_run_persists_range_and_parameter_set(self) -> None:
        with self.session_factory() as session:
            run = launch_backtest_run(
                session,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

        self.assertEqual(run.status, "running")
        self.assertEqual(run.backtest_lifecycle, "portfolio_return")
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
        self.assertEqual(latest.backtest_lifecycle, "portfolio_return")
        self.assertEqual(latest.rps_definition_version, APPROVED_RPS_DEFINITION_VERSION)
        self.assertEqual(fetched.backtest_lifecycle, "portfolio_return")
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
        self.assertEqual(runs[0].backtest_lifecycle, "portfolio_return")
        self.assertEqual(runs[1].backtest_lifecycle, "portfolio_return")
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
        self.assertEqual(first_completed.backtest_lifecycle, "portfolio_return")
        self.assertEqual(second_completed.backtest_lifecycle, "portfolio_return")
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
        self.assertEqual(fetched.backtest_lifecycle, "portfolio_return")
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
        self.assertEqual(fetched.backtest_lifecycle, "portfolio_return")
        self.assertEqual(fetched.rps_definition_version, LEGACY_UNRECORDED_RPS_DEFINITION_VERSION)

    def test_single_day_backtest_stays_aligned_with_screening_semantics(self) -> None:
        self._seed_backtest_context()

        with self.session_factory() as session:
            screen_run = execute_screen_run(session)
            backtest = launch_backtest_run(
                session,
                start_date=date(2024, 9, 16),
                end_date=date(2024, 9, 16),
            )
            completed = execute_backtest_run(session, backtest.id)

        self.assertEqual(screen_run.rps_definition_version, APPROVED_RPS_DEFINITION_VERSION)
        self.assertEqual(completed.rps_definition_version, APPROVED_RPS_DEFINITION_VERSION)
        self.assertEqual(completed.backtest_lifecycle, "portfolio_return")
        self.assertEqual(screen_run.trade_date, "2024-09-16")
        self.assertEqual(completed.dataset_trade_date_start, screen_run.trade_date)
        self.assertEqual(completed.dataset_trade_date_end, screen_run.trade_date)
        self.assertEqual(completed.result_summary["trade_dates_evaluated"], 1)
        self.assertEqual(completed.result_summary["qualifying_observations"], screen_run.qualified_count)

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

    def test_launch_portfolio_return_backtest_creates_run_from_completed_screen_run(self) -> None:
        screen_run_id = self._create_completed_screen_run()

        with self.session_factory() as session:
            launched = launch_portfolio_return_backtest(session, screen_run_id=screen_run_id)
            persisted = session.get(BacktestRun, launched.id)

        self.assertIsNotNone(persisted)
        assert persisted is not None
        self.assertEqual(launched.backtest_lifecycle, "portfolio_return")
        self.assertEqual(launched.status, "running")
        self.assertEqual(persisted.source_screen_run_id, screen_run_id)
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

    def test_launch_portfolio_return_backtest_deduplicates_same_screen_run_within_window(self) -> None:
        screen_run_id = self._create_completed_screen_run()

        with self.session_factory() as session:
            first = launch_portfolio_return_backtest(session, screen_run_id=screen_run_id)
            second = launch_portfolio_return_backtest(session, screen_run_id=screen_run_id)

        self.assertEqual(first.id, second.id)

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
