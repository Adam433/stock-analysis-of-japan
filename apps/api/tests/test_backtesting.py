from __future__ import annotations

import unittest
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stockanalyse_api.db.base import Base
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
                high_proximity_threshold_pct=Decimal("5.00"),
            )
            materialize_derived_indicator_facts(session)

    def test_launch_backtest_run_persists_range_and_parameter_set(self) -> None:
        with self.session_factory() as session:
            run = launch_backtest_run(
                session,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

        self.assertEqual(run.status, "running")
        self.assertEqual(run.start_date, "2024-01-01")
        self.assertEqual(run.end_date, "2024-12-31")
        self.assertEqual(run.parameter_set["version"], 1)

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
