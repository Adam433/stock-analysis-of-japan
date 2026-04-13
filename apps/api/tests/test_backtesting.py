from __future__ import annotations

import unittest
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stockanalyse_api.db.base import Base
from stockanalyse_api.services.backtesting import get_backtest_run, get_latest_backtest_run, launch_backtest_run


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
