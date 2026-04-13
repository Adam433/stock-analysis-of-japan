from __future__ import annotations

import unittest
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.domain.screens.models import ScreenRun, ScreenRunResult
from stockanalyse_api.services.factor_materialization import materialize_derived_indicator_facts
from stockanalyse_api.services.screening import execute_screen_run, get_latest_screen_run, get_screen_run
from stockanalyse_api.services.strategy_config import get_active_strategy_configuration, save_strategy_configuration


class ScreeningTests(unittest.TestCase):
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

    def _seed_market_data(self) -> None:
        start_date = date(2025, 1, 1)
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

    def test_execute_screen_run_persists_run_and_qualified_results(self) -> None:
        self._seed_market_data()

        with self.session_factory() as session:
            summary = execute_screen_run(session)
            runs = session.execute(select(ScreenRun)).scalars().all()
            results = session.execute(select(ScreenRunResult)).scalars().all()

        self.assertEqual(len(runs), 1)
        self.assertEqual(summary.total_candidates, 3)
        self.assertEqual(summary.qualified_count, 1)
        self.assertEqual(summary.parameter_set["version"], 2)
        self.assertEqual(len(summary.qualified_results), 1)
        self.assertEqual(summary.qualified_results[0]["symbol"], "7203")
        self.assertEqual(len(results), 3)
        self.assertTrue(any(result.passed for result in results))

    def test_get_screen_run_returns_traceable_result_values(self) -> None:
        self._seed_market_data()

        with self.session_factory() as session:
            created = execute_screen_run(session)
            fetched = get_screen_run(session, created.id)

        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched.id, created.id)
        self.assertEqual(fetched.parameter_set["rps_threshold"], 90)
        self.assertEqual(fetched.qualified_results[0]["rps_condition_passed"], True)
        self.assertEqual(fetched.qualified_results[0]["high_proximity_condition_passed"], True)
        self.assertIsNotNone(fetched.qualified_results[0]["best_rps_value"])

    def test_execute_screen_run_requires_derived_facts(self) -> None:
        with self.session_factory() as session:
            get_active_strategy_configuration(session)
            with self.assertRaises(ValueError):
                execute_screen_run(session)

    def test_get_latest_screen_run_returns_most_recent_run(self) -> None:
        self._seed_market_data()

        with self.session_factory() as session:
            first = execute_screen_run(session)
            second = execute_screen_run(session)
            latest = get_latest_screen_run(session)

        self.assertEqual(first.id + 1, second.id)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.id, second.id)


if __name__ == "__main__":
    unittest.main()
