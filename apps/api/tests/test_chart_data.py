from __future__ import annotations

import unittest
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.chart_data import get_stock_detail_payload
from stockanalyse_api.services.backtesting import execute_backtest_run, launch_backtest_run
from stockanalyse_api.services.factor_materialization import materialize_derived_indicator_facts
from stockanalyse_api.services.screening import execute_screen_run
from stockanalyse_api.services.strategy_config import get_active_strategy_configuration, save_strategy_configuration


class ChartDataTests(unittest.TestCase):
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

    def _seed_screen_context(self) -> tuple[int, int]:
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
                            open=leader_close - Decimal("1"),
                            high=leader_close + Decimal("2"),
                            low=leader_close - Decimal("2"),
                            close=leader_close,
                            adj_close=leader_close,
                            volume=1000 + index,
                            data_status="complete",
                            data_source="test",
                        ),
                        MarketDataDaily(
                            instrument_id=middle.id,
                            trade_date=trade_date,
                            open=middle_close - Decimal("1"),
                            high=middle_close + Decimal("2"),
                            low=middle_close - Decimal("2"),
                            close=middle_close,
                            adj_close=middle_close,
                            volume=2000 + index,
                            data_status="complete",
                            data_source="test",
                        ),
                        MarketDataDaily(
                            instrument_id=laggard.id,
                            trade_date=trade_date,
                            open=laggard_close - Decimal("1"),
                            high=laggard_close + Decimal("2"),
                            low=laggard_close - Decimal("2"),
                            close=laggard_close,
                            adj_close=laggard_close,
                            volume=3000 + index,
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
            run = execute_screen_run(session)
            qualified_instrument_id = run.qualified_results[0]["instrument_id"]
            return qualified_instrument_id, run.id

    def test_get_stock_detail_payload_returns_chart_data_and_rule_breakdown(self) -> None:
        instrument_id, screen_run_id = self._seed_screen_context()

        with self.session_factory() as session:
            payload = get_stock_detail_payload(
                session,
                instrument_id=instrument_id,
                screen_run_id=screen_run_id,
            )

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload.instrument["symbol"], "7203")
        self.assertEqual(payload.screen_run["id"], screen_run_id)
        self.assertTrue(payload.rule_breakdown["passed"])
        self.assertTrue(payload.rule_breakdown["rps_condition"]["passed"])
        self.assertTrue(payload.rule_breakdown["high_proximity_condition"]["passed"])
        self.assertEqual(payload.rule_breakdown["rps_condition"]["threshold"], 90)
        self.assertIsNotNone(payload.rule_breakdown["rps_condition"]["best_rps_value"])
        self.assertEqual(payload.latest_indicator_snapshot["trade_date"], payload.screen_run["trade_date"])
        self.assertGreaterEqual(len(payload.candlesticks), 100)
        self.assertEqual(payload.candlesticks[-1]["trade_date"], payload.screen_run["trade_date"])
        self.assertGreaterEqual(len(payload.indicator_history), 10)
        self.assertEqual(payload.indicator_history[-1]["trade_date"], payload.screen_run["trade_date"])
        self.assertIsNotNone(payload.indicator_history[-1]["rps_50"])
        self.assertEqual(
            payload.latest_indicator_snapshot["rps_50"],
            payload.indicator_history[-1]["rps_50"],
        )
        self.assertEqual(
            payload.latest_indicator_snapshot["rps_120"],
            payload.indicator_history[-1]["rps_120"],
        )
        self.assertEqual(
            payload.latest_indicator_snapshot["rps_250"],
            payload.indicator_history[-1]["rps_250"],
        )

    def test_get_stock_detail_payload_returns_none_for_invalid_run_binding(self) -> None:
        instrument_id, screen_run_id = self._seed_screen_context()

        with self.session_factory() as session:
            payload = get_stock_detail_payload(
                session,
                instrument_id=instrument_id + 999,
                screen_run_id=screen_run_id,
            )

        self.assertIsNone(payload)

    def test_chart_detail_trade_date_stays_aligned_with_single_day_backtest_dataset_context(self) -> None:
        instrument_id, screen_run_id = self._seed_screen_context()

        with self.session_factory() as session:
            payload = get_stock_detail_payload(
                session,
                instrument_id=instrument_id,
                screen_run_id=screen_run_id,
            )
            assert payload is not None
            backtest = launch_backtest_run(
                session,
                start_date=date.fromisoformat(payload.screen_run["trade_date"]),
                end_date=date.fromisoformat(payload.screen_run["trade_date"]),
            )
            completed = execute_backtest_run(session, backtest.id)

        self.assertEqual(completed.dataset_trade_date_start, payload.screen_run["trade_date"])
        self.assertEqual(completed.dataset_trade_date_end, payload.screen_run["trade_date"])
        self.assertEqual(payload.latest_indicator_snapshot["trade_date"], payload.screen_run["trade_date"])
        self.assertEqual(payload.indicator_history[-1]["trade_date"], payload.screen_run["trade_date"])


if __name__ == "__main__":
    unittest.main()
