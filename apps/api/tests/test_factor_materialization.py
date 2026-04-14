from __future__ import annotations

import unittest
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.jobs.materialize_derived_facts import main as materialize_job_main
from stockanalyse_api.services.factor_materialization import materialize_derived_indicator_facts


class FactorMaterializationTests(unittest.TestCase):
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
            laggard = Instrument(symbol="6758", exchange="TSE", name="Laggard")
            session.add_all([leader, laggard])
            session.flush()

            leader_rows: list[MarketDataDaily] = []
            laggard_rows: list[MarketDataDaily] = []
            for index in range(260):
                trade_date = start_date + timedelta(days=index)
                leader_close = Decimal("100") + Decimal(index)
                laggard_close = Decimal("100") + Decimal(index) / Decimal("10")
                if index >= 240:
                    laggard_close -= Decimal(index - 239) / Decimal("2")

                leader_rows.append(
                    MarketDataDaily(
                        instrument_id=leader.id,
                        trade_date=trade_date,
                        close=leader_close,
                        adj_close=leader_close,
                        data_status="complete",
                        data_source="test",
                    )
                )
                laggard_rows.append(
                    MarketDataDaily(
                        instrument_id=laggard.id,
                        trade_date=trade_date,
                        close=laggard_close,
                        adj_close=laggard_close,
                        data_status="complete",
                        data_source="test",
                    )
                )

            session.add_all(leader_rows + laggard_rows)
            session.commit()

    def test_materialize_derived_indicator_facts_persists_rps_and_high_proximity(self) -> None:
        self._seed_market_data()

        with self.session_factory() as session:
            result = materialize_derived_indicator_facts(session)
            facts = session.execute(
                select(DerivedIndicatorDaily).order_by(
                    DerivedIndicatorDaily.trade_date.desc(),
                    DerivedIndicatorDaily.instrument_id.asc(),
                )
            ).scalars().all()

        self.assertEqual(result["inserted"], 520)
        self.assertEqual(result["updated"], 0)
        self.assertEqual(len(facts), 520)

        latest_trade_date = max(fact.trade_date for fact in facts)
        latest_facts = [fact for fact in facts if fact.trade_date == latest_trade_date]
        self.assertEqual(len(latest_facts), 2)

        leader_fact = latest_facts[0]
        laggard_fact = latest_facts[1]
        self.assertEqual(leader_fact.rps_50, Decimal("100.00"))
        self.assertEqual(leader_fact.rps_120, Decimal("100.00"))
        self.assertEqual(leader_fact.rps_250, Decimal("100.00"))
        self.assertEqual(laggard_fact.rps_50, Decimal("0.00"))
        self.assertEqual(laggard_fact.rps_120, Decimal("0.00"))
        self.assertEqual(laggard_fact.rps_250, Decimal("0.00"))
        self.assertEqual(leader_fact.high_proximity_ratio, Decimal("1.000000"))
        self.assertLess(laggard_fact.high_proximity_ratio, Decimal("1.000000"))

    def test_materialize_derived_indicator_facts_updates_existing_rows(self) -> None:
        self._seed_market_data()

        with self.session_factory() as session:
            materialize_derived_indicator_facts(session)
            result = materialize_derived_indicator_facts(session)
            row_count = session.execute(select(DerivedIndicatorDaily)).scalars().all()

        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["updated"], 520)
        self.assertEqual(len(row_count), 520)

    def test_materialize_derived_indicator_facts_commits_across_trade_date_batches(self) -> None:
        self._seed_market_data()

        with self.session_factory() as session:
            result = materialize_derived_indicator_facts(session, commit_every_dates=3)
            row_count = session.execute(select(DerivedIndicatorDaily)).scalars().all()

        self.assertEqual(result["inserted"], 520)
        self.assertEqual(result["updated"], 0)
        self.assertEqual(len(row_count), 520)

    def test_materialize_job_entrypoint_runs_with_registered_models(self) -> None:
        self._seed_market_data()

        with patch("stockanalyse_api.jobs.materialize_derived_facts.SessionLocal", self.session_factory):
            materialize_job_main()

        with self.session_factory() as session:
            row_count = session.execute(select(DerivedIndicatorDaily)).scalars().all()

        self.assertEqual(len(row_count), 520)


if __name__ == "__main__":
    unittest.main()
