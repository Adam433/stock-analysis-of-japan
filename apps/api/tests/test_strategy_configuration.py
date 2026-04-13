from __future__ import annotations

import unittest
from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.screens.models import StrategyConfiguration
from stockanalyse_api.services.strategy_config import (
    get_active_strategy_configuration,
    save_strategy_configuration,
)


class StrategyConfigurationTests(unittest.TestCase):
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

    def test_get_active_strategy_configuration_creates_default_configuration(self) -> None:
        with self.session_factory() as session:
            configuration = get_active_strategy_configuration(session)

        self.assertEqual(configuration.version, 1)
        self.assertEqual(configuration.rps_threshold, 90)
        self.assertEqual(configuration.high_proximity_threshold_pct, "5.00")

    def test_save_strategy_configuration_creates_new_active_version(self) -> None:
        with self.session_factory() as session:
            get_active_strategy_configuration(session)
            updated_configuration = save_strategy_configuration(
                session,
                rps_threshold=95,
                high_proximity_threshold_pct=Decimal("3.50"),
            )
            rows = session.execute(
                select(StrategyConfiguration).order_by(StrategyConfiguration.version.asc())
            ).scalars().all()

        self.assertEqual(updated_configuration.version, 2)
        self.assertEqual(updated_configuration.rps_threshold, 95)
        self.assertEqual(updated_configuration.high_proximity_threshold_pct, "3.50")
        self.assertEqual(len(rows), 2)
        self.assertFalse(rows[0].is_active)
        self.assertTrue(rows[1].is_active)

    def test_save_strategy_configuration_rejects_out_of_range_values(self) -> None:
        with self.session_factory() as session:
            with self.assertRaises(ValueError):
                save_strategy_configuration(
                    session,
                    rps_threshold=101,
                    high_proximity_threshold_pct=Decimal("5.00"),
                )

            with self.assertRaises(ValueError):
                save_strategy_configuration(
                    session,
                    rps_threshold=90,
                    high_proximity_threshold_pct=Decimal("-1"),
                )


if __name__ == "__main__":
    unittest.main()
