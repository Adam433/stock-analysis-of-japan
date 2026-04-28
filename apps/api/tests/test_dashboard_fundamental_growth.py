from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.fundamentals.models import FundamentalsAnnual
from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.services.dashboard import FundamentalGrowthParams
from stockanalyse_api.services.dashboard import screen_universe


class DashboardFundamentalGrowthTests(unittest.TestCase):
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

    def test_screen_requires_minimum_rps_window_count_and_fundamental_growth(self) -> None:
        signal_date = date(2024, 8, 1)
        with self.session_factory() as session:
            leader = Instrument(symbol="7203.T", exchange="TSE", name="Leader")
            laggard = Instrument(symbol="6758.T", exchange="TSE", name="Laggard")
            session.add_all([leader, laggard])
            session.flush()
            session.add_all(
                [
                    DerivedIndicatorDaily(
                        instrument_id=leader.id,
                        trade_date=signal_date,
                        rps_50=Decimal("90"),
                        rps_120=Decimal("88"),
                        rps_250=Decimal("40"),
                    ),
                    DerivedIndicatorDaily(
                        instrument_id=laggard.id,
                        trade_date=signal_date,
                        rps_50=Decimal("90"),
                        rps_120=Decimal("40"),
                        rps_250=Decimal("40"),
                    ),
                ]
            )
            for instrument in (leader, laggard):
                session.add_all(
                    [
                        FundamentalsAnnual(
                            instrument_id=instrument.id,
                            fiscal_year_end_date=date(2021, 3, 31),
                            fiscal_year_label="FY2021",
                            net_income=Decimal("100"),
                            net_income_currency="JPY",
                            source="test",
                            source_as_of_date=signal_date,
                            data_status="complete",
                        ),
                        FundamentalsAnnual(
                            instrument_id=instrument.id,
                            fiscal_year_end_date=date(2022, 3, 31),
                            fiscal_year_label="FY2022",
                            net_income=Decimal("120"),
                            net_income_currency="JPY",
                            source="test",
                            source_as_of_date=signal_date,
                            data_status="complete",
                        ),
                        FundamentalsAnnual(
                            instrument_id=instrument.id,
                            fiscal_year_end_date=date(2023, 3, 31),
                            fiscal_year_label="FY2023",
                            net_income=Decimal("150"),
                            net_income_currency="JPY",
                            source="test",
                            source_as_of_date=signal_date,
                            data_status="complete",
                        ),
                    ]
                )
            session.commit()

            result = screen_universe(
                session,
                use_rps=True,
                rps_threshold=85,
                selected_rps_windows=[50, 120, 250],
                min_rps_windows_passing=2,
                use_cup_handle=False,
                fundamental_growth_params=FundamentalGrowthParams(enabled=True),
                trade_date=signal_date,
            )

        self.assertEqual([hit["symbol"] for hit in result["hits"]], ["7203.T"])
        self.assertEqual(result["hits"][0]["rps_pass_count"], 2)
        self.assertEqual(result["hits"][0]["fundamental_growth_count"], 2)

    def test_fundamental_growth_uses_reporting_lag_to_avoid_future_data(self) -> None:
        with self.session_factory() as session:
            instrument = Instrument(symbol="7203.T", exchange="TSE", name="Leader")
            session.add(instrument)
            session.flush()
            session.add(
                DerivedIndicatorDaily(
                    instrument_id=instrument.id,
                    trade_date=date(2023, 5, 1),
                    rps_50=Decimal("90"),
                    rps_120=Decimal("90"),
                    rps_250=Decimal("90"),
                )
            )
            for year, value in ((2021, "100"), (2022, "120"), (2023, "150")):
                session.add(
                    FundamentalsAnnual(
                        instrument_id=instrument.id,
                        fiscal_year_end_date=date(year, 3, 31),
                        fiscal_year_label=f"FY{year}",
                        net_income=Decimal(value),
                        net_income_currency="JPY",
                        source="test",
                        source_as_of_date=date(2026, 1, 1),
                        data_status="complete",
                    )
                )
            session.commit()

            result = screen_universe(
                session,
                use_rps=True,
                rps_threshold=85,
                selected_rps_windows=[50, 120, 250],
                min_rps_windows_passing=2,
                use_cup_handle=False,
                fundamental_growth_params=FundamentalGrowthParams(
                    enabled=True,
                    min_years=3,
                    reporting_lag_days=120,
                ),
                trade_date=date(2023, 5, 1),
            )

        self.assertEqual(result["hits"], [])


if __name__ == "__main__":
    unittest.main()
