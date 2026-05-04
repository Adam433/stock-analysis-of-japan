from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.orm import sessionmaker

from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.fundamentals.models import FundamentalsAnnual
from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
import stockanalyse_api.domain.screens.models  # noqa: F401
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
        self.assertEqual(result["hits"][0]["fundamental_growth_status"], "passed")
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
                            source_as_of_date=(
                                date(year, 6, 1) if year < 2023 else date(2026, 1, 1)
                            ),
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
        self.assertEqual(
            result["diagnostics"]["fundamental_growth_status_counts"]["insufficient_history"],
            1,
        )

    def test_fundamental_growth_reports_missing_status(self) -> None:
        signal_date = date(2024, 8, 1)
        with self.session_factory() as session:
            instrument = Instrument(symbol="AAPL", exchange="US", name="Apple")
            session.add(instrument)
            session.flush()
            session.add(
                DerivedIndicatorDaily(
                    instrument_id=instrument.id,
                    trade_date=signal_date,
                    rps_50=Decimal("95"),
                    rps_120=Decimal("95"),
                    rps_250=Decimal("95"),
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
                fundamental_growth_params=FundamentalGrowthParams(enabled=True),
                trade_date=signal_date,
                market="us",
            )

        self.assertEqual(result["hits"], [])
        self.assertEqual(result["diagnostics"]["fundamental_growth_status_counts"]["missing"], 1)

    def test_fundamental_growth_can_require_latest_valuation_thresholds(self) -> None:
        signal_date = date(2024, 8, 1)
        with self.session_factory() as session:
            cheap = Instrument(symbol="AAPL", exchange="US", name="Apple")
            expensive = Instrument(symbol="MSFT", exchange="US", name="Microsoft")
            missing = Instrument(symbol="NVDA", exchange="US", name="Nvidia")
            session.add_all([cheap, expensive, missing])
            session.flush()
            for instrument in (cheap, expensive, missing):
                session.add(
                    DerivedIndicatorDaily(
                        instrument_id=instrument.id,
                        trade_date=signal_date,
                        rps_50=Decimal("95"),
                        rps_120=Decimal("95"),
                        rps_250=Decimal("95"),
                    )
                )
                session.add(
                    MarketDataDaily(
                        instrument_id=instrument.id,
                        trade_date=signal_date,
                        close=(
                            Decimal("30")
                            if instrument is cheap
                            else Decimal("100")
                            if instrument is expensive
                            else Decimal("40")
                        ),
                        adj_close=(
                            Decimal("30")
                            if instrument is cheap
                            else Decimal("100")
                            if instrument is expensive
                            else Decimal("40")
                        ),
                        data_status="complete",
                    )
                )
                for year, value in ((2021, "100"), (2022, "120"), (2023, "150")):
                    session.add(
                        FundamentalsAnnual(
                            instrument_id=instrument.id,
                            fiscal_year_end_date=date(year, 12, 31),
                            fiscal_year_label=f"FY{year}",
                            net_income=Decimal(value),
                            net_income_currency="USD",
                            diluted_eps=Decimal("2") if instrument is not missing and year == 2023 else None,
                            stockholders_equity=(
                                Decimal("500") if instrument is not missing and year == 2023 else None
                            ),
                            weighted_average_diluted_shares=(
                                Decimal("100") if instrument is not missing and year == 2023 else None
                            ),
                            source="test",
                            source_as_of_date=date(2024, 2, 15),
                            data_status="complete" if instrument is not missing else "partial",
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
                    max_pe=Decimal("30"),
                    max_pb=Decimal("8"),
                ),
                trade_date=signal_date,
                market="us",
            )

        self.assertEqual([hit["symbol"] for hit in result["hits"]], ["AAPL"])
        self.assertEqual(result["diagnostics"]["fundamental_growth_status_counts"]["passed"], 1)
        self.assertEqual(
            result["diagnostics"]["fundamental_growth_status_counts"]["valuation_failed"],
            1,
        )
        self.assertEqual(
            result["diagnostics"]["fundamental_growth_status_counts"]["valuation_missing"],
            1,
        )

    def test_fundamental_growth_can_require_cash_flow_quality(self) -> None:
        signal_date = date(2024, 8, 1)
        with self.session_factory() as session:
            quality = Instrument(symbol="AAPL", exchange="US", name="Apple")
            weak = Instrument(symbol="MSFT", exchange="US", name="Microsoft")
            missing = Instrument(symbol="NVDA", exchange="US", name="Nvidia")
            session.add_all([quality, weak, missing])
            session.flush()
            for instrument in (quality, weak, missing):
                session.add(
                    DerivedIndicatorDaily(
                        instrument_id=instrument.id,
                        trade_date=signal_date,
                        rps_50=Decimal("95"),
                        rps_120=Decimal("95"),
                        rps_250=Decimal("95"),
                    )
                )
            values = {
                quality.id: [("100", "40"), ("130", "50"), ("170", "60")],
                weak.id: [("100", "40"), ("90", "30"), ("80", "20")],
                missing.id: [(None, None), (None, None), (None, None)],
            }
            for instrument in (quality, weak, missing):
                for index, year in enumerate((2021, 2022, 2023)):
                    operating, free = values[instrument.id][index]
                    session.add(
                        FundamentalsAnnual(
                            instrument_id=instrument.id,
                            fiscal_year_end_date=date(year, 12, 31),
                            fiscal_year_label=f"FY{year}",
                            net_income=Decimal("100") + Decimal(index * 20),
                            net_income_currency="USD",
                            operating_cash_flow=(
                                Decimal(operating) if operating is not None else None
                            ),
                            free_cash_flow=Decimal(free) if free is not None else None,
                            source="test",
                            source_as_of_date=signal_date,
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
                    require_positive_operating_cash_flow=True,
                    require_positive_free_cash_flow=True,
                    min_operating_cash_flow_growth_count=2,
                ),
                trade_date=signal_date,
                market="us",
            )

        self.assertEqual([hit["symbol"] for hit in result["hits"]], ["AAPL"])
        self.assertEqual(result["diagnostics"]["fundamental_growth_status_counts"]["passed"], 1)
        self.assertEqual(
            result["diagnostics"]["fundamental_growth_status_counts"]["cash_flow_growth_failed"],
            1,
        )
        self.assertEqual(
            result["diagnostics"]["fundamental_growth_status_counts"]["cash_flow_missing"],
            1,
        )

    def test_fundamental_growth_cache_reuses_loaded_annual_rows(self) -> None:
        first_date = date(2024, 8, 1)
        second_date = date(2024, 8, 2)
        with self.session_factory() as session:
            first = Instrument(symbol="AAPL", exchange="US", name="Apple")
            second = Instrument(symbol="MSFT", exchange="US", name="Microsoft")
            session.add_all([first, second])
            session.flush()
            for trade_date in (first_date, second_date):
                session.add_all(
                    [
                        DerivedIndicatorDaily(
                            instrument_id=first.id,
                            trade_date=trade_date,
                            rps_50=Decimal("95"),
                            rps_120=Decimal("95"),
                            rps_250=Decimal("95"),
                        ),
                        DerivedIndicatorDaily(
                            instrument_id=second.id,
                            trade_date=trade_date,
                            rps_50=Decimal("92"),
                            rps_120=Decimal("92"),
                            rps_250=Decimal("92"),
                        ),
                    ]
                )
            for instrument in (first, second):
                session.add_all(
                    [
                        FundamentalsAnnual(
                            instrument_id=instrument.id,
                            fiscal_year_end_date=date(2021, 12, 31),
                            fiscal_year_label="FY2021",
                            net_income=Decimal("100"),
                            net_income_currency="USD",
                            source="test",
                            source_as_of_date=first_date,
                            data_status="complete",
                        ),
                        FundamentalsAnnual(
                            instrument_id=instrument.id,
                            fiscal_year_end_date=date(2022, 12, 31),
                            fiscal_year_label="FY2022",
                            net_income=Decimal("120"),
                            net_income_currency="USD",
                            source="test",
                            source_as_of_date=first_date,
                            data_status="complete",
                        ),
                        FundamentalsAnnual(
                            instrument_id=instrument.id,
                            fiscal_year_end_date=date(2023, 12, 31),
                            fiscal_year_label="FY2023",
                            net_income=Decimal("150"),
                            net_income_currency="USD",
                            source="test",
                            source_as_of_date=first_date,
                            data_status="complete",
                        ),
                    ]
                )
            session.commit()

            statements = {"fundamental_selects": 0}

            def count_fundamental_selects(*args) -> None:
                statement = str(args[2]).lower()
                if "from fundamentals_annual" in statement:
                    statements["fundamental_selects"] += 1

            event.listen(self.engine, "before_cursor_execute", count_fundamental_selects)
            try:
                shared_cache: dict[tuple[object, ...], object] = {}
                for trade_date in (first_date, second_date):
                    result = screen_universe(
                        session,
                        use_rps=True,
                        rps_threshold=85,
                        selected_rps_windows=[50, 120, 250],
                        min_rps_windows_passing=2,
                        use_cup_handle=False,
                        fundamental_growth_params=FundamentalGrowthParams(
                            enabled=True,
                            reporting_lag_days=120,
                        ),
                        trade_date=trade_date,
                        market="us",
                        fundamental_growth_cache=shared_cache,
                    )
                    self.assertEqual(
                        [hit["symbol"] for hit in result["hits"]],
                        ["AAPL", "MSFT"],
                    )
            finally:
                event.remove(self.engine, "before_cursor_execute", count_fundamental_selects)

        self.assertEqual(statements["fundamental_selects"], 1)


if __name__ == "__main__":
    unittest.main()
