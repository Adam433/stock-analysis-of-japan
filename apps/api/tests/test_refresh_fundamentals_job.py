from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import stockanalyse_api.domain.indicators.models  # noqa: F401
import stockanalyse_api.domain.screens.models  # noqa: F401
from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.fundamentals.models import FundamentalsAnnual
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.jobs.refresh_fundamentals import _load_instruments


class RefreshFundamentalsJobTests(unittest.TestCase):
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

    def test_load_instruments_can_skip_latest_rows_with_cash_flow(self) -> None:
        with self.session_factory() as session:
            complete = Instrument(symbol="AAPL", exchange="US", name="Apple")
            missing = Instrument(symbol="MSFT", exchange="US", name="Microsoft")
            no_rows = Instrument(symbol="NVDA", exchange="US", name="Nvidia")
            session.add_all([complete, missing, no_rows])
            session.flush()
            for instrument in (complete, missing, no_rows):
                session.add(
                    MarketDataDaily(
                        instrument_id=instrument.id,
                        trade_date=date(2026, 4, 30),
                        close=Decimal("100"),
                        adj_close=Decimal("100"),
                    )
                )
            session.add_all(
                [
                    FundamentalsAnnual(
                        instrument_id=complete.id,
                        fiscal_year_end_date=date(2025, 12, 31),
                        fiscal_year_label="FY2025",
                        net_income=Decimal("100"),
                        net_income_currency="USD",
                        operating_cash_flow=Decimal("120"),
                        free_cash_flow=Decimal("90"),
                        source="test",
                        source_as_of_date=date(2026, 5, 4),
                        data_status="complete",
                    ),
                    FundamentalsAnnual(
                        instrument_id=missing.id,
                        fiscal_year_end_date=date(2025, 12, 31),
                        fiscal_year_label="FY2025",
                        net_income=Decimal("100"),
                        net_income_currency="USD",
                        operating_cash_flow=None,
                        free_cash_flow=None,
                        source="test",
                        source_as_of_date=date(2026, 5, 4),
                        data_status="partial",
                    ),
                ]
            )
            session.commit()

            instruments = _load_instruments(
                session,
                symbols=None,
                exchange="US",
                limit=None,
                missing_cash_flow_only=True,
            )

        self.assertEqual([instrument.symbol for instrument in instruments], ["MSFT", "NVDA"])

    def test_load_instruments_can_target_missing_valuation_inputs(self) -> None:
        with self.session_factory() as session:
            complete = Instrument(symbol="AAPL", exchange="US", name="Apple")
            missing = Instrument(symbol="MSFT", exchange="US", name="Microsoft")
            no_rows = Instrument(symbol="NVDA", exchange="US", name="Nvidia")
            session.add_all([complete, missing, no_rows])
            session.flush()
            for instrument in (complete, missing, no_rows):
                session.add(
                    MarketDataDaily(
                        instrument_id=instrument.id,
                        trade_date=date(2026, 4, 30),
                        close=Decimal("100"),
                        adj_close=Decimal("100"),
                    )
                )
            session.add_all(
                [
                    FundamentalsAnnual(
                        instrument_id=complete.id,
                        fiscal_year_end_date=date(2025, 12, 31),
                        fiscal_year_label="FY2025",
                        net_income=Decimal("100"),
                        net_income_currency="USD",
                        diluted_eps=Decimal("2"),
                        stockholders_equity=Decimal("500"),
                        weighted_average_diluted_shares=Decimal("100"),
                        source="test",
                        source_as_of_date=date(2026, 5, 4),
                        data_status="complete",
                    ),
                    FundamentalsAnnual(
                        instrument_id=missing.id,
                        fiscal_year_end_date=date(2025, 12, 31),
                        fiscal_year_label="FY2025",
                        net_income=Decimal("100"),
                        net_income_currency="USD",
                        diluted_eps=None,
                        stockholders_equity=Decimal("500"),
                        weighted_average_diluted_shares=Decimal("100"),
                        source="test",
                        source_as_of_date=date(2026, 5, 4),
                        data_status="partial",
                    ),
                ]
            )
            session.commit()

            instruments = _load_instruments(
                session,
                symbols=None,
                exchange="US",
                limit=None,
                missing_valuation_inputs_only=True,
            )

        self.assertEqual([instrument.symbol for instrument in instruments], ["MSFT", "NVDA"])

    def test_load_instruments_supports_offset_for_resume(self) -> None:
        with self.session_factory() as session:
            for symbol in ("AAPL", "MSFT", "NVDA"):
                instrument = Instrument(symbol=symbol, exchange="US", name=symbol)
                session.add(instrument)
                session.flush()
                session.add(
                    MarketDataDaily(
                        instrument_id=instrument.id,
                        trade_date=date(2026, 4, 30),
                        close=Decimal("100"),
                        adj_close=Decimal("100"),
                    )
                )
            session.commit()

            instruments = _load_instruments(
                session,
                symbols=None,
                exchange="US",
                limit=1,
                offset=1,
            )

        self.assertEqual([instrument.symbol for instrument in instruments], ["MSFT"])


if __name__ == "__main__":
    unittest.main()
