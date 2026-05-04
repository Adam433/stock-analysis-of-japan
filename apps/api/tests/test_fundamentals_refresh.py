from __future__ import annotations

import unittest
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import stockanalyse_api.domain.market_data.models  # noqa: F401
from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.fundamentals.models import FundamentalsAnnual
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.services.fundamentals_refresh import refresh_instrument_fundamentals
from stockanalyse_api.services.ingestion.provider_models import ProviderFundamentalsAnnual


class _StubFundamentalsProvider:
    provider_name = "stub"
    market_scope = "jp_equities_eod"
    credential_boundary = "backend_only"

    def __init__(self, rows: list[ProviderFundamentalsAnnual] | Exception) -> None:
        self.rows = rows

    def fetch_annual_fundamentals(self, symbol: str, *, exchange: str = "TSE") -> list[ProviderFundamentalsAnnual]:
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows


class FundamentalsRefreshTests(unittest.TestCase):
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

    def _seed_instrument(
        self,
        *,
        symbol: str = "7203.T",
        exchange: str = "TSE",
        name: str = "Toyota",
    ) -> int:
        with self.session_factory() as session:
            instrument = Instrument(symbol=symbol, exchange=exchange, name=name)
            session.add(instrument)
            session.commit()
            return instrument.id

    def test_refresh_instrument_fundamentals_upserts_recent_five_fiscal_years(self) -> None:
        instrument_id = self._seed_instrument()
        provider_rows = [
            ProviderFundamentalsAnnual(
                symbol="7203.T",
                exchange="TSE",
                fiscal_year_end_date=date(2019 + index, 3, 31),
                fiscal_year_label=f"FY{2019 + index}",
                net_income=Decimal("1000000") + Decimal(index),
                operating_cash_flow=Decimal("2000000") + Decimal(index),
                free_cash_flow=Decimal("1500000") + Decimal(index),
                diluted_eps=Decimal("2.5") + Decimal(index) / Decimal("10"),
                stockholders_equity=Decimal("5000000") + Decimal(index),
                weighted_average_diluted_shares=Decimal("1000000") + Decimal(index),
                pe=Decimal("10.1") + Decimal(index),
                pb=Decimal("1.11") + Decimal(index) / Decimal("10"),
                source="stub",
                source_as_of_date=date(2026, 4, 17),
                data_status="complete",
            )
            for index in range(6)
        ]

        with self.session_factory() as session:
            refreshed = refresh_instrument_fundamentals(
                session,
                instrument_id=instrument_id,
                provider=_StubFundamentalsProvider(provider_rows),
            )
            rows = session.execute(
                select(FundamentalsAnnual)
                .where(FundamentalsAnnual.instrument_id == instrument_id)
                .order_by(FundamentalsAnnual.fiscal_year_end_date.asc())
            ).scalars().all()

        self.assertTrue(refreshed)
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[0].fiscal_year_label, "FY2020")
        self.assertEqual(rows[-1].fiscal_year_label, "FY2024")
        self.assertEqual(rows[-1].operating_cash_flow, Decimal("2000005"))
        self.assertEqual(rows[-1].free_cash_flow, Decimal("1500005"))
        self.assertEqual(rows[-1].diluted_eps, Decimal("3.000000"))
        self.assertEqual(rows[-1].stockholders_equity, Decimal("5000005"))
        self.assertEqual(rows[-1].weighted_average_diluted_shares, Decimal("1000005"))

    def test_refresh_instrument_fundamentals_keeps_existing_status_when_provider_fails(self) -> None:
        instrument_id = self._seed_instrument()
        original_as_of = date(2026, 4, 1)

        with self.session_factory() as session:
            session.add(
                FundamentalsAnnual(
                    instrument_id=instrument_id,
                    fiscal_year_end_date=date(2024, 3, 31),
                    fiscal_year_label="FY2024",
                    net_income=Decimal("1234"),
                    net_income_currency="JPY",
                    pe=Decimal("11.2"),
                    pb=Decimal("1.15"),
                    source="seed",
                    source_as_of_date=original_as_of,
                    data_status="complete",
                )
            )
            session.commit()

            refreshed = refresh_instrument_fundamentals(
                session,
                instrument_id=instrument_id,
                provider=_StubFundamentalsProvider(RuntimeError("boom")),
            )
            row = session.execute(select(FundamentalsAnnual)).scalar_one()

        self.assertFalse(refreshed)
        self.assertEqual(row.data_status, "complete")
        self.assertGreaterEqual(row.source_as_of_date, original_as_of)

    def test_refresh_instrument_fundamentals_uses_us_default_provider_for_us_instruments(self) -> None:
        instrument_id = self._seed_instrument(symbol="AAPL", exchange="US", name="Apple")

        with (
            patch(
                "stockanalyse_api.services.fundamentals_refresh.get_us_fundamentals_provider",
                return_value="sec_companyfacts_yahoo_fallback",
            ),
            patch(
                "stockanalyse_api.services.fundamentals_refresh.build_ingestion_provider",
                return_value=_StubFundamentalsProvider([]),
            ) as build_provider,
            self.session_factory() as session,
        ):
            refreshed = refresh_instrument_fundamentals(session, instrument_id=instrument_id)

        self.assertFalse(refreshed)
        build_provider.assert_called_once_with("sec_companyfacts_yahoo_fallback")


if __name__ == "__main__":
    unittest.main()
