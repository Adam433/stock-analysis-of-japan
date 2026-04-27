from __future__ import annotations

import unittest
import os
from datetime import date
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.jobs.refresh_market_data import APP_ROOT, resolve_fixture_path
from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar
from stockanalyse_api.services.ingestion.provider_models import ProviderInstrument
from stockanalyse_api.services.ingestion.refresh_service import refresh_market_data


class DuplicateBarProvider:
    def list_supported_instruments(self) -> list[ProviderInstrument]:
        return [ProviderInstrument(symbol="7203", exchange="TSE", instrument_type="common_stock")]

    def fetch_daily_bars(self, _symbols: list[str]) -> list[ProviderDailyBar]:
        return [
            ProviderDailyBar(
                symbol="7203",
                exchange="TSE",
                trade_date=date(2026, 4, 11),
                open=Decimal("1000"),
                high=Decimal("1010"),
                low=Decimal("995"),
                close=Decimal("1005"),
                adj_close=Decimal("1005"),
                volume=100,
                data_source="test_provider",
                instrument_name="Toyota Motor",
            ),
            ProviderDailyBar(
                symbol="7203",
                exchange="TSE",
                trade_date=date(2026, 4, 11),
                open=Decimal("1000"),
                high=Decimal("1020"),
                low=Decimal("990"),
                close=Decimal("1015"),
                adj_close=Decimal("1015"),
                volume=120,
                data_source="test_provider",
                instrument_name="Toyota Motor",
            ),
        ]


class IncrementalAwareProvider:
    def __init__(self) -> None:
        self.last_start_after_by_symbol: dict[str, date] | None = None

    def list_supported_instruments(self) -> list[ProviderInstrument]:
        return [ProviderInstrument(symbol="7203", exchange="TSE", instrument_type="common_stock")]

    def fetch_daily_bars(
        self,
        _symbols: list[str],
        *,
        start_after_by_symbol: dict[str, date] | None = None,
    ) -> list[ProviderDailyBar]:
        self.last_start_after_by_symbol = start_after_by_symbol
        return [
            ProviderDailyBar(
                symbol="7203",
                exchange="TSE",
                trade_date=date(2026, 4, 12),
                open=Decimal("1020"),
                high=Decimal("1030"),
                low=Decimal("1015"),
                close=Decimal("1025"),
                adj_close=Decimal("1025"),
                volume=150,
                data_source="test_provider",
                instrument_name="Toyota Motor",
            )
        ]


class ZeroVolumeProvider:
    def list_supported_instruments(self) -> list[ProviderInstrument]:
        return [ProviderInstrument(symbol="7691", exchange="TSE", instrument_type="common_stock")]

    def fetch_daily_bars(self, _symbols: list[str]) -> list[ProviderDailyBar]:
        return [
            ProviderDailyBar(
                symbol="7691",
                exchange="TSE",
                trade_date=date(2025, 7, 18),
                open=Decimal("1"),
                high=Decimal("1"),
                low=Decimal("1"),
                close=Decimal("1"),
                adj_close=Decimal("1"),
                volume=0,
                data_status="complete",
                data_source="test_provider",
                instrument_name="Example Zero Volume",
            )
        ]


class IngestionReviewRegressionTests(unittest.TestCase):
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

    def test_refresh_market_data_deduplicates_duplicate_rows_within_same_batch(self) -> None:
        with self.session_factory() as session:
            result = refresh_market_data(session, DuplicateBarProvider(), ["7203"])

        self.assertEqual(
            result,
            {
                "processed": 2,
                "inserted": 1,
                "updated": 1,
                "partial_rows": 0,
                "unavailable_rows": 0,
                "latest_trade_date": "2026-04-11",
            },
        )

        with self.session_factory() as session:
            bars = session.execute(select(MarketDataDaily)).scalars().all()
            instruments = session.execute(select(Instrument)).scalars().all()

        self.assertEqual(len(instruments), 1)
        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].close, Decimal("1015"))
        self.assertEqual(bars[0].volume, 120)

    def test_refresh_market_data_deduplicates_duplicate_rows_across_commit_batches(self) -> None:
        with self.session_factory() as session:
            result = refresh_market_data(
                session,
                DuplicateBarProvider(),
                ["7203"],
                commit_every=1,
            )

        self.assertEqual(
            result,
            {
                "processed": 2,
                "inserted": 1,
                "updated": 1,
                "partial_rows": 0,
                "unavailable_rows": 0,
                "latest_trade_date": "2026-04-11",
            },
        )

    def test_refresh_market_data_passes_overlapped_date_to_incremental_provider(self) -> None:
        provider = IncrementalAwareProvider()

        with self.session_factory() as session:
            instrument = Instrument(symbol="7203", exchange="TSE")
            session.add(instrument)
            session.flush()
            session.add(
                MarketDataDaily(
                    instrument_id=instrument.id,
                    trade_date=date(2026, 4, 11),
                    open=Decimal("1000"),
                    high=Decimal("1010"),
                    low=Decimal("995"),
                    close=Decimal("1005"),
                    adj_close=Decimal("1005"),
                    volume=100,
                    data_source="seed",
                )
            )
            session.commit()

            result = refresh_market_data(session, provider, ["7203"])

        self.assertEqual(provider.last_start_after_by_symbol, {"7203": date(2026, 3, 12)})
        self.assertEqual(
            result,
            {
                "processed": 1,
                "inserted": 1,
                "updated": 0,
                "partial_rows": 0,
                "unavailable_rows": 0,
                "latest_trade_date": "2026-04-12",
            },
        )

    def test_refresh_market_data_marks_zero_volume_price_rows_unavailable(self) -> None:
        with self.session_factory() as session:
            result = refresh_market_data(session, ZeroVolumeProvider(), ["7691"])
            row = session.execute(select(MarketDataDaily)).scalar_one()

        self.assertEqual(
            result,
            {
                "processed": 1,
                "inserted": 1,
                "updated": 0,
                "partial_rows": 0,
                "unavailable_rows": 1,
                "latest_trade_date": "2025-07-18",
            },
        )
        self.assertEqual(row.data_status, "unavailable")

    def test_resolve_fixture_path_falls_back_to_app_root_for_repo_root_execution(self) -> None:
        original_cwd = Path.cwd()
        try:
            os.chdir(APP_ROOT.parent.parent)
            fixture_path = resolve_fixture_path("tests/fixtures/japan_equity_eod_fixture.json")
        finally:
            os.chdir(original_cwd)

        self.assertEqual(fixture_path, APP_ROOT / Path("tests/fixtures/japan_equity_eod_fixture.json"))
        self.assertTrue(fixture_path.exists())

    def test_market_data_daily_rejects_unknown_data_status(self) -> None:
        with self.session_factory() as session:
            instrument = Instrument(symbol="7203", exchange="TSE")
            session.add(instrument)
            session.flush()
            session.add(
                MarketDataDaily(
                    instrument_id=instrument.id,
                    trade_date=date(2026, 4, 11),
                    data_status="stale",
                )
            )

            with self.assertRaises(IntegrityError):
                session.commit()


if __name__ == "__main__":
    unittest.main()
