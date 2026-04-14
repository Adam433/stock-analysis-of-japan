from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
import tempfile

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stockanalyse_api.config.settings import get_tse_common_stock_symbols_path
from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.domain.operations.models import MarketDataRefreshRun
from stockanalyse_api.services.health import get_market_data_health
from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar
from stockanalyse_api.services.ingestion.provider_models import ProviderInstrument
from stockanalyse_api.services.ingestion.refresh_service import execute_market_data_refresh
from stockanalyse_api.services.ingestion.refresh_service import refresh_market_data


class MixedStatusProvider:
    provider_name = "mixed_fixture"

    def list_supported_instruments(self) -> list[ProviderInstrument]:
        return [
            ProviderInstrument(symbol="7203", exchange="TSE", instrument_type="common_stock"),
            ProviderInstrument(symbol="6758", exchange="TSE", instrument_type="common_stock"),
            ProviderInstrument(symbol="1343", exchange="TSE", instrument_type="etf"),
        ]

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
                data_source=self.provider_name,
                instrument_name="Toyota Motor",
            ),
            ProviderDailyBar(
                symbol="6758",
                exchange="TSE",
                trade_date=date(2026, 4, 11),
                open=Decimal("4000"),
                high=None,
                low=Decimal("3950"),
                close=Decimal("3980"),
                adj_close=Decimal("3980"),
                volume=50,
                data_source=self.provider_name,
                instrument_name="Sony Group",
            ),
        ]


class FailingProvider:
    provider_name = "broken_provider"

    def list_supported_instruments(self) -> list[ProviderInstrument]:
        return [ProviderInstrument(symbol="7203", exchange="TSE", instrument_type="common_stock")]

    def fetch_daily_bars(self, _symbols: list[str]) -> list[ProviderDailyBar]:
        raise RuntimeError("fixture source unavailable")


class CompleteSubsetProvider:
    provider_name = "complete_subset"

    def list_supported_instruments(self) -> list[ProviderInstrument]:
        return [ProviderInstrument(symbol="7203", exchange="TSE", instrument_type="common_stock")]

    def fetch_daily_bars(self, _symbols: list[str]) -> list[ProviderDailyBar]:
        return [
            ProviderDailyBar(
                symbol="7203",
                exchange="TSE",
                trade_date=date(2026, 4, 11),
                open=Decimal("1001"),
                high=Decimal("1011"),
                low=Decimal("996"),
                close=Decimal("1006"),
                adj_close=Decimal("1006"),
                volume=101,
                data_source=self.provider_name,
                instrument_name="Toyota Motor",
            )
        ]


class DuplicateOverwriteProvider:
    provider_name = "duplicate_overwrite"

    def list_supported_instruments(self) -> list[ProviderInstrument]:
        return [ProviderInstrument(symbol="7203", exchange="TSE", instrument_type="common_stock")]

    def fetch_daily_bars(self, _symbols: list[str]) -> list[ProviderDailyBar]:
        return [
            ProviderDailyBar(
                symbol="7203",
                exchange="TSE",
                trade_date=date(2026, 4, 11),
                open=Decimal("1000"),
                high=None,
                low=Decimal("995"),
                close=Decimal("1005"),
                adj_close=Decimal("1005"),
                volume=100,
                data_source=self.provider_name,
                instrument_name="Toyota Motor",
            ),
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
                data_source=self.provider_name,
                instrument_name="Toyota Motor",
            ),
        ]


class MarketDataHealthTests(unittest.TestCase):
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

    def test_execute_market_data_refresh_records_partial_refresh_run(self) -> None:
        with self.session_factory() as session:
            result = execute_market_data_refresh(session, MixedStatusProvider(), ["7203", "6758"])

        self.assertEqual(result["processed"], 2)
        self.assertEqual(result["partial_rows"], 1)
        self.assertEqual(result["unavailable_rows"], 0)

        with self.session_factory() as session:
            health = get_market_data_health(session, today=date(2026, 4, 12))
            refresh_run = session.query(MarketDataRefreshRun).one()
            daily_rows = session.query(MarketDataDaily).count()

        self.assertEqual(daily_rows, 2)
        self.assertEqual(refresh_run.status, "partial")
        self.assertEqual(health.freshness_state, "fresh")
        self.assertEqual(health.coverage_status, "partial")
        self.assertEqual(health.partial_rows, 1)
        self.assertEqual(health.unavailable_rows, 0)
        self.assertEqual(health.last_refresh["status"], "partial")

    def test_execute_market_data_refresh_records_failed_run(self) -> None:
        with self.session_factory() as session:
            with self.assertRaises(RuntimeError):
                execute_market_data_refresh(session, FailingProvider(), ["7203"])

        with self.session_factory() as session:
            health = get_market_data_health(session, today=date(2026, 4, 12))
            refresh_run = session.query(MarketDataRefreshRun).one()

        self.assertEqual(refresh_run.status, "failed")
        self.assertEqual(refresh_run.error_message, "fixture source unavailable")
        self.assertEqual(health.freshness_state, "missing")
        self.assertEqual(health.coverage_status, "failed")
        self.assertEqual(health.last_refresh["status"], "failed")

    def test_health_marks_old_data_as_stale(self) -> None:
        with self.session_factory() as session:
            execute_market_data_refresh(session, MixedStatusProvider(), ["7203", "6758"])
            health = get_market_data_health(session, today=date(2026, 4, 20))

        self.assertEqual(health.freshness_state, "stale")
        self.assertEqual(health.age_in_days, 9)

    def test_health_keeps_partial_coverage_when_latest_run_is_success_on_subset(self) -> None:
        with self.session_factory() as session:
            execute_market_data_refresh(session, MixedStatusProvider(), ["7203", "6758"])
            execute_market_data_refresh(session, CompleteSubsetProvider(), ["7203"])
            health = get_market_data_health(session, today=date(2026, 4, 12))

        self.assertEqual(health.last_refresh["status"], "succeeded")
        self.assertEqual(health.partial_rows, 1)
        self.assertEqual(health.coverage_status, "partial")

    def test_refresh_run_counts_final_row_state_after_duplicate_updates(self) -> None:
        with self.session_factory() as session:
            result = execute_market_data_refresh(session, DuplicateOverwriteProvider(), ["7203"])
            health = get_market_data_health(session, today=date(2026, 4, 12))

        self.assertEqual(result["partial_rows"], 0)
        self.assertEqual(result["unavailable_rows"], 0)
        self.assertEqual(health.last_refresh["status"], "succeeded")
        self.assertEqual(health.coverage_status, "complete")

    def test_health_exposes_null_refresh_history_without_claiming_failure(self) -> None:
        with self.session_factory() as session:
            result = refresh_market_data(session, MixedStatusProvider(), ["7203", "6758"])
            health = get_market_data_health(session, today=date(2026, 4, 12))

        self.assertEqual(result["processed"], 2)
        self.assertEqual(health.last_refresh, None)
        self.assertEqual(health.freshness_state, "fresh")
        self.assertEqual(health.coverage_status, "complete")

    def test_execute_market_data_refresh_records_full_universe_scope(self) -> None:
        with self.session_factory() as session:
            execute_market_data_refresh(
                session,
                MixedStatusProvider(),
                all_supported=True,
            )
            health = get_market_data_health(session, today=date(2026, 4, 12))

        self.assertEqual(health.last_refresh["universe_scope"], "full_universe")
        self.assertEqual(health.last_refresh["universe_filter"], "tse_common_stock")
        self.assertEqual(health.last_refresh["requested_symbol_count"], 2)
        self.assertEqual(health.last_refresh["requested_symbols"], [])

    def test_health_reports_universe_manifest_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "tse_common_stock_symbols.txt"
            manifest_path.write_text("7203.T\n6758.T\n", encoding="utf-8")

            import os

            original = os.environ.get("STOCKANALYSE_TSE_COMMON_STOCK_SYMBOLS_PATH")
            os.environ["STOCKANALYSE_TSE_COMMON_STOCK_SYMBOLS_PATH"] = str(manifest_path)
            try:
                with self.session_factory() as session:
                    health = get_market_data_health(session, today=date(2026, 4, 12))
            finally:
                if original is None:
                    os.environ.pop("STOCKANALYSE_TSE_COMMON_STOCK_SYMBOLS_PATH", None)
                else:
                    os.environ["STOCKANALYSE_TSE_COMMON_STOCK_SYMBOLS_PATH"] = original

        self.assertEqual(health.universe_manifest.symbol_count, 2)
        self.assertEqual(health.universe_manifest.universe_filter, "tse_common_stock")
        self.assertEqual(health.universe_manifest.source_path, str(manifest_path))


if __name__ == "__main__":
    unittest.main()
