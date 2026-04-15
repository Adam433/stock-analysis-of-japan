from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.operations.models import MarketDataRefreshRun
from stockanalyse_api.main import create_app
from stockanalyse_api.services.ingestion.provider_models import ProviderDailyBar
from stockanalyse_api.services.ingestion.provider_models import ProviderInstrument
from stockanalyse_api.services.operations.auto_refresh import AutoRefreshConfig
from stockanalyse_api.services.operations.auto_refresh import AutoRefreshRuntime
from stockanalyse_api.services.operations.auto_refresh import should_run_auto_refresh


class SuccessfulProvider:
    provider_name = "auto_success"

    def list_supported_instruments(self) -> list[ProviderInstrument]:
        return [ProviderInstrument(symbol="7203", exchange="TSE", instrument_type="common_stock")]

    def fetch_daily_bars(self, _symbols: list[str]):
        return [
            ProviderDailyBar(
                symbol="7203",
                exchange="TSE",
                trade_date=date(2026, 4, 15),
                open=Decimal("1000"),
                high=Decimal("1010"),
                low=Decimal("995"),
                close=Decimal("1005"),
                adj_close=Decimal("1005"),
                volume=100,
                data_source=self.provider_name,
                instrument_name="Toyota Motor",
            )
        ]


class FailingProvider:
    provider_name = "auto_fail"

    def list_supported_instruments(self) -> list[ProviderInstrument]:
        return [ProviderInstrument(symbol="7203", exchange="TSE", instrument_type="common_stock")]

    def fetch_daily_bars(self, _symbols: list[str]):
        raise RuntimeError("automatic refresh failed")


class BrokenProviderBuilder(Exception):
    pass


class SpyRuntime:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class AutoRefreshRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        Base.metadata.create_all(self.engine)
        self.session_factory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            future=True,
        )
        self.temp_dir = tempfile.TemporaryDirectory()
        self.symbols_file = Path(self.temp_dir.name) / "symbols.txt"
        self.symbols_file.write_text("7203\n", encoding="utf-8")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _config(self) -> AutoRefreshConfig:
        return AutoRefreshConfig(
            enabled=True,
            provider_name="static_fixture",
            symbols_file=self.symbols_file,
            csv_dir=Path(self.temp_dir.name),
            fixture_path=Path(self.temp_dir.name) / "fixture.json",
            commit_every=100,
            check_interval_seconds=1,
            timezone=UTC,
        )

    def test_run_once_if_due_creates_refresh_run(self) -> None:
        runtime = AutoRefreshRuntime(
            self._config(),
            session_factory=self.session_factory,
            provider_builder=lambda _config: SuccessfulProvider(),
            now_fn=lambda: datetime(2026, 4, 15, 9, 0, tzinfo=UTC),
        )

        ran = runtime.run_once_if_due()

        self.assertTrue(ran)
        with self.session_factory() as session:
            refresh_run = session.query(MarketDataRefreshRun).one()

        self.assertEqual(refresh_run.status, "succeeded")
        self.assertEqual(refresh_run.provider, "auto_success")

    def test_run_once_if_due_skips_second_run_on_same_day(self) -> None:
        runtime = AutoRefreshRuntime(
            self._config(),
            session_factory=self.session_factory,
            provider_builder=lambda _config: SuccessfulProvider(),
            now_fn=lambda: datetime(2026, 4, 15, 9, 0, tzinfo=UTC),
        )

        self.assertTrue(runtime.run_once_if_due())
        self.assertFalse(runtime.run_once_if_due())

        with self.session_factory() as session:
            refresh_runs = session.query(MarketDataRefreshRun).all()

        self.assertEqual(len(refresh_runs), 1)

    def test_run_once_if_due_records_failure_visibly(self) -> None:
        runtime = AutoRefreshRuntime(
            self._config(),
            session_factory=self.session_factory,
            provider_builder=lambda _config: FailingProvider(),
            now_fn=lambda: datetime(2026, 4, 15, 9, 0, tzinfo=UTC),
        )

        with self.assertRaises(RuntimeError):
            runtime.run_once_if_due()

        with self.session_factory() as session:
            refresh_run = session.query(MarketDataRefreshRun).one()

        self.assertEqual(refresh_run.status, "failed")
        self.assertEqual(refresh_run.error_message, "automatic refresh failed")

    def test_run_once_if_due_records_provider_build_failures(self) -> None:
        runtime = AutoRefreshRuntime(
            self._config(),
            session_factory=self.session_factory,
            provider_builder=lambda _config: (_ for _ in ()).throw(BrokenProviderBuilder("bad provider config")),
            now_fn=lambda: datetime(2026, 4, 15, 9, 0, tzinfo=UTC),
        )

        with self.assertRaises(BrokenProviderBuilder):
            runtime.run_once_if_due()

        with self.session_factory() as session:
            refresh_run = session.query(MarketDataRefreshRun).one()

        self.assertEqual(refresh_run.status, "failed")
        self.assertEqual(refresh_run.provider, "static_fixture")
        self.assertEqual(refresh_run.error_message, "bad provider config")

    def test_should_run_auto_refresh_advances_on_next_day(self) -> None:
        with self.session_factory() as session:
            session.add(
                MarketDataRefreshRun(
                    provider="auto_success",
                    universe_scope="full_universe",
                    universe_filter="tse_common_stock",
                    requested_symbol_count=1,
                    requested_symbols="",
                    status="succeeded",
                    started_at=datetime(2026, 4, 15, 9, 0, tzinfo=UTC),
                    completed_at=datetime(2026, 4, 15, 9, 5, tzinfo=UTC),
                )
            )
            session.commit()

            self.assertFalse(
                should_run_auto_refresh(
                    session,
                    now=datetime(2026, 4, 15, 12, 0, tzinfo=UTC),
                    timezone=UTC,
                )
            )
            self.assertTrue(
                should_run_auto_refresh(
                    session,
                    now=datetime(2026, 4, 16, 0, 1, tzinfo=UTC),
                    timezone=UTC,
                )
            )


class AppStartupLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_app_starts_and_stops_auto_refresh_runtime(self) -> None:
        runtime = SpyRuntime()
        app = create_app(auto_refresh_runtime=runtime)

        async with app.router.lifespan_context(app):
            self.assertTrue(runtime.started)
            self.assertFalse(runtime.stopped)

        self.assertTrue(runtime.stopped)


class AutoRefreshLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_background_loop_survives_exceptions(self) -> None:
        calls: list[str] = []
        runtime = AutoRefreshRuntime(
            AutoRefreshConfig(
                enabled=True,
                provider_name="static_fixture",
                symbols_file=Path("symbols.txt"),
                csv_dir=Path("."),
                fixture_path=Path("fixture.json"),
                commit_every=100,
                check_interval_seconds=0,
                timezone=UTC,
            ),
            session_factory=lambda: None,
        )

        outcomes = [RuntimeError("boom"), False]

        def fake_run_once_if_due() -> bool:
            result = outcomes.pop(0)
            calls.append("run")
            if isinstance(result, Exception):
                raise result
            raise asyncio.CancelledError

        runtime.run_once_if_due = fake_run_once_if_due  # type: ignore[method-assign]

        with self.assertRaises(asyncio.CancelledError):
            await runtime._run_loop()

        self.assertEqual(len(calls), 2)
