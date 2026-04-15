from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
import logging
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select

from stockanalyse_api.config.settings import get_auto_refresh_check_interval_seconds
from stockanalyse_api.config.settings import get_auto_refresh_commit_every
from stockanalyse_api.config.settings import get_auto_refresh_csv_dir
from stockanalyse_api.config.settings import get_auto_refresh_enabled
from stockanalyse_api.config.settings import get_auto_refresh_fixture_path
from stockanalyse_api.config.settings import get_auto_refresh_provider
from stockanalyse_api.config.settings import get_auto_refresh_symbols_file
from stockanalyse_api.config.settings import get_auto_refresh_timezone
from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.operations.models import MarketDataRefreshRun
from stockanalyse_api.services.ingestion.providers.registry import build_ingestion_provider
from stockanalyse_api.services.ingestion.refresh_service import DEFAULT_UNIVERSE_FILTER
from stockanalyse_api.services.ingestion.refresh_service import execute_market_data_refresh

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AutoRefreshConfig:
    enabled: bool
    provider_name: str
    symbols_file: Path
    csv_dir: Path
    fixture_path: Path
    commit_every: int
    check_interval_seconds: int
    timezone: ZoneInfo


def load_auto_refresh_config() -> AutoRefreshConfig:
    return AutoRefreshConfig(
        enabled=get_auto_refresh_enabled(),
        provider_name=get_auto_refresh_provider(),
        symbols_file=get_auto_refresh_symbols_file(),
        csv_dir=get_auto_refresh_csv_dir(),
        fixture_path=get_auto_refresh_fixture_path(),
        commit_every=get_auto_refresh_commit_every(),
        check_interval_seconds=get_auto_refresh_check_interval_seconds(),
        timezone=get_auto_refresh_timezone(),
    )


def build_default_provider(config: AutoRefreshConfig):
    return build_ingestion_provider(
        config.provider_name,
        fixture_path=config.fixture_path,
        csv_dir=config.csv_dir,
        symbols_file=config.symbols_file,
    )


def should_run_auto_refresh(session, *, now: datetime, timezone: ZoneInfo) -> bool:
    latest_refresh = session.execute(
        select(MarketDataRefreshRun)
        .order_by(MarketDataRefreshRun.started_at.desc(), MarketDataRefreshRun.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    if latest_refresh is None:
        return True

    last_started = latest_refresh.started_at
    if last_started.tzinfo is None:
        last_started = last_started.replace(tzinfo=UTC)
    return last_started.astimezone(timezone).date() < now.astimezone(timezone).date()


def _record_failed_auto_refresh_attempt(
    session,
    *,
    provider_name: str,
    started_at: datetime,
    error_message: str,
) -> None:
    session.add(
        MarketDataRefreshRun(
            provider=provider_name,
            universe_scope="full_universe",
            universe_filter=DEFAULT_UNIVERSE_FILTER,
            requested_symbol_count=0,
            requested_symbols="",
            status="failed",
            started_at=started_at.astimezone(UTC),
            completed_at=datetime.now(UTC),
            error_message=error_message,
        )
    )
    session.commit()


class AutoRefreshRuntime:
    def __init__(
        self,
        config: AutoRefreshConfig | None = None,
        *,
        session_factory=SessionLocal,
        provider_builder=build_default_provider,
        now_fn=None,
    ) -> None:
        self.config = config or load_auto_refresh_config()
        self.session_factory = session_factory
        self.provider_builder = provider_builder
        self.now_fn = now_fn or (lambda: datetime.now(self.config.timezone))
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if not self.config.enabled or self._task is not None:
            return
        self._task = asyncio.create_task(self._run_loop(), name="stockanalyse-auto-refresh")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _run_loop(self) -> None:
        while True:
            try:
                await asyncio.to_thread(self.run_once_if_due)
            except Exception:
                logger.exception("Automatic market-data refresh cycle failed.")
            await asyncio.sleep(self.config.check_interval_seconds)

    def run_once_if_due(self) -> bool:
        if not self.config.enabled:
            return False
        now = self.now_fn()
        with self.session_factory() as session:
            if not should_run_auto_refresh(
                session,
                now=now,
                timezone=self.config.timezone,
            ):
                return False
            latest_refresh_id = session.execute(
                select(MarketDataRefreshRun.id)
                .order_by(MarketDataRefreshRun.started_at.desc(), MarketDataRefreshRun.id.desc())
                .limit(1)
            ).scalar_one_or_none()

        try:
            provider = self.provider_builder(self.config)
            with self.session_factory() as session:
                execute_market_data_refresh(
                    session,
                    provider,
                    all_supported=True,
                    universe_filter=DEFAULT_UNIVERSE_FILTER,
                    commit_every=self.config.commit_every,
                )
        except Exception as exc:
            with self.session_factory() as session:
                new_latest_refresh_id = session.execute(
                    select(MarketDataRefreshRun.id)
                    .order_by(MarketDataRefreshRun.started_at.desc(), MarketDataRefreshRun.id.desc())
                    .limit(1)
                ).scalar_one_or_none()
                if new_latest_refresh_id == latest_refresh_id:
                    _record_failed_auto_refresh_attempt(
                        session,
                        provider_name=self.config.provider_name,
                        started_at=now,
                        error_message=str(exc),
                    )
            raise
        return True


__all__ = [
    "AutoRefreshConfig",
    "AutoRefreshRuntime",
    "build_default_provider",
    "load_auto_refresh_config",
    "should_run_auto_refresh",
]
