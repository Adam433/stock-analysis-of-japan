from __future__ import annotations

import unittest
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.fundamentals.models import FundamentalsAnnual
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.fundamentals_refresh import refresh_instrument_fundamentals
from stockanalyse_api.services.ingestion.provider_models import ProviderFundamentalsAnnual
from stockanalyse_api.services.inline_analysis import get_inline_analysis_payload
from stockanalyse_api.services.strategy_config import get_active_strategy_configuration, save_strategy_configuration
from stockanalyse_api.services.factor_materialization import materialize_derived_indicator_facts
from stockanalyse_api.services.screening import execute_screen_run


class _InlineFundamentalsProvider:
    provider_name = "inline-stub"
    market_scope = "jp_equities_eod"
    credential_boundary = "backend_only"

    def __init__(self, rows: list[ProviderFundamentalsAnnual] | Exception) -> None:
        self.rows = rows

    def fetch_annual_fundamentals(self, symbol: str, *, exchange: str = "TSE") -> list[ProviderFundamentalsAnnual]:
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows


class InlineAnalysisTests(unittest.TestCase):
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

    def _seed_screen_context(self, *, market_days: int) -> tuple[int, int]:
        start_date = date(2025, 1, 1)
        leader_id: int
        with self.session_factory() as session:
            leader = Instrument(symbol="7203.T", exchange="TSE", name="Leader")
            middle = Instrument(symbol="6758.T", exchange="TSE", name="Middle")
            laggard = Instrument(symbol="9984.T", exchange="TSE", name="Laggard")
            session.add_all([leader, middle, laggard])
            session.flush()
            leader_id = leader.id

            rows: list[MarketDataDaily] = []
            for index in range(market_days):
                trade_date = start_date + timedelta(days=index)
                leader_close = Decimal("100") + Decimal(index)
                middle_close = Decimal("120") + Decimal(index) / Decimal("3")
                laggard_close = Decimal("140") + Decimal(index) / Decimal("10")
                if index >= max(market_days - 20, 1):
                    middle_close -= Decimal(max(index - (market_days - 21), 0)) / Decimal("4")
                    laggard_close -= Decimal(max(index - (market_days - 21), 0))

                rows.extend(
                    [
                        MarketDataDaily(
                            instrument_id=leader.id,
                            trade_date=trade_date,
                            open=leader_close - Decimal("1"),
                            high=leader_close + Decimal("2"),
                            low=leader_close - Decimal("2"),
                            close=leader_close,
                            adj_close=leader_close,
                            volume=1000 + index,
                            data_status="complete",
                            data_source="test",
                        ),
                        MarketDataDaily(
                            instrument_id=middle.id,
                            trade_date=trade_date,
                            open=middle_close - Decimal("1"),
                            high=middle_close + Decimal("2"),
                            low=middle_close - Decimal("2"),
                            close=middle_close,
                            adj_close=middle_close,
                            volume=2000 + index,
                            data_status="complete",
                            data_source="test",
                        ),
                        MarketDataDaily(
                            instrument_id=laggard.id,
                            trade_date=trade_date,
                            open=laggard_close - Decimal("1"),
                            high=laggard_close + Decimal("2"),
                            low=laggard_close - Decimal("2"),
                            close=laggard_close,
                            adj_close=laggard_close,
                            volume=3000 + index,
                            data_status="complete",
                            data_source="test",
                        ),
                    ]
                )

            session.add_all(rows)
            session.commit()

        with self.session_factory() as session:
            get_active_strategy_configuration(session)
            save_strategy_configuration(
                session,
                rps_threshold=90,
                selected_rps_windows=[50, 120, 250],
                high_proximity_threshold_pct=Decimal("5.00"),
            )
            materialize_derived_indicator_facts(session)
            run = execute_screen_run(session)
            return leader_id, run.id

    def test_inline_analysis_payload_uses_one_year_candlestick_window(self) -> None:
        instrument_id, screen_run_id = self._seed_screen_context(market_days=260)

        with self.session_factory() as session:
            refresh_instrument_fundamentals(
                session,
                instrument_id=instrument_id,
                provider=_InlineFundamentalsProvider(
                    [
                        ProviderFundamentalsAnnual(
                            symbol="7203.T",
                            exchange="TSE",
                            fiscal_year_end_date=date(2024, 3, 31),
                            fiscal_year_label="FY2024",
                            net_income=Decimal("1000"),
                            net_income_currency="JPY",
                            pe=Decimal("10.1"),
                            pb=Decimal("1.11"),
                            source="inline-stub",
                            source_as_of_date=date.today(),
                            data_status="complete",
                        )
                    ]
                ),
            )
            payload = get_inline_analysis_payload(
                session,
                instrument_id=instrument_id,
                screen_run_id=screen_run_id,
            )

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload.candlestick_window_days_available, 252)
        self.assertEqual(len(payload.candlesticks), 252)
        self.assertEqual(payload.screen_run_ref["id"], screen_run_id)
        self.assertEqual(len(payload.valuation_by_fiscal_year), 1)

    def test_inline_analysis_payload_surfaces_short_history_for_recent_ipo(self) -> None:
        instrument_id, screen_run_id = self._seed_screen_context(market_days=30)

        with self.session_factory() as session:
            payload = get_inline_analysis_payload(
                session,
                instrument_id=instrument_id,
                screen_run_id=screen_run_id,
                provider=_InlineFundamentalsProvider([]),
            )

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload.candlestick_window_days_available, 30)
        self.assertEqual(len(payload.candlesticks), 30)

    def test_inline_analysis_payload_does_not_fail_when_fundamentals_provider_errors(self) -> None:
        instrument_id, screen_run_id = self._seed_screen_context(market_days=260)

        with self.session_factory() as session:
            payload = get_inline_analysis_payload(
                session,
                instrument_id=instrument_id,
                screen_run_id=screen_run_id,
                provider=_InlineFundamentalsProvider(RuntimeError("rate-limited")),
            )

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload.valuation_by_fiscal_year, [])

    def test_inline_analysis_payload_keeps_fiscal_years_in_ascending_order_and_limits_to_five(self) -> None:
        instrument_id, screen_run_id = self._seed_screen_context(market_days=260)

        provider_rows = [
            ProviderFundamentalsAnnual(
                symbol="7203.T",
                exchange="TSE",
                fiscal_year_end_date=date(2018 + index, 3, 31),
                fiscal_year_label=f"FY{2018 + index}",
                net_income=Decimal("1000") + Decimal(index),
                net_income_currency="JPY",
                pe=Decimal("10.1"),
                pb=Decimal("1.11"),
                source="inline-stub",
                source_as_of_date=date.today(),
                data_status="complete",
            )
            for index in range(6)
        ]

        with self.session_factory() as session:
            payload = get_inline_analysis_payload(
                session,
                instrument_id=instrument_id,
                screen_run_id=screen_run_id,
                provider=_InlineFundamentalsProvider(provider_rows),
            )

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(
            [row["fiscal_year_label"] for row in payload.valuation_by_fiscal_year],
            ["FY2019", "FY2020", "FY2021", "FY2022", "FY2023"],
        )


if __name__ == "__main__":
    unittest.main()
