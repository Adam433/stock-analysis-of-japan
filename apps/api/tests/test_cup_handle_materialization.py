from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from math import ceil
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import stockanalyse_api.domain.backtests.models  # noqa: F401
import stockanalyse_api.domain.fundamentals.models  # noqa: F401
import stockanalyse_api.domain.indicators.models  # noqa: F401
import stockanalyse_api.domain.instruments.models  # noqa: F401
import stockanalyse_api.domain.market_data.models  # noqa: F401
import stockanalyse_api.domain.screens.models  # noqa: F401
from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.backtests.models import (
    CupHandleMaterializationRun,
    CupHandlePatternEvent,
)
from stockanalyse_api.domain.indicators.models import DerivedIndicatorDaily
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.cup_handle_materialization import (
    CANDIDATE_GENERATION_BOUNDS,
    CUP_HANDLE_DETECTOR_VERSION,
    dump_json,
    materialize_cup_handle_candidates,
)
from stockanalyse_api.services.dashboard import DEFAULT_CUP_HANDLE_PARAMS, screen_universe


@dataclass(frozen=True, slots=True)
class _SyntheticCandle:
    trade_date: date
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int = 1000


def _d(value: float) -> Decimal:
    return Decimal(str(round(value, 4)))


def _make_cup_handle_candles(cup_duration: int = 130, handle_duration: int = 10) -> list[_SyntheticCandle]:
    start = date(2025, 1, 1)
    prior_duration = 60
    bottom_idx = cup_duration // 2
    candles: list[_SyntheticCandle] = []
    for idx in range(prior_duration):
        price = 62 + 38 * (idx / max(prior_duration - 1, 1))
        candles.append(_SyntheticCandle(start + timedelta(days=idx), _d(price), _d(price), _d(price)))
    for idx in range(cup_duration + 1):
        if idx <= bottom_idx:
            price = 100 - 25 * (idx / bottom_idx)
        else:
            price = 75 + 25 * ((idx - bottom_idx) / (cup_duration - bottom_idx))
        candles.append(
            _SyntheticCandle(
                start + timedelta(days=prior_duration + idx),
                _d(price),
                _d(price),
                _d(price),
            )
        )
    mid_handle = max(handle_duration // 2, 1)
    for offset in range(1, handle_duration):
        if offset <= mid_handle:
            price = 100 - 8 * (offset / mid_handle)
        else:
            price = 92 + 4 * ((offset - mid_handle) / max(handle_duration - mid_handle - 1, 1))
        idx = prior_duration + cup_duration + offset
        candles.append(_SyntheticCandle(start + timedelta(days=idx), _d(price), _d(price), _d(price)))
    breakout_idx = prior_duration + cup_duration + handle_duration
    candles.append(
        _SyntheticCandle(
            start + timedelta(days=breakout_idx),
            Decimal("106"),
            Decimal("101"),
            Decimal("105"),
            2000,
        )
    )
    return candles


class CupHandleMaterializationTests(unittest.TestCase):
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

    def _seed_rps_candidates(self, session, signal_date: date) -> tuple[Instrument, Instrument]:
        leader = Instrument(symbol="AAPL", exchange="US", name="Leader")
        laggard = Instrument(symbol="MSFT", exchange="US", name="No Pattern")
        session.add_all([leader, laggard])
        session.flush()
        for instrument in (leader, laggard):
            session.add(
                DerivedIndicatorDaily(
                    instrument_id=instrument.id,
                    trade_date=signal_date,
                    rps_50=Decimal("95"),
                    rps_120=Decimal("94"),
                    rps_250=Decimal("93"),
                )
            )
        return leader, laggard

    def _seed_materialization_run(
        self,
        session,
        *,
        signal_date: date,
    ) -> CupHandleMaterializationRun:
        run = CupHandleMaterializationRun(
            market="us",
            status="completed",
            started_at=datetime(2026, 5, 2, tzinfo=UTC),
            completed_at=datetime(2026, 5, 2, 0, 1, tzinfo=UTC),
            source_start_date=signal_date
            - timedelta(days=ceil(DEFAULT_CUP_HANDLE_PARAMS.effective_lookback_days * 8 / 5)),
            source_end_date=signal_date,
            latest_market_data_date=signal_date,
            generation_bounds_json=dump_json(CANDIDATE_GENERATION_BOUNDS),
            feature_windows_json=dump_json(
                {
                    "prior_uptrend": [60, 90, 120, 180],
                    "breakout_volume": [20, 50, 60],
                    "bottom_zones": [20, 35],
                }
            ),
            detector_version=CUP_HANDLE_DETECTOR_VERSION,
            events_created=0,
            symbols_processed=2,
        )
        session.add(run)
        session.flush()
        return run

    def _seed_event(
        self,
        session,
        *,
        run: CupHandleMaterializationRun,
        instrument: Instrument,
        signal_date: date,
        prior_uptrend_pct_120: str = "35",
    ) -> None:
        session.add(
            CupHandlePatternEvent(
                market="us",
                materialization_run_id=run.id,
                instrument_id=instrument.id,
                symbol_snapshot=instrument.symbol,
                breakout_date=signal_date,
                left_lip_date=date(2025, 1, 15),
                cup_bottom_date=date(2025, 3, 1),
                right_lip_date=date(2025, 5, 10),
                handle_low_date=date(2025, 5, 20),
                cup_duration=130,
                handle_duration=10,
                total_duration=140,
                cup_depth_pct=Decimal("25"),
                handle_depth_pct=Decimal("8"),
                right_lip_delta_pct=Decimal("2"),
                handle_low_position_pct=Decimal("75"),
                handle_depth_to_cup_depth_pct=Decimal("30"),
                handle_high_above_lip_pct=Decimal("1"),
                bottom_dwell_days_zone_20=6,
                bottom_dwell_days_zone_35=12,
                bottom_span_pct_zone_20=Decimal("12"),
                bottom_span_pct_zone_35=Decimal("25"),
                left_side_duration_pct=Decimal("45"),
                right_side_duration_pct=Decimal("55"),
                prior_uptrend_pct_60=Decimal("20"),
                prior_uptrend_pct_90=Decimal("28"),
                prior_uptrend_pct_120=Decimal(prior_uptrend_pct_120),
                prior_uptrend_pct_180=Decimal("50"),
                breakout_volume_ratio_20=Decimal("1.20"),
                breakout_volume_ratio_50=Decimal("1.50"),
                breakout_volume_ratio_60=Decimal("1.40"),
                breakout_close_over_resistance_pct=Decimal("3"),
                data_start_date=date(2025, 1, 1),
                data_end_date=signal_date,
                detector_version=CUP_HANDLE_DETECTOR_VERSION,
            )
        )
        run.events_created += 1

    def test_screen_universe_uses_materialized_cup_handle_events(self) -> None:
        signal_date = date(2025, 6, 1)
        with self.session_factory() as session:
            leader, _ = self._seed_rps_candidates(session, signal_date)
            run = self._seed_materialization_run(session, signal_date=signal_date)
            self._seed_event(session, run=run, instrument=leader, signal_date=signal_date)
            session.commit()

            with (
                patch("stockanalyse_api.services.dashboard._load_candles_by_instrument") as load_mock,
                patch("stockanalyse_api.services.dashboard._detect_cup_handle_pattern") as detect_mock,
            ):
                result = screen_universe(
                    session,
                    use_rps=True,
                    rps_threshold=85,
                    selected_rps_windows=[50, 120, 250],
                    min_rps_windows_passing=2,
                    use_cup_handle=True,
                    cup_handle_params=DEFAULT_CUP_HANDLE_PARAMS,
                    trade_date=signal_date,
                    market="us",
                )

        self.assertEqual(result["diagnostics"]["cup_handle_source"], "materialized")
        self.assertEqual([hit["symbol"] for hit in result["hits"]], ["AAPL"])
        self.assertEqual(result["hits"][0]["cup_handle_breakout_date"], signal_date.isoformat())
        load_mock.assert_not_called()
        detect_mock.assert_not_called()

    def test_materialized_cup_handle_events_apply_parameter_filters(self) -> None:
        signal_date = date(2025, 6, 1)
        with self.session_factory() as session:
            leader, _ = self._seed_rps_candidates(session, signal_date)
            run = self._seed_materialization_run(session, signal_date=signal_date)
            self._seed_event(
                session,
                run=run,
                instrument=leader,
                signal_date=signal_date,
                prior_uptrend_pct_120="25",
            )
            session.commit()

            result = screen_universe(
                session,
                use_rps=True,
                rps_threshold=85,
                selected_rps_windows=[50, 120, 250],
                min_rps_windows_passing=2,
                use_cup_handle=True,
                cup_handle_params=DEFAULT_CUP_HANDLE_PARAMS,
                trade_date=signal_date,
                market="us",
            )

        self.assertEqual(result["diagnostics"]["cup_handle_source"], "materialized")
        self.assertEqual(result["hits"], [])

    def test_materialized_run_requires_enough_source_history(self) -> None:
        signal_date = date(2025, 6, 1)
        with self.session_factory() as session:
            leader, _ = self._seed_rps_candidates(session, signal_date)
            run = self._seed_materialization_run(session, signal_date=signal_date)
            run.source_start_date = signal_date
            self._seed_event(session, run=run, instrument=leader, signal_date=signal_date)
            session.commit()

            with patch("stockanalyse_api.services.dashboard._load_candles_by_instrument", return_value={}):
                result = screen_universe(
                    session,
                    use_rps=True,
                    rps_threshold=85,
                    selected_rps_windows=[50, 120, 250],
                    min_rps_windows_passing=2,
                    use_cup_handle=True,
                    cup_handle_params=DEFAULT_CUP_HANDLE_PARAMS,
                    trade_date=signal_date,
                    market="us",
                )

        self.assertEqual(result["diagnostics"]["cup_handle_source"], "runtime_scan")
        self.assertEqual(result["hits"], [])

    def test_materialize_cup_handle_candidates_persists_events(self) -> None:
        candles = _make_cup_handle_candles()
        with self.session_factory() as session:
            instrument = Instrument(symbol="AAPL", exchange="US", name="Leader")
            session.add(instrument)
            session.flush()
            session.add_all(
                [
                    MarketDataDaily(
                        instrument_id=instrument.id,
                        trade_date=candle.trade_date,
                        open=candle.close,
                        high=candle.high,
                        low=candle.low,
                        close=candle.close,
                        adj_close=candle.close,
                        volume=candle.volume,
                        data_status="complete",
                        data_source="test",
                    )
                    for candle in candles
                ]
            )
            session.commit()

            run = materialize_cup_handle_candidates(
                session,
                market="us",
                source_start_date=candles[0].trade_date,
                source_end_date=candles[-1].trade_date,
                commit_every=1,
            )
            events = session.query(CupHandlePatternEvent).all()

        self.assertEqual(run.status, "completed")
        self.assertGreaterEqual(run.events_created, 1)
        self.assertEqual(len(events), run.events_created)
        self.assertEqual(events[-1].breakout_date, candles[-1].trade_date)


if __name__ == "__main__":
    unittest.main()
