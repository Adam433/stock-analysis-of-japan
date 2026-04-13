from __future__ import annotations

import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.services.watchlist import add_watchlist_entry, list_watchlist_entries, remove_watchlist_entry


class WatchlistTests(unittest.TestCase):
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

    def _seed_instrument(self) -> int:
        with self.session_factory() as session:
            instrument = Instrument(symbol="7203", exchange="TSE", name="Toyota")
            session.add(instrument)
            session.commit()
            return instrument.id

    def test_add_watchlist_entry_persists_canonical_instrument_binding(self) -> None:
        instrument_id = self._seed_instrument()

        with self.session_factory() as session:
            entry = add_watchlist_entry(session, instrument_id)
            entries = list_watchlist_entries(session)

        self.assertEqual(entry.instrument_id, instrument_id)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].symbol, "7203")

    def test_add_watchlist_entry_is_idempotent_for_same_instrument(self) -> None:
        instrument_id = self._seed_instrument()

        with self.session_factory() as session:
            first = add_watchlist_entry(session, instrument_id)
            second = add_watchlist_entry(session, instrument_id)
            entries = list_watchlist_entries(session)

        self.assertEqual(first.id, second.id)
        self.assertEqual(len(entries), 1)

    def test_remove_watchlist_entry_deletes_existing_binding(self) -> None:
        instrument_id = self._seed_instrument()

        with self.session_factory() as session:
            add_watchlist_entry(session, instrument_id)
            removed = remove_watchlist_entry(session, instrument_id)
            entries = list_watchlist_entries(session)

        self.assertTrue(removed)
        self.assertEqual(entries, [])


if __name__ == "__main__":
    unittest.main()
