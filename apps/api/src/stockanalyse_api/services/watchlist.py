from __future__ import annotations

from dataclasses import asdict, dataclass

from sqlalchemy import select

from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.watchlists.models import WatchlistEntry


@dataclass(slots=True)
class WatchlistEntrySummary:
    id: int
    instrument_id: int
    symbol: str
    exchange: str
    name: str | None
    added_at: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def list_watchlist_entries(session) -> list[WatchlistEntrySummary]:
    rows = session.execute(
        select(WatchlistEntry, Instrument)
        .join(Instrument, Instrument.id == WatchlistEntry.instrument_id)
        .order_by(Instrument.symbol.asc(), WatchlistEntry.id.asc())
    ).all()

    return [
        WatchlistEntrySummary(
            id=entry.id,
            instrument_id=instrument.id,
            symbol=instrument.symbol,
            exchange=instrument.exchange,
            name=instrument.name,
            added_at=entry.created_at.isoformat(),
        )
        for entry, instrument in rows
    ]


def add_watchlist_entry(session, instrument_id: int) -> WatchlistEntrySummary:
    instrument = session.get(Instrument, instrument_id)
    if instrument is None:
        raise ValueError("Instrument not found.")

    existing = session.execute(
        select(WatchlistEntry).where(WatchlistEntry.instrument_id == instrument_id).limit(1)
    ).scalar_one_or_none()
    if existing is not None:
        return WatchlistEntrySummary(
            id=existing.id,
            instrument_id=instrument.id,
            symbol=instrument.symbol,
            exchange=instrument.exchange,
            name=instrument.name,
            added_at=existing.created_at.isoformat(),
        )

    entry = WatchlistEntry(instrument_id=instrument_id)
    session.add(entry)
    session.commit()
    session.refresh(entry)

    return WatchlistEntrySummary(
        id=entry.id,
        instrument_id=instrument.id,
        symbol=instrument.symbol,
        exchange=instrument.exchange,
        name=instrument.name,
        added_at=entry.created_at.isoformat(),
    )


def remove_watchlist_entry(session, instrument_id: int) -> bool:
    entry = session.execute(
        select(WatchlistEntry).where(WatchlistEntry.instrument_id == instrument_id).limit(1)
    ).scalar_one_or_none()
    if entry is None:
        return False

    session.delete(entry)
    session.commit()
    return True
