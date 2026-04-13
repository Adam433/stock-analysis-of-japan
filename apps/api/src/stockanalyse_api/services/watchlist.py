from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime

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
    note: str | None
    observation_reason: str | None
    added_date: str
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
            note=entry.note,
            observation_reason=entry.observation_reason,
            added_date=entry.added_date.isoformat(),
            added_at=entry.created_at.isoformat(),
        )
        for entry, instrument in rows
    ]


def _to_summary(entry: WatchlistEntry, instrument: Instrument) -> WatchlistEntrySummary:
    return WatchlistEntrySummary(
        id=entry.id,
        instrument_id=instrument.id,
        symbol=instrument.symbol,
        exchange=instrument.exchange,
        name=instrument.name,
        note=entry.note,
        observation_reason=entry.observation_reason,
        added_date=entry.added_date.isoformat(),
        added_at=entry.created_at.isoformat(),
    )


def add_watchlist_entry(
    session,
    instrument_id: int,
    *,
    note: str | None = None,
    observation_reason: str | None = None,
) -> WatchlistEntrySummary:
    instrument = session.get(Instrument, instrument_id)
    if instrument is None:
        raise ValueError("Instrument not found.")

    existing = session.execute(
        select(WatchlistEntry).where(WatchlistEntry.instrument_id == instrument_id).limit(1)
    ).scalar_one_or_none()
    if existing is not None:
        if note is not None:
            existing.note = note
        if observation_reason is not None:
            existing.observation_reason = observation_reason
        session.commit()
        session.refresh(existing)
        return _to_summary(existing, instrument)

    entry = WatchlistEntry(
        instrument_id=instrument_id,
        note=note,
        observation_reason=observation_reason,
        added_date=datetime.now(UTC).date(),
    )
    session.add(entry)
    session.commit()
    session.refresh(entry)

    return _to_summary(entry, instrument)


def update_watchlist_entry(
    session,
    instrument_id: int,
    *,
    note: str | None,
    observation_reason: str | None,
) -> WatchlistEntrySummary:
    instrument = session.get(Instrument, instrument_id)
    if instrument is None:
        raise ValueError("Instrument not found.")

    entry = session.execute(
        select(WatchlistEntry).where(WatchlistEntry.instrument_id == instrument_id).limit(1)
    ).scalar_one_or_none()
    if entry is None:
        raise LookupError("Watchlist entry not found.")

    entry.note = note
    entry.observation_reason = observation_reason
    session.commit()
    session.refresh(entry)
    return _to_summary(entry, instrument)


def remove_watchlist_entry(session, instrument_id: int) -> bool:
    entry = session.execute(
        select(WatchlistEntry).where(WatchlistEntry.instrument_id == instrument_id).limit(1)
    ).scalar_one_or_none()
    if entry is None:
        return False

    session.delete(entry)
    session.commit()
    return True
