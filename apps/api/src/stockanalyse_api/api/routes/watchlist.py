from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.watchlist import add_watchlist_entry, list_watchlist_entries, remove_watchlist_entry

router = APIRouter(prefix="/watchlist", tags=["watchlist"])


class WatchlistCreateRequest(BaseModel):
    instrument_id: int


@router.get("")
def read_watchlist() -> dict[str, object]:
    with SessionLocal() as session:
        entries = list_watchlist_entries(session)
    return {"entries": [entry.to_dict() for entry in entries]}


@router.post("")
def create_watchlist_entry(payload: WatchlistCreateRequest) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            entry = add_watchlist_entry(session, payload.instrument_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {"entry": entry.to_dict()}


@router.delete("/{instrument_id}")
def delete_watchlist_entry(instrument_id: int) -> dict[str, object]:
    with SessionLocal() as session:
        removed = remove_watchlist_entry(session, instrument_id)

    if not removed:
        raise HTTPException(status_code=404, detail="Watchlist entry not found.")

    return {"removed": True, "instrument_id": instrument_id}
