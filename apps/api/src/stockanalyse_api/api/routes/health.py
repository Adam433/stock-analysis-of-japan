from __future__ import annotations

from fastapi import APIRouter

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.health import get_market_data_health

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/ready")
def read_health_ready() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/market-data")
def read_market_data_health() -> dict[str, object]:
    with SessionLocal() as session:
        return get_market_data_health(session).to_dict()
