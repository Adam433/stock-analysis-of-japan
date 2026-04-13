from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.strategy_config import (
    DEFAULT_HIGH_PROXIMITY_THRESHOLD_PCT,
    DEFAULT_RPS_THRESHOLD,
    get_active_strategy_configuration,
    save_strategy_configuration,
)

router = APIRouter(prefix="/screen", tags=["screen"])


class StrategyConfigurationPayload(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    rps_threshold: int = Field(ge=0, le=100)
    high_proximity_threshold_pct: Decimal = Field(ge=0, le=100)


@router.get("/configuration")
def read_strategy_configuration() -> dict[str, object]:
    with SessionLocal() as session:
        configuration = get_active_strategy_configuration(session)

    return {
        "configuration": configuration.to_dict(),
        "validation": {
            "rps_threshold": {"min": 0, "max": 100, "default": DEFAULT_RPS_THRESHOLD},
            "high_proximity_threshold_pct": {
                "min": "0.00",
                "max": "100.00",
                "default": f"{DEFAULT_HIGH_PROXIMITY_THRESHOLD_PCT:.2f}",
            },
        },
    }


@router.put("/configuration")
def update_strategy_configuration(payload: StrategyConfigurationPayload) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            configuration = save_strategy_configuration(
                session,
                rps_threshold=payload.rps_threshold,
                high_proximity_threshold_pct=payload.high_proximity_threshold_pct,
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return {"configuration": configuration.to_dict()}
