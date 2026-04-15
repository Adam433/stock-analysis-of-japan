from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.strategy_config import (
    APPROVED_RPS_WINDOWS,
    DEFAULT_HIGH_PROXIMITY_THRESHOLD_PCT,
    DEFAULT_MIN_RPS_LINES_REQUIRED,
    DEFAULT_RPS_THRESHOLD,
    DEFAULT_SELECTED_RPS_WINDOWS,
    get_active_strategy_configuration,
    save_strategy_configuration,
)

router = APIRouter(prefix="/screen", tags=["screen"])


class StrategyConfigurationPayload(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    rps_threshold: int = Field(ge=0, le=100)
    selected_rps_windows: list[int]
    min_rps_lines_required: int = Field(ge=1)
    high_proximity_threshold_pct: Decimal = Field(ge=0, le=100)


@router.get("/configuration")
def read_strategy_configuration() -> dict[str, object]:
    with SessionLocal() as session:
        configuration = get_active_strategy_configuration(session)

    return {
        "configuration": configuration.to_dict(),
        "validation": {
            "rps_threshold": {"min": 0, "max": 100, "default": DEFAULT_RPS_THRESHOLD},
            "selected_rps_windows": {
                "approved": list(APPROVED_RPS_WINDOWS),
                "default": DEFAULT_SELECTED_RPS_WINDOWS,
            },
            "min_rps_lines_required": {
                "min": 1,
                "max": len(APPROVED_RPS_WINDOWS),
                "default": DEFAULT_MIN_RPS_LINES_REQUIRED,
            },
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
                selected_rps_windows=payload.selected_rps_windows,
                min_rps_lines_required=payload.min_rps_lines_required,
                high_proximity_threshold_pct=payload.high_proximity_threshold_pct,
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return {"configuration": configuration.to_dict()}
