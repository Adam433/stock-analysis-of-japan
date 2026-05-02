from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.strategy_presets import (
    activate_strategy_preset,
    delete_strategy_preset,
    duplicate_strategy_preset,
    get_strategy_preset,
    list_strategy_presets,
    save_strategy_preset,
    serialize_strategy_preset,
    update_strategy_preset,
)

router = APIRouter(prefix="/strategy-presets", tags=["strategy-presets"])


class StrategyPresetCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market: str = Field(default="us", pattern="^(jp|us)$")
    name: str = Field(min_length=1, max_length=128)
    description: str | None = None
    parameters: dict[str, object]
    source_optimization_run_id: int | None = None
    source_optimization_result_id: int | None = None


class StrategyPresetUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=128)
    description: str | None = None
    parameters: dict[str, object] | None = None
    is_active: bool | None = None


class StrategyPresetDuplicateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=128)


@router.post("")
def create_strategy_preset(payload: StrategyPresetCreateRequest) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            preset = save_strategy_preset(
                session,
                market=payload.market,
                name=payload.name,
                description=payload.description,
                parameters=payload.parameters,
                source_optimization_run_id=payload.source_optimization_run_id,
                source_optimization_result_id=payload.source_optimization_result_id,
            )
            return {"strategy_preset": serialize_strategy_preset(preset)}
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("")
def read_strategy_presets(market: str = "us") -> dict[str, object]:
    try:
        with SessionLocal() as session:
            presets = list_strategy_presets(session, market=market)
            return {"strategy_presets": [serialize_strategy_preset(preset) for preset in presets]}
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/{preset_id}")
def read_strategy_preset(preset_id: int) -> dict[str, object]:
    with SessionLocal() as session:
        preset = get_strategy_preset(session, preset_id)
        if preset is None:
            raise HTTPException(status_code=404, detail="Strategy preset not found.")
        return {"strategy_preset": serialize_strategy_preset(preset)}


@router.patch("/{preset_id}")
def update_saved_strategy_preset(
    preset_id: int,
    payload: StrategyPresetUpdateRequest,
) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            preset = update_strategy_preset(
                session,
                preset_id,
                name=payload.name,
                description=payload.description,
                parameters=payload.parameters,
                is_active=payload.is_active,
            )
            return {"strategy_preset": serialize_strategy_preset(preset)}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/{preset_id}/duplicate")
def duplicate_saved_strategy_preset(
    preset_id: int,
    payload: StrategyPresetDuplicateRequest | None = None,
) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            preset = duplicate_strategy_preset(
                session,
                preset_id,
                name=payload.name if payload is not None else None,
            )
            return {"strategy_preset": serialize_strategy_preset(preset)}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/{preset_id}/activate")
def activate_saved_strategy_preset(preset_id: int) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            preset = activate_strategy_preset(session, preset_id)
            return {"strategy_preset": serialize_strategy_preset(preset)}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.delete("/{preset_id}")
def delete_saved_strategy_preset(preset_id: int) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            delete_strategy_preset(session, preset_id)
            return {"deleted": True}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
