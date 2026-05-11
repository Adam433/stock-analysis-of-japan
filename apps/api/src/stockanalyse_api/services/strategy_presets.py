from __future__ import annotations

from sqlalchemy import select

from stockanalyse_api.domain.backtests.models import StrategyPreset
from stockanalyse_api.services.dashboard import normalize_market
from stockanalyse_api.services.strategy_parameters import dump_json, load_json, stable_parameter_hash


def serialize_strategy_preset(preset: StrategyPreset) -> dict[str, object]:
    return {
        "id": preset.id,
        "market": preset.market,
        "name": preset.name,
        "description": preset.description,
        "parameters_hash": preset.parameters_hash,
        "parameters": load_json(preset.parameters_json, default={}),
        "is_active": preset.is_active,
        "source_optimization_run_id": preset.source_optimization_run_id,
        "source_optimization_result_id": preset.source_optimization_result_id,
        "created_at": preset.created_at.isoformat() if preset.created_at else None,
        "updated_at": preset.updated_at.isoformat() if preset.updated_at else None,
    }


def save_strategy_preset(
    session,
    *,
    market: str = "us",
    name: str,
    parameters: dict[str, object],
    description: str | None = None,
    source_optimization_run_id: int | None = None,
    source_optimization_result_id: int | None = None,
) -> StrategyPreset:
    resolved_market = normalize_market(market)
    if not name.strip():
        raise ValueError("name must not be empty.")
    preset = StrategyPreset(
        market=resolved_market,
        name=name.strip(),
        description=description,
        parameters_hash=stable_parameter_hash(parameters),
        parameters_json=dump_json(parameters),
        is_active=False,
        source_optimization_run_id=source_optimization_run_id,
        source_optimization_result_id=source_optimization_result_id,
    )
    session.add(preset)
    session.commit()
    session.refresh(preset)
    return preset


def list_strategy_presets(session, *, market: str = "us") -> list[StrategyPreset]:
    resolved_market = normalize_market(market)
    return list(
        session.execute(
            select(StrategyPreset)
            .where(StrategyPreset.market == resolved_market)
            .order_by(StrategyPreset.is_active.desc(), StrategyPreset.updated_at.desc(), StrategyPreset.id.desc())
        ).scalars()
    )


def get_strategy_preset(session, preset_id: int) -> StrategyPreset | None:
    return session.get(StrategyPreset, preset_id)


def activate_strategy_preset(session, preset_id: int) -> StrategyPreset:
    preset = session.get(StrategyPreset, preset_id)
    if preset is None:
        raise LookupError("Strategy preset not found.")
    presets = session.execute(
        select(StrategyPreset).where(StrategyPreset.market == preset.market)
    ).scalars()
    for row in presets:
        row.is_active = row.id == preset.id
    session.commit()
    session.refresh(preset)
    return preset


def update_strategy_preset(
    session,
    preset_id: int,
    *,
    name: str | None = None,
    description: str | None = None,
    parameters: dict[str, object] | None = None,
    is_active: bool | None = None,
) -> StrategyPreset:
    preset = session.get(StrategyPreset, preset_id)
    if preset is None:
        raise LookupError("Strategy preset not found.")
    if name is not None:
        if not name.strip():
            raise ValueError("name must not be empty.")
        preset.name = name.strip()
    if description is not None:
        preset.description = description
    if parameters is not None:
        preset.parameters_hash = stable_parameter_hash(parameters)
        preset.parameters_json = dump_json(parameters)
    if is_active is True:
        presets = session.execute(
            select(StrategyPreset).where(StrategyPreset.market == preset.market)
        ).scalars()
        for row in presets:
            row.is_active = row.id == preset.id
    elif is_active is False:
        preset.is_active = False
    session.commit()
    session.refresh(preset)
    return preset


def duplicate_strategy_preset(
    session,
    preset_id: int,
    *,
    name: str | None = None,
) -> StrategyPreset:
    preset = session.get(StrategyPreset, preset_id)
    if preset is None:
        raise LookupError("Strategy preset not found.")
    copy_name = (name or f"{preset.name} Copy").strip()
    if not copy_name:
        raise ValueError("name must not be empty.")
    duplicate = StrategyPreset(
        market=preset.market,
        name=copy_name,
        description=preset.description,
        parameters_hash=preset.parameters_hash,
        parameters_json=preset.parameters_json,
        is_active=False,
        source_optimization_run_id=preset.source_optimization_run_id,
        source_optimization_result_id=preset.source_optimization_result_id,
    )
    session.add(duplicate)
    session.commit()
    session.refresh(duplicate)
    return duplicate


def delete_strategy_preset(session, preset_id: int) -> None:
    preset = session.get(StrategyPreset, preset_id)
    if preset is None:
        raise LookupError("Strategy preset not found.")
    session.delete(preset)
    session.commit()
