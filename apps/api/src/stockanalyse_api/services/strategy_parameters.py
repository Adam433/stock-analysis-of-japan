from __future__ import annotations

import json
from dataclasses import fields
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Any

from stockanalyse_api.services.dashboard import (
    DEFAULT_CUP_HANDLE_PARAMS,
    DEFAULT_FUNDAMENTAL_GROWTH_PARAMS,
    CupHandleParams,
    FundamentalGrowthParams,
)
from stockanalyse_api.services.dashboard_strategy_backtest import (
    DEFAULT_CASH_FALLBACK_PARAMS,
    DEFAULT_MARKET_FILTER_PARAMS,
    DEFAULT_RELATIVE_STRENGTH_PARAMS,
    _normalize_cash_fallback_params,
    _normalize_market_filter_params,
    _normalize_relative_strength_params,
)

STRATEGY_PARAMETER_SCHEMA_VERSION = "strategy_parameters_v1"

DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS = FundamentalGrowthParams(
    enabled=True,
    min_years=3,
    min_growth_count=2,
    min_yoy_growth_pct=Decimal("0"),
    require_positive_net_income=True,
    reporting_lag_days=120,
    max_pe=Decimal("60"),
    max_pb=Decimal("15"),
    require_positive_operating_cash_flow=True,
    require_positive_free_cash_flow=False,
    min_operating_cash_flow_growth_count=1,
    min_operating_cash_flow_yoy_growth_pct=Decimal("0"),
)


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime, Decimal)):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable.")


def dump_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default)


def load_json(value: str | None, *, default: Any = None) -> Any:
    if value is None:
        return default
    return json.loads(value)


def stable_parameter_hash(parameters: dict[str, object]) -> str:
    return sha256(dump_json(parameters).encode("utf-8")).hexdigest()


def _coerce_decimal(value: object, *, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be a decimal number.") from exc


def _coerce_optional_decimal(value: object, *, field_name: str) -> Decimal | None:
    if value is None or value == "":
        return None
    return _coerce_decimal(value, field_name=field_name)


def _coerce_optional_int(value: object, *, field_name: str) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _dataclass_payload(default_obj: object, overrides: dict[str, object] | None) -> dict[str, object]:
    payload = {field.name: getattr(default_obj, field.name) for field in fields(default_obj)}
    payload.update(overrides or {})
    return payload


def _cup_handle_params_from_payload(payload: dict[str, object] | None) -> CupHandleParams:
    merged = _dataclass_payload(DEFAULT_CUP_HANDLE_PARAMS, payload)
    decimal_fields = {
        field.name
        for field in fields(CupHandleParams)
        if isinstance(getattr(DEFAULT_CUP_HANDLE_PARAMS, field.name), Decimal)
    }
    return CupHandleParams(
        **{
            key: _coerce_decimal(value, field_name=key) if key in decimal_fields else value
            for key, value in merged.items()
        }
    )


def _fundamental_params_from_payload(
    payload: dict[str, object] | None,
    *,
    default_params: FundamentalGrowthParams = DEFAULT_FUNDAMENTAL_GROWTH_PARAMS,
    force_enabled: bool = False,
) -> FundamentalGrowthParams:
    merged = _dataclass_payload(default_params, payload)
    if force_enabled:
        merged["enabled"] = True
    return FundamentalGrowthParams(
        enabled=bool(merged["enabled"]),
        min_years=int(merged["min_years"]),
        min_growth_count=(
            None if merged.get("min_growth_count") is None else int(merged["min_growth_count"])
        ),
        min_yoy_growth_pct=_coerce_decimal(
            merged["min_yoy_growth_pct"],
            field_name="min_yoy_growth_pct",
        ),
        require_positive_net_income=bool(merged["require_positive_net_income"]),
        reporting_lag_days=int(merged["reporting_lag_days"]),
        max_pe=(
            None
            if merged.get("max_pe") is None
            else _coerce_decimal(merged["max_pe"], field_name="max_pe")
        ),
        max_pb=(
            None
            if merged.get("max_pb") is None
            else _coerce_decimal(merged["max_pb"], field_name="max_pb")
        ),
        require_positive_operating_cash_flow=bool(
            merged["require_positive_operating_cash_flow"]
        ),
        require_positive_free_cash_flow=bool(merged["require_positive_free_cash_flow"]),
        min_operating_cash_flow_growth_count=(
            None
            if merged.get("min_operating_cash_flow_growth_count") is None
            else int(merged["min_operating_cash_flow_growth_count"])
        ),
        min_operating_cash_flow_yoy_growth_pct=_coerce_decimal(
            merged["min_operating_cash_flow_yoy_growth_pct"],
            field_name="min_operating_cash_flow_yoy_growth_pct",
        ),
    )


def normalize_strategy_parameter_set(parameters: dict[str, object]) -> dict[str, object]:
    use_rps = bool(parameters.get("use_rps", True))
    use_cup_handle = bool(parameters.get("use_cup_handle", True))
    cup_params = _cup_handle_params_from_payload(
        parameters.get("cup_handle_params")
        if use_cup_handle and isinstance(parameters.get("cup_handle_params"), dict)
        else {}
    )
    fundamental_params = _fundamental_params_from_payload(
        parameters.get("fundamental_growth_params")
        if isinstance(parameters.get("fundamental_growth_params"), dict)
        else {},
        default_params=DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS,
        force_enabled=True,
    )
    if not fundamental_params.enabled:
        fundamental_params = DEFAULT_OPTIMIZATION_FUNDAMENTAL_GROWTH_PARAMS
    rps_threshold = int(parameters.get("rps_threshold", 90)) if use_rps else 0
    if use_rps and not 0 <= rps_threshold <= 100:
        raise ValueError("rps_threshold must be between 0 and 100.")
    selected_windows = [int(window) for window in parameters.get("selected_rps_windows", [50, 120, 250])]  # type: ignore[arg-type]
    selected_windows = sorted(set(selected_windows), key=selected_windows.index)
    take_profit_pct = _coerce_optional_decimal(
        parameters.get("take_profit_pct"),
        field_name="take_profit_pct",
    )
    if take_profit_pct is not None and take_profit_pct <= Decimal("0"):
        raise ValueError("take_profit_pct must be greater than 0 when provided.")
    rps_exit_threshold = _coerce_optional_int(
        parameters.get("rps_exit_threshold"),
        field_name="rps_exit_threshold",
    )
    if rps_exit_threshold is not None and not 0 <= rps_exit_threshold <= 100:
        raise ValueError("rps_exit_threshold must be between 0 and 100 when provided.")
    if not use_rps:
        rps_exit_threshold = None
        selected_windows = [50, 120, 250]
    elif rps_exit_threshold is not None and rps_exit_threshold >= rps_threshold:
        raise ValueError(
            "rps_exit_threshold must be lower than rps_threshold when RPS exit is enabled."
        )
    holding_days = _coerce_optional_int(
        parameters.get("holding_days", 130),
        field_name="holding_days",
    )
    if holding_days is not None and holding_days < 1:
        raise ValueError("holding_days must be greater than or equal to 1.")
    position_weight_pct = _coerce_decimal(
        parameters.get("position_weight_pct", "0.10"),
        field_name="position_weight_pct",
    )
    if position_weight_pct <= Decimal("0") or position_weight_pct > Decimal("1"):
        raise ValueError("position_weight_pct must be greater than 0 and less than or equal to 1.")
    initial_capital = _coerce_decimal(
        parameters.get("initial_capital", "100000"),
        field_name="initial_capital",
    )
    if initial_capital <= Decimal("0"):
        raise ValueError("initial_capital must be greater than 0.")
    position_size_amount = _coerce_optional_decimal(
        parameters.get("position_size_amount"),
        field_name="position_size_amount",
    )
    if position_size_amount is not None and position_size_amount <= Decimal("0"):
        raise ValueError("position_size_amount must be greater than 0 when provided.")
    resolved_position_size = (
        position_size_amount
        if position_size_amount is not None
        else initial_capital * position_weight_pct
    )
    if resolved_position_size > initial_capital:
        raise ValueError("position_size_amount cannot exceed initial_capital.")
    market_filter_params = _normalize_market_filter_params(
        parameters.get("market_filter_params")
        if isinstance(parameters.get("market_filter_params"), dict)
        else {}
    )
    relative_strength_params = _normalize_relative_strength_params(
        parameters.get("relative_strength_params")
        if isinstance(parameters.get("relative_strength_params"), dict)
        else {}
    )
    cash_fallback_params = _normalize_cash_fallback_params(
        parameters.get("cash_fallback_params")
        if isinstance(parameters.get("cash_fallback_params"), dict)
        else {}
    )
    return {
        "strategy_schema_version": STRATEGY_PARAMETER_SCHEMA_VERSION,
        "use_rps": use_rps,
        "rps_threshold": rps_threshold,
        "selected_rps_windows": selected_windows,
        "min_rps_windows_passing": int(parameters.get("min_rps_windows_passing", 1)) if use_rps else 1,
        "use_cup_handle": use_cup_handle,
        "cup_handle_params": cup_params.to_dict(),
        "fundamental_growth_params": fundamental_params.to_dict(),
        "holding_days": holding_days,
        "stop_loss_pct": f"{_coerce_decimal(parameters.get('stop_loss_pct', '-0.08'), field_name='stop_loss_pct'):.4f}",
        "take_profit_pct": f"{take_profit_pct:.4f}" if take_profit_pct is not None else None,
        "rps_exit_threshold": rps_exit_threshold,
        "portfolio_cap": int(parameters.get("portfolio_cap", 10)),
        "position_weight_pct": f"{position_weight_pct:.4f}",
        "initial_capital": f"{initial_capital:.2f}",
        "position_size_amount": f"{position_size_amount:.2f}" if position_size_amount is not None else None,
        "allow_reentry_while_open": bool(parameters.get("allow_reentry_while_open", False)),
        "market_filter_params": market_filter_params,
        "relative_strength_params": relative_strength_params,
        "cash_fallback_params": cash_fallback_params,
        "entry_delay_days": int(parameters.get("entry_delay_days", 0)),
        "entry_deferral_window_days": int(parameters.get("entry_deferral_window_days", 5)),
    }
