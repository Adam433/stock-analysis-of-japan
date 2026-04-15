from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import select

from stockanalyse_api.domain.screens.models import StrategyConfiguration

DEFAULT_RPS_THRESHOLD = 90
DEFAULT_HIGH_PROXIMITY_THRESHOLD_PCT = Decimal("5.00")
APPROVED_RPS_WINDOWS = (50, 120, 250)
DEFAULT_SELECTED_RPS_WINDOWS = list(APPROVED_RPS_WINDOWS)
DEFAULT_MIN_RPS_LINES_REQUIRED = 1
MIN_THRESHOLD = Decimal("0")
MAX_THRESHOLD = Decimal("100")


@dataclass(slots=True)
class StrategyConfigurationSnapshot:
    id: int
    version: int
    rps_threshold: int
    selected_rps_windows: list[int]
    min_rps_lines_required: int
    high_proximity_threshold_pct: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _serialize(configuration: StrategyConfiguration) -> StrategyConfigurationSnapshot:
    return StrategyConfigurationSnapshot(
        id=configuration.id,
        version=configuration.version,
        rps_threshold=configuration.rps_threshold,
        selected_rps_windows=_deserialize_selected_rps_windows(configuration.selected_rps_windows),
        min_rps_lines_required=configuration.min_rps_lines_required,
        high_proximity_threshold_pct=f"{configuration.high_proximity_threshold_pct:.2f}",
    )


def serialize_selected_rps_windows(selected_rps_windows: list[int]) -> str:
    return ",".join(str(window) for window in selected_rps_windows)


def _deserialize_selected_rps_windows(raw: str) -> list[int]:
    return [int(part) for part in raw.split(",") if part]


def _validate_rps_threshold(value: int) -> int:
    if value < 0 or value > 100:
        raise ValueError("rps_threshold must be between 0 and 100.")
    return value


def _validate_high_proximity_threshold_pct(value: Decimal) -> Decimal:
    normalized = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if normalized < MIN_THRESHOLD or normalized > MAX_THRESHOLD:
        raise ValueError("high_proximity_threshold_pct must be between 0.00 and 100.00.")
    return normalized


def _validate_selected_rps_windows(values: list[int]) -> list[int]:
    if not values:
        raise ValueError("selected_rps_windows must not be empty.")
    unique_values = sorted(set(values), key=lambda window: APPROVED_RPS_WINDOWS.index(window) if window in APPROVED_RPS_WINDOWS else 999)
    if any(window not in APPROVED_RPS_WINDOWS for window in unique_values):
        raise ValueError("selected_rps_windows must be chosen from the approved RPS windows.")
    return unique_values


def _validate_min_rps_lines_required(value: int, *, selected_rps_windows: list[int]) -> int:
    if value < 1:
        raise ValueError("min_rps_lines_required must be at least 1.")
    if value > len(selected_rps_windows):
        raise ValueError("min_rps_lines_required cannot exceed the number of selected_rps_windows.")
    return value


def get_active_strategy_configuration(session) -> StrategyConfigurationSnapshot:
    configuration = session.execute(
        select(StrategyConfiguration)
        .where(StrategyConfiguration.is_active.is_(True))
        .order_by(StrategyConfiguration.version.desc(), StrategyConfiguration.id.desc())
        .limit(1)
    ).scalar_one_or_none()

    if configuration is None:
        configuration = StrategyConfiguration(
            version=1,
            rps_threshold=DEFAULT_RPS_THRESHOLD,
            selected_rps_windows=serialize_selected_rps_windows(DEFAULT_SELECTED_RPS_WINDOWS),
            min_rps_lines_required=DEFAULT_MIN_RPS_LINES_REQUIRED,
            high_proximity_threshold_pct=DEFAULT_HIGH_PROXIMITY_THRESHOLD_PCT,
            is_active=True,
        )
        session.add(configuration)
        session.commit()
        session.refresh(configuration)

    return _serialize(configuration)


def save_strategy_configuration(
    session,
    *,
    rps_threshold: int,
    selected_rps_windows: list[int],
    min_rps_lines_required: int,
    high_proximity_threshold_pct: Decimal,
) -> StrategyConfigurationSnapshot:
    validated_rps_threshold = _validate_rps_threshold(rps_threshold)
    validated_selected_rps_windows = _validate_selected_rps_windows(selected_rps_windows)
    validated_min_rps_lines_required = _validate_min_rps_lines_required(
        min_rps_lines_required,
        selected_rps_windows=validated_selected_rps_windows,
    )
    validated_high_proximity_threshold_pct = _validate_high_proximity_threshold_pct(
        high_proximity_threshold_pct
    )

    current_configuration = session.execute(
        select(StrategyConfiguration)
        .where(StrategyConfiguration.is_active.is_(True))
        .order_by(StrategyConfiguration.version.desc(), StrategyConfiguration.id.desc())
        .limit(1)
    ).scalar_one_or_none()

    next_version = 1
    if current_configuration is not None:
        current_configuration.is_active = False
        next_version = current_configuration.version + 1

    configuration = StrategyConfiguration(
        version=next_version,
        rps_threshold=validated_rps_threshold,
        selected_rps_windows=serialize_selected_rps_windows(validated_selected_rps_windows),
        min_rps_lines_required=validated_min_rps_lines_required,
        high_proximity_threshold_pct=validated_high_proximity_threshold_pct,
        is_active=True,
    )
    session.add(configuration)
    session.commit()
    session.refresh(configuration)

    return _serialize(configuration)
