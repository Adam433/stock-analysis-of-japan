"""Portfolio-return backtest metrics.

Normative metric definitions are sourced from `_bmad-output/planning-artifacts/prd.md:448` (FR45).
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

RATIO_PATTERN = Decimal("0.000001")


def _coerce_decimal(value: object | None) -> Decimal | None:
    if value is None:
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _quantize_ratio(value: Decimal) -> Decimal:
    return value.quantize(RATIO_PATTERN, rounding=ROUND_HALF_UP)


def calculate_win_rate(per_security_returns: list[dict[str, object]]) -> Decimal:
    realized_returns = [
        realized_return
        for item in per_security_returns
        if (realized_return := _coerce_decimal(item.get("realized_return"))) is not None
    ]
    if not realized_returns:
        return Decimal("0")

    winning_positions = sum(1 for realized_return in realized_returns if realized_return > Decimal("0"))
    return _quantize_ratio(Decimal(winning_positions) / Decimal(len(realized_returns)))


def calculate_max_drawdown(equity_curve: list[dict[str, object]]) -> Decimal:
    equity_values = [
        equity
        for point in equity_curve
        if (equity := _coerce_decimal(point.get("equity"))) is not None
    ]
    if not equity_values:
        return Decimal("0")

    peak = equity_values[0]
    max_drawdown = Decimal("0")
    for equity in equity_values:
        if equity > peak:
            peak = equity
        if peak <= Decimal("0"):
            continue
        drawdown = (peak - equity) / peak
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    return _quantize_ratio(max_drawdown)
