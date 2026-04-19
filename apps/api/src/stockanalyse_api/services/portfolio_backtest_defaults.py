"""MVP defaults for portfolio-return backtests.

The only normative source for these values is
_bmad-output/planning-artifacts/portfolio-backtest-anchor.md.
"""

from __future__ import annotations

from decimal import Decimal

MVP_HOLDING_DAYS = 20
MVP_STOP_LOSS_PCT = Decimal("-0.08")
MVP_PORTFOLIO_CAP = 20
MVP_ENTRY_DEFERRAL_WINDOW_DAYS = 5
MVP_PORTFOLIO_VALUE = Decimal("1.0")


def get_portfolio_backtest_defaults() -> dict[str, int | float]:
    return {
        "holding_days": MVP_HOLDING_DAYS,
        "stop_loss_pct": float(MVP_STOP_LOSS_PCT),
        "portfolio_cap": MVP_PORTFOLIO_CAP,
        "entry_deferral_window_days": MVP_ENTRY_DEFERRAL_WINDOW_DAYS,
    }
