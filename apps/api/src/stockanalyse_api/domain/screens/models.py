from __future__ import annotations

from decimal import Decimal

from sqlalchemy import Boolean, Integer, Numeric
from sqlalchemy.orm import Mapped, mapped_column

from stockanalyse_api.db.base import Base, TimestampMixin


class StrategyConfiguration(TimestampMixin, Base):
    __tablename__ = "strategy_configurations"

    id: Mapped[int] = mapped_column(primary_key=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    rps_threshold: Mapped[int] = mapped_column(Integer, nullable=False)
    high_proximity_threshold_pct: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="1")
