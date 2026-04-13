from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import Date, ForeignKey, Numeric, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from stockanalyse_api.db.base import Base, TimestampMixin


class DerivedIndicatorDaily(TimestampMixin, Base):
    __tablename__ = "derived_indicator_daily"
    __table_args__ = (
        UniqueConstraint(
            "instrument_id",
            "trade_date",
            name="uq_derived_indicator_daily_instrument_id_trade_date",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        index=True,
    )
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    rps_50: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    rps_120: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    rps_250: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    fifty_two_week_high: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    high_proximity_ratio: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
