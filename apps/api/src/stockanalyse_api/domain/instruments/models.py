from __future__ import annotations

from sqlalchemy import Boolean, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from stockanalyse_api.db.base import Base, TimestampMixin


class Instrument(TimestampMixin, Base):
    __tablename__ = "instruments"
    __table_args__ = (
        UniqueConstraint("symbol", "exchange", name="uq_instruments_symbol_exchange"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    exchange: Mapped[str] = mapped_column(String(16))
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    currency: Mapped[str] = mapped_column(String(8), default="JPY", server_default="JPY")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="1")

    daily_market_data = relationship(
        "MarketDataDaily",
        back_populates="instrument",
        cascade="all, delete-orphan",
    )
