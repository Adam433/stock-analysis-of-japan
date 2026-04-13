from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import BigInteger, CheckConstraint, Date, ForeignKey, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from stockanalyse_api.db.base import Base, TimestampMixin

DATA_STATUS_VALUES = ("complete", "partial", "unavailable")


class MarketDataDaily(TimestampMixin, Base):
    __tablename__ = "market_data_daily"
    __table_args__ = (
        UniqueConstraint(
            "instrument_id",
            "trade_date",
            name="uq_market_data_daily_instrument_id_trade_date",
        ),
        CheckConstraint(
            f"data_status IN {DATA_STATUS_VALUES}",
            name="market_data_daily_data_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        index=True,
    )
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    open: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    high: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    low: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    close: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    adj_close: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    volume: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    data_source: Mapped[str] = mapped_column(String(32), default="unknown", server_default="unknown")
    data_status: Mapped[str] = mapped_column(String(16), default="complete", server_default="complete")

    instrument = relationship("Instrument", back_populates="daily_market_data")
