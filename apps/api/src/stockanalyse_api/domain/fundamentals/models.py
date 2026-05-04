from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import CheckConstraint, Date, ForeignKey, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from stockanalyse_api.db.base import Base, TimestampMixin

FUNDAMENTALS_DATA_STATUS_VALUES = ("complete", "partial", "missing")


class FundamentalsAnnual(TimestampMixin, Base):
    __tablename__ = "fundamentals_annual"
    __table_args__ = (
        UniqueConstraint(
            "instrument_id",
            "fiscal_year_end_date",
            name="uq_fundamentals_annual_instrument_id_fiscal_year_end_date",
        ),
        CheckConstraint(
            f"data_status IN {FUNDAMENTALS_DATA_STATUS_VALUES}",
            name="fundamentals_annual_data_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        index=True,
    )
    fiscal_year_end_date: Mapped[date] = mapped_column(Date, index=True)
    fiscal_year_label: Mapped[str] = mapped_column(String(32), nullable=False)
    net_income: Mapped[Decimal | None] = mapped_column(Numeric(20, 2), nullable=True)
    net_income_currency: Mapped[str] = mapped_column(String(8), nullable=False, default="JPY", server_default="JPY")
    operating_cash_flow: Mapped[Decimal | None] = mapped_column(Numeric(20, 2), nullable=True)
    free_cash_flow: Mapped[Decimal | None] = mapped_column(Numeric(20, 2), nullable=True)
    diluted_eps: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    stockholders_equity: Mapped[Decimal | None] = mapped_column(Numeric(20, 2), nullable=True)
    weighted_average_diluted_shares: Mapped[Decimal | None] = mapped_column(Numeric(20, 2), nullable=True)
    pe: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    pb: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    source_as_of_date: Mapped[date] = mapped_column(Date, nullable=False)
    data_status: Mapped[str] = mapped_column(String(16), nullable=False, default="complete", server_default="complete")

    instrument = relationship("Instrument", back_populates="fundamentals_annual")
