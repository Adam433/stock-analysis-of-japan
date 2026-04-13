from __future__ import annotations

from datetime import date

from sqlalchemy import Date, ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from stockanalyse_api.db.base import Base, TimestampMixin


class WatchlistEntry(TimestampMixin, Base):
    __tablename__ = "watchlist_entries"
    __table_args__ = (UniqueConstraint("instrument_id", name="uq_watchlist_entries_instrument_id"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    observation_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    added_date: Mapped[date] = mapped_column(Date, nullable=False)
