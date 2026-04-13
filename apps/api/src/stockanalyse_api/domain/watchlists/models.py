from __future__ import annotations

from sqlalchemy import ForeignKey, UniqueConstraint
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
