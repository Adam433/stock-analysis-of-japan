from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import CheckConstraint, Date, DateTime, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from stockanalyse_api.db.base import Base, TimestampMixin

BACKTEST_RUN_STATUS_VALUES = ("running", "completed", "failed")


class BacktestRun(TimestampMixin, Base):
    __tablename__ = "backtest_runs"
    __table_args__ = (
        CheckConstraint(
            f"status IN {BACKTEST_RUN_STATUS_VALUES}",
            name="backtest_runs_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    strategy_configuration_id: Mapped[int] = mapped_column(
        ForeignKey("strategy_configurations.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    start_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    end_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running", server_default="running")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
