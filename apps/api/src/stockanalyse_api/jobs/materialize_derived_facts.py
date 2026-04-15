from __future__ import annotations

# Ensure SQLAlchemy relationship targets are registered before job execution.
import stockanalyse_api.domain.indicators.models  # noqa: F401
import stockanalyse_api.domain.instruments.models  # noqa: F401
import stockanalyse_api.domain.market_data.models  # noqa: F401
from sqlalchemy import func
from sqlalchemy import select

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.factor_materialization import materialize_derived_indicator_facts


def main() -> None:
    with SessionLocal() as session:
        total_trade_dates = session.execute(
            select(func.count(func.distinct(MarketDataDaily.trade_date)))
        ).scalar_one()

    def report_progress(payload: dict[str, object]) -> None:
        processed = int(payload["processed_trade_dates"])
        trade_date = str(payload["trade_date"])
        inserted = int(payload["inserted"])
        updated = int(payload["updated"])
        final = bool(payload.get("final", False))
        suffix = " [final]" if final else ""
        print(
            f"[materialize] {processed}/{total_trade_dates} trade dates through {trade_date} | "
            f"inserted={inserted} updated={updated}{suffix}",
            flush=True,
        )

    with SessionLocal() as session:
        result = materialize_derived_indicator_facts(session, progress_callback=report_progress)

    print(result)


if __name__ == "__main__":
    main()
