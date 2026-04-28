from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

# Ensure SQLAlchemy relationship targets are registered before job execution.
import stockanalyse_api.domain.indicators.models  # noqa: F401
import stockanalyse_api.domain.instruments.models  # noqa: F401
import stockanalyse_api.domain.market_data.models  # noqa: F401
from sqlalchemy import func
from sqlalchemy import select

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.factor_materialization import materialize_derived_indicator_facts


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize derived daily indicator facts.")
    parser.add_argument(
        "--exchange",
        action="append",
        choices=["TSE", "US"],
        default=None,
        help="Optional exchange scope. Repeat to process multiple exchanges.",
    )
    return parser.parse_args(argv)


def _count_trade_dates(exchanges: tuple[str, ...] | None) -> int:
    with SessionLocal() as session:
        query = select(func.count(func.distinct(MarketDataDaily.trade_date)))
        if exchanges is not None:
            query = query.where(
                MarketDataDaily.instrument_id.in_(
                    select(Instrument.id).where(Instrument.exchange.in_(exchanges))
                )
            )
        return int(session.execute(query).scalar_one())


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv or [])
    exchanges = tuple(args.exchange) if args.exchange else None
    total_trade_dates = _count_trade_dates(exchanges)

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
        result = materialize_derived_indicator_facts(
            session,
            progress_callback=report_progress,
            exchanges=exchanges,
        )

    print(result)


if __name__ == "__main__":
    main(sys.argv[1:])
