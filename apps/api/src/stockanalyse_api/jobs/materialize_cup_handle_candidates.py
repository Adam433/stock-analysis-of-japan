from __future__ import annotations

import argparse
from datetime import date

import stockanalyse_api.domain.backtests.models  # noqa: F401
import stockanalyse_api.domain.fundamentals.models  # noqa: F401
import stockanalyse_api.domain.instruments.models  # noqa: F401
import stockanalyse_api.domain.market_data.models  # noqa: F401
from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.cup_handle_materialization import materialize_cup_handle_candidates


def _parse_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize cup-handle candidate events.")
    parser.add_argument("--market", choices=("jp", "us"), default="us")
    parser.add_argument("--source-start-date")
    parser.add_argument("--source-end-date")
    parser.add_argument("--commit-every", type=int, default=100)
    args = parser.parse_args()

    with SessionLocal() as session:
        run = materialize_cup_handle_candidates(
            session,
            market=args.market,
            source_start_date=_parse_date(args.source_start_date),
            source_end_date=_parse_date(args.source_end_date),
            commit_every=args.commit_every,
        )
        print(
            f"cup_handle_materialization_run id={run.id} status={run.status} "
            f"market={run.market} symbols={run.symbols_processed} events={run.events_created} "
            f"error={run.error_message or ''}"
        )


if __name__ == "__main__":
    main()
