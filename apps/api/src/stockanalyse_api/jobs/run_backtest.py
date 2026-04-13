from __future__ import annotations

import argparse
from datetime import date

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.backtesting import launch_backtest_run


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch a persisted backtest run.")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    with SessionLocal() as session:
        result = launch_backtest_run(
            session,
            start_date=date.fromisoformat(args.start_date),
            end_date=date.fromisoformat(args.end_date),
        )

    print(result.to_dict())


if __name__ == "__main__":
    main()
