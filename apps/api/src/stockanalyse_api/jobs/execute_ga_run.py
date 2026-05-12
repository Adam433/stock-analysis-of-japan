from __future__ import annotations

import argparse

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.genetic_optimizer import execute_ga_run, serialize_ga_run


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute or resume a persisted GA run.")
    parser.add_argument("run_id", type=int)
    args = parser.parse_args()

    with SessionLocal() as session:
        run = execute_ga_run(session, args.run_id)
        print(serialize_ga_run(run), flush=True)


if __name__ == "__main__":
    main()
