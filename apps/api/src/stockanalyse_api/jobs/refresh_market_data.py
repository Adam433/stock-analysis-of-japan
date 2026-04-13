from __future__ import annotations

import argparse
from pathlib import Path

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.ingestion.providers.static_provider import StaticFixtureProvider
from stockanalyse_api.services.ingestion.refresh_service import refresh_market_data


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh Japan equity EOD market data.")
    parser.add_argument("--provider", choices=["static_fixture"], default="static_fixture")
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument(
        "--fixture",
        default="tests/fixtures/japan_equity_eod_fixture.json",
        help="Fixture path for static provider validation.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.provider == "static_fixture":
        provider = StaticFixtureProvider(Path(args.fixture))
    else:
        raise ValueError(f"Unsupported provider: {args.provider}")

    with SessionLocal() as session:
        result = refresh_market_data(session, provider, args.symbols)

    print(result)


if __name__ == "__main__":
    main()
