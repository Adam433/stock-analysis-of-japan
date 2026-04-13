from __future__ import annotations

import argparse
from pathlib import Path

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.ingestion.providers.registry import build_ingestion_provider
from stockanalyse_api.services.ingestion.refresh_service import execute_market_data_refresh

APP_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_FIXTURE_PATH = Path("tests/fixtures/japan_equity_eod_fixture.json")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh Japan equity EOD market data.")
    parser.add_argument("--provider", choices=["static_fixture"], default="static_fixture")
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument(
        "--fixture",
        default=str(DEFAULT_FIXTURE_PATH),
        help="Fixture path for static provider validation.",
    )
    return parser


def resolve_fixture_path(fixture: str) -> Path:
    path = Path(fixture)
    if path.is_absolute() or path.exists():
        return path
    return APP_ROOT / path


def main() -> None:
    args = build_parser().parse_args()

    provider = build_ingestion_provider(
        args.provider,
        fixture_path=resolve_fixture_path(args.fixture),
    )

    with SessionLocal() as session:
        result = execute_market_data_refresh(session, provider, args.symbols)

    print(result)


if __name__ == "__main__":
    main()
