from __future__ import annotations

import argparse
from pathlib import Path

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.ingestion.providers.registry import build_ingestion_provider
from stockanalyse_api.services.ingestion.refresh_service import DEFAULT_REFRESH_COMMIT_EVERY
from stockanalyse_api.services.ingestion.refresh_service import DEFAULT_UNIVERSE_FILTER
from stockanalyse_api.services.ingestion.refresh_service import execute_market_data_refresh

APP_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_FIXTURE_PATH = Path("tests/fixtures/japan_equity_eod_fixture.json")
DEFAULT_CSV_DIR = Path("data/archive/local_seed_csv")
DEFAULT_SYMBOLS_FILE = Path("data/tse_common_stock_symbols.txt")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh Japan equity EOD market data.")
    parser.add_argument(
        "--provider",
        choices=["static_fixture", "local_csv_directory", "yahoo_finance_chart"],
        default="static_fixture",
    )
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--symbols", nargs="+")
    target_group.add_argument(
        "--all-supported",
        action="store_true",
        help="Refresh the full symbol universe supported by the configured provider.",
    )
    parser.add_argument(
        "--fixture",
        default=str(DEFAULT_FIXTURE_PATH),
        help="Fixture path for static provider validation.",
    )
    parser.add_argument(
        "--csv-dir",
        default=str(DEFAULT_CSV_DIR),
        help="CSV directory for the local_csv_directory provider.",
    )
    parser.add_argument(
        "--symbols-file",
        default=str(DEFAULT_SYMBOLS_FILE),
        help="Allowed TSE common-stock symbol list for local_csv_directory full-universe refreshes.",
    )
    parser.add_argument(
        "--universe-filter",
        choices=["tse_common_stock"],
        default=DEFAULT_UNIVERSE_FILTER,
        help="Universe filter applied when --all-supported is used.",
    )
    parser.add_argument(
        "--commit-every",
        type=int,
        default=DEFAULT_REFRESH_COMMIT_EVERY,
        help="Commit market-data writes every N processed bars.",
    )
    return parser


def resolve_fixture_path(fixture: str) -> Path:
    path = Path(fixture)
    if path.is_absolute() or path.exists():
        return path
    return APP_ROOT / path


def resolve_app_path(path_like: str) -> Path:
    path = Path(path_like)
    if path.is_absolute() or path.exists():
        return path
    return APP_ROOT.parent.parent / path


def main() -> None:
    args = build_parser().parse_args()

    provider = build_ingestion_provider(
        args.provider,
        fixture_path=resolve_fixture_path(args.fixture),
        csv_dir=resolve_app_path(args.csv_dir),
        symbols_file=resolve_app_path(args.symbols_file),
    )

    with SessionLocal() as session:
        result = execute_market_data_refresh(
            session,
            provider,
            args.symbols,
            all_supported=args.all_supported,
            universe_filter=args.universe_filter,
            commit_every=args.commit_every,
        )

    print(result)


if __name__ == "__main__":
    main()
