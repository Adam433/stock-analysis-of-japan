from __future__ import annotations

import argparse
from pathlib import Path

from stockanalyse_api.services.ingestion.universe_manifest import (
    JPX_LISTED_ISSUES_PAGE_URL,
    build_tse_common_stock_symbols,
    download_file,
    fetch_latest_jpx_listed_issues_workbook_url,
    load_rows_from_table,
    write_symbol_manifest,
)

APP_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_PATH = APP_ROOT.parent.parent / "data/tse_common_stock_symbols.txt"
DEFAULT_DOWNLOAD_DIR = APP_ROOT.parent.parent / "data/reference"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync the TSE common-stock symbol universe from JPX listed-issues data.",
    )
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--workbook",
        help="Use an existing workbook/CSV instead of downloading from JPX.",
    )
    source_group.add_argument(
        "--page-url",
        default=JPX_LISTED_ISSUES_PAGE_URL,
        help="JPX page used to discover the latest listed-issues workbook link.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Destination symbol manifest path.",
    )
    parser.add_argument(
        "--download-dir",
        default=str(DEFAULT_DOWNLOAD_DIR),
        help="Directory used for storing downloaded JPX workbooks.",
    )
    return parser


def resolve_path(path_like: str) -> Path:
    path = Path(path_like)
    if path.is_absolute():
        return path
    return APP_ROOT.parent.parent / path


def main() -> None:
    args = build_parser().parse_args()
    output_path = resolve_path(args.output)

    if args.workbook:
        workbook_path = resolve_path(args.workbook)
    else:
        workbook_url = fetch_latest_jpx_listed_issues_workbook_url(args.page_url)
        destination = resolve_path(args.download_dir) / Path(workbook_url).name
        workbook_path = download_file(workbook_url, destination)

    rows = load_rows_from_table(workbook_path)
    symbols = build_tse_common_stock_symbols(rows)
    write_symbol_manifest(symbols, output_path)

    print(
        {
            "workbook_path": str(workbook_path),
            "output_path": str(output_path),
            "symbol_count": len(symbols),
        }
    )


if __name__ == "__main__":
    main()
