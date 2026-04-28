from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from stockanalyse_api.config.settings import get_us_stock_symbols_path
from stockanalyse_api.services.ingestion.us_universe_manifest import (
    DEFAULT_OTHER_EXCHANGE_CODES,
    sync_us_common_stock_universe,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh the US common-stock universe from NASDAQ Trader."
    )
    parser.add_argument(
        "--output",
        default=str(get_us_stock_symbols_path()),
        help="Output path for the US common-stock symbol allowlist.",
    )
    parser.add_argument(
        "--other-exchange-code",
        action="append",
        dest="other_exchange_codes",
        default=None,
        help=(
            "Exchange code from otherlisted.txt to include. Defaults to N "
            "(NYSE). Repeat to include more, such as A for NYSE American."
        ),
    )
    parser.add_argument("--timeout", type=int, default=30)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    manifest = sync_us_common_stock_universe(
        Path(args.output),
        other_exchange_codes=tuple(args.other_exchange_codes or DEFAULT_OTHER_EXCHANGE_CODES),
        timeout_seconds=args.timeout,
    )
    print(manifest.to_dict())


if __name__ == "__main__":
    main()
