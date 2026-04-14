#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)"
API_DIR="$ROOT_DIR/apps/api"

: "${PYTHON_BIN:=python3}"
: "${REFRESH_PROVIDER:=yahoo_finance_chart}"
: "${CSV_DIR:=$ROOT_DIR/data/archive/local_seed_csv}"
: "${SYMBOLS_FILE:=$ROOT_DIR/data/tse_common_stock_symbols.txt}"
: "${REFRESH_COMMIT_EVERY:=100}"

cd "$API_DIR"

PYTHONPATH=src "$PYTHON_BIN" -m stockanalyse_api.jobs.sync_tse_common_stock_universe \
  --output "$SYMBOLS_FILE"

if [ "$REFRESH_PROVIDER" = "local_csv_directory" ]; then
  PYTHONPATH=src "$PYTHON_BIN" -m stockanalyse_api.jobs.refresh_market_data \
    --provider "$REFRESH_PROVIDER" \
    --all-supported \
    --csv-dir "$CSV_DIR" \
    --symbols-file "$SYMBOLS_FILE" \
    --commit-every "$REFRESH_COMMIT_EVERY"
else
  PYTHONPATH=src "$PYTHON_BIN" -m stockanalyse_api.jobs.refresh_market_data \
    --provider "$REFRESH_PROVIDER" \
    --all-supported \
    --symbols-file "$SYMBOLS_FILE" \
    --commit-every "$REFRESH_COMMIT_EVERY"
fi

PYTHONPATH=src "$PYTHON_BIN" -m stockanalyse_api.jobs.materialize_derived_facts
