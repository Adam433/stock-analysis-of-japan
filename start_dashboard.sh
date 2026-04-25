#!/usr/bin/env zsh
set -eu

ROOT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
API_DIR="$ROOT_DIR/apps/api"

: "${PYTHON_BIN:=python3}"
: "${STOCKANALYSE_HOST:=127.0.0.1}"
: "${STOCKANALYSE_PORT:=8000}"
: "${STOCKANALYSE_RELOAD:=1}"
: "${STOCKANALYSE_DB_PATH:=$ROOT_DIR/data/stockanalyse.db}"
: "${STOCKANALYSE_AUTO_REFRESH_ENABLED:=0}"

export STOCKANALYSE_DB_PATH
export STOCKANALYSE_AUTO_REFRESH_ENABLED

cd "$API_DIR"

UVICORN_ARGS=(stockanalyse_api.main:app --host "$STOCKANALYSE_HOST" --port "$STOCKANALYSE_PORT")
if [ "$STOCKANALYSE_RELOAD" = "1" ]; then
  UVICORN_ARGS+=(--reload --reload-dir src)
fi

echo "[stockAnalyse] dashboard URL: http://${STOCKANALYSE_HOST}:${STOCKANALYSE_PORT}/dashboard"
echo "[stockAnalyse] PYTHON_BIN=$PYTHON_BIN  RELOAD=$STOCKANALYSE_RELOAD"
echo "[stockAnalyse] DB_PATH=$STOCKANALYSE_DB_PATH  AUTO_REFRESH=$STOCKANALYSE_AUTO_REFRESH_ENABLED"

PYTHONPATH="$API_DIR/src" exec "$PYTHON_BIN" -m uvicorn "${UVICORN_ARGS[@]}"
