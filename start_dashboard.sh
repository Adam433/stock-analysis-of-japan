#!/usr/bin/env zsh
set -eu

ROOT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
API_DIR="$ROOT_DIR/apps/api"
WEB_DIR="$ROOT_DIR/apps/web"

: "${PYTHON_BIN:=python3}"
: "${STOCKANALYSE_HOST:=127.0.0.1}"
: "${STOCKANALYSE_PORT:=8000}"
: "${STOCKANALYSE_RELOAD:=0}"
: "${STOCKANALYSE_DB_PATH:=$ROOT_DIR/data/stockanalyse.db}"
: "${STOCKANALYSE_AUTO_REFRESH_ENABLED:=0}"
: "${STOCKANALYSE_WEB_HOST:=127.0.0.1}"
: "${STOCKANALYSE_WEB_PORT:=3000}"
: "${STOCKANALYSE_API_BASE_URL:=http://${STOCKANALYSE_HOST}:${STOCKANALYSE_PORT}}"
: "${STOCKANALYSE_START_API:=1}"
: "${STOCKANALYSE_START_WEB:=1}"

export STOCKANALYSE_DB_PATH
export STOCKANALYSE_AUTO_REFRESH_ENABLED
export STOCKANALYSE_API_BASE_URL

PIDS=()
PID_LABELS=()

cleanup() {
  local pid
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  wait >/dev/null 2>&1 || true
}

trap cleanup INT TERM EXIT

UVICORN_ARGS=(stockanalyse_api.main:app --host "$STOCKANALYSE_HOST" --port "$STOCKANALYSE_PORT")
if [ "$STOCKANALYSE_RELOAD" = "1" ]; then
  UVICORN_ARGS+=(--reload --reload-dir src)
fi

start_api() {
  echo "[stockAnalyse] API URL: http://${STOCKANALYSE_HOST}:${STOCKANALYSE_PORT}"
  echo "[stockAnalyse] API dashboard URL: http://${STOCKANALYSE_HOST}:${STOCKANALYSE_PORT}/dashboard"
  echo "[stockAnalyse] PYTHON_BIN=$PYTHON_BIN  RELOAD=$STOCKANALYSE_RELOAD"
  echo "[stockAnalyse] DB_PATH=$STOCKANALYSE_DB_PATH  AUTO_REFRESH=$STOCKANALYSE_AUTO_REFRESH_ENABLED"
  (
    cd "$API_DIR"
    PYTHONPATH="$API_DIR/src" exec "$PYTHON_BIN" -m uvicorn "${UVICORN_ARGS[@]}"
  ) &
  PIDS+=("$!")
  PID_LABELS+=("api")
}

start_web() {
  if ! command -v npm >/dev/null 2>&1; then
    echo "[stockAnalyse] npm is required to start the web dashboard." >&2
    exit 1
  fi

  echo "[stockAnalyse] Web dashboard URL: http://${STOCKANALYSE_WEB_HOST}:${STOCKANALYSE_WEB_PORT}"
  echo "[stockAnalyse] Web API base: $STOCKANALYSE_API_BASE_URL"
  (
    cd "$ROOT_DIR"
    exec npm --prefix "$WEB_DIR" run dev -- \
      --hostname "$STOCKANALYSE_WEB_HOST" \
      --port "$STOCKANALYSE_WEB_PORT"
  ) &
  PIDS+=("$!")
  PID_LABELS+=("web")
}

if [ "$STOCKANALYSE_START_API" = "1" ]; then
  start_api
fi

if [ "$STOCKANALYSE_START_WEB" = "1" ]; then
  start_web
fi

if [ "${#PIDS[@]}" -eq 0 ]; then
  echo "[stockAnalyse] nothing to start: STOCKANALYSE_START_API=0 and STOCKANALYSE_START_WEB=0" >&2
  exit 1
fi

echo "[stockAnalyse] started ${#PIDS[@]} process(es). Press Ctrl+C to stop."

while true; do
  for (( index = 1; index <= ${#PIDS[@]}; index++ )); do
    pid="${PIDS[$index]}"
    label="${PID_LABELS[$index]}"
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      exit_code=1
      wait "$pid" || exit_code="$?"
      echo "[stockAnalyse] $label process exited."
      exit "$exit_code"
    fi
  done
  sleep 2
done
