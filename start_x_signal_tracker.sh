#!/usr/bin/env zsh
set -eu

ROOT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
API_DIR="$ROOT_DIR/apps/api"

: "${PYTHON_BIN:=python3}"
: "${STOCKANALYSE_HOST:=127.0.0.1}"
: "${STOCKANALYSE_PORT:=8000}"
: "${STOCKANALYSE_RELOAD:=0}"
: "${STOCKANALYSE_DB_PATH:=$ROOT_DIR/data/stockanalyse.db}"
: "${STOCKANALYSE_AUTO_REFRESH_ENABLED:=0}"
: "${STOCKANALYSE_X_SIGNAL_OPEN:=0}"

usage() {
  cat <<'EOF'
Usage: ./start_x_signal_tracker.sh [options]

Start the API-only X signal tracker page.

Options:
  --host HOST          API host. Default: 127.0.0.1
  --port PORT          API port. Default: 8000
  --db-path PATH       SQLite database path. Default: data/stockanalyse.db
  --python PATH        Python executable. Default: python3
  --reload             Enable uvicorn reload.
  --no-reload          Disable uvicorn reload.
  --auto-refresh       Enable stockAnalyse auto refresh runtime.
  --no-auto-refresh    Disable stockAnalyse auto refresh runtime.
  --open               Open the tracker page in the default browser after start.
  --no-open            Do not open a browser.
  -h, --help           Show this help.

Environment overrides:
  PYTHON_BIN
  STOCKANALYSE_HOST
  STOCKANALYSE_PORT
  STOCKANALYSE_DB_PATH
  STOCKANALYSE_RELOAD
  STOCKANALYSE_AUTO_REFRESH_ENABLED
  STOCKANALYSE_X_SIGNAL_OPEN
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --host)
      STOCKANALYSE_HOST="${2:?--host requires a value}"
      shift 2
      ;;
    --port)
      STOCKANALYSE_PORT="${2:?--port requires a value}"
      shift 2
      ;;
    --db-path)
      STOCKANALYSE_DB_PATH="${2:?--db-path requires a value}"
      shift 2
      ;;
    --python)
      PYTHON_BIN="${2:?--python requires a value}"
      shift 2
      ;;
    --reload)
      STOCKANALYSE_RELOAD=1
      shift
      ;;
    --no-reload)
      STOCKANALYSE_RELOAD=0
      shift
      ;;
    --auto-refresh)
      STOCKANALYSE_AUTO_REFRESH_ENABLED=1
      shift
      ;;
    --no-auto-refresh)
      STOCKANALYSE_AUTO_REFRESH_ENABLED=0
      shift
      ;;
    --open)
      STOCKANALYSE_X_SIGNAL_OPEN=1
      shift
      ;;
    --no-open)
      STOCKANALYSE_X_SIGNAL_OPEN=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[stockAnalyse] unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

export STOCKANALYSE_DB_PATH
export STOCKANALYSE_AUTO_REFRESH_ENABLED

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

TRACKER_URL="http://${STOCKANALYSE_HOST}:${STOCKANALYSE_PORT}/x-signals/tracker"
DASHBOARD_API_URL="http://${STOCKANALYSE_HOST}:${STOCKANALYSE_PORT}/x-signals/dashboard?limit=1000"

echo "[stockAnalyse] X signal tracker URL: $TRACKER_URL"
echo "[stockAnalyse] X signal dashboard API: $DASHBOARD_API_URL"
echo "[stockAnalyse] API URL: http://${STOCKANALYSE_HOST}:${STOCKANALYSE_PORT}"
echo "[stockAnalyse] PYTHON_BIN=$PYTHON_BIN  RELOAD=$STOCKANALYSE_RELOAD"
echo "[stockAnalyse] DB_PATH=$STOCKANALYSE_DB_PATH  AUTO_REFRESH=$STOCKANALYSE_AUTO_REFRESH_ENABLED"

(
  cd "$API_DIR"
  PYTHONPATH="$API_DIR/src" exec "$PYTHON_BIN" -m uvicorn "${UVICORN_ARGS[@]}"
) &
PIDS+=("$!")
PID_LABELS+=("api")

if [ "$STOCKANALYSE_X_SIGNAL_OPEN" = "1" ]; then
  if command -v open >/dev/null 2>&1; then
    open "$TRACKER_URL" >/dev/null 2>&1 || true
  else
    echo "[stockAnalyse] browser auto-open requested, but 'open' is not available." >&2
  fi
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
