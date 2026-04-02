#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  echo "Starting with Docker..."
  echo "Open: http://localhost:8000"
  exec docker compose up --build
fi

echo "Docker is not available. Starting with local Python..."

if ! command -v python3 >/dev/null 2>&1; then
  echo "Error: python3 is not installed."
  exit 1
fi

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

NEED_INSTALL=0
if ! python3 -c 'import importlib.util,sys;mods=["fastapi","uvicorn","pandas","openpyxl","multipart","holidays"];sys.exit(0 if all(importlib.util.find_spec(m) for m in mods) else 1)'; then
  NEED_INSTALL=1
fi

if [ "$NEED_INSTALL" -eq 1 ]; then
  echo "Installing dependencies..."
  if ! python3 -m pip install --default-timeout=120 --retries 10 -r requirements.txt; then
    echo
    echo "Failed to install dependencies (network issue)."
    echo "Please check internet and run again."
    exit 1
  fi
else
  echo "Dependencies are already installed."
fi

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
echo "Open: http://localhost:${PORT}"

if [ "${DEV_RELOAD:-0}" = "1" ]; then
  exec python3 -m uvicorn app.main:app \
    --host "$HOST" \
    --port "$PORT" \
    --reload \
    --reload-dir app \
    --reload-dir static \
    --reload-exclude ".venv/*" \
    --reload-exclude "outputs/*"
fi

exec python3 -m uvicorn app.main:app --host "$HOST" --port "$PORT"
