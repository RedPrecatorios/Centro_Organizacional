#!/usr/bin/env bash
set -euo pipefail
ROOT="/opt/levantamento-processual"
cd "$ROOT"

export DISPLAY="${DISPLAY:-:99}"
export PYTHONUNBUFFERED=1

if ! pgrep -f "Xvfb ${DISPLAY}" >/dev/null 2>&1; then
  rm -f "/tmp/.X${DISPLAY#:}-lock" "/tmp/.X11-unix/X${DISPLAY#:}" 2>/dev/null || true
  Xvfb "$DISPLAY" -screen 0 1920x1080x24 -ac +extension GLX +render -noreset \
    >/tmp/xvfb-levantamento.log 2>&1 &
  sleep 1
fi

exec "$ROOT/.venv/bin/uvicorn" api.app:app --host 127.0.0.1 --port 8003 --workers 1
