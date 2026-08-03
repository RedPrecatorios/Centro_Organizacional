#!/usr/bin/env bash
# Start Levantamento API (local or remote cloud).
#
# Env (from .env or systemd):
#   API_HOST   default 127.0.0.1  — use 0.0.0.0 behind nginx/firewall only
#   API_PORT   default 8003
#   API_KEY or API_TOKEN  — required for /searches*
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

export DISPLAY="${DISPLAY:-:99}"
export PYTHONUNBUFFERED=1

API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8003}"

if [[ -z "${API_KEY:-}${API_TOKEN:-}" ]]; then
  echo "ERROR: defina API_KEY ou API_TOKEN no .env antes de subir a API." >&2
  exit 1
fi

if [[ "$API_HOST" == "0.0.0.0" ]]; then
  echo "WARN: API escutando em 0.0.0.0 — use firewall + nginx HTTPS + API_ALLOWED_IPS." >&2
fi

if ! pgrep -f "Xvfb ${DISPLAY}" >/dev/null 2>&1; then
  rm -f "/tmp/.X${DISPLAY#:}-lock" "/tmp/.X11-unix/X${DISPLAY#:}" 2>/dev/null || true
  Xvfb "$DISPLAY" -screen 0 1920x1080x24 -ac +extension GLX +render -noreset \
    >/tmp/xvfb-levantamento.log 2>&1 &
  sleep 1
fi

VENV_PY="$ROOT/.venv/bin/uvicorn"
if [[ ! -x "$VENV_PY" ]]; then
  VENV_PY="$ROOT/venv/bin/uvicorn"
fi

exec "$VENV_PY" api.app:app --host "$API_HOST" --port "$API_PORT" --workers 1
