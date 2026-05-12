#!/usr/bin/env bash
# Recria .venv com um Python que tenha sqlite3 (evita ModuleNotFoundError: _sqlite3
# quando /usr/bin/python3 aponta para um 3.11 compilado sem SQLite).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
REQ="${ROOT}/requirements.txt"

if [[ ! -f "$REQ" ]]; then
  echo "ERRO: requirements.txt não encontrado em $ROOT"
  exit 1
fi

SELECTED=""
for exe in /usr/bin/python3.10 /usr/bin/python3.12 /usr/bin/python3.11 /usr/bin/python3; do
  [[ -x "$exe" ]] || continue
  if "$exe" -c "import sqlite3" 2>/dev/null; then
    SELECTED="$exe"
    break
  fi
done

if [[ -z "$SELECTED" ]]; then
  echo "ERRO: Nenhum interpretador com sqlite3 encontrado."
  echo "Tente: sudo apt-get install -y python3 python3-venv libsqlite3-0"
  exit 1
fi

echo "A usar: $SELECTED — $($SELECTED -V)"

if [[ -d .venv ]]; then
  echo "A remover .venv existente…"
  rm -rf .venv
fi

"$SELECTED" -m venv .venv
.venv/bin/pip install -U pip setuptools wheel
.venv/bin/pip install -r "$REQ"

echo ""
echo "Concluído. Ative o ambiente:"
echo "  source .venv/bin/activate"
echo "Teste:"
echo "  python3 -c \"import sqlite3; print('sqlite3 OK')\""
