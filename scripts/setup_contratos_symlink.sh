#!/usr/bin/env bash
# Symlink do motor PHP para paths hardcoded em /var/www/html/RedPrecatorios_Contratos
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${ROOT}/contratos_engine"
LINK="/var/www/html/RedPrecatorios_Contratos"

if [[ ! -d "${TARGET}" ]]; then
  echo "Motor não encontrado em ${TARGET}" >&2
  exit 1
fi

mkdir -p "$(dirname "${LINK}")"
ln -sfn "${TARGET}" "${LINK}"
echo "Symlink criado: ${LINK} -> ${TARGET}"
