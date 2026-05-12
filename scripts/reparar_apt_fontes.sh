#!/usr/bin/env bash
# Corrige falhas comuns de apt em servidores antigos (Ubuntu Kinetic EOL, Chrome 404).
# Uso no droplet:  sudo bash scripts/reparar_apt_fontes.sh
# Faz cópia de segurança em /root/apt-backup-<data>/ antes de alterar.
set -euo pipefail

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "Execute como root:  sudo bash $0"
  exit 1
fi

Bak="/root/apt-backup-$(date +%Y%m%d%H%M%S)"
mkdir -p "$Bak"
[[ -f /etc/apt/sources.list ]] && cp -a /etc/apt/sources.list "$Bak/"
[[ -d /etc/apt/sources.list.d ]] && cp -a /etc/apt/sources.list.d "$Bak/"
echo "Cópia de segurança: $Bak"

comment_kinetic_in_file() {
  local f="$1"
  [[ -f "$f" ]] || return 0
  if grep -i kinetic "$f" | grep -qv '^[[:space:]]*#'; then
    # Comenta linhas ativas que mencionam kinetic (repositórios EOL)
    sed -i.bak-kinetic-eol "s/^\([^#]*kinetic[^#]*\)$/# EOL-kinetic &/" "$f"
    echo "  Atualizado (kinetic comentado): $f"
  fi
}

# PPA deadsnakes: trocar kinetic -> jammy *antes* de comentar linhas com "kinetic"
shopt -s nullglob
for f in /etc/apt/sources.list.d/*deadsnakes*.list; do
  [[ -f "$f" ]] || continue
  if grep -qi kinetic "$f"; then
    sed -i.bak-deadsnakes 's/\bkinetic\b/jammy/g' "$f"
    echo "  deadsnakes: kinetic -> jammy em $f"
  fi
done

comment_kinetic_in_file /etc/apt/sources.list
for f in /etc/apt/sources.list.d/*.list; do
  comment_kinetic_in_file "$f"
done

# Repositório Google Chrome: desativar listas que quebram upgrade (404 / GPG)
for f in /etc/apt/sources.list.d/*google*chrome*.list; do
  [[ -f "$f" ]] || continue
  mv "$f" "${f}.disabled"
  echo "  Chrome APT desativado (reconfigurar com a doc Google se precisar): ${f}.disabled"
done
shopt -u nullglob

export DEBIAN_FRONTEND=noninteractive
apt-get clean || true
apt-get update -o Acquire::AllowInsecureRepositories=false || true

if apt-get install -y --reinstall python3-apt command-not-found 2>/dev/null; then
  echo "  python3-apt e command-not-found reinstalados."
else
  echo "  Aviso: não foi possível reinstalar python3-apt (corra apt-get update manualmente)."
fi

echo ""
echo "Próximo passo:  sudo apt-get upgrade -y"
echo "Se ainda houver erros, reveja duplicados em /etc/apt/sources.list (linhas repetidas)."
