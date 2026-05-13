"""
Insere na MySQL o template dos ficheiros default.html / default.txt (mesmo do script CLI).

Uso (na raiz do projeto, com env EDA_MYSQL_* como na app):
  python -m campanha.seed_default_template
"""
from __future__ import annotations

import os

from campanha.api_templates import garantir_template_padrao_script


def main() -> None:
    db_config = {
        "host": (os.getenv("EDA_MYSQL_HOST") or "localhost").strip(),
        "port": int(os.getenv("EDA_MYSQL_PORT", "3306") or "3306"),
        "user": (os.getenv("EDA_MYSQL_USER") or "root").strip(),
        "password": os.getenv("EDA_MYSQL_PASSWORD", "") or "",
        "connection_timeout": int(os.getenv("EDA_MYSQL_CONNECT_TIMEOUT", "15") or "15"),
    }
    db_name = (os.getenv("EDA_MYSQL_DATABASE") or "plataforma_central").strip()
    r = garantir_template_padrao_script(db_config, db_name)
    print(r)


if __name__ == "__main__":
    main()
