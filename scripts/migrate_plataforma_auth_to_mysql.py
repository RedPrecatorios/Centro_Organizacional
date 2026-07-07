#!/usr/bin/env python3
"""
Importa utilizadores/permissões do SQLite (instance/plataforma_auth.db) para MySQL.

Uso:
  python3 scripts/migrate_plataforma_auth_to_mysql.py
  python3 scripts/migrate_plataforma_auth_to_mysql.py --sqlite /caminho/plataforma_auth.db
  python3 scripts/migrate_plataforma_auth_to_mysql.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from messages_viewer.plataforma_auth_store import (  # noqa: E402
    MIGRATED_SQLITE_META_KEY,
    auth_connection,
    default_sqlite_path,
    import_from_sqlite,
    init_auth_schema,
    platform_meta_get,
    platform_meta_set,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrar auth SQLite → MySQL")
    parser.add_argument(
        "--sqlite",
        type=Path,
        default=None,
        help="Caminho do plataforma_auth.db (padrão: instance/ ou AUTH_SQLITE_PATH)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Só mostra o que seria importado, sem gravar no MySQL",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reimporta mesmo que já exista migração anterior",
    )
    args = parser.parse_args()

    sqlite_path = args.sqlite or default_sqlite_path()
    if not sqlite_path.is_file():
        print(f"ERRO: SQLite não encontrado: {sqlite_path}", file=sys.stderr)
        return 1

    with auth_connection() as conn:
        init_auth_schema(conn)
        if not args.force and platform_meta_get(MIGRATED_SQLITE_META_KEY, conn=conn) == "1":
            print(
                "Migração já executada. Use --force para reimportar "
                "(substitui todos os utilizadores no MySQL)."
            )
            return 0

        if args.dry_run:
            import sqlite3

            src = sqlite3.connect(str(sqlite_path))
            n_users = src.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            n_perms = src.execute("SELECT COUNT(*) FROM user_permissions").fetchone()[0]
            src.close()
            print(f"[dry-run] Origem: {sqlite_path}")
            print(f"[dry-run] Utilizadores: {n_users}, permissões: {n_perms}")
            return 0

        stats = import_from_sqlite(sqlite_path, conn)
        platform_meta_set(MIGRATED_SQLITE_META_KEY, "1", conn=conn)
        print(f"Importado de {sqlite_path}:")
        for k, v in stats.items():
            print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
