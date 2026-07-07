# -*- coding: utf-8 -*-
"""
Persistência da autenticação da plataforma em MySQL (plataforma_central).

Substitui o SQLite em ``instance/plataforma_auth.db`` — utilizadores, permissões,
meta e manutenção de módulos ficam no mesmo servidor que o EDA.
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursorDict

MIGRATED_SQLITE_META_KEY = "migrated_auth_sqlite_to_mysql_v1"


def auth_mysql_connect_kwargs() -> dict[str, Any]:
    host = (os.getenv("EDA_MYSQL_HOST") or "localhost").strip()
    port = int(os.getenv("EDA_MYSQL_PORT", "3306") or "3306")
    user = (os.getenv("EDA_MYSQL_USER") or "root").strip()
    password = os.getenv("EDA_MYSQL_PASSWORD", "") or ""
    database = (os.getenv("EDA_MYSQL_DATABASE") or "plataforma_central").strip()
    timeout = int(os.getenv("EDA_MYSQL_CONNECT_TIMEOUT", "15") or "15")
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "connection_timeout": timeout,
        "charset": "utf8mb4",
        "collation": "utf8mb4_unicode_ci",
        "autocommit": False,
    }


def default_sqlite_path() -> Path:
    raw = (os.getenv("AUTH_SQLITE_PATH") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    root = Path(__file__).resolve().parent.parent
    return root / "instance" / "plataforma_auth.db"


@contextmanager
def auth_connection() -> Iterator[MySQLConnection]:
    conn = mysql.connector.connect(**auth_mysql_connect_kwargs())
    try:
        yield conn
    finally:
        conn.close()


def auth_cursor(conn: MySQLConnection) -> MySQLCursorDict:
    return conn.cursor(dictionary=True)


def init_auth_schema(conn: MySQLConnection | None = None) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS plataforma_users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(100) NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            role ENUM('admin', 'colaborador') NOT NULL,
            active TINYINT(1) NOT NULL DEFAULT 1,
            perms_version INT NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_plataforma_users_username (username)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS plataforma_user_permissions (
            user_id INT NOT NULL,
            tab_id VARCHAR(64) NOT NULL,
            PRIMARY KEY (user_id, tab_id),
            CONSTRAINT fk_plataforma_user_permissions_user
                FOREIGN KEY (user_id) REFERENCES plataforma_users(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS plataforma_meta (
            meta_key VARCHAR(128) PRIMARY KEY,
            meta_value MEDIUMTEXT NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS plataforma_page_maintenance (
            tab_id VARCHAR(64) PRIMARY KEY,
            enabled TINYINT(1) NOT NULL DEFAULT 0,
            message TEXT NULL,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
    ]
    if conn is None:
        with auth_connection() as c:
            init_auth_schema(c)
        return
    cur = auth_cursor(conn)
    for stmt in statements:
        cur.execute(stmt)
    conn.commit()


def platform_meta_get(key: str, default: str = "", conn: MySQLConnection | None = None) -> str:
    def _read(c: MySQLConnection) -> str:
        cur = auth_cursor(c)
        cur.execute(
            "SELECT meta_value FROM plataforma_meta WHERE meta_key = %s",
            (key,),
        )
        row = cur.fetchone()
        if not row:
            return default
        return str(row["meta_value"] or "")

    if conn is not None:
        return _read(conn)
    with auth_connection() as c:
        return _read(c)


def platform_meta_set(key: str, value: str, conn: MySQLConnection | None = None) -> None:
    def _write(c: MySQLConnection) -> None:
        cur = auth_cursor(c)
        cur.execute(
            """
            INSERT INTO plataforma_meta (meta_key, meta_value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE meta_value = VALUES(meta_value)
            """,
            (key, value),
        )
        c.commit()

    if conn is not None:
        _write(conn)
        return
    with auth_connection() as c:
        _write(c)


def _sqlite_table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def import_from_sqlite(sqlite_path: Path, conn: MySQLConnection) -> dict[str, int]:
    """Importa utilizadores, permissões, meta e manutenção de um ficheiro SQLite."""
    if not sqlite_path.is_file():
        raise FileNotFoundError(str(sqlite_path))

    src = sqlite3.connect(str(sqlite_path))
    src.row_factory = sqlite3.Row
    cur = auth_cursor(conn)

    stats = {"users": 0, "permissions": 0, "meta": 0, "maintenance": 0}

    cur.execute("SET FOREIGN_KEY_CHECKS = 0")
    cur.execute("DELETE FROM plataforma_user_permissions")
    cur.execute("DELETE FROM plataforma_users")
    cur.execute("DELETE FROM plataforma_meta")
    cur.execute("DELETE FROM plataforma_page_maintenance")
    cur.execute("SET FOREIGN_KEY_CHECKS = 1")

    max_id = 0
    for row in src.execute(
        "SELECT id, username, password_hash, role, active, perms_version, created_at "
        "FROM users ORDER BY id"
    ):
        cur.execute(
            """
            INSERT INTO plataforma_users
                (id, username, password_hash, role, active, perms_version, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(row["id"]),
                str(row["username"]),
                str(row["password_hash"]),
                str(row["role"]),
                int(row["active"] or 0),
                int(row["perms_version"] or 0),
                str(row["created_at"]),
            ),
        )
        max_id = max(max_id, int(row["id"]))
        stats["users"] += 1

    if max_id:
        cur.execute("ALTER TABLE plataforma_users AUTO_INCREMENT = %s", (max_id + 1,))

    if _sqlite_table_exists(src, "user_permissions"):
        for row in src.execute(
            "SELECT user_id, tab_id FROM user_permissions ORDER BY user_id, tab_id"
        ):
            cur.execute(
                """
                INSERT IGNORE INTO plataforma_user_permissions (user_id, tab_id)
                VALUES (%s, %s)
                """,
                (int(row["user_id"]), str(row["tab_id"])),
            )
            stats["permissions"] += 1

    if _sqlite_table_exists(src, "platform_meta"):
        for row in src.execute("SELECT key, value FROM platform_meta ORDER BY key"):
            cur.execute(
                """
                INSERT INTO plataforma_meta (meta_key, meta_value)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE meta_value = VALUES(meta_value)
                """,
                (str(row["key"]), str(row["value"])),
            )
            stats["meta"] += 1

    if _sqlite_table_exists(src, "page_maintenance"):
        for row in src.execute(
            "SELECT tab_id, enabled, message, updated_at FROM page_maintenance ORDER BY tab_id"
        ):
            cur.execute(
                """
                INSERT INTO plataforma_page_maintenance (tab_id, enabled, message, updated_at)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    enabled = VALUES(enabled),
                    message = VALUES(message),
                    updated_at = VALUES(updated_at)
                """,
                (
                    str(row["tab_id"]),
                    int(row["enabled"] or 0),
                    row["message"],
                    str(row["updated_at"]),
                ),
            )
            stats["maintenance"] += 1

    src.close()
    conn.commit()
    return stats


def migrate_sqlite_to_mysql_if_needed() -> bool:
    """
    Importa uma vez do SQLite local se existir e o MySQL ainda não foi migrado.
    Retorna True se importou dados.
    """
    sqlite_path = default_sqlite_path()
    with auth_connection() as conn:
        init_auth_schema(conn)
        if platform_meta_get(MIGRATED_SQLITE_META_KEY, conn=conn) == "1":
            return False
        if not sqlite_path.is_file():
            platform_meta_set(MIGRATED_SQLITE_META_KEY, "1", conn=conn)
            return False
        import_from_sqlite(sqlite_path, conn)
        platform_meta_set(MIGRATED_SQLITE_META_KEY, "1", conn=conn)
        print(
            f"[plataforma_auth] Migrados utilizadores do SQLite → MySQL "
            f"({sqlite_path})"
        )
        return True
