from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import hashlib
import os
import secrets
import string
import unicodedata

import pymysql
from pymysql.err import IntegrityError


DB_NAME = "plataforma_central"
TABLE = "plataforma_users"
ROLE = "colaborador"


def _env(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip().strip('"').strip("'")
    return ""


def _settings() -> dict:
    host = _env("PLATAFORMA_DB_HOST")
    user = _env("PLATAFORMA_DB_USER")
    password = _env("PLATAFORMA_DB_PASSWORD", "PLATAFORMA_DB_PASS")
    if not host or not user or not password:
        raise RuntimeError(
            "Banco da Plataforma Central ausente. Defina PLATAFORMA_DB_HOST, "
            "PLATAFORMA_DB_USER e PLATAFORMA_DB_PASSWORD no arquivo .env"
        )
    port = int(_env("PLATAFORMA_DB_PORT") or "3306")
    database = _env("PLATAFORMA_DB_NAME") or DB_NAME
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "charset": "utf8mb4",
        "connect_timeout": 15,
        "cursorclass": pymysql.cursors.DictCursor,
    }


@contextmanager
def _connect():
    conn = pymysql.connect(**_settings())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _token(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    stripped = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return "".join(ch for ch in stripped if ch.isalnum())


def username_for(first_name: str, last_name: str) -> str:
    first = _token(first_name)
    last = _token(last_name)
    base = f"{first}{last}"[:100]
    if not base:
        raise RuntimeError("Não foi possível montar o usuário da Plataforma Central.")
    return base


def initial_password(first_name: str, year: int | None = None) -> str:
    name = (first_name or "").strip()
    if not name:
        raise RuntimeError("Nome é obrigatório para a senha da Plataforma Central.")
    return f"{name}@{year or datetime.now().year}"


def _hash_password(password: str) -> str:
    salt = "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))
    digest = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt.encode("utf-8"),
        n=32768,
        r=8,
        p=1,
        dklen=64,
        maxmem=64 * 1024 * 1024,
    ).hex()
    return f"scrypt:32768:8:1${salt}${digest}"


def _row_out(row: dict, *, created: bool, existed: bool, password: str = "") -> dict:
    return {
        "id": int(row.get("id") or 0),
        "username": str(row.get("username") or ""),
        "email": str(row.get("email") or ""),
        "created": created,
        "existed": existed,
        "password": password if created else "",
        "warning": "",
    }


def account_info() -> dict:
    cfg = _settings()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS total FROM `{TABLE}`")
            total = int((cur.fetchone() or {}).get("total") or 0)
    return {
        "host": cfg["host"],
        "database": cfg["database"],
        "users": total,
    }


def provision_user(email: str, first_name: str, last_name: str) -> dict:
    email = (email or "").strip()
    first_name = (first_name or "").strip()
    last_name = (last_name or "").strip()
    if not first_name or not last_name:
        raise RuntimeError("Nome e sobrenome são obrigatórios.")
    if "@" not in email:
        raise RuntimeError("Informe um e-mail válido para a Plataforma Central.")

    wanted_email = email.casefold()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, username, email, first_name, last_name FROM `{TABLE}` WHERE LOWER(email) = %s",
                (wanted_email,),
            )
            existing = cur.fetchone()
            if existing:
                return _row_out(existing, created=False, existed=True)

            base = username_for(first_name, last_name)
            username = base
            suffix = 2
            while True:
                cur.execute(
                    f"SELECT id, username, email FROM `{TABLE}` WHERE username = %s",
                    (username,),
                )
                taken = cur.fetchone()
                if not taken:
                    break
                if str(taken.get("email") or "").strip().casefold() == wanted_email:
                    return _row_out(taken, created=False, existed=True)
                extra = str(suffix)
                username = f"{base[: 100 - len(extra)]}{extra}"
                suffix += 1
                if suffix > 99:
                    raise RuntimeError("Não há login livre na Plataforma Central para esse nome.")

            password = initial_password(first_name)
            try:
                cur.execute(
                    f"""
                    INSERT INTO `{TABLE}`
                    (username, password_hash, role, active, perms_version, first_name, last_name, email)
                    VALUES (%s, %s, %s, 1, 0, %s, %s, %s)
                    """,
                    (username, _hash_password(password), ROLE, first_name, last_name, email),
                )
            except IntegrityError as exc:
                conn.rollback()
                cur.execute(
                    f"SELECT id, username, email, first_name, last_name FROM `{TABLE}` WHERE LOWER(email) = %s",
                    (wanted_email,),
                )
                raced = cur.fetchone()
                if raced:
                    return _row_out(raced, created=False, existed=True)
                raise RuntimeError("Não foi possível incluir o usuário na Plataforma Central.") from exc
            user_id = int(cur.lastrowid or 0)
            cur.execute(
                f"SELECT id, username, email, first_name, last_name FROM `{TABLE}` WHERE id = %s",
                (user_id,),
            )
            created = cur.fetchone() or {
                "id": user_id,
                "username": username,
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
            }
            return _row_out(created, created=True, existed=False, password=password)
