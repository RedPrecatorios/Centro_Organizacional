#!/usr/bin/env python3
"""
Importa registros de um arquivo gerado por dump MySQL no formato:
  INSERT INTO `blacklist` VALUES (id,'TIPO','valor',...),(...);

Insere em blacklist sem reutilizar id (AUTO_INCREMENT), usando upsert por (tipo, valor).

Uso:
  cd /opt/Centro_Organizacional && . .venv/bin/activate
  python3 import_inset_blacklist.py [caminho_do_arquivo]

Variáveis de ambiente: EDA_MYSQL_* (vide .env). Opcional:
  IMPORT_BLACKLIST_DATABASE — sobrescreve EDA_MYSQL_DATABASE
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv


def skip_ws(s: str, i: int) -> int:
    while i < len(s) and s[i] in " \t\n\r":
        i += 1
    return i


def parse_int(s: str, i: int) -> tuple[int, int]:
    j = i
    while j < len(s) and s[j].isdigit():
        j += 1
    if j == i:
        raise ValueError(f"inteiro esperado na posição {i}")
    return int(s[i:j]), j


def parse_sql_string(s: str, i: int) -> tuple[str, int]:
    if i >= len(s) or s[i] != "'":
        raise ValueError(f"' esperado na posição {i}")
    i += 1
    out: list[str] = []
    while i < len(s):
        ch = s[i]
        if ch == "\\" and i + 1 < len(s):
            nxt = s[i + 1]
            if nxt == "'":
                out.append("'")
            elif nxt == "\\":
                out.append("\\")
            elif nxt == "n":
                out.append("\n")
            elif nxt == "t":
                out.append("\t")
            elif nxt == "r":
                out.append("\r")
            elif nxt == "0":
                out.append("\0")
            elif nxt == "Z":
                out.append("\032")
            else:
                out.append(nxt)
            i += 2
            continue
        if ch == "'":
            if i + 1 < len(s) and s[i + 1] == "'":
                out.append("'")
                i += 2
                continue
            i += 1
            return "".join(out), i
        out.append(ch)
        i += 1
    raise ValueError("string SQL não terminada")


def parse_null_or_string(s: str, i: int) -> tuple[str | None, int]:
    i = skip_ws(s, i)
    if i + 4 <= len(s) and s[i : i + 4].upper() == "NULL":
        tail = i + 4
        if tail < len(s) and s[tail] not in ",)":
            raise ValueError("NULL mal formatado")
        return None, tail
    return parse_sql_string(s, i)


def expect_char(s: str, i: int, c: str) -> int:
    i = skip_ws(s, i)
    if i >= len(s) or s[i] != c:
        raise ValueError(f"esperado {c!r} na posição {i}")
    return i + 1


def parse_tuple(s: str, i: int) -> tuple[tuple[str, str, str | None, str, int], int]:
    i = expect_char(s, i, "(")
    i = skip_ws(s, i)
    _, i = parse_int(s, i)
    i = expect_char(s, i, ",")
    tipo, i = parse_sql_string(s, skip_ws(s, i))
    i = expect_char(s, i, ",")
    valor, i = parse_sql_string(s, skip_ws(s, i))
    i = expect_char(s, i, ",")
    motivo, i = parse_null_or_string(s, skip_ws(s, i))
    i = expect_char(s, i, ",")
    data_inclusao, i = parse_sql_string(s, skip_ws(s, i))
    i = expect_char(s, i, ",")
    ativo, i = parse_int(s, skip_ws(s, i))
    i = expect_char(s, i, ")")
    return (tipo.upper(), valor, motivo, data_inclusao, ativo), i


def merge_blacklist_insert_batches(sql: str) -> str:
    """Vários dumps `); INSERT INTO blacklist VALUES (` viram uma única lista de tuplas."""
    return re.sub(
        r"\)\s*;\s*INSERT\s+INTO\s+`?blacklist`?\s+VALUES\s*\(",
        "),(",
        sql.strip(),
        flags=re.I | re.DOTALL,
    )


def extract_values_rest(sql: str) -> str:
    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1].strip()
    sql = merge_blacklist_insert_batches(sql)
    m = re.search(r"INSERT\s+INTO\s+`?blacklist`?\s+VALUES\s*", sql, re.I | re.DOTALL)
    if not m:
        raise ValueError("Linha deve conter INSERT INTO blacklist ... VALUES (...)")
    rest = sql[m.end() :].lstrip()
    if not rest.startswith("("):
        raise ValueError("Após VALUES deveria começar com '('")
    return rest


def iter_blacklist_dump_rows(rest: str):
    i = 0
    while i < len(rest):
        i = skip_ws(rest, i)
        if i >= len(rest):
            break
        if rest[i] == ",":
            i += 1
            continue
        row, i = parse_tuple(rest, i)
        yield row


def main() -> int:
    repo = Path(__file__).resolve().parent
    load_dotenv(repo / ".env")

    path = Path(sys.argv[1] if len(sys.argv) > 1 else repo / "inset_blacklist.txt")
    if not path.is_file():
        print(f"Arquivo não encontrado: {path}", file=sys.stderr)
        return 1

    db_name = (os.getenv("IMPORT_BLACKLIST_DATABASE") or "").strip() or os.getenv(
        "EDA_MYSQL_DATABASE", "plataforma_central"
    ).strip()

    host = os.getenv("EDA_MYSQL_HOST", "127.0.0.1").strip()
    port = int(os.getenv("EDA_MYSQL_PORT", "3306") or "3306")
    user = os.getenv("EDA_MYSQL_USER", "root").strip()
    password = os.getenv("EDA_MYSQL_PASSWORD", "")

    raw = path.read_text(encoding="utf-8", errors="replace")
    rest = extract_values_rest(raw)
    rows = list(iter_blacklist_dump_rows(rest))
    print(f"[import] {len(rows)} tuplas parseadas de {path.name}")

    sql = """
        INSERT INTO blacklist (tipo, valor, motivo, data_inclusao, ativo)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            ativo = VALUES(ativo),
            motivo = COALESCE(VALUES(motivo), motivo),
            data_inclusao = VALUES(data_inclusao)
    """

    conn = mysql.connector.connect(
        host=host or "127.0.0.1",
        port=port,
        user=user,
        password=password,
        database=db_name,
    )
    cur = conn.cursor()
    batch_size = 500
    done = 0
    try:
        for off in range(0, len(rows), batch_size):
            chunk = rows[off : off + batch_size]
            cur.executemany(sql, chunk)
            conn.commit()
            done += len(chunk)
            print(f"[import] commit {done}/{len(rows)} …")
    finally:
        cur.close()
        conn.close()

    print(f"[import] Concluído — banco `{db_name}` — {done} registros aplicados.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
