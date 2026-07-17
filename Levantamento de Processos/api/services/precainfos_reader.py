"""MySQL reader for precainfosnew (API-stable; does not import REFACTOR)."""

from __future__ import annotations

import logging
from typing import Any

import pymysql
from pymysql.cursors import DictCursor

from tjsp_pipeline.config import Settings

logger = logging.getLogger(__name__)

PRECAINFOSNEW_TABLE = "precainfosnew"

SELECT_COLUMNS = (
    "id",
    "Natureza",
    "Assunto",
    "Ordem",
    "Quantidade_de_Oficios",
    "Vara",
    "SPPRE",
    "IAMSPE",
    "IPES",
    "ASSIT_MED_HOSPITAL",
    "INST_PREV_CAIXA_BENEF",
    "ASSIST_MED_CAIXA_BENEF",
    "Numero_de_Processo",
    "Numero_do_Incidente",
    "Termo_Inicial",
    "Termo_Final",
    "Termo_Total",
    "Foro",
    "Data_Base",
    "Data_Decisao",
    "Principal_Liquido",
    "Juros_Moratorio",
    "Juros_Compensatorios",
    "Calculo_Atualizado",
    "Entidade_Devedora",
    "Advogado",
    "OAB",
    "CPF_CNPJ",
    "Cabeca_da_Acao",
    "Requerente",
    "Data_de_Nascimento",
    "CPF",
    "Processo_Principal",
    "Controle",
    "DEPRE",
    "EP",
    "Valor_Negociavel",
    "Processo_Codigo",
    "Data_Preenchimento",
    "Valor_Requisitado",
    "Valor_Global",
    "TESTE_ALT",
    "Data_de_Entrada",
    "Data_Conhecimento",
    "ADVOGADO_CREDOR",
    "Atualizado_BLIP",
    "INST_PREV",
    "UPDATES_INDEX",
    "`TERMO/OFICIO`",
    "Script",
    "Numero_de_Meses",
    "Numero_de_Meses_TERMO",
)


def _normalize_incidente(value: str) -> str:
    try:
        return str(int(str(value).strip()))
    except (TypeError, ValueError):
        return str(value or "").strip()


def _incidente_variants(value: str) -> list[str]:
    normalized = _normalize_incidente(value)
    variants = [normalized]
    if normalized.isdigit():
        padded = normalized.zfill(2)
        if padded not in variants:
            variants.append(padded)
        padded4 = normalized.zfill(4)
        if padded4 not in variants:
            variants.append(padded4)
    return variants


class PrecainfosReader:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _connect(self) -> pymysql.Connection:
        return pymysql.connect(
            host=self.settings.flask_db_host,
            port=int(self.settings.flask_db_port or "3306"),
            user=self.settings.flask_db_user,
            password=self.settings.flask_db_password,
            database=self.settings.flask_db_database,
            charset="utf8mb4",
            cursorclass=DictCursor,
            connect_timeout=20,
            read_timeout=60,
            write_timeout=60,
        )

    def exists(self, numero_de_processo: str, numero_do_incidente: str) -> bool:
        return self.fetch_one(numero_de_processo, numero_do_incidente) is not None

    def fetch_one(
        self,
        numero_de_processo: str,
        numero_do_incidente: str,
    ) -> dict[str, Any] | None:
        cols = ", ".join(SELECT_COLUMNS)
        processo = str(numero_de_processo).strip()
        variants = _incidente_variants(numero_do_incidente)
        placeholders = ", ".join(["%s"] * len(variants))
        sql = f"""
            SELECT {cols}
            FROM `{PRECAINFOSNEW_TABLE}`
            WHERE `Numero_de_Processo` = %s
              AND CAST(`Numero_do_Incidente` AS CHAR) IN ({placeholders})
            ORDER BY id DESC
            LIMIT 1
        """
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (processo, *variants))
                    row = cur.fetchone()
            return dict(row) if row else None
        except Exception:
            logger.exception(
                "DB fetch failed | processo=%s incidente=%s",
                processo,
                numero_do_incidente,
            )
            raise

    def fetch_by_requerente(self, nome: str) -> list[dict[str, Any]]:
        """Best-effort match by Requerente (normalized LIKE)."""
        cols = ", ".join(SELECT_COLUMNS)
        needle = " ".join(str(nome or "").split()).strip()
        if not needle:
            return []
        sql = f"""
            SELECT {cols}
            FROM `{PRECAINFOSNEW_TABLE}`
            WHERE UPPER(TRIM(`Requerente`)) LIKE UPPER(%s)
            ORDER BY id DESC
        """
        pattern = f"%{needle}%"
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (pattern,))
                    rows = cur.fetchall() or []
            return [dict(r) for r in rows]
        except Exception:
            logger.exception("DB fetch_by_requerente failed | nome=%s", needle)
            raise

    def fetch_by_cpf(self, cpf: str) -> list[dict[str, Any]]:
        """Match by CPF digits (CPF or CPF_CNPJ)."""
        import re

        cols = ", ".join(SELECT_COLUMNS)
        digits = re.sub(r"\D+", "", str(cpf or ""))
        if len(digits) != 11:
            return []
        sql = f"""
            SELECT {cols}
            FROM `{PRECAINFOSNEW_TABLE}`
            WHERE REPLACE(REPLACE(REPLACE(REPLACE(IFNULL(`CPF`, ''), '.', ''), '-', ''), '/', ''), ' ', '') = %s
               OR REPLACE(REPLACE(REPLACE(REPLACE(IFNULL(`CPF_CNPJ`, ''), '.', ''), '-', ''), '/', ''), ' ', '') = %s
            ORDER BY id DESC
        """
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (digits, digits))
                    rows = cur.fetchall() or []
            return [dict(r) for r in rows]
        except Exception:
            logger.exception("DB fetch_by_cpf failed | cpf=%s", digits)
            raise

    def fetch_by_processo(self, numero_de_processo: str) -> list[dict[str, Any]]:
        """All incidents for a CNJ process number."""
        cols = ", ".join(SELECT_COLUMNS)
        processo = str(numero_de_processo or "").strip()
        if not processo:
            return []
        sql = f"""
            SELECT {cols}
            FROM `{PRECAINFOSNEW_TABLE}`
            WHERE `Numero_de_Processo` = %s
            ORDER BY id DESC
        """
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (processo,))
                    rows = cur.fetchall() or []
            return [dict(r) for r in rows]
        except Exception:
            logger.exception("DB fetch_by_processo failed | processo=%s", processo)
            raise
