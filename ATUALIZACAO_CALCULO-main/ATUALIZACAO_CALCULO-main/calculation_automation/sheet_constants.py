# -*- coding: utf-8 -*-
"""Constantes da folha de memória detalhada (DEPRE SP) — planilha V33."""

from __future__ import annotations

import os
import re
import tempfile
import zipfile

SHEET_NAME = "(02.2) Mem Detalhada (DEPRE SP)"

# Entradas (V33)
CELULA_CABECA = "B2"
CELULA_ENTIDADE = "B3"
CELULA_PROCESSO = "B5"
CELULA_OC = "B6"
CELULA_EP = "B7"
CELULA_CUMPRIMENTO = "B8"
CELULA_INCIDENTE = "B9"
CELULA_NOME = "B12"
CELULA_DATA_BASE = "F18"
CELULA_DATA_INSCRICAO = "J18"
CELULA_PRINCIPAL = "B23"
CELULA_JUROS = "D23"
CELULA_DESPESAS = "F23"
CELULA_DESCONTOS = "I23"

# Fração em Q14 (0,3 = 30%). R35 = -R32*Q14; B4 usa 100000/(1-Q14).
# Scalar 0,3 com formato 0% é zerado pelo Google Sheets; a fórmula `=0.3` não.
CELULA_HONORARIOS_PCT = "Q14"
HONORARIOS_PERCENT_TEMPLATE = 30.0


def honorarios_q14_formula(percent: float = HONORARIOS_PERCENT_TEMPLATE) -> str:
    """Fórmula de Q14. ``percent`` 0–100 → ``=0`` / ``=0.3`` / …"""
    try:
        value = float(percent)
    except (TypeError, ValueError):
        value = HONORARIOS_PERCENT_TEMPLATE
    if value < 0:
        value = HONORARIOS_PERCENT_TEMPLATE
    if value > 100:
        value = 100.0
    return f"={value / 100.0:.10g}"


def coerce_honorarios_percent(value: object) -> float | None:
    """Converte entrada da UI/API para 0–100. ``None`` se ausente ou ilegível."""
    if value is None:
        return None
    text = str(value).strip().replace("%", "").replace(" ", "").replace(",", ".")
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if number < 0:
        return None
    return round(min(number, 100.0), 2)


def honorarios_percent_from_main_dict(main_dict: object) -> float | None:
    if not isinstance(main_dict, dict):
        return None
    for key in ("Percentual_Honorarios", "percentual_honorarios"):
        parsed = coerce_honorarios_percent(main_dict.get(key))
        if parsed is not None:
            return parsed
    return None


def q14_value_to_percent(value: object) -> float | None:
    """Converte o valor de Q14 (0,3, ``=0.3``, 30, ``30%``) para 0–100."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("%", "").replace(" ", "").replace(",", ".")
    if text.startswith("="):
        text = text[1:].strip()
    if "/" in text:
        left, _sep, right = text.partition("/")
        try:
            number = float(left) / float(right)
        except (TypeError, ValueError, ZeroDivisionError):
            return None
    else:
        try:
            number = float(text)
        except (TypeError, ValueError):
            return None
    if number < 0:
        return None
    if number == 0:
        return 0.0
    if number <= 1.0:
        return round(number * 100.0, 2)
    return round(min(number, 100.0), 2)


_Q14_CELL_RE = re.compile(
    rb'<((?:[A-Za-z0-9._-]+:)?c)\b(?=[^>]*\br="Q14")([^>]*?)(?:/>|>(.*?)</\1>)',
    re.DOTALL,
)
_SHEET_TAG_RE = re.compile(rb"<sheet\b([^>]+)/?>", re.IGNORECASE)
_REL_TAG_RE = re.compile(rb"<Relationship\b([^>]+)/?>", re.IGNORECASE)


def _xlsx_sheet_xml_path(zf: zipfile.ZipFile, sheet_name: str) -> str | None:
    try:
        workbook_xml = zf.read("xl/workbook.xml")
        rels_xml = zf.read("xl/_rels/workbook.xml.rels")
    except KeyError:
        return None
    wanted = (sheet_name or SHEET_NAME).strip()
    rid = None
    for match in _SHEET_TAG_RE.finditer(workbook_xml):
        attrs = match.group(1)
        name_m = re.search(rb'\bname="([^"]*)"', attrs)
        rid_m = re.search(rb'\br:id="([^"]*)"', attrs)
        if not name_m or not rid_m:
            continue
        name = name_m.group(1).decode("utf-8", "replace").strip()
        if name == wanted:
            rid = rid_m.group(1)
            break
    if not rid:
        return None
    for match in _REL_TAG_RE.finditer(rels_xml):
        attrs = match.group(1)
        id_m = re.search(rb'\bId="([^"]*)"', attrs)
        target_m = re.search(rb'\bTarget="([^"]*)"', attrs)
        if not id_m or not target_m or id_m.group(1) != rid:
            continue
        target = target_m.group(1).decode("utf-8", "replace").lstrip("/")
        if target.startswith("xl/"):
            return target
        return "xl/" + target
    return None


def rewrite_q14_cell_xml(sheet_xml: bytes, formula_inner: str) -> bytes | None:
    """Troca só a célula Q14; mantém o resto do XML (caches de R30:R36)."""
    inner = (formula_inner or "0.3").encode("ascii")
    match = _Q14_CELL_RE.search(sheet_xml)
    if not match:
        return None
    tag, attrs = match.group(1), match.group(2)
    attrs = re.sub(rb'\s+t="[^"]*"', b"", attrs)
    new_cell = (
        b"<" + tag + attrs + b"><f>" + inner + b"</f><v>" + inner + b"</v></" + tag + b">"
    )
    return sheet_xml[: match.start()] + new_cell + sheet_xml[match.end() :]


def pin_q14_formula_preserving_caches(
    file_path: str,
    *,
    percent: float | None = None,
    sheet_name: str | None = None,
) -> bool:
    """Grava Q14 como fórmula ``0.3`` no ZIP, sem openpyxl (não apaga caches)."""
    if not file_path or not os.path.isfile(file_path):
        return False
    formula = honorarios_q14_formula(
        HONORARIOS_PERCENT_TEMPLATE if percent is None else percent
    )
    inner = formula.lstrip("=")
    name = (sheet_name or SHEET_NAME).strip() or SHEET_NAME
    directory = os.path.dirname(os.path.abspath(file_path)) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".q14pin_", suffix=".xlsx", dir=directory)
    os.close(fd)
    try:
        with zipfile.ZipFile(file_path, "r") as zin:
            member = _xlsx_sheet_xml_path(zin, name)
            if not member:
                return False
            rewritten = rewrite_q14_cell_xml(zin.read(member), inner)
            if rewritten is None:
                return False
            with zipfile.ZipFile(tmp_path, "w") as zout:
                for item in zin.infolist():
                    data = rewritten if item.filename == member else zin.read(item.filename)
                    zout.writestr(item, data)
        os.replace(tmp_path, file_path)
        tmp_path = ""
        return True
    except Exception:
        return False
    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

NUMERO_MESES_CELULA = "R24"
NUMERO_MESES_OBSERVACAO_CELULA = "B13"
NUMERO_MESES_FALLBACK = 1
NUMERO_MESES_DB_VERIFICAR = "Verificar Meses"
NUMERO_MESES_ALERTA_PLANILHA = (
    "ATENCAO: Numero de meses igual ou inferior a 9 no cadastro. "
    "Verificar e corrigir antes de utilizar os valores da memoria."
)
NUMERO_MESES_ALERTA_AUTOR = "ATUALIZACAO_CALCULO"

# Total líquido (R36) até este valor: R24 = 1000 para zerar IR na tabela RRA.
LIMITE_ISENCAO_IR_TOTAL_LIQUIDO = 200_000.0
NUMERO_MESES_ISENCAO_IR = 1000
ISENCAO_IR_OBSERVACAO = (
    "Isencao de IR: total liquido ate R$ 200.000,00. "
    "Numero de meses ajustado para 1000 para zerar o imposto."
)


def deve_aplicar_meses_isencao_ir(
    total_liquido: object,
    meses_atuais: object = 0,
) -> bool:
    """True quando R36 (após 1.º recálculo) está em (0, 200 mil] e R24 ainda não é 1000."""
    if total_liquido is None:
        return False
    try:
        valor = float(total_liquido)
    except (TypeError, ValueError):
        return False
    try:
        meses = int(meses_atuais or 0)
    except (TypeError, ValueError):
        meses = 0
    if meses >= NUMERO_MESES_ISENCAO_IR:
        return False
    return 0 < valor <= LIMITE_ISENCAO_IR_TOTAL_LIQUIDO

# Memória de cálculo (V33) — valores na coluna R
TOTAL_LIQUIDO_CELL = "R36"
CALCULO_RESULT_CELL = TOTAL_LIQUIDO_CELL
MEMORIA_CELL_TO_FIELD: tuple[tuple[str, str], ...] = (
    ("R30", "principal_bruto"),
    ("R31", "juros"),
    ("R32", "total_bruto"),
    ("R33", "desc_saude_prev"),
    ("R34", "desc_ir"),
    ("R35", "reserva_honorarios"),
    ("R36", "total_liquido"),
)
O_COLUMN_TO_FIELD = MEMORIA_CELL_TO_FIELD
