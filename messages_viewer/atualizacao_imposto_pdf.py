# -*- coding: utf-8 -*-
"""PDF do resumo de Atualização De Imposto (para o cliente)."""
from __future__ import annotations

import io
import re
from pathlib import Path
from typing import Any

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    Image,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

_ASSETS = Path(__file__).resolve().parents[1] / "logos_proposta"
_LOGO_CLARO = _ASSETS / "RED - Fundo Claro Horizontal_page-0002.jpg"
_STATIC_LOGO = (
    Path(__file__).resolve().parent / "static" / "img" / "red" / "logo-horizontal-claro.jpg"
)

COLOR_NAVY = colors.HexColor("#0f172a")
COLOR_TEXT = colors.HexColor("#0f172a")
COLOR_MUTED = colors.HexColor("#64748b")
COLOR_LINE = colors.HexColor("#e2e8f0")
COLOR_BG = colors.HexColor("#f8fafc")
COLOR_OK_BG = colors.HexColor("#ecfdf5")
COLOR_OK_TEXT = colors.HexColor("#065f46")


def _money_br(value: Any) -> str:
    if value is None or value == "":
        return "—"
    if isinstance(value, (int, float)):
        n = float(value)
    else:
        s = str(value).strip().replace("R$", "").strip()
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        try:
            n = float(s)
        except ValueError:
            return f"R$ {value}" if not str(value).strip().startswith("R$") else str(value)
    formatted = f"{n:,.2f}"
    intp, _, dec = formatted.partition(".")
    return f"R$ {intp.replace(',', '.')},{dec}"


def _safe_filename_part(text: str, max_len: int = 40) -> str:
    t = re.sub(r"[^\w\s-]", "", (text or "").strip(), flags=re.UNICODE)
    t = re.sub(r"\s+", "_", t)
    return (t[:max_len] or "cliente").strip("_")


def nome_arquivo_resumo(nome: str, processo: str) -> str:
    n = _safe_filename_part(nome, 30)
    p = re.sub(r"[^\d]", "", processo or "")[:20] or "processo"
    return f"Atualizacao_Imposto_RED_{n}_{p}.pdf"


def _draw_footer(canvas, doc) -> None:
    canvas.saveState()
    page = canvas.getPageNumber()
    canvas.setStrokeColor(COLOR_LINE)
    canvas.setLineWidth(0.6)
    y = 14 * mm
    canvas.line(18 * mm, y + 6, A4[0] - 18 * mm, y + 6)
    canvas.setFont("Helvetica-Bold", 9)
    canvas.setFillColor(COLOR_NAVY)
    canvas.drawString(18 * mm, y, "RED PRECATÓRIOS")
    canvas.setFont("Helvetica", 9)
    canvas.setFillColor(COLOR_MUTED)
    canvas.drawRightString(A4[0] - 18 * mm, y, f"Página {page}")
    canvas.restoreState()


def gerar_pdf_resumo_imposto(data: dict[str, Any]) -> bytes:
    """Gera PDF de 1 página com resumo para o cliente."""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=16 * mm,
        bottomMargin=22 * mm,
        title="Atualização de Imposto a Ser Restituído",
        author="RED Precatórios",
    )

    styles = getSampleStyleSheet()
    style_title = ParagraphStyle(
        "AiTitle",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=16,
        textColor=colors.white,
        alignment=TA_LEFT,
        leading=20,
        spaceAfter=0,
    )
    style_label = ParagraphStyle(
        "AiLabel",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=8,
        textColor=COLOR_MUTED,
        leading=10,
        spaceAfter=2,
    )
    style_value = ParagraphStyle(
        "AiValue",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=11,
        textColor=COLOR_TEXT,
        leading=14,
    )
    style_row_left = ParagraphStyle(
        "AiRowL",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=10,
        textColor=COLOR_MUTED,
        leading=13,
    )
    style_row_right = ParagraphStyle(
        "AiRowR",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=11,
        textColor=COLOR_TEXT,
        alignment=TA_RIGHT,
        leading=14,
    )
    style_row_right_ok = ParagraphStyle(
        "AiRowROK",
        parent=style_row_right,
        textColor=COLOR_OK_TEXT,
        fontSize=13,
    )
    style_foot = ParagraphStyle(
        "AiFoot",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8,
        textColor=COLOR_MUTED,
        alignment=TA_CENTER,
        leading=11,
    )

    nome = str(data.get("nome") or "—").strip() or "—"
    cpf = str(data.get("cpf_cnpj") or data.get("cpf") or "—").strip() or "—"
    processo = str(data.get("processo") or "—").strip() or "—"
    ir_retido = _money_br(data.get("ir_retido_comprovante") or data.get("restituido_comprovante"))
    valor_atualizado = _money_br(
        data.get("imposto_retido_indevidamente_atualizado")
        or data.get("valor_a_restituir_atualizado")
    )

    story: list[Any] = []

    logo_path = _LOGO_CLARO if _LOGO_CLARO.is_file() else _STATIC_LOGO
    logo_cell: Any = Spacer(1, 1)
    if logo_path.is_file():
        logo_cell = Image(str(logo_path), width=5.2 * cm, height=1.35 * cm, kind="proportional")

    head = Table(
        [
            [
                logo_cell,
                Paragraph("Atualização de Imposto A Ser Restituído", style_title),
            ]
        ],
        colWidths=[6.2 * cm, 11.3 * cm],
    )
    head.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), COLOR_NAVY),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (0, 0), 10),
                ("RIGHTPADDING", (0, 0), (0, 0), 8),
                ("LEFTPADDING", (1, 0), (1, 0), 8),
                ("RIGHTPADDING", (1, 0), (1, 0), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 12),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
            ]
        )
    )
    story.append(head)
    story.append(Spacer(1, 14))

    person = Table(
        [
            [
                [Paragraph("NOME", style_label), Paragraph(nome, style_value)],
                [Paragraph("CPF", style_label), Paragraph(cpf, style_value)],
            ],
            [
                [Paragraph("Nº DO PROCESSO", style_label), Paragraph(processo, style_value)],
                "",
            ],
        ],
        colWidths=[11 * cm, 6.5 * cm],
    )
    person.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("SPAN", (0, 1), (1, 1)),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ("LEFTPADDING", (0, 0), (-1, -1), 2),
                ("RIGHTPADDING", (0, 0), (-1, -1), 2),
            ]
        )
    )
    story.append(person)
    story.append(Spacer(1, 10))

    values = Table(
        [
            [
                Paragraph("Restituído no comprovante", style_row_left),
                Paragraph(ir_retido, style_row_right),
            ],
            [
                Paragraph("Valor a restituir atualizado", style_row_left),
                Paragraph(valor_atualizado, style_row_right_ok),
            ],
        ],
        colWidths=[11.5 * cm, 6 * cm],
    )
    values.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), COLOR_BG),
                ("BACKGROUND", (0, 1), (-1, 1), COLOR_OK_BG),
                ("BOX", (0, 0), (-1, -1), 0.8, COLOR_LINE),
                ("LINEBELOW", (0, 0), (-1, 0), 0.6, COLOR_LINE),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 12),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
            ]
        )
    )
    story.append(values)
    story.append(Spacer(1, 16))
    story.append(
        Paragraph(
            "Valores atualizados conforme demonstrativo e comprovante de levantamento.",
            style_foot,
        )
    )

    doc.build(story, onFirstPage=_draw_footer, onLaterPages=_draw_footer)
    return buf.getvalue()
