# -*- coding: utf-8 -*-
"""Geração de PDF de proposta comercial RED Precatórios."""
from __future__ import annotations

import io
import re
from pathlib import Path
from typing import Any

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

_ASSETS = Path(__file__).resolve().parents[1] / "logos_proposta"
_LOGO_CLARO = _ASSETS / "RED - Fundo Claro Horizontal_page-0002.jpg"
_SITE = "www.redprecatorios.com.br"

# Paleta RED Precatórios
COLOR_NAVY = colors.HexColor("#0f1a2e")
COLOR_BURGUNDY = colors.HexColor("#8f1d2f")
COLOR_BURGUNDY_DARK = colors.HexColor("#6b1523")
COLOR_TEXT = colors.HexColor("#1e293b")
COLOR_MUTED = colors.HexColor("#64748b")
COLOR_LINE = colors.HexColor("#e2e8f0")
COLOR_BG_SOFT = colors.HexColor("#f8fafc")


def _brl(value: Any) -> str:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return "—"
    s = f"{n:,.2f}"
    intp, _, dec = s.partition(".")
    intp = intp.replace(",", ".")
    return f"R$ {intp},{dec}"


def _safe_filename_part(text: str, max_len: int = 40) -> str:
    t = re.sub(r"[^\w\s-]", "", (text or "").strip(), flags=re.UNICODE)
    t = re.sub(r"\s+", "_", t)
    return (t[:max_len] or "proposta").strip("_")


def nome_arquivo_proposta(requerente: str, processo: str) -> str:
    req = _safe_filename_part(requerente, 30)
    proc = re.sub(r"[^\d]", "", processo or "")[:20] or "processo"
    return f"Proposta_RED_{req}_{proc}.pdf"


def _styles():
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "PropTitle",
            parent=base["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=22,
            textColor=COLOR_BURGUNDY,
            alignment=TA_CENTER,
            spaceAfter=6,
            leading=26,
        ),
        "subtitle": ParagraphStyle(
            "PropSub",
            parent=base["Normal"],
            fontName="Helvetica-Bold",
            fontSize=11,
            textColor=COLOR_NAVY,
            alignment=TA_CENTER,
            spaceAfter=14,
            leading=14,
        ),
        "client": ParagraphStyle(
            "PropClient",
            parent=base["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=14,
            textColor=COLOR_NAVY,
            alignment=TA_CENTER,
            spaceBefore=4,
            spaceAfter=10,
            leading=18,
        ),
        "entity": ParagraphStyle(
            "PropEntity",
            parent=base["Normal"],
            fontName="Helvetica-Bold",
            fontSize=11,
            textColor=COLOR_TEXT,
            alignment=TA_CENTER,
            spaceAfter=12,
        ),
        "body": ParagraphStyle(
            "PropBody",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=10.5,
            textColor=COLOR_TEXT,
            alignment=TA_LEFT,
            leading=15,
            spaceAfter=6,
        ),
        "bodyBold": ParagraphStyle(
            "PropBodyBold",
            parent=base["Normal"],
            fontName="Helvetica-Bold",
            fontSize=10.5,
            textColor=COLOR_TEXT,
            alignment=TA_LEFT,
            leading=15,
            spaceAfter=6,
        ),
        "tagline": ParagraphStyle(
            "PropTag",
            parent=base["Normal"],
            fontName="Helvetica-BoldOblique",
            fontSize=11,
            textColor=COLOR_BURGUNDY,
            alignment=TA_CENTER,
            spaceBefore=10,
            spaceAfter=12,
            leading=14,
        ),
        "offer": ParagraphStyle(
            "PropOffer",
            parent=base["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=20,
            textColor=COLOR_BURGUNDY_DARK,
            alignment=TA_CENTER,
            spaceBefore=6,
            spaceAfter=10,
            leading=24,
        ),
        "bullet": ParagraphStyle(
            "PropBullet",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=10,
            textColor=COLOR_TEXT,
            alignment=TA_JUSTIFY,
            leftIndent=14,
            bulletIndent=0,
            leading=14,
            spaceAfter=4,
        ),
        "footer": ParagraphStyle(
            "PropFooter",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=8.5,
            textColor=COLOR_MUTED,
            alignment=TA_CENTER,
            leading=11,
        ),
    }


def gerar_pdf_proposta(data: dict[str, Any]) -> bytes:
    """
    Gera bytes do PDF. ``data`` vem do formulário (edições só na plataforma).
    """
    requerente = str(data.get("requerente") or "—").strip()
    entidade = str(data.get("entidade_devedora") or "—").strip()
    processo = str(data.get("numero_de_processo") or "—").strip()
    incidente = str(data.get("numero_do_incidente") or "").strip()
    ordem = str(data.get("ordem") or "").strip()
    advogado = str(data.get("advogado") or "").strip()
    calculo = str(data.get("calculo_atualizado") or "").strip()

    valor_liquido = data.get("valor_liquido_atualizado")
    valor_proposta = data.get("valor_proposta")
    pct_honor = data.get("percentual_honorarios")
    valor_honor = data.get("valor_honorarios")

    try:
        pct_f = float(pct_honor) if pct_honor not in (None, "") else 10.0
    except (TypeError, ValueError):
        pct_f = 10.0

    if valor_honor in (None, "") and valor_liquido not in (None, ""):
        try:
            valor_honor = float(valor_liquido) * (pct_f / 100.0)
        except (TypeError, ValueError):
            valor_honor = None

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=1.6 * cm,
        bottomMargin=1.8 * cm,
        title=f"Proposta — {requerente}",
        author="RED Precatórios",
    )
    st = _styles()
    story: list[Any] = []

    # —— Capa / cabeçalho com logo ——
    if _LOGO_CLARO.is_file():
        img = Image(str(_LOGO_CLARO), width=14 * cm, height=3.2 * cm)
        img.hAlign = "CENTER"
        story.append(Spacer(1, 8 * mm))
        story.append(img)
    else:
        story.append(Paragraph("RED PRECATÓRIOS", st["title"]))

    story.append(Spacer(1, 6 * mm))
    bar = Table([[""]], colWidths=[16 * cm], rowHeights=[3])
    bar.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), COLOR_BURGUNDY),
                ("LINEBELOW", (0, 0), (-1, -1), 0, COLOR_BURGUNDY),
            ]
        )
    )
    bar.hAlign = "CENTER"
    story.append(bar)
    story.append(Spacer(1, 12 * mm))
    story.append(Paragraph("PROPOSTA COMERCIAL", st["title"]))
    story.append(Paragraph("Compra de Precatório — Proposta Aprovada", st["subtitle"]))
    story.append(Spacer(1, 8 * mm))

    story.append(PageBreak())

    # —— Página da proposta ——
    story.append(Paragraph(f"PROPOSTA APROVADA<br/>{requerente.upper()}", st["client"]))
    if entidade and entidade != "—":
        story.append(Paragraph(entidade.upper(), st["entity"]))

    proc_line = f"PROCESSO: {processo}"
    if incidente:
        proc_line += f"  ·  INCIDENTE: {incidente}"
    story.append(Paragraph(proc_line, st["bodyBold"]))
    if ordem:
        story.append(Paragraph(f"Ordem: {ordem}", st["body"]))
    if calculo:
        story.append(Paragraph(f"Cálculo atualizado: {calculo}", st["body"]))
    if advogado:
        story.append(Paragraph(f"Advogado: {advogado}", st["body"]))

    story.append(Spacer(1, 6 * mm))
    info_rows = [
        ["Valor líquido atualizado", _brl(valor_liquido)],
        [
            f"Honorários advocatícios ({pct_f:g}%)",
            f"{_brl(valor_honor)} (resguardado no processo)",
        ],
    ]
    info_tbl = Table(info_rows, colWidths=[6.2 * cm, 9.3 * cm])
    info_tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), COLOR_BG_SOFT),
                ("BOX", (0, 0), (-1, -1), 0.5, COLOR_LINE),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, COLOR_LINE),
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTNAME", (1, 0), (1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("TEXTCOLOR", (0, 0), (-1, -1), COLOR_TEXT),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 10),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    story.append(info_tbl)

    story.append(Paragraph(
        "SEUS SONHOS SE TORNANDO REALIDADE, HOJE!",
        st["tagline"],
    ))
    story.append(Paragraph(_brl(valor_proposta), st["offer"]))
    story.append(Paragraph(
        "Considerando os anos de espera do seu processo, oferecemos de imediato o valor acima.",
        st["body"],
    ))
    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph(
        "O pagamento será <b>líquido na conta do cliente</b>, ou seja:",
        st["bodyBold"],
    ))

    bullets = [
        "Não cobramos nenhum valor para negociação;",
        "O trâmite é validado por cartório de notas, garantindo total segurança e transparência;",
        "Deduções obrigatórias já consideradas na avaliação (honorários advocatícios 100% resguardados, "
        "sem descontos previdenciários, IR, etc.);",
        "Nosso pagamento é imediato, feito no ato da assinatura do contrato.",
    ]
    for b in bullets:
        story.append(Paragraph(f"• {b}", st["bullet"]))

    story.append(Spacer(1, 14 * mm))
    story.append(Paragraph(_SITE, st["footer"]))
    story.append(Paragraph(
        "RED Precatórios — Compra e venda de precatórios com segurança e agilidade.",
        st["footer"],
    ))

    doc.build(story)
    return buf.getvalue()
