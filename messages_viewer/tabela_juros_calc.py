# -*- coding: utf-8 -*-
"""
Simulação «Tabela de Juros (OC x Rendimento)» — espelha a planilha
``(TI) Tabela de juros _ Precatórios .xlsx`` (aba ``comparativo juros``).

Linha «atual»: juros simples (correção = meses × taxa mensal).
Linha «venda»: juros compostos (correção = (1 + taxa mensal)^meses − 1).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

TIPOS_ESFERA = ("Municipal", "Estadual", "Federal")

# Valores padrão da planilha de referência (Estadual).
DEFAULTS: dict[str, Any] = {
    "tipo": "Estadual",
    "anos": 15.0,
    "valor_atual": 50_000.0,
    "valor_venda": 15_980.0,
    "taxa_atual_anual_pct": 5.0,
    "taxa_venda_anual_pct": 15.0,
    "rotulo_atual": "IPCA",
    "rotulo_venda": "100% CDI CDB Banco",
}


@dataclass(frozen=True)
class LinhaRendimento:
    valor: float
    juros_mensal: float
    meses: float
    correcao: float
    valor_final: float
    tipo_juros: str
    referencia: str


@dataclass(frozen=True)
class ResultadoComparativo:
    tipo: str
    anos: float
    meses: float
    atual: LinhaRendimento
    venda: LinhaRendimento
    diferenca_final: float


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def calcular_comparativo(
    *,
    tipo: str = DEFAULTS["tipo"],
    anos: float = DEFAULTS["anos"],
    valor_atual: float = DEFAULTS["valor_atual"],
    valor_venda: float = DEFAULTS["valor_venda"],
    taxa_atual_anual_pct: float = DEFAULTS["taxa_atual_anual_pct"],
    taxa_venda_anual_pct: float = DEFAULTS["taxa_venda_anual_pct"],
    rotulo_atual: str = DEFAULTS["rotulo_atual"],
    rotulo_venda: str = DEFAULTS["rotulo_venda"],
) -> ResultadoComparativo:
    """Replica as fórmulas O307-equivalentes da planilha (linhas atual/venda)."""
    meses = max(0.0, _f(anos) * 12.0)
    juros_mensal_atual = (_f(taxa_atual_anual_pct) / 100.0) / 12.0
    juros_mensal_venda = (_f(taxa_venda_anual_pct) / 100.0) / 12.0

    # atual: E = D*C (simples), F = B*E + B
    correcao_atual = meses * juros_mensal_atual
    valor_final_atual = valor_atual * correcao_atual + valor_atual

    # venda: E = (1+C)^D - 1, F = (1+E)*B
    correcao_venda = (1.0 + juros_mensal_venda) ** meses - 1.0 if meses > 0 else 0.0
    valor_final_venda = (1.0 + correcao_venda) * valor_venda

    diferenca = valor_final_venda - valor_final_atual

    return ResultadoComparativo(
        tipo=(tipo or DEFAULTS["tipo"]).strip() or DEFAULTS["tipo"],
        anos=_f(anos, DEFAULTS["anos"]),
        meses=meses,
        atual=LinhaRendimento(
            valor=_f(valor_atual, DEFAULTS["valor_atual"]),
            juros_mensal=juros_mensal_atual,
            meses=meses,
            correcao=correcao_atual,
            valor_final=valor_final_atual,
            tipo_juros="juros simples",
            referencia=rotulo_atual or DEFAULTS["rotulo_atual"],
        ),
        venda=LinhaRendimento(
            valor=_f(valor_venda, DEFAULTS["valor_venda"]),
            juros_mensal=juros_mensal_venda,
            meses=meses,
            correcao=correcao_venda,
            valor_final=valor_final_venda,
            tipo_juros="juros compostos",
            referencia=rotulo_venda or DEFAULTS["rotulo_venda"],
        ),
        diferenca_final=diferenca,
    )


def resultado_para_api(r: ResultadoComparativo) -> dict[str, Any]:
    def linha(l: LinhaRendimento) -> dict[str, Any]:
        return {
            "valor": round(l.valor, 2),
            "juros_mensal": l.juros_mensal,
            "juros_mensal_pct": round(l.juros_mensal * 100.0, 6),
            "meses": round(l.meses, 2),
            "correcao": l.correcao,
            "correcao_pct": round(l.correcao * 100.0, 4),
            "valor_final": round(l.valor_final, 2),
            "tipo_juros": l.tipo_juros,
            "referencia": l.referencia,
        }

    return {
        "ok": True,
        "tipo": r.tipo,
        "anos": r.anos,
        "meses": round(r.meses, 2),
        "atual": linha(r.atual),
        "venda": linha(r.venda),
        "diferenca_final": round(r.diferenca_final, 2),
    }
