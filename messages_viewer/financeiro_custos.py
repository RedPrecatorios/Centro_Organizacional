# -*- coding: utf-8 -*-
"""Custos do painel Financeiro (plataforma_central.financeiro_custos)."""
from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from messages_viewer.plataforma_auth_store import auth_connection, auth_cursor

# Custo Geral não é departamento: é a visão que junta os três abaixo.
DEPARTAMENTOS: tuple[tuple[str, str], ...] = (
    ("comercial", "Custo Dep. Comercial"),
    ("juridico", "Custo Dep. Jurídico"),
    ("ti", "Custo Dep. TI"),
)
DEPARTAMENTO_IDS = {k for k, _ in DEPARTAMENTOS}
VISAO_GERAL = "geral"

VIGENCIAS: tuple[tuple[str, str], ...] = (
    ("ano", "Dura o ano inteiro"),
    ("variavel", "Todo mês, o valor muda"),
    ("mes", "Só neste mês"),
)
VIGENCIA_IDS = {k for k, _ in VIGENCIAS}
_PDF_MAX = 20 * 1024 * 1024

MOEDAS = ("BRL", "USD")
NATUREZAS: tuple[tuple[str, str], ...] = (
    ("fixo", "Fixo"),
    ("variavel", "Variável"),
)
SITUACOES: tuple[tuple[str, str], ...] = (
    ("previsto", "Previsto"),
    ("lancado", "Lançado"),
    ("pago", "Pago"),
)
MESES: tuple[tuple[int, str], ...] = (
    (1, "Janeiro"),
    (2, "Fevereiro"),
    (3, "Março"),
    (4, "Abril"),
    (5, "Maio"),
    (6, "Junho"),
    (7, "Julho"),
    (8, "Agosto"),
    (9, "Setembro"),
    (10, "Outubro"),
    (11, "Novembro"),
    (12, "Dezembro"),
)

_SEED_META = "financeiro_seed_informatica_v1"
# Linhas da tabela «Informática» enviada para teste. Valor vazio fica em branco.
_SEED_INFORMATICA: tuple[tuple[str, str | None], ...] = (
    ("Assertiva Tecnologia da Informação", "4100.00"),
    ("Confirma On Line", None),
    ("Jus Brasil", "137.80"),
    ("Reparalogo Comunicação", "16755.55"),
    ("Zenvia Mobile Serv Dig", "2000.00"),
    ("Lemit", "1048.22"),
    ("Syscall", "1700.00"),
    ("WebShare", None),
    ("Adobe", "190.00"),
    ("Digitalocean U$", "6106.44"),
    ("Google Gsuiteredpreca U$", "5145.83"),
    ("Google Cloud", None),
    ("Chat GPT/Openai", "1232.06"),
    ("NGROK INC.", "54.84"),
    ("Goto Comunicação", "1710.39"),
    ("Crawly Solucoes E Tec", "1200.00"),
    ("Bitdefender (Anti-Virus)", None),
    ("Webcis Desenvolvimento", None),
    ("Recaptcha", None),
    ("Enriquecimento de dados", None),
    ("Docusing", "188.00"),
    ("Sistema Monday", "7829.64"),
)


def parse_valor(raw: object) -> Decimal | None:
    """Aceita 4100, 4100.00, 4.100,00 e vazio."""
    if raw is None:
        return None
    if isinstance(raw, Decimal):
        valor = raw
    elif isinstance(raw, (int, float)) and not isinstance(raw, bool):
        valor = Decimal(str(raw))
    else:
        text = str(raw).strip().replace("R$", "").replace("US$", "").replace(" ", "")
        if not text:
            return None
        if "," in text and "." in text:
            text = text.replace(".", "").replace(",", ".")
        elif "," in text:
            text = text.replace(",", ".")
        try:
            valor = Decimal(text)
        except InvalidOperation as exc:
            raise ValueError("Valor inválido.") from exc
    valor = valor.quantize(Decimal("0.01"))
    if valor < 0:
        raise ValueError("Valor não pode ser negativo.")
    if valor > Decimal("999999999999.99"):
        raise ValueError("Valor acima do limite.")
    return valor


def valor_no_ano(
    valor: Decimal | None,
    vigencia: str,
    meses: dict[int, Decimal] | None = None,
) -> Decimal | None:
    if vigencia == "variavel":
        total = sum((meses or {}).values(), Decimal("0.00"))
        if not meses:
            return None
        return total.quantize(Decimal("0.01"))
    if valor is None:
        return None
    if vigencia == "ano":
        return (valor * 12).quantize(Decimal("0.01"))
    return valor


def resumir(rows: list[dict[str, Any]], mes: int | None) -> list[dict[str, str]]:
    """Totais por moeda.

    ``valor`` já é o da visão: mês corrente ou, no ano, a soma dos meses
    variáveis. Recorrente (mesmo valor) entra em todo mês. Variável e pontual
    entram uma vez.
    """
    por_moeda: dict[str, list[Decimal]] = {}
    for row in rows:
        valor = row.get("valor")
        if valor is None:
            continue
        dec = valor if isinstance(valor, Decimal) else Decimal(str(valor))
        moeda = str(row.get("moeda") or "BRL")
        slot = por_moeda.setdefault(
            moeda, [Decimal("0.00"), Decimal("0.00"), Decimal("0.00")]
        )
        vig = row.get("vigencia")
        if vig == "ano":
            slot[0] += dec
        elif vig == "variavel":
            slot[1] += dec
        else:
            slot[2] += dec
    if not por_moeda:
        por_moeda["BRL"] = [Decimal("0.00"), Decimal("0.00"), Decimal("0.00")]
    saida = []
    for moeda in sorted(por_moeda):
        recorrente, variavel, pontual = por_moeda[moeda]
        recorrente = recorrente.quantize(Decimal("0.01"))
        variavel = variavel.quantize(Decimal("0.01"))
        pontual = pontual.quantize(Decimal("0.01"))
        visivel = (recorrente + variavel + pontual).quantize(Decimal("0.01"))
        if mes is None:
            projecao = (recorrente * 12 + variavel + pontual).quantize(Decimal("0.01"))
        else:
            projecao = visivel
        saida.append(
            {
                "moeda": moeda,
                "recorrente_mensal": _money(recorrente),
                "variavel": _money(variavel),
                "pontual": _money(pontual),
                "total_visivel": _money(visivel),
                "projecao_ano": _money(projecao),
            }
        )
    return saida


def normalizar_payload(data: dict[str, Any]) -> dict[str, Any]:
    dep = str(data.get("departamento") or "").strip().lower()
    if dep == VISAO_GERAL or dep not in DEPARTAMENTO_IDS:
        raise ValueError("Escolha o departamento.")
    fornecedor = _clip(data.get("fornecedor") or data.get("descricao"), 255)
    if not fornecedor:
        raise ValueError("Informe o fornecedor ou a plataforma.")
    vigencia = str(data.get("vigencia") or "").strip().lower()
    if vigencia not in VIGENCIA_IDS:
        raise ValueError("Marque a vigência do custo.")
    ano = _int(data.get("ano"), "Ano inválido.")
    if ano < 2000 or ano > 2100:
        raise ValueError("Ano inválido.")
    meses = _normalizar_meses(data.get("meses")) if vigencia == "variavel" else {}
    mes: int | None
    valor: Decimal | None
    if vigencia == "mes":
        mes = _int(data.get("mes"), "Escolha o mês da competência.")
        if mes < 1 or mes > 12:
            raise ValueError("Mês inválido.")
        valor = parse_valor(data.get("valor"))
    elif vigencia == "variavel":
        mes = None
        valor = None
    else:
        mes = None
        valor = parse_valor(data.get("valor"))
    moeda = str(data.get("moeda") or "BRL").strip().upper()
    if moeda not in MOEDAS:
        raise ValueError("Moeda inválida.")
    natureza = str(data.get("natureza") or "fixo").strip().lower()
    if natureza not in {k for k, _ in NATUREZAS}:
        raise ValueError("Natureza inválida.")
    situacao = str(data.get("situacao") or "lancado").strip().lower()
    if situacao not in {k for k, _ in SITUACOES}:
        raise ValueError("Situação inválida.")
    vencimento = _clip(data.get("vencimento"), 10) or None
    if vencimento and len(vencimento) != 10:
        raise ValueError("Vencimento inválido.")
    return {
        "departamento": dep,
        "fornecedor": fornecedor,
        "valor": valor,
        "meses": meses,
        "moeda": moeda,
        "vigencia": vigencia,
        "ano": ano,
        "mes": mes,
        "natureza": natureza,
        "situacao": situacao,
        "documento": _clip(data.get("documento"), 80) or None,
        "vencimento": vencimento,
        "observacao": _clip(data.get("observacao"), 500) or None,
        "remover_pdf": str(data.get("remover_pdf") or "").strip().lower() in ("1", "true", "on", "sim"),
    }


def listar(departamento: str, ano: int, mes: int | None, situacao: str | None) -> dict[str, Any]:
    visao = departamento == VISAO_GERAL
    if not visao and departamento not in DEPARTAMENTO_IDS:
        raise ValueError("Departamento inválido.")
    if ano < 2000 or ano > 2100:
        raise ValueError("Ano inválido.")
    if mes is not None and (mes < 1 or mes > 12):
        raise ValueError("Mês inválido.")
    if situacao and situacao not in {k for k, _ in SITUACOES}:
        raise ValueError("Situação inválida.")
    _ensure()
    sql = """
        SELECT id, departamento, fornecedor, valor, moeda, vigencia,
               ano, mes, natureza, situacao, documento, vencimento, observacao,
               valores_meses, pdf_nome, criado_por, atualizado_em
        FROM financeiro_custos
        WHERE ativo = 1 AND ano = %s
    """
    params: list[Any] = [ano]
    if visao:
        sql += " AND departamento IN ('comercial', 'juridico', 'ti')"
    else:
        sql += " AND departamento = %s"
        params.append(departamento)
    if mes is None:
        sql += " AND vigencia IN ('ano', 'mes', 'variavel')"
    else:
        sql += " AND (vigencia IN ('ano', 'variavel') OR (vigencia = 'mes' AND mes = %s))"
        params.append(mes)
    if situacao:
        sql += " AND situacao = %s"
        params.append(situacao)
    sql += " ORDER BY departamento, fornecedor, id"
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute(sql, params)
        raw_rows = [dict(r) for r in cur.fetchall()]
    rows = [_row_api(r, mes) for r in raw_rows]
    return {
        "ok": True,
        "rows": rows,
        "resumo": resumir(
            [
                {
                    "valor": None if r["valor"] is None else Decimal(r["valor"]),
                    "vigencia": r["vigencia"],
                    "moeda": r["moeda"],
                }
                for r in rows
            ],
            mes,
        ),
    }


def criar(
    data: dict[str, Any],
    criado_por: str,
    pdf: tuple[str, bytes] | None = None,
) -> dict[str, Any]:
    payload = normalizar_payload(data)
    _validar_pdf(pdf)
    _ensure()
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute(
            """
            INSERT INTO financeiro_custos (
                departamento, categoria, fornecedor, valor, moeda, vigencia,
                ano, mes, natureza, situacao, documento, vencimento, observacao,
                valores_meses, criado_por
            ) VALUES (%s, '', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                payload["departamento"],
                payload["fornecedor"],
                payload["valor"],
                payload["moeda"],
                payload["vigencia"],
                payload["ano"],
                payload["mes"],
                payload["natureza"],
                payload["situacao"],
                payload["documento"],
                payload["vencimento"],
                payload["observacao"],
                _meses_json(payload["meses"]),
                _clip(criado_por, 100) or None,
            ),
        )
        new_id = int(cur.lastrowid)
        if pdf:
            _gravar_pdf(new_id, pdf[0], pdf[1])
            cur.execute(
                "UPDATE financeiro_custos SET pdf_nome = %s WHERE id = %s",
                (_clip(pdf[0], 255), new_id),
            )
        conn.commit()
    return {"ok": True, "id": new_id}


def atualizar(
    custo_id: int,
    data: dict[str, Any],
    pdf: tuple[str, bytes] | None = None,
) -> dict[str, Any]:
    payload = normalizar_payload(data)
    _validar_pdf(pdf)
    _ensure()
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute(
            "SELECT id FROM financeiro_custos WHERE id = %s AND ativo = 1",
            (custo_id,),
        )
        if not cur.fetchone():
            raise ValueError("Lançamento não encontrado.")
        cur.execute(
            """
            UPDATE financeiro_custos
            SET departamento = %s, fornecedor = %s, valor = %s,
                moeda = %s, vigencia = %s, ano = %s, mes = %s, natureza = %s,
                situacao = %s, documento = %s, vencimento = %s, observacao = %s,
                valores_meses = %s
            WHERE id = %s AND ativo = 1
            """,
            (
                payload["departamento"],
                payload["fornecedor"],
                payload["valor"],
                payload["moeda"],
                payload["vigencia"],
                payload["ano"],
                payload["mes"],
                payload["natureza"],
                payload["situacao"],
                payload["documento"],
                payload["vencimento"],
                payload["observacao"],
                _meses_json(payload["meses"]),
                custo_id,
            ),
        )
        if pdf:
            _gravar_pdf(custo_id, pdf[0], pdf[1])
            cur.execute(
                "UPDATE financeiro_custos SET pdf_nome = %s WHERE id = %s",
                (_clip(pdf[0], 255), custo_id),
            )
        elif payload["remover_pdf"]:
            _apagar_pdf(custo_id)
            cur.execute(
                "UPDATE financeiro_custos SET pdf_nome = NULL WHERE id = %s",
                (custo_id,),
            )
        conn.commit()
    return {"ok": True, "id": custo_id}


def desativar(custo_id: int) -> dict[str, Any]:
    _ensure()
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute(
            "UPDATE financeiro_custos SET ativo = 0 WHERE id = %s AND ativo = 1",
            (custo_id,),
        )
        if cur.rowcount == 0:
            raise ValueError("Lançamento não encontrado.")
        conn.commit()
    _apagar_pdf(custo_id)
    return {"ok": True}


def caminho_pdf(custo_id: int) -> tuple[Path, str] | None:
    _ensure()
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute(
            "SELECT pdf_nome FROM financeiro_custos WHERE id = %s AND ativo = 1",
            (custo_id,),
        )
        row = cur.fetchone()
    if not row or not row.get("pdf_nome"):
        return None
    path = _pdf_file(custo_id)
    if not path.is_file():
        return None
    return path, str(row["pdf_nome"])


def _row_api(row: dict[str, Any], mes_vista: int | None) -> dict[str, Any]:
    meses = _ler_meses(row.get("valores_meses"))
    vig = row["vigencia"]
    bruto = row.get("valor")
    dec = None if bruto is None else Decimal(str(bruto)).quantize(Decimal("0.01"))
    if vig == "variavel":
        if mes_vista:
            dec = meses.get(mes_vista)
        else:
            dec = valor_no_ano(None, "variavel", meses)
    no_ano = valor_no_ano(dec if vig != "variavel" else None, vig, meses)
    venc = row.get("vencimento")
    if hasattr(venc, "isoformat"):
        venc = venc.isoformat()
    atualizado = row.get("atualizado_em")
    if hasattr(atualizado, "isoformat"):
        atualizado = atualizado.isoformat(sep=" ", timespec="seconds")
    return {
        "id": int(row["id"]),
        "departamento": row["departamento"],
        "fornecedor": row["fornecedor"],
        "valor": None if dec is None else _money(dec),
        "valor_no_ano": None if no_ano is None else _money(no_ano),
        "meses": {str(k): _money(v) for k, v in meses.items()},
        "moeda": row["moeda"],
        "vigencia": vig,
        "ano": int(row["ano"]),
        "mes": None if row.get("mes") is None else int(row["mes"]),
        "natureza": row["natureza"],
        "situacao": row["situacao"],
        "documento": row.get("documento") or "",
        "tem_pdf": bool(row.get("pdf_nome")),
        "pdf_nome": row.get("pdf_nome") or "",
        "vencimento": venc or "",
        "observacao": row.get("observacao") or "",
        "criado_por": row.get("criado_por") or "",
        "atualizado_em": atualizado or "",
    }


def _ensure() -> None:
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS financeiro_custos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                departamento VARCHAR(20) NOT NULL,
                categoria VARCHAR(120) NOT NULL,
                fornecedor VARCHAR(255) NOT NULL,
                valor DECIMAL(14, 2) NULL,
                moeda CHAR(3) NOT NULL DEFAULT 'BRL',
                vigencia VARCHAR(10) NOT NULL,
                ano SMALLINT NOT NULL,
                mes TINYINT NULL,
                natureza VARCHAR(20) NOT NULL DEFAULT 'fixo',
                situacao VARCHAR(20) NOT NULL DEFAULT 'lancado',
                documento VARCHAR(80) NULL,
                vencimento DATE NULL,
                observacao VARCHAR(500) NULL,
                valores_meses TEXT NULL,
                pdf_nome VARCHAR(255) NULL,
                criado_por VARCHAR(100) NULL,
                ativo TINYINT(1) NOT NULL DEFAULT 1,
                criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                atualizado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                KEY idx_fin_custos_lista (departamento, ano, ativo)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        _ensure_colunas(cur)
        cur.execute(
            "SELECT 1 FROM plataforma_meta WHERE meta_key = %s",
            (_SEED_META,),
        )
        if not cur.fetchone():
            for fornecedor, valor in _SEED_INFORMATICA:
                cur.execute(
                    """
                    INSERT INTO financeiro_custos (
                        departamento, categoria, fornecedor, valor, moeda, vigencia,
                        ano, mes, natureza, situacao, criado_por
                    ) VALUES ('ti', '', %s, %s, 'BRL', 'ano', 2026, NULL,
                              'fixo', 'lancado', 'exemplo')
                    """,
                    (fornecedor, valor),
                )
            cur.execute(
                """
                INSERT INTO plataforma_meta (meta_key, meta_value)
                VALUES (%s, '1')
                ON DUPLICATE KEY UPDATE meta_value = VALUES(meta_value)
                """,
                (_SEED_META,),
            )
        conn.commit()


def _ensure_colunas(cur) -> None:
    cur.execute("SHOW COLUMNS FROM financeiro_custos")
    have = set()
    for col in cur.fetchall():
        have.add(col.get("Field") or col.get("field"))
    if "valores_meses" not in have:
        cur.execute("ALTER TABLE financeiro_custos ADD COLUMN valores_meses TEXT NULL")
    if "pdf_nome" not in have:
        cur.execute("ALTER TABLE financeiro_custos ADD COLUMN pdf_nome VARCHAR(255) NULL")


def _normalizar_meses(raw: object) -> dict[int, Decimal]:
    if raw is None or raw == "":
        return {}
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        try:
            raw = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("Valores dos meses inválidos.") from exc
    if not isinstance(raw, dict):
        raise ValueError("Valores dos meses inválidos.")
    meses: dict[int, Decimal] = {}
    for key, value in raw.items():
        try:
            mes = int(key)
        except (TypeError, ValueError) as exc:
            raise ValueError("Mês inválido na grade.") from exc
        if mes < 1 or mes > 12:
            raise ValueError("Mês inválido na grade.")
        valor = parse_valor(value)
        if valor is not None:
            meses[mes] = valor
    return meses


def _ler_meses(raw: object) -> dict[int, Decimal]:
    if not raw:
        return {}
    try:
        return _normalizar_meses(raw)
    except ValueError:
        return {}


def _meses_json(meses: dict[int, Decimal]) -> str | None:
    if not meses:
        return None
    return json.dumps({str(k): _money(v) for k, v in sorted(meses.items())})


def _pdf_file(custo_id: int) -> Path:
    root = Path(__file__).resolve().parents[1] / "instance" / "financeiro_pdfs"
    return root / f"{int(custo_id)}.pdf"


def _validar_pdf(pdf: tuple[str, bytes] | None) -> None:
    if pdf is None:
        return
    _nome, content = pdf
    if not content.startswith(b"%PDF"):
        raise ValueError("O arquivo da nota precisa ser um PDF.")
    if len(content) > _PDF_MAX:
        raise ValueError("O PDF passa de 20 MB.")


def _gravar_pdf(custo_id: int, nome: str, content: bytes) -> None:
    path = _pdf_file(custo_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _apagar_pdf(custo_id: int) -> None:
    path = _pdf_file(custo_id)
    if path.is_file():
        path.unlink()


def _clip(raw: object, limit: int) -> str:
    return str(raw or "").strip()[:limit]


def _int(raw: object, message: str) -> int:
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(message) from exc


def _money(valor: Decimal | None) -> str:
    if valor is None:
        return "0.00"
    return f"{valor.quantize(Decimal('0.01'))}"
