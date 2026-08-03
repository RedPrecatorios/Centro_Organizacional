"""
Herdeiros da ficha PRÉ Análise — tabelas relacionais e CRUD.

Tabelas (plataforma_central):
- pre_analise_credor
- pre_analise_herdeiro
- pre_analise_herdeiro_conjuge
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

# Secções repetíveis por herdeiro (UI)
HERDEIRO_SECTIONS: list[dict[str, Any]] = [
    {
        "id": "herdeiro",
        "title": "Dados do herdeiro",
        "fields": [
            "herdeiro_habilitado",
            "nome_herdeiro",
            "cpf_herdeiro",
            "parentesco_herdeiro",
            "nacionalidade_herdeiro",
            "estado_civil_herdeiro",
            "percentual_detido",
            "herdeiro_cedente",
            "percentual_honorarios_herdeiro",
            "mencionar",
            "rg_herdeiro",
            "uf_herdeiro",
            "email_herdeiro",
            "telefone_herdeiro",
            "ocupacao_herdeiro",
            "data_de_nascimento_herdeiro",
        ],
    },
    {
        "id": "endereco_herdeiro",
        "title": "Endereço do herdeiro",
        "fields": [
            "cep_herdeiro",
            "logradouro_herdeiro",
            "numero_logradouro_herdeiro",
            "bairro_herdeiro",
            "cidade_herdeiro",
            "estado_herdeiro",
        ],
    },
    {
        "id": "conjuge_herdeiro",
        "title": "Cônjuge do herdeiro",
        "collapsible": True,
        "fields": [
            "nome_conjuge_herdeiro",
            "cpf_conjuge_herdeiro",
            "rg_conjuge_herdeiro",
            "uf_conjuge_herdeiro",
            "data_de_nascimento_conjuge_herdeiro",
            "nacionalidade_conjuge_herdeiro",
            "ocupacao_conjuge_herdeiro",
            "email_conjuge_herdeiro",
            "telefone_conjuge_herdeiro",
        ],
    },
    {
        "id": "endereco_conjuge_herdeiro",
        "title": "Endereço do cônjuge do herdeiro",
        "collapsible": True,
        "fields": [
            "cep_conjuge_herdeiro",
            "logradouro_conjuge_herdeiro",
            "numero_logradouro_conjuge_herdeiro",
            "bairro_conjuge_herdeiro",
            "cidade_conjuge_herdeiro",
            "estado_conjuge_herdeiro",
        ],
    },
    {
        "id": "banco_herdeiro",
        "title": "Dados bancários do herdeiro",
        "fields": [
            "percentual_cedido",
            "valor_da_proposta_herdeiro",
            "codigo_banco_herdeiro",
            "banco_herdeiro",
            "agencia_herdeiro",
            "conta_herdeiro",
            "tipo_conta_herdeiro",
            "tem_chave_pix_herdeiro",
            "chave_pix_herdeiro",
        ],
    },
    {
        "id": "calculados_herdeiro",
        "title": "Campos calculados (herdeiro)",
        "fields": [
            "honorarios_reservados",
            "percentual_nao_cedido",
            "percentual_detido_herdeiros",
            "sobra",
        ],
    },
]

_CONJUGE_KEYS = {
    f
    for sec in HERDEIRO_SECTIONS
    if sec["id"] in ("conjuge_herdeiro", "endereco_conjuge_herdeiro")
    for f in sec["fields"]
}


def herdeiro_field_keys() -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for sec in HERDEIRO_SECTIONS:
        for f in sec["fields"]:
            if f not in seen:
                seen.add(f)
                keys.append(f)
    return keys


def empty_herdeiro_dados() -> dict[str, str]:
    return {k: "" for k in herdeiro_field_keys()}


def field_label(key: str) -> str:
    return key.replace("_", " ").strip().capitalize()


def herdeiro_schema_payload() -> dict[str, Any]:
    sections = []
    for sec in HERDEIRO_SECTIONS:
        sections.append(
            {
                "id": sec["id"],
                "title": sec["title"],
                "collapsible": bool(sec.get("collapsible")),
                "fields": [{"key": f, "label": field_label(f)} for f in sec["fields"]],
            }
        )
    return {"sections": sections, "keys": herdeiro_field_keys()}


def _serialize_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bool):
        return "sim" if value else "nao"
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError):
            return str(value)
    text = str(value).strip()
    if text.lower() in ("none", "null", "nan"):
        return ""
    return text


def detect_tem_herdeiros(flag_text: Any) -> bool:
    """Interpreta texto Mongo herdeiros_habilitados."""
    text = _serialize_cell(flag_text).lower()
    if not text:
        return False
    if re.search(r"\b(n[aã]o|sem|nenhum|nenhuma|null)\b", text) and not re.search(
        r"\b(sim|habilit|sucessor)", text
    ):
        return False
    return bool(re.search(r"sim|habilit|sucessor|herdeir", text))


def _clean_mongo_text(raw: Any) -> str:
    """Extrai texto útil de {valor, ...} ou string, sem sufixo de evidência."""
    if raw is None:
        return ""
    if isinstance(raw, dict):
        if "valor" in raw:
            return _clean_mongo_text(raw.get("valor"))
        return ""
    text = _serialize_cell(raw)
    if not text:
        return ""
    for sep in (" (doc:", " (DOC:", "\n"):
        if sep in text:
            text = text.split(sep, 1)[0].strip()
    return text.strip()


def _parse_endereco_br(endereco: str) -> dict[str, str]:
    """Extrai CEP / cidade / UF / logradouro de endereço brasileiro livre."""
    text = (endereco or "").strip()
    out: dict[str, str] = {}
    if not text:
        return out

    cep_m = re.search(r"CEP[:\s]*([0-9]{5}-?[0-9]{3})", text, flags=re.IGNORECASE)
    if cep_m:
        out["cep"] = cep_m.group(1)
        text = (text[: cep_m.start()] + text[cep_m.end() :]).strip(" ,;-–")

    cidade_m = re.search(
        r"([A-Za-zÀ-ÿ .'-]+)\s*[-–/]\s*([A-Z]{2})\s*$",
        text,
    )
    if cidade_m:
        out["cidade"] = cidade_m.group(1).strip(" ,;-")
        out["estado"] = cidade_m.group(2).strip()
        text = text[: cidade_m.start()].strip(" ,;-–")

    # "nº 118" / "n. 118" / ", 118,"
    num_m = re.search(
        r"(?:,|\s)(?:n[º°o.\s]*)\s*([0-9A-Za-z\-]+)(?=,|\s|$)",
        text,
        flags=re.IGNORECASE,
    )
    if num_m:
        out["numero"] = num_m.group(1).strip()
        # Remove o trecho do número, mantém o restante como logradouro+bairro
        before = text[: num_m.start()].strip(" ,;-")
        after = text[num_m.end() :].strip(" ,;-")
        parts = [p.strip() for p in after.split(",") if p.strip()]
        if parts:
            out["bairro"] = parts[-1]
            meio = ", ".join(parts[:-1]).strip()
            out["logradouro"] = (before + (", " + meio if meio else "")).strip(" ,")
        else:
            out["logradouro"] = before
    else:
        out["logradouro"] = text
    return {k: v for k, v in out.items() if v}


def _percentual_from_obs(obs: str) -> str:
    """Tenta extrair percentual do crédito nas observações do herdeiro."""
    text = obs or ""
    m = re.search(
        r"(?:direito\s+a|quinh[aã]o\s+de|percentual)\s*[:=]?\s*([0-9]+(?:[.,][0-9]+)?\s*%)",
        text,
        flags=re.IGNORECASE,
    )
    if m:
        return m.group(1).replace(" ", "")
    m2 = re.search(r"([0-9]+(?:[.,][0-9]+)?\s*%)\s*(?:do\s+cr[eé]dito)?", text, flags=re.IGNORECASE)
    if m2:
        return m2.group(1).replace(" ", "")
    return ""


def map_mongo_herdeiro_item(item: dict[str, Any], ordem: int = 1) -> dict[str, Any]:
    """Converte um item de quadro_parecer.dados_credor.herdeiros.itens -> formulário."""
    dados = empty_herdeiro_dados()
    if not isinstance(item, dict):
        return {"id": None, "ordem": ordem, "validado": False, "dados": dados}

    def g(*keys: str) -> str:
        for k in keys:
            if k in item and item.get(k) is not None:
                val = _clean_mongo_text(item.get(k))
                if val:
                    return val
        return ""

    nome = g("nome", "nome_herdeiro")
    cpf = g("cpf_cnpj", "cpf", "cpf_herdeiro")
    parentesco = g("parentesco", "parentesco_herdeiro")
    habilitado = g("habilitado", "herdeiro_habilitado")
    doc_pessoal = g("doc_pessoal", "documento_pessoal", "rg", "rg_herdeiro")
    nascimento = g("nascimento", "data_nascimento", "data_de_nascimento")
    estado_civil = g("estado_civil")
    email = g("email", "e_mail")
    telefone = g("telefone", "fone")
    cidade = g("cidade", "cidade_residencia")
    endereco = g("endereco", "endereco_completo")
    observacoes = g("observacoes", "obs")
    ocupacao = g("ocupacao", "profissao")
    nacionalidade = g("nacionalidade")

    if nome:
        dados["nome_herdeiro"] = nome
    if cpf:
        dados["cpf_herdeiro"] = cpf
    if parentesco:
        dados["parentesco_herdeiro"] = parentesco
    if habilitado:
        dados["herdeiro_habilitado"] = habilitado
    elif nome:
        dados["herdeiro_habilitado"] = "Sim"
    if doc_pessoal:
        dados["rg_herdeiro"] = doc_pessoal
    if nascimento:
        dados["data_de_nascimento_herdeiro"] = nascimento
    if estado_civil:
        dados["estado_civil_herdeiro"] = estado_civil
    if email:
        dados["email_herdeiro"] = email
    if telefone:
        dados["telefone_herdeiro"] = telefone
    if ocupacao:
        dados["ocupacao_herdeiro"] = ocupacao
    if nacionalidade:
        dados["nacionalidade_herdeiro"] = nacionalidade

    pct = g("percentual", "percentual_detido", "quinhao") or _percentual_from_obs(observacoes)
    if pct:
        dados["percentual_detido"] = pct

    parsed = _parse_endereco_br(endereco)
    if parsed.get("cep"):
        dados["cep_herdeiro"] = parsed["cep"]
    if parsed.get("logradouro"):
        dados["logradouro_herdeiro"] = parsed["logradouro"]
    if parsed.get("numero"):
        dados["numero_logradouro_herdeiro"] = parsed["numero"]
    if parsed.get("bairro"):
        dados["bairro_herdeiro"] = parsed["bairro"]
    if parsed.get("cidade"):
        dados["cidade_herdeiro"] = parsed["cidade"]
    elif cidade:
        # "Águas de Lindóia/SP" → cidade + UF
        cm = re.match(r"^(.+?)\s*[/–-]\s*([A-Z]{2})\s*$", cidade)
        if cm:
            dados["cidade_herdeiro"] = cm.group(1).strip()
            dados["estado_herdeiro"] = cm.group(2).strip()
        else:
            dados["cidade_herdeiro"] = cidade
    if parsed.get("estado"):
        dados["estado_herdeiro"] = parsed["estado"]

    # Guarda data de habilitação / obs no campo mencionar (livre)
    extras = []
    data_hab = g("data_habilitacao")
    if data_hab:
        extras.append(f"Habilitação: {data_hab}")
    if observacoes:
        extras.append(observacoes)
    if extras:
        dados["mencionar"] = " | ".join(extras)

    return {
        "id": None,
        "ordem": ordem,
        "validado": False,
        "dados": dados,
    }


def extract_herdeiros_from_parecer(dados_credor: Any) -> tuple[list[dict[str, Any]], str, bool]:
    """
    Lê quadro_parecer.dados_credor e devolve:
    (lista_herdeiros_form, flag_texto, tem_herdeiros).
    """
    if not isinstance(dados_credor, dict):
        return [], "", False

    flag_block = dados_credor.get("herdeiros_habilitados")
    flag_texto = _clean_mongo_text(flag_block)
    if not flag_texto and isinstance(flag_block, dict):
        flag_texto = _clean_mongo_text(flag_block.get("valor"))

    herdeiros_block = dados_credor.get("herdeiros")
    itens: list[Any] = []
    if isinstance(herdeiros_block, dict):
        raw_itens = herdeiros_block.get("itens")
        if isinstance(raw_itens, list):
            itens = raw_itens
        if not flag_texto:
            flag_texto = _clean_mongo_text(herdeiros_block.get("valor"))
    elif isinstance(herdeiros_block, list):
        itens = herdeiros_block

    # Fallback: itens em herdeiros_habilitados
    if not itens and isinstance(flag_block, dict):
        raw_itens = flag_block.get("itens")
        if isinstance(raw_itens, list):
            itens = raw_itens

    mapped: list[dict[str, Any]] = []
    for i, item in enumerate(itens, start=1):
        if isinstance(item, dict):
            mapped.append(map_mongo_herdeiro_item(item, ordem=i))

    qtd = 0
    if isinstance(herdeiros_block, dict):
        try:
            qtd = int(herdeiros_block.get("quantidade") or 0)
        except (TypeError, ValueError):
            qtd = 0

    tem = bool(mapped) or qtd > 0 or detect_tem_herdeiros(flag_texto)
    # Óbito do credor também indica possível sucessão
    obito = _clean_mongo_text(dados_credor.get("obito"))
    if obito and not re.search(r"\b(n[aã]o|sem|nenhum)\b", obito.lower()):
        tem = True
        if not flag_texto:
            flag_texto = f"Óbito do credor: {obito}"

    return mapped, flag_texto, tem


def ensure_herdeiro_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pre_analise_credor (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ficha_id BIGINT NOT NULL,
            cumprimento_de_sentenca VARCHAR(200) NOT NULL,
            incidente VARCHAR(50) NOT NULL,
            nome_credor VARCHAR(500) NULL,
            cpf_credor VARCHAR(40) NULL,
            herdeiros_habilitados TINYINT(1) NOT NULL DEFAULT 0,
            herdeiros_flag_texto TEXT NULL,
            validado TINYINT(1) NOT NULL DEFAULT 0,
            criado_em DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            atualizado_em DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),
            UNIQUE KEY uq_credor_ficha (ficha_id),
            UNIQUE KEY uq_credor_proc_inc (cumprimento_de_sentenca, incidente),
            INDEX idx_credor_cpf (cpf_credor),
            CONSTRAINT fk_credor_ficha
                FOREIGN KEY (ficha_id) REFERENCES pre_analise_ficha(id)
                ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pre_analise_herdeiro (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            id_credor BIGINT NOT NULL,
            ordem INT NOT NULL DEFAULT 1,
            nome_herdeiro VARCHAR(500) NULL,
            cpf_herdeiro VARCHAR(40) NULL,
            dados JSON NOT NULL,
            validado TINYINT(1) NOT NULL DEFAULT 0,
            criado_em DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            atualizado_em DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),
            INDEX idx_herdeiro_credor (id_credor, ordem),
            CONSTRAINT fk_herdeiro_credor
                FOREIGN KEY (id_credor) REFERENCES pre_analise_credor(id)
                ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pre_analise_herdeiro_conjuge (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            id_herdeiro BIGINT NOT NULL,
            dados JSON NOT NULL,
            criado_em DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            atualizado_em DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),
            UNIQUE KEY uq_conjuge_herdeiro (id_herdeiro),
            CONSTRAINT fk_conjuge_herdeiro
                FOREIGN KEY (id_herdeiro) REFERENCES pre_analise_herdeiro(id)
                ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def _parse_json_field(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def load_herdeiros_for_ficha(cur, ficha_id: int | None) -> dict[str, Any]:
    """Retorna {id_credor, herdeiros_habilitados, herdeiros: [...]}."""
    empty = {
        "id_credor": None,
        "herdeiros_habilitados": False,
        "herdeiros_flag_texto": "",
        "credor_validado": False,
        "herdeiros": [],
    }
    if not ficha_id:
        return empty
    cur.execute(
        """
        SELECT id, herdeiros_habilitados, herdeiros_flag_texto, validado,
               nome_credor, cpf_credor
        FROM pre_analise_credor
        WHERE ficha_id = %s
        LIMIT 1
        """,
        (ficha_id,),
    )
    credor = cur.fetchone()
    if not credor:
        return empty
    id_credor = int(credor["id"])
    cur.execute(
        """
        SELECT id, ordem, nome_herdeiro, cpf_herdeiro, dados, validado
        FROM pre_analise_herdeiro
        WHERE id_credor = %s
        ORDER BY ordem ASC, id ASC
        """,
        (id_credor,),
    )
    rows = cur.fetchall() or []
    herdeiros: list[dict[str, Any]] = []
    for row in rows:
        dados = empty_herdeiro_dados()
        stored = _parse_json_field(row.get("dados"))
        for k in herdeiro_field_keys():
            if k in stored:
                dados[k] = _serialize_cell(stored.get(k))
        if row.get("nome_herdeiro"):
            dados["nome_herdeiro"] = _serialize_cell(row.get("nome_herdeiro"))
        if row.get("cpf_herdeiro"):
            dados["cpf_herdeiro"] = _serialize_cell(row.get("cpf_herdeiro"))

        herdeiro_id = int(row["id"])
        cur.execute(
            "SELECT dados FROM pre_analise_herdeiro_conjuge WHERE id_herdeiro = %s LIMIT 1",
            (herdeiro_id,),
        )
        conj = cur.fetchone()
        if conj:
            conj_dados = _parse_json_field(conj.get("dados"))
            for k in _CONJUGE_KEYS:
                if k in conj_dados:
                    dados[k] = _serialize_cell(conj_dados.get(k))

        herdeiros.append(
            {
                "id": herdeiro_id,
                "ordem": int(row.get("ordem") or len(herdeiros) + 1),
                "validado": bool(row.get("validado")),
                "dados": dados,
            }
        )

    return {
        "id_credor": id_credor,
        "herdeiros_habilitados": bool(credor.get("herdeiros_habilitados")),
        "herdeiros_flag_texto": _serialize_cell(credor.get("herdeiros_flag_texto")),
        "credor_validado": bool(credor.get("validado")),
        "herdeiros": herdeiros,
    }


def upsert_credor_e_herdeiros(
    cur,
    *,
    ficha_id: int,
    cumprimento: str,
    incidente: str,
    nome_credor: str | None,
    cpf_credor: str | None,
    herdeiros_habilitados: bool,
    herdeiros_flag_texto: str | None,
    herdeiros: list[dict[str, Any]],
    validar_herdeiros: bool,
) -> dict[str, Any]:
    """
    Upsert credor + substitui lista de herdeiros.
    validar_herdeiros=True → validado=1; False → rascunho validado=0.
    """
    flag = 1 if herdeiros_habilitados or bool(herdeiros) else 0
    validado = 1 if validar_herdeiros else 0

    cur.execute(
        """
        INSERT INTO pre_analise_credor (
            ficha_id, cumprimento_de_sentenca, incidente,
            nome_credor, cpf_credor, herdeiros_habilitados,
            herdeiros_flag_texto, validado
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            nome_credor = VALUES(nome_credor),
            cpf_credor = VALUES(cpf_credor),
            herdeiros_habilitados = VALUES(herdeiros_habilitados),
            herdeiros_flag_texto = VALUES(herdeiros_flag_texto),
            validado = VALUES(validado),
            atualizado_em = CURRENT_TIMESTAMP(6)
        """,
        (
            ficha_id,
            cumprimento,
            incidente,
            (nome_credor or "").strip() or None,
            (cpf_credor or "").strip() or None,
            flag,
            (herdeiros_flag_texto or "").strip() or None,
            validado,
        ),
    )
    cur.execute(
        "SELECT id FROM pre_analise_credor WHERE ficha_id = %s LIMIT 1",
        (ficha_id,),
    )
    row = cur.fetchone() or {}
    id_credor = int(row["id"])

    cur.execute("SELECT id FROM pre_analise_herdeiro WHERE id_credor = %s", (id_credor,))
    old_ids = [int(r["id"]) for r in (cur.fetchall() or [])]
    if old_ids:
        placeholders = ", ".join(["%s"] * len(old_ids))
        cur.execute(
            f"DELETE FROM pre_analise_herdeiro_conjuge WHERE id_herdeiro IN ({placeholders})",
            tuple(old_ids),
        )
        cur.execute(
            "DELETE FROM pre_analise_herdeiro WHERE id_credor = %s",
            (id_credor,),
        )

    saved: list[dict[str, Any]] = []
    for index, item in enumerate(herdeiros or [], start=1):
        if not isinstance(item, dict):
            continue
        raw = item.get("dados") if isinstance(item.get("dados"), dict) else item
        dados = empty_herdeiro_dados()
        for k in herdeiro_field_keys():
            if k in raw:
                dados[k] = _serialize_cell(raw.get(k))
        nome = dados.get("nome_herdeiro") or ""
        cpf = dados.get("cpf_herdeiro") or ""
        if not any(dados.values()) and not validar_herdeiros:
            continue

        conjuge_dados = {k: dados.get(k, "") for k in _CONJUGE_KEYS}
        herdeiro_only = {k: v for k, v in dados.items() if k not in _CONJUGE_KEYS}

        cur.execute(
            """
            INSERT INTO pre_analise_herdeiro (
                id_credor, ordem, nome_herdeiro, cpf_herdeiro, dados, validado
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                id_credor,
                int(item.get("ordem") or index),
                nome or None,
                cpf or None,
                json.dumps(herdeiro_only, ensure_ascii=False),
                validado,
            ),
        )
        herdeiro_id = int(cur.lastrowid)
        if any(conjuge_dados.values()):
            cur.execute(
                """
                INSERT INTO pre_analise_herdeiro_conjuge (id_herdeiro, dados)
                VALUES (%s, %s)
                """,
                (herdeiro_id, json.dumps(conjuge_dados, ensure_ascii=False)),
            )
        saved.append(
            {
                "id": herdeiro_id,
                "ordem": int(item.get("ordem") or index),
                "validado": bool(validado),
                "dados": dados,
            }
        )

    return {
        "id_credor": id_credor,
        "herdeiros_habilitados": bool(flag),
        "herdeiros": saved,
        "validado": bool(validado),
    }
