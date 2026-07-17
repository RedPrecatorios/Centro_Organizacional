"""
Ficha editável da PRÉ Análise Processual.

Prioridade de carga:
1. Registo local (pre_analise_ficha) — se o utilizador já salvou
2. MongoDB (se configurado) — preenche campos
3. precainfosnew (flaskdb) — preenche só o que ainda estiver vazio
4. Em branco

Ao salvar: upsert na tabela local plataforma_central.pre_analise_ficha.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import mysql.connector

# ---------------------------------------------------------------------------
# Schema do formulário (ordem = UI)
# ---------------------------------------------------------------------------

FIELD_SECTIONS: list[dict[str, Any]] = [
    {
        "id": "processo",
        "title": "1. Informações do processo",
        "fields": [
            "cumprimento_de_sentenca",
            "incidente",
            "entidade_devedora",
            "numero_processo_principal",
            "vara",
            "foro",
            "ordem_cronologica",
            "controle",
            "depre",
            "ep",
            "expedicao_oficio_requisitorio",
            "procuradoria",
            "procuradoria_tipo",
        ],
    },
    {
        "id": "credor",
        "title": "2. Credor (beneficiário originário)",
        "fields": [
            "nome_credor",
            "cpf_credor",
            "rg_credor",
            "uf_credor",
            "data_de_nascimento_credor",
            "estado_civil_credor",
            "regime_conjuge_credor",
            "nacionalidade_credor",
            "ocupacao_credor",
            "email_credor",
            "telefone_credor",
            "credor_falecido",
            "data_do_obito",
            "anuente_credor",
            "exequente",
            "complemento_credor",
        ],
    },
    {
        "id": "endereco_credor",
        "title": "3. Endereço do credor",
        "fields": [
            "cep_credor",
            "logradouro_credor",
            "numero_logradouro_credor",
            "bairro_credor",
            "cidade_credor",
            "estado_credor",
        ],
    },
    {
        "id": "banco_credor",
        "title": "4. Dados bancários do credor",
        "fields": [
            "codigo_banco_credor",
            "banco_credor",
            "agencia_credor",
            "conta_credor",
            "tipo_conta_credor",
            "tem_chave_pix_credor",
            "chave_pix_credor",
        ],
    },
    {
        "id": "conjuge_credor",
        "title": "5. Cônjuge do credor",
        "fields": [
            "nome_conjuge_credor",
            "cpf_conjuge_credor",
            "rg_conjuge_credor",
            "uf_conjuge_credor",
            "data_de_nascimento_conjuge_credor",
            "email_conjuge_credor",
            "telefone_conjuge_credor",
            "nacionalidade_conjuge_credor",
            "ocupacao_conjuge_credor",
        ],
    },
    {
        "id": "endereco_conjuge_credor",
        "title": "6. Endereço do cônjuge do credor",
        "fields": [
            "cep_conjuge_credor",
            "logradouro_conjuge_credor",
            "numero_logradouro_conjuge_credor",
            "bairro_conjuge_credor",
            "cidade_conjuge_credor",
            "estado_conjuge_credor",
        ],
    },
    {
        "id": "banco_conjuge_credor",
        "title": "7. Dados bancários do cônjuge do credor",
        "fields": [
            "codigo_banco_conjuge_credor",
            "banco_conjuge_credor",
            "agencia_conjuge_credor",
            "conta_conjuge_credor",
            "tipo_conta_conjuge_credor",
            "tem_chave_pix_conjuge_credor",
            "chave_pix_conjuge_credor",
        ],
    },
    {
        "id": "anuente",
        "title": "8. Anuente do credor",
        "fields": [
            "nome_anuente_credor",
            "cpf_anuente_credor",
            "rg_anuente_credor",
            "uf_anuente_credor",
            "interveniente_anuente",
            "estado_civil_anuente_credor",
            "nacionalidade_anuente_credor",
        ],
    },
    {
        "id": "endereco_anuente",
        "title": "9. Endereço do anuente",
        "fields": [
            "cep_anuente_credor",
            "logradouro_anuente_credor",
            "numero_logradouro_anuente_credor",
            "bairro_anuente_credor",
            "cidade_anuente_credor",
            "estado_anuente_credor",
        ],
    },
    {
        "id": "valores_expedidos",
        "title": "10. Valores expedidos",
        "fields": [
            "principal_liquido",
            "juros_moratorio",
            "juros_compensatorio",
            "descontos_previdenciarios",
            "descontos_de_assistencia_medica",
            "custas",
            "total_da_requisicao",
        ],
    },
    {
        "id": "valores_atualizados",
        "title": "11. Valores atualizados / negociação",
        "fields": [
            "percentual_honorarios",
            "percentual_de_compra",
            "data_base",
            "preco_de_compra",
            "valor_liquido_do_oficio",
        ],
    },
    {
        "id": "cessionaria",
        "title": "12. Cessionária",
        "fields": ["cessionaria", "template"],
    },
    {
        "id": "advogado",
        "title": "13. Advogado",
        "fields": [
            "nome_advogado",
            "cpf_advogado",
            "cep_advogado",
            "logradouro_advogado",
            "numero_logradouro_advogado",
            "bairro_advogado",
            "cidade_advogado",
            "estado_advogado",
        ],
    },
    {
        "id": "metadados",
        "title": "20. Metadados do processo",
        "fields": [
            "qtd_herdeiros",
            "recupere",
            "incluido_por",
            "incluido_em",
            "alterado_por",
            "alterado_em",
        ],
    },
]

# Mapeamento: campo formulário -> possíveis chaves no Mongo / aliases
_MONGO_ALIASES: dict[str, tuple[str, ...]] = {
    "cumprimento_de_sentenca": (
        "cumprimento_de_sentenca",
        "numero_cumprimento",
        "Numero_de_Processo",
        "numero_de_processo",
    ),
    "incidente": ("incidente", "numero_incidente", "Numero_do_Incidente", "numero_do_incidente"),
    "juros_compensatorio": ("juros_compensatorio", "juros_compensatorios", "Juros_Compensatorios"),
}

# precainfosnew (flaskdb) -> formulário
_PRECAINFOS_MAP: dict[str, tuple[str, ...]] = {
    "cumprimento_de_sentenca": ("Numero_de_Processo", "numero_de_processo"),
    "incidente": ("Numero_do_Incidente", "numero_do_incidente"),
    "entidade_devedora": ("Entidade_Devedora", "entidade_devedora"),
    "numero_processo_principal": ("Processo_Principal", "processo_principal"),
    "vara": ("Vara", "vara"),
    "foro": ("Foro", "foro"),
    "ordem_cronologica": ("Ordem", "ordem"),
    "controle": ("Controle", "controle"),
    "depre": ("DEPRE", "Depre", "depre"),
    "ep": ("EP", "ep"),
    "nome_credor": ("Requerente", "Cabeca_da_Acao", "requerente"),
    "cpf_credor": ("CPF", "cpf"),
    "data_de_nascimento_credor": ("Data_de_Nascimento", "data_de_nascimento"),
    "principal_liquido": ("Principal_Liquido", "principal_liquido"),
    "juros_moratorio": ("Juros_Moratorio", "juros_moratorio"),
    "juros_compensatorio": ("Juros_Compensatorios", "juros_compensatorios"),
    "data_base": ("Data_Base", "data_base"),
    "nome_advogado": ("Advogado", "ADVOGADO_CREDOR", "advogado"),
    "cpf_advogado": ("CPF_CNPJ", "cpf_cnpj"),
    "total_da_requisicao": ("Valor_Requisitado", "Valor_Global", "valor_requisitado"),
    "valor_liquido_do_oficio": ("Valor_Negociavel", "Calculo_Atualizado", "valor_negociavel"),
}


def all_field_keys() -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for sec in FIELD_SECTIONS:
        for f in sec["fields"]:
            if f not in seen:
                seen.add(f)
                keys.append(f)
    return keys


def empty_dados() -> dict[str, str]:
    return {k: "" for k in all_field_keys()}


def field_label(key: str) -> str:
    return key.replace("_", " ").strip().capitalize()


def schema_payload() -> dict[str, Any]:
    from messages_viewer.pre_analise_herdeiros import herdeiro_schema_payload

    sections = []
    for sec in FIELD_SECTIONS:
        sections.append(
            {
                "id": sec["id"],
                "title": sec["title"],
                "fields": [
                    {"key": f, "label": field_label(f)} for f in sec["fields"]
                ],
            }
        )
    return {
        "sections": sections,
        "keys": all_field_keys(),
        "herdeiros": herdeiro_schema_payload(),
    }


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _env_strip(raw: str | None) -> str:
    return (raw or "").strip().strip("'\"")


def _eda_mysql_config() -> dict | None:
    name = _env_strip(os.getenv("EDA_MYSQL_DATABASE") or "plataforma_central")
    if not name:
        return None
    try:
        port = int(_env_strip(os.getenv("EDA_MYSQL_PORT") or "3306") or "3306")
    except ValueError:
        port = 3306
    return {
        "host": _env_strip(os.getenv("EDA_MYSQL_HOST") or "127.0.0.1"),
        "port": port,
        "database": name,
        "user": _env_strip(os.getenv("EDA_MYSQL_USER") or "root"),
        "password": os.getenv("EDA_MYSQL_PASSWORD", "") or "",
        "connection_timeout": 15,
    }


def _flask_mysql_config() -> dict | None:
    name = _env_strip(os.getenv("FLASK_MYSQL_DATABASE"))
    if not name:
        return None
    try:
        port = int(_env_strip(os.getenv("FLASK_MYSQL_PORT") or "3306") or "3306")
    except ValueError:
        port = 3306
    return {
        "host": _env_strip(os.getenv("FLASK_MYSQL_HOST") or "127.0.0.1"),
        "port": port,
        "database": name,
        "user": _env_strip(os.getenv("FLASK_MYSQL_USER") or "root"),
        "password": _env_strip(os.getenv("FLASK_MYSQL_PASSWORD")),
        "connection_timeout": 15,
    }


def _db_connect():
    cfg = _eda_mysql_config()
    if not cfg:
        raise RuntimeError("MySQL da plataforma não configurado (EDA_MYSQL_*).")
    return mysql.connector.connect(**cfg, charset="utf8mb4", collation="utf8mb4_unicode_ci")


def _ensure_table(cur) -> None:
    from messages_viewer.pre_analise_herdeiros import ensure_herdeiro_tables

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pre_analise_ficha (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            caso_id CHAR(36) NULL,
            cumprimento_de_sentenca VARCHAR(200) NOT NULL,
            incidente VARCHAR(50) NOT NULL,
            id_externo VARCHAR(64) NULL,
            dados JSON NOT NULL,
            fontes JSON NULL,
            criado_em DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            atualizado_em DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),
            criado_por_user_id INT NULL,
            alterado_por_user_id INT NULL,
            incluido_por VARCHAR(200) NULL,
            alterado_por VARCHAR(200) NULL,
            UNIQUE KEY uq_pre_analise_ficha_proc_inc (cumprimento_de_sentenca, incidente),
            INDEX idx_caso_id (caso_id),
            INDEX idx_id_externo (id_externo)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    ensure_herdeiro_tables(cur)


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


def _is_blank(value: Any) -> bool:
    return _serialize_cell(value) == ""


def _merge_fill(target: dict[str, str], source: dict[str, Any]) -> list[str]:
    """Preenche apenas chaves vazias em target. Retorna chaves preenchidas."""
    filled: list[str] = []
    for key in all_field_keys():
        if not _is_blank(target.get(key)):
            continue
        if key not in source:
            continue
        val = _serialize_cell(source.get(key))
        if val:
            target[key] = val
            filled.append(key)
    return filled


def _pick_from_doc(doc: dict[str, Any], *candidates: str) -> Any:
    if not doc:
        return None
    lower = {str(k).lower(): k for k in doc.keys()}
    for c in candidates:
        if c in doc and not _is_blank(doc.get(c)):
            return doc.get(c)
        lk = c.lower()
        if lk in lower:
            real = lower[lk]
            if not _is_blank(doc.get(real)):
                return doc.get(real)
    return None


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------


def mongo_configured() -> bool:
    return bool(_env_strip(os.getenv("PRE_ANALISE_MONGO_URI") or os.getenv("MONGO_URI")))


def _clean_parecer_valor(raw: Any) -> str:
    """Extrai o texto útil de campos no formato {valor, fonte, evidencias}."""
    if raw is None:
        return ""
    if isinstance(raw, dict):
        if "valor" in raw:
            return _clean_parecer_valor(raw.get("valor"))
        return ""
    text = _serialize_cell(raw)
    if not text:
        return ""
    # Remove sufixo de evidência: "NOME (doc: arquivo.pdf, pág. 1)"
    for sep in (" (doc:", " (DOC:", "\n"):
        if sep in text:
            text = text.split(sep, 1)[0].strip()
    return text.strip()


def _parecer_block_values(block: Any) -> dict[str, str]:
    """Converte um bloco do quadro_parecer (dados_credor, etc.) em dict plano."""
    out: dict[str, str] = {}
    if not isinstance(block, dict):
        return out
    for k, v in block.items():
        val = _clean_parecer_valor(v)
        if val:
            out[str(k)] = val
    return out


def _map_parecer_doc(doc: dict[str, Any]) -> dict[str, Any]:
    """Mapeia documento de pareceres_redator -> campos do formulário."""
    out: dict[str, Any] = {}
    # Top-level
    top_map = {
        "cumprimento_de_sentenca": ("numero_cumprimento", "cumprimento_de_sentenca"),
        "incidente": ("numero_incidente", "incidente"),
        "nome_credor": ("nome_credor",),
        "depre": ("numero_depre", "depre"),
        "numero_processo_principal": ("numero_processo_principal",),
        "id_externo": ("id_externo",),
    }
    for form_key, aliases in top_map.items():
        val = _pick_from_doc(doc, *aliases)
        cleaned = _clean_parecer_valor(val)
        if cleaned:
            out[form_key] = cleaned

    qp = doc.get("quadro_parecer") if isinstance(doc.get("quadro_parecer"), dict) else {}
    credor = _parecer_block_values(qp.get("dados_credor"))
    processo = _parecer_block_values(qp.get("dados_processo"))
    valores = _parecer_block_values(qp.get("valores"))
    cessionaria = _parecer_block_values(qp.get("cessionaria"))

    nested_map: list[tuple[str, dict[str, str], tuple[str, ...]]] = [
        ("nome_credor", credor, ("credor_originario",)),
        ("cpf_credor", credor, ("cpf",)),
        ("estado_civil_credor", credor, ("estado_civil",)),
        ("regime_conjuge_credor", credor, ("regime_bens",)),
        ("data_de_nascimento_credor", credor, ("idade", "data_nascimento", "data_de_nascimento")),
        ("rg_credor", credor, ("documento_pessoal", "rg")),
        ("cidade_credor", credor, ("cidade_residencia", "cidade")),
        ("complemento_credor", credor, ("observacoes_complementares", "homonimo")),
        ("numero_processo_principal", processo, ("processo_principal",)),
        ("cumprimento_de_sentenca", processo, ("cumprimento_sentenca",)),
        ("incidente", processo, ("incidente",)),
        ("depre", processo, ("depre",)),
        ("entidade_devedora", processo, ("ente_devedor",)),
        ("vara", processo, ("vara",)),
        ("nome_advogado", processo, ("advogado_originario", "advogado")),
        ("expedicao_oficio_requisitorio", processo, ("oficio_requisitorio",)),
        ("principal_liquido", valores, ("valor_expedido_oficio", "valor_atualizado_planilha")),
        ("preco_de_compra", valores, ("valor_proposta", "valor_atualizado_comprador")),
        ("percentual_de_compra", valores, ("percentual_proposta",)),
        ("percentual_honorarios", valores, ("percentual_honorarios",)),
        ("valor_liquido_do_oficio", valores, ("valor_blip", "saldo_tjsp", "valor_atualizado_planilha")),
        ("cessionaria", cessionaria, ("cessionaria", "nome", "nome_cessionaria")),
        ("template", cessionaria, ("template",)),
    ]
    for form_key, block, aliases in nested_map:
        if form_key in out and not _is_blank(out.get(form_key)):
            continue
        for alias in aliases:
            if alias in block and block[alias]:
                out[form_key] = block[alias]
                break

    # Flag de herdeiros (não vai para o form principal)
    herdeiros_flag = credor.get("herdeiros_habilitados") or ""
    from messages_viewer.pre_analise_herdeiros import detect_tem_herdeiros

    meta = {
        "tem_herdeiros": detect_tem_herdeiros(herdeiros_flag),
        "herdeiros_flag_texto": herdeiros_flag,
    }

    # Também tenta aliases genéricos no doc plano
    flat: dict[str, Any] = dict(doc)
    flat.update(credor)
    flat.update(processo)
    flat.update(valores)
    for key in all_field_keys():
        if key in out and not _is_blank(out.get(key)):
            continue
        aliases = _MONGO_ALIASES.get(key, (key,))
        val = _pick_from_doc(flat, *aliases)
        cleaned = _clean_parecer_valor(val)
        if cleaned:
            out[key] = cleaned
    return {"dados": out, "meta": meta}


def _mongo_fetch(
    cumprimento: str,
    incidente: str,
    id_externo: str | None = None,
) -> dict[str, Any] | None:
    uri = _env_strip(os.getenv("PRE_ANALISE_MONGO_URI") or os.getenv("MONGO_URI"))
    if not uri:
        return None
    db_name = _env_strip(
        os.getenv("PRE_ANALISE_MONGO_DB") or os.getenv("MONGO_DB") or "pa_juridica"
    )
    coll_name = _env_strip(
        os.getenv("PRE_ANALISE_MONGO_COLLECTION")
        or os.getenv("MONGO_COLLECTION")
        or "pareceres_redator"
    )
    try:
        from pymongo import MongoClient
    except ImportError:
        return None

    client = None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        coll = client[db_name][coll_name]
        queries: list[dict[str, Any]] = []
        if id_externo:
            queries.append({"id_externo": str(id_externo)})
        queries.append(
            {
                "numero_cumprimento": cumprimento,
                "numero_incidente": str(incidente),
            }
        )
        queries.append(
            {
                "$or": [
                    {"cumprimento_de_sentenca": cumprimento, "incidente": str(incidente)},
                    {"Numero_de_Processo": cumprimento, "Numero_do_Incidente": str(incidente)},
                ]
            }
        )
        doc = None
        for q in queries:
            doc = coll.find_one(q)
            if doc:
                break
        if not doc:
            return None
        mapped = _map_parecer_doc(doc)
        if not mapped or not mapped.get("dados"):
            return {
                "dados": {},
                "meta": (mapped or {}).get("meta") or {"tem_herdeiros": False, "herdeiros_flag_texto": ""},
            }
        return mapped
    except Exception:
        return None
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# precainfosnew
# ---------------------------------------------------------------------------


def _precainfos_fetch(cumprimento: str, incidente: str) -> dict[str, Any] | None:
    cfg = _flask_mysql_config()
    if not cfg:
        return None
    conn = None
    try:
        conn = mysql.connector.connect(**cfg, charset="utf8mb4", collation="utf8mb4_unicode_ci")
        cur = conn.cursor(dictionary=True)
        cur.execute("SHOW TABLES LIKE 'precainfosnew'")
        if not cur.fetchone():
            return None
        cur.execute("SHOW COLUMNS FROM `precainfosnew`")
        cols = {str(r.get("Field") or r.get("field") or "") for r in (cur.fetchall() or [])}
        cols.discard("")
        lower = {c.lower(): c for c in cols}

        def col(*names: str) -> str | None:
            for n in names:
                if n in cols:
                    return n
                if n.lower() in lower:
                    return lower[n.lower()]
            return None

        f_proc = col("Numero_de_Processo", "numero_de_processo")
        f_inc = col("Numero_do_Incidente", "numero_do_incidente")
        if not f_proc:
            return None

        if f_inc:
            cur.execute(
                f"""
                SELECT * FROM precainfosnew
                WHERE TRIM(COALESCE(`{f_proc}`, '')) = %s
                  AND TRIM(COALESCE(`{f_inc}`, '')) = %s
                ORDER BY id DESC LIMIT 1
                """,
                (cumprimento, incidente),
            )
        else:
            cur.execute(
                f"""
                SELECT * FROM precainfosnew
                WHERE TRIM(COALESCE(`{f_proc}`, '')) = %s
                ORDER BY id DESC LIMIT 1
                """,
                (cumprimento,),
            )
        row = cur.fetchone()
        if not row:
            return None

        out: dict[str, Any] = {}
        for form_key, candidates in _PRECAINFOS_MAP.items():
            for cand in candidates:
                real = col(cand)
                if real and real in row and not _is_blank(row.get(real)):
                    out[form_key] = row.get(real)
                    break
        return out or None
    except Exception:
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Local table
# ---------------------------------------------------------------------------


def _local_fetch(cumprimento: str, incidente: str) -> dict[str, Any] | None:
    conn = None
    try:
        conn = _db_connect()
        cur = conn.cursor(dictionary=True)
        _ensure_table(cur)
        cur.execute(
            """
            SELECT id, dados, incluido_por, alterado_por, criado_em, atualizado_em,
                   criado_por_user_id, alterado_por_user_id, caso_id, id_externo
            FROM pre_analise_ficha
            WHERE cumprimento_de_sentenca = %s AND incidente = %s
            LIMIT 1
            """,
            (cumprimento, incidente),
        )
        row = cur.fetchone()
        if not row:
            return None
        raw = row.get("dados")
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="replace")
        if isinstance(raw, str):
            try:
                dados = json.loads(raw)
            except json.JSONDecodeError:
                dados = {}
        elif isinstance(raw, dict):
            dados = raw
        else:
            dados = {}

        from messages_viewer.pre_analise_herdeiros import load_herdeiros_for_ficha

        herdeiros_pack = load_herdeiros_for_ficha(cur, int(row["id"]))
        return {
            "id": int(row["id"]),
            "dados": dados,
            "incluido_por": row.get("incluido_por"),
            "alterado_por": row.get("alterado_por"),
            "criado_em": row.get("criado_em"),
            "atualizado_em": row.get("atualizado_em"),
            "caso_id": row.get("caso_id"),
            "id_externo": row.get("id_externo"),
            "herdeiros_pack": herdeiros_pack,
        }
    except Exception:
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def carregar_ficha(
    *,
    cumprimento_de_sentenca: str,
    incidente: str,
    caso_id: str | None = None,
    id_externo: str | None = None,
    nome_credor_hint: str | None = None,
    depre_hint: str | None = None,
) -> tuple[dict, int]:
    from messages_viewer.pre_analise_herdeiros import empty_herdeiro_dados

    cumprimento = (cumprimento_de_sentenca or "").strip()
    incidente = (incidente or "").strip()
    if not cumprimento or not incidente:
        return (
            {"ok": False, "error": "cumprimento_de_sentenca e incidente são obrigatórios."},
            400,
        )

    dados = empty_dados()
    fontes: dict[str, str] = {}
    avisos: list[str] = []
    tem_herdeiros_mongo = False
    herdeiros_flag_texto = ""

    dados["cumprimento_de_sentenca"] = cumprimento
    dados["incidente"] = incidente
    if nome_credor_hint:
        dados["nome_credor"] = str(nome_credor_hint).strip()
        fontes["nome_credor"] = "lista"
    if depre_hint:
        dados["depre"] = str(depre_hint).strip()
        fontes["depre"] = "lista"

    mongo_pack = _mongo_fetch(cumprimento, incidente, id_externo)
    if mongo_pack:
        mongo_dados = mongo_pack.get("dados") or {}
        meta = mongo_pack.get("meta") or {}
        tem_herdeiros_mongo = bool(meta.get("tem_herdeiros"))
        herdeiros_flag_texto = str(meta.get("herdeiros_flag_texto") or "")
        for k in _merge_fill(dados, mongo_dados):
            fontes[k] = "mongodb"
    elif mongo_configured():
        avisos.append("MongoDB configurado, mas nenhum documento encontrado para este processo.")
    else:
        avisos.append("MongoDB não configurado (PRE_ANALISE_MONGO_URI).")

    preca = _precainfos_fetch(cumprimento, incidente)
    if preca:
        for k in _merge_fill(dados, preca):
            fontes[k] = "precainfosnew"

    local = _local_fetch(cumprimento, incidente)
    saved = False
    herdeiros: list[dict[str, Any]] = []
    id_credor = None
    herdeiros_validado = False
    ficha_id = None

    if local and isinstance(local.get("dados"), dict):
        saved = True
        ficha_id = local.get("id")
        for k, v in local["dados"].items():
            if k in dados:
                dados[k] = _serialize_cell(v)
                fontes[k] = "local"
        if local.get("incluido_por"):
            dados["incluido_por"] = _serialize_cell(local.get("incluido_por"))
        if local.get("alterado_por"):
            dados["alterado_por"] = _serialize_cell(local.get("alterado_por"))
        if local.get("criado_em"):
            dados["incluido_em"] = _serialize_cell(local.get("criado_em"))
        if local.get("atualizado_em"):
            dados["alterado_em"] = _serialize_cell(local.get("atualizado_em"))

        pack = local.get("herdeiros_pack") or {}
        id_credor = pack.get("id_credor")
        herdeiros = list(pack.get("herdeiros") or [])
        herdeiros_validado = bool(pack.get("credor_validado"))
        if pack.get("herdeiros_flag_texto"):
            herdeiros_flag_texto = pack.get("herdeiros_flag_texto") or herdeiros_flag_texto
        if pack.get("herdeiros_habilitados"):
            tem_herdeiros_mongo = True

    # Abrir bloco: local com herdeiros OU flag mongo
    herdeiros_abertos = bool(herdeiros) or tem_herdeiros_mongo
    if herdeiros_abertos and not herdeiros:
        herdeiros = [
            {"id": None, "ordem": 1, "validado": False, "dados": empty_herdeiro_dados()}
        ]

    dados["qtd_herdeiros"] = str(len(herdeiros) if herdeiros_abertos else 0)

    return (
        {
            "ok": True,
            "dados": dados,
            "fontes": fontes,
            "saved": saved,
            "schema": schema_payload(),
            "mongo_configured": mongo_configured(),
            "avisos": avisos,
            "caso_id": caso_id,
            "id_externo": id_externo,
            "ficha_id": ficha_id,
            "id_credor": id_credor,
            "tem_herdeiros": tem_herdeiros_mongo or bool(herdeiros),
            "herdeiros_abertos": herdeiros_abertos,
            "herdeiros_flag_texto": herdeiros_flag_texto,
            "herdeiros_validado": herdeiros_validado,
            "herdeiros": herdeiros,
        },
        200,
    )


def salvar_ficha(
    data: dict[str, Any],
    *,
    user_id: int | None = None,
    user_name: str | None = None,
) -> tuple[dict, int]:
    from messages_viewer.pre_analise_herdeiros import upsert_credor_e_herdeiros

    cumprimento = str(
        data.get("cumprimento_de_sentenca")
        or (data.get("dados") or {}).get("cumprimento_de_sentenca")
        or ""
    ).strip()
    incidente = str(
        data.get("incidente") or (data.get("dados") or {}).get("incidente") or ""
    ).strip()
    if not cumprimento or not incidente:
        return (
            {"ok": False, "error": "cumprimento_de_sentenca e incidente são obrigatórios."},
            400,
        )

    raw_dados = data.get("dados") if isinstance(data.get("dados"), dict) else data
    dados = empty_dados()
    for k in all_field_keys():
        if k in raw_dados:
            dados[k] = _serialize_cell(raw_dados.get(k))
    dados["cumprimento_de_sentenca"] = cumprimento
    dados["incidente"] = incidente

    caso_id = str(data.get("caso_id") or "").strip() or None
    id_externo = str(data.get("id_externo") or "").strip() or None
    who = (user_name or "").strip() or (f"user:{user_id}" if user_id else "sistema")
    now_iso = datetime.now().isoformat(sep=" ", timespec="seconds")

    herdeiros_payload = data.get("herdeiros")
    if not isinstance(herdeiros_payload, list):
        herdeiros_payload = []
    herdeiros_abertos = bool(data.get("herdeiros_abertos"))
    validar_herdeiros = bool(data.get("validar_herdeiros"))
    herdeiros_flag_texto = str(data.get("herdeiros_flag_texto") or "").strip()
    tem_herdeiros = bool(data.get("tem_herdeiros")) or herdeiros_abertos

    if herdeiros_abertos:
        dados["qtd_herdeiros"] = str(len(herdeiros_payload))
    else:
        dados["qtd_herdeiros"] = "0"
        herdeiros_payload = []

    existing = _local_fetch(cumprimento, incidente)
    if existing:
        dados["incluido_por"] = _serialize_cell(
            existing.get("incluido_por") or dados.get("incluido_por") or who
        )
        dados["incluido_em"] = _serialize_cell(
            existing.get("criado_em") or dados.get("incluido_em") or now_iso
        )
        dados["alterado_por"] = who
        dados["alterado_em"] = now_iso
    else:
        dados["incluido_por"] = who
        dados["incluido_em"] = now_iso
        dados["alterado_por"] = who
        dados["alterado_em"] = now_iso

    conn = None
    try:
        conn = _db_connect()
        cur = conn.cursor(dictionary=True)
        _ensure_table(cur)
        payload_json = json.dumps(dados, ensure_ascii=False)
        fontes_json = json.dumps({"saved_by": who}, ensure_ascii=False)

        cur.execute(
            """
            INSERT INTO pre_analise_ficha (
                caso_id, cumprimento_de_sentenca, incidente, id_externo,
                dados, fontes, criado_por_user_id, alterado_por_user_id,
                incluido_por, alterado_por
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                caso_id = COALESCE(VALUES(caso_id), caso_id),
                id_externo = COALESCE(VALUES(id_externo), id_externo),
                dados = VALUES(dados),
                fontes = VALUES(fontes),
                alterado_por_user_id = VALUES(alterado_por_user_id),
                alterado_por = VALUES(alterado_por),
                atualizado_em = CURRENT_TIMESTAMP(6)
            """,
            (
                caso_id,
                cumprimento,
                incidente,
                id_externo,
                payload_json,
                fontes_json,
                user_id,
                user_id,
                dados.get("incluido_por") or who,
                who,
            ),
        )
        cur.execute(
            """
            SELECT id FROM pre_analise_ficha
            WHERE cumprimento_de_sentenca = %s AND incidente = %s
            LIMIT 1
            """,
            (cumprimento, incidente),
        )
        ficha_row = cur.fetchone() or {}
        ficha_id = int(ficha_row["id"])

        herdeiros_result = upsert_credor_e_herdeiros(
            cur,
            ficha_id=ficha_id,
            cumprimento=cumprimento,
            incidente=incidente,
            nome_credor=dados.get("nome_credor"),
            cpf_credor=dados.get("cpf_credor"),
            herdeiros_habilitados=tem_herdeiros,
            herdeiros_flag_texto=herdeiros_flag_texto,
            herdeiros=herdeiros_payload if herdeiros_abertos else [],
            validar_herdeiros=validar_herdeiros and herdeiros_abertos,
        )
        conn.commit()

        msg = "Ficha salva com sucesso."
        if herdeiros_abertos:
            if validar_herdeiros:
                msg = "Ficha e herdeiros validados e salvos."
            else:
                msg = "Ficha salva (herdeiros em rascunho — use «Validar herdeiros» para confirmar)."

        return (
            {
                "ok": True,
                "mensagem": msg,
                "dados": dados,
                "saved": True,
                "ficha_id": ficha_id,
                "id_credor": herdeiros_result.get("id_credor"),
                "herdeiros": herdeiros_result.get("herdeiros") or [],
                "herdeiros_validado": bool(herdeiros_result.get("validado")),
                "herdeiros_abertos": herdeiros_abertos,
                "tem_herdeiros": tem_herdeiros,
            },
            200,
        )
    except Exception as e:
        return {"ok": False, "error": f"Falha ao salvar ficha: {e}"}, 500
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
