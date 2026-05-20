from __future__ import annotations

import io
from pathlib import Path

import mysql.connector
from mysql.connector import MySQLConnection
from datetime import datetime
import os
import pandas as pd
import re

from modulo_blacklist import normalizar_valor_para_blacklist

# ─────────────────────────────────────────────
# Configuracao da conexao
# ─────────────────────────────────────────────
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


DB_CONFIG = {
    "host": _env("EDA_MYSQL_HOST", "localhost").strip() or "localhost",
    "user": _env("EDA_MYSQL_USER", "root").strip() or "root",
    "password": _env("EDA_MYSQL_PASSWORD", ""),
    "port": int(_env("EDA_MYSQL_PORT", "3306") or "3306"),
    "connection_timeout": int(_env("EDA_MYSQL_CONNECT_TIMEOUT", "15") or "15"),
}

# Nome do schema (database) usado pelo EDA Diário
DB_NAME = (_env("EDA_MYSQL_DATABASE", "plataforma_central").strip() or "plataforma_central")

FORNECEDOR_P2 = "Lemitti"
FORNECEDOR_P3 = "Assertiva"


# ══════════════════════════════════════════════════════════════════════════════
# CONEXAO
# ══════════════════════════════════════════════════════════════════════════════

def conectar() -> MySQLConnection:
    return mysql.connector.connect(**DB_CONFIG, database=DB_NAME)


# ══════════════════════════════════════════════════════════════════════════════
# CRIACAO DO BANCO E TABELAS
# ══════════════════════════════════════════════════════════════════════════════

def criar_banco_e_tabelas() -> None:
    """Cria o banco de dados e todas as tabelas caso nao existam."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cur  = conn.cursor()

    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    cur.execute(f"USE `{DB_NAME}`")

    cur.executescript = lambda s: [cur.execute(stmt.strip()) for stmt in s.split(";") if stmt.strip()]

    tabelas = """
    CREATE TABLE IF NOT EXISTS execucoes (
        id                    INT AUTO_INCREMENT PRIMARY KEY,
        data_execucao         DATETIME        NOT NULL,
        etapa                 TINYINT         NOT NULL COMMENT '1=P2 | 2=P3',
        arquivo_principal     VARCHAR(500),
        arquivo_p2            VARCHAR(500),
        arquivo_p3            VARCHAR(500),
        total_registros       INT DEFAULT 0,
        total_enriquecidos_p2 INT DEFAULT 0,
        total_enriquecidos_p3 INT DEFAULT 0,
        total_sem_contato     INT DEFAULT 0
    ) ENGINE=InnoDB;

    CREATE TABLE IF NOT EXISTS pessoas (
        id                   INT AUTO_INCREMENT PRIMARY KEY,
        cpf                  VARCHAR(11)  NOT NULL UNIQUE,
        nome                 VARCHAR(300),
        data_nascimento      DATE,
        primeira_vez_visto   DATETIME     NOT NULL,
        ultimo_processamento DATETIME     NOT NULL,
        count_processamentos INT DEFAULT 1
    ) ENGINE=InnoDB;

    CREATE TABLE IF NOT EXISTS processos_juridicos (
        id                   INT AUTO_INCREMENT PRIMARY KEY,
        id_pessoa            INT          NOT NULL,
        cpf                  VARCHAR(11)  NOT NULL,
        numero_processo      VARCHAR(100) NOT NULL UNIQUE,
        numero_incidente     VARCHAR(100),
        natureza             VARCHAR(200),
        assunto              VARCHAR(300),
        ordem                VARCHAR(50),
        foro                 VARCHAR(200),
        data_base            DATE,
        data_decisao         DATE,
        principal_liquido    DECIMAL(18,2),
        juros_moratorio      DECIMAL(18,2),
        valor_requisitado    DECIMAL(18,2),
        calculo_atualizado   DECIMAL(18,2),
        entidade_devedora    VARCHAR(300),
        advogado             VARCHAR(300),
        requerente           VARCHAR(300),
        processo_codigo      VARCHAR(100),
        data_preenchimento   DATE,
        index_eda            INT,
        data_entrada         DATETIME     NOT NULL,
        ultimo_processamento DATETIME     NOT NULL,
        count_reprocessamentos INT DEFAULT 1,
        CONSTRAINT fk_pj_pessoa FOREIGN KEY (id_pessoa) REFERENCES pessoas(id)
    ) ENGINE=InnoDB;

    CREATE TABLE IF NOT EXISTS sms (
        id                   INT AUTO_INCREMENT PRIMARY KEY,
        id_processo_juridico INT          NOT NULL,
        id_execucao          INT          NOT NULL,
        cpf                  VARCHAR(11)  NOT NULL,
        telefone             VARCHAR(30)  NOT NULL,
        fornecedor           VARCHAR(30)  NOT NULL COMMENT 'Lemitti | Assertiva',
        primeira_aparicao    DATETIME     NOT NULL,
        ultimo_processamento DATETIME     NOT NULL,
        count_aparicoes      INT DEFAULT 1,
        UNIQUE KEY uq_sms (id_processo_juridico, telefone),
        CONSTRAINT fk_sms_pj  FOREIGN KEY (id_processo_juridico) REFERENCES processos_juridicos(id),
        CONSTRAINT fk_sms_exe FOREIGN KEY (id_execucao)          REFERENCES execucoes(id)
    ) ENGINE=InnoDB;

    CREATE TABLE IF NOT EXISTS emails (
        id                   INT AUTO_INCREMENT PRIMARY KEY,
        id_processo_juridico INT          NOT NULL,
        id_execucao          INT          NOT NULL,
        cpf                  VARCHAR(11)  NOT NULL,
        email                VARCHAR(300) NOT NULL,
        fornecedor           VARCHAR(30)  NOT NULL COMMENT 'Lemitti | Assertiva',
        primeira_aparicao    DATETIME     NOT NULL,
        ultimo_processamento DATETIME     NOT NULL,
        count_aparicoes      INT          DEFAULT 1,
        campanha_disparo_status        VARCHAR(40) DEFAULT NULL COMMENT 'sent,failed,skipped_blacklist,skipped_duplicate',
        campanha_disparo_erro            TEXT DEFAULT NULL,
        campanha_disparo_data_entrada DATETIME(3) DEFAULT NULL COMMENT 'Primeiro disparo registrado pela campanha',
        campanha_disparo_ultimo        DATETIME(3) DEFAULT NULL COMMENT 'Ultimo disparo registrado pela campanha',
        campanha_disparo_campaign_id    VARCHAR(191) DEFAULT NULL,
        campanha_disparo_dry_run       TINYINT(1) DEFAULT NULL COMMENT '1=simulacao',
        campanha_disparo_dominio        VARCHAR(128) DEFAULT NULL COMMENT 'Chave domains do disparo',
        campanha_disparo_remetente       VARCHAR(320) DEFAULT NULL COMMENT 'Remetente from',
        UNIQUE KEY uq_email (id_processo_juridico, email),
        CONSTRAINT fk_email_pj  FOREIGN KEY (id_processo_juridico) REFERENCES processos_juridicos(id),
        CONSTRAINT fk_email_exe FOREIGN KEY (id_execucao)          REFERENCES execucoes(id)
    ) ENGINE=InnoDB;

    CREATE TABLE IF NOT EXISTS disparo_hsm (
        id                   INT AUTO_INCREMENT PRIMARY KEY,
        id_processo_juridico INT          NOT NULL,
        id_execucao          INT          NOT NULL,
        cpf                  VARCHAR(11)  NOT NULL,
        telefone_hsm         VARCHAR(30)  NOT NULL,
        fornecedor           VARCHAR(30)  NOT NULL COMMENT 'Lemitti',
        nome                 VARCHAR(300),
        numero_processo      VARCHAR(100),
        numero_incidente     VARCHAR(100),
        primeira_aparicao    DATETIME     NOT NULL,
        ultimo_processamento DATETIME     NOT NULL,
        count_aparicoes      INT DEFAULT 1,
        UNIQUE KEY uq_disparo_hsm (id_processo_juridico, telefone_hsm),
        CONSTRAINT fk_disparo_hsm_pj  FOREIGN KEY (id_processo_juridico) REFERENCES processos_juridicos(id),
        CONSTRAINT fk_disparo_hsm_exe FOREIGN KEY (id_execucao)          REFERENCES execucoes(id)
    ) ENGINE=InnoDB;

    CREATE TABLE IF NOT EXISTS blacklist (
        id             INT AUTO_INCREMENT PRIMARY KEY,
        tipo           ENUM('CPF','NOME','TELEFONE','EMAIL','PROCESSO_INCIDENTE') NOT NULL COMMENT 'Escopo do bloqueio',
        valor          VARCHAR(300) NOT NULL,
        motivo         TEXT,
        data_inclusao  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        ativo          TINYINT(1) NOT NULL DEFAULT 1 COMMENT '1=ativo | 0=inativo',
        UNIQUE KEY uq_blacklist (tipo, valor)
    ) ENGINE=InnoDB;

    CREATE TABLE IF NOT EXISTS campanha_dominios (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        nome            VARCHAR(128) NOT NULL UNIQUE,
        dominio         VARCHAR(255) NOT NULL,
        from_name       VARCHAR(255) NOT NULL DEFAULT 'RED PRECATORIOS',
        from_email      VARCHAR(320) NOT NULL,
        reply_to        VARCHAR(320) DEFAULT NULL,
        mailgun_state   ENUM('pending','active','failed','deleted') NOT NULL DEFAULT 'pending',
        dns_configured  TINYINT(1) NOT NULL DEFAULT 0,
        ativo           TINYINT(1) NOT NULL DEFAULT 1,
        criado_em       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        atualizado_em   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB;

    CREATE TABLE IF NOT EXISTS campanha_disparos (
        id                   INT AUTO_INCREMENT PRIMARY KEY,
        campaign_id          VARCHAR(191) NOT NULL UNIQUE,
        assunto              VARCHAR(500) NOT NULL,
        total_destinatarios  INT NOT NULL DEFAULT 0,
        enviados             INT NOT NULL DEFAULT 0,
        falhos               INT NOT NULL DEFAULT 0,
        blacklist_skip       INT NOT NULL DEFAULT 0,
        duplicados           INT NOT NULL DEFAULT 0,
        status               ENUM('preparando','rodando','concluido','erro','cancelado') NOT NULL DEFAULT 'preparando',
        progresso_pct        DECIMAL(5,2) NOT NULL DEFAULT 0,
        origem               ENUM('csv','base','unico') NOT NULL DEFAULT 'csv',
        filtros_json         TEXT DEFAULT NULL,
        log_json             TEXT DEFAULT NULL COMMENT 'Ultimas N linhas do log para exibicao',
        iniciado_em          DATETIME DEFAULT NULL,
        concluido_em         DATETIME DEFAULT NULL,
        criado_por           VARCHAR(200) DEFAULT NULL
    ) ENGINE=InnoDB;

    CREATE TABLE IF NOT EXISTS campanha_templates (
        id               INT AUTO_INCREMENT PRIMARY KEY,
        nome             VARCHAR(200) NOT NULL UNIQUE,
        assunto          TEXT NOT NULL,
        corpo_html       MEDIUMTEXT NOT NULL,
        corpo_texto      MEDIUMTEXT NOT NULL,
        mapeamento_json  TEXT DEFAULT NULL COMMENT 'JSON: variavel_template -> fonte (__email__,__nome__,coluna_csv)',
        ativo            TINYINT(1) NOT NULL DEFAULT 1,
        criado_em        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        atualizado_em    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB;

    CREATE TABLE IF NOT EXISTS relatorio_discagem (
        id                          INT AUTO_INCREMENT PRIMARY KEY,
        arquivo                     VARCHAR(500) NOT NULL,
        formato                     VARCHAR(20) NOT NULL COMMENT 'PRC IMP | PRC TJSP | PRC CMP',
        aba                         VARCHAR(64) NOT NULL,
        telefone                    VARCHAR(50),
        nome                        VARCHAR(300),
        cpf                         VARCHAR(20),
        ordem                       VARCHAR(100) COMMENT 'IMP:OC | TJSP:Ordem | CMP:Cumprimento',
        processo_principal          VARCHAR(200) COMMENT 'CMP: processo_principal',
        processo                    VARCHAR(200) COMMENT 'IMP:processosOriginarios | TJSP:Processo | CMP:numero cumprimento',
        numero_incidente            VARCHAR(200) COMMENT 'TJSP:Numero Incidente | CMP:Cumprimento',
        data_base                   VARCHAR(50),
        desconto_previdenciario     VARCHAR(100),
        desconto_assistencia_medica VARCHAR(100),
        honorarios                  VARCHAR(100),
        principal                   VARCHAR(200) COMMENT 'IMP:Oficio | TJSP:Principal | CMP:Valor Total',
        pre_calculo                 VARCHAR(200),
        ir_retido                   VARCHAR(200) COMMENT 'IMP: IR Retido',
        advogado                    VARCHAR(300),
        entidade_devedora           VARCHAR(300),
        assunto                     VARCHAR(300),
        telefone_discagem           VARCHAR(50),
        status_ligacao              VARCHAR(200),
        origem                      VARCHAR(200),
        resultado                   VARCHAR(200),
        tempo                       VARCHAR(50),
        aba_origem                  VARCHAR(64) DEFAULT NULL,
        motivo_blacklist            VARCHAR(50) DEFAULT NULL,
        importado_em                DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_rel_arquivo (arquivo),
        INDEX idx_rel_formato (formato),
        INDEX idx_rel_processo (processo),
        INDEX idx_rel_cpf (cpf)
    ) ENGINE=InnoDB;
    """

    for stmt in tabelas.split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)

    conn.commit()
    _migrar_blacklist_enum_processo_incidente(cur)
    _migrar_relatorio_discagem_colunas(cur)
    _migrar_relatorio_discagem_formato(cur)
    conn.commit()
    cur.close()
    conn.close()
    print("[DB] Banco e tabelas verificados/criados com sucesso.")

    try:
        from campanha.api_templates import garantir_template_padrao_script

        r = garantir_template_padrao_script(DB_CONFIG, DB_NAME)
        if r.get("skipped"):
            print("[DB] Campanha: template do script (default.html) ja existia:", r.get("nome"))
        elif r.get("ok"):
            print("[DB] Campanha: template do script inserido na plataforma (id=%s)." % r.get("id"))
    except Exception as e:
        print("[DB] Campanha: aviso ao inserir template padrao —", e)


# ══════════════════════════════════════════════════════════════════════════════
# RELATÓRIO DE DISCAGEM (PRC TJSP / PRC CMP / PRC IMP)
# ══════════════════════════════════════════════════════════════════════════════

_COLUNAS_RELATORIO_INSERT: tuple[str, ...] = (
    "arquivo",
    "formato",
    "aba",
    "telefone",
    "nome",
    "cpf",
    "ordem",
    "processo_principal",
    "processo",
    "numero_incidente",
    "data_base",
    "desconto_previdenciario",
    "desconto_assistencia_medica",
    "honorarios",
    "principal",
    "pre_calculo",
    "ir_retido",
    "advogado",
    "entidade_devedora",
    "assunto",
    "telefone_discagem",
    "status_ligacao",
    "origem",
    "resultado",
    "tempo",
    "aba_origem",
    "motivo_blacklist",
    "importado_em",
)

_LIMITES_RELATORIO: dict[str, int | None] = {
    "telefone": 50,
    "nome": 300,
    "cpf": 20,
    "ordem": 100,
    "processo_principal": 200,
    "processo": 200,
    "numero_incidente": 200,
    "data_base": 50,
    "desconto_previdenciario": 100,
    "desconto_assistencia_medica": 100,
    "honorarios": 100,
    "principal": 200,
    "pre_calculo": 200,
    "ir_retido": 200,
    "advogado": 300,
    "entidade_devedora": 300,
    "assunto": 300,
    "telefone_discagem": 50,
    "status_ligacao": 200,
    "origem": 200,
    "resultado": 200,
    "tempo": 50,
    "aba": 64,
    "aba_origem": 64,
    "motivo_blacklist": 50,
}


def _migrar_relatorio_discagem_formato(cur) -> None:
    """ENUM legado/campanha/federal → VARCHAR PRC TJSP / PRC CMP / PRC IMP."""
    try:
        cur.execute(
            "ALTER TABLE relatorio_discagem MODIFY formato VARCHAR(20) NOT NULL"
        )
    except Exception as e:
        msg = str(e).lower()
        if "unknown column" not in msg:
            print(f"[DB] Aviso: migracao relatorio_discagem.formato tipo — {e}")

    renomear = (
        ("legado", "PRC TJSP"),
        ("campanha", "PRC CMP"),
        ("federal", "PRC IMP"),
    )
    for antigo, novo in renomear:
        try:
            cur.execute(
                "UPDATE relatorio_discagem SET formato = %s WHERE formato = %s",
                (novo, antigo),
            )
        except Exception as e:
            print(f"[DB] Aviso: migracao formato {antigo}→{novo} — {e}")


def _migrar_relatorio_discagem_colunas(cur) -> None:
    """Alinha schema antigo ao mapeamento PRC IMP / TJSP / CMP."""
    novas = (
        ("processo_principal", "VARCHAR(200) NULL"),
        ("ir_retido", "VARCHAR(200) NULL"),
        ("assunto", "VARCHAR(300) NULL"),
        ("data_base", "VARCHAR(50) NULL"),
        ("desconto_previdenciario", "VARCHAR(100) NULL"),
        ("desconto_assistencia_medica", "VARCHAR(100) NULL"),
        ("honorarios", "VARCHAR(100) NULL"),
    )
    for col, ddl in novas:
        try:
            cur.execute(f"ALTER TABLE relatorio_discagem ADD COLUMN {col} {ddl}")
        except Exception as e:
            msg = str(e).lower()
            if "duplicate column" not in msg:
                print(f"[DB] Aviso: migracao relatorio_discagem.{col} — {e}")
    for col_obsoleta in ("oc", "oficio"):
        try:
            cur.execute(f"ALTER TABLE relatorio_discagem DROP COLUMN {col_obsoleta}")
        except Exception:
            pass


def _valor_coluna_relatorio(row: dict, col: str, arquivo: str, fmt: str, agora) -> Any:
    if col == "arquivo":
        return arquivo
    if col == "formato":
        return fmt
    if col == "importado_em":
        return agora
    raw = row.get(col)
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    if not s:
        return None
    lim = _LIMITES_RELATORIO.get(col)
    if lim:
        return s[:lim]
    return s


def salvar_relatorio_discagem(arquivo: str, linhas: list[dict]) -> int:
    """
    Substitui no banco todas as linhas do mesmo ``arquivo`` (nome do CSV)
    e insere o lote actual (uma linha por registo parseado).
    Cada linha traz ``formato``: PRC IMP, PRC TJSP ou PRC CMP.
    """
    from modulo_relatorio_corrigido import normalizar_formato_relatorio

    linhas = [r for r in linhas if (r.get("aba") or "") != "_vazio"]
    if not linhas:
        return 0

    agora = datetime.now()
    conn = conectar()
    cur = conn.cursor()
    cur.execute("DELETE FROM relatorio_discagem WHERE arquivo = %s", (arquivo,))

    cols_sql = ", ".join(_COLUNAS_RELATORIO_INSERT)
    placeholders = ", ".join(["%s"] * len(_COLUNAS_RELATORIO_INSERT))
    sql = f"INSERT INTO relatorio_discagem ({cols_sql}) VALUES ({placeholders})"

    params = []
    fmt_counts: dict[str, int] = {}
    for row in linhas:
        fmt = normalizar_formato_relatorio(row.get("formato") or "")
        fmt_counts[fmt] = fmt_counts.get(fmt, 0) + 1
        params.append(
            tuple(
                _valor_coluna_relatorio(row, col, arquivo, fmt, agora)
                for col in _COLUNAS_RELATORIO_INSERT
            )
        )

    cur.executemany(sql, params)
    conn.commit()
    n = len(params)
    cur.close()
    conn.close()
    resumo_fmt = ", ".join(f"{k}:{v}" for k, v in sorted(fmt_counts.items()))
    print(f"[relatorio_discagem] {n} linha(s) — {arquivo} [{resumo_fmt}]")
    return n


def _valores_formato_relatorio_sql(formato: str) -> tuple[str, ...]:
    """Valores na coluna ``formato`` (inclui aliases antigos na BD)."""
    from modulo_relatorio_corrigido import (
        FORMATO_PRC_CMP,
        FORMATO_PRC_IMP,
        FORMATO_PRC_TJSP,
    )

    mapa = {
        FORMATO_PRC_TJSP: (FORMATO_PRC_TJSP, "legado"),
        FORMATO_PRC_CMP: (FORMATO_PRC_CMP, "campanha"),
        FORMATO_PRC_IMP: (FORMATO_PRC_IMP, "federal"),
    }
    return mapa.get((formato or "").strip(), (formato,))


def carregar_relatorio_discagem(
    arquivo: str | None = None,
    formato: str | None = None,
) -> list[dict]:
    """
    Lê registos da tabela ``relatorio_discagem``.
    Se ``arquivo`` for indicado, filtra só esse nome de CSV.
    Se ``formato`` for indicado (PRC TJSP / PRC CMP / PRC IMP), filtra por tipo de mailing.
    """
    cols = ", ".join(_COLUNAS_RELATORIO_INSERT)
    sql = f"SELECT {cols} FROM relatorio_discagem"
    clauses: list[str] = []
    params: list = []
    if arquivo:
        clauses.append("arquivo = %s")
        params.append(arquivo)
    if formato:
        vals = _valores_formato_relatorio_sql(formato)
        placeholders = ", ".join(["%s"] * len(vals))
        clauses.append(f"formato IN ({placeholders})")
        params.extend(vals)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY arquivo, id"
    params_tuple: tuple = tuple(params)

    conn = conectar()
    cur = conn.cursor(buffered=True)
    cur.execute(sql, params_tuple)
    nomes = [d[0] for d in cur.description]
    rows = [dict(zip(nomes, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()

    for row in rows:
        for k, v in list(row.items()):
            if v is None:
                row[k] = ""
            elif hasattr(v, "isoformat"):
                row[k] = v.strftime("%Y-%m-%d %H:%M:%S")
            else:
                row[k] = str(v).strip() if isinstance(v, str) else v
    return rows


def contar_relatorio_discagem(
    arquivo: str | None = None,
    formato: str | None = None,
) -> int:
    conn = conectar()
    cur = conn.cursor()
    clauses: list[str] = []
    params: list = []
    if arquivo:
        clauses.append("arquivo = %s")
        params.append(arquivo)
    if formato:
        vals = _valores_formato_relatorio_sql(formato)
        placeholders = ", ".join(["%s"] * len(vals))
        clauses.append(f"formato IN ({placeholders})")
        params.extend(vals)
    sql = "SELECT COUNT(*) FROM relatorio_discagem"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    cur.execute(sql, tuple(params) if params else ())
    n = int(cur.fetchone()[0])
    cur.close()
    conn.close()
    return n


# ══════════════════════════════════════════════════════════════════════════════
# BLACKLIST
# ══════════════════════════════════════════════════════════════════════════════

def _migrar_blacklist_enum_processo_incidente(cur) -> None:
    """Inclui PROCESSO_INCIDENTE no ENUM em bases já existentes."""
    try:
        cur.execute(
            """
            ALTER TABLE blacklist
            MODIFY tipo ENUM(
                'CPF','NOME','TELEFONE','EMAIL','PROCESSO_INCIDENTE'
            ) NOT NULL COMMENT 'Escopo do bloqueio'
            """
        )
    except Exception as e:
        msg = str(e).lower()
        if "duplicate" not in msg and "already" not in msg:
            print(f"[DB] Aviso: migracao blacklist PROCESSO_INCIDENTE — {e}")


def carregar_blacklist() -> dict[str, set]:
    """
    Retorna a blacklist ativa separada por tipo.
    Formato: {
        'CPF':      {'12345678900', ...},
        'NOME':     {'JOAO DA SILVA', ...},
        'TELEFONE': {'11999990000', ...},
        'EMAIL':    {'joao@email.com', ...},
        'PROCESSO_INCIDENTE': {'0001234-56.2023.8.26.0100|123', ...},
    }
    """
    conn = conectar()
    cur  = conn.cursor(buffered=True)
    cur.execute("SELECT tipo, valor FROM blacklist WHERE ativo = 1")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    bl: dict[str, set] = {
        "CPF": set(),
        "NOME": set(),
        "TELEFONE": set(),
        "EMAIL": set(),
        "PROCESSO_INCIDENTE": set(),
    }
    valid_tipos = frozenset(bl.keys())
    for tipo_raw, valor in rows:
        tipo = str(tipo_raw).strip().upper()
        if tipo not in valid_tipos:
            continue
        nv = normalizar_valor_para_blacklist(tipo, valor)
        if nv:
            bl[tipo].add(nv)

    total = sum(len(v) for v in bl.values())
    print(
        f"[BLACKLIST] {total} entradas ativas de `{DB_NAME}` "
        f"(CPF:{len(bl['CPF'])} | NOME:{len(bl['NOME'])} | "
        f"TEL:{len(bl['TELEFONE'])} | EMAIL:{len(bl['EMAIL'])} | "
        f"PROC+INC:{len(bl['PROCESSO_INCIDENTE'])})"
    )
    return bl


def importar_blacklist_txt(caminho_txt: str) -> None:
    """
    Le o arquivo blacklist.txt e importa todas as entradas validas no banco.
    Linhas que comecam com # sao ignoradas.
    Formato esperado: TIPO | VALOR | MOTIVO (motivo opcional)
    """
    tipos_validos = {"CPF", "NOME", "TELEFONE", "EMAIL", "PROCESSO_INCIDENTE"}
    importados = 0
    ignorados  = 0

    with open(caminho_txt, encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            if not linha or linha.startswith("#"):
                continue

            partes = [p.strip() for p in linha.split("|")]
            if len(partes) < 2:
                ignorados += 1
                continue

            tipo   = partes[0].upper()
            valor  = partes[1]
            motivo = partes[2] if len(partes) >= 3 else None

            if tipo not in tipos_validos or not valor:
                ignorados += 1
                continue

            adicionar_blacklist(tipo, valor, motivo)
            importados += 1

    print(f"[BLACKLIST] Importacao concluida: {importados} entradas | {ignorados} ignoradas")


def adicionar_blacklist(tipo: str, valor: str, motivo: str = None) -> None:
    """
    Adiciona ou reativa uma entrada na blacklist.
    tipo: 'CPF' | 'NOME' | 'TELEFONE' | 'EMAIL' | 'PROCESSO_INCIDENTE'
    """
    tipo_u = tipo.upper().strip()
    valor_grav = valor.strip()
    nv = normalizar_valor_para_blacklist(tipo_u, valor_grav)
    if nv:
        valor_grav = nv
    conn = conectar()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO blacklist (tipo, valor, motivo, ativo)
        VALUES (%s, %s, %s, 1)
        ON DUPLICATE KEY UPDATE
            ativo  = 1,
            motivo = COALESCE(%s, motivo)
    """, (tipo_u, valor_grav, motivo, motivo))
    conn.commit()
    cur.close()
    conn.close()
    print(f"[BLACKLIST] Adicionado: tipo={tipo_u} | valor={valor_grav}")


_TIPOS_BL = frozenset({"CPF", "NOME", "TELEFONE", "EMAIL", "PROCESSO_INCIDENTE"})


def _bl_csv_limpar_cell(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    if s.lower() in ("nan", "none", "nat"):
        return ""
    return s


def _bl_csv_resolver_ativo(raw) -> tuple[bool | None, bool]:
    """
    Retorna (importar_linha?, pulou_explícito?)
    Pulou_explícito: True quando ativo=false/0 (linha omitida pelo utilizador).
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return True, False
    s = str(raw).strip().lower()
    if s in ("", "1", "true", "t", "yes", "sim", "ativo"):
        return True, False
    if s in ("0", "false", "f", "no", "não", "nao", "inativo"):
        return False, True
    try:
        n = float(s)
        if n == 1:
            return True, False
        if n == 0:
            return False, True
    except ValueError:
        pass
    return True, False


def _bl_csv_headers_map(columns: list) -> tuple[dict[str, str], list[str]]:
    """
    Associa cada coluna lógica (tipo/valor/motivo/ativo) ao nome original no CSV.
    Colunas desconhecidas passam para avisos_ignorados (ex.: id, data_inclusao).
    """
    logic = {"tipo": None, "valor": None, "motivo": None, "ativo": None}
    ignorar = {"id", "data_inclusao", "data_inclusão"}
    avisos: list[str] = []
    for c in columns:
        orig = str(c).strip()
        chave = orig.lower().replace(" ", "_")
        if chave in ignorar:
            continue
        if chave == "tipo":
            logic["tipo"] = orig
        elif chave == "valor":
            logic["valor"] = orig
        elif chave == "motivo":
            logic["motivo"] = orig
        elif chave == "ativo":
            logic["ativo"] = orig
        else:
            avisos.append(orig)
    return logic, avisos


def importar_blacklist_csv(
    caminho_ou_buffer: str | Path | io.BytesIO,
    *,
    encoding: str = "utf-8-sig",
) -> dict:
    """
    Importa linhas de um CSV com colunas alinhadas à tabela ``blacklist``.

    Obrigatórias: ``tipo``, ``valor`` (nomes no ficheiro, case-insensitive).
    Opcionais: ``motivo``, ``ativo`` (0/false = não importa a linha).

    ``id`` e ``data_inclusao`` são ignorados se existirem.

    Retorno: ``importados``, ``ignorados``, ``pulados_ativo``, ``erros`` (lista de str),
    ``colunas_ignoradas`` (lista).
    """
    out: dict = {
        "importados": 0,
        "ignorados": 0,
        "pulados_ativo": 0,
        "erros": [],
        "colunas_ignoradas": [],
    }

    if isinstance(caminho_ou_buffer, io.BytesIO):
        caminho_ou_buffer.seek(0)
        df = pd.read_csv(
            caminho_ou_buffer,
            sep=None,
            engine="python",
            dtype=str,
            encoding=encoding,
        )
    else:
        df = pd.read_csv(
            Path(caminho_ou_buffer),
            sep=None,
            engine="python",
            dtype=str,
            encoding=encoding,
        )

    df.columns = [str(c).strip() for c in df.columns]
    logic, extra_cols = _bl_csv_headers_map(list(df.columns))
    out["colunas_ignoradas"] = extra_cols

    if not logic["tipo"] or not logic["valor"]:
        out["erros"].append(
            "CSV precisa das colunas `tipo` e `valor` (como na tabela blacklist)."
        )
        return out

    sql = """
        INSERT INTO blacklist (tipo, valor, motivo, ativo)
        VALUES (%s, %s, %s, 1)
        ON DUPLICATE KEY UPDATE
            ativo  = 1,
            motivo = COALESCE(%s, motivo)
    """

    conn = conectar()
    cur = conn.cursor()
    try:
        for num, (_, row) in enumerate(df.iterrows(), start=2):
            tipo = _bl_csv_limpar_cell(row.get(logic["tipo"])).upper()
            valor = _bl_csv_limpar_cell(row.get(logic["valor"]))
            if not tipo and not valor:
                continue
            if logic.get("ativo"):
                raw_at = row.get(logic["ativo"])
                imp, explicit_skip = _bl_csv_resolver_ativo(raw_at)
                if not imp:
                    if explicit_skip:
                        out["pulados_ativo"] += 1
                    else:
                        out["ignorados"] += 1
                    continue

            motivo = None
            if logic.get("motivo"):
                m = _bl_csv_limpar_cell(row.get(logic["motivo"]))
                motivo = m or None

            if not tipo or not valor:
                out["ignorados"] += 1
                continue
            if tipo not in _TIPOS_BL:
                out["ignorados"] += 1
                err = (
                    f"Linha {num}: tipo inválido `{tipo}` "
                    "(use CPF, NOME, TELEFONE, EMAIL ou PROCESSO_INCIDENTE)."
                )
                if len(out["erros"]) < 40:
                    out["erros"].append(err)
                continue

            valor_grav = valor
            nv = normalizar_valor_para_blacklist(tipo, valor)
            if nv:
                valor_grav = nv
            elif tipo == "PROCESSO_INCIDENTE":
                out["ignorados"] += 1
                continue

            cur.execute(sql, (tipo, valor_grav, motivo, motivo))
            out["importados"] += 1
        conn.commit()
    finally:
        cur.close()
        conn.close()

    print(
        f"[BLACKLIST] CSV: {out['importados']} aplicados | "
        f"{out['ignorados']} ignorados | "
        f"{out['pulados_ativo']} com ativo=0"
    )
    return out


# ══════════════════════════════════════════════════════════════════════════════
# COOLDOWN
# ══════════════════════════════════════════════════════════════════════════════

def buscar_cpfs_cooldown(dias: int = 14) -> set[str]:
    """
    Retorna CPFs que foram processados nos ultimos N dias.
    Deve ser chamada ANTES de atualizar o banco para checar dados historicos.
    """
    conn = conectar()
    cur  = conn.cursor(buffered=True)
    cur.execute("""
        SELECT cpf FROM pessoas
        WHERE ultimo_processamento >= DATE_SUB(NOW(), INTERVAL %s DAY)
    """, (dias,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    cpfs = {row[0] for row in rows}
    print(f"[COOLDOWN] {len(cpfs)} CPFs em cooldown (ultimos {dias} dias).")
    return cpfs


# ══════════════════════════════════════════════════════════════════════════════
# EXPORTACAO POR PERIODO
# ══════════════════════════════════════════════════════════════════════════════

def exportar_por_periodo(
    disparo_inicio: str = None,
    disparo_fim:    str = None,
    entrada_inicio: str = None,
    entrada_fim:    str = None,
) -> dict:
    """
    Retorna DataFrames {principal, sms, emails} com filtros opcionais de data.
    Cada parametro e opcional; os que forem informados sao combinados com AND.
    Datas no formato 'YYYY-MM-DD'.
    """
    conn = conectar()

    # Monta clausulas WHERE dinamicamente
    def _where(colunas_disparo: list[str], colunas_entrada: list[str]) -> tuple[str, list]:
        """
        colunas_disparo: lista de colunas a aplicar o filtro de ultimo_processamento.
        colunas_entrada: lista de colunas a aplicar o filtro de data_entrada.
        Retorna (clausula_where, params).
        """
        condicoes, params = [], []

        if disparo_inicio:
            for col in colunas_disparo:
                condicoes.append(f"{col} >= %s")
                params.append(disparo_inicio + " 00:00:00")
        if disparo_fim:
            for col in colunas_disparo:
                condicoes.append(f"{col} <= %s")
                params.append(disparo_fim + " 23:59:59")
        if entrada_inicio:
            for col in colunas_entrada:
                condicoes.append(f"{col} >= %s")
                params.append(entrada_inicio + " 00:00:00")
        if entrada_fim:
            for col in colunas_entrada:
                condicoes.append(f"{col} <= %s")
                params.append(entrada_fim + " 23:59:59")

        clausula = ("WHERE " + " AND ".join(condicoes)) if condicoes else ""
        return clausula, params

    where_pj, params_pj   = _where(["pj.ultimo_processamento"], ["pj.data_entrada"])
    where_sms, params_sms = _where(["s.ultimo_processamento"],  ["pj.data_entrada"])
    where_em, params_em   = _where(["e.ultimo_processamento"],  ["pj.data_entrada"])

    def _query(sql: str, params: list) -> pd.DataFrame:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params if params else ())
        rows = cur.fetchall()
        cur.close()
        return pd.DataFrame(rows)

    df_principal = _query(f"""
        SELECT
            p.cpf, p.nome AS nome_pessoa,
            pj.numero_processo, pj.numero_incidente, pj.natureza, pj.assunto,
            pj.ordem, pj.foro, pj.data_base, pj.data_decisao,
            pj.principal_liquido, pj.juros_moratorio, pj.valor_requisitado,
            pj.calculo_atualizado, pj.entidade_devedora, pj.advogado,
            pj.requerente, pj.data_entrada, pj.ultimo_processamento,
            pj.count_reprocessamentos
        FROM processos_juridicos pj
        JOIN pessoas p ON p.id = pj.id_pessoa
        {where_pj}
        ORDER BY pj.ultimo_processamento DESC
    """, params_pj)

    df_sms = _query(f"""
        SELECT
            s.cpf, pj.numero_processo, pj.requerente, pj.foro,
            s.telefone, s.fornecedor,
            s.primeira_aparicao, s.ultimo_processamento, s.count_aparicoes
        FROM sms s
        JOIN processos_juridicos pj ON pj.id = s.id_processo_juridico
        {where_sms}
        ORDER BY s.ultimo_processamento DESC
    """, params_sms)

    df_emails = _query(f"""
        SELECT
            e.cpf, pj.numero_processo, pj.requerente, pj.foro,
            e.email, e.fornecedor,
            e.primeira_aparicao, e.ultimo_processamento, e.count_aparicoes
        FROM emails e
        JOIN processos_juridicos pj ON pj.id = e.id_processo_juridico
        {where_em}
        ORDER BY e.ultimo_processamento DESC
    """, params_em)

    conn.close()
    return {"principal": df_principal, "sms": df_sms, "emails": df_emails}


# ══════════════════════════════════════════════════════════════════════════════
# EXECUCOES
# ══════════════════════════════════════════════════════════════════════════════

def registrar_execucao(
    etapa: int,
    arquivo_principal: str = None,
    arquivo_p2: str = None,
    arquivo_p3: str = None,
    total_registros: int = 0,
    total_enriquecidos_p2: int = 0,
    total_enriquecidos_p3: int = 0,
    total_sem_contato: int = 0,
) -> int:
    """Insere um registro de execucao e retorna o id gerado."""
    conn = conectar()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO execucoes
            (data_execucao, etapa, arquivo_principal, arquivo_p2, arquivo_p3,
             total_registros, total_enriquecidos_p2, total_enriquecidos_p3, total_sem_contato)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (datetime.now(), etapa, arquivo_principal, arquivo_p2, arquivo_p3,
          total_registros, total_enriquecidos_p2, total_enriquecidos_p3, total_sem_contato))
    id_execucao = cur.lastrowid
    conn.commit()
    cur.close()
    conn.close()
    return id_execucao


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _val(row, col):
    """Retorna None se o valor for NaN, NaT ou vazio, senao retorna o valor."""
    v = row.get(col)
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    s = str(v).strip()
    return None if s in ("", "nan", "NaT", "None") else s


def normalizar_cpf(valor) -> str:
    """
    Normaliza CPF para 11 digitos (somente numeros).
    - Remove mascara (pontos/hifen/espacos).
    - Trata casos comuns do Excel ('.0', notacao cientifica).
    - Se vier com menos de 11 digitos, preenche com zeros a esquerda.
    - Se vier com mais de 11, trunca para os 11 primeiros digitos.
    """
    if valor is None:
        return ""

    # Pandas/NumPy podem trazer NaN/NaT como float
    try:
        if pd.isna(valor):
            return ""
    except Exception:
        pass

    s = str(valor).strip()
    if not s or s.lower() in ("nan", "nat", "none"):
        return ""

    digitos = "".join(re.findall(r"\d+", s))
    if not digitos:
        return ""

    if len(digitos) < 11:
        digitos = digitos.zfill(11)
    elif len(digitos) > 11:
        digitos = digitos[:11]

    return digitos


def _val_cpf_cadastro(row) -> str:
    """Chave alinhada ao P2/P3: com duas colunas CPF (CMP/IMP), preferir a segunda."""
    for col in ("CPF.1", "cpf.1"):
        if col in row.index:
            v = _val(row, col)
            if v:
                return normalizar_cpf(v)
    return normalizar_cpf(_val(row, "CPF") or "")


def _data(row, col):
    """Converte coluna para date ou None."""
    v = _val(row, col)
    if v is None:
        return None
    try:
        return pd.to_datetime(v).date()
    except Exception:
        return None


def _decimal(row, col):
    """Converte coluna para float ou None."""
    v = _val(row, col)
    if v is None:
        return None
    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# UPSERT PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def salvar_processos(df: pd.DataFrame, id_execucao: int) -> dict[str, int]:
    """
    Insere ou atualiza pessoas e processos_juridicos a partir do DataFrame principal.
    Retorna mapa {numero_processo: id_processo_juridico}.
    """
    conn = conectar()
    cur  = conn.cursor(buffered=True)
    agora = datetime.now()
    mapa_ids = {}

    for _, row in df.iterrows():
        cpf             = _val_cpf_cadastro(row)
        nome            = _val(row, "Requerente")
        data_nasc       = _data(row, "Data_de_Nascimento")
        numero_processo = _val(row, "Numero_de_Processo") or ""

        if not cpf or len(cpf) != 11 or not numero_processo:
            continue

        # ── Pessoa (upsert) — LAST_INSERT_ID(id) retorna o id mesmo no UPDATE ──
        cur.execute("""
            INSERT INTO pessoas (cpf, nome, data_nascimento, primeira_vez_visto, ultimo_processamento, count_processamentos)
            VALUES (%s, %s, %s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE
                id                   = LAST_INSERT_ID(id),
                ultimo_processamento = %s,
                count_processamentos = count_processamentos + 1,
                nome                 = COALESCE(%s, nome),
                data_nascimento      = COALESCE(%s, data_nascimento)
        """, (cpf, nome, data_nasc, agora, agora, agora, nome, data_nasc))
        id_pessoa = cur.lastrowid

        # ── Processo juridico (upsert) ────────────────────────────────────────
        cur.execute("""
            INSERT INTO processos_juridicos
                (id_pessoa, cpf, numero_processo, numero_incidente, natureza, assunto,
                 ordem, foro, data_base, data_decisao, principal_liquido, juros_moratorio,
                 valor_requisitado, calculo_atualizado, entidade_devedora, advogado,
                 requerente, processo_codigo, data_preenchimento, index_eda,
                 data_entrada, ultimo_processamento, count_reprocessamentos)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1)
            ON DUPLICATE KEY UPDATE
                id                     = LAST_INSERT_ID(id),
                ultimo_processamento   = %s,
                count_reprocessamentos = count_reprocessamentos + 1,
                index_eda              = %s
        """, (
            id_pessoa, cpf, numero_processo,
            _val(row, "Numero_do_Incidente"), _val(row, "Natureza"), _val(row, "Assunto"),
            _val(row, "Ordem"), _val(row, "Foro"),
            _data(row, "Data_Base"), _data(row, "Data_Decisao"),
            _decimal(row, "Principal_Liquido"), _decimal(row, "Juros_Moratorio"),
            _decimal(row, "Valor_Requisitado"), _decimal(row, "Calculo_Atualizado"),
            _val(row, "Entidade_Devedora"), _val(row, "Advogado"),
            nome, _val(row, "Processo_Codigo"),
            _data(row, "Data_Preenchimento"), _val(row, "INDEX"),
            agora, agora,
            agora, _val(row, "INDEX"),
        ))
        id_proc = cur.lastrowid
        mapa_ids[numero_processo] = id_proc

    conn.commit()
    cur.close()
    conn.close()
    print(f"[DB] Processos salvos: {len(mapa_ids)} registros.")
    return mapa_ids


# ══════════════════════════════════════════════════════════════════════════════
# UPSERT SMS E EMAILS
# ══════════════════════════════════════════════════════════════════════════════

def salvar_contatos(
    df: pd.DataFrame,
    registros_tel: list,
    registros_email: list,
    mapa_ids: dict,
    id_execucao: int,
) -> None:
    """
    Insere ou atualiza telefones e emails no banco.
    Usa mapa_ids {numero_processo: id_processo_juridico} para vincular os registros.
    """
    conn = conectar()
    cur  = conn.cursor(buffered=True)
    agora = datetime.now()

    total_tel = total_email = 0

    for i, row in df.iterrows():
        numero_processo = _val(row, "Numero_de_Processo") or ""
        cpf             = _val_cpf_cadastro(row)
        id_proc         = mapa_ids.get(numero_processo)

        if not id_proc or not cpf or len(cpf) != 11:
            continue

        # ── Telefones ─────────────────────────────────────────────────────────
        for telefone, is_red in registros_tel[i]:
            fornecedor = FORNECEDOR_P2 if is_red else FORNECEDOR_P3
            cur.execute("""
                INSERT INTO sms (id_processo_juridico, id_execucao, cpf, telefone, fornecedor,
                                 primeira_aparicao, ultimo_processamento, count_aparicoes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
                ON DUPLICATE KEY UPDATE
                    ultimo_processamento = %s,
                    count_aparicoes      = count_aparicoes + 1,
                    id_execucao          = %s
            """, (id_proc, id_execucao, cpf, telefone, fornecedor, agora, agora,
                  agora, id_execucao))
            total_tel += 1

        # ── Emails ────────────────────────────────────────────────────────────
        for email, is_red in registros_email[i]:
            fornecedor = FORNECEDOR_P2 if is_red else FORNECEDOR_P3
            cur.execute("""
                INSERT INTO emails (id_processo_juridico, id_execucao, cpf, email, fornecedor,
                                    primeira_aparicao, ultimo_processamento, count_aparicoes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
                ON DUPLICATE KEY UPDATE
                    ultimo_processamento = %s,
                    count_aparicoes      = count_aparicoes + 1,
                    id_execucao          = %s
            """, (id_proc, id_execucao, cpf, email, fornecedor, agora, agora,
                  agora, id_execucao))
            total_email += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"[DB] Contatos salvos: {total_tel} telefones | {total_email} emails.")


def salvar_disparo_hsm(
    df: pd.DataFrame,
    registros_hsm: list,
    mapa_ids: dict,
    id_execucao: int,
) -> None:
    """
    Insere/upserta telefones HSM (BA+BB Lemitti) na tabela `disparo_hsm`.
    Uma linha por (processo_juridico, telefone_hsm).
    """
    conn = conectar()
    cur  = conn.cursor(buffered=True)
    agora = datetime.now()

    total = 0

    def _nome(row) -> str | None:
        return _val(row, "Requerente") or _val(row, "NOME") or _val(row, "Nome")

    def _proc(row) -> str:
        return _val(row, "Numero_de_Processo") or _val(row, "Processo") or ""

    def _inc(row) -> str | None:
        return _val(row, "Numero_do_Incidente") or _val(row, "Incidente")

    for i, row in df.iterrows():
        numero_processo = _proc(row)
        cpf             = _val_cpf_cadastro(row)
        id_proc         = mapa_ids.get(numero_processo)

        if not id_proc or not cpf or len(cpf) != 11 or not numero_processo:
            continue

        for telefone_hsm, _is_red in registros_hsm[i]:
            tel = _val({"x": telefone_hsm}, "x")
            if not tel:
                continue
            cur.execute("""
                INSERT INTO disparo_hsm (
                    id_processo_juridico, id_execucao, cpf, telefone_hsm, fornecedor,
                    nome, numero_processo, numero_incidente,
                    primeira_aparicao, ultimo_processamento, count_aparicoes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                ON DUPLICATE KEY UPDATE
                    ultimo_processamento = %s,
                    count_aparicoes      = count_aparicoes + 1,
                    id_execucao          = %s,
                    nome                 = COALESCE(%s, nome),
                    numero_incidente     = COALESCE(%s, numero_incidente)
            """, (
                id_proc, id_execucao, cpf, tel, FORNECEDOR_P2,
                _nome(row), numero_processo, _inc(row),
                agora, agora,
                agora, id_execucao, _nome(row), _inc(row),
            ))
            total += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"[DB] Disparo_HSM salvo: {total} telefone(s).")
