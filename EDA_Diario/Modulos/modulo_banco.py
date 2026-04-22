import mysql.connector
from mysql.connector import MySQLConnection
from datetime import datetime
import pandas as pd

from modulo_blacklist import normalizar_valor_para_blacklist

# ─────────────────────────────────────────────
# Configuracao da conexao
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "",
    "port":     3306,
}
DB_NAME = "eda_diario"

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
        count_aparicoes      INT DEFAULT 1,
        UNIQUE KEY uq_email (id_processo_juridico, email),
        CONSTRAINT fk_email_pj  FOREIGN KEY (id_processo_juridico) REFERENCES processos_juridicos(id),
        CONSTRAINT fk_email_exe FOREIGN KEY (id_execucao)          REFERENCES execucoes(id)
    ) ENGINE=InnoDB;

    CREATE TABLE IF NOT EXISTS blacklist (
        id             INT AUTO_INCREMENT PRIMARY KEY,
        tipo           ENUM('CPF','NOME','TELEFONE','EMAIL') NOT NULL COMMENT 'Escopo do bloqueio',
        valor          VARCHAR(300) NOT NULL,
        motivo         TEXT,
        data_inclusao  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        ativo          TINYINT(1) NOT NULL DEFAULT 1 COMMENT '1=ativo | 0=inativo',
        UNIQUE KEY uq_blacklist (tipo, valor)
    ) ENGINE=InnoDB;
    """

    for stmt in tabelas.split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)

    conn.commit()
    cur.close()
    conn.close()
    print("[DB] Banco e tabelas verificados/criados com sucesso.")


# ══════════════════════════════════════════════════════════════════════════════
# BLACKLIST
# ══════════════════════════════════════════════════════════════════════════════

def carregar_blacklist() -> dict[str, set]:
    """
    Retorna a blacklist ativa separada por tipo.
    Formato: {
        'CPF':      {'12345678900', ...},
        'NOME':     {'JOAO DA SILVA', ...},
        'TELEFONE': {'11999990000', ...},
        'EMAIL':    {'joao@email.com', ...},
    }
    """
    conn = conectar()
    cur  = conn.cursor(buffered=True)
    cur.execute("SELECT tipo, valor FROM blacklist WHERE ativo = 1")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    bl: dict[str, set] = {"CPF": set(), "NOME": set(), "TELEFONE": set(), "EMAIL": set()}
    for tipo, valor in rows:
        nv = normalizar_valor_para_blacklist(tipo, valor)
        if nv:
            bl[tipo].add(nv)

    total = sum(len(v) for v in bl.values())
    print(f"[BLACKLIST] {total} entradas ativas carregadas "
          f"(CPF:{len(bl['CPF'])} | NOME:{len(bl['NOME'])} | "
          f"TEL:{len(bl['TELEFONE'])} | EMAIL:{len(bl['EMAIL'])})")
    return bl


def importar_blacklist_txt(caminho_txt: str) -> None:
    """
    Le o arquivo blacklist.txt e importa todas as entradas validas no banco.
    Linhas que comecam com # sao ignoradas.
    Formato esperado: TIPO | VALOR | MOTIVO (motivo opcional)
    """
    tipos_validos = {"CPF", "NOME", "TELEFONE", "EMAIL"}
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
    tipo: 'CPF' | 'NOME' | 'TELEFONE' | 'EMAIL'
    """
    conn = conectar()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO blacklist (tipo, valor, motivo, ativo)
        VALUES (%s, %s, %s, 1)
        ON DUPLICATE KEY UPDATE
            ativo  = 1,
            motivo = COALESCE(%s, motivo)
    """, (tipo.upper(), valor.strip(), motivo, motivo))
    conn.commit()
    cur.close()
    conn.close()
    print(f"[BLACKLIST] Adicionado: tipo={tipo} | valor={valor}")


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
        cpf             = _val(row, "CPF") or ""
        nome            = _val(row, "Requerente")
        data_nasc       = _data(row, "Data_de_Nascimento")
        numero_processo = _val(row, "Numero_de_Processo") or ""

        if not cpf or not numero_processo:
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
        cpf             = _val(row, "CPF") or ""
        id_proc         = mapa_ids.get(numero_processo)

        if not id_proc:
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
