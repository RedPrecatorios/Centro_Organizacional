# -*- coding: utf-8 -*-
"""Página da blacklist na aba principal: tabela do MySQL do EDA."""
from __future__ import annotations

import csv
import io
import os
import re
import sys
from pathlib import Path
from typing import Any

PAGE_SIZE = 20
SORT_COLUMNS = {
    "tipo": "tipo",
    "valor": "valor",
    "motivo": "motivo",
    "data_inclusao": "data_inclusao",
}
TIPOS = ("CPF", "NOME", "TELEFONE", "EMAIL", "PROCESSO_INCIDENTE")
MOTIVOS = (
    "Solicitou remoção",
    "Fez Acordo",
    "Acordo",
    "Sem Interesse",
    "Engano",
    "PF",
    "Já incluído",
    "Telefone Incorreto",
    "Inapto",
    "Blacklist",
)


def _modulos_eda() -> None:
    root = Path(__file__).resolve().parent.parent / "EDA_Diario" / "Modulos"
    p = str(root)
    if p not in sys.path:
        sys.path.insert(0, p)


def _banco():
    _modulos_eda()
    from modulo_banco import (  # noqa: WPS433
        adicionar_blacklist,
        conectar,
        importar_blacklist_csv,
    )
    from modulo_blacklist import (  # noqa: WPS433
        normalizar_chave_processo_incidente,
        normalizar_valor_para_blacklist,
    )

    return {
        "adicionar": adicionar_blacklist,
        "conectar": conectar,
        "importar": importar_blacklist_csv,
        "chave_proc": normalizar_chave_processo_incidente,
        "normalizar": normalizar_valor_para_blacklist,
    }


def _like(busca: str) -> str:
    if not busca:
        return "%"
    term = busca.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{term}%"


def motivos_na_base() -> list[str]:
    """Motivos distintos das entradas ativas, inclusive os que vieram de CSV."""
    conn = _banco()["conectar"]()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT motivo
            FROM blacklist
            WHERE ativo = 1 AND motivo IS NOT NULL AND motivo <> ''
            ORDER BY motivo
            """
        )
        out = [str(row[0]) for row in (cur.fetchall() or []) if row and row[0]]
        cur.close()
    finally:
        conn.close()
    return out


def listar(
    busca: str,
    pagina: int,
    *,
    tipo: str = "",
    motivo: str = "",
    sort: str = "data_inclusao",
    direction: str = "desc",
) -> dict[str, Any]:
    page = pagina if pagina > 0 else 1
    offset = (page - 1) * PAGE_SIZE
    tipo_f = (tipo or "").strip().upper()
    if tipo_f not in TIPOS:
        tipo_f = ""
    motivo_f = (motivo or "").strip()
    sort_key = (sort or "").strip().lower()
    if sort_key not in SORT_COLUMNS:
        sort_key = "data_inclusao"
    direc = "ASC" if (direction or "").strip().lower() == "asc" else "DESC"
    clauses = ["ativo = 1"]
    params: list[Any] = []
    termo = busca.strip()
    if termo:
        clauses.append("valor LIKE %s ESCAPE '\\\\'")
        params.append(_like(termo))
    if tipo_f:
        clauses.append("tipo = %s")
        params.append(tipo_f)
    if motivo_f:
        clauses.append("motivo = %s")
        params.append(motivo_f)
    where = " WHERE " + " AND ".join(clauses)
    order_sql = f"ORDER BY {SORT_COLUMNS[sort_key]} {direc}, id DESC"
    banco = _banco()
    conn = banco["conectar"]()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"""
            SELECT id, tipo, valor, motivo, data_inclusao
            FROM blacklist
            {where}
            {order_sql}
            LIMIT %s OFFSET %s
            """,
            tuple(params) + (PAGE_SIZE, offset),
        )
        rows = list(cur.fetchall() or [])
        cur.execute(
            f"SELECT COUNT(*) AS total FROM blacklist{where}",
            tuple(params),
        )
        total = int((cur.fetchone() or {}).get("total") or 0)
        cur.close()
    finally:
        conn.close()
    for row in rows:
        dt = row.get("data_inclusao")
        if dt is not None and hasattr(dt, "strftime"):
            row["data_inclusao"] = dt.strftime("%d/%m/%Y %H:%M")
        else:
            row["data_inclusao"] = ""
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE) if total else 1
    return {
        "registros": rows,
        "total": total,
        "pagina": page,
        "paginas": pages,
        "por_pag": PAGE_SIZE,
        "tipo": tipo_f,
        "motivo": motivo_f,
        "sort": sort_key,
        "dir": direc.lower(),
    }


def _tipo(raw: str) -> str:
    tipo = (raw or "").strip().upper()
    if tipo not in TIPOS:
        raise ValueError(
            "Tipo inválido. Use CPF, NOME, TELEFONE, EMAIL ou PROCESSO_INCIDENTE."
        )
    return tipo


def _motivo_manual(raw: str) -> str:
    motivo = (raw or "").strip()
    if motivo not in MOTIVOS:
        raise ValueError("Motivo inválido. Selecione uma opção da lista padronizada.")
    return motivo


def valor_gravado(form: dict[str, str]) -> tuple[str, str]:
    """Devolve (tipo, valor normalizado) a partir do formulário."""
    banco = _banco()
    tipo = _tipo(form.get("tipo") or "")
    if tipo == "PROCESSO_INCIDENTE":
        valor = banco["chave_proc"](form.get("processo") or "", form.get("incidente") or "")
        if not valor:
            raise ValueError(
                "Número de processo é obrigatório para bloqueio por processo/incidente."
            )
        return tipo, valor
    bruto = (form.get("valor") or "").strip()
    if not bruto:
        raise ValueError("Tipo e valor são obrigatórios.")
    valor = banco["normalizar"](tipo, bruto)
    if not valor:
        raise ValueError("Valor inválido para o tipo escolhido.")
    return tipo, valor


def _digitos_cpf(bruto: str) -> str:
    return re.sub(r"\D", "", bruto or "")


def _cpf_obrigatorio(bruto: str, vazio: str) -> str:
    if not (bruto or "").strip():
        raise ValueError(vazio)
    digitos = _digitos_cpf(bruto)
    if len(digitos) != 11:
        raise ValueError("CPF inválido. Informe os 11 dígitos.")
    valor = _banco()["normalizar"]("CPF", digitos)
    if not valor:
        raise ValueError("CPF inválido. Informe os 11 dígitos.")
    return valor


def _nome_opcional(bruto: str) -> str:
    if not (bruto or "").strip():
        return ""
    valor = _banco()["normalizar"]("NOME", bruto)
    if not valor:
        raise ValueError("Nome inválido.")
    return valor


def acompanhantes(tipo: str, form: dict[str, str]) -> list[tuple[str, str]]:
    """CPF exigido no nome; nome ou CPF exigido no processo. Também são gravados."""
    if tipo == "NOME":
        cpf = _cpf_obrigatorio(
            form.get("cpf") or "",
            "Para incluir um nome, informe também o CPF.",
        )
        return [("CPF", cpf)]
    if tipo == "PROCESSO_INCIDENTE":
        nome = _nome_opcional(form.get("nome") or "")
        cpf_bruto = (form.get("cpf") or "").strip()
        cpf = ""
        if cpf_bruto:
            cpf = _cpf_obrigatorio(cpf_bruto, "CPF inválido. Informe os 11 dígitos.")
        if not nome and not cpf:
            raise ValueError("Para incluir um processo, informe o nome ou o CPF.")
        extras: list[tuple[str, str]] = []
        if nome:
            extras.append(("NOME", nome))
        if cpf:
            extras.append(("CPF", cpf))
        return extras
    return []


def _rotulo(tipo: str, valor: str) -> str:
    if tipo == "PROCESSO_INCIDENTE":
        return f"[PROCESSO+INCIDENTE] {valor.replace('|', ' · ')}"
    return f"[{tipo}] {valor}"


def _texto(form: dict[str, str], chave: str) -> str:
    return (form.get(chave) or "").strip()


def _valores(form, chave: str) -> list[str]:
    """Aceita um valor ou vários com o mesmo nome, como os telefones do formulário."""
    if hasattr(form, "getlist"):
        bruto = form.getlist(chave)
    else:
        bruto = form.get(chave)
        if bruto is None:
            bruto = []
        elif isinstance(bruto, str):
            bruto = [bruto]
    saida: list[str] = []
    for item in bruto:
        texto = str(item or "").strip()
        if texto:
            saida.append(texto)
    return saida


def registros_do_caso(form: dict[str, str]) -> list[tuple[str, str]]:
    """Separa um caso preenchido de uma vez nos registros já usados na blacklist."""
    banco = _banco()
    processo = _texto(form, "processo")
    incidente = _texto(form, "incidente")
    nome_bruto = _texto(form, "nome")
    cpf_bruto = _texto(form, "cpf")
    telefones_brutos = _valores(form, "telefone")
    emails_brutos = _valores(form, "email")

    if incidente and not processo:
        raise ValueError("Número de processo é obrigatório quando há incidente.")

    nome = _nome_opcional(nome_bruto) if nome_bruto else ""
    cpf = ""
    if cpf_bruto:
        cpf = _cpf_obrigatorio(cpf_bruto, "CPF inválido. Informe os 11 dígitos.")
    elif nome:
        raise ValueError("Para incluir um nome, informe também o CPF.")

    itens: list[tuple[str, str]] = []
    if processo:
        chave = banco["chave_proc"](processo, incidente)
        if not chave:
            raise ValueError(
                "Número de processo é obrigatório para bloqueio por processo/incidente."
            )
        if not nome and not cpf:
            raise ValueError("Para incluir um processo, informe o nome ou o CPF.")
        itens.append(("PROCESSO_INCIDENTE", chave))
    if nome:
        itens.append(("NOME", nome))
    if cpf:
        itens.append(("CPF", cpf))
    vistos_tel: set[str] = set()
    for telefone_bruto in telefones_brutos:
        telefone = banco["normalizar"]("TELEFONE", telefone_bruto)
        if not telefone:
            raise ValueError(f"Telefone inválido: {telefone_bruto}.")
        if telefone in vistos_tel:
            continue
        vistos_tel.add(telefone)
        itens.append(("TELEFONE", telefone))
    vistos_email: set[str] = set()
    for email_bruto in emails_brutos:
        email = banco["normalizar"]("EMAIL", email_bruto)
        if not email:
            raise ValueError(f"E-mail inválido: {email_bruto}.")
        if email in vistos_email:
            continue
        vistos_email.add(email)
        itens.append(("EMAIL", email))
    if not itens:
        raise ValueError("Informe ao menos um dado: processo, nome, CPF, telefone ou e-mail.")
    return itens


_COLUNA = re.compile(r"^[A-Za-z0-9_]+$")
_LIMITE_CASOS = 25
_LIMITE_CONTATOS = 40


def criterios_busca(cpf: str, processo: str, incidente: str) -> dict[str, str]:
    """CPF com 11 dígitos e/ou processo. Incidente só vale junto com o processo."""
    digitos = re.sub(r"\D", "", cpf or "")
    if len(digitos) > 11:
        digitos = digitos[-11:]
    if digitos and len(digitos) != 11:
        raise ValueError("CPF inválido. Informe os 11 dígitos.")
    proc = " ".join(str(processo or "").split())
    inc = " ".join(str(incidente or "").split())
    if inc and not proc:
        raise ValueError("Informe o número do processo junto com o incidente.")
    if not digitos and not proc:
        raise ValueError("Informe o CPF ou o número do processo.")
    return {"cpf": digitos, "processo": proc, "incidente": inc}


def _coluna(fields: set[str], *candidatos: str) -> str:
    por_minuscula = {nome.lower(): nome for nome in fields}
    for candidato in candidatos:
        coluna = candidato if candidato in fields else por_minuscula.get(candidato.lower())
        if coluna and _COLUNA.match(coluna):
            return coluna
    return ""


def where_precainfos(criterios: dict[str, str], colunas: dict[str, str]) -> tuple[str, list[str]]:
    partes: list[str] = []
    params: list[str] = []
    if criterios.get("cpf"):
        if not colunas.get("cpf"):
            raise ValueError("A base não tem coluna de CPF.")
        partes.append(f"`{colunas['cpf']}` = %s")
        params.append(criterios["cpf"])
    if criterios.get("processo"):
        if not colunas.get("processo"):
            raise ValueError("A base não tem coluna de processo.")
        partes.append(f"TRIM(COALESCE(`{colunas['processo']}`, '')) = %s")
        params.append(criterios["processo"])
    if criterios.get("incidente"):
        if not colunas.get("incidente"):
            raise ValueError("A base não tem coluna de incidente.")
        partes.append(f"TRIM(COALESCE(`{colunas['incidente']}`, '')) = %s")
        params.append(criterios["incidente"])
    if not partes:
        raise ValueError("Informe o CPF ou o número do processo.")
    return " AND ".join(partes), params


def caso_de_linha(row: dict, colunas: dict[str, str]) -> dict[str, str]:
    def texto(coluna: str) -> str:
        if not coluna:
            return ""
        return " ".join(str(row.get(coluna) or "").split())

    cpf = re.sub(r"\D", "", texto(colunas.get("cpf") or ""))
    if len(cpf) > 11:
        cpf = cpf[-11:]
    if len(cpf) != 11:
        cpf = ""
    return {
        "nome": texto(colunas.get("nome") or ""),
        "cpf": cpf,
        "processo": texto(colunas.get("processo") or ""),
        "incidente": texto(colunas.get("incidente") or ""),
    }


def _mysql_cfg(prefixo: str, base_padrao: str = "") -> dict[str, Any]:
    base = (os.getenv(f"{prefixo}_DATABASE") or base_padrao).strip().strip("'\"")
    if not base:
        raise RuntimeError(f"MySQL não configurado ({prefixo}_DATABASE).")
    try:
        porta = int((os.getenv(f"{prefixo}_PORT") or "3306").strip())
    except ValueError:
        porta = 3306
    return {
        "host": (os.getenv(f"{prefixo}_HOST") or "127.0.0.1").strip().strip("'\"") or "127.0.0.1",
        "port": porta,
        "database": base,
        "user": (os.getenv(f"{prefixo}_USER") or "root").strip().strip("'\"") or "root",
        "password": (os.getenv(f"{prefixo}_PASSWORD") or "").strip().strip("'\""),
        "connection_timeout": 15,
    }


def _conectar_mysql(prefixo: str, base_padrao: str = ""):
    import mysql.connector

    return mysql.connector.connect(
        **_mysql_cfg(prefixo, base_padrao),
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
    )


def localizar_casos(*, cpf: str = "", processo: str = "", incidente: str = "") -> list[dict[str, str]]:
    """Casos de precainfosnew para preencher o formulário. Não grava nada."""
    criterios = criterios_busca(cpf, processo, incidente)
    conn = _conectar_mysql("FLASK_MYSQL")
    try:
        cur = conn.cursor()
        cur.execute("SHOW COLUMNS FROM precainfosnew")
        fields = {str(linha[0]) for linha in (cur.fetchall() or []) if linha and linha[0]}
        cur.close()
        colunas = {
            "cpf": _coluna(fields, "CPF", "cpf"),
            "nome": _coluna(fields, "Requerente", "requerente"),
            "processo": _coluna(fields, "Numero_de_Processo", "numero_de_processo"),
            "incidente": _coluna(fields, "Numero_do_Incidente", "numero_do_incidente"),
        }
        where, params = where_precainfos(criterios, colunas)
        selecionadas = [f"`{coluna}`" for coluna in colunas.values() if coluna]
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"""
            SELECT {", ".join(selecionadas)}
            FROM precainfosnew
            WHERE {where}
            ORDER BY id DESC
            LIMIT {_LIMITE_CASOS}
            """,
            tuple(params),
        )
        linhas = [caso_de_linha(linha, colunas) for linha in (cur.fetchall() or [])]
        cur.close()
    finally:
        conn.close()
    vistos: set[tuple[str, str, str, str]] = set()
    casos: list[dict[str, str]] = []
    for linha in linhas:
        chave = (linha["nome"], linha["cpf"], linha["processo"], linha["incidente"])
        if chave in vistos or not any(chave):
            continue
        vistos.add(chave)
        casos.append(linha)
    return casos


def pares_de_contatos(telefones, emails) -> list[tuple[str, str]]:
    """Telefones e e-mails já no formato da blacklist, sem repetir."""
    banco = _banco()
    itens: list[tuple[str, str]] = []
    vistos: set[tuple[str, str]] = set()
    for bruto in telefones or []:
        telefone = banco["normalizar"]("TELEFONE", bruto)
        par = ("TELEFONE", telefone)
        if not telefone or par in vistos:
            continue
        vistos.add(par)
        itens.append(par)
    for bruto in emails or []:
        email = banco["normalizar"]("EMAIL", bruto)
        par = ("EMAIL", email)
        if not email or par in vistos:
            continue
        vistos.add(par)
        itens.append(par)
    return itens


def _listar_contatos(cpf: str) -> tuple[list[str], list[str]]:
    conn = _conectar_mysql("EDA_MYSQL", "plataforma_central")
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT DISTINCT telefone
            FROM sms
            WHERE cpf = %s OR LPAD(cpf, 11, '0') = %s
            LIMIT {_LIMITE_CONTATOS}
            """,
            (cpf, cpf),
        )
        telefones = [str(linha[0]) for linha in (cur.fetchall() or []) if linha and linha[0]]
        cur.execute(
            f"""
            SELECT DISTINCT email
            FROM emails
            WHERE cpf = %s OR LPAD(cpf, 11, '0') = %s
            LIMIT {_LIMITE_CONTATOS}
            """,
            (cpf, cpf),
        )
        emails = [str(linha[0]) for linha in (cur.fetchall() or []) if linha and linha[0]]
        cur.close()
    finally:
        conn.close()
    return telefones, emails


def incluir_contatos(cpf: str, motivo: str) -> dict[str, Any]:
    """Busca sms e emails do CPF e grava cada um na blacklist com o motivo informado."""
    motivo_ok = _motivo_manual(motivo)
    cpf_ok = _cpf_obrigatorio(cpf, "Informe o CPF para buscar telefones e e-mails.")
    telefones, emails = _listar_contatos(cpf_ok)
    itens = pares_de_contatos(telefones, emails)
    gravar = _banco()["adicionar"]
    for tipo, valor in itens:
        gravar(tipo, valor, motivo_ok)
    return {
        "telefones": [valor for tipo, valor in itens if tipo == "TELEFONE"],
        "emails": [valor for tipo, valor in itens if tipo == "EMAIL"],
        "gravados": len(itens),
    }


def adicionar(form: dict[str, str]) -> str:
    motivo = _motivo_manual(form.get("motivo") or "")
    itens = registros_do_caso(form)
    gravar = _banco()["adicionar"]
    for tipo, valor in itens:
        gravar(tipo, valor, motivo)
    return "Adicionado à blacklist: " + ", ".join(_rotulo(tipo, valor) for tipo, valor in itens)


def editar(row_id: int, form: dict[str, str]) -> str:
    if row_id <= 0:
        raise LookupError("Registro não encontrado.")
    tipo, valor = valor_gravado(form)
    motivo = _motivo_manual(form.get("motivo") or "")
    extras = acompanhantes(tipo, form)
    conn = _banco()["conectar"]()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM blacklist WHERE id = %s AND ativo = 1", (row_id,))
        if cur.fetchone() is None:
            cur.close()
            raise LookupError("Registro não encontrado.")
        cur.execute(
            "SELECT id FROM blacklist WHERE tipo = %s AND valor = %s AND id <> %s",
            (tipo, valor, row_id),
        )
        if cur.fetchone() is not None:
            cur.close()
            raise ValueError("Já existe um registro com este tipo e valor.")
        cur.execute(
            """
            UPDATE blacklist
               SET tipo = %s, valor = %s, motivo = %s
             WHERE id = %s
            """,
            (tipo, valor, motivo, row_id),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    if extras:
        gravar = _banco()["adicionar"]
        for extra_tipo, extra_valor in extras:
            gravar(extra_tipo, extra_valor, motivo)
    texto = "Registro atualizado."
    if extras:
        texto += " Também incluído: " + ", ".join(_rotulo(t, v) for t, v in extras) + "."
    return texto


def remover(row_id: int) -> str:
    if row_id <= 0:
        raise LookupError("Registro não encontrado.")
    conn = _banco()["conectar"]()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE blacklist SET ativo = 0 WHERE id = %s AND ativo = 1", (row_id,))
        changed = cur.rowcount
        conn.commit()
        cur.close()
    finally:
        conn.close()
    if not changed:
        raise LookupError("Registro não encontrado.")
    return "Entrada removida da blacklist."


def _linhas_csv(conteudo: bytes) -> list[tuple[int, dict[str, str]]]:
    try:
        texto = conteudo.decode("utf-8-sig")
    except UnicodeDecodeError:
        texto = conteudo.decode("latin-1")
    primeira = texto.splitlines()[0] if texto.splitlines() else ""
    delimitador = ";" if ";" in primeira and "," not in primeira else ","
    leitor = csv.DictReader(io.StringIO(texto), delimiter=delimitador)
    if not leitor.fieldnames:
        raise ValueError("CSV precisa das colunas tipo e valor.")
    campos = {str(c or "").strip().lower().replace(" ", "_") for c in leitor.fieldnames}
    if "tipo" not in campos or "valor" not in campos:
        raise ValueError("CSV precisa das colunas tipo e valor.")
    linhas: list[tuple[int, dict[str, str]]] = []
    for numero, row in enumerate(leitor, start=2):
        normal: dict[str, str] = {}
        for chave, valor in row.items():
            if chave is None:
                continue
            normal[str(chave).strip().lower().replace(" ", "_")] = (valor or "").strip()
        if any(normal.values()):
            linhas.append((numero, normal))
    return linhas


def preparar_csv_inclusao(conteudo: bytes) -> tuple[bytes | None, list[str]]:
    """Recusa nome sem CPF e processo sem nome e sem CPF. Devolve o CSV já com esses vínculos."""
    recusados: list[str] = []
    aceitos: list[dict[str, str]] = []
    for numero, row in _linhas_csv(conteudo):
        tipo = (row.get("tipo") or "").strip().upper()
        valor = (row.get("valor") or "").strip()
        motivo = (row.get("motivo") or "").strip()
        ativo = (row.get("ativo") or "").strip()
        base = {"tipo": tipo, "valor": valor, "motivo": motivo, "ativo": ativo}
        if tipo in ("NOME", "PROCESSO_INCIDENTE") and valor:
            try:
                extras = acompanhantes(tipo, {"cpf": row.get("cpf") or "", "nome": row.get("nome") or ""})
            except ValueError as exc:
                recusados.append(f"Linha {numero}: {exc}")
                continue
        else:
            extras = []
        aceitos.append(base)
        for extra_tipo, extra_valor in extras:
            aceitos.append(
                {"tipo": extra_tipo, "valor": extra_valor, "motivo": motivo, "ativo": ativo or "1"}
            )
    if not aceitos:
        return None, recusados
    buf = io.StringIO()
    escritor = csv.DictWriter(buf, fieldnames=["tipo", "valor", "motivo", "ativo"], lineterminator="\n")
    escritor.writeheader()
    escritor.writerows(aceitos)
    return buf.getvalue().encode("utf-8"), recusados


def importar_csv(conteudo: bytes) -> tuple[str, list[str]]:
    preparado, recusados = preparar_csv_inclusao(conteudo)
    if preparado is None:
        if not recusados:
            recusados = [
                "Nenhuma linha importada. Use colunas tipo e valor; "
                "nome exige cpf e processo exige nome ou cpf."
            ]
        raise ValueError(recusados[0] if len(recusados) == 1 else " ".join(recusados[:8]))
    banco = _banco()
    buf = io.BytesIO(preparado)
    res = banco["importar"](buf)
    dbn = (os.getenv("EDA_MYSQL_DATABASE") or "plataforma_central").strip()
    if not res.get("importados"):
        erros = list(res.get("erros") or [])
        if not erros:
            erros = [
                "Nenhuma linha importada. Use colunas tipo e valor; "
                "tipos CPF, NOME, TELEFONE, EMAIL ou PROCESSO_INCIDENTE."
            ]
        raise ValueError(erros[0] if len(erros) == 1 else " ".join(erros[:8]))
    partes = [
        f"CSV aplicado na base MySQL `{dbn}`: {res['importados']} linha(s) gravada(s) "
        "(upsert por tipo+valor)."
    ]
    if res.get("ignorados"):
        partes.append(f"{res['ignorados']} ignorada(s) (vazio ou tipo inválido).")
    if res.get("pulados_ativo"):
        partes.append(f"{res['pulados_ativo']} omitida(s) (ativo = 0 / falso).")
    ignore = res.get("colunas_ignoradas") or []
    if ignore:
        amostra = ", ".join(ignore[:12])
        if len(ignore) > 12:
            amostra += " …"
        partes.append(f"Colunas extra ignoradas: {amostra}.")
    if recusados:
        partes.append(
            f"{len(recusados)} linha(s) recusada(s): nome sem CPF ou processo sem nome e sem CPF."
        )
    avisos = recusados[:8] + list(res.get("erros") or [])
    return " ".join(partes), avisos[:8]
