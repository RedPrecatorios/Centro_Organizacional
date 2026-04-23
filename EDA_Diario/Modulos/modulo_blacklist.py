import re

import pandas as pd

# ─────────────────────────────────────────────
# Filtragem de blacklist para as abas sms e Emails
# ─────────────────────────────────────────────
# Regras:
#   - Bloqueio total  (CPF ou NOME): remove TODOS os contatos da pessoa
#   - Bloqueio parcial (TELEFONE ou EMAIL): remove apenas aquele valor


def _normalizar(valor: str) -> str:
    """Normaliza texto generico: strip + uppercase."""
    return str(valor).strip().upper() if valor else ""


def _normalizar_cpf_cmp(cpf) -> str:
    """So digitos, 11 posicoes — alinha planilha (pontuacao/float Excel) com a blacklist."""
    if cpf is None or (isinstance(cpf, float) and pd.isna(cpf)):
        return ""
    s = str(cpf).strip()
    if s.lower() in ("", "nan", "none"):
        return ""
    if s.endswith(".0") and s[:-2].replace(".", "").isdigit():
        s = s[:-2]
    dig = re.sub(r"\D", "", s)
    if not dig:
        return ""
    return dig.zfill(11) if len(dig) <= 11 else dig


def _normalizar_tel_cmp(telefone) -> str:
    """So digitos para bater com TELEFONE na blacklist."""
    if telefone is None or (isinstance(telefone, float) and pd.isna(telefone)):
        return ""
    dig = re.sub(r"\D", "", str(telefone).strip())
    return dig if dig else ""


def _telefone_bloqueado(telefone, bl_tel: set) -> bool:
    """True se o numero bater com alguma entrada (com ou sem prefixo 55)."""
    d = _normalizar_tel_cmp(telefone)
    if not d or not bl_tel:
        return False
    if d in bl_tel:
        return True
    if d.startswith("55") and len(d) > 11 and d[2:] in bl_tel:
        return True
    cand55 = "55" + d
    if cand55 in bl_tel:
        return True
    return False


def _normalizar_nome_cmp(nome) -> str:
    """Espacos colapsados + maiusculas — evita falha por espacos duplos."""
    if nome is None or (isinstance(nome, float) and pd.isna(nome)):
        return ""
    s = str(nome).strip()
    if not s or s.lower() == "nan":
        return ""
    return " ".join(s.split()).upper()


def _normalizar_email_cmp(email) -> str:
    if email is None or (isinstance(email, float) and pd.isna(email)):
        return ""
    s = str(email).strip()
    return s.upper() if s and s.lower() != "nan" else ""


def normalizar_valor_para_blacklist(tipo: str, valor) -> str:
    """
    Mesma logica usada na filtragem — valores da blacklist no banco sao normalizados assim ao carregar.
    """
    t = str(tipo).upper()
    if t == "CPF":
        return _normalizar_cpf_cmp(valor)
    if t == "TELEFONE":
        return _normalizar_tel_cmp(valor)
    if t == "NOME":
        return _normalizar_nome_cmp(valor)
    if t == "EMAIL":
        return _normalizar_email_cmp(valor)
    return ""


def _pessoa_bloqueada(cpf, nome, bl: dict[str, set]) -> bool:
    """Retorna True se o CPF ou nome da pessoa estiver na blacklist."""
    return _motivo_bloqueio_pessoa(cpf, nome, bl) is not None


def _motivo_bloqueio_pessoa(cpf, nome, bl: dict[str, set]) -> str | None:
    """'PESSOA_CPF', 'PESSOA_NOME' ou None."""
    cpf_norm  = _normalizar_cpf_cmp(cpf)
    nome_norm = _normalizar_nome_cmp(nome)
    if cpf_norm and cpf_norm in bl["CPF"]:
        return "PESSOA_CPF"
    if nome_norm and nome_norm in bl["NOME"]:
        return "PESSOA_NOME"
    return None


def filtrar_registros_por_blacklist(
    df: pd.DataFrame,
    registros_tel: list[list[tuple]],
    registros_email: list[list[tuple]],
    bl: dict[str, set],
) -> tuple[list[list[tuple]], list[list[tuple]], int, int, int, list[dict]]:
    """
    Aplica a blacklist sobre os registros antes de gerar as abas sms e Emails.

    Regras:
    - CPF ou NOME na blacklist -> remove TODOS os telefones e emails da linha
    - TELEFONE especifico na blacklist -> remove apenas aquele numero
    - EMAIL especifico na blacklist -> remove apenas aquele email

    Args:
        df:               DataFrame principal (deve ter colunas CPF e Requerente).
        registros_tel:    Lista de [(telefone, is_red), ...] por linha.
        registros_email:  Lista de [(email, is_red), ...] por linha.
        bl:               Blacklist carregada via carregar_blacklist().

    Returns:
        (registros_tel_filtrados, registros_email_filtrados,
         total_pessoas_bloqueadas, total_tel_bloqueados, total_email_bloqueados,
         detalhes) — detalhes = lista de dicts para relatorio/CSV
    """
    tel_filtrado   = []
    email_filtrado = []
    pessoas_bloq   = 0
    tel_bloq       = 0
    email_bloq     = 0
    detalhes: list[dict] = []

    def _cpf_para_regra(r) -> str:
        """Alinha com o cruzamento P2/P3: prefere 2.ª coluna CPF (CMP/IMP)."""
        for col in ("CPF.1", "cpf.1"):
            if col in r.index:
                v = r.get(col)
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    s = str(v).strip()
                    if s and s.lower() != "nan":
                        return s
        return r.get("CPF")

    def _row_meta(r) -> dict:
        proc = r.get("Numero_de_Processo")
        if proc is not None and not (isinstance(proc, float) and pd.isna(proc)):
            proc_str = str(proc).strip()
        else:
            proc_str = ""
        cpf = _cpf_para_regra(r)
        cpf_s = "" if cpf is None or (isinstance(cpf, float) and pd.isna(cpf)) else str(cpf).strip()
        req = r.get("Requerente")
        req_s = "" if req is None or (isinstance(req, float) and pd.isna(req)) else str(req).strip()
        return {"cpf": cpf_s, "requerente": req_s, "numero_processo": proc_str}

    n = len(df)
    if len(registros_tel) != n or len(registros_email) != n:
        raise ValueError(
            f"[BLACKLIST] Inconsistencia: df tem {n} linhas, "
            f"registros_tel {len(registros_tel)}, registros_email {len(registros_email)}."
        )

    # Posicao 0..n-1 (iloc) alinha com as listas de contatos (nao usar rotulo de indice do DataFrame).
    for pos in range(n):
        row = df.iloc[pos]
        meta = _row_meta(row)

        # ── Bloqueio total ────────────────────────────────────────────────────
        motivo_p = _motivo_bloqueio_pessoa(
            _cpf_para_regra(row), row.get("Requerente"), bl
        )
        if motivo_p:
            n_t = len(registros_tel[pos])
            n_e = len(registros_email[pos])
            tel_bloq   += n_t
            email_bloq += n_e
            tel_filtrado.append([])
            email_filtrado.append([])
            pessoas_bloq += 1
            detalhes.append({
                "tipo_bloqueio": motivo_p,
                "cpf":            meta["cpf"],
                "requerente":     meta["requerente"],
                "numero_processo": meta["numero_processo"],
                "valor_removido": f"todos os contatos ({n_t} tel, {n_e} emails)",
            })
            continue

        # ── Bloqueio parcial — telefones ──────────────────────────────────────
        tel_limpo = []
        for telefone, is_red in registros_tel[pos]:
            if _telefone_bloqueado(telefone, bl["TELEFONE"]):
                tel_bloq += 1
                detalhes.append({
                    "tipo_bloqueio":  "TELEFONE",
                    "cpf":            meta["cpf"],
                    "requerente":     meta["requerente"],
                    "numero_processo": meta["numero_processo"],
                    "valor_removido": str(telefone).strip(),
                })
            else:
                tel_limpo.append((telefone, is_red))
        tel_filtrado.append(tel_limpo)

        # ── Bloqueio parcial — emails ─────────────────────────────────────────
        email_limpo = []
        for email, is_red in registros_email[pos]:
            if _normalizar_email_cmp(email) in bl["EMAIL"]:
                email_bloq += 1
                detalhes.append({
                    "tipo_bloqueio":  "EMAIL",
                    "cpf":            meta["cpf"],
                    "requerente":     meta["requerente"],
                    "numero_processo": meta["numero_processo"],
                    "valor_removido": str(email).strip(),
                })
            else:
                email_limpo.append((email, is_red))
        email_filtrado.append(email_limpo)

    return tel_filtrado, email_filtrado, pessoas_bloq, tel_bloq, email_bloq, detalhes
