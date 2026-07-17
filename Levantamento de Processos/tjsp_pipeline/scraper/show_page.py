"""Extract numero_de_processo / numero_do_incidente and party names from show.do capas."""

from __future__ import annotations

import html as html_lib
import logging
import re
import unicodedata
from dataclasses import dataclass

from bs4 import BeautifulSoup, NavigableString, Tag

logger = logging.getLogger(__name__)

PRECATORIO_TITLE_PATTERN = re.compile(
    r"Precat[oó]rio\s*\((\d+-\d+\.\d+\.\d+\.\d+\.\d+)\)\s*\((\d+)\)",
    re.IGNORECASE,
)
# Qualquer classe processuais na capa: "... (CNJ) (incidente)"
GENERIC_TITLE_PATTERN = re.compile(
    r"\((\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})\)\s*\((\d+)\)",
)
NUMERO_PROCESSO_PATTERN = re.compile(
    r"Precat[oó]rio.*?(\d+-\d+\.\d+\.\d+\.\d+\.\d+).*?\((\d+)\)",
    re.IGNORECASE | re.DOTALL,
)
NUMERO_INCIDENTE_PATTERN = re.compile(
    r"Precat[oó]rio.*?\d+-\d+\.\d+\.\d+\.\d+\.\d+.*?\((\d+)\)",
    re.IGNORECASE | re.DOTALL,
)
CNJ_PATTERN = re.compile(r"\b(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})\b")
_DISALLOWED_NAME_CHARS = re.compile(r"[^A-Z0-9\s]")


@dataclass(frozen=True)
class PrecatorioRecord:
    numero_de_processo: str
    numero_do_incidente: str
    processo_codigo: str
    url: str
    label: str
    processo_principal: str | None = None
    partes_capa: tuple[str, ...] = ()

    @property
    def txt_line(self) -> str:
        incidente = str(int(self.numero_do_incidente))
        return f"{self.numero_de_processo}/{incidente}"


def _text_before_br(element: Tag) -> str | None:
    """Mesmo critério do REFACTOR EsajHtmlParser: nome da parte antes do <br> (advogado)."""
    parts: list[str] = []
    for child in element.children:
        if isinstance(child, Tag) and child.name == "br":
            break
        if isinstance(child, NavigableString):
            parts.append(str(child))
    text = "".join(parts).strip()
    return text or None


def extract_partes_capa(page_html: str) -> list[str]:
    """Nomes das partes na capa (td.nomeParteEAdvogado) — sem abrir autos."""
    text = html_lib.unescape(page_html or "")
    soup = BeautifulSoup(text, "html.parser")
    names: list[str] = []
    seen: set[str] = set()
    for cell in soup.select("td.nomeParteEAdvogado"):
        name = _text_before_br(cell)
        if not name:
            # fallback: texto completo da célula
            name = cell.get_text(" ", strip=True) or None
        if not name:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        names.append(name)
    return names


def normalize_name_key(value: str | None) -> str:
    """Normaliza para comparação: sem acento, maiúsculas, só A-Z/0-9/espaço."""
    raw = " ".join(str(value or "").split()).strip()
    if not raw:
        return ""
    nfd = unicodedata.normalize("NFD", raw)
    without = "".join(ch for ch in nfd if unicodedata.category(ch) != "Mn")
    upper = without.upper()
    cleaned = _DISALLOWED_NAME_CHARS.sub(" ", upper)
    return re.sub(r"\s+", " ", cleaned).strip()


def names_match(target: str, partes: list[str] | tuple[str, ...]) -> bool:
    """
    True se o nome Monday bate com alguma parte da capa.

    Aceita igualdade, contenção e subconjunto de tokens (ex.: JOSE GAMERO ⊂ JOSE GAMERO SILVA).
    """
    needle = normalize_name_key(target)
    if len(needle) < 3:
        return False
    needle_tokens = [t for t in needle.split() if len(t) > 1]
    if not needle_tokens:
        return False

    for parte in partes:
        hay = normalize_name_key(parte)
        if not hay:
            continue
        if needle == hay or needle in hay or hay in needle:
            return True
        hay_tokens = set(hay.split())
        if needle_tokens and all(t in hay_tokens for t in needle_tokens):
            return True
        # Invertido: tokens da capa (se curtos) contidos no alvo
        short = [t for t in hay.split() if len(t) > 1]
        if short and len(short) >= 2 and all(t in set(needle_tokens) for t in short):
            return True
    return False


def extract_from_show_html(
    page_html: str,
    *,
    url: str,
    processo_codigo: str,
    label: str = "",
    require_precatorio_title: bool = False,
) -> PrecatorioRecord | None:
    text = html_lib.unescape(page_html or "")
    soup = BeautifulSoup(text, "html.parser")

    numero_processo: str | None = None
    numero_incidente: str | None = None

    title_match = PRECATORIO_TITLE_PATTERN.search(text)
    if title_match:
        numero_processo, numero_incidente = title_match.group(1), title_match.group(2)
    elif not require_precatorio_title:
        generic = GENERIC_TITLE_PATTERN.search(text)
        if generic:
            numero_processo, numero_incidente = generic.group(1), generic.group(2)

    if not numero_processo:
        proc_matches = NUMERO_PROCESSO_PATTERN.findall(text)
        inc_matches = NUMERO_INCIDENTE_PATTERN.findall(text)
        if proc_matches:
            numero_processo = (
                proc_matches[0][0] if isinstance(proc_matches[0], tuple) else proc_matches[0]
            )
        if inc_matches:
            numero_incidente = inc_matches[0]

    if not numero_processo:
        for selector in ("span.unj-larger", "span.unj-larger-1", "a.unj-larger-1"):
            for el in soup.select(selector):
                el_text = el.get_text(strip=True)
                m = CNJ_PATTERN.search(el_text)
                if m:
                    numero_processo = m.group(1)
                    if not numero_incidente:
                        im = re.search(r"\((\d+)\)\s*$", el_text)
                        if im:
                            numero_incidente = im.group(1)
                    break
            if numero_processo:
                break

    if not numero_incidente:
        unj = soup.select_one("span.unj-larger, span.unj-larger-1")
        if unj:
            m = re.search(r"\((\d+)\)\s*$", unj.get_text(strip=True))
            if m:
                numero_incidente = m.group(1)

    if not numero_processo or not numero_incidente:
        logger.warning(
            "Could not extract process/incident from show.do | codigo=%s url=%s",
            processo_codigo,
            url[:80],
        )
        return None

    processo_principal = None
    principal_el = soup.select_one("a.processoPrinc, a.unj-larger-1.processoPrinc")
    if principal_el:
        processo_principal = principal_el.get_text(strip=True) or None

    partes = extract_partes_capa(page_html)

    record = PrecatorioRecord(
        numero_de_processo=numero_processo.strip(),
        numero_do_incidente=str(int(numero_incidente.strip())),
        processo_codigo=processo_codigo,
        url=url,
        label=label,
        processo_principal=processo_principal,
        partes_capa=tuple(partes),
    )
    logger.info(
        "Extracted incidente | %s | codigo=%s | partes=%s",
        record.txt_line,
        processo_codigo,
        len(partes),
    )
    return record
