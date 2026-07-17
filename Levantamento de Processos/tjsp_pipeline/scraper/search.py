"""Parse e-SAJ search results for Precatório incident links."""

from __future__ import annotations

import html as html_lib
import logging
import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

INCIDENTE_PATTERN = re.compile(
    r'<a\s+class="incidente"[^>]*href="([^"]*processo\.codigo=([A-Z0-9]+)[^"]*)"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
FALLBACK_PATTERN = re.compile(
    r'href="([^"]*show\.do[^"]*processo\.codigo=([A-Z0-9]+)[^"]*)"[^>]*>.*?[Pp]recat[oó]rio',
    re.IGNORECASE | re.DOTALL,
)
PROCESSO_CODIGO_PATTERN = re.compile(r"processo\.codigo=([A-Z0-9]+)", re.IGNORECASE)


@dataclass(frozen=True)
class PrecatorioLink:
    url: str
    processo_codigo: str
    label: str
    source: str  # search | js | fallback

    @property
    def is_incident_child(self) -> bool:
        return "cdProcessoMaster" in self.url

    @property
    def is_precatorio_label(self) -> bool:
        return "precat" in self.label.lower()


def filter_precatorio_links(links: list[PrecatorioLink]) -> list[PrecatorioLink]:
    """Keep only true Precatório incidents; prefer child incidents over parent process."""
    filtered = [link for link in links if link.is_precatorio_label or link.is_incident_child]
    filtered.sort(
        key=lambda link: (
            not link.is_precatorio_label,
            not link.is_incident_child,
            link.processo_codigo,
        )
    )
    return filtered


def _normalize_href(href: str, base: str = "https://esaj.tjsp.jus.br/cpopg/") -> str:
    return urljoin(base, href.replace("&amp;", "&"))


def _extract_label(raw_html: str) -> str:
    text = BeautifulSoup(raw_html, "html.parser").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text)


def parse_precatorio_links_from_html(page_html: str) -> list[PrecatorioLink]:
    """Extract unique Precatório show.do links from search or process page HTML."""
    unescaped = html_lib.unescape(page_html or "")
    found: dict[str, PrecatorioLink] = {}

    for pattern, source in (
        (INCIDENTE_PATTERN, "search"),
        (FALLBACK_PATTERN, "fallback"),
    ):
        for match in pattern.finditer(unescaped):
            if source == "fallback":
                href, codigo = match.group(1), match.group(2)
                label = "Precatório"
            else:
                href, codigo, inner = match.group(1), match.group(2), match.group(3)
                label = _extract_label(inner)
                if "precat" not in label.lower():
                    continue
            url = _normalize_href(href)
            found[codigo] = PrecatorioLink(
                url=url,
                processo_codigo=codigo,
                label=label or "Precatório",
                source=source,
            )

    soup = BeautifulSoup(unescaped, "html.parser")

    # Modern e-SAJ search: expanded incidentes use linkProcesso + classeProcesso
    for block in soup.select("div.home__lista-de-processos, li"):
        classe = block.select_one("div.classeProcesso")
        if classe and "precat" not in classe.get_text(" ", strip=True).lower():
            continue
        anchor = block.select_one('a.linkProcesso[href*="show.do"]')
        if not anchor:
            continue
        href = anchor.get("href") or ""
        codigo_match = PROCESSO_CODIGO_PATTERN.search(href)
        if not codigo_match:
            continue
        codigo = codigo_match.group(1)
        label = anchor.get_text(" ", strip=True) or "Precatório"
        if "precat" not in label.lower() and (
            not classe or "precat" not in classe.get_text(" ", strip=True).lower()
        ):
            continue
        url = _normalize_href(href)
        found[codigo] = PrecatorioLink(
            url=url,
            processo_codigo=codigo,
            label=label,
            source="linkProcesso",
        )

    for anchor in soup.select("a.incidente, a.linkProcesso[href*='show.do']"):
        href = anchor.get("href") or ""
        text = anchor.get_text(" ", strip=True)
        parent = anchor.find_parent("div.home__lista-de-processos") or anchor.find_parent("li")
        classe_text = ""
        if parent:
            cp = parent.select_one("div.classeProcesso")
            if cp:
                classe_text = cp.get_text(" ", strip=True)
        is_precatorio = (
            "precat" in text.lower()
            or "precat" in classe_text.lower()
        )
        if not is_precatorio:
            continue
        codigo_match = PROCESSO_CODIGO_PATTERN.search(href)
        if not codigo_match:
            continue
        codigo = codigo_match.group(1)
        url = _normalize_href(href)
        found.setdefault(
            codigo,
            PrecatorioLink(
                url=url,
                processo_codigo=codigo,
                label=text or classe_text or "Precatório",
                source="bs4",
            ),
        )

    links = list(found.values())
    logger.info("Parsed %s unique Precatório link(s) from HTML", len(links))
    return links


def parse_all_incidente_links_from_html(page_html: str) -> list[PrecatorioLink]:
    """
    Extrai TODOS os links de incidente/show.do da página (não só Precatório).

    Usado na busca por processo + filtro de nome na capa.
    """
    unescaped = html_lib.unescape(page_html or "")
    found: dict[str, PrecatorioLink] = {}
    soup = BeautifulSoup(unescaped, "html.parser")

    for match in INCIDENTE_PATTERN.finditer(unescaped):
        href, codigo, inner = match.group(1), match.group(2), match.group(3)
        label = _extract_label(inner) or "Incidente"
        url = _normalize_href(href)
        found[codigo] = PrecatorioLink(
            url=url,
            processo_codigo=codigo,
            label=label,
            source="search",
        )

    for anchor in soup.select("a.incidente, a.linkProcesso[href*='show.do']"):
        href = anchor.get("href") or ""
        codigo_match = PROCESSO_CODIGO_PATTERN.search(href)
        if not codigo_match:
            continue
        codigo = codigo_match.group(1)
        text = anchor.get_text(" ", strip=True)
        parent = anchor.find_parent("div.home__lista-de-processos") or anchor.find_parent("li")
        classe_text = ""
        if parent:
            cp = parent.select_one("div.classeProcesso")
            if cp:
                classe_text = cp.get_text(" ", strip=True)
        label = text or classe_text or "Incidente"
        url = _normalize_href(href)
        found.setdefault(
            codigo,
            PrecatorioLink(
                url=url,
                processo_codigo=codigo,
                label=label,
                source="bs4",
            ),
        )

    links = list(found.values())
    logger.info("Parsed %s unique incidente link(s) from HTML (all kinds)", len(links))
    return links


def merge_js_links(
    html_links: list[PrecatorioLink],
    js_links: list[dict],
) -> list[PrecatorioLink]:
    merged = {link.processo_codigo: link for link in html_links}
    for item in js_links:
        href = item.get("href") or ""
        text = item.get("text") or "Precatório"
        codigo_match = PROCESSO_CODIGO_PATTERN.search(href)
        if not codigo_match:
            continue
        codigo = codigo_match.group(1)
        merged.setdefault(
            codigo,
            PrecatorioLink(
                url=_normalize_href(href),
                processo_codigo=codigo,
                label=text,
                source="js",
            ),
        )
    return list(merged.values())


def expand_nested_incidentes(page_html: str) -> str:
    """Return HTML with incidentes section — e-SAJ may lazy-load; caller loads full page."""
    return page_html
