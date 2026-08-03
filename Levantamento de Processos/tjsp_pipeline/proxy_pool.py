"""Pool de proxies Webshare (ip:porta:user:senha) com round-robin."""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from pathlib import Path

from tjsp_pipeline.config import ProxyConfig, PROJECT_ROOT

logger = logging.getLogger(__name__)

_DEFAULT_LIST_NAME = "webshare-proxies-list.txt"


@dataclass(frozen=True)
class ProxyEndpoint:
    host: str
    port: int
    username: str
    password: str

    def to_proxy_config(self) -> ProxyConfig:
        return ProxyConfig(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
        )


def parse_proxy_line(line: str) -> ProxyEndpoint | None:
    text = line.strip()
    if not text or text.startswith("#"):
        return None
    parts = text.split(":")
    if len(parts) != 4:
        return None
    host, port_raw, username, password = parts
    if not host or not username or not password:
        return None
    try:
        port = int(port_raw)
    except ValueError:
        return None
    return ProxyEndpoint(
        host=host.strip(),
        port=port,
        username=username.strip(),
        password=password.strip(),
    )


def resolve_proxies_list_path(*, refactor_path: Path | None = None) -> Path:
    raw = (os.getenv("WEBSHARE_PROXIES_LIST") or "").strip()
    if raw:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (PROJECT_ROOT / path).resolve()
        return path

    candidates: list[Path] = []
    if refactor_path is not None:
        candidates.append(Path(refactor_path) / _DEFAULT_LIST_NAME)
    candidates.extend(
        [
            PROJECT_ROOT / _DEFAULT_LIST_NAME,
            PROJECT_ROOT.parent / "REFACTOR_TJSP" / _DEFAULT_LIST_NAME,
            PROJECT_ROOT.parent / "REFACTOR_TJSP-main" / _DEFAULT_LIST_NAME,
        ]
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    return (candidates[0] if candidates else PROJECT_ROOT / _DEFAULT_LIST_NAME).resolve()


def load_proxy_endpoints(path: Path) -> list[ProxyEndpoint]:
    if not path.is_file():
        return []
    endpoints: list[ProxyEndpoint] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        endpoint = parse_proxy_line(line)
        if endpoint is not None:
            endpoints.append(endpoint)
    return endpoints


class ProxyPool:
    def __init__(self, endpoints: list[ProxyEndpoint]) -> None:
        self._endpoints = list(endpoints)
        self._index = 0
        self._lock = threading.Lock()

    @property
    def size(self) -> int:
        return len(self._endpoints)

    def next(self) -> ProxyEndpoint | None:
        if not self._endpoints:
            return None
        with self._lock:
            endpoint = self._endpoints[self._index % len(self._endpoints)]
            self._index += 1
            return endpoint


_pool_lock = threading.Lock()
_shared_pool: ProxyPool | None = None
_shared_pool_path: str | None = None


def get_shared_proxy_pool(*, refactor_path: Path | None = None, force_reload: bool = False) -> ProxyPool:
    global _shared_pool, _shared_pool_path
    path = resolve_proxies_list_path(refactor_path=refactor_path)
    path_key = str(path)
    with _pool_lock:
        if force_reload or _shared_pool is None or _shared_pool_path != path_key:
            endpoints = load_proxy_endpoints(path)
            _shared_pool = ProxyPool(endpoints)
            _shared_pool_path = path_key
            if endpoints:
                logger.info(
                    "Proxy pool carregado | arquivo=%s | endpoints=%s",
                    path.name,
                    len(endpoints),
                )
            else:
                logger.warning(
                    "Proxy pool vazio | arquivo=%s — fallback WEBSHARE_PROXY_*",
                    path,
                )
        return _shared_pool


def next_proxy_config(*, refactor_path: Path | None = None) -> ProxyConfig | None:
    endpoint = get_shared_proxy_pool(refactor_path=refactor_path).next()
    return endpoint.to_proxy_config() if endpoint else None
