"""Chrome MV3 extension for Webshare authenticated proxy rotation."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from tjsp_pipeline.config import ProxyConfig

MANIFEST_TEMPLATE = """{
    "version": "<ext_ver>",
    "manifest_version": 3,
    "name": "<ext_name>",
    "permissions": [
        "proxy",
        "tabs",
        "storage",
        "webRequest",
        "webRequestAuthProvider"
    ],
    "host_permissions": ["<all_urls>"],
    "background": {"service_worker": "background.js"},
    "minimum_chrome_version": "109.0.0"
}"""

BACKGROUND_JS_TEMPLATE = """
var config = {
    mode: "fixed_servers",
    rules: {
        singleProxy: {
            scheme: "http",
            host: "<proxy_host>",
            port: parseInt("<proxy_port>")
        },
        bypassList: ["localhost", "127.0.0.1"]
    }
};

chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

function callbackFn(details) {
    return {
        authCredentials: {
            username: "<proxy_username>",
            password: "<proxy_password>"
        }
    };
}

chrome.webRequest.onAuthRequired.addListener(
    callbackFn,
    {urls: ["<all_urls>"]},
    ["blocking"]
);
"""


def build_proxy_extension(
    proxy: ProxyConfig,
    *,
    name: str = "TJSP Webshare Proxy",
    version: str = "1.0.0",
    target_dir: Path | None = None,
) -> str:
    """Create MV3 proxy auth extension; returns directory path."""
    ext_dir = target_dir or Path(tempfile.mkdtemp(prefix="tjsp_proxy_ext_"))
    ext_dir.mkdir(parents=True, exist_ok=True)

    manifest = (
        MANIFEST_TEMPLATE.replace("<ext_name>", name)
        .replace("<ext_ver>", version)
    )
    background_js = (
        BACKGROUND_JS_TEMPLATE.replace("<proxy_host>", proxy.host)
        .replace("<proxy_port>", str(proxy.port))
        .replace("<proxy_username>", proxy.username)
        .replace("<proxy_password>", proxy.password)
    )

    (ext_dir / "manifest.json").write_text(manifest, encoding="utf-8")
    (ext_dir / "background.js").write_text(background_js, encoding="utf-8")
    return str(ext_dir)


def remove_extension_dir(path: str) -> None:
    try:
        import shutil

        shutil.rmtree(path, ignore_errors=True)
    except OSError:
        pass
