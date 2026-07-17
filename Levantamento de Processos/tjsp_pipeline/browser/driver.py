"""Undetected ChromeDriver with Webshare proxy + CDP JS injection."""

from __future__ import annotations

import logging
import os
import platform
import re
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

import undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait

from tjsp_pipeline.browser.js_monitor import (
    CONSOLE_BRIDGE_INJECT,
    DOM_MONITOR_INJECT,
    STEALTH_INJECT,
    inject_on_load,
    read_console_bridge,
    read_dom_monitor,
)
from tjsp_pipeline.browser.proxy_auth import build_proxy_extension, remove_extension_dir
from tjsp_pipeline.config import ProxyConfig, Settings

logger = logging.getLogger(__name__)


def _chrome_binaries_to_probe() -> list[str]:
    """Candidate Chrome/Chromium binaries for Linux and Windows."""
    env_binary = os.getenv("CHROME_BINARY", "").strip()
    candidates: list[str] = []
    if env_binary:
        candidates.append(env_binary)

    if platform.system() == "Windows":
        local = os.environ.get("LOCALAPPDATA", "")
        candidates.extend(
            [
                r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                rf"{local}\Google\Chrome\Application\chrome.exe",
                r"C:\Program Files\Chromium\Application\chrome.exe",
            ]
        )
    else:
        candidates.extend(
            ("chromium", "chromium-browser", "google-chrome", "google-chrome-stable", "chrome")
        )
    return candidates


def _major_from_version_string(text: str) -> int | None:
    match = re.search(r"(\d+)\.", text or "")
    if match:
        return int(match.group(1))
    return None


def _detect_chrome_major_from_registry() -> int | None:
    if platform.system() != "Windows":
        return None
    try:
        import winreg
    except ImportError:
        return None

    for root, path in (
        (winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon"),
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Google\Chrome\BLBeacon"),
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Google\Chrome\BLBeacon"),
    ):
        try:
            with winreg.OpenKey(root, path) as key:
                version, _ = winreg.QueryValueEx(key, "version")
                major = _major_from_version_string(str(version))
                if major:
                    logger.info("Detected Chrome major version %s via registry", major)
                    return major
        except OSError:
            continue
    return None


def _detect_chrome_major_from_install_dir(binary: str) -> int | None:
    """Parse version from parent Application folder (e.g. .../Application/150.0.x.x/)."""
    try:
        app_dir = Path(binary).resolve().parent
    except OSError:
        return None
    if not app_dir.is_dir():
        return None
    best: int | None = None
    for child in app_dir.iterdir():
        if child.is_dir():
            major = _major_from_version_string(child.name)
            if major is not None and (best is None or major > best):
                best = major
    if best is not None:
        logger.info("Detected Chrome major version %s via install dir", best)
    return best


def _detect_chrome_binary() -> str | None:
    for binary in _chrome_binaries_to_probe():
        if not binary:
            continue
        path = Path(binary)
        if path.is_file():
            return str(path)
        # PATH lookup (Linux package names)
        try:
            result = subprocess.run(
                ["where" if platform.system() == "Windows" else "which", binary],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            found = (result.stdout or "").strip().splitlines()
            if found and Path(found[0]).is_file():
                return found[0]
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
            continue
    return None


def _detect_chrome_major_version() -> int | None:
    major = _detect_chrome_major_from_registry()
    if major is not None:
        return major

    binary = _detect_chrome_binary()
    if binary:
        major = _detect_chrome_major_from_install_dir(binary)
        if major is not None:
            return major
        # Prefer --product-version: Chrome on Windows often ignores --version
        # and opens a browser window instead of printing to stdout.
        for args in (
            [binary, "--product-version"],
            [binary, "--version"],
        ):
            try:
                result = subprocess.run(
                    args,
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                text = (result.stdout or "") + (result.stderr or "")
                major = _major_from_version_string(text)
                if major is not None:
                    logger.info("Detected Chrome major version %s via %s", major, args[1])
                    return major
            except (FileNotFoundError, subprocess.TimeoutExpired, ValueError, OSError):
                continue

    # Last resort: PATH names without absolute path (Linux)
    for name in ("chromium", "chromium-browser", "google-chrome", "chrome"):
        try:
            result = subprocess.run(
                [name, "--version"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            major = _major_from_version_string((result.stdout or "") + (result.stderr or ""))
            if major is not None:
                logger.info("Detected %s major version %s", name, major)
                return major
        except (FileNotFoundError, subprocess.TimeoutExpired, ValueError, OSError):
            continue
    return None


class UndetectedEsajBrowser:
    """Browser session for e-SAJ with proxy rotation and debug instrumentation."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.driver: uc.Chrome | None = None
        self._proxy_ext_path: str | None = None
        self._console_logs: list[dict] = []

    def start(self) -> None:
        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--lang=pt-BR")
        options.add_argument("--window-size=1920,1080")

        # Authenticated proxy: MV3 extension supplies host + credentials.
        # Do NOT also pass --proxy-server — it bypasses extension auth and yields blank pages.
        use_proxy_ext = bool(
            self.settings.proxy.username and self.settings.proxy.password
        )
        if use_proxy_ext:
            self._proxy_ext_path = build_proxy_extension(self.settings.proxy)
            # headless=new may still ignore extensions on some Chrome builds; prefer headed.
            if self.settings.headless:
                logger.warning(
                    "Authenticated proxy requires the MV3 extension; "
                    "disabling headless so proxy auth works on Windows/Chrome"
                )
                self.settings.headless = False
            options.add_argument(f"--load-extension={self._proxy_ext_path}")
        else:
            options.add_argument(self.settings.proxy.server_arg)

        if self.settings.headless:
            options.add_argument("--headless=new")

        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        }
        options.add_experimental_option("prefs", prefs)
        options.page_load_strategy = "eager"

        logger.info(
            "Starting undetected-chromedriver | proxy=%s@%s:%s headless=%s ext=%s",
            self.settings.proxy.username,
            self.settings.proxy.host,
            self.settings.proxy.port,
            self.settings.headless,
            bool(self._proxy_ext_path),
        )

        version_main = _detect_chrome_major_version()
        chrome_binary = _detect_chrome_binary()
        if chrome_binary:
            logger.info("Using Chrome binary: %s", chrome_binary)

        kwargs: dict = {
            "options": options,
            "use_subprocess": True,
            "version_main": version_main,
        }
        if chrome_binary:
            kwargs["browser_executable_path"] = chrome_binary

        self.driver = uc.Chrome(**kwargs)
        self.driver.set_page_load_timeout(self.settings.browser_timeout_seconds)
        self.driver.implicitly_wait(5)

        self._setup_cdp_injection()
        # Proxy auth: MV3 extension only (Fetch.enable pauses navigations without a
        # live handler — headed Chrome + extension is the supported path).

    def _setup_cdp_injection(self) -> None:
        assert self.driver is not None
        for script in (STEALTH_INJECT, CONSOLE_BRIDGE_INJECT, DOM_MONITOR_INJECT):
            inject_on_load(self.driver, script)
        self.driver.execute_cdp_cmd("Network.enable", {})
        self.driver.execute_cdp_cmd(
            "Runtime.enable",
            {},
        )
        logger.debug("CDP stealth + console/DOM monitors injected")

    def navigate(self, url: str, *, wait_seconds: float | None = None) -> str:
        assert self.driver is not None
        wait = wait_seconds if wait_seconds is not None else self.settings.page_load_wait_seconds
        last_error: Exception | None = None

        for attempt in range(1, 4):
            try:
                logger.info("Navigate attempt %s → %s", attempt, url[:120])
                self.driver.get(url)
                WebDriverWait(self.driver, 60).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                time.sleep(wait)
                html = self.driver.page_source or ""
                if len(html) > 300:
                    self._capture_debug_state(url)
                    return html
                logger.warning("Short page source (%s chars), retrying…", len(html))
            except (TimeoutException, WebDriverException) as exc:
                last_error = exc
                logger.warning("Navigation failed attempt %s: %s", attempt, exc)
                time.sleep(3 * attempt)

        raise RuntimeError(f"Failed to load {url}: {last_error}")

    def _capture_debug_state(self, url: str) -> None:
        assert self.driver is not None
        bridge = read_console_bridge(self.driver)
        dom = read_dom_monitor(self.driver)
        self._console_logs.extend(bridge[-20:])
        if bridge:
            logger.debug(
                "Console bridge (%s entries) last=%s",
                len(bridge),
                bridge[-1].get("message", "")[:120],
            )
        if dom:
            logger.debug(
                "DOM monitor | mutations=%s htmlLen=%s samples=%s",
                dom.get("mutations"),
                dom.get("lastHtmlLen"),
                len(dom.get("samples", [])),
            )

    def save_debug_html(self, label: str, html: str) -> str:
        self.settings.debug_html_dir.mkdir(parents=True, exist_ok=True)
        safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in label)[:80]
        path = self.settings.debug_html_dir / f"{safe}.html"
        path.write_text(html, encoding="utf-8")
        logger.info("Debug HTML saved: %s", path)
        return str(path)

    def resolve_url(self, href: str, base: str = "https://esaj.tjsp.jus.br") -> str:
        if href.startswith("http"):
            return href
        return urljoin(base, href)

    def close(self) -> None:
        if self.driver:
            try:
                self.driver.quit()
            except OSError:
                pass
            self.driver = None
        if self._proxy_ext_path:
            remove_extension_dir(self._proxy_ext_path)
            self._proxy_ext_path = None

    @property
    def console_logs(self) -> list[dict]:
        return list(self._console_logs)


@contextmanager
def esaj_browser(settings: Settings) -> Generator[UndetectedEsajBrowser, None, None]:
    browser = UndetectedEsajBrowser(settings)
    try:
        browser.start()
        yield browser
    finally:
        browser.close()
