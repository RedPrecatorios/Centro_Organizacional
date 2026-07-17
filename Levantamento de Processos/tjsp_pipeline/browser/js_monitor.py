"""JavaScript injection for stealth, console capture, and DOM monitoring."""

from __future__ import annotations

STEALTH_INJECT = """
(() => {
  try {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    window.chrome = window.chrome || { runtime: {} };
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) =>
      parameters.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : originalQuery(parameters);
    Object.defineProperty(navigator, 'plugins', {
      get: () => [1, 2, 3, 4, 5],
    });
    Object.defineProperty(navigator, 'languages', {
      get: () => ['pt-BR', 'pt', 'en-US', 'en'],
    });
  } catch (e) {
    console.debug('[tjsp-inject] stealth patch skipped', e);
  }
})();
"""

CONSOLE_BRIDGE_INJECT = """
(() => {
  if (window.__tjspConsoleBridge) return;
  window.__tjspConsoleBridge = [];
  const levels = ['log', 'warn', 'error', 'info', 'debug'];
  levels.forEach((level) => {
    const original = console[level].bind(console);
    console[level] = (...args) => {
      try {
        window.__tjspConsoleBridge.push({
          level,
          ts: Date.now(),
          message: args.map((a) => {
            try { return typeof a === 'string' ? a : JSON.stringify(a); }
            catch { return String(a); }
          }).join(' ')
        });
        if (window.__tjspConsoleBridge.length > 200) {
          window.__tjspConsoleBridge.shift();
        }
      } catch (_) {}
      original(...args);
    };
  });
})();
"""

DOM_MONITOR_INJECT = """
(() => {
  if (window.__tjspDomMonitor) return;
  window.__tjspDomMonitor = { mutations: 0, lastHtmlLen: 0, samples: [] };
  const pushSample = (kind, detail) => {
    window.__tjspDomMonitor.samples.push({ kind, detail, ts: Date.now() });
    if (window.__tjspDomMonitor.samples.length > 100) {
      window.__tjspDomMonitor.samples.shift();
    }
  };
  pushSample('init', document.documentElement?.outerHTML?.length || 0);
  const observer = new MutationObserver((records) => {
    window.__tjspDomMonitor.mutations += records.length;
    const len = document.documentElement?.outerHTML?.length || 0;
    if (Math.abs(len - window.__tjspDomMonitor.lastHtmlLen) > 500) {
      window.__tjspDomMonitor.lastHtmlLen = len;
      pushSample('html-growth', len);
    }
  });
  observer.observe(document.documentElement, {
    childList: true,
    subtree: true,
    characterData: true,
  });
})();
"""

PRECATORIO_LINKS_INJECT = """
(() => {
  const links = [];
  document.querySelectorAll('a.incidente, a.linkProcesso, a[href*="show.do"]').forEach((a) => {
    const text = (a.textContent || '').replace(/\\s+/g, ' ').trim();
    const row = a.closest('div.home__lista-de-processos, li') || a.parentElement;
    const classe = row ? (row.querySelector('.classeProcesso')?.textContent || '') : '';
    const href = a.href || a.getAttribute('href') || '';
    if (/precat/i.test(text) || /precat/i.test(classe)) {
      links.push({ href, text: text || classe || 'Precatório' });
    }
  });
  return links;
})();
"""


def inject_on_load(driver, script: str) -> None:
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": script},
    )


def read_console_bridge(driver) -> list[dict]:
    try:
        return driver.execute_script(
            "return window.__tjspConsoleBridge || [];"
        ) or []
    except Exception:
        return []


def read_dom_monitor(driver) -> dict:
    try:
        return driver.execute_script(
            "return window.__tjspDomMonitor || {};"
        ) or {}
    except Exception:
        return {}


def collect_precatorio_links_js(driver) -> list[dict]:
    try:
        return driver.execute_script(PRECATORIO_LINKS_INJECT) or []
    except Exception:
        return []
