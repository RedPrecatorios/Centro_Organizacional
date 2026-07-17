"""Expand lazy-loaded incidentes on e-SAJ search results."""

from __future__ import annotations

import logging
import time

from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger(__name__)

EXPAND_ALL_INCIDENTES_JS = """
const toggles = Array.from(document.querySelectorAll('a[id^="incidentesRecursos_"]'));
let clicked = 0;
for (const el of toggles) {
  try {
    el.click();
    clicked++;
  } catch (e) {}
}
return clicked;
"""

WAIT_INCIDENTES_LOADED_JS = """
const divs = Array.from(document.querySelectorAll('[id^="divFilhos"]'));
const stats = { total: divs.length, populated: 0, loading: 0 };
for (const div of divs) {
  const id = div.id || '';
  const codigo = id.replace('divFilhos', '');
  const loader = document.getElementById('loading' + codigo);
  if (loader && loader.style.display !== 'none') {
    stats.loading++;
  }
  if ((div.innerHTML || '').trim().length > 20) {
    stats.populated++;
  }
}
stats.incidentLinks = document.querySelectorAll('a.incidente').length;
return stats;
"""


def expand_all_incidentes(driver: WebDriver, *, timeout_seconds: float = 45) -> int:
    """Click every 'Incidentes e recursos' collapse on the search page."""
    clicked = driver.execute_script(EXPAND_ALL_INCIDENTES_JS) or 0
    logger.info("Clicked %s incidente toggle(s) on search page", clicked)

    def _loaded(_driver: WebDriver) -> bool:
        stats = _driver.execute_script(WAIT_INCIDENTES_LOADED_JS) or {}
        loading = int(stats.get("loading", 1))
        populated = int(stats.get("populated", 0))
        links = int(stats.get("incidentLinks", 0))
        logger.debug(
            "Incidente load state | populated=%s loading=%s links=%s",
            populated,
            loading,
            links,
        )
        if links > 0:
            return True
        if clicked == 0:
            return True
        return loading == 0 and populated >= clicked

    try:
        WebDriverWait(driver, timeout_seconds).until(_loaded)
    except Exception:
        logger.warning("Timeout waiting for incidentes AJAX — continuing with partial HTML")

    time.sleep(2)
    final_stats = driver.execute_script(WAIT_INCIDENTES_LOADED_JS) or {}
    logger.info(
        "Incidentes expanded | populated=%s incidentLinks=%s",
        final_stats.get("populated"),
        final_stats.get("incidentLinks"),
    )
    return int(final_stats.get("incidentLinks", 0))
