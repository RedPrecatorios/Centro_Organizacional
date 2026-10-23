"""Parse REFACTOR log lines to drive live incident cards."""

from __future__ import annotations

import re

_CNJ = r"(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})"
_PAIR = re.compile(
    rf"processo[=:\s]+{_CNJ}.{{0,160}}?incidente[=:\s]+(\d+)",
    re.IGNORECASE | re.DOTALL,
)


def parse_refactor_log_line(line: str) -> dict[str, str] | None:
    text = str(line or "").strip()
    if not text:
        return None
    match = _PAIR.search(text)
    if not match:
        return None
    processo, incidente = match.group(1), match.group(2)
    low = text.lower()
    event = "touch"
    if "calculoapi" in low.replace("ú", "u") or "calculo/fila" in low:
        event = "calculo"
    elif "[download]" in low or "auto salvo" in low:
        event = "autos"
    elif "blacklisted" in low:
        event = "blacklist"
    elif "status bloqueado" in low or "status=extinto" in low:
        event = "extinto"
    elif "pasta temporaria removida" in low or "pasta temporária removida" in low:
        event = "done"
    elif "[incidente] verificando" in low:
        event = "start"
    depre_m = re.search(rf"depre[=:\s]+{_CNJ}", text, flags=re.IGNORECASE)
    status_hits = re.findall(r"status[=:\s]+([A-Za-z0-9_-]+)", text, flags=re.IGNORECASE)
    blocked_status = ""
    if event == "extinto":
        blocked_status = next(
            (s for s in reversed(status_hits) if s.lower() not in {"200", "ok"}),
            status_hits[-1] if status_hits else "extinto",
        )
    return {
        "processo": processo,
        "incidente": str(int(incidente)),
        "event": event,
        "depre": depre_m.group(1) if depre_m else "",
        "blocked_status": blocked_status,
        "from_depre": "[depre]" in low,
    }
