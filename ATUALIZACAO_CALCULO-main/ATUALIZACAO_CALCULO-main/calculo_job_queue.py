# -*- coding: utf-8 -*-
"""Fila serializada para actualização de cálculo (um caso de cada vez)."""
from __future__ import annotations

import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable

_DEFAULT_AVG_SEC = 120.0
_HISTORY_MAX = 50

RunFn = Callable[[int, str | None], dict[str, Any]]

_run_atualizacao: RunFn | None = None


@dataclass
class _Job:
    prec_id: int
    feito_por: str | None
    event: threading.Event = field(default_factory=threading.Event)
    result: dict[str, Any] | None = None


_guard = threading.Lock()
_queue: deque[_Job] = deque()
_jobs_by_prec: dict[int, _Job] = {}
_worker_running = False
_current_job: dict[str, Any] | None = None
_duration_history: deque[float] = deque(maxlen=_HISTORY_MAX)


def configure_runner(fn: RunFn) -> None:
    global _run_atualizacao
    _run_atualizacao = fn


def _default_avg_seconds() -> float:
    raw = (os.getenv("CALCULO_FILA_MEDIA_SEGUNDOS") or "").strip()
    if raw:
        try:
            v = float(raw.replace(",", "."))
            if v > 0:
                return v
        except ValueError:
            pass
    return _DEFAULT_AVG_SEC


def _avg_duration_seconds() -> float:
    if not _duration_history:
        return _default_avg_seconds()
    return sum(_duration_history) / len(_duration_history)


def get_fila_status() -> dict[str, Any]:
    """Estado da fila para UI (operador actual, tamanho, estimativa)."""
    with _guard:
        total = len(_queue)
        em_execucao = _current_job is not None
        operador = None
        caso_atual_id = None
        restante_atual = 0.0
        if _current_job:
            operador = _current_job.get("feito_por")
            caso_atual_id = _current_job.get("id_precainfosnew")
            started = float(_current_job.get("started_at") or 0)
            if started > 0:
                elapsed = max(0.0, time.monotonic() - started)
                restante_atual = max(0.0, _avg_duration_seconds() - elapsed)
        na_fila = max(0, total - (1 if em_execucao else 0))
        avg = _avg_duration_seconds()
        estimativa = restante_atual + na_fila * avg
        return {
            "em_execucao": em_execucao,
            "operador_atual": operador,
            "id_precainfosnew_atual": caso_atual_id,
            "casos_na_fila": na_fila,
            "pedidos_totais": total,
            "media_segundos": round(avg, 1),
            "estimativa_espera_segundos": round(max(0.0, estimativa), 1),
            "amostras_media": len(_duration_history),
        }


def _kick_worker() -> None:
    global _worker_running
    with _guard:
        if _worker_running:
            return
        if not _queue:
            return
        _worker_running = True
    threading.Thread(target=_worker_loop, name="calculo-fila", daemon=True).start()


def _worker_loop() -> None:
    global _worker_running, _current_job
    runner = _run_atualizacao
    if runner is None:
        with _guard:
            _worker_running = False
        return
    while True:
        with _guard:
            if not _queue:
                _worker_running = False
                _current_job = None
                return
            job = _queue[0]
            _current_job = {
                "id_precainfosnew": job.prec_id,
                "feito_por": (job.feito_por or "").strip() or "automação",
                "started_at": time.monotonic(),
            }
        t0 = time.monotonic()
        try:
            job.result = runner(job.prec_id, feito_por=job.feito_por)
        except Exception as e:
            job.result = {"ok": False, "error": str(e)}
        finally:
            _duration_history.append(time.monotonic() - t0)
            with _guard:
                if _queue and _queue[0] is job:
                    _queue.popleft()
                _current_job = None
            job.event.set()


def enqueue(
    prec_id: int,
    *,
    feito_por: str | None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Coloca na fila e responde de imediato (sem bloquear a ligação HTTP)."""
    job = _Job(prec_id=prec_id, feito_por=feito_por)
    fila_snapshot: dict[str, Any] | None = None
    with _guard:
        had_busy = bool(_queue) or _current_job is not None
        _queue.append(job)
        _jobs_by_prec[prec_id] = job
    _kick_worker()
    if had_busy:
        fila_snapshot = get_fila_status()
    payload: dict[str, Any] = {
        "ok": True,
        "accepted": True,
        "id_precainfosnew": prec_id,
        "fila": get_fila_status(),
    }
    if fila_snapshot:
        payload["fila_ao_entrar"] = fila_snapshot
    return payload, fila_snapshot


def get_prec_id_status(prec_id: int) -> dict[str, Any]:
    """Estado do pedido de um ``id_precainfosnew`` (para polling)."""
    with _guard:
        job = _jobs_by_prec.get(prec_id)
        if job is None:
            return {
                "ok": True,
                "status": "unknown",
                "done": False,
                "id_precainfosnew": prec_id,
            }
        done = job.event.is_set()
        is_current = bool(
            _current_job and int(_current_job.get("id_precainfosnew") or 0) == prec_id
        )
        in_queue = job in _queue
    fila = get_fila_status()
    if done:
        result = (
            job.result
            if isinstance(job.result, dict)
            else {"ok": False, "error": "Resposta inválida."}
        )
        return {
            "ok": True,
            "status": "done",
            "done": True,
            "id_precainfosnew": prec_id,
            "result": result,
            "fila": fila,
        }
    status = "running" if is_current else ("queued" if in_queue else "pending")
    return {
        "ok": True,
        "status": status,
        "done": False,
        "id_precainfosnew": prec_id,
        "fila": fila,
    }


def submit_and_wait(
    prec_id: int,
    *,
    feito_por: str | None,
    timeout: float,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """
    Enfileira e bloqueia até concluir ou timeout.

    Returns (resultado, fila_ao_entrar) — ``fila_ao_entrar`` preenchido se havia fila/execução.
    """
    job = _Job(prec_id=prec_id, feito_por=feito_por)
    fila_snapshot: dict[str, Any] | None = None
    with _guard:
        had_busy = bool(_queue) or _current_job is not None
        _queue.append(job)
        _jobs_by_prec[prec_id] = job
    _kick_worker()
    if had_busy:
        fila_snapshot = get_fila_status()

    if not job.event.wait(timeout=timeout):
        return (
            {
                "ok": False,
                "error": "Tempo esgotado à espera na fila de cálculo. Tente novamente.",
                "fila": get_fila_status(),
            },
            fila_snapshot,
        )
    out = job.result if isinstance(job.result, dict) else {"ok": False, "error": "Resposta inválida."}
    return out, fila_snapshot
