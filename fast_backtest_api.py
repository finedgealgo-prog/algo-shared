"""
fast_backtest_api.py
---------------------
HTTP API for the Parquet-based fast backtest engine (shared/fast_backtest/).

Mirrors the existing /algo/backtest/start|status|result job-polling
contract (same request/response shape) so the frontend's Strategy page can
point at this instead without changing its payload handling — but this
path reads shared/parquet_data/ instead of MongoDB, and runs the minimal
strategy_runner.py instead of shared/features/backtest_engine.py. Neither
of those existing files/paths is touched by this module.

Mounted the same way as chart_api.py: symlinked into each service dir and
included from that service's *_main.py.
"""

from __future__ import annotations

import logging
import threading
import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException

from fast_backtest.strategy_runner import run_strategy

log = logging.getLogger("fast_backtest.api")

router = APIRouter(prefix="/algo/fast-backtest")

PARQUET_ROOT = Path(__file__).resolve().parent / "parquet_data"

_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()


def _run_job(job_id: str, request: dict) -> None:
    def on_progress(completed: int, total: int, day: str) -> None:
        with _jobs_lock:
            _jobs[job_id].update(
                status="running",
                completed=completed,
                total=total,
                percent=int(completed / total * 100) if total else 0,
                day=day,
            )

    try:
        trades = run_strategy(
            request["strategy"], request["start_date"], request["end_date"],
            PARQUET_ROOT, on_progress=on_progress,
        )
        with _jobs_lock:
            _jobs[job_id].update(status="done", trades=trades)
    except Exception as exc:
        log.exception("fast-backtest job %s failed", job_id)
        with _jobs_lock:
            _jobs[job_id].update(status="error", error=str(exc))


@router.post("/start")
def start_backtest(request: dict):
    if not all(k in request for k in ("strategy", "start_date", "end_date")):
        raise HTTPException(status_code=400, detail="strategy, start_date, end_date are required")

    job_id = uuid.uuid4().hex
    with _jobs_lock:
        _jobs[job_id] = {"status": "running", "completed": 0, "total": 0, "percent": 0}

    threading.Thread(target=_run_job, args=(job_id, request), daemon=True).start()
    return {"job_id": job_id, "status": "running"}


@router.get("/status/{job_id}")
def get_status(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return {k: v for k, v in job.items() if k != "trades"}


@router.get("/result/{job_id}")
def get_result(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if job["status"] != "done":
        raise HTTPException(status_code=409, detail=f"job status is '{job['status']}', not done")
    return {"trades": job.get("trades", [])}
