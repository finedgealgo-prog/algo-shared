"""
Generic weekday market-hours auto start/stop for a service's own background
monitor loop (broker ticker, live/paper monitors, snapshot collectors, ...).

Every service that wants "come up near market open, go down after market
close" for one of its own start()/stop() functions calls
`run_market_hours_scheduler(...)` once, as a background asyncio task, from
its own `@app.on_event("startup")` hook — the same pattern each of these
services already uses for its own auto-start-on-boot hook.

Weekday-only (Mon-Fri, server's local clock — every affected service already
assumes IST elsewhere, e.g. shared/features/alert_checker.py's MARKET_OPEN_
MINUTES). NSE trading holidays are NOT special-cased: a holiday just means
the 09:10 auto-start comes up into a quiet market, same as manually starting
it would on a holiday. The existing manual start/stop endpoints on every
service remain a normal override at any time — this loop only ever fires
once per day at each edge and never fights a manual action taken in between.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import inspect
import logging
import os
from typing import Any, Callable

log = logging.getLogger(__name__)

# One knob, per-process — set MARKET_HOURS_AUTOSCHEDULE=false in a given
# service's own .env to opt that whole process out without touching code.
_ENABLED = os.getenv("MARKET_HOURS_AUTOSCHEDULE", "true").strip().lower() not in ("0", "false", "no")

DEFAULT_OPEN_HHMM = (9, 10)
DEFAULT_CLOSE_HHMM = (15, 35)  # NSE close 15:30 + 5 min buffer


async def _call(fn: Callable[[], Any]) -> Any:
    result = fn()
    if inspect.isawaitable(result):
        result = await result
    return result


def parse_running(status: Any) -> bool:
    """Best-effort 'is it running' read from whatever shape a monitor's
    status()/get_status() happens to return — mirrors algo-admin's
    Monitors.tsx parseRunning so the scheduler and the admin UI agree."""
    if isinstance(status, bool):
        return status
    if isinstance(status, dict):
        running = status.get("running")
        if isinstance(running, bool):
            return running
        text = status.get("status") or status.get("monitor_status")
        if isinstance(text, str):
            return "run" in text.lower()
    return False


async def run_market_hours_scheduler(
    name: str,
    start_fn: Callable[[], Any],
    stop_fn: Callable[[], Any],
    is_running_fn: Callable[[], Any],
    open_hhmm: tuple[int, int] = DEFAULT_OPEN_HHMM,
    close_hhmm: tuple[int, int] = DEFAULT_CLOSE_HHMM,
    poll_seconds: int = 20,
) -> None:
    """Runs forever. Fires start_fn once per weekday at open_hhmm (if not
    already running) and stop_fn once per weekday at close_hhmm (if
    running). Each edge fires at most once per calendar day regardless of
    how long this loop keeps running."""
    if not _ENABLED:
        log.info("[market_hours_scheduler:%s] disabled via MARKET_HOURS_AUTOSCHEDULE=false", name)
        return

    log.info(
        "[market_hours_scheduler:%s] active — auto-start ~%02d:%02d, auto-stop ~%02d:%02d, Mon-Fri",
        name, *open_hhmm, *close_hhmm,
    )

    last_start_date: dt.date | None = None
    last_stop_date: dt.date | None = None

    while True:
        try:
            now = dt.datetime.now()
            today = now.date()
            if now.weekday() < 5:  # Mon=0 .. Fri=4
                open_dt = now.replace(hour=open_hhmm[0], minute=open_hhmm[1], second=0, microsecond=0)
                close_dt = now.replace(hour=close_hhmm[0], minute=close_hhmm[1], second=0, microsecond=0)

                if now >= open_dt and last_start_date != today:
                    last_start_date = today
                    if not parse_running(await _call(is_running_fn)):
                        log.info("[market_hours_scheduler:%s] auto-start firing", name)
                        await _call(start_fn)

                if now >= close_dt and last_stop_date != today:
                    last_stop_date = today
                    if parse_running(await _call(is_running_fn)):
                        log.info("[market_hours_scheduler:%s] auto-stop firing", name)
                        await _call(stop_fn)
        except Exception:
            log.exception("[market_hours_scheduler:%s] tick error", name)

        await asyncio.sleep(poll_seconds)
