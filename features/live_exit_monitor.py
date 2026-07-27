"""
live_exit_monitor.py — Live Strategy Exit-Time Monitor
────────────────────────────────────────────────────────
Safety-net twin of live_entry_monitor.py: checks all Live_Running
live/fast-forward/forward-test strategies every second for a scheduled
exit_time that has already passed, and force-closes them at current LTP via
execution_socket._square_off_trade_like_manual — the same routine the
"Overall Square Off" button uses.

Why this exists: the "real" exit_time force-exit lives inside the
broker-tick-driven path (execution_socket._process_backtest_trade_tick,
called from trading_core.process_broker_tick on every incoming broker tick,
despite its name — it's shared by backtest and live/fast-forward/forward-test
alike). It only runs when a tick actually arrives for that trade's tokens.
If no tick lands around the scheduled exit_time (WS hiccup, thin liquidity,
a quiet token) the exit is simply never evaluated until the next tick
happens to arrive — which can be minutes late — and if the server was
down/restarting right around exit_time, it's never evaluated at all until
this monitor's next check.

This monitor is tick-independent: it fires within ~1s of the scheduled time
regardless of broker tick activity, and its very first check right after
start() immediately catches anything already overdue (the restart-catch-up
case).

Race safety: both this monitor and the tick-driven path acquire the SAME
short-TTL DB lock (execution_socket.acquire_exit_time_squareoff_lock) before
acting on a trade, so they can never both place an exit order for the same
position in the same window — see that function's docstring for why it's a
short TTL rather than a one-time claim.

Lifecycle
─────────
  start(loop)   ← called from FastAPI, alongside live_entry_monitor.start()
                  (see algo.trade/api.py's _start_monitor_services)
  stop()        ← called from FastAPI @shutdown / /monitor/stop
"""

from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

from features.debug_flags import runtime_print

log = logging.getLogger(__name__)


def _trace_stdout(message: str) -> None:
    runtime_print(message, flush=True)


RUNNING_STATUS    = 'StrategyStatus.Live_Running'
OPEN_LEG_STATUS   = 1
CACHE_TTL_SECONDS = 5.0


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _now_ist_ts() -> str:
    """Current IST as ISO string: 'YYYY-MM-DDTHH:MM:SS'."""
    return (_now_utc() + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%dT%H:%M:%S')


def _extract_exit_hhmm(raw: str) -> str:
    """Extract HH:MM from exit_time, which may be full ISO or just HH:MM."""
    raw = str(raw or '').strip()
    if len(raw) >= 16:
        return raw[11:16]
    return raw[:5]


class LiveExitMonitor:
    """Asyncio-based exit-time safety net that runs every second."""

    def __init__(self) -> None:
        self._loop:    asyncio.AbstractEventLoop | None = None
        self._running: bool = False
        self._task:    asyncio.Task | None = None
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='live-exit')

        # Persistent DB connection (opened in start())
        self._db = None

        # In-memory trade cache with TTL — same pattern as LiveEntryMonitor
        self._trades_cache:    list[dict] = []
        self._cache_loaded_at: float = 0.0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._running:
            _trace_stdout('[LiveExitMonitor] already running — start() ignored')
            return

        self._loop    = loop
        self._running = True
        self._trades_cache    = []
        self._cache_loaded_at = 0.0  # force immediate reload on first tick

        if self._db is None:
            from features.mongo_data import MongoData
            self._db = MongoData()
            _trace_stdout('[LiveExitMonitor] DB connection opened')

        if self._task and not self._task.done():
            self._task.cancel()

        self._task = loop.create_task(self._monitor_loop(), name='live-exit-monitor')
        _trace_stdout('[LiveExitMonitor] started — checking every second for overdue exit_time')

    def stop(self) -> None:
        self._running = False

        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None

        self._trades_cache    = []
        self._cache_loaded_at = 0.0

        if self._db:
            try:
                self._db.close()
            except Exception:
                pass
            self._db = None

        _trace_stdout('[LiveExitMonitor] stopped')

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _monitor_loop(self) -> None:
        _trace_stdout('[LiveExitMonitor] loop started')
        while self._running:
            try:
                await self._loop.run_in_executor(self._executor, self._check_all_strategies)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception('[LiveExitMonitor] loop error: %s', exc)
            await asyncio.sleep(1.0)
        _trace_stdout('[LiveExitMonitor] loop exited')

    # ── Core: check every strategy every second ───────────────────────────────

    def _check_all_strategies(self) -> None:
        now_ts   = _now_ist_ts()
        now_hhmm = now_ts[11:16]

        cache_age = time.monotonic() - self._cache_loaded_at
        if cache_age >= CACHE_TTL_SECONDS or not self._trades_cache:
            self._reload_cache()

        if not self._trades_cache:
            return

        for trade in self._trades_cache:
            try:
                self._process_trade(trade, now_ts, now_hhmm)
            except Exception as exc:
                trade_id = str(trade.get('_id') or '')
                log.exception('[LiveExitMonitor] trade=%s error: %s', trade_id, exc)

    def _process_trade(self, trade: dict, now_ts: str, now_hhmm: str) -> None:
        trade_id  = str(trade.get('_id') or '')
        exit_hhmm = _extract_exit_hhmm(trade.get('exit_time') or '')
        if not exit_hhmm or len(exit_hhmm) < 5:
            return
        if now_hhmm < exit_hhmm:
            return  # not due yet

        activation_mode = str(trade.get('activation_mode') or '').strip()
        strategy_name    = str(trade.get('name') or trade.get('strategy_name') or '')

        from features.execution_socket import acquire_exit_time_squareoff_lock, _square_off_trade_like_manual

        # Only one of {this monitor, the tick-driven force-exit} may act on
        # this trade within the lock's TTL window — see that function's
        # docstring. If we don't get it, the other path already has this.
        if not acquire_exit_time_squareoff_lock(self._db, trade_id, now_ts):
            return

        # Re-check fresh DB state after acquiring the lock — the cached
        # `trade` can be up to CACHE_TTL_SECONDS stale, and the tick-driven
        # path may have already fully closed this trade in the meantime.
        open_legs = self._db._db['algo_trade_positions_history'].count_documents(
            {'trade_id': trade_id, 'status': OPEN_LEG_STATUS, 'exit_trade': None}
        )
        if open_legs <= 0:
            return

        fresh_trade = self._db._db['algo_trades'].find_one({'_id': trade_id}) or trade
        _trace_stdout(
            f'[EXIT MONITOR TRIGGER]  strategy={strategy_name}  trade_id={trade_id}  '
            f'exit_time={exit_hhmm}  current={now_hhmm}  open_legs={open_legs}  '
            f'→ squaring off at current LTP'
        )
        try:
            _square_off_trade_like_manual(
                self._db, fresh_trade, exit_timestamp=now_ts, activation_mode=activation_mode,
            )
        except Exception as exc:
            log.exception('[LiveExitMonitor] square-off failed trade=%s: %s', trade_id, exc)

    def _reload_cache(self) -> None:
        """
        Reload active strategies with a set exit_time from DB into the
        in-memory cache. Same trade_status/activation_mode/status filter as
        LiveEntryMonitor, narrowed to trades that actually have an exit_time
        set (nothing to check otherwise).
        """
        try:
            query: dict = {
                'trade_status': 1,
                'activation_mode': {'$in': ['live', 'fast-forward', 'forward-test']},
                'status': RUNNING_STATUS,
                'exit_time': {'$nin': [None, '']},
            }
            self._trades_cache    = list(self._db._db['algo_trades'].find(query))
            self._cache_loaded_at = time.monotonic()
        except Exception as exc:
            log.exception('[LiveExitMonitor] _reload_cache error: %s', exc)


# ─── Module-level singleton ────────────────────────────────────────────────────

_monitor: LiveExitMonitor | None = None


def get_monitor() -> LiveExitMonitor:
    global _monitor
    if _monitor is None:
        _monitor = LiveExitMonitor()
    return _monitor


def start(loop: asyncio.AbstractEventLoop) -> None:
    get_monitor().start(loop)


def stop() -> None:
    if _monitor:
        _monitor.stop()
