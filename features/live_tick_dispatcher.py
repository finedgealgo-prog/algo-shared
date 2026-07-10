"""
live_tick_dispatcher.py
───────────────────────
Ultra-thin broker tick dispatcher for live + fast-forward modes.

Goals
─────
1. Keep the broker WebSocket on_ticks callback extremely light.
2. Give live order execution its own dedicated worker thread.
3. Prevent fast-forward processing from blocking live execution.
4. Run minute-entry work outside the socket callback.

Design
──────
- One spot writer worker (coalesced).
- One live worker   (preserve tick order, no coalescing).
- One fast-forward worker (coalesced to newest pending tick).

Live is never blocked by fast-forward because each mode has its own worker.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from threading import Condition, Lock, Thread
import logging
import time

from features.mongo_data import MongoData
from features.runtime_mode_registry import runtime_mode_registry

logger = logging.getLogger(__name__)

# Forward Test intentionally checks SL/target/lazy-leg/momentum conditions on a
# slow heartbeat instead of every broker tick — it's a lighter-weight cousin of
# Fast Forward, not a real-time mode. Ticks that arrive inside the window are
# simply dropped (the coalescing queue below always keeps the newest one, so
# the next check runs against current price, not a stale tick).
FORWARD_TEST_CHECK_INTERVAL_SECONDS = 30.0

SPOT_TOKEN_BY_UNDERLYING = {
    "NIFTY":      "256265",
    "BANKNIFTY":  "260105",
    "FINNIFTY":   "257801",
    "SENSEX":     "265",
    "MIDCPNIFTY": "288009",
}

# Standardised token key stored in option_chain_index_spot (matches frontend NSE_0x keys)
NSE_TOKEN_BY_UNDERLYING = {
    "NIFTY":      "NSE_01",
    "INDIAVIX":   "NSE_00",
    "SENSEX":     "NSE_02",
    "BANKNIFTY":  "NSE_03",
    "BANKEX":     "NSE_04",
    "FINNIFTY":   "NSE_05",
    "MIDCPNIFTY": "NSE_06",
}


@dataclass
class TickTask:
    trade_date: str
    now_ts: str
    now_minute: str
    listen_time: str
    broker_ltp_map: dict[str, float]
    spot_ticks_received: list[tuple[str, float, str]]


class _WorkerQueue:
    def __init__(self, *, coalesce_pending: bool = False) -> None:
        self._items: deque[TickTask] = deque()
        self._cond = Condition()
        self._coalesce_pending = coalesce_pending

    def put(self, item: TickTask) -> None:
        with self._cond:
            if self._coalesce_pending and self._items:
                self._items.clear()
            self._items.append(item)
            self._cond.notify()

    def get(self) -> TickTask:
        with self._cond:
            while not self._items:
                self._cond.wait()
            return self._items.popleft()


class _SpotWriterQueue:
    def __init__(self) -> None:
        self._items: deque[list[tuple[str, float, str]]] = deque()
        self._cond = Condition()

    def put(self, item: list[tuple[str, float, str]]) -> None:
        if not item:
            return
        with self._cond:
            # Keep only the newest pending spot batch; old pending writes are stale.
            self._items.clear()
            self._items.append(item)
            self._cond.notify()

    def get(self) -> list[tuple[str, float, str]]:
        with self._cond:
            while not self._items:
                self._cond.wait()
            return self._items.popleft()


def _persist_spot_ticks(db: MongoData, spot_ticks_received: list[tuple[str, float, str]], source: str) -> None:
    if not spot_ticks_received:
        return
    try:
        spot_col = db._db["option_chain_index_spot"]
        for underlying, spot_price, ts in spot_ticks_received:
            ul = str(underlying or "").upper()
            kite_tok = SPOT_TOKEN_BY_UNDERLYING.get(ul, "")
            nse_tok  = NSE_TOKEN_BY_UNDERLYING.get(ul, kite_tok)
            spot_col.update_one(
                {"underlying": ul, "timestamp": ts},
                {"$set": {
                    "underlying":  ul,
                    "timestamp":   ts,
                    "token":       nse_tok,
                    "kite_token":  kite_tok,
                    "close":       float(spot_price),
                    "spot_price":  float(spot_price),  # kept for backward compat
                    "open":        0.0,
                    "high":        0.0,
                    "low":         0.0,
                    "volume":      0,
                    "oi":          0,
                    "source":      source,
                }},
                upsert=True,
            )
    except Exception as exc:
        logger.error("spot write error source=%s: %s", source, exc)


def _run_momentum_for_live_ff(
    db: MongoData,
    trade_date: str,
    activation_mode: str,
    now_ts: str,
    records: list,
) -> None:
    """
    Process momentum-pending legs for ALL running live/fast-forward trades on every tick.
    This runs independently of entry_time so already-running strategies with queued
    momentum legs are also checked.
    """
    from features.execution_socket import (
        OPTION_CHAIN_COLLECTION,
        _process_momentum_pending_feature_legs,
        mark_execute_order_dirty_from_trade,
    )

    chain_col = db._db[OPTION_CHAIN_COLLECTION]

    # Build ltp_map and spot_map from Kite ticker once for all trades
    ltp_map: dict = {}
    spot_map: dict = {}
    try:
        from features.broker_gateway import broker_ticker_manager as _tm_run  # type: ignore
        ltp_map = dict(_tm_run.ltp_map or {})
        spot_map = dict(_tm_run.spot_map or {})
    except Exception:
        pass

    for record in (records or []):
        trade_id = str(record.get('_id') or '').strip()
        if not trade_id:
            continue
        trade = db._db['algo_trades'].find_one({'_id': trade_id})
        if not trade:
            continue
        underlying = str(
            (trade.get('config') or {}).get('Ticker')
            or trade.get('ticker') or ''
        ).strip().upper()
        try:
            lot_size = db.get_lot_size(trade_date, underlying)
        except Exception:
            lot_size = 75

        # Build index_spot_doc from Kite spot_map for live/FF
        index_spot_doc: dict = {}
        kite_spot = float(spot_map.get(underlying) or 0)
        if kite_spot > 0:
            index_spot_doc = {
                'underlying': underlying,
                'spot_price': kite_spot,
                'timestamp': now_ts,
                'source': 'kite_live',
            }

        entered_ids = _process_momentum_pending_feature_legs(
            db, trade, chain_col, trade_date, now_ts, lot_size,
            index_spot_doc=index_spot_doc or None,
            ltp_map=ltp_map,
            activation_mode=activation_mode,
        )
        if entered_ids:
            trade = db._db['algo_trades'].find_one({'_id': trade_id}) or trade
            mark_execute_order_dirty_from_trade(trade)
            print(
                f'[MOMENTUM ENTERED] trade_id={trade_id} '
                f'mode={activation_mode} legs={entered_ids}'
            )


def _run_entries_for_mode(
    db: MongoData,
    trade_date: str,
    activation_mode: str,
    listen_time: str,
    now_ts: str,
) -> None:
    from features.execution_socket import (
        _execute_backtest_entries,
        _load_running_trade_records,
        _sync_entered_legs_to_history,
        _validate_trade_leg_storage,
        build_entry_spot_snapshots,
    )

    records = _load_running_trade_records(
        db, trade_date, activation_mode=activation_mode,
    )
    if not records:
        return

    # For live/fast-forward/forward-test: process momentum-pending legs for ALL
    # running trades on every tick — independent of entry_time matching.
    if activation_mode in {'live', 'fast-forward', 'forward-test'}:
        _run_momentum_for_live_ff(db, trade_date, activation_mode, now_ts, records)

    if activation_mode not in {"live", "fast-forward", "forward-test"}:
        build_entry_spot_snapshots(db, records, listen_time, now_ts)
    entries_executed = _execute_backtest_entries(
        db, records, listen_time, now_ts,
    )
    if not entries_executed:
        return

    synced_ids: dict[str, dict] = {}
    for entry in entries_executed:
        if not entry.get("entered"):
            continue
        trade_id = str(entry.get("trade_id") or "").strip()
        if trade_id:
            synced_ids[trade_id] = {"_id": trade_id}
    if not synced_ids:
        return

    _sync_entered_legs_to_history(db, list(synced_ids.values()))
    for trade_id in synced_ids:
        _validate_trade_leg_storage(db, trade_id)


class _LiveTickDispatcher:
    def __init__(self) -> None:
        self._started = False
        self._start_lock = Lock()
        self._spot_queue = _SpotWriterQueue()
        self._live_queue = _WorkerQueue(coalesce_pending=False)
        self._fast_forward_queue = _WorkerQueue(coalesce_pending=True)
        self._forward_test_queue = _WorkerQueue(coalesce_pending=True)

    def ensure_started(self) -> None:
        if self._started:
            return
        with self._start_lock:
            if self._started:
                return
            Thread(target=self._spot_writer_loop, daemon=True, name="spot_tick_writer").start()
            Thread(
                target=self._mode_worker_loop,
                args=("live", self._live_queue, "kite_live"),
                daemon=True,
                name="live_tick_worker",
            ).start()
            Thread(
                target=self._mode_worker_loop,
                args=("fast-forward", self._fast_forward_queue, "kite_live"),
                daemon=True,
                name="fast_forward_tick_worker",
            ).start()
            Thread(
                target=self._mode_worker_loop,
                args=("forward-test", self._forward_test_queue, "kite_live", FORWARD_TEST_CHECK_INTERVAL_SECONDS),
                daemon=True,
                name="forward_test_tick_worker",
            ).start()
            self._started = True

    def dispatch_tick(
        self,
        *,
        trade_date: str,
        now_ts: str,
        now_minute: str,
        listen_time: str,
        broker_ltp_map: dict[str, float],
        spot_ticks_received: list[tuple[str, float, str]],
    ) -> None:
        self.ensure_started()
        task = TickTask(
            trade_date=trade_date,
            now_ts=now_ts,
            now_minute=now_minute,
            listen_time=listen_time,
            broker_ltp_map=dict(broker_ltp_map or {}),
            spot_ticks_received=list(spot_ticks_received or []),
        )
        if task.spot_ticks_received:
            self._spot_queue.put(task.spot_ticks_received)
        if runtime_mode_registry.has_active_mode("live"):
            self._live_queue.put(task)
        if runtime_mode_registry.has_active_mode("fast-forward"):
            self._fast_forward_queue.put(task)
        if runtime_mode_registry.has_active_mode("forward-test"):
            self._forward_test_queue.put(task)

    def _spot_writer_loop(self) -> None:
        while True:
            spot_ticks = self._spot_queue.get()
            db = MongoData()
            try:
                _persist_spot_ticks(db, spot_ticks, "kite_live")
            finally:
                db.close()

    def _mode_worker_loop(
        self,
        activation_mode: str,
        queue_obj: _WorkerQueue,
        spot_source: str,
        min_interval_seconds: float = 0.0,
    ) -> None:
        from features.kite_event import broker_live_tick

        last_run_at = 0.0
        while True:
            task = queue_obj.get()
            if min_interval_seconds > 0:
                now_monotonic = time.monotonic()
                if now_monotonic - last_run_at < min_interval_seconds:
                    continue
                last_run_at = now_monotonic
                print(
                    f'[{activation_mode.upper()} {int(min_interval_seconds)}s CHECK] '
                    f'tick-driven SL/target/lazy-leg check running '
                    f'trade_date={task.trade_date} now_ts={task.now_ts}'
                )
            db = MongoData()
            try:
                broker_live_tick(
                    db,
                    task.trade_date,
                    task.now_ts,
                    task.broker_ltp_map,
                    activation_mode=activation_mode,
                )

                if task.spot_ticks_received:
                    _persist_spot_ticks(db, task.spot_ticks_received, spot_source)
                _run_entries_for_mode(
                    db,
                    task.trade_date,
                    activation_mode,
                    task.listen_time,
                    task.now_ts,
                )
            except Exception as exc:
                logger.error(
                    "mode tick worker error mode=%s ts=%s: %s",
                    activation_mode,
                    task.now_ts,
                    exc,
                )
            finally:
                db.close()


live_tick_dispatcher = _LiveTickDispatcher()
