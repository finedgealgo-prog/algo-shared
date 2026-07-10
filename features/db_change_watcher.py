"""
db_change_watcher.py
────────────────────
MongoDB change watcher for the three trading collections.

Monitors:
  • algo_trades
  • algo_trade_positions_history
  • algo_leg_feature_status

When any document in these collections is inserted, updated, replaced, or
deleted, the watcher identifies which user_id owns that trade (via a cached
trade_id → user_id map built from today's active algo_trades rows) and calls
mark_execute_order_dirty so the next execute-orders WebSocket flush sends
fresh state to that user.

Covers ALL event types automatically:
  SL hit · momentum trigger · target hit · trail SL moved ·
  overall SL · reentry · re-momentum · lazy-leg entry · etc.

Two modes (auto-detected at startup):
  • Change Streams  – preferred; requires MongoDB replica set
  • Polling         – timestamp/signature-based fallback (standalone MongoDB)

Usage:
    from features.db_change_watcher import db_change_watcher
    db_change_watcher.start(trade_date='2026-04-22')
    ...
    db_change_watcher.stop()
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from features.mongo_data import MongoData

log = logging.getLogger(__name__)

WATCHED_COLLECTIONS = (
    'algo_trades',
    'algo_trade_positions_history',
    'algo_leg_feature_status',
)

# How often the polling fallback checks each collection (seconds)
POLL_INTERVAL_SECONDS = 1.5

# How often to refresh the trade_id → user_id cache (seconds)
CACHE_REFRESH_INTERVAL_SECONDS = 30


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _now_ist_date() -> str:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    total_minutes = now.hour * 60 + now.minute + 330  # UTC+5:30
    extra_days = total_minutes // 1440
    return (now.date() + timedelta(days=extra_days)).strftime('%Y-%m-%d')


# ─── Trade-user cache ─────────────────────────────────────────────────────────

class _TradeUserCache:
    """Thread-safe map: trade_id → {user_id, trade_date, activation_mode, group_id}."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, dict[str, str]] = {}
        self._last_refresh = 0.0

    # ── public API ─────────────────────────────────────────────────────────────

    def refresh(self, db: MongoData, trade_date: str) -> None:
        try:
            rows = list(db._db['algo_trades'].find(
                {'creation_ts': {'$regex': f'^{trade_date}'}},
                {'_id': 1, 'user_id': 1, 'creation_ts': 1,
                 'activation_mode': 1, 'portfolio': 1},
            ))
            new_data: dict[str, dict[str, str]] = {}
            for row in rows:
                tid = str(row.get('_id') or '').strip()
                uid = str(row.get('user_id') or '').strip()
                if not tid or not uid:
                    continue
                new_data[tid] = {
                    'user_id':         uid,
                    'trade_date':      trade_date,
                    'activation_mode': str(row.get('activation_mode') or 'live').strip(),
                    'group_id':        str(((row.get('portfolio') or {}).get('group_id')) or '').strip(),
                    'trade_id':        tid,
                }
            with self._lock:
                self._data = new_data
                self._last_refresh = time.monotonic()
            log.debug('[DB WATCHER] cache: %d trades for %s', len(new_data), trade_date)
        except Exception as exc:
            log.warning('[DB WATCHER] cache refresh error: %s', exc)

    def due_for_refresh(self) -> bool:
        return (time.monotonic() - self._last_refresh) > CACHE_REFRESH_INTERVAL_SECONDS

    def get(self, trade_id: str) -> dict[str, str] | None:
        with self._lock:
            return self._data.get(str(trade_id or '').strip())

    def active_trade_ids(self) -> list[str]:
        with self._lock:
            return list(self._data.keys())

    def lookup_or_fetch(self, db: MongoData, trade_id: str) -> dict[str, str] | None:
        """Return entry from cache; fall back to a single DB lookup if missing."""
        entry = self.get(trade_id)
        if entry:
            return entry
        try:
            row = db._db['algo_trades'].find_one(
                {'_id': trade_id},
                {'_id': 1, 'user_id': 1, 'creation_ts': 1,
                 'activation_mode': 1, 'portfolio': 1},
            )
            if not row:
                return None
            uid = str(row.get('user_id') or '').strip()
            ts  = str(row.get('creation_ts') or '')
            entry = {
                'user_id':         uid,
                'trade_date':      ts[:10] if len(ts) >= 10 else '',
                'activation_mode': str(row.get('activation_mode') or 'live').strip(),
                'group_id':        str(((row.get('portfolio') or {}).get('group_id')) or '').strip(),
                'trade_id':        trade_id,
            }
            with self._lock:
                self._data[trade_id] = entry
            return entry
        except Exception:
            return None


# ─── Change watcher ───────────────────────────────────────────────────────────

class DbChangeWatcher:
    """
    Watches three MongoDB collections for any write event and emits
    execute_order updates to the owning user's WebSocket room.
    """

    def __init__(self) -> None:
        self._running   = False
        self._threads:  list[threading.Thread] = []
        self._cache     = _TradeUserCache()
        self._cs_ok:    bool | None = None   # None = not yet probed

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def start(self, trade_date: str = '') -> None:
        if self._running:
            return
        resolved_date = str(trade_date or '').strip() or _now_ist_date()
        self._running = True

        # Seed the cache before any worker thread starts
        db = MongoData()
        try:
            self._cache.refresh(db, resolved_date)
        finally:
            db.close()

        if self._cs_ok is None:
            self._cs_ok = self._probe_change_streams()

        if self._cs_ok:
            for coll in WATCHED_COLLECTIONS:
                t = threading.Thread(
                    target=self._cs_worker,
                    args=(coll, resolved_date),
                    daemon=True,
                    name=f'db_watcher_cs_{coll}',
                )
                t.start()
                self._threads.append(t)
            log.info('[DB WATCHER] change-stream mode, trade_date=%s', resolved_date)
        else:
            t = threading.Thread(
                target=self._poll_worker,
                args=(resolved_date,),
                daemon=True,
                name='db_watcher_poll',
            )
            t.start()
            self._threads.append(t)
            log.info('[DB WATCHER] polling mode (no replica set), trade_date=%s', resolved_date)

    def stop(self) -> None:
        self._running = False
        self._threads.clear()
        log.info('[DB WATCHER] stopped')

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _probe_change_streams(self) -> bool:
        try:
            db = MongoData()
            try:
                info = db._db.client.admin.command('replSetGetStatus')
                return isinstance(info, dict)
            except Exception:
                return False
            finally:
                db.close()
        except Exception:
            return False

    def _mark_dirty(self, db: MongoData, trade_id: str) -> None:
        from features.execution_socket import mark_execute_order_dirty
        entry = self._cache.lookup_or_fetch(db, trade_id)
        if not entry or not entry.get('user_id'):
            return
        mark_execute_order_dirty(
            user_id=entry['user_id'],
            trade_date=entry['trade_date'],
            activation_mode=entry['activation_mode'],
            group_id=entry.get('group_id', ''),
            trade_id=trade_id,
        )
        log.debug(
            '[DB WATCHER] dirty user=%s trade=%s mode=%s',
            entry['user_id'], trade_id, entry['activation_mode'],
        )

    # ── Change Stream worker (one thread per collection) ───────────────────────

    def _cs_worker(self, coll_name: str, trade_date: str) -> None:
        pipeline = [{'$match': {
            'operationType': {'$in': ['insert', 'update', 'replace', 'delete']},
        }}]
        while self._running:
            db = MongoData()
            try:
                col = db._db[coll_name]
                with col.watch(pipeline, full_document='updateLookup', max_await_time_ms=2000) as stream:
                    while self._running:
                        if self._cache.due_for_refresh():
                            self._cache.refresh(db, trade_date)

                        change = stream.try_next()
                        if change is None:
                            time.sleep(0.05)
                            continue

                        trade_id = self._trade_id_from_change(coll_name, change)
                        if trade_id:
                            self._mark_dirty(db, trade_id)
            except Exception as exc:
                if self._running:
                    log.warning(
                        '[DB WATCHER CS] coll=%s error: %s — retry in 3s',
                        coll_name, exc,
                    )
                    time.sleep(3)
            finally:
                db.close()

    @staticmethod
    def _trade_id_from_change(coll_name: str, change: dict[str, Any]) -> str:
        op = str(change.get('operationType') or '')
        if op == 'delete':
            if coll_name == 'algo_trades':
                doc_key = change.get('documentKey') or {}
                return str(doc_key.get('_id') or '').strip()
            # For history/feature deletes we don't easily get trade_id; skip
            return ''
        full_doc: dict = change.get('fullDocument') or {}
        if coll_name == 'algo_trades':
            return str(full_doc.get('_id') or '').strip()
        # algo_trade_positions_history and algo_leg_feature_status both have trade_id
        return str(full_doc.get('trade_id') or '').strip()

    # ── Polling worker (fallback for standalone MongoDB) ──────────────────────

    def _poll_worker(self, trade_date: str) -> None:
        """
        Poll all three collections every POLL_INTERVAL_SECONDS.
        Detects changes by comparing a lightweight signature of key fields.
        Emits dirty marks only for rows whose signature changed since last poll.
        """
        # sig_key → last known signature string
        prev_sigs: dict[str, str] = {}

        while self._running:
            db = MongoData()
            try:
                if self._cache.due_for_refresh():
                    self._cache.refresh(db, trade_date)

                active_ids = self._cache.active_trade_ids()
                if not active_ids:
                    db.close()
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                dirty: set[str] = set()

                # ── algo_trades ──────────────────────────────────────────────
                for row in db._db['algo_trades'].find(
                    {'_id': {'$in': active_ids}},
                    {'_id': 1, 'updated_at': 1, 'trade_status': 1,
                     'status': 1, 'legs': 1},
                ):
                    tid = str(row.get('_id') or '').strip()
                    if not tid:
                        continue
                    legs = row.get('legs') or []
                    sig = (
                        str(row.get('updated_at') or '')
                        + str(row.get('trade_status') or '')
                        + str(row.get('status') or '')
                        + str(len(legs))
                        # include per-leg SL so trail-SL moves are caught
                        + '|'.join(
                            str(lg.get('current_sl_price') or '') + str(lg.get('display_sl_value') or '')
                            for lg in legs if isinstance(lg, dict)
                        )
                    )
                    key = f'at:{tid}'
                    if key in prev_sigs and prev_sigs[key] != sig:
                        dirty.add(tid)
                    prev_sigs[key] = sig

                # ── algo_trade_positions_history ─────────────────────────────
                for row in db._db['algo_trade_positions_history'].find(
                    {'trade_id': {'$in': active_ids}},
                    {'_id': 1, 'trade_id': 1, 'status': 1, 'updated_at': 1,
                     'exit_price': 1, 'current_sl_price': 1, 'exit_trade': 1},
                ):
                    tid = str(row.get('trade_id') or '').strip()
                    rid = str(row.get('_id') or '').strip()
                    if not tid or not rid:
                        continue
                    sig = (
                        str(row.get('updated_at') or '')
                        + str(row.get('status') or '')
                        + str(row.get('exit_price') or '')
                        + str(row.get('current_sl_price') or '')
                        + str(row.get('exit_trade') or '')
                    )
                    key = f'ph:{rid}'
                    if key in prev_sigs and prev_sigs[key] != sig:
                        dirty.add(tid)
                    prev_sigs[key] = sig

                # ── algo_leg_feature_status ──────────────────────────────────
                for row in db._db['algo_leg_feature_status'].find(
                    {'trade_id': {'$in': active_ids}},
                    {'_id': 1, 'trade_id': 1, 'updated_at': 1,
                     'base_trigger_value': 1, 'next_trigger_value': 1, 'enabled': 1},
                ):
                    tid = str(row.get('trade_id') or '').strip()
                    rid = str(row.get('_id') or '').strip()
                    if not tid or not rid:
                        continue
                    sig = (
                        str(row.get('updated_at') or '')
                        + str(row.get('base_trigger_value') or '')
                        + str(row.get('next_trigger_value') or '')
                        + str(row.get('enabled') or '')
                    )
                    key = f'fs:{rid}'
                    if key in prev_sigs and prev_sigs[key] != sig:
                        dirty.add(tid)
                    prev_sigs[key] = sig

                for tid in dirty:
                    self._mark_dirty(db, tid)

            except Exception as exc:
                log.warning('[DB WATCHER POLL] error: %s', exc)
            finally:
                db.close()

            time.sleep(POLL_INTERVAL_SECONDS)


# ── Module-level singleton ────────────────────────────────────────────────────

db_change_watcher = DbChangeWatcher()
