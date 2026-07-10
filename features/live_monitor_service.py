"""
live_monitor_service.py  — Ultra-speed Live Position Monitor
─────────────────────────────────────────────────────────────
Designed for high-frequency live trading: checks 1000+ positions
per tick in milliseconds.

Speed strategy
──────────────
1. In-memory trade cache  — DB is read once every 5s, NOT on every tick.
   On each tick the cache (plain Python dicts) is scanned — pure RAM ops.

2. Legs pre-loaded        — history-ref legs are inlined into the cache
   during refresh, so process_broker_tick never hits DB to load legs.

3. Persistent DB conn     — one MongoData for the whole process lifetime.
   No connect/close overhead per tick.

4. No throttle            — every Kite tick is processed immediately.
   Kite emits ticks only when price changes, so natural rate-limiting.

5. DB writes only on action — SL hit / TP hit / trail SL update / exit.
   Normal ticks (no action) are pure in-memory and complete in < 1 ms.

6. Thread pool for DB I/O — blocking PyMongo calls run in a worker thread
   so the asyncio event loop is never blocked.

Lifecycle
─────────
  start(loop)           ← FastAPI @startup
  stop()                ← FastAPI @shutdown
  attach_kite_listener()← after POST /kite/config sets credentials
"""

from __future__ import annotations

import asyncio
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from bson import ObjectId

log = logging.getLogger(__name__)

# How often to reload running trades from DB (seconds).
# Between refreshes, all checking is pure in-memory.
CACHE_TTL_SECONDS = 5.0

# Collection name for open leg history docs
COL_POSITIONS_HIST = 'algo_trade_positions_history'
RUNNING_STATUS     = 'StrategyStatus.Live_Running'
OPEN_LEG_STATUS    = 1
CLOSED_LEG_STATUS  = 0


def _is_history_ref(value) -> bool:
    return isinstance(value, (str, ObjectId))


def _build_exit_trade_payload(exit_price: float, exit_reason: str, now_ts: str) -> dict:
    return {
        'trigger_timestamp': now_ts,
        'trigger_price': exit_price,
        'price': exit_price,
        'traded_timestamp': now_ts,
        'exchange_timestamp': now_ts,
        'exit_reason': exit_reason,
    }


def _safe_overall_parser(original_parser):
    """
    trading_core.process_broker_tick logs overall config using:
      (parse_overall_sl(cfg)[0] or {}).get('Type')
    but position_manager.parse_overall_sl() returns (str, float).
    Wrap the first item so live monitor can reuse trading_core unchanged.
    """
    def _wrapper(strategy_cfg: dict) -> tuple[dict, float]:
        sl_type, sl_value = original_parser(strategy_cfg)
        return ({'Type': str(sl_type or '')}, sl_value)
    return _wrapper


def _format_action_with_live_ltp(action: str,
                                 ltp_map: dict[str, float],
                                 leg_token_map: dict[tuple, str]) -> str:
    """
    For action strings like:
      trade_id/leg_id: SL hit @ 174.0
    append the current live LTP from this tick when we can resolve the leg token.
    """
    raw = str(action or '').strip()
    if not raw or ': ' not in raw or '/' not in raw:
        return raw

    trade_leg_part, _rest = raw.split(': ', 1)
    trade_id, leg_id = trade_leg_part.split('/', 1)
    token = str(leg_token_map.get((trade_id.strip(), leg_id.strip())) or '').strip()
    current_ltp = None
    if token:
        current_ltp = ltp_map.get(token)
    if current_ltp in (None, ''):
        return f'{raw} - ltp - UNAVAILABLE'
    return f'{raw} - ltp - {current_ltp}'


def _live_safe_close_leg_in_db(db, trade_id: str, leg_index: int,
                               exit_price: float, exit_reason: str, now_ts: str,
                               leg_id: str = '') -> None:
    """
    Live-monitor-only close helper.

    algo_trades.legs can be either:
      1. embedded dict legs
      2. string/ObjectId refs to algo_trade_positions_history

    Updating legs.<idx>.exit_reason fails for ref arrays, so in live mode we
    always update position history first and only update algo_trades when an
    embedded leg object actually exists.
    """
    exit_payload = _build_exit_trade_payload(exit_price, exit_reason, now_ts)
    try:
        if leg_id:
            db._db[COL_POSITIONS_HIST].update_one(
                {'trade_id': trade_id, 'leg_id': leg_id, 'exit_trade': None},
                {'$set': {
                    'exit_trade': exit_payload,
                    'last_saw_price': exit_price,
                    'status': CLOSED_LEG_STATUS,
                    'exit_reason': exit_reason,
                }},
            )
        else:
            db._db[COL_POSITIONS_HIST].update_one(
                {'trade_id': trade_id, 'exit_trade': None},
                {'$set': {
                    'exit_trade': exit_payload,
                    'last_saw_price': exit_price,
                    'status': CLOSED_LEG_STATUS,
                    'exit_reason': exit_reason,
                }},
                sort=[('_id', 1)],
            )
    except Exception as exc:
        log.error('live_safe_close_leg history error trade=%s leg=%s: %s', trade_id, leg_id or leg_index, exc)

    try:
        if leg_id:
            db._db['algo_trades'].update_one(
                {'_id': trade_id, 'legs.id': leg_id},
                {'$set': {
                    'legs.$.status': CLOSED_LEG_STATUS,
                    'legs.$.exit_reason': exit_reason,
                    'legs.$.exit_trade': exit_payload,
                    'legs.$.last_saw_price': exit_price,
                }},
            )
        else:
            db._db['algo_trades'].update_one(
                {'_id': trade_id},
                {'$set': {
                    f'legs.{leg_index}.status': CLOSED_LEG_STATUS,
                    f'legs.{leg_index}.exit_reason': exit_reason,
                    f'legs.{leg_index}.exit_trade': exit_payload,
                    f'legs.{leg_index}.last_saw_price': exit_price,
                }},
            )
    except Exception as exc:
        log.error('live_safe_close_leg trade=%s leg=%s: %s', trade_id, leg_id or leg_index, exc)

    try:
        from features.execution_socket import mark_execute_order_dirty_from_trade_id
        mark_execute_order_dirty_from_trade_id(db, trade_id)
    except Exception as exc:
        log.warning('live_safe_close_leg dirty-mark error trade=%s: %s', trade_id, exc)


def _live_safe_update_leg_sl_in_db(db, trade_id: str, leg_index: int,
                                   new_sl: float, last_price: float, leg_id: str = '') -> None:
    """
    Live-monitor-only SL updater that tolerates ref-based algo_trades.legs.
    """
    try:
        if leg_id:
            db._db[COL_POSITIONS_HIST].update_one(
                {'trade_id': trade_id, 'leg_id': leg_id, 'exit_trade': None},
                {'$set': {
                    'current_sl_price': new_sl,
                    'display_sl_value': new_sl,
                    'last_saw_price': last_price,
                }},
            )
        else:
            db._db[COL_POSITIONS_HIST].update_one(
                {'trade_id': trade_id, 'exit_trade': None},
                {'$set': {
                    'current_sl_price': new_sl,
                    'display_sl_value': new_sl,
                    'last_saw_price': last_price,
                }},
                sort=[('_id', 1)],
            )
    except Exception as exc:
        log.error('live_safe_update_leg_sl history error trade=%s leg=%s: %s', trade_id, leg_id or leg_index, exc)

    try:
        if leg_id:
            db._db['algo_trades'].update_one(
                {'_id': trade_id, 'legs.id': leg_id},
                {'$set': {
                    'legs.$.current_sl_price': new_sl,
                    'legs.$.display_sl_value': new_sl,
                    'legs.$.last_saw_price': last_price,
                }},
            )
        else:
            db._db['algo_trades'].update_one(
                {'_id': trade_id},
                {'$set': {
                    f'legs.{leg_index}.current_sl_price': new_sl,
                    f'legs.{leg_index}.display_sl_value': new_sl,
                    f'legs.{leg_index}.last_saw_price': last_price,
                }},
            )
    except Exception as exc:
        log.error('live_safe_update_leg_sl trade=%s leg=%s: %s', trade_id, leg_id or leg_index, exc)

    try:
        from features.execution_socket import mark_execute_order_dirty_from_trade_id
        mark_execute_order_dirty_from_trade_id(db, trade_id)
    except Exception as exc:
        log.warning('live_safe_update_leg_sl dirty-mark error trade=%s: %s', trade_id, exc)

    if leg_id and new_sl > 0:
        try:
            from features.live_order_manager import modify_broker_sl_order
            modify_broker_sl_order(db, trade_id, leg_id, new_sl)
        except Exception as exc:
            log.warning('live_safe_update_leg_sl broker-modify error trade=%s leg=%s: %s', trade_id, leg_id, exc)


# ─── Singleton service ────────────────────────────────────────────────────────

class LiveMonitorService:

    def __init__(self) -> None:
        self._loop:          asyncio.AbstractEventLoop | None = None
        self._queue:         asyncio.Queue  = asyncio.Queue(maxsize=5000)
        self._running:       bool           = False
        self._task:          asyncio.Task | None = None
        self._executor:      ThreadPoolExecutor | None = ThreadPoolExecutor(max_workers=3, thread_name_prefix='live-mon')

        # ── Persistent DB connection (opened once, reused forever) ────────
        self._db = None   # MongoData — created in start()

        # ── In-memory trade cache ─────────────────────────────────────────
        # Refreshed from DB every CACHE_TTL_SECONDS.
        # Each entry is a fully populated trade dict with all legs as dicts
        # (history refs are inlined during refresh so tick processing is pure RAM).
        self._trades_cache:    list[dict]        = []
        self._cache_loaded_at: float             = 0.0   # monotonic timestamp

        # Derived lookup maps — rebuilt on every cache refresh.
        # trade_id → user_id
        self._trade_user_map:  dict[str, str]        = {}
        # (trade_id, leg_id) → instrument token string
        self._leg_token_map:   dict[tuple, str]      = {}

        # ── Stats (for print output) ──────────────────────────────────────
        self._ticks_received:  int = 0
        self._ticks_processed: int = 0
        self._actions_total:   int = 0
        self._last_stat_print: float = 0.0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._running:
            print('[LiveMonitor] already running — start() ignored')
            return
        self._loop    = loop
        self._running = True
        self._queue   = asyncio.Queue(maxsize=5000)
        self._task    = None
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix='live-mon')

        # Open persistent DB connection
        from features.mongo_data import MongoData
        self._db = MongoData()
        print('[LiveMonitor] persistent DB connection opened')

        # Start asyncio processing coroutine
        self._task = loop.create_task(self._process_loop(), name='live-monitor')
        print('[LiveMonitor] background task created')

        # Attach Kite listener if credentials already available
        from features.broker_gateway import broker_add_tick_listener as add_tick_listener, broker_is_configured as is_configured
        if is_configured():
            add_tick_listener(self._on_kite_tick)
            print('[LiveMonitor] Kite tick listener registered at startup')
            self._bootstrap_kite_tokens_from_cache()
        else:
            print('[LiveMonitor] Kite not configured yet — call POST /kite/config')

    def attach_kite_listener(self) -> None:
        """
        Called after Kite credentials are set (POST /kite/config or /live/start).
        Safe to call multiple times — duplicate listener is ignored.
        """
        from features.broker_gateway import broker_add_tick_listener as add_tick_listener
        add_tick_listener(self._on_kite_tick)
        print('[LiveMonitor] Kite tick listener attached — monitoring active')
        self._bootstrap_kite_tokens_from_cache()

    def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None
        from features.broker_gateway import broker_remove_tick_listener as remove_tick_listener
        remove_tick_listener(self._on_kite_tick)
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None
        if self._db:
            try:
                self._db.close()
            except Exception:
                pass
            self._db = None
        self._trades_cache = []
        self._trade_user_map = {}
        self._leg_token_map = {}
        self._cache_loaded_at = 0.0
        print('[LiveMonitor] stopped')

    # ── Kite tick receiver (KiteTicker thread → asyncio queue) ────────────────

    def _on_kite_tick(self, ltp_map: dict) -> None:
        """
        Entry point from KiteTicker thread.
        Must be microsecond-fast — just enqueue and return.
        """
        if not self._loop or not self._running:
            return
        self._ticks_received += 1
        try:
            # put_nowait: if queue is full we drop the oldest tick (stale anyway)
            self._loop.call_soon_threadsafe(self._queue.put_nowait, ltp_map)
        except Exception:
            pass   # queue full → skip this tick (next one will have fresher prices)

    # ── Main async loop ───────────────────────────────────────────────────────

    async def _process_loop(self) -> None:
        """
        Drains the tick queue.
        Consecutive ticks are merged (latest price per token wins) so we
        always act on the freshest snapshot even during bursts.
        """
        print('[LiveMonitor] process loop started — waiting for Kite ticks')
        while self._running:
            try:
                # ── Wait for first tick ───────────────────────────────────
                ltp_map: dict[str, float] = await self._queue.get()

                # ── Drain burst: merge all waiting ticks ──────────────────
                # After a price move Kite sends ticks for multiple tokens
                # at once. Merge them all before processing — no point
                # checking SL/TP with stale prices if fresher ones are queued.
                merged: dict[str, float] = dict(ltp_map)
                drained = 0
                while not self._queue.empty():
                    try:
                        merged.update(self._queue.get_nowait())
                        drained += 1
                    except asyncio.QueueEmpty:
                        break

                self._ticks_processed += 1
                tok_count = len(merged)

                # print(f'[TICK] #{self._ticks_processed} tokens={tok_count} burst_drained={drained}')

                # ── Check if cache needs reload ───────────────────────────
                cache_age = time.monotonic() - self._cache_loaded_at
                if cache_age >= CACHE_TTL_SECONDS or not self._trades_cache:
                    # Cache expired — reload in thread pool (blocks DB read)
                    print(f'[LiveMonitor] cache refresh (age={cache_age:.1f}s)')
                    await self._loop.run_in_executor(
                        self._executor, self._reload_cache
                    )

                trade_count = len(self._trades_cache)
                if not trade_count:
                    # print('[LiveMonitor] no live running trades — tick skipped')
                    continue

                # print(f'[TICK PROCESS] trades={trade_count} tokens_received={tok_count}')

                # ── Run SL/TP/trail check in thread pool ──────────────────
                # Blocking DB writes (on hits) happen here.
                await self._loop.run_in_executor(
                    self._executor, self._check_tick, dict(merged)
                )

                # ── Periodic stats print (every 60s) ─────────────────────
                now = time.monotonic()
                if now - self._last_stat_print >= 60.0:
                    self._last_stat_print = now
                    print(
                        f'[LiveMonitor STATS] '
                        f'ticks_received={self._ticks_received} '
                        f'ticks_processed={self._ticks_processed} '
                        f'actions_total={self._actions_total} '
                        f'live_trades={trade_count} '
                        f'cache_age={cache_age:.1f}s'
                    )

            except asyncio.CancelledError:
                print('[LiveMonitor] process loop cancelled')
                break
            except Exception as exc:
                log.exception('[LiveMonitor] process_loop error: %s', exc)

    # ── Cache reload (runs in thread pool) ────────────────────────────────────

    def _reload_cache(self) -> None:
        """
        Reload all live running trades from DB and pre-inline their history legs.
        This is the ONLY place that reads algo_trades from DB per cycle.
        After this, tick processing is pure in-memory until next TTL expiry.
        """
        t0 = time.perf_counter()

        try:
            # Query both 'live' and 'fast-forward' modes without a creation_ts date
            # filter — strategies may have been created on prior dates and still be
            # actively running today (same approach as live_entry_monitor).
            query = {
                'trade_status': 1,
                'activation_mode': {'$in': ['live', 'fast-forward', 'forward-test']},
                'status': RUNNING_STATUS,
            }
            raw_trades = list(self._db._db['algo_trades'].find(query))

            # ── Pre-inline history-ref legs ───────────────────────────────
            # algo_trades.legs[] can contain string IDs (refs to
            # algo_trade_positions_history). Inline them NOW so
            # process_broker_tick never hits DB during tick processing.
            hist_col = self._db._db[COL_POSITIONS_HIST]
            trades_ready: list[dict] = []
            total_legs = 0

            for trade in raw_trades:
                trade_id = str(trade.get('_id') or '')
                legs     = trade.get('legs') or []

                has_refs = any(_is_history_ref(item) for item in legs)
                if has_refs:
                    hist_docs = self._load_open_history_docs_for_trade_refs(trade_id, legs)
                    inlined: list[dict] = []
                    for item in legs:
                        if isinstance(item, dict):
                            inlined.append(item)
                        elif _is_history_ref(item):
                            matched = hist_docs.get(str(item).strip())
                            if matched:
                                inlined.append(matched)
                    trade['legs'] = inlined

                open_legs = [
                    l for l in (trade.get('legs') or [])
                    if isinstance(l, dict) and int(l.get('status') or 0) == OPEN_LEG_STATUS
                ]
                total_legs += len(open_legs)
                trades_ready.append(trade)

            # ── Build lookup maps ─────────────────────────────────────────
            trade_user_map: dict[str, str]   = {}
            leg_token_map:  dict[tuple, str] = {}

            for trade in trades_ready:
                tid = str(trade.get('_id') or '')
                uid = str(trade.get('user_id') or '')
                if tid and uid:
                    trade_user_map[tid] = uid
                for leg in (trade.get('legs') or []):
                    if not isinstance(leg, dict):
                        continue
                    entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
                    tok    = str(
                        leg.get('token')
                        or leg.get('instrument_token')
                        or entry_trade.get('token')
                        or entry_trade.get('instrument_token')
                        or ''
                    )
                    if not (tid and tok):
                        continue

                    candidate_leg_ids = {
                        str(leg.get('leg_id') or '').strip(),
                        str(leg.get('id') or '').strip(),
                        str(entry_trade.get('leg_id') or '').strip(),
                        str(entry_trade.get('id') or '').strip(),
                    }
                    for candidate_leg_id in candidate_leg_ids:
                        if candidate_leg_id:
                            leg_token_map[(tid, candidate_leg_id)] = tok

            # Fallback source-of-truth for live legs:
            # when algo_trades cache shape doesn't preserve the exact leg key used
            # by action logs, pull open history docs directly and map leg_id -> token.
            trade_ids = [str(trade.get('_id') or '').strip() for trade in trades_ready if str(trade.get('_id') or '').strip()]
            if trade_ids:
                for hdoc in hist_col.find(
                    {'trade_id': {'$in': trade_ids}, 'exit_trade': None},
                    {'trade_id': 1, 'leg_id': 1, 'token': 1, 'instrument_token': 1},
                ):
                    tid = str(hdoc.get('trade_id') or '').strip()
                    h_leg_id = str(hdoc.get('leg_id') or '').strip()
                    tok = str(hdoc.get('token') or hdoc.get('instrument_token') or '').strip()
                    if tid and h_leg_id and tok:
                        leg_token_map[(tid, h_leg_id)] = tok

            # ── Pre-seed SL/TP prices on legs that lack them ──────────────
            # process_broker_tick → check_leg_sl → calc_sl_price has wrong
            # arg order for live mode (works in backtest because stored_sl is
            # always set there).  Ensuring current_sl_price / current_tp_price
            # are populated here means stored_sl is never None on first tick,
            # so the broken fallback call is never reached.
            self._seed_leg_sl_tp_prices(trades_ready)

            # ── Commit atomically ─────────────────────────────────────────
            self._trades_cache    = trades_ready
            self._trade_user_map  = trade_user_map
            self._leg_token_map   = leg_token_map
            self._cache_loaded_at = time.monotonic()

            active_tokens = self._collect_active_cache_tokens()
            self._sync_kite_tokens(active_tokens)

            elapsed_ms = (time.perf_counter() - t0) * 1000
            print(
                f'[LiveMonitor CACHE] trades={len(trades_ready)} '
                f'open_legs={total_legs} '
                f'load_time={elapsed_ms:.1f}ms'
            )

        except Exception as exc:
            log.exception('[LiveMonitor] _reload_cache error: %s', exc)

    def _load_open_history_docs_for_trade_refs(self, trade_id: str, legs: list) -> dict[str, dict]:
        """
        Source of truth for restart/bootstrap:
          algo_trades.legs[] history refs (string/ObjectId) -> algo_trade_positions_history._id
          only records where exit_trade is null are considered open.
        """
        hist_col = self._db._db[COL_POSITIONS_HIST]
        ref_ids = [
            str(item).strip()
            for item in (legs or [])
            if _is_history_ref(item) and str(item).strip()
        ]
        if not ref_ids:
            print(f'[LiveMonitor HISTORY REFS] trade_id={trade_id} legs_refs=0 open_history_matches=0 tokens=-')
            return {}

        object_ids: list[ObjectId] = []
        for ref_id in ref_ids:
            try:
                object_ids.append(ObjectId(ref_id))
            except Exception:
                pass

        query: dict = {
            'trade_id': trade_id,
            'exit_trade': None,
        }
        if object_ids:
            query['_id'] = {'$in': object_ids}
        else:
            query['_id'] = {'$in': ref_ids}

        docs = list(hist_col.find(query))
        by_ref: dict[str, dict] = {}
        for doc in docs:
            ref_key = str(doc.get('_id') or '').strip()
            if ref_key:
                by_ref[ref_key] = doc

        print(
            f'[LiveMonitor HISTORY REFS] trade_id={trade_id} '
            f'legs_refs={len(ref_ids)} open_history_matches={len(by_ref)} '
            f'tokens={",".join(str((doc or {}).get("token") or "-") for doc in by_ref.values()) or "-"}'
        )
        for doc in docs:
            print(
                f'[OPEN LEG TOKEN] trade_id={trade_id} '
                f'history_id={str(doc.get("_id") or "").strip() or "-"} '
                f'leg_id={str(doc.get("leg_id") or doc.get("id") or "").strip() or "-"} '
                f'token={str(doc.get("token") or doc.get("instrument_token") or "").strip() or "-"} '
                f'symbol={str(doc.get("symbol") or "-").strip() or "-"} '
                f'exit_trade={"YES" if doc.get("exit_trade") else "NO"}'
            )
        return by_ref

    def _seed_leg_sl_tp_prices(self, trades: list[dict]) -> None:
        """
        For every open leg that lacks current_sl_price / current_tp_price,
        compute them from entry_price + leg config and set them in-place.

        This ensures process_broker_tick always finds stored_sl / stored_tp
        as non-None, so the broken calc_sl_price(leg_cfg, …) fallback inside
        check_leg_sl (which has wrong arg order for live mode) is never reached.
        """
        from features.position_manager import calc_sl_price, calc_tp_price  # type: ignore

        for trade in trades:
            strategy_cfg = trade.get('strategy') or trade.get('config') or {}
            # Build leg_cfg lookup: id → config dict
            all_cfgs: dict[str, dict] = {}
            for cfg in (strategy_cfg.get('ListOfLegConfigs') or []):
                cfg_id = str(cfg.get('id') or '')
                if cfg_id:
                    all_cfgs[cfg_id] = cfg
            idle = strategy_cfg.get('IdleLegConfigs') or {}
            if isinstance(idle, dict):
                for k, v in idle.items():
                    if isinstance(v, dict):
                        all_cfgs[str(k)] = v

            for leg in (trade.get('legs') or []):
                if not isinstance(leg, dict):
                    continue
                if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
                    continue
                entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
                if not entry_trade:
                    continue  # pending — no entry yet

                # History docs store original config id in 'leg_id'; 'id' is
                # overwritten with the MongoDB ObjectId after insertion — use leg_id first.
                leg_id = str(leg.get('leg_id') or leg.get('id') or '')
                leg_cfg = all_cfgs.get(leg_id) or {}
                if not leg_cfg:
                    continue

                entry_price = float(entry_trade.get('price') or entry_trade.get('trigger_price') or 0)
                if not entry_price:
                    continue

                is_sell = 'sell' in str(leg.get('position') or '').lower()

                if not (leg.get('current_sl_price') or leg.get('sl_price')):
                    try:
                        sl_price = calc_sl_price(entry_price, is_sell, leg_cfg.get('LegStopLoss') or {})
                        if sl_price:
                            leg['current_sl_price'] = sl_price
                    except Exception:
                        pass

                if not (leg.get('current_tp_price') or leg.get('tp_price')):
                    try:
                        tp_price = calc_tp_price(entry_price, is_sell, leg_cfg.get('LegTarget') or {})
                        if tp_price:
                            leg['current_tp_price'] = tp_price
                    except Exception:
                        pass

    def _collect_active_cache_tokens(self) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for trade in (self._trades_cache or []):
            for leg in (trade.get('legs') or []):
                if not isinstance(leg, dict):
                    continue
                if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
                    continue
                entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
                token = str(
                    leg.get('token')
                    or leg.get('instrument_token')
                    or entry_trade.get('token')
                    or entry_trade.get('instrument_token')
                    or ''
                ).strip()
                if token and token.isdigit() and token not in seen:
                    seen.add(token)
                    ordered.append(token)
        return ordered

    # ── Tick check (runs in thread pool) ─────────────────────────────────────

    def _check_tick(self, ltp_map: dict[str, float]) -> None:
        """
        Core SL/TP/trail/overall/broker check — called on every tick.
        Uses cached trades (no DB read). DB writes only when action taken.
        Target: < 1ms for 1000 positions with no hits.
        """
        t0 = time.perf_counter()

        try:
            from features.execution_socket import (
                _broker_live_tick,
                _today_ist,
                _now_iso,
            )
            from features import trading_core as _trading_core
            from features.position_manager import (
                parse_overall_sl as _pm_parse_overall_sl,
                parse_overall_tgt as _pm_parse_overall_tgt,
            )

            trade_date = self._today_ist()
            now_ts     = self._now_iso()

            # Live monitor reuses trading_core.process_broker_tick(), but that
            # path still assumes older overall parser and embedded leg updates.
            # Patch only for this live tick so algo-backtest paths stay intact.
            original_parse_overall_sl = _trading_core.parse_overall_sl
            original_parse_overall_tgt = _trading_core.parse_overall_tgt
            original_close_leg_in_db = _trading_core.close_leg_in_db
            original_update_leg_sl_in_db = _trading_core.update_leg_sl_in_db
            _trading_core.parse_overall_sl = _safe_overall_parser(_pm_parse_overall_sl)
            _trading_core.parse_overall_tgt = _safe_overall_parser(_pm_parse_overall_tgt)
            _trading_core.close_leg_in_db = _live_safe_close_leg_in_db
            _trading_core.update_leg_sl_in_db = _live_safe_update_leg_sl_in_db

            # ── SL / TP / trail / overall / broker settings check ─────────
            # Passes pre-loaded cached trades — NO DB read inside for normal ticks.
            # DB writes happen only when SL/TP fires or trail SL updates.
            try:
                result = _broker_live_tick(
                    db              = self._db,
                    trade_date      = trade_date,
                    now_ts          = now_ts,
                    broker_ltp_map  = ltp_map,
                    activation_mode = 'live',
                    running_trades  = self._trades_cache,   # ← cached, no DB read
                )
            finally:
                _trading_core.parse_overall_sl = original_parse_overall_sl
                _trading_core.parse_overall_tgt = original_parse_overall_tgt
                _trading_core.close_leg_in_db = original_close_leg_in_db
                _trading_core.update_leg_sl_in_db = original_update_leg_sl_in_db

            elapsed_ms     = (time.perf_counter() - t0) * 1000
            actions        = result.get('actions_taken') or []
            hit_ids        = result.get('hit_trade_ids') or []
            open_pos       = result.get('open_positions') or []
            active_tokens  = result.get('subscribe_tokens') or []

            # ── Always print per-tick summary ─────────────────────────────
            print(
                f'[TICK CHECK] '
                f'time={elapsed_ms:.2f}ms '
                f'trades={len(self._trades_cache)} '
                f'tokens_in={len(ltp_map)} '
                f'positions_checked={len(open_pos)} '
                f'actions={len(actions)} '
                f'sl_tp_hits={len(hit_ids)}'
            )

            # ── Print each action taken ───────────────────────────────────
            for action in actions:
                display_action = _format_action_with_live_ltp(
                    action,
                    ltp_map,
                    self._leg_token_map,
                )
                print(f'  [ACTION] {display_action}')

            # ── Print SL/TP hits ──────────────────────────────────────────
            for hit_id in hit_ids:
                uid = self._trade_user_map.get(str(hit_id), '?')
                print(f'  [HIT] trade_id={hit_id} user={uid}')

            self._actions_total += len(actions)

            # ── Sync Kite subscriptions from active token list ────────────
            if active_tokens:
                self._sync_kite_tokens(active_tokens)

            # ── Broadcast to connected frontends (if any) ─────────────────
            if open_pos or hit_ids:
                asyncio.run_coroutine_threadsafe(
                    self._broadcast(
                        trade_date      = trade_date,
                        now_ts          = now_ts,
                        open_positions  = open_pos,
                        hit_trade_ids   = hit_ids,
                        hit_ltp_snaps   = result.get('hit_ltp_snapshots') or {},
                        actions_taken   = actions,
                    ),
                    self._loop,
                )

            # ── Force cache refresh if positions changed (SL/TP hit) ──────
            if hit_ids:
                print(f'[LiveMonitor] SL/TP hit — forcing cache refresh on next tick')
                self._cache_loaded_at = 0.0   # expire cache immediately

        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            log.exception('[LiveMonitor] _check_tick error (%.1fms): %s', elapsed_ms, exc)

    # ── Kite subscription sync ────────────────────────────────────────────────

    def _sync_kite_tokens(self, active_token_strs: list[str]) -> None:
        """
        Keep Kite subscriptions in sync with currently open legs.
        Refreshes both '__live_monitor__' and '__live_entry_option__' so that
        tokens for SL/TP-hit (closed) legs are unsubscribed from Kite WS.
        """
        from features.broker_gateway import broker_extract_instrument_tokens as extract_instrument_tokens, broker_refresh_user_tokens as refresh_user_tokens
        positions = [{'token': t} for t in active_token_strs]
        num_toks  = extract_instrument_tokens(positions)
        refresh_user_tokens('__live_monitor__', num_toks)
        # Also sync the entry-monitor user — it registers tokens at entry time
        # but never cleans them up on SL/TP hit, causing stale Kite subscriptions.
        refresh_user_tokens('__live_entry_option__', num_toks)
        print(
            f'[LiveMonitor TOKEN SYNC] owner=__live_monitor__+__live_entry_option__ '
            f'count={len(num_toks)} tokens={",".join(str(tok) for tok in num_toks) or "-"}'
        )

    def _bootstrap_register_kite_tokens(self, active_token_strs: list[str]) -> None:
        """
        On /live/start, aggressively register current open-leg tokens so the
        shared Kite socket subscribes immediately even before first tick cycle.
        """
        from features.broker_gateway import broker_extract_instrument_tokens as extract_instrument_tokens, broker_register_user_tokens as register_user_tokens
        positions = [{'token': t} for t in active_token_strs]
        num_toks = extract_instrument_tokens(positions)
        if not num_toks:
            print('[LiveMonitor BOOTSTRAP REGISTER] owner=__live_monitor__ count=0 tokens=-')
            return
        register_user_tokens('__live_monitor__', num_toks)
        print(
            f'[LiveMonitor BOOTSTRAP REGISTER] owner=__live_monitor__ '
            f'count={len(num_toks)} tokens={",".join(str(tok) for tok in num_toks)}'
        )

    def _bootstrap_kite_tokens_from_cache(self) -> None:
        """
        On startup or /live/start attach, immediately load open live legs and
        subscribe their tokens before the first market tick arrives.
        """
        if not self._db:
            print('[LiveMonitor BOOTSTRAP] skipped reason=db_not_ready')
            return
        try:
            self._reload_cache()
            active_tokens = self._collect_active_cache_tokens()
            self._bootstrap_register_kite_tokens(active_tokens)
            print(
                f'[LiveMonitor BOOTSTRAP] trades={len(self._trades_cache)} '
                f'open_leg_tokens={len(active_tokens)} '
                f'tokens={",".join(active_tokens) or "-"}'
            )
        except Exception as exc:
            log.exception('[LiveMonitor] bootstrap token sync error: %s', exc)

    # ── Async broadcast to connected frontends ────────────────────────────────

    async def _broadcast(
        self,
        trade_date:     str,
        now_ts:         str,
        open_positions: list[dict],
        hit_trade_ids:  list,
        hit_ltp_snaps:  dict,
        actions_taken:  list[str],
    ) -> None:
        """
        Send ltp_update and execute_order to connected frontend WebSockets.
        If frontend is disconnected — no-op. All DB changes already persisted.
        """
        from features.execution_socket import (
            _broadcast_user_channel_message,
            _build_execute_order_socket_payload,
            _build_message,
            _populate_legs_from_history,
            _serialize_trade_record,
        )
        from bson import ObjectId

        listen_time = self._now_ist_str()

        # ── ltp_update per user (only their own tokens) ───────────────────
        by_user: dict[str, list[dict]] = {}
        for pos in (open_positions or []):
            tid    = str(pos.get('trade_id') or '')
            leg_id = str(pos.get('leg_id')   or '')
            uid    = self._trade_user_map.get(tid, '')
            tok    = self._leg_token_map.get((tid, leg_id), '')
            if uid and tok:
                by_user.setdefault(uid, []).append(
                    {'token': tok, 'ltp': float(pos.get('ltp') or 0)}
                )

        for uid, ltp_list in by_user.items():
            # print(f'[BROADCAST] ltp_update user={uid} tokens={len(ltp_list)}')
            msg = _build_message(
                'ltp_update',
                'Live monitor LTP',
                {
                    'trade_date':       trade_date,
                    'listen_time':      listen_time,
                    'listen_timestamp': now_ts,
                    'ltp':              ltp_list,
                },
            )
            await _broadcast_user_channel_message(uid, 'update', msg)

        # ── execute_order for SL/TP hits ──────────────────────────────────
        if hit_trade_ids:
            try:
                oids = []
                for tid in hit_trade_ids:
                    try:
                        oids.append(ObjectId(str(tid)))
                    except Exception:
                        pass
                if oids:
                    hit_raw = list(self._db._db['algo_trades'].find({'_id': {'$in': oids}}))
                    hit_ser = [_serialize_trade_record(r) for r in hit_raw]
                    hit_enr = _populate_legs_from_history(self._db, hit_ser)

                    hit_by_user: dict[str, list[dict]] = {}
                    for rec in hit_enr:
                        uid = str(rec.get('user_id') or '').strip()
                        hit_by_user.setdefault(uid, []).append(rec)

                    for uid, records in hit_by_user.items():
                        payload              = _build_execute_order_socket_payload(records, trigger='sl-target-hit')
                        payload['hit_ltp_snapshots'] = hit_ltp_snaps
                        payload['actions_taken']     = actions_taken
                        hit_msg = _build_message('execute_order', 'SL/Target hit', payload)
                        await _broadcast_user_channel_message(uid, 'execute-orders', hit_msg)
                        print(f'[BROADCAST] execute_order SL/TGT user={uid} trades={len(records)}')
            except Exception as exc:
                log.warning('[LiveMonitor] hit broadcast error: %s', exc)

    # ── Time helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _today_ist() -> str:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        total_minutes = now.hour * 60 + now.minute + 330
        extra_days    = total_minutes // 1440
        from datetime import timedelta
        d = now.date() + timedelta(days=extra_days)
        return d.strftime('%Y-%m-%d')

    @staticmethod
    def _now_iso() -> str:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        total_minutes = now.hour * 60 + now.minute + 330
        extra_days    = total_minutes // 1440
        from datetime import timedelta
        base = now + timedelta(hours=5, minutes=30)
        return base.strftime('%Y-%m-%dT%H:%M:%SZ')

    @staticmethod
    def _now_ist_str() -> str:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        total_minutes = (now.hour * 60 + now.minute + 330) % 1440
        return f'{total_minutes // 60:02d}:{total_minutes % 60:02d}'


# ─── Module-level singleton ────────────────────────────────────────────────────

_service: LiveMonitorService | None = None


def get_service() -> LiveMonitorService:
    global _service
    if _service is None:
        _service = LiveMonitorService()
    return _service


def start(loop: asyncio.AbstractEventLoop) -> None:
    get_service().start(loop)


def stop() -> None:
    if _service:
        _service.stop()


def attach_kite_listener() -> None:
    if _service:
        _service.attach_kite_listener()
