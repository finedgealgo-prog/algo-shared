"""
async_entry_engine.py
──────────────────────
Phase 1 of the async entry-processing migration (see
/home/ashok-innoppl/.claude/plans/golden-dazzling-garden.md for the full
phased plan). Async twin of execution_socket._process_momentum_pending_feature_legs
and _store_position_history — scoped to activation_mode == 'fast-forward'
ONLY. Live and forward-test modes are untouched and keep using the existing
synchronous path (live_tick_dispatcher.py's ThreadPoolExecutor).

Why fast-forward only: confirmed by reading the sync function in full —
for fast-forward, the `if activation_mode == 'live':` branches (real broker
order placement) are never reached. Fast-forward's "entry" is purely
simulated Mongo writes, so this migration needs zero broker HTTP work.

Design
──────
- Runs on its own dedicated asyncio event loop, in its own background
  thread — deliberately NOT uvicorn's main loop (would compete with API
  request handling) and NOT CentralTickClient's own loop (would delay tick
  ingestion, the exact head-of-line-blocking problem this migration exists
  to remove). See _get_loop().
- Uses shared/features/async_mongo.py's motor client for all DB I/O in this
  module — a separate client from the pymongo MongoData used everywhere
  else in the codebase (which is far too widely used, 85+ files, to
  migrate wholesale).
- Real blocking I/O that isn't worth reimplementing (broker-backed chain
  fetch, Telegram notifications, the simulator-order recorder, the
  display-text-only leg-feature audit row) is dispatched via
  `asyncio.to_thread(...)`, calling the existing, unmodified, already-
  tested sync functions — not reimplemented. Every such call site is
  commented with why.
- Every helper confirmed pure (no I/O) is imported and called directly,
  unchanged, from execution_socket.py / position_manager.py / notification_manager.py.

Entry point for the sync world: run_fast_forward_batch(...), called from
live_tick_dispatcher.py behind the ASYNC_FAST_FORWARD_ENTRY_ENABLED flag.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from features.async_mongo import get_async_db
from features.mongo_data import MongoData

log = logging.getLogger(__name__)

# ── pure helpers, imported and called directly (no I/O, no async needed) ──
from features.execution_socket import (  # noqa: E402
    PROCESSING_CLAIM_STALE_SECONDS,
    _build_pending_leg,
    _build_position_history_doc,
    _is_market_order,
    _is_sell,
    _normalize_expiry_datetime,
    _resolve_leg_cfg,
    _resolve_simple_momentum_target,
    _resolve_trade_leg_configs,
    _is_simple_momentum_triggered,
    _safe_float,
    mark_execute_order_dirty_from_trade,
)
from features.debug_flags import entry_print, runtime_print, trade_event_print  # noqa: E402
from features.notification_manager import (  # noqa: E402
    LEG_FEATURE_STATUS_COLLECTION,
    NOTIFICATION_COLLECTION,
    _base as _notif_base,
    _build_what_happened,
    _safe_float as _notif_safe_float,
)
from features.position_manager import calc_sl_price, parse_overall_sl, parse_overall_tgt  # noqa: E402


def _env_flag_enabled(name: str, default: bool = False) -> bool:
    """Same convention as live_order_manager.py / telegram_notifier.py — read
    fresh each call (not cached at import time) so it's toggleable without a
    process restart via anything that can set os.environ at runtime."""
    raw = str(os.getenv(name, '')).strip().lower()
    if not raw:
        return default
    return raw in {'1', 'true', 'yes', 'on'}


def async_fast_forward_enabled() -> bool:
    return _env_flag_enabled('ASYNC_FAST_FORWARD_ENTRY_ENABLED', default=False)


# ── dedicated event loop ───────────────────────────────────────────────────

_loop: asyncio.AbstractEventLoop | None = None
_loop_lock = threading.Lock()
_batch_semaphore: asyncio.Semaphore | None = None
_BATCH_CONCURRENCY = 90  # mirrors live_tick_dispatcher._MOMENTUM_BATCH_MAX_WORKERS


def _get_loop() -> asyncio.AbstractEventLoop:
    global _loop
    if _loop is None:
        with _loop_lock:
            if _loop is None:
                new_loop = asyncio.new_event_loop()

                def _run() -> None:
                    asyncio.set_event_loop(new_loop)
                    new_loop.run_forever()

                threading.Thread(target=_run, daemon=True, name='async_entry_engine_loop').start()
                _loop = new_loop
    return _loop


def _get_semaphore() -> asyncio.Semaphore:
    global _batch_semaphore
    if _batch_semaphore is None:
        _batch_semaphore = asyncio.Semaphore(_BATCH_CONCURRENCY)
    return _batch_semaphore


def run_fast_forward_batch(
    trade_ids: list[str],
    trade_date: str,
    now_ts: str,
    ltp_map: dict,
    spot_map: dict,
    timeout_seconds: float = 25.0,
) -> None:
    """
    Sync-callable entry point — invoked from live_tick_dispatcher.py's
    fast-forward worker thread. Schedules the whole batch as one coroutine
    on the dedicated loop and blocks the calling thread until it finishes,
    preserving the same "batch done before returning" contract the existing
    ThreadPoolExecutor path has today (see live_tick_dispatcher.py:264-274).
    """
    if not trade_ids:
        return
    loop = _get_loop()
    future = asyncio.run_coroutine_threadsafe(
        _run_batch(trade_ids, trade_date, now_ts, ltp_map, spot_map),
        loop,
    )
    try:
        future.result(timeout=timeout_seconds)
    except Exception as exc:
        log.error('async fast-forward batch error (mode=fast-forward): %s', exc)


async def _run_batch(
    trade_ids: list[str],
    trade_date: str,
    now_ts: str,
    ltp_map: dict,
    spot_map: dict,
) -> None:
    sem = _get_semaphore()

    async def _bounded(trade_id: str) -> None:
        async with sem:
            await _async_process_one_trade_ff(trade_id, trade_date, now_ts, ltp_map, spot_map)

    await asyncio.gather(*[_bounded(tid) for tid in trade_ids], return_exceptions=False)


async def _async_process_one_trade_ff(
    trade_id: str,
    trade_date: str,
    now_ts: str,
    ltp_map: dict,
    spot_map: dict,
) -> None:
    """Async twin of live_tick_dispatcher._process_one_trade_momentum, fast-forward only."""
    adb = get_async_db()
    try:
        trade = await adb['algo_trades'].find_one({'_id': trade_id})
        if not trade:
            return
        underlying = str(
            (trade.get('config') or {}).get('Ticker')
            or trade.get('ticker') or ''
        ).strip().upper()
        try:
            # get_lot_size is a MongoData method (sync, pymongo) — cheap, infrequent
            # per-trade lookup, not worth a motor twin for one call per trade per tick.
            lot_size = await asyncio.to_thread(MongoData().get_lot_size, trade_date, underlying)
        except Exception:
            lot_size = 75

        index_spot_doc: dict = {}
        kite_spot = float(spot_map.get(underlying) or 0)
        if kite_spot > 0:
            index_spot_doc = {
                'underlying': underlying,
                'spot_price': kite_spot,
                'timestamp': now_ts,
                'source': 'kite_live',
            }

        entered_ids = await _async_process_momentum_pending_feature_legs_ff(
            adb, trade, trade_date, now_ts, lot_size,
            index_spot_doc=index_spot_doc or None,
            ltp_map=ltp_map,
        )
        if entered_ids:
            trade = await adb['algo_trades'].find_one({'_id': trade_id}) or trade
            mark_execute_order_dirty_from_trade(trade)
            print(f'[ASYNC MOMENTUM ENTERED] trade_id={trade_id} mode=fast-forward legs={entered_ids}')
    except Exception as exc:
        log.error('async momentum batch error trade=%s mode=fast-forward: %s', trade_id, exc)


# ── async twins of the DB-touching helpers ─────────────────────────────────

async def _async_record_entry_blocked(
    adb, trade: dict, leg_id: str, reason: str, message: str, timestamp: str,
    option_type: str = '', expiry_kind: str = '', strike_parameter: Any = None,
) -> None:
    strategy_id = str(trade.get('strategy_id') or '')
    trade_id = str(trade.get('_id') or '')
    ticker = str(
        (trade.get('config') or {}).get('Ticker')
        or (trade.get('strategy') or {}).get('Ticker')
        or trade.get('ticker') or ''
    )
    strategy_name = str(trade.get('name') or '')
    doc = _notif_base(strategy_id, trade_id, 'entry_blocked', timestamp, strategy_name, ticker, leg_id=leg_id)
    doc['data'] = {
        'reason': reason, 'message': message, 'option_type': option_type,
        'expiry_kind': expiry_kind, 'strike_parameter': strike_parameter,
    }
    try:
        doc['what_happened'] = _build_what_happened('entry_blocked', doc['data'])
        await adb[NOTIFICATION_COLLECTION].insert_one(doc)
    except Exception as exc:
        log.error('async record_entry_blocked insert error: %s', exc)


async def _async_record_live_entry_blocked(
    adb, trade: dict, leg_id: str, reason: str, message: str, now_ts: str,
    option_type: str = '', expiry_kind: str = '', strike_parameter: Any = None,
) -> None:
    """Async twin of execution_socket._record_live_entry_blocked."""
    trade_id = str(trade.get('_id') or '')
    try:
        await adb['algo_trades'].update_one(
            {'_id': trade_id},
            {'$set': {'entry_error': {'leg_id': leg_id, 'reason': reason, 'message': message, 'at': now_ts}}},
        )
    except Exception as exc:
        log.warning('async live entry-blocked write error trade=%s leg=%s: %s', trade_id, leg_id, exc)

    try:
        mark_execute_order_dirty_from_trade(trade)
    except Exception:
        pass

    try:
        await _async_record_entry_blocked(
            adb, trade, leg_id, reason, message, now_ts,
            option_type=option_type, expiry_kind=expiry_kind, strike_parameter=strike_parameter,
        )
    except Exception as exc:
        log.warning('async record_entry_blocked error trade=%s leg=%s: %s', trade_id, leg_id, exc)

    # Telegram — error path only, infrequent; not worth a raw-httpx reimplementation.
    try:
        from features.telegram_notifier import notify_admin, notify_user_for

        async def _notify() -> None:
            _ctx = {'trade_id': trade_id, 'leg_id': leg_id, 'reason': reason}
            await asyncio.to_thread(notify_admin, 'entry_blocked', f'Entry blocked for leg {leg_id}: {message or reason}', _ctx)
            _uid = str(trade.get('user_id') or '').strip()
            if _uid:
                await asyncio.to_thread(
                    notify_user_for, _uid, 'entry_blocked',
                    f'Your strategy could not take entry for leg {leg_id}: {message or reason}', _ctx,
                )
        await _notify()
    except Exception as exc:
        log.warning('async entry-blocked telegram notify error trade=%s leg=%s: %s', trade_id, leg_id, exc)


async def _async_get_active_feed_broker(adb) -> str:
    try:
        cfg = await adb['kite_market_config'].find_one({'enabled': True}, {'broker': 1}) or {}
        return str(cfg.get('broker') or 'kite').strip().lower()
    except Exception:
        return 'kite'


async def _async_resolve_expiry_from_tokens(adb, underlying: str, opt_norm: str, trade_date: str, expiry_kind: str, broker_filter: dict | None = None) -> str:
    base_q: dict = {**(broker_filter or {}), 'instrument': underlying, 'option_type': opt_norm, 'expiry': {'$gte': trade_date}}
    raw_expiries = await adb['active_option_tokens'].distinct('expiry', base_q)
    expiries = sorted(str(e)[:10] for e in raw_expiries if e)
    if not expiries:
        return ''
    kind = str(expiry_kind or 'ExpiryType.Weekly')
    if 'Monthly' in kind:
        monthly: list[str] = []
        from itertools import groupby
        for _month_key, group in groupby(expiries, key=lambda d: d[:7]):
            monthly.append(list(group)[-1])
        if not monthly:
            return ''
        return (monthly[1] if len(monthly) > 1 else monthly[0]) if 'NextMonthly' in kind else monthly[0]
    if 'NextWeekly' in kind:
        return expiries[1] if len(expiries) > 1 else expiries[0]
    return expiries[0]


async def _async_resolve_feature_leg_id(adb, trade_id: str, leg_id: str) -> str:
    resolved_trade_id = str(trade_id or '').strip()
    resolved_leg_id = str(leg_id or '').strip()
    if not resolved_trade_id or not resolved_leg_id:
        return resolved_leg_id
    try:
        existing_feature = await adb[LEG_FEATURE_STATUS_COLLECTION].find_one(
            {'trade_id': resolved_trade_id, 'leg_id': resolved_leg_id}, {'_id': 1},
        )
        if existing_feature:
            return resolved_leg_id
        history_doc = await adb['algo_trade_positions_history'].find_one(
            {
                'trade_id': resolved_trade_id,
                '$or': [{'_id': resolved_leg_id}, {'leg_id': resolved_leg_id, 'exit_trade': None}],
            },
            {'_id': 1},
        )
        if history_doc and history_doc.get('_id') is not None:
            return str(history_doc.get('_id'))
    except Exception:
        pass
    return resolved_leg_id


async def _async_record_entry_taken(
    adb, trade: dict, leg: dict, leg_cfg: dict, timestamp: str,
    overall_sl_type: str = 'None', overall_sl_value: float = 0.0,
    overall_tgt_type: str = 'None', overall_tgt_value: float = 0.0,
) -> None:
    """Async twin of notification_manager.record_entry_taken."""
    strategy_id = str(trade.get('strategy_id') or '')
    trade_id = str(trade.get('_id') or '')
    ticker = str(
        (trade.get('config') or {}).get('Ticker')
        or (trade.get('strategy') or {}).get('Ticker')
        or trade.get('ticker') or ''
    )
    strategy_name = str(trade.get('name') or '')
    entry_trade = leg.get('entry_trade') or {}
    sl_config = leg_cfg.get('LegStopLoss') or {}
    tp_config = leg_cfg.get('LegTarget') or {}
    trail_cfg = leg_cfg.get('LegTrailSL') or {}
    entry_price = _notif_safe_float(entry_trade.get('price'))
    sl_value = _notif_safe_float(sl_config.get('Value'))
    tp_value = _notif_safe_float(tp_config.get('Value'))
    is_sell = 'sell' in str(leg.get('position') or '').lower()
    from features.position_manager import calc_sl_price as _csl, calc_tp_price as _ctp
    sl_price = _csl(entry_price, is_sell, sl_config)
    tp_price = _ctp(entry_price, is_sell, tp_config)

    doc = _notif_base(strategy_id, trade_id, 'entry_taken', timestamp, strategy_name, ticker, leg_id=str(leg.get('id') or ''))
    doc['data'] = {
        'strike': leg.get('strike'), 'option_type': str(leg.get('option') or ''),
        'expiry': str(leg.get('expiry_date') or ''), 'position': str(leg.get('position') or ''),
        'entry_kind': str(leg.get('entry_kind') or ''), 'strike_parameter': leg.get('strike_parameter'),
        'entry_price': entry_price,
        'spot_at_entry': _notif_safe_float(entry_trade.get('underlying_at_trade') or entry_trade.get('underlying_trigger_price')),
        'momentum_type': str((leg_cfg.get('LegMomentum') or {}).get('Type') or 'None'),
        'momentum_value': _notif_safe_float((leg_cfg.get('LegMomentum') or {}).get('Value')),
        'momentum_base_price': _notif_safe_float(leg.get('momentum_base_price')),
        'momentum_target_price': _notif_safe_float(leg.get('momentum_target_price')),
        'sl_price': sl_price, 'sl_type': str(sl_config.get('Type') or 'None'), 'sl_value': sl_value,
        'tp_price': tp_price, 'tp_type': str(tp_config.get('Type') or 'None'), 'tp_value': tp_value,
        'trail_sl_type': str(trail_cfg.get('Type') or 'None'),
        'trail_instrument_move': _notif_safe_float((trail_cfg.get('Value') or {}).get('InstrumentMove')),
        'trail_sl_move': _notif_safe_float((trail_cfg.get('Value') or {}).get('StopLossMove')),
        'overall_sl_type': overall_sl_type, 'overall_sl_value': overall_sl_value,
        'overall_tgt_type': overall_tgt_type, 'overall_tgt_value': overall_tgt_value,
    }
    try:
        doc['what_happened'] = _build_what_happened('entry_taken', doc['data'])
        await adb[NOTIFICATION_COLLECTION].insert_one(doc)
    except Exception as exc:
        log.error('async record_entry_taken insert error: %s', exc)


async def _async_upsert_simple_momentum_feature_status(
    adb, trade: dict, leg: dict, leg_cfg: dict, timestamp: str,
    base_price: float, target_price: float, current_price: float | None = None,
    status: str = 'pending', enabled: bool = True,
) -> None:
    trade_id = str(trade.get('_id') or '')
    leg_id = await _async_resolve_feature_leg_id(adb, trade_id, str(leg.get('_id') or leg.get('id') or ''))
    if not trade_id or not leg_id:
        return
    momentum_cfg = leg_cfg.get('LegMomentum') or {}
    momentum_type = str(momentum_cfg.get('Type') or 'None')
    momentum_value = _notif_safe_float(momentum_cfg.get('Value'))
    current_value = _notif_safe_float(current_price if current_price is not None else leg.get('last_saw_price'))
    from features.notification_manager import _format_rupee as _fmt_rupee, _lfs_now
    description = (
        f"Simple Momentum active: {momentum_type} {momentum_value}. "
        f"Base price: {_fmt_rupee(base_price)}. "
        f"Trigger price: {_fmt_rupee(target_price)}. "
        f"Current price: {_fmt_rupee(current_value)}."
    )
    now = timestamp or _lfs_now()
    try:
        await adb[LEG_FEATURE_STATUS_COLLECTION].update_one(
            {'trade_id': trade_id, 'leg_id': leg_id, 'feature': 'simpleMomentum'},
            {'$set': {
                'trade_id': trade_id, 'leg_id': leg_id, 'feature': 'simpleMomentum',
                'status': status, 'enabled': enabled, 'description': description,
                'momentum_type': momentum_type, 'momentum_value': momentum_value,
                'base_price': round(base_price, 2), 'target_price': round(target_price, 2),
                'current_price': round(current_value, 2), 'updated_at': now,
            }, '$setOnInsert': {'created_at': now}},
            upsert=True,
        )
    except Exception as exc:
        log.warning('async upsert_simple_momentum_feature_status error leg=%s: %s', leg_id, exc)


async def _async_store_position_history(adb, trade: dict, leg: dict, override_leg_cfg: dict | None = None) -> tuple[bool, dict | None]:
    """Async twin of execution_socket._store_position_history."""
    # Dhan-only branch: resolve security ID + entry IV. Ported because Dhan is
    # confirmed the active broker in this deployment (checked kite_market_config
    # directly this session) — this is not a rare/skippable path.
    try:
        active_broker = await _async_get_active_feed_broker(adb)
        if active_broker == 'dhan':
            underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '').strip().upper()
            strike_raw = leg.get('strike')
            expiry = str(leg.get('expiry_date') or '')[:10]
            opt = str(leg.get('option') or '').upper()
            try:
                s_int = int(float(str(strike_raw))) if strike_raw is not None else None
            except Exception:
                s_int = None
            strike_filter = {'$in': [s_int, float(s_int)]} if s_int is not None else strike_raw
            at_doc = await adb['active_option_tokens'].find_one(
                {
                    'broker': 'dhan', 'instrument': {'$regex': f'^{underlying}$', '$options': 'i'},
                    'strike': strike_filter, 'option_type': {'$regex': f'^{opt}$', '$options': 'i'},
                    'expiry': {'$regex': f'^{expiry}'},
                },
                {'token': 1, 'tokens': 1, 'ws_segment': 1},
            )
            if at_doc:
                dhan_tok = str(at_doc.get('token') or at_doc.get('tokens') or '').strip()
                dhan_seg = str(at_doc.get('ws_segment') or 'NSE_FNO')
                if dhan_tok:
                    leg = dict(leg)
                    leg['token'] = dhan_tok
                    leg['ws_segment'] = dhan_seg
                    try:
                        from features.dhan_ticker import dhan_ticker_manager as _dtm
                        await asyncio.to_thread(_dtm.subscribe_tokens, [dhan_tok], exchange=dhan_seg)
                    except Exception as sub_e:
                        print(f'[ASYNC STORE_HIST_TOKEN] subscribe error: {sub_e}')
                    try:
                        from features.live_option_chain import _bs
                        from features.broker_gateway import broker_ticker_manager as _btm
                        et = leg.get('entry_trade') or {}
                        iv_ltp = _safe_float(et.get('price') or et.get('trigger_price'))
                        iv_spot = (
                            _safe_float(_btm.spot_map.get(underlying))
                            or _safe_float(et.get('underlying_at_trade'))
                            or _safe_float(et.get('underlying_trigger_price'))
                        )
                        if iv_ltp > 0 and iv_spot > 0 and strike_raw is not None:
                            iv_calc, _, tte, _, rfr, div_yields, def_div = _bs()
                            iv_T = tte(expiry)
                            iv_q = div_yields.get(underlying, def_div)
                            iv_raw = iv_calc(iv_ltp, iv_spot, float(strike_raw), iv_T, rfr, opt, iv_q)
                            entry_iv_val = round(iv_raw * 100, 2)
                            if entry_iv_val > 0:
                                leg['entry_iv'] = entry_iv_val
                    except Exception as iv_e:
                        print(f'[ASYNC ENTRY_IV_DEBUG] error: {iv_e}')
    except Exception:
        pass

    history_doc = _build_position_history_doc(trade, leg)
    if not history_doc:
        return False, None
    query = {'trade_id': history_doc['trade_id'], 'leg_id': history_doc['leg_id'], 'entry_timestamp': history_doc['entry_timestamp']}
    history_col = adb['algo_trade_positions_history']
    existing = await history_col.find_one(query, {'_id': 1})
    if existing:
        return False, history_doc
    result = await history_col.insert_one(history_doc)
    inserted_id = str(result.inserted_id)
    history_doc['_id'] = inserted_id
    history_doc['id'] = inserted_id
    try:
        await history_col.update_one({'_id': result.inserted_id}, {'$set': {'id': inserted_id}})
    except Exception as exc:
        log.error('async _store_position_history id sync error trade=%s leg=%s: %s', history_doc['trade_id'], history_doc['leg_id'], exc)

    leg_id_val = history_doc.get('leg_id', '')
    trade_id_val = history_doc['trade_id']
    try:
        await adb['algo_trades'].update_one({'_id': trade_id_val}, {'$pull': {'legs': {'id': leg_id_val}}})
    except Exception as exc:
        log.error('async _store_position_history legs pull error trade=%s leg=%s: %s', trade_id_val, leg_id_val, exc)
    try:
        current_trade = await adb['algo_trades'].find_one({'_id': trade_id_val}, {'legs': 1, 'activation_mode': 1}) or {}
        if str(current_trade.get('activation_mode') or '').strip() in {'live', 'fast-forward', 'forward-test'}:
            clean_legs = [l for l in (current_trade.get('legs') or []) if isinstance(l, str)]
            await adb['algo_trades'].update_one({'_id': trade_id_val}, {'$set': {'legs': clean_legs}})
    except Exception as exc:
        log.error('async _store_position_history legs clean error trade=%s: %s', trade_id_val, exc)
    try:
        await adb['algo_trades'].update_one({'_id': trade_id_val}, {'$push': {'legs': inserted_id}})
    except Exception as exc:
        log.error('async _store_position_history legs push error trade=%s: %s', trade_id_val, exc)

    resolved_leg_cfg: dict = override_leg_cfg or {}
    if not resolved_leg_cfg:
        try:
            all_leg_cfgs = _resolve_trade_leg_configs(trade)
            resolved_leg_cfg = _resolve_leg_cfg(str(leg.get('id') or ''), leg, all_leg_cfgs)
        except Exception as exc:
            log.warning('async _store_position_history leg_cfg lookup error: %s', exc)

    try:
        strategy_cfg = trade.get('strategy') or trade.get('config') or {}
        osl_type, osl_val = parse_overall_sl(strategy_cfg)
        otgt_type, otgt_val = parse_overall_tgt(strategy_cfg)
        await _async_record_entry_taken(
            adb, trade, leg, resolved_leg_cfg,
            timestamp=history_doc.get('entry_timestamp') or inserted_id,
            overall_sl_type=osl_type, overall_sl_value=osl_val,
            overall_tgt_type=otgt_type, overall_tgt_value=otgt_val,
        )
    except Exception as ne:
        log.warning('async notification entry_taken error: %s', ne)

    # record_leg_features_at_entry: NOT ported. Its _build_leg_entry_description
    # sub-path does variable-depth extra Mongo reads purely for a display/audit
    # string (doesn't gate correctness) — dispatched via to_thread rather than
    # porting, per the migration plan's explicit scoping decision.
    try:
        from features.notification_manager import record_leg_features_at_entry
        feature_leg_id = inserted_id
        if not bool(leg.get('is_lazy')):
            feature_leg_id = str(history_doc.get('leg_id') or leg.get('id') or inserted_id)
        await asyncio.to_thread(
            record_leg_features_at_entry, MongoData()._db, trade, leg, resolved_leg_cfg,
            timestamp=history_doc.get('entry_timestamp') or inserted_id, feature_leg_id=feature_leg_id,
        )
    except Exception as lfe:
        log.warning('async leg_feature_status entry error: %s', lfe)

    try:
        momentum_cfg = resolved_leg_cfg.get('LegMomentum') or {}
        momentum_type = str(momentum_cfg.get('Type') or 'None')
        momentum_value = _safe_float(momentum_cfg.get('Value'))
        momentum_base_price = _safe_float(leg.get('momentum_base_price'))
        momentum_target_price = _safe_float(leg.get('momentum_target_price'))
        if ('None' not in momentum_type and momentum_type and momentum_value > 0
                and momentum_base_price > 0 and momentum_target_price > 0):
            feature_leg = dict(leg)
            feature_leg['_id'] = inserted_id
            await _async_upsert_simple_momentum_feature_status(
                adb, trade, feature_leg, resolved_leg_cfg,
                timestamp=history_doc.get('entry_timestamp') or inserted_id,
                base_price=momentum_base_price, target_price=momentum_target_price,
                current_price=_safe_float((leg.get('entry_trade') or {}).get('price')),
                status='triggered' if str(leg.get('momentum_triggered_at') or '').strip() else 'pending',
                enabled=False if str(leg.get('momentum_triggered_at') or '').strip() else True,
            )
    except Exception as mfe:
        log.warning('async simple_momentum feature seed error: %s', mfe)

    mark_execute_order_dirty_from_trade(trade)

    # Simulator-order recording: NOT ported (real-money-adjacent order-record
    # logic with its own dedup/insert sequence) — dispatched via to_thread.
    try:
        from features.live_simulator_order import is_simulator_order_enabled, record_entry_with_orders
        if is_simulator_order_enabled(trade):
            await asyncio.to_thread(record_entry_with_orders, MongoData(), trade, leg, resolved_leg_cfg)
    except Exception as soe:
        log.warning('async simulator_order record error trade=%s leg=%s: %s', history_doc.get('trade_id'), history_doc.get('leg_id'), soe)

    return True, history_doc


async def _async_process_momentum_pending_feature_legs_ff(
    adb, trade: dict, trade_date: str, now_ts: str, lot_size: int,
    index_spot_doc: dict | None = None, ltp_map: dict | None = None,
) -> list[str]:
    """
    Async twin of execution_socket._process_momentum_pending_feature_legs,
    scoped to activation_mode == 'fast-forward' only (the live-only and
    algo-backtest branches of the original function are intentionally
    absent here — they don't apply).
    """
    trade_id = str(trade.get('_id') or '')
    entered_ids: list[str] = []

    if trade.get('is_paused'):
        return entered_ids

    raw_et_early = str((trade.get('config') or {}).get('entry_time') or trade.get('entry_time') or '').strip()
    et_hhmm = raw_et_early[11:16] if len(raw_et_early) >= 16 else raw_et_early[:5]
    now_hhmm_early = now_ts[11:16] if len(now_ts) >= 16 else ''
    if et_hhmm and now_hhmm_early and now_hhmm_early < et_hhmm:
        return entered_ids

    feature_col = adb['algo_leg_feature_status']

    try:
        stale_before_utc = (datetime.now(timezone.utc) - timedelta(seconds=PROCESSING_CLAIM_STALE_SECONDS)).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')
        await feature_col.update_many(
            {
                'trade_id': trade_id, 'feature': {'$in': ['momentum_pending', 'pending_entry']}, 'status': 'processing',
                '$or': [{'processing_started_at': None}, {'processing_started_at': {'$exists': False}}, {'processing_started_at': {'$lt': stale_before_utc}}],
            },
            {'$set': {'status': 'active', 'processing_started_at': None}},
        )
    except Exception as exc:
        print(f'[ASYNC PENDING FEATURE FLOW] trade_id={trade_id} state=processing_rows_reactivate_error error={exc}')

    try:
        active_docs = await feature_col.find({
            'trade_id': trade_id, 'feature': {'$in': ['momentum_pending', 'pending_entry']}, 'status': 'active',
        }).to_list(length=None)
    except Exception as exc:
        log.warning('async _process_momentum_pending_feature_legs fetch error trade=%s: %s', trade_id, exc)
        return entered_ids
    if not active_docs:
        return entered_ids

    deduped_active_docs: list[dict] = []
    seen_feature_keys: set[tuple[str, str]] = set()
    duplicate_row_ids: list[Any] = []
    for feat_doc in active_docs:
        feature_name = str(feat_doc.get('feature') or '').strip()
        leg_id_key = str(feat_doc.get('leg_id') or '').strip()
        dedupe_key = (feature_name, leg_id_key)
        if not feature_name or not leg_id_key:
            deduped_active_docs.append(feat_doc)
            continue
        if dedupe_key in seen_feature_keys:
            duplicate_row_ids.append(feat_doc.get('_id'))
            continue
        seen_feature_keys.add(dedupe_key)
        deduped_active_docs.append(feat_doc)
    if duplicate_row_ids:
        try:
            await feature_col.update_many(
                {'_id': {'$in': [rid for rid in duplicate_row_ids if rid is not None]}},
                {'$set': {'status': 'disabled', 'enabled': False, 'disabled_reason': 'duplicate_queue_cleanup', 'updated_at': now_ts, 'disabled_at': now_ts}},
            )
        except Exception as exc:
            log.warning('async duplicate cleanup error trade=%s: %s', trade_id, exc)
    active_docs = deduped_active_docs

    underlying = str((trade.get('strategy') or {}).get('Ticker') or (trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
    if not underlying:
        return entered_ids

    live_chain_cache: dict[tuple, dict] = {}

    index_spot_doc = index_spot_doc or {}
    spot_price = _safe_float(index_spot_doc.get('spot_price'))
    if spot_price <= 0:
        try:
            from features.live_monitor_socket import _get_active_ticker_manager
            spot_price = _safe_float(_get_active_ticker_manager().get_spot(underlying))
        except Exception:
            pass
    if spot_price <= 0:
        try:
            from features.fast_forward_event import QUOTE_INSTRUMENT_BY_UNDERLYING
            from features.broker_gateway import get_broker_rest_client
            spot_instr = QUOTE_INSTRUMENT_BY_UNDERLYING.get(underlying.upper(), '')
            if spot_instr:
                kite = await asyncio.to_thread(get_broker_rest_client, MongoData())
                if kite:
                    q = (await asyncio.to_thread(kite.quote, [spot_instr]) or {}).get(spot_instr) or {}
                    qp = _safe_float(q.get('last_price'))
                    if qp > 0:
                        spot_price = qp
        except Exception as exc:
            log.warning('[ASYNC SPOT QUOTE] fallback error underlying=%s: %s', underlying, exc)

    for feat_doc in active_docs:
        doc_id = feat_doc.get('_id')
        claimed = await feature_col.find_one_and_update(
            {'_id': doc_id, 'status': 'active'},
            {'$set': {'status': 'processing', 'processing_started_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')}},
        )
        if claimed is None:
            continue

        leg_id = str(feat_doc.get('leg_id') or '')
        lazy_leg_ref = str(feat_doc.get('lazy_leg_ref') or leg_id or '')
        option_type = str(feat_doc.get('option') or 'CE')
        expiry_kind = str(feat_doc.get('expiry_kind') or 'ExpiryType.Weekly')
        entry_kind = str(feat_doc.get('entry_kind') or '')
        strike_param_raw = feat_doc.get('strike_parameter')
        position_str = str(feat_doc.get('position') or 'PositionType.Sell')
        lot_config_value = max(1, int(feat_doc.get('lot_config_value') or 1))
        momentum_type = str(feat_doc.get('momentum_type') or 'None')
        momentum_value = _safe_float(feat_doc.get('momentum_value'))
        triggered_by = str(feat_doc.get('triggered_by') or '')
        leg_type_str = str(feat_doc.get('leg_type') or '')
        feat_feature = str(feat_doc.get('feature') or '').strip()

        if feat_feature == 'pending_entry' and not triggered_by:
            raw_et = str((trade.get('config') or {}).get('entry_time') or trade.get('entry_time') or '').strip()
            entry_hhmm = raw_et[11:16] if len(raw_et) >= 16 else raw_et[:5]
            now_hhmm = now_ts[11:16] if len(now_ts) >= 16 else ''
            if entry_hhmm and now_hhmm and now_hhmm < entry_hhmm:
                await feature_col.update_one({'_id': doc_id, 'status': 'processing'}, {'$set': {'status': 'active', 'processing_started_at': None}})
                continue

        expiry = str(feat_doc.get('expiry_date') or '')
        strike = feat_doc.get('strike')
        token = str(feat_doc.get('token') or '')
        symbol = str(feat_doc.get('symbol') or '')
        base_price = _safe_float(feat_doc.get('momentum_base_price'))
        target_price = _safe_float(feat_doc.get('momentum_target_price'))
        needs_db_write = False
        strike_meta: dict = {}
        lazy_chain_iv: float | None = None
        chain_ltp: float = 0.0

        strike_locked = bool(strike not in (None, '') and token)
        ws_ltp_for_locked = _safe_float((ltp_map or {}).get(str(token))) if (strike_locked and token) else 0.0
        if strike_locked and ws_ltp_for_locked > 0:
            chain_ltp = ws_ltp_for_locked
        else:
            try:
                from features.live_option_chain import fetch_full_chain, select_strike_live
                opt_norm = option_type.upper()

                expiry_broker_filter: dict = {}
                try:
                    expiry_broker_filter = {'broker': await _async_get_active_feed_broker(adb)}
                except Exception:
                    pass
                live_expiry = await _async_resolve_expiry_from_tokens(adb, underlying, opt_norm, trade_date, expiry_kind, broker_filter=expiry_broker_filter)
                if not live_expiry:
                    entry_print(f'[ASYNC MOMENTUM PENDING] leg={leg_id} no expiry in active_option_tokens — skipping')
                    await _async_record_live_entry_blocked(
                        adb, trade, leg_id, 'expiry_missing',
                        f'No {expiry_kind} expiry found in active_option_tokens for {underlying} {opt_norm}.',
                        now_ts, option_type=opt_norm, expiry_kind=expiry_kind, strike_parameter=strike_param_raw,
                    )
                    continue

                cache_key = (underlying, live_expiry)
                if cache_key not in live_chain_cache:
                    # fetch_full_chain does broker REST + chain-wide Black-Scholes
                    # greeks; already has its own 2s TTL + single-flight lock
                    # (live_option_chain.py) so this is cheap on cache hits even
                    # dispatched via a thread.
                    live_chain_cache[cache_key] = await asyncio.to_thread(
                        fetch_full_chain, MongoData(), underlying, live_expiry, spot_price, leg_id=leg_id,
                    )
                chain = live_chain_cache[cache_key]
                if not chain.get('CE') and not chain.get('PE'):
                    entry_print(f'[ASYNC MOMENTUM PENDING] leg={leg_id} empty chain {underlying} {live_expiry} — skipping')
                    await _async_record_live_entry_blocked(
                        adb, trade, leg_id, 'chain_empty',
                        f'Option chain for {underlying} {live_expiry} came back empty (no CE/PE rows).',
                        now_ts, option_type=opt_norm, expiry_kind=expiry_kind, strike_parameter=strike_param_raw,
                    )
                    continue

                if strike_locked:
                    opt_rows = chain.get(opt_norm) or []
                    locked_row = next((r for r in opt_rows if _safe_float(r.get('strike')) == _safe_float(strike)), None)
                    chain_ltp = _safe_float((locked_row or {}).get('ltp'))
                    if token and chain_ltp > 0 and ltp_map is not None:
                        ltp_map[token] = chain_ltp
                else:
                    sel = select_strike_live(chain, entry_kind, strike_param_raw, opt_norm, position_str, spot_price, underlying, leg_id=leg_id)
                    if not sel:
                        entry_print(f'[ASYNC MOMENTUM PENDING] leg={leg_id} no strike found — skipping')
                        await _async_record_live_entry_blocked(
                            adb, trade, leg_id, 'strike_missing',
                            f'Could not select a strike for {underlying} {live_expiry} {opt_norm} '
                            f'(entry_kind={entry_kind or "-"}, strike_parameter={strike_param_raw!r}).',
                            now_ts, option_type=opt_norm, expiry_kind=expiry_kind, strike_parameter=strike_param_raw,
                        )
                        continue
                    expiry = live_expiry
                    strike = sel['strike']
                    token = sel['token']
                    symbol = sel['symbol']
                    strike_meta = sel.get('meta') or {}
                    lazy_chain_iv = _safe_float(sel.get('iv')) or None
                    needs_db_write = True
                    chain_ltp = _safe_float(sel.get('ltp'))
                    if token and chain_ltp > 0 and ltp_map is not None:
                        ltp_map[token] = chain_ltp
            except Exception as exc:
                log.warning('async live chain resolve error leg=%s: %s', leg_id, exc)
                await _async_record_live_entry_blocked(
                    adb, trade, leg_id, 'resolve_exception',
                    f'Unexpected error resolving live chain/strike for {underlying} {option_type.upper()}: {exc}',
                    now_ts, option_type=option_type.upper(), expiry_kind=expiry_kind, strike_parameter=strike_param_raw,
                )
                continue

        if expiry and strike not in (None, ''):
            try:
                broker_filter: dict = {}
                try:
                    broker_filter = {'broker': await _async_get_active_feed_broker(adb)}
                except Exception:
                    pass
                tok_doc = await adb['active_option_tokens'].find_one({
                    **broker_filter, 'instrument': underlying, 'expiry': str(expiry)[:10],
                    'strike': strike, 'option_type': option_type.upper(),
                }) or {}
                broker_tok = str(tok_doc.get('token') or tok_doc.get('tokens') or '').strip()
                if broker_tok and broker_tok != token:
                    token = broker_tok
                    symbol = str(tok_doc.get('symbol') or symbol or broker_tok)
                    needs_db_write = True
            except Exception as kt_exc:
                log.warning('async broker token lookup error leg=%s: %s', leg_id, kt_exc)

        if needs_db_write:
            await feature_col.update_one({'_id': feat_doc['_id']}, {'$set': {'expiry_date': expiry, 'strike': strike, 'token': token, 'symbol': symbol}})

        if needs_db_write and token and str(token).isdigit():
            try:
                from features.fast_forward_event import _subscribe_option_token
                await asyncio.to_thread(_subscribe_option_token, token, symbol)
            except Exception as sub_exc:
                log.warning('async token subscribe error leg=%s: %s', leg_id, sub_exc)

        current_price: float = 0.0
        live_ltp = _safe_float((ltp_map or {}).get(token)) if token else 0.0
        if live_ltp > 0:
            current_price = live_ltp
        elif chain_ltp > 0:
            current_price = chain_ltp
        else:
            print(f'[ASYNC LIVE PRICE MISSING] leg={leg_id} token={token} underlying={underlying} strike={strike} option={option_type} — no live LTP, retry next tick')
            await feature_col.update_one({'_id': feat_doc['_id'], 'status': 'processing'}, {'$set': {'status': 'active', 'processing_started_at': None}})
            continue

        is_instant_entry = str(feat_doc.get('feature') or '') == 'pending_entry'

        if is_instant_entry:
            if current_price <= 0:
                entry_print(f'[ASYNC PENDING ENTRY WAIT] leg={leg_id} waiting for price data')
                await feature_col.update_one({'_id': feat_doc['_id'], 'status': 'processing'}, {'$set': {'status': 'active', 'processing_started_at': None}})
                continue
            trade_event_print(f'[ASYNC PENDING ENTRY] leg={leg_id} current_price={current_price} strike={strike} option={option_type} — entering immediately')
        else:
            mom_check_price = spot_price if 'Underlying' in str(momentum_type or '') else current_price
            if base_price <= 0 or target_price <= 0:
                if mom_check_price <= 0:
                    entry_print(f'[ASYNC MOMENTUM PENDING] leg={leg_id} waiting for price data')
                    await feature_col.update_one({'_id': feat_doc['_id'], 'status': 'processing'}, {'$set': {'status': 'active', 'processing_started_at': None}})
                    continue
                base_price = mom_check_price
                target_price = _resolve_simple_momentum_target(base_price, momentum_type, momentum_value)
                await feature_col.update_one(
                    {'_id': feat_doc['_id']},
                    {'$set': {
                        'momentum_base_price': base_price, 'momentum_target_price': target_price,
                        'armed_at': now_ts, 'strike': strike, 'expiry_date': expiry, 'token': token,
                        'symbol': symbol, 'status': 'active', 'processing_started_at': None,
                    }},
                )
                print(f'[ASYNC MOMENTUM ARMED] leg={leg_id} type={momentum_type} value={momentum_value} base={base_price} target={target_price} strike={strike} option={option_type}')
                continue  # fast-forward never takes the activation_mode=='live' arm-time broker-queue branch

            if not _is_simple_momentum_triggered(mom_check_price, target_price, momentum_type):
                runtime_print(f'[ASYNC MOMENTUM WAIT] leg={leg_id} type={momentum_type} value={momentum_value} base={base_price} target={target_price} current={mom_check_price} strike={strike}')
                await feature_col.update_one({'_id': feat_doc['_id'], 'status': 'processing'}, {'$set': {'status': 'active', 'processing_started_at': None}})
                continue
            print(f'[ASYNC MOMENTUM OK] leg={leg_id} type={momentum_type} value={momentum_value} base={base_price} target={target_price} current={mom_check_price} — entering')

        is_sell_pos = _is_sell(position_str)
        all_leg_configs = _resolve_trade_leg_configs(trade)
        leg_cfg = all_leg_configs.get(lazy_leg_ref) or all_leg_configs.get(leg_id) or {}
        entry_price = current_price if (is_instant_entry or not target_price or _is_market_order(leg_cfg, 'entry')) else target_price
        sl_config = leg_cfg.get('LegStopLoss') or {}
        sl_price = calc_sl_price(entry_price, is_sell_pos, sl_config)
        actual_quantity = lot_config_value

        lazy_entry_vix: float | None = None
        try:
            from features.trading_core import get_vix_at_time as _get_vix
            # VIX resolution is once-per-entry (not per waiting tick) — cheap
            # enough to dispatch via to_thread rather than porting trading_core.
            vix_val = await asyncio.to_thread(_get_vix, MongoData(), now_ts, None)
            lazy_entry_vix = vix_val if vix_val > 0 else None
        except Exception:
            pass

        exchange_ts = now_ts.replace('T', ' ')[:19] if 'T' in now_ts else now_ts[:19]
        entry_trade_payload = {
            'trigger_timestamp': exchange_ts, 'trigger_price': entry_price,
            'underlying_trigger_price': spot_price, 'price': entry_price, 'quantity': actual_quantity,
            'underlying_at_trade': spot_price, 'traded_timestamp': exchange_ts, 'exchange_timestamp': exchange_ts,
            'strike_meta': strike_meta or {}, 'entry_iv': lazy_chain_iv, 'entry_vix': lazy_entry_vix,
            'entry_lifecycle_status': 'active',  # fast-forward: always active immediately (never 'order_open')
        }

        new_leg = _build_pending_leg(leg_id, leg_cfg or {
            'PositionType': position_str, 'InstrumentKind': f'LegType.{option_type}',
            'ExpiryKind': expiry_kind, 'StrikeParameter': strike_param_raw or 'StrikeType.ATM',
            'EntryType': entry_kind, 'LotConfig': {'Value': lot_config_value},
        }, trade, now_ts, triggered_by, leg_type=leg_type_str)

        broker_token = token
        kite_token = token
        if symbol:
            try:
                from features.live_event import resolve_kite_token_for_symbol
                kt = await asyncio.to_thread(resolve_kite_token_for_symbol, symbol)
                if kt:
                    kite_token = kt
            except Exception:
                pass

        new_leg.update({
            'strike': strike, 'expiry_date': _normalize_expiry_datetime(expiry), 'token': kite_token,
            'broker_token': broker_token, 'symbol': symbol, 'quantity': actual_quantity, 'lot_size': lot_size,
            'lot_config_value': lot_config_value, 'current_sl_price': sl_price, 'initial_sl_value': sl_price,
            'display_sl_value': sl_price, 'last_saw_price': entry_price, 'is_lazy': False,
            'lazy_leg_ref': lazy_leg_ref, 'momentum_base_price': base_price, 'momentum_target_price': target_price,
            'momentum_reference_set_at': str(feat_doc.get('armed_at') or feat_doc.get('queued_at') or now_ts),
            'momentum_triggered_at': now_ts, 'entry_trade': entry_trade_payload, 'exit_trade': None,
            'spot_at_queue': _safe_float(feat_doc.get('spot_at_queue')),
        })

        # fast-forward never places a real broker order — the sync function's
        # `if activation_mode == 'live':` order-placement block is intentionally
        # absent here (confirmed unreachable for this mode).

        try:
            await feature_col.update_one({'_id': feat_doc['_id']}, {'$set': {'status': 'triggered', 'triggered_at': now_ts}})
            entry_label = 'ASYNC PENDING ENTRY' if is_instant_entry else 'ASYNC MOMENTUM ENTRY'
            print(f'[{entry_label}] trade={trade_id} leg={leg_id} entry_price={entry_price} sl={sl_price} strike={strike} option={option_type}')
            await _async_store_position_history(adb, trade, new_leg, override_leg_cfg=leg_cfg)
            entered_ids.append(leg_id)
            cur_err_doc = await adb['algo_trades'].find_one({'_id': trade_id}, {'entry_error': 1}) or {}
            if str((cur_err_doc.get('entry_error') or {}).get('leg_id') or '') == leg_id:
                await adb['algo_trades'].update_one({'_id': trade_id}, {'$unset': {'entry_error': ''}})
        except Exception as exc:
            log.error('async momentum_pending entry error leg=%s: %s', leg_id, exc)
            try:
                await feature_col.update_one({'_id': feat_doc['_id'], 'status': 'processing'}, {'$set': {'status': 'active', 'processing_started_at': None}})
            except Exception:
                pass

    return entered_ids
