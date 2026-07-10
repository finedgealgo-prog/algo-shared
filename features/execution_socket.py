"""
execution_socket.py
────────────────────
WebSocket handler for two modes:

1. BACKTEST SIMULATION  (action: start_listening)
   Countdown-based market replay using option_chain snapshots.

2. LIVE MONITOR  (action: start_live_monitor)
   Ticks every 60 s. Reads running strategies from algo_trades,
   fetches latest option_chain data per open leg, checks SL/TP/entry-time,
   executes exits + re-entries + lazy-leg activations directly on the DB,
   and broadcasts the updated position snapshot via the socket.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from pymongo import DESCENDING

from features.mongo_data import MongoData
from features import auth as app_auth
from features.broker_accounts import DEFAULT_APP_USER_ID
import builtins as _builtins_exec
import features.debug_flags as _debug_flags
from features.debug_flags import debug_print, entry_print, runtime_print, trade_event_print

def print(*_a, **_kw):  # suppress plain print() in fast-forward mode
    if not _debug_flags.fast_forward_mode:
        _builtins_exec.print(*_a, **_kw)
from features.spot_atm_utils import (
    build_entry_spot_snapshots,
    clear_market_data_cache,
    get_cached_chain_doc,
    get_cached_spot_doc,
    preload_market_data_cache,
)
from features.mode_market_adapter import (
    get_spot_price,
    get_option_ltp,
    get_open_legs_ltp_array,
    get_latest_chain_doc,
    get_chain_doc_at_time,
    get_chain_doc_by_token,
    get_spot_doc_at_time,
    resolve_pending_entry_snapshot_for_mode,
    resolve_entry_execution_payload_for_mode,
)

log = logging.getLogger(__name__)

SHOW_PRINT_STATEMENT = False

# ─── trading_core integration ─────────────────────────────────────────────────
# trading_core.py is the canonical implementation of all trading logic.
# It is the single source of truth shared by every execution mode:
#   algo-backtest, strategy-backtest, portfolio-backtest, fast-forward, live-trade.
#
# Functions imported here replace their local duplicates in this file.
# Migration order (inch by inch):
#   ✓ Phase 1: compute_strategy_mtm  (replaces _compute_strategy_mtm_snapshot)
#   → Phase 2: square_off_trade      (replaces _square_off_trade_like_manual)
#   → Phase 3: process_tick          (replaces inner loop of _backtest_minute_tick)
#   → Phase 4: process_pending_entries (replaces _execute_backtest_entries)
from features.trading_core import (          # type: ignore
    TickContext,            # §2b  per-tick context object
    TickResult,             # §2b  return type of process_tick()
    compute_strategy_mtm,   # §8   MTM/PnL — replaces _compute_strategy_mtm_snapshot
    process_pending_entries as core_process_pending_entries,
    resolve_trade_leg_configs,
    resolve_leg_cfg,
    resolve_leg_expiry,
    get_spot_at_time,
)
# Backward-compat alias so ALL existing callers continue to work unchanged.
_compute_strategy_mtm_snapshot = compute_strategy_mtm
# ─────────────────────────────────────────────────────────────────────────────

socket_router = APIRouter(prefix='/algo')
MARKET_START_MINUTES = (9 * 60) + 15
MARKET_CLOSE_MINUTES = (15 * 60) + 30   # 15:30 IST
RUNNING_STATUS = 'StrategyStatus.Live_Running'
BACKTEST_IMPORT_STATUS = 'StrategyStatus.Import'
OPEN_LEG_STATUS = 1
CLOSED_LEG_STATUS = 0
LIVE_TICK_SECONDS = 60          # check interval for live mode
ENABLE_BACKTEST_DEBUG_LOGS = False
OPTION_CHAIN_COLLECTION = 'option_chain_historical_data'
SLOW_BACKTEST_WARN_MS = 250
DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
LISTENING_STRATEGY_SNAPSHOTS: dict[str, list[dict[str, Any]]] = {}
LISTENING_TIME_STATE: dict[str, dict[str, Any]] = {}
PENDING_EXECUTE_ORDER_GROUP_STARTS: dict[str, list[dict[str, Any]]] = {}
CONNECTED_CHANNEL_WEBSOCKETS: dict[str, list[WebSocket]] = {
    'executions': [],
    'execute-orders': [],
    'update': [],
}
# user_id-based rooms: key = "{user_id}:{channel}" → list of WebSockets
CONNECTED_USER_CHANNEL_WEBSOCKETS: dict[str, list[WebSocket]] = {}
# reverse map: id(websocket) → (user_id, channel) for fast cleanup
WEBSOCKET_USER_MAP: dict[int, tuple[str, str]] = {}

# ── Change-detection signatures ───────────────────────────────────────────────
# Keyed by user_id.  Only emit execute_order / update when data actually changed.
# algo_trades + algo_trade_positions_history + algo_leg_feature_status changes
# are all reflected in normalized_records (via _populate_legs_from_history →
# _attach_leg_feature_statuses), so a single signature over those records covers
# all three collections the user cares about.
LAST_EXECUTE_ORDER_SIG: dict[str, str] = {}   # user_id → md5 of record structure
LAST_OPEN_POSITION_SIG: dict[str, str] = {}   # user_id → md5 of open positions
PENDING_EXECUTE_ORDER_DIRTY: dict[str, dict[str, Any]] = {}
# Separate from PENDING_EXECUTE_ORDER_DIRTY on purpose: the execute-orders socket's
# own 1s loop consumes (pops) that dict to flush execute_order broadcasts, which was
# racing with the update socket's fast-forward subscribe_tokens refresh below —
# whichever loop's timeout fired first stole the flag, so the LTP token-filter
# refresh would intermittently never run after a new fast-forward leg was entered.
PENDING_FF_TOKEN_REFRESH_DIRTY: dict[str, bool] = {}
PENDING_BROKER_SETTINGS_DIRTY: dict[str, str] = {}  # "user_id:activation_mode" → activation_mode
PENDING_STRATEGY_ACTIVITIES: dict[str, list[dict]] = {}  # user_id → list of activity dicts

# Per-user subscribed token registry for the 'update' channel.
# live_fast_monitor reads this to filter LTP broadcasts per-user
# so only active strategy / open-position tokens are sent.
UPDATE_USER_SUBSCRIBED_TOKENS: dict[str, set[str]] = {}  # user_id → set of token strings

OVERALL_FEATURE_LEG_ID = '__overall__'
SPOT_TOKEN_BY_UNDERLYING = {
    'NIFTY': '256265',
    'BANKNIFTY': '260105',
    'FINNIFTY': '257801',
    'SENSEX': '265',
    'MIDCPNIFTY': '288009',
}
SPOT_UNDERLYING_BY_TOKEN = {token: underlying for underlying, token in SPOT_TOKEN_BY_UNDERLYING.items()}
QUOTE_INSTRUMENT_BY_UNDERLYING = {
    'NIFTY': 'NSE:NIFTY 50',
    'BANKNIFTY': 'NSE:NIFTY BANK',
    'FINNIFTY': 'NSE:NIFTY FIN SERVICE',
    'SENSEX': 'BSE:SENSEX',
    'MIDCPNIFTY': 'NSE:NIFTY MID SELECT',
}


# ─── Time helpers ─────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _now_iso() -> str:
    return _now_utc().strftime('%Y-%m-%dT%H:%M:%SZ')


def _now_ist_str() -> str:
    """Return current IST time as HH:MM for comparing with entry_time / exit_time."""
    # IST = UTC + 5:30
    now = _now_utc()
    total_minutes = now.hour * 60 + now.minute + 330  # +330 for IST offset
    total_minutes %= 1440
    return f"{total_minutes // 60:02d}:{total_minutes % 60:02d}"


def _today_ist() -> str:
    """Return today's date in IST as YYYY-MM-DD."""
    now = _now_utc()
    # Add 5:30 hours for IST
    total_minutes = now.hour * 60 + now.minute + 330
    extra_days = total_minutes // 1440
    from datetime import timedelta
    d = now.date() + timedelta(days=extra_days)
    return d.strftime('%Y-%m-%d')


def _parse_listen_timestamp(value: Any) -> datetime | None:
    raw_value = str(value or '').strip()
    if not raw_value:
        return None
    normalized_value = raw_value.replace(' ', 'T')
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M'):
        try:
            return datetime.strptime(normalized_value, fmt)
        except ValueError:
            continue
    return None


def _format_listen_timestamp(value: datetime) -> str:
    return value.strftime('%Y-%m-%dT%H:%M:%S')


def queue_execute_order_group_start(group_id: str, records: list[dict[str, Any]] | None) -> None:
    normalized_group_id = str(group_id or '').strip()
    if not normalized_group_id:
        return
    normalized_records = [dict(item) for item in (records or []) if isinstance(item, dict)]
    if normalized_records:
        PENDING_EXECUTE_ORDER_GROUP_STARTS[normalized_group_id] = normalized_records
        return
    PENDING_EXECUTE_ORDER_GROUP_STARTS.pop(normalized_group_id, None)


def _consume_execute_order_group_start(group_id: str) -> list[dict[str, Any]]:
    normalized_group_id = str(group_id or '').strip()
    if not normalized_group_id:
        return []
    return PENDING_EXECUTE_ORDER_GROUP_STARTS.pop(normalized_group_id, [])


def _consume_all_execute_order_group_starts() -> list[dict[str, Any]]:
    if not PENDING_EXECUTE_ORDER_GROUP_STARTS:
        return []
    records: list[dict[str, Any]] = []
    for group_records in PENDING_EXECUTE_ORDER_GROUP_STARTS.values():
        records.extend([dict(item) for item in (group_records or []) if isinstance(item, dict)])
    PENDING_EXECUTE_ORDER_GROUP_STARTS.clear()
    return records


def _register_channel_websocket(channel: str, websocket: WebSocket) -> None:
    channel_key = str(channel or '').strip()
    if not channel_key:
        return
    bucket = CONNECTED_CHANNEL_WEBSOCKETS.setdefault(channel_key, [])
    if websocket not in bucket:
        bucket.append(websocket)


def _unregister_channel_websocket(channel: str, websocket: WebSocket) -> None:
    channel_key = str(channel or '').strip()
    bucket = CONNECTED_CHANNEL_WEBSOCKETS.get(channel_key) or []
    if websocket in bucket:
        bucket.remove(websocket)


async def _broadcast_channel_message(channel: str, message: str) -> int:
    """Send to sockets NOT registered in any user room (non-user / legacy connections)."""
    channel_key = str(channel or '').strip()
    if not channel_key or not message:
        return 0
    sockets = list(CONNECTED_CHANNEL_WEBSOCKETS.get(channel_key) or [])
    delivered = 0
    for socket in sockets:
        if id(socket) in WEBSOCKET_USER_MAP:
            # This socket belongs to a user room — skip it.
            # It will only receive messages via _broadcast_user_channel_message.
            continue
        try:
            await socket.send_text(message)
            delivered += 1
        except Exception:
            _unregister_channel_websocket(channel_key, socket)
    return delivered


# ─── User room helpers ────────────────────────────────────────────────────────

async def _ws_authenticate(websocket: WebSocket, *, timeout: float = 10.0) -> str | None:
    """Require the client's first message to be {"type": "auth", "token": "<jwt>"}.

    Replaces the old "trust whatever user_id is in the URL" model — the JWT
    travels in-band as the first frame (never in the URL/query string, so it
    never lands in access/proxy logs) and is verified with the same
    app_auth.decode_access_token() used for REST routes. Closes the socket on
    timeout/missing/invalid/expired token and returns None; caller must stop
    without registering or sending anything. On success, returns the real
    user_id (the token's "sub" claim) — callers used to discard this and
    register every authenticated connection under the same shared
    DEFAULT_APP_USER_ID, which meant every real user's live strategies,
    running-count, and history all landed in one commingled bucket.
    """
    try:
        raw = await asyncio.wait_for(websocket.receive_text(), timeout=timeout)
    except (asyncio.TimeoutError, WebSocketDisconnect):
        await websocket.close(code=4401, reason='Authentication timed out')
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = {}
    token = str((parsed or {}).get('token') or '').strip()
    if str((parsed or {}).get('type') or '').strip() != 'auth' or not token:
        await websocket.close(code=4401, reason='Authentication required')
        return None
    try:
        claims = app_auth.decode_access_token(token)
    except HTTPException:
        await websocket.close(code=4401, reason='Invalid or expired token')
        return None
    user_id = str((claims or {}).get('sub') or '').strip()
    if not user_id:
        await websocket.close(code=4401, reason='Invalid token payload')
        return None
    return user_id


def _register_user_websocket(channel: str, user_id: str, websocket: WebSocket) -> None:
    uid = str(user_id or '').strip()
    ch  = str(channel or '').strip()
    if not uid or not ch:
        return
    existing_entry = WEBSOCKET_USER_MAP.get(id(websocket))
    if existing_entry and existing_entry != (uid, ch):
        _unregister_user_websocket(websocket)
    room_key = f'{uid}:{ch}'
    bucket = CONNECTED_USER_CHANNEL_WEBSOCKETS.setdefault(room_key, [])
    if websocket not in bucket:
        bucket.append(websocket)
    WEBSOCKET_USER_MAP[id(websocket)] = (uid, ch)


def _unregister_user_websocket(websocket: WebSocket) -> None:
    entry = WEBSOCKET_USER_MAP.pop(id(websocket), None)
    if not entry:
        return
    uid, ch = entry
    room_key = f'{uid}:{ch}'
    bucket = CONNECTED_USER_CHANNEL_WEBSOCKETS.get(room_key) or []
    if websocket in bucket:
        bucket.remove(websocket)


async def _broadcast_user_channel_message(user_id: str, channel: str, message: str) -> int:
    """Send message only to WebSocket connections registered for this user_id + channel."""
    uid = str(user_id or '').strip()
    ch  = str(channel or '').strip()
    if not uid or not ch or not message:
        return 0
    room_key = f'{uid}:{ch}'
    sockets = list(CONNECTED_USER_CHANNEL_WEBSOCKETS.get(room_key) or [])
    delivered = 0
    for socket in sockets:
        try:
            await socket.send_text(message)
            delivered += 1
        except Exception:
            _unregister_user_websocket(socket)
    return delivered


async def _broadcast_all_user_rooms(channel: str, message: str) -> int:
    """Send message to ALL connected user rooms for this channel."""
    ch = str(channel or '').strip()
    if not ch or not message:
        return 0
    suffix = f':{ch}'
    delivered = 0
    for room_key, sockets in list(CONNECTED_USER_CHANNEL_WEBSOCKETS.items()):
        if not room_key.endswith(suffix):
            continue
        for socket in list(sockets):
            try:
                await socket.send_text(message)
                delivered += 1
            except Exception:
                _unregister_user_websocket(socket)
    return delivered


async def broadcast_to_channel(channel: str, message: str) -> int:
    """
    Master broadcast: sends to non-user-room sockets AND all user rooms.

    Use this instead of _broadcast_channel_message when you want every
    connected client (with or without a user_id room) to receive the message.
    """
    if not message:
        return 0
    delivered  = await _broadcast_channel_message(channel, message)
    delivered += await _broadcast_all_user_rooms(channel, message)
    return delivered


# ─── Per-user subscribed token helpers (used by live_fast_monitor) ────────────

def register_update_user_tokens(user_id: str, tokens: list[dict]) -> None:
    """Store the token set a user has subscribed to in the 'update' channel."""
    uid = str(user_id or '').strip()
    if not uid:
        return
    UPDATE_USER_SUBSCRIBED_TOKENS[uid] = {
        str(t.get('token') or '').strip()
        for t in (tokens or [])
        if str(t.get('token') or '').strip()
    }


def unregister_update_user_tokens(user_id: str) -> None:
    """Remove the token set for a user who has disconnected."""
    UPDATE_USER_SUBSCRIBED_TOKENS.pop(str(user_id or '').strip(), None)


def get_update_user_subscribed_tokens() -> dict[str, set[str]]:
    """Return a snapshot of the per-user subscribed token registry."""
    return dict(UPDATE_USER_SUBSCRIBED_TOKENS)


# ─── Change-detection helpers ─────────────────────────────────────────────────

def _record_signature(records: list[dict]) -> str:
    """
    MD5 of structural fields from algo_trades + algo_trade_positions_history
    + algo_leg_feature_status (all reflected in normalized_records).

    Deliberately excludes LTP / market prices so a pure price tick with no
    structural change does NOT trigger an execute_order emit.
    """
    import hashlib, json as _json

    def _normalize_feature_rows(rows: Any) -> list[dict[str, Any]]:
        normalized_rows: list[dict[str, Any]] = []
        if not isinstance(rows, list):
            return normalized_rows
        for row in rows:
            if not isinstance(row, dict):
                continue
            normalized_rows.append({
                'id': str(row.get('_id') or ''),
                'feature': str(row.get('feature') or ''),
                'status': str(row.get('status') or ''),
                'leg_id': str(row.get('leg_id') or ''),
                'queued_at': str(row.get('queued_at') or ''),
                'armed_at': str(row.get('armed_at') or ''),
                'triggered_at': str(row.get('triggered_at') or ''),
                'base': row.get('momentum_base_price'),
                'target': row.get('momentum_target_price'),
            })
        normalized_rows.sort(key=lambda item: (item['feature'], item['leg_id'], item['id']))
        return normalized_rows

    sig: list[dict] = []
    for r in sorted(records, key=lambda x: str(x.get('_id') or '')):
        legs = r.get('legs') or []
        pending_feature_legs = r.get('pending_feature_legs') or []
        sig.append({
            'id':     str(r.get('_id') or ''),
            'status': str(r.get('status') or ''),
            'active': bool(r.get('active_on_server')),
            'legs': sorted(
                [
                    {
                        'id':     str(l.get('id') or l.get('leg_id') or ''),
                        'status': str(l.get('status') or ''),
                        'entry':  l.get('entry_price'),
                        'exit':   l.get('exit_price'),
                        'entry_trade': l.get('entry_trade'),
                        'exit_trade': l.get('exit_trade'),
                        'feat_rows': _normalize_feature_rows(l.get('feature_status_rows')),
                    }
                    for l in legs
                ],
                key=lambda x: x['id'],
            ),
            'pending_feature_legs': sorted(
                [
                    {
                        'id': str(l.get('id') or l.get('leg_id') or ''),
                        'status': str(l.get('status') or ''),
                        'feature': str(l.get('pending_feature') or ''),
                        'queued_at': str(l.get('queued_at') or ''),
                        'armed_at': str(l.get('armed_at') or ''),
                        'triggered_at': str(l.get('triggered_at') or ''),
                        'entry_trade': l.get('entry_trade'),
                        'exit_trade': l.get('exit_trade'),
                        'feat_rows': _normalize_feature_rows(l.get('feature_status_rows')),
                    }
                    for l in pending_feature_legs
                    if isinstance(l, dict)
                ],
                key=lambda x: x['id'],
            ),
            'strategy_feature_rows': _normalize_feature_rows(r.get('strategy_feature_status_rows')),
        })
    return hashlib.md5(_json.dumps(sig, sort_keys=True, default=str).encode()).hexdigest()


def _position_signature(open_positions: list[dict]) -> str:
    """MD5 of open-position structural fields (excludes LTP)."""
    import hashlib, json as _json
    sig = sorted(
        [
            {
                'trade_id': str(p.get('trade_id') or ''),
                'leg_id':   str(p.get('leg_id') or ''),
                'position': str(p.get('position') or ''),
                'entry':    p.get('entry_price'),
            }
            for p in open_positions
        ],
        key=lambda x: x['trade_id'] + x['leg_id'],
    )
    return hashlib.md5(_json.dumps(sig, sort_keys=True, default=str).encode()).hexdigest()


def clear_user_emit_signatures(user_id: str = '') -> None:
    """
    Clear cached signatures for a user (or all users if user_id is empty).
    Call when a new trade date is selected so the next tick always emits fresh data.
    """
    if user_id:
        LAST_EXECUTE_ORDER_SIG.pop(user_id, None)
        LAST_OPEN_POSITION_SIG.pop(user_id, None)
    else:
        LAST_EXECUTE_ORDER_SIG.clear()
        LAST_OPEN_POSITION_SIG.clear()


def _extract_trade_date_from_doc(trade: dict | None) -> str:
    if not isinstance(trade, dict):
        return ''
    creation_ts = str(trade.get('creation_ts') or '').strip()
    if len(creation_ts) >= 10:
        return creation_ts[:10]
    return ''


def mark_execute_order_dirty(
    *,
    user_id: str,
    trade_date: str,
    activation_mode: str = 'algo-backtest',
    group_id: str = '',
    trade_id: str = '',
) -> None:
    uid = str(user_id or '').strip()
    normalized_trade_date = str(trade_date or '').strip()
    normalized_mode = str(activation_mode or 'algo-backtest').strip() or 'algo-backtest'
    normalized_group_id = str(group_id or '').strip()
    normalized_trade_id = str(trade_id or '').strip()
    if not uid or not normalized_trade_date:
        return
    dirty_key = f'{uid}|{normalized_trade_date}|{normalized_mode}'
    dirty_entry = PENDING_EXECUTE_ORDER_DIRTY.setdefault(
        dirty_key,
        {
            'user_id': uid,
            'trade_date': normalized_trade_date,
            'activation_mode': normalized_mode,
            'group_ids': set(),
            'trade_ids': set(),
        },
    )
    if normalized_group_id:
        dirty_entry['group_ids'].add(normalized_group_id)
    if normalized_trade_id:
        dirty_entry['trade_ids'].add(normalized_trade_id)
    if normalized_mode in ('fast-forward', 'forward-test'):
        PENDING_FF_TOKEN_REFRESH_DIRTY[f'{uid}|{normalized_trade_date}|{normalized_mode}'] = True


def mark_execute_order_dirty_from_trade(trade: dict | None) -> None:
    if not isinstance(trade, dict):
        return
    mark_execute_order_dirty(
        user_id=str(trade.get('user_id') or '').strip(),
        trade_date=_extract_trade_date_from_doc(trade),
        activation_mode=str(trade.get('activation_mode') or 'algo-backtest').strip() or 'algo-backtest',
        group_id=str(((trade.get('portfolio') or {}).get('group_id')) or '').strip(),
        trade_id=str(trade.get('_id') or '').strip(),
    )


def mark_execute_order_dirty_from_trade_id(
    db: MongoData,
    trade_id: str,
) -> None:
    normalized_trade_id = str(trade_id or '').strip()
    if not normalized_trade_id:
        return
    try:
        trade = db._db['algo_trades'].find_one(
            {'_id': normalized_trade_id},
            {'_id': 1, 'user_id': 1, 'creation_ts': 1, 'activation_mode': 1, 'portfolio.group_id': 1},
        ) or {}
    except Exception as exc:
        log.warning('mark_execute_order_dirty_from_trade_id lookup error trade=%s: %s', normalized_trade_id, exc)
        return
    if trade:
        mark_execute_order_dirty_from_trade(trade)


def mark_strategy_activity(trade: dict, activity_type: str, data: dict | None = None) -> None:
    """
    Sync — queue a strategy activity event for a specific user_id.
    Called from tick-processing (synchronous) code.
    flush_pending_strategy_activities() emits all queued events asynchronously.

    activity_type values:
        'trail_sl'        – leg trail SL moved
        'sl_hit'          – leg SL hit
        'tp_hit'          – leg TP hit
        'reentry'         – leg reentry queued (SL or TP)
        'overall_sl'      – overall SL hit
        'overall_tgt'     – overall target hit
        'overall_reentry' – overall reentry queued
    """
    uid = str((trade or {}).get('user_id') or '').strip()
    if not uid:
        return
    activity = {
        'activity_type': str(activity_type or ''),
        'trade_id':      str((trade or {}).get('_id') or ''),
        'strategy_name': str((trade or {}).get('name') or ''),
        'ticker':        str((trade or {}).get('ticker') or ''),
        'timestamp':     _now_iso(),
        **(data or {}),
    }
    PENDING_STRATEGY_ACTIVITIES.setdefault(uid, []).append(activity)


async def flush_pending_strategy_activities() -> int:
    """
    Async — emit all queued strategy activity events to each user's WebSocket room.
    Channel: 'algo-activity'   Message type: 'strategy_activity'
    Called from the same async loops that call flush_pending_execute_order_emits.
    """
    if not PENDING_STRATEGY_ACTIVITIES:
        return 0
    users = list(PENDING_STRATEGY_ACTIVITIES.keys())
    delivered = 0
    for uid in users:
        activities = PENDING_STRATEGY_ACTIVITIES.pop(uid, [])
        if not activities:
            continue
        message = _build_message('strategy_activity', 'Strategy activity update', {'activities': activities})
        delivered += await _broadcast_user_channel_message(uid, 'algo-activity', message)
    return delivered


def mark_broker_settings_dirty(user_id: str, activation_mode: str) -> None:
    uid = str(user_id or '').strip()
    mode = str(activation_mode or '').strip()
    if uid and mode:
        PENDING_BROKER_SETTINGS_DIRTY[f'{uid}:{mode}'] = mode


def _consume_pending_broker_settings_dirty(
    user_id: str,
    activation_mode: str,
) -> bool:
    key = f'{str(user_id or "").strip()}:{str(activation_mode or "").strip()}'
    return bool(PENDING_BROKER_SETTINGS_DIRTY.pop(key, None))


def _consume_pending_execute_order_dirty(
    *,
    user_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    allowed_user_ids = {str(uid or '').strip() for uid in (user_ids or set()) if str(uid or '').strip()}
    dirty_items: list[dict[str, Any]] = []
    keys_to_remove: list[str] = []
    for dirty_key, dirty_entry in PENDING_EXECUTE_ORDER_DIRTY.items():
        uid = str((dirty_entry or {}).get('user_id') or '').strip()
        if allowed_user_ids and uid not in allowed_user_ids:
            continue
        dirty_items.append({
            'user_id': uid,
            'trade_date': str((dirty_entry or {}).get('trade_date') or '').strip(),
            'activation_mode': str((dirty_entry or {}).get('activation_mode') or 'algo-backtest').strip() or 'algo-backtest',
            'group_ids': sorted(str(item).strip() for item in ((dirty_entry or {}).get('group_ids') or set()) if str(item).strip()),
            'trade_ids': sorted(str(item).strip() for item in ((dirty_entry or {}).get('trade_ids') or set()) if str(item).strip()),
        })
        keys_to_remove.append(dirty_key)
    for dirty_key in keys_to_remove:
        PENDING_EXECUTE_ORDER_DIRTY.pop(dirty_key, None)
    return dirty_items


def _build_execute_order_socket_payload(
    records: list[dict[str, Any]] | None,
    *,
    trigger: str,
    group_id: str = '',
) -> dict[str, Any]:
    normalized_records = [dict(item) for item in (records or []) if isinstance(item, dict)]
    resolved_group_id = str(group_id or '').strip()
    if normalized_records:
        first_portfolio = normalized_records[0].get('portfolio') if isinstance(normalized_records[0].get('portfolio'), dict) else {}
        resolved_group_id = str(first_portfolio.get('group_id') or '').strip() or resolved_group_id
    return {
        'group_id': resolved_group_id,
        'records': [
            dict(item, position_refresh_trigger=trigger)
            for item in normalized_records
        ],
    }


async def emit_execute_order_for_user(
    db: MongoData,
    *,
    user_id: str,
    trade_date: str,
    activation_mode: str = 'algo-backtest',
    group_id: str = '',
    trade_ids: list[str] | tuple[str, ...] | None = None,
    trigger: str = 'db-change',
    message: str = 'Strategy execution updated',
    force: bool = False,
) -> int:
    """
    Emit execute_order only to the specific user's room after a DB mutation.

    Signature comparison is done over the user's full strategy state
    (algo_trades + algo_trade_positions_history + algo_leg_feature_status)
    so repeated calls with no effective DB change are suppressed.
    """
    uid = str(user_id or '').strip()
    normalized_trade_date = str(trade_date or '').strip()
    normalized_group_id = str(group_id or '').strip()
    normalized_trade_ids = [
        str(item or '').strip()
        for item in (trade_ids or [])
        if str(item or '').strip()
    ]
    if not uid or not normalized_trade_date:
        return 0

    full_records = _load_execute_order_group_records(
        db,
        normalized_trade_date,
        activation_mode,
        '',
        uid,
    )
    # When a specific group is targeted (e.g. after squared-off), also load
    # that group's records (includes squared-off entries) and merge them in
    # so the emit contains the full group state including closed legs.
    if normalized_group_id:
        group_specific_records = _load_execute_order_group_records(
            db,
            normalized_trade_date,
            activation_mode,
            normalized_group_id,
            uid,
        )
        seen_full_ids = {str(r.get('_id') or '') for r in full_records}
        for r in group_specific_records:
            if str(r.get('_id') or '') not in seen_full_ids:
                full_records.append(r)

    full_enriched_records = _populate_legs_from_history(db, full_records)
    full_signature = _record_signature(full_enriched_records)
    if not force and LAST_EXECUTE_ORDER_SIG.get(uid) == full_signature:
        return 0
    LAST_EXECUTE_ORDER_SIG[uid] = full_signature

    payload_records = full_enriched_records
    resolved_group_id = normalized_group_id
    if normalized_group_id:
        payload_records = [
            record
            for record in full_enriched_records
            if str(((record.get('portfolio') or {}).get('group_id')) or '').strip() == normalized_group_id
        ]
    elif normalized_trade_ids:
        affected_group_ids = {
            str(((record.get('portfolio') or {}).get('group_id')) or '').strip()
            for record in full_enriched_records
            if str(record.get('_id') or '').strip() in normalized_trade_ids
            and str(((record.get('portfolio') or {}).get('group_id')) or '').strip()
        }
        if affected_group_ids:
            payload_records = [
                record
                for record in full_enriched_records
                if str(((record.get('portfolio') or {}).get('group_id')) or '').strip() in affected_group_ids
            ]
            if len(affected_group_ids) == 1:
                resolved_group_id = next(iter(affected_group_ids))
        else:
            payload_records = [
                record
                for record in full_enriched_records
                if str(record.get('_id') or '').strip() in normalized_trade_ids
            ]

    payload = _build_execute_order_socket_payload(
        payload_records,
        trigger=trigger,
        group_id=resolved_group_id,
    )
    if normalized_trade_ids:
        payload['trade_ids'] = normalized_trade_ids
    return await _broadcast_user_channel_message(
        uid,
        'execute-orders',
        _build_message('execute_order', message, payload),
    )


async def flush_pending_execute_order_emits(
    db: MongoData,
    *,
    user_ids: set[str] | None = None,
    force: bool = False,
) -> int:
    """
    Flush batched execute_order emits for dirty users.

    This is the preferred professional pattern:
      DB writes mark the user dirty
      batch/tick boundary flushes one consolidated emit per user
    """
    delivered = 0
    dirty_items = _consume_pending_execute_order_dirty(user_ids=user_ids)
    for dirty_entry in dirty_items:
        group_ids = dirty_entry.get('group_ids') or []
        group_id = group_ids[0] if len(group_ids) == 1 else ''
        delivered += await emit_execute_order_for_user(
            db,
            user_id=str(dirty_entry.get('user_id') or '').strip(),
            trade_date=str(dirty_entry.get('trade_date') or '').strip(),
            activation_mode=str(dirty_entry.get('activation_mode') or 'algo-backtest').strip() or 'algo-backtest',
            group_id=group_id,
            trade_ids=dirty_entry.get('trade_ids') or [],
            trigger='db-change',
            message='Strategy execution updated',
            force=force,
        )
    return delivered


def _collect_group_records_for_execute_order(records: list[dict], changed_records: list[dict]) -> list[dict]:
    changed_group_ids = []
    seen_group_ids: set[str] = set()

    for record in changed_records or []:
        group_id = str((((record.get('portfolio') or {}).get('group_id')) or '')).strip()
        if not group_id or group_id in seen_group_ids:
            continue
        seen_group_ids.add(group_id)
        changed_group_ids.append(group_id)

    if not changed_group_ids:
        return [dict(item) for item in (changed_records or []) if isinstance(item, dict)]

    grouped_records: list[dict] = []
    for group_id in changed_group_ids:
        grouped_records.extend([
            dict(record) for record in (records or [])
            if str((((record.get('portfolio') or {}).get('group_id')) or '')).strip() == group_id
        ])
    return grouped_records


def _collect_records_for_group(records: list[dict], group_id: str) -> list[dict]:
    normalized_group_id = str(group_id or '').strip()
    if not normalized_group_id:
        return [dict(record) for record in (records or []) if isinstance(record, dict)]
    return [
        dict(record) for record in (records or [])
        if str((((record.get('portfolio') or {}).get('group_id')) or '')).strip() == normalized_group_id
    ]


# ─── Simulation helpers ───────────────────────────────────────────────────────

def _format_remaining_minutes(total_minutes: int) -> str:
    normalized = max(0, int(total_minutes or 0))
    hours = normalized // 60
    minutes = normalized % 60
    return f'{hours:02d}:{minutes:02d}'


def _format_market_time(remaining_minutes: int) -> str:
    simulated_minutes = max(0, MARKET_START_MINUTES - max(0, int(remaining_minutes or 0)))
    hours = (simulated_minutes // 60) % 24
    minutes = simulated_minutes % 60
    return f'{hours:02d}:{minutes:02d}'


def _format_market_time_from_minutes(total_market_minutes: int) -> str:
    normalized = max(0, int(total_market_minutes or 0))
    hours = (normalized // 60) % 24
    minutes = normalized % 60
    return f'{hours:02d}:{minutes:02d}'


def _build_payload(selected_date: str, remaining_minutes: int, behind_time: int | None = None) -> dict:
    payload = {
        'selected_date': selected_date,
        'remaining_minutes': max(0, int(remaining_minutes or 0)),
        'remaining_time': _format_remaining_minutes(remaining_minutes),
        'market_time': _format_market_time(remaining_minutes),
    }
    if behind_time is not None:
        payload['behind_time'] = max(0, int(behind_time or 0))
    return payload


def _build_message(message_type: str, message: str, data=None) -> str:
    payload = {
        'type': message_type,
        'message': message,
        'server_time': _now_iso(),
    }
    if data is not None:
        payload['data'] = data
    return json.dumps(payload)


def _print_position_refresh_status(
    stage: str,
    *,
    trade_date: str = '',
    activation_mode: str = '',
    reason: str = '',
    count: int | None = None,
) -> None:
    parts = [
        f"stage={str(stage or '').strip() or '-'}",
        f"trade_date={str(trade_date or '').strip() or '-'}",
        f"activation_mode={str(activation_mode or '').strip() or '-'}",
        f"reason={str(reason or '').strip() or '-'}",
    ]
    if count is not None:
        parts.append(f'count={int(count)}')
    print('[POSITION REFRESH STATUS] ' + ' '.join(parts))


def _print_open_position_groups(open_positions: list[dict] | None, *, trade_date: str = '') -> None:
    positions = [item for item in (open_positions or []) if isinstance(item, dict)]
    if not positions:
        print(
            '[OPEN POSITION STATUS] '
            f"trade_date={str(trade_date or '').strip() or '-'} "
            'count=0'
        )
        return
    for item in positions:
        print(
            '[OPEN POSITION STATUS] '
            f"trade_date={str(trade_date or '').strip() or '-'} "
            f"group_name={str(item.get('group_name') or '').strip() or '-'} "
            f"strategy_name={str(item.get('strategy_name') or '').strip() or '-'}"
        )


def _print_running_trade_record_groups(
    records: list[dict] | None,
    *,
    trade_date: str = '',
    activation_mode: str = '',
    listen_time: str = '',
) -> None:
    matched_records = [item for item in (records or []) if isinstance(item, dict)]
    if not matched_records:
        print(
            '[RUNNING TRADE STATUS] '
            f"trade_date={str(trade_date or '').strip() or '-'} "
            f"activation_mode={str(activation_mode or '').strip() or '-'} "
            'count=0'
        )
        return
    listen_hhmm = str(listen_time or '').strip()[:5]
    for item in matched_records:
        entry_time_raw = str(item.get('entry_time') or '').strip()
        entry_time_hhmm = entry_time_raw[11:16] if len(entry_time_raw) >= 16 else entry_time_raw[:5]
        # Determine entry status when listen_time is available
        if listen_hhmm:
            legs = item.get('legs') if isinstance(item.get('legs'), list) else []
            has_entry = any(
                isinstance(leg, dict)
                and isinstance(leg.get('entry_trade'), dict)
                and leg.get('entry_trade')
                and str((leg.get('entry_trade') or {}).get('order_status') or '').strip().upper() != 'OPEN'
                for leg in legs
            )
            if has_entry:
                entry_status = 'position_taking'
            elif listen_hhmm == entry_time_hhmm:
                entry_status = 'checking_position'
            elif entry_time_hhmm and listen_hhmm < entry_time_hhmm:
                entry_status = 'waiting_for_entry'
            else:
                entry_status = 'pending_entry'
            status_suffix = f' status={entry_status}'
        else:
            status_suffix = ''
        print(
            '[RUNNING TRADE STATUS] '
            f"trade_date={str(trade_date or '').strip() or '-'} "
            f"activation_mode={str(activation_mode or '').strip() or '-'} "
            f"group_name={str(((item.get('portfolio') or {}).get('group_name') or item.get('group_name') or '')).strip() or '-'} "
            f"strategy_name={str(item.get('name') or item.get('strategy_name') or '').strip() or '-'} "
            f"entry_time={entry_time_hhmm or '-'}"
            f"{status_suffix}"
        )


def _print_trade_query_debug(
    query: dict | None,
    *,
    trade_date: str = '',
    activation_mode: str = '',
) -> None:
    print(
        '[RUNNING TRADE QUERY] '
        f"trade_date={str(trade_date or '').strip() or '-'} "
        f"activation_mode={str(activation_mode or '').strip() or '-'} "
        f"query={json.dumps(query or {}, default=str, sort_keys=True)}"
    )


def _print_open_strategy_fetch_flow(
    stage: str,
    *,
    trade_date: str = '',
    activation_mode: str = '',
    count: int | None = None,
) -> None:
    parts = [
        f"stage={str(stage or '').strip() or '-'}",
        f"trade_date={str(trade_date or '').strip() or '-'}",
        f"activation_mode={str(activation_mode or '').strip() or '-'}",
    ]
    if count is not None:
        parts.append(f'count={int(count)}')
    print('[OPEN STRATEGY FLOW] ' + ' '.join(parts))


def _print_first_open_strategy_payload(
    records: list[dict] | None,
    *,
    trade_date: str = '',
    activation_mode: str = '',
) -> None:
    matched_records = [item for item in (records or []) if isinstance(item, dict)]
    if not matched_records:
        print(
            '[FIRST OPEN STRATEGY PAYLOAD] '
            f"trade_date={str(trade_date or '').strip() or '-'} "
            f"activation_mode={str(activation_mode or '').strip() or '-'} "
            'payload=null'
        )
        return
    print(
        '[FIRST OPEN STRATEGY PAYLOAD] '
        f"trade_date={str(trade_date or '').strip() or '-'} "
        f"activation_mode={str(activation_mode or '').strip() or '-'} "
        f"payload={json.dumps(matched_records[0], default=str)}"
    )


def _normalize_expiry_datetime(expiry_value: Any) -> str | None:
    raw = str(expiry_value or '').strip()
    if not raw:
        return None
    if ' ' in raw:
        return raw
    return f'{raw} 15:30:00'


def _backtest_debug_log(message: str) -> None:
    if ENABLE_BACKTEST_DEBUG_LOGS:
        print(message)


def _slow_backtest_log(stage: str, elapsed_ms: float, **context: Any) -> None:
    if elapsed_ms < SLOW_BACKTEST_WARN_MS:
        return
    suffix = ' '.join(f'{key}={value}' for key, value in context.items())
    log.warning('BACKTEST_SLOW stage=%s elapsed_ms=%.2f %s', stage, elapsed_ms, suffix)


# ─── Type helpers ─────────────────────────────────────────────────────────────

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _is_market_order(leg_cfg: dict, side: str = 'exit') -> bool:
    """Return True only when the leg's entry/exit order type is plain MARKET (not LIMIT or MPP)."""
    key = 'EntryOrder' if side == 'entry' else 'ExitOrder'
    order_cfg = leg_cfg.get(key) or {}
    if isinstance(order_cfg.get('Config'), dict):
        order_cfg = order_cfg['Config']
    raw = str(order_cfg.get('Type') or '').lower()
    return 'market' in raw and 'mpp' not in raw


def _is_terminal_broker_rejection(error: str) -> bool:
    """True for broker rejections that are config/input problems, not transient ones —
    retrying changes nothing until a human fixes the config, so the entry loop must
    stop instead of hitting the broker again every tick forever."""
    err = str(error or '').lower()
    return any(marker in err for marker in (
        'dh-905',        # Dhan: Invalid IP (server IP not whitelisted)
        'invalid ip',
    ))


def _build_mode_entry_order_payload(
    db: MongoData,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
    symbol: str,
    quantity: int,
    entry_price: float,
    exchange_ts: str,
    lot_size: int = 1,
) -> dict[str, Any]:
    """
    Build mode-specific broker-order metadata for a leg entry.

    Common entry storage stays shared for every mode; only live mode injects
    the real broker order details here.

    `quantity` is the stored lot count (matches the algo_trades/positions_history
    storage convention used everywhere else); the broker only ever receives actual
    contracts, so the live order call below multiplies by `lot_size` itself —
    callers must not pre-multiply `quantity` before passing it in here.
    """
    activation_mode = str(trade.get('activation_mode') or '').strip()
    if activation_mode != 'live' or not symbol:
        return {
            'allowed': True,
            'payload': {},
            'error': '',
        }

    leg_id_str = str(leg.get('id') or '')
    broker_quantity = max(0, int(quantity)) * max(1, int(lot_size or 1))
    try:
        from features.live_order_manager import place_live_entry_order

        live_order = place_live_entry_order(
            db, trade, leg, leg_cfg, symbol, broker_quantity, entry_price
        )
        if not live_order.get('order_id'):
            order_error = str(live_order.get('error') or 'live_order_failed')
            log.warning(
                '[LIVE ORDER] placement failed leg=%s trade=%s: %s',
                leg_id_str,
                str(trade.get('_id') or ''),
                order_error,
            )
            # Config/input-level broker rejections (wrong IP, invalid symbol, etc.) will
            # fail identically on every retry — without bailing out here, the monitor's
            # ~1s loop hits the broker again every tick forever, spamming the same error
            # and blocking the event loop on each failing call. Square off so it stops.
            if _is_terminal_broker_rejection(order_error):
                trade_id = str(trade.get('_id') or '')
                log.error(
                    '[LIVE ORDER] terminal broker rejection — squaring off trade=%s leg=%s: %s',
                    trade_id, leg_id_str, order_error,
                )
                try:
                    _square_off_trade_like_manual(db, trade, exit_timestamp=exchange_ts, activation_mode='live')
                except Exception as _sq_exc:
                    log.error('[LIVE ORDER] auto squareoff failed trade=%s: %s', trade_id, _sq_exc)
                from features.telegram_notifier import notify_user
                notify_user(
                    'order_placement_failed',
                    f'Trade {trade_id} squared off — broker rejected entry leg={leg_id_str}: {order_error}',
                    {'leg_id': leg_id_str, 'trade_id': trade_id},
                )
            return {
                'allowed': False,
                'payload': {},
                'error': order_error,
            }

        payload: dict[str, Any] = {
            'order_id': live_order['order_id'],
            'order_type': live_order['order_type'],
            'exchange': live_order.get('exchange', ''),
            'order_status': live_order['order_status'],
            'limit_price': live_order['limit_price'],
            'trigger_price': live_order['trigger_price'],
            'order_placed_at': exchange_ts,
            'convert_after': live_order.get('convert_after', 40),
        }
        return {
            'allowed': True,
            'payload': payload,
            'error': '',
        }
    except Exception as exc:
        log.error('[LIVE ORDER] entry order error leg=%s: %s', leg_id_str, exc)
        from features.telegram_notifier import notify_user
        notify_user('order_placement_failed', f'Entry order error leg={leg_id_str}: {exc}', {'leg_id': leg_id_str})
        return {
            'allowed': False,
            'payload': {},
            'error': str(exc),
        }


def _is_live_entry_order_pending(
    activation_mode: str,
    entry_trade_payload: dict | None,
) -> bool:
    if str(activation_mode or '').strip() != 'live':
        return False
    payload = entry_trade_payload if isinstance(entry_trade_payload, dict) else {}
    return str(payload.get('order_status') or '').strip().upper() == 'OPEN'


def _decorate_entry_trade_lifecycle(
    activation_mode: str,
    entry_trade_payload: dict | None,
) -> dict:
    payload = dict(entry_trade_payload or {})
    if str(activation_mode or '').strip() != 'live':
        return payload
    payload['entry_lifecycle_status'] = (
        'order_open' if _is_live_entry_order_pending(activation_mode, payload) else 'active'
    )
    return payload


def _dispatch_mode_exit_order(
    db: MongoData,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
    symbol: str,
    quantity: int,
    exit_price: float,
    exit_reason: str,
) -> None:
    """
    Trigger mode-specific exit-order side effects without branching the shared
    close-leg DB flow.
    """
    activation_mode = str(trade.get('activation_mode') or '').strip()
    if activation_mode != 'live' or not symbol or quantity <= 0:
        return

    leg_id = str(leg.get('id') or '')
    try:
        from features.live_order_manager import (
            cancel_open_exit_orders_for_leg,
            has_open_exit_order,
            place_live_exit_order,
        )

        trade_id = str(trade.get('_id') or '')
        if exit_reason in {'stoploss', 'target'} and has_open_exit_order(db, trade_id, leg_id, exit_reason):
            cancel_open_exit_orders_for_leg(
                db,
                trade,
                leg_id,
                keep_reason=exit_reason,
                cancel_reason=f'exit_dispatch_keep_existing:{exit_reason}',
            )
            print(
                f'[LIVE EXIT ORDER SKIP] trade={trade_id} leg={leg_id} '
                f'reason={exit_reason} existing_protection_order=open'
            )
            return

        cancel_open_exit_orders_for_leg(
            db,
            trade,
            leg_id,
            cancel_reason=f'exit_dispatch_replace:{exit_reason}',
        )

        place_live_exit_order(
            db, trade, leg, leg_cfg, symbol, quantity, exit_price, exit_reason
        )
    except Exception as exc:
        log.error(
            '[LIVE EXIT ORDER] error trade=%s leg=%s: %s',
            str(trade.get('_id') or ''),
            leg_id,
            exc,
        )
        from features.telegram_notifier import notify_user
        notify_user(
            'order_placement_failed',
            f'Exit order error trade={trade.get("_id")} leg={leg_id}: {exc}',
            {'trade_id': str(trade.get('_id') or ''), 'leg_id': leg_id},
        )


def _is_valid_trade_date(value: Any) -> bool:
    return bool(DATE_RE.fullmatch(str(value or '').strip()))


def _build_listening_snapshot_key(trade_date: str, activation_mode: str | None = None) -> str:
    normalized_date = str(trade_date or '').strip()
    normalized_mode = str(activation_mode or 'algo-backtest').strip() or 'algo-backtest'
    return f'{normalized_mode}::{normalized_date}'


def _set_listening_time_state(
    trade_date: str,
    *,
    activation_mode: str | None = None,
    listen_time: str = '',
    listen_timestamp: str = '',
) -> None:
    if not _is_valid_trade_date(trade_date):
        return
    snapshot_key = _build_listening_snapshot_key(trade_date, activation_mode)
    LISTENING_TIME_STATE[snapshot_key] = {
        'trade_date': trade_date,
        'activation_mode': str(activation_mode or 'algo-backtest').strip() or 'algo-backtest',
        'listen_time': str(listen_time or '').strip(),
        'listen_timestamp': str(listen_timestamp or '').strip(),
    }


def _get_listening_time_state(
    trade_date: str,
    *,
    activation_mode: str | None = None,
) -> dict[str, Any]:
    if not _is_valid_trade_date(trade_date):
        return {}
    snapshot_key = _build_listening_snapshot_key(trade_date, activation_mode)
    state = LISTENING_TIME_STATE.get(snapshot_key) or {}
    return dict(state) if isinstance(state, dict) else {}


def _set_listening_strategy_snapshot(
    trade_date: str,
    records: list[dict] | None,
    *,
    activation_mode: str | None = None,
) -> None:
    if not _is_valid_trade_date(trade_date):
        return
    snapshot_key = _build_listening_snapshot_key(trade_date, activation_mode)
    normalized_records: list[dict[str, Any]] = []
    for record in records or []:
        if isinstance(record, dict):
            normalized_records.append(dict(record))
    LISTENING_STRATEGY_SNAPSHOTS[snapshot_key] = normalized_records


def _get_listening_strategy_snapshot(
    trade_date: str,
    *,
    activation_mode: str | None = None,
) -> list[dict[str, Any]]:
    if not _is_valid_trade_date(trade_date):
        return []
    snapshot_key = _build_listening_snapshot_key(trade_date, activation_mode)
    records = LISTENING_STRATEGY_SNAPSHOTS.get(snapshot_key) or []
    return [dict(record) for record in records if isinstance(record, dict)]


def _clear_listening_strategy_snapshot(
    trade_date: str | None = None,
    *,
    activation_mode: str | None = None,
) -> None:
    if trade_date is None:
        LISTENING_STRATEGY_SNAPSHOTS.clear()
        return
    if not _is_valid_trade_date(trade_date):
        return
    snapshot_key = _build_listening_snapshot_key(trade_date, activation_mode)
    LISTENING_STRATEGY_SNAPSHOTS.pop(snapshot_key, None)
    LISTENING_TIME_STATE.pop(snapshot_key, None)


def _build_trade_query(
    trade_date: str,
    *,
    activation_mode: str | None = None,
    statuses: list[str] | tuple[str, ...] | None = None,
) -> dict:
    query: dict[str, Any] = {'trade_status': 1}
    normalized_date = str(trade_date or '').strip()
    if normalized_date:
        query['creation_ts'] = {'$regex': f'^{re.escape(normalized_date)}'}
    if activation_mode:
        query['activation_mode'] = str(activation_mode).strip()
    normalized_statuses = [str(status or '').strip() for status in (statuses or []) if str(status or '').strip()]
    if len(normalized_statuses) == 1:
        query['status'] = normalized_statuses[0]
    elif normalized_statuses:
        query['status'] = {'$in': normalized_statuses}
    return query


# ─── Token generation ─────────────────────────────────────────────────────────

def make_token(underlying: str, expiry: str, strike: Any, option_type: str) -> str:
    """
    Generate a composite token for an option contract.
    Example: make_token('NIFTY', '2025-11-04', 24500, 'CE')
             → 'NIFTY_2025-11-04_24500_CE'
    """
    strike_str = str(int(float(strike))) if strike is not None else '0'
    return f"{underlying}_{expiry}_{strike_str}_{option_type}"


# ─── Option chain / spot lookup ──────────────────────────────────────────────
# All backtest market-data DB reads go through algo_backtest_event.py.
# These aliases keep every existing caller in this file working unchanged.

def get_chain_at_time(
    chain_col,
    underlying,
    expiry,
    strike,
    option_type,
    snapshot_ts,
    market_cache=None,
    activation_mode=None,
):
    return get_chain_doc_at_time(
        chain_col,
        underlying,
        expiry,
        strike,
        option_type,
        snapshot_ts,
        market_cache,
        activation_mode=activation_mode,
    )

def get_chain_by_token_at_time(chain_col, token, snapshot_ts, activation_mode=None):
    return get_chain_doc_by_token(chain_col, token, snapshot_ts, activation_mode=activation_mode)

def get_index_spot_at_time(index_spot_col, underlying, snapshot_ts, market_cache=None, activation_mode=None):
    return get_spot_doc_at_time(
        index_spot_col,
        underlying,
        snapshot_ts,
        market_cache,
        activation_mode=activation_mode,
    )


# ─── SL / TP calculations ──────────────────────────────────────────────────────

def _is_sell(position_str: str) -> bool:
    return 'sell' in str(position_str or '').lower()


from features.position_manager import (
    calc_sl_price,
    calc_tp_price,
    is_sl_hit,
    is_tp_hit,
    update_trail_sl,
    get_trail_config,
    get_reentry_sl_config,
    get_reentry_tp_config,
    parse_overall_sl,
    parse_overall_tgt,
    check_overall_sl,
    check_overall_tgt,
    parse_overall_trail_sl,
    update_overall_trail_sl,
    parse_lock_and_trail,
    check_lock_and_trail,
    parse_overall_reentry_sl,
    parse_overall_reentry_tgt,
    build_reentry_action,
    check_leg_exit,
    LockAndTrailConfig,
    ReentryAction,
    LegCheckResult,
)


# ─── DB operations ────────────────────────────────────────────────────────────

def close_leg_in_db(db: MongoData, trade_id: str, leg_index: int,
                    exit_price: float, exit_reason: str, now_ts: str,
                    leg_id: str = '', activation_mode: str = '',
                    exit_iv: float | None = None,
                    exit_vix: float | None = None) -> None:
    """Mark a leg as closed in algo_trades and update position history exit_trade."""
    from features.app_logger import log_db_write, log_db_error
    exit_trade_payload = {
        'trigger_timestamp': now_ts,
        'trigger_price': exit_price,
        'price': exit_price,
        'traded_timestamp': now_ts,
        'exchange_timestamp': now_ts,
        'exit_reason': exit_reason,
    }
    if exit_iv is not None:
        exit_trade_payload['exit_iv'] = exit_iv
    if exit_vix is not None:
        exit_trade_payload['exit_vix'] = exit_vix
    try:
        if leg_id:
            db._db['algo_trades'].update_one(
                {'_id': trade_id},
                {'$set': {
                    'legs.$[elem].status': CLOSED_LEG_STATUS,
                    'legs.$[elem].exit_reason': exit_reason,
                    'legs.$[elem].exit_trade': exit_trade_payload,
                    'legs.$[elem].last_saw_price': exit_price,
                }},
                array_filters=[{'elem.id': leg_id}],
            )
        else:
            db._db['algo_trades'].update_one(
                {'_id': trade_id},
                {'$set': {
                    f'legs.{leg_index}.status': CLOSED_LEG_STATUS,
                    f'legs.{leg_index}.exit_reason': exit_reason,
                    f'legs.{leg_index}.exit_trade': exit_trade_payload,
                    f'legs.{leg_index}.last_saw_price': exit_price,
                }},
            )
        log_db_write('algo_trades', 'close_leg', trade_id, {
            'leg_id': leg_id or leg_index, 'exit_reason': exit_reason,
            'exit_price': exit_price, 'ts': now_ts,
        })
    except Exception as exc:
        log.error('close_leg_in_db error trade=%s leg=%s: %s', trade_id, leg_id or leg_index, exc)
        log_db_error('algo_trades', 'close_leg', exc, trade_id, {
            'leg_id': leg_id or leg_index, 'exit_reason': exit_reason,
        })
    if leg_id:
        _update_position_history_exit(db, trade_id, leg_id, exit_price, exit_reason, now_ts, exit_iv=exit_iv, exit_vix=exit_vix)
    if leg_id:
        _hist = db._db['algo_trade_positions_history'].find_one(
            {'trade_id': trade_id, 'leg_id': leg_id, 'exit_trade': None},
            {'symbol': 1, 'quantity': 1, 'lot_size': 1, 'position': 1},
        ) or {}
        _symbol = str(_hist.get('symbol') or '').strip()
        # quantity stored is lot count, not contracts — broker exit order needs the
        # actual contract count, same convention as live_manual_square_off_trade.
        _qty = int(_hist.get('quantity') or 1) * max(1, int(_hist.get('lot_size') or 1))
        _trade = db._db['algo_trades'].find_one({'_id': trade_id}) or {}
        _leg = next(
            (l for l in (_trade.get('legs') or [])
             if isinstance(l, dict) and str(l.get('id') or '') == leg_id),
            {},
        )
        _all_cfgs = _resolve_trade_leg_configs(_trade)
        _leg_cfg = _resolve_leg_cfg(leg_id, _leg, _all_cfgs)
        _dispatch_mode_exit_order(
            db,
            _trade,
            _leg,
            _leg_cfg,
            _symbol,
            _qty,
            exit_price,
            exit_reason,
        )
    mark_execute_order_dirty_from_trade_id(db, trade_id)


def update_leg_sl_in_db(db: MongoData, trade_id: str, leg_index: int,
                         new_sl: float, last_price: float, leg_id: str = '') -> None:
    """Update running SL fields only when the stoploss level actually changes."""
    try:
        if leg_id:
            db._db['algo_trades'].update_one(
                {'_id': trade_id},
                {'$set': {
                    'legs.$[elem].current_sl_price': new_sl,
                    'legs.$[elem].display_sl_value': new_sl,
                    'legs.$[elem].last_saw_price': last_price,
                }},
                array_filters=[{'elem.id': leg_id}],
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
        log.error('update_leg_sl_in_db error: %s', exc)
    if leg_id:
        try:
            _sl_set = {'current_sl_price': new_sl, 'display_sl_value': new_sl, 'last_saw_price': last_price}
            print(f'[HIST_UPDATE][SL_SYNC] trade={trade_id} leg={leg_id} data={_sl_set}')
            db._db['algo_trade_positions_history'].update_one(
                {'trade_id': trade_id, 'leg_id': leg_id, 'exit_trade': None},
                {'$set': _sl_set},
            )
        except Exception as exc:
            log.error('update_leg_sl_in_db history sync error trade=%s leg=%s: %s', trade_id, leg_id, exc)
    mark_execute_order_dirty_from_trade_id(db, trade_id)


def push_new_leg_in_db(db: MongoData, trade_id: str, new_leg: dict) -> None:
    """Append a new leg entry to algo_trades.legs."""
    leg_id = str((new_leg or {}).get('id') or '').strip()
    if not leg_id:
        return
    try:
        db._db['algo_trades'].update_one(
            {
                '_id': trade_id,
                'legs': {'$not': {'$elemMatch': {'id': leg_id}}},
            },
            {'$push': {'legs': new_leg}},
        )
    except Exception as exc:
        log.error('push_new_leg_in_db error: %s', exc)
    mark_execute_order_dirty_from_trade_id(db, trade_id)


def _is_strict_history_leg_mode(activation_mode: str | None = None) -> bool:
    normalized_mode = str(activation_mode or '').strip()
    return normalized_mode in {'live', 'fast-forward', 'forward-test'}


def _count_trade_history_rows(db: MongoData, trade_id: str) -> int:
    normalized_trade_id = str(trade_id or '').strip()
    if not normalized_trade_id:
        return 0
    try:
        return int(db._db['algo_trade_positions_history'].count_documents({'trade_id': normalized_trade_id}))
    except Exception:
        return 0


def _build_live_pending_entry_snapshot(
    db: MongoData,
    trade: dict,
    leg_cfg: dict,
    *,
    now_ts: str,
    ) -> dict[str, Any]:
    return resolve_pending_entry_snapshot_for_mode(
        db,
        trade,
        leg_cfg,
        now_ts=now_ts,
    ) or {}


def _resolve_mode_spot_doc_for_entry(
    db: MongoData,
    trade: dict,
    leg: dict,
    *,
    now_ts: str,
) -> dict[str, Any]:
    underlying = str(
        (trade.get('strategy') or {}).get('Ticker')
        or (trade.get('config') or {}).get('Ticker')
        or trade.get('ticker')
        or ''
    ).strip()
    spot_price = _safe_float(leg.get('spot_at_queue'))
    if spot_price > 0:
        return {
            'underlying': underlying,
            'spot_price': spot_price,
            'timestamp': now_ts,
            'source': 'pending_entry_snapshot',
        }
    live_snapshot = _build_live_pending_entry_snapshot(
        db,
        trade,
        {
            'id': str(leg.get('id') or ''),
            'ContractType': {
                'Option': str(leg.get('option') or ''),
                'Expiry': str(leg.get('expiry_kind') or ''),
                'StrikeParameter': str(leg.get('strike_parameter') or ''),
            },
            'InstrumentKind': str(leg.get('option') or ''),
            'ExpiryKind': str(leg.get('expiry_kind') or ''),
            'StrikeParameter': str(leg.get('strike_parameter') or ''),
        },
        now_ts=now_ts,
    )
    live_spot_price = _safe_float(
        live_snapshot.get('spot_at_queue')
        or live_snapshot.get('live_spot_price')
    )
    if live_spot_price > 0:
        return {
            'underlying': underlying,
            'spot_price': live_spot_price,
            'timestamp': now_ts,
            'source': 'mode_adapter',
        }
    return {}


def _mark_trade_squared_off_at_exit_time(db: MongoData, trade_id: str) -> None:
    from features.app_logger import log_db_write, log_db_error
    normalized_trade_id = str(trade_id or '').strip()
    if not normalized_trade_id:
        return
    try:
        db._db['algo_trades'].update_one(
            {'_id': normalized_trade_id},
            {'$set': {
                'active_on_server': False,
                'status': 'StrategyStatus.SquaredOff',
                'trade_status': 2,
            }},
        )
        log_db_write('algo_trades', 'squared_off_exit_time', normalized_trade_id)
    except Exception as exc:
        log.error('_mark_trade_squared_off_at_exit_time error trade=%s: %s', normalized_trade_id, exc)
        log_db_error('algo_trades', 'squared_off_exit_time', exc, normalized_trade_id)
    mark_execute_order_dirty_from_trade_id(db, normalized_trade_id)


def _square_off_trade_like_manual(
    db: MongoData,
    trade_rec: dict,
    *,
    exit_timestamp: str,
    activation_mode: str = '',
) -> bool:
    t_id = str((trade_rec or {}).get('_id') or '').strip()
    if not t_id:
        return False

    _mode = str(activation_mode or trade_rec.get('activation_mode') or '').strip()

    # Live mode: cancel open orders + place broker exit orders FIRST
    if _mode == 'live':
        try:
            from features.live_order_manager import live_manual_square_off_trade
            live_manual_square_off_trade(db, trade_rec)
        except Exception as _lsq_exc:
            log.error('[MANUAL SQ LIVE] trade=%s: %s', t_id, _lsq_exc)

    chain_col = db._db[OPTION_CHAIN_COLLECTION]
    algo_trades_col = db._db['algo_trades']
    history_col = db._db['algo_trade_positions_history']
    underlying = str(
        trade_rec.get('ticker')
        or ((trade_rec.get('config') or {}).get('Ticker') or '')
    ).strip()

    trade_legs = [leg for leg in (trade_rec.get('legs') or []) if isinstance(leg, dict)]
    trade_leg_index_by_id = {
        str(leg.get('id') or '').strip(): leg_index
        for leg_index, leg in enumerate(trade_legs)
        if str(leg.get('id') or '').strip()
    }
    all_open_legs_closed = True
    history_open_legs_by_id = {}
    for history_leg in history_col.find({'trade_id': t_id, 'status': OPEN_LEG_STATUS}):
        history_leg_id = str(history_leg.get('leg_id') or history_leg.get('id') or '').strip()
        if history_leg_id:
            history_open_legs_by_id[history_leg_id] = history_leg

    legs_to_close = []
    for leg_index, leg in enumerate(trade_legs):
        leg_exit_trade = leg.get('exit_trade') if isinstance(leg.get('exit_trade'), dict) else None
        leg_status = int(leg.get('status') or 0)
        if leg_status == OPEN_LEG_STATUS or leg_exit_trade is None:
            legs_to_close.append((leg_index, leg))
    if not legs_to_close:
        legs_to_close = [(-1, leg) for leg in history_open_legs_by_id.values()]

    for leg_index, open_leg in legs_to_close:
        leg_id = str(open_leg.get('id') or open_leg.get('leg_id') or '').strip()
        history_open_leg = history_open_legs_by_id.get(leg_id) or {}
        leg_token = str(
            open_leg.get('token')
            or history_open_leg.get('token')
            or ''
        ).strip()
        strike = open_leg.get('strike')
        expiry_date = str(open_leg.get('expiry_date') or '')[:10]
        option_type = str(open_leg.get('option') or '')
        # Live / fast-forward: get current exit price via broker REST (most accurate)
        exit_price = 0.0
        if activation_mode in {'fast-forward', 'live', 'forward-test'}:
            try:
                from features.broker_gateway import _active_broker, get_broker_rest_quotes  # type: ignore
                if _active_broker() == 'dhan':
                    # Resolve correct Dhan token — leg_token may be an old/Kite token
                    _sq_underlying = underlying
                    _sq_strike_raw = open_leg.get('strike')
                    _sq_expiry     = str(open_leg.get('expiry_date') or '')[:10]
                    _sq_opt        = str(open_leg.get('option') or '').upper()
                    _dhan_sq_token = ''
                    _dhan_sq_seg   = str(open_leg.get('ws_segment') or 'NSE_FNO')
                    try:
                        _sq_s_int = int(float(str(_sq_strike_raw))) if _sq_strike_raw is not None else None
                    except Exception:
                        _sq_s_int = None
                    _sq_strike_filter = {'$in': [_sq_s_int, float(_sq_s_int)]} if _sq_s_int is not None else _sq_strike_raw
                    try:
                        _sq_at = db._db['active_option_tokens'].find_one(
                            {
                                'broker': 'dhan',
                                'instrument': {'$regex': f'^{_sq_underlying}$', '$options': 'i'},
                                'strike': _sq_strike_filter,
                                'option_type': {'$regex': f'^{_sq_opt}$', '$options': 'i'},
                                'expiry': {'$regex': f'^{_sq_expiry}'},
                            },
                            {'token': 1, 'tokens': 1, 'ws_segment': 1},
                        )
                        if _sq_at:
                            _dhan_sq_token = str(_sq_at.get('token') or _sq_at.get('tokens') or '').strip()
                            _dhan_sq_seg   = str(_sq_at.get('ws_segment') or _dhan_sq_seg)
                    except Exception:
                        pass
                    _use_sq_token = _dhan_sq_token or leg_token
                    if _use_sq_token:
                        _quotes = get_broker_rest_quotes([_use_sq_token], db._db, {_use_sq_token: _dhan_sq_seg})
                        exit_price = float((_quotes.get(_use_sq_token) or {}).get('ltp') or 0)
                else:
                    if leg_token:
                        from features.live_monitor_socket import _get_active_ticker_manager
                        _tm = _get_active_ticker_manager()
                        exit_price = float(_tm.ltp_map.get(str(leg_token)) or 0)
            except Exception:
                pass

        # Fallback: historical chain doc (backtest, or live/ff ticker miss)
        if exit_price <= 0:
            chain_doc = (
                get_chain_by_token_at_time(chain_col, leg_token, exit_timestamp)
                if (leg_token and exit_timestamp)
                else {}
            )
            if not chain_doc and exit_timestamp:
                chain_doc = get_chain_at_time(
                    chain_col,
                    underlying,
                    expiry_date,
                    strike,
                    option_type,
                    exit_timestamp,
                )
            chain_doc = _normalize_chain_market_fields(chain_doc)
            exit_price = _resolve_chain_market_price(chain_doc) if chain_doc else 0.0

        # Final fallback: entry price (never skip the leg)
        if exit_price <= 0:
            exit_price = float((open_leg.get('entry_trade') or {}).get('price') or 0)
        trade_event_print(
            f'  [SQUARE OFF] leg={leg_id} token={leg_token} exit_price={exit_price} mode={activation_mode}'
        )
        resolved_leg_index = leg_index if leg_index >= 0 else trade_leg_index_by_id.get(leg_id, -1)
        if resolved_leg_index >= 0:
            close_leg_in_db(
                db, t_id, resolved_leg_index, exit_price, 'squared_off', exit_timestamp,
                leg_id=leg_id, activation_mode=activation_mode,
            )
        else:
            _update_position_history_exit(
                db, t_id, leg_id, exit_price, 'squared_off', exit_timestamp
            )
        print(
            f'[SQUARE OFF] closed leg trade={t_id} leg={leg_id} '
            f'price={exit_price} ts={exit_timestamp}'
        )

    refreshed_trade = algo_trades_col.find_one({'_id': t_id}) or {}
    refreshed_legs = [leg for leg in (refreshed_trade.get('legs') or []) if isinstance(leg, dict)]
    remaining_history_open_legs = list(history_col.find({'trade_id': t_id, 'status': OPEN_LEG_STATUS}))
    pending_exit_legs = [
        leg for leg in refreshed_legs
        if int(leg.get('status') or 0) == OPEN_LEG_STATUS
        or not isinstance(leg.get('exit_trade'), dict)
    ]
    if all_open_legs_closed and not pending_exit_legs and not remaining_history_open_legs:
        algo_trades_col.update_one(
            {'_id': t_id},
            {'$set': {
                'active_on_server': False,
                'status': 'StrategyStatus.SquaredOff',
                'trade_status': 2,
            }},
        )
        print(f'[SQUARE OFF] trade={t_id} marked SquaredOff')
        return True

    print(
        f'[SQUARE OFF] trade={t_id} not marked SquaredOff because one or more legs were not closed '
        f'pending_exit_legs={len(pending_exit_legs)} '
        f'remaining_history_open_legs={len(remaining_history_open_legs)}'
    )
    return False


def _resolve_overall_cycle_value(base_value: float, completed_reentries: int) -> float:
    normalized_base = _safe_float(base_value)
    if normalized_base <= 0:
        return 0.0
    cycle_index = max(0, int(completed_reentries or 0)) + 1
    return round(normalized_base * cycle_index, 2)


def _resolve_next_overall_cycle_value(base_value: float, completed_reentries: int) -> float:
    normalized_base = _safe_float(base_value)
    if normalized_base <= 0:
        return 0.0
    next_cycle_index = max(0, int(completed_reentries or 0)) + 2
    return round(normalized_base * next_cycle_index, 2)


def _set_overall_feature_status_state(
    db: MongoData,
    trade_id: str,
    feature: str,
    *,
    enabled: bool,
    status: str,
    timestamp: str,
    current_mtm: float | None = None,
    disabled_reason: str | None = None,
) -> None:
    normalized_trade_id = str(trade_id or '').strip()
    normalized_feature = str(feature or '').strip()
    if not normalized_trade_id or not normalized_feature:
        return
    update_fields: dict[str, Any] = {
        'enabled': enabled,
        'status': status,
        'updated_at': timestamp,
    }
    if current_mtm is not None:
        update_fields['current_mtm'] = round(_safe_float(current_mtm), 2)
    if not enabled:
        update_fields['disabled_at'] = timestamp
        update_fields['disabled_reason'] = str(disabled_reason or status or 'disabled')
    try:
        db._db['algo_leg_feature_status'].update_many(
            {
                'trade_id': normalized_trade_id,
                'leg_id': OVERALL_FEATURE_LEG_ID,
                'feature': normalized_feature,
                'enabled': True,
            },
            {'$set': update_fields},
        )
    except Exception as exc:
        log.warning('_set_overall_feature_status_state error trade=%s feature=%s: %s', normalized_trade_id, normalized_feature, exc)
    mark_execute_order_dirty_from_trade_id(db, normalized_trade_id)


def _sync_overall_strategy_feature_status(
    db: MongoData,
    trade: dict,
    timestamp: str,
    *,
    current_mtm: float = 0.0,
    overall_sl_done: int | None = None,
    overall_tgt_done: int | None = None,
) -> None:
    trade_id = str(trade.get('_id') or '').strip()
    if not trade_id:
        return

    strategy_cfg = trade.get('strategy') or trade.get('config') or {}
    osl_type, osl_val = parse_overall_sl(strategy_cfg)
    otgt_type, otgt_val = parse_overall_tgt(strategy_cfg)
    ore_type, ore_count = parse_overall_reentry_sl(strategy_cfg)
    ort_type, ort_count = parse_overall_reentry_tgt(strategy_cfg)
    ore_done = int(overall_sl_done if overall_sl_done is not None else (trade.get('overall_sl_reentry_done') or 0))
    ort_done = int(overall_tgt_done if overall_tgt_done is not None else (trade.get('overall_tgt_reentry_done') or 0))
    meta_strategy_id = str(trade.get('strategy_id') or '')
    meta_strategy_name = str(trade.get('name') or '')
    meta_ticker = str(
        trade.get('ticker')
        or ((trade.get('config') or {}).get('Ticker') or '')
        or ((trade.get('strategy') or {}).get('Ticker') or '')
    )
    today = str(timestamp or '').replace('T', ' ')[:10]
    feature_col = db._db['algo_leg_feature_status']

    def _upsert_row(feature: str, base_type: str, base_value: float, done_count: int, re_type: str, re_count: int) -> None:
        if base_type == 'None' or _safe_float(base_value) <= 0:
            _set_overall_feature_status_state(
                db, trade_id, feature,
                enabled=False,
                status='disabled',
                timestamp=timestamp,
                current_mtm=current_mtm,
                disabled_reason='feature_not_configured',
            )
            return

        cycle_number = max(0, int(done_count or 0)) + 1
        current_value = _resolve_overall_cycle_value(base_value, done_count)
        next_value = _resolve_next_overall_cycle_value(base_value, done_count)
        description_label = 'Overall SL' if feature == 'overall_sl' else 'Overall Target'
        description = (
            f'{description_label} active for cycle {cycle_number}. '
            f'Current threshold: ₹{current_value:.2f}. '
            f'Re-entry type: {re_type or "None"}. '
            f'Re-entry used: {done_count}/{re_count}. '
            f'Next cycle threshold: ₹{next_value:.2f}.'
        )
        query = {
            'trade_id': trade_id,
            'leg_id': OVERALL_FEATURE_LEG_ID,
            'feature': feature,
            'enabled': True,
        }
        update = {
            '$set': {
                'strategy_id': meta_strategy_id,
                'strategy_name': meta_strategy_name,
                'ticker': meta_ticker,
                'trade_date': today,
                'feature': feature,
                'feature_scope': 'strategy',
                'leg_id': OVERALL_FEATURE_LEG_ID,
                'enabled': True,
                'status': 'active',
                'trigger_type': base_type,
                'trigger_value': current_value,
                'base_trigger_value': round(_safe_float(base_value), 2),
                'next_trigger_value': next_value,
                'reentry_type': re_type,
                'reentry_count': int(re_count or 0),
                'reentry_done': int(done_count or 0),
                'cycle_number': cycle_number,
                'current_mtm': round(_safe_float(current_mtm), 2),
                'trigger_description': description,
                'updated_at': timestamp,
                'disabled_at': None,
                'disabled_reason': None,
            },
            '$setOnInsert': {
                'created_at': timestamp,
            },
        }
        try:
            feature_col.update_one(query, update, upsert=True)
        except Exception as exc:
            log.warning('_sync_overall_strategy_feature_status upsert error trade=%s feature=%s: %s', trade_id, feature, exc)

    _upsert_row('overall_sl', osl_type, osl_val, ore_done, ore_type, ore_count)
    _upsert_row('overall_target', otgt_type, otgt_val, ort_done, ort_type, ort_count)
    mark_execute_order_dirty_from_trade(trade)


def _disable_trade_feature_rows_for_new_cycle(
    db: MongoData,
    trade_id: str,
    *,
    reason: str,
    timestamp: str,
) -> None:
    normalized_trade_id = str(trade_id or '').strip()
    if not normalized_trade_id:
        return
    feature_col = db._db['algo_leg_feature_status']
    now = str(timestamp or '').strip()
    base_set = {
        'enabled': False,
        'disabled_at': now,
        'disabled_reason': str(reason or 'cycle_completed'),
        'updated_at': now,
    }
    try:
        feature_col.update_many(
            {
                'trade_id': normalized_trade_id,
                'enabled': True,
                'status': {'$in': ['pending', 'active']},
            },
            {'$set': dict(base_set, status='disabled')},
        )
        feature_col.update_many(
            {
                'trade_id': normalized_trade_id,
                'enabled': True,
                'status': {'$nin': ['pending', 'active']},
            },
            {'$set': base_set},
        )
    except Exception as exc:
        log.warning('_disable_trade_feature_rows_for_new_cycle error trade=%s: %s', normalized_trade_id, exc)
    mark_execute_order_dirty_from_trade_id(db, normalized_trade_id)


# _compute_strategy_mtm_snapshot is now imported from trading_core.py (§8).
# Alias defined at top of file:
#   _compute_strategy_mtm_snapshot = compute_strategy_mtm
#
# trading_core.compute_strategy_mtm has the same signature and algorithm.
# It additionally includes `ltp` and `entry_price` in the legs snapshot
# (a superset — existing callers that ignore extra fields are unaffected).


def _is_reentered_leg(leg: dict, trade: dict) -> bool:
    """
    Returns True only for reentry / lazy legs.
    Parent legs (original ListOfLegConfigs entries) return False.
    """
    if leg.get('is_lazy'):
        return True
    if leg.get('triggered_by'):
        return True
    leg_id = str(leg.get('id') or '')
    # Build original parent leg id set: config.LegConfigs first, then strategy.ListOfLegConfigs
    original_leg_configs: dict = ((trade.get('config') or {}).get('LegConfigs') or {})
    if not original_leg_configs:
        strategy = trade.get('strategy') or {}
        list_of_legs = strategy.get('ListOfLegConfigs') or []
        original_leg_configs = {
            str(lc.get('id') or str(i)): lc
            for i, lc in enumerate(list_of_legs)
            if isinstance(lc, dict)
        }
    if leg_id and original_leg_configs and leg_id not in original_leg_configs:
        return True
    return False


def _build_position_history_doc(trade: dict, leg: dict) -> dict | None:
    entry_trade = leg.get('entry_trade') or {}
    entry_timestamp = str(entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or '').strip()
    if not entry_timestamp:
        return None
    # Store entry_iv from leg or entry_trade if available
    _entry_iv = (
        _safe_float(leg.get('entry_iv'))
        or _safe_float(entry_trade.get('entry_iv'))
        or _safe_float(entry_trade.get('iv'))
        or None
    )
    # Copy entry_trade and inject the resolved entry_iv so both top-level and nested fields match
    entry_trade_stored = dict(entry_trade)
    if _entry_iv is not None:
        entry_trade_stored['entry_iv'] = _entry_iv
    history_leg = {
        'id': str(leg.get('id') or ''),
        'status': int(leg.get('status') if leg.get('status') is not None else OPEN_LEG_STATUS),
        'token': leg.get('token') or str(entry_trade.get('instrument_token') or '').strip() or None,
        'symbol': leg.get('symbol'),
        'exchange': str(leg.get('exchange') or entry_trade.get('exchange') or ''),
        'entry_iv': _entry_iv,
        'quantity': _safe_int(leg.get('quantity')),
        'lot_size': _safe_int(leg.get('lot_size') or leg.get('quantity')),
        'position': leg.get('position'),
        'option': leg.get('option'),
        'expiry_date': _normalize_expiry_datetime(leg.get('expiry_date')),
        'strike': leg.get('strike'),
        'last_saw_price': _safe_float(leg.get('last_saw_price')),
        'entry_trade': entry_trade_stored,
        'exit_trade': leg.get('exit_trade') if leg.get('exit_trade') is not None else None,
        'is_reentered_leg': _is_reentered_leg(leg, trade),
        'transactions': leg.get('transactions') or {},
        'current_transaction_id': leg.get('current_transaction_id'),
        'initial_sl_value': leg.get('initial_sl_value', leg.get('current_sl_price')),
        'display_sl_value': leg.get('display_sl_value', leg.get('current_sl_price')),
        'display_target_value': leg.get('display_target_value'),
        'leg_type': str(leg.get('leg_type') or ''),
        'triggered_by': str(leg.get('triggered_by') or ''),
        'lazy_leg_ref': str(leg.get('lazy_leg_ref') or ''),
        'parent_leg_id': str(leg.get('parent_leg_id') or ''),
        'reentry_type': str(leg.get('reentry_type') or ''),
        'reentry_count_remaining': _safe_int(leg.get('reentry_count_remaining')),
        'lot_config_value': _safe_int(leg.get('lot_config_value') or 1),
        'momentum_base_price': _safe_float(leg.get('momentum_base_price')) or None,
        'momentum_target_price': _safe_float(leg.get('momentum_target_price')) or None,
        'momentum_reference_set_at': str(leg.get('momentum_reference_set_at') or ''),
        'momentum_triggered_at': str(leg.get('momentum_triggered_at') or ''),
    }
    ticker = str(
        (trade.get('strategy') or {}).get('Ticker')
        or (trade.get('config') or {}).get('Ticker')
        or trade.get('ticker')
        or ''
    )
    return {
        **history_leg,
        'trade_id': str(trade.get('_id') or ''),
        'strategy_id': str(trade.get('strategy_id') or ''),
        'strategy_name': str(trade.get('name') or ''),
        'group_name': str(((trade.get('portfolio') or {}).get('group_name') or '')),
        'ticker': ticker,
        'creation_ts': str(trade.get('creation_ts') or ''),
        'entry_timestamp': entry_timestamp,
        'leg_id': history_leg['id'],
        'history_type': 'position_entry',
        'created_at': _now_iso(),
    }


def _store_position_history(
    db: MongoData, trade: dict, leg: dict,
    override_leg_cfg: dict | None = None,
) -> tuple[bool, dict | None]:
    # For Dhan: resolve the correct Dhan security ID and store it in history
    # so exit/square-off can find the right token without re-lookup
    try:
        from features.broker_gateway import _active_broker  # type: ignore
        if _active_broker() == 'dhan':
            _underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '').strip().upper()
            _strike_raw = leg.get('strike')
            _expiry     = str(leg.get('expiry_date') or '')[:10]
            _opt        = str(leg.get('option') or '').upper()
            try:
                _s_int = int(float(str(_strike_raw))) if _strike_raw is not None else None
            except Exception:
                _s_int = None
            _strike_filter = {'$in': [_s_int, float(_s_int)]} if _s_int is not None else _strike_raw
            _at_doc = db._db['active_option_tokens'].find_one(
                {
                    'broker': 'dhan',
                    'instrument': {'$regex': f'^{_underlying}$', '$options': 'i'},
                    'strike': _strike_filter,
                    'option_type': {'$regex': f'^{_opt}$', '$options': 'i'},
                    'expiry': {'$regex': f'^{_expiry}'},
                },
                {'token': 1, 'tokens': 1, 'ws_segment': 1},
            )
            if _at_doc:
                _dhan_tok = str(_at_doc.get('token') or _at_doc.get('tokens') or '').strip()
                _dhan_seg = str(_at_doc.get('ws_segment') or 'NSE_FNO')
                if _dhan_tok:
                    leg = dict(leg)  # shallow copy — don't mutate caller's dict
                    leg['token']      = _dhan_tok
                    leg['ws_segment'] = _dhan_seg
                    print(f'[STORE_HIST_TOKEN] {_underlying} {_opt} {_strike_raw} {_expiry} → dhan_token={_dhan_tok} seg={_dhan_seg}')
                    # Subscribe this token to Dhan WS so we receive live ticks for it
                    try:
                        from features.dhan_ticker import dhan_ticker_manager as _dtm  # type: ignore
                        _dtm.subscribe_tokens([_dhan_tok], exchange=_dhan_seg)
                        print(f'[STORE_HIST_TOKEN] subscribed {_dhan_tok} to {_dhan_seg}')
                    except Exception as _sub_e:
                        print(f'[STORE_HIST_TOKEN] subscribe error: {_sub_e}')
                    # Get entry IV: Black-Scholes with entry_trade price + spot
                    try:
                        from features.live_option_chain import _bs  # type: ignore
                        from features.broker_gateway import broker_ticker_manager as _btm  # type: ignore
                        _et = leg.get('entry_trade') or {}
                        _iv_ltp = _safe_float(_et.get('price') or _et.get('trigger_price'))
                        # spot: live map first, fallback to underlying_at_trade stored in entry_trade
                        _iv_spot = (_safe_float(_btm.spot_map.get(_underlying))
                                    or _safe_float(_et.get('underlying_at_trade'))
                                    or _safe_float(_et.get('underlying_trigger_price')))
                        print(f'[ENTRY_IV_DEBUG] ltp={_iv_ltp} spot={_iv_spot} strike={_strike_raw} opt={_opt}')
                        if _iv_ltp > 0 and _iv_spot > 0 and _strike_raw is not None:
                            _iv_calc, _, _tte, _, _rfr, _div_yields, _def_div = _bs()
                            _iv_T = _tte(_expiry)
                            _iv_q = _div_yields.get(_underlying, _def_div)
                            _iv_raw = _iv_calc(_iv_ltp, _iv_spot, float(_strike_raw), _iv_T, _rfr, _opt, _iv_q)
                            _entry_iv_val = round(_iv_raw * 100, 2)
                            print(f'[ENTRY_IV_DEBUG] computed iv={_entry_iv_val}')
                            if _entry_iv_val > 0:
                                leg['entry_iv'] = _entry_iv_val
                    except Exception as _iv_e:
                        print(f'[ENTRY_IV_DEBUG] error: {_iv_e}')
    except Exception:
        pass

    history_doc = _build_position_history_doc(trade, leg)
    if not history_doc:
        return False, None
    query = {
        'trade_id': history_doc['trade_id'],
        'leg_id': history_doc['leg_id'],
        'entry_timestamp': history_doc['entry_timestamp'],
    }
    history_col = db._db['algo_trade_positions_history']
    existing = history_col.find_one(query, {'_id': 1})
    if existing:
        return False, history_doc
    _insert_preview = {k: history_doc.get(k) for k in ('trade_id', 'leg_id', 'token', 'symbol', 'strike', 'option', 'expiry_date', 'position', 'entry_iv', 'entry_timestamp', 'status')}
    print(f'[HIST_INSERT] algo_trade_positions_history data={_insert_preview}')
    result = history_col.insert_one(history_doc)
    inserted_id = str(result.inserted_id)
    history_doc['_id'] = inserted_id
    history_doc['id'] = inserted_id
    print(f'[HIST_INSERT] inserted_id={inserted_id}')
    try:
        history_col.update_one(
            {'_id': result.inserted_id},
            {'$set': {'id': inserted_id}},
        )
    except Exception as exc:
        log.error('_store_position_history id sync error trade=%s leg=%s: %s', history_doc['trade_id'], history_doc['leg_id'], exc)
    # Replace the original dict leg (same leg_id) with the history string _id.
    # This keeps legs as pure string IDs — no mixed dict/string state.
    leg_id_val = history_doc.get('leg_id', '')
    trade_id_val = history_doc['trade_id']
    try:
        # Remove ALL dict-type legs with matching id (pending placeholders + any stale dicts)
        db._db['algo_trades'].update_one(
            {'_id': trade_id_val},
            {'$pull': {'legs': {'id': leg_id_val}}},
        )
    except Exception as exc:
        log.error('_store_position_history legs pull error trade=%s leg=%s: %s', trade_id_val, leg_id_val, exc)
    try:
        # Safety: remove any remaining non-string dict objects from legs[]
        # legs[] must contain only string history IDs for live/fast-forward trades
        current_trade = db._db['algo_trades'].find_one({'_id': trade_id_val}, {'legs': 1, 'activation_mode': 1}) or {}
        if str(current_trade.get('activation_mode') or '').strip() in {'live', 'fast-forward', 'forward-test'}:
            clean_legs = [l for l in (current_trade.get('legs') or []) if isinstance(l, str)]
            db._db['algo_trades'].update_one(
                {'_id': trade_id_val},
                {'$set': {'legs': clean_legs}},
            )
    except Exception as exc:
        log.error('_store_position_history legs clean error trade=%s: %s', trade_id_val, exc)
    try:
        db._db['algo_trades'].update_one(
            {'_id': trade_id_val},
            {'$push': {'legs': inserted_id}},
        )
    except Exception as exc:
        log.error('_store_position_history legs push error trade=%s: %s', trade_id_val, exc)
    print(
        f"[POSITION ENTRY] trade={history_doc['trade_id']} "
        f"leg={history_doc['leg_id']} "
        f"entry={history_doc['entry_timestamp']} "
        f"history_id={inserted_id}"
    )

    # ── Resolve leg_cfg once — used by both notification calls below ──────────
    resolved_leg_cfg: dict = override_leg_cfg or {}
    if not resolved_leg_cfg:
        try:
            all_leg_cfgs = _resolve_trade_leg_configs(trade)
            resolved_leg_cfg = _resolve_leg_cfg(str(leg.get('id') or ''), leg, all_leg_cfgs)
        except Exception as exc:
            log.warning('_store_position_history leg_cfg lookup error: %s', exc)

    # ── Notification: entry_taken ──────────────────────────────────────────
    try:
        from features.notification_manager import record_entry_taken
        from features.position_manager import parse_overall_sl, parse_overall_tgt
        strategy_cfg = trade.get('strategy') or trade.get('config') or {}
        osl_type, osl_val = parse_overall_sl(strategy_cfg)
        otgt_type, otgt_val = parse_overall_tgt(strategy_cfg)
        record_entry_taken(
            db._db, trade, leg, resolved_leg_cfg,
            timestamp=history_doc.get('entry_timestamp') or inserted_id,
            overall_sl_type=osl_type, overall_sl_value=osl_val,
            overall_tgt_type=otgt_type, overall_tgt_value=otgt_val,
        )
    except Exception as _ne:
        log.warning('notification entry_taken error: %s', _ne)

    # ── Leg feature status: SL / Target / TrailSL records ────────────────────
    try:
        from features.notification_manager import record_leg_features_at_entry
        feature_leg_id = inserted_id
        # Parent legs and their direct reentries are tracked in runtime flows by the
        # business leg id (for example `callLeg1` / `callLeg1_re_...`). Seed those
        # feature rows with the same id so cycle 2+ feature state stays attached to
        # the parent leg lineage instead of only the history document id.
        if not bool(leg.get('is_lazy')):
            feature_leg_id = str(history_doc.get('leg_id') or leg.get('id') or inserted_id)
        record_leg_features_at_entry(
            db._db, trade, leg, resolved_leg_cfg,
            timestamp=history_doc.get('entry_timestamp') or inserted_id,
            feature_leg_id=feature_leg_id,
        )
    except Exception as _lfe:
        log.warning('leg_feature_status entry error: %s', _lfe)

    try:
        from features.notification_manager import upsert_simple_momentum_feature_status
        momentum_cfg = resolved_leg_cfg.get('LegMomentum') or {}
        momentum_type = str(momentum_cfg.get('Type') or 'None')
        momentum_value = _safe_float(momentum_cfg.get('Value'))
        momentum_base_price = _safe_float(leg.get('momentum_base_price'))
        momentum_target_price = _safe_float(leg.get('momentum_target_price'))
        if (
            'None' not in momentum_type
            and momentum_type
            and momentum_value > 0
            and momentum_base_price > 0
            and momentum_target_price > 0
        ):
            feature_leg = dict(leg)
            feature_leg['_id'] = inserted_id
            upsert_simple_momentum_feature_status(
                db._db,
                trade,
                feature_leg,
                resolved_leg_cfg,
                timestamp=history_doc.get('entry_timestamp') or inserted_id,
                base_price=momentum_base_price,
                target_price=momentum_target_price,
                current_price=_safe_float((leg.get('entry_trade') or {}).get('price')),
                status='triggered' if str(leg.get('momentum_triggered_at') or '').strip() else 'pending',
                enabled=False if str(leg.get('momentum_triggered_at') or '').strip() else True,
            )
        print(
            '[ENTRY FEATURE SEED]',
            {
                'trade_id': history_doc.get('trade_id'),
                'leg_id': history_doc.get('leg_id'),
                'history_leg_id': inserted_id,
                'sl_enabled': bool(resolved_leg_cfg.get('LegStopLoss')),
                'target_enabled': bool(resolved_leg_cfg.get('LegTarget')),
                'trail_enabled': bool(resolved_leg_cfg.get('LegTrailSL')),
                'momentum_type': str((resolved_leg_cfg.get('LegMomentum') or {}).get('Type') or 'None'),
            },
        )
    except Exception as _mfe:
        log.warning('simple_momentum feature seed error: %s', _mfe)

    mark_execute_order_dirty_from_trade(trade)

    # ── Simulator order recording (fast-forward + live_sim_order flag) ────────
    try:
        from features.live_simulator_order import is_simulator_order_enabled, record_entry_with_orders  # type: ignore
        if is_simulator_order_enabled(trade):
            record_entry_with_orders(db, trade, leg, resolved_leg_cfg)
    except Exception as _soe:
        log.warning('simulator_order record error trade=%s leg=%s: %s',
                    history_doc.get('trade_id'), history_doc.get('leg_id'), _soe)

    return True, history_doc


def _validate_trade_leg_storage(db: MongoData, trade_id: str) -> bool:
    normalized_trade_id = str(trade_id or '').strip()
    if not normalized_trade_id:
        return False
    trade = db._db['algo_trades'].find_one({'_id': normalized_trade_id}, {'legs': 1}) or {}
    legs = trade.get('legs') if isinstance(trade.get('legs'), list) else []
    invalid_legs = []
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else None
        exit_trade = leg.get('exit_trade') if isinstance(leg.get('exit_trade'), dict) else None
        if isinstance(entry_trade, dict) and str(entry_trade.get('order_status') or '').strip().upper() == 'OPEN':
            continue
        if entry_trade or exit_trade:
            invalid_legs.append({
                'id': str(leg.get('id') or '').strip(),
                'has_entry_trade': bool(entry_trade),
                'has_exit_trade': bool(exit_trade),
            })
    if invalid_legs:
        print(
            '[LEGS STORAGE ERROR]',
            {
                'trade_id': normalized_trade_id,
                'message': 'Entered/closed legs must be stored as string history ids only',
                'invalid_legs': invalid_legs,
            },
        )
        return False
    return True


def _update_position_history_exit(
    db: MongoData,
    trade_id: str,
    leg_id: str,
    exit_price: float,
    exit_reason: str,
    now_ts: str,
    exit_iv: float | None = None,
    exit_vix: float | None = None,
) -> None:
    """Update exit_trade in algo_trade_positions_history when a leg is closed."""
    exit_trade_payload = {
        'trigger_timestamp': now_ts,
        'trigger_price': exit_price,
        'price': exit_price,
        'traded_timestamp': now_ts,
        'exchange_timestamp': now_ts,
        'exit_reason': exit_reason,
    }
    if exit_iv is not None:
        exit_trade_payload['exit_iv'] = exit_iv
    if exit_vix is not None:
        exit_trade_payload['exit_vix'] = exit_vix
    history_matchers = [
        {'leg_id': leg_id},
        {'id': leg_id},
    ]
    try:
        history_matchers.append({'_id': ObjectId(str(leg_id))})
    except Exception:
        pass
    try:
        _q = {'trade_id': trade_id, 'exit_trade': None, '$or': history_matchers}
        _existing = db._db['algo_trade_positions_history'].find_one(
            {'trade_id': trade_id}, {'leg_id': 1, 'id': 1, 'exit_trade': 1, '_id': 0}
        )
        _exit_set = {
            'exit_trade':   exit_trade_payload,
            'history_type': 'position_closed',
            'status':       2,
            'exit_timestamp': now_ts,
            'last_saw_price': exit_price,
        }
        print(f'[HIST_UPDATE][EXIT] trade={trade_id} leg={leg_id} query={_q} existing_sample={_existing}')
        print(f'[HIST_UPDATE][EXIT] set_data={_exit_set}')
        result = db._db['algo_trade_positions_history'].update_one(_q, {'$set': _exit_set})
        print(f'[HIST_UPDATE][EXIT] modified_count={result.modified_count} matched_count={result.matched_count}')
        if result.modified_count > 0:
            print(
                f"[POSITION EXIT] trade={trade_id} "
                f"leg={leg_id} "
                f"exit={now_ts} "
                f"price={exit_price} "
                f"reason={exit_reason}"
            )
        else:
            # Check what's in history for this trade — helps diagnose mismatch
            _all = list(db._db['algo_trade_positions_history'].find(
                {'trade_id': trade_id},
                {'leg_id': 1, 'id': 1, 'exit_trade': 1, '_id': 0}
            ))
            print(f'[POS_EXIT_DEBUG] NO_MATCH trade={trade_id} leg={leg_id} all_history={_all}')
    except Exception as exc:
        log.error('_update_position_history_exit error trade=%s leg=%s: %s', trade_id, leg_id, exc)
        print(f'[POS_EXIT_DEBUG] EXCEPTION trade={trade_id} leg={leg_id}: {exc}')
    mark_execute_order_dirty_from_trade_id(db, trade_id)


def _resolve_chain_market_price(chain_doc: dict | None) -> float:
    if not isinstance(chain_doc, dict):
        return 0.0
    for field_name in ('close', 'ltp', 'current_price', 'price', 'last_price'):
        resolved_price = _safe_float(chain_doc.get(field_name))
        if resolved_price > 0:
            return resolved_price
    return 0.0


def _normalize_chain_market_fields(chain_doc: dict | None) -> dict:
    if not isinstance(chain_doc, dict):
        return {}
    normalized_doc = dict(chain_doc)
    close_price = _safe_float(normalized_doc.get('close'))
    if close_price <= 0:
        return normalized_doc
    for field_name in ('ltp', 'current_price', 'price', 'last_price'):
        if _safe_float(normalized_doc.get(field_name)) <= 0:
            normalized_doc[field_name] = close_price
    return normalized_doc


# Per-process memo of underlying/expiry/strike/option_type → (dhan_token, dhan_ws_seg).
# active_option_tokens is the broker's contract master — once resolved for a contract it
# doesn't change intraday, so this turns what used to be a regex find_one() on EVERY tick
# of EVERY open leg into a single Mongo round-trip per unique contract per process lifetime.
_DHAN_TOKEN_RESOLVE_CACHE: dict[tuple, tuple[str, str] | None] = {}


def _resolve_quote_first_option_price(
    db: MongoData,
    trade: dict,
    leg: dict,
    *,
    activation_mode: str,
    fallback_price: float = 0.0,
) -> float:
    # For Dhan broker: use REST quotes directly (WebSocket and historical data can be stale)
    try:
        from features.broker_gateway import _active_broker, get_broker_rest_quotes  # type: ignore
        if _active_broker() == 'dhan':
            leg_token = str(leg.get('token') or '').strip()
            ws_seg    = str(leg.get('ws_segment') or 'NSE_FNO')

            # leg['token'] may be a Kite/historical token — look up real Dhan security ID
            # from active_option_tokens using instrument/expiry/strike/option_type/broker
            underlying  = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '').strip().upper()
            strike_raw  = leg.get('strike')
            expiry_date = str(leg.get('expiry_date') or '')[:10]
            option_type = str(leg.get('option') or '').upper()

            # Normalize strike: try int first, then float, for DB type tolerance
            try:
                strike_int = int(float(str(strike_raw))) if strike_raw is not None else None
            except Exception:
                strike_int = None

            cache_key = (underlying, expiry_date, strike_int, option_type)
            if cache_key in _DHAN_TOKEN_RESOLVE_CACHE:
                cached = _DHAN_TOKEN_RESOLVE_CACHE[cache_key]
            else:
                dhan_token  = ''
                dhan_ws_seg = ws_seg
                try:
                    # Build strike filter: match int OR float stored in DB
                    _strike_filter = {'$in': [strike_int, float(strike_int)]} if strike_int is not None else strike_raw
                    at_doc = db._db['active_option_tokens'].find_one(
                        {
                            'broker': 'dhan',
                            'instrument': {'$regex': f'^{underlying}$', '$options': 'i'},
                            'strike': _strike_filter,
                            'option_type': {'$regex': f'^{option_type}$', '$options': 'i'},
                            'expiry': {'$regex': f'^{expiry_date}'},
                        },
                        {'token': 1, 'tokens': 1, 'ws_segment': 1},
                    )
                    if at_doc:
                        dhan_token  = str(at_doc.get('token') or at_doc.get('tokens') or '').strip()
                        dhan_ws_seg = str(at_doc.get('ws_segment') or ws_seg)
                    else:
                        debug_print(f'[EXIT_TOKEN_DEBUG] NO_AT_DOC strike_raw={strike_raw} strike_int={strike_int} '
                                     f'opt={option_type} expiry={expiry_date}')
                except Exception as _e:
                    debug_print(f'[EXIT_TOKEN_DEBUG] at_lookup_error: {_e}')

                cached = (dhan_token, dhan_ws_seg) if dhan_token else None
                _DHAN_TOKEN_RESOLVE_CACHE[cache_key] = cached
                debug_print(f'[EXIT_TOKEN_DEBUG] resolved underlying={underlying} leg_token={leg_token} '
                             f'strike={strike_raw} expiry={expiry_date} opt={option_type} -> cached={cached}')

            dhan_token, dhan_ws_seg = cached if cached else ('', ws_seg)

            # prefer real Dhan token, fall back to leg token
            use_token = dhan_token or leg_token
            if use_token:
                try:
                    quotes = get_broker_rest_quotes([use_token], db._db, {use_token: dhan_ws_seg})
                    ltp = _safe_float((quotes.get(use_token) or {}).get('ltp'))
                    if ltp > 0:
                        return ltp
                except Exception as _e:
                    debug_print(f'[EXIT_TOKEN_DEBUG] REST error: {_e}')
    except Exception:
        pass

    symbol = str(leg.get('symbol') or '').strip()
    if activation_mode in ('fast-forward', 'forward-test'):
        try:
            from features.fast_forward_event import _get_fast_forward_quote_price
            quote_price = _safe_float(_get_fast_forward_quote_price(db, trade, symbol))
            if quote_price > 0:
                return quote_price
        except Exception:
            pass
    elif activation_mode == 'live':
        try:
            from features.live_event import get_live_option_quote_price
            quote_price = _safe_float(get_live_option_quote_price(db, trade, symbol))
            if quote_price > 0:
                return quote_price
        except Exception:
            pass
    return _safe_float(fallback_price)


def _resolve_simple_momentum_target(base_price: float, momentum_type: str, momentum_value: float) -> float:
    base = _safe_float(base_price)
    value = _safe_float(momentum_value)
    if base <= 0 or value <= 0:
        return 0.0
    if 'Percentage' in str(momentum_type or ''):
        delta = base * (value / 100.0)
    else:
        delta = value
    if 'Down' in str(momentum_type or ''):
        return round(base - delta, 2)
    return round(base + delta, 2)


def _is_simple_momentum_triggered(current_price: float, target_price: float, momentum_type: str) -> bool:
    current = _safe_float(current_price)
    target = _safe_float(target_price)
    if current <= 0 or target <= 0:
        return False
    if 'Down' in str(momentum_type or ''):
        return current <= target
    return current >= target


# ─── Re-entry / lazy-leg helpers ──────────────────────────────────────────────

def _build_pending_leg(leg_id: str, leg_config: dict, trade: dict,
                        now_ts: str, triggered_by: str,
                        leg_type: str = '') -> dict:
    """
    Build a new leg dict (status=OPEN_LEG_STATUS, no entry_trade yet) from a leg config.
    Supports both config.LegConfigs (ContractType-nested) and strategy.ListOfLegConfigs (flat) format.
    The next live tick will try to fill the entry price from option_chain.
    """
    contract = leg_config.get('ContractType') or {}

    # option: ContractType.Option OR extract from InstrumentKind ("LegType.CE" → "CE")
    option_raw = contract.get('Option') or ''
    if not option_raw:
        instrument = str(leg_config.get('InstrumentKind') or '')
        option_raw = instrument.split('.')[-1] if '.' in instrument else instrument

    # expiry_kind: ContractType.Expiry OR ExpiryKind
    expiry_kind = contract.get('Expiry') or str(leg_config.get('ExpiryKind') or '')

    # strike_parameter: ContractType.StrikeParameter OR StrikeParameter
    strike_parameter = contract.get('StrikeParameter') or str(leg_config.get('StrikeParameter') or 0)

    # entry_kind: ContractType.EntryKind OR EntryType
    entry_kind = contract.get('EntryKind') or str(leg_config.get('EntryType') or '')

    # lot_config_value: how many lots (multiplier on lot_size)
    lot_config_value = max(1, int((leg_config.get('LotConfig') or {}).get('Value') or 1))

    return {
        'id': leg_id,
        'status': OPEN_LEG_STATUS,
        'position': str(leg_config.get('PositionType') or ''),
        'option': option_raw,
        'expiry_kind': expiry_kind,
        'strike_parameter': strike_parameter,
        'entry_kind': entry_kind,
        'entry_trade': None,
        'quantity': 0,                # will be lot_size * lot_config_value on actual entry
        'lot_config_value': lot_config_value,
        'last_saw_price': 0.0,
        'current_sl_price': None,
        'strike': None,
        'expiry_date': None,
        'token': None,
        'symbol': None,
        'triggered_by': triggered_by,
        'queued_at': now_ts,
        'is_lazy': True,
        'leg_type': leg_type,
        'momentum_base_price': None,
        'momentum_target_price': None,
        'momentum_reference_set_at': None,
        'momentum_triggered_at': None,
        'momentum_armed_notified_at': None,
        'momentum_triggered_notified_at': None,
    }


def _queue_original_legs_if_needed(db: MongoData, trade: dict, now_ts: str) -> tuple[bool, dict]:
    """
    Ensure original parent legs from config exist in algo_trades.legs.
    Supports both config.LegConfigs (dict keyed by id) and strategy.ListOfLegConfigs (list).
    Returns (created_any, refreshed_trade).
    """
    trade_id = str(trade.get('_id') or '')
    activation_mode = str(trade.get('activation_mode') or '').strip()
    strict_history_mode = _is_strict_history_leg_mode(activation_mode)

    # Build leg_configs dict: prefer config.LegConfigs, fallback to strategy.ListOfLegConfigs
    leg_configs: dict = ((trade.get('config') or {}).get('LegConfigs') or {})
    if not leg_configs:
        strategy = trade.get('strategy') or {}
        list_of_legs = strategy.get('ListOfLegConfigs') or []
        leg_configs = {
            str(lc.get('id') or str(i)): lc
            for i, lc in enumerate(list_of_legs)
            if isinstance(lc, dict)
        }

    # Dict legs still present (not yet synced to history)
    existing_ids = {
        str(leg.get('id') or '')
        for leg in (trade.get('legs') or [])
        if isinstance(leg, dict)
    }
    # Also include leg_ids already stored in algo_trade_positions_history for this trade
    # so that socket restarts don't re-queue legs that were already entered
    try:
        history_col = db._db['algo_trade_positions_history']
        history_leg_ids = {
            str(doc.get('leg_id') or '')
            for doc in history_col.find({'trade_id': trade_id}, {'leg_id': 1})
        }
        existing_ids |= history_leg_ids
    except Exception as exc:
        log.warning('_queue_original_legs_if_needed history check error trade=%s: %s', trade_id, exc)

    # Also include leg_ids already queued as momentum_pending in algo_leg_feature_status
    # so that socket restarts don't double-queue parent legs with LegMomentum.
    # IMPORTANT: also add lazy_leg_ref (the original config leg id) — OverallReentry
    # queues legs with triggered_by='overall_<orig_id>', so the stored leg_id becomes
    # '<orig_id>-overall_<orig_id>' which doesn't match the config key '<orig_id>'.
    # Adding lazy_leg_ref prevents _queue_original_legs_if_needed from creating a
    # duplicate momentum_pending for the same underlying leg after reentry.
    try:
        momentum_queued_ids: set[str] = set()
        for _doc in db._db['algo_leg_feature_status'].find(
            {
                'trade_id': trade_id,
                'feature': {'$in': ['momentum_pending', 'pending_entry']},
                # Include triggered/processing — feature marked triggered means leg was entered.
                # Checking only 'active' caused duplicate entries (TOCTOU race condition).
                'status': {'$in': ['active', 'triggered', 'processing']},
            },
            {'leg_id': 1, 'lazy_leg_ref': 1},
        ):
            momentum_queued_ids.add(str(_doc.get('leg_id') or ''))
            _lazy_ref = str(_doc.get('lazy_leg_ref') or '').strip()
            if _lazy_ref:
                momentum_queued_ids.add(_lazy_ref)
        existing_ids |= momentum_queued_ids
    except Exception as exc:
        log.warning('_queue_original_legs_if_needed momentum_pending check error trade=%s: %s', trade_id, exc)

    created_any = False

    for idx, (leg_id, leg_config) in enumerate(leg_configs.items()):
        if leg_id in existing_ids:
            continue
        leg_type = f'leg_{idx + 1}'

        # Parent leg with LegMomentum → same flow as IdleLeg + LegMomentum:
        # queue to algo_leg_feature_status as momentum_pending.
        # _process_momentum_pending_feature_legs enters it when momentum triggers
        # and pushes the history ID into legs[].
        if _has_momentum_config(leg_config):
            queued = _queue_lazy_momentum_to_feature_status(
                db, trade_id, leg_id, leg_config,
                triggered_by='',
                leg_type=leg_type,
                now_ts=now_ts,
            )
            if queued:
                print(
                    f'[LEG MOMENTUM QUEUED] trade={trade_id} leg_id={leg_id} leg_type={leg_type} '
                    f'momentum={leg_config.get("LegMomentum")}'
                )
                created_any = True
            continue

        if strict_history_mode:
            queued = _queue_pending_entry_feature_status(
                db,
                trade,
                str(leg_id),
                leg_config,
                leg_type=leg_type,
                now_ts=now_ts,
                is_lazy=False,
            )
            if queued:
                created_any = True
            continue

        new_leg = _build_pending_leg(str(leg_id), leg_config, trade, now_ts, '', leg_type=leg_type)
        new_leg['is_lazy'] = False
        push_new_leg_in_db(db, trade_id, new_leg)
        print(
            f'[LEG QUEUED] trade={trade_id} leg_id={leg_id} leg_type={leg_type} '
            f'option={new_leg.get("option")} expiry_kind={new_leg.get("expiry_kind")} '
            f'position={new_leg.get("position")} lot_config_value={new_leg.get("lot_config_value")}'
        )
        created_any = True

    if not created_any:
        return False, trade
    refreshed_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or trade
    return True, refreshed_trade


def _get_parent_leg_type(trade: dict, triggered_by: str) -> str:
    """Find the leg_type of the parent leg by its id. Falls back to triggered_by id."""
    for leg in (trade.get('legs') or []):
        if isinstance(leg, dict) and str(leg.get('id') or '') == triggered_by:
            return str(leg.get('leg_type') or triggered_by or '')
    return triggered_by or ''


def _resolve_leg_cfg(leg_id: str, leg: dict, all_leg_configs: dict) -> dict:
    """
    Resolve leg config for SL / Target / TrailSL / NextLeg checks.

    Lazy legs entered via momentum have a composite id such as
    'putLeg1-ttx2772l' while their config is stored under 'putLeg1' in
    IdleLegConfigs.  Standard dict-get misses these, causing tsl_enabled=False
    and NextLeg chaining to silently skip.

    Resolution priority:
      1. Exact id match  (original legs, non-momentum lazy legs)
      2. lazy_leg_ref field  (momentum lazy legs created after the fix was applied)
      3. triggered_by field  (parent-leg direct reentries like leg1_re_...)
      4. Parent reentry base-id prefix match  (leg1_re_20260416...)
      5. Longest dash-prefix match  (momentum lazy legs from existing/pre-fix history docs)
    """
    # 1. Exact match
    cfg = all_leg_configs.get(leg_id)
    if cfg:
        return cfg

    # 2. Stored lazy_leg_ref  (set by _process_momentum_pending_feature_legs)
    #    This must win over triggered_by for momentum-entered lazy legs:
    #    triggered_by points to the parent leg, while lazy_leg_ref points to the
    #    actual IdleLegConfigs key whose SL/Target/NextLeg chain should be used.
    lazy_ref = str(leg.get('lazy_leg_ref') or '').strip()
    if lazy_ref:
        cfg = all_leg_configs.get(lazy_ref)
        if cfg:
            return cfg

    # 3. Direct parent reentries keep the original parent id in triggered_by.
    #    Example: leg_id='putLeg2_re_20260416...', triggered_by='putLeg2'
    triggered_by = str(leg.get('triggered_by') or '').strip()
    if triggered_by:
        cfg = all_leg_configs.get(triggered_by)
        if cfg:
            return cfg

    # 4. Parent immediate/at-cost/like-original reentries use "<orig>_re_<ts>" ids.
    #    Recover the original parent leg config from that base id.
    if '_re_' in leg_id:
        reentry_base = leg_id.split('_re_', 1)[0].strip()
        if reentry_base:
            cfg = all_leg_configs.get(reentry_base)
            if cfg:
                return cfg

    # 5. Longest-prefix match — handles existing history docs without lazy_leg_ref.
    #    'putLeg1-ttx2772l'.startswith('putLeg1-') → True → use putLeg1 config.
    #    Longest key wins so 'callLeg1Extra' beats 'callLeg1' for 'callLeg1Extra-xyz'.
    best_key = ''
    for k in all_leg_configs:
        if leg_id.startswith(k + '-') and len(k) > len(best_key):
            best_key = k
    if best_key:
        return all_leg_configs[best_key]

    return {}


def _count_child_legs(trade: dict, triggered_by: str, is_lazy: bool) -> int:
    """Count how many reentry or lazy legs already exist for a parent."""
    base_count = sum(
        1 for leg in (trade.get('legs') or [])
        if isinstance(leg, dict)
        and str(leg.get('triggered_by') or '') == triggered_by
        and bool(leg.get('is_lazy')) == is_lazy
    )
    pending_count = sum(
        1 for leg in (trade.get('pending_feature_legs') or [])
        if isinstance(leg, dict)
        and str(leg.get('triggered_by') or '') == triggered_by
        and bool(leg.get('is_lazy')) == is_lazy
    )
    return base_count + pending_count


def _has_momentum_config(leg_cfg: dict) -> bool:
    """Return True if the leg config has a non-None momentum type with value > 0."""
    mom = leg_cfg.get('LegMomentum') or {}
    mom_type = str(mom.get('Type') or 'None')
    mom_val = _safe_float(mom.get('Value'))
    return 'None' not in mom_type and mom_val > 0


def _build_momentum_feature_leg_id(lazy_ref: str, triggered_by: str) -> str:
    lazy_leg_ref = str(lazy_ref or '').strip()
    parent_leg_id = str(triggered_by or '').strip()
    if lazy_leg_ref and parent_leg_id:
        return f'{lazy_leg_ref}-{parent_leg_id}'
    return lazy_leg_ref or parent_leg_id


def _queue_lazy_momentum_to_feature_status(
    db: MongoData, trade_id: str, lazy_ref: str, lazy_cfg: dict,
    triggered_by: str, leg_type: str, now_ts: str
) -> bool:
    """
    Queue a lazy leg with momentum into algo_leg_feature_status (status='active')
    instead of pushing it directly into the legs array.
    Returns True if successfully queued, False if already exists.
    """
    feature_col = db._db['algo_leg_feature_status']
    queued_leg_id = _build_momentum_feature_leg_id(lazy_ref, triggered_by)
    mom = lazy_cfg.get('LegMomentum') or {}
    option_raw = str(lazy_cfg.get('InstrumentKind') or '')
    option_type = option_raw.split('.')[-1] if '.' in option_raw else option_raw
    trade_doc = db._db['algo_trades'].find_one({'_id': trade_id}, {'ticker': 1, 'config.Ticker': 1, 'strategy.Ticker': 1}) or {}
    underlying = str(
        (trade_doc.get('strategy') or {}).get('Ticker')
        or (trade_doc.get('config') or {}).get('Ticker')
        or trade_doc.get('ticker')
        or ''
    ).strip().upper()

    doc = {
        'trade_id': trade_id,
        'leg_id': queued_leg_id,
        'lazy_leg_ref': lazy_ref,
        'parent_leg_id': triggered_by,
        'feature': 'momentum_pending',
        'enabled': True,
        'status': 'active',
        'underlying': underlying,
        'option': option_type,
        'expiry_kind': str(lazy_cfg.get('ExpiryKind') or 'ExpiryType.Weekly'),
        'strike_parameter': str(lazy_cfg.get('StrikeParameter') or 'StrikeType.ATM'),
        'entry_kind': str(lazy_cfg.get('EntryType') or 'EntryType.EntryByStrikeType'),
        'position': str(lazy_cfg.get('PositionType') or ''),
        'lot_config_value': int((lazy_cfg.get('LotConfig') or {}).get('Value') or 1),
        'triggered_by': triggered_by,
        'leg_type': leg_type,
        'momentum_type': str(mom.get('Type') or 'None'),
        'momentum_value': _safe_float(mom.get('Value')),
        'momentum_base_price': None,
        'momentum_target_price': None,
        'strike': None,
        'expiry_date': None,
        'token': None,
        'symbol': None,
        'queued_at': now_ts,
        'armed_at': None,
        'triggered_at': None,
    }
    try:
        result = feature_col.update_one(
            {
                'trade_id': trade_id,
                'leg_id': queued_leg_id,
                'feature': 'momentum_pending',
                'status': 'active',
            },
            {'$setOnInsert': doc},
            upsert=True,
        )
        if not result.upserted_id:
            log.info('momentum_pending already active for trade=%s leg=%s — skipping', trade_id, queued_leg_id)
            return False
        print(
            f'[MOMENTUM QUEUED] trade={trade_id} leg={queued_leg_id} parent={triggered_by} '
            f'type={doc["momentum_type"]} value={doc["momentum_value"]} option={option_type} leg_type={leg_type}'
        )
        mark_execute_order_dirty_from_trade_id(db, trade_id)
        return True
    except Exception as exc:
        log.error('_queue_lazy_momentum_to_feature_status insert error: %s', exc)
        return False


def _queue_pending_entry_feature_status(
    db: MongoData,
    trade: dict,
    leg_id: str,
    leg_cfg: dict,
    *,
    triggered_by: str = '',
    leg_type: str = '',
    now_ts: str,
    reentry_type: str = '',
    reentry_count_remaining: int | None = None,
    is_lazy: bool = False,
    skip_momentum_check: bool = False,
    strike: Any = None,
    expiry_date: Any = None,
    token: Any = None,
    symbol: Any = None,
    momentum_base_price: Any = None,
    momentum_target_price: Any = None,
    momentum_reference_set_at: Any = None,
    momentum_armed_notified_at: Any = None,
    spot_at_queue: Any = None,
) -> bool:
    trade_id = str(trade.get('_id') or '').strip()
    normalized_leg_id = str(leg_id or '').strip()
    if not trade_id or not normalized_leg_id:
        return False

    feature_col = db._db['algo_leg_feature_status']
    option_raw = str(
        ((leg_cfg.get('ContractType') or {}).get('Option'))
        or leg_cfg.get('option')
        or leg_cfg.get('option_type')
        or leg_cfg.get('InstrumentKind')
        or ''
    )
    option_type = option_raw.split('.')[-1] if '.' in option_raw else option_raw
    expiry_kind = str(
        ((leg_cfg.get('ContractType') or {}).get('Expiry'))
        or leg_cfg.get('expiry_kind')
        or leg_cfg.get('ExpiryKind')
        or 'ExpiryType.Weekly'
    )
    strike_parameter = str(
        ((leg_cfg.get('ContractType') or {}).get('StrikeParameter'))
        or leg_cfg.get('strike_parameter')
        or leg_cfg.get('StrikeParameter')
        or 'StrikeType.ATM'
    )
    entry_kind = str(
        ((leg_cfg.get('ContractType') or {}).get('EntryType'))
        or leg_cfg.get('entry_kind')
        or leg_cfg.get('EntryType')
        or 'EntryType.EntryByStrikeType'
    )
    ticker = str(
        (trade.get('strategy') or {}).get('Ticker')
        or (trade.get('config') or {}).get('Ticker')
        or trade.get('ticker')
        or ''
    ).strip()
    live_snapshot = _build_live_pending_entry_snapshot(
        db,
        trade,
        dict(leg_cfg, id=normalized_leg_id),
        now_ts=now_ts,
    )
    lot_config_value = max(
        1,
        _safe_int(
            ((leg_cfg.get('LotConfig') or {}).get('Value'))
            or leg_cfg.get('lot_config_value')
            or 1
        ),
    )

    doc = {
        'trade_id': trade_id,
        'strategy_id': str(trade.get('strategy_id') or ''),
        'strategy_name': str(trade.get('name') or ''),
        'ticker': ticker,
        'trade_date': str(now_ts or '').replace('T', ' ')[:10],
        'feature': 'pending_entry',
        'feature_scope': 'leg',
        'enabled': True,
        'status': 'active',
        'leg_id': normalized_leg_id,
        'lazy_leg_ref': str(leg_cfg.get('lazy_leg_ref') or leg_cfg.get('id') or normalized_leg_id),
        'parent_leg_id': str(leg_cfg.get('parent_leg_id') or triggered_by or ''),
        'triggered_by': str(triggered_by or ''),
        'leg_type': str(leg_type or normalized_leg_id),
        'position': str(leg_cfg.get('PositionType') or leg_cfg.get('position') or ''),
        'option': option_type,
        'expiry_kind': expiry_kind,
        'strike_parameter': strike_parameter,
        'entry_kind': entry_kind,
        'lot_config_value': lot_config_value,
        'is_lazy': bool(is_lazy),
        'skip_momentum_check': bool(skip_momentum_check),
        'reentry_type': str(reentry_type or ''),
        'reentry_count_remaining': (
            None if reentry_count_remaining is None
            else max(0, _safe_int(reentry_count_remaining))
        ),
        'strike': (
            live_snapshot.get('strike')
            if live_snapshot.get('strike') not in (None, '')
            else strike
        ),
        'expiry_date': _normalize_expiry_datetime(
            live_snapshot.get('expiry_date') or expiry_date
        ),
        'token': str(live_snapshot.get('token') or token or ''),
        'symbol': str(live_snapshot.get('symbol') or symbol or ''),
        'spot_at_queue': _safe_float(
            live_snapshot.get('spot_at_queue') or spot_at_queue
        ) or None,
        'momentum_base_price': _safe_float(momentum_base_price) or None,
        'momentum_target_price': _safe_float(momentum_target_price) or None,
        'momentum_reference_set_at': str(momentum_reference_set_at or ''),
        'momentum_armed_notified_at': str(momentum_armed_notified_at or ''),
        'queued_at': now_ts,
        'armed_at': None,
        'triggered_at': None,
        'trigger_description': (
            f'Pending entry queued for {option_type or "-"} leg {normalized_leg_id}.'
        ),
        'created_at': now_ts,
        'updated_at': now_ts,
    }
    try:
        # Filter must include triggered/processing — if feature was already entered
        # (status='triggered'), we must NOT insert a new 'active' feature.
        # Using only status='active' caused duplicate entries: after first entry the
        # feature becomes 'triggered', upsert misses it, inserts a new 'active' doc.
        existing = feature_col.find_one(
            {
                'trade_id': trade_id,
                'leg_id': normalized_leg_id,
                'feature': 'pending_entry',
                'status': {'$in': ['active', 'triggered', 'processing']},
            },
            {'_id': 1},
        )
        if existing:
            return False  # already queued or already entered
        result = feature_col.insert_one(doc)
        if not result.inserted_id:
            return False
        print(
            f'[PENDING ENTRY QUEUED] trade={trade_id} leg={normalized_leg_id} '
            f'leg_type={doc["leg_type"]} reentry_type={doc["reentry_type"] or "-"} '
            f'is_lazy={doc["is_lazy"]} mode={str(trade.get("activation_mode") or "")}'
        )
        mark_execute_order_dirty_from_trade(trade)
        return True
    except Exception as exc:
        log.error('_queue_pending_entry_feature_status insert error trade=%s leg=%s: %s', trade_id, normalized_leg_id, exc)
        return False


def _resolve_expiry_from_tokens(
    tok_col, underlying: str, opt_norm: str, trade_date: str, expiry_kind: str,
    broker_filter: dict | None = None,
) -> str:
    """
    Resolve expiry date string from active_option_tokens based on expiry_kind.

    ExpiryType.Weekly     → nearest expiry >= trade_date
    ExpiryType.NextWeekly → second nearest expiry >= trade_date
    ExpiryType.Monthly    → nearest monthly expiry >= trade_date
                            (last expiry of a calendar month)
    ExpiryType.NextMonthly→ second monthly expiry >= trade_date
    broker_filter: e.g. {'broker': 'dhan'} to restrict to one broker's tokens.
    """
    # Fetch all unique expiries >= trade_date for this instrument/option_type
    _base_q: dict = {**(broker_filter or {}), 'instrument': underlying, 'option_type': opt_norm, 'expiry': {'$gte': trade_date}}
    raw_expiries = tok_col.distinct('expiry', _base_q)
    expiries = sorted(str(e)[:10] for e in raw_expiries if e)
    if not expiries:
        return ''

    kind = str(expiry_kind or 'ExpiryType.Weekly')

    if 'Monthly' in kind:
        # Monthly expiry = last expiry in each calendar month
        monthly: list[str] = []
        from itertools import groupby
        for month_key, group in groupby(expiries, key=lambda d: d[:7]):  # group by YYYY-MM
            monthly.append(list(group)[-1])
        if not monthly:
            return ''
        if 'NextMonthly' in kind:
            return monthly[1] if len(monthly) > 1 else monthly[0]
        return monthly[0]

    # Weekly / NextWeekly
    if 'NextWeekly' in kind:
        return expiries[1] if len(expiries) > 1 else expiries[0]
    return expiries[0]


def _queue_live_broker_pending_momentum_entry(
    db: MongoData,
    trade: dict,
    feat_doc: dict,
    *,
    trade_id: str,
    leg_id: str,
    lazy_leg_ref: str,
    triggered_by: str,
    leg_type_str: str,
    option_type: str,
    position_str: str,
    expiry_kind: str,
    entry_kind: str,
    strike_param_raw: Any,
    lot_config_value: int,
    strike: Any,
    expiry: str,
    token: str,
    symbol: str,
    current_price: float,
    target_price: float,
    base_price: float,
    spot_price: float,
    sl_price: float,
    lot_size: int,
    leg_cfg: dict,
    now_ts: str,
    strike_meta: dict,
    lazy_chain_iv: float | None,
    lazy_entry_vix: float | None,
) -> bool:
    if current_price <= 0 or target_price <= 0:
        return False

    is_sell_pos = _is_sell(position_str)
    use_stop_order = (
        (not is_sell_pos and target_price > current_price)
        or (is_sell_pos and target_price < current_price)
    )
    if use_stop_order:
        # SL-LMT: FlatTrade blocks SL-MKT for API orders
        # Momentum entry: if BUY entry (price rises), limit above trigger; SELL entry, limit below
        from features.live_order_manager import _sl_limit_price, _round_to_tick
        forced_order_type    = 'SL'
        forced_trigger_price = target_price
        # is_sell_pos=True → SELL entry → limit below trigger; False → BUY → limit above
        forced_limit_price   = _sl_limit_price(target_price, is_sell_position=is_sell_pos, buffer_pct=2.0)
    else:
        forced_order_type    = 'LIMIT'
        forced_limit_price   = _round_to_tick(target_price)
        forced_trigger_price = 0.0
    actual_quantity = lot_config_value * lot_size
    exchange_ts = now_ts.replace('T', ' ')[:19] if 'T' in now_ts else now_ts[:19]

    try:
        from features.live_order_manager import place_live_entry_order

        broker_entry = place_live_entry_order(
            db,
            trade,
            {'id': leg_id, 'position': position_str, 'option': option_type},
            leg_cfg,
            symbol,
            actual_quantity,
            current_price,
            force_order_type=forced_order_type,
            force_limit_price=forced_limit_price,
            force_trigger_price=forced_trigger_price,
        )
    except Exception as exc:
        log.error('momentum live broker entry error leg=%s: %s', leg_id, exc)
        return False

    if not broker_entry.get('order_id'):
        momentum_order_error = str(broker_entry.get('error') or 'live_order_failed')
        print(
            f'[MOMENTUM BROKER ORDER BLOCKED] trade={trade_id} leg={leg_id} '
            f'reason={momentum_order_error}'
        )
        if _is_terminal_broker_rejection(momentum_order_error):
            log.error(
                '[MOMENTUM BROKER ORDER] terminal broker rejection — squaring off trade=%s leg=%s: %s',
                trade_id, leg_id, momentum_order_error,
            )
            try:
                _square_off_trade_like_manual(db, trade, exit_timestamp=exchange_ts, activation_mode='live')
            except Exception as _sq_exc:
                log.error('[MOMENTUM BROKER ORDER] auto squareoff failed trade=%s: %s', trade_id, _sq_exc)
            from features.telegram_notifier import notify_user
            notify_user(
                'order_placement_failed',
                f'Trade {trade_id} squared off — broker rejected momentum entry leg={leg_id}: {momentum_order_error}',
                {'leg_id': leg_id, 'trade_id': trade_id},
            )
        return False

    entry_trade_payload = {
        'trigger_timestamp': exchange_ts,
        'trigger_price': target_price,
        'underlying_trigger_price': spot_price,
        'price': target_price,
        'quantity': lot_config_value,
        'underlying_at_trade': spot_price,
        'traded_timestamp': exchange_ts,
        'exchange_timestamp': exchange_ts,
        'strike_meta': strike_meta or {},
        'entry_iv': lazy_chain_iv,
        'entry_vix': lazy_entry_vix,
        'order_id': broker_entry.get('order_id'),
        'order_type': broker_entry.get('order_type'),
        'exchange': broker_entry.get('exchange', ''),
        'order_status': broker_entry.get('order_status'),
        'limit_price': broker_entry.get('limit_price'),
        'trigger_price': broker_entry.get('trigger_price', target_price),
        'order_placed_at': exchange_ts,
        'convert_after': broker_entry.get('convert_after', 40),
        'entry_lifecycle_status': 'order_open',
        'momentum_order_armed_at': exchange_ts,
    }
    broker_entry_status = str(broker_entry.get('order_status') or '').strip().upper()
    if broker_entry_status == 'COMPLETE':
        entry_trade_payload['entry_lifecycle_status'] = 'active'
        entry_trade_payload['filled_at'] = exchange_ts

    new_leg = _build_pending_leg(leg_id, leg_cfg or {
        'PositionType': position_str,
        'InstrumentKind': f'LegType.{option_type}',
        'ExpiryKind': expiry_kind,
        'StrikeParameter': strike_param_raw or 'StrikeType.ATM',
        'EntryType': entry_kind,
        'LotConfig': {'Value': lot_config_value},
    }, trade, now_ts, triggered_by, leg_type=leg_type_str)
    new_leg.update({
        'strike': strike,
        'expiry_date': _normalize_expiry_datetime(expiry),
        'token': token,
        'symbol': symbol,
        'exchange': str(broker_entry.get('exchange') or ''),
        'quantity': lot_config_value,
        'lot_size': lot_size,
        'lot_config_value': lot_config_value,
        'current_sl_price': sl_price,
        'initial_sl_value': sl_price,
        'display_sl_value': sl_price,
        'last_saw_price': target_price,
        'is_lazy': False,
        'lazy_leg_ref': lazy_leg_ref,
        'momentum_base_price': base_price,
        'momentum_target_price': target_price,
        'momentum_reference_set_at': str(feat_doc.get('armed_at') or feat_doc.get('queued_at') or now_ts),
        'entry_trade': entry_trade_payload,
        'exit_trade': None,
        'spot_at_queue': _safe_float(feat_doc.get('spot_at_queue')),
    })

    db._db['algo_trades'].update_one(
        {'_id': trade_id},
        {'$push': {'legs': new_leg}},
    )
    db._db['algo_leg_feature_status'].update_one(
        {'_id': feat_doc['_id']},
        {'$set': {
            'status': 'triggered',
            'triggered_at': now_ts,
            'updated_at': now_ts,
            'broker_order_id': str(broker_entry.get('order_id') or ''),
            'broker_order_type': str(broker_entry.get('order_type') or ''),
            'broker_trigger_price': _safe_float(broker_entry.get('trigger_price')),
            'broker_limit_price': _safe_float(broker_entry.get('limit_price')),
        }},
    )
    print(
        f'[MOMENTUM BROKER ORDER ARMED] trade={trade_id} leg={leg_id} '
        f'current={current_price} target={target_price} order_type={forced_order_type} '
        f'order_id={str(broker_entry.get("order_id") or "-")}'
    )
    if broker_entry_status == 'COMPLETE':
        refreshed_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or trade
        refreshed_legs = [l for l in (refreshed_trade.get('legs') or []) if isinstance(l, dict)]
        entered_leg = next((l for l in refreshed_legs if str(l.get('id') or '') == leg_id), None)
        if entered_leg:
            _store_position_history(db, refreshed_trade, entered_leg, override_leg_cfg=leg_cfg)
    return True


def _process_momentum_pending_feature_legs(
    db: MongoData, trade: dict, chain_col, trade_date: str, now_ts: str,
    lot_size: int, index_spot_doc: dict | None = None, market_cache: dict | None = None,
    ltp_map: dict | None = None,
    activation_mode: str = '',
) -> list[str]:
    """
    Every tick: check algo_leg_feature_status for active momentum_pending legs.
    - If momentum target not yet reached: arm (set base/target price) and wait.
    - If momentum triggered: push full entry leg dict into algo_trades.legs.
    Returns list of leg_ids that were entered.
    ltp_map: optional {token: ltp} from broker WebSocket — used for live/fast-forward
             as the primary price source instead of historical chain data.
    activation_mode: 'live' | 'fast-forward' — when set, resolves Kite integer token
                     from active_option_tokens so Kite subscription works correctly.
    """
    trade_id = str(trade.get('_id') or '')
    entered_ids: list[str] = []

    # Strategy paused (e.g. a sibling leg's entry already failed) — don't take any
    # further entries until someone clears it. Pure read of an already-fetched
    # field, doesn't touch the surrounding momentum-timing logic itself.
    if trade.get('is_paused'):
        return entered_ids

    # Early exit if entry time hasn't arrived — avoids all DB ops and noisy prints
    if activation_mode in {'live', 'fast-forward', 'forward-test'}:
        _raw_et_early = str((trade.get('config') or {}).get('entry_time') or trade.get('entry_time') or '').strip()
        _et_hhmm = _raw_et_early[11:16] if len(_raw_et_early) >= 16 else _raw_et_early[:5]
        _now_hhmm_early = now_ts[11:16] if len(now_ts) >= 16 else ''
        if _et_hhmm and _now_hhmm_early and _now_hhmm_early < _et_hhmm:
            # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} mode={activation_mode} state=waiting_for_entry_time entry_time={_et_hhmm} now={_now_hhmm_early}')
            return entered_ids

    feature_col = db._db['algo_leg_feature_status']
    # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} mode={activation_mode or "-"} state=start trade_date={trade_date or "-"} now_ts={now_ts}')

    try:
        reactivated = feature_col.update_many(
            {
                'trade_id': trade_id,
                'feature': {'$in': ['momentum_pending', 'pending_entry']},
                'status': 'processing',
            },
            {'$set': {'status': 'active', 'processing_started_at': None}},
        )
        # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} mode={activation_mode or "-"} state=processing_rows_reactivated count={int(reactivated.modified_count or 0)}')
    except Exception as exc:
        print(f'[PENDING FEATURE FLOW] trade_id={trade_id} mode={activation_mode or "-"} state=processing_rows_reactivate_error error={exc}')

    try:
        active_docs = list(feature_col.find({
            'trade_id': trade_id,
            'feature': {'$in': ['momentum_pending', 'pending_entry']},
            'status': 'active',
        }))
    except Exception as exc:
        log.warning('_process_momentum_pending_feature_legs fetch error trade=%s: %s', trade_id, exc)
        # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} mode={activation_mode or "-"} state=fetch_error error={exc}')
        return entered_ids

    # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} mode={activation_mode or "-"} state=active_docs_loaded count={len(active_docs)}')

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
            feature_col.update_many(
                {'_id': {'$in': [row_id for row_id in duplicate_row_ids if row_id is not None]}},
                {'$set': {
                    'status': 'disabled',
                    'enabled': False,
                    'disabled_reason': 'duplicate_queue_cleanup',
                    'updated_at': now_ts,
                    'disabled_at': now_ts,
                }},
            )
        except Exception as exc:
            log.warning('_process_momentum_pending_feature_legs duplicate cleanup error trade=%s: %s', trade_id, exc)
            # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} mode={activation_mode or "-"} state=duplicate_cleanup_error error={exc}')
    active_docs = deduped_active_docs
    # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} mode={activation_mode or "-"} state=deduped_active_docs count={len(active_docs)}')

    underlying = str(
        (trade.get('strategy') or {}).get('Ticker')
        or (trade.get('config') or {}).get('Ticker')
        or trade.get('ticker') or ''
    )
    if not underlying:
        # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} mode={activation_mode or "-"} state=missing_underlying')
        return entered_ids

    from features.strike_selector import resolve_expiry, resolve_strike as _resolve_strike_selector

    # Tick-level option chain cache — fetched once per (underlying, expiry) per tick.
    # All legs in this tick reuse the same chain data. Next tick = new dict = fresh fetch.
    _live_chain_cache: dict[tuple, dict] = {}

    index_spot_doc = index_spot_doc or {}
    if not index_spot_doc:
        index_spot_col = db._db['option_chain_index_spot']
        index_spot_doc = get_index_spot_at_time(
            index_spot_col, underlying, now_ts, market_cache=market_cache
        )
    spot_price = _safe_float(index_spot_doc.get('spot_price'))
    # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} mode={activation_mode or "-"} state=spot_from_db underlying={underlying} spot_price={spot_price}')

    # For live/fast-forward: try socket ticker, then Kite quote API as final fallback.
    # The quote API handles post-3:30 PM entries where the socket is no longer streaming.
    if spot_price <= 0 and activation_mode in {'live', 'fast-forward', 'forward-test'}:
        try:
            from features.live_monitor_socket import _get_active_ticker_manager
            spot_price = _safe_float(_get_active_ticker_manager().get_spot(underlying))
        except Exception:
            pass
    if spot_price <= 0 and activation_mode in {'live', 'fast-forward', 'forward-test'}:
        try:
            from features.fast_forward_event import QUOTE_INSTRUMENT_BY_UNDERLYING
            from features.broker_gateway import get_broker_rest_client  # type: ignore
            _spot_instr = QUOTE_INSTRUMENT_BY_UNDERLYING.get(underlying.upper(), '')
            if _spot_instr:
                _kite = get_broker_rest_client(db)
                if _kite:
                    _q = (_kite.quote([_spot_instr]) or {}).get(_spot_instr) or {}
                    _qp = _safe_float(_q.get('last_price'))
                    if _qp > 0:
                        spot_price = _qp
                        print(f'[SPOT QUOTE] underlying={underlying} spot={spot_price} source=kite_quote')
        except Exception as exc:
            log.warning('[SPOT QUOTE] fallback error underlying=%s: %s', underlying, exc)
            # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} mode={activation_mode or "-"} state=spot_quote_error underlying={underlying} error={exc}')

    # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} mode={activation_mode or "-"} state=spot_ready underlying={underlying} spot_price={spot_price}')

    for feat_doc in active_docs:
        # Atomically claim this doc — prevents duplicate processing when the function
        # is called concurrently (e.g. FAST-FORWARD QUOTE CYCLE vs BROKER TICK handler).
        _doc_id = feat_doc.get('_id')
        _claimed = feature_col.find_one_and_update(
            {'_id': _doc_id, 'status': 'active'},
            {'$set': {'status': 'processing', 'processing_started_at': now_ts}},
        )
        if _claimed is None:
            # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} mode={activation_mode or "-"} state=claim_skipped feature_id={feat_doc.get("_id")} reason=already_claimed')
            continue  # Another concurrent call already claimed this leg

        feat_id = str(feat_doc.get('_id') or '')
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
        _feat_feature = str(feat_doc.get('feature') or '').strip()
        # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} leg_id={leg_id or "-"} mode={activation_mode or "-"} state=processing_doc feature={_feat_feature or "-"} option_type={option_type or "-"} entry_kind={entry_kind or "-"} triggered_by={triggered_by or "-"}')

        # Entry time gate for original pending_entry legs in live/fast-forward mode.
        # Re-entries (triggered_by is set) skip this check — they enter on their own conditions.
        if _feat_feature == 'pending_entry' and not triggered_by and activation_mode in {'live', 'fast-forward', 'forward-test'}:
            _raw_et = str(
                (trade.get('config') or {}).get('entry_time')
                or trade.get('entry_time')
                or ''
            ).strip()
            _entry_hhmm = _raw_et[11:16] if len(_raw_et) >= 16 else _raw_et[:5]
            _now_hhmm = now_ts[11:16] if len(now_ts) >= 16 else ''
            if _entry_hhmm and _now_hhmm and _now_hhmm < _entry_hhmm:
                feature_col.update_one(
                    {'_id': _doc_id, 'status': 'processing'},
                    {'$set': {'status': 'active', 'processing_started_at': None}},
                )
                print(f'[ENTRY TIME GATE] leg={leg_id} mode={activation_mode} now={_now_hhmm} entry_time={_entry_hhmm} — waiting')
                continue

        # ── Resolve expiry / strike if not yet done ──────────────────────────
        expiry = str(feat_doc.get('expiry_date') or '')
        strike = feat_doc.get('strike')
        token = str(feat_doc.get('token') or '')
        symbol = str(feat_doc.get('symbol') or '')
        base_price = _safe_float(feat_doc.get('momentum_base_price'))
        target_price = _safe_float(feat_doc.get('momentum_target_price'))
        _needs_db_write = False
        _strike_meta: dict = {}
        _lazy_chain_iv: float | None = None
        _chain_ltp: float = 0.0  # set inside live/FF block; kept here so price block sees it

        if activation_mode in {'live', 'fast-forward', 'forward-test'}:
            # Lock strike from first resolution — do not re-pick from chain
            # once a strike+token is stored for this leg in feat_doc.
            # Same strike must be used from queue start until leg closes.
            # A new lazy leg (after SL/TG) has its own feat_doc row with no
            # strike yet, so it gets a fresh pick on its first tick.
            _strike_locked = bool(strike not in (None, '') and token)
            # If strike is locked and WebSocket already has LTP for this token,
            # skip the full chain fetch entirely — WS tick is the price source.
            _ws_ltp_for_locked = _safe_float((ltp_map or {}).get(str(token))) if (_strike_locked and token) else 0.0
            if _strike_locked and _ws_ltp_for_locked > 0:
                _chain_ltp = _ws_ltp_for_locked
                runtime_print(
                    f'[LIVE CHAIN SKIPPED] leg={leg_id} strike={strike} '
                    f'base={base_price} target={target_price} ltp={_chain_ltp} source=ws_token'
                )
            else:
                try:
                    from features.live_option_chain import fetch_full_chain, select_strike_live  # type: ignore
                    opt_norm = option_type.upper()
                    tok_col  = db._db['active_option_tokens']

                    _expiry_broker_filter: dict = {}
                    try:
                        from features.market_feed_tokens import active_token_broker_filter as _atbf_ex  # type: ignore
                        _expiry_broker_filter = _atbf_ex(db)
                    except Exception:
                        pass
                    live_expiry = _resolve_expiry_from_tokens(tok_col, underlying, opt_norm, trade_date, expiry_kind, broker_filter=_expiry_broker_filter)
                    if not live_expiry:
                        # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} leg_id={leg_id or "-"} mode={activation_mode or "-"} state=live_expiry_missing underlying={underlying} option_type={opt_norm}')
                        entry_print(f'[MOMENTUM PENDING] leg={leg_id} no expiry in active_option_tokens — skipping')
                        continue

                    _cache_key = (underlying, live_expiry)
                    # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} leg_id={leg_id or "-"} mode={activation_mode or "-"} state=live_chain_about_to_fetch underlying={underlying} expiry={live_expiry} spot_price={spot_price}')
                    if _cache_key not in _live_chain_cache:
                        _live_chain_cache[_cache_key] = fetch_full_chain(
                            db, underlying, live_expiry, spot_price, leg_id=leg_id
                        )
                    else:
                        entry_print(f'[LIVE CHAIN] leg={leg_id} reusing cached chain {underlying} {live_expiry}')
                    chain = _live_chain_cache[_cache_key]
                    # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} leg_id={leg_id or "-"} mode={activation_mode or "-"} state=live_chain_fetched underlying={underlying} expiry={live_expiry} ce_count={len(chain.get("CE") or [])} pe_count={len(chain.get("PE") or [])}')
                    if not chain.get('CE') and not chain.get('PE'):
                        # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} leg_id={leg_id or "-"} mode={activation_mode or "-"} state=live_chain_empty underlying={underlying} expiry={live_expiry}')
                        entry_print(f'[MOMENTUM PENDING] leg={leg_id} empty chain {underlying} {live_expiry} — skipping')
                        continue

                    if _strike_locked:
                        # Strike locked but WS had no LTP — use chain as fallback
                        opt_rows = chain.get(opt_norm) or []
                        _locked_row = next(
                            (r for r in opt_rows
                             if _safe_float(r.get('strike')) == _safe_float(strike)),
                            None,
                        )
                        _chain_ltp = _safe_float((_locked_row or {}).get('ltp'))
                        if token and _chain_ltp > 0 and ltp_map is not None:
                            ltp_map[token] = _chain_ltp
                        runtime_print(
                            f'[LIVE CHAIN LOCKED] leg={leg_id} strike={strike} '
                            f'base={base_price} target={target_price} ltp={_chain_ltp}'
                        )
                    else:
                        # Not yet armed — pick strike fresh from current chain.
                        sel = select_strike_live(
                            chain, entry_kind, strike_param_raw,
                            opt_norm, position_str, spot_price, underlying, leg_id=leg_id,
                        )
                        if not sel:
                            # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} leg_id={leg_id or "-"} mode={activation_mode or "-"} state=strike_selection_failed underlying={underlying} expiry={live_expiry} entry_kind={entry_kind or "-"}')
                            entry_print(f'[MOMENTUM PENDING] leg={leg_id} no strike found — skipping')
                            continue

                        expiry         = live_expiry
                        strike         = sel['strike']
                        token          = sel['token']
                        symbol         = sel['symbol']
                        _strike_meta   = sel.get('meta') or {}
                        _lazy_chain_iv = _safe_float(sel.get('iv')) or None
                        _needs_db_write = True
                        _chain_ltp = _safe_float(sel.get('ltp'))
                        if token and _chain_ltp > 0 and ltp_map is not None:
                            ltp_map[token] = _chain_ltp
                        print(f'[LIVE CHAIN RESOLVED] leg={leg_id} underlying={underlying} expiry={expiry} strike={strike} ltp={_chain_ltp} token={token}')
                except Exception as exc:
                    log.warning('live chain resolve error leg=%s: %s', leg_id, exc)
                    # print(f'[PENDING FEATURE FLOW] trade_id={trade_id} leg_id={leg_id or "-"} mode={activation_mode or "-"} state=live_chain_exception error={exc}')
                    continue

        elif not expiry or strike in (None, ''):
            # Backtest: resolve from historical chain_col
            try:
                expiry, expiry_err = resolve_expiry(chain_col, underlying, option_type, trade_date, now_ts)
                if expiry_err:
                    entry_print(f'[MOMENTUM PENDING] leg={leg_id} expiry resolve error: {expiry_err}')
                    continue
                sel = _resolve_strike_selector(
                    chain_col=chain_col,
                    underlying=underlying,
                    option_type=option_type,
                    entry_kind=entry_kind,
                    strike_param_raw=strike_param_raw,
                    position=position_str,
                    spot_price=spot_price,
                    expiry=expiry,
                    trade_date=trade_date,
                    snapshot_timestamp=now_ts,
                    market_cache=market_cache,
                    leg_id=leg_id,
                )
                if sel.error:
                    entry_print(f'[MOMENTUM PENDING] leg={leg_id} strike resolve error: {sel.error}')
                    continue
                strike = sel.strike
                _strike_meta = sel.meta or {}
                resolved_doc = _normalize_chain_market_fields(sel.chain_doc or {})
                token = str(resolved_doc.get('token') or token or make_token(underlying, expiry, strike, option_type))
                symbol = str(resolved_doc.get('symbol') or symbol or token)
                _lazy_chain_iv = _safe_float(resolved_doc.get('iv')) or None
                _needs_db_write = True
            except Exception as exc:
                log.warning('momentum_pending strike resolve error leg=%s: %s', leg_id, exc)
                continue

        # ── Once strike+expiry+option_type are final, look up the broker token
        #    from active_option_tokens for live/fast-forward mode.
        #    Filter by active broker so Kite and Dhan tokens don't mix.
        if activation_mode in {'live', 'fast-forward', 'forward-test'} and expiry and strike not in (None, ''):
            try:
                _broker_filter: dict = {}
                try:
                    from features.market_feed_tokens import active_token_broker_filter as _atbf  # type: ignore
                    _broker_filter = _atbf(db)
                except Exception:
                    pass
                tok_doc = db._db['active_option_tokens'].find_one({
                    **_broker_filter,
                    'instrument': underlying,
                    'expiry': str(expiry)[:10],
                    'strike': strike,
                    'option_type': option_type.upper(),
                }) or {}
                broker_tok = str(tok_doc.get('token') or tok_doc.get('tokens') or '').strip()
                if broker_tok and broker_tok != token:
                    token = broker_tok
                    symbol = str(tok_doc.get('symbol') or symbol or broker_tok)
                    _needs_db_write = True
                    print(
                        f'[MOMENTUM BROKER TOKEN] leg={leg_id} '
                        f'underlying={underlying} strike={strike} '
                        f'expiry={str(expiry)[:10]} option={option_type} '
                        f'token={token} symbol={symbol}'
                    )
            except Exception as _kt_exc:
                log.warning('momentum_pending broker token lookup error leg=%s: %s', leg_id, _kt_exc)

        if _needs_db_write:
            feature_col.update_one(
                {'_id': feat_doc['_id']},
                {'$set': {'expiry_date': expiry, 'strike': strike, 'token': token, 'symbol': symbol}},
            )

        # Resolve Kite token for subscription (chain token may be broker-specific)
        _sub_token = token
        if activation_mode == 'live' and symbol:
            try:
                from features.live_event import resolve_kite_token_for_symbol as _rkt
                _kt = _rkt(symbol)
                if _kt:
                    _sub_token = _kt
            except Exception:
                pass

        # Subscribe only when a new token is resolved for the first time (_needs_db_write).
        # Already-locked positions are subscribed at initial load — no re-subscribe per tick.
        if _needs_db_write and activation_mode in {'live', 'fast-forward', 'forward-test'} and _sub_token and str(_sub_token).isdigit():
            try:
                if activation_mode in ('fast-forward', 'forward-test'):
                    from features.fast_forward_event import _subscribe_option_token
                    _subscribe_option_token(_sub_token, symbol)
                    runtime_print(
                        f'[MOMENTUM TOKEN SUBSCRIBE] mode={activation_mode} leg={leg_id} '
                        f'token={_sub_token} symbol={symbol or "-"}'
                    )
                else:
                    from features.live_event import _subscribe_live_option_token
                    _subscribe_live_option_token(_sub_token, symbol)
                    runtime_print(
                        f'[MOMENTUM TOKEN SUBSCRIBE] mode=live leg={leg_id} '
                        f'kite_token={_sub_token} broker_token={token} symbol={symbol or "-"}'
                    )
            except Exception as _sub_exc:
                log.warning('momentum_pending token subscribe error leg=%s: %s', leg_id, _sub_exc)

        # ── Get current option price ─────────────────────────────────────────────
        # For live/fast-forward: ONLY use live LTP from broker WebSocket or the
        # live chain fetch (_chain_ltp).  Historical chain_col fallback is blocked
        # because it returns stale backtest prices (e.g. 345.65) that would cause
        # a real-money order to be placed at the wrong price.
        # For backtest: historical chain data is the correct source.
        current_price: float = 0.0
        live_ltp = _safe_float((ltp_map or {}).get(token)) if token else 0.0
        if live_ltp > 0:
            current_price = live_ltp
        elif activation_mode in {'live', 'fast-forward', 'forward-test'}:
            # Use chain LTP from the live fetch above if WS hasn't populated yet
            if _chain_ltp > 0:
                current_price = _chain_ltp
            else:
                print(
                    f'[LIVE PRICE MISSING] leg={leg_id} token={token} '
                    f'underlying={underlying} strike={strike} option={option_type} '
                    f'— no live LTP from WS or chain, skipping tick (will retry next tick)'
                )
                try:
                    feature_col.update_one(
                        {'_id': feat_doc['_id'], 'status': 'processing'},
                        {'$set': {'status': 'active', 'processing_started_at': None}},
                    )
                except Exception:
                    pass
                continue
        else:
            try:
                current_chain_doc = _normalize_chain_market_fields(
                    get_chain_at_time(chain_col, underlying, expiry, strike, option_type, now_ts, market_cache=market_cache)
                )
                current_price = _resolve_chain_market_price(current_chain_doc)
                if _lazy_chain_iv is None:
                    _lazy_chain_iv = _safe_float(current_chain_doc.get('iv')) or None
            except Exception as exc:
                log.warning('momentum_pending chain fetch error leg=%s: %s', leg_id, exc)
                continue

        is_instant_entry = str(feat_doc.get('feature') or '') == 'pending_entry'

        if is_instant_entry:
            # ── No-momentum lazy leg: enter immediately on current price ──────
            if current_price <= 0:
                # print(
                #     f'[PENDING FEATURE FLOW] trade_id={trade_id} '
                #     f'leg_id={leg_id or "-"} '
                #     f'mode={activation_mode or "-"} '
                #     f'state=current_price_missing '
                #     f'token={token or "-"} '
                #     f'strike={strike} '
                #     f'option_type={option_type or "-"}'
                # )
                entry_print(f'[PENDING ENTRY WAIT] leg={leg_id} waiting for price data')
                try:
                    feature_col.update_one(
                        {'_id': feat_doc['_id'], 'status': 'processing'},
                        {'$set': {'status': 'active', 'processing_started_at': None}},
                    )
                except Exception:
                    pass
                continue
            # print(
            #     f'[PENDING FEATURE FLOW] trade_id={trade_id} '
            #     f'leg_id={leg_id or "-"} '
            #     f'mode={activation_mode or "-"} '
            #     f'state=instant_entry_ready '
            #     f'current_price={current_price} '
            #     f'strike={strike} '
            #     f'token={token or "-"}'
            # )
            trade_event_print(f'[PENDING ENTRY] leg={leg_id} current_price={current_price} strike={strike} option={option_type} — entering immediately')
        else:
            # Underlying momentum types track the index/spot; all others track the option price
            _mom_check_price = spot_price if 'Underlying' in str(momentum_type or '') else current_price
            # ── Arm: set base/target price on first tick ──────────────────────
            if base_price <= 0 or target_price <= 0:
                if _mom_check_price <= 0:
                    entry_print(f'[MOMENTUM PENDING] leg={leg_id} waiting for price data')
                    try:
                        feature_col.update_one(
                            {'_id': feat_doc['_id'], 'status': 'processing'},
                            {'$set': {'status': 'active', 'processing_started_at': None}},
                        )
                    except Exception:
                        pass
                    continue
                base_price = _mom_check_price
                target_price = _resolve_simple_momentum_target(base_price, momentum_type, momentum_value)
                armed_now = now_ts
                try:
                    feature_col.update_one(
                        {'_id': feat_doc['_id']},
                        {'$set': {
                            'momentum_base_price': base_price,
                            'momentum_target_price': target_price,
                            'armed_at': armed_now,
                            'strike': strike,
                            'expiry_date': expiry,
                            'token': token,
                            'symbol': symbol,
                            'status': 'active',
                            'processing_started_at': None,
                        }},
                    )
                except Exception as exc:
                    log.warning('momentum_pending arm error leg=%s: %s', leg_id, exc)
                print(
                    f'[MOMENTUM ARMED] leg={leg_id} type={momentum_type} value={momentum_value} '
                    f'base={base_price} target={target_price} strike={strike} option={option_type}'
                )
                if activation_mode == 'live':
                    is_sell_pos = _is_sell(position_str)
                    all_leg_configs = _resolve_trade_leg_configs(trade)
                    leg_cfg = all_leg_configs.get(lazy_leg_ref) or all_leg_configs.get(leg_id) or {}
                    entry_price = (
                        current_price
                        if not target_price or _is_market_order(leg_cfg, 'entry')
                        else target_price
                    )
                    sl_config = leg_cfg.get('LegStopLoss') or {}
                    sl_price = calc_sl_price(entry_price, is_sell_pos, sl_config)
                    lazy_entry_vix: float | None = None
                    try:
                        from features.trading_core import get_vix_at_time as _get_vix  # type: ignore
                        _vix_val = _get_vix(db, now_ts, market_cache)
                        lazy_entry_vix = _vix_val if _vix_val > 0 else None
                    except Exception:
                        pass
                    queued = _queue_live_broker_pending_momentum_entry(
                        db,
                        trade,
                        feat_doc,
                        trade_id=trade_id,
                        leg_id=leg_id,
                        lazy_leg_ref=lazy_leg_ref,
                        triggered_by=triggered_by,
                        leg_type_str=leg_type_str,
                        option_type=option_type,
                        position_str=position_str,
                        expiry_kind=expiry_kind,
                        entry_kind=entry_kind,
                        strike_param_raw=strike_param_raw,
                        lot_config_value=lot_config_value,
                        strike=strike,
                        expiry=expiry,
                        token=token,
                        symbol=symbol,
                        current_price=current_price,
                        target_price=target_price,
                        base_price=base_price,
                        spot_price=spot_price,
                        sl_price=sl_price,
                        lot_size=lot_size,
                        leg_cfg=leg_cfg,
                        now_ts=now_ts,
                        strike_meta=_strike_meta,
                        lazy_chain_iv=_lazy_chain_iv,
                        lazy_entry_vix=lazy_entry_vix,
                    )
                continue

            # ── Live: retry broker SL-L order if arming placement failed ─────────
            if activation_mode == 'live' and not str(feat_doc.get('broker_order_id') or '').strip():
                all_leg_configs = _resolve_trade_leg_configs(trade)
                leg_cfg = all_leg_configs.get(lazy_leg_ref) or all_leg_configs.get(leg_id) or {}
                is_sell_pos = _is_sell(position_str)
                sl_config = leg_cfg.get('LegStopLoss') or {}
                sl_price = calc_sl_price(target_price, is_sell_pos, sl_config)
                _retry_vix: float | None = None
                try:
                    from features.trading_core import get_vix_at_time as _get_vix
                    _v = _get_vix(db, now_ts, market_cache)
                    _retry_vix = _v if _v > 0 else None
                except Exception:
                    pass
                queued = _queue_live_broker_pending_momentum_entry(
                    db, trade, feat_doc,
                    trade_id=trade_id, leg_id=leg_id, lazy_leg_ref=lazy_leg_ref,
                    triggered_by=triggered_by, leg_type_str=leg_type_str,
                    option_type=option_type, position_str=position_str,
                    expiry_kind=expiry_kind, entry_kind=entry_kind,
                    strike_param_raw=strike_param_raw, lot_config_value=lot_config_value,
                    strike=strike, expiry=expiry, token=token, symbol=symbol,
                    current_price=current_price, target_price=target_price,
                    base_price=base_price, spot_price=spot_price, sl_price=sl_price,
                    lot_size=lot_size, leg_cfg=leg_cfg, now_ts=now_ts,
                    strike_meta=_strike_meta, lazy_chain_iv=_lazy_chain_iv,
                    lazy_entry_vix=_retry_vix,
                )
                if queued:
                    print(f'[MOMENTUM BROKER ORDER RETRY OK] trade={trade_id} leg={leg_id}')
                    continue
                # Placement still failing — fall through to tick-level trigger check

            # ── Check if momentum target is reached ───────────────────────────
            if not _is_simple_momentum_triggered(_mom_check_price, target_price, momentum_type):
                runtime_print(
                    f'[MOMENTUM WAIT] leg={leg_id} type={momentum_type} value={momentum_value} '
                    f'base={base_price} target={target_price} current={_mom_check_price} strike={strike}'
                )
                try:
                    feature_col.update_one(
                        {'_id': feat_doc['_id'], 'status': 'processing'},
                        {'$set': {'status': 'active', 'processing_started_at': None}},
                    )
                except Exception:
                    pass
                continue

            print(
                f'[MOMENTUM OK] leg={leg_id} type={momentum_type} value={momentum_value} '
                f'base={base_price} target={target_price} current={_mom_check_price} — entering'
            )
        is_sell_pos = _is_sell(position_str)
        all_leg_configs = _resolve_trade_leg_configs(trade)
        leg_cfg = all_leg_configs.get(lazy_leg_ref) or all_leg_configs.get(leg_id) or {}
        entry_price = (
            current_price
            if is_instant_entry or not target_price or _is_market_order(leg_cfg, 'entry')
            else target_price
        )
        sl_config = leg_cfg.get('LegStopLoss') or {}
        sl_price = calc_sl_price(entry_price, is_sell_pos, sl_config)
        actual_quantity = lot_config_value

        _lazy_entry_vix: float | None = None
        try:
            from features.trading_core import get_vix_at_time as _get_vix  # type: ignore
            _vix_val = _get_vix(db, now_ts, market_cache)
            _lazy_entry_vix = _vix_val if _vix_val > 0 else None
        except Exception:
            pass

        exchange_ts = now_ts.replace('T', ' ')[:19] if 'T' in now_ts else now_ts[:19]
        entry_trade_payload = {
            'trigger_timestamp': exchange_ts,
            'trigger_price': entry_price,
            'underlying_trigger_price': spot_price,
            'price': entry_price,
            'quantity': actual_quantity,
            'underlying_at_trade': spot_price,
            'traded_timestamp': exchange_ts,
            'exchange_timestamp': exchange_ts,
            'strike_meta': _strike_meta or {},
            'entry_iv': _lazy_chain_iv,
            'entry_vix': _lazy_entry_vix,
            # fast-forward: active immediately; live: order_open until broker confirms
            'entry_lifecycle_status': 'active' if activation_mode != 'live' else 'order_open',
        }

        new_leg = _build_pending_leg(leg_id, leg_cfg or {
            'PositionType': position_str,
            'InstrumentKind': f'LegType.{option_type}',
            'ExpiryKind': expiry_kind,
            'StrikeParameter': strike_param_raw or 'StrikeType.ATM',
            'EntryType': entry_kind,
            'LotConfig': {'Value': lot_config_value},
        }, trade, now_ts, triggered_by, leg_type=leg_type_str)
        # Resolve Kite instrument token for WebSocket LTP subscription.
        # Chain token may be broker-specific (FlatTrade); we need Kite's integer token.
        broker_token = token
        kite_token = token
        if symbol and activation_mode in {'live', 'fast-forward', 'forward-test'}:
            try:
                from features.live_event import resolve_kite_token_for_symbol
                _kt = resolve_kite_token_for_symbol(symbol)
                if _kt:
                    kite_token = _kt
            except Exception:
                pass

        new_leg.update({
            'strike': strike,
            'expiry_date': _normalize_expiry_datetime(expiry),
            'token': kite_token,
            'broker_token': broker_token,
            'symbol': symbol,
            'quantity': actual_quantity,
            'lot_size': lot_size,
            'lot_config_value': lot_config_value,
            'current_sl_price': sl_price,
            'initial_sl_value': sl_price,
            'display_sl_value': sl_price,
            'last_saw_price': entry_price,
            'is_lazy': False,
            # Store the original IdleLegConfigs key so SL/Target/TrailSL/NextLeg
            # config can be resolved even when leg_id is a composite key
            # (e.g. 'putLeg1-callLeg1' vs the config key 'putLeg1').
            'lazy_leg_ref': lazy_leg_ref,
            'momentum_base_price': base_price,
            'momentum_target_price': target_price,
            'momentum_reference_set_at': str(feat_doc.get('armed_at') or feat_doc.get('queued_at') or now_ts),
            'momentum_triggered_at': now_ts,
            'entry_trade': entry_trade_payload,
            'exit_trade': None,
            'spot_at_queue': _safe_float(feat_doc.get('spot_at_queue')),
        })

        # ── Live mode: place broker order BEFORE writing leg to DB ──────────────
        if activation_mode == 'live':
            try:
                from features.live_order_manager import place_live_entry_order as _place_entry
                broker_result = _place_entry(
                    db, trade, new_leg, leg_cfg, symbol,
                    actual_quantity * lot_size, entry_price,
                )
                order_id = str(broker_result.get('order_id') or '').strip()
                if not order_id:
                    pending_leg_order_error = str(broker_result.get('error') or 'live_order_failed')
                    log.warning(
                        '[LIVE ENTRY] order placement failed leg=%s trade=%s: %s',
                        leg_id, trade_id, pending_leg_order_error,
                    )
                    if _is_terminal_broker_rejection(pending_leg_order_error):
                        log.error(
                            '[LIVE ENTRY] terminal broker rejection — squaring off trade=%s leg=%s: %s',
                            trade_id, leg_id, pending_leg_order_error,
                        )
                        try:
                            _square_off_trade_like_manual(db, trade, exit_timestamp=exchange_ts, activation_mode='live')
                        except Exception as _sq_exc:
                            log.error('[LIVE ENTRY] auto squareoff failed trade=%s: %s', trade_id, _sq_exc)
                        from features.telegram_notifier import notify_user
                        notify_user(
                            'order_placement_failed',
                            f'Trade {trade_id} squared off — broker rejected pending-leg entry leg={leg_id}: {pending_leg_order_error}',
                            {'leg_id': leg_id, 'trade_id': trade_id},
                        )
                    feature_col.update_one(
                        {'_id': feat_doc['_id'], 'status': 'processing'},
                        {'$set': {'status': 'active', 'processing_started_at': None}},
                    )
                    continue
                # Attach order details to entry_trade payload
                new_leg['entry_trade'].update({
                    'order_id':       order_id,
                    'order_type':     broker_result.get('order_type', ''),
                    'order_status':   broker_result.get('order_status', 'OPEN'),
                    'limit_price':    broker_result.get('limit_price', entry_price),
                    'trigger_price':  broker_result.get('trigger_price', 0),
                    'order_placed_at': exchange_ts,
                    'convert_after':  broker_result.get('convert_after', 40),
                })
                print(
                    f'[LIVE ENTRY ORDER PLACED] trade={trade_id} leg={leg_id} '
                    f'order_id={order_id} strike={strike} option={option_type} '
                    f'entry_price={entry_price} sl={sl_price}'
                )
            except Exception as _oe:
                log.error('[LIVE ENTRY] order error leg=%s trade=%s: %s', leg_id, trade_id, _oe)
                feature_col.update_one(
                    {'_id': feat_doc['_id'], 'status': 'processing'},
                    {'$set': {'status': 'active', 'processing_started_at': None}},
                )
                continue

        try:
            # Mark feature as triggered
            feature_col.update_one(
                {'_id': feat_doc['_id']},
                {'$set': {'status': 'triggered', 'triggered_at': now_ts}},
            )
            entry_label = 'PENDING ENTRY' if is_instant_entry else 'MOMENTUM ENTRY'
            print(
                f'[{entry_label}] trade={trade_id} leg={leg_id} entry_price={entry_price} '
                f'sl={sl_price} strike={strike} option={option_type}'
            )
            # _store_position_history handles everything:
            #   1. Insert full leg data → algo_trade_positions_history
            #   2. $pull any dict with matching leg_id from legs[] (removes pending placeholder)
            #   3. $push string history_id to legs[]
            # Result: legs[] has only string IDs, no full dicts
            _store_position_history(db, trade, new_leg, override_leg_cfg=leg_cfg)
            entered_ids.append(leg_id)
        except Exception as exc:
            log.error('momentum_pending entry error leg=%s: %s', leg_id, exc)
            try:
                feature_col.update_one(
                    {'_id': feat_doc['_id'], 'status': 'processing'},
                    {'$set': {'status': 'active', 'processing_started_at': None}},
                )
            except Exception:
                pass

    return entered_ids


def _handle_reentry(db: MongoData, trade: dict, leg_config: dict,
                     reentry_config: dict, triggered_by: str, now_ts: str) -> str | None:
    """
    Process a re-entry or lazy-leg trigger.
    Returns a string describing what was queued, or None.
    """
    if not reentry_config:
        return None

    reentry_type = str(reentry_config.get('Type') or '')
    reentry_value = reentry_config.get('Value')

    trade_id = str(trade.get('_id') or '')
    strict_history_mode = _is_strict_history_leg_mode(str(trade.get('activation_mode') or '').strip())

    # IdleLegConfigs: prefer strategy.IdleLegConfigs, fallback to config.IdleLegConfigs
    strategy = trade.get('strategy') or {}
    idle_configs = strategy.get('IdleLegConfigs') or {}
    if not idle_configs:
        idle_configs = (trade.get('config') or {}).get('IdleLegConfigs') or {}

    parent_leg_type = _get_parent_leg_type(trade, triggered_by)

    # ── NextLeg → activate an idle (lazy) leg ─────────────────────────────────
    if 'NextLeg' in reentry_type:
        lazy_ref = str((reentry_value or {}).get('NextLegRef') or reentry_value or '')
        lazy_cfg = idle_configs.get(lazy_ref)
        if not lazy_cfg:
            log.warning('Lazy leg ref %s not found in IdleLegConfigs for trade %s', lazy_ref, trade_id)
            return None
        existing_ids = {str(l.get('id') or '') for l in (trade.get('legs') or []) if isinstance(l, dict)}
        try:
            for _doc in db._db['algo_trade_positions_history'].find({'trade_id': trade_id}, {'leg_id': 1}):
                existing_ids.add(str(_doc.get('leg_id') or ''))
            for _doc in db._db['algo_leg_feature_status'].find(
                {'trade_id': trade_id, 'feature': {'$in': ['momentum_pending', 'pending_entry']}, 'status': 'active'},
                {'leg_id': 1, 'lazy_leg_ref': 1},
            ):
                existing_ids.add(str(_doc.get('leg_id') or ''))
                if _doc.get('lazy_leg_ref'):
                    existing_ids.add(str(_doc.get('lazy_leg_ref') or ''))
        except Exception:
            pass
        if lazy_ref in existing_ids:
            log.info('Lazy leg %s already exists for trade %s — skipping', lazy_ref, trade_id)
            return None
        lazy_count = _count_child_legs(trade, triggered_by, is_lazy=True)
        leg_type = f'{parent_leg_type}-lazyleg_{lazy_count + 1}' if parent_leg_type else f'lazyleg_{lazy_count + 1}'
        # If lazy leg has momentum, queue it to algo_leg_feature_status instead of legs array
        if _has_momentum_config(lazy_cfg):
            queued = _queue_lazy_momentum_to_feature_status(
                db, trade_id, lazy_ref, lazy_cfg, triggered_by, leg_type, now_ts
            )
            if queued:
                return f'LazyLeg:{lazy_ref} momentum_pending queued to feature_status leg_type={leg_type}'
            return None
        if strict_history_mode:
            queued = _queue_pending_entry_feature_status(
                db, trade, lazy_ref, lazy_cfg,
                triggered_by=triggered_by,
                leg_type=leg_type,
                now_ts=now_ts,
                is_lazy=True,
            )
            if queued:
                return f'LazyLeg:{lazy_ref} pending_entry queued leg_type={leg_type}'
            return None
        new_leg = _build_pending_leg(lazy_ref, lazy_cfg, trade, now_ts, triggered_by, leg_type=leg_type)
        push_new_leg_in_db(db, trade_id, new_leg)
        return f'LazyLeg:{lazy_ref} queued leg_type={leg_type}'

    # ── Immediate → re-enter same leg ASAP ────────────────────────────────────
    if 'Immediate' in reentry_type:
        count = _safe_int((reentry_value or {}).get('ReentryCount') if isinstance(reentry_value, dict) else reentry_value)
        if count <= 0:
            return None
        orig_id = str(leg_config.get('id') or triggered_by)
        reentry_id = f'{orig_id}_re_{now_ts.replace(":", "").replace("T", "")}'
        reentry_count = _count_child_legs(trade, triggered_by, is_lazy=False)
        leg_type = f'{parent_leg_type}-reentry_{reentry_count + 1}' if parent_leg_type else f'reentry_{reentry_count + 1}'
        new_leg = _build_pending_leg(reentry_id, leg_config, trade, now_ts, triggered_by, leg_type=leg_type)
        new_leg['reentry_count_remaining'] = count - 1
        new_leg['reentry_type'] = 'Immediate'
        new_leg['skip_momentum_check'] = True  # Immediate reentry must enter ASAP, skip LegMomentum gate
        if strict_history_mode:
            queued = _queue_pending_entry_feature_status(
                db, trade, reentry_id, leg_config,
                triggered_by=triggered_by,
                leg_type=leg_type,
                now_ts=now_ts,
                reentry_type='Immediate',
                reentry_count_remaining=count - 1,
                is_lazy=True,
                skip_momentum_check=True,
            )
            if queued:
                return f'Reentry:Immediate pending_entry queued ({count}x) leg_type={leg_type}'
            return None
        push_new_leg_in_db(db, trade_id, new_leg)
        return f'Reentry:Immediate queued ({count}x) leg_type={leg_type}'

    # ── AtCost → wait for price to come back to entry cost ────────────────────
    if 'AtCost' in reentry_type:
        orig_id = str(leg_config.get('id') or triggered_by)
        reentry_id = f'{orig_id}_re_{now_ts.replace(":", "").replace("T", "")}'
        reentry_count = _count_child_legs(trade, triggered_by, is_lazy=False)
        leg_type = f'{parent_leg_type}-reentry_{reentry_count + 1}' if parent_leg_type else f'reentry_{reentry_count + 1}'
        new_leg = _build_pending_leg(reentry_id, leg_config, trade, now_ts, triggered_by, leg_type=leg_type)
        new_leg['reentry_type'] = 'AtCost'
        _rc_count = _safe_int((reentry_value or {}).get('ReentryCount') if isinstance(reentry_value, dict) else reentry_value)
        new_leg['reentry_count_remaining'] = max(0, _rc_count - 1)
        # AtCost must skip LegMomentum gate — it uses its own cost price check instead.
        new_leg['skip_momentum_check'] = True
        # Copy parent leg's strike/expiry/token/symbol (same strike for re-entry) and cost price.
        _at_parent = next(
            (l for l in (trade.get('legs') or [])
             if isinstance(l, dict) and str(l.get('id') or '') == triggered_by),
            None,
        )
        if _at_parent is None:
            try:
                _at_parent = db._db['algo_trade_positions_history'].find_one(
                    {'trade_id': trade_id, 'leg_id': triggered_by},
                )
            except Exception:
                _at_parent = None
        if _at_parent:
            _at_cost_price = _safe_float((_at_parent.get('entry_trade') or {}).get('price'))
            new_leg['strike']      = _at_parent.get('strike')
            new_leg['expiry_date'] = _at_parent.get('expiry_date')
            new_leg['token']       = _at_parent.get('token')
            new_leg['symbol']      = _at_parent.get('symbol')
            new_leg['spot_at_queue'] = _at_parent.get('last_saw_price') or _at_parent.get('spot_at_queue')
            # momentum_base_price = cost price (original entry price to wait for)
            if _at_cost_price > 0:
                new_leg['momentum_base_price'] = _at_cost_price
                # momentum_target_price = same as cost price (AtCost enters exactly at cost)
                new_leg['momentum_target_price'] = _at_cost_price
                new_leg['momentum_reference_set_at'] = now_ts
                new_leg['momentum_armed_notified_at'] = now_ts
        if strict_history_mode:
            queued = _queue_pending_entry_feature_status(
                db, trade, reentry_id, leg_config,
                triggered_by=triggered_by,
                leg_type=leg_type,
                now_ts=now_ts,
                reentry_type='AtCost',
                reentry_count_remaining=max(0, _rc_count - 1),
                is_lazy=True,
                skip_momentum_check=True,
                strike=new_leg.get('strike'),
                expiry_date=new_leg.get('expiry_date'),
                token=new_leg.get('token'),
                symbol=new_leg.get('symbol'),
                momentum_base_price=new_leg.get('momentum_base_price'),
                momentum_target_price=new_leg.get('momentum_target_price'),
                momentum_reference_set_at=new_leg.get('momentum_reference_set_at'),
                momentum_armed_notified_at=new_leg.get('momentum_armed_notified_at'),
                spot_at_queue=new_leg.get('spot_at_queue'),
            )
            if queued:
                return f'Reentry:AtCost pending_entry queued leg_type={leg_type}'
            return None
        push_new_leg_in_db(db, trade_id, new_leg)
        return f'Reentry:AtCost queued leg_type={leg_type}'

    # ── LikeOriginal (Momentum) ────────────────────────────────────────────────
    if 'LikeOriginal' in reentry_type:
        count = _safe_int(reentry_value)
        if count <= 0:
            return None
        orig_id = str(leg_config.get('id') or triggered_by)
        reentry_id = f'{orig_id}_re_{now_ts.replace(":", "").replace("T", "")}'
        reentry_count = _count_child_legs(trade, triggered_by, is_lazy=False)
        leg_type = f'{parent_leg_type}-reentry_{reentry_count + 1}' if parent_leg_type else f'reentry_{reentry_count + 1}'
        new_leg = _build_pending_leg(reentry_id, leg_config, trade, now_ts, triggered_by, leg_type=leg_type)
        new_leg['reentry_type'] = 'LikeOriginal'
        new_leg['reentry_count_remaining'] = count - 1
        if strict_history_mode:
            queued = _queue_pending_entry_feature_status(
                db, trade, reentry_id, leg_config,
                triggered_by=triggered_by,
                leg_type=leg_type,
                now_ts=now_ts,
                reentry_type='LikeOriginal',
                reentry_count_remaining=count - 1,
                is_lazy=True,
            )
            if queued:
                return f'Reentry:LikeOriginal pending_entry queued leg_type={leg_type}'
            return None
        push_new_leg_in_db(db, trade_id, new_leg)
        return f'Reentry:LikeOriginal queued leg_type={leg_type}'

    return None


def trigger_live_exit_followups(
    db: MongoData,
    trade_id: str,
    leg_id: str,
    exit_reason: str,
    now_ts: str,
    fill_price: float = 0.0,
) -> list[str]:
    """
    Called after broker-side fill is confirmed and leg is locally closed.
    Order: broker fill → local close → THIS function → lazy leg activation.
    Handles: feature status updates, reentry/lazy-leg queuing, momentum check.
    """
    normalized_trade_id = str(trade_id or '').strip()
    normalized_leg_id = str(leg_id or '').strip()
    normalized_reason = str(exit_reason or '').strip().lower()
    if not normalized_trade_id or not normalized_leg_id:
        return []
    if normalized_reason not in {'stoploss', 'target'}:
        return []

    trade = db._db['algo_trades'].find_one({'_id': normalized_trade_id}) or {}
    if not trade:
        return []

    live_leg = next(
        (
            item for item in (trade.get('legs') or [])
            if isinstance(item, dict) and str(item.get('id') or '').strip() == normalized_leg_id
        ),
        None,
    )
    if live_leg is None:
        live_leg = db._db['algo_trade_positions_history'].find_one(
            {'trade_id': normalized_trade_id, 'leg_id': normalized_leg_id},
        ) or {}

    all_leg_configs = _resolve_trade_leg_configs(trade)
    leg_cfg = _resolve_leg_cfg(normalized_leg_id, live_leg or {}, all_leg_configs)

    actions: list[str] = []

    # ── Feature status + notification (SL/TP only) ────────────────────────────
    if normalized_reason in {'stoploss', 'target'} and live_leg and fill_price > 0:
        try:
            from features.notification_manager import (
                record_sl_hit, record_target_hit,
                trigger_leg_feature, disable_leg_features,
            )
            from features.position_manager import calc_sl_price, calc_tp_price  # type: ignore
            _entry_price = _safe_float((live_leg.get('entry_trade') or {}).get('price') or live_leg.get('entry_price'))
            _is_sell = _is_sell(str(live_leg.get('position') or ''))
            if normalized_reason == 'stoploss':
                _sl_cfg = (leg_cfg or {}).get('LegStopLoss') or {}
                sl_price = _safe_float(live_leg.get('current_sl_price') or live_leg.get('initial_sl_value'))
                if not sl_price and _entry_price and _sl_cfg:
                    sl_price = _safe_float(calc_sl_price(_entry_price, _is_sell, _sl_cfg))
                record_sl_hit(db._db, trade, live_leg, now_ts, fill_price, sl_price or 0.0)
                trigger_leg_feature(db._db, normalized_trade_id, normalized_leg_id, 'sl', fill_price, now_ts)
                disable_leg_features(db._db, normalized_trade_id, normalized_leg_id, except_feature='sl', reason='sl_triggered', timestamp=now_ts)
            else:
                _tp_cfg = (leg_cfg or {}).get('LegTarget') or {}
                tp_price = 0.0
                if _entry_price and _tp_cfg:
                    tp_price = _safe_float(calc_tp_price(_entry_price, _is_sell, _tp_cfg))
                record_target_hit(db._db, trade, live_leg, now_ts, fill_price, tp_price or 0.0)
                trigger_leg_feature(db._db, normalized_trade_id, normalized_leg_id, 'target', fill_price, now_ts)
                disable_leg_features(db._db, normalized_trade_id, normalized_leg_id, except_feature='target', reason='target_triggered', timestamp=now_ts)
        except Exception as _nf_exc:
            log.warning('trigger_live_exit_followups notification error trade=%s leg=%s: %s', normalized_trade_id, normalized_leg_id, _nf_exc)

    # ── Reentry / lazy-leg queuing (SL/TP only) ───────────────────────────────
    if normalized_reason in {'stoploss', 'target'} and leg_cfg:
        reentry_cfg = (
            get_reentry_sl_config(leg_cfg)
            if normalized_reason == 'stoploss'
            else get_reentry_tp_config(leg_cfg)
        )
        if reentry_cfg:
            refreshed_trade_for_reentry = db._db['algo_trades'].find_one({'_id': normalized_trade_id}) or trade
            reentry_result = _handle_reentry(
                db,
                refreshed_trade_for_reentry,
                dict(leg_cfg, id=normalized_leg_id),
                reentry_cfg,
                normalized_leg_id,
                now_ts,
            )
            if reentry_result:
                actions.append(reentry_result)
                print(f'[LIVE REENTRY QUEUED] trade={normalized_trade_id} leg={normalized_leg_id} reason={normalized_reason} result={reentry_result}')

    # ── Momentum pending leg check (always — lazy legs may be waiting) ─────────
    refreshed_trade = db._db['algo_trades'].find_one({'_id': normalized_trade_id}) or trade
    underlying = str(
        refreshed_trade.get('ticker')
        or ((refreshed_trade.get('config') or {}).get('Ticker') or '')
    ).strip()
    try:
        lot_size = db.get_lot_size(now_ts[:10], underlying)
    except Exception:
        lot_size = 75

    try:
        entered_ids = _process_momentum_pending_feature_legs(
            db,
            refreshed_trade,
            db._db[OPTION_CHAIN_COLLECTION],
            now_ts[:10],
            now_ts,
            lot_size,
        ) or []
        actions.extend([f'immediate_check:{_id}' for _id in entered_ids if _id])
    except Exception as exc:
        log.warning(
            'trigger_live_exit_followups immediate check error trade=%s leg=%s reason=%s: %s',
            normalized_trade_id, normalized_leg_id, normalized_reason, exc,
        )

    if actions:
        print(
            f'[LIVE EXIT FOLLOWUPS] trade={normalized_trade_id} leg={normalized_leg_id} '
            f'reason={normalized_reason} actions={actions}'
        )
    return actions


# ─── Entry resolver for pending legs ──────────────────────────────────────────

def _try_enter_pending_leg(db: MongoData, trade: dict, leg: dict,
                            leg_index: int, chain_col, trade_date: str,
                            now_ts: str, lot_size: int,
                            snapshot_timestamp: str | None = None,
                            index_spot_doc: dict | None = None,
                            market_cache: dict | None = None,
                            live_chain_cache: dict | None = None) -> tuple[bool, str | None]:
    """
    Try to fill a pending (is_lazy=True, entry_trade=None) leg.
    Resolves strike/expiry from option_chain ATM and sets entry_trade.
    Returns True if entered.
    """
    if trade.get('is_paused'):
        return False, 'strategy_paused'

    underlying = str(
        (trade.get('strategy') or {}).get('Ticker')
        or (trade.get('config') or {}).get('Ticker')
        or trade.get('ticker')
        or ''
    )
    option_type = str(leg.get('option') or 'CE')
    activation_mode = str(trade.get('activation_mode') or '').strip() or 'algo-backtest'
    feature_row_id = str(leg.get('feature_row_id') or '')
    pending_feature_type = str(leg.get('pending_feature') or '')
    is_pending_feature_leg = bool(leg.get('is_pending_feature_leg'))
    entry_kind = str(leg.get('entry_kind') or '')
    leg_id_log = str(leg.get('id') or '')
    strike_param_raw = leg.get('strike_parameter')

    if not underlying:
        entry_print(f'[ENTRY MISS] leg={leg_id_log} reason=no_underlying trade_id={trade.get("_id")}')
        return False, 'no_underlying'

    mode_entry_payload = {}
    if activation_mode in {'fast-forward', 'live', 'forward-test'}:
        mode_entry_payload = resolve_entry_execution_payload_for_mode(
            db,
            trade,
            leg,
            now_ts=snapshot_timestamp or now_ts,
        ) or {}

    index_spot_doc = index_spot_doc or {}
    if not index_spot_doc:
        if activation_mode in {'fast-forward', 'live', 'forward-test'}:
            mode_spot_price = _safe_float(mode_entry_payload.get('spot_price'))
            if mode_spot_price > 0:
                index_spot_doc = {
                    'underlying': underlying,
                    'spot_price': mode_spot_price,
                    'timestamp': snapshot_timestamp or now_ts,
                    'source': 'fast_forward_event' if activation_mode in ('fast-forward', 'forward-test') else 'kite_live',
                }
        elif _is_strict_history_leg_mode(activation_mode):
            index_spot_doc = _resolve_mode_spot_doc_for_entry(
                db,
                trade,
                leg,
                now_ts=snapshot_timestamp or now_ts,
            )
        if not index_spot_doc:
            if activation_mode in {'fast-forward', 'live', 'forward-test'}:
                # Live/FF: get spot directly from Kite ticker
                try:
                    from features.live_monitor_socket import _get_active_ticker_manager
                    _kite_spot_fb = _safe_float(_get_active_ticker_manager().get_spot(underlying))
                    if _kite_spot_fb > 0:
                        index_spot_doc = {
                            'underlying': underlying,
                            'spot_price': _kite_spot_fb,
                            'timestamp': snapshot_timestamp or now_ts,
                            'source': 'kite_ticker_direct',
                        }
                except Exception:
                    pass
            else:
                index_spot_col = db._db['option_chain_index_spot']
                index_spot_doc = get_index_spot_at_time(
                    index_spot_col,
                    underlying,
                    snapshot_timestamp or now_ts,
                    market_cache=market_cache,
                    activation_mode=activation_mode,
                )
                if not index_spot_doc.get('spot_price') and activation_mode == 'algo-backtest':
                    from features.algo_backtest_event import get_spot_price_from_chain_col as _gspfc3
                    _chain_col_fb = db._db[OPTION_CHAIN_COLLECTION]
                    _fb_spot3 = _gspfc3(_chain_col_fb, underlying, snapshot_timestamp or now_ts)
                    if _fb_spot3 > 0:
                        index_spot_doc = {'underlying': underlying, 'spot_price': _fb_spot3, 'timestamp': snapshot_timestamp or now_ts, 'source': 'chain_col_fallback'}

    spot_price = _safe_float(index_spot_doc.get('spot_price'))

    # Kite quote API fallback for spot price — handles post-3:30 PM entries where
    # the socket is no longer streaming. Only for live/fast-forward mode.
    if spot_price <= 0 and activation_mode in {'live', 'fast-forward', 'forward-test'}:
        try:
            from features.fast_forward_event import QUOTE_INSTRUMENT_BY_UNDERLYING  # type: ignore
            from features.broker_gateway import get_broker_rest_client  # type: ignore
            _spot_instr = QUOTE_INSTRUMENT_BY_UNDERLYING.get(underlying.upper(), '')
            if _spot_instr:
                _kite = get_broker_rest_client(db)
                if _kite:
                    _q = (_kite.quote([_spot_instr]) or {}).get(_spot_instr) or {}
                    _qp = _safe_float(_q.get('last_price'))
                    if _qp > 0:
                        spot_price = _qp
                        print(f'[SPOT QUOTE] leg={leg_id_log} underlying={underlying} spot={spot_price} source=kite_quote')
        except Exception as _spot_exc:
            log.warning('[SPOT QUOTE] leg=%s underlying=%s error: %s', leg_id_log, underlying, _spot_exc)

    print(
        f'[STRIKE CALC] leg={leg_id_log} type={option_type} '
        f'spot_price={spot_price} mode={activation_mode} '
        f'source={"fast_forward_event" if activation_mode in ("fast-forward", "forward-test") else ("kite_live" if _is_strict_history_leg_mode(activation_mode) else "db")}'
    )

    if spot_price <= 0:
        entry_print(f'[ENTRY MISS] leg={leg_id_log} underlying={underlying} snapshot={snapshot_timestamp} reason=spot_price_missing_or_zero')
        return False, 'spot_missing'

    # ── Resolve leg config early (needed for momentum gate) ──────────────────
    _leg_id_str_early = str(leg.get('id') or '')
    _all_leg_cfgs_early = _resolve_trade_leg_configs(trade)
    _leg_cfg_early = _resolve_leg_cfg(_leg_id_str_early, leg, _all_leg_cfgs_early)

    # ── LegMomentum gate ─────────────────────────────────────────────────────
    # skip_momentum_check=True bypasses LegMomentum for this leg.
    # Set on all overall-reentry legs (both Immediate and LikeOriginal) so they
    # always enter directly — momentum is only for the initial parent entry.
    if leg.get('skip_momentum_check'):
        _momentum_type  = 'None'
        _momentum_value = 0.0
    else:
        _momentum_cfg   = _leg_cfg_early.get('LegMomentum') or {}
        _momentum_type  = str(_momentum_cfg.get('Type') or 'None')
        _momentum_value = _safe_float(_momentum_cfg.get('Value'))
    # Resolve expiry / strike selector early so momentum can be armed on a fixed contract.
    from features.strike_selector import resolve_expiry, resolve_strike as _resolve_strike_selector
    expiry = str(mode_entry_payload.get('expiry_date') or leg.get('expiry_date') or '')
    strike = mode_entry_payload.get('strike')
    if strike in (None, ''):
        strike = leg.get('strike')
    token = str(mode_entry_payload.get('token') or leg.get('token') or '')
    symbol = str(mode_entry_payload.get('symbol') or leg.get('symbol') or '')
    resolved_chain_doc: dict = {}

    if activation_mode in {'fast-forward', 'live', 'forward-test'}:
        expiry = expiry[:10] if ' ' in expiry else expiry
        if not expiry:
            _expiry_kind = str(leg.get('expiry_kind') or 'ExpiryType.Weekly')
            tok_col = db._db['active_option_tokens']
            _ff_broker_filter: dict = {}
            try:
                from features.market_feed_tokens import active_token_broker_filter as _atbf_ff  # type: ignore
                _ff_broker_filter = _atbf_ff(db)
            except Exception:
                pass
            expiry  = _resolve_expiry_from_tokens(tok_col, underlying, option_type.upper(), trade_date, _expiry_kind, broker_filter=_ff_broker_filter) or ''
        # Always fetch full live option chain and select strike — no conditions.
        from features.live_option_chain import fetch_full_chain, select_strike_live  # type: ignore
        _chain_cache_key = (underlying, expiry)
        if live_chain_cache is not None and _chain_cache_key in live_chain_cache:
            _chain = live_chain_cache[_chain_cache_key]
            entry_print(f'[LIVE CHAIN] leg={leg_id_log} reusing cached chain {underlying} {expiry}')
        else:
            _chain = fetch_full_chain(db, underlying, expiry, spot_price, leg_id=leg_id_log)
            if live_chain_cache is not None:
                live_chain_cache[_chain_cache_key] = _chain
        if not _chain.get('CE') and not _chain.get('PE'):
            print(
                f'[ENTRY MISS] leg={leg_id_log} underlying={underlying} option={option_type} '
                f'expiry={expiry} reason=empty_live_chain'
            )
            return False, 'empty_live_chain'
        _sel = select_strike_live(
            _chain, entry_kind, strike_param_raw,
            option_type, str(leg.get('position') or ''),
            spot_price, underlying, leg_id=leg_id_log,
        )
        if not _sel:
            print(
                f'[ENTRY MISS] leg={leg_id_log} underlying={underlying} option={option_type} '
                f'expiry={expiry} reason=no_strike_found entry_kind={entry_kind}'
            )
            return False, 'no_strike_found'
        strike = _sel['strike']
        token  = str(_sel['token'] or '')
        symbol = str(_sel['symbol'] or '')
        mode_entry_payload = {
            **mode_entry_payload,
            'strike': strike, 'token': token, 'symbol': symbol,
            'entry_price': _sel['ltp'],
            'current_option_price': _sel['ltp'],
            'ltp': _sel['ltp'],
            'chain_iv': _safe_float(_sel.get('iv')),
            'chain_delta': _safe_float(_sel.get('delta')),
        }
    elif not expiry or strike in (None, ''):
        expiry, expiry_err = resolve_expiry(chain_col, underlying, option_type, trade_date, snapshot_timestamp)
        if expiry_err:
            entry_print(f'[ENTRY MISS] leg={leg_id_log} underlying={underlying} option={option_type} trade_date={trade_date} reason={expiry_err}')
            return False, expiry_err
        entry_print(f'[STRIKE CALC] leg={leg_id_log} type={option_type} expiry_resolved={expiry}')

        sel = _resolve_strike_selector(
            chain_col=chain_col,
            underlying=underlying,
            option_type=option_type,
            entry_kind=entry_kind,
            strike_param_raw=strike_param_raw,
            position=str(leg.get('position') or ''),
            spot_price=spot_price,
            expiry=expiry,
            trade_date=trade_date,
            snapshot_timestamp=snapshot_timestamp,
            market_cache=market_cache,
            leg_id=leg_id_log,
        )
        if sel.error:
            entry_print(f'[ENTRY MISS] leg={leg_id_log} underlying={underlying} option={option_type} expiry={expiry} reason={sel.error}')
            return False, sel.error
        strike = sel.strike
        resolved_chain_doc = _normalize_chain_market_fields(sel.chain_doc or {})
        token = str(resolved_chain_doc.get('token') or token or make_token(underlying, expiry, strike, option_type))
        symbol = str(resolved_chain_doc.get('symbol') or symbol or token)
    else:
        expiry = expiry[:10] if ' ' in expiry else expiry

    # For live/FF: upgrade any chain-format token to Kite integer token from active_option_tokens
    if activation_mode in {'fast-forward', 'live', 'forward-test'} and expiry and strike not in (None, ''):
        if not token or not str(token).isdigit():
            try:
                _atok = db._db['active_option_tokens'].find_one({
                    'instrument': underlying,
                    'expiry': expiry[:10],
                    'strike': strike,
                    'option_type': option_type.upper(),
                }) or {}
                _kite_tok = str(_atok.get('token') or _atok.get('tokens') or '').strip()
                if _kite_tok and _kite_tok.isdigit():
                    token = _kite_tok
                    symbol = str(_atok.get('symbol') or symbol or token)
                    entry_print(f'[ENTRY KITE TOKEN] leg={leg_id_log} underlying={underlying} strike={strike} option={option_type} kite_tok={token}')
            except Exception:
                pass

    if 'None' not in _momentum_type and _momentum_value > 0:
        current_chain_doc = resolved_chain_doc
        if activation_mode in {'fast-forward', 'live', 'forward-test'}:
            current_option_price = _safe_float(
                mode_entry_payload.get('current_option_price')
                or mode_entry_payload.get('entry_price')
                or mode_entry_payload.get('ltp')
            )
        else:
            current_option_price = 0.0
        if not current_chain_doc and activation_mode not in {'fast-forward', 'live', 'forward-test'}:
            if snapshot_timestamp:
                current_chain_doc = _normalize_chain_market_fields(
                    get_chain_at_time(
                        chain_col,
                        underlying,
                        expiry,
                        strike,
                        option_type,
                        snapshot_timestamp,
                        market_cache=market_cache,
                        activation_mode=activation_mode,
                    )
                )
            else:
                current_chain_doc = _normalize_chain_market_fields(
                    get_latest_chain_doc(
                        chain_col,
                        underlying,
                        expiry,
                        strike,
                        option_type,
                        trade_date,
                        activation_mode=activation_mode,
                    )
                )

        base_price = _safe_float(leg.get('momentum_base_price'))
        target_price = _safe_float(leg.get('momentum_target_price'))
        if activation_mode not in {'fast-forward', 'live', 'forward-test'}:
            current_option_price = _resolve_chain_market_price(current_chain_doc)
            token = str(current_chain_doc.get('token') or token or make_token(underlying, expiry, strike, option_type))
            symbol = str(current_chain_doc.get('symbol') or symbol or token)

        # Underlying momentum types track the index/spot; all others track the option price
        _mom_check_px = spot_price if 'Underlying' in str(_momentum_type or '') else current_option_price

        if base_price <= 0 or target_price <= 0:
            if _mom_check_px <= 0:
                entry_print(f'[MOMENTUM WAIT] leg={leg_id_log} type={_momentum_type} reason=base_price_missing')
                return False, 'momentum_wait'
            base_price = _mom_check_px
            target_price = _resolve_simple_momentum_target(base_price, _momentum_type, _momentum_value)
            try:
                if is_pending_feature_leg and feature_row_id:
                    db._db['algo_leg_feature_status'].update_one(
                        {'_id': ObjectId(feature_row_id)},
                        {'$set': {
                            'strike': strike,
                            'expiry_date': _normalize_expiry_datetime(expiry),
                            'token': token,
                            'symbol': symbol,
                            'spot_at_queue': spot_price,
                            'momentum_base_price': base_price,
                            'momentum_target_price': target_price,
                            'momentum_reference_set_at': now_ts,
                            'momentum_armed_notified_at': now_ts,
                            'armed_at': now_ts,
                            'updated_at': now_ts,
                        }},
                    )
                else:
                    db._db['algo_trades'].update_one(
                        {'_id': str(trade.get('_id') or '')},
                        {'$set': {
                            'legs.$[elem].strike': strike,
                            'legs.$[elem].expiry_date': _normalize_expiry_datetime(expiry),
                            'legs.$[elem].token': token,
                            'legs.$[elem].symbol': symbol,
                            'legs.$[elem].spot_at_queue': spot_price,
                            'legs.$[elem].momentum_base_price': base_price,
                            'legs.$[elem].momentum_target_price': target_price,
                            'legs.$[elem].momentum_reference_set_at': now_ts,
                            'legs.$[elem].momentum_armed_notified_at': now_ts,
                        }},
                        array_filters=[{'elem.id': leg_id_log}],
                    )
                leg['strike'] = strike
                leg['expiry_date'] = _normalize_expiry_datetime(expiry)
                leg['token'] = token
                leg['symbol'] = symbol
                leg['spot_at_queue'] = spot_price
                leg['momentum_base_price'] = base_price
                leg['momentum_target_price'] = target_price
                leg['momentum_reference_set_at'] = now_ts
                leg['momentum_armed_notified_at'] = now_ts
            except Exception as exc:
                log.warning('momentum arm state save error leg=%s: %s', leg_id_log, exc)
            try:
                from features.notification_manager import record_simple_momentum_armed
                record_simple_momentum_armed(
                    db._db, trade, leg, _leg_cfg_early, now_ts,
                    base_price=base_price, target_price=target_price, spot_price=spot_price,
                )
            except Exception as exc:
                log.warning('simple momentum armed notification error leg=%s: %s', leg_id_log, exc)
            print(
                f'[MOMENTUM ARMED] leg={leg_id_log} type={_momentum_type} '
                f'value={_momentum_value} base={base_price} target={target_price} '
                f'strike={strike} option={option_type}'
            )
            return False, 'momentum_wait'

        if not _is_simple_momentum_triggered(_mom_check_px, target_price, _momentum_type):
            runtime_print(
                f'[MOMENTUM WAIT] leg={leg_id_log} type={_momentum_type} '
                f'value={_momentum_value} base={base_price} target={target_price} '
                f'current={_mom_check_px} strike={strike}'
            )
            return False, 'momentum_wait'

        runtime_print(
            f'[MOMENTUM OK] leg={leg_id_log} type={_momentum_type} value={_momentum_value} '
            f'base={base_price} target={target_price} current={_mom_check_px} — proceeding to entry'
        )
        entry_price = current_option_price if _is_market_order(_leg_cfg_early, 'entry') else target_price
        resolved_chain_doc = current_chain_doc
        try:
            if not str(leg.get('momentum_triggered_notified_at') or '').strip():
                from features.notification_manager import record_simple_momentum_triggered
                record_simple_momentum_triggered(
                    db._db, trade, leg, _leg_cfg_early, now_ts,
                    base_price=base_price,
                    target_price=target_price,
                    current_price=current_option_price,
                    spot_price=spot_price,
                )
            if is_pending_feature_leg and feature_row_id:
                db._db['algo_leg_feature_status'].update_one(
                    {'_id': ObjectId(feature_row_id)},
                    {'$set': {
                        'momentum_triggered_at': now_ts,
                        'momentum_triggered_notified_at': now_ts,
                        'triggered_at': now_ts,
                        'updated_at': now_ts,
                    }},
                )
            else:
                db._db['algo_trades'].update_one(
                    {'_id': str(trade.get('_id') or '')},
                    {'$set': {
                        'legs.$[elem].momentum_triggered_at': now_ts,
                        'legs.$[elem].momentum_triggered_notified_at': now_ts,
                    }},
                    array_filters=[{'elem.id': leg_id_log}],
                )
            leg['momentum_triggered_at'] = now_ts
            leg['momentum_triggered_notified_at'] = now_ts
        except Exception as exc:
            log.warning('simple momentum trigger state error leg=%s: %s', leg_id_log, exc)
    else:
        # Resolve strike on the current tick when no simple momentum is configured.
        if not resolved_chain_doc:
            # ── AtCost gate: wait for option price to return to original entry cost ──
            # strike/expiry/token/symbol are already set (copied from parent at queue time).
            # momentum_base_price = cost price (parent's entry price).
            if str(leg.get('reentry_type') or '') == 'AtCost' and leg.get('strike') and leg.get('expiry_date'):
                _at_expiry = str(leg.get('expiry_date') or '')
                if ' ' in _at_expiry:
                    _at_expiry = _at_expiry[:10]
                _at_strike  = leg['strike']
                if activation_mode in {'fast-forward', 'live', 'forward-test'}:
                    _at_cur_price = _safe_float(
                        mode_entry_payload.get('current_option_price')
                        or mode_entry_payload.get('entry_price')
                        or mode_entry_payload.get('ltp')
                    )
                    _at_chain = {}
                else:
                    _at_chain   = _normalize_chain_market_fields(
                        get_chain_at_time(
                            chain_col,
                            underlying,
                            _at_expiry,
                            _at_strike,
                            option_type,
                            snapshot_timestamp or now_ts,
                            market_cache=market_cache,
                            activation_mode=activation_mode,
                        )
                        if snapshot_timestamp
                        else get_latest_chain_doc(
                            chain_col,
                            underlying,
                            _at_expiry,
                            _at_strike,
                            option_type,
                            trade_date,
                            activation_mode=activation_mode,
                        )
                    )
                    _at_cur_price = _resolve_chain_market_price(_at_chain)
                _at_cost      = _safe_float(leg.get('momentum_base_price'))
                _at_is_sell   = 'sell' in str(leg.get('position') or '').lower()
                # SELL: SL hit = price ROSE above cost; wait for price to FALL back to cost
                # BUY:  SL hit = price FELL below cost; wait for price to RISE back to cost
                _at_reached = (_at_cur_price <= _at_cost) if _at_is_sell else (_at_cur_price >= _at_cost)
                if _at_cost <= 0 or not _at_reached:
                    print(
                        f'[ATCOST WAIT] leg={leg_id_log} cost={_at_cost} '
                        f'current={_at_cur_price} sell={_at_is_sell}'
                    )
                    return False, 'atcost_wait'
                print(f'[ATCOST OK] leg={leg_id_log} cost={_at_cost} current={_at_cur_price} — entering at cost')
                expiry            = _at_expiry
                strike            = _at_strike
                resolved_chain_doc = _at_chain
                if activation_mode not in {'fast-forward', 'live', 'forward-test'}:
                    token  = str(_at_chain.get('token') or token or make_token(underlying, expiry, strike, option_type))
                    symbol = str(_at_chain.get('symbol') or symbol or token)
                entry_price = _at_cost  # limit-order style: enter exactly at original entry cost
            else:
                if activation_mode in {'fast-forward', 'live', 'forward-test'}:
                    entry_price = _safe_float(mode_entry_payload.get('entry_price'))
                    if entry_price <= 0:
                        miss_reason = 'fast_forward_ltp_missing' if activation_mode in ('fast-forward', 'forward-test') else 'live_ltp_missing'
                        entry_print(f'[ENTRY MISS] leg={leg_id_log} underlying={underlying} option={option_type} reason={miss_reason}')
                        return False, miss_reason
                else:
                    expiry, expiry_err = resolve_expiry(chain_col, underlying, option_type, trade_date, snapshot_timestamp)
                    if expiry_err:
                        entry_print(f'[ENTRY MISS] leg={leg_id_log} underlying={underlying} option={option_type} trade_date={trade_date} reason={expiry_err}')
                        return False, expiry_err
                    entry_print(f'[STRIKE CALC] leg={leg_id_log} type={option_type} expiry_resolved={expiry}')

                    sel = _resolve_strike_selector(
                        chain_col=chain_col,
                        underlying=underlying,
                        option_type=option_type,
                        entry_kind=entry_kind,
                        strike_param_raw=strike_param_raw,
                        position=str(leg.get('position') or ''),
                        spot_price=spot_price,
                        expiry=expiry,
                        trade_date=trade_date,
                        snapshot_timestamp=snapshot_timestamp,
                        market_cache=market_cache,
                        leg_id=leg_id_log,
                    )
                    if sel.error:
                        entry_print(f'[ENTRY MISS] leg={leg_id_log} underlying={underlying} option={option_type} expiry={expiry} reason={sel.error}')
                        return False, sel.error
                    strike = sel.strike
                    entry_price = sel.entry_price
                    resolved_chain_doc = _normalize_chain_market_fields(sel.chain_doc or {})
                    token = str(resolved_chain_doc.get('token') or token or make_token(underlying, expiry, strike, option_type))
                    symbol = str(resolved_chain_doc.get('symbol') or symbol or token)
        else:
            entry_price = _resolve_chain_market_price(resolved_chain_doc)

    entry_price_source = 'chain'
    trade_activation_mode = str(trade.get('activation_mode') or '').strip()
    if trade_activation_mode in {'fast-forward', 'live', 'forward-test'}:
        entry_price_source = str(
            mode_entry_payload.get('entry_price_source')
            or ('fast_forward_event' if trade_activation_mode in ('fast-forward', 'forward-test') else 'kite_live')
        )

    spot_price = _safe_float(index_spot_doc.get('spot_price'))
    position_str = str(leg.get('position') or 'PositionType.Sell')
    is_sell_pos = _is_sell(position_str)

    # Compute SL price from config — check strategy.ListOfLegConfigs + IdleLegConfigs first
    leg_id_str = str(leg.get('id') or '')
    all_leg_configs = _resolve_trade_leg_configs(trade)
    leg_cfg = _resolve_leg_cfg(leg_id_str, leg, all_leg_configs)
    sl_config = leg_cfg.get('LegStopLoss') or {}
    sl_price = calc_sl_price(entry_price, is_sell_pos, sl_config)

    # quantity = LotConfig.Value directly (not multiplied by lot_size)
    lot_config_value = max(1, int(
        (leg_cfg.get('LotConfig') or {}).get('Value')
        or leg.get('lot_config_value')
        or 1
    ))
    actual_quantity = lot_config_value

    trade_date_str = str(trade_date or '')
    exchange_ts = now_ts.replace('T', ' ')[:19] if 'T' in now_ts else now_ts[:19]

    # entry_iv: from live chain (fast-forward/live) or historical chain doc (backtest)
    _chain_iv = _safe_float(mode_entry_payload.get('chain_iv')) or _safe_float((resolved_chain_doc or {}).get('iv')) or None
    # entry_vix: from Kite WS token 264969 → fallback to india_vix DB
    _entry_vix: float | None = None
    try:
        from features.trading_core import get_vix_at_time as _get_vix  # type: ignore
        _vix_val = _get_vix(db, now_ts, market_cache)
        _entry_vix = _vix_val if _vix_val > 0 else None
    except Exception:
        pass

    log.warning(
        '[ENTRY IV/VIX] leg=%s mode=%s chain_doc_iv=%s mode_payload_chain_iv=%s '
        'entry_iv=%s entry_vix=%s chain_doc_keys=%s',
        leg_id_log, activation_mode,
        _safe_float((resolved_chain_doc or {}).get('iv')),
        _safe_float(mode_entry_payload.get('chain_iv')),
        _chain_iv, _entry_vix,
        list((resolved_chain_doc or {}).keys()),
    )

    entry_trade_payload = {
        'trigger_timestamp': exchange_ts,
        'trigger_price': entry_price,
        'underlying_trigger_price': spot_price,
        'price': entry_price,
        'quantity': actual_quantity,
        'lot_size': lot_size,
        'underlying_at_trade': spot_price,
        'spot_price': spot_price,
        'traded_timestamp': exchange_ts,
        'exchange_timestamp': exchange_ts,
        'expiry': expiry,
        'strike': strike,
        'option_type': option_type,
        'instrument_token': token,
        'entry_iv': _chain_iv,
        'entry_vix': _entry_vix,
    }
    _backtest_debug_log(
        f"[BACKTEST ENTRY SPOT] timestamp={exchange_ts} underlying={underlying} "
        f"spot_price={spot_price} atm_strike={strike} entry_price={entry_price} "
        f"lot_config_value={lot_config_value} actual_quantity={actual_quantity}"
    )
    print(
        f'[ENTRY PRICE SOURCE] trade_id={str(trade.get("_id") or "")} '
        f'leg={leg_id_str} mode={str(trade.get("activation_mode") or "")} '
        f'source={entry_price_source} token={token or "-"} symbol={symbol or "-"} '
        f'price={entry_price}'
    )

    # ── Pre-entry delta check (live and fast-forward) ─────────────────────────
    # If full chain was already fetched (DeltaRange / Premium entry), chain_delta is
    # already in mode_entry_payload — use it directly, no extra Kite call.
    # For known-strike legs (ATM, re-entry etc.), fetch single-strike delta via WS+REST.
    if activation_mode in {'live', 'fast-forward', 'forward-test'} and strike and option_type:
        try:
            _live_delta = _safe_float(mode_entry_payload.get('chain_delta'))
            _live_ltp   = _safe_float(mode_entry_payload.get('ltp'))
            _live_iv    = _safe_float(mode_entry_payload.get('chain_iv'))
            if _live_delta == 0.0:
                from features.live_option_chain import get_live_delta_for_strike  # type: ignore
                _delta_info = get_live_delta_for_strike(db, underlying, expiry, strike, option_type)
                _live_delta = _safe_float(_delta_info.get('delta'))
                _live_ltp   = _safe_float(_delta_info.get('ltp')) or _live_ltp
                _live_iv    = _safe_float(_delta_info.get('iv')) or _live_iv
            print(
                f'[PRE-ENTRY DELTA] leg={leg_id_str} underlying={underlying} expiry={expiry} '
                f'strike={strike} opt={option_type} delta={_live_delta} ltp={_live_ltp} iv={_live_iv}%'
            )
            _entry_kind = str(leg_cfg.get('EntryType') or '').strip()
            if 'DeltaRange' in _entry_kind and _live_delta != 0.0:
                _sp_raw = leg_cfg.get('StrikeParameter') or {}
                _lower = _safe_float((_sp_raw if isinstance(_sp_raw, dict) else {}).get('LowerRange', 0)) / 100.0
                _upper = _safe_float((_sp_raw if isinstance(_sp_raw, dict) else {}).get('UpperRange', 0)) / 100.0
                _abs_delta = abs(_live_delta)
                if _lower > 0 and _upper > 0 and not (_lower <= _abs_delta <= _upper):
                    print(
                        f'[PRE-ENTRY DELTA BLOCK] leg={leg_id_str} strike={strike} opt={option_type} '
                        f'delta={_live_delta} abs={_abs_delta:.4f} range=[{_lower:.2f},{_upper:.2f}] '
                        f'— delta out of range, skipping entry'
                    )
                    return False, 'delta_out_of_range'
        except Exception as _dex:
            log.warning('[PRE-ENTRY DELTA] check error leg=%s: %s', leg_id_str, _dex)

    mode_entry_order = _build_mode_entry_order_payload(
        db,
        trade,
        leg,
        leg_cfg,
        symbol,
        actual_quantity,
        entry_price,
        exchange_ts,
        lot_size=lot_size,
    )
    if not bool(mode_entry_order.get('allowed', True)):
        error_message = str(mode_entry_order.get('error') or 'live_order_failed')
        print(
            f'[ENTRY BLOCKED] trade_id={str(trade.get("_id") or "")} '
            f'leg={leg_id_str} mode={activation_mode} reason={error_message}'
        )
        return False, error_message
    entry_trade_payload.update(mode_entry_order.get('payload') or {})
    entry_trade_payload = _decorate_entry_trade_lifecycle(
        activation_mode,
        entry_trade_payload,
    )

    leg_is_reentered = _is_reentered_leg(leg, trade)

    try:
        if is_pending_feature_leg:
            refreshed_trade = db._db['algo_trades'].find_one({'_id': str(trade.get('_id') or '')}) or trade
            entered_leg = dict(leg)
            entered_leg.update({
                'strike': strike,
                'expiry_date': _normalize_expiry_datetime(expiry),
                'token': token,
                'symbol': symbol,
                'exchange': str(entry_trade_payload.get('exchange') or ''),
                'quantity': actual_quantity,
                'lot_size': lot_size,
                'lot_config_value': lot_config_value,
                'current_sl_price': sl_price,
                'initial_sl_value': sl_price,
                'display_sl_value': sl_price,
                'display_target_value': leg.get('display_target_value'),
                'last_saw_price': entry_price,
                'status': OPEN_LEG_STATUS,
                'is_lazy': bool(leg.get('is_lazy')),
                'is_reentered_leg': leg_is_reentered,
                'momentum_base_price': _safe_float(leg.get('momentum_base_price')) or None,
                'momentum_target_price': _safe_float(leg.get('momentum_target_price')) or None,
                'momentum_reference_set_at': leg.get('momentum_reference_set_at'),
                'momentum_triggered_at': leg.get('momentum_triggered_at') or now_ts,
                'momentum_triggered_notified_at': leg.get('momentum_triggered_notified_at'),
                'transactions': leg.get('transactions') or {},
                'current_transaction_id': leg.get('current_transaction_id'),
                'exit_trade': None,
                'entry_trade': entry_trade_payload,
            })
            if _is_live_entry_order_pending(activation_mode, entry_trade_payload):
                db._db['algo_trades'].update_one(
                    {'_id': str(trade.get('_id') or '')},
                    {
                        '$pull': {'legs': {'id': leg_id_str}},
                        '$push': {'legs': entered_leg},
                    },
                )
                inserted, history_doc = False, None
            else:
                inserted, history_doc = _store_position_history(
                    db, refreshed_trade, entered_leg, override_leg_cfg=leg_cfg
                )
            if feature_row_id:
                db._db['algo_leg_feature_status'].update_one(
                    {'_id': ObjectId(feature_row_id)},
                    {'$set': {
                        'status': 'triggered',
                        'enabled': False,
                        'triggered_at': now_ts,
                        'updated_at': now_ts,
                        'disabled_at': now_ts,
                        'disabled_reason': 'entry_taken',
                    }},
                )
            if inserted and history_doc:
                print(
                    f"[LAZY LEG ENTRY SYNC] trade={history_doc['trade_id']} "
                    f"leg={history_doc['leg_id']} "
                    f"history_id={history_doc.get('_id') or '-'}"
                )
            elif _is_live_entry_order_pending(activation_mode, entry_trade_payload):
                print(
                    f"[LIVE ENTRY ORDER OPEN] trade={str(trade.get('_id') or '')} "
                    f"leg={leg_id_str} order_id={str(entry_trade_payload.get('order_id') or '-')}"
                )
        else:
            db._db['algo_trades'].update_one(
                {'_id': str(trade.get('_id') or '')},
                {'$set': {
                    'legs.$[elem].strike': strike,
                    'legs.$[elem].expiry_date': _normalize_expiry_datetime(expiry),
                    'legs.$[elem].token': token,
                    'legs.$[elem].symbol': symbol,
                    'legs.$[elem].exchange': str(entry_trade_payload.get('exchange') or ''),
                    'legs.$[elem].quantity': actual_quantity,
                    'legs.$[elem].lot_size': lot_size,
                    'legs.$[elem].lot_config_value': lot_config_value,
                    'legs.$[elem].current_sl_price': sl_price,
                    'legs.$[elem].initial_sl_value': sl_price,
                    'legs.$[elem].display_sl_value': sl_price,
                    'legs.$[elem].display_target_value': leg.get('display_target_value'),
                    'legs.$[elem].last_saw_price': entry_price,
                    'legs.$[elem].is_lazy': False,
                    'legs.$[elem].is_reentered_leg': leg_is_reentered,
                    'legs.$[elem].momentum_base_price': _safe_float(leg.get('momentum_base_price')) or None,
                    'legs.$[elem].momentum_target_price': _safe_float(leg.get('momentum_target_price')) or None,
                    'legs.$[elem].momentum_reference_set_at': leg.get('momentum_reference_set_at'),
                    'legs.$[elem].momentum_triggered_at': leg.get('momentum_triggered_at') or now_ts,
                    'legs.$[elem].momentum_triggered_notified_at': leg.get('momentum_triggered_notified_at'),
                    'legs.$[elem].transactions': leg.get('transactions') or {},
                    'legs.$[elem].current_transaction_id': leg.get('current_transaction_id'),
                    'legs.$[elem].exit_trade': None,
                    'legs.$[elem].entry_trade': entry_trade_payload,
                }},
                array_filters=[{'elem.id': leg_id_str}],
            )
            refreshed_trade = db._db['algo_trades'].find_one({'_id': str(trade.get('_id') or '')}) or trade
            refreshed_legs = [item for item in (refreshed_trade.get('legs') or []) if isinstance(item, dict)]
            refreshed_leg = next((item for item in refreshed_legs if str(item.get('id') or '') == leg_id_str), None)
            if refreshed_leg and not _is_live_entry_order_pending(activation_mode, entry_trade_payload):
                inserted, history_doc = _store_position_history(db, refreshed_trade, refreshed_leg)
                if inserted and history_doc:
                    print(
                        f"[LAZY LEG ENTRY SYNC] trade={history_doc['trade_id']} "
                        f"leg={history_doc['leg_id']} "
                        f"history_id={history_doc.get('_id') or '-'}"
                    )
            elif refreshed_leg and _is_live_entry_order_pending(activation_mode, entry_trade_payload):
                print(
                    f"[LIVE ENTRY ORDER OPEN] trade={str(trade.get('_id') or '')} "
                    f"leg={leg_id_str} order_id={str(entry_trade_payload.get('order_id') or '-')}"
                )

        # ── AtCost reentry entered: update feature status to triggered ─────────
        if str(leg.get('reentry_type') or '') == 'AtCost':
            _triggered_by = str(leg.get('triggered_by') or '')
            if _triggered_by:
                try:
                    from features.notification_manager import upsert_recost_feature_status
                    _parent_leg = next(
                        (l for l in (trade.get('legs') or [])
                         if isinstance(l, dict) and str(l.get('id') or '') == _triggered_by),
                        None,
                    )
                    if _parent_leg is None:
                        _parent_leg = db._db['algo_trade_positions_history'].find_one(
                            {'trade_id': str(trade.get('_id') or ''), 'leg_id': _triggered_by},
                        )
                    if _parent_leg:
                        upsert_recost_feature_status(
                            db._db, trade, _parent_leg, now_ts,
                            cost_price=entry_price,
                            exit_price=_safe_float((_parent_leg.get('entry_trade') or {}).get('price')),
                            status='triggered',
                            recost_leg_id=leg_id_str,
                        )
                except Exception as _rce:
                    log.warning('recost feature status triggered error leg=%s: %s', leg_id_str, _rce)

        mark_execute_order_dirty_from_trade(trade)
        return True, None
    except Exception as exc:
        log.error('_try_enter_pending_leg error: %s', exc)
        return False, None


def apply_resolved_live_entries(
    db: MongoData,
    trade_doc: dict,
    leg_entries: list[dict],
    now_ts: str,
) -> list[dict]:
    """
    Shared live/fast-forward entry writer.

    Market data resolution happens outside this function.
    This function only applies the resolved entry payload into algo_trades using
    the same common execution/storage flow used elsewhere in this file.
    """
    trade_id = str(trade_doc.get('_id') or '')
    if not trade_id or not leg_entries:
        return []
    if trade_doc.get('is_paused'):
        return []

    created_original_legs, refreshed_trade = _queue_original_legs_if_needed(db, trade_doc, now_ts)
    if created_original_legs:
        trade_doc = refreshed_trade
    trade_doc = db._db['algo_trades'].find_one({'_id': trade_id}) or trade_doc

    entry_map = {
        str(item.get('leg_id') or '').strip(): dict(item)
        for item in (leg_entries or [])
        if str(item.get('leg_id') or '').strip()
    }
    if not entry_map:
        return []

    all_leg_configs = _resolve_trade_leg_configs(trade_doc)
    history_leg_ids: set[str] = set()
    try:
        for history_doc in db._db['algo_trade_positions_history'].find({'trade_id': trade_id}, {'leg_id': 1}):
            history_leg_ids.add(str(history_doc.get('leg_id') or ''))
    except Exception:
        pass
    exchange_ts = now_ts.replace('T', ' ')[:19] if 'T' in now_ts else now_ts[:19]
    applied_entries: list[dict] = []

    for leg_id, leg_cfg in all_leg_configs.items():
        leg_id = str(leg_id or '').strip()
        if not leg_id or leg_id in history_leg_ids:
            continue
        info = entry_map.get(leg_id) or {}
        if not info:
            continue

        entry_price = _safe_float(info.get('ltp'))
        if entry_price <= 0:
            entry_print(f'[LIVE ENTRY SKIP] trade={trade_id} leg={leg_id} reason=ltp_zero')
            continue

        spot_price = _safe_float(info.get('spot_price'))
        position_str = str(leg_cfg.get('PositionType') or 'PositionType.Sell')
        is_sell_pos = _is_sell(position_str)
        sl_config = leg_cfg.get('LegStopLoss') or {}
        sl_price = calc_sl_price(entry_price, is_sell_pos, sl_config)

        lot_config_value = max(1, int(
            (leg_cfg.get('LotConfig') or {}).get('Value')
            or 1
        ))
        actual_quantity = lot_config_value

        # Lot size for the live broker call: prefer the per-instrument value already
        # synced from Dhan's feed onto the resolved token doc (info['lot_size']) — it
        # reflects the exact contract being traded, not a hand-maintained date table.
        # Falls back to the static lot_sizes lookup when the Dhan-sourced value isn't
        # available (e.g. non-Dhan broker, or tokens not yet synced).
        dhan_lot_size = _safe_int(info.get('lot_size'))
        if dhan_lot_size > 0:
            leg_lot_size = dhan_lot_size
        else:
            underlying_for_lot = str(
                trade_doc.get('ticker')
                or (trade_doc.get('config') or {}).get('Ticker')
                or ''
            ).strip()
            try:
                leg_lot_size = int(db.get_lot_size(_extract_trade_date_from_doc(trade_doc), underlying_for_lot))
            except Exception:
                leg_lot_size = 1

        entry_trade_payload = {
            'trigger_timestamp': exchange_ts,
            'trigger_price': entry_price,
            'underlying_trigger_price': spot_price,
            'price': entry_price,
            'quantity': actual_quantity,
            'underlying_at_trade': spot_price,
            'traded_timestamp': exchange_ts,
            'exchange_timestamp': exchange_ts,
        }
        synthetic_leg = {
            'id': leg_id,
            'position': position_str,
        }
        mode_entry_order = _build_mode_entry_order_payload(
            db,
            trade_doc,
            synthetic_leg,
            leg_cfg,
            str(info.get('symbol') or ''),
            actual_quantity,
            entry_price,
            exchange_ts,
            lot_size=leg_lot_size,
        )
        if not bool(mode_entry_order.get('allowed', True)):
            print(
                f'[LIVE ENTRY SKIP] trade={trade_id} leg={leg_id} '
                f'reason={str(mode_entry_order.get("error") or "live_order_failed")}'
            )
            continue
        entry_trade_payload.update(mode_entry_order.get('payload') or {})
        entry_trade_payload = _decorate_entry_trade_lifecycle(
            str(trade_doc.get('activation_mode') or ''),
            entry_trade_payload,
        )

        entered_leg = {
            'id': leg_id,
            'status': OPEN_LEG_STATUS,
            'position': position_str,
            'option': str(
                ((leg_cfg.get('ContractType') or {}).get('Option'))
                or leg_cfg.get('InstrumentKind')
                or ''
            ).split('.')[-1],
            'strike': info.get('strike'),
            'expiry_date': _normalize_expiry_datetime(info.get('expiry')),
            'token': str(info.get('token') or ''),
            'symbol': str(info.get('symbol') or ''),
            'quantity': actual_quantity,
            'lot_size': leg_lot_size,
            'lot_config_value': lot_config_value,
            'current_sl_price': sl_price,
            'initial_sl_value': sl_price,
            'display_sl_value': sl_price,
            'display_target_value': None,
            'last_saw_price': entry_price,
            'is_lazy': False,
            'is_reentered_leg': False,
            'entry_trade': entry_trade_payload,
            'exit_trade': None,
            'leg_type': str(leg_cfg.get('leg_type') or leg_id),
            'triggered_by': '',
            'lazy_leg_ref': leg_id,
            'parent_leg_id': '',
            'reentry_type': '',
            'reentry_count_remaining': None,
            'transactions': {},
            'current_transaction_id': None,
        }

        is_live_pending_order = _is_live_entry_order_pending(
            str(trade_doc.get('activation_mode') or ''),
            entry_trade_payload,
        )
        if is_live_pending_order:
            db._db['algo_trades'].update_one(
                {'_id': trade_id},
                {
                    '$pull': {'legs': {'id': leg_id}},
                    '$push': {'legs': entered_leg},
                },
            )
            inserted, history_doc = False, None
        else:
            inserted, history_doc = _store_position_history(
                db, trade_doc, entered_leg, override_leg_cfg=leg_cfg
            )
        try:
            db._db['algo_leg_feature_status'].update_many(
                {
                    'trade_id': trade_id,
                    'leg_id': leg_id,
                    'feature': 'pending_entry',
                    'status': 'active',
                },
                {'$set': {
                    'status': 'triggered',
                    'enabled': False,
                    'triggered_at': now_ts,
                    'disabled_at': now_ts,
                    'disabled_reason': 'entry_taken',
                    'updated_at': now_ts,
                }},
            )
        except Exception:
            pass
        if inserted and history_doc:
            print(
                f'[LIVE ENTRY SYNC] trade={trade_id} '
                f'leg={history_doc["leg_id"]} '
                f'entry={history_doc["entry_timestamp"]} '
                f'history_id={history_doc.get("_id") or "-"}'
            )
            applied_entries.append({
                'trade_id': trade_id,
                'leg_id': history_doc['leg_id'],
                'history_id': history_doc.get('_id') or '',
                'entry_timestamp': history_doc['entry_timestamp'],
            })
        elif is_live_pending_order:
            print(
                f'[LIVE ENTRY ORDER OPEN] trade={trade_id} '
                f'leg={leg_id} order_id={str(entry_trade_payload.get("order_id") or "-")}'
            )

    _validate_trade_leg_storage(db, trade_id)
    return applied_entries


# ─── Core: simulate-mode position extractor ───────────────────────────────────

def _build_snapshot_timestamp(selected_date: str, market_time: str) -> str:
    return f'{selected_date}T{market_time}:00'


def _extract_hhmm(raw_time: str) -> str:
    raw_value = str(raw_time or '').strip()
    if len(raw_value) >= 16:
        return raw_value[11:16]
    return raw_value[:5]


def _serialize_trade_record(item: dict) -> dict:
    ticker_value = str(
        item.get('ticker')
        or ((item.get('config') or {}).get('Ticker'))
        or ((item.get('strategy') or {}).get('Ticker'))
        or ''
    )
    return {
        '_id': str(item.get('_id') or ''),
        'strategy_id': str(item.get('strategy_id') or ''),
        'name': str(item.get('name') or ''),
        'status': str(item.get('status') or ''),
        'trade_status': _safe_int(item.get('trade_status')),
        'active_on_server': bool(item.get('active_on_server')),
        'activation_mode': str(item.get('activation_mode') or ''),
        'broker': str(item.get('broker') or ''),
        'user_id': str(item.get('user_id') or ''),
        'ticker': ticker_value,
        'underlying': ticker_value,
        'legs': item.get('legs') if isinstance(item.get('legs'), list) else [],
        'creation_ts': str(item.get('creation_ts') or ''),
        'entry_time': str(item.get('entry_time') or ''),
        'exit_time': str(item.get('exit_time') or ''),
        'overall_sl_reentry_done': _safe_int(item.get('overall_sl_reentry_done')),
        'overall_tgt_reentry_done': _safe_int(item.get('overall_tgt_reentry_done')),
        'current_overall_sl_threshold': _safe_float(item.get('current_overall_sl_threshold')),
        'peak_mtm': _safe_float(item.get('peak_mtm')),
        'last_overall_event_at': str(item.get('last_overall_event_at') or ''),
        'last_overall_event_reason': str(item.get('last_overall_event_reason') or ''),
        'config': item.get('config') if isinstance(item.get('config'), dict) else {},
        'portfolio': item.get('portfolio') if isinstance(item.get('portfolio'), dict) else {},
        'live_sim_order': item.get('live_sim_order'),
        'strategy': item.get('strategy') if isinstance(item.get('strategy'), dict) else {},
    }


def _extract_broker_configuration_label(document: dict, fallback_broker_id: str = '') -> str:
    if not isinstance(document, dict):
        return str(fallback_broker_id or '').strip()
    for key in (
        'broker_name',
        'display_name',
        'name',
        'title',
        'broker',
        'broker_type',
        'provider',
        'vendor',
    ):
        value = str(document.get(key) or '').strip()
        if value:
            return value
    return str(fallback_broker_id or '').strip()


def _attach_broker_configuration_details(db: MongoData, records: list[dict]) -> list[dict]:
    if not records:
        return records

    broker_object_ids: list[ObjectId] = []
    for record in records:
        broker_id = str((record or {}).get('broker') or '').strip()
        if not broker_id:
            continue
        try:
            broker_object_ids.append(ObjectId(broker_id))
        except Exception:
            continue

    if not broker_object_ids:
        return records

    broker_docs_by_id: dict[str, dict] = {}
    try:
        cursor = db._db['broker_configuration'].find(
            {'_id': {'$in': broker_object_ids}},
            {
                '_id': 1,
                'broker_name': 1,
                'display_name': 1,
                'name': 1,
                'title': 1,
                'broker': 1,
                'broker_icon': 1,
                'broker_type': 1,
                'provider': 1,
                'vendor': 1,
            },
        )
        for item in cursor:
            item_id = str((item or {}).get('_id') or '').strip()
            if item_id:
                normalized_item = dict(item)
                normalized_item['_id'] = item_id
                broker_docs_by_id[item_id] = normalized_item
    except Exception:
        return records

    if not broker_docs_by_id:
        return records

    enriched_records: list[dict] = []
    for record in records:
        new_record = dict(record)
        broker_id = str(new_record.get('broker') or '').strip()
        broker_doc = broker_docs_by_id.get(broker_id)
        if broker_doc:
            new_record['broker_details'] = dict(broker_doc)
            new_record['broker_label'] = _extract_broker_configuration_label(broker_doc, broker_id)
        enriched_records.append(new_record)
    return enriched_records


def _build_broker_settings_snapshot(db: MongoData, records: list[dict]) -> list[dict]:
    """
    Build broker-settings snapshot for all brokers found in records.
    Queries algo_borker_stoploss_settings (any status) for each unique
    (user_id, broker, activation_mode) group found in the trade records.
    Returns a list of dicts ready for frontend display.
    """
    seen: dict[str, tuple] = {}
    for rec in (records or []):
        uid  = str(rec.get('user_id') or '').strip()
        bkr  = str(rec.get('broker') or '').strip()
        mode = str(rec.get('activation_mode') or '').strip()
        if not uid or not bkr or not mode:
            continue
        key = f'{uid}|{bkr}|{mode}'
        if key not in seen:
            broker_details = rec.get('broker_details') or {}
            label = (
                rec.get('broker_label')
                or broker_details.get('broker_name')
                or broker_details.get('display_name')
                or broker_details.get('name')
                or bkr
            )
            seen[key] = (uid, bkr, mode, str(label).strip())

    result: list[dict] = []
    for _key, (uid, bkr, mode, label) in seen.items():
        try:
            doc = db._db['algo_borker_stoploss_settings'].find_one({
                'user_id': uid, 'broker': bkr, 'activation_mode': mode,
            })
        except Exception as exc:
            log.warning('_build_broker_settings_snapshot error broker=%s: %s', bkr, exc)
            continue
        if not doc:
            continue

        sl_val    = doc.get('StopLoss')
        tgt_val   = doc.get('Target')
        lat_cfg   = doc.get('LockAndTrail') or {}
        trail_cfg = doc.get('OverallTrailSL') or {}

        lat_out = None
        if lat_cfg and lat_cfg.get('InstrumentMove') and lat_cfg.get('StopLossMove'):
            lat_out = {
                'instrument_move': lat_cfg.get('InstrumentMove'),
                'stop_loss_move':  lat_cfg.get('StopLossMove'),
            }

        trail_out = None
        if trail_cfg and trail_cfg.get('InstrumentMove') and trail_cfg.get('StopLossMove'):
            trail_out = {
                'instrument_move': trail_cfg.get('InstrumentMove'),
                'stop_loss_move':  trail_cfg.get('StopLossMove'),
            }

        result.append({
            'broker_id':      bkr,
            'broker_label':   label,
            'user_id':        uid,
            'mode':           mode,
            'settings_active': bool(doc.get('status') == 1),
            'stop_loss':      sl_val,
            'target':         tgt_val,
            'lock_and_trail': lat_out,
            'trail_sl':       trail_out,
            'state': {
                'lock_activated':     bool(doc.get('lock_activated') or False),
                'current_lock_floor': float(doc.get('current_lock_floor') or 0),
                'lock_peak_mtm':      float(doc.get('lock_peak_mtm') or 0),
                'effective_sl':       doc.get('effective_sl'),
                'sl_peak_mtm':        float(doc.get('sl_peak_mtm') or 0),
            },
        })

    return result


async def emit_broker_settings_for_user(user_id: str, activation_mode: str = '') -> int:
    """
    Query ALL broker settings for user_id from DB and emit broker-settings
    via execute-orders WebSocket channel.

    Call this after any DB action that touches broker data for a user so the
    frontend always receives the latest full snapshot.

    Returns the number of WebSocket clients the message was delivered to.
    """
    uid = str(user_id or '').strip()
    if not uid:
        return 0

    from features.mongo_data import MongoData as _MongoData
    from bson import ObjectId

    db = _MongoData()
    brokers_out: list[dict] = []
    try:
        query: dict = {'user_id': uid}
        if activation_mode:
            query['activation_mode'] = str(activation_mode).strip()

        docs = list(db._db['algo_borker_stoploss_settings'].find(query))

        # Batch-load broker labels from broker_configuration
        broker_ids = []
        for doc in docs:
            bkr = str(doc.get('broker') or '').strip()
            if bkr:
                try:
                    broker_ids.append(ObjectId(bkr))
                except Exception:
                    pass

        label_map: dict[str, str] = {}
        if broker_ids:
            for bconf in db._db['broker_configuration'].find(
                {'_id': {'$in': broker_ids}},
                {'broker_name': 1, 'display_name': 1, 'name': 1, 'title': 1, 'broker': 1},
            ):
                bid = str(bconf.get('_id') or '')
                label_map[bid] = _extract_broker_configuration_label(bconf, bid)

        for doc in docs:
            bkr   = str(doc.get('broker') or '').strip()
            mode  = str(doc.get('activation_mode') or '').strip()
            label = label_map.get(bkr) or bkr

            lat_cfg   = doc.get('LockAndTrail') or {}
            trail_cfg = doc.get('OverallTrailSL') or {}

            lat_out = None
            if lat_cfg and lat_cfg.get('InstrumentMove') and lat_cfg.get('StopLossMove'):
                lat_out = {
                    'instrument_move': lat_cfg['InstrumentMove'],
                    'stop_loss_move':  lat_cfg['StopLossMove'],
                }

            trail_out = None
            if trail_cfg and trail_cfg.get('InstrumentMove') and trail_cfg.get('StopLossMove'):
                trail_out = {
                    'instrument_move': trail_cfg['InstrumentMove'],
                    'stop_loss_move':  trail_cfg['StopLossMove'],
                }

            brokers_out.append({
                'broker_id':       bkr,
                'broker_label':    label,
                'user_id':         uid,
                'mode':            mode,
                'settings_active': bool(doc.get('status') == 1),
                'stop_loss':       doc.get('StopLoss'),
                'target':          doc.get('Target'),
                'lock_and_trail':  lat_out,
                'trail_sl':        trail_out,
                'state': {
                    'lock_activated':     bool(doc.get('lock_activated') or False),
                    'current_lock_floor': float(doc.get('current_lock_floor') or 0),
                    'lock_peak_mtm':      float(doc.get('lock_peak_mtm') or 0),
                    'effective_sl':       doc.get('effective_sl'),
                    'sl_peak_mtm':        float(doc.get('sl_peak_mtm') or 0),
                },
            })
    except Exception as exc:
        log.warning('emit_broker_settings_for_user error user=%s: %s', uid, exc)
        return 0
    finally:
        try:
            db.close()
        except Exception:
            pass

    msg = _build_message(
        'broker-settings',
        'Broker settings updated',
        {'brokers': brokers_out},
    )
    delivered = await _broadcast_user_channel_message(uid, 'execute-orders', msg)
    log.info('[BROKER SETTINGS EMIT] user=%s brokers=%d delivered=%d', uid, len(brokers_out), delivered)
    return delivered


def _load_running_trade_records(
    db: MongoData,
    trade_date: str,
    activation_mode: str = 'algo-backtest',
    user_id: str = '',
) -> list[dict]:
    query = _build_trade_query(
        trade_date,
        activation_mode=activation_mode,
        statuses=[RUNNING_STATUS],
    )
    query['active_on_server'] = True
    normalized_user_id = str(user_id or '').strip()
    if normalized_user_id:
        query['user_id'] = normalized_user_id
    cursor = db._db['algo_trades'].find(query).sort('creation_ts', 1)
    serialized_records = [_serialize_trade_record(item) for item in cursor]
    return _attach_broker_configuration_details(db, serialized_records)


def _load_execute_order_group_records(
    db: MongoData,
    trade_date: str,
    activation_mode: str,
    group_id: str = '',
    user_id: str = '',
) -> list[dict]:
    normalized_user_id = str(user_id or '').strip()
    running_records = _load_running_trade_records(
        db,
        trade_date,
        activation_mode=activation_mode,
        user_id=normalized_user_id,
    )
    normalized_group_id = str(group_id or '').strip()
    if not normalized_group_id:
        return running_records

    grouped_running_records = _collect_records_for_group(running_records, normalized_group_id)
    seen_trade_ids = {str(item.get('_id') or '') for item in grouped_running_records}

    group_query = _build_trade_query(trade_date, activation_mode=activation_mode)
    group_query.pop('trade_status', None)
    group_query.pop('status', None)
    group_query.pop('active_on_server', None)
    group_query['portfolio.group_id'] = normalized_group_id
    if normalized_user_id:
        group_query['user_id'] = normalized_user_id

    extra_records: list[dict] = []
    for item in db._db['algo_trades'].find(group_query).sort('creation_ts', 1):
        serialized = _serialize_trade_record(item)
        trade_id = str(serialized.get('_id') or '')
        if trade_id in seen_trade_ids:
            continue
        extra_records.append(serialized)
        seen_trade_ids.add(trade_id)

    squared_off_query = _build_trade_query(trade_date, activation_mode=activation_mode)
    squared_off_query['portfolio.group_id'] = normalized_group_id
    squared_off_query['active_on_server'] = False
    squared_off_query['status'] = 'StrategyStatus.SquaredOff'
    squared_off_query['trade_status'] = {'$in': [1, 2]}
    if normalized_user_id:
        squared_off_query['user_id'] = normalized_user_id

    for item in db._db['algo_trades'].find(squared_off_query).sort('creation_ts', 1):
        serialized = _serialize_trade_record(item)
        trade_id = str(serialized.get('_id') or '')
        if trade_id in seen_trade_ids:
            continue
        extra_records.append(serialized)
        seen_trade_ids.add(trade_id)

    return _attach_broker_configuration_details(db, grouped_running_records + extra_records)


def _trade_emit_signature(record: dict) -> str:
    legs = record.get('legs') if isinstance(record.get('legs'), list) else []
    parts = [
        str(record.get('_id') or ''),
        str(record.get('status') or ''),
        str(record.get('active_on_server') or ''),
        str(record.get('overall_sl_reentry_done') or ''),
        str(record.get('overall_tgt_reentry_done') or ''),
        str(record.get('current_overall_sl_threshold') or ''),
        str(record.get('peak_mtm') or ''),
        str(record.get('last_overall_event_at') or ''),
        str(record.get('last_overall_event_reason') or ''),
    ]
    for leg in legs:
        if not isinstance(leg, dict):
            # string history _id reference — include it in signature as-is
            parts.append(str(leg))
            continue
        entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
        exit_trade = leg.get('exit_trade') if isinstance(leg.get('exit_trade'), dict) else {}
        parts.append(
            '|'.join([
                str(leg.get('id') or ''),
                str(_safe_int(leg.get('status'))),
                str(entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or ''),
                str(exit_trade.get('traded_timestamp') or exit_trade.get('trigger_timestamp') or ''),
                str(leg.get('current_sl_price') or ''),
                str(leg.get('display_sl_value') or ''),
            ])
        )
    return '::'.join(parts)


def _has_opened_order(record: dict) -> bool:
    for leg in (record.get('legs') if isinstance(record.get('legs'), list) else []):
        if not isinstance(leg, dict):
            # String ID in legs = history record exists = order was entered
            if str(leg).strip():
                return True
            continue
        entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
        if entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp'):
            return True
    return False


def _resolve_trade_leg_configs(trade: dict) -> dict:
    """
    Build leg config lookup for a trade.

    Priority:
      1. trade.strategy.ListOfLegConfigs / IdleLegConfigs
      2. trade.config.LegConfigs / IdleLegConfigs

    execution_config_extra.ListOfLegExecutionConfig entries are merged by index
    into the corresponding ListOfLegConfigs entries (EntryOrder, ExitOrder, ProductType).
    """
    all_leg_configs: dict = {}
    strategy_cfg = trade.get('strategy') or {}
    leg_list = list(strategy_cfg.get('ListOfLegConfigs') or [])
    for lc in leg_list:
        if isinstance(lc, dict):
            all_leg_configs[str(lc.get('id') or '')] = lc
    idle_cfg = strategy_cfg.get('IdleLegConfigs') or {}
    if isinstance(idle_cfg, dict):
        all_leg_configs.update(idle_cfg)

    cfg = trade.get('config') or {}
    all_leg_configs.update(cfg.get('LegConfigs') or {})
    all_leg_configs.update(cfg.get('IdleLegConfigs') or {})

    # Merge per-leg execution config (EntryOrder, ExitOrder, ProductType) by index
    exec_extra = trade.get('execution_config_extra') or {}
    leg_exec_cfgs = exec_extra.get('ListOfLegExecutionConfig') or []
    for idx, lec in enumerate(leg_exec_cfgs):
        if not isinstance(lec, dict) or idx >= len(leg_list):
            continue
        base_leg = leg_list[idx] if isinstance(leg_list[idx], dict) else {}
        leg_id = str(base_leg.get('id') or '')
        if not leg_id or leg_id not in all_leg_configs:
            continue
        merged = dict(all_leg_configs[leg_id])
        for key in ('EntryOrder', 'ExitOrder', 'ProductType', 'ReferenceForTgtSL', 'EntryDelay'):
            if key in lec:
                merged[key] = lec[key]
        all_leg_configs[leg_id] = merged

    return all_leg_configs


def _build_execute_order_events(
    records: list[dict],
    seen_signatures: dict[str, str],
) -> list[dict]:
    events: list[dict] = []
    for record in records:
        trade_id = str(record.get('_id') or '')
        signature = _trade_emit_signature(record)
        previous_signature = seen_signatures.get(trade_id)
        seen_signatures[trade_id] = signature
        if previous_signature == signature:
            continue
        has_overall_event = bool(str(record.get('last_overall_event_reason') or '').strip())
        if not has_overall_event and not _has_opened_order(record):
            continue
        events.append(record)
    return events


def _seed_execute_order_signatures(
    records: list[dict] | None,
    seen_signatures: dict[str, str],
) -> None:
    for record in (records or []):
        if not isinstance(record, dict):
            continue
        trade_id = str(record.get('_id') or '').strip()
        if not trade_id:
            continue
        seen_signatures[trade_id] = _trade_emit_signature(record)


def _populate_legs_from_history(db: 'MongoData', records: list[dict]) -> list[dict]:
    """Enrich records with full leg objects from algo_trade_positions_history (same as API)."""
    trade_ids = [str(r.get('_id') or '') for r in records if r.get('_id')]
    if not trade_ids:
        return records
    try:
        history_col = db._db['algo_trade_positions_history']
        history_by_trade: dict[str, list] = {tid: [] for tid in trade_ids}
        for doc in history_col.find({'trade_id': {'$in': trade_ids}}):
            doc['_id'] = str(doc.get('_id') or '')
            tid = str(doc.get('trade_id') or '')
            if tid in history_by_trade:
                history_by_trade[tid].append(doc)
    except Exception as exc:
        log.warning('_populate_legs_from_history error: %s', exc)
        return records
    enriched = []
    for rec in records:
        trade_id = str(rec.get('_id') or '')
        history_legs = history_by_trade.get(trade_id) or []
        new_rec = dict(rec)
        new_rec['legs'] = history_legs
        new_rec['open_legs_count'] = sum(1 for l in history_legs if int(l.get('status') or 0) == 1)
        new_rec['closed_legs_count'] = sum(1 for l in history_legs if int(l.get('status') or 0) == 2)
        new_rec['pending_legs_count'] = sum(1 for l in history_legs if int(l.get('status') or 0) == 0)
        enriched.append(new_rec)
    return _attach_leg_feature_statuses(db, enriched)


def _attach_leg_feature_statuses(db: 'MongoData', records: list[dict]) -> list[dict]:
    def _format_feature_status_timestamp(value: Any) -> str:
        raw_value = str(value or '').strip()
        if not raw_value:
            return ''
        return raw_value.replace('T', ' ')

    def _format_feature_status_price(value: Any) -> str:
        numeric = _safe_float(value)
        if numeric <= 0:
            return '-'
        return f'₹{numeric:.2f}'

    def _describe_feature_status_row(row: dict) -> str:
        if not isinstance(row, dict):
            return ''

        description = str(row.get('trigger_description') or '').strip()
        if re.match(r'^Pending entry queued for .* leg .+\.$', description, re.IGNORECASE):
            description = ''
        if description:
            return description

        feature_key = str(row.get('feature') or '').strip()
        if feature_key in {'overall_sl', 'overall_target'}:
            label = 'Overall SL' if feature_key == 'overall_sl' else 'Overall Target'
            cycle_number = int(row.get('cycle_number') or 1)
            trigger_value = _format_feature_status_price(row.get('trigger_value'))
            next_value = _format_feature_status_price(row.get('next_trigger_value'))
            reentry_type = str(row.get('reentry_type') or 'None')
            reentry_count = int(row.get('reentry_count') or 0)
            reentry_done = int(row.get('reentry_done') or 0)
            return (
                f'{label} active for cycle {cycle_number}. '
                f'Current threshold {trigger_value}. '
                f'Re-entry {reentry_type} used {reentry_done}/{reentry_count}. '
                f'Next cycle threshold {next_value}.'
            )
        if feature_key == 'reCost':
            strike = str(row.get('strike') or '').strip() or '-'
            option = str(row.get('option_type') or row.get('option') or '').strip().upper() or '-'
            position = str(row.get('position') or '').split('.')[-1].strip() or 'Position'
            cost_price = _format_feature_status_price(row.get('trigger_price'))
            exit_price = _format_feature_status_price(row.get('exit_price'))
            re_status = str(row.get('status') or '').strip().lower()
            if re_status == 'triggered':
                triggered_at = _format_feature_status_timestamp(row.get('triggered_at'))
                return (
                    f'RE-COST triggered for {strike} {option} {position} at {triggered_at or "-"}. '
                    f'Price returned to {cost_price}. Re-entry taken.'
                )
            if re_status == 'disabled':
                disabled_at = _format_feature_status_timestamp(row.get('disabled_at'))
                return (
                    f'RE-COST disabled for {strike} {option} {position} at {disabled_at or "-"}. '
                    f'Waiting cost {cost_price} not reached.'
                )
            # pending
            is_sell = 'sell' in position.lower()
            direction = 'falls back to' if is_sell else 'rises back to'
            return (
                f'RE-COST pending: {position} {strike} {option} exited @ {exit_price}. '
                f'Waiting for price to {direction} {cost_price} (original entry cost) to re-enter.'
            )

        if feature_key == 'pending_entry':
            option = str(row.get('option') or '').strip().upper() or '-'
            position = str(row.get('position') or '').split('.')[-1].strip() or 'Position'
            strike = str(row.get('strike') or '').strip() or '-'
            queued_at = _format_feature_status_timestamp(row.get('queued_at'))
            triggered_at = _format_feature_status_timestamp(row.get('triggered_at'))
            status = str(row.get('status') or '').strip().lower()

            if status == 'triggered':
                return f'Pending entry triggered for {strike} {option} {position} leg at {triggered_at or "-"}.'

            return (
                f'Pending entry active for {strike} {option} {position} leg since {queued_at or "-"}. '
                f'Waiting for next entry cycle.'
            )

        if feature_key != 'momentum_pending':
            return ''

        status = str(row.get('status') or '').strip().lower()
        option = str(row.get('option') or '').strip().upper() or '-'
        position = str(row.get('position') or '').split('.')[-1].strip() or 'Position'
        strike = str(row.get('strike') or '').strip() or '-'
        momentum_type = str(row.get('momentum_type') or '').split('.')[-1].strip() or 'Momentum'
        momentum_value = _safe_float(row.get('momentum_value'))
        base_price = _format_feature_status_price(row.get('momentum_base_price'))
        target_price = _format_feature_status_price(row.get('momentum_target_price'))
        queued_at = _format_feature_status_timestamp(row.get('queued_at'))
        armed_at = _format_feature_status_timestamp(row.get('armed_at'))

        if status == 'triggered':
            triggered_at = _format_feature_status_timestamp(row.get('triggered_at'))
            return f'Momentum triggered for {strike} {option} {position} leg at {triggered_at or "-"}.'

        if _safe_float(row.get('momentum_base_price')) > 0 and _safe_float(row.get('momentum_target_price')) > 0:
            return (
                f'Momentum waiting for {strike} {option} {position} leg. '
                f'{momentum_type} {momentum_value:g} armed at {armed_at or queued_at or "-"} '
                f'with base {base_price} and target {target_price}.'
            )

        return (
            f'Momentum queue active for {strike} {option} {position} leg since {queued_at or "-"}. '
            f'Waiting to arm {momentum_type} {momentum_value:g}.'
        )

    def _build_pending_feature_leg(row: dict) -> dict:
        row_copy = dict(row)
        description = _describe_feature_status_row(row_copy)
        if description:
            row_copy['trigger_description'] = description

        feature_map: dict[str, dict] = {}
        feature_key = str(row_copy.get('feature') or '').strip()
        if feature_key:
            feature_map[feature_key] = row_copy

        # Live LTP from Kite ticker for this token (live/fast-forward modes)
        _pending_token = str(row_copy.get('token') or '').strip()
        _live_ltp: float | None = None
        if _pending_token:
            try:
                from features.broker_gateway import broker_ticker_manager as _tm_pfl  # type: ignore
                _raw_ltp = (_tm_pfl.ltp_map or {}).get(_pending_token)
                if _raw_ltp is not None:
                    _live_ltp = float(_raw_ltp)
            except Exception:
                pass

        return {
            'id': str(row_copy.get('leg_id') or ''),
            'leg_id': str(row_copy.get('leg_id') or ''),
            'status': 0,
            'position': row_copy.get('position'),
            'option': row_copy.get('option'),
            'strike': row_copy.get('strike'),
            'expiry_date': row_copy.get('expiry_date'),
            'token': row_copy.get('token'),
            'symbol': row_copy.get('symbol'),
            'quantity': 0,
            'lot_config_value': int(row_copy.get('lot_config_value') or 1),
            'entry_trade': None,
            'exit_trade': None,
            'ltp': _live_ltp,
            'last_saw_price': row_copy.get('momentum_base_price'),
            'is_lazy': bool(row_copy.get('is_lazy', True)),
            'is_pending_feature_leg': True,
            'queued_at': row_copy.get('queued_at'),
            'armed_at': row_copy.get('armed_at'),
            'triggered_at': row_copy.get('triggered_at'),
            'leg_type': row_copy.get('leg_type'),
            'triggered_by': row_copy.get('triggered_by'),
            'lazy_leg_ref': row_copy.get('lazy_leg_ref'),
            'parent_leg_id': row_copy.get('parent_leg_id'),
            'reentry_type': row_copy.get('reentry_type'),
            'reentry_count_remaining': row_copy.get('reentry_count_remaining'),
            'entry_kind': row_copy.get('entry_kind'),
            'strike_parameter': row_copy.get('strike_parameter'),
            'expiry_kind': row_copy.get('expiry_kind'),
            'skip_momentum_check': bool(row_copy.get('skip_momentum_check')),
            'spot_at_queue': row_copy.get('spot_at_queue'),
            'momentum_base_price': row_copy.get('momentum_base_price'),
            'momentum_target_price': row_copy.get('momentum_target_price'),
            'momentum_reference_set_at': row_copy.get('momentum_reference_set_at'),
            'momentum_armed_notified_at': row_copy.get('momentum_armed_notified_at'),
            'pending_feature': row_copy.get('feature'),
            'feature_row_id': str(row_copy.get('_id') or ''),
            'feature_status_rows': [row_copy],
            'feature_status_map': feature_map,
            'active_trigger_descriptions': [description] if description else [],
        }

    if not records:
        return records

    trade_ids = [str(rec.get('_id') or '') for rec in records if rec.get('_id')]
    if not trade_ids:
        return records

    feature_rows_by_key: dict[tuple[str, str], list[dict]] = {}
    try:
        feature_col = db._db['algo_leg_feature_status']
        # Keep normal active feature rows, but also include simpleMomentum rows
        # even after they transition to triggered/disabled so execute_order socket
        # payload still carries the simple momentum details for entered legs.
        for doc in feature_col.find({
            'trade_id': {'$in': trade_ids},
            '$or': [
                {'enabled': True},
                {'feature': 'simpleMomentum'},
            ],
        }):
            trade_id = str(doc.get('trade_id') or '')
            leg_id = str(doc.get('leg_id') or '')
            if not trade_id or not leg_id:
                continue
            doc['_id'] = str(doc.get('_id') or '')
            feature_rows_by_key.setdefault((trade_id, leg_id), []).append(doc)
    except Exception as exc:
        log.warning('_attach_leg_feature_statuses error: %s', exc)
        return records

    enriched_records: list[dict] = []
    for rec in records:
        trade_id = str(rec.get('_id') or '')
        legs = rec.get('legs') if isinstance(rec.get('legs'), list) else []
        existing_leg_ids: set[str] = set()
        enriched_legs: list[dict] = []
        for leg in legs:
            if not isinstance(leg, dict):
                continue
            leg_id_candidates: list[str] = []
            for raw_leg_id in (leg.get('_id'), leg.get('leg_id'), leg.get('id')):
                normalized_leg_id = str(raw_leg_id or '').strip()
                if normalized_leg_id and normalized_leg_id not in leg_id_candidates:
                    leg_id_candidates.append(normalized_leg_id)

            for candidate in leg_id_candidates:
                existing_leg_ids.add(candidate)

            feature_rows: list[dict] = []
            seen_feature_row_ids: set[str] = set()
            for candidate in leg_id_candidates:
                for row in feature_rows_by_key.get((trade_id, candidate), []):
                    row_id = str(row.get('_id') or '').strip()
                    if row_id and row_id in seen_feature_row_ids:
                        continue
                    if row_id:
                        seen_feature_row_ids.add(row_id)
                    feature_rows.append(row)

            leg_copy = dict(leg)
            leg_copy['feature_status_rows'] = feature_rows
            feature_map: dict[str, dict] = {}
            active_descriptions: list[str] = []
            for row in feature_rows:
                feature_key = str(row.get('feature') or '').strip()
                if not feature_key:
                    continue
                row_copy = dict(row)
                description = _describe_feature_status_row(row_copy)
                if description:
                    row_copy['trigger_description'] = description
                feature_map[feature_key] = row_copy
                if description:
                    active_descriptions.append(description)
            leg_copy['feature_status_map'] = feature_map
            leg_copy['feature_status_rows'] = list(feature_map.values()) if feature_map else feature_rows
            leg_copy['active_trigger_descriptions'] = active_descriptions
            enriched_legs.append(leg_copy)

        pending_feature_legs: list[dict] = []
        strategy_feature_rows: list[dict] = []
        for (feature_trade_id, feature_leg_id), feature_rows in feature_rows_by_key.items():
            if feature_trade_id != trade_id or not feature_leg_id or feature_leg_id in existing_leg_ids:
                continue
            if feature_leg_id == OVERALL_FEATURE_LEG_ID:
                for row in feature_rows:
                    row_copy = dict(row)
                    description = _describe_feature_status_row(row_copy)
                    if description:
                        row_copy['trigger_description'] = description
                    strategy_feature_rows.append(row_copy)
                continue
            for row in feature_rows:
                if str(row.get('feature') or '').strip() not in {'momentum_pending', 'pending_entry'}:
                    continue
                if str(row.get('status') or '').strip().lower() != 'active':
                    continue
                pending_feature_legs.append(_build_pending_feature_leg(row))
        new_rec = dict(rec)
        new_rec['legs'] = enriched_legs
        new_rec['pending_feature_legs'] = pending_feature_legs
        new_rec['strategy_feature_status_rows'] = strategy_feature_rows
        enriched_records.append(new_rec)
    return enriched_records


def _extract_market_underlyings(records: list[dict]) -> list[str]:
    return sorted({
        str(
            record.get('ticker')
            or ((record.get('config') or {}).get('Ticker') or '')
        ).strip().upper()
        for record in (records or [])
        if str(
            record.get('ticker')
            or ((record.get('config') or {}).get('Ticker') or '')
        ).strip()
    })


def _preload_trade_date_market_cache(
    db: MongoData,
    trade_date: str,
    records: list[dict] | None = None,
) -> dict | None:
    normalized_date = str(trade_date or '').strip()
    if not normalized_date:
        return None
    underlyings = _extract_market_underlyings(records or [])
    try:
        return preload_market_data_cache(db, normalized_date, underlyings)
    except Exception as exc:
        log.warning('market cache preload error trade_date=%s underlyings=%s error=%s', normalized_date, underlyings, exc)
        return None


def _clear_market_cache_snapshot(market_cache: dict | None) -> None:
    cache_key = str((market_cache or {}).get('cache_key') or '').strip()
    if cache_key:
        clear_market_data_cache(cache_key)


def _is_algo_backtest_status(value: Any) -> bool:
    normalized = str(value or '').strip().lower()
    return normalized in {'algo-backtest', 'fast-forward', 'forward-test'}


def _prefetch_backtest_market_data_once(db: MongoData, trade_date: str, records: list[dict] | None = None) -> None:
    market_cache = _preload_trade_date_market_cache(db, trade_date, records or [])
    if not market_cache:
        return
    underlyings = list((market_cache or {}).get('underlyings') or [])
    print(
        '[BACKTEST MARKET DATA READY] '
        f'trade_date={trade_date} '
        f'underlyings={",".join(underlyings) if underlyings else "-"} '
        f'option_chain_and_spot_loaded=true'
    )
    _clear_market_cache_snapshot(market_cache)


def _build_open_position_updates(
    records: list[dict],
    db: MongoData,
    trade_date: str,
    market_cache: dict | None = None,
    activation_mode: str | None = None,
) -> list[dict]:
    chain_col = db._db[OPTION_CHAIN_COLLECTION]
    open_positions: list[dict] = []
    for record in records:
        underlying = str(record.get('ticker') or ((record.get('config') or {}).get('Ticker') or ''))
        for leg in (record.get('legs') if isinstance(record.get('legs'), list) else []):
            if not isinstance(leg, dict):
                continue
            if _safe_int(leg.get('status')) != OPEN_LEG_STATUS:
                continue
            entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
            if not entry_trade:
                continue
            strike = leg.get('strike')
            expiry_date = str(leg.get('expiry_date') or '')[:10]
            option_type = str(leg.get('option') or '')
            chain_doc = get_latest_chain_doc(
                chain_col,
                underlying,
                expiry_date,
                strike,
                option_type,
                trade_date,
                market_cache=market_cache,
                activation_mode=activation_mode or str(record.get('activation_mode') or '').strip() or None,
            )
            ltp = _safe_float(chain_doc.get('close'), _safe_float(leg.get('last_saw_price')))
            open_positions.append({
                'trade_id': str(record.get('_id') or ''),
                'strategy_id': str(record.get('strategy_id') or ''),
                'strategy_name': str(record.get('name') or ''),
                'group_name': str(((record.get('portfolio') or {}).get('group_name') or '')),
                'ticker': underlying,
                'underlying': underlying,
                'leg_id': str(leg.get('id') or ''),
                'position': str(leg.get('position') or ''),
                'option': option_type,
                'strike': strike,
                'expiry_date': str(leg.get('expiry_date') or ''),
                'entry_price': _safe_float(entry_trade.get('price')),
                'ltp': ltp,
                'spot_price': _safe_float(chain_doc.get('spot_price')),
                'quantity': _safe_int(leg.get('quantity') or entry_trade.get('quantity')),
                'timestamp': str(chain_doc.get('timestamp') or ''),
                'symbol': str(leg.get('symbol') or chain_doc.get('symbol') or ''),
                'token': str(leg.get('token') or chain_doc.get('token') or ''),
            })
    return open_positions


def _latest_trading_day(raw_db, today: str) -> str:
    """
    Most recent NSE trading day on/before `today` — `today` itself on a normal
    trading weekday (the common case), otherwise walks back skipping
    weekends/market_holidays dates until it lands on one. Used so that
    checking the app on a non-trading day (weekend/holiday) shows the last
    real session's own price/change (e.g. Friday's vs Thursday's close)
    instead of comparing that session's frozen, still-being-repeated price
    against itself and reading +0.00%.
    """
    from datetime import date as _date, timedelta as _timedelta

    holidays_col = raw_db['market_holidays']
    cursor_date = _date.fromisoformat(today[:10])
    for _ in range(10):  # bounded — comfortably covers any realistic holiday run
        if cursor_date.weekday() < 5 and not holidays_col.find_one({'date': cursor_date.isoformat()}):
            return cursor_date.isoformat()
        cursor_date -= _timedelta(days=1)
    return today[:10]


def _previous_session_close(col, underlying: str, today: str) -> float:
    """
    The most recent prior TRADING day's close for `underlying`, anchored to
    NSE/BSE's 15:30 IST close (+ a 5 min buffer for the final settlement
    tick) rather than the literal last tick before today.

    "Most recent tick before today" alone isn't enough to mean "previous
    trading day" — the feed keeps writing ticks (a frozen repeat of the last
    real price) even on weekends/holidays when markets are shut, so on a
    Monday — or right now, since today is itself a Sunday — that naive
    lookup lands on Saturday's/Sunday's stale repeat of Friday's close
    instead of Friday's real close, and change_pct/change_points round to
    ~0%. Step back by calendar day, skipping anything that isn't an actual
    NSE trading day (weekend or a market_holidays date — that collection
    only stores actual holidays, not weekends, so both checks are needed),
    until a real session is found.

    Ticks well after market close are after-hours noise. For BSE indices
    (SENSEX/BANKEX) specifically, that noise can — hours later — overwrite
    the correct close with a wrong value (confirmed against live data: SENSEX
    froze correctly at 15:30:48, then started flip-flopping with a wrong
    value from ~17:42 onward). Picking "whichever tick is chronologically
    last in the day" used to grab that wrong value; anchoring on the close
    window itself sidesteps the noise regardless of when/why it appears.
    """
    from datetime import date as _date, timedelta as _timedelta

    holidays_col = col.database['market_holidays']
    cursor_date = _date.fromisoformat(today[:10])
    for _ in range(10):  # bounded — comfortably covers any realistic holiday run
        cursor_date -= _timedelta(days=1)
        if cursor_date.weekday() >= 5:  # Saturday=5, Sunday=6
            continue
        day_str = cursor_date.isoformat()
        if holidays_col.find_one({'date': day_str}):
            continue

        close_doc = col.find_one(
            {'underlying': underlying, 'timestamp': {'$gte': day_str, '$lte': f'{day_str}T15:35:00'}},
            sort=[('timestamp', -1)],
        )
        if close_doc:
            return _safe_float(close_doc.get('spot_price'))
        # No tick landed in the post-close window that day (sparse feed for this
        # underlying) — fall back to whatever's chronologically latest that day
        # rather than treating it as a non-trading day and stepping past it.
        any_doc = col.find_one(
            {'underlying': underlying, 'timestamp': {'$gte': day_str, '$lt': f'{day_str}T23:59:59'}},
            sort=[('timestamp', -1)],
        )
        if any_doc:
            return _safe_float(any_doc.get('spot_price'))
    return 0.0


def _equity_previous_close(raw_db, symbol: str) -> float:
    """
    Genuine previous-trading-day close for an individual F&O stock (e.g.
    BSE, TCS) — read from the scanner's own daily EOD sync
    (scanner_stock_historical_data), the same table scanner/service.py's
    _load_latest_closes() already reads for the portfolio-detail page.

    Deliberately not Dhan's NSE_EQ /marketfeed/quote 'ohlc.close': that
    field mirrors the current/live price all day (confirmed live: ltp and
    ohlc.close came back identical for BSE), the same quirk
    _previous_session_close() above already routes around for indices.
    Using it for stocks made change_pct/change_points round to ~0.00% on
    every refresh, no matter how often the page re-fetched.
    """
    doc = raw_db['scanner_stock_historical_data'].find_one(
        {'h_symbol': symbol},
        {'_id': 0, 'ch_closing_price': 1},
        sort=[('ch_timestamp', -1)],
    ) or {}
    return _safe_float(doc.get('ch_closing_price'))


# underlying → last-seen-good quote dict (same shape _quote_entry() returns).
# Dhan's /marketfeed/quote can come back genuinely empty for a segment (no
# trade yet today — common pre-market, or a transient 429/network hiccup),
# in which case _fetch_dhan_index_quotes() below would otherwise just drop
# that underlying entirely. Never evict: a slightly stale real quote is
# always a better fallback than the UI showing "—" for change_pct/points.
_LAST_GOOD_UNDERLYING_QUOTE: dict[str, dict] = {}


def _fetch_dhan_index_quotes(db: MongoData, underlyings: set[str]) -> dict[str, dict[str, float]]:
    """
    One shared {spot_price, change_pct, change_points} object per underlying.

    Dhan's IDX_I /marketfeed/quote 'ohlc.close' tracks the current/live price
    (live_option_chain.py falls back to that same field for LTP, not for a
    prior close), so it can't be used as the previous-day reference — using it
    that way made change_pct/change_points round to ~0 every time. spot_price
    and OHLC open/high/low still come from that live REST call; the actual
    previous-close reference always comes from _previous_session_close() (the
    last genuine close tick of the prior trading day in option_chain_index_spot).
    """
    if not underlyings:
        return {}

    raw_db = db._db if hasattr(db, '_db') else db
    col = raw_db['option_chain_index_spot']
    today = datetime.now().strftime('%Y-%m-%d')
    # On a non-trading day (weekend/holiday) this resolves back to the last real
    # session (e.g. Friday) — both "what counts as the current price" and "what's
    # the previous close to compare it against" anchor off that day instead of
    # literal today, so the weekend view shows Friday-vs-Thursday's real move
    # rather than Friday's frozen price compared against itself.
    session_day = _latest_trading_day(raw_db, today)
    quotes: dict[str, dict[str, float]] = {}

    dhan_spot_ids = {
        'NIFTY': '13',
        'BANKNIFTY': '25',
        'FINNIFTY': '27',
        'SENSEX': '51',
        'MIDCPNIFTY': '11915',
        'BANKEX': '69',
        # Same Dhan IDX_I security id kite_ticker.py/dhan_ticker.py/broker_gateway.py
        # already use for VIX elsewhere (BROKER_VIX_TOKEN's dhan branch) — routed
        # through this same batched quote + option_chain_index_spot prev-close
        # lookup so the ticker bar's VIX tile gets change_pct/change_points too,
        # not just a bare LTP (option_chain_index_spot rows tag VIX as 'INDIAVIX').
        'INDIAVIX': '20225',
    }

    def _quote_entry(spot_price: float, prev_close: float, ohlc_open: float, ohlc_high: float, ohlc_low: float) -> dict[str, float]:
        change_pct = round((spot_price - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0
        change_points = round(spot_price - prev_close, 2) if prev_close > 0 else 0.0
        return {
            'spot_price': spot_price,
            'ltp': spot_price,
            'previous_close': prev_close,
            'change_pct': change_pct,
            'change_points': change_points,
            'ohlc_open': ohlc_open,
            'ohlc_high': ohlc_high,
            'ohlc_low': ohlc_low,
            'ohlc_close': prev_close,
        }

    try:
        cfg = raw_db['kite_market_config'].find_one({'broker': 'dhan', 'enabled': True}) or {}
        access_token = str(cfg.get('access_token') or '').strip()
        client_id = str(cfg.get('user_id') or cfg.get('dhan_client_id') or '').strip()
        idx_tokens = [int(tok) for und, tok in dhan_spot_ids.items() if und in underlyings and str(tok).strip().isdigit()]

        if access_token and client_id and idx_tokens and session_day == today:
            from features.broker_gateway import dhan_quote_post

            # Was a blind time.sleep(1.1) here to dodge a 429 against whatever
            # else might be calling Dhan's quote endpoint "inside the same
            # positions API cycle" — but that's only one of several
            # independent callers in this app (option chain, stock spot,
            # live-quote WS's background refresh, ...), so a local sleep here
            # couldn't actually coordinate with any of them. dhan_quote_post()
            # is the single shared gate every one of those now goes through.
            response = dhan_quote_post({'IDX_I': idx_tokens}, access_token, client_id, timeout=5.0)
            payload = response.json() if response is not None and response.ok else {}
            idx_data = (payload.get('data') or {}).get('IDX_I') or payload.get('IDX_I') or {}
            for underlying, token in dhan_spot_ids.items():
                if underlying not in underlyings:
                    continue
                # Isolated per-underlying: one bad entry (missing from Dhan's
                # response, a 429, a parsing hiccup) no longer aborts the whole
                # batch — every underlying up to and including the one that
                # errored used to get silently dropped for that fetch cycle
                # since this all lived under one try/except.
                try:
                    info = idx_data.get(str(token)) if isinstance(idx_data, dict) else None
                    if not isinstance(info, dict):
                        continue
                    spot_price = _safe_float(info.get('last_price') or info.get('ltp'))
                    if spot_price <= 0:
                        continue
                    ohlc = info.get('ohlc') if isinstance(info.get('ohlc'), dict) else {}
                    prev_close = _previous_session_close(col, underlying, session_day)
                    quotes[underlying] = _quote_entry(
                        spot_price, prev_close,
                        _safe_float(ohlc.get('open')), _safe_float(ohlc.get('high')), _safe_float(ohlc.get('low')),
                    )
                    _LAST_GOOD_UNDERLYING_QUOTE[underlying] = quotes[underlying]
                except Exception as exc:
                    log.debug('dhan index quote per-underlying error underlying=%s token=%s: %s', underlying, token, exc)
    except Exception as exc:
        log.debug('dhan index quote rest fallback error: %s', exc)

    # Stock underlyings (anything not in the hardcoded index set above, e.g.
    # individual F&O stocks like BSE/TCS) — there's no spot token for these
    # stored anywhere in this app's own collections, but Dhan's CSV scrip
    # master always carries the cash-equity security id for any NSE symbol,
    # quotable via the same /marketfeed/quote endpoint (segment NSE_EQ, one
    # batched call for every stock underlying still missing). Same approach
    # already proven for the option chain's own stock spot price
    # (api.py get_live_greeks_chain, NSE_EQ branch) — reused here instead of
    # duplicated so this function can price *any* underlying, not just the
    # 6 named indices, and callers (e.g. /simulator/positions/all's
    # `underlyings` map) get change_pct/change_points for stocks too.
    #
    # prev_close specifically does NOT come from that NSE_EQ quote's
    # info['prev_close'] (sourced from Dhan's ohlc.close) — confirmed live
    # that ohlc.close mirrors the current/live price all day for NSE_EQ too,
    # exactly like the IDX_I quirk this function already routes around above
    # via _previous_session_close(). Using it made every stock's
    # change_pct/change_points round to ~0.00% on every single refresh,
    # no matter how often the page re-fetched. The scanner's own daily EOD
    # sync (scanner_stock_historical_data, h_symbol/ch_timestamp/
    # ch_closing_price — same table scanner/service.py's _load_latest_closes
    # already reads for the portfolio-detail page) carries the real
    # previous-trading-day close instead.
    stock_underlyings = [u for u in underlyings if u not in quotes and u not in dhan_spot_ids]
    if stock_underlyings and session_day == today:
        try:
            from api import _get_dhan_equity_sec_id, _fetch_dhan_market_data  # type: ignore  (lazy: api.py imports this module's package at startup)
            sec_id_by_underlying = {
                u: sec_id
                for u in stock_underlyings
                if (sec_id := _get_dhan_equity_sec_id(u))
            }
            if sec_id_by_underlying:
                equity_quotes = _fetch_dhan_market_data(
                    'NSE_EQ', [int(sec_id) for sec_id in sec_id_by_underlying.values()], db,
                )
                for underlying, sec_id in sec_id_by_underlying.items():
                    info = equity_quotes.get(str(sec_id)) or {}
                    spot_price = _safe_float(info.get('ltp'))
                    if spot_price <= 0:
                        continue
                    prev_close = _equity_previous_close(raw_db, underlying)
                    quotes[underlying] = _quote_entry(spot_price, prev_close, 0.0, 0.0, 0.0)
                    _LAST_GOOD_UNDERLYING_QUOTE[underlying] = quotes[underlying]
        except Exception as exc:
            log.debug('dhan stock equity quote error underlyings=%s: %s', stock_underlyings, exc)

    for underlying in underlyings:
        if underlying in quotes:
            continue
        session_doc = col.find_one(
            {'underlying': underlying, 'timestamp': {'$gte': session_day, '$lt': f'{session_day}T23:59:59'}},
            sort=[('timestamp', -1)],
        ) or {}
        spot_price = _safe_float(session_doc.get('spot_price'))
        if spot_price > 0:
            prev_close = _previous_session_close(col, underlying, session_day)
            quotes[underlying] = _quote_entry(spot_price, prev_close, 0.0, 0.0, 0.0)
            _LAST_GOOD_UNDERLYING_QUOTE[underlying] = quotes[underlying]

    # Last resort — Dhan returned nothing for this underlying on this call
    # (pre-market quiet segment, transient 429, etc.): reuse the last real
    # quote we ever saw for it instead of dropping it (which the UI renders
    # as a bare "—" for spot/change_pct/change_points).
    for underlying in underlyings:
        if underlying not in quotes and underlying in _LAST_GOOD_UNDERLYING_QUOTE:
            quotes[underlying] = _LAST_GOOD_UNDERLYING_QUOTE[underlying]

    return quotes

# Token metadata (active_option_tokens, broker='dhan') is contract
# definitions written once per token-sync run, not per price tick — yet
# _fetch_dhan_broker_option_positions() below re-scanned the whole
# 130k+-row collection and rebuilt both lookup dicts from scratch on every
# single positions poll, which timing confirmed was the single largest
# fixed cost on /simulator/positions/all (~0.7-0.9s out of ~2.5-4.4s total).
# A short TTL absorbs that for free without ever serving metadata more than
# a few seconds stale — same pattern as _FNO_STOCKS_CACHE/_india_vix_cache.
_DHAN_TOKEN_MAPS_CACHE: dict[str, Any] = {}
_DHAN_TOKEN_MAPS_CACHE_TTL = 30.0


def _get_dhan_token_maps(raw_db) -> tuple[dict[str, dict], dict[tuple[str, str, float, str], dict]]:
    cached = _DHAN_TOKEN_MAPS_CACHE
    if cached and (perf_counter() - cached.get('fetched_at', 0)) < _DHAN_TOKEN_MAPS_CACHE_TTL:
        return cached['token_map'], cached['dhan_token_by_contract']

    token_rows = list(raw_db['active_option_tokens'].find(
        {'broker': 'dhan'},
        {
            '_id': 0,
            'token': 1,
            'instrument': 1,
            'expiry': 1,
            'strike': 1,
            'option_type': 1,
            'symbol': 1,
            'lot_size': 1,
            'ws_segment': 1,
        },
    ))
    token_map = {
        str(row.get('token') or '').strip(): row
        for row in token_rows
        if str(row.get('token') or '').strip()
    }
    # Reverse index so a FlatTrade leg (which has no Dhan token of its own)
    # can be matched to Dhan's token for the same contract — the join key the
    # whole "LTP always from Dhan" design relies on.
    dhan_token_by_contract = {
        (
            str(row.get('instrument') or '').strip().upper(),
            str(row.get('expiry') or '').strip()[:10],
            _safe_float(row.get('strike')),
            str(row.get('option_type') or '').strip().upper(),
        ): row
        for row in token_rows
    }
    _DHAN_TOKEN_MAPS_CACHE['token_map'] = token_map
    _DHAN_TOKEN_MAPS_CACHE['dhan_token_by_contract'] = dhan_token_by_contract
    _DHAN_TOKEN_MAPS_CACHE['fetched_at'] = perf_counter()
    return token_map, dhan_token_by_contract


def _fetch_dhan_broker_option_positions(
    db: MongoData,
    selected_broker_id: str | None = None,
    include_broker_status: bool = True,
) -> dict[str, Any]:
    """
    Fetch live option positions normalized to the update-socket open_positions
    shape used by the simulator frontend.

    Positions/orders are per-broker (Dhan here via kite_market_config,
    FlatTrade via broker_configuration — see the flattrade block below), but
    LTP/spot is always resolved through Dhan's own feed (active_option_tokens
    + dhan_ticker_manager/REST quotes) regardless of which broker actually
    holds the leg. Same split AlgoTest/Sensibull use: trade execution stays
    broker-native, market data is centralized on one feed.
    """
    from features.broker_gateway import (
        broker_refresh_user_tokens as _broker_refresh_user_tokens,
        get_broker_rest_quotes as _get_broker_rest_quotes,
    )

    raw_db = db._db if hasattr(db, '_db') else db
    broker_status: list[dict] = []
    normalized_selected_broker_id = str(selected_broker_id or '').strip()
    selected_broker_kind = ''
    # broker-status calls (include_broker_status=True) only need login state
    # for the dropdown badges, never position data — every fetch below is
    # skipped in that mode instead of pulling full positions and discarding
    # them.
    fetch_positions = not include_broker_status

    # ── Dhan: positions fetched directly via kite_market_config ──
    # Resolved from Dhan's own doc only — never gated by _active_broker()/the
    # "enabled" toggle, which is a separate single-active-broker concept for
    # market-data/WS routing (e.g. which ticker singleton starts), not for
    # whether this user can view a given broker's positions here. Logging
    # into one broker must never block reading another's (same reasoning as
    # the FlatTrade/Kite branches below, which were never gated this way).
    raw_positions: list[dict] = []
    dhan_detail = ''
    cfg = raw_db['kite_market_config'].find_one({'broker': 'dhan'}) or {}
    dhan_broker_id = str(cfg.get('_id') or '').strip()
    if normalized_selected_broker_id and normalized_selected_broker_id == dhan_broker_id:
        selected_broker_kind = 'dhan'
    dhan_access_token = str(cfg.get('access_token') or '').strip()
    dhan_client_id = str(cfg.get('user_id') or cfg.get('dhan_client_id') or '').strip()
    dhan_logged_in = bool(dhan_access_token and dhan_client_id)
    if not dhan_logged_in:
        dhan_detail = 'Dhan credentials not configured'
    elif include_broker_status:
        # Stored-credential presence can't tell a stale token from a live one
        # — only worth the extra API ping here, on the low-frequency
        # broker-status badge call, never on the position-poll hot path below
        # (where the positions fetch itself already proves the session).
        from features.dhan_broker_ws import validate_access_token as _validate_dhan_token
        if not _validate_dhan_token(dhan_access_token):
            dhan_logged_in = False
            dhan_detail = 'Dhan session expired. Please login again.'

    # Reverse index so a FlatTrade leg (which has no Dhan token of its own)
    # can be matched to Dhan's token for the same contract — the join key the
    # whole "LTP always from Dhan" design relies on. Cached (see
    # _get_dhan_token_maps) since this metadata only changes when token-sync
    # runs, not on every poll.
    token_map, dhan_token_by_contract = _get_dhan_token_maps(raw_db)

    # ── FlatTrade / Kite: account docs only (mongo) ──
    # broker_configuration has no persisted "enabled"/"is_logged_in" flag for
    # these two — historically the only way to know was a live API ping
    # (kite.profile() / FlatTrade session validate), which is as slow as
    # fetching positions itself. So, same as Dhan above, treat stored
    # credential presence as "logged in" here too; the real proof of a still-
    # valid session is the positions fetch below (already wrapped in
    # try/except), which only runs for the one broker actually being used.
    from features.flattrade_broker import get_flattrade_instance, parse_flattrade_tsym
    from features.kite_broker_ws import parse_kite_tradingsymbol

    def _broker_login_url(broker_name: str, broker_doc_id: str) -> str:
        normalized_name = str(broker_name or '').lower()
        if 'flattrade' in normalized_name:
            return f'/broker/flattrade/login?broker_doc_id={broker_doc_id}'
        if 'zerodha' in normalized_name or 'kite' in normalized_name:
            return f'/broker/kite/login?broker_doc_id={broker_doc_id}'
        return ''

    def _list_broker_docs(broker_keywords: tuple[str, ...]) -> list[dict]:
        query: dict[str, Any] = {'broker_type': 'live'}
        if normalized_selected_broker_id:
            try:
                query['_id'] = ObjectId(normalized_selected_broker_id)
            except Exception:
                return []
        docs: list[dict] = []
        for doc in raw_db['broker_configuration'].find(query):
            broker_name = str(doc.get('broker_name') or doc.get('name') or '').strip()
            if any(keyword in broker_name.lower() for keyword in broker_keywords):
                docs.append(doc)
        return docs

    def _account_login_state(doc: dict, verify_live: bool = False) -> dict:
        broker_doc_id = str(doc.get('_id') or '').strip()
        broker_name = str(doc.get('broker_name') or doc.get('name') or '').strip()
        access_token = str(doc.get('access_token') or '').strip()
        api_key = str(doc.get('api_key') or '').strip()
        is_kite = 'zerodha' in broker_name.lower() or 'kite' in broker_name.lower()
        has_credentials = bool(access_token) and (bool(api_key) if is_kite else True)

        is_logged_in = has_credentials
        message = '' if has_credentials else 'Credentials not configured'
        if has_credentials and verify_live:
            # Stored-token presence can't tell a stale Kite/FlatTrade token
            # from a live one — both expire silently server-side with no DB
            # write, so only a real API ping (kite.profile() / FlatTrade
            # OrderBook, via the same check /broker-configurations already
            # runs) tells the truth. Only paid for here, on the
            # low-frequency broker-status badge call — never on the
            # position-poll hot path, where the positions fetch itself is
            # already a live proof of the session.
            from features.broker_accounts import validate_broker_configuration_session
            is_logged_in, _expired, validate_message = validate_broker_configuration_session(doc, raw_db)
            if not is_logged_in:
                message = validate_message or 'Session expired. Please login again.'

        return {
            '_id': broker_doc_id,
            'broker_name': broker_name,
            'broker_icon': str(doc.get('broker_icon') or '').strip(),
            'account_id': str(doc.get('user_id') or '').strip(),
            'user_name': str(doc.get('user_name') or '').strip(),
            'api_key': api_key,
            'access_token': access_token,
            'is_logged_in': is_logged_in,
            'login_url': '' if is_logged_in else _broker_login_url(broker_name, broker_doc_id),
            'message': message,
        }

    try:
        flattrade_docs = _list_broker_docs(('flattrade',))
    except Exception as exc:
        flattrade_docs = []
        log.debug('flattrade account lookup error: %s', exc)
    try:
        kite_docs = _list_broker_docs(('kite', 'zerodha'))
    except Exception as exc:
        kite_docs = []
        log.debug('kite account lookup error: %s', exc)

    for doc in flattrade_docs:
        if normalized_selected_broker_id and normalized_selected_broker_id == str(doc.get('_id') or '').strip():
            selected_broker_kind = 'flattrade'
    for doc in kite_docs:
        if normalized_selected_broker_id and normalized_selected_broker_id == str(doc.get('_id') or '').strip():
            selected_broker_kind = 'kite'

    # ── Decide which single broker's positions actually need fetching ──
    # A specific broker_id (the dropdown's by-broker call) always wins.
    # Otherwise (the "All Brokers" view) only the first logged-in broker in
    # dropdown order — Dhan, then FlatTrade, then Kite — is used, instead of
    # merging all three brokers' positions on every load.
    flattrade_accounts: list[dict] = []
    kite_accounts: list[dict] = []
    target_kind = selected_broker_kind

    if fetch_positions and not normalized_selected_broker_id:
        if dhan_logged_in:
            target_kind = 'dhan'
        else:
            flattrade_accounts = [_account_login_state(doc) for doc in flattrade_docs]
            if any(account['is_logged_in'] for account in flattrade_accounts):
                target_kind = 'flattrade'
            else:
                kite_accounts = [_account_login_state(doc) for doc in kite_docs]
                if any(account['is_logged_in'] for account in kite_accounts):
                    target_kind = 'kite'

    if include_broker_status:
        # Dropdown badges need every broker's login state regardless of which
        # one (if any) ends up supplying position data above.
        if not flattrade_accounts:
            flattrade_accounts = [_account_login_state(doc, verify_live=True) for doc in flattrade_docs]
        if not kite_accounts:
            kite_accounts = [_account_login_state(doc, verify_live=True) for doc in kite_docs]
        broker_status.append({
            'broker_id': dhan_broker_id,
            'broker_name': 'dhan',
            'broker_key': 'dhan',
            'broker_icon': 'dhan.svg',
            'user_name': str(cfg.get('user_name') or '').strip(),
            'is_logged_in': dhan_logged_in,
            'login_url': '',
            'message': dhan_detail,
        })
        for account in flattrade_accounts:
            broker_status.append({
                'broker_id': account['_id'],
                'broker_name': account.get('broker_name') or 'FlatTrade',
                'broker_key': 'flattrade',
                'broker_icon': account.get('broker_icon') or 'flattrade.svg',
                'user_name': account.get('user_name') or '',
                'is_logged_in': bool(account.get('is_logged_in')),
                'login_url': account.get('login_url') or '',
                'message': account.get('message') or '',
            })
        for account in kite_accounts:
            broker_status.append({
                'broker_id': account['_id'],
                'broker_name': account.get('broker_name') or 'kite',
                'broker_key': 'kite',
                'broker_icon': account.get('broker_icon') or 'kite-logo.svg',
                'user_name': account.get('user_name') or '',
                'is_logged_in': bool(account.get('is_logged_in')),
                'login_url': account.get('login_url') or '',
                'message': account.get('message') or '',
            })

    if target_kind == 'dhan' and dhan_logged_in:
        try:
            import requests as _req  # type: ignore

            response = _req.get(
                'https://api.dhan.co/v2/positions',
                headers={
                    'access-token': dhan_access_token,
                    'client-id': dhan_client_id,
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                },
                timeout=8,
            )
            payload = response.json() if response.ok else {}
            # Debug: raw Dhan /v2/positions response, untouched by our entry_price/exit_price
            # normalization below — compare this against Dhan's own UI numbers when they drift.
            print(f"[DHAN RAW /v2/positions] broker_id={dhan_broker_id} status={response.status_code} payload={payload}")
            if isinstance(payload, list):
                raw_positions = payload
            elif isinstance(payload, dict):
                for key in ('data', 'positions', 'open_positions', 'result'):
                    candidate = payload.get(key)
                    if isinstance(candidate, list):
                        raw_positions = candidate
                        break
        except Exception as exc:
            dhan_detail = f'Failed to fetch dhan positions: {exc}'

    flattrade_detail = ''
    flattrade_raw_legs: list[dict] = []
    if target_kind == 'flattrade':
        if not flattrade_accounts:
            flattrade_accounts = [_account_login_state(doc) for doc in flattrade_docs]
        for account in flattrade_accounts:
            if not account.get('is_logged_in'):
                # Reached when a specific broker_id was requested directly
                # (e.g. the Positions page's by-broker dropdown) and that
                # account has no/missing credentials — surface why instead of
                # silently returning zero positions.
                flattrade_detail = account.get('message') or 'FlatTrade not logged in'
                continue
            adapter = get_flattrade_instance(str(account.get('account_id') or ''), str(account.get('access_token') or ''))
            if adapter is None:
                flattrade_detail = 'FlatTrade account_id or access_token missing'
                continue
            try:
                rows = adapter.raw_positions()
            except Exception as exc:
                # raw_positions() only raises on a session/auth failure (see its
                # docstring) — anything else it tolerates as "no positions" —
                # so this is always a real "please log in again", not a blip.
                log.debug('flattrade raw_positions error account=%s: %s', account.get('_id'), exc)
                flattrade_detail = f'FlatTrade session expired: {exc}'
                continue
            flattrade_detail = ''  # this account's fetch succeeded
            account_doc_id = str(account.get('_id') or '').strip()
            for row in (rows or []):
                if not isinstance(row, dict):
                    continue
                net_qty = _safe_int(row.get('netqty'))
                if net_qty == 0:
                    # Closed-leg booked P&L isn't mapped yet for FlatTrade (no
                    # confirmed day-buy/day-sell/realized-pnl field names) — only
                    # open legs are surfaced for now.
                    continue
                parsed = parse_flattrade_tsym(row.get('tsym'))
                if not parsed:
                    continue
                contract_key = (parsed['underlying'], parsed['expiry'], parsed['strike'], parsed['option_type'])
                flattrade_raw_legs.append({
                    **parsed,
                    'broker_id': account_doc_id,
                    'net_qty': net_qty,
                    'avg_price': _safe_float(row.get('netavgprc')),
                    'own_ltp': _safe_float(row.get('lp')),
                    'tsym': str(row.get('tsym') or ''),
                    'dhan_token_doc': dhan_token_by_contract.get(contract_key) or {},
                })
            break  # only the first logged-in FlatTrade account in dropdown order

    kite_detail = ''
    kite_raw_legs: list[dict] = []
    if target_kind == 'kite':
        if not kite_accounts:
            kite_accounts = [_account_login_state(doc) for doc in kite_docs]
        for account in kite_accounts:
            if not account.get('is_logged_in'):
                # Reached when a specific broker_id was requested directly
                # and that account has no/missing credentials — surface why
                # instead of silently returning zero positions.
                kite_detail = account.get('message') or 'Kite not logged in'
                continue
            try:
                from kiteconnect import KiteConnect  # type: ignore
                from kiteconnect.exceptions import TokenException  # type: ignore

                api_key = str(account.get('api_key') or '').strip()
                access_token = str(account.get('access_token') or '').strip()
                if not api_key or not access_token:
                    log.debug('kite account missing api_key/access_token account=%s', account.get('_id'))
                    kite_detail = 'Kite api_key or access_token missing'
                    continue
                kite = KiteConnect(api_key=api_key)
                kite.set_access_token(access_token)
                positions = kite.positions() if kite is not None else {}
                rows = positions.get('net') if isinstance(positions, dict) else []
            except TokenException as exc:
                log.debug('kite access token invalid account=%s: %s', account.get('_id'), exc)
                kite_detail = f'Kite session expired: {exc}'
                continue
            except Exception as exc:
                log.debug('kite positions error account=%s: %s', account.get('_id'), exc)
                kite_detail = f'Failed to fetch Kite positions: {exc}'
                continue
            kite_detail = ''  # this account's fetch succeeded
            account_doc_id = str(account.get('_id') or '').strip()
            for row in (rows or []):
                if not isinstance(row, dict):
                    continue
                net_qty = _safe_int(row.get('quantity'))
                if net_qty == 0:
                    continue
                # Reverse-lookup against Kite's own instrument cache rather than a
                # hand-rolled regex — see parse_kite_tradingsymbol's docstring for
                # why (monthly vs weekly symbol grammar differs). Returns None
                # (leg skipped, not crashed) if that cache is empty — notably when
                # Dhan is the active feed broker, which it is today.
                parsed = parse_kite_tradingsymbol(row.get('tradingsymbol'))
                if not parsed:
                    log.debug('kite leg symbol unresolved, skipping: %s', row.get('tradingsymbol'))
                    continue
                contract_key = (parsed['underlying'], parsed['expiry'], parsed['strike'], parsed['option_type'])
                kite_raw_legs.append({
                    **parsed,
                    'broker_id': account_doc_id,
                    'net_qty': net_qty,
                    'avg_price': _safe_float(row.get('average_price')),
                    'own_ltp': _safe_float(row.get('last_price')),
                    'tsym': str(row.get('tradingsymbol') or ''),
                    'dhan_token_doc': dhan_token_by_contract.get(contract_key) or {},
                })
            break  # only the first logged-in Kite account in dropdown order

    spot_ltp_by_underlying: dict[str, float] = {}
    try:
        from features.dhan_ticker import dhan_ticker_manager
        spot_ltp_by_underlying = {
            str(key or '').strip().upper(): _safe_float(value)
            for key, value in dict(dhan_ticker_manager.spot_map or {}).items()
            if str(key or '').strip()
        }
        live_ltp_map = dict(dhan_ticker_manager.ltp_map or {})
    except Exception:
        spot_ltp_by_underlying = {}
        live_ltp_map = {}

    open_positions: list[dict] = []
    subscribe_tokens: list[dict] = []
    token_market_data: list[dict] = []
    seen_tokens: set[str] = set()

    ws_segment_by_token: dict[str, str] = {}
    for row in (raw_positions or []):
        if not isinstance(row, dict):
            continue
        token = str(
            row.get("securityId")
            or row.get("security_id")
            or row.get("token")
            or row.get("instrument_token")
            or ""
        ).strip()
        if not token:
            continue
        token_doc = token_map.get(token) or {}
        ws_segment = str(token_doc.get("ws_segment") or row.get("exchangeSegment") or "").strip().upper()
        if ws_segment in {"NSE_FNO", "BSE_FNO"}:
            ws_segment_by_token[token] = ws_segment

    # FlatTrade/Kite legs matched to a Dhan token also need a quote
    # prefetched — same centralized-LTP join as the Dhan-native rows above.
    for leg in flattrade_raw_legs + kite_raw_legs:
        dhan_token_doc = leg.get('dhan_token_doc') or {}
        token = str(dhan_token_doc.get('token') or '').strip()
        ws_segment = str(dhan_token_doc.get('ws_segment') or '').strip().upper()
        if token and ws_segment in {"NSE_FNO", "BSE_FNO"}:
            ws_segment_by_token[token] = ws_segment

    rest_quote_map: dict[str, dict] = {}
    if ws_segment_by_token:
        try:
            rest_quote_map = _get_broker_rest_quotes(list(ws_segment_by_token.keys()), raw_db, ws_segment_by_token)
        except Exception as exc:
            log.debug("dhan broker rest quote prefetch error: %s", exc)

    def _append_external_broker_leg(leg: dict, source: str, broker_label: str) -> None:
        """
        Shared by the FlatTrade and Kite branches below — both produce legs
        in the same {underlying, option_type, strike, expiry, net_qty,
        avg_price, own_ltp, tsym, dhan_token_doc} shape, so they share one
        leg-builder rather than duplicating it per broker.
        """
        underlying = leg['underlying']
        option_type = leg['option_type']
        strike = leg['strike']
        expiry_date = leg['expiry']
        net_qty = leg['net_qty']
        position = 'BUY' if net_qty > 0 else 'SELL'
        quantity = abs(net_qty)
        entry_price = leg['avg_price']

        dhan_token_doc = leg.get('dhan_token_doc') or {}
        dhan_token = str(dhan_token_doc.get('token') or '').strip()
        ws_segment = str(dhan_token_doc.get('ws_segment') or '').strip().upper()
        rest_quote = rest_quote_map.get(dhan_token) or {} if dhan_token else {}
        # Same centralized-LTP join as Dhan legs: prefer Dhan's live ticker/REST
        # quote for this contract; only fall back to the source broker's own
        # quote when Dhan hasn't synced this exact strike/expiry.
        ltp = _safe_float(
            live_ltp_map.get(dhan_token)
            or rest_quote.get('ltp')
            or leg['own_ltp']
        )
        if ltp <= 0:
            ltp = leg['own_ltp'] or entry_price
        spot_price = _safe_float(spot_ltp_by_underlying.get(underlying))
        symbol = leg['tsym']

        open_positions.append({
            'trade_id': f'{source}:{dhan_token or symbol or len(open_positions)}',
            'strategy_id': f'{source}-broker:{underlying or "UNKNOWN"}',
            'strategy_name': f'{broker_label} Live Positions - {underlying}'.strip(' -') if underlying else f'{broker_label} Live Positions',
            'group_name': 'Broker',
            'broker_id': str(leg.get('broker_id') or '').strip(),
            'broker_key': source,
            'ticker': underlying,
            'underlying': underlying,
            'leg_id': f'{source}:{dhan_token or symbol or len(open_positions)}',
            'position': position,
            'option': option_type,
            'strike': strike,
            'expiry_date': expiry_date,
            'entry_price': entry_price,
            'exit_price': 0.0,
            'ltp': ltp,
            'spot_price': spot_price,
            'quantity': quantity,
            'exited': False,
            'realized_pnl': 0.0,
            'timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            'symbol': symbol,
            'token': dhan_token,
            'source': f'{source}_positions',
        })

        if dhan_token and dhan_token not in seen_tokens:
            seen_tokens.add(dhan_token)
            subscribe_tokens.append({
                'token': dhan_token,
                'symbol': symbol,
                'underlying': underlying,
                'ws_segment': ws_segment or 'NSE_FNO',
            })
            token_market_data.append({
                'token': dhan_token,
                'symbol': symbol,
                'underlying': underlying,
                'ltp': ltp,
                'option_type': option_type,
                'exchange_segment': ws_segment or 'NSE_FNO',
            })

    for row in (raw_positions or []):
        if not isinstance(row, dict):
            continue
        if normalized_selected_broker_id and selected_broker_kind and selected_broker_kind != 'dhan':
            continue

        token = str(
            row.get('securityId')
            or row.get('security_id')
            or row.get('token')
            or row.get('instrument_token')
            or ''
        ).strip()
        token_doc = token_map.get(token) or {}

        # FNO-only filter: Dhan's /v2/positions also returns NSE_EQ/CNC cash
        # holdings in the same list — those must never reach the simulator.
        ws_segment = str(token_doc.get('ws_segment') or row.get('exchangeSegment') or '').strip().upper()
        if ws_segment not in {'NSE_FNO', 'BSE_FNO'}:
            continue

        raw_option_type = str(
            token_doc.get('option_type')
            or row.get('drvOptionType')
            or row.get('optionType')
            or row.get('right')
            or ''
        ).strip().upper()
        # Dhan reports CALL/PUT (or NA for futures) instead of CE/PE.
        option_type = {'CALL': 'CE', 'PUT': 'PE'}.get(raw_option_type, raw_option_type)
        if option_type not in {'CE', 'PE'}:
            option_type = 'FUT'

        buy_qty = _safe_int(row.get('buyQty'))
        sell_qty = _safe_int(row.get('sellQty'))
        net_qty = _safe_int(
            row.get('netQty')
            or row.get('netqty')
            or row.get('quantity')
            or row.get('qty')
        )
        # netQty stays 0 once a leg is squared off — that's a closed position,
        # not a missing one, so it must still be reported (with realized PnL).
        exited = net_qty == 0
        if exited and buy_qty == 0 and sell_qty == 0:
            continue

        # Dhan trading symbols are "<UNDERLYING>-<expiry>-<strike>-<CE/PE>" (or
        # "<UNDERLYING>-<expiry>-FUT") — when the contract hasn't been synced
        # into active_option_tokens yet, parse the underlying out of that
        # symbol instead of falling back to the raw, unsplit string.
        trading_symbol_raw = str(row.get('tradingSymbol') or row.get('securitySymbol') or '').strip()
        parsed_underlying = trading_symbol_raw.split('-')[0].strip() if trading_symbol_raw else ''
        underlying = str(
            token_doc.get('instrument')
            or row.get('underlying')
            or parsed_underlying
            or trading_symbol_raw
            or ''
        ).strip().upper()
        expiry_date = str(
            token_doc.get('expiry') or row.get('drvExpiryDate') or row.get('expiryDate') or row.get('expiry') or ''
        ).strip()[:10]
        strike = token_doc.get('strike')
        if strike in (None, ''):
            strike = _safe_float(row.get('drvStrikePrice') or row.get('strikePrice') or row.get('strike') or 0)

        buy_avg = _safe_float(row.get('buyAvg'))
        sell_avg = _safe_float(row.get('sellAvg'))
        cost_price = _safe_float(row.get('costPrice'))
        # Dhan doesn't expose fill order, so for a squared-off leg we assume the
        # side with the larger filled quantity was the original entry direction.
        position = 'BUY' if (net_qty > 0 or (exited and buy_qty >= sell_qty)) else 'SELL'

        if exited:
            entry_price = (buy_avg if position == 'BUY' else sell_avg) or _safe_float(
                row.get('avgPrice') or row.get('averagePrice') or row.get('netPrice')
            )
            exit_price = sell_avg if position == 'BUY' else buy_avg
        else:
            # Dhan's own "Avg Price" UI column is buyAvg/sellAvg (whichever
            # side this leg is on) — costPrice is only a fallback for when
            # Dhan hasn't populated buyAvg/sellAvg for this leg at all.
            entry_price = (buy_avg if position == 'BUY' else sell_avg) or cost_price
            exit_price = 0.0

        rest_quote = rest_quote_map.get(token) or {}
        ltp = _safe_float(
            live_ltp_map.get(token)
            or rest_quote.get("ltp")
            or row.get('lastPrice')
            or row.get('ltp')
            or row.get('lastTradedPrice')
        )
        if exited:
            ltp = exit_price or entry_price
        elif ltp <= 0:
            ltp = entry_price

        quantity = abs(net_qty) if not exited else max(buy_qty, sell_qty)
        realized_pnl = _safe_float(row.get('realizedProfit')) if exited else 0.0
        symbol = str(
            token_doc.get('symbol')
            or row.get('tradingSymbol')
            or row.get('securitySymbol')
            or ''
        ).strip()
        spot_price = _safe_float(spot_ltp_by_underlying.get(underlying))

        open_positions.append({
            'trade_id': f'dhan:{token or symbol or len(open_positions)}',
            # Keyed per underlying so the simulator UI groups each
            # underlying (NIFTY, SENSEX, ...) into its own card instead of
            # merging every broker position under one shared strategy id.
            'strategy_id': f'dhan-broker:{underlying or "UNKNOWN"}',
            'strategy_name': f'Dhan Live Positions - {underlying}'.strip(' -') if underlying else 'Dhan Live Positions',
            'group_name': 'Broker',
            'broker_id': dhan_broker_id,
            'broker_key': 'dhan',
            'ticker': underlying,
            'underlying': underlying,
            'leg_id': f'dhan:{token or symbol or len(open_positions)}',
            'position': position,
            'option': option_type,
            'strike': strike,
            'expiry_date': expiry_date,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'ltp': ltp,
            'spot_price': spot_price,
            'quantity': quantity,
            'exited': exited,
            'realized_pnl': realized_pnl,
            'timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            'symbol': symbol,
            'token': token,
            'source': 'dhan_positions',
        })

        if token and token not in seen_tokens:
            seen_tokens.add(token)
            subscribe_tokens.append({
                'token': token,
                'symbol': symbol,
                'underlying': underlying,
                'ws_segment': ws_segment or 'NSE_FNO',
            })
            token_market_data.append({
                'token': token,
                'symbol': symbol,
                'underlying': underlying,
                'ltp': ltp,
                'option_type': option_type,
                'exchange_segment': ws_segment or 'NSE_FNO',
            })

    for leg in flattrade_raw_legs:
        _append_external_broker_leg(leg, 'flattrade', 'FlatTrade')
    for leg in kite_raw_legs:
        _append_external_broker_leg(leg, 'kite', 'Kite')

    try:
        if subscribe_tokens:
            _broker_refresh_user_tokens('__simulator_dhan_positions__', [item.get('token') for item in subscribe_tokens if item.get('token')])
    except Exception as exc:
        log.debug('dhan positions subscribe refresh error: %s', exc)

    # One shared {spot_price, change_pct} object per underlying — fetched
    # once here, then every leg of that underlying is pointed at the same
    # entry below instead of each leg holding its own (possibly stale or
    # inconsistent) copy.
    underlyings = {str(item.get('underlying') or '').strip() for item in open_positions if item.get('underlying')}
    try:
        underlying_quotes = _fetch_dhan_index_quotes(db, underlyings)
    except Exception as exc:
        log.warning('dhan index quote error: %s', exc)
        underlying_quotes = {}
    for u in underlyings:
        if u in underlying_quotes:
            continue
        # _fetch_dhan_index_quotes() comes back without this underlying whenever there's no
        # live Dhan quote AND no tick already stored for *today* (e.g. weekends/holidays, or
        # the simulator's forwarded feed running outside real market hours) — that used to
        # hardcode change_pct/change_points to 0.0 here even though a live spot_price (from
        # the ticker, not Dhan) was right there. Anchor on the latest actual trading day (same
        # as _fetch_dhan_index_quotes) so a weekend/holiday view compares that day's own close
        # against the one before it, instead of always reading +0.00%.
        spot_price = _safe_float(spot_ltp_by_underlying.get(u))
        prev_close = 0.0
        try:
            today_str = datetime.now().strftime('%Y-%m-%d')
            session_day = _latest_trading_day(raw_db, today_str)
            prev_close = _previous_session_close(raw_db['option_chain_index_spot'], u, session_day)
        except Exception as exc:
            log.debug('previous session close fallback error underlying=%s: %s', u, exc)
        underlying_quotes[u] = {
            'spot_price': spot_price,
            'change_pct': round((spot_price - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0,
            'change_points': round(spot_price - prev_close, 2) if prev_close > 0 else 0.0,
        }
    for item in open_positions:
        quote = underlying_quotes.get(str(item.get('underlying') or '').strip()) or {'spot_price': 0.0, 'change_pct': 0.0, 'change_points': 0.0}
        item['spot_price'] = quote['spot_price']
        item['change_pct'] = quote['change_pct']
        item['change_points'] = quote.get('change_points', 0.0)

    # Attach each leg's saved SL/Target ("simulator_triggers"), but only if the position's
    # entry_price/quantity/exited state still matches what it was when the trigger was saved —
    # otherwise the saved %/points was computed off stale numbers (e.g. the user added to the
    # position, or this leg_id slot was closed and reopened by a genuinely different trade) and
    # must not be silently reapplied. A mismatch flips the trigger to status="stale" (never
    # deleted, so there's an audit trail) instead of attaching `risk` to the item below.
    try:
        triggers_col = raw_db['simulator_triggers']
        leg_ids = list({str(item.get('leg_id') or '').strip() for item in open_positions if item.get('leg_id')})
        trig_by_key: dict[tuple[str, str], dict] = {}
        if leg_ids:
            for trig in triggers_col.find({'leg_id': {'$in': leg_ids}, 'status': 'active'}):
                trig_by_key[(str(trig.get('broker_id') or '').strip(), str(trig.get('leg_id') or '').strip())] = trig
        stale_ids = []
        for item in open_positions:
            key = (str(item.get('broker_id') or '').strip(), str(item.get('leg_id') or '').strip())
            trig = trig_by_key.get(key)
            if not trig:
                continue
            entry_matches = abs(_safe_float(trig.get('entry_price_at_set')) - _safe_float(item.get('entry_price'))) < 0.01
            qty_matches = _safe_int(trig.get('quantity_at_set')) == _safe_int(item.get('quantity'))
            was_exited = bool(trig.get('exited_at_set'))
            exited_matches = was_exited == bool(item.get('exited'))
            if entry_matches and qty_matches and exited_matches and not was_exited:
                item['risk'] = {
                    'sl_mode': trig.get('sl_mode'), 'sl_value': trig.get('sl_value'),
                    'tp_mode': trig.get('tp_mode'), 'tp_value': trig.get('tp_value'),
                }
            else:
                stale_ids.append(trig['_id'])
        if stale_ids:
            triggers_col.update_many(
                {'_id': {'$in': stale_ids}},
                {'$set': {'status': 'stale', 'updated_at': datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}},
            )
    except Exception as exc:
        log.debug('paper trade trigger drift-check error: %s', exc)

    # Attach the payoff chart's saved upper/lower stoploss marker ("simulator_portfolio_triggers"),
    # basket-level (one doc per broker+underlying, not per-leg) — only while the set of open
    # legs (and their quantities) for that underlying still exactly matches what it was when the
    # marker was saved. Any leg added/removed/resized since then means the payoff curve the
    # marker was set against no longer reflects reality, so it's invalidated (status flips to
    # "stale", never deleted) instead of being silently reapplied. One call here is always
    # single-broker/multi-underlying (target_kind resolves to exactly one broker), so this is one
    # batched query over underlying, not a query per underlying.
    try:
        portfolio_col = raw_db['simulator_portfolio_triggers']
        call_broker_id = str(open_positions[0].get('broker_id') or '').strip() if open_positions else ''
        trig_by_underlying: dict[str, dict] = {}
        if call_broker_id and underlyings:
            for trig in portfolio_col.find({'broker_id': call_broker_id, 'underlying': {'$in': list(underlyings)}, 'status': 'active'}):
                trig_by_underlying[str(trig.get('underlying') or '').strip()] = trig
        stale_portfolio_ids = []
        for u in underlyings:
            trig = trig_by_underlying.get(u)
            if not trig:
                continue
            current_legs = {
                (str(item.get('leg_id') or '').strip(), _safe_int(item.get('quantity')))
                for item in open_positions
                if str(item.get('underlying') or '').strip() == u and not item.get('exited') and _safe_int(item.get('quantity')) > 0
            }
            snapshot_legs = {(s.get('leg_id'), _safe_int(s.get('quantity'))) for s in (trig.get('legs_snapshot') or [])}
            if current_legs == snapshot_legs:
                for item in open_positions:
                    if str(item.get('underlying') or '').strip() == u:
                        item['portfolio_risk'] = {'sl_upper': trig.get('sl_upper'), 'sl_lower': trig.get('sl_lower')}
            else:
                stale_portfolio_ids.append(trig['_id'])
        if stale_portfolio_ids:
            portfolio_col.update_many(
                {'_id': {'$in': stale_portfolio_ids}},
                {'$set': {'status': 'stale', 'updated_at': datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}},
            )
    except Exception as exc:
        log.debug('paper trade portfolio trigger drift-check error: %s', exc)

    # Each broker's own *_detail above is only ever non-empty after a real
    # failed attempt against the broker actually targeted (target_kind) —
    # never for "no open positions right now" — so this is the one place
    # that turns a swallowed auth/session error into a caller-visible
    # ok=False + message instead of a quietly empty positions list.
    final_detail = ''
    if target_kind == 'dhan' and dhan_detail:
        final_detail = dhan_detail
    elif target_kind == 'flattrade' and flattrade_detail:
        final_detail = flattrade_detail
    elif target_kind == 'kite' and kite_detail:
        final_detail = kite_detail

    return {
        'ok': not final_detail,
        'detail': final_detail,
        'underlyings': underlying_quotes,
        'open_positions': open_positions,
        'subscribe_tokens': subscribe_tokens,
        'token_market_data': token_market_data,
        'broker_status': broker_status,
    }


def _build_strategy_spot_subscribe_tokens(
    records: list[dict],
    db: MongoData,
    trade_date: str,
    market_cache: dict | None = None,
    activation_mode: str | None = None,
) -> list[dict]:
    index_spot_col = db._db['option_chain_index_spot']
    snapshot_ts = f'{str(trade_date or "").strip()} 23:59:59.999999'
    token_map: dict[str, dict] = {}
    for record in (records or []):
        underlying = str(
            record.get('ticker')
            or ((record.get('config') or {}).get('Ticker') or '')
        ).strip().upper()
        if not underlying:
            continue
        spot_doc = get_cached_spot_doc(market_cache, underlying, snapshot_ts) if market_cache else {}
        if not spot_doc:
            spot_doc = get_index_spot_at_time(
                index_spot_col,
                underlying,
                snapshot_ts,
                activation_mode=activation_mode or str(record.get('activation_mode') or '').strip() or None,
            )
        token = str((spot_doc or {}).get('token') or '').strip()
        if not token or token in token_map:
            continue
        token_map[token] = {
            'token': token,
            'underlying': underlying,
            'symbol': str((spot_doc or {}).get('symbol') or underlying).strip(),
            'expiry_date': '',
            'strike': None,
            'option': 'SPOT',
        }
    return list(token_map.values())


def _build_subscribe_tokens(
    open_positions: list[dict],
    strategy_spot_tokens: list[dict] | None = None,
) -> list[dict]:
    token_map: dict[str, dict] = {}
    for item in (strategy_spot_tokens or []):
        token = str(item.get('token') or '').strip()
        if not token or token in token_map:
            continue
        token_map[token] = {
            'token': token,
            'underlying': str(item.get('underlying') or '').strip(),
            'symbol': str(item.get('symbol') or '').strip(),
            'expiry_date': str(item.get('expiry_date') or '').strip(),
            'strike': item.get('strike'),
            'option': str(item.get('option') or '').strip(),
        }
    for item in (open_positions or []):
        token = str(item.get('token') or '').strip()
        if not token:
            continue
        if token in token_map:
            continue
        token_map[token] = {
            'token': token,
            'underlying': str(item.get('underlying') or item.get('ticker') or '').strip(),
            'symbol': str(item.get('symbol') or '').strip(),
            'expiry_date': str(item.get('expiry_date') or '').strip(),
            'strike': item.get('strike'),
            'option': str(item.get('option') or '').strip(),
        }
    return list(token_map.values())


def _build_subscribed_token_market_data(
    open_positions: list[dict],
    strategy_spot_tokens: list[dict] | None = None,
    db: MongoData | None = None,
    trade_date: str = '',
    market_cache: dict | None = None,
    activation_mode: str | None = None,
) -> list[dict]:
    token_data_map: dict[str, dict] = {}
    if db and strategy_spot_tokens:
        index_spot_col = db._db['option_chain_index_spot']
        snapshot_ts = f'{str(trade_date or "").strip()} 23:59:59.999999'
        for item in strategy_spot_tokens:
            token = str(item.get('token') or '').strip()
            underlying = str(item.get('underlying') or '').strip().upper()
            if not token or not underlying:
                continue
            spot_doc = get_cached_spot_doc(market_cache, underlying, snapshot_ts) if market_cache else {}
            if not spot_doc:
                spot_doc = get_index_spot_at_time(
                    index_spot_col,
                    underlying,
                    snapshot_ts,
                    activation_mode=activation_mode,
                )
            token_data_map[token] = {
                'token': token,
                'underlying': underlying,
                'symbol': str((spot_doc or {}).get('symbol') or item.get('symbol') or underlying).strip(),
                'option': 'SPOT',
                'strike': None,
                'expiry_date': '',
                'ltp': _safe_float((spot_doc or {}).get('spot_price')),
                'spot_price': _safe_float((spot_doc or {}).get('spot_price')),
                'timestamp': str((spot_doc or {}).get('timestamp') or '').strip(),
            }
    for item in (open_positions or []):
        token = str(item.get('token') or '').strip()
        if not token:
            continue
        token_data_map[token] = {
            'token': token,
            'underlying': str(item.get('underlying') or item.get('ticker') or '').strip(),
            'symbol': str(item.get('symbol') or '').strip(),
            'option': str(item.get('option') or '').strip(),
            'strike': item.get('strike'),
            'expiry_date': str(item.get('expiry_date') or '').strip(),
            'ltp': _safe_float(item.get('ltp')),
            'spot_price': _safe_float(item.get('spot_price')),
            'timestamp': str(item.get('timestamp') or '').strip(),
        }
    return list(token_data_map.values())


def _build_leg_token(leg: dict, underlying: str) -> str:
    return str(
        leg.get('token')
        or make_token(
            underlying,
            str(leg.get('expiry_date') or ''),
            leg.get('strike'),
            str(leg.get('option') or ''),
        )
    )


def _build_active_leg_contract(leg: dict, trade: dict) -> dict | None:
    entry_trade = leg.get('entry_trade') or {}
    if not entry_trade:
        return None
    underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
    return {
        'trade_id': str(trade.get('_id') or ''),
        'strategy_id': str(trade.get('strategy_id') or ''),
        'strategy_name': str(trade.get('name') or ''),
        'leg_id': str(leg.get('id') or ''),
        'token': _build_leg_token(leg, underlying),
        'symbol': str(leg.get('symbol') or ''),
        'underlying': underlying,
        'expiry_date': str(leg.get('expiry_date') or ''),
        'strike': leg.get('strike'),
        'option': str(leg.get('option') or ''),
        'entry_timestamp': str(entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or ''),
    }


def _fetch_active_leg_ticks(
    chain_col,
    active_contracts: list[dict],
    snapshot_timestamp: str,
    market_cache: dict | None = None,
    activation_mode: str | None = None,
) -> list[dict]:
    token_ticks: list[dict] = []
    normalized_mode = str(activation_mode or '').strip()
    if _is_strict_history_leg_mode(normalized_mode):
        try:
            from features.broker_gateway import broker_ticker_manager as ticker_manager  # type: ignore
        except Exception:
            ticker_manager = None
        for contract in active_contracts:
            token = str(contract.get('token') or '').strip()
            if not token or not ticker_manager:
                continue
            live_ltp = _safe_float(ticker_manager.get_ltp(token))
            if live_ltp <= 0:
                continue
            token_ticks.append({
                'trade_id': contract.get('trade_id'),
                'strategy_id': contract.get('strategy_id'),
                'strategy_name': contract.get('strategy_name'),
                'group_name': contract.get('group_name'),
                'leg_id': contract.get('leg_id'),
                'token': token,
                'symbol': str(contract.get('symbol') or ''),
                'underlying': str(contract.get('underlying') or ''),
                'expiry_date': str(contract.get('expiry_date') or ''),
                'strike': contract.get('strike'),
                'option': str(contract.get('option') or ''),
                'entry_timestamp': contract.get('entry_timestamp'),
                'snapshot': {
                    'token': token,
                    'timestamp': snapshot_timestamp,
                    'underlying': contract.get('underlying'),
                    'expiry': contract.get('expiry_date'),
                    'strike': contract.get('strike'),
                    'type': contract.get('option'),
                    'close': live_ltp,
                    'spot_price': 0,
                    'bb_qty': 0,
                    'bb_price': 0,
                    'ba_qty': 0,
                    'ba_price': 0,
                    'vol_in_day': 0,
                },
            })
        return token_ticks

    for contract in active_contracts:
        chain_doc = get_chain_at_time(
            chain_col,
            contract.get('underlying') or '',
            contract.get('expiry_date') or '',
            contract.get('strike'),
            contract.get('option') or '',
            snapshot_timestamp,
            market_cache=market_cache,
            activation_mode=activation_mode,
        )
        if not chain_doc:
            continue
        # Prefer the actual token from option_chain DB doc (e.g. "NSE_2025110406910")
        chain_token = str(chain_doc.get('token') or '').strip()
        token_ticks.append({
            'trade_id': contract.get('trade_id'),
            'strategy_id': contract.get('strategy_id'),
            'strategy_name': contract.get('strategy_name'),
            'group_name': contract.get('group_name'),
            'leg_id': contract.get('leg_id'),
            'token': chain_token or contract.get('token'),
            'symbol': str(chain_doc.get('symbol') or contract.get('symbol') or ''),
            'underlying': str(chain_doc.get('underlying') or contract.get('underlying') or ''),
            'expiry_date': str(chain_doc.get('expiry') or contract.get('expiry_date') or ''),
            'strike': chain_doc.get('strike') or contract.get('strike'),
            'option': str(chain_doc.get('type') or contract.get('option') or ''),
            'entry_timestamp': contract.get('entry_timestamp'),
            'snapshot': {
                'token': chain_token or contract.get('token'),
                'timestamp': chain_doc.get('timestamp') or snapshot_timestamp,
                'underlying': chain_doc.get('underlying') or contract.get('underlying'),
                'expiry': chain_doc.get('expiry') or contract.get('expiry_date'),
                'strike': chain_doc.get('strike', contract.get('strike')),
                'type': chain_doc.get('type') or contract.get('option'),
                'close': _safe_float(chain_doc.get('close')),
                'oi': chain_doc.get('oi'),
                'iv': chain_doc.get('iv'),
                'delta': chain_doc.get('delta'),
                'gamma': chain_doc.get('gamma'),
                'theta': chain_doc.get('theta'),
                'vega': chain_doc.get('vega'),
                'rho': chain_doc.get('rho'),
                'spot_price': chain_doc.get('spot_price'),
                'bb_qty': chain_doc.get('bb_qty') or chain_doc.get('bid_qty') or 0,
                'bb_price': chain_doc.get('bb_price') or chain_doc.get('bid_price') or 0,
                'ba_qty': chain_doc.get('ba_qty') or chain_doc.get('ask_qty') or 0,
                'ba_price': chain_doc.get('ba_price') or chain_doc.get('ask_price') or 0,
                'vol_in_day': chain_doc.get('vol_in_day') or chain_doc.get('volume') or 0,
            },
        })
    return token_ticks


def _build_active_contracts_from_records(
    records: list[dict],
    db: MongoData | None = None,
    trade_date: str = '',
    market_cache: dict | None = None,
    activation_mode: str | None = None,
) -> list[dict]:
    """
    Collect all open option legs + underlying spot tokens as active contracts.
    Prefers algo_trade_positions_history (status=1) when db is provided.
    Adds one SPOT contract per unique underlying that has open legs — no duplicates.
    Spot tokens are active from entry_time to exit_time of the strategy.
    """
    contracts: list[dict] = []
    trade_ids = [str(r.get('_id') or '') for r in (records or []) if isinstance(r, dict) and r.get('_id')]
    trade_meta: dict[str, dict] = {
        str(r.get('_id') or ''): r for r in (records or []) if isinstance(r, dict)
    }

    # Primary: query history for open legs (status=1)
    if db and trade_ids:
        try:
            history_col = db._db['algo_trade_positions_history']
            for doc in history_col.find({'trade_id': {'$in': trade_ids}, 'status': OPEN_LEG_STATUS}):
                tid = str(doc.get('trade_id') or '')
                rec = trade_meta.get(tid) or {}
                underlying = str(doc.get('ticker') or (rec.get('config') or {}).get('Ticker') or rec.get('ticker') or '')
                entry_trade = doc.get('entry_trade') if isinstance(doc.get('entry_trade'), dict) else None
                if not entry_trade:
                    continue
                contracts.append({
                    'trade_id': tid,
                    'strategy_id': str(doc.get('strategy_id') or rec.get('strategy_id') or ''),
                    'strategy_name': str(doc.get('strategy_name') or rec.get('name') or ''),
                    'group_name': str(doc.get('group_name') or ''),
                    'leg_id': str(doc.get('leg_id') or ''),
                    'token': _build_leg_token(doc, underlying),
                    'symbol': str(doc.get('symbol') or ''),
                    'underlying': underlying,
                    # _normalize_expiry_datetime stores '2025-11-06 15:30:00'; chain
                    # collection uses plain '2025-11-06' — strip the time component.
                    'expiry_date': str(doc.get('expiry_date') or '')[:10],
                    'strike': doc.get('strike'),
                    'option': str(doc.get('option') or ''),
                    'entry_timestamp': str(
                        entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or ''
                    ),
                })
        except Exception as exc:
            log.warning('_build_active_contracts_from_records history query error: %s', exc)

    # Fallback: dict legs not yet synced to history
    history_leg_ids = {c['leg_id'] for c in contracts}
    for record in (records or []):
        if not isinstance(record, dict):
            continue
        underlying = str(record.get('underlying') or (record.get('config') or {}).get('Ticker') or record.get('ticker') or '')
        for leg in (record.get('legs') or []):
            if not isinstance(leg, dict):
                continue
            if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
                continue
            entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else None
            if not entry_trade:
                continue
            leg_id = str(leg.get('id') or '')
            if leg_id in history_leg_ids:
                continue
            contracts.append({
                'trade_id': str(record.get('_id') or ''),
                'strategy_id': str(record.get('strategy_id') or ''),
                'strategy_name': str(record.get('name') or ''),
                'group_name': str(record.get('group_name') or ''),
                'leg_id': leg_id,
                'token': _build_leg_token(leg, underlying),
                'symbol': str(leg.get('symbol') or ''),
                'underlying': underlying,
                'expiry_date': str(leg.get('expiry_date') or ''),
                'strike': leg.get('strike'),
                'option': str(leg.get('option') or ''),
                'entry_timestamp': str(
                    entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or ''
                ),
            })

    # Add underlying SPOT token for each unique underlying that has open legs
    # Deduplicated — one SPOT contract per underlying, no matter how many strategies share it
    active_underlyings = {c['underlying'].strip().upper() for c in contracts if c.get('underlying')}
    if active_underlyings and db:
        index_spot_col = db._db['option_chain_index_spot']
        # ISO format required — collection stores timestamps as "2025-11-03T15:29:00"
        snapshot_ts = f'{str(trade_date or "").strip()}T23:59:59' if trade_date else ''
        spot_token_set: set[str] = set()
        for underlying in sorted(active_underlyings):
            try:
                spot_doc = (
                    get_cached_spot_doc(market_cache, underlying, snapshot_ts)
                    if market_cache and snapshot_ts else {}
                )
                if not spot_doc:
                    spot_doc = (
                        get_index_spot_at_time(
                            index_spot_col,
                            underlying,
                            snapshot_ts,
                            activation_mode=activation_mode,
                        )
                        if snapshot_ts else {}
                    )
                if not spot_doc:
                    # fallback: latest available spot doc for this underlying
                    spot_doc = index_spot_col.find_one(
                        {'underlying': underlying},
                        sort=[('timestamp', DESCENDING)],
                    ) or {}
                spot_token = str((spot_doc or {}).get('token') or '').strip()
                if not spot_token or spot_token in spot_token_set:
                    continue
                spot_token_set.add(spot_token)
                contracts.append({
                    'trade_id': '',
                    'strategy_id': '',
                    'strategy_name': '',
                    'group_name': '',
                    'leg_id': f'spot_{underlying.lower()}',
                    'token': spot_token,
                    'symbol': str((spot_doc or {}).get('symbol') or underlying).strip(),
                    'underlying': underlying,
                    'expiry_date': '',
                    'strike': None,
                    'option': 'SPOT',
                    'entry_timestamp': '',
                })
                print(f'[SPOT SUBSCRIBE] underlying={underlying} token={spot_token}')
            except Exception as exc:
                log.warning('_build_active_contracts_from_records spot token error underlying=%s: %s', underlying, exc)

    # Deduplicate option leg tokens — multiple strategies can share the same strike/token
    seen_tokens: set[str] = set()
    deduped: list[dict] = []
    for c in contracts:
        tok = str(c.get('token') or '')
        if tok and tok in seen_tokens:
            continue
        if tok:
            seen_tokens.add(tok)
        deduped.append(c)

    return deduped


def _build_default_spot_contracts(
    db: MongoData | None = None,
    trade_date: str = '',
    market_cache: dict | None = None,
    activation_mode: str | None = None,
) -> list[dict]:
    """
    Always expose the main index spot contracts to the update socket so the
    forward-test dashboard receives spot LTP even when no strategy legs are
    active yet.
    """
    if not db:
        return []

    index_spot_col = db._db['option_chain_index_spot']
    snapshot_ts = f'{str(trade_date or "").strip()}T23:59:59' if trade_date else ''
    default_underlyings = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'SENSEX', 'MIDCPNIFTY']
    contracts: list[dict] = []
    seen_tokens: set[str] = set()

    for underlying in default_underlyings:
        try:
            spot_doc = (
                get_cached_spot_doc(market_cache, underlying, snapshot_ts)
                if market_cache and snapshot_ts else {}
            )
            if not spot_doc:
                spot_doc = (
                    get_index_spot_at_time(
                        index_spot_col,
                        underlying,
                        snapshot_ts,
                        activation_mode=activation_mode,
                    )
                    if snapshot_ts else {}
                )
            if not spot_doc:
                spot_doc = index_spot_col.find_one(
                    {'underlying': underlying},
                    sort=[('timestamp', DESCENDING)],
                ) or {}

            token = str((spot_doc or {}).get('token') or '').strip()
            if not token or token in seen_tokens:
                continue
            seen_tokens.add(token)
            contracts.append({
                'trade_id': '',
                'strategy_id': '',
                'strategy_name': '',
                'group_name': '',
                'leg_id': f'default_spot_{underlying.lower()}',
                'token': token,
                'symbol': str((spot_doc or {}).get('symbol') or underlying).strip(),
                'underlying': underlying,
                'expiry_date': '',
                'strike': None,
                'option': 'SPOT',
                'entry_timestamp': '',
            })
        except Exception as exc:
            log.warning('_build_default_spot_contracts error underlying=%s: %s', underlying, exc)

    return contracts


def _merge_default_spot_contracts(
    contracts: list[dict] | None,
    *,
    db: MongoData | None = None,
    trade_date: str = '',
    market_cache: dict | None = None,
    activation_mode: str | None = None,
) -> list[dict]:
    merged_contracts = list(contracts or [])
    default_spot_contracts = _build_default_spot_contracts(
        db=db,
        trade_date=trade_date,
        market_cache=market_cache,
        activation_mode=activation_mode,
    )
    if not default_spot_contracts:
        return merged_contracts

    existing_tokens = {
        str(contract.get('token') or '').strip()
        for contract in merged_contracts
        if str(contract.get('token') or '').strip()
    }
    for spot_contract in default_spot_contracts:
        token = str(spot_contract.get('token') or '').strip()
        if token and token not in existing_tokens:
            merged_contracts.append(spot_contract)
            existing_tokens.add(token)
    return merged_contracts


def _build_momentum_pending_contracts(
    db: MongoData,
    records: list[dict],
) -> list[dict]:
    """
    Return contract dicts for legs that are actively monitoring momentum
    (feature='momentum_pending', status='active' in algo_leg_feature_status).
    Only includes docs where strike/expiry_date/token have been resolved
    (after the first tick), so _fetch_active_leg_ticks can look up LTP data.
    These contracts are appended to active_contracts so the frontend receives
    real-time LTP updates while a lazy leg waits for its momentum trigger price.
    """
    contracts: list[dict] = []
    trade_ids = [
        str(r.get('_id') or '')
        for r in (records or [])
        if isinstance(r, dict) and r.get('_id')
    ]
    if not trade_ids or not db:
        return contracts

    trade_meta: dict[str, dict] = {
        str(r.get('_id') or ''): r for r in (records or []) if isinstance(r, dict)
    }

    try:
        feature_col = db._db['algo_leg_feature_status']
        pending_docs = list(feature_col.find({
            'trade_id': {'$in': trade_ids},
            'feature': 'momentum_pending',
            'status': 'active',
            'token': {'$nin': [None, '']},
            'strike': {'$ne': None},
            'expiry_date': {'$nin': [None, '']},
        }))
    except Exception as exc:
        log.warning('_build_momentum_pending_contracts query error: %s', exc)
        return contracts

    for doc in pending_docs:
        token = str(doc.get('token') or '').strip()
        strike = doc.get('strike')
        expiry_date = str(doc.get('expiry_date') or '')[:10]
        option = str(doc.get('option') or '')
        if not token or strike is None or not expiry_date or not option:
            continue

        tid = str(doc.get('trade_id') or '')
        rec = trade_meta.get(tid) or {}
        underlying = str(
            doc.get('underlying') or
            (rec.get('strategy') or {}).get('Ticker') or
            (rec.get('config') or {}).get('Ticker') or
            rec.get('ticker') or ''
        )
        if not underlying:
            continue

        contracts.append({
            'trade_id': tid,
            'strategy_id': str(rec.get('strategy_id') or ''),
            'strategy_name': str(rec.get('name') or ''),
            'group_name': str(rec.get('group_name') or ''),
            'leg_id': str(doc.get('leg_id') or ''),
            'token': token,
            'symbol': str(doc.get('symbol') or ''),
            'underlying': underlying,
            'expiry_date': expiry_date,
            'strike': strike,
            'option': option,
            'entry_timestamp': '',
            'is_momentum_pending': True,
        })

    return contracts


def _append_momentum_pending_to_contracts(
    active_contracts: list[dict],
    db: MongoData | None,
    records: list[dict],
) -> list[dict]:
    """
    Merge momentum-pending contracts into active_contracts (token-deduped).
    Returns the extended list (same object, mutated in-place for efficiency).
    """
    if not db or not records:
        return active_contracts
    pending = _build_momentum_pending_contracts(db, records)
    if not pending:
        return active_contracts
    existing_tokens = {str(c.get('token') or '') for c in active_contracts if c.get('token')}
    for mc in pending:
        tok = str(mc.get('token') or '')
        if tok and tok not in existing_tokens:
            active_contracts.append(mc)
            existing_tokens.add(tok)
            print(
                f'[MOMENTUM PENDING SUBSCRIBE] leg={mc.get("leg_id")} '
                f'token={tok} strike={mc.get("strike")} option={mc.get("option")} '
                f'expiry={mc.get("expiry_date")}'
            )
    return active_contracts


def _format_chain_tick_as_ltp(tick: dict) -> dict:
    """Convert a _fetch_active_leg_ticks entry to the ltp wire format.
    Token is taken from the chain_doc (e.g. 'NSE_2025110406910'), not composite key.
    """
    snapshot = tick.get('snapshot') or {}
    # Use chain_doc token stored in snapshot['token'], fallback to tick token
    token = str(snapshot.get('token') or tick.get('token') or '')
    return {
        'token': token,
        'timestamp': str(snapshot.get('timestamp') or ''),
        'ltp': _safe_float(snapshot.get('close')),
        'bb_qty': int(snapshot.get('bb_qty') or 0),
        'bb_price': _safe_float(snapshot.get('bb_price')),
        'ba_qty': int(snapshot.get('ba_qty') or 0),
        'ba_price': _safe_float(snapshot.get('ba_price')),
        'vol_in_day': int(snapshot.get('vol_in_day') or snapshot.get('oi') or 0),
        'expiry': str(snapshot.get('expiry') or tick.get('expiry_date') or ''),
        'strike': snapshot.get('strike') or tick.get('strike'),
        'option_type': str(snapshot.get('type') or tick.get('option') or ''),
    }


def _fetch_spot_ticks(
    index_spot_col,
    spot_contracts: list[dict],
    snapshot_timestamp: str,
    market_cache: dict | None = None,
    activation_mode: str | None = None,
) -> list[dict]:
    """Fetch LTP for SPOT (underlying index) contracts from option_chain_index_spot."""
    ticks: list[dict] = []
    normalized_mode = str(activation_mode or '').strip()
    if _is_strict_history_leg_mode(normalized_mode):
        try:
            from features.broker_gateway import broker_ticker_manager as ticker_manager  # type: ignore
        except Exception:
            ticker_manager = None
        for contract in spot_contracts:
            underlying = str(contract.get('underlying') or '').strip().upper()
            if not underlying or not ticker_manager:
                continue
            live_spot = _safe_float(ticker_manager.get_spot(underlying))
            if live_spot <= 0:
                continue
            ticks.append({
                'token': str(contract.get('token') or '').strip(),
                'timestamp': snapshot_timestamp,
                'ltp': live_spot,
                'bb_qty': 0,
                'bb_price': 0.0,
                'ba_qty': 0,
                'ba_price': 0.0,
                'vol_in_day': 0,
                'underlying': underlying,
                'option_type': 'SPOT',
            })
        return ticks

    for contract in spot_contracts:
        underlying = str(contract.get('underlying') or '').strip().upper()
        if not underlying:
            continue
        try:
            spot_doc = get_index_spot_at_time(
                index_spot_col,
                underlying,
                snapshot_timestamp,
                market_cache=market_cache,
                activation_mode=activation_mode,
            )
            if not spot_doc:
                continue
            token = str(spot_doc.get('token') or contract.get('token') or '').strip()
            ticks.append({
                'token': token,
                'timestamp': str(spot_doc.get('timestamp') or snapshot_timestamp),
                'ltp': _safe_float(spot_doc.get('spot_price')),
                'bb_qty': 0,
                'bb_price': 0.0,
                'ba_qty': 0,
                'ba_price': 0.0,
                'vol_in_day': 0,
                'underlying': underlying,
                'option_type': 'SPOT',
            })
        except Exception as exc:
            log.warning('_fetch_spot_ticks error underlying=%s: %s', underlying, exc)
    return ticks


VIX_TOKEN = 'NSE_00'


def _fetch_vix_ltp_item(
    db: MongoData,
    snapshot_timestamp: str,
    market_cache: dict | None = None,
) -> dict | None:
    """Return India VIX as an LTP item (token=NSE_00) for inclusion in ltp_update payloads."""
    try:
        from features.trading_core import get_vix_at_time
        vix = get_vix_at_time(db, snapshot_timestamp, market_cache)
        if vix <= 0:
            return None
        return {
            'token': VIX_TOKEN,
            'underlying': 'INDIA VIX',
            'option_type': 'SPOT',
            'ltp': vix,
            'iv': vix,
            'timestamp': snapshot_timestamp,
            'bb_qty': 0,
            'bb_price': 0.0,
            'ba_qty': 0,
            'ba_price': 0.0,
            'vol_in_day': 0,
        }
    except Exception:
        return None


def _is_original_leg(leg: dict, trade: dict) -> bool:
    leg_id = str(leg.get('id') or '')
    original_leg_configs = ((trade.get('config') or {}).get('LegConfigs') or {})
    return bool(leg_id and leg_id in original_leg_configs)


def _print_backtest_strategy_status(trade: dict, listening_time: str, market_time: str, entry_time_hhmm: str, state: str) -> None:
    trade_id = str(trade.get('_id') or '')
    strategy_name = str(trade.get('name') or '')
    group_name = str(((trade.get('portfolio') or {}).get('group_name') or 'No Group'))
    _backtest_debug_log(
        f"[BACKTEST CHECK] listening_time={listening_time} trade={trade_id} group={group_name} strategy={strategy_name} "
        f"entry_time={entry_time_hhmm or '--:--'} market_time={market_time} state={state}"
    )


def _extract_running_positions(
    db: MongoData,
    selected_date: str,
    market_time: str,
    include_position_snapshots: bool = True,
    running_trades: list[dict] | None = None,
    market_cache: dict | None = None,
    activation_mode: str = 'algo-backtest',
) -> dict:
    """
    Used by backtest simulation mode.
    Fetches open legs from algo_trades and queries option_chain by composite key.
    Also evaluates pending leg entry eligibility on every simulated minute.
    """
    extract_started_at = perf_counter()
    algo_trades_col = db._db['algo_trades']
    chain_col = db._db[OPTION_CHAIN_COLLECTION]
    index_spot_col = db._db['option_chain_index_spot']

    trade_query = _build_trade_query(
        selected_date,
        activation_mode=activation_mode,
        statuses=[BACKTEST_IMPORT_STATUS, RUNNING_STATUS],
    )

    if running_trades is None:
        trade_load_started_at = perf_counter()
        running_trades = list(algo_trades_col.find(trade_query))
        _slow_backtest_log(
            'load_running_trades',
            (perf_counter() - trade_load_started_at) * 1000,
            date=selected_date,
            total=len(running_trades),
        )
    snapshot_timestamp = _build_snapshot_timestamp(selected_date, market_time)
    listening_time = _now_iso()
    _backtest_debug_log(f"[BACKTEST LISTENING] listening_time={listening_time} snapshot={snapshot_timestamp}")
    open_positions = []
    strategy_map: dict[str, dict] = {}
    position_entry_events: list[dict] = []
    active_leg_contracts: list[dict] = []
    wait_for_spot_data = False
    blocked_snapshot = None
    market_hhmm = _extract_hhmm(market_time)
    lot_size_cache: dict[str, int] = {}
    index_spot_cache: dict[tuple[str, str], dict] = {}

    for trade_index, trade in enumerate(running_trades):
        trade_started_at = perf_counter()
        trade_id = str(trade.get('_id') or '')
        underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        all_leg_configs: dict = {}
        cfg = (trade.get('config') or {})
        all_leg_configs.update(cfg.get('LegConfigs') or {})
        all_leg_configs.update(cfg.get('IdleLegConfigs') or {})

        raw_entry_time = str(trade.get('entry_time') or '')
        entry_time_hhmm = _extract_hhmm(raw_entry_time)
        before_entry = bool(entry_time_hhmm and market_hhmm < entry_time_hhmm)
        entry_snapshot_timestamp = _build_snapshot_timestamp(selected_date, entry_time_hhmm) if entry_time_hhmm else snapshot_timestamp
        _print_backtest_strategy_status(
            trade,
            listening_time,
            snapshot_timestamp,
            entry_time_hhmm,
            'waiting_for_entry' if before_entry else 'checking_positions',
        )

        if underlying not in lot_size_cache:
            try:
                lot_size_cache[underlying] = db.get_lot_size(selected_date, underlying)
            except Exception:
                lot_size_cache[underlying] = 75
        default_lot_size = lot_size_cache[underlying]

        refreshed_trade = trade
        if not before_entry:
            created_original_legs, refreshed_trade = _queue_original_legs_if_needed(
                db, refreshed_trade, entry_snapshot_timestamp
            )
            if refreshed_trade is not trade:
                running_trades[trade_index] = refreshed_trade
            if created_original_legs:
                _backtest_debug_log(
                    f"[BACKTEST ORIGINAL LEGS] listening_time={listening_time} trade={trade_id} "
                    f"strategy={str(refreshed_trade.get('name') or '')} entry_snapshot={entry_snapshot_timestamp}"
                )
            # ── Process momentum-pending legs from algo_leg_feature_status ────
            spot_cache_key_mt = (underlying, snapshot_timestamp)
            if spot_cache_key_mt not in index_spot_cache:
                _spot_doc_mt = get_index_spot_at_time(
                    index_spot_col,
                    underlying,
                    snapshot_timestamp,
                    market_cache=market_cache,
                    activation_mode=activation_mode,
                )
                if not _spot_doc_mt.get('spot_price') and activation_mode == 'algo-backtest':
                    from features.algo_backtest_event import get_spot_price_from_chain_col as _gspfc
                    _fb_spot = _gspfc(chain_col, underlying, snapshot_timestamp)
                    if _fb_spot > 0:
                        _spot_doc_mt = {'underlying': underlying, 'spot_price': _fb_spot, 'timestamp': snapshot_timestamp, 'source': 'chain_col_fallback'}
                index_spot_cache[spot_cache_key_mt] = _spot_doc_mt
            _entered_mt = _process_momentum_pending_feature_legs(
                db, refreshed_trade, chain_col, selected_date, snapshot_timestamp,
                default_lot_size,
                index_spot_doc=index_spot_cache[spot_cache_key_mt],
                market_cache=market_cache,
            )
            if _entered_mt:
                refreshed_trade = algo_trades_col.find_one({'_id': trade_id}) or refreshed_trade
                running_trades[trade_index] = refreshed_trade
                _backtest_debug_log(
                    f"[BACKTEST MOMENTUM ENTERED] trade={trade_id} legs={_entered_mt} snapshot={snapshot_timestamp}"
                )
        refreshed_legs = [l for l in (refreshed_trade.get('legs') or []) if isinstance(l, dict)]
        _entry_live_chain_cache: dict = {}
        for leg_index, leg in enumerate(refreshed_legs):
            leg_started_at = perf_counter()
            if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
                continue

            is_pending = bool(leg.get('is_lazy') or leg.get('entry_trade') is None)
            if is_pending:
                if before_entry:
                    _backtest_debug_log(
                        f"[BACKTEST ENTRY WAIT] listening_time={listening_time} trade={trade_id} leg={str(leg.get('id') or '')} "
                        f"entry_time={entry_time_hhmm or '--:--'} market_time={snapshot_timestamp}"
                    )
                    continue
                # algo-backtest: always use entry_snapshot_timestamp (strategy entry_time)
                # so the spot lookup uses the correct candle regardless of leg_id format.
                # live/fast-forward: keep _is_original_leg check (uses Kite socket spot anyway).
                if activation_mode == 'algo-backtest':
                    entry_lookup_snapshot = entry_snapshot_timestamp
                else:
                    entry_lookup_snapshot = (
                        entry_snapshot_timestamp
                        if _is_original_leg(leg, refreshed_trade)
                        else snapshot_timestamp
                    )
                spot_cache_key = (underlying, entry_lookup_snapshot)
                if spot_cache_key not in index_spot_cache:
                    _spot_doc_entry = get_index_spot_at_time(
                        index_spot_col,
                        underlying,
                        entry_lookup_snapshot,
                        market_cache=market_cache,
                        activation_mode=activation_mode,
                    )
                    if not _spot_doc_entry.get('spot_price') and activation_mode == 'algo-backtest':
                        from features.algo_backtest_event import get_spot_price_from_chain_col as _gspfc2
                        _fb_spot2 = _gspfc2(chain_col, underlying, entry_lookup_snapshot)
                        if _fb_spot2 > 0:
                            _spot_doc_entry = {'underlying': underlying, 'spot_price': _fb_spot2, 'timestamp': entry_lookup_snapshot, 'source': 'chain_col_fallback'}
                    index_spot_cache[spot_cache_key] = _spot_doc_entry
                lot_size = _safe_int(leg.get('quantity')) or default_lot_size
                entered, wait_reason = _try_enter_pending_leg(
                    db, refreshed_trade, leg, leg_index, chain_col, selected_date,
                    entry_lookup_snapshot, lot_size, snapshot_timestamp=entry_lookup_snapshot,
                    index_spot_doc=index_spot_cache[spot_cache_key], market_cache=market_cache,
                    live_chain_cache=_entry_live_chain_cache,
                )
                if wait_reason == 'spot_missing':
                    wait_for_spot_data = True
                    blocked_snapshot = entry_lookup_snapshot
                    _backtest_debug_log(
                        f"[BACKTEST SPOT WAIT] listening_time={listening_time} trade={trade_id} "
                        f"leg={str(leg.get('id') or '')} wait_snapshot={entry_lookup_snapshot}"
                    )
                    continue
                if not entered:
                    _slow_backtest_log(
                        'pending_leg_not_entered',
                        (perf_counter() - leg_started_at) * 1000,
                        trade_id=trade_id,
                        leg_id=str(leg.get('id') or ''),
                    )
                    _backtest_debug_log(
                        f"[BACKTEST ENTRY MISS] listening_time={listening_time} trade={trade_id} leg={str(leg.get('id') or '')} "
                        f"entry_time={entry_time_hhmm or '--:--'} lookup_snapshot={entry_lookup_snapshot} market_time={snapshot_timestamp}"
                    )
                    continue
                _backtest_debug_log(
                    f"[BACKTEST ENTRY OPENED] listening_time={listening_time} trade={trade_id} leg={str(leg.get('id') or '')} "
                    f"entry_time={entry_time_hhmm or '--:--'} lookup_snapshot={entry_lookup_snapshot} market_time={snapshot_timestamp}"
                )
                refreshed_trade = algo_trades_col.find_one({'_id': trade_id}) or refreshed_trade
                running_trades[trade_index] = refreshed_trade
                refreshed_legs = [l for l in (refreshed_trade.get('legs') or []) if isinstance(l, dict)]
                _leg_id_to_find = str(leg.get('id') or '')
                leg = next((l for l in refreshed_legs if str(l.get('id') or '') == _leg_id_to_find), None)
                if leg is None:
                    continue

            entry_trade = leg.get('entry_trade') or {}
            if not entry_trade:
                _backtest_debug_log(
                    f"[BACKTEST ENTRY EMPTY] listening_time={listening_time} trade={trade_id} leg={str(leg.get('id') or '')} "
                    f"entry_time={entry_time_hhmm or '--:--'} market_time={snapshot_timestamp}"
                )
                continue

            inserted, history_doc = _store_position_history(db, refreshed_trade, leg)
            if inserted and history_doc:
                position_entry_events.append({
                    'trade_id': history_doc['trade_id'],
                    'leg_id': history_doc['leg_id'],
                    'entry_timestamp': history_doc['entry_timestamp'],
                    'message': 'Position entry stored successfully',
                    'history': history_doc,
                })

            active_contract = _build_active_leg_contract(leg, refreshed_trade)
            if active_contract:
                active_leg_contracts.append(active_contract)

            if not include_position_snapshots:
                continue

            entry_price = _safe_float(entry_trade.get('price'))
            quantity = _safe_int(leg.get('quantity') or entry_trade.get('quantity'))
            lot_size = _safe_int(leg.get('lot_size'), 1)
            effective_quantity = max(0, quantity) * max(1, lot_size)
            strike = leg.get('strike')
            expiry_date = str(leg.get('expiry_date') or '')
            option_type = str(leg.get('option') or '')
            position_str = str(leg.get('position') or '')
            is_sell_pos = _is_sell(position_str)

            chain_doc = get_chain_at_time(
                chain_col,
                underlying,
                expiry_date,
                strike,
                option_type,
                snapshot_timestamp,
                market_cache=market_cache,
                activation_mode=activation_mode,
            )
            current_price = _safe_float(chain_doc.get('close'), leg.get('last_saw_price', 0.0))
            pnl = (
                (entry_price - current_price) * effective_quantity
                if is_sell_pos else
                (current_price - entry_price) * effective_quantity
            )

            token = make_token(underlying, expiry_date, strike, option_type)
            leg_id = str(leg.get('id') or '')
            leg_cfg = all_leg_configs.get(leg_id) or {}
            entry_timestamp = str(entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or '')

            _backtest_debug_log(f"[BACKTEST POSITION] listening_time={listening_time} trade={trade_id} leg={leg_id} entry_time={entry_timestamp} market_time={snapshot_timestamp}")

            position_payload = {
                'strategy_id': str(refreshed_trade.get('strategy_id') or ''),
                'strategy_name': str(refreshed_trade.get('name') or ''),
                'trade_id': trade_id,
                'leg_id': leg_id,
                'symbol': str(leg.get('symbol') or ''),
                'token': token,
                'position': position_str,
                'option': option_type,
                'strike': strike,
                'expiry_date': expiry_date,
                'entry_price': entry_price,
                'current_price': current_price,
                'quantity': effective_quantity,
                'lots': quantity,
                'lot_size': lot_size,
                'pnl': round(pnl, 2),
                'entry_timestamp': entry_timestamp,
                'sl_price': calc_sl_price(entry_price, is_sell_pos, leg_cfg.get('LegStopLoss') or {}),
                'tp_price': calc_tp_price(entry_price, is_sell_pos, leg_cfg.get('LegTarget') or {}),
                'snapshot': {
                    'timestamp': chain_doc.get('timestamp') or snapshot_timestamp,
                    'underlying': chain_doc.get('underlying') or underlying,
                    'expiry': chain_doc.get('expiry') or expiry_date,
                    'strike': chain_doc.get('strike', strike),
                    'type': chain_doc.get('type') or option_type,
                    'close': current_price,
                    'oi': chain_doc.get('oi'),
                    'iv': chain_doc.get('iv'),
                    'delta': chain_doc.get('delta'),
                    'gamma': chain_doc.get('gamma'),
                    'theta': chain_doc.get('theta'),
                    'vega': chain_doc.get('vega'),
                    'rho': chain_doc.get('rho'),
                    'spot_price': chain_doc.get('spot_price'),
                },
            }
            open_positions.append(position_payload)

            strat_entry = strategy_map.setdefault(trade_id, {
                'trade_id': trade_id,
                'strategy_id': str(refreshed_trade.get('strategy_id') or ''),
                'strategy_name': str(refreshed_trade.get('name') or ''),
                'entry_time': entry_time_hhmm,
                'open_positions': [],
                'total_pnl': 0.0,
            })
            strat_entry['open_positions'].append(position_payload)
            strat_entry['total_pnl'] = round(strat_entry['total_pnl'] + position_payload['pnl'], 2)
            _slow_backtest_log(
                'process_leg',
                (perf_counter() - leg_started_at) * 1000,
                trade_id=trade_id,
                leg_id=leg_id,
            )

        _slow_backtest_log(
            'process_trade',
            (perf_counter() - trade_started_at) * 1000,
            trade_id=trade_id,
            underlying=underlying,
        )

    result = {
        'snapshot_timestamp': snapshot_timestamp,
        'running_strategies': list(strategy_map.values()),
        'open_positions': open_positions,
        'active_leg_contracts': active_leg_contracts,
        'active_leg_tokens': [contract.get('token') for contract in active_leg_contracts if contract.get('token')],
        'active_leg_ticks': _fetch_active_leg_ticks(
            chain_col,
            active_leg_contracts,
            snapshot_timestamp,
            market_cache=market_cache,
            activation_mode=activation_mode,
        ),
        'position_entry_events': position_entry_events,
        'wait_for_spot_data': wait_for_spot_data,
        'blocked_snapshot': blocked_snapshot,
    }
    _slow_backtest_log(
        'extract_running_positions_total',
        (perf_counter() - extract_started_at) * 1000,
        date=selected_date,
        market_time=market_time,
        total_running=len(running_trades),
        total_open=len(open_positions),
    )
    return result


# ─── Per-trade backtest event processor ───────────────────────────────────────

def _process_backtest_trade_tick(
    db: MongoData,
    trade: dict,
    legs: list[dict],
    ltp_map: dict[str, float],
    now_ts: str,
    now_time: str,
    chain_col,
    market_cache: dict | None,
    past_exit: bool,
    all_leg_configs: dict,
    underlying: str,
    trade_date: str,
    activation_mode: str = 'algo-backtest',
) -> dict:
    """
    ONE function that runs all backtest events for a single trade.

    Data access (spot / LTP) is delegated exclusively to algo_backtest_event.
    All trading logic — SL, TP, trail SL, overall SL/MTM, re-entry,
    re-execute, recost, lock-and-trail — lives here, not in the data layer.

    Parameters
    ----------
    ltp_map : pre-fetched {leg_id: ltp} from algo_backtest_event.get_open_legs_ltp_array

    Returns
    -------
    dict with keys: open_positions, strategy_entry, actions_taken,
                    hit_trade_id, hit_ltp_snapshot, trade_mtm
    """
    if SHOW_PRINT_STATEMENT:
        print("----------------------_process_backtest_trade_tick inside ---------------")
    trade_id     = str(trade.get('_id') or '')
    cfg          = trade.get('config') or {}

    open_positions: list[dict] = []
    actions_taken:  list[str]  = []
    strategy_entry: dict | None = None
    hit_trade_id:   str | None  = None
    hit_ltp_snapshot: list[dict] = []
    trade_mtm: float = 0.0

    raw_exit_time  = str(trade.get('exit_time') or '')
    exit_time_hhmm = raw_exit_time[11:16] if len(raw_exit_time) >= 16 else raw_exit_time[:5]

    from features.notification_manager import (
        record_sl_hit, record_target_hit,
        record_trail_sl_changed, record_reentry_queued, record_force_exit,
        trigger_leg_feature, disable_leg_features, rotate_trail_sl_record,
    )

    # ── Per-leg event checks ───────────────────────────────────────────────────
    for leg_index, leg in enumerate(legs):
        leg_status = int(leg.get('status') or 0)
        if leg_status != OPEN_LEG_STATUS:
            continue
        entry_trade = leg.get('entry_trade') or {}
        if not entry_trade:
            continue  # pending leg, not yet entered

        leg_id      = str(leg.get('id') or '')
        leg_cfg     = _resolve_leg_cfg(leg_id, leg, all_leg_configs)
        entry_price = _safe_float(entry_trade.get('price'))
        quantity    = _safe_int(leg.get('quantity') or entry_trade.get('quantity'))
        lot_size    = _safe_int(leg.get('lot_size'), 1)
        effective_quantity = max(0, quantity) * max(1, lot_size)
        strike      = leg.get('strike')
        expiry_date = str(leg.get('expiry_date') or '')
        option_type = str(leg.get('option') or '')
        position_str = str(leg.get('position') or '')
        is_sell_pos  = _is_sell(position_str)

        # ── Force-exit at exit_time ────────────────────────────────────────
        if past_exit:
            _exit_strike_int = int(float(str(strike or 0))) if strike is not None else 0
            _expiry_date_10  = expiry_date[:10]
            _opt_upper       = option_type.upper()

            # Step 1: resolve correct Dhan token and subscribe BEFORE fetching price
            leg_token_str   = str(leg.get('token') or '')
            _exit_dhan_token = leg_token_str
            _exit_ws_seg     = str(leg.get('ws_segment') or 'NSE_FNO')
            try:
                from features.broker_gateway import _active_broker  # type: ignore
                if _active_broker() == 'dhan':
                    _s_int = int(float(str(strike))) if strike is not None else None
                    _sf = {'$in': [_s_int, float(_s_int)]} if _s_int is not None else strike
                    _at = db._db['active_option_tokens'].find_one(
                        {
                            'broker': 'dhan',
                            'instrument': {'$regex': f'^{underlying}$', '$options': 'i'},
                            'strike': _sf,
                            'option_type': {'$regex': f'^{_opt_upper}$', '$options': 'i'},
                            'expiry': {'$regex': f'^{_expiry_date_10}'},
                        },
                        {'token': 1, 'tokens': 1, 'ws_segment': 1},
                    )
                    if _at:
                        _tok = str(_at.get('token') or _at.get('tokens') or '').strip()
                        if _tok:
                            _exit_dhan_token = _tok
                            _exit_ws_seg = str(_at.get('ws_segment') or _exit_ws_seg)
                    # Subscribe token now so Dhan WS has it registered before price fetch
                    from features.dhan_ticker import dhan_ticker_manager as _dtm  # type: ignore
                    _dtm.subscribe_tokens([_exit_dhan_token], exchange=_exit_ws_seg)
                    # Patch leg token so _resolve_quote_first_option_price uses correct token
                    if _exit_dhan_token and _exit_dhan_token != leg_token_str:
                        leg = dict(leg)
                        leg['token']      = _exit_dhan_token
                        leg['ws_segment'] = _exit_ws_seg
                        leg_token_str     = _exit_dhan_token
            except Exception:
                pass

            # Step 2: fetch exit price using correct token
            base_exit_price = ltp_map.get(leg_id) or _safe_float(leg.get('last_saw_price'))
            exit_price = _resolve_quote_first_option_price(
                db, trade, leg, activation_mode=activation_mode, fallback_price=base_exit_price
            )

            # Step 3: get IV from full live option chain (CE/PE IV per strike)
            _bt_exit_iv: float | None = None
            try:
                from features.live_option_chain import fetch_full_chain  # type: ignore
                _iv_chain = fetch_full_chain(db, underlying, _expiry_date_10, 0, leg_id)
                for _c in (_iv_chain.get(_opt_upper) or []):
                    if int(float(str(_c.get('strike') or 0))) == _exit_strike_int:
                        _bt_exit_iv = _safe_float(_c.get('iv')) or None
                        break
            except Exception:
                pass
            # Fallback to historical chain IV (backtest mode or if live chain unavailable)
            if _bt_exit_iv is None:
                _bt_exit_chain = get_chain_at_time(chain_col, underlying, expiry_date, strike, option_type, now_ts, market_cache=market_cache) or {}
                _bt_exit_iv = _safe_float(_bt_exit_chain.get('iv')) or None

            _bt_exit_vix: float | None = None
            try:
                from features.trading_core import get_vix_at_time as _get_vix_bt  # type: ignore
                _v = _get_vix_bt(db, now_ts, market_cache)
                _bt_exit_vix = _v if _v > 0 else None
            except Exception:
                pass
            print(
                f'[EXIT TIME] trade={trade_id} leg={leg_id} token={leg_token_str} '
                f'exit_price={exit_price} exit_iv={_bt_exit_iv} mode={activation_mode}'
            )
            close_leg_in_db(db, trade_id, leg_index, exit_price, 'exit_time', now_ts, leg_id=leg_id, activation_mode=activation_mode, exit_iv=_bt_exit_iv, exit_vix=_bt_exit_vix)
            actions_taken.append(f'{trade_id}/{leg_id}: force-exit at {exit_price} (exit_time)')
            try:
                record_force_exit(db._db, trade, leg, now_ts, exit_price, exit_reason='exit_time')
                disable_leg_features(db._db, trade_id, leg_id, reason='force_exit', timestamp=now_ts)
            except Exception as _e:
                log.warning('bt notification force_exit error: %s', _e)
            continue

        # ── Get LTP from pre-fetched map (via algo_backtest_event) ────────
        current_price = ltp_map.get(leg_id) or _safe_float(leg.get('last_saw_price'))
        current_price = _resolve_quote_first_option_price(
            db, trade, leg, activation_mode=activation_mode, fallback_price=current_price
        )
        if current_price <= 0:
            continue

        _bt_sl_tp_chain = get_chain_at_time(chain_col, underlying, expiry_date, strike, option_type, now_ts, market_cache=market_cache) or {}
        _bt_sl_tp_iv  = _safe_float(_bt_sl_tp_chain.get('iv')) or None
        _bt_sl_tp_vix: float | None = None
        try:
            from features.trading_core import get_vix_at_time as _get_vix_sltp  # type: ignore
            _v = _get_vix_sltp(db, now_ts, market_cache)
            _bt_sl_tp_vix = _v if _v > 0 else None
        except Exception:
            pass

        sl_config    = leg_cfg.get('LegStopLoss') or {}
        tp_config    = leg_cfg.get('LegTarget') or {}
        trail_config = get_trail_config(leg_cfg)

        initial_sl  = _safe_float(leg.get('initial_sl_value')) or calc_sl_price(entry_price, is_sell_pos, sl_config)
        stored_sl   = _safe_float(leg.get('current_sl_price')) or None
        sl_price    = stored_sl or initial_sl
        if sl_price and trail_config:
            sl_price = update_trail_sl(entry_price, current_price, sl_price, is_sell_pos, trail_config, initial_sl=initial_sl)

        tp_price = calc_tp_price(entry_price, is_sell_pos, tp_config)
        pnl      = (
            (entry_price - current_price) * effective_quantity
            if is_sell_pos else
            (current_price - entry_price) * effective_quantity
        )

        # ── SL hit ────────────────────────────────────────────────────────
        if is_sl_hit(current_price, sl_price, is_sell_pos):
            _sl_exit_price = current_price if _is_market_order(leg_cfg, 'exit') else sl_price
            close_leg_in_db(db, trade_id, leg_index, _sl_exit_price, 'stoploss', now_ts, leg_id=leg_id, activation_mode=activation_mode, exit_iv=_bt_sl_tp_iv, exit_vix=_bt_sl_tp_vix)
            actions_taken.append(f'{trade_id}/{leg_id}: SL hit @ {_sl_exit_price}')
            record_sl_hit(db._db, trade, leg, now_ts, _sl_exit_price, sl_price or 0.0)
            trigger_leg_feature(db._db, trade_id, leg_id, 'sl', current_price, now_ts)
            disable_leg_features(db._db, trade_id, leg_id, except_feature='sl', reason='sl_triggered', timestamp=now_ts)
            mark_strategy_activity(trade, 'sl_hit', {
                'leg_id': leg_id, 'option': option_type, 'position': position_str,
                'sl_price': round(sl_price or 0.0, 2), 'exit_price': round(current_price, 2),
            })
            reentry_cfg = get_reentry_sl_config(leg_cfg)
            if reentry_cfg:
                leg_cfg_with_id = dict(leg_cfg, id=leg_id)
                result = _handle_reentry(db, trade, leg_cfg_with_id, reentry_cfg, leg_id, now_ts)
                if result:
                    actions_taken.append(f'{trade_id}/{leg_id}: {result}')
                    re_type = str(reentry_cfg.get('Type') or '')
                    re_kind = ('lazy' if 'NextLeg' in re_type else
                               'immediate' if 'Immediate' in re_type else
                               'at_cost' if 'AtCost' in re_type else 'like_original')
                    new_leg_id = str((reentry_cfg.get('Value') or {}).get('NextLegRef') or '')
                    record_reentry_queued(db._db, trade, now_ts, leg_id, re_kind, new_leg_id, re_type, reason='sl')
                    mark_strategy_activity(trade, 'reentry', {
                        'leg_id': leg_id, 'reason': 'sl', 'reentry_type': re_type,
                        'reentry_kind': re_kind, 'new_leg_id': new_leg_id,
                    })
                    if 'AtCost' in re_type:
                        try:
                            from features.notification_manager import upsert_recost_feature_status
                            _cost_price = _safe_float((leg.get('entry_trade') or {}).get('price'))
                            _reentry_id = f'{leg_id}_re_{now_ts.replace(":", "").replace("T", "")}'
                            upsert_recost_feature_status(
                                db._db, trade, leg, now_ts,
                                cost_price=_cost_price, exit_price=current_price,
                                status='pending', recost_leg_id=_reentry_id,
                            )
                        except Exception as _rce:
                            log.warning('recost feature status armed error leg=%s: %s', leg_id, _rce)
            continue

        # ── TP hit ────────────────────────────────────────────────────────
        if is_tp_hit(current_price, tp_price, is_sell_pos):
            _tp_exit_price = current_price if _is_market_order(leg_cfg, 'exit') else tp_price
            close_leg_in_db(db, trade_id, leg_index, _tp_exit_price, 'target', now_ts, leg_id=leg_id, activation_mode=activation_mode, exit_iv=_bt_sl_tp_iv, exit_vix=_bt_sl_tp_vix)
            actions_taken.append(f'{trade_id}/{leg_id}: TP hit @ {_tp_exit_price}')
            record_target_hit(db._db, trade, leg, now_ts, _tp_exit_price, tp_price or 0.0)
            trigger_leg_feature(db._db, trade_id, leg_id, 'target', _tp_exit_price, now_ts)
            disable_leg_features(db._db, trade_id, leg_id, except_feature='target', reason='target_triggered', timestamp=now_ts)
            mark_strategy_activity(trade, 'tp_hit', {
                'leg_id': leg_id, 'option': option_type, 'position': position_str,
                'tp_price': round(tp_price or 0.0, 2), 'exit_price': round(current_price, 2),
            })
            reentry_cfg = get_reentry_tp_config(leg_cfg)
            if reentry_cfg:
                leg_cfg_with_id = dict(leg_cfg, id=leg_id)
                result = _handle_reentry(db, trade, leg_cfg_with_id, reentry_cfg, leg_id, now_ts)
                if result:
                    actions_taken.append(f'{trade_id}/{leg_id}: {result}')
                    re_type = str(reentry_cfg.get('Type') or '')
                    re_kind = ('lazy' if 'NextLeg' in re_type else
                               'immediate' if 'Immediate' in re_type else
                               'at_cost' if 'AtCost' in re_type else 'like_original')
                    new_leg_id = str((reentry_cfg.get('Value') or {}).get('NextLegRef') or '')
                    record_reentry_queued(db._db, trade, now_ts, leg_id, re_kind, new_leg_id, re_type, reason='target')
                    mark_strategy_activity(trade, 'reentry', {
                        'leg_id': leg_id, 'reason': 'target', 'reentry_type': re_type,
                        'reentry_kind': re_kind, 'new_leg_id': new_leg_id,
                    })
                    if 'AtCost' in re_type:
                        try:
                            from features.notification_manager import upsert_recost_feature_status
                            _cost_price = _safe_float((leg.get('entry_trade') or {}).get('price'))
                            _reentry_id = f'{leg_id}_re_{now_ts.replace(":", "").replace("T", "")}'
                            upsert_recost_feature_status(
                                db._db, trade, leg, now_ts,
                                cost_price=_cost_price, exit_price=current_price,
                                status='pending', recost_leg_id=_reentry_id,
                            )
                        except Exception as _rce:
                            log.warning('recost feature status armed error leg=%s: %s', leg_id, _rce)
            continue

        # ── Trail SL moved ────────────────────────────────────────────────
        if sl_price and sl_price != stored_sl:
            update_leg_sl_in_db(db, trade_id, leg_index, sl_price, current_price, leg_id=leg_id)
            if stored_sl and sl_price != stored_sl:
                print(
                    f'[TRAIL SL CHANGED] trade={trade_id} leg={leg_id} '
                    f'old_sl={stored_sl} new_sl={sl_price} ltp={current_price} ts={now_ts}'
                )
                record_trail_sl_changed(db._db, trade, leg, now_ts, stored_sl, sl_price, current_price, trail_config=trail_config)
                rotate_trail_sl_record(
                    db._db, trade_id, leg_id,
                    old_sl_price=stored_sl, new_sl_price=sl_price,
                    current_option_price=current_price, trail_config=trail_config, timestamp=now_ts,
                )
                mark_strategy_activity(trade, 'trail_sl', {
                    'leg_id': leg_id, 'option': option_type, 'position': position_str,
                    'old_sl': round(stored_sl, 2), 'new_sl': round(sl_price, 2),
                    'current_price': round(current_price, 2),
                })
                # Simulator: update SL order
                try:
                    from features.live_simulator_order import is_simulator_order_enabled, update_trail_sl_order  # type: ignore
                    if is_simulator_order_enabled(trade):
                        update_trail_sl_order(db, trade_id, leg_id, sl_price, current_price, updated_at=now_ts)
                except Exception as _tsl_e:
                    log.warning('simulator_order trail_sl error trade=%s leg=%s: %s', trade_id, leg_id, _tsl_e)
                # Live broker: modify the broker SL order to reflect new trail SL price
                if activation_mode == 'live':
                    try:
                        from features.live_order_manager import modify_broker_sl_order
                        modify_broker_sl_order(db, trade_id, leg_id, sl_price)
                    except Exception as _tsl_mod_e:
                        log.warning('live trail SL broker modify error trade=%s leg=%s: %s', trade_id, leg_id, _tsl_mod_e)

        # ── Position snapshot for MTM ─────────────────────────────────────
        if strategy_entry is None:
            strategy_entry = {
                'trade_id': trade_id,
                'strategy_id': str(trade.get('strategy_id') or ''),
                'strategy_name': str(trade.get('name') or ''),
                'ticker': underlying,
                'open_positions': [],
                'total_pnl': 0.0,
            }
        pos_payload = {
            'leg_id': leg_id, 'pnl': round(pnl, 2),
            'current_price': current_price, 'ltp': current_price,
            'entry_price': entry_price, 'quantity': effective_quantity,
            'lots': quantity, 'lot_size': lot_size,
        }
        strategy_entry['open_positions'].append(pos_payload)
        strategy_entry['total_pnl'] = round(strategy_entry['total_pnl'] + pnl, 2)
        open_positions.append(pos_payload)

    # ── Overall SL / Target / Trail / Lock checks ──────────────────────────────
    if not past_exit:
        strategy_cfg        = trade.get('strategy') or cfg
        osl_type, osl_val   = parse_overall_sl(strategy_cfg)
        otgt_type, otgt_val = parse_overall_tgt(strategy_cfg)
        current_trade_mtm, legs_pnl_snapshot = _compute_strategy_mtm_snapshot(
            db, trade_id, now_ts,
            (strategy_entry or {}).get('open_positions', []),
        )
        trade_mtm    = current_trade_mtm
        open_leg_ids = {p['leg_id'] for p in (strategy_entry or {}).get('open_positions', [])}

        # Helper: close all remaining open legs at current LTP
        def _close_remaining(exit_reason: str) -> None:
            for _leg in legs:
                if str(_leg.get('id') or '') not in open_leg_ids:
                    continue
                if int(_leg.get('status') or 0) != OPEN_LEG_STATUS:
                    continue
                _lid = str(_leg.get('id') or '')
                _idx = next((i for i, l in enumerate(legs) if str(l.get('id') or '') == _lid), 0)
                # Use pre-fetched LTP; call algo_backtest_event only if missing
                _ep = ltp_map.get(_lid) or get_option_ltp(
                    chain_col, underlying, str(_leg.get('expiry_date') or ''),
                    _leg.get('strike'), str(_leg.get('option') or ''), now_ts,
                    market_cache=market_cache, fallback=_safe_float(_leg.get('last_saw_price')),
                    activation_mode=activation_mode,
                )
                _ep = _resolve_quote_first_option_price(
                    db, trade, _leg, activation_mode=activation_mode, fallback_price=_ep
                )
                close_leg_in_db(db, trade_id, _idx, _ep, exit_reason, now_ts, leg_id=_lid, activation_mode=activation_mode)
                try:
                    from features.notification_manager import record_force_exit, disable_leg_features
                    record_force_exit(db._db, trade, _leg, now_ts, _ep, exit_reason=exit_reason)
                    disable_leg_features(db._db, trade_id, _lid, reason=exit_reason, timestamp=now_ts)
                except Exception as _e:
                    log.warning('bt notification force_exit(%s) error: %s', exit_reason, _e)

        _peak_mtm  = _safe_float(trade.get('peak_mtm'), current_trade_mtm)
        _new_peak  = max(_peak_mtm, current_trade_mtm)
        _ore_type, _ore_count = parse_overall_reentry_sl(strategy_cfg)
        _ort_type, _ort_count = parse_overall_reentry_tgt(strategy_cfg)
        _ore_done  = int(trade.get('overall_sl_reentry_done') or 0)
        _ort_done  = int(trade.get('overall_tgt_reentry_done') or 0)
        _effective_sl  = _resolve_overall_cycle_value(osl_val, _ore_done)
        _effective_tgt = _resolve_overall_cycle_value(otgt_val, _ort_done)
        _dyn_sl    = _safe_float(trade.get('current_overall_sl_threshold'), _effective_sl or osl_val)
        _last_overall_event_at = str(trade.get('last_overall_event_at') or '').strip().replace(' ', 'T')[:19]
        _last_overall_event_reason = str(trade.get('last_overall_event_reason') or '').strip()
        _skip_same_tick_overall = bool(
            open_leg_ids
            and _last_overall_event_at
            and _last_overall_event_at == str(now_ts or '').strip()[:19]
            and _last_overall_event_reason in {'overall_sl', 'overall_target'}
        )
        _sync_overall_strategy_feature_status(
            db, trade, now_ts,
            current_mtm=current_trade_mtm,
            overall_sl_done=_ore_done,
            overall_tgt_done=_ort_done,
        )
        if SHOW_PRINT_STATEMENT:
            print('[OVERALL CHECK]', {
                'mode': 'backtest', 'trade_id': trade_id,
                'strategy_name': str(trade.get('name') or ''), 'timestamp': now_ts,
                'current_mtm': round(current_trade_mtm, 2),
                'overall_sl_type': osl_type, 'overall_sl_base': round(_safe_float(osl_val), 2),
                'overall_sl_current': _effective_sl, 'overall_sl_reentry_type': _ore_type,
                'overall_sl_reentry_done': _ore_done, 'overall_sl_reentry_count': _ore_count,
                'overall_tgt_type': otgt_type, 'overall_tgt_base': round(_safe_float(otgt_val), 2),
                'overall_tgt_current': _effective_tgt, 'overall_tgt_reentry_type': _ort_type,
                'overall_tgt_reentry_done': _ort_done, 'overall_tgt_reentry_count': _ort_count,
                'dynamic_sl_threshold': round(_safe_float(_dyn_sl), 2),
            })
            print('[OVERALL SL DEBUG]', {
                'mode': 'backtest', 'trade_id': trade_id,
                'strategy_name': str(trade.get('name') or ''), 'timestamp': now_ts,
                'current_mtm': round(current_trade_mtm, 2),
                'base_overall_sl': round(_safe_float(osl_val), 2),
                'reentry_done': _ore_done, 'cycle_number': _ore_done + 1,
                'cycle_overall_sl': round(_safe_float(_effective_sl), 2),
                'dynamic_sl_threshold': round(_safe_float(_dyn_sl), 2),
                'checked_stoploss': round(_safe_float(_dyn_sl or _effective_sl), 2),
                'would_hit_cycle_sl': bool(_safe_float(_effective_sl) > 0 and current_trade_mtm <= -_safe_float(_effective_sl)),
                'would_hit_dynamic_sl': bool(_dyn_sl > 0 and current_trade_mtm <= -_dyn_sl),
                'skip_same_tick_overall': _skip_same_tick_overall,
                'last_overall_event_at': _last_overall_event_at,
                'last_overall_event_reason': _last_overall_event_reason,
            })

        if _skip_same_tick_overall:
            actions_taken.append(f'{trade_id}: skipped same-tick overall recheck after {_last_overall_event_reason}')
            return {
                'open_positions': open_positions, 'strategy_entry': strategy_entry,
                'actions_taken': actions_taken, 'hit_trade_id': hit_trade_id,
                'hit_ltp_snapshot': hit_ltp_snapshot, 'trade_mtm': trade_mtm,
            }

        _osl_hit = (
            osl_type != 'None' and _safe_float(_effective_sl) > 0
            and current_trade_mtm <= -_safe_float(_effective_sl)
        ) or (_dyn_sl > 0 and current_trade_mtm <= -_dyn_sl)
        if _osl_hit and not open_leg_ids:
            _osl_hit = False

        # ── Overall SL hit ────────────────────────────────────────────────
        if _osl_hit:
            db._db['algo_trades'].update_one(
                {'_id': trade_id},
                {'$set': {'last_overall_event_at': now_ts, 'last_overall_event_reason': 'overall_sl'}},
            )
            mark_execute_order_dirty_from_trade(trade)
            _set_overall_feature_status_state(db, trade_id, 'overall_sl', enabled=False, status='triggered', timestamp=now_ts, current_mtm=current_trade_mtm, disabled_reason='overall_sl_hit')
            _set_overall_feature_status_state(db, trade_id, 'overall_target', enabled=False, status='disabled', timestamp=now_ts, current_mtm=current_trade_mtm, disabled_reason='overall_sl_hit')
            _disable_trade_feature_rows_for_new_cycle(db, trade_id, reason='overall_sl_cycle_completed', timestamp=now_ts)
            try:
                from features.notification_manager import disable_all_trade_notifications
                disable_all_trade_notifications(db._db, trade_id, reason='overall_sl', timestamp=now_ts)
            except Exception as _e:
                log.warning('bt disable_all_trade_notifications overall_sl error: %s', _e)
            _close_remaining('overall_sl')
            actions_taken.append(f'{trade_id}: overall SL hit mtm={current_trade_mtm}')
            hit_trade_id    = trade_id
            hit_ltp_snapshot = list((strategy_entry or {}).get('open_positions') or [])
            mark_strategy_activity(trade, 'overall_sl', {
                'sl_type': osl_type, 'sl_value': round(_safe_float(_effective_sl), 2),
                'current_mtm': round(current_trade_mtm, 2), 'cycle': _ore_done + 1,
            })
            try:
                from features.notification_manager import record_overall_sl_hit
                record_overall_sl_hit(db._db, trade, now_ts, osl_type, _effective_sl, current_trade_mtm, legs_pnl_snapshot, cycle_number=_ore_done + 1, configured_reentry_count=_ore_count)
            except Exception as _e:
                log.warning('bt notification overall_sl_hit error: %s', _e)
            if _ore_type != 'None' and _ore_count > 0 and _ore_done < _ore_count:
                _next_cycle_sl = _resolve_overall_cycle_value(osl_val, _ore_done + 1)
                db._db['algo_trades'].update_one({'_id': trade_id}, {'$set': {'overall_sl_reentry_done': _ore_done + 1, 'peak_mtm': 0.0, 'current_overall_sl_threshold': _next_cycle_sl, 'last_overall_event_at': now_ts, 'last_overall_event_reason': 'overall_sl'}})
                mark_execute_order_dirty_from_trade(trade)
                _reentry_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or trade
                print('[OVERALL REENTRY PREPARE]', {'mode': 'backtest', 'trade_id': trade_id, 'reason': 'overall_sl', 'timestamp': now_ts, 'base_overall_sl': round(_safe_float(osl_val), 2), 'updated_reentry_done': _ore_done + 1, 'updated_cycle_number': _ore_done + 2, 'updated_overall_sl': round(_safe_float(_next_cycle_sl), 2), 'reentry_type': _ore_type})
                _new_ids = _requeue_original_legs_bt(db, _reentry_trade, trade_id, strategy_cfg, now_ts, reentry_type=_ore_type)
                if _new_ids:
                    _reentry_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or _reentry_trade
                    _reentry_entries = _execute_backtest_entries(db, [_serialize_trade_record(_reentry_trade)], now_time, now_ts, market_cache=market_cache)
                    _sync_overall_strategy_feature_status(db, trade, now_ts, current_mtm=current_trade_mtm, overall_sl_done=_ore_done + 1, overall_tgt_done=_ort_done)
                    print('[OVERALL REENTRY QUEUED]', {'mode': 'backtest', 'trade_id': trade_id, 'reason': 'overall_sl', 'reentry_type': _ore_type, 'reentry_done': _ore_done + 1, 'legs_queued': _new_ids, 'immediate_entries': len(_reentry_entries)})
                    mark_strategy_activity(trade, 'overall_reentry', {
                        'reason': 'overall_sl', 'reentry_type': _ore_type,
                        'reentry_done': _ore_done + 1, 'reentry_count': _ore_count,
                        'legs_queued': _new_ids,
                    })
                    try:
                        from features.notification_manager import record_overall_reentry_queued
                        record_overall_reentry_queued(db._db, trade, now_ts, 'overall_sl', _ore_type, _ore_count, _new_ids, completed_reentries=_ore_done + 1, next_cycle_number=_ore_done + 2, next_overall_sl_value=_resolve_next_overall_cycle_value(osl_val, _ore_done), next_overall_tgt_value=_effective_tgt)
                    except Exception as _e:
                        log.warning('bt notification overall_reentry_sl error: %s', _e)
                else:
                    _square_off_trade_like_manual(db, db._db['algo_trades'].find_one({'_id': trade_id}) or trade, exit_timestamp=now_ts)
            else:
                _square_off_trade_like_manual(db, db._db['algo_trades'].find_one({'_id': trade_id}) or trade, exit_timestamp=now_ts)

        # ── Overall Target hit ────────────────────────────────────────────
        elif (open_leg_ids and otgt_type != 'None' and _safe_float(_effective_tgt) > 0 and current_trade_mtm >= _safe_float(_effective_tgt)):
            db._db['algo_trades'].update_one({'_id': trade_id}, {'$set': {'last_overall_event_at': now_ts, 'last_overall_event_reason': 'overall_target'}})
            mark_execute_order_dirty_from_trade(trade)
            _set_overall_feature_status_state(db, trade_id, 'overall_target', enabled=False, status='triggered', timestamp=now_ts, current_mtm=current_trade_mtm, disabled_reason='overall_target_hit')
            _set_overall_feature_status_state(db, trade_id, 'overall_sl', enabled=False, status='disabled', timestamp=now_ts, current_mtm=current_trade_mtm, disabled_reason='overall_target_hit')
            _disable_trade_feature_rows_for_new_cycle(db, trade_id, reason='overall_target_cycle_completed', timestamp=now_ts)
            try:
                from features.notification_manager import disable_all_trade_notifications
                disable_all_trade_notifications(db._db, trade_id, reason='overall_target', timestamp=now_ts)
            except Exception as _e:
                log.warning('bt disable_all_trade_notifications overall_target error: %s', _e)
            _close_remaining('overall_target')
            actions_taken.append(f'{trade_id}: overall Target hit mtm={current_trade_mtm}')
            hit_trade_id     = trade_id
            hit_ltp_snapshot = list((strategy_entry or {}).get('open_positions') or [])
            try:
                from features.notification_manager import record_overall_target_hit
                record_overall_target_hit(db._db, trade, now_ts, otgt_type, _effective_tgt, current_trade_mtm, legs_pnl_snapshot, cycle_number=_ort_done + 1, configured_reentry_count=_ort_count)
            except Exception as _e:
                log.warning('bt notification overall_tgt_hit error: %s', _e)
            if _ort_type != 'None' and _ort_count > 0 and _ort_done < _ort_count:
                _reset_sl_threshold = _resolve_overall_cycle_value(osl_val, _ore_done)
                db._db['algo_trades'].update_one({'_id': trade_id}, {'$set': {'overall_tgt_reentry_done': _ort_done + 1, 'peak_mtm': 0.0, 'current_overall_sl_threshold': _reset_sl_threshold, 'last_overall_event_at': now_ts, 'last_overall_event_reason': 'overall_target'}})
                mark_execute_order_dirty_from_trade(trade)
                _reentry_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or trade
                print('[OVERALL REENTRY PREPARE]', {'mode': 'backtest', 'trade_id': trade_id, 'reason': 'overall_target', 'timestamp': now_ts, 'updated_reentry_done': _ort_done + 1, 'reentry_type': _ort_type})
                _new_ids = _requeue_original_legs_bt(db, _reentry_trade, trade_id, strategy_cfg, now_ts, reentry_type=_ort_type)
                if _new_ids:
                    _reentry_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or _reentry_trade
                    _reentry_entries = _execute_backtest_entries(db, [_serialize_trade_record(_reentry_trade)], now_time, now_ts, market_cache=market_cache)
                    _sync_overall_strategy_feature_status(db, trade, now_ts, current_mtm=current_trade_mtm, overall_sl_done=_ore_done, overall_tgt_done=_ort_done + 1)
                    print('[OVERALL REENTRY QUEUED]', {'mode': 'backtest', 'trade_id': trade_id, 'reason': 'overall_target', 'reentry_type': _ort_type, 'reentry_done': _ort_done + 1, 'legs_queued': _new_ids, 'immediate_entries': len(_reentry_entries)})
                    try:
                        from features.notification_manager import record_overall_reentry_queued
                        record_overall_reentry_queued(db._db, trade, now_ts, 'overall_target', _ort_type, _ort_count, _new_ids, completed_reentries=_ort_done + 1, next_cycle_number=_ort_done + 2, next_overall_sl_value=_effective_sl, next_overall_tgt_value=_resolve_next_overall_cycle_value(otgt_val, _ort_done))
                    except Exception as _e:
                        log.warning('bt notification overall_reentry_tgt error: %s', _e)
                else:
                    _square_off_trade_like_manual(db, db._db['algo_trades'].find_one({'_id': trade_id}) or trade, exit_timestamp=now_ts)
            else:
                _square_off_trade_like_manual(db, db._db['algo_trades'].find_one({'_id': trade_id}) or trade, exit_timestamp=now_ts)

        # ── Lock & Trail / Overall Trail SL ───────────────────────────────
        elif open_leg_ids:
            _lock_cfg  = parse_lock_and_trail(strategy_cfg)
            _lock_exit, _lock_floor = check_lock_and_trail(_lock_cfg, current_trade_mtm, _new_peak)
            if _lock_exit:
                _close_remaining('lock_and_trail')
                actions_taken.append(f'{trade_id}: LockAndTrail exit mtm={current_trade_mtm}')
                try:
                    from features.notification_manager import record_lock_and_trail_exit
                    record_lock_and_trail_exit(db._db, trade, now_ts, _lock_floor, current_trade_mtm, legs_pnl_snapshot)
                except Exception as _e:
                    log.warning('bt notification lock_and_trail error: %s', _e)
            else:
                trail_type, for_every, trail_by = parse_overall_trail_sl(strategy_cfg)
                if trail_type != 'None' and for_every > 0:
                    _old_thresh = _safe_float(trade.get('current_overall_sl_threshold'), osl_val)
                    _new_thresh = update_overall_trail_sl(for_every, trail_by, osl_val, _new_peak)
                    if _new_thresh < _old_thresh:
                        db._db['algo_trades'].update_one({'_id': trade_id}, {'$set': {'peak_mtm': _new_peak, 'current_overall_sl_threshold': _new_thresh}})
                        actions_taken.append(f'{trade_id}: overall trail SL {_old_thresh}->{_new_thresh}')
                        try:
                            from features.notification_manager import record_overall_trail_sl_changed
                            record_overall_trail_sl_changed(db._db, trade, now_ts, _old_thresh, _new_thresh, _new_peak, current_trade_mtm)
                        except Exception as _e:
                            log.warning('bt notification overall_trail_sl error: %s', _e)
                    elif _new_peak > _peak_mtm:
                        db._db['algo_trades'].update_one({'_id': trade_id}, {'$set': {'peak_mtm': _new_peak}})
                elif _new_peak > _peak_mtm:
                    db._db['algo_trades'].update_one({'_id': trade_id}, {'$set': {'peak_mtm': _new_peak}})
    else:
        # Safety sweep: close any open position history records that the per-leg loop missed
        # (happens when legs=[] because history wasn't loaded, or close_leg_in_db failed silently)
        try:
            _sweep_hist = db._db['algo_trade_positions_history']
            _open_recs = list(_sweep_hist.find({
                'trade_id': trade_id,
                'status': OPEN_LEG_STATUS,
                'exit_trade': None,
            }))
            if _open_recs:
                log.warning('[PAST_EXIT SWEEP] trade=%s found %d unclosed position history records', trade_id, len(_open_recs))
            for _rec in _open_recs:
                _rec_leg_id = str(_rec.get('leg_id') or _rec.get('id') or '')
                if not _rec_leg_id:
                    continue
                _rec_token = str(_rec.get('token') or '')
                _rec_seg   = str(_rec.get('ws_segment') or 'NSE_FNO')
                _rec_price = 0.0
                # Try live REST quote first (fast-forward / live)
                if _rec_token and activation_mode in {'fast-forward', 'live', 'forward-test'}:
                    try:
                        from features.broker_gateway import get_broker_rest_quotes as _gbrq  # type: ignore
                        _rq = _gbrq([_rec_token], db._db, {_rec_token: _rec_seg})
                        _rec_price = float((_rq.get(_rec_token) or {}).get('ltp') or 0)
                    except Exception:
                        pass
                # Fallbacks: last_saw_price → entry price
                if _rec_price <= 0:
                    _rec_price = _safe_float(_rec.get('last_saw_price'))
                if _rec_price <= 0:
                    _rec_price = _safe_float((_rec.get('entry_trade') or {}).get('price') or 0)
                log.warning('[PAST_EXIT SWEEP] trade=%s leg=%s price=%s — closing', trade_id, _rec_leg_id, _rec_price)
                _update_position_history_exit(db, trade_id, _rec_leg_id, _rec_price, 'exit_time', now_ts)
        except Exception as _sweep_err:
            log.warning('[PAST_EXIT SWEEP ERR] trade=%s: %s', trade_id, _sweep_err)
        _mark_trade_squared_off_at_exit_time(db, trade_id)

    return {
        'open_positions':   open_positions,
        'strategy_entry':   strategy_entry,
        'actions_taken':    actions_taken,
        'hit_trade_id':     hit_trade_id,
        'hit_ltp_snapshot': hit_ltp_snapshot,
        'trade_mtm':        trade_mtm,
    }


# ─── Broker-level settings fetch ─────────────────────────────────────────────

def _get_broker_sl_settings(
    db: MongoData,
    user_id: str,
    activation_mode: str,
    broker_id: str | None = None,
) -> list[dict]:
    """
    Fetch active broker SL/Target/LockAndTrail settings from DB.

    Query pattern:
      General  (no broker_id) : {activation_mode, user_id, status=1}
      Specific (with broker_id): {activation_mode, user_id, status=1, broker: broker_id}

    Works for all modes: algo-backtest / live / fast-forward.
    All DB activity stays in execution_socket.py.
    """
    query: dict = {
        'user_id':         str(user_id or '').strip(),
        'activation_mode': str(activation_mode or '').strip(),
        'status':          1,
    }
    if broker_id:
        query['broker'] = str(broker_id).strip()
    try:
        return list(db._db['algo_borker_stoploss_settings'].find(query))
    except Exception as exc:
        log.warning('[BROKER SL SETTINGS] fetch error user=%s mode=%s: %s', user_id, activation_mode, exc)
        return []


# ─── Broker-level event processor ────────────────────────────────────────────

def _process_broker_level_events(
    db: MongoData,
    running_trades: list[dict],
    trade_date: str,
    now_ts: str,
    activation_mode: str,
    strategy_map: dict[str, dict],
    trade_mtm_map: dict[str, float],
    hit_trade_ids: list[str],
    hit_ltp_snapshots: dict[str, list],
    actions_taken: list[str],
) -> None:
    """
    Broker-level SL / Target / LockAndTrail checks.

    Runs after all per-trade events are processed.
    Groups running trades by (user_id, broker, activation_mode),
    fetches broker settings via _get_broker_sl_settings, sums MTMs,
    and triggers square-off when any broker feature is hit.

    All DB reads and writes happen here — execution_socket.py only.
    Works for algo-backtest, live, and fast-forward modes identically.
    """
    # ── Group trades by user + broker + mode ─────────────────────────────────
    _broker_groups: dict[str, list[dict]] = {}
    for _t in running_trades:
        _uid  = str(_t.get('user_id') or '').strip()
        _bkr  = str(_t.get('broker') or '').strip()
        _mode = str(_t.get('activation_mode') or activation_mode).strip()
        if not _uid or not _bkr or not _mode:
            continue
        _broker_groups.setdefault(f'{_uid}|{_bkr}|{_mode}', []).append(_t)

    for _gkey, _group in _broker_groups.items():
        _uid, _bkr, _mode = _gkey.split('|', 2)

        # ── Resolve display name ──────────────────────────────────────────
        _broker_name = _bkr
        try:
            _bconf = db._db['broker_configuration'].find_one(
                {'_id': ObjectId(_bkr)},
                {'broker_name': 1, 'display_name': 1, 'name': 1, 'title': 1, 'broker': 1},
            )
            if _bconf:
                _broker_name = _extract_broker_configuration_label(_bconf, _bkr)
        except Exception:
            pass

        # runtime_print(
        #     f'\n{"━" * 65}\n'
        #     f'  [BROKER GROUP]  broker={_broker_name}  |  mode={_mode}  |  user={_uid}\n'
        #     f'{"━" * 65}'
        # )

        # ── Fetch settings: specific broker query ─────────────────────────
        # Query: {activation_mode, user_id, status=1, broker: broker_id}
        _settings_docs = _get_broker_sl_settings(db, _uid, _mode, broker_id=_bkr)
        if not _settings_docs:
            continue
        _bsl_doc = _settings_docs[0]  # one active doc per broker

        _bsl_val   = _bsl_doc.get('StopLoss')
        _btgt_val  = _bsl_doc.get('Target')
        _lat_cfg   = _bsl_doc.get('LockAndTrail') or {}
        _trail_cfg = _bsl_doc.get('OverallTrailSL')

        if not _bsl_val and not _btgt_val and not _lat_cfg and not _trail_cfg:
            continue

        # ── Total broker MTM = closed (realized) + open (live) ───────────
        _group_mtm = 0.0
        _open_trades: list[dict] = []
        _closed_mtm = 0.0
        try:
            _all_broker_trades = list(db._db['algo_trades'].find({
                'user_id':    _uid,
                'broker':     _bkr,
                'creation_ts': {'$regex': f'^{re.escape(trade_date)}'},
            }))
        except Exception as _qe:
            log.warning('[BROKER SL CHECK] all-trades query error: %s', _qe)
            _all_broker_trades = list(_group)

        for _t in _all_broker_trades:
            _tid = str(_t.get('_id') or '').strip()
            if not _tid:
                continue
            if _tid in trade_mtm_map:
                _group_mtm += trade_mtm_map[_tid]
                if any(str(_rt.get('_id') or '') == _tid for _rt in _group):
                    _open_trades.append(_t)
            else:
                try:
                    _c_mtm, _ = _compute_strategy_mtm_snapshot(db, _tid, now_ts)
                    _closed_mtm += _c_mtm
                    _group_mtm  += _c_mtm
                except Exception as _ce:
                    log.warning('[BROKER SL CHECK] closed MTM error trade=%s: %s', _tid, _ce)

        if not _open_trades and _closed_mtm == 0.0:
            continue

        _settings_id = _bsl_doc.get('_id')

        # ── OverallTrailSL standalone ─────────────────────────────────────
        _effective_sl_val = _bsl_val
        if _trail_cfg and not _lat_cfg and _bsl_val:
            _sl_trail_every = _safe_float((_trail_cfg or {}).get('InstrumentMove') or 0)
            _sl_trail_by    = _safe_float((_trail_cfg or {}).get('StopLossMove') or 0)
            if _sl_trail_every > 0 and _sl_trail_by > 0:
                _sl_sig = f"{trade_date}|{_bsl_val}|{_sl_trail_every}|{_sl_trail_by}"
                _stored_sl_sig = _bsl_doc.get('sl_settings_sig') or ''
                if _stored_sl_sig != _sl_sig:
                    _sl_peak_mtm      = 0.0
                    _effective_sl_val = _bsl_val
                    try:
                        db._db['algo_borker_stoploss_settings'].update_one(
                            {'_id': _settings_id},
                            {'$set': {'sl_settings_sig': _sl_sig, 'sl_peak_mtm': 0.0, 'effective_sl': _bsl_val}},
                        )
                        mark_broker_settings_dirty(_uid, _mode)
                    except Exception as _sue:
                        log.warning('[SL TRAIL STATE RESET] save error: %s', _sue)
                    # print(f'[SL TRAIL STATE RESET - {_bkr}]', {'timestamp': now_ts, 'broker': _bkr, 'mode': _mode, 'reason': 'settings changed or new run date', 'old_sig': _stored_sl_sig, 'new_sig': _sl_sig})
                else:
                    _sl_peak_mtm = _safe_float(_bsl_doc.get('sl_peak_mtm') or 0)
                _sl_state_upd: dict = {}
                if _group_mtm > _sl_peak_mtm:
                    _sl_peak_mtm = _group_mtm
                    _sl_state_upd['sl_peak_mtm'] = _group_mtm
                _steps    = int(_sl_peak_mtm / _sl_trail_every) if _sl_peak_mtm >= _sl_trail_every else 0
                _new_eff  = round(float(_bsl_val) - _steps * _sl_trail_by, 2)
                _prev_eff = _safe_float(_bsl_doc.get('effective_sl') or _bsl_val)
                _effective_sl_val = min(_new_eff, _prev_eff)
                if _effective_sl_val < _prev_eff:
                    _sl_state_upd['effective_sl'] = _effective_sl_val
                if _sl_state_upd and _settings_id is not None:
                    try:
                        db._db['algo_borker_stoploss_settings'].update_one({'_id': _settings_id}, {'$set': _sl_state_upd})
                        mark_broker_settings_dirty(_uid, _mode)
                    except Exception as _sue:
                        log.warning('[SL TRAIL STATE] save error: %s', _sue)
                # print(f'[BROKER SL TRAIL CHECK - {_bkr}]', {'timestamp': now_ts, 'broker': _bkr, 'mode': _mode, 'broker_mtm': round(_group_mtm, 2), 'original_sl': _bsl_val, 'effective_sl': _effective_sl_val, 'sl_peak_mtm': _sl_peak_mtm})

        # print(f'[BROKER SL/TGT CHECK - {_bkr}]', {
        #     'timestamp': now_ts, 'user_id': _uid, 'broker': _bkr, 'mode': _mode,
        #     'open_mtm': round(_open_mtm, 2), 'closed_mtm': round(_closed_mtm, 2),
        #     'broker_mtm': round(_group_mtm, 2),
        #     'broker_sl': _effective_sl_val, 'original_sl': _bsl_val if _effective_sl_val != _bsl_val else None,
        #     'sl_remaining': _sl_rem, 'sl_status': 'HIT' if (_effective_sl_val and _group_mtm <= -float(_effective_sl_val)) else 'active',
        #     'broker_target': _btgt_val, 'tgt_remaining': _tgt_rem,
        #     'tgt_status': 'HIT' if (_btgt_val and _group_mtm >= float(_btgt_val)) else 'active',
        #     'open_trades': len(_open_trades), 'total_trades': len(_all_broker_trades),
        # })

        # ── Square-off helper ─────────────────────────────────────────────
        def _broker_square_off_all(reason: str, lock_floor: float = 0.0) -> None:
            print(f'[BROKER {reason} - {_bkr}]', {'timestamp': now_ts, 'user_id': _uid, 'broker': _bkr, 'mode': _mode, 'broker_mtm': round(_group_mtm, 2), 'lock_floor': lock_floor or None, 'squaring_off_trades': [str(_t.get('_id') or '') for _t in _open_trades]})
            for _t in _open_trades:
                _fresh = db._db['algo_trades'].find_one({'_id': _t['_id']}) or _t
                _square_off_trade_like_manual(db, _fresh, exit_timestamp=now_ts)
                print(f'  [BROKER {reason} - {_bkr}] trade={str(_t.get("_id") or "")[:16]} legs closed')
            try:
                _bulk = db._db['algo_trades'].update_many(
                    {'broker': _bkr, 'user_id': _uid, 'activation_mode': _mode, 'active_on_server': True},
                    {'$set': {'active_on_server': False, 'status': 'StrategyStatus.SquaredOff', 'trade_status': 2}},
                )
                print(f'  [BROKER {reason} - {_bkr}] group close matched={_bulk.matched_count}')
            except Exception as _be:
                log.warning('[BROKER %s] bulk close error: %s', reason, _be)
            try:
                # Disable settings: query by (activation_mode, user_id, status=1, broker_id)
                db._db['algo_borker_stoploss_settings'].update_one(
                    {'user_id': _uid, 'broker': _bkr, 'activation_mode': _mode, 'status': 1},
                    {'$set': {'status': 0}},
                )
            except Exception as _de:
                log.warning('[BROKER %s] disable settings error: %s', reason, _de)
            for _t in _all_broker_trades:
                mark_execute_order_dirty_from_trade(_t)
            try:
                from features.notification_manager import upsert_broker_feature_status
                _feat = 'broker_sl' if 'SL' in str(reason or '').upper() else 'broker_target'
                upsert_broker_feature_status(db._db, trade=(_group[0] if _group else {}), user_id=_uid, broker=_bkr, activation_mode=_mode, feature=_feat, trigger_value=_safe_float(_effective_sl_val if _feat == 'broker_sl' else _btgt_val), current_mtm=_group_mtm, timestamp=now_ts)
            except Exception as _bfe:
                log.warning('[BROKER %s] feature status upsert error: %s', reason, _bfe)
            try:
                _broker_trade_ids = [str(_t.get('_id') or '') for _t in _all_broker_trades if _t.get('_id')]
                if _broker_trade_ids:
                    _fs_result = db._db['algo_leg_feature_status'].update_many(
                        {'trade_id': {'$in': _broker_trade_ids}, 'enabled': True},
                        {'$set': {'enabled': False, 'status': 'disabled'}},
                    )
                    print(f'  [BROKER {reason} - {_bkr}] disabled feature status rows matched={_fs_result.matched_count}')
            except Exception as _fse:
                log.warning('[BROKER %s] disable feature status error: %s', reason, _fse)
            actions_taken.append(f'broker_{reason.lower().replace(" ","_")} broker={_bkr} mtm={round(_group_mtm,2)}')
            for _t in _group:
                _tid = str(_t.get('_id') or '').strip()
                if _tid and _tid not in hit_trade_ids:
                    hit_trade_ids.append(_tid)
                    hit_ltp_snapshots[_tid] = list((strategy_map.get(_tid) or {}).get('open_positions') or [])

        # ── 1. StopLoss hit ───────────────────────────────────────────────
        if _effective_sl_val and _group_mtm <= -float(_effective_sl_val):
            _broker_square_off_all('SL HIT')
            continue

        # ── 2. Target hit ─────────────────────────────────────────────────
        if _btgt_val and _group_mtm >= float(_btgt_val):
            _broker_square_off_all('TARGET HIT')
            continue

        # ── 3. LockAndTrail check ─────────────────────────────────────────
        if not _lat_cfg:
            continue

        _lat_instr_move = _safe_float(_lat_cfg.get('InstrumentMove') or 0)
        _lat_sl_move    = _safe_float(_lat_cfg.get('StopLossMove') or 0)
        if not _lat_instr_move or not _lat_sl_move:
            continue

        _trail_every = _safe_float((_trail_cfg or {}).get('InstrumentMove') or 0)
        _trail_by    = _safe_float((_trail_cfg or {}).get('StopLossMove') or 0)
        _has_trail   = bool(_trail_cfg and _trail_every > 0 and _trail_by > 0)
        _state_upd: dict = {}
        _lock_sig = f"{trade_date}|{_lat_instr_move}|{_lat_sl_move}|{_trail_every}|{_trail_by}|{_bsl_val}|{_btgt_val}"
        _stored_lock_sig    = _bsl_doc.get('lock_settings_sig') or ''
        _was_lock_activated = bool(_bsl_doc.get('lock_activated') or False)

        if _stored_lock_sig != _lock_sig:
            if _was_lock_activated:
                _lock_activated     = True
                _current_lock_floor = _safe_float(_bsl_doc.get('current_lock_floor') or 0)
                _lock_peak_mtm      = _safe_float(_bsl_doc.get('lock_peak_mtm') or 0)
                _state_upd['lock_settings_sig'] = _lock_sig
            else:
                _lock_activated     = False
                _current_lock_floor = 0.0
                _lock_peak_mtm      = 0.0
                _state_upd.update({'lock_settings_sig': _lock_sig, 'lock_activated': False, 'current_lock_floor': 0.0, 'lock_peak_mtm': 0.0})
        else:
            _lock_activated     = _was_lock_activated
            _current_lock_floor = _safe_float(_bsl_doc.get('current_lock_floor') or 0)
            _lock_peak_mtm      = _safe_float(_bsl_doc.get('lock_peak_mtm') or 0)
            if _lock_activated and _lock_peak_mtm > _lat_instr_move:
                if _has_trail and _trail_every > 0:
                    _pa = _lock_peak_mtm - _lat_instr_move
                    _implied_floor = round(_lat_sl_move + int(_pa / _trail_every) * _trail_by, 2)
                else:
                    _implied_floor = _lat_sl_move
                if _implied_floor > _current_lock_floor:
                    _current_lock_floor = _implied_floor
                    _state_upd['current_lock_floor'] = _implied_floor

        if not _lock_activated:
            if _group_mtm >= _lat_instr_move:
                _lock_activated = True
                _current_lock_floor = _lat_sl_move
                _lock_peak_mtm  = _group_mtm
                _state_upd.update({'lock_activated': True, 'current_lock_floor': _lat_sl_move, 'lock_peak_mtm': _group_mtm, 'lock_activated_at': now_ts, 'lock_activation_mtm': _group_mtm})
                # print(f'[LOCK ACTIVATED - {_bkr}]', {'timestamp': now_ts, 'broker': _bkr, 'mode': _mode, 'broker_mtm': _group_mtm, 'instrument_move': _lat_instr_move, 'lock_floor_set_to': _lat_sl_move, 'trail_enabled': _has_trail})
            else:
                # print(f'[BROKER LOCK CHECK - {_bkr}]', {'timestamp': now_ts, 'broker': _bkr, 'mode': _mode, 'broker_mtm': round(_group_mtm, 2), 'lock_activated': False, 'activate_at': _lat_instr_move, 'lock_profit': _lat_sl_move, 'remaining_to_lock': round(_lat_instr_move - _group_mtm, 2), 'trail_enabled': _has_trail})
                if _state_upd and _settings_id is not None:
                    try:
                        db._db['algo_borker_stoploss_settings'].update_one({'_id': _settings_id}, {'$set': _state_upd})
                        mark_broker_settings_dirty(_uid, _mode)
                    except Exception as _sue:
                        log.warning('[LOCK STATE] save error: %s', _sue)
                continue

        if _group_mtm > _lock_peak_mtm:
            _lock_peak_mtm = _group_mtm
            _state_upd['lock_peak_mtm'] = _group_mtm

        if _has_trail:
            _profit_above = _lock_peak_mtm - _lat_instr_move
            _steps = int(_profit_above / _trail_every) if _profit_above >= _trail_every else 0
            _new_floor = round(_lat_sl_move + _steps * _trail_by, 2)
            if _new_floor > _current_lock_floor:
                _current_lock_floor = _new_floor
                _state_upd['current_lock_floor'] = _new_floor
                # print(f'[LOCK TRAIL UPDATE - {_bkr}]', {'timestamp': now_ts, 'broker': _bkr, 'mode': _mode, 'broker_mtm': _group_mtm, 'lock_peak_mtm': _lock_peak_mtm, 'old_lock_floor': _current_lock_floor, 'new_lock_floor': _new_floor, 'trail_steps': _steps})

        # Persist lock state to DB
        if _state_upd and _settings_id is not None:
            try:
                _max_f = {k: v for k, v in _state_upd.items() if k in ('lock_peak_mtm', 'current_lock_floor')}
                _set_f = {k: v for k, v in _state_upd.items() if k not in ('lock_peak_mtm', 'current_lock_floor')}
                _op: dict = {}
                if _max_f: _op['$max'] = _max_f
                if _set_f: _op['$set'] = _set_f
                if _op:
                    db._db['algo_borker_stoploss_settings'].update_one({'_id': _settings_id}, _op)
                    mark_broker_settings_dirty(_uid, _mode)
            except Exception as _sue:
                log.warning('[LOCK STATE] save error: %s', _sue)

        # print(f'[BROKER LOCK CHECK - {_bkr}]', {'timestamp': now_ts, 'broker': _bkr, 'mode': _mode, 'broker_mtm': round(_group_mtm, 2), 'lock_activated': _lock_activated, 'lock_floor': _current_lock_floor, 'lock_peak_mtm': _lock_peak_mtm, 'remaining_to_exit': round(_group_mtm - _current_lock_floor, 2), 'activate_at': _lat_instr_move, 'lock_profit': _lat_sl_move, 'trail_enabled': _has_trail})

        if _group_mtm < _current_lock_floor:
            _broker_square_off_all('LOCK AND TRAIL HIT', lock_floor=_current_lock_floor)


# ─── Core: backtest SL/TP/exit tick ───────────────────────────────────────────

def _backtest_minute_tick(
    db: MongoData,
    trade_date: str,
    listen_timestamp: str,
    activation_mode: str = 'algo-backtest',
    market_cache: dict | None = None,
    running_trades: list[dict] | None = None,
) -> dict:
    """
    Called every simulated minute for backtest / forward-test strategies.
    Mirrors _live_minute_tick but uses historical chain data at listen_timestamp.
    Checks SL/TP/exit_time/overall SL-target for every open leg and records
    notifications in algo_trade_notification.

    Pass running_trades to avoid an extra DB load (caller already has them).
    """
    algo_trades_col = db._db['algo_trades']
    chain_col       = db._db[OPTION_CHAIN_COLLECTION]
    now_ts          = listen_timestamp
    now_time        = listen_timestamp[11:16] if len(listen_timestamp) >= 16 else ''  # HH:MM

    if running_trades is None:
        trade_query = _build_trade_query(
            trade_date,
            activation_mode=activation_mode,
            statuses=[RUNNING_STATUS, BACKTEST_IMPORT_STATUS],
        )
        running_trades = list(algo_trades_col.find(trade_query))

    open_positions     = []
    strategy_map: dict[str, dict] = {}
    actions_taken: list[str]      = []
    # broker SL: collect per-trade MTM for group-level check after the loop
    _trade_mtm_map: dict[str, float] = {}   # trade_id → current_trade_mtm
    # track trade IDs hit by overall SL/Target or broker SL/Target → emit to execute-orders
    hit_trade_ids: list[str] = []
    # LTP snapshot at moment of hit: trade_id → [{leg_id, ltp, entry_price, pnl, ...}]
    hit_ltp_snapshots: dict[str, list] = {}

    for trade in running_trades:
        trade_id   = str(trade.get('_id') or '')
        underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        cfg        = trade.get('config') or {}
        all_leg_configs = _resolve_trade_leg_configs(trade)

        raw_exit_time  = str(trade.get('exit_time') or '')
        exit_time_hhmm = raw_exit_time[11:16] if len(raw_exit_time) >= 16 else raw_exit_time[:5]
        past_exit = bool(exit_time_hhmm and now_time and now_time >= exit_time_hhmm)

        # Dict legs (pending / not yet synced to history)
        legs = [leg for leg in (trade.get('legs') or []) if isinstance(leg, dict)]
        _dict_leg_ids = {str(l.get('id') or '') for l in legs if l.get('id')}

        # String refs = legs already stored in algo_trade_positions_history.
        # Re-load them so SL/TP/trail checks can run.
        _has_str_refs = any(isinstance(item, str) for item in (trade.get('legs') or []))
        if _has_str_refs:
            _hist_col = db._db['algo_trade_positions_history']
            for _hdoc in _hist_col.find({'trade_id': trade_id}):
                if not isinstance(_hdoc.get('entry_trade'), dict) or _hdoc.get('exit_trade'):
                    continue
                _hleg_id = str(_hdoc.get('leg_id') or _hdoc.get('id') or '')
                if not _hleg_id or _hleg_id in _dict_leg_ids:
                    continue  # already in legs or invalid
                # Normalize expiry_date: history stores '2025-11-06 15:30:00',
                # chain collection uses plain '2025-11-06'
                _raw_exp = str(_hdoc.get('expiry_date') or '')
                _hdoc['expiry_date'] = _raw_exp[:10] if ' ' in _raw_exp else _raw_exp
                _hdoc['id'] = _hleg_id  # ensure 'id' key is set
                _hdoc.setdefault('status', OPEN_LEG_STATUS)
                legs.append(_hdoc)


        # ── Get LTP array from algo_backtest_event (data layer only) ──────────
        _ltp_array = get_open_legs_ltp_array(
            chain_col,
            legs,
            underlying,
            now_ts,
            market_cache,
            activation_mode=activation_mode,
        )
        _ltp_map: dict[str, float] = {item['leg_id']: item['ltp'] for item in _ltp_array}

        # ── Run all events (SL/TP/trail/overall/MTM) in ONE function ──────────
        print("----------------------_process_backtest_trade_tick ---------------")
        _tick_result = _process_backtest_trade_tick(
            db=db,
            trade=trade,
            legs=legs,
            ltp_map=_ltp_map,
            now_ts=now_ts,
            now_time=now_time,
            chain_col=chain_col,
            market_cache=market_cache,
            past_exit=past_exit,
            all_leg_configs=all_leg_configs,
            underlying=underlying,
            trade_date=trade_date,
            activation_mode=activation_mode,
        )
        open_positions.extend(_tick_result['open_positions'])
        if _tick_result['strategy_entry']:
            strategy_map[trade_id] = _tick_result['strategy_entry']
        actions_taken.extend(_tick_result['actions_taken'])
        if _tick_result['hit_trade_id']:
            hit_trade_ids.append(_tick_result['hit_trade_id'])
            hit_ltp_snapshots[_tick_result['hit_trade_id']] = _tick_result['hit_ltp_snapshot']
        _trade_mtm_map[trade_id] = _tick_result['trade_mtm']


    # ── Broker-level SL / Target / LockAndTrail ─────────────────────────────
    # All DB activity happens inside _process_broker_level_events.
    # Works identically for algo-backtest, live, and fast-forward modes.
    _process_broker_level_events(
        db=db,
        running_trades=running_trades,
        trade_date=trade_date,
        now_ts=now_ts,
        activation_mode=activation_mode,
        strategy_map=strategy_map,
        trade_mtm_map=_trade_mtm_map,
        hit_trade_ids=hit_trade_ids,
        hit_ltp_snapshots=hit_ltp_snapshots,
        actions_taken=actions_taken,
    )

    # ── Strategy-level SL / Target / TrailSL / LockAndTrail check ───────────
    # Group running trades by executed_strategy_id.
    # Sum their MTMs (open live + closed realized) and apply the strategy's own
    # OverallSL / OverallTgt / OverallTrailSL / LockAndTrail fields.
    # State (peak MTM, lock floor etc.) is persisted in algo_strategy_sl_state.
    #
    # Config formats supported:
    #   OverallSL        : {"Type": "..MTM..",  "Value": 1300}
    #   OverallTgt       : {"Type": "..MTM..",  "Value": 600}
    #   OverallTrailSL   : {"Type": "..MTM..",  "Value": {"InstrumentMove": 100, "StopLossMove": 50}}
    #   LockAndTrail     : {"Type": "TrailingOption.LockAndTrail",
    #                       "Value": {"ProfitReaches": 1000, "LockProfit": 600,
    #                                 "IncreaseInProfitBy": 100, "TrailProfitBy": 60}}
    # ────────────────────────────────────────────────────────────────────────
    _strat_groups: dict[str, list[dict]] = {}
    for _t in running_trades:
        _esid = str(_t.get('executed_strategy_id') or _t.get('strategy_id') or '').strip()
        if not _esid:
            continue
        _strat_groups.setdefault(_esid, []).append(_t)

    for _esid, _sgroup in _strat_groups.items():
        # ── Get strategy config from first trade in group ─────────────────
        _strat_trade = _sgroup[0]
        _strat_cfg   = _strat_trade.get('strategy') or {}
        _s_uid       = str(_strat_trade.get('user_id') or '').strip()
        _s_bkr       = str(_strat_trade.get('broker') or '').strip()
        _s_mode      = str(_strat_trade.get('activation_mode') or activation_mode).strip()

        # ── Parse OverallSL ───────────────────────────────────────────────
        _osl_cfg  = _strat_cfg.get('OverallSL') or {}
        _osl_type = str(_osl_cfg.get('Type') or 'None')
        _ssl_val  = float(_osl_cfg.get('Value') or 0) if 'None' not in _osl_type else 0.0
        _ssl_val  = _ssl_val if _ssl_val > 0 else None

        # ── Parse OverallTgt ──────────────────────────────────────────────
        _otgt_cfg  = _strat_cfg.get('OverallTgt') or {}
        _otgt_type = str(_otgt_cfg.get('Type') or 'None')
        _stgt_val  = float(_otgt_cfg.get('Value') or 0) if 'None' not in _otgt_type else 0.0
        _stgt_val  = _stgt_val if _stgt_val > 0 else None

        # ── Parse OverallTrailSL {InstrumentMove, StopLossMove} ───────────
        _otrail_cfg  = _strat_cfg.get('OverallTrailSL') or {}
        _otrail_type = str(_otrail_cfg.get('Type') or 'None')
        _otrail_val  = _otrail_cfg.get('Value') or {}
        _s_trail_cfg = None
        if 'None' not in _otrail_type and isinstance(_otrail_val, dict):
            _s_trl_every = _safe_float(_otrail_val.get('InstrumentMove') or 0)
            _s_trl_by    = _safe_float(_otrail_val.get('StopLossMove') or 0)
            if _s_trl_every > 0 and _s_trl_by > 0:
                _s_trail_cfg = {'InstrumentMove': _s_trl_every, 'StopLossMove': _s_trl_by}

        # ── Parse LockAndTrail {ProfitReaches, LockProfit, IncreaseInProfitBy, TrailProfitBy} ──
        _olat_cfg  = _strat_cfg.get('LockAndTrail') or {}
        _olat_type = str(_olat_cfg.get('Type') or 'None')
        _olat_val  = _olat_cfg.get('Value') or {}
        _s_lat_cfg = None
        if 'None' not in _olat_type and isinstance(_olat_val, dict):
            _slat_pr = _safe_float(_olat_val.get('ProfitReaches') or 0)
            if _slat_pr > 0:
                _s_lat_cfg = _olat_val  # keep original dict; parsed below

        # Skip if no strategy-level feature is configured
        if not _ssl_val and not _stgt_val and not _s_trail_cfg and not _s_lat_cfg:
            continue

        # ── Compute total strategy MTM (all trades today for this strategy) ──
        _sg_mtm      = 0.0
        _sg_open_trd: list[dict] = []
        _sg_closed   = 0.0
        try:
            _all_strat_trades = list(db._db['algo_trades'].find({
                'executed_strategy_id': _esid,
                'creation_ts': {'$regex': f'^{re.escape(trade_date)}'},
            }))
        except Exception as _sqe:
            log.warning('[STRAT SL CHECK] trades query error: %s', _sqe)
            _all_strat_trades = list(_sgroup)

        for _st in _all_strat_trades:
            _stid = str(_st.get('_id') or '').strip()
            if not _stid:
                continue
            if _stid in _trade_mtm_map:
                _sg_mtm += _trade_mtm_map[_stid]
                if any(str(_rt.get('_id') or '') == _stid for _rt in _sgroup):
                    _sg_open_trd.append(_st)
            else:
                try:
                    _sc_mtm, _ = _compute_strategy_mtm_snapshot(db, _stid, now_ts)
                    _sg_closed += _sc_mtm
                    _sg_mtm    += _sc_mtm
                except Exception as _sce:
                    log.warning('[STRAT SL CHECK] closed MTM error trade=%s: %s', _stid, _sce)

        _same_tick_overall_trade = next(
            (
                _st for _st in _all_strat_trades
                if str((_st or {}).get('last_overall_event_at') or '').strip().replace(' ', 'T')[:19] == str(now_ts or '').strip()[:19]
                and str((_st or {}).get('last_overall_event_reason') or '').strip() in {'overall_sl', 'overall_target'}
            ),
            None,
        )
        if _same_tick_overall_trade:
            print(f'[STRAT CHECK SKIPPED - {_s_bkr}]', {
                'timestamp': now_ts,
                'strat': _esid,
                'mode': _s_mode,
                'reason': 'same_tick_overall_event',
                'trade_id': str((_same_tick_overall_trade or {}).get('_id') or ''),
                'last_overall_event_at': str((_same_tick_overall_trade or {}).get('last_overall_event_at') or ''),
                'last_overall_event_reason': str((_same_tick_overall_trade or {}).get('last_overall_event_reason') or ''),
            })
            continue

        if not _sg_open_trd and _sg_closed == 0.0:
            continue

        _sg_open_mtm = _sg_mtm - _sg_closed
        _runtime_strat_sl = next(
            (
                _safe_float((_st.get('current_overall_sl_threshold') or 0))
                for _st in (_sg_open_trd or _all_strat_trades)
                if _safe_float((_st.get('current_overall_sl_threshold') or 0)) > 0
            ),
            0.0,
        )

        # ── Load persistent state from algo_strategy_sl_state ────────────
        try:
            _sstate_doc = db._db['algo_strategy_sl_state'].find_one({
                'executed_strategy_id': _esid,
                'trade_date':           trade_date,
            }) or {}
        except Exception as _sle:
            log.warning('[STRAT SL] state load error: %s', _sle)
            _sstate_doc = {}

        _sstate_upd: dict = {}

        def _save_sstate(
            _esid=_esid, _trade_date=trade_date,
            _s_uid=_s_uid, _s_bkr=_s_bkr, _s_mode=_s_mode,
        ) -> None:
            """Upsert state changes into algo_strategy_sl_state."""
            if not _sstate_upd:
                return
            try:
                _smax: dict = {}
                _sset: dict = {}
                for _k, _v in _sstate_upd.items():
                    if _k in ('strat_lock_peak_mtm', 'strat_current_lock_floor', 'strat_sl_peak_mtm'):
                        _smax[_k] = _v
                    else:
                        _sset[_k] = _v
                _sset.update({
                    'executed_strategy_id': _esid,
                    'trade_date':           _trade_date,
                    'user_id':              _s_uid,
                    'broker':               _s_bkr,
                    'activation_mode':      _s_mode,
                })
                _sop: dict = {'$set': _sset}
                if _smax:
                    _sop['$max'] = _smax
                db._db['algo_strategy_sl_state'].update_one(
                    {'executed_strategy_id': _esid, 'trade_date': _trade_date},
                    _sop,
                    upsert=True,
                )
            except Exception as _se:
                log.warning('[STRAT SL] state save error: %s', _se)

        # ── Shared square-off helper ──────────────────────────────────────
        def _strat_square_off_all(
            reason: str, lock_floor: float = 0.0,
            _esid=_esid, _s_uid=_s_uid, _s_bkr=_s_bkr, _s_mode=_s_mode,
            _sg_open_trd=_sg_open_trd, _sg_mtm=_sg_mtm, _sgroup=_sgroup,
        ) -> None:
            """Close all open trades for this strategy."""
            _strade_ids = [str(_st2.get('_id') or '') for _st2 in _sg_open_trd]
            print(f'[STRAT {reason} - {_s_bkr}]', {
                'timestamp':            now_ts,
                'executed_strategy_id': _esid,
                'user_id':              _s_uid,
                'broker':               _s_bkr,
                'mode':                 _s_mode,
                'strat_mtm':            round(_sg_mtm, 2),
                'lock_floor':           lock_floor if lock_floor else None,
                'squaring_off_trades':  _strade_ids,
            })
            for _st2 in _sg_open_trd:
                _sfresh = db._db['algo_trades'].find_one({'_id': _st2['_id']}) or _st2
                _square_off_trade_like_manual(db, _sfresh, exit_timestamp=now_ts)
                print(f'  [STRAT {reason} - {_s_bkr}] trade={str(_st2.get("_id") or "")[:16]} legs closed')
            try:
                _sbulk = db._db['algo_trades'].update_many(
                    {'executed_strategy_id': _esid, 'activation_mode': _s_mode, 'active_on_server': True},
                    {'$set': {'active_on_server': False, 'status': 'StrategyStatus.SquaredOff', 'trade_status': 2}},
                )
                print(f'  [STRAT {reason} - {_s_bkr}] group close matched={_sbulk.matched_count} modified={_sbulk.modified_count}')
            except Exception as _sbe:
                log.warning('[STRAT %s] bulk close error: %s', reason, _sbe)
            for _st2 in _sgroup:
                mark_execute_order_dirty_from_trade(_st2)
            actions_taken.append(f'strat_{reason.lower().replace(" ","_")} strat={_esid} mtm={round(_sg_mtm,2)}')
            for _st2 in _sgroup:
                _stid2 = str(_st2.get('_id') or '').strip()
                if _stid2 and _stid2 not in hit_trade_ids:
                    hit_trade_ids.append(_stid2)
                    hit_ltp_snapshots[_stid2] = list((strategy_map.get(_stid2) or {}).get('open_positions') or [])

        # ── Case A: OverallTrailSL standalone (no LockAndTrail) ──────────
        _s_effective_sl = _runtime_strat_sl or _ssl_val
        if _s_trail_cfg and not _s_lat_cfg and _ssl_val:
            _st_every = _safe_float(_s_trail_cfg.get('InstrumentMove') or 0)
            _st_by    = _safe_float(_s_trail_cfg.get('StopLossMove') or 0)
            if _st_every > 0 and _st_by > 0:
                _st_sig        = f"{trade_date}|{_ssl_val}|{_st_every}|{_st_by}"
                _stored_st_sig = _sstate_doc.get('strat_sl_settings_sig') or ''
                if _stored_st_sig != _st_sig:
                    _st_peak_mtm    = 0.0
                    _s_effective_sl = _ssl_val
                    _sstate_upd.update({
                        'strat_sl_settings_sig': _st_sig,
                        'strat_sl_peak_mtm':     0.0,
                        'strat_effective_sl':    _ssl_val,
                    })
                    print(f'[STRAT SL TRAIL RESET - {_s_bkr}]', {
                        'timestamp': now_ts, 'strat': _esid,
                        'reason': 'settings changed or new run date',
                        'old_sig': _stored_st_sig, 'new_sig': _st_sig,
                    })
                else:
                    _st_peak_mtm = _safe_float(_sstate_doc.get('strat_sl_peak_mtm') or 0)
                _stsl_upd: dict = {}
                if _sg_mtm > _st_peak_mtm:
                    _st_peak_mtm = _sg_mtm
                    _stsl_upd['strat_sl_peak_mtm'] = _sg_mtm
                _st_steps   = int(_st_peak_mtm / _st_every) if _st_peak_mtm >= _st_every else 0
                _new_eff_s  = round(float(_ssl_val) - _st_steps * _st_by, 2)
                _prev_eff_s = _safe_float(_sstate_doc.get('strat_effective_sl') or _ssl_val)
                _s_effective_sl = min(_new_eff_s, _prev_eff_s)
                if _s_effective_sl < _prev_eff_s:
                    _stsl_upd['strat_effective_sl'] = _s_effective_sl
                    print(f'[STRAT SL TRAIL UPDATE - {_s_bkr}]', {
                        'timestamp':    now_ts, 'strat': _esid,
                        'strat_mtm':    round(_sg_mtm, 2),
                        'original_sl':  _ssl_val,
                        'old_eff_sl':   _prev_eff_s,
                        'new_eff_sl':   _s_effective_sl,
                        'sl_peak_mtm':  _st_peak_mtm,
                        'trail_steps':  _st_steps,
                        'trail_every':  _st_every,
                        'trail_by':     _st_by,
                    })
                _sstate_upd.update(_stsl_upd)
                print(f'[STRAT SL TRAIL CHECK - {_s_bkr}]', {
                    'timestamp':    now_ts, 'strat': _esid,
                    'strat_mtm':    round(_sg_mtm, 2),
                    'original_sl':  _ssl_val,
                    'effective_sl': _s_effective_sl,
                    'sl_peak_mtm':  _st_peak_mtm,
                    'trail_steps':  _st_steps,
                    'trail_every':  _st_every,
                    'trail_by':     _st_by,
                })

        _ssl_rem     = round(_sg_mtm + float(_s_effective_sl or 0), 2) if _s_effective_sl else None
        _stgt_rem    = round(float(_stgt_val or 0) - _sg_mtm, 2) if _stgt_val else None
        _ssl_status  = 'HIT' if (_s_effective_sl and _sg_mtm <= -float(_s_effective_sl)) else 'active'
        _stgt_status = 'HIT' if (_stgt_val and _sg_mtm >= float(_stgt_val)) else 'active'
        print(f'[STRAT SL/TGT CHECK - {_s_bkr}]', {
            'timestamp':     now_ts,
            'strat':         _esid,
            'user_id':       _s_uid,
            'mode':          _s_mode,
            'open_mtm':      round(_sg_open_mtm, 2),
            'closed_mtm':    round(_sg_closed, 2),
            'strat_mtm':     round(_sg_mtm, 2),
            'strat_sl':      _s_effective_sl,
            'runtime_current_overall_sl_threshold': _runtime_strat_sl or None,
            'original_sl':   _ssl_val if _s_effective_sl != _ssl_val else None,
            'sl_remaining':  _ssl_rem,
            'sl_status':     _ssl_status,
            'strat_target':  _stgt_val,
            'tgt_remaining': _stgt_rem,
            'tgt_status':    _stgt_status,
            'open_trades':   len(_sg_open_trd),
            'total_trades':  len(_all_strat_trades),
        })

        # ── 1. StopLoss hit ───────────────────────────────────────────────
        if _s_effective_sl and _sg_mtm <= -float(_s_effective_sl):
            _save_sstate()
            _strat_square_off_all('SL HIT')
            continue

        # ── 2. Target hit ─────────────────────────────────────────────────
        if _stgt_val and _sg_mtm >= float(_stgt_val):
            _save_sstate()
            _strat_square_off_all('TARGET HIT')
            continue

        # ── 3. LockAndTrail (Case B) ──────────────────────────────────────
        if not _s_lat_cfg:
            _save_sstate()
            continue

        _slat_instr_move  = _safe_float(_s_lat_cfg.get('ProfitReaches') or 0)
        _slat_sl_move     = _safe_float(_s_lat_cfg.get('LockProfit') or 0)
        _slat_trail_every = _safe_float(_s_lat_cfg.get('IncreaseInProfitBy') or 0)
        _slat_trail_by    = _safe_float(_s_lat_cfg.get('TrailProfitBy') or 0)
        _s_has_trail      = _slat_trail_every > 0 and _slat_trail_by > 0

        if _slat_instr_move <= 0:
            _save_sstate()
            continue

        # ── Build lock signature ──────────────────────────────────────────
        _s_lock_sig = (
            f"{trade_date}|{_slat_instr_move}|{_slat_sl_move}|"
            f"{_slat_trail_every}|{_slat_trail_by}|"
            f"{_ssl_val}|{_stgt_val}"
        )
        _s_stored_lock_sig    = _sstate_doc.get('strat_lock_settings_sig') or ''
        _s_was_lock_activated = bool(_sstate_doc.get('strat_lock_activated') or False)

        if _s_stored_lock_sig != _s_lock_sig:
            if _s_was_lock_activated:
                # Lock already activated — NEVER reset. Only update signature.
                _s_lock_activated     = True
                _s_current_lock_floor = _safe_float(_sstate_doc.get('strat_current_lock_floor') or 0)
                _s_lock_peak_mtm      = _safe_float(_sstate_doc.get('strat_lock_peak_mtm') or 0)
                _sstate_upd['strat_lock_settings_sig'] = _s_lock_sig
                print(f'[STRAT LOCK SIG UPDATED - ACTIVATION PRESERVED - {_s_bkr}]', {
                    'timestamp':  now_ts, 'strat': _esid,
                    'reason':     'settings changed but lock already activated — not resetting',
                    'lock_floor': _s_current_lock_floor,
                    'old_sig':    _s_stored_lock_sig, 'new_sig': _s_lock_sig,
                })
            else:
                # Lock not yet activated — safe to reset for new config
                _s_lock_activated     = False
                _s_current_lock_floor = 0.0
                _s_lock_peak_mtm      = 0.0
                _sstate_upd.update({
                    'strat_lock_settings_sig':  _s_lock_sig,
                    'strat_lock_activated':     False,
                    'strat_current_lock_floor': 0.0,
                    'strat_lock_peak_mtm':      0.0,
                })
                print(f'[STRAT LOCK STATE RESET - {_s_bkr}]', {
                    'timestamp': now_ts, 'strat': _esid,
                    'reason':    'settings changed or new run date',
                    'old_sig':   _s_stored_lock_sig, 'new_sig': _s_lock_sig,
                })
        else:
            _s_lock_activated     = _s_was_lock_activated
            _s_current_lock_floor = _safe_float(_sstate_doc.get('strat_current_lock_floor') or 0)
            _s_lock_peak_mtm      = _safe_float(_sstate_doc.get('strat_lock_peak_mtm') or 0)

            # Safety guard: floor must never decrease once set
            if _s_lock_activated and _s_lock_peak_mtm > _slat_instr_move:
                if _s_has_trail and _slat_trail_every > 0:
                    _spa       = _s_lock_peak_mtm - _slat_instr_move
                    _ssteps    = int(_spa / _slat_trail_every) if _spa >= _slat_trail_every else 0
                    _simplied  = round(_slat_sl_move + _ssteps * _slat_trail_by, 2)
                else:
                    _simplied = _slat_sl_move
                if _simplied > _s_current_lock_floor:
                    _s_current_lock_floor = _simplied
                    _sstate_upd['strat_current_lock_floor'] = _simplied

        # ── 3a. Activate lock when MTM first crosses ProfitReaches ───────
        if not _s_lock_activated:
            if _sg_mtm >= _slat_instr_move:
                _s_lock_activated     = True
                _s_current_lock_floor = _slat_sl_move
                _s_lock_peak_mtm      = _sg_mtm
                _sstate_upd.update({
                    'strat_lock_activated':      True,
                    'strat_current_lock_floor':  _slat_sl_move,
                    'strat_lock_peak_mtm':       _sg_mtm,
                    'strat_lock_activated_at':   now_ts,
                    'strat_lock_activation_mtm': _sg_mtm,
                })
                print(f'[STRAT LOCK ACTIVATED - {_s_bkr}]', {
                    'timestamp':         now_ts, 'strat': _esid,
                    'mode':              _s_mode,
                    'strat_mtm':         _sg_mtm,
                    'profit_reaches':    _slat_instr_move,
                    'lock_floor_set_to': _slat_sl_move,
                    'trail_enabled':     _s_has_trail,
                })
            else:
                print(f'[STRAT LOCK CHECK - {_s_bkr}]', {
                    'timestamp':          now_ts, 'strat': _esid,
                    'mode':               _s_mode,
                    'strat_mtm':          round(_sg_mtm, 2),
                    'lock_activated':     False,
                    'activate_at':        _slat_instr_move,
                    'lock_profit':        _slat_sl_move,
                    'remaining_to_lock':  round(_slat_instr_move - _sg_mtm, 2),
                    'trail_enabled':      _s_has_trail,
                    'trail_every':        _slat_trail_every if _s_has_trail else None,
                    'trail_by':           _slat_trail_by    if _s_has_trail else None,
                })
        else:
            # ── 3b. Update peak MTM ───────────────────────────────────────
            if _sg_mtm > _s_lock_peak_mtm:
                _s_lock_peak_mtm = _sg_mtm
                _sstate_upd['strat_lock_peak_mtm'] = _sg_mtm

            # ── 3c. Trail the floor upward ────────────────────────────────
            if _s_has_trail:
                _s_profit_above = _s_lock_peak_mtm - _slat_instr_move
                _s_steps        = int(_s_profit_above / _slat_trail_every) if _s_profit_above >= _slat_trail_every else 0
                _s_new_floor    = round(_slat_sl_move + _s_steps * _slat_trail_by, 2)
                if _s_new_floor > _s_current_lock_floor:
                    print(f'[STRAT LOCK TRAIL UPDATE - {_s_bkr}]', {
                        'timestamp':      now_ts, 'strat': _esid,
                        'mode':           _s_mode,
                        'strat_mtm':      _sg_mtm,
                        'lock_peak_mtm':  _s_lock_peak_mtm,
                        'old_lock_floor': _s_current_lock_floor,
                        'new_lock_floor': _s_new_floor,
                        'trail_steps':    _s_steps,
                        'trail_every':    _slat_trail_every,
                        'trail_by':       _slat_trail_by,
                    })
                    _s_current_lock_floor = _s_new_floor
                    _sstate_upd['strat_current_lock_floor'] = _s_new_floor

            print(f'[STRAT LOCK CHECK - {_s_bkr}]', {
                'timestamp':          now_ts, 'strat': _esid,
                'mode':               _s_mode,
                'strat_mtm':          round(_sg_mtm, 2),
                'lock_activated':     _s_lock_activated,
                'lock_floor':         _s_current_lock_floor,
                'lock_peak_mtm':      _s_lock_peak_mtm,
                'remaining_to_exit':  round(_sg_mtm - _s_current_lock_floor, 2),
                'activate_at':        _slat_instr_move,
                'lock_profit':        _slat_sl_move,
                'trail_enabled':      _s_has_trail,
                'trail_every':        _slat_trail_every if _s_has_trail else None,
                'trail_by':           _slat_trail_by    if _s_has_trail else None,
                'strat_sl':           _ssl_val,
                'strat_target':       _stgt_val,
                'settings_sig':       _s_lock_sig,
            })

            # ── 3d. Exit when MTM drops below lock floor ──────────────────
            if _sg_mtm < _s_current_lock_floor:
                _save_sstate()
                _strat_square_off_all('LOCK AND TRAIL HIT', lock_floor=_s_current_lock_floor)
                continue

        _save_sstate()

    return {
        'actions_taken':    actions_taken,
        'checked_at':       now_ts,
        'total_open':       len(open_positions),
        'hit_trade_ids':    hit_trade_ids,      # strategies hit by overall SL/TGT or broker SL/TGT
        'hit_ltp_snapshots': hit_ltp_snapshots, # {trade_id: [{leg_id, ltp, entry_price, pnl}]} at hit time
    }


def _requeue_original_legs_bt(db, trade: dict, trade_id: str, strategy_cfg: dict, now_ts: str, reentry_type: str = '') -> list[str]:
    """Re-queue original legs for overall reentry in backtest mode.

    reentry_type: ore_type / ort_type from parse_overall_reentry_sl/tgt.

    LikeOriginal behaviour (same method as NextLeg + momentum):
      - If original leg has LegMomentum → queue to algo_leg_feature_status as
        momentum_pending (handled by _process_momentum_pending_feature_legs).
      - If no LegMomentum → push as regular pending leg (enters on next tick).

    Immediate (and other types):
      - Push with skip_momentum_check=True → enters directly, no momentum wait.
    """
    original_cfgs = strategy_cfg.get('ListOfLegConfigs') or []
    if not original_cfgs:
        original_cfgs = list((_resolve_trade_leg_configs(trade) or {}).values())
    new_ids: list[str] = []
    existing_ids = {str(l.get('id') or '') for l in (trade.get('legs') or []) if isinstance(l, dict)}
    ts_sfx = now_ts.replace(':', '').replace('T', '').replace('-', '')[:14]
    is_like_original = 'LikeOriginal' in reentry_type
    for lc in original_cfgs:
        if not isinstance(lc, dict):
            continue
        orig_id = str(lc.get('id') or '')
        if not orig_id:
            continue
        new_id  = f'{orig_id}-ore-{ts_sfx}'
        if new_id in existing_ids:
            continue
        leg_type = f'{orig_id}-overall_reentry'

        new_leg = _build_pending_leg(
            new_id,
            lc,
            trade,
            now_ts,
            f'overall_{orig_id}',
            leg_type=leg_type,
        )
        new_leg.update({
            'is_lazy': True,
            'is_reentered_leg': True,
            'lazy_leg_ref': orig_id,
            'parent_leg_id': orig_id,
            # Overall reentry always skips momentum — the leg must enter immediately
            # regardless of the original leg's LegMomentum config.
            'skip_momentum_check': True,
        })
        if is_like_original:
            new_leg['reentry_type'] = 'LikeOriginal'
        try:
            db._db['algo_trades'].update_one({'_id': trade_id}, {'$push': {'legs': new_leg}})
            new_ids.append(new_id)
            mark_execute_order_dirty_from_trade(trade)
        except Exception as _e:
            log.warning('bt requeue leg error: %s', _e)
    return new_ids


# ─── Core: live-mode minute tick ──────────────────────────────────────────────

def _live_minute_tick(db: MongoData, trade_date: str) -> dict:
    """
    Called every 60 s in live mode.
    1. Loads running strategies from algo_trades.
    2. For pending legs: attempts entry from latest option_chain.
    3. For open legs: fetches latest option_chain, checks SL/TP/exit_time.
    4. Executes exits + queues re-entries / lazy legs in the DB.
    5. Returns the full position snapshot (only open positions with chain data).
    """
    algo_trades_col = db._db['algo_trades']
    chain_col = db._db[OPTION_CHAIN_COLLECTION]
    now_ts = _now_iso()
    now_time = _now_ist_str()   # HH:MM for comparing with entry/exit times

    trade_query = _build_trade_query(
        trade_date,
        activation_mode='live',
        statuses=[RUNNING_STATUS],
    )
    running_trades = list(algo_trades_col.find(trade_query))

    open_positions = []
    strategy_map: dict[str, dict] = {}
    actions_taken: list[str] = []
    position_entry_events: list[dict] = []

    for trade in running_trades:
        trade_id = str(trade.get('_id') or '')
        underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        cfg = trade.get('config') or {}

        # Merge all leg configs (original + idle)
        all_leg_configs = _resolve_trade_leg_configs(trade)

        # Parse entry/exit time bounds  (format: "YYYY-MM-DD HH:MM:SS" or "HH:MM:SS")
        raw_entry_time = str(trade.get('entry_time') or '')
        raw_exit_time = str(trade.get('exit_time') or '')
        entry_time_hhmm = raw_entry_time[11:16] if len(raw_entry_time) >= 16 else raw_entry_time[:5]
        exit_time_hhmm = raw_exit_time[11:16] if len(raw_exit_time) >= 16 else raw_exit_time[:5]

        past_exit = bool(exit_time_hhmm and now_time >= exit_time_hhmm)
        before_entry = bool(entry_time_hhmm and now_time < entry_time_hhmm)

        # ── Live exit_time: cancel pending orders + broker exit — same as manual squareoff ──
        if past_exit:
            try:
                _square_off_trade_like_manual(db, trade, exit_timestamp=now_ts)
                _mark_trade_squared_off_at_exit_time(db, trade_id)
                print(f'[EXIT TIME SQUAREOFF] trade={trade_id} exit_time={exit_time_hhmm}')
                actions_taken.append(f'{trade_id}: exit_time squareoff')
            except Exception as _sqoff_exc:
                log.error('[EXIT TIME SQUAREOFF] trade=%s: %s', trade_id, _sqoff_exc)
            continue

        try:
            lot_size = db.get_lot_size(trade_date, underlying)
        except Exception:
            lot_size = 75

        refreshed_trade = trade
        created_original_legs, refreshed_trade = _queue_original_legs_if_needed(db, refreshed_trade, now_ts)
        if created_original_legs:
            trade = refreshed_trade

        # ── Process momentum-pending legs from algo_leg_feature_status ────────
        if not before_entry:
            _entered_momentum_ids = _process_momentum_pending_feature_legs(
                db, trade, chain_col, trade_date, now_ts, lot_size
            )
            if _entered_momentum_ids:
                trade = db._db['algo_trades'].find_one({'_id': trade_id}) or trade
                actions_taken.extend(
                    f'{trade_id}/{lid}: momentum_pending entered' for lid in _entered_momentum_ids
                )

        trade_record_enriched = _populate_legs_from_history(db, [_serialize_trade_record(trade)])
        enriched_trade = trade_record_enriched[0] if trade_record_enriched else {'legs': [], 'pending_feature_legs': []}
        legs = [leg for leg in (enriched_trade.get('legs') or []) if isinstance(leg, dict)]
        pending_feature_legs = [
            leg for leg in (enriched_trade.get('pending_feature_legs') or [])
            if isinstance(leg, dict)
        ]
        legs.extend(pending_feature_legs)
        _dict_leg_ids_live = {str(l.get('id') or '') for l in legs if l.get('id')}
        _has_str_refs_live = any(isinstance(item, str) for item in (trade.get('legs') or []))
        if _has_str_refs_live:
            _hist_col_live = db._db['algo_trade_positions_history']
            for _hdoc_live in _hist_col_live.find({'trade_id': trade_id}):
                if not isinstance(_hdoc_live.get('entry_trade'), dict) or _hdoc_live.get('exit_trade'):
                    continue
                _hleg_id_live = str(_hdoc_live.get('leg_id') or _hdoc_live.get('id') or '')
                if not _hleg_id_live or _hleg_id_live in _dict_leg_ids_live:
                    continue
                _raw_exp_live = str(_hdoc_live.get('expiry_date') or '')
                _hdoc_live['expiry_date'] = _raw_exp_live[:10] if ' ' in _raw_exp_live else _raw_exp_live
                _hdoc_live['id'] = _hleg_id_live
                _hdoc_live.setdefault('status', OPEN_LEG_STATUS)
                legs.append(_hdoc_live)

        for leg_index, leg in enumerate(legs):
            leg_status = int(leg.get('status') or 0)
            leg_id  = str(leg.get('id') or '')
            leg_cfg = _resolve_leg_cfg(leg_id, leg, all_leg_configs)

            # ── Force-exit all open legs if past exit_time ─────────────────
            if past_exit and (leg_status == OPEN_LEG_STATUS or leg.get('entry_trade')) and not leg.get('exit_trade'):
                # Fetch last price for the exit
                strike = leg.get('strike')
                expiry_date = str(leg.get('expiry_date') or '')
                option_type = str(leg.get('option') or '')
                leg_token = str(leg.get('token') or '')

                exit_price = _resolve_quote_first_option_price(
                    db, trade, leg, activation_mode='live', fallback_price=0.0
                )

                _exit_time_chain_doc: dict = {}
                if exit_price <= 0:
                    _exit_time_chain_doc = get_latest_chain_doc(chain_col, underlying, expiry_date, strike, option_type, trade_date)
                    exit_price = _safe_float(_exit_time_chain_doc.get('close'), leg.get('last_saw_price', 0.0))

                print(
                    f'[EXIT TIME] trade={trade_id} leg={leg_id} token={leg_token} '
                    f'exit_price={exit_price} exit_time={exit_time_hhmm} now={now_time}'
                )
                # Live mode: place broker exit order — do NOT close locally.
                # Close only when broker fill postback arrives → _sync_live_exit_fill.
                try:
                    from features.live_order_manager import (
                        _get_open_exit_orders_for_leg as _get_etime_orders,
                        cancel_open_exit_orders_for_leg as _cancel_etime_orders,
                        place_live_exit_order as _place_etime_order,
                    )
                    if _get_etime_orders(db, trade_id, leg_id):
                        print(f'[EXIT TIME SKIP] trade={trade_id} leg={leg_id} broker_exit_order_already_open')
                    else:
                        _etime_symbol = str(leg.get('symbol') or '')
                        _etime_qty = int(leg.get('quantity') or 0)
                        _leg_for_etime = dict(leg)
                        _leg_for_etime['id'] = leg_id
                        _cancel_etime_orders(db, trade, leg_id, cancel_reason='exit_time_replace')
                        _place_etime_order(
                            db, trade, _leg_for_etime, leg_cfg,
                            _etime_symbol, _etime_qty, exit_price, 'exit_time',
                        )
                        actions_taken.append(f'{trade_id}/{leg_id}: exit_time broker order placed @ {exit_price}')
                        try:
                            from features.notification_manager import record_force_exit, disable_leg_features
                            record_force_exit(db._db, trade, leg, now_ts, exit_price, exit_reason='exit_time')
                            disable_leg_features(db._db, trade_id, leg_id, reason='force_exit', timestamp=now_ts)
                        except Exception as _nfe:
                            log.warning('notification force_exit error: %s', _nfe)
                except Exception as _etime_exc:
                    log.error('[EXIT TIME] broker order error trade=%s leg=%s: %s', trade_id, leg_id, _etime_exc)
                continue

            # ── Skip pending legs before entry_time ───────────────────────
            if before_entry:
                continue

            # ── Try to enter pending (lazy/re-entry) legs ─────────────────
            # Live mode: all entries go through process_momentum_pending_legs
            # (fetch_full_chain → select_strike_live). Never enter directly here.
            is_pending = (leg_status == OPEN_LEG_STATUS
                          and (leg.get('is_lazy') or leg.get('entry_trade') is None))
            if is_pending:
                continue

            # ── Active open legs: SL/TP check ─────────────────────────────
            if not leg.get('entry_trade') or leg.get('exit_trade'):
                continue

            entry_trade = leg.get('entry_trade') or {}
            entry_price = _safe_float(entry_trade.get('price'))
            quantity = _safe_int(leg.get('quantity') or entry_trade.get('quantity'))
            lot_size = _safe_int(leg.get('lot_size'), 1)
            effective_quantity = max(0, quantity) * max(1, lot_size)
            strike = leg.get('strike')
            expiry_date = str(leg.get('expiry_date') or '')
            option_type = str(leg.get('option') or '')
            position_str = str(leg.get('position') or '')
            is_sell_pos = _is_sell(position_str)

            # Fetch latest option_chain
            chain_doc = get_latest_chain_doc(chain_col, underlying, expiry_date, strike, option_type, trade_date)
            current_price = _safe_float(chain_doc.get('close'), _safe_float(leg.get('last_saw_price')))
            current_price = _resolve_quote_first_option_price(
                db, trade, leg, activation_mode='live', fallback_price=current_price
            )
            spot_price = _safe_float(chain_doc.get('spot_price'))
            _live_exit_iv  = _safe_float(chain_doc.get('iv')) or None
            _live_exit_vix: float | None = None
            try:
                from features.trading_core import get_vix_at_time as _get_vix_exit  # type: ignore
                _vix_val = _get_vix_exit(db, now_ts, None)
                _live_exit_vix = _vix_val if _vix_val > 0 else None
            except Exception:
                pass

            sl_config = leg_cfg.get('LegStopLoss') or {}
            tp_config = leg_cfg.get('LegTarget') or {}
            trail_config = sl_config.get('Trail') or {}

            # Compute / update SL price
            initial_sl = _safe_float(leg.get('initial_sl_value')) or calc_sl_price(entry_price, is_sell_pos, sl_config)
            stored_sl = _safe_float(leg.get('current_sl_price')) or None
            sl_price = stored_sl or initial_sl
            if sl_price and trail_config:
                sl_price = update_trail_sl(
                    entry_price,
                    current_price,
                    sl_price,
                    is_sell_pos,
                    trail_config,
                    initial_sl=initial_sl,
                )

            tp_price = calc_tp_price(entry_price, is_sell_pos, tp_config)
            pnl = (
                (entry_price - current_price) * effective_quantity
                if is_sell_pos else
                (current_price - entry_price) * effective_quantity
            )

            token = make_token(underlying, expiry_date, strike, option_type)

            from features.notification_manager import (
                record_sl_hit, record_target_hit,
                record_trail_sl_changed, record_reentry_queued, record_force_exit,
                trigger_leg_feature, disable_leg_features, rotate_trail_sl_record,
            )

            # ── SL hit ────────────────────────────────────────────────────
            if is_sl_hit(current_price, sl_price, is_sell_pos):
                _sl_exit_price = current_price if _is_market_order(leg_cfg, 'exit') else sl_price
                try:
                    from features.live_order_manager import has_open_exit_order as _has_sl_order
                    if _has_sl_order(db, trade_id, leg_id, 'stoploss'):
                        print(
                            f'[LIVE SL SKIP LOCAL CLOSE] trade={trade_id} leg={leg_id} '
                            f'ltp={current_price} sl={sl_price} broker_order_pending'
                        )
                        continue
                except Exception:
                    pass
                close_leg_in_db(db, trade_id, leg_index, _sl_exit_price, 'stoploss', now_ts, leg_id=leg_id, activation_mode='live', exit_iv=_live_exit_iv, exit_vix=_live_exit_vix)
                actions_taken.append(f'{trade_id}/{leg_id}: SL hit @ {_sl_exit_price} (ltp={current_price})')
                record_sl_hit(db._db, trade, leg, now_ts, _sl_exit_price, sl_price or 0.0)
                # Feature status: mark sl=triggered, disable target+trailSL
                trigger_leg_feature(db._db, trade_id, leg_id, 'sl', _sl_exit_price, now_ts)
                disable_leg_features(db._db, trade_id, leg_id, except_feature='sl', reason='sl_triggered', timestamp=now_ts)
                reentry_cfg = get_reentry_sl_config(leg_cfg)
                if reentry_cfg:
                    leg_cfg_with_id = dict(leg_cfg)
                    leg_cfg_with_id['id'] = leg_id
                    result = _handle_reentry(db, trade, leg_cfg_with_id, reentry_cfg, leg_id, now_ts)
                    if result:
                        actions_taken.append(f'{trade_id}/{leg_id}: {result}')
                        re_type = str(reentry_cfg.get('Type') or '')
                        re_kind = ('lazy' if 'NextLeg' in re_type else
                                   'immediate' if 'Immediate' in re_type else
                                   'at_cost' if 'AtCost' in re_type else 'like_original')
                        new_leg_id = str((reentry_cfg.get('Value') or {}).get('NextLegRef') or '')
                        record_reentry_queued(db._db, trade, now_ts, leg_id, re_kind, new_leg_id, re_type, reason='sl')
                        if 'AtCost' in re_type:
                            try:
                                from features.notification_manager import upsert_recost_feature_status
                                _cost_price = _safe_float((leg.get('entry_trade') or {}).get('price'))
                                _reentry_id = f'{leg_id}_re_{now_ts.replace(":", "").replace("T", "")}'
                                upsert_recost_feature_status(
                                    db._db, trade, leg, now_ts,
                                    cost_price=_cost_price,
                                    exit_price=current_price,
                                    status='pending',
                                    recost_leg_id=_reentry_id,
                                )
                            except Exception as _rce:
                                log.warning('recost feature status armed error leg=%s: %s', leg_id, _rce)
                continue  # leg is now closed

            # ── TP hit ────────────────────────────────────────────────────
            if is_tp_hit(current_price, tp_price, is_sell_pos):
                _tp_exit_price = current_price if _is_market_order(leg_cfg, 'exit') else tp_price
                try:
                    from features.live_order_manager import has_open_exit_order as _has_tp_order
                    if _has_tp_order(db, trade_id, leg_id, 'target'):
                        print(
                            f'[LIVE TP SKIP LOCAL CLOSE] trade={trade_id} leg={leg_id} '
                            f'ltp={current_price} tp={tp_price} broker_order_pending'
                        )
                        continue
                except Exception:
                    pass
                close_leg_in_db(db, trade_id, leg_index, _tp_exit_price, 'target', now_ts, leg_id=leg_id, activation_mode='live', exit_iv=_live_exit_iv, exit_vix=_live_exit_vix)
                actions_taken.append(f'{trade_id}/{leg_id}: TP hit @ {_tp_exit_price} (ltp={current_price})')
                record_target_hit(db._db, trade, leg, now_ts, _tp_exit_price, tp_price or 0.0)
                # Feature status: mark target=triggered, disable sl+trailSL
                trigger_leg_feature(db._db, trade_id, leg_id, 'target', _tp_exit_price, now_ts)
                disable_leg_features(db._db, trade_id, leg_id, except_feature='target', reason='target_triggered', timestamp=now_ts)
                reentry_cfg = get_reentry_tp_config(leg_cfg)
                if reentry_cfg:
                    leg_cfg_with_id = dict(leg_cfg)
                    leg_cfg_with_id['id'] = leg_id
                    result = _handle_reentry(db, trade, leg_cfg_with_id, reentry_cfg, leg_id, now_ts)
                    if result:
                        actions_taken.append(f'{trade_id}/{leg_id}: {result}')
                        re_type = str(reentry_cfg.get('Type') or '')
                        re_kind = ('lazy' if 'NextLeg' in re_type else
                                   'immediate' if 'Immediate' in re_type else
                                   'at_cost' if 'AtCost' in re_type else 'like_original')
                        new_leg_id = str((reentry_cfg.get('Value') or {}).get('NextLegRef') or '')
                        record_reentry_queued(db._db, trade, now_ts, leg_id, re_kind, new_leg_id, re_type, reason='target')
                        if 'AtCost' in re_type:
                            try:
                                from features.notification_manager import upsert_recost_feature_status
                                _cost_price = _safe_float((leg.get('entry_trade') or {}).get('price'))
                                _reentry_id = f'{leg_id}_re_{now_ts.replace(":", "").replace("T", "")}'
                                upsert_recost_feature_status(
                                    db._db, trade, leg, now_ts,
                                    cost_price=_cost_price,
                                    exit_price=current_price,
                                    status='pending',
                                    recost_leg_id=_reentry_id,
                                )
                            except Exception as _rce:
                                log.warning('recost feature status armed error leg=%s: %s', leg_id, _rce)
                continue  # leg is now closed

            # ── Trail SL moved — rotate feature records + modify broker order ──
            if sl_price and sl_price != stored_sl:
                update_leg_sl_in_db(db, trade_id, leg_index, sl_price, current_price, leg_id=leg_id)
                if stored_sl and sl_price != stored_sl:
                    print(
                        f'[TRAIL SL CHANGED] trade={trade_id} leg={leg_id} '
                        f'old_sl={stored_sl} new_sl={sl_price} ltp={current_price} ts={now_ts}'
                    )
                    record_trail_sl_changed(
                        db._db, trade, leg, now_ts, stored_sl, sl_price, current_price,
                        trail_config=trail_config,
                    )
                    # Feature status: disable old SL+trailSL records, insert fresh
                    # pending records with the updated SL price (step-by-step audit trail)
                    rotate_trail_sl_record(
                        db._db, trade_id, leg_id,
                        old_sl_price=stored_sl,
                        new_sl_price=sl_price,
                        current_option_price=current_price,
                        trail_config=trail_config,
                        timestamp=now_ts,
                    )
                    # Modify the live broker SL order to reflect the new trail SL price
                    try:
                        from features.live_order_manager import modify_broker_sl_order
                        modify_broker_sl_order(db, trade_id, leg_id, sl_price)
                    except Exception as _tsl_mod_e:
                        log.warning('live trail SL broker modify error trade=%s leg=%s: %s', trade_id, leg_id, _tsl_mod_e)

            # ── Build position snapshot ────────────────────────────────────
            position_payload = {
                'strategy_id': str(trade.get('strategy_id') or ''),
                'strategy_name': str(trade.get('name') or ''),
                'trade_id': trade_id,
                'leg_id': leg_id,
                'symbol': str(leg.get('symbol') or token),
                'token': token,
                'position': position_str,
                'option': option_type,
                'strike': strike,
                'expiry_date': expiry_date,
                'entry_price': entry_price,
                'current_price': current_price,
                'quantity': effective_quantity,
                'lots': quantity,
                'lot_size': lot_size,
                'pnl': round(pnl, 2),
                'sl_price': sl_price,
                'tp_price': tp_price,
                'entry_timestamp': str(entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or ''),
                'snapshot': {
                    'timestamp': chain_doc.get('timestamp') or now_ts,
                    'underlying': chain_doc.get('underlying') or underlying,
                    'expiry': chain_doc.get('expiry') or expiry_date,
                    'strike': chain_doc.get('strike', strike),
                    'type': chain_doc.get('type') or option_type,
                    'close': current_price,
                    'oi': chain_doc.get('oi'),
                    'iv': chain_doc.get('iv'),
                    'delta': chain_doc.get('delta'),
                    'gamma': chain_doc.get('gamma'),
                    'theta': chain_doc.get('theta'),
                    'vega': chain_doc.get('vega'),
                    'rho': chain_doc.get('rho'),
                    'spot_price': spot_price,
                },
            }
            open_positions.append(position_payload)

            strat_entry = strategy_map.setdefault(trade_id, {
                'trade_id': trade_id,
                'strategy_id': str(trade.get('strategy_id') or ''),
                'strategy_name': str(trade.get('name') or ''),
                'ticker': underlying,
                'entry_time': entry_time_hhmm,
                'exit_time': exit_time_hhmm,
                'open_positions': [],
                'total_pnl': 0.0,
            })
            strat_entry['open_positions'].append(position_payload)
            strat_entry['total_pnl'] = round(strat_entry['total_pnl'] + pnl, 2)

        # ── Overall SL / Target / Trail SL check (per strategy) ───────────
        if not past_exit:
            trade_strat_entry = strategy_map.get(trade_id)
            strategy_cfg = trade.get('strategy') or cfg
            osl_type, osl_val = parse_overall_sl(strategy_cfg)
            otgt_type, otgt_val = parse_overall_tgt(strategy_cfg)

            current_trade_mtm, legs_pnl_snapshot = _compute_strategy_mtm_snapshot(
                db,
                trade_id,
                now_ts,
                (trade_strat_entry or {}).get('open_positions', []),
            )
            # Leg IDs still open (not yet closed by individual SL/TP above)
            open_leg_ids = {p['leg_id'] for p in (trade_strat_entry or {}).get('open_positions', [])}

            def _close_remaining_open_legs(exit_reason: str) -> None:
                from features.live_order_manager import (
                    _get_open_exit_orders_for_leg as _get_rem_orders,
                    cancel_open_exit_orders_for_leg as _cancel_rem_orders,
                    place_live_exit_order as _place_rem_order,
                )
                for _leg in legs:
                    if str(_leg.get('id') or '') not in open_leg_ids:
                        continue
                    if int(_leg.get('status') or 0) != OPEN_LEG_STATUS:
                        continue
                    _leg_id = str(_leg.get('id') or '')
                    _cdoc = get_latest_chain_doc(
                        chain_col, underlying,
                        str(_leg.get('expiry_date') or ''),
                        _leg.get('strike'),
                        str(_leg.get('option') or ''),
                        trade_date,
                    )
                    _ep = _safe_float(_cdoc.get('close'), _leg.get('last_saw_price', 0.0))
                    # Live mode: place broker exit order — do NOT close locally.
                    if _get_rem_orders(db, trade_id, _leg_id):
                        print(f'[CLOSE REMAINING SKIP] trade={trade_id} leg={_leg_id} reason={exit_reason} broker_exit_order_already_open')
                        continue
                    _leg_cfg_rem = _resolve_leg_cfg(_leg_id, _leg, all_leg_configs)
                    _leg_for_rem = dict(_leg)
                    _leg_for_rem['id'] = _leg_id
                    _cancel_rem_orders(db, trade, _leg_id, cancel_reason=f'{exit_reason}_replace')
                    try:
                        _place_rem_order(
                            db, trade, _leg_for_rem, _leg_cfg_rem,
                            str(_leg.get('symbol') or ''), int(_leg.get('quantity') or 0),
                            _ep, exit_reason,
                        )
                    except Exception as _place_exc:
                        log.error('[CLOSE REMAINING] broker order error trade=%s leg=%s reason=%s: %s', trade_id, _leg_id, exit_reason, _place_exc)
                    try:
                        from features.notification_manager import record_force_exit, disable_leg_features
                        record_force_exit(db._db, trade, _leg, now_ts, _ep, exit_reason=exit_reason)
                        disable_leg_features(db._db, trade_id, _leg_id, reason=exit_reason, timestamp=now_ts)
                    except Exception as _nfe:
                        log.warning('notification force_exit(%s) error: %s', exit_reason, _nfe)

            # Track peak MTM for trail/lock features
            _peak_mtm = _safe_float(trade.get('peak_mtm'), current_trade_mtm)
            _new_peak = max(_peak_mtm, current_trade_mtm)

            # ── Helper: re-queue all original legs (OverallReentry) ───────────
            def _requeue_original_legs(reason: str) -> list[str]:
                """Push original leg configs back as pending legs. Returns new leg IDs."""
                _original_cfgs = strategy_cfg.get('ListOfLegConfigs') or []
                if not _original_cfgs:
                    _original_cfgs = list((_resolve_trade_leg_configs(trade) or {}).values())
                _new_ids: list[str] = []
                _existing_ids = {
                    str(l.get('id') or '') for l in (trade.get('legs') or []) if isinstance(l, dict)
                }
                _ts_sfx = now_ts.replace(':', '').replace('T', '').replace('-', '')[:14]
                for _lc in _original_cfgs:
                    if not isinstance(_lc, dict):
                        continue
                    _orig_id = str(_lc.get('id') or '')
                    if not _orig_id:
                        continue
                    _new_id  = f'{_orig_id}-ore-{_ts_sfx}'
                    if _new_id in _existing_ids:
                        continue
                    _new_leg = _build_pending_leg(
                        _new_id,
                        _lc,
                        trade,
                        now_ts,
                        f'overall_{reason}_{_orig_id}',
                        leg_type=f'{_orig_id}-overall_reentry',
                    )
                    _new_leg.update({
                        'is_lazy': True,
                        'is_reentered_leg': True,
                        'lazy_leg_ref': _orig_id,
                        'parent_leg_id': _orig_id,
                        # Overall reentry must enter immediately — skip LegMomentum gate.
                        'skip_momentum_check': True,
                    })
                    try:
                        db._db['algo_trades'].update_one(
                            {'_id': trade_id}, {'$push': {'legs': _new_leg}}
                        )
                        _new_ids.append(_new_id)
                        mark_execute_order_dirty_from_trade(trade)
                    except Exception as _pe:
                        log.warning('overall reentry push error: %s', _pe)
                return _new_ids

            # ── Check overall SL ──────────────────────────────────────────────
            _ore_type, _ore_count = parse_overall_reentry_sl(strategy_cfg)
            _ort_type, _ort_count = parse_overall_reentry_tgt(strategy_cfg)
            _ore_done = int(trade.get('overall_sl_reentry_done') or 0)
            _ort_done = int(trade.get('overall_tgt_reentry_done') or 0)
            _effective_sl = _resolve_overall_cycle_value(osl_val, _ore_done)
            _effective_tgt = _resolve_overall_cycle_value(otgt_val, _ort_done)
            _osl_triggered = (
                osl_type != 'None'
                and _safe_float(_effective_sl) > 0
                and current_trade_mtm <= -_safe_float(_effective_sl)
            )
            _last_overall_event_at = str(trade.get('last_overall_event_at') or '').strip().replace(' ', 'T')[:19]
            _last_overall_event_reason = str(trade.get('last_overall_event_reason') or '').strip()

            # Also check dynamic SL threshold from overall trail SL
            _dyn_sl_threshold = _safe_float(
                trade.get('current_overall_sl_threshold'),
                _effective_sl or osl_val,
            )
            _skip_same_tick_overall = bool(
                open_leg_ids
                and _last_overall_event_at
                and _last_overall_event_at == str(now_ts or '').strip()[:19]
                and _last_overall_event_reason in {'overall_sl', 'overall_target'}
            )
            _sync_overall_strategy_feature_status(
                db, trade, now_ts,
                current_mtm=current_trade_mtm,
                overall_sl_done=_ore_done,
                overall_tgt_done=_ort_done,
            )
            print(
                '[OVERALL CHECK]',
                {
                    'mode': 'live',
                    'trade_id': trade_id,
                    'strategy_name': str(trade.get('name') or ''),
                    'timestamp': now_ts,
                    'current_mtm': round(current_trade_mtm, 2),
                    'overall_sl_type': osl_type,
                    'overall_sl_base': round(_safe_float(osl_val), 2),
                    'overall_sl_current': _effective_sl,
                    'overall_sl_reentry_type': _ore_type,
                    'overall_sl_reentry_done': _ore_done,
                    'overall_sl_reentry_count': _ore_count,
                    'overall_tgt_type': otgt_type,
                    'overall_tgt_base': round(_safe_float(otgt_val), 2),
                    'overall_tgt_current': _effective_tgt,
                    'overall_tgt_reentry_type': _ort_type,
                    'overall_tgt_reentry_done': _ort_done,
                    'overall_tgt_reentry_count': _ort_count,
                    'dynamic_sl_threshold': round(_safe_float(_dyn_sl_threshold), 2),
                },
            )
            print(
                '[OVERALL SL DEBUG]',
                {
                    'mode': 'live',
                    'trade_id': trade_id,
                    'strategy_name': str(trade.get('name') or ''),
                    'timestamp': now_ts,
                    'current_mtm': round(current_trade_mtm, 2),
                    'base_overall_sl': round(_safe_float(osl_val), 2),
                    'reentry_done': _ore_done,
                    'cycle_number': _ore_done + 1,
                    'cycle_overall_sl': round(_safe_float(_effective_sl), 2),
                    'dynamic_sl_threshold': round(_safe_float(_dyn_sl_threshold), 2),
                    'checked_stoploss': round(_safe_float(_dyn_sl_threshold or _effective_sl), 2),
                    'would_hit_cycle_sl': bool(_safe_float(_effective_sl) > 0 and current_trade_mtm <= -_safe_float(_effective_sl)),
                    'would_hit_dynamic_sl': bool(_dyn_sl_threshold > 0 and current_trade_mtm <= -_dyn_sl_threshold),
                    'skip_same_tick_overall': _skip_same_tick_overall,
                    'last_overall_event_at': _last_overall_event_at,
                    'last_overall_event_reason': _last_overall_event_reason,
                },
            )
            if _skip_same_tick_overall:
                actions_taken.append(
                    f'{trade_id}: skipped same-tick overall recheck after {_last_overall_event_reason}'
                )
                continue
            if not _osl_triggered and _dyn_sl_threshold > 0 and current_trade_mtm <= -_dyn_sl_threshold:
                _osl_triggered = True

            # Guard: if no legs are open, these events have already fired — skip.
            if _osl_triggered and not open_leg_ids:
                _osl_triggered = False

            if _osl_triggered:
                db._db['algo_trades'].update_one(
                    {'_id': trade_id},
                    {'$set': {
                        'last_overall_event_at': now_ts,
                        'last_overall_event_reason': 'overall_sl',
                    }},
                )
                mark_execute_order_dirty_from_trade(trade)
                _set_overall_feature_status_state(
                    db, trade_id, 'overall_sl',
                    enabled=False, status='triggered', timestamp=now_ts,
                    current_mtm=current_trade_mtm, disabled_reason='overall_sl_hit',
                )
                _set_overall_feature_status_state(
                    db, trade_id, 'overall_target',
                    enabled=False, status='disabled', timestamp=now_ts,
                    current_mtm=current_trade_mtm, disabled_reason='overall_sl_hit',
                )
                _disable_trade_feature_rows_for_new_cycle(
                    db,
                    trade_id,
                    reason='overall_sl_cycle_completed',
                    timestamp=now_ts,
                )
                try:
                    from features.notification_manager import disable_all_trade_notifications
                    disable_all_trade_notifications(db._db, trade_id, reason='overall_sl', timestamp=now_ts)
                except Exception as _e:
                    log.warning('disable_all_trade_notifications overall_sl error: %s', _e)
                _close_remaining_open_legs('overall_sl')
                actions_taken.append(f'{trade_id}: overall SL hit mtm={current_trade_mtm}')
                try:
                    from features.notification_manager import record_overall_sl_hit
                    record_overall_sl_hit(
                        db._db, trade, now_ts,
                        osl_type, _effective_sl, current_trade_mtm, legs_pnl_snapshot,
                        cycle_number=_ore_done + 1,
                        configured_reentry_count=_ore_count,
                    )
                except Exception as _e:
                    log.warning('notification overall_sl_hit error: %s', _e)
                # ── OverallReentrySL ─────────────────────────────────────────
                if _ore_type != 'None' and _ore_count > 0 and _ore_done < _ore_count:
                    _new_leg_ids = _requeue_original_legs('sl')
                    if _new_leg_ids:
                        _next_cycle_sl = _resolve_overall_cycle_value(osl_val, _ore_done + 1)
                        db._db['algo_trades'].update_one(
                            {'_id': trade_id},
                            {'$set': {
                                'overall_sl_reentry_done': _ore_done + 1,
                                'peak_mtm': 0.0,
                                'current_overall_sl_threshold': _next_cycle_sl,
                            }},
                        )
                        mark_execute_order_dirty_from_trade(trade)
                        _sync_overall_strategy_feature_status(
                            db, trade, now_ts,
                            current_mtm=current_trade_mtm,
                            overall_sl_done=_ore_done + 1,
                            overall_tgt_done=_ort_done,
                        )
                        actions_taken.append(
                            f'{trade_id}: OverallReentrySL queued legs={_new_leg_ids}'
                        )
                        print(
                            '[OVERALL REENTRY QUEUED]',
                            {
                                'mode': 'live',
                                'trade_id': trade_id,
                                'reason': 'overall_sl',
                                'reentry_type': _ore_type,
                                'reentry_done': _ore_done + 1,
                                'reentry_count': _ore_count,
                                'next_cycle_number': _ore_done + 2,
                                'next_overall_sl_value': _resolve_next_overall_cycle_value(osl_val, _ore_done),
                                'next_overall_tgt_value': _effective_tgt,
                                'legs_queued': _new_leg_ids,
                            },
                        )
                        try:
                            from features.notification_manager import record_overall_reentry_queued
                            record_overall_reentry_queued(
                                db._db, trade, now_ts, reason='overall_sl',
                                reentry_type=_ore_type, count=_ore_count,
                                legs_queued=_new_leg_ids,
                                completed_reentries=_ore_done + 1,
                                next_cycle_number=_ore_done + 2,
                                next_overall_sl_value=_resolve_next_overall_cycle_value(osl_val, _ore_done),
                                next_overall_tgt_value=_effective_tgt,
                            )
                        except Exception as _e:
                            log.warning('notification overall_reentry_queued error: %s', _e)
                    else:
                        _square_off_trade_like_manual(
                            db,
                            db._db['algo_trades'].find_one({'_id': trade_id}) or trade,
                            exit_timestamp=now_ts,
                        )
                else:
                    _square_off_trade_like_manual(
                        db,
                        db._db['algo_trades'].find_one({'_id': trade_id}) or trade,
                        exit_timestamp=now_ts,
                    )

            elif (
                open_leg_ids
                and otgt_type != 'None'
                and _safe_float(_effective_tgt) > 0
                and current_trade_mtm >= _safe_float(_effective_tgt)
            ):
                db._db['algo_trades'].update_one(
                    {'_id': trade_id},
                    {'$set': {
                        'last_overall_event_at': now_ts,
                        'last_overall_event_reason': 'overall_target',
                    }},
                )
                mark_execute_order_dirty_from_trade(trade)
                _set_overall_feature_status_state(
                    db, trade_id, 'overall_target',
                    enabled=False, status='triggered', timestamp=now_ts,
                    current_mtm=current_trade_mtm, disabled_reason='overall_target_hit',
                )
                _set_overall_feature_status_state(
                    db, trade_id, 'overall_sl',
                    enabled=False, status='disabled', timestamp=now_ts,
                    current_mtm=current_trade_mtm, disabled_reason='overall_target_hit',
                )
                _disable_trade_feature_rows_for_new_cycle(
                    db,
                    trade_id,
                    reason='overall_target_cycle_completed',
                    timestamp=now_ts,
                )
                try:
                    from features.notification_manager import disable_all_trade_notifications
                    disable_all_trade_notifications(db._db, trade_id, reason='overall_target', timestamp=now_ts)
                except Exception as _e:
                    log.warning('disable_all_trade_notifications overall_target error: %s', _e)
                _close_remaining_open_legs('overall_target')
                actions_taken.append(f'{trade_id}: overall Target hit mtm={current_trade_mtm}')
                try:
                    from features.notification_manager import record_overall_target_hit
                    record_overall_target_hit(
                        db._db, trade, now_ts,
                        otgt_type, _effective_tgt, current_trade_mtm, legs_pnl_snapshot,
                        cycle_number=_ort_done + 1,
                        configured_reentry_count=_ort_count,
                    )
                except Exception as _e:
                    log.warning('notification overall_tgt_hit error: %s', _e)
                # ── OverallReentryTgt ────────────────────────────────────────
                if _ort_type != 'None' and _ort_count > 0 and _ort_done < _ort_count:
                    _new_leg_ids = _requeue_original_legs('target')
                    if _new_leg_ids:
                        _reset_sl_threshold = _resolve_overall_cycle_value(osl_val, _ore_done)
                        db._db['algo_trades'].update_one(
                            {'_id': trade_id},
                            {'$set': {
                                'overall_tgt_reentry_done': _ort_done + 1,
                                'peak_mtm': 0.0,
                                'current_overall_sl_threshold': _reset_sl_threshold,
                            }},
                        )
                        mark_execute_order_dirty_from_trade(trade)
                        _sync_overall_strategy_feature_status(
                            db, trade, now_ts,
                            current_mtm=current_trade_mtm,
                            overall_sl_done=_ore_done,
                            overall_tgt_done=_ort_done + 1,
                        )
                        actions_taken.append(
                            f'{trade_id}: OverallReentryTgt queued legs={_new_leg_ids}'
                        )
                        print(
                            '[OVERALL REENTRY QUEUED]',
                            {
                                'mode': 'live',
                                'trade_id': trade_id,
                                'reason': 'overall_target',
                                'reentry_type': _ort_type,
                                'reentry_done': _ort_done + 1,
                                'reentry_count': _ort_count,
                                'next_cycle_number': _ort_done + 2,
                                'next_overall_sl_value': _effective_sl,
                                'next_overall_tgt_value': _resolve_next_overall_cycle_value(otgt_val, _ort_done),
                                'legs_queued': _new_leg_ids,
                            },
                        )
                        try:
                            from features.notification_manager import record_overall_reentry_queued
                            record_overall_reentry_queued(
                                db._db, trade, now_ts, reason='overall_target',
                                reentry_type=_ort_type, count=_ort_count,
                                legs_queued=_new_leg_ids,
                                completed_reentries=_ort_done + 1,
                                next_cycle_number=_ort_done + 2,
                                next_overall_sl_value=_effective_sl,
                                next_overall_tgt_value=_resolve_next_overall_cycle_value(otgt_val, _ort_done),
                            )
                        except Exception as _e:
                            log.warning('notification overall_reentry_tgt_queued error: %s', _e)
                    else:
                        _square_off_trade_like_manual(
                            db,
                            db._db['algo_trades'].find_one({'_id': trade_id}) or trade,
                            exit_timestamp=now_ts,
                        )
                else:
                    _square_off_trade_like_manual(
                        db,
                        db._db['algo_trades'].find_one({'_id': trade_id}) or trade,
                        exit_timestamp=now_ts,
                    )

            elif open_leg_ids:
                # ── LockAndTrail ─────────────────────────────────────────────
                _lock_cfg = parse_lock_and_trail(strategy_cfg)
                _lock_exit, _lock_floor = check_lock_and_trail(_lock_cfg, current_trade_mtm, _new_peak)
                if _lock_exit:
                    _close_remaining_open_legs('lock_and_trail')
                    actions_taken.append(
                        f'{trade_id}: LockAndTrail exit mtm={current_trade_mtm} floor={_lock_floor}'
                    )
                    try:
                        from features.notification_manager import record_lock_and_trail_exit
                        record_lock_and_trail_exit(
                            db._db, trade, now_ts,
                            floor=_lock_floor, current_mtm=current_trade_mtm,
                            legs_pnl=legs_pnl_snapshot,
                        )
                    except Exception as _e:
                        log.warning('notification lock_and_trail_exit error: %s', _e)

                else:
                    # ── Overall Trail SL ──────────────────────────────────────
                    trail_type, for_every, trail_by = parse_overall_trail_sl(strategy_cfg)
                    if trail_type != 'None' and for_every > 0:
                        initial_osl = osl_val
                        old_sl_threshold = _safe_float(
                            trade.get('current_overall_sl_threshold'), initial_osl
                        )
                        new_sl_threshold = update_overall_trail_sl(
                            for_every, trail_by, initial_osl, _new_peak
                        )
                        if new_sl_threshold < old_sl_threshold:
                            db._db['algo_trades'].update_one(
                                {'_id': trade_id},
                                {'$set': {
                                    'peak_mtm': _new_peak,
                                    'current_overall_sl_threshold': new_sl_threshold,
                                }},
                            )
                            actions_taken.append(
                                f'{trade_id}: overall trail SL {old_sl_threshold}->{new_sl_threshold}'
                            )
                            try:
                                from features.notification_manager import record_overall_trail_sl_changed
                                record_overall_trail_sl_changed(
                                    db._db, trade, now_ts,
                                    old_sl_threshold, new_sl_threshold, _new_peak, current_trade_mtm,
                                )
                            except Exception as _e:
                                log.warning('notification overall_trail_sl_changed error: %s', _e)
                        elif _new_peak > _peak_mtm:
                            db._db['algo_trades'].update_one(
                                {'_id': trade_id},
                                {'$set': {'peak_mtm': _new_peak}},
                            )
                    elif _new_peak > _peak_mtm:
                        # Update peak even without trail SL (needed for LockAndTrail)
                        db._db['algo_trades'].update_one(
                            {'_id': trade_id},
                            {'$set': {'peak_mtm': _new_peak}},
                        )
        else:
            _mark_trade_squared_off_at_exit_time(db, trade_id)

    return {
        'trade_date': trade_date,
        'checked_at': now_ts,
        'current_ist_time': now_time,
        'running_strategies': list(strategy_map.values()),
        'open_positions': open_positions,
        'actions_taken': actions_taken,
        'position_entry_events': position_entry_events,
        'total_running': len(running_trades),
        'total_open_legs': len(open_positions),
    }



# ─── Sync already-entered legs to history ────────────────────────────────────

def _sync_entered_legs_to_history(db: MongoData, records: list[dict]) -> list[dict]:
    """
    For trades whose legs already have entry_trade data (imported/pre-filled),
    store each entered leg to algo_trade_positions_history and push _id to algo_trades.legs.
    Idempotent — duplicate entries are skipped by _store_position_history.
    Returns list of synced entries.
    """
    synced: list[dict] = []
    for record in (records or []):
        trade_id = str(record.get('_id') or '')
        if not trade_id:
            continue
        trade = db._db['algo_trades'].find_one({'_id': trade_id})
        if not trade:
            continue
        legs = [leg for leg in (trade.get('legs') or []) if isinstance(leg, dict)]
        for leg in legs:
            entry_trade = leg.get('entry_trade')
            if not isinstance(entry_trade, dict) or not entry_trade:
                continue  # not yet entered
            leg_id_val = str(leg.get('id') or '')
            inserted, history_doc = _store_position_history(db, trade, leg)
            # Always remove the original dict leg — replaced by string history _id
            if leg_id_val:
                try:
                    db._db['algo_trades'].update_one(
                        {'_id': trade_id},
                        {'$pull': {'legs': {'id': leg_id_val}}},
                    )
                except Exception as exc:
                    log.error('_sync_entered_legs_to_history pull dict leg error trade=%s leg=%s: %s', trade_id, leg_id_val, exc)
            if inserted and history_doc:
                print(
                    f'[HISTORY SYNC] trade={trade_id} '
                    f'leg={history_doc["leg_id"]} '
                    f'leg_type={leg.get("leg_type") or ""} '
                    f'entry={history_doc["entry_timestamp"]} '
                    f'history_id={history_doc["_id"]}'
                )
                synced.append({
                    'trade_id': trade_id,
                    'leg_id': history_doc['leg_id'],
                    'history_id': history_doc['_id'],
                    'entry_timestamp': history_doc['entry_timestamp'],
                })
        _validate_trade_leg_storage(db, trade_id)
    return synced


# ─── Backtest entry executor ──────────────────────────────────────────────────

def _execute_backtest_entries(
    db: MongoData,
    records: list[dict],
    listen_time: str,
    listen_timestamp: str,
    market_cache: dict | None = None,
) -> list[dict]:
    """
    For each strategy whose entry_time matches listen_time, ensure legs are
    queued and attempt to fill any pending (not yet entered) legs.
    Returns a list of dicts describing each entry action taken.
    """
    trade_date = listen_timestamp[:10] if listen_timestamp and len(listen_timestamp) >= 10 else ''
    chain_col = db._db[OPTION_CHAIN_COLLECTION]
    entries_executed: list[dict] = []

    def _extract_time_hhmmss(value: Any) -> str:
        raw = str(value or '').strip()
        if not raw:
            return ''
        if 'T' in raw:
            raw = raw.split('T', 1)[1]
        elif ' ' in raw:
            raw = raw.split(' ', 1)[1]
        return raw[:8]

    listen_time_hhmm = str(listen_time or '').strip()[:5]
    listen_time_hhmmss = _extract_time_hhmmss(listen_timestamp) or (
        f"{listen_time_hhmm}:00" if listen_time_hhmm else ''
    )

    for record in (records or []):
        record_entry_time = _extract_hhmm(record.get('entry_time') or '')
        record_entry_time_hhmmss = _extract_time_hhmmss(record.get('entry_time') or '')
        trade_id = str(record.get('_id') or '')
        if not trade_id:
            continue

        if record_entry_time_hhmmss and len(record_entry_time_hhmmss) >= 8 and listen_time_hhmmss:
            entry_time_match = (record_entry_time_hhmmss == listen_time_hhmmss)
            entry_time_past = (listen_time_hhmmss > record_entry_time_hhmmss)
        else:
            entry_time_match = (record_entry_time == listen_time_hhmm)
            entry_time_past = bool(
                record_entry_time
                and listen_time_hhmm
                and listen_time_hhmm > record_entry_time
            )

        # Catch-up: entry time already passed and trade has never been entered
        # (e.g. strategy activated while listen_time is ahead of entry_time)
        if entry_time_past and not entry_time_match:
            # Check if any legs have been entered for this trade in history
            try:
                history_col = db._db['algo_trade_positions_history']
                already_entered = history_col.count_documents({
                    'trade_id': trade_id,
                    'entry_trade': {'$exists': True, '$ne': None},
                }) > 0
            except Exception:
                already_entered = True  # safe: don't double-enter on error
            if already_entered:
                continue
            # No legs entered yet but listen_time > entry_time → instant catch-up entry
            catch_up = True
        else:
            catch_up = False

        if not entry_time_match and not catch_up:
            continue

        print(
            f'[BACKTEST ENTRY TRIGGER] trade_id={trade_id} '
            f'group_name={record.get("group_name") or ""} '
            f'strategy_name={record.get("name") or ""} '
            f'entry_time={(record_entry_time_hhmmss or record_entry_time or "--:--")} '
            f'listen_time={(listen_time_hhmmss or listen_time_hhmm)} '
            f'triggering_previous_entry_execution=true'
            + (' catch_up=true' if catch_up else '')
        )

        # Re-fetch latest trade state from DB
        trade = db._db['algo_trades'].find_one({'_id': trade_id})
        if not trade:
            continue

        # Ensure original legs exist in DB
        created, trade = _queue_original_legs_if_needed(db, trade, listen_timestamp)
        if created:
            print(
                f'[BACKTEST ENTRY] trade_id={trade_id} '
                f'strategy={record.get("name") or ""} '
                f'entry_time={(record_entry_time_hhmmss or record_entry_time or "--:--")} '
                'original_legs_queued=true'
            )

        underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        preview_listen_time = record_entry_time or listen_time_hhmm
        preview_snapshots = build_entry_spot_snapshots(
            db,
            [record],
            preview_listen_time,
            listen_timestamp,
            market_cache=market_cache,
        )
        for snapshot in preview_snapshots:
            print(
                '[BACKTEST ENTRY SNAPSHOT] '
                f'trade_id={trade_id} '
                f'strategy_name={snapshot.get("strategy_name") or ""} '
                f'underlying={snapshot.get("underlying") or ""} '
                f'spot_price={snapshot.get("spot_price") or 0} '
                f'atm_price={snapshot.get("atm_price") or 0}'
            )
            for option_chain in snapshot.get('option_chain') or []:
                print(
                    '[BACKTEST ENTRY SNAPSHOT] '
                    f'leg_id={option_chain.get("leg_id") or ""} '
                    f'position={option_chain.get("position") or ""} '
                    f'expiry_kind={option_chain.get("expiry_kind") or ""} '
                    f'expiry={option_chain.get("expiry") or ""} '
                    f'option_type={option_chain.get("option_type") or ""} '
                    f'strike={option_chain.get("strike") or 0} '
                    f'close={option_chain.get("close") or 0} '
                    f'timestamp={option_chain.get("timestamp") or ""}'
                )

        try:
            lot_size = db.get_lot_size(trade_date, underlying)
        except Exception:
            lot_size = 75

        trade_record_enriched = _populate_legs_from_history(db, [_serialize_trade_record(trade)])
        enriched_trade = trade_record_enriched[0] if trade_record_enriched else {'legs': [], 'pending_feature_legs': []}
        legs = [leg for leg in (trade.get('legs') or []) if isinstance(leg, dict)]
        pending_feature_legs = [
            leg for leg in (enriched_trade.get('pending_feature_legs') or [])
            if isinstance(leg, dict)
        ]
        # For strict-history modes (fast-forward / live), original legs are bare configs
        # — no spot_at_queue / token / strike resolved.  The enriched pending_feature_leg
        # from DB has all that data.  Replace matching original legs so _try_enter_pending_leg
        # gets the enriched version with spot_at_queue already populated.
        _trade_activation_mode = str(trade.get('activation_mode') or '').strip()
        if _is_strict_history_leg_mode(_trade_activation_mode):
            pending_by_id = {str(pl.get('id') or ''): pl for pl in pending_feature_legs if isinstance(pl, dict)}
            legs = [pending_by_id.get(str(leg.get('id') or ''), leg) for leg in legs]
            for pending_leg in pending_feature_legs:
                pid = str(pending_leg.get('id') or '')
                if any(str(l.get('id') or '') == pid for l in legs):
                    continue
                legs.append(pending_leg)
        else:
            existing_pending_ids = {str(leg.get('id') or '') for leg in legs if isinstance(leg, dict)}
            for pending_leg in pending_feature_legs:
                if str(pending_leg.get('id') or '') in existing_pending_ids:
                    continue
                legs.append(pending_leg)
        if _trade_activation_mode in {'live', 'fast-forward', 'forward-test'}:
            print(
                f'[ENTRY STATE] trade_id={trade_id} '
                f'strategy={record.get("name") or ""} '
                f'mode={_trade_activation_mode} '
                f'state=strict_mode_entry_path_started '
                f'listen_timestamp={listen_timestamp}'
            )
            try:
                from features.live_option_chain import fetch_full_chain  # type: ignore
                all_leg_configs = resolve_trade_leg_configs(trade)
                printed_expiries: set[str] = set()
                for leg in legs:
                    leg_id = str(leg.get('id') or '').strip()
                    leg_cfg = resolve_leg_cfg(leg_id, leg, all_leg_configs)
                    option_type = str(leg.get('option') or leg_cfg.get('OptionType') or '').strip()
                    print(
                        f'[ENTRY STATE] trade_id={trade_id} '
                        f'leg_id={leg_id or "-"} '
                        f'state=leg_preview_started '
                        f'underlying={underlying or "-"} '
                        f'option_type={option_type or "-"}'
                    )
                    live_spot_price = get_spot_at_time(db, underlying, listen_timestamp, None)
                    print(
                        f'[ENTRY STATE] trade_id={trade_id} '
                        f'leg_id={leg_id or "-"} '
                        f'state=spot_resolved '
                        f'spot_price={live_spot_price}'
                    )
                    if live_spot_price <= 0:
                        print(
                            f'[ENTRY STATE] trade_id={trade_id} '
                            f'leg_id={leg_id or "-"} '
                            f'state=spot_missing '
                            f'reason=spot_price_zero_or_unavailable'
                        )
                        continue
                    expiry = resolve_leg_expiry(db, leg_cfg, underlying, listen_timestamp, None)
                    print(
                        f'[ENTRY STATE] trade_id={trade_id} '
                        f'leg_id={leg_id or "-"} '
                        f'state=expiry_resolved '
                        f'expiry={expiry or "-"}'
                    )
                    if not expiry:
                        print(
                            f'[ENTRY STATE] trade_id={trade_id} '
                            f'leg_id={leg_id or "-"} '
                            f'state=expiry_missing '
                            f'reason=no_active_expiry_found'
                        )
                        continue
                    if expiry in printed_expiries:
                        print(
                            f'[ENTRY STATE] trade_id={trade_id} '
                            f'leg_id={leg_id or "-"} '
                            f'state=full_chain_reuse '
                            f'expiry={expiry}'
                        )
                        continue
                    printed_expiries.add(expiry)
                    print(
                        f'[ENTRY STATE] trade_id={trade_id} '
                        f'leg_id={leg_id or "-"} '
                        f'state=full_chain_fetch_start '
                        f'underlying={underlying or "-"} '
                        f'expiry={expiry} '
                        f'spot_price={live_spot_price}'
                    )
                    chain = fetch_full_chain(db, underlying, expiry, live_spot_price, leg_id=leg_id)
                    ce_count = len((chain or {}).get('CE') or [])
                    pe_count = len((chain or {}).get('PE') or [])
                    print(
                        f'[ENTRY STATE] trade_id={trade_id} '
                        f'leg_id={leg_id or "-"} '
                        f'state=full_chain_fetch_done '
                        f'expiry={expiry} '
                        f'ce_count={ce_count} '
                        f'pe_count={pe_count}'
                    )
            except Exception as strict_chain_exc:
                print(
                    f'[ENTRY STATE] trade_id={trade_id} '
                    f'strategy={record.get("name") or ""} '
                    f'mode={_trade_activation_mode} '
                    f'state=full_chain_fetch_error '
                    f'error={strict_chain_exc}'
                )
            strict_ctx = TickContext(
                db=db,
                trade_date=trade_date,
                now_ts=listen_timestamp,
                activation_mode=_trade_activation_mode,
                market_cache=None,
            )
            print(
                f'[ENTRY STATE] trade_id={trade_id} '
                f'strategy={record.get("name") or ""} '
                f'mode={_trade_activation_mode} '
                f'state=pending_feature_leg_processor_start '
                f'listen_timestamp={listen_timestamp}'
            )
            entered_feature_leg_ids = _process_momentum_pending_feature_legs(
                db,
                trade,
                chain_col,
                trade_date,
                listen_timestamp,
                lot_size,
                market_cache=market_cache,
                ltp_map={},
                activation_mode=_trade_activation_mode,
            )
            if entered_feature_leg_ids:
                print(
                    f'[ENTRY STATE] trade_id={trade_id} '
                    f'strategy={record.get("name") or ""} '
                    f'mode={_trade_activation_mode} '
                    f'state=pending_feature_leg_processor_done '
                    f'entered_ids={entered_feature_leg_ids}'
                )
                refreshed_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or trade
                for strict_leg_id in entered_feature_leg_ids:
                    entries_executed.append({
                        'trade_id': trade_id,
                        'strategy_name': record.get('name') or '',
                        'group_name': record.get('group_name') or '',
                        'entry_time': record_entry_time_hhmmss or record_entry_time or '',
                        'leg_id': strict_leg_id,
                        'entered': True,
                        'reason': 'pending_feature_leg_processor',
                        'listen_time': listen_time_hhmmss or listen_time_hhmm,
                        'listen_timestamp': listen_timestamp,
                    })
                _sync_entered_legs_to_history(db, [refreshed_trade])
                continue
            print(
                f'[ENTRY STATE] trade_id={trade_id} '
                f'strategy={record.get("name") or ""} '
                f'mode={_trade_activation_mode} '
                f'state=pending_feature_leg_processor_done '
                f'entered_ids=[]'
            )
            print(
                f'[ENTRY STATE] trade_id={trade_id} '
                f'strategy={record.get("name") or ""} '
                f'mode={_trade_activation_mode} '
                f'state=core_process_pending_entries_start '
                f'listen_timestamp={listen_timestamp}'
            )
            strict_entries = core_process_pending_entries(strict_ctx, [trade])
            if strict_entries:
                print(
                    f'[ENTRY STATE] trade_id={trade_id} '
                    f'strategy={record.get("name") or ""} '
                    f'mode={_trade_activation_mode} '
                    f'state=core_process_pending_entries_done '
                    f'entries={len(strict_entries)}'
                )
                for strict_entry in strict_entries:
                    strict_leg_id = str(strict_entry.get('leg_id') or '')
                    print(
                        f'[BACKTEST ENTRY] trade_id={trade_id} '
                        f'strategy={record.get("name") or ""} '
                        f'entry_time={(record_entry_time_hhmmss or record_entry_time or "--:--")} '
                        f'leg_id={strict_leg_id or "-"} '
                        f'entered=True '
                        f'reason=core_process_pending_entries '
                        f'listen_time={(listen_time_hhmmss or listen_time_hhmm)}'
                    )
                    entries_executed.append({
                        'trade_id': trade_id,
                        'strategy_name': record.get('name') or '',
                        'group_name': record.get('group_name') or '',
                        'entry_time': record_entry_time_hhmmss or record_entry_time or '',
                        'leg_id': strict_leg_id,
                        'entered': True,
                        'reason': 'core_process_pending_entries',
                        'listen_time': listen_time_hhmmss or listen_time_hhmm,
                        'listen_timestamp': listen_timestamp,
                        'entry_price': strict_entry.get('entry_price'),
                        'ltp': strict_entry.get('ltp'),
                        'strike': strict_entry.get('strike'),
                        'expiry': strict_entry.get('expiry'),
                        'option_type': strict_entry.get('option_type'),
                        'token': strict_entry.get('token'),
                        'symbol': strict_entry.get('symbol'),
                    })
            else:
                print(
                    f'[BACKTEST ENTRY] trade_id={trade_id} '
                    f'strategy={record.get("name") or ""} '
                    f'entry_time={(record_entry_time_hhmmss or record_entry_time or "--:--")} '
                    f'entered=False reason=no_pending_entries_resolved '
                    f'listen_time={(listen_time_hhmmss or listen_time_hhmm)}'
                )
                print(
                    f'[ENTRY STATE] trade_id={trade_id} '
                    f'strategy={record.get("name") or ""} '
                    f'mode={_trade_activation_mode} '
                    f'state=core_process_pending_entries_done '
                    f'entries=0'
                )
            continue

        for leg_index, leg in enumerate(legs):
            leg_status = int(leg.get('status') or 0)
            is_pending_feature_leg = bool(leg.get('is_pending_feature_leg'))
            if not is_pending_feature_leg and leg_status != OPEN_LEG_STATUS:
                continue
            # Pending = open leg with no entry_trade yet
            entry_trade = leg.get('entry_trade')
            if entry_trade is not None:
                continue

            entered, reason = _try_enter_pending_leg(
                db,
                trade,
                leg,
                leg_index,
                chain_col,
                trade_date,
                listen_timestamp,
                lot_size,
                snapshot_timestamp=listen_timestamp,
                market_cache=market_cache,
            )
            leg_id = str(leg.get('id') or str(leg_index))
            print(
                f'[BACKTEST ENTRY] trade_id={trade_id} '
                f'strategy={record.get("name") or ""} '
                f'entry_time={(record_entry_time_hhmmss or record_entry_time or "--:--")} '
                f'leg_id={leg_id} '
                f'entered={entered} '
                f'reason={reason or "-"} '
                f'listen_time={(listen_time_hhmmss or listen_time_hhmm)}'
            )
            import json as _json
            underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
            option_type = str(leg.get('option') or 'CE').split('.')[-1]
            activation_mode = str(trade.get('activation_mode') or '').strip()
            if not entered:
                print(f'[ENTRY FAILED DEBUG] ── Failed Leg Object ──')
                print(_json.dumps(leg, indent=2, default=str))
                print(f'[ENTRY FAILED DEBUG] ── Trade Legs entry_trade details ──')
                for _i, _l in enumerate(trade.get('legs') or []):
                    if isinstance(_l, dict):
                        print(f'  leg[{_i}] id={_l.get("id")} option={_l.get("option")} strike={_l.get("strike")} entry_trade={_json.dumps(_l.get("entry_trade"), default=str)}')
                    else:
                        print(f'  leg[{_i}] => history_ref (string): {_l}')
                if activation_mode in {'live', 'fast-forward', 'forward-test'}:
                    print(f'[ENTRY FAILED DEBUG] mode={activation_mode} ── Live/FF debug ──')
                    try:
                        from features.live_monitor_socket import _get_active_ticker_manager
                        _tm_dbg = _get_active_ticker_manager()
                        _dbg_spot = _tm_dbg.get_spot(underlying)
                        _dbg_leg_token = str(leg.get('token') or '')
                        _dbg_ltp = _tm_dbg.get_ltp(_dbg_leg_token) if _dbg_leg_token else None
                        print(f'[ENTRY FAILED DEBUG] underlying={underlying} kite_spot={_dbg_spot} leg_token={_dbg_leg_token or "-"} kite_ltp={_dbg_ltp}')
                        print(f'[ENTRY FAILED DEBUG] ltp_map_keys={list(dict(_tm_dbg.ltp_map or {}).keys())[:10]}')
                    except Exception as _dbg_e:
                        print(f'[ENTRY FAILED DEBUG] kite_ticker unavailable: {_dbg_e}')
                    try:
                        _dbg_expiry = str(leg.get('expiry_date') or '')[:10]
                        _dbg_strike = leg.get('strike')
                        _dbg_opt = str(leg.get('option') or option_type or '').split('.')[-1].upper()
                        _tok_docs = list(db._db['active_option_tokens'].find(
                            {'instrument': underlying, 'option_type': _dbg_opt},
                            {'strike': 1, 'expiry': 1, 'token': 1, 'symbol': 1}
                        ).sort('expiry', 1).limit(10))
                        print(f'[ENTRY FAILED DEBUG] active_option_tokens for {underlying}/{_dbg_opt} expiry={_dbg_expiry or "-"} strike={_dbg_strike or "-"}:')
                        for _td in _tok_docs:
                            print(f'  tok: strike={_td.get("strike")} expiry={_td.get("expiry")} token={_td.get("token")} symbol={_td.get("symbol")}')
                        if not _tok_docs:
                            print(f'  [NO TOKEN DOCS] instrument={underlying} option_type={_dbg_opt}')
                    except Exception as _tok_e:
                        print(f'[ENTRY FAILED DEBUG] active_option_tokens query error: {_tok_e}')
                else:
                    print(f'[ENTRY FAILED DEBUG] ── Option Chain at timestamp={listen_timestamp} underlying={underlying} option={option_type} ──')
                    try:
                        _chain_docs = list(chain_col.find(
                            {'underlying': underlying, 'type': option_type, 'timestamp': listen_timestamp},
                            {'_id': 0, 'strike': 1, 'expiry': 1, 'close': 1, 'open': 1, 'timestamp': 1, 'token': 1}
                        ).sort('strike', 1).limit(20))
                        if _chain_docs:
                            for _cd in _chain_docs:
                                print(f'  chain: strike={_cd.get("strike")} expiry={_cd.get("expiry")} close={_cd.get("close")} token={_cd.get("token")}')
                        else:
                            print(f'  [NO CHAIN DATA FOUND] underlying={underlying} option={option_type} timestamp={listen_timestamp}')
                            _nearby = chain_col.find_one(
                                {'underlying': underlying, 'type': option_type, 'timestamp': {'$lte': listen_timestamp}},
                                sort=[('timestamp', -1)]
                            )
                            print(f'  nearest available chain doc: {_json.dumps(_nearby, indent=2, default=str) if _nearby else "None"}')
                    except Exception as _ce:
                        print(f'  [CHAIN QUERY ERROR] {_ce}')
            # Failed legs stay in legs array for retry on next tick.
            # Persist failure to entry_errors for tracking.
            try:
                import datetime as _dt_err
                db._db['entry_errors'].insert_one({
                    'trade_id':        trade_id,
                    'strategy_name':   record.get('name') or '',
                    'group_name':      (record.get('portfolio') or {}).get('group_name') or record.get('group_name') or '',
                    'leg_id':          leg_id,
                    'reason':          reason or 'unknown',
                    'underlying':      underlying,
                    'option_type':     option_type,
                    'activation_mode': activation_mode,
                    'entry_kind':      str(leg.get('entry_kind') or ''),
                    'expiry_date':     str(leg.get('expiry_date') or ''),
                    'strike':          leg.get('strike'),
                    'token':           str(leg.get('token') or ''),
                    'symbol':          str(leg.get('symbol') or ''),
                    'listen_timestamp': listen_timestamp,
                    'created_at':      _dt_err.datetime.utcnow().isoformat(),
                })
                print(f'[ENTRY ERROR LOG] saved to entry_errors: trade={trade_id} leg={leg_id} reason={reason or "unknown"}')
            except Exception as _ins_e:
                print(f'[ENTRY ERROR LOG] db write failed: {_ins_e}')

            entries_executed.append({
                'trade_id': trade_id,
                'strategy_name': record.get('name') or '',
                'group_name': record.get('group_name') or '',
                'entry_time': record_entry_time_hhmmss or record_entry_time or '',
                'leg_id': leg_id,
                'entered': entered,
                'reason': reason or '',
                'listen_time': listen_time_hhmmss or listen_time_hhmm,
                'listen_timestamp': listen_timestamp,
            })

    return entries_executed


def run_backtest_simulation_step(
    db: MongoData,
    listen_timestamp: str,
    activation_mode: str = 'algo-backtest',
) -> dict:
    """
    Run one backtest simulation step for a fixed listen timestamp.

    Reuses the same backend conditions as websocket autorun:
      - original leg queue
      - pending leg entry / simple momentum / lazy leg activation
      - SL / target / trail SL / overall checks
      - active leg LTP snapshot
      - open position snapshot
    """
    normalized_timestamp = str(listen_timestamp or '').strip()
    if not normalized_timestamp:
        return {
            'listen_timestamp': '',
            'listen_time': '',
            'trade_date': '',
            'records': [],
            'entry_snapshots': [],
            'entries_executed': [],
            'actions_taken': [],
            'ltp': [],
            'open_positions': [],
            'active_leg_tokens': [],
            'count': 0,
            'open_positions_count': 0,
        }

    trade_date = normalized_timestamp[:10] if len(normalized_timestamp) >= 10 else ''
    listen_time = normalized_timestamp[11:19] if len(normalized_timestamp) >= 19 else normalized_timestamp[11:16]
    listen_hhmm = listen_time[:5]

    # ── Load trades + warm market cache ────────────────────────────────────────
    # Market cache is intentionally NOT cleared at the end of this function.
    # Consecutive calls for the same date hit the in-memory cache (milliseconds).
    # The cache is keyed by (date, underlyings) inside preload_market_data_cache
    # so switching dates automatically gets a fresh key.
    records = _load_running_trade_records(db, trade_date, activation_mode=activation_mode)
    market_cache = _preload_trade_date_market_cache(db, trade_date, records)

    entry_snapshots = build_entry_spot_snapshots(
        db,
        records,
        listen_hhmm,
        normalized_timestamp,
        market_cache=market_cache,
    )

    # ── Entry execution ─────────────────────────────────────────────────────────
    entries_executed: list[dict] = []
    if records:
        entries_executed = _execute_backtest_entries(
            db,
            records,
            listen_hhmm,
            normalized_timestamp,
            market_cache=market_cache,
        )
        if entries_executed:
            synced_trade_ids: dict[str, dict] = {}
            for executed_entry in entries_executed:
                if not executed_entry.get('entered'):
                    continue
                synced_trade_id = str(executed_entry.get('trade_id') or '').strip()
                if synced_trade_id:
                    synced_trade_ids[synced_trade_id] = {'_id': synced_trade_id}
            if synced_trade_ids:
                _sync_entered_legs_to_history(db, list(synced_trade_ids.values()))
                for synced_trade_id in synced_trade_ids:
                    _validate_trade_leg_storage(db, synced_trade_id)
            # Reload only when entries actually happened
            records = _load_running_trade_records(db, trade_date, activation_mode=activation_mode)

    _set_listening_strategy_snapshot(trade_date, records, activation_mode=activation_mode)
    _set_listening_time_state(
        trade_date,
        activation_mode=activation_mode,
        listen_time=listen_time,
        listen_timestamp=normalized_timestamp,
    )

    # ── SL / Target / TrailSL / Overall checks + simple momentum pending ────────
    # _backtest_minute_tick and _extract_running_positions must load their own
    # trades from DB.  _load_running_trade_records filters to RUNNING_STATUS +
    # active_on_server=True only; those functions need BOTH RUNNING_STATUS and
    # BACKTEST_IMPORT_STATUS trades.  Passing the wrong subset caused SL/TG/TSL
    # and momentum checks to silently skip backtest-import trades.
    # The market cache is already warm at this point so all chain/spot lookups
    # inside these functions are O(1) memory hits — no extra MongoDB round-trips.
    bt_tick_result = _backtest_minute_tick(
        db,
        trade_date,
        normalized_timestamp,
        activation_mode=activation_mode,
        market_cache=market_cache,
        # running_trades intentionally NOT passed — let it load with the correct
        # status filter ([RUNNING_STATUS, BACKTEST_IMPORT_STATUS]).
    )

    # ── Emit hit strategies + LTP snapshot to execute-orders socket ─────────
    _bt_hit_ids       = bt_tick_result.get('hit_trade_ids') or []
    _bt_ltp_snapshots = bt_tick_result.get('hit_ltp_snapshots') or {}
    if _bt_hit_ids:
        try:
            _hit_query = {'_id': {'$in': _bt_hit_ids}}
            _hit_raw   = list(db._db['algo_trades'].find(_hit_query))
            _hit_ser   = [_serialize_trade_record(r) for r in _hit_raw]
            _hit_enr   = _populate_legs_from_history(db, _hit_ser)

            # Group hit records by user_id for per-user routing
            _hit_by_user: dict[str, list[dict]] = {}
            for _r in _hit_enr:
                _u = str(_r.get('user_id') or '').strip()
                _hit_by_user.setdefault(_u, []).append(_r)

            async def _emit_hit_per_user() -> None:
                for _uid, _uid_records in _hit_by_user.items():
                    _payload = _build_execute_order_socket_payload(_uid_records, trigger='sl-target-hit')
                    _payload['hit_ltp_snapshots'] = _bt_ltp_snapshots
                    _msg = _build_message('execute_order', 'Strategy SL/Target hit', _payload)
                    if _uid:
                        await _broadcast_user_channel_message(_uid, 'execute-orders', _msg)
                    else:
                        await _broadcast_channel_message('execute-orders', _msg)

            try:
                _loop = asyncio.get_event_loop()
                _loop.create_task(_emit_hit_per_user())
            except RuntimeError:
                asyncio.run(_emit_hit_per_user())
            print(f'[SL/TGT HIT EMIT] sent {len(_hit_enr)} strategy(s) to execute-orders hit_ids={_bt_hit_ids}')
            for _tid, _ltp_snap in _bt_ltp_snapshots.items():
                print(f'  [LTP SNAPSHOT] trade={_tid} legs={[{"leg_id": l.get("leg_id"), "ltp": l.get("ltp"), "entry_price": l.get("entry_price"), "pnl": l.get("pnl")} for l in _ltp_snap]}')
        except Exception as _he:
            log.warning('[SL/TGT HIT EMIT] error: %s', _he)

    # ── Reload after tick (SL/Target may have closed/changed legs) ─────────────
    records = _load_running_trade_records(db, trade_date, activation_mode=activation_mode)
    _set_listening_strategy_snapshot(trade_date, records, activation_mode=activation_mode)

    # ── LTP for active + momentum-pending contracts ─────────────────────────────
    active_contracts = _build_active_contracts_from_records(
        records,
        db=db,
        trade_date=trade_date,
        market_cache=market_cache,
        activation_mode=activation_mode,
    )
    _append_momentum_pending_to_contracts(active_contracts, db, records)

    chain_col = db._db[OPTION_CHAIN_COLLECTION]
    index_spot_col = db._db['option_chain_index_spot']
    option_contracts = [c for c in active_contracts if c.get('option') != 'SPOT']
    spot_contracts   = [c for c in active_contracts if c.get('option') == 'SPOT']

    option_ticks = _fetch_active_leg_ticks(
        chain_col,
        option_contracts,
        normalized_timestamp,
        market_cache=market_cache,
        activation_mode=activation_mode,
    )
    option_ltp = [_format_chain_tick_as_ltp(t) for t in option_ticks]
    spot_ltp   = _fetch_spot_ticks(
        index_spot_col,
        spot_contracts,
        normalized_timestamp,
        market_cache=market_cache,
        activation_mode=activation_mode,
    )

    # ── Open position snapshot + simple momentum pending check ─────────────────
    # _extract_running_positions calls _process_momentum_pending_feature_legs for
    # every trade (simple momentum entry check).  It must also use its own DB load
    # so that BACKTEST_IMPORT_STATUS trades are included.
    position_snapshot = _extract_running_positions(
        db,
        trade_date,
        listen_hhmm,
        include_position_snapshots=True,
        running_trades=None,    # load with correct status filter inside
        market_cache=market_cache,
        activation_mode=activation_mode,
    )
    # NOTE: market_cache is intentionally kept alive here for the next call.

    return {
        'listen_timestamp': normalized_timestamp,
        'listen_time': listen_time,
        'trade_date': trade_date,
        'records': records,
        'entry_snapshots': entry_snapshots,
        'entries_executed': entries_executed,
        'actions_taken': list(bt_tick_result.get('actions_taken') or []),
        'ltp': option_ltp + spot_ltp + ([_vix] if (_vix := _fetch_vix_ltp_item(db, normalized_timestamp, market_cache)) else []),
        'open_positions': list(position_snapshot.get('open_positions') or []),
        'active_leg_tokens': list(position_snapshot.get('active_leg_tokens') or []),
        'count': len(records),
        'open_positions_count': len(position_snapshot.get('open_positions') or []),
    }


def build_backtest_simulation_socket_messages(
    db: MongoData,
    result: dict,
) -> dict[str, str]:
    """
    Build all WebSocket messages for one backtest simulation step.
    Called exactly once per step (the result is passed to broadcast_backtest_simulation_step
    to avoid a second, redundant _populate_legs_from_history call).
    """
    normalized_records = _populate_legs_from_history(db, result.get('records') or [])
    normalized_records = _attach_broker_configuration_details(db, normalized_records)
    selected_date    = str(result.get('trade_date') or '')
    listen_time      = str(result.get('listen_time') or '')
    listen_timestamp = str(result.get('listen_timestamp') or '')
    entries_executed = result.get('entries_executed') or []

    # ── execute-orders: full strategy records + executed order details ──────────
    execute_order_payload = _build_execute_order_socket_payload(
        normalized_records,
        trigger='manual-simulator',
    )
    # Attach entries_executed (the actual order fills) so the frontend gets
    # entry_price, quantity, timestamps etc. for every order placed this tick.
    execute_order_payload['entries_executed'] = entries_executed
    execute_order_payload['entries_count']    = len(entries_executed)

    return {
        'executions': _build_message(
            'countdown_update',
            'Execution listening time updated',
            {
                'selected_date': selected_date,
                'listen_time': listen_time,
                'listen_timestamp': listen_timestamp,
                'records': normalized_records,
                'count': len(normalized_records),
                'entry_snapshots': result.get('entry_snapshots') or [],
                'entries_executed': entries_executed,
                'mode': 'backtest',
            },
        ),
        'execute-orders': _build_message(
            'execute_order',
            'Strategy execution refreshed',
            execute_order_payload,
        ),
        'update': _build_message(
            'update',
            'Open position snapshot refreshed',
            {
                'trade_date': selected_date,
                'activation_mode': 'algo-backtest',
                'status': 'algo-backtest',
                'trigger_reason': 'manual_simulator_run',
                'refresh_status': 'completed',
                'open_positions': result.get('open_positions') or [],
                'count': result.get('open_positions_count') or 0,
            },
        ),
        # LTP goes to the update channel so the frontend receives it alongside
        # open-position data.  Kept as a separate message type so the frontend
        # can route it independently (ltp_update handler).
        'ltp_update': _build_message(
            'ltp_update',
            'LTP update for active positions',
            {
                'trade_date': selected_date,
                'listen_time': listen_time,
                'listen_timestamp': listen_timestamp,
                'ltp': result.get('ltp') or [],
            },
        ),
    }


def _get_connected_user_ids(channel: str) -> list[str]:
    """Return list of user_ids that have at least one active socket for this channel."""
    ch = str(channel or '').strip()
    if not ch:
        return []
    suffix = f':{ch}'
    result: list[str] = []
    for room_key, sockets in CONNECTED_USER_CHANNEL_WEBSOCKETS.items():
        if room_key.endswith(suffix) and sockets:
            uid = room_key[: -len(suffix)]
            if uid:
                result.append(uid)
    return result


def _build_user_simulation_messages(
    result: dict,
    normalized_records: list[dict],
    user_id: str,
) -> dict[str, str]:
    """
    Build all socket messages for one user by filtering normalized_records to
    only that user's trades.  LTP and entry_snapshots are shared market data
    so they are sent to every user unfiltered.
    """
    uid = str(user_id or '').strip()

    # Records for this user only
    uid_records = [r for r in normalized_records if str(r.get('user_id') or '').strip() == uid]

    # trade_id set for this user (used to filter entries_executed + open_positions)
    uid_trade_ids: set[str] = {str(r.get('_id') or '').strip() for r in uid_records if r.get('_id')}

    entries_executed  = result.get('entries_executed') or []
    open_positions    = result.get('open_positions') or []
    entry_snapshots   = result.get('entry_snapshots') or []
    selected_date     = str(result.get('trade_date') or '')
    listen_time       = str(result.get('listen_time') or '')
    listen_timestamp  = str(result.get('listen_timestamp') or '')

    uid_entries        = [e for e in entries_executed if str(e.get('trade_id') or '').strip() in uid_trade_ids]
    uid_open_positions = [p for p in open_positions    if str(p.get('trade_id') or '').strip() in uid_trade_ids]

    execute_order_payload = _build_execute_order_socket_payload(uid_records, trigger='manual-simulator')
    execute_order_payload['entries_executed'] = uid_entries
    execute_order_payload['entries_count']    = len(uid_entries)

    return {
        'execute-orders': _build_message(
            'execute_order',
            'Strategy execution refreshed',
            execute_order_payload,
        ),
        'update': _build_message(
            'update',
            'Open position snapshot refreshed',
            {
                'trade_date':       selected_date,
                'activation_mode':  'algo-backtest',
                'status':           'algo-backtest',
                'trigger_reason':   'manual_simulator_run',
                'refresh_status':   'completed',
                'open_positions':   uid_open_positions,
                'count':            len(uid_open_positions),
            },
        ),
        'executions': _build_message(
            'countdown_update',
            'Execution listening time updated',
            {
                'selected_date':    selected_date,
                'listen_time':      listen_time,
                'listen_timestamp': listen_timestamp,
                'records':          uid_records,
                'count':            len(uid_records),
                'entry_snapshots':  entry_snapshots,   # market data — sent to all users
                'entries_executed': uid_entries,
                'mode':             'backtest',
            },
        ),
        'ltp_update': _build_message(
            'ltp_update',
            'LTP update for active positions',
            {
                'trade_date':       selected_date,
                'listen_time':      listen_time,
                'listen_timestamp': listen_timestamp,
                'ltp':              result.get('ltp') or [],  # market data — sent to all users
            },
        ),
    }


async def broadcast_backtest_simulation_step(
    db: MongoData,
    result: dict,
    prebuilt_messages: dict[str, str] | None = None,
) -> dict[str, int]:
    """
    Broadcast simulation step to connected WebSocket clients.

    Change-detection rules (per user_id):
      • execute_order  → only when algo_trades / algo_trade_positions_history /
                         algo_leg_feature_status data changed (signature diff)
      • update         → only when open_positions changed (signature diff)
      • ltp_update     → every tick (market prices)
      • countdown_update → every tick (time progresses)

    Non-user-room (legacy) sockets always receive the full payload unchanged.
    """
    # Populate legs once — covers algo_trades + algo_trade_positions_history
    # + algo_leg_feature_status (called inside _populate_legs_from_history)
    normalized_records = _populate_legs_from_history(db, result.get('records') or [])
    normalized_records = _attach_broker_configuration_details(db, normalized_records)

    # ── Non-user (legacy) sockets → full payload every tick ─────────────────
    full_msgs = prebuilt_messages or build_backtest_simulation_socket_messages(db, result)
    # execute_order is user-sensitive strategy state, so never fan it out through
    # legacy non-user sockets. It must flow only via user rooms below.
    d_eo  = 0
    d_upd = await _broadcast_channel_message('update',         full_msgs.get('update') or '')
    d_ltp = await _broadcast_channel_message('update',         full_msgs.get('ltp_update') or '')
    d_exc = await _broadcast_channel_message('executions',     full_msgs.get('executions') or '')

    # ── User rooms → per-user, change-gated payload ─────────────────────────
    eo_uids  = set(_get_connected_user_ids('execute-orders'))
    upd_uids = set(_get_connected_user_ids('update'))
    exc_uids = set(_get_connected_user_ids('executions'))
    d_eo += await flush_pending_execute_order_emits(db, user_ids=eo_uids, force=False)
    all_uids = upd_uids | exc_uids

    for uid in all_uids:
        user_msgs = _build_user_simulation_messages(result, normalized_records, uid)

        # ── update (open_positions): only if positions changed
        if uid in upd_uids:
            uid_open_positions = [
                p for p in (result.get('open_positions') or [])
                if str(p.get('trade_id') or '').strip() in
                {str(r.get('_id') or '') for r in normalized_records if str(r.get('user_id') or '').strip() == uid}
            ]
            pos_sig = _position_signature(uid_open_positions)
            if LAST_OPEN_POSITION_SIG.get(uid) != pos_sig:
                LAST_OPEN_POSITION_SIG[uid] = pos_sig
                d_upd += await _broadcast_user_channel_message(uid, 'update', user_msgs['update'])
                log.debug('[EMIT update] user=%s sig_changed=True', uid)

            # ltp_update always (market prices change every tick)
            d_ltp += await _broadcast_user_channel_message(uid, 'update', user_msgs['ltp_update'])

        # ── countdown_update always (time progresses every tick)
        if uid in exc_uids:
            d_exc += await _broadcast_user_channel_message(uid, 'executions', user_msgs['executions'])

    return {
        'execute-orders': d_eo,
        'update':         d_upd + d_ltp,
        'executions':     d_exc,
    }


# ─── WebSocket handler ────────────────────────────────────────────────────────

@socket_router.websocket('/ws/executions')
async def executions_socket(
    websocket: WebSocket,
    user_id: str = Query(default=''),
):
    await websocket.accept()
    _register_channel_websocket('executions', websocket)
    uid = str(user_id or '').strip()
    if uid:
        _register_user_websocket('executions', uid, websocket)
    await websocket.send_text(_build_message(
        'connection_established',
        'Algo execution websocket connected',
        {'channel': 'executions', 'user_id': uid},
    ))
    listening_active = False
    selected_date = ''
    simulated_minutes = MARKET_START_MINUTES
    listening_records: list[dict] = []
    market_cache: dict | None = None
    db = MongoData()

    try:
        while True:
            try:
                raw_message = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
            except asyncio.TimeoutError:
                if listening_active:
                    listen_time = _format_market_time_from_minutes(simulated_minutes)
                    listen_timestamp = (
                        f'{selected_date}T{listen_time}:00'
                        if selected_date
                        else listen_time
                    )
                    _set_listening_time_state(
                        selected_date,
                        activation_mode='algo-backtest',
                        listen_time=listen_time,
                        listen_timestamp=listen_timestamp,
                    )

                    # ── Market close at 15:30 → stop listening ────────────────
                    if simulated_minutes >= MARKET_CLOSE_MINUTES:
                        listening_active = False
                        _clear_listening_strategy_snapshot(selected_date, activation_mode='algo-backtest')
                        await websocket.send_text(_build_message(
                            'listening_completed',
                            'Market closed at 15:30 — backtest listening ended',
                            {
                                'selected_date': selected_date,
                                'listen_time': listen_time,
                                'listen_timestamp': listen_timestamp,
                                'mode': 'backtest',
                            },
                        ))
                        continue

                    print(f"[EXECUTION SOCKET] current_time={listen_timestamp}")
                    for record in listening_records:
                        rec_entry_raw = str(record.get('entry_time') or '').strip()
                        rec_entry_hhmm = rec_entry_raw[11:16] if len(rec_entry_raw) >= 16 else rec_entry_raw[:5]
                        # Determine per-strategy status
                        legs = record.get('legs') if isinstance(record.get('legs'), list) else []
                        has_entry = any(
                            isinstance(leg, dict) and isinstance(leg.get('entry_trade'), dict) and leg.get('entry_trade')
                            for leg in legs
                        )
                        if has_entry:
                            entry_status = 'position_taking'
                        elif listen_time == rec_entry_hhmm:
                            entry_status = 'checking_position'
                        elif rec_entry_hhmm and listen_time < rec_entry_hhmm:
                            entry_status = 'waiting_for_entry'
                        else:
                            entry_status = 'pending_entry'
                        print(
                            "[EXECUTION SOCKET] "
                            f"group_name={record.get('group_name') or ''} "
                            f"ticker={record.get('underlying') or ''} "
                            f"strategy_name={record.get('name') or ''} "
                            f"entry_time={rec_entry_hhmm or '-'} "
                            f"status={entry_status}"
                        )
                        await websocket.send_text(_build_message(
                            'strategy_tick',
                            'Strategy tick update',
                            {
                                'group_name':    record.get('group_name') or '',
                                'ticker':        record.get('underlying') or '',
                                'strategy_name': record.get('name') or '',
                                'entry_time':    rec_entry_hhmm or '',
                                'listen_time':   listen_time,
                                'entry_status':  entry_status,
                            },
                        ))

                    # ── Build entry snapshots (strategy entry_time == listen_time) ──
                    entry_snapshots = build_entry_spot_snapshots(
                        db,
                        listening_records,
                        listen_time,
                        listen_timestamp,
                        market_cache=market_cache,
                    )
                    for snapshot in entry_snapshots:
                        print(
                            "[EXECUTION SOCKET] "
                            f"group_name={snapshot['group_name']} "
                            f"strategy_name={snapshot['strategy_name']} "
                            f"entry_time={snapshot['entry_time']} "
                            f"underlying={snapshot['underlying']} "
                            f"spot_price={snapshot['spot_price']} "
                            f"atm_price={snapshot['atm_price']}"
                        )
                        for option_chain in snapshot.get('option_chain') or []:
                            print(
                                "[EXECUTION SOCKET] "
                                f"leg_id={option_chain.get('leg_id') or ''} "
                                f"position={option_chain.get('position') or ''} "
                                f"expiry_kind={option_chain.get('expiry_kind') or ''} "
                                f"expiry={option_chain.get('expiry') or ''} "
                                f"option_type={option_chain.get('option_type') or ''} "
                                f"strike={option_chain.get('strike') or 0} "
                                f"close={option_chain.get('close') or 0} "
                                f"timestamp={option_chain.get('timestamp') or ''}"
                            )

                    # ── Execute entries for matched strategies ─────────────────
                    entries_executed: list[dict] = []
                    if entry_snapshots and selected_date:
                        entries_executed = _execute_backtest_entries(
                            db,
                            listening_records,
                            listen_time,
                            listen_timestamp,
                            market_cache=market_cache,
                        )
                        if entries_executed:
                            synced_trade_ids = {}
                            for executed_entry in entries_executed:
                                if not executed_entry.get('entered'):
                                    continue
                                synced_trade_ids[str(executed_entry.get('trade_id') or '').strip()] = {
                                    '_id': str(executed_entry.get('trade_id') or '').strip(),
                                }
                            if synced_trade_ids:
                                _sync_entered_legs_to_history(db, list(synced_trade_ids.values()))
                                for synced_trade_id in synced_trade_ids:
                                    _validate_trade_leg_storage(db, synced_trade_id)
                            # Refresh listening_records from DB after entry writes
                            refreshed_cursor = db._db['algo_trades'].find({
                                'activation_mode': 'algo-backtest',
                                'active_on_server': True,
                                'trade_status': 1,
                                'status': RUNNING_STATUS,
                                'creation_ts': {'$regex': f'^{re.escape(selected_date)}'},
                            }).sort('creation_ts', 1)
                            refreshed_records = []
                            for item in refreshed_cursor:
                                refreshed_records.append({
                                    '_id': str(item.get('_id') or ''),
                                    'strategy_id': str(item.get('strategy_id') or ''),
                                    'name': str(item.get('name') or ''),
                                    'status': str(item.get('status') or ''),
                                    'trade_status': int(item.get('trade_status') or 0),
                                    'active_on_server': bool(item.get('active_on_server')),
                                    'creation_ts': str(item.get('creation_ts') or ''),
                                    'entry_time': str(item.get('entry_time') or ''),
                                    'exit_time': str(item.get('exit_time') or ''),
                                    'group_name': str(((item.get('portfolio') or {}).get('group_name') or '')),
                                    'underlying': str(
                                        ((item.get('config') or {}).get('Ticker'))
                                        or item.get('ticker')
                                        or ''
                                    ),
                                    'config': item.get('config') if isinstance(item.get('config'), dict) else {},
                                    'portfolio': item.get('portfolio') if isinstance(item.get('portfolio'), dict) else {},
                                    'legs': item.get('legs') if isinstance(item.get('legs'), list) else [],
                                })
                            listening_records = refreshed_records
                            _set_listening_strategy_snapshot(selected_date, refreshed_records)

                    _set_listening_strategy_snapshot(selected_date, listening_records)
                    await websocket.send_text(_build_message(
                        'countdown_update',
                        'Execution listening time updated',
                        {
                            'selected_date': selected_date,
                            'listen_time': listen_time,
                            'listen_timestamp': listen_timestamp,
                            'records': listening_records,
                            'count': len(listening_records),
                            'entry_snapshots': entry_snapshots,
                            'entries_executed': entries_executed,
                            'mode': 'backtest',
                        },
                    ))
                    simulated_minutes += 1
                else:
                    print(f"[EXECUTION SOCKET] current_time={_now_iso()}")
                continue

            try:
                parsed_message = json.loads(raw_message)
            except json.JSONDecodeError:
                parsed_message = {'raw': raw_message}

            action = str((parsed_message or {}).get('action') or '').strip().lower()

            # ── subscribe_executions ───────────────────────────────────────
            if action == 'subscribe_executions':
                await websocket.send_text(_build_message(
                    'subscription_ack',
                    'Execution websocket subscription confirmed',
                    parsed_message,
                ))
                continue

            if action == 'start_live_monitor':
                _clear_market_cache_snapshot(market_cache)
                market_cache = None
                snapshot = {
                    'listen_time': _now_iso(),
                    'trade_date': str((parsed_message or {}).get('trade_date') or '').strip(),
                    'mode': 'live',
                }
                await websocket.send_text(_build_message(
                    'live_monitor_started',
                    'Live monitor started',
                    snapshot,
                ))
                continue

            if action == 'stop_live_monitor':
                await websocket.send_text(_build_message(
                    'live_monitor_stopped',
                    'Live monitor stopped',
                    {'listen_time': _now_iso(), 'mode': 'live'},
                ))
                continue

            if action == 'start_listening':
                _clear_market_cache_snapshot(market_cache)
                selected_date = str((parsed_message or {}).get('selected_date') or '').strip()
                status_tag = str((parsed_message or {}).get('status') or '').strip()
                behind_time = max(0, _safe_int((parsed_message or {}).get('behind_time')))
                simulated_minutes = max(0, MARKET_START_MINUTES - behind_time)
                listen_time = _format_market_time_from_minutes(simulated_minutes)
                listen_timestamp = (
                    f'{selected_date}T{listen_time}:00'
                    if selected_date
                    else listen_time
                )
                listening_active = True
                initial_records = []
                if selected_date:
                    cursor = db._db['algo_trades'].find({
                        'activation_mode': 'algo-backtest',
                        'active_on_server': True,
                        'trade_status': 1,
                        'status': RUNNING_STATUS,
                        'creation_ts': {'$regex': f'^{re.escape(selected_date)}'},
                    }).sort('creation_ts', 1)
                    for item in cursor:
                        initial_records.append({
                            '_id': str(item.get('_id') or ''),
                            'strategy_id': str(item.get('strategy_id') or ''),
                            'name': str(item.get('name') or ''),
                            'status': str(item.get('status') or ''),
                            'trade_status': int(item.get('trade_status') or 0),
                            'active_on_server': bool(item.get('active_on_server')),
                            'creation_ts': str(item.get('creation_ts') or ''),
                            'entry_time': str(item.get('entry_time') or ''),
                            'exit_time': str(item.get('exit_time') or ''),
                            'group_name': str(((item.get('portfolio') or {}).get('group_name') or '')),
                            'underlying': str(
                                ((item.get('config') or {}).get('Ticker'))
                                or item.get('ticker')
                                or ''
                            ),
                            'config': item.get('config') if isinstance(item.get('config'), dict) else {},
                            'portfolio': item.get('portfolio') if isinstance(item.get('portfolio'), dict) else {},
                            'legs': item.get('legs') if isinstance(item.get('legs'), list) else [],
                        })
                listening_records = initial_records
                _set_listening_strategy_snapshot(selected_date, initial_records)
                if _is_algo_backtest_status(status_tag):
                    market_cache = _preload_trade_date_market_cache(db, selected_date, initial_records)
                    if market_cache:
                        print(
                            '[BACKTEST MARKET DATA READY] '
                            f'trade_date={selected_date} '
                            f'underlyings={",".join((market_cache or {}).get("underlyings") or []) or "-"} '
                            'option_chain_and_spot_loaded=true'
                        )
                else:
                    market_cache = None
                payload = {
                    'selected_date': selected_date,
                    'behind_time': behind_time,
                    'remaining_minutes': (parsed_message or {}).get('remaining_minutes'),
                    'listen_time': listen_time,
                    'listen_timestamp': listen_timestamp,
                    'records': initial_records,
                    'count': len(initial_records),
                    'mode': 'backtest',
                }
                _set_listening_time_state(
                    selected_date,
                    activation_mode='algo-backtest',
                    listen_time=listen_time,
                    listen_timestamp=listen_timestamp,
                )
                await websocket.send_text(_build_message(
                    'listening_started',
                    'Execution listening started',
                    payload,
                ))
                continue

            if action == 'stop_listening':
                listening_active = False
                listening_records = []
                _clear_listening_strategy_snapshot(selected_date)
                _clear_market_cache_snapshot(market_cache)
                market_cache = None
                await websocket.send_text(_build_message(
                    'listening_stopped',
                    'Execution listening stopped',
                    {
                        'selected_date': selected_date,
                        'listen_time': _format_market_time_from_minutes(simulated_minutes),
                        'listen_timestamp': (
                            f'{selected_date}T{_format_market_time_from_minutes(simulated_minutes)}:00'
                            if selected_date
                            else _format_market_time_from_minutes(simulated_minutes)
                        ),
                        'mode': 'backtest',
                    },
                ))
                _clear_listening_strategy_snapshot(selected_date, activation_mode='algo-backtest')
                continue

            await websocket.send_text(_build_message(
                'test_response',
                'Execution websocket test response from backend',
                parsed_message,
            ))

    except WebSocketDisconnect:
        return
    finally:
        _unregister_channel_websocket('executions', websocket)
        _unregister_user_websocket(websocket)
        _clear_listening_strategy_snapshot(selected_date)
        _clear_market_cache_snapshot(market_cache)
        db.close()


@socket_router.websocket('/ws/execute-orders')
async def execute_orders_socket(
    websocket: WebSocket,
    user_id: str = Query(default=''),
    activation_mode: str = Query(default=''),
):
    await websocket.accept()
    # Legacy live/algo-backtest/fast-forward .html dashboards (via
    # assets/js/algo-stream-sockets.js) connect with ?user_id=... in the URL
    # and no auth handshake — keep that contract untouched. Callers that omit
    # user_id (algo-admin React app) are the new path and must authenticate
    # via the first message instead (see _ws_authenticate).
    is_legacy_caller = bool(str(user_id or '').strip())
    if is_legacy_caller:
        uid = str(user_id or '').strip()
    else:
        authenticated_user_id = await _ws_authenticate(websocket)
        if not authenticated_user_id:
            return
        uid = authenticated_user_id
    _register_channel_websocket('execute-orders', websocket)
    _register_user_websocket('execute-orders', uid, websocket)
    await websocket.send_text(_build_message(
        'connection_established',
        'Execute orders websocket connected',
        {'channel': 'execute-orders', 'user_id': uid, 'activation_mode': activation_mode},
    ))
    print(
        '[WS CONNECTED] '
        f'channel=execute-orders '
        f'user_id={uid} '
        f'activation_mode={activation_mode or "-"}'
    )
    db = MongoData()
    trade_date = _today_ist()
    activation_mode = str(activation_mode or '').strip() or 'algo-backtest'
    subscribed_group_id = ''
    autorun_enabled = False
    seen_signatures: dict[str, str] = {}

    # ── Initial emit: all today's strategies (running + cancelled + squared-off) ──
    try:
        _init_query: dict = {
            'trade_status': {'$in': [1, 2]},
            'creation_ts':  {'$regex': f'^{re.escape(trade_date)}'},
            'activation_mode': activation_mode,
        }
        if uid:
            _init_query['user_id'] = uid
        _init_raw     = list(db._db['algo_trades'].find(_init_query).sort('creation_ts', 1))
        _init_records = [_serialize_trade_record(r) for r in _init_raw]
        _init_enriched = _populate_legs_from_history(db, _init_records)
        _init_enriched = _attach_broker_configuration_details(db, _init_enriched)
        _seed_execute_order_signatures(_init_enriched, seen_signatures)
        await websocket.send_text(_build_message(
            'execute_order',
            'Initial strategies loaded',
            _build_execute_order_socket_payload(_init_enriched, trigger='initial_load', group_id=''),
        ))
        print(
            f'[EXECUTE-ORDERS INITIAL EMIT] '
            f'user={uid or "-"} '
            f'mode={activation_mode} '
            f'trade_date={trade_date} '
            f'total={len(_init_enriched)}'
        )
    except Exception as _init_exc:
        log.error('execute-orders initial emit error: %s', _init_exc)

    # ── Initial broker-settings emit: send if any open legs exist ──────────────
    try:
        _init_has_open = any(
            isinstance(leg, dict) and not leg.get('exit_trade') and leg.get('entry_trade')
            for r in _init_enriched
            for leg in (r.get('legs') or [])
        )
        if uid and _init_has_open:
            await emit_broker_settings_for_user(uid, activation_mode)
            print(f'[EXECUTE-ORDERS BROKER-SETTINGS INITIAL] user={uid} mode={activation_mode}')
    except Exception as _bs_init_exc:
        log.warning('execute-orders initial broker-settings emit error: %s', _bs_init_exc)

    try:
        while True:
            try:
                raw_message = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                print(
                    '[WS MESSAGE] '
                    f'channel=execute-orders '
                    f'user_id={uid or "-"} '
                    f'raw={raw_message}'
                )
                try:
                    parsed_message = json.loads(raw_message)
                except json.JSONDecodeError:
                    parsed_message = {}
                message_type = str((parsed_message or {}).get('type') or '').strip()
                requested_date = str((parsed_message or {}).get('trade_date') or '').strip()
                requested_mode = str((parsed_message or {}).get('activation_mode') or '').strip()
                requested_group_id = str((parsed_message or {}).get('group_id') or '').strip()
                requested_autoload = (parsed_message or {}).get('autoload')
                if is_legacy_caller:
                    requested_user_id = str((parsed_message or {}).get('user_id') or '').strip()
                    if requested_user_id and requested_user_id != uid:
                        uid = requested_user_id
                        _register_user_websocket('execute-orders', uid, websocket)
                if requested_date:
                    trade_date = requested_date
                    seen_signatures = {}
                    clear_user_emit_signatures(uid)
                if requested_mode:
                    activation_mode = requested_mode
                if isinstance(requested_autoload, bool):
                    autorun_enabled = requested_autoload
                elif str(requested_autoload).strip().lower() in {'true', 'false'}:
                    autorun_enabled = str(requested_autoload).strip().lower() == 'true'
                if requested_group_id != subscribed_group_id:
                    seen_signatures = {}
                subscribed_group_id = requested_group_id
                print(
                    '[WS SUBSCRIBED] '
                    f'channel=execute-orders '
                    f'user_id={uid or "-"} '
                    f'trade_date={trade_date or "-"} '
                    f'activation_mode={activation_mode or "-"} '
                    f'group_id={subscribed_group_id or "-"} '
                    f'autoload={autorun_enabled}'
                )
                if message_type == 'cancel-deployment':
                    cancel_strategy_id = str((parsed_message or {}).get('strategy_id') or '').strip()
                    cancel_group_id = str((parsed_message or {}).get('group_id') or '').strip()
                    print(
                        f'[CANCEL DEPLOYMENT] strategy_id={cancel_strategy_id!r} '
                        f'group_id={cancel_group_id!r} trade_date={trade_date}'
                    )
                    # Build query to find the records to cancel
                    cancel_query: dict[str, Any] = _build_trade_query(
                        trade_date, activation_mode=activation_mode, statuses=[RUNNING_STATUS]
                    )
                    cancel_query['active_on_server'] = True
                    if uid:
                        cancel_query['user_id'] = uid
                    if cancel_strategy_id:
                        cancel_query['_id'] = cancel_strategy_id
                    elif cancel_group_id:
                        cancel_query['portfolio.group_id'] = cancel_group_id
                    algo_trades_col = db._db['algo_trades']
                    cancelled_records: list[dict] = []
                    affected_group_id = cancel_group_id
                    try:
                        trade_records = list(algo_trades_col.find(cancel_query))
                        for trade_rec in trade_records:
                            t_id = str(trade_rec.get('_id') or '')
                            algo_trades_col.update_one(
                                {'_id': t_id},
                                {'$set': {'active_on_server': False, 'status': 'StrategyStatus.SquaredOff'}},
                            )
                            print(f'[CANCEL DEPLOYMENT] set active_on_server=False trade_id={t_id}')
                        # Reload the affected group including cancelled records (active_on_server=False)
                        affected_group_id = cancel_group_id or str(
                            (((trade_records[0].get('portfolio') or {}).get('group_id')) or '') if trade_records else ''
                        )
                        # Query without status filter — include SquaredOff records too
                        group_reload_query = _build_trade_query(trade_date, activation_mode=activation_mode)
                        if uid:
                            group_reload_query['user_id'] = uid
                        if cancel_strategy_id:
                            # Single strategy cancel — return only that strategy
                            group_reload_query['_id'] = cancel_strategy_id
                        elif affected_group_id:
                            # Group cancel — return all strategies in the group
                            group_reload_query['portfolio.group_id'] = affected_group_id
                        raw_group_records = list(algo_trades_col.find(group_reload_query))
                        cancelled_records = [_serialize_trade_record(r) for r in raw_group_records]
                    except Exception as exc:
                        log.error('cancel-deployment error: %s', exc)
                        print(f'[CANCEL DEPLOYMENT] error: {exc}')
                    if uid:
                        await emit_execute_order_for_user(
                            db,
                            user_id=uid,
                            trade_date=trade_date,
                            activation_mode=activation_mode,
                            group_id=cancel_group_id or affected_group_id,
                            trade_ids=[cancel_strategy_id] if cancel_strategy_id else [],
                            trigger='cancel-deployment',
                            message='Deployment cancelled',
                            force=True,
                        )
                    continue

                if message_type == 'squared-off':
                    sq_strategy_id = str((parsed_message or {}).get('strategy_id') or '').strip()
                    sq_group_id = str((parsed_message or {}).get('group_id') or '').strip()
                    print(
                        '[SQUARE OFF RECEIVED]',
                        {
                            'type': message_type,
                            'strategy_id': sq_strategy_id,
                            'group_id': sq_group_id,
                            'listen_timestamp': str((parsed_message or {}).get('listen_timestamp') or '').strip(),
                            'trade_date': trade_date,
                            'activation_mode': activation_mode,
                        },
                    )
                    print(
                        f'[SQUARE OFF] strategy_id={sq_strategy_id!r} '
                        f'group_id={sq_group_id!r} trade_date={trade_date}'
                    )
                    # Use listen_timestamp sent by frontend (latestListenTimestamp = backtest time)
                    exit_timestamp = str((parsed_message or {}).get('listen_timestamp') or '').strip()
                    # Normalize to "YYYY-MM-DD HH:MM:SS" format if needed
                    if exit_timestamp:
                        exit_timestamp = exit_timestamp.replace('T', ' ').rstrip('Z')
                    # Derive sq_trade_date from exit_timestamp or fall back via LISTENING_TIME_STATE
                    sq_trade_date = exit_timestamp[:10] if len(exit_timestamp) >= 10 else ''
                    if not exit_timestamp:
                        for _k, _v in LISTENING_TIME_STATE.items():
                            _ts = str(_v.get('listen_timestamp') or '').strip()
                            if _ts:
                                exit_timestamp = _ts.replace('T', ' ').rstrip('Z')
                                sq_trade_date = exit_timestamp[:10]
                                break
                    if not exit_timestamp:
                        exit_timestamp = _now_iso()
                        sq_trade_date = trade_date
                    print(
                        f'[SQUARE OFF] exit_timestamp={exit_timestamp!r} sq_trade_date={sq_trade_date!r}'
                    )
                    chain_col = db._db[OPTION_CHAIN_COLLECTION]
                    algo_trades_col = db._db['algo_trades']
                    history_col = db._db['algo_trade_positions_history']

                    # Build query to find trade records to square off
                    def _build_sq_query(date: str) -> dict[str, Any]:
                        q: dict[str, Any] = _build_trade_query(
                            date, activation_mode=activation_mode, statuses=[RUNNING_STATUS]
                        )
                        q['active_on_server'] = True
                        if uid:
                            q['user_id'] = uid
                        if sq_strategy_id:
                            q['$or'] = [{'_id': sq_strategy_id}, {'strategy_id': sq_strategy_id}]
                        elif sq_group_id:
                            q['portfolio.group_id'] = sq_group_id
                        return q

                    sq_records: list[dict] = []
                    affected_sq_group_id = sq_group_id
                    resolved_sq_date = sq_trade_date or trade_date
                    try:
                        trade_records = list(algo_trades_col.find(_build_sq_query(sq_trade_date)))

                        # Fallback: listen_timestamp date may not match strategy creation date.
                        # Retry without date filter so socket-added strategies are always found.
                        if not trade_records and (sq_strategy_id or sq_group_id):
                            print(
                                f'[SQUARE OFF] date-filtered query found 0 records '
                                f'(sq_trade_date={sq_trade_date!r}), retrying without date filter'
                            )
                            trade_records = list(algo_trades_col.find(_build_sq_query('')))
                            if trade_records:
                                resolved_sq_date = str(
                                    trade_records[0].get('creation_ts') or sq_trade_date or trade_date
                                )[:10]
                                print(f'[SQUARE OFF] fallback found {len(trade_records)} record(s), resolved_date={resolved_sq_date!r}')

                        for trade_rec in trade_records:
                            _square_off_trade_like_manual(
                                db,
                                trade_rec,
                                exit_timestamp=exit_timestamp,
                                activation_mode=activation_mode,
                            )

                        # Reload affected group and emit
                        affected_sq_group_id = sq_group_id or str(
                            (((trade_records[0].get('portfolio') or {}).get('group_id')) or '') if trade_records else ''
                        )
                        sq_reload_query = _build_trade_query(resolved_sq_date, activation_mode=activation_mode)
                        sq_reload_query.pop('trade_status', None)
                        sq_reload_query.pop('status', None)
                        if uid:
                            sq_reload_query['user_id'] = uid
                        if sq_strategy_id:
                            sq_reload_query['$or'] = [{'_id': sq_strategy_id}, {'strategy_id': sq_strategy_id}]
                        elif affected_sq_group_id:
                            sq_reload_query['portfolio.group_id'] = affected_sq_group_id
                        raw_sq_records = list(algo_trades_col.find(sq_reload_query))
                        sq_records = [_serialize_trade_record(r) for r in raw_sq_records]
                    except Exception as exc:
                        log.error('squared-off error: %s', exc)
                        print(f'[SQUARE OFF] error: {exc}')

                    emit_uid = uid
                    if emit_uid:
                        await emit_execute_order_for_user(
                            db,
                            user_id=emit_uid,
                            trade_date=resolved_sq_date,
                            activation_mode=activation_mode,
                            group_id=sq_group_id or affected_sq_group_id,
                            trade_ids=[sq_strategy_id] if sq_strategy_id else [],
                            trigger='squared-off',
                            message='Strategy squared off',
                            force=True,
                        )
                    continue

                if message_type in {'get_trades', 'get_orders'}:
                    trade_query = _build_trade_query(
                        trade_date,
                        activation_mode=activation_mode,
                        statuses=[RUNNING_STATUS],
                    )
                    trade_query['active_on_server'] = True
                    _print_open_strategy_fetch_flow(
                        'getting_open_strategy',
                        trade_date=trade_date,
                        activation_mode=activation_mode,
                    )
                    _print_trade_query_debug(
                        trade_query,
                        trade_date=trade_date,
                        activation_mode=activation_mode,
                    )
                    current_records = _load_execute_order_group_records(
                        db,
                        trade_date,
                        activation_mode,
                        subscribed_group_id,
                        uid,
                    )
                    _print_open_strategy_fetch_flow(
                        'open_strategy_taken',
                        trade_date=trade_date,
                        activation_mode=activation_mode,
                        count=len(current_records),
                    )
                    _print_running_trade_record_groups(
                        current_records,
                        trade_date=trade_date,
                        activation_mode=activation_mode,
                    )
                    enriched_current_records = _populate_legs_from_history(db, current_records)
                    _seed_execute_order_signatures(enriched_current_records, seen_signatures)
                    if subscribed_group_id:
                        _consume_execute_order_group_start(subscribed_group_id)
                    else:
                        _consume_all_execute_order_group_starts()
                    await websocket.send_text(_build_message(
                        'execute_order',
                        'Strategy execution refreshed',
                        _build_execute_order_socket_payload(
                            enriched_current_records,
                            trigger=message_type,
                            group_id=subscribed_group_id,
                        ),
                    ))
                    continue
                await websocket.send_text(_build_message(
                    'subscription_ack',
                    'Execute orders websocket subscription confirmed',
                    {
                        'trade_date': trade_date,
                        'activation_mode': activation_mode,
                        'group_id': subscribed_group_id,
                    },
                ))
                continue
            except asyncio.TimeoutError:
                # ── fast-forward / live: flush dirty-marked DB changes only ────
                if activation_mode in {'fast-forward', 'live', 'forward-test'} and uid:
                    # algo_trades + algo_trade_positions_history + algo_leg_feature_status
                    try:
                        await flush_pending_execute_order_emits(db, user_ids={uid}, force=False)
                    except Exception as _fe:
                        log.debug('execute-orders flush error: %s', _fe)
                    # strategy activity events (trail SL / SL hit / overall SL / reentry)
                    try:
                        await flush_pending_strategy_activities()
                    except Exception as _ae:
                        log.debug('strategy-activity flush error: %s', _ae)
                    # algo_borker_stoploss_settings
                    try:
                        if _consume_pending_broker_settings_dirty(uid, activation_mode):
                            await emit_broker_settings_for_user(uid, activation_mode)
                    except Exception as _be:
                        log.debug('broker-settings flush error: %s', _be)
                if not autorun_enabled:
                    continue

            # execute_order emits are driven by explicit DB-mutation call sites.
            # Keep the socket alive here, but do not poll and push structural state.
            continue

    except WebSocketDisconnect:
        return
    finally:
        _unregister_channel_websocket('execute-orders', websocket)
        _unregister_user_websocket(websocket)
        db.close()


@socket_router.websocket('/ws/update')
async def update_socket(
    websocket: WebSocket,
    user_id: str = Query(default=''),
    activation_mode: str = Query(default=''),
):
    await websocket.accept()
    # Legacy .html dashboards connect with ?user_id=... and no auth handshake
    # — keep that contract untouched. Callers that omit user_id (algo-admin
    # React app) must authenticate via the first message instead.
    is_legacy_caller = bool(str(user_id or '').strip())
    if is_legacy_caller:
        uid = str(user_id or '').strip()
    else:
        authenticated_user_id = await _ws_authenticate(websocket)
        if not authenticated_user_id:
            return
        uid = authenticated_user_id
    _register_channel_websocket('update', websocket)
    _register_user_websocket('update', uid, websocket)
    activation_mode = str(activation_mode or '').strip() or 'algo-backtest'
    await websocket.send_text(_build_message(
        'connection_established',
        'Update websocket connected',
        {'channel': 'update', 'user_id': uid, 'activation_mode': activation_mode},
    ))
    print(
        '[WS CONNECTED] '
        f'channel=update '
        f'user_id={uid} '
        f'activation_mode={activation_mode}'
    )
    db = MongoData()
    trade_date = _today_ist()
    subscription_status = ''
    subscribe_tokens: list[dict] = []
    market_cache: dict | None = None
    open_positions: list[dict] = []
    token_market_data: list[dict] = []
    last_running_trade_records: list[dict] = []
    first_open_strategy_payload_printed = False
    simulated_listen_at: datetime | None = None
    autorun_enabled = False
    prev_subscribed_token_set: set[str] = set()
    current_listen_hhmm: str = ''
    active_contracts: list[dict] = []
    send_lock = asyncio.Lock()
    live_tick_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=1)
    live_tick_listener = None
    live_tick_source = None
    live_tick_task: asyncio.Task | None = None
    live_tick_emit_interval_seconds = 0.5

    async def _send_update_message(message: str) -> None:
        async with send_lock:
            await websocket.send_text(message)

    def _build_live_tick_ltp_payload(tick_payload: dict) -> dict:
        timestamp = str(tick_payload.get('timestamp') or datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
        listen_time = timestamp[11:19] if len(timestamp) >= 19 else timestamp[11:16]
        changed_ltp_map = tick_payload.get('changed_ltp_map') if isinstance(tick_payload.get('changed_ltp_map'), dict) else {}
        full_ltp_map = tick_payload.get('ltp_map') if isinstance(tick_payload.get('ltp_map'), dict) else {}
        spot_map = tick_payload.get('spot_map') if isinstance(tick_payload.get('spot_map'), dict) else {}

        subscribed_token_values = {
            str(item.get('token') or '').strip()
            for item in (subscribe_tokens or [])
            if str(item.get('token') or '').strip()
        }
        if subscribed_token_values:
            option_token_values = [tok for tok in subscribed_token_values if tok not in SPOT_UNDERLYING_BY_TOKEN]
        else:
            option_token_values = [tok for tok in changed_ltp_map.keys() if tok not in SPOT_UNDERLYING_BY_TOKEN]

        option_ltp = []
        for tok in option_token_values:
            ltp_value = changed_ltp_map.get(tok, full_ltp_map.get(tok))
            if ltp_value is None:
                continue
            ltp_float = _safe_float(ltp_value)
            if ltp_float <= 0:
                continue
            option_ltp.append({
                'token': tok,
                'ltp': ltp_float,
                'timestamp': timestamp,
            })

        spot_ltp = []
        for token, underlying in SPOT_UNDERLYING_BY_TOKEN.items():
            ltp_value = spot_map.get(underlying)
            if ltp_value is None:
                ltp_value = changed_ltp_map.get(token, full_ltp_map.get(token))
            ltp_float = _safe_float(ltp_value)
            if ltp_float <= 0:
                continue
            spot_ltp.append({
                'token': token,
                'ltp': ltp_float,
                'underlying': underlying,
                'option_type': 'SPOT',
                'timestamp': timestamp,
            })

        return {
            'trade_date': trade_date,
            'listen_time': listen_time,
            'listen_timestamp': timestamp,
            'ltp': spot_ltp + option_ltp,
            'spot_map': dict(spot_map),
            'broker_status': str(tick_payload.get('status') or ''),
            'mode': activation_mode,
            'tick_count': int(tick_payload.get('tick_count') or 0),
            'emit_source': 'kite_tick_listener',
        }

    def _detach_live_tick_listener() -> None:
        nonlocal live_tick_listener
        nonlocal live_tick_source
        if live_tick_source and live_tick_listener and hasattr(live_tick_source, 'remove_tick_listener'):
            try:
                live_tick_source.remove_tick_listener(live_tick_listener)
            except Exception as exc:
                log.debug('remove live tick listener error: %s', exc)
        live_tick_listener = None
        live_tick_source = None

    def _attach_live_tick_listener() -> None:
        nonlocal live_tick_listener
        nonlocal live_tick_source
        if activation_mode != 'live':
            _detach_live_tick_listener()
            return
        try:
            from features.live_monitor_socket import _get_active_ticker_manager
            ticker_source = _get_active_ticker_manager()
        except Exception as exc:
            log.debug('get active ticker manager error: %s', exc)
            return
        if ticker_source is live_tick_source and live_tick_listener:
            return
        _detach_live_tick_listener()
        if not hasattr(ticker_source, 'add_tick_listener'):
            return

        loop = asyncio.get_running_loop()

        def _push_tick_payload(payload: dict) -> None:
            try:
                if live_tick_queue.full():
                    try:
                        live_tick_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                live_tick_queue.put_nowait(payload)
            except Exception:
                pass

        def _on_live_tick(tick_payload: dict) -> None:
            try:
                loop.call_soon_threadsafe(_push_tick_payload, dict(tick_payload or {}))
            except Exception:
                pass

        ticker_source.add_tick_listener(_on_live_tick)
        live_tick_listener = _on_live_tick
        live_tick_source = ticker_source

    async def _live_tick_sender() -> None:
        while True:
            tick_payload = await live_tick_queue.get()
            if activation_mode != 'live':
                continue
            try:
                latest_tick_payload = dict(tick_payload or {})
                window_deadline = asyncio.get_running_loop().time() + live_tick_emit_interval_seconds
                while True:
                    remaining = window_deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        break
                    try:
                        next_tick_payload = await asyncio.wait_for(live_tick_queue.get(), timeout=remaining)
                    except asyncio.TimeoutError:
                        break
                    latest_tick_payload = dict(next_tick_payload or {})

                payload = _build_live_tick_ltp_payload(latest_tick_payload)
                await _send_update_message(_build_message(
                    'ltp_update',
                    'Live LTP tick (500ms)',
                    payload,
                ))
            except Exception as exc:
                log.debug('live tick sender error: %s', exc)

    def refresh_position_snapshot(refresh_reason: str = '') -> None:
        nonlocal market_cache
        nonlocal open_positions
        nonlocal subscribe_tokens
        nonlocal token_market_data
        nonlocal last_running_trade_records
        nonlocal first_open_strategy_payload_printed
        nonlocal active_contracts

        _print_position_refresh_status(
            'refresh_started',
            trade_date=trade_date,
            activation_mode=activation_mode,
            reason=refresh_reason,
        )

        trade_query = _build_trade_query(
            trade_date,
            activation_mode=activation_mode,
            statuses=[RUNNING_STATUS],
        )
        trade_query['active_on_server'] = True
        if _is_algo_backtest_status(subscription_status):
            _print_open_strategy_fetch_flow(
                'getting_open_strategy',
                trade_date=trade_date,
                activation_mode=activation_mode,
            )
            _print_trade_query_debug(
                trade_query,
                trade_date=trade_date,
                activation_mode=activation_mode,
            )

        records = _load_running_trade_records(
            db,
            trade_date,
            activation_mode=activation_mode,
            user_id=uid,
        )
        # live/fast-forward: legs[] stores string history IDs, not full dicts.
        # _build_open_position_updates skips non-dict legs, so subscribe_tokens
        # ends up with only spot tokens. Enrich with full leg objects from history
        # so open_positions and subscribe_tokens are correctly populated.
        if _is_strict_history_leg_mode(activation_mode):
            records = _populate_legs_from_history(db, records)
        last_running_trade_records = [dict(record) for record in records if isinstance(record, dict)]
        if _is_algo_backtest_status(subscription_status):
            _print_open_strategy_fetch_flow(
                'open_strategy_taken',
                trade_date=trade_date,
                activation_mode=activation_mode,
                count=len(records),
            )
            _print_running_trade_record_groups(
                records,
                trade_date=trade_date,
                activation_mode=activation_mode,
            )
            if not first_open_strategy_payload_printed and records:
                _print_first_open_strategy_payload(
                    records,
                    trade_date=trade_date,
                    activation_mode=activation_mode,
                )
                first_open_strategy_payload_printed = True
        if _is_algo_backtest_status(subscription_status) and not market_cache:
            market_cache = _preload_trade_date_market_cache(db, trade_date, records)
        strategy_spot_tokens = _build_strategy_spot_subscribe_tokens(
            records,
            db,
            trade_date,
            market_cache=market_cache,
            activation_mode=activation_mode,
        )
        open_positions = _build_open_position_updates(
            records,
            db,
            trade_date,
            market_cache=market_cache,
            activation_mode=activation_mode,
        )
        subscribe_tokens = _build_subscribe_tokens(
            open_positions,
            strategy_spot_tokens=strategy_spot_tokens,
        )
        if uid:
            register_update_user_tokens(uid, subscribe_tokens)
        token_market_data = _build_subscribed_token_market_data(
            open_positions,
            strategy_spot_tokens=strategy_spot_tokens,
            db=db,
            trade_date=trade_date,
            market_cache=market_cache,
            activation_mode=activation_mode,
        )
        active_contracts = _build_active_contracts_from_records(
            last_running_trade_records,
            db=db,
            trade_date=trade_date,
            market_cache=market_cache,
            activation_mode=activation_mode,
        )
        _append_momentum_pending_to_contracts(active_contracts, db, last_running_trade_records)
        subscribed_token_values = [
            str(item.get('token') or '').strip()
            for item in subscribe_tokens
            if str(item.get('token') or '').strip()
        ]
        print(f'[SUBSCRIBED TOKENS] {json.dumps(subscribed_token_values)}')
        _print_position_refresh_status(
            'refresh_completed',
            trade_date=trade_date,
            activation_mode=activation_mode,
            reason=refresh_reason,
            count=len(open_positions),
        )

    live_tick_task = asyncio.create_task(_live_tick_sender())
    _attach_live_tick_listener()

    # For live/fast-forward: immediately load positions and emit on connect
    if activation_mode in {'live', 'fast-forward', 'forward-test'}:
        try:
            await asyncio.get_running_loop().run_in_executor(None, refresh_position_snapshot, 'initial_connect')
            await websocket.send_text(_build_message(
                'update',
                'Open position snapshot (initial)',
                {
                    'trade_date': trade_date,
                    'activation_mode': activation_mode,
                    'status': subscription_status,
                    'trigger_reason': 'initial_connect',
                    'refresh_status': 'completed',
                    'open_positions': open_positions,
                    'subscribe_tokens': subscribe_tokens,
                    'token_market_data': token_market_data,
                    'count': len(open_positions),
                },
            ))
            # Re-subscribe all open position tokens to Kite WS on connect.
            # Needed when ws/update reconnects after Kite ticker was already running
            # (token subscriptions from initial leg entry may not cover a new client session).
            if activation_mode == 'live' and subscribe_tokens:
                try:
                    from features.live_event import _subscribe_live_option_token
                    for _st in subscribe_tokens:
                        _tok = str(_st.get('token') or '').strip()
                        _sym = str(_st.get('symbol') or '').strip()
                        if _tok and _tok.isdigit():
                            _subscribe_live_option_token(_tok, _sym)
                except Exception as _sub_exc:
                    log.debug('ws/update initial kite re-subscribe error: %s', _sub_exc)
        except Exception as _init_exc:
            log.debug('ws/update initial live refresh error: %s', _init_exc)

    try:
        while True:
            try:
                raw_message = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                print(
                    '[WS MESSAGE] '
                    f'channel=update '
                    f'user_id={uid or "-"} '
                    f'raw={raw_message}'
                )
                try:
                    parsed_message = json.loads(raw_message)
                except json.JSONDecodeError:
                    parsed_message = {}
                requested_date = str((parsed_message or {}).get('trade_date') or '').strip()
                requested_mode = str((parsed_message or {}).get('activation_mode') or '').strip()
                requested_status = str((parsed_message or {}).get('status') or '').strip()
                requested_action = str((parsed_message or {}).get('action') or '').strip().lower()
                requested_reason = str((parsed_message or {}).get('reason') or '').strip()
                requested_listen_timestamp = str((parsed_message or {}).get('listen_timestamp') or '').strip()
                requested_autoload = (parsed_message or {}).get('autoload')
                if is_legacy_caller:
                    requested_user_id = str((parsed_message or {}).get('user_id') or '').strip()
                    if requested_user_id and requested_user_id != uid:
                        uid = requested_user_id
                        _register_user_websocket('update', uid, websocket)
                _print_position_refresh_status(
                    'message_received',
                    trade_date=requested_date or trade_date,
                    activation_mode=requested_mode or activation_mode,
                    reason=requested_reason or requested_action or 'subscription',
                )
                if requested_date:
                    trade_date = requested_date
                    first_open_strategy_payload_printed = False
                if requested_mode:
                    activation_mode = requested_mode
                    first_open_strategy_payload_printed = False
                    _attach_live_tick_listener()
                if requested_status:
                    subscription_status = requested_status
                if isinstance(requested_autoload, bool):
                    autorun_enabled = requested_autoload
                elif str(requested_autoload).strip().lower() in {'true', 'false'}:
                    autorun_enabled = str(requested_autoload).strip().lower() == 'true'
                if requested_listen_timestamp:
                    parsed_listen_at = _parse_listen_timestamp(requested_listen_timestamp)
                    if parsed_listen_at is not None:
                        simulated_listen_at = parsed_listen_at
                elif simulated_listen_at is None:
                    listening_time_state = _get_listening_time_state(
                        requested_date or trade_date,
                        activation_mode=requested_mode or activation_mode,
                    )
                    fallback_listen_timestamp = str((listening_time_state or {}).get('listen_timestamp') or '').strip()
                    parsed_listen_at = _parse_listen_timestamp(fallback_listen_timestamp)
                    if parsed_listen_at is not None:
                        simulated_listen_at = parsed_listen_at
                    elif requested_date or trade_date:
                        simulated_listen_at = _parse_listen_timestamp(f'{requested_date or trade_date}T09:15:00')
                print(
                    '[WS SUBSCRIBED] '
                    f'channel=update '
                    f'user_id={uid or "-"} '
                    f'trade_date={trade_date or "-"} '
                    f'activation_mode={activation_mode or "-"} '
                    f'status={subscription_status or "-"} '
                    f'autoload={autorun_enabled} '
                    f'listen_timestamp={requested_listen_timestamp or ((listening_time_state or {}).get("listen_timestamp") if "listening_time_state" in locals() else "") or "-"}'
                )
                if requested_date or requested_mode or requested_status:
                    _clear_market_cache_snapshot(market_cache)
                    market_cache = None
                    open_positions = []
                    subscribe_tokens = []
                    token_market_data = []
                    last_running_trade_records = []
                should_refresh_now = requested_action == 'get-position' or bool(
                    requested_date or requested_mode or requested_status
                )
                if requested_action in {'get-broker-positions', 'get-dhan-positions'}:
                    broker_positions_payload = _fetch_dhan_broker_option_positions(db)
                    open_positions = list(broker_positions_payload.get('open_positions') or [])
                    subscribe_tokens = list(broker_positions_payload.get('subscribe_tokens') or [])
                    token_market_data = list(broker_positions_payload.get('token_market_data') or [])
                    if uid:
                        register_update_user_tokens(uid, subscribe_tokens)
                    await _send_update_message(_build_message(
                        'update',
                        'Broker positions snapshot refreshed',
                        {
                            'trade_date': trade_date,
                            'activation_mode': activation_mode,
                            'status': subscription_status,
                            'trigger_reason': requested_reason or 'broker_positions',
                            'refresh_status': 'completed' if broker_positions_payload.get('ok') else 'error',
                            'source': 'dhan_positions',
                            'detail': str(broker_positions_payload.get('detail') or ''),
                            'open_positions': open_positions,
                            'subscribe_tokens': subscribe_tokens,
                            'token_market_data': token_market_data,
                            'subscribed_tokens_count': len(subscribe_tokens),
                            'count': len(open_positions),
                        },
                    ))
                    await _send_update_message(_build_message(
                        'ltp_update',
                        'Broker LTP update for active positions',
                        {
                            'trade_date': trade_date,
                            'listen_time': current_listen_hhmm,
                            'listen_timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                            'source': 'dhan_positions',
                            'ltp': [
                                {
                                    'token': str(item.get('token') or ''),
                                    'ltp': _safe_float(item.get('ltp')),
                                    'underlying': str(item.get('underlying') or ''),
                                    'option_type': str(item.get('option') or ''),
                                }
                                for item in open_positions
                                if str(item.get('token') or '').strip()
                            ],
                        },
                    ))
                    continue
                if should_refresh_now:
                    _refresh_reason = requested_reason or ('explicit_trigger' if requested_action == 'get-position' else 'subscription')
                    await asyncio.get_running_loop().run_in_executor(None, refresh_position_snapshot, _refresh_reason)
                    # For live: re-subscribe option tokens to Kite WS so a refresh
                    # triggered by a client message also ensures subscription is current.
                    if activation_mode == 'live' and subscribe_tokens:
                        try:
                            from features.live_event import _subscribe_live_option_token as _resub
                            for _rst in subscribe_tokens:
                                _rtok = str(_rst.get('token') or '').strip()
                                _rsym = str(_rst.get('symbol') or '').strip()
                                if _rtok and _rtok.isdigit():
                                    _resub(_rtok, _rsym)
                        except Exception:
                            pass
                    await _send_update_message(_build_message(
                        'update',
                        'Open position snapshot refreshed',
                        {
                            'trade_date': trade_date,
                            'activation_mode': activation_mode,
                            'status': subscription_status,
                            'trigger_reason': requested_reason or (
                                'explicit_trigger' if requested_action == 'get-position' else 'subscription'
                            ),
                            'refresh_status': 'completed',
                            'open_positions': open_positions,
                            'subscribe_tokens': subscribe_tokens,
                            'token_market_data': token_market_data,
                            'subscribed_tokens_count': len(subscribe_tokens),
                            'count': len(open_positions),
                        },
                    ))
                    manual_listen_timestamp = requested_listen_timestamp or (
                        f'{trade_date}T{current_listen_hhmm}:00'
                        if trade_date and current_listen_hhmm else ''
                    )
                    ltp_list: list[dict] = []
                    if manual_listen_timestamp and active_contracts:
                        if _is_strict_history_leg_mode(activation_mode):
                            active_contracts = _merge_default_spot_contracts(
                                active_contracts,
                                db=db,
                                trade_date=trade_date,
                                market_cache=market_cache,
                                activation_mode=activation_mode,
                            )
                        chain_col = db._db[OPTION_CHAIN_COLLECTION]
                        index_spot_col = db._db['option_chain_index_spot']
                        option_contracts = [c for c in active_contracts if c.get('option') != 'SPOT']
                        spot_contracts = [c for c in active_contracts if c.get('option') == 'SPOT']
                        option_ticks = _fetch_active_leg_ticks(
                            chain_col,
                            option_contracts,
                            manual_listen_timestamp,
                            market_cache=market_cache,
                            activation_mode=activation_mode,
                        )
                        option_ltp = [_format_chain_tick_as_ltp(t) for t in option_ticks]
                        spot_ltp = _fetch_spot_ticks(
                            index_spot_col,
                            spot_contracts,
                            manual_listen_timestamp,
                            market_cache=market_cache,
                            activation_mode=activation_mode,
                        )
                        _vix_item = _fetch_vix_ltp_item(db, manual_listen_timestamp, market_cache)
                        ltp_list = option_ltp + spot_ltp + ([_vix_item] if _vix_item else [])
                    await _send_update_message(_build_message(
                        'ltp_update',
                        'LTP update for active positions',
                        {
                            'trade_date': trade_date,
                            'listen_time': current_listen_hhmm,
                            'listen_timestamp': manual_listen_timestamp,
                            'ltp': ltp_list,
                        },
                    ))
                    if requested_action == 'get-position':
                        continue
                await _send_update_message(_build_message(
                    'subscription_ack',
                    'Update websocket subscription confirmed',
                    {
                        'trade_date': trade_date,
                        'activation_mode': activation_mode,
                        'status': subscription_status,
                        'position_refresh_enabled': True,
                    },
                ))
                continue
            except asyncio.TimeoutError:
                # ── Fast-forward / live: emit live ticker LTP, skip backtest simulation ──
                # NOTE: runs regardless of autorun_enabled — no gate here
                # Only emit when monitor server is running (not needed for pure backtest)
                try:
                    from features.live_monitor_socket import live_monitor_loop as _lml
                    _live_monitor_running = _lml.running
                except Exception:
                    _live_monitor_running = False
                try:
                    from features.live_fast_monitor import live_fast_monitor_supervisor as _lffms
                    _ff_monitor_running = _lffms._running
                except Exception:
                    _ff_monitor_running = False
                _monitor_running = (
                    _ff_monitor_running if activation_mode in ('fast-forward', 'forward-test') else _live_monitor_running
                )
                # Fast-forward / live: _live_tick_sender handles LTP via tick listener.
                # Timeout handler must not run the backtest simulation path for these modes.
                # If no tick listener (Kite not connected), emit current IST time as fallback.
                if activation_mode == 'live':
                    if _monitor_running and not live_tick_listener:
                        try:
                            from features.live_monitor_socket import _get_active_ticker_manager, _SPOT_TOKEN_BY_UNDERLYING
                            _live_tm = _get_active_ticker_manager()
                            _ist = timezone(timedelta(hours=5, minutes=30))
                            _now_ts = datetime.now(_ist).strftime('%Y-%m-%dT%H:%M:%S')
                            _listen_hhmm = _now_ts[11:16]
                            _spot_token_set = set(_SPOT_TOKEN_BY_UNDERLYING.values())
                            _spot_ltp = [
                                {
                                    'token': _SPOT_TOKEN_BY_UNDERLYING.get(und, ''),
                                    'ltp': float(lp),
                                    'underlying': und,
                                    'option_type': 'SPOT',
                                    'timestamp': _now_ts,
                                }
                                for und, lp in _live_tm.spot_map.items()
                                if lp and float(lp) > 0
                            ]
                            _option_ltp = [
                                {
                                    'token': tok,
                                    'ltp': float(lp),
                                    'timestamp': _now_ts,
                                }
                                for tok, lp in _live_tm.ltp_map.items()
                                if tok not in _spot_token_set and lp and float(lp) > 0
                            ]
                            await _send_update_message(_build_message(
                                'ltp_update',
                                'Live LTP tick',
                                {
                                    'trade_date':       trade_date,
                                    'listen_time':      _listen_hhmm,
                                    'listen_timestamp': _now_ts,
                                    'ltp':              _spot_ltp + _option_ltp,
                                    'spot_map':         dict(_live_tm.spot_map),
                                    'broker_status':    _live_tm.status,
                                    'mode':             activation_mode,
                                },
                            ))
                            _spot_summary = '  '.join(
                                f'{s["underlying"]} = {s["ltp"]:.2f}' for s in _spot_ltp
                            ) or 'no spot data'
                            print(
                                f'[EMIT → update socket]  {_now_ts}'
                                f'  |  user: {uid or "-"}'
                                f'  |  mode: {activation_mode}'
                                f'  |  broker: {_live_tm.status}'
                                f'  |  spot: {_spot_summary}'
                                f'  |  option tokens: {len(_option_ltp)}'
                            )
                        except Exception as _ltp_exc:
                            log.debug('live ltp_update error: %s', _ltp_exc)
                    # Always skip backtest simulation for live mode
                    continue
                # Fast-forward: refresh subscribe_tokens when dirty (new positions entered).
                # Without this, _user_tokens in the LTP broadcast stays stale after a leg
                # is entered and the new token never appears in the per-user LTP filter.
                if activation_mode in ('fast-forward', 'forward-test'):
                    if uid:
                        _ff_dirty_key = f'{uid}|{trade_date}|{activation_mode}'
                        _ff_is_dirty = bool(PENDING_FF_TOKEN_REFRESH_DIRTY.pop(_ff_dirty_key, False))
                    else:
                        _ff_is_dirty = False
                    if _ff_is_dirty:
                        try:
                            await asyncio.get_running_loop().run_in_executor(
                                None, refresh_position_snapshot, 'ff_new_position'
                            )
                        except Exception as _ffd_exc:
                            log.debug('ff dirty position refresh error: %s', _ffd_exc)
                    continue
                # backtest simulation path — only runs when autorun is enabled
                if not autorun_enabled:
                    continue
                if simulated_listen_at is None and trade_date:
                    listening_time_state = _get_listening_time_state(
                        trade_date,
                        activation_mode=activation_mode,
                    )
                    fallback_listen_timestamp = str((listening_time_state or {}).get('listen_timestamp') or '').strip()
                    simulated_listen_at = (
                        _parse_listen_timestamp(fallback_listen_timestamp)
                        or _parse_listen_timestamp(f'{trade_date}T09:15:00')
                    )
                market_close_at = _parse_listen_timestamp(f'{trade_date}T15:30:00')
                if simulated_listen_at is not None and market_close_at is not None and simulated_listen_at <= market_close_at:
                    listen_timestamp = _format_listen_timestamp(simulated_listen_at)
                    listen_time = simulated_listen_at.strftime('%H:%M:%S')
                    current_listen_hhmm = listen_time[:5]
                    print(f"[UPDATE SOCKET] listen_time={listen_timestamp}")

                    # ── Reload records every tick to pick up newly activated strategies ──
                    last_running_trade_records = _load_running_trade_records(
                        db, trade_date, activation_mode=activation_mode, user_id=uid
                    )

                    # ── Entry execution on matching entry_time ─────────────────
                    if _is_algo_backtest_status(subscription_status) and last_running_trade_records:
                        entries_executed = _execute_backtest_entries(
                            db,
                            last_running_trade_records,
                            current_listen_hhmm,
                            listen_timestamp,
                            market_cache=market_cache,
                        )
                        if entries_executed:
                            entered_count = sum(1 for e in entries_executed if e.get('entered'))
                            failed_count = len(entries_executed) - entered_count
                            for entry in entries_executed:
                                if entry.get('entered'):
                                    print(
                                        f'[ENTRY SUCCESS] '
                                        f"trade_date={trade_date} "
                                        f"listen_time={current_listen_hhmm} "
                                        f"strategy_name={entry.get('strategy_name') or '-'} "
                                        f"group_name={entry.get('group_name') or '-'} "
                                        f"leg_id={entry.get('leg_id') or '-'} "
                                        f"status=entry_taken"
                                    )
                                else:
                                    print(
                                        f'[ENTRY FAILED] '
                                        f"trade_date={trade_date} "
                                        f"listen_time={current_listen_hhmm} "
                                        f"strategy_name={entry.get('strategy_name') or '-'} "
                                        f"leg_id={entry.get('leg_id') or '-'} "
                                        f"reason={entry.get('reason') or '-'}"
                                    )
                            print(
                                f'[ENTRY SUMMARY] '
                                f"listen_time={current_listen_hhmm} "
                                f"entered={entered_count} "
                                f"failed={failed_count}"
                            )

                    # ── SL / TP / Overall checks for all open legs ─────────────
                    if _is_algo_backtest_status(subscription_status):
                        try:
                            bt_tick_result = _backtest_minute_tick(
                                db,
                                trade_date,
                                listen_timestamp,
                                activation_mode=activation_mode,
                                market_cache=market_cache,
                            )
                            if bt_tick_result.get('actions_taken'):
                                print(
                                    f'[BT TICK] listen_time={current_listen_hhmm} '
                                    f"actions={bt_tick_result['actions_taken']}"
                                )
                            # emit hit strategies + LTP snapshot to execute-orders socket
                            _ws_hit_ids       = bt_tick_result.get('hit_trade_ids') or []
                            _ws_ltp_snapshots = bt_tick_result.get('hit_ltp_snapshots') or {}
                            if _ws_hit_ids:
                                try:
                                    _ws_hit_raw = list(db._db['algo_trades'].find({'_id': {'$in': _ws_hit_ids}}))
                                    _ws_hit_ser = [_serialize_trade_record(r) for r in _ws_hit_raw]
                                    _ws_hit_enr = _populate_legs_from_history(db, _ws_hit_ser)
                                    # Group by user_id and send per-user
                                    _ws_by_user: dict[str, list[dict]] = {}
                                    for _wr in _ws_hit_enr:
                                        _wu = str(_wr.get('user_id') or '').strip()
                                        _ws_by_user.setdefault(_wu, []).append(_wr)
                                    for _wu, _wu_records in _ws_by_user.items():
                                        _ws_payload = _build_execute_order_socket_payload(_wu_records, trigger='sl-target-hit')
                                        _ws_payload['hit_ltp_snapshots'] = _ws_ltp_snapshots
                                        _ws_hit_msg = _build_message('execute_order', 'Strategy SL/Target hit', _ws_payload)
                                        if _wu:
                                            await _broadcast_user_channel_message(_wu, 'execute-orders', _ws_hit_msg)
                                        else:
                                            await _broadcast_channel_message('execute-orders', _ws_hit_msg)
                                    print(f'[SL/TGT HIT EMIT] sent {len(_ws_hit_enr)} strategy(s) to execute-orders hit_ids={_ws_hit_ids}')
                                    for _tid, _ltp_snap in _ws_ltp_snapshots.items():
                                        print(f'  [LTP SNAPSHOT] trade={_tid} legs={[{"leg_id": l.get("leg_id"), "ltp": l.get("ltp"), "entry_price": l.get("entry_price"), "pnl": l.get("pnl")} for l in _ltp_snap]}')
                                except Exception as _whe:
                                    log.warning('[SL/TGT HIT EMIT] ws error: %s', _whe)
                        except Exception as _bte:
                            log.warning('_backtest_minute_tick error: %s', _bte)

                    if _is_algo_backtest_status(subscription_status):
                        try:
                            await flush_pending_execute_order_emits(
                                db,
                                user_ids=set(_get_connected_user_ids('execute-orders')),
                                force=False,
                            )
                        except Exception as _fee:
                            log.warning('flush_pending_execute_order_emits error: %s', _fee)
                        try:
                            await flush_pending_strategy_activities()
                        except Exception as _fae:
                            log.warning('flush_pending_strategy_activities error: %s', _fae)

                    # ── Always rebuild active_contracts from current records ────
                    # Includes already-entered legs from any previous tick
                    active_contracts = _build_active_contracts_from_records(
                        last_running_trade_records, db=db,
                        trade_date=trade_date,
                        market_cache=market_cache,
                        activation_mode=activation_mode,
                    )
                    active_contracts = _merge_default_spot_contracts(
                        active_contracts,
                        db=db,
                        trade_date=trade_date,
                        market_cache=market_cache,
                        activation_mode=activation_mode,
                    )
                    # Also include momentum-pending legs so their LTP is sent to the frontend
                    # while they wait for the momentum trigger price to be hit.
                    _append_momentum_pending_to_contracts(active_contracts, db, last_running_trade_records)

                    # ── LTP tick for active positions ──────────────────────────
                    subscribed_token_strs = [
                        c.get('token') or '' for c in active_contracts if c.get('token')
                    ]
                    current_token_set = set(subscribed_token_strs)
                    newly_subscribed = current_token_set - prev_subscribed_token_set
                    newly_unsubscribed = prev_subscribed_token_set - current_token_set
                    for tok in newly_subscribed:
                        leg_meta = next((c for c in active_contracts if c.get('token') == tok), {})
                        print(
                            f'[TOKEN SUBSCRIBE] listen_time={current_listen_hhmm} '
                            f"token={tok} "
                            f"leg_id={leg_meta.get('leg_id') or '-'} "
                            f"strike={leg_meta.get('strike') or '-'} "
                            f"option={leg_meta.get('option') or '-'} "
                            f"expiry={leg_meta.get('expiry_date') or '-'}"
                        )
                    for tok in newly_unsubscribed:
                        print(f'[TOKEN UNSUBSCRIBE] listen_time={current_listen_hhmm} token={tok}')
                    prev_subscribed_token_set = current_token_set
                    print(
                        f'[SUBSCRIBED TOKENS] '
                        f"listen_time={current_listen_hhmm} "
                        f"count={len(subscribed_token_strs)} "
                        f"tokens={subscribed_token_strs}"
                    )
                    if listen_timestamp:
                        ltp_list: list[dict] = []
                        if active_contracts:
                            chain_col = db._db[OPTION_CHAIN_COLLECTION]
                            index_spot_col = db._db['option_chain_index_spot']

                            option_contracts = [c for c in active_contracts if c.get('option') != 'SPOT']
                            spot_contracts   = [c for c in active_contracts if c.get('option') == 'SPOT']

                            option_ticks = _fetch_active_leg_ticks(
                                chain_col,
                                option_contracts,
                                listen_timestamp,
                                market_cache=market_cache,
                                activation_mode=activation_mode,
                            )
                            option_ltp = [_format_chain_tick_as_ltp(t) for t in option_ticks]
                            spot_ltp = _fetch_spot_ticks(
                                index_spot_col,
                                spot_contracts,
                                listen_timestamp,
                                market_cache=market_cache,
                                activation_mode=activation_mode,
                            )
                            _vix_item = _fetch_vix_ltp_item(db, listen_timestamp, market_cache)
                            ltp_list = option_ltp + spot_ltp + ([_vix_item] if _vix_item else [])
                            print(
                                f'[LTP] listen_time={current_listen_hhmm} '
                                f'option={len(option_ltp)} spot={len(spot_ltp)} vix={1 if _vix_item else 0} total={len(ltp_list)}'
                            )
                        # Always emit ltp_update every tick so frontend timestamp stays
                        # in sync even before any legs enter (ltp=[] keeps countdown ticking)
                        await _send_update_message(_build_message(
                            'ltp_update',
                            'LTP update for active positions',
                            {
                                'trade_date': trade_date,
                                'listen_time': current_listen_hhmm,
                                'listen_timestamp': listen_timestamp,
                                'ltp': ltp_list,
                            },
                        ))

                    simulated_listen_at = simulated_listen_at + timedelta(minutes=1)
                else:
                    print('[UPDATE SOCKET] listen_time=unavailable')
                if _is_algo_backtest_status(subscription_status):
                    _print_running_trade_record_groups(
                        last_running_trade_records,
                        trade_date=trade_date,
                        activation_mode=activation_mode,
                        listen_time=current_listen_hhmm,
                    )
                    # Sync already-entered legs (imported with entry_trade) to history
                    _sync_entered_legs_to_history(db, last_running_trade_records)
                _print_open_position_groups(open_positions, trade_date=trade_date)
            continue

    except WebSocketDisconnect:
        return
    finally:
        _detach_live_tick_listener()
        if live_tick_task:
            live_tick_task.cancel()
        _unregister_channel_websocket('update', websocket)
        _unregister_user_websocket(websocket)
        if uid and not (CONNECTED_USER_CHANNEL_WEBSOCKETS.get(f'{uid}:update') or []):
            unregister_update_user_tokens(uid)
        _clear_market_cache_snapshot(market_cache)
        db.close()
