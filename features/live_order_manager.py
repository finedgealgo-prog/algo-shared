"""
live_order_manager.py
─────────────────────
Places, tracks and converts broker orders for live-mode strategy entries and exits.

Broker selection
────────────────
Each algo_trade has a `broker` field (ObjectId → broker_configuration).
get_broker_for_trade() reads the broker doc's `name` / `broker_icon` field:
  - "flattrade" in name/icon → FlatTradeAdapter  (FlatTrade REST API)
  - otherwise                → KiteConnect        (Zerodha Kite API)

LTP data always comes from Kite WebSocket (kite_ticker), regardless of which
broker is used for order placement.

Entry flow
──────────
1. Entry conditions met → place_live_entry_order()
   - Reads EntryOrder config from leg_cfg (defaults: LIMIT, LimitBuffer=3pts, ConvertAfter=40s)
   - Calculates limit_price = LTP ± LimitBuffer
   - Places order via selected broker (Kite or FlatTrade)
   - Returns {order_id, order_type, limit_price, order_status}

2. entry_trade_payload stored in DB with order_id + order_status='OPEN'

3. Background poller (poll_pending_order_fills) called from live_fast_monitor loop
   - Finds legs with order_status='OPEN'
   - Calls broker.orders() to check fill
   - On fill  → updates entry_trade.price = actual_fill_price, order_status='COMPLETE'
   - On cancel/reject → marks order_status='REJECTED', entry_trade.price=0
   - On timeout (ConvertAfter exceeded) → cancel + re-place as aggressive limit (bid/ask based)

Exit flow
─────────
close_leg_in_db already handles writing exit_trade.
This module provides place_live_exit_order() for SL/TP/exit_time → limit orders.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from features.telegram_notifier import notify_admin, notify_user

# algo.order — dedicated order-execution service. The actual broker place/modify/
# cancel call for every live SL/TG/entry/squareoff scenario below goes through here
# instead of calling the adapter in-process; everything above that one call (tick
# processing, decision logic, registry bookkeeping) is unchanged and still lives here.
ORDER_SERVICE_URL = 'http://localhost:8004/order'

# Server-to-server auth for the calls above — this is a background tick-processing
# loop, not a logged-in user's request, so there's no user JWT to send. Must match
# algo.order's INTERNAL_SERVICE_TOKEN (same shared/.env both processes load).
INTERNAL_SERVICE_TOKEN = os.getenv('INTERNAL_SERVICE_TOKEN', '')
_INTERNAL_HEADERS = {'X-Internal-Token': INTERNAL_SERVICE_TOKEN}

# ── broker_orders collection name ─────────────────────────────────────────────
_BROKER_ORDERS_COL = 'broker_orders'

log = logging.getLogger(__name__)

# ── Exchange / product constants ──────────────────────────────────────────────
_NFO  = 'NFO'
_BFO  = 'BFO'
_NSE  = 'NSE'
_MIS  = 'MIS'
_NRML = 'NRML'
_VARIETY_REGULAR = 'regular'

_ORDER_TYPE_MARKET = 'MARKET'
_ORDER_TYPE_LIMIT  = 'LIMIT'
_ORDER_TYPE_MPP    = 'MPP'      # Market Price Protection (internally → LIMIT with bid/ask base)
_ORDER_TYPE_SL     = 'SL'       # SL-L: trigger + limit
_ORDER_TYPE_SLM    = 'SL-M'     # SL-Market

_TXN_BUY  = 'BUY'
_TXN_SELL = 'SELL'

_ORDER_STATUS_COMPLETE = 'COMPLETE'
_ORDER_STATUS_OPEN     = 'OPEN'
_ORDER_STATUS_REJECTED = 'REJECTED'
_ORDER_STATUS_CANCELLED= 'CANCELLED'
_ORDER_STATUS_TRIGGER_PENDING = 'TRIGGER_PENDING'

_NFO_TICK_SIZE = 0.05  # NSE F&O tick size


def _remote_place_broker_order(
    trade_broker_id: str | None,
    *,
    tradingsymbol: str,
    exchange: str,
    transaction_type: str,
    quantity: int,
    order_type: str,
    product: str,
    variety: str = 'regular',
    price: float = 0.0,
    trigger_price: float = 0.0,
    validity: str = 'DAY',
    context: dict | None = None,
) -> dict:
    """
    Same contract as features.order_execution.place_broker_order (never raises —
    any failure comes back as {"status": "error", ...}) — the broker adapter is
    resolved and called inside algo.order instead of in this process.
    """
    try:
        resp = requests.post(
            f'{ORDER_SERVICE_URL}/broker/place',
            json={
                'trade_broker_id': trade_broker_id or '',
                'tradingsymbol': tradingsymbol,
                'exchange': exchange,
                'transaction_type': transaction_type,
                'quantity': quantity,
                'order_type': order_type,
                'product': product,
                'variety': variety,
                'price': price,
                'trigger_price': trigger_price,
                'validity': validity,
                'context': context or {},
            },
            headers=_INTERNAL_HEADERS,
            timeout=5.0,
        )
        return resp.json()
    except Exception as exc:
        log.warning('[REMOTE PLACE ORDER] algo.order unreachable: %s', exc)
        return {'order_id': '', 'status': 'error', 'message': f'order service unreachable: {exc}', 'raw': None}


def _remote_cancel_broker_order(trade_broker_id: str | None, *, variety: str, order_id: str) -> None:
    """Same contract as adapter.cancel_order(...) — every call site already wraps this in its own try/except."""
    resp = requests.post(
        f'{ORDER_SERVICE_URL}/broker/cancel',
        json={'trade_broker_id': trade_broker_id or '', 'variety': variety, 'order_id': order_id},
        headers=_INTERNAL_HEADERS,
        timeout=5.0,
    )
    resp.raise_for_status()


def _remote_modify_broker_order(
    trade_broker_id: str | None,
    *,
    order_id: str,
    order_type: str,
    price: float,
    trigger_price: float,
    exchange: str,
    tradingsymbol: str,
    quantity: int,
) -> dict:
    """Same contract as adapter.modify_order(...) — the call site already wraps this in its own try/except."""
    resp = requests.post(
        f'{ORDER_SERVICE_URL}/broker/modify',
        json={
            'trade_broker_id': trade_broker_id or '',
            'order_id': order_id, 'order_type': order_type, 'price': price,
            'trigger_price': trigger_price, 'exchange': exchange,
            'tradingsymbol': tradingsymbol, 'quantity': quantity,
        },
        headers=_INTERNAL_HEADERS,
        timeout=5.0,
    )
    resp.raise_for_status()
    return resp.json()


def _round_to_tick(price: float, tick: float = _NFO_TICK_SIZE, round_up: bool = False) -> float:
    """Round price to nearest tick size. round_up=True for BUY limit (round up), False for SELL limit (round down)."""
    import math
    if tick <= 0 or price <= 0:
        return round(price, 2)
    if round_up:
        return round(math.ceil(price / tick) * tick, 2)
    return round(math.floor(price / tick) * tick, 2)


def _sl_limit_price(trigger: float, is_sell_position: bool, buffer_pct: float = 1.0) -> float:
    """
    Compute SL-LMT limit price for a position exit.
    SELL position (exit=BUY):  limit ABOVE trigger → round UP to tick
    BUY  position (exit=SELL): limit BELOW trigger → round DOWN to tick
    """
    if is_sell_position:
        raw = trigger * (1 + buffer_pct / 100)
        return _round_to_tick(raw, round_up=True)
    else:
        raw = trigger * (1 - buffer_pct / 100)
        return _round_to_tick(raw, round_up=False)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _is_sell(position_str: str) -> bool:
    return 'sell' in str(position_str or '').lower()


def _env_flag_enabled(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, '')).strip().lower()
    if not raw:
        return default
    return raw in {'1', 'true', 'yes', 'on'}


def _is_live_order_punch_enabled() -> bool:
    return _env_flag_enabled('LIVE_ORDER_STATUS', default=False)


def _extract_option_type_from_symbol(symbol: str) -> str:
    text = str(symbol or '').strip().upper()
    if not text:
        return ''
    match = re.search(r'(CE|PE)(?![A-Z])', text)
    return str(match.group(1) if match else '')


def _expected_leg_option_type(leg: dict | None = None, leg_cfg: dict | None = None) -> str:
    contract = (leg_cfg or {}).get('ContractType') or {}
    option = str(contract.get('Option') or '').strip().upper()
    if option in {'CE', 'PE'}:
        return option

    instrument = str((leg_cfg or {}).get('InstrumentKind') or '').strip().upper()
    if instrument:
        instrument = instrument.split('.')[-1]
    if instrument in {'CE', 'PE'}:
        return instrument

    leg_option = str((leg or {}).get('option') or '').strip().upper()
    if leg_option:
        leg_option = leg_option.split('.')[-1]
    return leg_option if leg_option in {'CE', 'PE'} else ''


def _find_existing_trade_option_conflicts(db, trade_id: str, leg_id: str, option_type: str) -> list[str]:
    conflicts: list[str] = []
    if not trade_id or not leg_id or option_type not in {'CE', 'PE'}:
        return conflicts
    try:
        history_rows = db._db['algo_trade_positions_history'].find(
            {'trade_id': trade_id},
            {'leg_id': 1, 'symbol': 1, 'option': 1},
        )
        for row in history_rows:
            other_leg_id = str(row.get('leg_id') or '').strip()
            if not other_leg_id or other_leg_id == leg_id:
                continue
            other_option = str(row.get('option') or '').strip().upper()
            if not other_option:
                other_option = _extract_option_type_from_symbol(str(row.get('symbol') or ''))
            else:
                other_option = other_option.split('.')[-1]
            if other_option == option_type:
                conflicts.append(other_leg_id)
    except Exception:
        return conflicts
    return conflicts


def _build_simulated_live_order_id(trade_id: str, leg_id: str, side: str) -> str:
    trade_part = str(trade_id or '').strip() or 'trade'
    leg_part = str(leg_id or '').strip() or 'leg'
    side_part = str(side or 'entry').strip() or 'entry'
    now_part = datetime.now().strftime('%Y%m%d%H%M%S')
    return f'sim-{side_part}-{trade_part}-{leg_part}-{now_part}'


_MIN_OPTION_PRICE = 0.05   # NSE minimum option tick / floor price

def _round_price(price: float) -> float:
    """Round to nearest 0.05 (Kite NSE option tick size)."""
    return round(round(price / 0.05) * 0.05, 2)


def _clamp_limit_price(price: float, is_buy: bool) -> float:
    """
    NSE MPP price rules:
      1. Align to nearest 0.05 tick
      2. Sell price cannot be below ₹0.05 — default to ₹0.05
    """
    rounded = _round_price(price)
    if not is_buy and rounded < _MIN_OPTION_PRICE:
        return _MIN_OPTION_PRICE
    return rounded


def _mpp_protection_pct(ltp: float, is_option: bool = True) -> float:
    """
    MPP protection % by security type and LTP range (NSE official rules).

    OPT:  <10 → 5%  |  10-100 → 3%  |  100-500 → 2%  |  >500 → 1%
    EQ/FUT: <100 → 2%  |  100-500 → 1%  |  >500 → 0.5%
    """
    if is_option:
        if ltp < 10:   return 5.0
        if ltp < 100:  return 3.0
        if ltp < 500:  return 2.0
        return 1.0
    else:
        if ltp < 100:  return 2.0
        if ltp < 500:  return 1.0
        return 0.5


def _resolve_exchange(symbol: str = '', trade: dict | None = None, leg: dict | None = None, fallback: str = _NFO) -> str:
    """Resolve option exchange for order placement. SENSEX options trade on BFO."""
    candidates = [
        (leg or {}).get('exchange'),
        ((leg or {}).get('entry_trade') or {}).get('exchange'),
        (trade or {}).get('exchange'),
        (trade or {}).get('ticker'),
        ((trade or {}).get('strategy') or {}).get('Ticker'),
        ((trade or {}).get('config') or {}).get('Ticker'),
        symbol,
    ]
    text = ' '.join(str(item or '').upper() for item in candidates)
    if 'BFO' in text or 'BSE' in text or 'SENSEX' in text:
        return _BFO
    if 'NFO' in text or 'NSE' in text:
        return _NFO
    return fallback


def _get_bid_ask(kite, symbol: str, ltp: float, exchange: str = _NFO) -> tuple[float, float]:
    """
    Fetch best bid/ask via kite.quote(). Returns (0.0, 0.0) — never `ltp` — when depth is
    unavailable for any reason (no quote, empty depth, exception).

    Previously fell back to ltp here, which silently defeats MPP's whole purpose: ltp is
    not a protected/live-book price, and using it as a stand-in bid/ask can submit a real
    order far from where the book actually is with no indication anything went wrong.
    Callers (place_live_entry_order/place_live_exit_order) MUST treat a 0.0 return as
    "MPP price unresolved" and abort the order instead of computing a price from it.
    """
    try:
        exch = str(exchange or _NFO).upper()
        sym_key = f'{exch}:{symbol}'
        q = kite.quote([sym_key])
        depth = (q.get(sym_key) or {}).get('depth') or {}
        buy_depth  = depth.get('buy')  or []
        sell_depth = depth.get('sell') or []
        bid = _safe_float((buy_depth[0]  if buy_depth  else {}).get('price'), 0.0)
        ask = _safe_float((sell_depth[0] if sell_depth else {}).get('price'), 0.0)
        return bid, ask
    except Exception as exc:
        log.debug('_get_bid_ask error exchange=%s symbol=%s: %s', exchange, symbol, exc)
        return 0.0, 0.0


# ── Kite instance ─────────────────────────────────────────────────────────────

def _get_leg_modification_config(trade: dict, leg_id: str) -> tuple[bool, int]:
    """Return (continuous_monitoring, modification_frequency_seconds) for a leg."""
    strategy_cfg  = trade.get('strategy') or {}
    leg_list      = list(strategy_cfg.get('ListOfLegConfigs') or [])
    exec_extra    = trade.get('execution_config_extra') or {}
    leg_exec_cfgs = exec_extra.get('ListOfLegExecutionConfig') or []
    for idx, base_leg in enumerate(leg_list):
        if not isinstance(base_leg, dict):
            continue
        if str(base_leg.get('id') or '') == leg_id and idx < len(leg_exec_cfgs):
            lec = leg_exec_cfgs[idx]
            if not isinstance(lec, dict):
                continue
            entry_order = lec.get('EntryOrder') or {}
            value       = entry_order.get('Value') or {}
            mod         = value.get('Modification') or {}
            continuous  = str(mod.get('ContinuousMonitoring') or 'False').lower() == 'true'
            freq        = int(mod.get('ModificationFrequency') or 0)
            return continuous, freq
    return False, 0


def _get_leg_entry_buffer(trade: dict, leg_id: str) -> tuple[float, str]:
    """Read LimitBuffer + buffer_type for a leg from execution_config_extra."""
    strategy_cfg  = trade.get('strategy') or {}
    leg_list      = list(strategy_cfg.get('ListOfLegConfigs') or [])
    exec_extra    = trade.get('execution_config_extra') or {}
    leg_exec_cfgs = exec_extra.get('ListOfLegExecutionConfig') or []
    for idx, base_leg in enumerate(leg_list):
        if not isinstance(base_leg, dict):
            continue
        if str(base_leg.get('id') or '') == leg_id and idx < len(leg_exec_cfgs):
            lec = leg_exec_cfgs[idx]
            if not isinstance(lec, dict):
                continue
            entry_order = lec.get('EntryOrder') or {}
            value       = entry_order.get('Value') or {}
            buf_cfg     = value.get('Buffer') or {}
            buf_val     = buf_cfg.get('Value') or {}
            buf_type_raw = str(buf_cfg.get('Type') or 'BufferType.Points').lower()
            buffer_type  = 'percentage' if 'percent' in buf_type_raw else 'points'
            limit_buffer = _safe_float(buf_val.get('LimitBuffer', 3))
            return limit_buffer, buffer_type
    return 3.0, 'points'   # default


# ── broker_orders helpers ─────────────────────────────────────────────────────

def _broker_type_label(broker) -> str:
    """Return 'flattrade' or 'kite' based on broker instance type."""
    try:
        from features.flattrade_broker import FlatTradeAdapter
        if isinstance(broker, FlatTradeAdapter):
            return 'flattrade'
    except Exception:
        pass
    return 'kite'


def _save_broker_order(
    db,
    trade: dict,
    broker,
    order_id: str,
    order_side: str,        # 'entry' | 'exit'
    symbol: str,
    exchange: str,
    txn_type: str,          # 'BUY' | 'SELL'
    qty: int,
    order_type: str,
    price: float,
    trigger_price: float,
    product: str,
    leg_id: str = '',
    exit_reason: str = '',
) -> None:
    """Insert a new row into broker_orders when an order is placed."""
    try:
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        trade_id   = str(trade.get('_id') or '').strip()
        broker_id  = str(trade.get('broker') or '').strip()
        broker_lbl = _broker_type_label(broker)
        db._db[_BROKER_ORDERS_COL].insert_one({
            'order_id':         order_id,
            'broker_doc_id':    broker_id,
            'broker_type':      broker_lbl,
            'trade_id':         trade_id,
            'leg_id':           leg_id,
            'order_side':       order_side,
            'symbol':           symbol,
            'exchange':         exchange,
            'transaction_type': txn_type,
            'quantity':         int(qty),
            'order_type':       order_type,
            'price':            float(price or 0),
            'trigger_price':    float(trigger_price or 0),
            'product':          product,
            'exit_reason':      exit_reason,
            'status':           'OPEN',
            'fill_price':       0.0,
            'fill_qty':         0,
            'rejection_reason': '',
            'placed_at':        now,
            'updated_at':       now,
            'filled_at':        '',
        })
    except Exception as exc:
        log.debug('[BROKER ORDERS] save failed order_id=%s: %s', order_id, exc)


def _update_broker_order_status(
    db,
    order_id: str,
    status: str,
    fill_price: float = 0.0,
    fill_qty: int = 0,
    rejection_reason: str = '',
) -> None:
    """Update status in broker_orders collection only."""
    try:
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        set_fields: dict = {
            'status':     status,
            'updated_at': now,
        }
        if status == 'COMPLETE':
            set_fields['fill_price'] = float(fill_price or 0)
            set_fields['fill_qty']   = int(fill_qty or 0)
            set_fields['filled_at']  = now
        elif status in ('REJECTED', 'CANCELLED'):
            set_fields['rejection_reason'] = str(rejection_reason or '')
        db._db[_BROKER_ORDERS_COL].update_one(
            {'order_id': order_id},
            {'$set': set_fields},
        )
    except Exception as exc:
        log.debug('[BROKER ORDERS] update failed order_id=%s: %s', order_id, exc)


def _load_trade_and_leg_context(
    db,
    trade_id: str,
    leg_id: str,
) -> tuple[dict, dict, dict, dict]:
    trade = db._db['algo_trades'].find_one({'_id': trade_id}) or {}
    if not trade:
        return {}, {}, {}, {}
    leg = next(
        (
            item for item in (trade.get('legs') or [])
            if isinstance(item, dict) and str(item.get('id') or '').strip() == str(leg_id or '').strip()
        ),
        {},
    )
    if not leg:
        hist_leg = db._db['algo_trade_positions_history'].find_one(
            {'trade_id': trade_id, 'leg_id': leg_id},
        ) or {}
        if hist_leg:
            leg = hist_leg
    try:
        from features.execution_socket import _resolve_trade_leg_configs, _resolve_leg_cfg

        all_leg_cfgs = _resolve_trade_leg_configs(trade)
        leg_cfg = _resolve_leg_cfg(str(leg.get('id') or leg_id), leg, all_leg_cfgs) if leg else {}
    except Exception:
        leg_cfg = {}
    hist_doc = db._db['algo_trade_positions_history'].find_one(
        {'trade_id': trade_id, 'leg_id': leg_id},
    ) or {}
    return trade, leg, leg_cfg, hist_doc


def _get_open_exit_orders_for_leg(
    db,
    trade_id: str,
    leg_id: str,
) -> list[dict]:
    return list(db._db[_BROKER_ORDERS_COL].find({
        'trade_id': str(trade_id or '').strip(),
        'leg_id': str(leg_id or '').strip(),
        'order_side': 'exit',
        'status': _ORDER_STATUS_OPEN,
    }))


def has_open_exit_order(
    db,
    trade_id: str,
    leg_id: str,
    exit_reason: str = '',
) -> bool:
    query = {
        'trade_id': str(trade_id or '').strip(),
        'leg_id': str(leg_id or '').strip(),
        'order_side': 'exit',
        'status': _ORDER_STATUS_OPEN,
    }
    if exit_reason:
        query['exit_reason'] = str(exit_reason or '').strip()
    return bool(db._db[_BROKER_ORDERS_COL].find_one(query, {'_id': 1}))


def cancel_open_exit_orders_for_leg(
    db,
    trade: dict,
    leg_id: str,
    *,
    keep_reason: str = '',
    cancel_reason: str = '',
) -> int:
    trade_id = str((trade or {}).get('_id') or '').strip()
    if not trade_id or not leg_id:
        return 0
    orders_to_cancel = _get_open_exit_orders_for_leg(db, trade_id, leg_id)
    if keep_reason:
        orders_to_cancel = [
            row for row in orders_to_cancel
            if str(row.get('exit_reason') or '').strip() != str(keep_reason or '').strip()
        ]
    if not orders_to_cancel:
        return 0
    broker = get_broker_for_trade(db, trade)
    cancelled = 0
    for row in orders_to_cancel:
        order_id = str(row.get('order_id') or '').strip()
        if not order_id:
            continue
        exit_reason = str(row.get('exit_reason') or '').strip() or '-'
        order_type = str(row.get('order_type') or '').strip() or '-'
        try:
            if broker and _is_live_order_punch_enabled():
                _remote_cancel_broker_order(trade.get('broker'), variety=_VARIETY_REGULAR, order_id=order_id)
            _update_broker_order_status(db, order_id, _ORDER_STATUS_CANCELLED)
            cancelled += 1
            print(
                f'[LIVE EXIT ORDER CANCEL] trade={trade_id} leg={leg_id} '
                f'order={order_id} exit_reason={exit_reason} order_type={order_type} '
                f'keep_reason={keep_reason or "-"} cancel_reason={cancel_reason or "-"}'
            )
        except Exception as exc:
            log.warning(
                '[LIVE EXIT ORDER CANCEL] trade=%s leg=%s order=%s exit_reason=%s cancel_reason=%s: %s',
                trade_id, leg_id, order_id, exit_reason, cancel_reason or '-', exc,
            )
    if cancelled:
        print(
            f'[LIVE EXIT ORDER CANCEL] trade={trade_id} leg={leg_id} '
            f'cancelled={cancelled} keep_reason={keep_reason or "-"} '
            f'cancel_reason={cancel_reason or "-"}'
        )
    return cancelled


def modify_broker_sl_order(db, trade_id: str, leg_id: str, new_sl: float) -> None:
    """
    Called after trail SL update — modifies the pending TRIGGER_PENDING SL order
    on the broker so the trigger price reflects the new SL value.
    """
    try:
        sl_order_id = _get_sl_order_id(trade_id, leg_id)
        if not sl_order_id:
            print(f'[BROKER SL MODIFY SKIP] trade={trade_id} leg={leg_id} reason=not_in_sl_registry')
            return

        # Double-check: verify the SL order is still open in broker_orders before modifying
        existing_order = db._db[_BROKER_ORDERS_COL].find_one(
            {'order_id': sl_order_id, 'status': _ORDER_STATUS_OPEN},
            {'_id': 1, 'trigger_price': 1, 'exchange': 1, 'symbol': 1, 'quantity': 1},
        )
        if not existing_order:
            print(
                f'[BROKER SL MODIFY SKIP] trade={trade_id} leg={leg_id} '
                f'order={sl_order_id} reason=order_not_open_in_broker_orders'
            )
            _deregister_sl_order(trade_id, leg_id)
            return

        trade, leg, _leg_cfg, _hist = _load_trade_and_leg_context(db, trade_id, leg_id)
        if not trade or not leg:
            return

        position_str = str(leg.get('position') or (_hist or {}).get('position') or '')
        is_sell = _is_sell(position_str)
        new_sl_ticked = _round_to_tick(new_sl, round_up=is_sell)

        exit_cfg      = _resolve_exit_order_config(_leg_cfg or {})
        lmt_buf       = exit_cfg['limit_buffer']
        buf_type      = exit_cfg['buffer_type']
        new_limit_price = _round_to_tick(
            _apply_buffer(new_sl_ticked, lmt_buf, buf_type, is_buy=is_sell),
            round_up=is_sell,
        )

        broker = get_broker_for_trade(db, trade)
        if not broker:
            print(f'[BROKER SL MODIFY SKIP] trade={trade_id} leg={leg_id} order={sl_order_id} reason=no_broker')
            return

        old_trigger = _safe_float((existing_order or {}).get('trigger_price'))
        _exch = str((existing_order or {}).get('exchange') or 'NFO').strip().upper()
        _tsym = str((existing_order or {}).get('symbol') or '').strip()

        # Fallback: get symbol from leg / positions history if broker_orders has it empty
        if not _tsym:
            _tsym = str(
                leg.get('symbol')
                or (_hist or {}).get('symbol')
                or ''
            ).strip()
        if not _exch or _exch == 'NFO':
            _exch = str(
                leg.get('exchange')
                or (_hist or {}).get('exchange')
                or 'NFO'
            ).strip().upper()

        if not _tsym:
            print(
                f'[BROKER SL MODIFY SKIP] trade={trade_id} leg={leg_id} '
                f'order={sl_order_id} reason=symbol_not_found_anywhere'
            )
            return

        if abs(new_sl_ticked - old_trigger) < 0.05:
            print(
                f'[BROKER SL MODIFY SKIP] trade={trade_id} leg={leg_id} '
                f'order={sl_order_id} reason=trigger_unchanged old={old_trigger} new={new_sl_ticked}'
            )
            return

        print(
            f'[BROKER SL MODIFY ATTEMPT] trade={trade_id} leg={leg_id} '
            f'order={sl_order_id} exch={_exch} tsym={_tsym} '
            f'old_trigger={old_trigger} new_trigger={new_sl_ticked} new_limit={new_limit_price}'
        )
        _qty = int((existing_order or {}).get('quantity') or leg.get('quantity') or (_hist or {}).get('quantity') or 0)
        modify_response = _remote_modify_broker_order(
            trade.get('broker'),
            order_id=sl_order_id,
            order_type=_ORDER_TYPE_SL,
            price=new_limit_price,
            trigger_price=new_sl_ticked,
            exchange=_exch,
            tradingsymbol=_tsym,
            quantity=_qty,
        )
        now_ts = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        print(
            f'[BROKER SL MODIFY SUCCESS] trade={trade_id} leg={leg_id} '
            f'order={sl_order_id} response={modify_response} '
            f'new_trigger={new_sl_ticked} new_limit={new_limit_price} ts={now_ts}'
        )
        # Update broker_orders with new trigger so the record reflects what was sent
        try:
            db._db[_BROKER_ORDERS_COL].update_one(
                {'order_id': sl_order_id},
                {'$set': {
                    'trigger_price': new_sl_ticked,
                    'price': new_limit_price,
                    'updated_at': now_ts,
                }},
            )
        except Exception as _bo_exc:
            log.warning('[BROKER SL MODIFIED] broker_orders sync failed trade=%s leg=%s: %s', trade_id, leg_id, _bo_exc)
        try:
            db._db['algo_leg_feature_status'].update_many(
                {
                    'trade_id': trade_id,
                    'leg_id': leg_id,
                    'feature': {'$in': ['sl', 'trailSL', 'trail_sl', 'trailing_sl']},
                    'status': {'$nin': ['triggered', 'disabled', 'completed', 'cancelled']},
                },
                {'$set': {
                    'trigger_price': new_sl_ticked,
                    'current_sl_price': new_sl_ticked,
                    'order_limit_price': new_limit_price,
                    'updated_at': now_ts,
                }},
            )
        except Exception as _sync_exc:
            log.warning(
                '[BROKER SL MODIFIED] feature row sync failed trade=%s leg=%s: %s',
                trade_id, leg_id, _sync_exc,
            )
    except Exception as exc:
        print(f'[BROKER SL MODIFY ERROR] trade={trade_id} leg={leg_id} new_sl={new_sl} error={exc}')
        log.warning('[BROKER SL MODIFY ERROR] trade=%s leg=%s new_sl=%s: %s', trade_id, leg_id, new_sl, exc)


def _persist_protection_order_refs(
    db,
    trade_id: str,
    leg_id: str,
    *,
    stoploss_order_id: str = '',
    target_order_id: str = '',
    protection_orders_placed: bool | None = None,
) -> None:
    set_fields_trade: dict[str, Any] = {}
    set_fields_hist: dict[str, Any] = {}
    if stoploss_order_id:
        set_fields_trade['legs.$[elem].broker_stoploss_order_id'] = stoploss_order_id
        set_fields_hist['broker_stoploss_order_id'] = stoploss_order_id
    if target_order_id:
        set_fields_trade['legs.$[elem].broker_target_order_id'] = target_order_id
        set_fields_hist['broker_target_order_id'] = target_order_id
    if protection_orders_placed is not None:
        set_fields_trade['legs.$[elem].entry_trade.protection_orders_placed'] = bool(protection_orders_placed)
        set_fields_hist['entry_trade.protection_orders_placed'] = bool(protection_orders_placed)
    if set_fields_trade:
        db._db['algo_trades'].update_one(
            {'_id': trade_id},
            {'$set': set_fields_trade},
            array_filters=[{'elem.id': leg_id}],
        )
    if set_fields_hist:
        print(f'[HIST_UPDATE][ENTRY_FILL] trade={trade_id} leg={leg_id} data={set_fields_hist}')
        db._db['algo_trade_positions_history'].update_one(
            {'trade_id': trade_id, 'leg_id': leg_id},
            {'$set': set_fields_hist},
        )


def _recalc_sl_for_actual_fill(db, trade_id: str, leg_id: str, actual_fill: float) -> None:
    """
    When actual broker avg_price differs from initial limit_price basis,
    recalculate SL from actual fill and modify the existing broker SL order.
    """
    if actual_fill <= 0:
        return
    try:
        from features.position_manager import calc_sl_price  # type: ignore
        trade, leg, leg_cfg, hist_doc = _load_trade_and_leg_context(db, trade_id, leg_id)
        if not trade or not leg or not leg_cfg:
            return
        position_str = str(leg.get('position') or (hist_doc or {}).get('position') or '')
        is_sell = _is_sell(position_str)
        sl_config = leg_cfg.get('LegStopLoss') or {}
        if not sl_config:
            return
        new_sl = _safe_float(calc_sl_price(actual_fill, is_sell, sl_config))
        if not new_sl:
            return
        new_sl = _round_to_tick(new_sl, round_up=is_sell)
        current_sl = _safe_float(
            leg.get('current_sl_price') or (hist_doc or {}).get('current_sl_price')
        )
        if current_sl and abs(new_sl - current_sl) < 0.05:
            return  # negligible difference — skip
        # Update DB
        db._db['algo_trades'].update_one(
            {'_id': trade_id, 'legs.id': leg_id},
            {'$set': {'legs.$.current_sl_price': new_sl, 'legs.$.initial_sl_value': new_sl}},
        )
        print(f'[HIST_UPDATE][SL_RECALC] trade={trade_id} leg={leg_id} data={{"current_sl_price": {new_sl}, "initial_sl_value": {new_sl}}}')
        db._db['algo_trade_positions_history'].update_one(
            {'trade_id': trade_id, 'leg_id': leg_id, 'exit_trade': None},
            {'$set': {'current_sl_price': new_sl, 'initial_sl_value': new_sl}},
        )
        # Modify broker SL order to match actual fill basis
        modify_broker_sl_order(db, trade_id, leg_id, new_sl)
        print(
            f'[SL RECALC] trade={trade_id} leg={leg_id} '
            f'fill={actual_fill} old_sl={current_sl} new_sl={new_sl}'
        )
    except Exception as exc:
        log.warning('[SL RECALC ERROR] trade=%s leg=%s: %s', trade_id, leg_id, exc)


def _sync_leg_feature_entry_price(
    db, trade_id: str, leg_id: str, entry_price: float, sl_price: float, tp_price: float,
    sl_order_price: float = 0.0,
) -> None:
    """
    Update algo_leg_feature_status rows with the actual fill price (avg_price from broker).
    Called after fill is verified — updates entry_price and recalculated trigger prices
    so UI display and trail SL logic use the correct fill price, not the original limit_price.
    Updates active/pending rows, while leaving disabled/triggered rows untouched.
    """
    now_ts = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    col    = db._db['algo_leg_feature_status']
    query  = {
        'trade_id': trade_id,
        'leg_id': leg_id,
        'status': {'$nin': ['triggered', 'disabled', 'completed', 'cancelled']},
    }

    if sl_price > 0:
        col.update_many(
            {**query, 'feature': 'sl'},
            {'$set': {
                'entry_price':      entry_price,
                'trigger_price':    sl_price,
                'order_limit_price': sl_order_price if sl_order_price > 0 else sl_price,
                'current_sl_price': sl_price,
                'initial_sl_price': sl_price,
                'updated_at':       now_ts,
            }},
        )
    if tp_price > 0:
        col.update_many(
            {**query, 'feature': 'target'},
            {'$set': {
                'entry_price':   entry_price,
                'trigger_price': tp_price,
                'updated_at':    now_ts,
            }},
        )
    # trail_sl: update entry_price AND recalculated SL so it matches the actual fill basis
    _trail_sl_set = {'entry_price': entry_price, 'updated_at': now_ts}
    if sl_price > 0:
        _trail_sl_set.update({
            'current_sl_price': sl_price,
            'initial_sl_price': sl_price,
            'trigger_price':    sl_price,
        })
    col.update_many(
        {**query, 'feature': {'$in': ['trailSL', 'trail_sl', 'trailing_sl']}},
        {'$set': _trail_sl_set},
    )
    # leg_entry: sync the confirmed fill price so UI and downstream logic see the real entry
    col.update_many(
        {'leg_id': leg_id, 'feature': 'leg_entry'},
        {'$set': {
            'entry_price': entry_price,
            'updated_at':  now_ts,
        }},
    )
    print(
        f'[FEATURE STATUS SYNCED] trade={trade_id} leg={leg_id} '
        f'entry_price={entry_price} sl_trigger={sl_price} '
        f'sl_order_price={sl_order_price if sl_order_price > 0 else sl_price} '
        f'tp_trigger={tp_price}'
    )


def _sync_leg_entry_feature_from_positions_history(
    db,
    trade_id: str,
    leg_id: str,
) -> float:
    """
    Read the latest entry price from algo_trade_positions_history and sync only the
    matching algo_leg_feature_status(feature='leg_entry') row for this leg.
    """
    trade_id = str(trade_id or '').strip()
    leg_id = str(leg_id or '').strip()
    if not trade_id or not leg_id:
        return 0.0

    hist_doc = db._db['algo_trade_positions_history'].find_one(
        {'trade_id': trade_id, 'leg_id': leg_id},
        {'entry_trade.price': 1},
    ) or {}
    entry_trade = hist_doc.get('entry_trade') if isinstance(hist_doc.get('entry_trade'), dict) else {}
    entry_price = _safe_float(entry_trade.get('price'))
    if entry_price <= 0:
        return 0.0

    now_ts = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    db._db['algo_leg_feature_status'].update_many(
        {'trade_id': trade_id, 'leg_id': leg_id, 'feature': 'leg_entry'},
        {'$set': {'entry_price': entry_price, 'updated_at': now_ts}},
    )
    print(
        f'[LEG ENTRY SYNC] trade={trade_id} leg={leg_id} '
        f'entry_price(history)={entry_price}'
    )
    return entry_price


def _fetch_broker_avg_price(db, trade: dict, order_id: str) -> float:
    """Fetch actual avg_price from broker for a completed order_id."""
    price, _ = _fetch_broker_fill(db, trade, order_id)
    return price


def _fetch_broker_fill(db, trade: dict, order_id: str) -> tuple[float, int]:
    """Fetch (avg_price, filled_qty) from broker for a completed order_id."""
    try:
        broker = get_broker_for_trade(db, trade)
        if not broker:
            return 0.0, 0
        all_orders = broker.orders() or []
        matched = next(
            (o for o in all_orders if str(o.get('order_id') or '') == order_id),
            None,
        )
        if matched and str(matched.get('status') or '').upper() == _ORDER_STATUS_COMPLETE:
            price = _safe_float(matched.get('average_price') or matched.get('price'))
            qty   = int(matched.get('filled_quantity') or matched.get('quantity') or 0)
            return price, qty
    except Exception as exc:
        log.warning('[FETCH BROKER FILL] order=%s: %s', order_id, exc)
    return 0.0, 0


def _confirm_and_sync_fill_price(
    db, trade_id: str, leg_id: str, order_id: str, fill_price: float,
) -> float:
    """
    Before placing SL: verify DB entry_trade.price == broker avg_price.
    If mismatch: re-fetch from broker and sync DB.
    Returns the verified price to use for SL calculation (always > 0 before SL is placed).
    """
    hist_col   = db._db['algo_trade_positions_history']
    trades_col = db._db['algo_trades']
    now_ts     = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    doc = hist_col.find_one(
        {'entry_trade.order_id': order_id},
        {'entry_trade.price': 1},
    )
    db_price = _safe_float((doc.get('entry_trade') or {}).get('price') if doc else None)

    if db_price > 0 and db_price == fill_price:
        print(f'[PRICE VERIFIED] order={order_id} db={db_price} == broker={fill_price} ✓')
        return fill_price

    # Mismatch or db_price=0: re-fetch actual avg_price from broker
    print(
        f'[PRICE MISMATCH] order={order_id} db={db_price} broker={fill_price} '
        f'— re-fetching from broker'
    )
    trade_doc  = trades_col.find_one({'_id': trade_id}) or {}
    fetched    = _fetch_broker_avg_price(db, trade_doc, order_id)

    if fetched > 0:
        hist_col.update_one(
            {'entry_trade.order_id': order_id},
            {'$set': {
                'entry_trade.price':     fetched,
                'entry_trade.filled_at': now_ts,
            }},
        )
        trades_col.update_one(
            {'_id': trade_id},
            {'$set': {
                'legs.$[elem].entry_trade.price': fetched,
                'legs.$[elem].last_saw_price':    fetched,
            }},
            array_filters=[{'elem.id': leg_id}],
        )
        _sync_leg_entry_feature_from_positions_history(db, trade_id, leg_id)
        print(f'[PRICE SYNCED] order={order_id} synced={fetched} from broker')
        return fetched

    return db_price if db_price > 0 else fill_price


def _place_initial_protection_orders(
    db,
    trade_id: str,
    leg_id: str,
    fill_price: float,
    fill_qty: int,
) -> None:
    if fill_price <= 0:
        return
    trade, leg, leg_cfg, hist_doc = _load_trade_and_leg_context(db, trade_id, leg_id)
    if not trade or not leg:
        return
    if str(trade.get('activation_mode') or '').strip() != 'live':
        return

    from features.position_manager import calc_sl_price, calc_tp_price  # type: ignore

    position_str = str(leg.get('position') or hist_doc.get('position') or '')
    is_sell = _is_sell(position_str)
    sl_config = leg_cfg.get('LegStopLoss') or {}
    tp_config = leg_cfg.get('LegTarget') or {}

    # fill_price = verified price from algo_trade_positions_history.entry_trade.price
    # Always calculate SL/TP from this price — never from algo_leg_feature_status.
    sl_price = _safe_float(calc_sl_price(fill_price, is_sell, sl_config)) if sl_config else 0.0
    tp_price = _safe_float(calc_tp_price(fill_price, is_sell, tp_config)) if tp_config else 0.0

    print(
        f'[SL CALC] trade={trade_id} leg={leg_id} '
        f'entry_price(positions_history)={fill_price} '
        f'sl={sl_price} tp={tp_price}'
    )

    # Round trigger prices to NFO tick size (0.05) — FlatTrade rejects non-multiples
    # SELL position SL (trigger on price rise) → round UP; BUY position → round DOWN
    if sl_price > 0:
        sl_price = _round_to_tick(sl_price, round_up=is_sell)
    if tp_price > 0:
        # Target: SELL position exits when price falls → round DOWN; BUY → round UP
        tp_price = _round_to_tick(tp_price, round_up=not is_sell)

    symbol = str(leg.get('symbol') or hist_doc.get('symbol') or '').strip()
    if fill_qty:
        # Broker's own reported filled_quantity — already actual contracts.
        qty = int(fill_qty)
    else:
        # Fallback: stored quantity is lot count, not contracts — multiply by lot_size.
        _fallback_lot_size = int(leg.get('lot_size') or hist_doc.get('lot_size') or 1)
        qty = int(leg.get('quantity') or hist_doc.get('quantity') or 0) * max(1, _fallback_lot_size)
    sl_order_id = ''
    tgt_order_id = ''
    leg_for_order = dict(leg)
    leg_for_order['id'] = leg_id
    sl_limit = 0.0
    if sl_price > 0:
        # Use leg config buffer: TriggerBuffer=0 (exact SL price), LimitBuffer=N points
        exit_cfg   = _resolve_exit_order_config(leg_cfg)
        lmt_buf    = exit_cfg['limit_buffer']    # e.g. 3 points
        buf_type   = exit_cfg['buffer_type']     # 'points' or 'percentage'
        # For a SELL position exit=BUY: limit above trigger; BUY position: limit below
        sl_limit = _apply_buffer(sl_price, lmt_buf, buf_type, is_buy=is_sell)
        sl_limit = _round_to_tick(sl_limit, round_up=is_sell)

    # Write the actual-fill-based SL back to the leg document so check_leg_exit
    # reads the correct price, not the pre-fill stale value from feature creation.
    if sl_price > 0:
        try:
            db._db['algo_trades'].update_one(
                {'_id': trade_id, 'legs.id': leg_id},
                {'$set': {
                    'legs.$.current_sl_price': sl_price,
                    'legs.$.initial_sl_value': sl_price,
                }},
            )
            print(f'[HIST_UPDATE][SL_SYNC] trade={trade_id} leg={leg_id} data={{"current_sl_price": {sl_price}, "initial_sl_value": {sl_price}}}')
            db._db['algo_trade_positions_history'].update_one(
                {'trade_id': trade_id, 'leg_id': leg_id, 'exit_trade': None},
                {'$set': {'current_sl_price': sl_price, 'initial_sl_value': sl_price}},
            )
        except Exception as _slupd_exc:
            log.warning('[SL SYNC] leg sl update error trade=%s leg=%s: %s', trade_id, leg_id, _slupd_exc)

    # Sync algo_leg_feature_status BEFORE the exit-order guard so the entry_price
    # and recalculated SL/TP are always written, even if protection orders already exist.
    _sync_leg_feature_entry_price(
        db, trade_id, leg_id, fill_price, sl_price, tp_price, sl_order_price=sl_limit,
    )
    # Rebuild trigger_description text using actual fill price so UI shows correct values.
    try:
        from features.notification_manager import refresh_leg_feature_status_at_fill
        refresh_leg_feature_status_at_fill(
            db._db, trade_id, leg_id,
            fill_price=fill_price,
            leg_cfg=leg_cfg,
            is_sell=is_sell,
            sl_price=sl_price,
            tp_price=tp_price,
            now=datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        )
    except Exception as _rfr_exc:
        log.warning('[FEATURE REFRESH] trade=%s leg=%s: %s', trade_id, leg_id, _rfr_exc)

    if _get_open_exit_orders_for_leg(db, trade_id, leg_id):
        return

    if not symbol or qty <= 0:
        return

    if sl_price > 0:
        result = place_live_exit_order(
            db, trade, leg_for_order, leg_cfg, symbol, qty, sl_price, 'stoploss',
            force_order_type=_ORDER_TYPE_SL,
            force_limit_price=sl_limit,
            force_trigger_price=sl_price,
        )
        sl_order_id = str(result.get('order_id') or '').strip()
        if sl_order_id:
            _register_sl_order(trade_id, leg_id, sl_order_id)
    if tp_price > 0:
        tp_price_ticked = _round_to_tick(tp_price, round_up=not is_sell)
        result = place_live_exit_order(
            db, trade, leg_for_order, leg_cfg, symbol, qty, tp_price_ticked, 'target',
            force_order_type=_ORDER_TYPE_LIMIT,
            force_limit_price=tp_price_ticked,
            force_trigger_price=0.0,
        )
        tgt_order_id = str(result.get('order_id') or '').strip()
    if sl_order_id or tgt_order_id:
        _persist_protection_order_refs(
            db,
            trade_id,
            leg_id,
            stoploss_order_id=sl_order_id,
            target_order_id=tgt_order_id,
            protection_orders_placed=True,
        )
        print(
            f'[LIVE PROTECTION ARMED] trade={trade_id} leg={leg_id} '
            f'sl_order={sl_order_id or "-"} tgt_order={tgt_order_id or "-"}'
        )


def _find_pending_live_leg_by_order_id(db, order_id: str) -> tuple[dict | None, dict | None]:
    normalized_order_id = str(order_id or '').strip()
    if not normalized_order_id:
        return None, None
    trade = db._db['algo_trades'].find_one(
        {
            'activation_mode': 'live',
            'legs.entry_trade.order_id': normalized_order_id,
        }
    ) or None
    if not trade:
        return None, None
    for leg in (trade.get('legs') or []):
        if not isinstance(leg, dict):
            continue
        entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
        if str(entry_trade.get('order_id') or '').strip() == normalized_order_id:
            return trade, leg
    return trade, None


def _promote_pending_live_leg_to_position_history(
    db,
    trade_id: str,
    leg_id: str,
) -> bool:
    trade = db._db['algo_trades'].find_one({'_id': trade_id}) or {}
    if not trade:
        return False
    leg = next(
        (
            item for item in (trade.get('legs') or [])
            if isinstance(item, dict) and str(item.get('id') or '').strip() == str(leg_id or '').strip()
        ),
        None,
    )
    if not isinstance(leg, dict):
        return False
    try:
        from features.execution_socket import _resolve_trade_leg_configs, _resolve_leg_cfg, _store_position_history

        all_leg_cfgs = _resolve_trade_leg_configs(trade)
        resolved_leg_cfg = _resolve_leg_cfg(str(leg.get('id') or ''), leg, all_leg_cfgs)
        inserted, _history_doc = _store_position_history(
            db, trade, leg, override_leg_cfg=resolved_leg_cfg
        )
        return bool(inserted)
    except Exception as exc:
        log.error(
            '[LIVE ENTRY ACTIVATE] promote failed trade=%s leg=%s: %s',
            trade_id,
            leg_id,
            exc,
        )
        return False


def _iter_pending_live_entry_orders(db) -> list[dict]:
    pending: list[dict] = []

    for hist_doc in db._db['algo_trade_positions_history'].find(
        {
            'entry_trade.order_status': _ORDER_STATUS_OPEN,
            'exit_trade': None,
        },
        {
            'trade_id': 1,
            'leg_id': 1,
            'entry_trade': 1,
        },
    ):
        pending.append({
            'source': 'history',
            'trade_id': str(hist_doc.get('trade_id') or ''),
            'leg_id': str(hist_doc.get('leg_id') or ''),
            'entry_trade': hist_doc.get('entry_trade') or {},
            'history_id': hist_doc.get('_id'),
        })

    live_trades = db._db['algo_trades'].find(
        {
            'activation_mode': 'live',
            'trade_status': 1,
        },
        {
            '_id': 1,
            'broker': 1,
            'legs': 1,
        },
    )
    for trade in live_trades:
        trade_id = str(trade.get('_id') or '')
        for leg in (trade.get('legs') or []):
            if not isinstance(leg, dict):
                continue
            entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
            if str(entry_trade.get('order_status') or '').strip().upper() != _ORDER_STATUS_OPEN:
                continue
            if leg.get('exit_trade'):
                continue
            pending.append({
                'source': 'embedded',
                'trade_id': trade_id,
                'leg_id': str(leg.get('id') or ''),
                'entry_trade': entry_trade,
                'history_id': None,
            })
    return pending


def _iter_open_live_exit_orders(db) -> list[dict]:
    return list(db._db[_BROKER_ORDERS_COL].find({
        'order_side': 'exit',
        'status': _ORDER_STATUS_OPEN,
    }))


def _sync_live_exit_fill(
    db,
    trade_id: str,
    leg_id: str,
    exit_reason: str,
    fill_price: float,
) -> None:
    trade, leg, _leg_cfg, _hist_doc = _load_trade_and_leg_context(db, trade_id, leg_id)
    if not trade:
        return
    cancel_open_exit_orders_for_leg(
        db,
        trade,
        leg_id,
        keep_reason=exit_reason,
        cancel_reason=f'exit_fill:{exit_reason}',
    )
    now_ts = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    try:
        from features.live_monitor_service import _live_safe_close_leg_in_db
        _live_safe_close_leg_in_db(db, trade_id, 0, fill_price, exit_reason, now_ts, leg_id=leg_id)
    except Exception as exc:
        log.error('[LIVE EXIT FILL SYNC] trade=%s leg=%s reason=%s: %s', trade_id, leg_id, exit_reason, exc)

    try:
        from features.execution_socket import trigger_live_exit_followups
        followup_actions = trigger_live_exit_followups(db, trade_id, leg_id, exit_reason, now_ts, fill_price=fill_price)
        if followup_actions:
            print(
                f'[LIVE EXIT FOLLOWUP SYNC] trade={trade_id} leg={leg_id} '
                f'reason={exit_reason} actions={followup_actions}'
            )
    except Exception as exc:
        log.warning(
            '[LIVE EXIT FOLLOWUP SYNC] trade=%s leg=%s reason=%s: %s',
            trade_id, leg_id, exit_reason, exc,
        )

    # These reasons mean a pre-placed broker order filled for THIS leg only.
    # Other legs have their own independent SL/Target orders — do NOT touch them.
    _independent_reasons = ('broker_sync', 'stoploss', 'target')
    if exit_reason in _independent_reasons:
        print(f'[BROKER EXIT SQUAREOFF SKIP] trade={trade_id} leg={leg_id} reason={exit_reason} other_legs_independent')
        return

    # Only for explicit all-legs-exit reasons (overall_sl, exit_time, squared_off, manual)
    try:
        refreshed_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or trade
        live_manual_square_off_trade(db, refreshed_trade)
        print(f'[BROKER EXIT SQUAREOFF] trade={trade_id} leg={leg_id} reason={exit_reason}')
    except Exception as exc:
        log.error('[BROKER EXIT SQUAREOFF] trade=%s leg=%s: %s', trade_id, leg_id, exc)


def process_broker_order_update(
    db,
    order_id: str,
    status: str,
    fill_price: float = 0.0,
    fill_qty: int = 0,
    rejection_reason: str = '',
    source: str = 'poll',       # 'poll' | 'postback'
) -> bool:
    """
    Central handler for any broker order status change (fill / reject / cancel).

    Called from:
      - poll_pending_order_fills()  (every 5 sec, fallback)
      - flattrade_postback()        (real-time push)

    Updates:
      1. broker_orders collection
      2. algo_trade_positions_history  (entry_trade.price / order_status)
      3. algo_trades legs array
      4. Marks execution socket dirty

    Returns True if algo DB was updated, False if order not found / already processed.
    """
    now_ts = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    if status in (_ORDER_STATUS_COMPLETE, _ORDER_STATUS_REJECTED, _ORDER_STATUS_CANCELLED):
        # Entry orders: only deregister when fill confirmed (fill_price > 0).
        # COMPLETE with fill_price=0 means postback arrived before avg_price settled
        # — keep in active set; we try an immediate broker fetch below before giving up.
        if status != _ORDER_STATUS_COMPLETE or fill_price > 0:
            _deregister_active_entry_order(order_id)
        _deregister_active_exit_order(order_id)
        with _sl_order_registry_lock:
            stale_keys = [k for k, v in _sl_order_registry.items() if v == order_id]
            for k in stale_keys:
                del _sl_order_registry[k]

    # 1. Update broker_orders collection
    _update_broker_order_status(db, order_id, status, fill_price, fill_qty, rejection_reason)

    # 2. Find the matching history leg document
    hist_col   = db._db['algo_trade_positions_history']
    trades_col = db._db['algo_trades']

    hist_doc = hist_col.find_one(
        {'entry_trade.order_id': order_id},
        {'_id': 1, 'trade_id': 1, 'leg_id': 1, 'entry_trade.order_status': 1},
    )
    embedded_trade = None
    embedded_leg = None
    if not hist_doc:
        embedded_trade, embedded_leg = _find_pending_live_leg_by_order_id(db, order_id)
        if not embedded_trade or not embedded_leg:
            # Check if this is an exit order fill (SL-L / target / exit_time etc.)
            # process_broker_order_update only searches entry_trade.order_id, so exit
            # order fills via postback fall through here — handle them explicitly so
            # the counterpart order is cancelled and the leg is closed in DB.
            if status == _ORDER_STATUS_COMPLETE and fill_price > 0:
                exit_doc = db._db[_BROKER_ORDERS_COL].find_one({'order_id': order_id, 'order_side': 'exit'})
                if exit_doc:
                    _trade_id = str(exit_doc.get('trade_id') or '').strip()
                    _leg_id   = str(exit_doc.get('leg_id')   or '').strip()
                    _reason   = str(exit_doc.get('exit_reason') or 'stoploss').strip() or 'stoploss'
                    if _trade_id and _leg_id:
                        _sync_live_exit_fill(db, _trade_id, _leg_id, _reason, fill_price)
                        return True
                # Entry order not in DB yet (race condition between order placement and postback).
                # Re-register so poll can detect the fill and place SL.
                _register_active_entry_order(order_id)
                log.info('[ORDER NOT FOUND] order=%s re-registered for poll (race condition)', order_id)
            return False

    # Skip if already processed to avoid double updates
    current_entry_trade = (
        (hist_doc.get('entry_trade') or {})
        if hist_doc else
        ((embedded_leg or {}).get('entry_trade') or {})
    )
    current_status = str(current_entry_trade.get('order_status') or '').upper()
    if current_status == status:
        return False

    trade_id = str((hist_doc or {}).get('trade_id') or (embedded_trade or {}).get('_id') or '')
    leg_id   = str((hist_doc or {}).get('leg_id')   or (embedded_leg or {}).get('id') or '')

    # Postback arrived with COMPLETE but avgprc=0 (FlatTrade sends fill notification
    # before avg_price settles). Fetch actual fill price + qty from broker immediately
    # so SL/target orders are placed without waiting for the 30-second poll cycle.
    if status == _ORDER_STATUS_COMPLETE and fill_price == 0:
        _trade_doc = trades_col.find_one({'_id': trade_id}) or {}
        _fetched_price, _fetched_qty = _fetch_broker_fill(db, _trade_doc, order_id)
        if _fetched_price > 0:
            fill_price = _fetched_price
            if _fetched_qty > 0:
                fill_qty = _fetched_qty
            _deregister_active_entry_order(order_id)
            log.info(
                '[POSTBACK FILL FETCH] order=%s fetched price=%.2f qty=%d from broker (postback avgprc was 0)',
                order_id, fill_price, fill_qty,
            )
        else:
            # Broker price not ready yet — leave in active set, poll will handle it.
            log.debug('[POSTBACK FILL FETCH] order=%s broker price not ready, poll will handle', order_id)
            return False

    if status == _ORDER_STATUS_COMPLETE and fill_price > 0:
        if hist_doc:
            hist_col.update_one(
                {'_id': hist_doc['_id']},
                {'$set': {
                    'entry_trade.price':               fill_price,
                    'entry_trade.order_status':        _ORDER_STATUS_COMPLETE,
                    'entry_trade.fill_qty':            int(fill_qty),
                    'entry_trade.filled_at':           now_ts,
                    'entry_trade.traded_timestamp':    now_ts,
                    'entry_trade.exchange_timestamp':  now_ts,
                    'entry_trade.entry_lifecycle_status': 'active',
                }},
            )
        trades_col.update_one(
            {'_id': trade_id},
            {'$set': {
                'legs.$[elem].last_saw_price':                   fill_price,
                'legs.$[elem].entry_trade.price':                fill_price,
                'legs.$[elem].entry_trade.order_status':         _ORDER_STATUS_COMPLETE,
                'legs.$[elem].entry_trade.fill_qty':             int(fill_qty),
                'legs.$[elem].entry_trade.filled_at':            now_ts,
                'legs.$[elem].entry_trade.traded_timestamp':     now_ts,
                'legs.$[elem].entry_trade.exchange_timestamp':   now_ts,
                'legs.$[elem].entry_trade.entry_lifecycle_status': 'active',
            }},
            array_filters=[{'elem.id': leg_id}],
        )
        if not hist_doc:
            _promote_pending_live_leg_to_position_history(db, trade_id, leg_id)

        verified_price = _confirm_and_sync_fill_price(
            db, trade_id, leg_id, order_id, fill_price,
        )
        if verified_price > 0:
            _sync_leg_entry_feature_from_positions_history(db, trade_id, leg_id)
            _place_initial_protection_orders(db, trade_id, leg_id, verified_price, fill_qty)

        try:
            from features.execution_socket import mark_execute_order_dirty_from_trade_id
            mark_execute_order_dirty_from_trade_id(db, trade_id)
        except Exception:
            pass
        print(
            f'[ORDER FILLED][{source}] trade={trade_id} leg={leg_id} '
            f'order_id={order_id} fill_price={fill_price} qty={fill_qty}'
        )
        return True

    elif status in (_ORDER_STATUS_REJECTED, _ORDER_STATUS_CANCELLED):
        if hist_doc:
            hist_col.update_one(
                {'_id': hist_doc['_id']},
                {'$set': {
                    'entry_trade.order_status':     status,
                    'entry_trade.rejection_reason': rejection_reason,
                    'entry_trade.entry_lifecycle_status': 'entry_failed',
                }},
            )
        trades_col.update_one(
            {'_id': trade_id},
            {'$set': {
                'legs.$[elem].entry_trade.order_status': status,
                'legs.$[elem].entry_trade.rejection_reason': rejection_reason,
                'legs.$[elem].entry_trade.entry_lifecycle_status': 'entry_failed',
            }},
            array_filters=[{'elem.id': leg_id}],
        )
        try:
            from features.execution_socket import mark_execute_order_dirty_from_trade_id
            mark_execute_order_dirty_from_trade_id(db, trade_id)
        except Exception:
            pass
        print(
            f'[ORDER {status}][{source}] trade={trade_id} leg={leg_id} '
            f'order_id={order_id} reason={rejection_reason or "-"}'
        )
        # SquareOffAllLegs (opt-in config) OR — unconditionally — a sibling leg of
        # this same strategy already entered and is still open: that's a partial-
        # entry failure (one leg in, one leg errored), so pause the strategy and
        # auto-exit whatever did get entered, regardless of the config flag.
        try:
            trade_doc = trades_col.find_one({'_id': trade_id})
            if trade_doc and str(trade_doc.get('activation_mode') or '').strip() == 'live':
                strategy_cfg = trade_doc.get('strategy') or {}
                sq_all_raw = str(strategy_cfg.get('SquareOffAllLegs') or 'False').strip().lower()
                square_off_all_legs_enabled = sq_all_raw in ('true', '1', 'yes')
                has_sibling_open_leg = bool(hist_col.find_one({
                    'trade_id': trade_id,
                    'status': 1,
                    'exit_trade': None,
                    'entry_trade.entry_lifecycle_status': 'active',
                }, {'_id': 1}))
                if square_off_all_legs_enabled or has_sibling_open_leg:
                    broker = get_broker_for_trade(db, trade_doc)
                    if broker:
                        _rejection_squareoff_all(db, trade_doc, broker, now_ts, leg_id)
                        pause_reason = 'partial_entry_failure' if has_sibling_open_leg else 'square_off_all_legs_config'
                        notify_user(
                            'strategy_paused',
                            f'Strategy paused — leg {leg_id} entry {status.lower()} '
                            f'({rejection_reason or "no reason given"}). Open legs were auto-exited.',
                            {'trade_id': trade_id, 'leg_id': leg_id, 'reason': pause_reason},
                        )
        except Exception as _sq_exc:
            log.error('[SQUAREOFF ALL LEGS ERROR] trade=%s: %s', trade_id, _sq_exc)
        return True

    return False


def get_broker_for_trade(db, trade: dict):
    """
    Return an authenticated broker instance for the trade's mapped broker.

    Reads broker_configuration by trade['broker'] ObjectId.
    Detects broker type from doc's `name` / `broker_icon` field:
      - "flattrade" in name/icon → FlatTradeAdapter
      - otherwise                → KiteConnect (Zerodha)

    Falls back to default broker via kite_market_config when no broker is mapped
    on the trade — Kite or Dhan, whichever has enabled=True there. Dhan is only
    reachable through this global-default fallback today, not as a per-trade
    selectable broker_configuration account (Dhan credentials live in
    kite_market_config, not broker_configuration).
    """
    broker_id = str(trade.get('broker') or '').strip()

    if broker_id:
        try:
            from bson import ObjectId
            from features.flattrade_broker import _is_flattrade_doc, get_flattrade_instance
            from features.dhan_broker import _is_dhan_doc, get_dhan_instance
            broker_doc = db._db['broker_configuration'].find_one(
                {'_id': ObjectId(broker_id)},
                {'access_token': 1, 'user_id': 1, 'name': 1, 'broker_icon': 1, 'broker_user_id': 1},
            ) or {}
            access_token = str(broker_doc.get('access_token') or '').strip()
            if access_token and _is_flattrade_doc(broker_doc):
                user_id = str(broker_doc.get('user_id') or '').strip()
                ft = get_flattrade_instance(user_id, access_token)
                if ft:
                    log.debug('broker=flattrade trade=%s', str(trade.get('_id') or ''))
                    return ft
            elif access_token and _is_dhan_doc(broker_doc):
                client_id = str(broker_doc.get('broker_user_id') or broker_doc.get('user_id') or '').strip()
                dhan = get_dhan_instance(db, client_id, access_token)
                if dhan:
                    log.debug('broker=dhan trade=%s', str(trade.get('_id') or ''))
                    return dhan
            elif access_token:
                from features.broker_gateway import get_broker_rest_client_with_token as get_kite_instance
                return get_kite_instance(access_token)
        except Exception as exc:
            log.debug('broker lookup error broker=%s: %s', broker_id, exc)

    # Fallback — default broker via kite_market_config (Kite or Dhan, whichever is enabled)
    try:
        market_cfg = db._db['kite_market_config'].find_one({'enabled': True}, {'broker': 1, 'access_token': 1, 'user_id': 1, 'dhan_client_id': 1}) or {}
        access_token = str(market_cfg.get('access_token') or '').strip()
        if access_token and str(market_cfg.get('broker') or '').strip().lower() == 'dhan':
            from features.dhan_broker import get_dhan_instance
            client_id = str(market_cfg.get('user_id') or market_cfg.get('dhan_client_id') or '').strip()
            dhan = get_dhan_instance(db, client_id, access_token)
            if dhan:
                log.debug('broker=dhan (default) trade=%s', str(trade.get('_id') or ''))
                return dhan
        elif access_token:
            from features.broker_gateway import get_broker_rest_client_with_token as get_kite_instance
            return get_kite_instance(access_token)
    except Exception as exc:
        log.debug('market config token lookup error: %s', exc)

    return None


# Keep old name as alias so other callers (if any) don't break
get_kite_for_trade = get_broker_for_trade


# ── Order config resolution ───────────────────────────────────────────────────

def _resolve_entry_order_config(leg_cfg: dict) -> dict:
    """
    Read EntryOrder config from leg_cfg.
    Returns dict with keys:
      order_type        – 'LIMIT' or 'MARKET'
      limit_buffer      – float (points or %)
      trigger_buffer    – float (for SL-L; 0 = plain LIMIT)
      buffer_type       – 'points' or 'percentage'
      convert_after     – int seconds (0 = never convert to market)
    """
    entry_order = (leg_cfg.get('EntryOrder') or {})
    if isinstance(entry_order.get('Config'), dict):
        entry_order = entry_order['Config']

    raw_type = str(entry_order.get('Type') or 'OrderType.MPP').lower()
    is_mpp   = 'mpp' in raw_type
    is_limit = 'limit' in raw_type and not is_mpp

    value = entry_order.get('Value') or {}
    if not isinstance(value, dict):
        value = {}
    buffer_cfg = value.get('Buffer') or {}
    if not isinstance(buffer_cfg, dict):
        buffer_cfg = {}
    buf_val = buffer_cfg.get('Value') or {}
    if not isinstance(buf_val, dict):
        buf_val = {}
    buf_type_raw = str(buffer_cfg.get('Type') or 'BufferType.Points').lower()
    buffer_type = 'percentage' if 'percent' in buf_type_raw else 'points'

    limit_buffer   = _safe_float(buf_val.get('LimitBuffer', 3))
    trigger_buffer = _safe_float(buf_val.get('TriggerBuffer', 0))

    mod = value.get('Modification') or {}
    _raw_after = mod.get('MarketOrderAfter')
    convert_after = int(_raw_after) if _raw_after is not None and str(_raw_after) != '' else 40

    if is_mpp:
        order_type = _ORDER_TYPE_MPP
    elif is_limit:
        order_type = _ORDER_TYPE_LIMIT
    else:
        order_type = _ORDER_TYPE_MARKET

    return {
        'order_type':     order_type,
        'limit_buffer':   limit_buffer,
        'trigger_buffer': trigger_buffer,
        'buffer_type':    buffer_type,
        'convert_after':  convert_after,
    }


def _resolve_exit_order_config(leg_cfg: dict) -> dict:
    exit_order = (leg_cfg.get('ExitOrder') or {})
    if isinstance(exit_order.get('Config'), dict):
        exit_order = exit_order['Config']

    raw_type = str(exit_order.get('Type') or 'OrderType.MPP').lower()
    is_mpp   = 'mpp' in raw_type
    is_limit = 'limit' in raw_type and not is_mpp

    value = exit_order.get('Value') or {}
    if not isinstance(value, dict):
        value = {}
    buffer_cfg = value.get('Buffer') or {}
    if not isinstance(buffer_cfg, dict):
        buffer_cfg = {}
    buf_val = buffer_cfg.get('Value') or {}
    if not isinstance(buf_val, dict):
        buf_val = {}
    buf_type_raw = str(buffer_cfg.get('Type') or 'BufferType.Points').lower()
    buffer_type = 'percentage' if 'percent' in buf_type_raw else 'points'

    limit_buffer   = _safe_float(buf_val.get('LimitBuffer', 3))
    trigger_buffer = _safe_float(buf_val.get('TriggerBuffer', 0))

    mod = value.get('Modification') or {}
    _raw_after = mod.get('MarketOrderAfter')
    convert_after = int(_raw_after) if _raw_after is not None and str(_raw_after) != '' else 40

    if is_mpp:
        order_type = _ORDER_TYPE_MPP
    elif is_limit:
        order_type = _ORDER_TYPE_LIMIT
    else:
        order_type = _ORDER_TYPE_MARKET

    return {
        'order_type':     order_type,
        'limit_buffer':   limit_buffer,
        'trigger_buffer': trigger_buffer,
        'buffer_type':    buffer_type,
        'convert_after':  convert_after,
    }


def _apply_buffer(price: float, buffer: float, buffer_type: str, is_buy: bool) -> float:
    """Add buffer for BUY (chase price up), subtract for SELL (chase price down)."""
    if buffer_type == 'percentage':
        delta = price * buffer / 100.0
    else:
        delta = buffer
    return _clamp_limit_price(price + delta if is_buy else price - delta, is_buy)


# ── Order placement ───────────────────────────────────────────────────────────

def place_live_entry_order(
    db,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
    symbol: str,
    qty: int,
    ltp: float,
    force_order_type: str = '',
    force_limit_price: float = 0.0,
    force_trigger_price: float = 0.0,
) -> dict:
    """
    Place an entry order for a live strategy leg.

    Returns dict:
      order_id     – Kite order ID string (empty on failure)
      order_type   – 'LIMIT' / 'MARKET' / 'SL'
      limit_price  – price submitted to broker
      trigger_price– trigger price (for SL-L; 0 otherwise)
      order_status – 'OPEN' / 'MARKET_PLACED' / 'FAILED'
      error        – error message if failed
    """
    from features.app_logger import log_db_write, log_db_error
    trade_id = str(trade.get('_id') or '')
    leg_id   = str(leg.get('id') or '')

    if str(trade.get('activation_mode') or '').strip() != 'live':
        log.warning('[LIVE ORDER] skipped — not live mode trade=%s', trade_id)
        return {'order_id': '', 'order_type': _ORDER_TYPE_MARKET, 'limit_price': ltp,
                'trigger_price': 0.0, 'order_status': 'FAILED', 'error': 'not_live_mode'}

    if not symbol:
        log.warning('[LIVE ORDER] no symbol for leg=%s trade=%s', leg_id, trade_id)
        return {'order_id': '', 'order_type': _ORDER_TYPE_MARKET, 'limit_price': ltp,
                'trigger_price': 0.0, 'order_status': 'FAILED', 'error': 'no_symbol'}

    expected_option_type = _expected_leg_option_type(leg, leg_cfg)
    resolved_option_type = _extract_option_type_from_symbol(symbol)
    if expected_option_type and resolved_option_type and expected_option_type != resolved_option_type:
        print(
            f'[LIVE ORDER BLOCKED] trade={trade_id} leg={leg_id} '
            f'symbol={symbol} expected_option={expected_option_type} '
            f'resolved_option={resolved_option_type} '
            f'reason=option_type_mismatch_before_order'
        )
        return {
            'order_id': '',
            'order_type': _ORDER_TYPE_MARKET,
            'limit_price': ltp,
            'trigger_price': 0.0,
            'order_status': 'BLOCKED',
            'error': f'option_type_mismatch:{expected_option_type}!={resolved_option_type}',
        }

    kite = get_broker_for_trade(db, trade)
    if not kite:
        log.warning('[LIVE ORDER] no broker instance for trade=%s', trade_id)
        return {'order_id': '', 'order_type': _ORDER_TYPE_MARKET, 'limit_price': ltp,
                'trigger_price': 0.0, 'order_status': 'FAILED', 'error': 'no_kite_session'}

    cfg = _resolve_entry_order_config(leg_cfg)
    position_str = str(leg.get('position') or 'PositionType.Sell')
    is_sell = _is_sell(position_str)
    # Entry: BUY means we're buying the option; SELL means we're selling it
    txn_type = _TXN_SELL if is_sell else _TXN_BUY
    is_buy_order = not is_sell

    # Product type from config
    product_raw = str((leg_cfg.get('ProductType') or leg_cfg.get('Product') or _NRML)).upper()
    product = _MIS if 'MIS' in product_raw else _NRML
    exchange = _resolve_exchange(symbol, trade, leg)

    order_type    = cfg['order_type']
    limit_buffer  = cfg['limit_buffer']
    trigger_buffer= cfg['trigger_buffer']
    buffer_type   = cfg['buffer_type']

    limit_price   = 0.0
    trigger_price = 0.0
    kite_order_type = str(force_order_type or order_type).strip() or order_type

    if force_order_type:
        limit_price = _safe_float(force_limit_price)
        trigger_price = _safe_float(force_trigger_price)

    if force_order_type:
        pass
    elif order_type == _ORDER_TYPE_MPP:
        # Algotest MPP formula:
        #   BUY  → BID + pct%  (crosses ask → guaranteed fill, less overpay)
        #   SELL → ASK - pct%  (crosses bid → guaranteed fill, less slippage)
        # Then: tick align to 0.05, min sell price ₹0.05
        bid, ask = _get_bid_ask(kite, symbol, ltp, exchange)
        # Never substitute ltp for a missing bid/ask — that isn't a live book price and
        # would place a real order at a fabricated "protected" price with no depth behind
        # it. Abort instead of guessing; the caller must not proceed to place this order.
        if (is_buy_order and bid <= 0) or (not is_buy_order and ask <= 0):
            log.error('[MPP ENTRY BLOCKED] symbol=%s bid=%s ask=%s — no live depth, order NOT placed', symbol, bid, ask)
            notify_admin(
                'mpp_price_unresolved',
                f'MPP entry price unavailable for {symbol} (bid={bid}, ask={ask}) — order NOT placed. '
                f'trade={trade_id} leg={leg_id}',
            )
            return {'order_id': '', 'order_type': _ORDER_TYPE_MPP, 'limit_price': 0.0,
                    'trigger_price': 0.0, 'order_status': 'FAILED', 'error': 'mpp_price_unavailable'}
        pct = _mpp_protection_pct(ltp, is_option=True)
        if is_buy_order:
            raw_price = bid * (1 + pct / 100)
        else:
            raw_price = ask * (1 - pct / 100)
        limit_price = _clamp_limit_price(raw_price, is_buy_order)
        kite_order_type = _ORDER_TYPE_LIMIT
        print(
            f'[MPP ENTRY] symbol={symbol} ltp={ltp} bid={bid} ask={ask} '
            f'pct={pct}% limit_price={limit_price} is_buy={is_buy_order}'
        )
    elif order_type == _ORDER_TYPE_LIMIT:
        if trigger_buffer > 0:
            # SL-L order: trigger first, then limit fills
            trigger_price = _apply_buffer(ltp, trigger_buffer, buffer_type, is_buy_order)
            limit_price   = _apply_buffer(trigger_price, limit_buffer, buffer_type, is_buy_order)
            kite_order_type = _ORDER_TYPE_SL
        else:
            limit_price   = _apply_buffer(ltp, limit_buffer, buffer_type, is_buy_order)
            kite_order_type = _ORDER_TYPE_LIMIT

    try:
        order_params: dict[str, Any] = {
            'tradingsymbol':   symbol,
            'exchange':        exchange,
            'transaction_type':txn_type,
            'quantity':        int(qty),
            'order_type':      kite_order_type,
            'product':         product,
            'variety':         _VARIETY_REGULAR,
        }
        # FlatTrade blocks SL-MKT for API orders — convert to SL-LMT automatically
        if kite_order_type == _ORDER_TYPE_SLM:
            kite_order_type = _ORDER_TYPE_SL
            order_params['order_type'] = _ORDER_TYPE_SL
            if not limit_price:
                is_buy = (txn_type == _TXN_BUY)
                limit_price = _sl_limit_price(trigger_price, is_sell_position=not is_buy)
        # Ensure all prices are multiples of NFO tick size (0.05)
        if limit_price > 0:
            limit_price = _round_to_tick(limit_price, round_up=(txn_type == _TXN_BUY))
        if trigger_price > 0:
            trigger_price = _round_to_tick(trigger_price, round_up=(txn_type == _TXN_BUY))
        if kite_order_type in (_ORDER_TYPE_LIMIT, _ORDER_TYPE_SL):
            order_params['price'] = limit_price
        if kite_order_type == _ORDER_TYPE_SL:
            order_params['trigger_price'] = trigger_price

        same_option_legs = _find_existing_trade_option_conflicts(
            db,
            trade_id,
            leg_id,
            expected_option_type or resolved_option_type,
        )
        if same_option_legs:
            print(
                f'[LIVE ORDER BLOCKED] trade={trade_id} leg={leg_id} '
                f'exchange={exchange} symbol={symbol} txn={txn_type} qty={qty} '
                f'order_type={kite_order_type} limit_price={limit_price} '
                f'trigger_price={trigger_price} expected_option={expected_option_type or "-"} '
                f'resolved_option={resolved_option_type or "-"} '
                f'conflicting_legs={",".join(same_option_legs)} '
                f'reason=duplicate_option_type_before_order'
            )
            return {
                'order_id': '', 'order_type': kite_order_type,
                'exchange': exchange, 'limit_price': limit_price,
                'trigger_price': trigger_price, 'order_status': 'BLOCKED',
                'error': 'duplicate_option_type_before_order',
            }

        if not _is_live_order_punch_enabled():
            simulated_order_id = _build_simulated_live_order_id(trade_id, leg_id, 'entry')
            print(
                f'[LIVE ORDER SIMULATED] trade={trade_id} leg={leg_id} '
                f'exchange={exchange} symbol={symbol} txn={txn_type} qty={qty} '
                f'order_type={kite_order_type} limit_price={limit_price} '
                f'trigger_price={trigger_price} expected_option={expected_option_type or "-"} '
                f'resolved_option={resolved_option_type or "-"} '
                f'env=LIVE_ORDER_STATUS:false simulated_order_id={simulated_order_id}'
            )
            return {
                'order_id': simulated_order_id,
                'order_type': kite_order_type,
                'exchange': exchange,
                'limit_price': limit_price,
                'trigger_price': trigger_price,
                'order_status': _ORDER_STATUS_COMPLETE,
                'convert_after': 0,
                'error': '',
            }

        result = _remote_place_broker_order(
            trade.get('broker'),
            tradingsymbol=order_params['tradingsymbol'],
            exchange=order_params['exchange'],
            transaction_type=order_params['transaction_type'],
            quantity=order_params['quantity'],
            order_type=order_params['order_type'],
            product=order_params['product'],
            variety=order_params['variety'],
            price=order_params.get('price', 0.0),
            trigger_price=order_params.get('trigger_price', 0.0),
            context={'trade_id': trade_id, 'leg_id': leg_id, 'purpose': 'entry', 'symbol': symbol},
        )
        if result['status'] != 'success':
            log.error('[LIVE ORDER FAILED] trade=%s leg=%s symbol=%s: %s', trade_id, leg_id, symbol, result['message'])
            log_db_error('broker_orders', 'place_entry_order', Exception(result['message']), f'{trade_id}/{leg_id}')
            return {
                'order_id':      '',
                'order_type':    kite_order_type,
                'limit_price':   limit_price,
                'trigger_price': trigger_price,
                'order_status':  'FAILED',
                'error':         result['message'],
            }
        order_id = result['order_id']
        _register_active_entry_order(order_id)

        print(
            f'[LIVE ORDER PLACED] trade={trade_id} leg={leg_id} '
            f'exchange={exchange} symbol={symbol} txn={txn_type} qty={qty} '
            f'order_type={kite_order_type} limit_price={limit_price} '
            f'trigger_price={trigger_price} order_id={order_id}'
        )
        _save_broker_order(
            db, trade, kite, order_id, 'entry',
            symbol, exchange, txn_type, qty,
            kite_order_type, limit_price, trigger_price, product,
            leg_id=leg_id,
        )
        log_db_write('broker_orders', 'place_entry_order', order_id, {
            'trade_id': trade_id, 'leg_id': leg_id, 'exchange': exchange, 'symbol': symbol,
            'order_type': kite_order_type, 'limit_price': limit_price,
        })
        return {
            'order_id':      order_id,
            'order_type':    kite_order_type,
            'exchange':      exchange,
            'limit_price':   limit_price,
            'trigger_price': trigger_price,
            'order_status':  _ORDER_STATUS_OPEN,
            'convert_after': cfg['convert_after'],
            'error':         '',
        }
    except Exception as exc:
        log.error('[LIVE ORDER FAILED] trade=%s leg=%s symbol=%s: %s', trade_id, leg_id, symbol, exc)
        log_db_error('broker_orders', 'place_entry_order', exc, f'{trade_id}/{leg_id}')
        return {
            'order_id':      '',
            'order_type':    kite_order_type,
            'limit_price':   limit_price,
            'trigger_price': trigger_price,
            'order_status':  'FAILED',
            'error':         str(exc),
        }


def place_live_exit_order(
    db,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
    symbol: str,
    qty: int,
    exit_price: float,
    exit_reason: str,
    force_order_type: str = '',
    force_limit_price: float = 0.0,
    force_trigger_price: float = 0.0,
) -> dict:
    """
    Place an exit order for a live strategy leg.
    exit_reason: 'stoploss' | 'target' | 'exit_time' | 'squared_off' | 'overall_sl' etc.
    """
    from features.app_logger import log_db_write, log_db_error
    trade_id = str(trade.get('_id') or '')
    leg_id   = str(leg.get('id') or '')

    if str(trade.get('activation_mode') or '').strip() != 'live':
        log.warning('[LIVE EXIT ORDER] skipped — not live mode trade=%s', trade_id)
        return {'order_id': '', 'order_type': _ORDER_TYPE_MARKET, 'order_status': 'FAILED', 'error': 'not_live_mode'}

    if not symbol:
        return {'order_id': '', 'order_type': _ORDER_TYPE_MARKET, 'order_status': 'FAILED', 'error': 'no_symbol'}

    kite = get_broker_for_trade(db, trade)
    if not kite:
        return {'order_id': '', 'order_type': _ORDER_TYPE_MARKET, 'order_status': 'FAILED', 'error': 'no_broker_session'}

    cfg = _resolve_exit_order_config(leg_cfg)
    position_str = str(leg.get('position') or 'PositionType.Sell')
    is_sell = _is_sell(position_str)
    # Exit: reverse of position — SELL exits BUY; BUY exits SELL
    txn_type = _TXN_BUY if is_sell else _TXN_SELL
    is_buy_order = not is_sell  # direction of the exit order itself

    product_raw = str((leg_cfg.get('ProductType') or leg_cfg.get('Product') or _NRML)).upper()
    product = _MIS if 'MIS' in product_raw else _NRML
    exchange = _resolve_exchange(symbol, trade, leg)

    order_type     = cfg['order_type']
    limit_buffer   = cfg['limit_buffer']
    trigger_buffer = cfg['trigger_buffer']
    buffer_type    = cfg['buffer_type']

    limit_price   = 0.0
    trigger_price = 0.0
    kite_order_type = str(force_order_type or order_type).strip() or order_type

    if force_order_type:
        limit_price = _safe_float(force_limit_price)
        trigger_price = _safe_float(force_trigger_price)

    if force_order_type:
        pass
    elif order_type == _ORDER_TYPE_MPP:
        # Algotest MPP formula:
        #   BUY to close  → BID + pct%
        #   SELL to close → ASK - pct%
        bid, ask = _get_bid_ask(kite, symbol, exit_price, exchange)
        is_exit_buy = is_sell   # sell position → BUY to close; buy position → SELL to close
        # Never substitute exit_price for a missing bid/ask — same reasoning as the entry
        # path (see place_live_entry_order). Note: this blocks the exit order itself, so a
        # pending SL/target on this leg stays unprotected until the next monitor cycle
        # retries — surfaced loudly via Telegram specifically because of that.
        if (is_exit_buy and bid <= 0) or (not is_exit_buy and ask <= 0):
            log.error('[MPP EXIT BLOCKED] symbol=%s bid=%s ask=%s reason=%s — no live depth, order NOT placed', symbol, bid, ask, exit_reason)
            notify_admin(
                'mpp_price_unresolved',
                f'MPP exit price unavailable for {symbol} (bid={bid}, ask={ask}, reason={exit_reason}) — '
                f'order NOT placed, leg remains open. trade={trade_id} leg={leg_id}',
            )
            return {'order_id': '', 'order_type': _ORDER_TYPE_MPP, 'limit_price': 0.0,
                    'trigger_price': 0.0, 'order_status': 'FAILED', 'error': 'mpp_price_unavailable'}
        pct = _mpp_protection_pct(exit_price, is_option=True)
        if is_exit_buy:
            raw_price = bid * (1 + pct / 100)
        else:
            raw_price = ask * (1 - pct / 100)
        limit_price = _clamp_limit_price(raw_price, is_exit_buy)
        kite_order_type = _ORDER_TYPE_LIMIT
        print(
            f'[MPP EXIT] symbol={symbol} exit_price={exit_price} bid={bid} ask={ask} '
            f'pct={pct}% limit_price={limit_price} reason={exit_reason}'
        )
    elif exit_reason == 'stoploss':
        # SL-L: already at SL price; trigger = exit_price, limit = exit_price - buffer
        if trigger_buffer > 0:
            trigger_price = _round_price(exit_price)
            limit_price   = _apply_buffer(exit_price, limit_buffer, buffer_type, not is_buy_order)
            kite_order_type = _ORDER_TYPE_SL
        else:
            limit_price = _apply_buffer(exit_price, limit_buffer, buffer_type, not is_buy_order)
            kite_order_type = _ORDER_TYPE_LIMIT
    elif exit_reason == 'target':
        limit_price = _apply_buffer(exit_price, limit_buffer, buffer_type, not is_buy_order)
        kite_order_type = _ORDER_TYPE_LIMIT
    else:
        # exit_time / squared_off / overall_sl etc.
        if order_type == _ORDER_TYPE_LIMIT:
            limit_price = _apply_buffer(exit_price, limit_buffer, buffer_type, not is_buy_order)
            kite_order_type = _ORDER_TYPE_LIMIT
        else:
            kite_order_type = _ORDER_TYPE_MARKET

    try:
        order_params: dict[str, Any] = {
            'tradingsymbol':    symbol,
            'exchange':         exchange,
            'transaction_type': txn_type,
            'quantity':         int(qty),
            'order_type':       kite_order_type,
            'product':          product,
            'variety':          _VARIETY_REGULAR,
        }
        # FlatTrade blocks SL-MKT for API orders — convert to SL-LMT automatically
        if kite_order_type == _ORDER_TYPE_SLM:
            kite_order_type = _ORDER_TYPE_SL
            order_params['order_type'] = _ORDER_TYPE_SL
            if not limit_price:
                # is_sell = position direction; exit for SELL position is BUY
                limit_price = _sl_limit_price(trigger_price, is_sell_position=is_sell)
        # Ensure all prices are multiples of NFO tick size (0.05) — FlatTrade rejects otherwise
        if limit_price > 0:
            limit_price = _round_to_tick(limit_price, round_up=(txn_type == _TXN_BUY))
        if trigger_price > 0:
            trigger_price = _round_to_tick(trigger_price, round_up=(txn_type == _TXN_BUY))
        if kite_order_type in (_ORDER_TYPE_LIMIT, _ORDER_TYPE_SL):
            order_params['price'] = limit_price
        if kite_order_type == _ORDER_TYPE_SL:
            order_params['trigger_price'] = trigger_price

        if not _is_live_order_punch_enabled():
            simulated_order_id = _build_simulated_live_order_id(trade_id, leg_id, 'exit')
            print(
                f'[LIVE EXIT ORDER SIMULATED] trade={trade_id} leg={leg_id} '
                f'exchange={exchange} symbol={symbol} txn={txn_type} qty={qty} '
                f'reason={exit_reason} order_type={kite_order_type} '
                f'limit_price={limit_price} trigger_price={trigger_price} '
                f'env=LIVE_ORDER_STATUS:false simulated_order_id={simulated_order_id}'
            )
            return {
                'order_id': simulated_order_id,
                'order_type': kite_order_type,
                'exchange': exchange,
                'limit_price': limit_price,
                'trigger_price': trigger_price,
                'order_status': _ORDER_STATUS_COMPLETE,
                'error': '',
            }

        result = _remote_place_broker_order(
            trade.get('broker'),
            tradingsymbol=order_params['tradingsymbol'],
            exchange=order_params['exchange'],
            transaction_type=order_params['transaction_type'],
            quantity=order_params['quantity'],
            order_type=order_params['order_type'],
            product=order_params['product'],
            variety=order_params['variety'],
            price=order_params.get('price', 0.0),
            trigger_price=order_params.get('trigger_price', 0.0),
            context={'trade_id': trade_id, 'leg_id': leg_id, 'purpose': f'exit:{exit_reason}', 'symbol': symbol},
        )
        if result['status'] != 'success':
            log.error('[LIVE EXIT ORDER FAILED] trade=%s leg=%s: %s', trade_id, leg_id, result['message'])
            log_db_error('broker_orders', 'place_exit_order', Exception(result['message']), f'{trade_id}/{leg_id}')
            return {
                'order_id':      '',
                'order_type':    kite_order_type,
                'limit_price':   limit_price,
                'trigger_price': trigger_price,
                'order_status':  'FAILED',
                'error':         result['message'],
            }
        order_id = result['order_id']
        _register_active_exit_order(order_id)

        print(
            f'[LIVE EXIT ORDER] trade={trade_id} leg={leg_id} '
            f'exchange={exchange} symbol={symbol} txn={txn_type} qty={qty} '
            f'reason={exit_reason} order_type={kite_order_type} '
            f'limit_price={limit_price} order_id={order_id}'
        )
        _save_broker_order(
            db, trade, kite, order_id, 'exit',
            symbol, exchange, txn_type, qty,
            kite_order_type, limit_price, trigger_price, product,
            leg_id=leg_id, exit_reason=exit_reason,
        )
        log_db_write('broker_orders', 'place_exit_order', order_id, {
            'trade_id': trade_id, 'leg_id': leg_id, 'exchange': exchange, 'symbol': symbol,
            'exit_reason': exit_reason, 'order_type': kite_order_type,
        })
        return {
            'order_id':      order_id,
            'order_type':    kite_order_type,
            'exchange':      exchange,
            'limit_price':   limit_price,
            'trigger_price': trigger_price,
            'order_status':  _ORDER_STATUS_OPEN,
            'error':         '',
        }
    except Exception as exc:
        log.error('[LIVE EXIT ORDER FAILED] trade=%s leg=%s: %s', trade_id, leg_id, exc)
        log_db_error('broker_orders', 'place_exit_order', exc, f'{trade_id}/{leg_id}')
        return {
            'order_id':      '',
            'order_type':    kite_order_type,
            'limit_price':   limit_price,
            'trigger_price': trigger_price,
            'order_status':  'FAILED',
            'error':         str(exc),
        }


# ── Order fill poller ─────────────────────────────────────────────────────────

_poll_lock     = threading.Lock()
_pos_sync_lock = threading.Lock()

# Track only orders placed in the current session — poll will ignore pre-existing DB entries
_active_entry_order_ids: set[str] = set()
_active_exit_order_ids: set[str] = set()
_active_entry_lock = threading.Lock()
_active_exit_lock  = threading.Lock()

# SL order registry: maps "trade_id:leg_id" → order_id
# Populated when protection SL order is placed; used for trail SL modification
_sl_order_registry: dict[str, str] = {}
_sl_order_registry_lock = threading.Lock()


def _register_sl_order(trade_id: str, leg_id: str, order_id: str) -> None:
    if trade_id and leg_id and order_id:
        key = f'{trade_id}:{leg_id}'
        with _sl_order_registry_lock:
            _sl_order_registry[key] = order_id
        print(f'[SL ORDER REGISTERED] trade={trade_id} leg={leg_id} order={order_id}')


def _deregister_sl_order(trade_id: str, leg_id: str) -> None:
    key = f'{trade_id}:{leg_id}'
    with _sl_order_registry_lock:
        _sl_order_registry.pop(key, None)


def _get_sl_order_id(trade_id: str, leg_id: str) -> str:
    key = f'{trade_id}:{leg_id}'
    with _sl_order_registry_lock:
        return _sl_order_registry.get(key, '')


def restore_sl_order_registry(db) -> None:
    """
    On monitor start:
    1. Reload existing TRIGGER_PENDING SL orders into sl_order_registry (for trail SL modify).
    2. Reload existing OPEN entry orders into _active_entry_order_ids (so poll detects fills).
    """
    sl_loaded = entry_loaded = 0
    try:
        # Restore SL orders
        for hist_doc in db._db['algo_trade_positions_history'].find(
            {'broker_stoploss_order_id': {'$exists': True, '$ne': ''}, 'exit_trade': None},
            {'trade_id': 1, 'leg_id': 1, 'broker_stoploss_order_id': 1},
        ):
            trade_id = str(hist_doc.get('trade_id') or '').strip()
            leg_id   = str(hist_doc.get('leg_id') or '').strip()
            order_id = str(hist_doc.get('broker_stoploss_order_id') or '').strip()
            if trade_id and leg_id and order_id:
                with _sl_order_registry_lock:
                    _sl_order_registry[f'{trade_id}:{leg_id}'] = order_id
                sl_loaded += 1

        # Restore pending entry orders (order_status=OPEN, not yet filled)
        for leg_doc in db._db['algo_trade_positions_history'].find(
            {
                'entry_trade.order_status': 'OPEN',
                'entry_trade.entry_lifecycle_status': 'order_open',
                'exit_trade': None,
            },
            {'entry_trade.order_id': 1},
        ):
            order_id = str((leg_doc.get('entry_trade') or {}).get('order_id') or '').strip()
            if order_id:
                with _active_entry_lock:
                    _active_entry_order_ids.add(order_id)
                entry_loaded += 1
    except Exception as exc:
        log.warning('[REGISTRY RESTORE ERROR] %s', exc)
    print(f'[REGISTRY RESTORED] sl_orders={sl_loaded} pending_entries={entry_loaded}')


def has_pending_entry_orders() -> bool:
    with _active_entry_lock:
        return bool(_active_entry_order_ids)


def _register_active_entry_order(order_id: str) -> None:
    if order_id:
        with _active_entry_lock:
            _active_entry_order_ids.add(order_id)


def _deregister_active_entry_order(order_id: str) -> None:
    if order_id:
        with _active_entry_lock:
            _active_entry_order_ids.discard(order_id)


def _register_active_exit_order(order_id: str) -> None:
    if order_id:
        with _active_exit_lock:
            _active_exit_order_ids.add(order_id)


def _deregister_active_exit_order(order_id: str) -> None:
    if order_id:
        with _active_exit_lock:
            _active_exit_order_ids.discard(order_id)


def _broker_net_positions(broker) -> dict[str, int] | None:
    """
    Call broker.positions() and return {tradingsymbol: net_qty} map.
    Returns None if the call fails or broker doesn't support it.
    For KiteConnect, positions() returns {"net": [...]}; for FlatTradeAdapter it returns a list.
    """
    if not hasattr(broker, 'positions'):
        return None
    try:
        raw = broker.positions()
    except Exception as exc:
        log.warning('[POSITION SYNC] broker.positions() error: %s', exc)
        return None
    # KiteConnect returns {"net": [...], "day": [...]}; FlatTradeAdapter returns a list
    if isinstance(raw, dict):
        raw = raw.get('net') or []
    if not isinstance(raw, list):
        return None
    pos_map: dict[str, int] = {}
    for p in raw:
        sym = str(p.get('tradingsymbol') or '').strip()
        if sym:
            pos_map[sym] = int(p.get('quantity') or 0)
    return pos_map


def sync_open_leg_positions(db) -> int:
    """
    Reconcile active open legs against actual broker net positions.

    For each open leg (entry COMPLETE, no exit yet) the system expects a non-zero
    net qty at the broker.  If the broker shows qty=0 the position was closed
    externally (manual trade in broker terminal, broker risk-management square-off,
    etc.).  The affected leg is closed via _sync_live_exit_fill() with
    exit_kind='broker_sync'.

    Works for any broker that implements positions() — both KiteConnect and
    FlatTradeAdapter support it after this change.

    Called every ~30 s from live_fast_monitor (every 120 ticks).
    Returns number of legs closed due to external exit.
    """
    if not _pos_sync_lock.acquire(blocking=False):
        return 0
    closed = 0
    try:
        hist_col   = db._db['algo_trade_positions_history']
        trades_col = db._db['algo_trades']

        # All active open positions (entry filled, not yet exited)
        open_docs = list(hist_col.find(
            {
                'status': 1,
                'exit_trade': None,
                'entry_trade.entry_lifecycle_status': 'active',
            },
            {'trade_id': 1, 'leg_id': 1, 'symbol': 1, 'quantity': 1, 'position': 1},
        ))
        if not open_docs:
            return 0

        trade_ids = list({str(d.get('trade_id') or '') for d in open_docs if d.get('trade_id')})
        trades = {
            str(t.get('_id') or ''): t
            for t in trades_col.find(
                {'_id': {'$in': trade_ids}, 'activation_mode': 'live'},
                {'_id': 1, 'broker': 1},
            )
        }

        # Cache broker positions per broker account (avoid redundant API calls)
        pos_cache: dict[str, dict[str, int] | None] = {}

        for doc in open_docs:
            trade_id = str(doc.get('trade_id') or '')
            leg_id   = str(doc.get('leg_id')   or '')
            symbol   = str(doc.get('symbol')    or '').strip()
            if not trade_id or not leg_id or not symbol:
                continue

            # Skip legs that have active SL/target exit orders — those are handled
            # exclusively by the postback URL and order poll. Position sync must not
            # interfere with broker-placed protection orders.
            if _get_open_exit_orders_for_leg(db, trade_id, leg_id):
                continue

            trade = trades.get(trade_id)
            if not trade:
                continue

            broker_id = str(trade.get('broker') or '').strip()
            cache_key = broker_id or '_default'

            if cache_key not in pos_cache:
                broker = get_broker_for_trade(db, trade)
                pos_cache[cache_key] = _broker_net_positions(broker) if broker else None

            pos_map = pos_cache.get(cache_key)
            if pos_map is None:
                continue  # Can't reach this broker; skip

            broker_qty = pos_map.get(symbol, 0)  # 0 if symbol absent (= fully closed)
            expected_abs = int(doc.get('quantity') or 0)

            # Only act when broker confirms position is fully closed (qty=0)
            # and we still think it's open
            if broker_qty == 0 and expected_abs > 0:
                # Check if one of our own exit orders already filled —
                # if so, use its fill price and reason (stoploss/target) so
                # the SL order is NOT cancelled (it already executed).
                sync_exit_price = 0.0
                sync_exit_reason = 'broker_sync'
                try:
                    filled_exit = db._db[_BROKER_ORDERS_COL].find_one(
                        {
                            'trade_id': trade_id,
                            'leg_id':   leg_id,
                            'order_side': 'exit',
                            'status':   _ORDER_STATUS_COMPLETE,
                        },
                        sort=[('_id', -1)],
                    )
                    if filled_exit:
                        sync_exit_price  = _safe_float(filled_exit.get('fill_price') or 0)
                        sync_exit_reason = str(filled_exit.get('exit_reason') or 'broker_sync').strip() or 'broker_sync'
                except Exception:
                    pass
                log.info(
                    '[POSITION SYNC] External close: trade=%s leg=%s symbol=%s qty_expected=%d exit_price=%s reason=%s → closing leg',
                    trade_id, leg_id, symbol, expected_abs, sync_exit_price or 'unknown', sync_exit_reason,
                )
                print(
                    f'[POSITION SYNC] External close detected: '
                    f'trade={trade_id} leg={leg_id} symbol={symbol} '
                    f'exit_price={sync_exit_price or "unknown"} reason={sync_exit_reason} → auto-closing'
                )
                try:
                    _sync_live_exit_fill(db, trade_id, leg_id, sync_exit_reason, sync_exit_price)
                    closed += 1
                except Exception as exc:
                    log.error('[POSITION SYNC] close failed trade=%s leg=%s: %s', trade_id, leg_id, exc)

    except Exception as exc:
        log.error('[POSITION SYNC ERROR] %s', exc, exc_info=True)
    finally:
        _pos_sync_lock.release()
    return closed


def poll_pending_order_fills(db) -> int:
    """
    Check all live legs with order_status='OPEN' and update fill price.
    Called from live_fast_monitor loop every ~5 seconds.
    Returns number of legs updated.
    """
    if not _poll_lock.acquire(blocking=False):
        return 0
    updated = 0
    try:
        now_ts = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        trades_col = db._db['algo_trades']
        pending_legs = _iter_pending_live_entry_orders(db)

        # Group by trade to share Kite instance
        trade_ids = list({str(doc.get('trade_id') or '') for doc in pending_legs if doc.get('trade_id')})
        trades: dict[str, dict] = {}
        if trade_ids:
            trades = {
                str(t.get('_id') or ''): t
                for t in trades_col.find({'_id': {'$in': trade_ids}, 'activation_mode': 'live'})
            }

        # Fetch kite orders once per unique broker
        broker_orders_cache: dict[str, list[dict]] = {}

        seen_order_ids: set[str] = set()
        for pending_doc in pending_legs:
            trade_id = str(pending_doc.get('trade_id') or '')
            leg_id   = str(pending_doc.get('leg_id') or '')
            trade    = trades.get(trade_id)
            if not trade:
                continue

            entry_trade = pending_doc.get('entry_trade') or {}
            order_id    = str(entry_trade.get('order_id') or '').strip()
            if not order_id or order_id in seen_order_ids:
                continue
            with _active_entry_lock:
                if order_id not in _active_entry_order_ids:
                    continue
            seen_order_ids.add(order_id)

            broker_id = str(trade.get('broker') or '').strip()
            cache_key = broker_id or '_default'

            if cache_key not in broker_orders_cache:
                kite = get_broker_for_trade(db, trade)
                if kite:
                    try:
                        broker_orders_cache[cache_key] = kite.orders() or []
                    except Exception as exc:
                        log.error('kite.orders() error broker=%s: %s', broker_id or 'default', exc)
                        notify_admin('broker_unreachable', f'kite.orders() failed broker={broker_id or "default"}: {exc}', {'broker': broker_id or 'default'})
                        broker_orders_cache[cache_key] = []
                else:
                    broker_orders_cache[cache_key] = []

            orders_list = broker_orders_cache.get(cache_key) or []
            kite_order  = next((o for o in orders_list if str(o.get('order_id') or '') == order_id), None)

            if not kite_order:
                print(
                    f'[ORDER POLL] order_id={order_id} trade={trade_id} leg={leg_id} '
                    f'NOT FOUND in broker order book (total_orders={len(orders_list)})'
                )
                continue

            status     = str(kite_order.get('status') or '').upper()
            fill_price = _safe_float(kite_order.get('average_price') or kite_order.get('price'))  # actual broker fill price
            fill_qty   = int(kite_order.get('filled_quantity') or 0)

            # ── Debug: print raw broker response for every pending order ─────
            print(
                f'[ORDER POLL SYNC] order_id={order_id} trade={trade_id} leg={leg_id} '
                f'broker_status={status} avg_price={fill_price} '
                f'filled_qty={fill_qty} '
                f'raw_avgprc={kite_order.get("average_price")} '
                f'raw_price={kite_order.get("price")} '
                f'raw_status={kite_order.get("status")}'
            )

            if status == _ORDER_STATUS_TRIGGER_PENDING:
                # SL order waiting for trigger — do not modify or convert
                continue

            if status == _ORDER_STATUS_COMPLETE and fill_price > 0:
                if process_broker_order_update(
                    db, order_id, _ORDER_STATUS_COMPLETE,
                    fill_price=fill_price, fill_qty=fill_qty, source='poll',
                ):
                    updated += 1

            elif status in (_ORDER_STATUS_REJECTED, _ORDER_STATUS_CANCELLED):
                status_msg = str(
                    kite_order.get('status_message') or kite_order.get('status_message_raw') or ''
                ).lower()
                process_broker_order_update(
                    db, order_id, status,
                    rejection_reason=status_msg, source='poll',
                )
                # MarginAutoSquareOff: margin rejection → exit all open legs
                is_margin_error = any(
                    kw in status_msg for kw in ('margin', 'insufficient', 'rms')
                )
                if is_margin_error:
                    exec_base = trade.get('execution_config_base') or {}
                    if bool(exec_base.get('MarginAutoSquareOff', False)):
                        kite_sq = get_broker_for_trade(db, trade)
                        if kite_sq:
                            print(
                                f'[MARGIN AUTO SQUAREOFF] trade={trade_id} '
                                f'leg={leg_id} triggering full exit'
                            )
                            _margin_squareoff_trade(db, trade, kite_sq, now_ts)

            else:
                # Still pending — check convert_after and ContinuousMonitoring
                placed_at     = str(entry_trade.get('order_placed_at') or '').strip()
                _raw_ca = entry_trade.get('convert_after')
                convert_after = int(_raw_ca) if _raw_ca is not None and str(_raw_ca) != '' else 40
                if not placed_at:
                    continue
                try:
                    placed_dt = datetime.fromisoformat(placed_at)
                    total_elapsed = (datetime.now() - placed_dt).total_seconds()

                    if convert_after > 0 and total_elapsed >= convert_after:
                        # Final retry — convert_after timeout exceeded
                        kite = get_broker_for_trade(db, trade)
                        if kite:
                            _convert_to_aggressive_limit(
                                db, kite, trade, pending_doc, order_id, kite_order, now_ts
                            )
                    else:
                        # ContinuousMonitoring — re-place at ModificationFrequency intervals
                        continuous, mod_freq = _get_leg_modification_config(trade, leg_id)
                        if continuous and mod_freq > 0:
                            last_mod = str(
                                entry_trade.get('last_modified_at') or placed_at
                            ).strip()
                            last_mod_dt      = datetime.fromisoformat(last_mod)
                            elapsed_since_mod = (datetime.now() - last_mod_dt).total_seconds()
                            if elapsed_since_mod >= mod_freq:
                                kite = get_broker_for_trade(db, trade)
                                if kite:
                                    _convert_to_aggressive_limit(
                                        db, kite, trade, pending_doc, order_id, kite_order, now_ts
                                    )
                except Exception as exc:
                    log.debug('order modification check error: %s', exc)

        open_exit_orders = _iter_open_live_exit_orders(db)
        seen_exit_ids: set[str] = set()
        for exit_doc in open_exit_orders:
            order_id = str(exit_doc.get('order_id') or '').strip()
            trade_id = str(exit_doc.get('trade_id') or '').strip()
            leg_id = str(exit_doc.get('leg_id') or '').strip()
            if not order_id or order_id in seen_exit_ids:
                continue
            with _active_exit_lock:
                if order_id not in _active_exit_order_ids:
                    continue
            seen_exit_ids.add(order_id)
            trade = trades.get(trade_id)
            if not trade:
                trade = trades_col.find_one({'_id': trade_id, 'activation_mode': 'live'}) or {}
                if trade:
                    trades[trade_id] = trade
            if not trade:
                continue

            broker_id = str(trade.get('broker') or '').strip()
            cache_key = f'exit::{broker_id or "_default"}'
            if cache_key not in broker_orders_cache:
                kite = get_broker_for_trade(db, trade)
                if kite:
                    try:
                        broker_orders_cache[cache_key] = kite.orders() or []
                    except Exception as exc:
                        log.error('kite.orders() exit error broker=%s: %s', broker_id or 'default', exc)
                        notify_admin('broker_unreachable', f'kite.orders() (exit poll) failed broker={broker_id or "default"}: {exc}', {'broker': broker_id or 'default'})
                        broker_orders_cache[cache_key] = []
                else:
                    broker_orders_cache[cache_key] = []
            orders_list = broker_orders_cache.get(cache_key) or []
            kite_order = next((o for o in orders_list if str(o.get('order_id') or '') == order_id), None)
            if not kite_order:
                continue

            status = str(kite_order.get('status') or '').upper()
            fill_price = _safe_float(kite_order.get('average_price') or kite_order.get('price'))
            fill_qty = int(kite_order.get('filled_quantity') or 0)

            if status == _ORDER_STATUS_COMPLETE and fill_price > 0:
                _update_broker_order_status(db, order_id, _ORDER_STATUS_COMPLETE, fill_price, fill_qty)
                _sync_live_exit_fill(
                    db,
                    trade_id,
                    leg_id,
                    str(exit_doc.get('exit_reason') or 'target').strip() or 'target',
                    fill_price,
                )
                updated += 1
            elif status in (_ORDER_STATUS_REJECTED, _ORDER_STATUS_CANCELLED):
                status_msg = str(
                    kite_order.get('status_message') or kite_order.get('status_message_raw') or ''
                ).lower()
                _update_broker_order_status(db, order_id, status, rejection_reason=status_msg)

    except Exception as exc:
        log.error('[ORDER POLL ERROR] %s', exc, exc_info=True)
    finally:
        _poll_lock.release()
    return updated


def _get_leg_product(trade: dict, leg_id: str) -> str:
    """Read NRML/MIS for a leg from execution_config_extra by matching leg index."""
    strategy_cfg  = trade.get('strategy') or {}
    leg_list      = list(strategy_cfg.get('ListOfLegConfigs') or [])
    exec_extra    = trade.get('execution_config_extra') or {}
    leg_exec_cfgs = exec_extra.get('ListOfLegExecutionConfig') or []
    for idx, base_leg in enumerate(leg_list):
        if not isinstance(base_leg, dict):
            continue
        if str(base_leg.get('id') or '') == leg_id and idx < len(leg_exec_cfgs):
            lec = leg_exec_cfgs[idx]
            if isinstance(lec, dict):
                product_raw = str(lec.get('ProductType') or _NRML).upper()
                return _MIS if 'MIS' in product_raw else _NRML
    return _NRML


def _get_aggressive_exit_price(broker, exchange: str, symbol: str, txn_type: str) -> float:
    """
    Get aggressive LIMIT price for immediate exit.
    SELL exit → use bid price (bp1); BUY exit → use ask price (sp1).
    Falls back to last_price with ±2% buffer if bid/ask unavailable.
    Used because MARKET orders are blocked for options in FlatTrade.
    """
    try:
        sym_key = f"{exchange}:{symbol}"
        quotes  = broker.quote([sym_key])
        q       = quotes.get(sym_key) or {}
        depth   = q.get('depth') or {}
        lp      = float(q.get('last_price') or 0)
        uc      = float(q.get('upper_circuit') or 0)
        lc      = float(q.get('lower_circuit') or 0)
        if txn_type == _TXN_SELL:
            bid = float(((depth.get('buy') or [{}])[0]).get('price') or 0)
            if bid > 0:
                return round(bid, 2)
            if lp > 0:
                return round(lp * 0.95, 2)
            return round(lc, 2) if lc > 0 else 0.05
        else:
            ask = float(((depth.get('sell') or [{}])[0]).get('price') or 0)
            if ask > 0:
                return round(ask, 2)
            if lp > 0:
                return round(min(lp * 1.05, uc) if uc > 0 else lp * 1.05, 2)
            return round(uc, 2) if uc > 0 else 0.05
    except Exception:
        return 0.05 if txn_type == _TXN_SELL else 9999.0


def live_manual_square_off_trade(db, trade: dict) -> dict:
    """
    Manual square-off for live mode:
      1. Cancel all pending ENTRY orders (order_open status)
      2. Cancel all open EXIT orders (SL + Target broker orders)
      3. Place aggressive LIMIT exit orders for all filled open positions

    Called from _square_off_trade_like_manual when activation_mode='live'.
    Returns summary dict.
    """
    trade_id  = str(trade.get('_id') or '').strip()
    if not trade_id:
        return {}

    now_ts   = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    hist_col = db._db['algo_trade_positions_history']
    broker   = get_broker_for_trade(db, trade)

    cancelled_entry  = 0
    cancelled_exit   = 0
    exit_orders_placed = 0

    # ── 1. Cancel pending ENTRY orders (order_open) ───────────────────────────
    pending_entry_docs = list(hist_col.find({
        'trade_id': trade_id,
        'exit_trade': None,
        'entry_trade.entry_lifecycle_status': 'order_open',
        'entry_trade.order_id': {'$nin': ['', None]},
    }, {'leg_id': 1, 'entry_trade': 1}))

    for doc in pending_entry_docs:
        order_id = str((doc.get('entry_trade') or {}).get('order_id') or '').strip()
        leg_id   = str(doc.get('leg_id') or '').strip()
        if not order_id:
            continue
        try:
            if broker and _is_live_order_punch_enabled():
                _remote_cancel_broker_order(trade.get('broker'), variety=_VARIETY_REGULAR, order_id=order_id)
            _update_broker_order_status(db, order_id, _ORDER_STATUS_CANCELLED,
                                        rejection_reason='manual_square_off')
            hist_col.update_one(
                {'_id': doc['_id']},
                {'$set': {'entry_trade.entry_lifecycle_status': 'entry_failed',
                           'entry_trade.rejection_reason': 'manual_square_off'}},
            )
            cancelled_entry += 1
            print(f'[MANUAL SQ] cancelled entry order trade={trade_id} leg={leg_id} order={order_id}')
        except Exception as exc:
            log.warning('[MANUAL SQ] cancel entry order failed trade=%s leg=%s: %s', trade_id, leg_id, exc)

    # ── 2. Cancel all open EXIT orders (SL + Target) per leg ─────────────────
    active_hist_docs = list(hist_col.find({
        'trade_id': trade_id,
        'status': 1,
        'exit_trade': None,
        'entry_trade.entry_lifecycle_status': 'active',
    }, {'leg_id': 1, 'symbol': 1, 'quantity': 1, 'lot_size': 1, 'position': 1}))

    for hist in active_hist_docs:
        leg_id = str(hist.get('leg_id') or '').strip()
        n = cancel_open_exit_orders_for_leg(
            db,
            trade,
            leg_id,
            cancel_reason='manual_square_off',
        )
        cancelled_exit += n

    # ── 3. Place aggressive LIMIT exit orders for filled open positions ───────
    for hist in active_hist_docs:
        symbol    = str(hist.get('symbol')   or '').strip()
        lot_size  = int(hist.get('lot_size') or 1)
        qty       = int(hist.get('quantity') or 0) * lot_size
        leg_id    = str(hist.get('leg_id')   or '').strip()
        if not symbol or qty <= 0 or not broker:
            continue
        exchange = _resolve_exchange(symbol, trade, {'id': leg_id})
        is_sell  = 'sell' in str(hist.get('position') or '').lower()
        txn_type = _TXN_BUY if is_sell else _TXN_SELL
        product  = _get_leg_product(trade, leg_id)
        exit_price = _get_aggressive_exit_price(broker, exchange, symbol, txn_type)
        if exit_price <= 0:
            log.warning('[MANUAL SQ] no price for %s leg=%s', symbol, leg_id)
            continue
        if _is_live_order_punch_enabled():
            result = _remote_place_broker_order(
                trade.get('broker'),
                tradingsymbol=symbol, exchange=exchange, transaction_type=txn_type,
                quantity=qty, order_type=_ORDER_TYPE_LIMIT, price=exit_price, product=product,
                variety=_VARIETY_REGULAR,
                context={'trade_id': trade_id, 'leg_id': leg_id, 'purpose': 'manual_squareoff', 'symbol': symbol},
            )
            if result['status'] != 'success':
                log.error('[MANUAL SQ] exit order failed trade=%s leg=%s symbol=%s: %s',
                          trade_id, leg_id, symbol, result['message'])
                continue
            order_id = result['order_id']
        else:
            order_id = _build_simulated_live_order_id(trade_id, leg_id, 'manual_sq')
        print(
            f'[MANUAL SQ] exit placed trade={trade_id} leg={leg_id} '
            f'symbol={symbol} txn={txn_type} qty={qty} price={exit_price} order={order_id}'
        )
        exit_orders_placed += 1

    summary = {
        'trade_id': trade_id,
        'cancelled_entry_orders': cancelled_entry,
        'cancelled_exit_orders': cancelled_exit,
        'exit_orders_placed': exit_orders_placed,
        'timestamp': now_ts,
    }
    print(f'[MANUAL SQ DONE] {summary}')
    return summary


def _rejection_squareoff_all(db, trade: dict, broker, _now_ts: str, rejected_leg_id: str) -> None:
    """
    Exit all filled (active) open legs when an entry is rejected — either because
    SquareOffAllLegs=True, or unconditionally when a sibling leg already entered
    (partial-entry failure). Always marks the strategy paused: legs got exited
    involuntarily here, so it shouldn't keep taking new entries until someone
    looks at it.
    """
    trade_id = str(trade.get('_id') or '')
    db._db['algo_trades'].update_one(
        {'_id': trade_id},
        {'$set': {
            'is_paused': True,
            'paused_reason': 'partial_entry_failure',
            'paused_at': _now_ts,
            'paused_context': {'rejected_leg_id': rejected_leg_id},
        }},
    )
    hist_col = db._db['algo_trade_positions_history']
    open_legs = list(hist_col.find(
        {
            'trade_id': trade_id,
            'status': 1,
            'exit_trade': None,
            'entry_trade.entry_lifecycle_status': 'active',
        },
        {'leg_id': 1, 'symbol': 1, 'quantity': 1, 'lot_size': 1, 'position': 1},
    ))
    for hist in open_legs:
        symbol   = str(hist.get('symbol') or '').strip()
        lot_size = int(hist.get('lot_size') or 1)
        qty      = int(hist.get('quantity') or 0) * lot_size
        leg_id   = str(hist.get('leg_id') or '').strip()
        if not symbol or qty <= 0:
            continue
        exchange = _resolve_exchange(symbol, trade, {'id': leg_id})
        is_sell  = 'sell' in str(hist.get('position') or '').lower()
        txn_type = _TXN_BUY if is_sell else _TXN_SELL
        product  = _get_leg_product(trade, leg_id)
        exit_price = _get_aggressive_exit_price(broker, exchange, symbol, txn_type)
        result = _remote_place_broker_order(
            trade.get('broker'),
            tradingsymbol=symbol, exchange=exchange, transaction_type=txn_type,
            quantity=qty, order_type=_ORDER_TYPE_LIMIT, price=exit_price, product=product,
            variety=_VARIETY_REGULAR,
            context={'trade_id': trade_id, 'leg_id': leg_id, 'purpose': 'rejection_squareoff', 'symbol': symbol},
        )
        if result['status'] != 'success':
            log.error('[SQUAREOFF ALL LEGS FAILED] trade=%s leg=%s symbol=%s: %s', trade_id, leg_id, symbol, result['message'])
            continue
        print(
            f'[SQUAREOFF ALL LEGS] trade={trade_id} rejected_leg={rejected_leg_id} '
            f'exiting leg={leg_id} symbol={symbol} txn={txn_type} qty={qty} '
            f'price={exit_price} order_id={result["order_id"]}'
        )


def _margin_squareoff_trade(db, trade: dict, kite, _now_ts: str) -> None:
    """Place aggressive LIMIT exit orders for all open legs when a margin rejection triggers full squareoff."""
    trade_id = str(trade.get('_id') or '')
    if str(trade.get('activation_mode') or '').strip() != 'live':
        log.warning('[MARGIN SQUAREOFF] skipped — not live mode trade=%s', trade_id)
        return
    hist_col = db._db['algo_trade_positions_history']
    open_legs = list(hist_col.find(
        {'trade_id': trade_id, 'status': 1, 'exit_trade': None},
        {'leg_id': 1, 'symbol': 1, 'quantity': 1, 'lot_size': 1, 'position': 1},
    ))
    for hist in open_legs:
        symbol   = str(hist.get('symbol') or '').strip()
        lot_size = int(hist.get('lot_size') or 1)
        qty      = int(hist.get('quantity') or 0) * lot_size
        leg_id   = str(hist.get('leg_id') or '').strip()
        exchange = _resolve_exchange(symbol, trade, {'id': leg_id})
        is_sell  = 'sell' in str(hist.get('position') or '').lower()
        txn_type = _TXN_BUY if is_sell else _TXN_SELL
        product  = _get_leg_product(trade, leg_id)
        if not symbol or qty <= 0:
            continue
        exit_price = _get_aggressive_exit_price(kite, exchange, symbol, txn_type)
        result = _remote_place_broker_order(
            trade.get('broker'),
            tradingsymbol=symbol, exchange=exchange, transaction_type=txn_type,
            quantity=qty, order_type=_ORDER_TYPE_LIMIT, price=exit_price, product=product,
            variety=_VARIETY_REGULAR,
            context={'trade_id': trade_id, 'leg_id': leg_id, 'purpose': 'margin_squareoff', 'symbol': symbol},
        )
        if result['status'] != 'success':
            log.error('[MARGIN SQUAREOFF FAILED] trade=%s symbol=%s: %s', trade_id, symbol, result['message'])
            continue
        print(
            f'[MARGIN SQUAREOFF] trade={trade_id} leg={leg_id} '
            f'exchange={exchange} symbol={symbol} txn={txn_type} qty={qty} '
            f'price={exit_price} product={product} order_id={result["order_id"]}'
        )


def _convert_to_aggressive_limit(
    db, kite, trade, hist_doc, order_id: str, kite_order: dict, now_ts: str
) -> None:
    """
    Cancel a stale pending limit order and re-place it as a fresh aggressive
    limit order using real-time bid/ask (3× normal protection buffer).
    Used instead of converting to MARKET (which many brokers block for options).
    """
    trade_id = str(trade.get('_id') or '')
    leg_id   = str(hist_doc.get('leg_id') or '')
    symbol   = str(kite_order.get('tradingsymbol') or '').strip()
    exchange = str(kite_order.get('exchange') or '').strip().upper()
    txn_type = str(kite_order.get('transaction_type') or '').upper()
    product  = str(kite_order.get('product') or _NRML).upper()
    qty      = int(kite_order.get('quantity') or 0)

    if not symbol or qty <= 0 or txn_type not in (_TXN_BUY, _TXN_SELL):
        log.warning('[AGGRESSIVE LIMIT] missing order info trade=%s leg=%s', trade_id, leg_id)
        return
    if not exchange:
        exchange = _resolve_exchange(symbol, trade, {'id': leg_id})

    is_buy_order = txn_type == _TXN_BUY

    # Step 1 — Cancel the stale pending order
    try:
        _remote_cancel_broker_order(trade.get('broker'), variety=_VARIETY_REGULAR, order_id=order_id)
        print(f'[AGGRESSIVE LIMIT] cancelled stale order trade={trade_id} leg={leg_id} order_id={order_id}')
    except Exception as exc:
        log.warning('[AGGRESSIVE LIMIT] cancel failed order_id=%s: %s — placing fresh order anyway', order_id, exc)

    # Step 2 — Fresh bid/ask via kite.quote()
    ltp = _safe_float(kite_order.get('last_price') or kite_order.get('average_price'))
    bid, ask = _get_bid_ask(kite, symbol, ltp, exchange)
    base_price = ask if is_buy_order else bid
    if base_price <= 0:
        # Unlike the MPP entry/exit paths, this is a stuck-order *recovery* retry — refusing
        # to place anything here would leave the leg with no order at all (worse than a
        # degraded price). Falls back to the order's own last known price on purpose, but
        # surfaced loudly since it's still "protection formula ran with no live depth".
        notify_admin(
            'mpp_price_unresolved',
            f'Aggressive-limit retry for {symbol} had no live bid/ask — using last known price {ltp} instead. '
            f'trade={trade_id} leg={leg_id} order_id={order_id}',
        )
        base_price = ltp

    # Use LimitBuffer from config (same buffer user configured for this leg)
    limit_buffer, buffer_type = _get_leg_entry_buffer(trade, leg_id)
    limit_price = _apply_buffer(base_price, limit_buffer, buffer_type, is_buy_order)

    # Step 3 — Place fresh aggressive limit order
    try:
        result = _remote_place_broker_order(
            trade.get('broker'),
            tradingsymbol=symbol, exchange=exchange, transaction_type=txn_type,
            quantity=qty, order_type=_ORDER_TYPE_LIMIT, price=limit_price, product=product,
            variety=_VARIETY_REGULAR,
            context={'trade_id': trade_id, 'leg_id': leg_id, 'purpose': 'aggressive_retry', 'symbol': symbol},
        )
        if result['status'] != 'success':
            log.error('[AGGRESSIVE LIMIT FAILED] trade=%s leg=%s symbol=%s: %s', trade_id, leg_id, symbol, result['message'])
            return
        new_order_id = result['order_id']

        # Update DB so poller tracks the new order_id + reset modification timer
        history_id = hist_doc.get('_id')
        if history_id is not None:
            _retry_set = {
                'entry_trade.order_id': new_order_id,
                'entry_trade.order_status': _ORDER_STATUS_OPEN,
                'entry_trade.order_placed_at': now_ts,
                'entry_trade.last_modified_at': now_ts,
                'entry_trade.aggressive_retry': True,
                'entry_trade.limit_price': limit_price,
                'entry_trade.price': limit_price,
            }
            print(f'[HIST_UPDATE][ORDER_RETRY] history_id={history_id} trade={trade_id} leg={leg_id} data={_retry_set}')
            db._db['algo_trade_positions_history'].update_one(
                {'_id': history_id},
                {'$set': _retry_set},
            )
            _sync_leg_entry_feature_from_positions_history(db, trade_id, leg_id)
        db._db['algo_trades'].update_one(
            {'_id': trade_id},
            {'$set': {
                'legs.$[elem].entry_trade.order_id':    new_order_id,
                'legs.$[elem].entry_trade.order_status': _ORDER_STATUS_OPEN,
                'legs.$[elem].entry_trade.order_placed_at': now_ts,
                'legs.$[elem].entry_trade.last_modified_at': now_ts,
                'legs.$[elem].entry_trade.aggressive_retry': True,
                'legs.$[elem].entry_trade.entry_lifecycle_status': 'order_open',
                'legs.$[elem].entry_trade.limit_price': limit_price,
                'legs.$[elem].entry_trade.price':       limit_price,
            }},
            array_filters=[{'elem.id': leg_id}],
        )
        print(
            f'[AGGRESSIVE LIMIT PLACED] trade={trade_id} leg={leg_id} '
            f'exchange={exchange} symbol={symbol} txn={txn_type} bid={bid} ask={ask} '
            f'limit_price={limit_price} new_order_id={new_order_id}'
        )
    except Exception as exc:
        log.error('[AGGRESSIVE LIMIT FAILED] trade=%s leg=%s symbol=%s: %s', trade_id, leg_id, symbol, exc)
