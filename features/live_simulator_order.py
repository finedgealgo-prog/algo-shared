"""
live_simulator_order.py
───────────────────────
Simulator order log for fast-forward mode.

When a trade has `live_sim_order: true` and runs in fast-forward mode,
every leg entry, SL order, target order, and trail-SL update is recorded
to the `live_simulator_order` MongoDB collection — without placing any
real broker orders (no access token required).

Document structure uses FlatTrade PlaceOrder field names so the data can
be replayed as real orders later without additional transformation.

Collection: live_simulator_order

Indexes recommended:
  { trade_id: 1, leg_id: 1, order_type: 1 }
  { trade_id: 1, leg_id: 1, status: 1 }
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

LIVE_SIM_ORDER_COLLECTION = 'live_simulator_order'


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')


# ── Condition check ───────────────────────────────────────────────────────────

def _parse_flag(flag) -> bool | None:
    if flag is None:
        return None
    if isinstance(flag, bool):
        return flag
    return str(flag).strip().lower() in {'1', 'true', 'yes', 'on'}


def is_simulator_order_enabled(trade: dict) -> bool:
    """
    Return True when:
      - activation_mode is 'fast-forward' or 'forward-test' (same simulated-order engine)
      - live_sim_order flag is truthy in trade / trade.config / trade.strategy

    If flag is missing from the trade dict (stripped by _serialize_trade_record),
    falls back to a direct DB lookup to avoid false negatives.
    """
    if str(trade.get('activation_mode') or '').strip() not in {'fast-forward', 'forward-test'}:
        return False

    # Check in-memory first (fast path)
    for src in (trade, trade.get('config') or {}, trade.get('strategy') or {}):
        parsed = _parse_flag(src.get('live_sim_order'))
        if parsed is not None:
            return parsed

    # Field missing from serialized dict — query DB directly
    trade_id = str(trade.get('_id') or '').strip()
    if not trade_id:
        return False
    try:
        from features.mongo_data import MongoData  # type: ignore
        db = MongoData()
        doc = db._db['algo_trades'].find_one({'_id': trade_id}, {'live_sim_order': 1})
        db.close()
        parsed = _parse_flag((doc or {}).get('live_sim_order'))
        return bool(parsed)
    except Exception:
        return False


# ── ID generator ─────────────────────────────────────────────────────────────

def _build_order_id(trade_id: str, leg_id: str, kind: str) -> str:
    ts = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')[:18]
    tid = str(trade_id or '')[-8:] or 'trade'
    lid = str(leg_id or '')[:12] or 'leg'
    return f'sim-{kind}-{tid}-{lid}-{ts}'


# ── Symbol / exchange helpers ─────────────────────────────────────────────────

def _resolve_exchange(symbol: str, ticker: str) -> str:
    text = f'{symbol} {ticker}'.upper()
    return 'BFO' if any(x in text for x in ('SENSEX', 'BANKEX', 'BFO', 'BSE')) else 'NFO'


def _resolve_ft_symbol(symbol: str, exchange: str) -> str:
    """Best-effort FlatTrade symbol; falls back to Kite symbol on error."""
    if not symbol:
        return symbol
    try:
        from features.broker_gateway import _active_broker  # type: ignore
        if _active_broker() != 'flattrade':
            return symbol
        from features.flattrade_broker import _to_flattrade_symbol  # type: ignore
        return _to_flattrade_symbol(symbol, exchange) or symbol
    except Exception:
        return symbol


def _build_display_symbol(ticker: str, expiry: str, strike: Any, option_type: str) -> str:
    """Construct a Kite-style symbol string from components."""
    if not (ticker and expiry and strike is not None and option_type):
        return ''
    try:
        exp_dt = datetime.strptime(expiry[:10], '%Y-%m-%d')
        exp_str = exp_dt.strftime('%y%b').upper()
        strike_int = int(float(str(strike)))
        return f'{ticker}{exp_str}{strike_int}{option_type}'
    except Exception:
        return f'{ticker}_{expiry[:10]}_{strike}_{option_type}'


# ── Field extractor ───────────────────────────────────────────────────────────

def _extract_leg_fields(leg: dict, trade: dict) -> dict:
    """
    Extract all leg-level fields needed to build an order document.
    Works with legs from both _store_position_history and apply_resolved_live_entries.
    """
    ticker = str(
        (trade.get('config') or {}).get('Ticker')
        or (trade.get('strategy') or {}).get('Ticker')
        or trade.get('ticker') or ''
    ).strip().upper()

    entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
    symbol = str(leg.get('symbol') or entry_trade.get('symbol') or '').strip()
    token = str(leg.get('token') or entry_trade.get('token') or '').strip()

    position = str(leg.get('position') or '').strip()
    is_sell_pos = 'sell' in position.lower()

    expiry_raw = str(leg.get('expiry_date') or entry_trade.get('expiry') or '').strip()
    expiry = expiry_raw[:10] if expiry_raw else ''

    strike = leg.get('strike') or entry_trade.get('strike')
    option_raw = str(leg.get('option') or entry_trade.get('option_type') or '').strip()
    option_type = option_raw.split('.')[-1].upper() if option_raw else ''

    quantity = _safe_int(leg.get('quantity') or entry_trade.get('quantity'))
    lot_size = _safe_int(leg.get('lot_size') or entry_trade.get('lot_size') or 1)

    entry_price = _safe_float(entry_trade.get('price') or entry_trade.get('trigger_price'))
    spot_price = _safe_float(entry_trade.get('spot_price') or entry_trade.get('underlying_at_trade'))
    entry_ts = str(
        entry_trade.get('traded_timestamp')
        or entry_trade.get('trigger_timestamp')
        or entry_trade.get('exchange_timestamp')
        or ''
    ).strip()

    exchange = _resolve_exchange(symbol, ticker)

    if not symbol and ticker and expiry and strike is not None and option_type:
        symbol = _build_display_symbol(ticker, expiry, strike, option_type)

    ft_symbol = _resolve_ft_symbol(symbol, exchange)

    return {
        'ticker': ticker,
        'token': token,
        'symbol': symbol,
        'ft_symbol': ft_symbol,
        'exchange': exchange,
        'is_sell_position': is_sell_pos,
        'expiry': expiry,
        'strike': strike,
        'option_type': option_type,
        'quantity': quantity,
        'lot_size': lot_size,
        'entry_price': entry_price,
        'spot_price': spot_price,
        'entry_ts': entry_ts,
    }


# ── Document builders ─────────────────────────────────────────────────────────

def _build_entry_doc(
    trade_id: str, leg_id: str, strategy_name: str,
    fields: dict, order_id: str, now: str, activation_mode: str = 'fast-forward',
) -> dict:
    qty = fields['quantity']
    entry_price = fields['entry_price']
    is_sell = fields['is_sell_position']
    exchange = fields['exchange']
    ft_symbol = fields['ft_symbol']
    trantype = 'S' if is_sell else 'B'
    return {
        'order_id': order_id,
        'parent_order_id': None,
        'trade_id': trade_id,
        'leg_id': leg_id,
        'strategy_name': strategy_name,
        'ticker': fields['ticker'],
        'order_type': 'ENTRY',
        'transaction_type': 'SELL' if is_sell else 'BUY',
        'exchange': exchange,
        'symbol': fields['symbol'],
        'ft_symbol': ft_symbol,
        'token': fields['token'],
        'strike': fields['strike'],
        'expiry': fields['expiry'],
        'option_type': fields['option_type'],
        'quantity': qty,
        'lot_size': fields['lot_size'],
        'price': entry_price,
        'trigger_price': 0.0,
        'order_price_type': 'LMT',
        'product': 'I',
        'status': 'COMPLETE',
        'placed_at': fields['entry_ts'] or now,
        'triggered_at': fields['entry_ts'] or now,
        'spot_price': fields['spot_price'],
        'activation_mode': activation_mode,
        'ft_request': {
            'exch': exchange,
            'tsym': ft_symbol,
            'qty': str(qty),
            'prc': str(round(entry_price, 2)),
            'trgprc': '0',
            'prd': 'I',
            'trantype': trantype,
            'prctyp': 'LMT',
            'ret': 'DAY',
            'ordersource': 'API',
        },
        'created_at': now,
        'updated_at': now,
    }


def _build_sl_doc(
    trade_id: str, leg_id: str, strategy_name: str,
    fields: dict, sl_price: float, entry_order_id: str,
    order_id: str, now: str, activation_mode: str = 'fast-forward',
) -> dict:
    qty = fields['quantity']
    exchange = fields['exchange']
    ft_symbol = fields['ft_symbol']
    is_sell = fields['is_sell_position']
    # Exit direction is opposite of entry
    trantype = 'B' if is_sell else 'S'
    return {
        'order_id': order_id,
        'parent_order_id': entry_order_id,
        'trade_id': trade_id,
        'leg_id': leg_id,
        'strategy_name': strategy_name,
        'ticker': fields['ticker'],
        'order_type': 'SL',
        'transaction_type': 'BUY' if is_sell else 'SELL',
        'exchange': exchange,
        'symbol': fields['symbol'],
        'ft_symbol': ft_symbol,
        'token': fields['token'],
        'strike': fields['strike'],
        'expiry': fields['expiry'],
        'option_type': fields['option_type'],
        'quantity': qty,
        'lot_size': fields['lot_size'],
        'price': sl_price,
        'trigger_price': sl_price,
        'order_price_type': 'SL-LMT',
        'product': 'I',
        'status': 'PENDING',
        'placed_at': now,
        'triggered_at': None,
        'spot_price': fields['spot_price'],
        'current_sl_price': sl_price,
        'initial_sl_price': sl_price,
        'sl_history': [],
        'activation_mode': activation_mode,
        'ft_request': {
            'exch': exchange,
            'tsym': ft_symbol,
            'qty': str(qty),
            'prc': str(round(sl_price, 2)),
            'trgprc': str(round(sl_price, 2)),
            'prd': 'I',
            'trantype': trantype,
            'prctyp': 'SL-LMT',
            'ret': 'DAY',
            'ordersource': 'API',
        },
        'created_at': now,
        'updated_at': now,
    }


def _build_target_doc(
    trade_id: str, leg_id: str, strategy_name: str,
    fields: dict, tp_price: float, entry_order_id: str,
    order_id: str, now: str, activation_mode: str = 'fast-forward',
) -> dict:
    qty = fields['quantity']
    exchange = fields['exchange']
    ft_symbol = fields['ft_symbol']
    is_sell = fields['is_sell_position']
    trantype = 'B' if is_sell else 'S'
    return {
        'order_id': order_id,
        'parent_order_id': entry_order_id,
        'trade_id': trade_id,
        'leg_id': leg_id,
        'strategy_name': strategy_name,
        'ticker': fields['ticker'],
        'order_type': 'TARGET',
        'transaction_type': 'BUY' if is_sell else 'SELL',
        'exchange': exchange,
        'symbol': fields['symbol'],
        'ft_symbol': ft_symbol,
        'token': fields['token'],
        'strike': fields['strike'],
        'expiry': fields['expiry'],
        'option_type': fields['option_type'],
        'quantity': qty,
        'lot_size': fields['lot_size'],
        'price': tp_price,
        'trigger_price': tp_price,
        'order_price_type': 'LMT',
        'product': 'I',
        'status': 'PENDING',
        'placed_at': now,
        'triggered_at': None,
        'spot_price': fields['spot_price'],
        'activation_mode': activation_mode,
        'ft_request': {
            'exch': exchange,
            'tsym': ft_symbol,
            'qty': str(qty),
            'prc': str(round(tp_price, 2)),
            'trgprc': '0',
            'prd': 'I',
            'trantype': trantype,
            'prctyp': 'LMT',
            'ret': 'DAY',
            'ordersource': 'API',
        },
        'created_at': now,
        'updated_at': now,
    }


# ── Public API ────────────────────────────────────────────────────────────────

def record_entry_with_orders(
    db,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
) -> str | None:
    """
    Insert ENTRY + SL + TARGET orders into live_simulator_order.

    Called from _store_position_history in execution_socket.py after a new
    position history record is successfully inserted.

    Returns entry order_id, or None if simulator is disabled or an error occurred.
    """
    if not is_simulator_order_enabled(trade):
        return None
    try:
        from features.position_manager import calc_sl_price, calc_tp_price  # type: ignore

        trade_id = str(trade.get('_id') or '').strip()
        leg_id = str(leg.get('id') or leg.get('leg_id') or '').strip()
        if not trade_id or not leg_id:
            return None

        # Skip if entry price is missing (pending / incomplete entry)
        entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
        entry_price = _safe_float(entry_trade.get('price') or entry_trade.get('trigger_price'))
        if not entry_price:
            return None

        col = db._db[LIVE_SIM_ORDER_COLLECTION]

        # Avoid duplicate entry orders for the same leg
        if col.find_one({'trade_id': trade_id, 'leg_id': leg_id, 'order_type': 'ENTRY'}, {'_id': 1}):
            return None

        strategy_name = str(trade.get('name') or '').strip()
        now = _now_iso()
        activation_mode = str(trade.get('activation_mode') or 'fast-forward').strip()

        fields = _extract_leg_fields(leg, trade)
        is_sell_pos = fields['is_sell_position']

        # ── Entry order ───────────────────────────────────────────────────────
        entry_order_id = _build_order_id(trade_id, leg_id, 'entry')
        entry_doc = _build_entry_doc(trade_id, leg_id, strategy_name, fields, entry_order_id, now, activation_mode)
        col.insert_one(entry_doc)
        print(
            f'[SIM ORDER ENTRY] trade={trade_id} leg={leg_id} '
            f'strike={fields["strike"]} option={fields["option_type"]} '
            f'price={entry_price} order_id={entry_order_id}'
        )

        # ── SL order ──────────────────────────────────────────────────────────
        sl_config = leg_cfg.get('LegStopLoss') or {}
        # Use stored SL price if already set, otherwise compute from config
        stored_sl = _safe_float(leg.get('current_sl_price') or leg.get('initial_sl_value')) or None
        sl_price = stored_sl or (calc_sl_price(entry_price, is_sell_pos, sl_config) if sl_config else None)
        if sl_price:
            sl_order_id = _build_order_id(trade_id, leg_id, 'sl')
            sl_doc = _build_sl_doc(
                trade_id, leg_id, strategy_name, fields,
                sl_price, entry_order_id, sl_order_id, now, activation_mode,
            )
            col.insert_one(sl_doc)
            print(f'[SIM ORDER SL] trade={trade_id} leg={leg_id} sl_price={sl_price} order_id={sl_order_id}')

        # ── Target order ──────────────────────────────────────────────────────
        tp_config = leg_cfg.get('LegTarget') or {}
        tp_price = calc_tp_price(entry_price, is_sell_pos, tp_config) if tp_config else None
        if tp_price:
            tp_order_id = _build_order_id(trade_id, leg_id, 'target')
            tp_doc = _build_target_doc(
                trade_id, leg_id, strategy_name, fields,
                tp_price, entry_order_id, tp_order_id, now, activation_mode,
            )
            col.insert_one(tp_doc)
            print(f'[SIM ORDER TARGET] trade={trade_id} leg={leg_id} tp_price={tp_price} order_id={tp_order_id}')

        return entry_order_id

    except Exception as exc:
        log.warning(
            '[SIM ORDER] record_entry_with_orders error trade=%s leg=%s: %s',
            trade.get('_id'), leg.get('id') or leg.get('leg_id'), exc,
        )
        return None


def update_trail_sl_order(
    db,
    trade_id: str,
    leg_id: str,
    new_sl_price: float,
    current_price: float,
    updated_at: str = '',
) -> bool:
    """
    When trail SL moves, update the SL order in live_simulator_order:
      - Push {sl_price, current_price, updated_at} to sl_history for audit
      - Update current_sl_price, price, trigger_price to new_sl_price
      - Sync ft_request.prc and ft_request.trgprc

    Called from _process_backtest_trade_tick in execution_socket.py after
    update_leg_sl_in_db confirms the new SL was persisted to algo_trades.
    """
    if not trade_id or not leg_id:
        return False
    try:
        col = db._db[LIVE_SIM_ORDER_COLLECTION]
        now = updated_at or _now_iso()
        result = col.update_one(
            {
                'trade_id': trade_id,
                'leg_id': leg_id,
                'order_type': 'SL',
                'status': {'$nin': ['TRIGGERED', 'CANCELLED', 'COMPLETE']},
            },
            {
                '$push': {
                    'sl_history': {
                        'sl_price': new_sl_price,
                        'current_option_price': current_price,
                        'updated_at': now,
                    },
                },
                '$set': {
                    'current_sl_price': new_sl_price,
                    'price': new_sl_price,
                    'trigger_price': new_sl_price,
                    'ft_request.prc': str(round(new_sl_price, 2)),
                    'ft_request.trgprc': str(round(new_sl_price, 2)),
                    'updated_at': now,
                },
            },
        )
        if result.modified_count:
            print(
                f'[SIM ORDER TRAIL SL] trade={trade_id} leg={leg_id} '
                f'new_sl={new_sl_price} ltp={current_price}'
            )
        return bool(result.modified_count)
    except Exception as exc:
        log.warning('[SIM ORDER] update_trail_sl_order error trade=%s leg=%s: %s', trade_id, leg_id, exc)
        return False


__all__ = [
    'LIVE_SIM_ORDER_COLLECTION',
    'is_simulator_order_enabled',
    'record_entry_with_orders',
    'update_trail_sl_order',
]
