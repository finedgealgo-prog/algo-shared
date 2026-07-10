"""
fast_forward_event.py
─────────────────────
Fast-forward-mode market-data adapter.

Job:
  - expose price-fetch helpers for fast-forward mode
  - fetch underlying spot / quote data
  - resolve option token + symbol + entry LTP for a pending leg

Important:
  - This file is data-only.
  - No SL / TP / trail / re-entry / recost / DB write logic belongs here.
  - All execution/event handling must remain in execution_socket.py /
    trading_core.py.
"""

from __future__ import annotations

import time
from typing import Any

from bson import ObjectId

from features.mongo_data import MongoData
from features.algo_backtest_event import (
    INDEX_SPOT_COLLECTION,
    OPEN_LEG_STATUS,
    OPTION_CHAIN_COLLECTION,
    get_chain_doc_at_time,
    get_chain_doc_by_token,
    get_latest_chain_doc,
    get_open_legs_ltp_array,
    get_option_ltp,
    get_spot_doc_at_time,
    get_spot_price,
)

QUOTE_INSTRUMENT_BY_UNDERLYING = {
    'NIFTY': 'NSE:NIFTY 50',
    'BANKNIFTY': 'NSE:NIFTY BANK',
    'FINNIFTY': 'NSE:NIFTY FIN SERVICE',
    'SENSEX': 'BSE:SENSEX',
    'MIDCPNIFTY': 'NSE:NIFTY MID SELECT',
}


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


def _normalize_expiry_key(value: Any) -> str:
    raw = str(value or '').strip()
    if not raw:
        return ''
    return raw[:10]


def _is_truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value or '').strip().lower()
    return normalized in {'1', 'true', 'yes', 'y', 'on'}


def should_use_fast_forward_quote(trade: dict) -> bool:
    if str(trade.get('activation_mode') or '').strip() not in ('fast-forward', 'forward-test'):
        return False
    if 'get_quote' in trade:
        return _is_truthy_flag(trade.get('get_quote'))
    strategy_cfg = trade.get('strategy') or {}
    config_cfg = trade.get('config') or {}
    if 'get_quote' in strategy_cfg:
        return _is_truthy_flag(strategy_cfg.get('get_quote'))
    return _is_truthy_flag(config_cfg.get('get_quote'))


def _build_kite_quote_instrument(symbol: str, underlying: str) -> str:
    normalized_symbol = str(symbol or '').strip()
    if not normalized_symbol:
        return ''
    if ':' in normalized_symbol:
        return normalized_symbol
    normalized_underlying = str(underlying or '').strip().upper()
    exchange = 'BFO' if normalized_underlying in {'SENSEX', 'BANKEX'} else 'NFO'
    return f'{exchange}:{normalized_symbol}'


def _get_socket_entry_ltp(token: str) -> float:
    normalized_token = str(token or '').strip()
    if not normalized_token:
        return 0.0
    try:
        from features.live_monitor_socket import _get_active_ticker_manager
        ticker_manager = _get_active_ticker_manager()
        return _safe_float(ticker_manager.get_ltp(normalized_token))
    except Exception:
        return 0.0


def _wait_for_socket_entry_ltp(token: str, timeout_seconds: float = 0.75) -> float:
    normalized_token = str(token or '').strip()
    if not normalized_token:
        return 0.0
    deadline = time.monotonic() + max(0.0, float(timeout_seconds or 0.0))
    while time.monotonic() < deadline:
        ltp = _get_socket_entry_ltp(normalized_token)
        if ltp > 0:
            return ltp
        time.sleep(0.05)
    return _get_socket_entry_ltp(normalized_token)


def _subscribe_option_token(token: str, symbol: str = '') -> None:
    normalized_token = str(token or '').strip()
    if not normalized_token:
        return
    try:
        from features.live_monitor_socket import _get_active_ticker_manager
        ticker_manager = _get_active_ticker_manager()

        if not ticker_manager._ticker or ticker_manager.status != 'running':
            return
        if normalized_token in getattr(ticker_manager, 'subscribed_tokens', set()):
            if symbol:
                ticker_manager.register_option_token(normalized_token, symbol)
            return
        # Strip exchange prefix if present: "NSE_54808" → 54808
        numeric_part = normalized_token.split('_', 1)[-1] if '_' in normalized_token else normalized_token
        subscribe_token = int(numeric_part)
        ticker_manager._ticker.subscribe([subscribe_token])
        ticker_manager._ticker.set_mode(ticker_manager._ticker.MODE_LTP, [subscribe_token])
        ticker_manager.register_option_token(normalized_token, symbol)
        print(f'[FF OPTION SUBSCRIBE] token={normalized_token}')
    except Exception:
        return


def sync_fast_forward_open_position_subscriptions(trade_date: str = '') -> int:
    db = MongoData()
    try:
        query: dict[str, Any] = {
            'activation_mode': {'$in': ['fast-forward', 'forward-test']},
            'active_on_server': True,
            'trade_status': 1,
            'status': 'StrategyStatus.Live_Running',
        }
        normalized_trade_date = str(trade_date or '').strip()
        if normalized_trade_date:
            query['creation_ts'] = {'$regex': f'^{normalized_trade_date}'}

        trades = list(db._db['algo_trades'].find(query, {'_id': 1, 'name': 1}))
        trade_ids = [str(item.get('_id') or '').strip() for item in trades if str(item.get('_id') or '').strip()]
        if not trade_ids:
            return 0

        subscribed = 0
        dirty_trade_ids: list[str] = []
        hist_col = db._db['algo_trade_positions_history']
        for row in hist_col.find(
            {
                'trade_id': {'$in': trade_ids},
                'status': 1,
                'exit_trade': None,
            },
            {
                'trade_id': 1,
                'token': 1,
                'symbol': 1,
                'leg_id': 1,
                'entry_trade': 1,
                'ticker': 1,
                'strike': 1,
                'expiry_date': 1,
                'option': 1,
            },
        ):
            _entry_trade = row.get('entry_trade') or {}
            token = str(row.get('token') or _entry_trade.get('instrument_token') or '').strip()
            symbol = str(row.get('symbol') or '').strip()

            # Non-numeric token (chain format) — resolve Kite integer token and patch DB
            if token and not token.isdigit():
                underlying = str(row.get('ticker') or '').strip().upper()
                expiry_raw = str(row.get('expiry_date') or '').strip()
                expiry = expiry_raw[:10] if expiry_raw else ''
                strike = row.get('strike')
                opt_raw = str(row.get('option') or '').strip()
                option_type = opt_raw.split('.')[-1].upper() if '.' in opt_raw else opt_raw.upper()
                if underlying and expiry and strike not in (None, '') and option_type:
                    try:
                        tok_doc = db._db['active_option_tokens'].find_one({
                            'instrument': underlying,
                            'expiry': expiry,
                            'strike': strike,
                            'option_type': option_type,
                        }) or {}
                        kite_tok = str(tok_doc.get('token') or tok_doc.get('tokens') or '').strip()
                        if kite_tok and kite_tok.isdigit():
                            new_sym = str(tok_doc.get('symbol') or symbol or kite_tok)
                            hist_col.update_one(
                                {'_id': row['_id']},
                                {'$set': {'token': kite_tok, 'symbol': new_sym}},
                            )
                            print(
                                f'[FF TOKEN PATCH] leg_id={row.get("leg_id")} '
                                f'chain={token} → kite={kite_tok} sym={new_sym}'
                            )
                            token = kite_tok
                            symbol = new_sym
                            tid = str(row.get('trade_id') or '').strip()
                            if tid and tid not in dirty_trade_ids:
                                dirty_trade_ids.append(tid)
                    except Exception:
                        pass

            if not token:
                continue
            _subscribe_option_token(token, symbol)
            subscribed += 1

        for _dtid in dirty_trade_ids:
            try:
                from features.execution_socket import mark_execute_order_dirty_from_trade_id
                mark_execute_order_dirty_from_trade_id(db, _dtid)
            except Exception:
                pass

        momentum_subscribed = 0
        for mrow in db._db['algo_leg_feature_status'].find(
            {
                'trade_id': {'$in': trade_ids},
                'feature': 'momentum_pending',
                'status': 'active',
                'token': {'$nin': [None, '']},
            },
            {'token': 1, 'symbol': 1, 'leg_id': 1},
        ):
            mtoken = str(mrow.get('token') or '').strip()
            if not mtoken:
                continue
            msymbol = str(mrow.get('symbol') or '').strip()
            _subscribe_option_token(mtoken, msymbol)
            momentum_subscribed += 1
            print(
                f'[FF MOMENTUM PENDING SUBSCRIBE] '
                f'leg_id={str(mrow.get("leg_id") or "-")} '
                f'token={mtoken}'
            )

        print(
            f'[FF OPEN POSITION SUBSCRIBE] trade_date={normalized_trade_date or "-"} '
            f'trades={len(trade_ids)} subscribed_tokens={subscribed} momentum_tokens={momentum_subscribed}'
        )
        return subscribed + momentum_subscribed
    except Exception:
        return 0
    finally:
        try:
            db.close()
        except Exception:
            pass


def _get_quote_access_token(db, trade: dict) -> str:
    broker_id = str(trade.get('broker') or '').strip()
    if broker_id:
        broker_doc = db._db['broker_configuration'].find_one(
            {'_id': ObjectId(broker_id)},
            {'access_token': 1},
        ) or {}
        access_token = str(broker_doc.get('access_token') or '').strip()
        if access_token:
            return access_token
    market_cfg = db._db['kite_market_config'].find_one({'enabled': True}, {'access_token': 1}) or {}
    return str(market_cfg.get('access_token') or '').strip()


def get_fast_forward_quote_spot_price(db, trade: dict, underlying: str) -> tuple[float, str]:
    normalized_underlying = str(underlying or '').strip().upper()
    instrument = QUOTE_INSTRUMENT_BY_UNDERLYING.get(normalized_underlying, '')
    if not instrument or not should_use_fast_forward_quote(trade):
        return 0.0, instrument
    try:
        from features.broker_gateway import _active_broker  # type: ignore
        if _active_broker() == 'dhan':
            return 0.0, instrument  # Dhan spot comes from WebSocket, not Kite quote
    except Exception:
        pass
    try:
        from features.broker_gateway import get_broker_rest_client_with_token as get_kite_instance  # type: ignore

        access_token = _get_quote_access_token(db, trade)
        if not access_token:
            return 0.0, instrument
        kite = get_kite_instance(access_token)
        quote_map = kite.quote([instrument]) or {}
        quote_doc = quote_map.get(instrument) or {}
        for key in ('last_price', 'last_trade_price'):
            price = _safe_float(quote_doc.get(key))
            if price > 0:
                return price, instrument
        ohlc = quote_doc.get('ohlc') if isinstance(quote_doc.get('ohlc'), dict) else {}
        for key in ('close', 'open'):
            price = _safe_float(ohlc.get(key))
            if price > 0:
                return price, instrument
    except Exception:
        return 0.0, instrument
    return 0.0, instrument


def resolve_fast_forward_pending_entry_snapshot(
    db,
    trade: dict,
    leg_cfg: dict,
    *,
    now_ts: str,
    fallback_spot_price: float = 0.0,
) -> dict:
    underlying = str(
        (trade.get('strategy') or {}).get('Ticker')
        or (trade.get('config') or {}).get('Ticker')
        or trade.get('ticker')
        or ''
    ).strip().upper()
    if not underlying:
        return {}

    spot_price, quote_instrument = get_fast_forward_quote_spot_price(db, trade, underlying)
    if spot_price <= 0:
        try:
            from features.live_monitor_socket import _get_active_ticker_manager
            spot_price = _safe_float(_get_active_ticker_manager().get_spot(underlying))
        except Exception:
            spot_price = 0.0
    if spot_price <= 0:
        spot_price = _safe_float(fallback_spot_price)
    # fallback to DB if ticker has no data (e.g. server restart)
    if spot_price <= 0:
        try:
            from features.execution_socket import get_index_spot_at_time
            _index_spot_col = db._db['option_chain_index_spot']
            _spot_doc = get_index_spot_at_time(_index_spot_col, underlying, now_ts)
            spot_price = _safe_float((_spot_doc or {}).get('spot_price'))
        except Exception:
            spot_price = 0.0
    if spot_price <= 0:
        return {}

    from features.backtest_engine import STRIKE_STEPS, _resolve_expiry, _resolve_strike
    from features.spot_atm_utils import resolve_atm_price

    contract_cfg = leg_cfg.get('ContractType') or {}
    option_raw = str(contract_cfg.get('Option') or leg_cfg.get('InstrumentKind') or '')
    option_type = option_raw.split('.')[-1] if '.' in option_raw else option_raw
    expiry_kind = str(contract_cfg.get('Expiry') or leg_cfg.get('ExpiryKind') or 'ExpiryType.Weekly')
    entry_kind  = str(contract_cfg.get('EntryType') or leg_cfg.get('EntryType') or leg_cfg.get('entry_kind') or '')
    atm_price   = resolve_atm_price(underlying, spot_price) if spot_price > 0 else 0

    # Strike is always resolved at entry time via live option chain (fetch_full_chain +
    # select_strike_live). The snapshot only collects spot/atm — no DB lookup needed.
    strike = None
    expiry = None
    token  = ''
    symbol = ''
    ltp    = 0.0
    ltp_source = 'token_not_found'


    spot_source = 'quote_api' if should_use_fast_forward_quote(trade) else 'socket'
    print(
        '[FF SPOT SNAPSHOT] '
        f'mode={trade.get("activation_mode") or "fast-forward"} '
        f'ticker={underlying} '
        f'quote_enabled={should_use_fast_forward_quote(trade)} '
        f'source={spot_source} '
        f'instrument={quote_instrument or "SOCKET"} '
        f'spot_price={spot_price} '
        f'atm_price={atm_price}'
    )
    print(
        '[FF ENTRY SNAPSHOT] '
        f'trade={str(trade.get("_id") or "")} '
        f'leg={str(leg_cfg.get("id") or "")} '
        f'underlying={underlying} '
        f'spot_price={spot_price} '
        f'atm_price={atm_price} '
        f'strike={strike if strike not in (None, "") else "NOT_FOUND"} '
        f'option={option_type or "-"} '
        f'expiry={expiry or "NOT_FOUND"} '
        f'token={token or "NOT_FOUND"} '
        f'symbol={symbol or "-"} '
        f'ltp={ltp} '
        f'ltp_source={ltp_source} '
        f'entry_kind={entry_kind}'
    )
    return {
        'spot_at_queue': spot_price,
        'live_spot_price': spot_price,
        'atm_price': atm_price,
        'strike': strike,
        'expiry_date': expiry,
        'token': token,
        'symbol': symbol,
        'ltp': ltp,
        'quote_instrument': quote_instrument,
    }


def _get_fast_forward_quote_price(db, trade: dict, symbol: str) -> float:
    try:
        from features.broker_gateway import _active_broker  # type: ignore
        if _active_broker() == 'dhan':
            return 0.0  # Dhan quote via socket/REST — handled by _wait_for_socket_entry_ltp
    except Exception:
        pass

    instrument = _build_kite_quote_instrument(
        symbol,
        str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or ''),
    )
    if not instrument:
        return 0.0
    try:
        from features.broker_gateway import get_broker_rest_client_with_token as get_kite_instance  # type: ignore

        broker_id = str(trade.get('broker') or '').strip()
        access_token = ''
        if broker_id:
            broker_doc = db._db['broker_configuration'].find_one(
                {'_id': ObjectId(broker_id)},
                {'access_token': 1},
            )
            access_token = str((broker_doc or {}).get('access_token') or '').strip()
        if not access_token:
            market_cfg = db._db['kite_market_config'].find_one({'enabled': True}, {'access_token': 1})
            access_token = str((market_cfg or {}).get('access_token') or '').strip()
        if not access_token:
            return 0.0

        kite = get_kite_instance(access_token)
        quote_map = kite.quote([instrument]) or {}
        quote_doc = quote_map.get(instrument) or {}
        for key in ('last_price', 'last_trade_price'):
            price = _safe_float(quote_doc.get(key))
            if price > 0:
                return price
        ohlc = quote_doc.get('ohlc') if isinstance(quote_doc.get('ohlc'), dict) else {}
        for key in ('close', 'open'):
            price = _safe_float(ohlc.get(key))
            if price > 0:
                return price
    except Exception:
        return 0.0
    return 0.0


def _extract_chain_price(chain_doc: dict | None) -> float:
    doc = chain_doc if isinstance(chain_doc, dict) else {}
    for key in ('close', 'last_price', 'ltp', 'price', 'open'):
        price = _safe_float(doc.get(key))
        if price > 0:
            return price
    return 0.0


def _get_fast_forward_chain_price(
    db,
    *,
    token: str = '',
    underlying: str = '',
    expiry: str = '',
    strike: Any = None,
    option_type: str = '',
    now_ts: str = '',
) -> float:
    chain_col = db._db[OPTION_CHAIN_COLLECTION]
    normalized_token = str(token or '').strip()
    if normalized_token:
        try:
            return _extract_chain_price(
                get_chain_doc_by_token(chain_col, normalized_token, now_ts, activation_mode='fast-forward')
            )
        except Exception:
            pass
    if underlying and expiry and strike not in (None, '') and option_type:
        try:
            return _extract_chain_price(
                get_chain_doc_at_time(
                    chain_col,
                    underlying,
                    expiry,
                    strike,
                    option_type,
                    now_ts,
                    activation_mode='fast-forward',
                )
            )
        except Exception:
            pass
    return 0.0


def resolve_fast_forward_entry_price(
    db,
    trade: dict,
    token: str,
    symbol: str,
    fallback_price: float,
) -> tuple[float, str]:
    if str(trade.get('activation_mode') or '').strip() not in ('fast-forward', 'forward-test'):
        return fallback_price, 'default'

    quote_price = _get_fast_forward_quote_price(db, trade, symbol)
    if quote_price > 0:
        return quote_price, 'quote'

    # Quote returned 0 — try Kite WebSocket LTP (token already subscribed above)
    socket_price = _wait_for_socket_entry_ltp(token, timeout_seconds=0.75)
    if socket_price > 0:
        return socket_price, 'socket'

    return fallback_price, 'chain_fallback'


def resolve_fast_forward_entry_execution_payload(
    db,
    trade: dict,
    leg: dict,
    *,
    now_ts: str,
) -> dict:
    underlying = str(
        (trade.get('strategy') or {}).get('Ticker')
        or (trade.get('config') or {}).get('Ticker')
        or trade.get('ticker') or ''
    ).strip().upper()

    leg_token = str(leg.get('token') or '').strip()
    leg_strike = leg.get('strike')
    leg_expiry = str(leg.get('expiry_date') or '').strip()
    if ' ' in leg_expiry:
        leg_expiry = leg_expiry[:10]
    leg_symbol = str(leg.get('symbol') or '').strip()

    # Fast path: contract already fully resolved → skip DB scan and resolve
    # entry price from quote first, then historical fast-forward chain fallback.
    # Accept both plain numeric ("54808") and exchange-prefixed ("NSE_54808") tokens.
    _leg_numeric = leg_token.split('_', 1)[-1] if '_' in leg_token else leg_token
    if underlying and leg_token and _leg_numeric.isdigit() and leg_strike not in (None, '') and leg_expiry:
        try:
            from features.live_monitor_socket import _get_active_ticker_manager
            _tm_ff = _get_active_ticker_manager()
            spot_price = _safe_float(_tm_ff.get_spot(underlying))
        except Exception:
            spot_price = 0.0
        # Socket may not have spot yet — use the price captured at queue time
        if spot_price <= 0:
            spot_price = _safe_float(leg.get('spot_at_queue') or leg.get('spot_price') or leg.get('live_spot_price'))
        _subscribe_option_token(leg_token, leg_symbol)
        ltp = _get_fast_forward_chain_price(
            db,
            token=leg_token,
            underlying=underlying,
            expiry=leg_expiry,
            strike=leg_strike,
            option_type=str(leg.get('option') or '').split('.')[-1].upper(),
            now_ts=now_ts,
        )
        entry_price, price_source = resolve_fast_forward_entry_price(db, trade, leg_token, leg_symbol, ltp)
        return {
            'spot_price': spot_price,
            'strike': leg_strike,
            'expiry_date': leg_expiry,
            'token': leg_token,
            'symbol': leg_symbol,
            'entry_price': entry_price,
            'current_option_price': entry_price if entry_price > 0 else ltp,
            'entry_price_source': price_source,
            'ltp': ltp,
            'atm_price': 0,
        }

    contract_cfg = {
        'id': str(leg.get('id') or ''),
        'ContractType': {
            'Option': str(leg.get('option') or leg.get('InstrumentKind') or ''),
            'Expiry': str(leg.get('expiry_kind') or leg.get('ExpiryKind') or 'ExpiryType.Weekly'),
            'StrikeParameter': str(leg.get('strike_parameter') or leg.get('StrikeParameter') or 'StrikeType.ATM'),
            'EntryType': str(leg.get('entry_kind') or leg.get('EntryType') or ''),
        },
        'InstrumentKind': str(leg.get('option') or leg.get('InstrumentKind') or ''),
        'ExpiryKind': str(leg.get('expiry_kind') or leg.get('ExpiryKind') or 'ExpiryType.Weekly'),
        'StrikeParameter': str(leg.get('strike_parameter') or leg.get('StrikeParameter') or 'StrikeType.ATM'),
        'entry_kind': str(leg.get('entry_kind') or leg.get('EntryType') or ''),
    }
    leg_spot_fallback = _safe_float(leg.get('spot_at_queue') or leg.get('spot_price') or 0)
    snapshot = resolve_fast_forward_pending_entry_snapshot(
        db,
        trade,
        contract_cfg,
        now_ts=now_ts,
        fallback_spot_price=leg_spot_fallback,
    ) or {}

    spot_price = _safe_float(
        snapshot.get('spot_at_queue')
        or snapshot.get('live_spot_price')
        or leg.get('spot_at_queue')
    )
    strike = snapshot.get('strike')
    if strike in (None, ''):
        strike = leg.get('strike')
    expiry = str(snapshot.get('expiry_date') or leg.get('expiry_date') or '').strip()
    if ' ' in expiry:
        expiry = expiry[:10]
    token = str(snapshot.get('token') or leg.get('token') or '').strip()
    symbol = str(snapshot.get('symbol') or leg.get('symbol') or '').strip()
    ltp_fallback = _safe_float(snapshot.get('ltp'))
    entry_price, price_source = resolve_fast_forward_entry_price(
        db,
        trade,
        token,
        symbol,
        ltp_fallback,
    )
    current_option_price = entry_price if entry_price > 0 else ltp_fallback
    return {
        'spot_price': spot_price,
        'strike': strike,
        'expiry_date': expiry,
        'token': token,
        'symbol': symbol,
        'entry_price': entry_price,
        'current_option_price': current_option_price,
        'entry_price_source': price_source,
        'ltp': ltp_fallback,
        'atm_price': snapshot.get('atm_price'),
    }


__all__ = [
    'OPTION_CHAIN_COLLECTION',
    'INDEX_SPOT_COLLECTION',
    'OPEN_LEG_STATUS',
    'get_latest_chain_doc',
    'get_chain_doc_at_time',
    'get_chain_doc_by_token',
    'get_spot_doc_at_time',
    'get_spot_price',
    'get_option_ltp',
    'get_open_legs_ltp_array',
    'should_use_fast_forward_quote',
    'get_fast_forward_quote_spot_price',
    '_get_fast_forward_quote_price',
    'resolve_fast_forward_pending_entry_snapshot',
    'resolve_fast_forward_entry_price',
    'resolve_fast_forward_entry_execution_payload',
    'sync_fast_forward_open_position_subscriptions',
]
