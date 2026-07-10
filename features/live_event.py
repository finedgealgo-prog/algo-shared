"""
live_event.py
─────────────
Live-mode market-data adapter.

Job: expose only market-data fetch helpers for live mode with the same public
contract as algo_backtest_event.py / fast_forward_event.py.

Important:
  - This file is data-only.
  - No execution logic, SL/TP logic, re-entry logic, or DB write logic belongs here.
  - execution_socket.py remains the shared execution/action layer.

Today, the live adapter reuses the common helper implementations so the mode
boundary is stable even while live entry prices come from Kite socket flow in
live_monitor_socket.py.
"""

from __future__ import annotations

from typing import Any
from bson import ObjectId

from features.mongo_data import MongoData
from features.debug_flags import entry_print
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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


_SPOT_TOKEN_BY_UNDERLYING = {
    'NIFTY': '256265',
    'BANKNIFTY': '260105',
    'FINNIFTY': '257801',
    'SENSEX': '265',
    'MIDCPNIFTY': '288009',
}
_LIVE_KITE_OWNER = '__live_event__'


def _get_live_ltp_map() -> dict[str, float]:
    try:
        from features.broker_gateway import get_broker_ltp_map  # type: ignore
        return dict(get_broker_ltp_map() or {})
    except Exception as exc:
        from features.telegram_notifier import notify_admin
        notify_admin('ltp_fetch_error', f'_get_live_ltp_map failed: {exc}')
        return {}


def _get_live_spot_price(underlying: str) -> float:
    normalized_underlying = str(underlying or '').strip().upper()
    if not normalized_underlying:
        return 0.0
    token = _SPOT_TOKEN_BY_UNDERLYING.get(normalized_underlying, '')
    if not token:
        return 0.0
    return _safe_float(_get_live_ltp_map().get(token))


def resolve_kite_token_for_symbol(kite_symbol: str) -> str:
    """
    Given a Kite-format tradingsymbol (e.g. NIFTY2651923700PE),
    return the Kite integer instrument_token as a string.
    Falls back to '' if not found.
    Used to ensure 'token' field stores Kite token (for WebSocket LTP),
    not FlatTrade/other broker token.
    """
    sym = str(kite_symbol or '').strip()
    if not sym:
        return ''
    try:
        from features.broker_gateway import load_broker_instruments  # type: ignore
        cache = load_broker_instruments()
        for (_name, _exp, _strike, _opt), inst in cache.items():
            if str(inst.get('symbol') or '').strip() == sym:
                tok = inst.get('token')
                if tok:
                    return str(int(tok))
    except Exception:
        pass
    return ''


def _subscribe_live_option_token(token: str, symbol: str = '') -> None:
    normalized_token = str(token or '').strip()
    if not normalized_token:
        return
    # Strip exchange prefix if present: "NSE_54808" → "54808"
    numeric_part = normalized_token.split('_', 1)[-1] if '_' in normalized_token else normalized_token
    if not numeric_part.isdigit():
        return
    try:
        from features.live_monitor_socket import _get_active_ticker_manager
        ticker_manager = _get_active_ticker_manager()
        if not ticker_manager._ticker or ticker_manager.status != 'running':
            print(
                f'[LIVE SUBSCRIBE SKIPPED] ticker not running — '
                f'token={normalized_token} symbol={symbol or "-"}'
            )
            return
        if normalized_token in getattr(ticker_manager, 'subscribed_tokens', set()):
            if symbol:
                ticker_manager.register_option_token(normalized_token, symbol)
            return
        subscribe_token = int(numeric_part)
        ticker_manager._ticker.subscribe([subscribe_token])
        ticker_manager._ticker.set_mode(ticker_manager._ticker.MODE_LTP, [subscribe_token])
        ticker_manager.register_option_token(normalized_token, symbol)
        print(f'[LIVE OPTION SUBSCRIBE] token={normalized_token} symbol={symbol or "-"}')
    except Exception:
        return


def _subscribe_mode_option_token(activation_mode: str, token: str, symbol: str = '') -> None:
    normalized_mode = str(activation_mode or '').strip().lower()
    if normalized_mode in ('fast-forward', 'forward-test'):
        try:
            from features.fast_forward_event import _subscribe_option_token
            _subscribe_option_token(token, symbol)
            return
        except Exception:
            return
    _subscribe_live_option_token(token, symbol)


def _build_kite_quote_instrument(symbol: str, underlying: str) -> str:
    normalized_symbol = str(symbol or '').strip()
    if not normalized_symbol:
        return ''
    if ':' in normalized_symbol:
        return normalized_symbol
    normalized_underlying = str(underlying or '').strip().upper()
    exchange = 'BFO' if normalized_underlying in {'SENSEX', 'BANKEX'} else 'NFO'
    return f'{exchange}:{normalized_symbol}'


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


def get_live_option_quote_price(db, trade: dict, symbol: str) -> float:
    instrument = _build_kite_quote_instrument(
        symbol,
        str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or ''),
    )
    if not instrument:
        return 0.0
    try:
        from features.broker_gateway import get_broker_rest_client_with_token as get_kite_instance  # type: ignore

        access_token = _get_quote_access_token(db, trade)
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


def sync_live_open_position_subscriptions(trade_date: str = '') -> int:
    db = MongoData()
    try:
        query: dict[str, Any] = {
            'activation_mode': {'$in': ['live', 'fast-forward', 'forward-test']},
            'active_on_server': True,
            'trade_status': 1,
            'status': 'StrategyStatus.Live_Running',
        }
        normalized_trade_date = str(trade_date or '').strip()
        if normalized_trade_date:
            query['creation_ts'] = {'$regex': f'^{normalized_trade_date}'}

        trades = list(db._db['algo_trades'].find(query, {'_id': 1, 'name': 1, 'activation_mode': 1}))
        trade_ids = [str(item.get('_id') or '').strip() for item in trades if str(item.get('_id') or '').strip()]
        if not trade_ids:
            return 0
        trade_mode_by_id = {
            str(item.get('_id') or '').strip(): str(item.get('activation_mode') or '').strip().lower()
            for item in trades
            if str(item.get('_id') or '').strip()
        }

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
                        from features.market_feed_tokens import active_token_broker_filter as _atbf  # type: ignore
                        tok_doc = db._db['active_option_tokens'].find_one({
                            **_atbf(db),
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
                            entry_print(
                                f'[LIVE TOKEN PATCH] leg_id={row.get("leg_id")} '
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
            trade_mode = trade_mode_by_id.get(str(row.get('trade_id') or '').strip(), 'live')
            _subscribe_mode_option_token(trade_mode, token, symbol)
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
            trade_mode = trade_mode_by_id.get(str(mrow.get('trade_id') or '').strip(), 'live')
            _subscribe_mode_option_token(trade_mode, mtoken, msymbol)
            momentum_subscribed += 1
            entry_print(
                f'[LIVE MOMENTUM PENDING SUBSCRIBE] '
                f'leg_id={str(mrow.get("leg_id") or "-")} '
                f'token={mtoken}'
            )

        entry_print(
            f'[LIVE OPEN POSITION SUBSCRIBE] trade_date={normalized_trade_date or "-"} '
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


def resolve_live_pending_entry_snapshot(
    db,
    trade: dict,
    leg_cfg: dict,
    *,
    now_ts: str,
) -> dict:
    underlying = str(
        (trade.get('strategy') or {}).get('Ticker')
        or (trade.get('config') or {}).get('Ticker')
        or trade.get('ticker')
        or ''
    ).strip().upper()
    if not underlying:
        return {}
    try:
        from features.backtest_engine import STRIKE_STEPS, _resolve_expiry, _resolve_strike
        from features.spot_atm_utils import resolve_atm_price
    except Exception:
        return {}

    spot_price = _get_live_spot_price(underlying)
    if spot_price <= 0:
        try:
            from features.live_monitor_socket import _get_active_ticker_manager
            spot_price = _safe_float(_get_active_ticker_manager().get_spot(underlying))
        except Exception:
            spot_price = 0.0
    if spot_price <= 0:
        return {}

    contract_cfg = leg_cfg.get('ContractType') or {}
    option_raw = str(contract_cfg.get('Option') or leg_cfg.get('InstrumentKind') or '')
    option_type = option_raw.split('.')[-1] if '.' in option_raw else option_raw
    expiry_kind = str(contract_cfg.get('Expiry') or leg_cfg.get('ExpiryKind') or 'ExpiryType.Weekly')
    atm_price = resolve_atm_price(underlying, spot_price) if spot_price > 0 else 0
    # Always resolve strike at entry time via live option chain.
    strike = None

    expiry = None
    token = ''
    symbol = ''
    ltp = 0.0
    from features.market_feed_tokens import active_token_broker_filter as _atbf2  # type: ignore
    expiries = sorted([
        str(e)
        for e in db._db['active_option_tokens'].distinct(
            'expiry',
            {**_atbf2(db), 'instrument': underlying, 'expiry': {'$gte': str(now_ts or '')[:10]}},
        )
        if e
    ])
    if not expiries:
        try:
            from features.broker_gateway import get_broker_expiries  # type: ignore
            expiries = get_broker_expiries(underlying, str(now_ts or '')[:10])
        except Exception:
            pass
    expiry = _resolve_expiry(str(now_ts or '')[:10], expiry_kind, expiries) if expiries else None

    # Pre-resolve strike from cached chain (0ms when parent leg chain is cached in same tick).
    # Next entry check finds _strike_locked=True → skips REST chain fetch entirely.
    if expiry:
        try:
            from features.live_option_chain import fetch_full_chain, select_strike_live
            entry_kind = str(
                contract_cfg.get('EntryType')
                or leg_cfg.get('EntryType')
                or leg_cfg.get('entry_kind')
                or ''
            )
            strike_param = str(
                contract_cfg.get('StrikeParameter')
                or leg_cfg.get('StrikeParameter')
                or 'StrikeType.ATM'
            )
            position_str = str(leg_cfg.get('PositionType') or leg_cfg.get('position') or '')
            chain = fetch_full_chain(db, underlying, expiry, spot_price)
            if chain.get(option_type.upper()):
                sel = select_strike_live(
                    chain, entry_kind, strike_param,
                    option_type.upper(), position_str, spot_price, underlying,
                )
                if sel:
                    strike = sel.get('strike')
                    token  = str(sel.get('token')  or '')
                    symbol = str(sel.get('symbol') or '')
                    ltp    = _safe_float(sel.get('ltp'))
                    if token:
                        _subscribe_live_option_token(token, symbol)
                    entry_print(
                        f'[SNAPSHOT PRE-RESOLVE] leg={str(leg_cfg.get("id") or "")} '
                        f'underlying={underlying} expiry={expiry} strike={strike} '
                        f'token={token} ltp={ltp}'
                    )
        except Exception as _pre_exc:
            entry_print(f'[SNAPSHOT PRE-RESOLVE] error leg={str(leg_cfg.get("id") or "")} : {_pre_exc}')

    if expiry and strike not in (None, ''):
        if not token:
            from features.market_feed_tokens import active_token_broker_filter as _atbf3  # type: ignore
            token_doc = db._db['active_option_tokens'].find_one({
                **_atbf3(db),
                'instrument': underlying,
                'expiry': expiry,
                'strike': strike,
                'option_type': option_type.upper(),
            }) or {}
            token = str(token_doc.get('token') or token_doc.get('tokens') or '').strip()
            symbol = str(token_doc.get('symbol') or '').strip()
            if not token:
                try:
                    from features.broker_gateway import get_broker_chain_doc  # type: ignore
                    _kcd = get_broker_chain_doc(underlying, expiry, strike, option_type.upper())
                    token = str(_kcd.get('token') or '').strip()
                    symbol = str(_kcd.get('symbol') or symbol or '').strip()
                    if token:
                        entry_print(
                            f'[LIVE ENTRY SNAPSHOT FALLBACK] '
                            f'underlying={underlying} expiry={expiry} strike={strike} '
                            f'option={option_type} token={token} symbol={symbol}'
                        )
                except Exception:
                    pass
            if token:
                _subscribe_live_option_token(token, symbol)
        if not ltp and token:
            ltp = _safe_float(_get_live_ltp_map().get(token))

    entry_print(
        '[LIVE ENTRY SNAPSHOT] '
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
        f'ltp={ltp}'
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
    }


def resolve_live_entry_execution_payload(
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

    # Fast path: contract already fully resolved → get LTP from Kite ticker directly, skip DB scan
    if underlying and leg_token and leg_token.isdigit() and leg_strike not in (None, '') and leg_expiry:
        ltp_map = _get_live_ltp_map()
        spot_price = _get_live_spot_price(underlying)
        ltp = get_live_option_quote_price(db, trade, leg_symbol)
        if ltp <= 0:
            ltp = _safe_float(ltp_map.get(leg_token))
        if ltp <= 0:
            # Token not yet subscribed or no tick received — subscribe now so next tick delivers LTP
            _subscribe_live_option_token(leg_token, leg_symbol)
            entry_print(f'[LIVE FAST PATH SUBSCRIBE] token={leg_token} symbol={leg_symbol or "-"} ltp_missing=True')
        return {
            'spot_price': spot_price,
            'strike': leg_strike,
            'expiry_date': leg_expiry,
            'token': leg_token,
            'symbol': leg_symbol,
            'entry_price': ltp,
            'current_option_price': ltp,
            'entry_price_source': 'quote' if entry_price > 0 and entry_price != _safe_float(ltp_map.get(leg_token)) else 'kite_live',
            'ltp': ltp,
            'atm_price': 0,
        }

    contract_cfg = {
        'id': str(leg.get('id') or ''),
        'ContractType': {
            'Option': str(leg.get('option') or leg.get('InstrumentKind') or ''),
            'Expiry': str(leg.get('expiry_kind') or leg.get('ExpiryKind') or 'ExpiryType.Weekly'),
            'StrikeParameter': str(leg.get('strike_parameter') or leg.get('StrikeParameter') or 'StrikeType.ATM'),
        },
        'InstrumentKind': str(leg.get('option') or leg.get('InstrumentKind') or ''),
        'ExpiryKind': str(leg.get('expiry_kind') or leg.get('ExpiryKind') or 'ExpiryType.Weekly'),
        'StrikeParameter': str(leg.get('strike_parameter') or leg.get('StrikeParameter') or 'StrikeType.ATM'),
    }
    snapshot = resolve_live_pending_entry_snapshot(
        db,
        trade,
        contract_cfg,
        now_ts=now_ts,
    ) or {}
    entry_price = get_live_option_quote_price(
        db,
        trade,
        str(snapshot.get('symbol') or leg.get('symbol') or '').strip(),
    )
    if entry_price <= 0:
        entry_price = _safe_float(snapshot.get('ltp'))
    return {
        'spot_price': _safe_float(
            snapshot.get('spot_at_queue')
            or snapshot.get('live_spot_price')
            or leg.get('spot_at_queue')
        ),
        'strike': snapshot.get('strike') if snapshot.get('strike') not in (None, '') else leg.get('strike'),
        'expiry_date': str(snapshot.get('expiry_date') or leg.get('expiry_date') or '').strip(),
        'token': str(snapshot.get('token') or leg.get('token') or '').strip(),
        'symbol': str(snapshot.get('symbol') or leg.get('symbol') or '').strip(),
        'entry_price': entry_price,
        'current_option_price': entry_price,
        'entry_price_source': 'quote' if entry_price > 0 and entry_price != _safe_float(snapshot.get('ltp')) else 'kite_live',
        'ltp': entry_price,
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
    'get_live_option_quote_price',
    'resolve_live_pending_entry_snapshot',
    'resolve_live_entry_execution_payload',
    'sync_live_open_position_subscriptions',
]
