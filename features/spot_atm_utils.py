"""
Helpers for shared spot-price lookup and ATM derivation.

This module is intended to be reused across backtest, fast-forward,
execution socket, and live-trade flows.

Live / fast-forward mode NEVER touches option_chain_historical_data.
All option chain data for live comes from Kite instruments API + Kite LTP map.
"""

from __future__ import annotations

import logging
import threading
from bisect import bisect_right
from typing import Any
from pymongo import DESCENDING

from features.backtest_engine import _resolve_expiry, _resolve_strike

log = logging.getLogger(__name__)


def _trace_stdout(message: str) -> None:
    """Print live option-chain traces immediately to the Python terminal."""
    print(message, flush=True)

OPTION_CHAIN_COLLECTION = 'option_chain_historical_data'
INDEX_SPOT_COLLECTION = 'option_chain_index_spot'
INDIA_VIX_COLLECTION = 'india_vix'
MARKET_DATA_CACHE: dict[str, dict] = {}

# ─── Kite instruments daily cache ─────────────────────────────────────────────
# Loaded once per trading day. Keyed by (underlying, expiry, strike, type).
# Value: {'token': int, 'symbol': str, 'exchange': str}
# Used ONLY for live / fast-forward mode (never for backtest).

_kite_inst_lock  = threading.Lock()
_kite_inst_date  = ''                          # date the cache was loaded for
_kite_inst_cache: dict[tuple, dict] = {}       # (underlying, expiry, strike, type) → inst


def _ist_today() -> str:
    from datetime import datetime, timezone, timedelta
    return (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d')


def _load_kite_instruments(force: bool = False) -> dict[tuple, dict]:
    """
    Load option instruments from Kite and cache them for the trading day.
    Returns the cache dict (may be empty if Kite not configured).
    Thread-safe.
    """
    global _kite_inst_date, _kite_inst_cache

    today = _ist_today()
    with _kite_inst_lock:
        if not force and _kite_inst_date == today and _kite_inst_cache:
            return _kite_inst_cache

        try:
            # Skip Kite REST API entirely when Dhan is the active broker
            try:
                from features.market_feed_tokens import get_active_feed_broker as _gafb  # type: ignore
                from features.mongo_data import MongoData as _MD2  # type: ignore
                _cfg_db = _MD2()
                try:
                    if _gafb(_cfg_db._db) == 'dhan':
                        return _kite_inst_cache
                finally:
                    _cfg_db.close()
            except Exception:
                pass

            from features.broker_gateway import get_broker_credentials, broker_is_configured, load_broker_credentials_from_db  # type: ignore
            if not broker_is_configured():
                try:
                    from features.mongo_data import MongoData  # type: ignore

                    _db = MongoData()
                    try:
                        load_broker_credentials_from_db(_db)
                    finally:
                        _db.close()
                except Exception as exc:
                    log.warning('[kite_instruments] credential load error: %s', exc)

            if not broker_is_configured():
                log.warning('[kite_instruments] Kite access token not configured')
                return _kite_inst_cache

            from kiteconnect import KiteConnect  # type: ignore
            api_key, access_token = get_broker_credentials()
            if not api_key or not access_token:
                log.warning('[kite_instruments] api_key/access_token missing after credential load')
                return _kite_inst_cache

            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            new_cache: dict[tuple, dict] = {}
            for segment in ('NFO', 'BFO'):
                instruments = kite.instruments(segment)
                for inst in instruments:
                    name      = str(inst.get('name') or '').strip().upper()
                    inst_type = str(inst.get('instrument_type') or '').strip().upper()
                    exp       = inst.get('expiry')
                    stk       = inst.get('strike')
                    tok       = inst.get('instrument_token')
                    sym       = str(inst.get('tradingsymbol') or '').strip()

                    if not (name and inst_type in ('CE', 'PE') and exp and stk is not None and tok):
                        continue

                    try:
                        exp_str = exp.strftime('%Y-%m-%d')
                    except AttributeError:
                        exp_str = str(exp)[:10]

                    key = (name, exp_str, float(stk), inst_type)
                    new_cache[key] = {
                        'token':    int(tok),
                        'symbol':   sym,
                        'exchange': str(inst.get('exchange') or segment),
                    }

            _kite_inst_cache = new_cache
            _kite_inst_date  = today
            log.info('[kite_instruments] loaded %d option instruments for %s', len(new_cache), today)

        except Exception as exc:
            log.warning('[kite_instruments] load error: %s', exc)

        return _kite_inst_cache


def get_kite_chain_doc(
    underlying: str,
    expiry: str,
    strike: float,
    option_type: str,
) -> dict:
    """
    Build a synthetic option chain doc from Kite instruments cache + live LTP.

    Used ONLY for live / fast-forward mode.
    Returns {} if the instrument is not found or Kite is not configured.
    """
    cache = _load_kite_instruments()
    key   = (
        str(underlying  or '').strip().upper(),
        str(expiry      or '').strip()[:10],
        float(strike),
        str(option_type or '').strip().upper(),
    )
    inst = cache.get(key)
    if not inst:
        _trace_stdout(
            f'[LIVE OPTION CHAIN] underlying={key[0]} expiry={key[1]} '
            f'strike={key[2]} type={key[3]} instrument=NOT_FOUND'
        )
        return {}

    token_int = inst['token']
    token_str = str(token_int)
    ltp       = 0.0

    # 1. REST quote cache — works even before WebSocket subscription
    quotes = fetch_kite_quotes_for_expiry(key[0], key[1])
    ltp    = float(quotes.get(token_str, 0.0))

    # 2. Fallback: WebSocket LTP map (available after token is subscribed)
    if ltp <= 0:
        try:
            from features.broker_gateway import get_broker_ltp_map  # type: ignore
            ltp = float(get_broker_ltp_map().get(token_str, 0.0))
        except Exception:
            pass

    spot = _get_live_spot_for_underlying(key[0])
    iv   = _calculate_live_iv(spot, key[2], key[1], ltp, key[3])

    _trace_stdout(
        f'[LIVE OPTION CHAIN] underlying={key[0]} expiry={key[1]} '
        f'strike={key[2]} type={key[3]} token={token_str} '
        f'symbol={inst["symbol"]} ltp={ltp if ltp > 0 else "UNAVAILABLE"} '
        f'iv={round(iv * 100, 2) if iv else "N/A"}'
    )

    return {
        'underlying': key[0],
        'expiry':     key[1],
        'strike':     key[2],
        'type':       key[3],
        'token':      str(token_int),
        'symbol':     inst['symbol'],
        'exchange':   inst['exchange'],
        'close':      ltp,
        'ltp':        ltp,
        'current_price': ltp,
        'price':      ltp,
        'last_price': ltp,
        'iv':         iv or None,
    }


def get_kite_expiries(underlying: str, from_date: str, *, force_refresh: bool = False) -> list[str]:
    """
    Return sorted expiry date strings for *underlying* >= from_date
    from the Kite instruments cache.  Used by live / fast-forward mode.
    """
    cache = _load_kite_instruments(force=force_refresh)
    und   = str(underlying or '').strip().upper()
    expiries: set[str] = set()
    for (name, exp, _strike, _type) in cache:
        if name == und and exp >= from_date:
            expiries.add(exp)
    return sorted(expiries)


def list_kite_option_contracts(underlying: str, expiry: str, *, force_refresh: bool = False) -> list[dict]:
    """
    Return all cached Kite option contracts for an underlying + expiry.

    Each item contains:
      {
        'instrument': 'NIFTY',
        'expiry': '2026-04-23',
        'strike': 24500,
        'option_type': 'CE',
        'token': '123456',
        'tokens': '123456',
        'symbol': 'NIFTY26APR24500CE',
        'exchange': 'NFO',
      }
    """
    und = str(underlying or '').strip().upper()
    exp = str(expiry or '').strip()[:10]
    if not und or not exp:
        return []

    cache = _load_kite_instruments(force=force_refresh)
    contracts: list[dict] = []
    for (name, exp_key, strike, option_type), inst in cache.items():
        if name != und or exp_key != exp:
            continue
        token_value = str(inst.get('token') or '').strip()
        if not token_value:
            continue
        strike_value = int(strike) if float(strike).is_integer() else float(strike)
        contracts.append({
            'instrument': und,
            'expiry': exp,
            'strike': strike_value,
            'option_type': str(option_type or '').strip().upper(),
            'token': token_value,
            'tokens': token_value,
            'symbol': str(inst.get('symbol') or '').strip(),
            'exchange': str(inst.get('exchange') or 'NFO').strip() or 'NFO',
        })

    contracts.sort(key=lambda item: (item['strike'], item['option_type']))
    return contracts


# ─── Kite option-chain quote cache ────────────────────────────────────────────
# At entry time we need LTP for ALL strikes of a specific expiry — but those
# tokens are NOT yet subscribed on the WebSocket.  We solve this by calling
# the Kite REST quote() API once per (underlying, expiry) and caching the
# result for _QUOTE_CACHE_TTL seconds.  This avoids per-token subscriptions
# before entry and works even when ltp_map is empty.
#
# Used ONLY for live / fast-forward mode.

import time as _time

_QUOTE_CACHE_TTL              = 3.0   # seconds
_kite_quote_lock              = threading.Lock()
_kite_quote_cache: dict       = {}    # {(underlying, expiry): {'ts': float, 'data': {str(token): float}}}


def fetch_kite_quotes_for_expiry(underlying: str, expiry: str) -> dict[str, float]:
    """
    Fetch real-time LTP for ALL options of *underlying* + *expiry* via the
    Kite REST quote() API.  Returns {str(instrument_token): float(ltp)}.

    Results are cached for _QUOTE_CACHE_TTL seconds so multiple legs of the
    same strategy share one API call at entry time.

    Live / fast-forward only — never called for backtest.
    """
    und       = str(underlying or '').strip().upper()
    exp       = str(expiry     or '').strip()[:10]
    cache_key = (und, exp)

    with _kite_quote_lock:
        cached = _kite_quote_cache.get(cache_key)
        if cached and (_time.monotonic() - cached['ts']) < _QUOTE_CACHE_TTL:
            return cached['data']

    # Collect all instrument tokens for this underlying + expiry
    inst_cache = _load_kite_instruments()
    tokens: list[int] = [
        inst['token']
        for (name, exp_k, _stk, _typ), inst in inst_cache.items()
        if name == und and exp_k == exp
    ]

    if not tokens:
        return {}

    ltp_data: dict[str, float] = {}
    try:
        from features.broker_gateway import get_broker_credentials, broker_is_configured  # type: ignore
        if not broker_is_configured():
            return {}
        from kiteconnect import KiteConnect  # type: ignore
        api_key, access_token = get_broker_credentials()
        if not api_key or not access_token:
            return {}

        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)

        # Kite quote() accepts up to 500 tokens per call
        for i in range(0, len(tokens), 500):
            batch  = tokens[i:i + 500]
            quotes = kite.quote(batch)
            for _sym, q in quotes.items():
                tok = str(q.get('instrument_token') or '').strip()
                ltp = float(q.get('last_price') or 0.0)
                if tok:
                    ltp_data[tok] = ltp

        log.info(
            '[kite_quotes] fetched %d quotes  underlying=%s  expiry=%s',
            len(ltp_data), und, exp,
        )
        _trace_stdout(
            f'[LIVE OPTION QUOTES] underlying={und} expiry={exp} quotes={len(ltp_data)}'
        )
    except Exception as exc:
        log.warning('[kite_quotes] fetch error underlying=%s expiry=%s: %s', und, exp, exc)
        _trace_stdout(
            f'[LIVE OPTION QUOTES] underlying={und} expiry={exp} fetch_error={exc}'
        )

    with _kite_quote_lock:
        _kite_quote_cache[cache_key] = {'ts': _time.monotonic(), 'data': ltp_data}

    return ltp_data


# ─── Dhan instrument helpers (active_option_tokens collection) ────────────────
# For Dhan broker, all option tokens are pre-synced into active_option_tokens
# (broker="dhan") from Dhan instruments CSV.  No REST instrument API needed.

_dhan_inst_cache: dict = {}
_dhan_inst_date:  str  = ''
_dhan_inst_lock         = threading.Lock()


def _load_dhan_instruments(force: bool = False) -> dict:
    """
    Load Dhan option instruments from active_option_tokens (broker=dhan).
    Returns same dict structure as _load_kite_instruments():
      {(name, expiry, strike, option_type): {token, symbol, exchange}}
    """
    global _dhan_inst_cache, _dhan_inst_date

    today = _ist_today()
    with _dhan_inst_lock:
        if not force and _dhan_inst_date == today and _dhan_inst_cache:
            return _dhan_inst_cache

        try:
            from features.mongo_data import MongoData  # type: ignore
            _db = MongoData()
            try:
                new_cache: dict = {}
                for doc in _db._db['active_option_tokens'].find(
                    {'broker': 'dhan'}, {'_id': 0}
                ):
                    inst  = str(doc.get('instrument') or '').strip().upper()
                    opt   = str(doc.get('option_type') or '').strip().upper()
                    exp   = str(doc.get('expiry') or '').strip()[:10]
                    stk   = doc.get('strike')
                    token = str(doc.get('token') or '').strip()
                    sym   = str(doc.get('symbol') or '').strip()
                    exch  = str(doc.get('exchange') or doc.get('ws_segment') or 'NSE_FNO').strip()
                    if inst and opt in ('CE', 'PE') and exp and stk is not None and token:
                        key = (inst, exp, float(stk), opt)
                        new_cache[key] = {'token': token, 'symbol': sym, 'exchange': exch}
                _dhan_inst_cache = new_cache
                _dhan_inst_date  = today
                log.info('[dhan_instruments] loaded %d option instruments', len(new_cache))
            finally:
                _db.close()
        except Exception as exc:
            log.warning('[dhan_instruments] load error: %s', exc)

        return _dhan_inst_cache


def get_dhan_expiries(underlying: str, from_date: str, *, force_refresh: bool = False) -> list[str]:  # noqa: ARG001
    """Return sorted expiry date strings for underlying from active_option_tokens (dhan)."""
    try:
        from features.mongo_data import MongoData  # type: ignore
        _db = MongoData()
        try:
            return sorted([
                str(e) for e in _db._db['active_option_tokens'].distinct(
                    'expiry',
                    {
                        'broker': 'dhan',
                        'instrument': str(underlying or '').strip().upper(),
                        'expiry': {'$gte': str(from_date or '')[:10]},
                    },
                ) if e
            ])
        finally:
            _db.close()
    except Exception as exc:
        log.warning('[dhan_expiries] error: %s', exc)
        return []


def list_dhan_option_contracts(underlying: str, expiry: str, *, force_refresh: bool = False) -> list[dict]:  # noqa: ARG001
    """Return all active_option_tokens docs for underlying + expiry (dhan)."""
    try:
        from features.mongo_data import MongoData  # type: ignore
        _db = MongoData()
        try:
            return list(_db._db['active_option_tokens'].find(
                {
                    'broker':     'dhan',
                    'instrument': str(underlying or '').strip().upper(),
                    'expiry':     str(expiry or '').strip()[:10],
                },
                {'_id': 0},
            ))
        finally:
            _db.close()
    except Exception as exc:
        log.warning('[dhan_contracts] error: %s', exc)
        return []


def get_dhan_chain_doc(
    underlying: str,
    expiry: str,
    strike: float,
    option_type: str,
) -> dict:
    """
    Build a synthetic option chain doc from active_option_tokens (dhan) + Dhan WS LTP.
    Same return shape as get_kite_chain_doc().
    """
    try:
        from features.mongo_data import MongoData  # type: ignore
        _db = MongoData()
        try:
            doc = _db._db['active_option_tokens'].find_one({
                'broker':      'dhan',
                'instrument':  str(underlying  or '').strip().upper(),
                'expiry':      str(expiry      or '').strip()[:10],
                'strike':      float(strike),
                'option_type': str(option_type or '').strip().upper(),
            }) or {}
        finally:
            _db.close()

        if not doc:
            return {}

        token  = str(doc.get('token') or '').strip()
        symbol = str(doc.get('symbol') or '').strip()
        exch   = str(doc.get('exchange') or doc.get('ws_segment') or 'NSE_FNO').strip()
        ltp    = 0.0
        if token:
            from features.dhan_ticker import dhan_ticker_manager  # type: ignore
            ltp = float(dhan_ticker_manager.ltp_map.get(token) or 0)

        spot = _get_live_spot_for_underlying(str(underlying or '').strip().upper())
        iv   = _calculate_live_iv(spot, float(strike), str(expiry or '')[:10], ltp, str(option_type or '').upper())

        return {
            'underlying':    str(underlying or '').strip().upper(),
            'expiry':        str(expiry or '')[:10],
            'strike':        float(strike),
            'type':          str(option_type or '').strip().upper(),
            'token':         token,
            'symbol':        symbol,
            'exchange':      exch,
            'close':         ltp,
            'ltp':           ltp,
            'current_price': ltp,
            'price':         ltp,
            'last_price':    ltp,
            'iv':            iv or None,
        }
    except Exception as exc:
        log.warning('[dhan_chain_doc] error: %s', exc)
        return {}


def fetch_dhan_quotes_for_expiry(underlying: str, expiry: str) -> dict[str, float]:
    """
    Get LTP for all Dhan option tokens of underlying + expiry from the WS ltp_map.
    Returns {str(security_id): float(ltp)}.
    """
    try:
        from features.dhan_ticker import dhan_ticker_manager  # type: ignore
        from features.mongo_data import MongoData  # type: ignore
        _db = MongoData()
        try:
            tokens = [
                str(d.get('token') or '').strip()
                for d in _db._db['active_option_tokens'].find(
                    {
                        'broker':     'dhan',
                        'instrument': str(underlying or '').strip().upper(),
                        'expiry':     str(expiry or '')[:10],
                    },
                    {'token': 1, '_id': 0},
                )
                if d.get('token')
            ]
        finally:
            _db.close()

        ltp_map = dhan_ticker_manager.ltp_map
        return {tok: float(ltp_map.get(tok) or 0) for tok in tokens}
    except Exception as exc:
        log.warning('[dhan_quotes] error: %s', exc)
        return {}


# Kite numeric instrument tokens for major index underlyings.
# Used by live / fast-forward mode to get real-time spot price from
# kite_broker_ws LTP map instead of querying the DB.
KITE_INDEX_TOKENS: dict[str, int] = {
    'NIFTY':      256265,
    'BANKNIFTY':  260105,
    'SENSEX':     265,
    'FINNIFTY':   257801,
    'MIDCPNIFTY': 288009,
    'INDIA_VIX':  264969,
}
INDIA_VIX_KITE_TOKEN = 264969


def _get_live_spot_for_underlying(underlying: str) -> float:
    """Get real-time spot price for an underlying from broker WebSocket spot_map."""
    und = str(underlying or '').strip().upper()
    try:
        from features.broker_gateway import broker_ticker_manager  # type: ignore
        price = broker_ticker_manager.get_spot(und)
        if price and float(price) > 0:
            return float(price)
    except Exception:
        pass
    # Fallback: direct ltp_map lookup via broker index token
    try:
        from features.broker_gateway import BROKER_INDEX_TOKENS, get_broker_ltp_map  # type: ignore
        token = BROKER_INDEX_TOKENS.get(und, 0)
        if token:
            return float(get_broker_ltp_map().get(str(token), 0.0))
    except Exception:
        pass
    return 0.0


def _calculate_live_iv(spot: float, strike: float, expiry: str, ltp: float, option_type: str) -> float:
    """Calculate IV using Black-Scholes Newton-Raphson for live/fast-forward mode."""
    if not (spot > 0 and strike > 0 and ltp > 0):
        return 0.0
    try:
        from datetime import date as _date
        expiry_date = _date.fromisoformat(str(expiry or '')[:10])
        T = max((expiry_date - _date.today()).days, 0) / 365.0
        if T <= 0:
            return 0.0
        from features.span_margin import implied_vol, RISK_FREE_RATE  # type: ignore
        return implied_vol(spot, strike, T, RISK_FREE_RATE, ltp, option_type)
    except Exception:
        return 0.0


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def resolve_atm_price(underlying: str, spot_price: float) -> int:
    normalized_underlying = str(underlying or '').upper()
    step = get_strike_step(normalized_underlying)
    if spot_price <= 0:
        return 0
    return int(round(spot_price / step) * step)


_STRIKE_STEP_MAP = {
    'NIFTY': 50, 'BANKNIFTY': 100, 'FINNIFTY': 50,
    'MIDCPNIFTY': 25, 'SENSEX': 100, 'BANKEX': 100,
}

def get_strike_step(underlying: str) -> int:
    return _STRIKE_STEP_MAP.get(str(underlying or '').strip().upper(), 100)


def _normalize_underlyings(underlyings: list[str] | tuple[str, ...] | set[str] | None) -> list[str]:
    return sorted({
        str(item or '').strip().upper()
        for item in (underlyings or [])
        if str(item or '').strip()
    })


def _build_market_cache_key(trade_date: str, underlyings: list[str]) -> str:
    suffix = ','.join(underlyings) if underlyings else '*'
    return f'{str(trade_date or "").strip()}::{suffix}'


def clear_market_data_cache(cache_key: str | None = None) -> None:
    normalized_key = str(cache_key or '').strip()
    if not normalized_key:
        return
    MARKET_DATA_CACHE.pop(normalized_key, None)


def _find_latest_snapshot(items: list[dict], timestamps: list[str], snapshot_ts: str) -> dict:
    if not items or not timestamps:
        return {}
    index = bisect_right(timestamps, snapshot_ts) - 1
    if index < 0:
        return {}
    return items[index] or {}


def preload_market_data_cache(
    db,
    trade_date: str,
    underlyings: list[str] | tuple[str, ...] | set[str] | None = None,
    *,
    force_refresh: bool = False,
) -> dict:
    normalized_date = str(trade_date or '').strip()
    normalized_underlyings = _normalize_underlyings(underlyings)
    cache_key = _build_market_cache_key(normalized_date, normalized_underlyings)
    if not force_refresh and cache_key in MARKET_DATA_CACHE:
        return MARKET_DATA_CACHE[cache_key]

    option_query: dict[str, Any] = {'timestamp': {'$regex': f'^{normalized_date}'}}
    spot_query: dict[str, Any] = {'timestamp': {'$regex': f'^{normalized_date}'}}
    if normalized_underlyings:
        option_query['underlying'] = {'$in': normalized_underlyings}
        spot_query['underlying'] = {'$in': normalized_underlyings}

    chain_docs: dict[tuple[str, str, float, str], list[dict]] = {}
    chain_timestamps: dict[tuple[str, str, float, str], list[str]] = {}
    latest_chain_docs: dict[tuple[str, str, float, str], dict] = {}
    expiries_by_underlying: dict[str, set[str]] = {}

    for item in db._db[OPTION_CHAIN_COLLECTION].find(
        option_query,
        {
            '_id': 0,
            'underlying': 1,
            'expiry': 1,
            'strike': 1,
            'type': 1,
            'timestamp': 1,
            'close': 1,
            'spot_price': 1,
            'symbol': 1,
            'token': 1,
            'oi': 1,
            'iv': 1,
            'delta': 1,
            'gamma': 1,
            'theta': 1,
            'vega': 1,
            'rho': 1,
        },
    ).sort([('underlying', 1), ('expiry', 1), ('strike', 1), ('type', 1), ('timestamp', 1)]):
        underlying = str(item.get('underlying') or '').strip().upper()
        expiry = str(item.get('expiry') or '').strip()
        option_type = str(item.get('type') or '').strip().upper()
        strike = safe_float(item.get('strike'))
        timestamp = str(item.get('timestamp') or '').strip()
        if not underlying or not expiry or not option_type or not timestamp:
            continue
        key = (underlying, expiry, strike, option_type)
        chain_docs.setdefault(key, []).append(item)
        chain_timestamps.setdefault(key, []).append(timestamp)
        latest_chain_docs[key] = item
        expiries_by_underlying.setdefault(underlying, set()).add(expiry)

    spot_docs: dict[str, list[dict]] = {}
    spot_timestamps: dict[str, list[str]] = {}
    latest_spot_docs: dict[str, dict] = {}
    for item in db._db[INDEX_SPOT_COLLECTION].find(
        spot_query,
        {
            '_id': 0,
            'underlying': 1,
            'timestamp': 1,
            'spot_price': 1,
            'token': 1,
            'symbol': 1,
        },
    ).sort([('underlying', 1), ('timestamp', 1)]):
        underlying = str(item.get('underlying') or '').strip().upper()
        timestamp = str(item.get('timestamp') or '').strip()
        if not underlying or not timestamp:
            continue
        spot_docs.setdefault(underlying, []).append(item)
        spot_timestamps.setdefault(underlying, []).append(timestamp)
        latest_spot_docs[underlying] = item

    # Load India VIX for the trade date (used for entry_vix in blue line calculation)
    vix_docs: list[dict] = []
    vix_timestamps: list[str] = []
    try:
        for item in db._db[INDIA_VIX_COLLECTION].find(
            {'timestamp': {'$regex': f'^{normalized_date}'}},
            {'_id': 0, 'timestamp': 1, 'close': 1},
        ).sort([('timestamp', 1)]):
            ts = str(item.get('timestamp') or '').strip()
            if ts:
                vix_docs.append(item)
                vix_timestamps.append(ts)
    except Exception:
        pass

    cache = {
        'cache_key': cache_key,
        'trade_date': normalized_date,
        'underlyings': normalized_underlyings,
        'chain_docs': chain_docs,
        'chain_timestamps': chain_timestamps,
        'latest_chain_docs': latest_chain_docs,
        'spot_docs': spot_docs,
        'spot_timestamps': spot_timestamps,
        'latest_spot_docs': latest_spot_docs,
        'vix_docs': vix_docs,
        'vix_timestamps': vix_timestamps,
        'expiries_by_underlying': {
            underlying: sorted(values)
            for underlying, values in expiries_by_underlying.items()
        },
    }
    MARKET_DATA_CACHE[cache_key] = cache
    return cache


def _kite_spot_doc(underlying_norm: str, ts: str) -> dict:
    """
    Return a synthetic spot doc built from live broker WS spot_map / LTP map.
    Works for both Kite and Dhan — uses spot_map (keyed by underlying name).
    """
    try:
        from features.broker_gateway import broker_ticker_manager  # type: ignore
        price = float(broker_ticker_manager.get_spot(underlying_norm) or 0)
        if price > 0:
            return {
                'underlying': underlying_norm,
                'spot_price': price,
                'close':      price,
                'ltp':        price,
                'timestamp':  ts or '',
            }
    except Exception:
        pass
    return {}


def _active_option_token_doc(
    db,
    underlying_norm: str,
    expiry_norm: str,
    strike_val: float,
    opt_norm: str,
    ts: str,
) -> dict:
    """
    Resolve live option contract metadata from active_option_tokens and
    overlay the latest Kite WebSocket LTP.

    This is the live / fast-forward replacement for any option-chain-based
    contract lookup. No REST quote fetch is performed here.
    """
    if db is None:
        return {}

    try:
        from features.market_feed_tokens import active_token_broker_filter as _atbf  # type: ignore
        doc = db['active_option_tokens'].find_one(
            {
                **_atbf(db),
                'instrument': underlying_norm,
                'expiry': expiry_norm,
                'strike': strike_val,
                'option_type': opt_norm,
            },
            {
                '_id': 0,
                'instrument': 1,
                'expiry': 1,
                'strike': 1,
                'option_type': 1,
                'token': 1,
                'tokens': 1,
                'symbol': 1,
                'exchange': 1,
            },
        ) or {}
    except Exception as exc:
        log.warning(
            '[active_option_tokens] lookup error instrument=%s expiry=%s strike=%s type=%s: %s',
            underlying_norm, expiry_norm, strike_val, opt_norm, exc,
        )
        return {}

    token_str = str(doc.get('token') or doc.get('tokens') or '').strip()
    if not doc or not token_str:
        _trace_stdout(
            f'[ACTIVE OPTION TOKEN] instrument={underlying_norm} expiry={expiry_norm} '
            f'strike={strike_val} type={opt_norm} token=NOT_FOUND'
        )
        return {}

    ltp = 0.0
    try:
        from features.broker_gateway import get_broker_ltp_map  # type: ignore
        ltp = float(get_broker_ltp_map().get(token_str, 0.0))
    except Exception:
        pass

    spot = _get_live_spot_for_underlying(underlying_norm)
    iv   = _calculate_live_iv(spot, strike_val, expiry_norm, ltp, opt_norm)

    _trace_stdout(
        f'[ACTIVE OPTION TOKEN] instrument={underlying_norm} expiry={expiry_norm} '
        f'strike={strike_val} type={opt_norm} token={token_str} '
        f'symbol={str(doc.get("symbol") or "").strip() or "-"} '
        f'ltp={ltp if ltp > 0 else "UNAVAILABLE"} iv={round(iv * 100, 2) if iv else "N/A"}'
    )

    return {
        'underlying': underlying_norm,
        'expiry': expiry_norm,
        'strike': strike_val,
        'type': opt_norm,
        'token': token_str,
        'symbol': str(doc.get('symbol') or '').strip(),
        'exchange': str(doc.get('exchange') or 'NFO').strip() or 'NFO',
        'close': ltp,
        'ltp': ltp,
        'current_price': ltp,
        'price': ltp,
        'last_price': ltp,
        'timestamp': ts or '',
        'iv': iv or None,
    }


def get_cached_spot_doc(
    db_or_cache,
    underlying: str,
    snapshot_ts: str | None = None,
    *,
    timestamp: str | None = None,
    cache: dict | None = None,
) -> dict:
    """
    Fetch spot price document.

    Modes
    ─────
    • backtest        : first arg is a pre-loaded dict market_cache  →  cache lookup
    • live / fast-fwd : first arg is a pymongo Database              →  kite LTP first,
                        then DB fallback (never uses historical cache)

    ``timestamp`` is an alias for ``snapshot_ts`` (used by trading_core.py).
    ``cache`` is an explicit dict cache (takes priority over db_or_cache).
    """
    ts = snapshot_ts or timestamp

    # Determine the actual dict cache (backtest path)
    actual_cache = cache
    if actual_cache is None and isinstance(db_or_cache, dict):
        actual_cache = db_or_cache

    normalized_underlying = str(underlying or '').strip().upper()
    if not normalized_underlying:
        return {}

    # ── Backtest: use pre-loaded dict cache ───────────────────────────────────
    if actual_cache:
        if not ts:
            return (actual_cache.get('latest_spot_docs') or {}).get(normalized_underlying) or {}
        return _find_latest_snapshot(
            (actual_cache.get('spot_docs') or {}).get(normalized_underlying) or [],
            (actual_cache.get('spot_timestamps') or {}).get(normalized_underlying) or [],
            ts,
        )

    # ── Live / fast-forward: db_or_cache is a pymongo Database ───────────────
    db = db_or_cache
    if db is None:
        return {}

    # 1. Try live broker LTP for the index token (real-time, most accurate)
    kite_doc = _kite_spot_doc(normalized_underlying, ts or '')
    if kite_doc:
        return kite_doc

    # 2. Fallback: direct DB query (if kite not yet connected / token not subscribed)
    try:
        query: dict = {'underlying': normalized_underlying}
        if ts:
            query['timestamp'] = {'$lte': ts}
        doc = db[INDEX_SPOT_COLLECTION].find_one(query, sort=[('timestamp', -1)])
        return doc or {}
    except Exception:
        return {}


def get_cached_chain_doc(
    db_or_cache,
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    snapshot_ts: str | None = None,
    *,
    timestamp: str | None = None,
    cache: dict | None = None,
) -> dict:
    """
    Fetch option chain document.

    Modes
    ─────
    • backtest        : first arg is a pre-loaded dict market_cache  →  cache lookup
    • live / fast-fwd : first arg is a pymongo Database              →  DB query for
                        contract metadata (expiry, strike, instrument_token) then
                        overlay the price fields with live Kite LTP so the entry /
                        SL / TP uses real-time tick data, not a stale DB close price.

    ``timestamp`` is an alias for ``snapshot_ts`` (used by trading_core.py).
    ``cache`` is an explicit dict cache (takes priority over db_or_cache).
    """
    ts = snapshot_ts or timestamp

    # Determine the actual dict cache (backtest path)
    actual_cache = cache
    if actual_cache is None and isinstance(db_or_cache, dict):
        actual_cache = db_or_cache

    key = (
        str(underlying or '').strip().upper(),
        str(expiry or '').strip()[:10],
        safe_float(strike),
        str(option_type or '').strip().upper(),
    )

    # ── Backtest: use pre-loaded dict cache ───────────────────────────────────
    if actual_cache:
        if not ts:
            return (actual_cache.get('latest_chain_docs') or {}).get(key) or {}
        return _find_latest_snapshot(
            (actual_cache.get('chain_docs') or {}).get(key) or [],
            (actual_cache.get('chain_timestamps') or {}).get(key) or [],
            ts,
        )

    # ── Live / fast-forward: use active_option_tokens + Kite socket LTP ─────
    # Never query option_chain_historical_data here — that collection is for backtest only.
    underlying_norm, expiry_norm, strike_val, opt_norm = key
    return _active_option_token_doc(
        db_or_cache,
        underlying_norm,
        expiry_norm,
        strike_val,
        opt_norm,
        ts or '',
    )


def build_entry_spot_snapshots(
    db,
    records: list[dict],
    listen_time: str,
    listen_timestamp: str,
    market_cache: dict | None = None,
) -> list[dict]:
    matched_records = [
        record for record in (records or [])
        if _extract_hhmm(record.get('entry_time') or '') == listen_time
    ]
    if not matched_records:
        return []

    underlyings = sorted({
        str(record.get('underlying') or '').strip().upper()
        for record in matched_records
        if str(record.get('underlying') or '').strip()
    })

    spot_map: dict[str, dict] = {}
    expiry_map: dict[str, list[str]] = {}
    resolved_market_cache = market_cache
    if underlyings:
        resolved_market_cache = market_cache or preload_market_data_cache(db, listen_timestamp[:10], underlyings)
        for underlying in underlyings:
            spot_doc = get_cached_spot_doc(resolved_market_cache, underlying, listen_timestamp)
            if spot_doc:
                spot_map[underlying] = spot_doc
            expiry_map[underlying] = list(
                ((resolved_market_cache.get('expiries_by_underlying') or {}).get(underlying) or [])
            )

    snapshots = []
    for record in matched_records:
        underlying = str(record.get('underlying') or '').strip().upper()
        spot_doc = spot_map.get(underlying) or {}
        spot_price = safe_float(spot_doc.get('spot_price'))
        option_chain = build_option_chain_snapshots_for_record(
            db=db,
            record=record,
            underlying=underlying,
            trade_date=listen_timestamp[:10],
            listen_timestamp=listen_timestamp,
            spot_price=spot_price,
            expiries=expiry_map.get(underlying) or [],
            market_cache=resolved_market_cache,
        )
        snapshots.append({
            'group_name': record.get('group_name') or '',
            'strategy_name': record.get('name') or '',
            'entry_time': record.get('entry_time') or '',
            'underlying': underlying,
            'spot_price': spot_price,
            'atm_price': resolve_atm_price(underlying, spot_price),
            'spot_timestamp': spot_doc.get('timestamp') or listen_timestamp,
            'option_chain': option_chain,
        })
    return snapshots


def build_option_chain_snapshots_for_record(
    *,
    db,
    record: dict,
    underlying: str,
    trade_date: str,
    listen_timestamp: str,
    spot_price: float,
    expiries: list[str],
    market_cache: dict | None = None,
) -> list[dict]:
    config = record.get('config') if isinstance(record.get('config'), dict) else {}
    strategy = record.get('strategy') if isinstance(record.get('strategy'), dict) else {}
    leg_configs = config.get('LegConfigs') if isinstance(config.get('LegConfigs'), dict) else {}
    if not leg_configs:
        strategy_leg_list = strategy.get('ListOfLegConfigs') if isinstance(strategy.get('ListOfLegConfigs'), list) else []
        normalized_leg_configs: dict[str, dict] = {}
        for leg_cfg in strategy_leg_list:
            if not isinstance(leg_cfg, dict):
                continue
            leg_id = str(leg_cfg.get('id') or '').strip()
            if not leg_id:
                continue
            inst_kind = str(leg_cfg.get('InstrumentKind') or '').strip().upper()
            option_type = 'CE' if 'CE' in inst_kind else 'PE' if 'PE' in inst_kind else ''
            normalized_leg_configs[leg_id] = {
                **leg_cfg,
                'PositionType': leg_cfg.get('PositionType') or leg_cfg.get('Position') or '',
                'ContractType': {
                    'Option': option_type,
                    'Expiry': leg_cfg.get('ExpiryKind') or 'ExpiryType.Weekly',
                    'EntryKind': leg_cfg.get('EntryType') or 'EntryType.EntryByStrikeType',
                    'StrikeParameter': leg_cfg.get('StrikeParameter'),
                },
            }
        leg_configs = normalized_leg_configs
    snapshots: list[dict] = []

    for leg_id, leg_config in leg_configs.items():
        if not isinstance(leg_config, dict):
            continue
        contract = leg_config.get('ContractType') if isinstance(leg_config.get('ContractType'), dict) else {}
        option_type = str(contract.get('Option') or '').strip().upper()
        if not option_type:
            inst_kind = str(leg_config.get('InstrumentKind') or '').strip().upper()
            option_type = 'CE' if 'CE' in inst_kind else 'PE' if 'PE' in inst_kind else ''
        expiry_kind = str(contract.get('Expiry') or leg_config.get('ExpiryKind') or 'ExpiryType.Weekly').strip()
        entry_kind = str(contract.get('EntryKind') or leg_config.get('EntryType') or 'EntryType.EntryByStrikeType').strip()
        strike_param = contract.get('StrikeParameter') if contract.get('StrikeParameter') is not None else leg_config.get('StrikeParameter')
        expiry = _resolve_expiry(trade_date, expiry_kind, expiries)
        if not expiry:
            continue
        strike = resolve_leg_strike(
            db=db,
            underlying=underlying,
            expiry=expiry,
            option_type=option_type,
            entry_kind=entry_kind,
            strike_param=strike_param,
            spot_price=spot_price,
            listen_timestamp=listen_timestamp,
            market_cache=market_cache,
        )
        if strike is None:
            continue
        chain_doc = get_cached_chain_doc(
            market_cache,
            underlying,
            expiry,
            strike,
            option_type,
            listen_timestamp,
        )
        if not chain_doc:
            chain_doc = db._db[OPTION_CHAIN_COLLECTION].find_one(
                {
                    'underlying': underlying,
                    'expiry': expiry,
                    'strike': float(strike),
                    'type': option_type,
                    'timestamp': {'$lte': listen_timestamp},
                },
                sort=[('timestamp', DESCENDING)],
            ) or {}
        snapshots.append({
            'leg_id': str(leg_id),
            'position': str(leg_config.get('PositionType') or ''),
            'entry_kind': entry_kind,
            'expiry_kind': expiry_kind,
            'expiry': expiry,
            'option_type': option_type,
            'strike': strike,
            'close': safe_float(chain_doc.get('close')),
            'timestamp': chain_doc.get('timestamp') or listen_timestamp,
            'spot_price': safe_float(chain_doc.get('spot_price'), spot_price),
            'symbol': str(chain_doc.get('symbol') or ''),
            'token': str(chain_doc.get('token') or ''),
        })
    return snapshots


def resolve_leg_strike(
    *,
    db,
    underlying: str,
    expiry: str,
    option_type: str,
    entry_kind: str,
    strike_param: Any,
    spot_price: float,
    listen_timestamp: str,
    market_cache: dict | None = None,
) -> int | None:
    step = get_strike_step(underlying)

    if entry_kind == 'EntryType.EntryByPremium':
        target = safe_float(strike_param)
        return resolve_strike_by_premium(
            db=db,
            underlying=underlying,
            expiry=expiry,
            option_type=option_type,
            target_premium=target,
            listen_timestamp=listen_timestamp,
            market_cache=market_cache,
        )

    if entry_kind == 'EntryType.EntryByPremiumRange' and isinstance(strike_param, dict):
        lower = safe_float(strike_param.get('LowerRange'))
        upper = safe_float(strike_param.get('UpperRange'), lower)
        return resolve_strike_by_premium(
            db=db,
            underlying=underlying,
            expiry=expiry,
            option_type=option_type,
            target_premium=(lower + upper) / 2,
            listen_timestamp=listen_timestamp,
            market_cache=market_cache,
        )

    if entry_kind in ('EntryType.EntryByDelta', 'EntryType.EntryByDeltaRange'):
        return _resolve_strike(spot_price, 'StrikeType.ATM', option_type, step)

    if entry_kind == 'EntryType.EntryByAtmMultiplier':
        try:
            scaled_spot = safe_float(spot_price) * safe_float(strike_param, 1.0)
            return resolve_atm_price(underlying, scaled_spot)
        except Exception:
            return _resolve_strike(spot_price, 'StrikeType.ATM', option_type, step)

    if entry_kind in ('EntryType.EntryByStraddlePrice', 'EntryType.EntryByPremiumCloseToStraddle') and isinstance(strike_param, dict):
        strike_kind = str(strike_param.get('StrikeKind') or 'StrikeType.ATM')
        return _resolve_strike(spot_price, strike_kind, option_type, step)

    if isinstance(strike_param, str):
        return _resolve_strike(spot_price, strike_param, option_type, step)

    return _resolve_strike(spot_price, 'StrikeType.ATM', option_type, step)


def resolve_strike_by_premium(
    *,
    db,
    underlying: str,
    expiry: str,
    option_type: str,
    target_premium: float,
    listen_timestamp: str,
    market_cache: dict | None = None,
) -> int | None:
    if target_premium <= 0:
        return None
    best_strike = None
    best_diff = float('inf')
    seen_strikes: set[float] = set()
    if market_cache:
        chain_docs = market_cache.get('chain_docs') or {}
        chain_timestamps = market_cache.get('chain_timestamps') or {}
        for key, items in chain_docs.items():
            cache_underlying, cache_expiry, strike, cache_option_type = key
            if cache_underlying != str(underlying or '').strip().upper():
                continue
            if cache_expiry != str(expiry or '').strip():
                continue
            if cache_option_type != str(option_type or '').strip().upper():
                continue
            item = _find_latest_snapshot(items, chain_timestamps.get(key) or [], listen_timestamp)
            strike = safe_float(item.get('strike'))
            if strike in seen_strikes:
                continue
            seen_strikes.add(strike)
            close_price = safe_float(item.get('close'))
            if close_price <= 0:
                continue
            diff = abs(close_price - target_premium)
            if diff < best_diff:
                best_diff = diff
                best_strike = int(strike)
    else:
        cursor = db._db[OPTION_CHAIN_COLLECTION].find(
            {
                'underlying': underlying,
                'expiry': expiry,
                'type': option_type,
                'timestamp': {'$lte': listen_timestamp},
            },
            {
                '_id': 0,
                'strike': 1,
                'close': 1,
                'timestamp': 1,
            },
        ).sort([('timestamp', DESCENDING)])
        for item in cursor:
            strike = safe_float(item.get('strike'))
            if strike in seen_strikes:
                continue
            seen_strikes.add(strike)
            close_price = safe_float(item.get('close'))
            if close_price <= 0:
                continue
            diff = abs(close_price - target_premium)
            if diff < best_diff:
                best_diff = diff
                best_strike = int(strike)
    return best_strike


def _extract_hhmm(raw_time: str) -> str:
    raw_value = str(raw_time or '').strip()
    if len(raw_value) >= 16:
        return raw_value[11:16]
    return raw_value[:5]
