"""
live_option_chain.py
────────────────────
Fetch the full live option chain (CE + PE) from Kite before taking entry.
Used for ALL entry types in forward/live activation mode.

Before any leg entry the caller fetches both sides, prints the combined table,
then calls select_strike_live() to pick the correct strike for any entry type.

Public API
──────────
  fetch_full_chain(db, underlying, expiry, spot_price, leg_id='')
    → {'CE': [rows], 'PE': [rows]}
    Each row: {strike, ltp, iv, delta, gamma, theta, vega, oi, token, symbol,
               bid, ask, oi_change_pct, ltp_change_pct}
    (bid/ask/oi_change_pct/ltp_change_pct are Dhan-only for now — 0 on Kite.)

  select_strike_live(chain, entry_kind, strike_param_raw, option_type,
                     position, spot_price, underlying, leg_id='')
    → {'strike', 'ltp', 'token', 'symbol', 'meta'} | None
"""

from __future__ import annotations

import ast
import logging
import math
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from features.debug_flags import entry_print

log = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

# ── module-level TTL cache shared across all callers ──────────────────────────
# Prevents duplicate Kite REST calls when _handle_entry_leg and
# process_momentum_pending_legs both run for the same leg in the same tick.
_CHAIN_CACHE: dict[tuple, tuple[dict, float]] = {}
_CHAIN_TTL_SECONDS = 2.0

# Previous trading day's final {close, oi} per option token — from
# option_chain_historical_data (the same backfill collection the paper-trade
# backtest replay reads), used to compute oi_change_pct/ltp_change_pct the
# same way live_greeks_chain_socket.py's _resolve_previous_close computes the
# underlying's change_pct. Cached generously (5 min) since yesterday's close
# never changes intraday — this only needs to be re-read once per session,
# not on every 2s chain refresh.
_BASELINE_CACHE: dict[tuple, tuple[dict, float]] = {}
_BASELINE_TTL_SECONDS = 300.0


def _baseline_key(strike: float, option_type: str) -> str:
    return f'{strike:g}|{option_type}'


def _resolve_previous_day_baseline(db, underlying: str, expiry: str) -> dict[str, dict]:
    """
    Keyed by (strike, option_type) — NOT token. active_option_tokens (live)
    stores bare broker security IDs ("44620"), while option_chain_historical_
    data (backfill) stores its own differently-formatted token ("NSE_2025110
    402072" style) for the same contract; joining on token silently matches
    nothing and every oi_change_pct/ltp_change_pct-from-baseline came back 0.
    strike+type are the one identifier both collections agree on for the
    same (underlying, expiry) contract.
    """
    cache_key = (underlying, expiry)
    cached = _BASELINE_CACHE.get(cache_key)
    if cached and (time.perf_counter() - cached[1]) < _BASELINE_TTL_SECONDS:
        return cached[0]

    today = datetime.now(IST).strftime('%Y-%m-%d')
    day_start = f'{today}T00:00:00'
    baseline: dict[str, dict] = {}
    try:
        pipeline = [
            {'$match': {
                'underlying': underlying,
                'expiry': expiry,
                'timestamp': {'$lt': day_start},
            }},
            {'$sort': {'timestamp': -1}},
            {'$group': {
                '_id': {'strike': '$strike', 'type': '$type'},
                'close': {'$first': '$close'},
                'oi': {'$first': '$oi'},
            }},
        ]
        for row in db['option_chain_historical_data'].aggregate(pipeline):
            _id = row['_id']
            key = _baseline_key(_safe_float(_id.get('strike')), str(_id.get('type') or ''))
            baseline[key] = {
                'close': _safe_float(row.get('close')),
                'oi': int(row.get('oi') or 0),
            }
    except Exception as exc:
        log.warning('[CHAIN BASELINE] underlying=%s expiry=%s error: %s', underlying, expiry, exc)

    _BASELINE_CACHE[cache_key] = (baseline, time.perf_counter())
    return baseline


# Per-process "first tick seen today" snapshot, used only when the DB has no
# prior-day row for a contract (e.g. a freshly-listed weekly expiry, or a
# backfill gap). Without this, oi_change_pct/ltp_change_pct sit at 0 all day
# for any contract the backfill hasn't caught up on yet, even though the live
# feed itself is moving tick to tick. Resets automatically at day rollover
# since the cache key includes today's date.
_INTRADAY_BASELINE: dict[tuple, dict[str, dict]] = {}


def _intraday_fallback_baseline(underlying: str, expiry: str, key: str, close: float, oi: int) -> dict:
    today = datetime.now(IST).strftime('%Y-%m-%d')
    day_map = _INTRADAY_BASELINE.setdefault((underlying, expiry, today), {})
    seen = day_map.get(key)
    if seen is None:
        seen = {'close': close, 'oi': oi}
        day_map[key] = seen
    return seen

# ── in-flight deduplication ───────────────────────────────────────────────────
# When two threads request the same (underlying, expiry) simultaneously, only the
# first thread fetches from Kite; the second waits for the first's result.
_CHAIN_FETCHING: dict[tuple, threading.Event] = {}
_CHAIN_MUTEX = threading.Lock()
_FIRST_FULL_CHAIN_PRINTED: set[tuple[str, str]] = set()


def _cache_get(key: tuple) -> dict | None:
    entry = _CHAIN_CACHE.get(key)
    if entry and (time.perf_counter() - entry[1]) < _CHAIN_TTL_SECONDS:
        return entry[0]
    return None


def _cache_set(key: tuple, chain: dict) -> None:
    _CHAIN_CACHE[key] = (chain, time.perf_counter())


# ── re-use BS helpers and broker credential helper from broker_gateway ─────────
def _bs():
    from features.broker_gateway import get_bs_helpers, get_broker_credentials_from_db  # type: ignore
    _calc_iv, _calc_greeks, _time_to_expiry, _RISK_FREE_RATE, _DIVIDEND_YIELDS, _DEFAULT_DIVIDEND_YIELD = get_bs_helpers()
    return (
        _calc_iv,
        _calc_greeks,
        _time_to_expiry,
        get_broker_credentials_from_db,   # same signature as _get_kite_credentials(db)
        _RISK_FREE_RATE,
        _DIVIDEND_YIELDS,
        _DEFAULT_DIVIDEND_YIELD,
    )


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _parse_sp(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and '{' in raw:
        try:
            return ast.literal_eval(raw)
        except Exception:
            pass
    return {}


def _is_sell(position: str) -> bool:
    return 'sell' in str(position or '').lower()


_STRIKE_STEP_MAP = {
    'NIFTY': 50, 'BANKNIFTY': 100, 'FINNIFTY': 50,
    'MIDCPNIFTY': 25, 'SENSEX': 100, 'BANKEX': 100,
}

def _atm_step(underlying: str) -> int:
    return _STRIKE_STEP_MAP.get(str(underlying or '').strip().upper(), 100)


def _atm_from_spot(spot: float, underlying: str) -> int:
    step = _atm_step(underlying)
    return int(round(spot / step) * step)


def _find_atm_row(rows: list[dict], atm_strike: float) -> dict:
    """Return the row whose strike is closest to atm_strike."""
    if not rows:
        return {}
    return min(rows, key=lambda r: abs(_safe_float(r.get('strike')) - atm_strike))


def _get_kite_rest_client(db) -> Any | None:
    """Return a configured broker REST client using the shared credential path."""
    try:
        from features.broker_gateway import get_broker_rest_client  # type: ignore
        return get_broker_rest_client(db)
    except Exception:
        return None


def _resolve_chain_reference_spot(
    rows_by_side: dict[str, dict[float, dict]],
    spot_price: float,
    T: float,
    r: float,
    q: float,
) -> float:
    """Mirror the live API reference-spot logic for identical chain pricing."""
    if spot_price <= 0:
        return 0.0

    ce_by_strike = rows_by_side.get('CE') or {}
    pe_by_strike = rows_by_side.get('PE') or {}
    common_strikes = [
        strike
        for strike in set(ce_by_strike) & set(pe_by_strike)
        if _safe_float((ce_by_strike.get(strike) or {}).get('ltp')) > 0
        and _safe_float((pe_by_strike.get(strike) or {}).get('ltp')) > 0
    ]
    if not common_strikes:
        return spot_price

    atm_strike = min(common_strikes, key=lambda strike: abs(strike - spot_price))
    ce_ltp = _safe_float((ce_by_strike.get(atm_strike) or {}).get('ltp'))
    pe_ltp = _safe_float((pe_by_strike.get(atm_strike) or {}).get('ltp'))
    synthetic_future = atm_strike + ce_ltp - pe_ltp
    if synthetic_future <= 0:
        return spot_price
    return synthetic_future * math.exp(-(r - q) * max(T, 0.0))


# ── fetch full chain from Kite ────────────────────────────────────────────────

def fetch_full_chain(
    db,
    underlying: str,
    expiry: str,
    spot_price: float,
    leg_id: str = '',
) -> dict[str, list[dict]]:
    """
    Fetch ALL strikes for the expiry from Kite (both CE and PE).
    Computes Black-Scholes IV + Greeks for each row.
    Prints the combined CE/PE chain table.
    Returns {'CE': [rows], 'PE': [rows]}.

    Results are cached for 2 seconds so multiple code paths in the same tick
    share one Kite REST call instead of each fetching independently.
    """
    _cache_key = (underlying, expiry)

    # Fast path: valid cache hit (no lock needed for read)
    _cached = _cache_get(_cache_key)
    if _cached is not None:
        return _cached

    # Serialize concurrent fetches for the same (underlying, expiry) key.
    with _CHAIN_MUTEX:
        # Re-check under lock — another thread may have just populated the cache.
        _cached = _cache_get(_cache_key)
        if _cached is not None:
            return _cached
        if _cache_key in _CHAIN_FETCHING:
            _wait_ev = _CHAIN_FETCHING[_cache_key]
            _is_fetcher = False
        else:
            _wait_ev = threading.Event()
            _CHAIN_FETCHING[_cache_key] = _wait_ev
            _is_fetcher = True

    if not _is_fetcher:
        _wait_ev.wait(timeout=10.0)
        _cached = _cache_get(_cache_key)
        if _cached is not None:
            return _cached
        return {'CE': [], 'PE': []}

    # We are the fetcher — route to correct broker.
    try:
        try:
            from features.broker_gateway import _active_broker  # type: ignore
            _broker = _active_broker()
        except Exception:
            _broker = 'kite'

        if _broker == 'dhan':
            try:
                return _fetch_full_chain_from_dhan(db, underlying, expiry, spot_price, leg_id)
            except Exception as exc:
                log.warning('[DHAN CHAIN] leg=%s chain fetch error underlying=%s expiry=%s: %s',
                            leg_id, underlying, expiry, exc)
                return {'CE': [], 'PE': []}

        return _fetch_full_chain_from_kite(
            db, underlying, expiry, spot_price, leg_id,
        )
    finally:
        with _CHAIN_MUTEX:
            _CHAIN_FETCHING.pop(_cache_key, None)
        _wait_ev.set()


def _fetch_full_chain_from_dhan(
    db,
    underlying: str,
    expiry: str,
    spot_price: float,
    leg_id: str,
) -> dict[str, list[dict]]:
    """
    Fetch option chain for Dhan broker.
    LTPs come from:
      1. dhan_ticker_manager.ltp_map (WebSocket, fastest)
      2. Dhan REST POST /v2/marketfeed/ltp (fallback when WS data absent)
    Spot comes from broker_ticker_manager.spot_map.
    Contracts come from active_option_tokens (broker=dhan).
    """
    _cache_key = (underlying, expiry)
    (
        _calc_iv, _calc_greeks, _time_to_expiry,
        _get_kite_credentials, _RISK_FREE_RATE, _DIVIDEND_YIELDS, _DEFAULT_DIVIDEND_YIELD,
    ) = _bs()

    # ── 1. Spot price ──────────────────────────────────────────────────────────
    try:
        from features.broker_gateway import broker_ticker_manager as _btm  # type: ignore
        ws_spot = float(_btm.spot_map.get(underlying) or 0)
        if ws_spot > 0:
            spot_price = ws_spot
    except Exception:
        pass

    # ── 2. Contracts from active_option_tokens (broker=dhan) ──────────────────
    contracts: list[dict] = []
    try:
        tok_col = db._db['active_option_tokens']
        contracts = list(tok_col.find(
            {
                'broker': 'dhan',
                'instrument': str(underlying or '').strip().upper(),
                'expiry': {'$regex': f'^{str(expiry or "").strip()[:10]}'},
            },
            {'_id': 0, 'strike': 1, 'option_type': 1, 'token': 1, 'tokens': 1, 'symbol': 1, 'ws_segment': 1},
        ))
    except Exception as exc:
        log.warning('[DHAN CHAIN] leg=%s active_option_tokens error: %s', leg_id, exc)

    if not contracts:
        log.warning('[DHAN CHAIN] leg=%s no contracts found underlying=%s expiry=%s',
                    leg_id, underlying, expiry)
        return {'CE': [], 'PE': []}

    ce_count = sum(1 for c in contracts if str(c.get('option_type') or '').upper() == 'CE')
    pe_count = sum(1 for c in contracts if str(c.get('option_type') or '').upper() == 'PE')
    # [DHAN CHAIN] underlying/expiry print suppressed

    # ── 3. LTP + OI via broker_gateway (WS first, REST /marketfeed/quote fallback) ──
    all_tok_ids = [str(c.get('token') or c.get('tokens') or '').strip() for c in contracts]
    all_tok_ids = [t for t in all_tok_ids if t]
    ws_segments = {
        str(c.get('token') or c.get('tokens') or '').strip(): str(c.get('ws_segment') or 'NSE_FNO')
        for c in contracts if c.get('token') or c.get('tokens')
    }

    # Warm the WHOLE chain on the dedicated chain-feed connection (not just the
    # strike this leg eventually picks) so the next delta/premium scan for this
    # underlying+expiry — this tick, or any later one — reads fresh LTP straight
    # from ltp_map instead of hitting the REST fallback for cold strikes. No-op
    # after the first call for a given chain (already-subscribed tokens are
    # filtered inside warm_chain_tokens). Isolated connection — never adds
    # latency to the live trade-execution tick path.
    try:
        # Via the proxy, NOT a direct dhan_ticker_manager import — in
        # central-tick mode (algo.trade/algo.simulator) this routes to
        # CentralTickClient.warm_chain_tokens(), which forwards to algo.
        # websocket's real chain-feed pool over HTTP. A direct import here
        # would silently warm nothing in those processes (their own local
        # dhan_ticker_manager is never started, so it has no credentials).
        from features.broker_gateway import broker_ticker_manager as _btm_warm  # type: ignore
        _by_segment: dict[str, list[str]] = {}
        for _tid in all_tok_ids:
            _by_segment.setdefault(ws_segments.get(_tid, 'NSE_FNO'), []).append(_tid)
        for _seg, _ids in _by_segment.items():
            _btm_warm.warm_chain_tokens(_ids, _seg)
    except Exception as exc:
        log.warning('[DHAN CHAIN] leg=%s chain warm error: %s', leg_id, exc)

    broker_quotes: dict[str, dict] = {}
    try:
        from features.broker_gateway import get_broker_rest_quotes  # type: ignore
        broker_quotes = get_broker_rest_quotes(all_tok_ids, db._db, ws_segments)
    except Exception as exc:
        log.warning('[DHAN CHAIN] leg=%s broker_quotes error: %s', leg_id, exc)
    ltp_count = sum(1 for v in broker_quotes.values() if v.get('ltp', 0) > 0)
    # [DHAN CHAIN] quotes print suppressed

    # ── 3c. Depth (bid/ask) + previous-day baseline (for oi/ltp change%) ──────
    # WS-first (chain tokens now subscribe REQ_FULL_SUB, same as live
    # positions — see dhan_ticker.py's _handle_chain_binary): any token
    # that's actually ticking already has bid/ask/prev_close in
    # broker_ticker_manager's maps, zero REST, zero rate-gate. Only tokens
    # genuinely missing from WS (a chain opened this instant, before its
    # first Full packet arrived) fall back to the REST path below; the
    # previous-day baseline is a once-per-session Mongo read (long-TTL
    # cached in this module), unrelated to either.
    broker_depth: dict[str, dict] = {}
    try:
        from features.broker_gateway import get_broker_ws_depth, get_broker_rest_depth  # type: ignore
        broker_depth = get_broker_ws_depth(all_tok_ids)
        _missing_depth = [t for t in all_tok_ids if t not in broker_depth]
        if _missing_depth:
            broker_depth.update(get_broker_rest_depth(_missing_depth, db._db, ws_segments))
    except Exception as exc:
        log.warning('[DHAN CHAIN] leg=%s broker_depth error: %s', leg_id, exc)
    baseline = _resolve_previous_day_baseline(db._db, str(underlying or '').strip().upper(), expiry)

    # ── 3b. Commodity fallback: options-on-futures have no index "spot" ──────
    # (no market_feed_tokens "spot" entry for MCX underlyings), so spot_map
    # lookup in step 1 stays 0 for them. Use the corresponding FUTCOM
    # contract's LTP instead — FUT contracts are bi-monthly but options are
    # ~monthly, so they never share an exact expiry; query separately for the
    # nearest FUT expiring on/after this option's expiry (the future this
    # option actually settles against), not an exact-date match. Without
    # this, every commodity option would show ltp but iv/delta/etc. all
    # zeroed out (see the "ltp > 0 and spot_price > 0" gate below).
    if spot_price <= 0:
        try:
            _fut_doc = tok_col.find_one(
                {
                    'broker': 'dhan', 'instrument': str(underlying or '').strip().upper(), 'option_type': 'FUT',
                    'expiry': {'$gte': str(expiry or '').strip()[:10]},
                },
                {'_id': 0, 'token': 1, 'tokens': 1, 'ws_segment': 1},
                sort=[('expiry', 1)],
            )
            if _fut_doc:
                _fut_tok = str(_fut_doc.get('token') or _fut_doc.get('tokens') or '').strip()
                if _fut_tok:
                    from features.broker_gateway import get_broker_rest_quotes as _get_fut_quote  # type: ignore
                    _fut_quotes = _get_fut_quote([_fut_tok], db._db, {_fut_tok: str(_fut_doc.get('ws_segment') or 'MCX_COMM')})
                    _fut_ltp = _safe_float((_fut_quotes.get(_fut_tok) or {}).get('ltp'))
                    if _fut_ltp > 0:
                        spot_price = _fut_ltp
                        pass  # [DHAN CHAIN] FUT spot fallback print suppressed
        except Exception as exc:
            log.warning('[DHAN CHAIN] leg=%s commodity FUT spot fallback error: %s', leg_id, exc)

    # ── 4. Build chain rows ────────────────────────────────────────────────────
    T = _time_to_expiry(expiry)
    r = _RISK_FREE_RATE
    q_yield = _DIVIDEND_YIELDS.get(str(underlying or '').strip().upper(), _DEFAULT_DIVIDEND_YIELD)

    chain: dict[str, list[dict]] = {'CE': [], 'PE': []}
    for contract in contracts:
        opt = str(contract.get('option_type') or '').strip().upper()
        if opt not in ('CE', 'PE'):
            continue
        stk = _safe_float(contract.get('strike'))
        tok = str(contract.get('token') or contract.get('tokens') or '').strip()
        sym = str(contract.get('symbol') or '').strip()
        if not stk or not tok:
            continue
        bq = broker_quotes.get(tok) or {}
        depth = broker_depth.get(tok) or {}
        base = baseline.get(_baseline_key(stk, opt)) or {}
        ltp = _safe_float(bq.get('ltp'))
        oi = int(bq.get('oi') or 0)
        if ltp > 0 and spot_price > 0:
            iv = _calc_iv(ltp, spot_price, stk, T, r, opt, q_yield)
            greeks = _calc_greeks(spot_price, stk, T, r, iv, opt, q_yield)
        else:
            iv, greeks = 0.0, {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0}
        # prev_close from today's live depth quote (Dhan's own ohlc.close) is
        # preferred over the historical backfill's close — same trading day,
        # zero backfill lag — falling back to the backfill only when the
        # depth REST call hasn't resolved this token yet (e.g. just opened).
        prev_ltp = _safe_float(depth.get('prev_close')) or _safe_float(base.get('close'))
        prev_oi = int(base.get('oi') or 0)
        if not prev_ltp and not prev_oi and ltp > 0:
            # No DB baseline for this contract (fresh expiry / backfill gap) —
            # fall back to the first live tick seen today so the % columns
            # track intraday movement instead of sitting frozen at 0.
            fallback = _intraday_fallback_baseline(str(underlying or '').strip().upper(), expiry, _baseline_key(stk, opt), ltp, oi)
            prev_ltp = fallback['close'] or prev_ltp
            prev_oi = fallback['oi'] or prev_oi
        ltp_change_pct = round((ltp - prev_ltp) / prev_ltp * 100, 2) if prev_ltp else 0.0
        oi_change_pct = round((oi - prev_oi) / prev_oi * 100, 2) if prev_oi else 0.0
        chain[opt].append({
            'strike': stk,
            'ltp':    ltp,
            'iv':     round(iv * 100, 2),
            'delta':  greeks['delta'],
            'gamma':  greeks['gamma'],
            'theta':  greeks['theta'],
            'vega':   greeks['vega'],
            'oi':     oi,
            'bid':    _safe_float(depth.get('bid')),
            'ask':    _safe_float(depth.get('ask')),
            'oi_change_pct':  oi_change_pct,
            'ltp_change_pct': ltp_change_pct,
            'volume': 0,
            'token':  tok,
            'symbol': sym,
        })

    chain['CE'].sort(key=lambda r: _safe_float(r.get('strike')))
    chain['PE'].sort(key=lambda r: _safe_float(r.get('strike')))

    ltp_count = sum(1 for row in chain['CE'] + chain['PE'] if row.get('ltp', 0) > 0)
    # [DHAN CHAIN] chain_built print suppressed
    # _print_combined_table suppressed
    _cache_set(_cache_key, chain)
    return chain


def _fetch_full_chain_from_kite(
    db,
    underlying: str,
    expiry: str,
    spot_price: float,
    leg_id: str,
) -> dict[str, list[dict]]:
    _cache_key = (underlying, expiry)
    (
        _calc_iv,
        _calc_greeks,
        _time_to_expiry,
        _get_kite_credentials,
        _RISK_FREE_RATE,
        _DIVIDEND_YIELDS,
        _DEFAULT_DIVIDEND_YIELD,
    ) = _bs()

    api_key, access_token = _get_kite_credentials(db)
    if not api_key or not access_token:
        log.warning('[LIVE CHAIN] leg=%s no Kite credentials', leg_id)
        return {'CE': [], 'PE': []}

    T = _time_to_expiry(expiry)
    r = _RISK_FREE_RATE
    q_yield = _DIVIDEND_YIELDS.get(str(underlying or '').strip().upper(), _DEFAULT_DIVIDEND_YIELD)

    # ── load contracts from the same source used by the live-greeks API ─────
    contracts: list[dict] = []
    try:
        tok_col = db._db['active_option_tokens']
        contracts = list(tok_col.find(
            {'instrument': str(underlying or '').strip().upper(), 'expiry': {'$regex': f'^{str(expiry or "").strip()[:10]}'}},
            {'_id': 0, 'strike': 1, 'option_type': 1, 'token': 1, 'tokens': 1, 'symbol': 1},
        ))
    except Exception as exc:
        log.warning('[LIVE CHAIN] leg=%s active_option_tokens read error: %s', leg_id, exc)

    if not contracts:
        log.warning('[LIVE CHAIN] leg=%s no contracts found underlying=%s expiry=%s',
                    leg_id, underlying, expiry)
        return {'CE': [], 'PE': []}

    ce_count = sum(1 for c in contracts if str(c.get('option_type') or '').strip().upper() == 'CE')
    pe_count = sum(1 for c in contracts if str(c.get('option_type') or '').strip().upper() == 'PE')
    total    = ce_count + pe_count
    # [LIVE CHAIN] Getting option chain print suppressed

    import time as _time
    _t0 = _time.perf_counter()

    kite = _get_kite_rest_client(db)
    token_to_quote: dict[str, dict] = {}
    if kite:
        try:
            index_symbols = {
                'NIFTY': 'NSE:NIFTY 50',
                'BANKNIFTY': 'NSE:NIFTY BANK',
                'FINNIFTY': 'NSE:NIFTY FIN SERVICE',
                'SENSEX': 'BSE:SENSEX',
                'MIDCPNIFTY': 'NSE:NIFTY MID SELECT',
            }
            index_sym = index_symbols.get(str(underlying or '').strip().upper(), '')
            if index_sym:
                idx_q = kite.quote([index_sym]) or {}
                rest_spot_price = _safe_float((idx_q.get(index_sym) or {}).get('last_price'))
                if rest_spot_price > 0:
                    spot_price = rest_spot_price
        except Exception as exc:
            log.warning('[LIVE CHAIN] leg=%s spot quote error: %s', leg_id, exc)

        all_tokens = [
            int(str(c.get('token') or c.get('tokens') or 0))
            for c in contracts
            if c.get('token') or c.get('tokens')
        ]
        all_tokens = [tok for tok in all_tokens if tok]
        for i in range(0, len(all_tokens), 500):
            try:
                quotes = kite.quote(all_tokens[i:i + 500]) or {}
                for _sym, quote in quotes.items():
                    token = str(quote.get('instrument_token') or '').strip()
                    if token:
                        token_to_quote[token] = quote
            except Exception as exc:
                log.warning('[LIVE CHAIN] leg=%s quote batch[%d] error: %s', leg_id, i, exc)

    _elapsed_ms = round((_time.perf_counter() - _t0) * 1000, 1)
    # [LIVE CHAIN] Got option chain print suppressed

    rows_by_side: dict[str, dict[float, dict]] = {'CE': {}, 'PE': {}}
    raw_rows: list[dict] = []
    for contract in contracts:
        opt = str(contract.get('option_type') or '').strip().upper()
        if opt not in ('CE', 'PE'):
            continue
        stk = _safe_float(contract.get('strike'))
        tok = str(contract.get('token') or contract.get('tokens') or '').strip()
        sym = str(contract.get('symbol') or '').strip()
        if not stk or not tok:
            continue
        quote = token_to_quote.get(tok) or {}
        ltp = _safe_float(quote.get('last_price'))
        if ltp == 0:
            ltp = _safe_float((quote.get('ohlc') or {}).get('close'))
        oi = int(quote.get('oi') or 0)
        vol = int(quote.get('volume') or 0)
        row = {
            'opt': opt,
            'strike': stk,
            'ltp': ltp,
            'oi': oi,
            'volume': vol,
            'token': tok,
            'symbol': sym,
        }
        raw_rows.append(row)
        rows_by_side[opt][stk] = row

    pricing_spot = _resolve_chain_reference_spot(rows_by_side, spot_price, T, r, q_yield) or spot_price

    # ── compute Greeks per side ───────────────────────────────────────────────
    chain: dict[str, list[dict]] = {'CE': [], 'PE': []}
    for row in raw_rows:
        opt = str(row.get('opt') or '')
        stk = _safe_float(row.get('strike'))
        ltp = _safe_float(row.get('ltp'))
        if pricing_spot > 0 and ltp > 0:
            iv = _calc_iv(ltp, pricing_spot, stk, T, r, opt, q_yield)
            greeks = _calc_greeks(pricing_spot, stk, T, r, iv, opt, q_yield)
        else:
            iv, greeks = 0.0, {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0}
        chain[opt].append({
            'strike': stk,
            'ltp':    ltp,
            'iv':     round(iv * 100, 2),
            'delta':  greeks['delta'],
            'gamma':  greeks['gamma'],
            'theta':  greeks['theta'],
            'vega':   greeks['vega'],
            'oi':     int(row.get('oi') or 0),
            'bid':    0.0,
            'ask':    0.0,
            'oi_change_pct':  0.0,
            'ltp_change_pct': 0.0,
            'volume': int(row.get('volume') or 0),
            'token':  str(row.get('token') or ''),
            'symbol': str(row.get('symbol') or ''),
        })

    chain['CE'].sort(key=lambda item: _safe_float(item.get('strike')))
    chain['PE'].sort(key=lambda item: _safe_float(item.get('strike')))

    # _print_combined_table suppressed
    _cache_set(_cache_key, chain)
    return chain


# ── per-strike delta fetch for pre-entry check ───────────────────────────────

def get_live_delta_for_strike(
    db,
    underlying: str,
    expiry: str,
    strike: float,
    option_type: str,
) -> dict:
    """
    Fetch live delta for a single strike using WS ltp_map (primary) + Kite REST
    (fallback) — same pricing source as the /live-greeks-chain API.

    Returns {'delta': float, 'ltp': float, 'iv': float} or {} on failure.
    """
    (
        _calc_iv, _calc_greeks, _time_to_expiry,
        _get_kite_credentials, _RISK_FREE_RATE,
        _DIVIDEND_YIELDS, _DEFAULT_DIVIDEND_YIELD,
    ) = _bs()

    opt = str(option_type or '').strip().upper()
    und = str(underlying or '').strip().upper()
    exp = str(expiry or '').strip()[:10]
    stk = _safe_float(strike)
    if not opt or not und or not exp or not stk:
        return {}

    # ── Step 1: spot price from WS ltp_map ────────────────────────────────────
    try:
        from features.broker_gateway import get_broker_ltp_map, BROKER_INDEX_TOKENS  # type: ignore
        ltp_map: dict = get_broker_ltp_map() or {}
        idx_token = str(BROKER_INDEX_TOKENS.get(und, 0))
        spot_price = _safe_float(ltp_map.get(idx_token))
    except Exception:
        ltp_map = {}
        spot_price = 0.0

    # ── Step 2: load token for this strike from active_option_tokens ──────────
    token = ''
    symbol = ''
    try:
        tok_col = db._db['active_option_tokens']
        doc = tok_col.find_one(
            {
                'instrument': und,
                'expiry': {'$regex': f'^{exp}'},
                'option_type': opt,
                'strike': {'$in': [stk, int(stk), str(int(stk)), str(stk)]},
            },
            {'_id': 0, 'token': 1, 'tokens': 1, 'symbol': 1},
        )
        if doc:
            token = str(doc.get('token') or doc.get('tokens') or '').strip()
            symbol = str(doc.get('symbol') or '').strip()
    except Exception as exc:
        log.warning('[DELTA CHECK] active_option_tokens error: %s', exc)

    if not token:
        return {}

    # ── Step 3: ltp — WS first, Kite REST fallback ────────────────────────────
    ltp = _safe_float(ltp_map.get(token)) if ltp_map else 0.0
    if ltp <= 0:
        kite = _get_kite_rest_client(db)
        if kite:
            try:
                q = (kite.quote([int(token)]) or {})
                for _, qdata in q.items():
                    ltp = _safe_float(qdata.get('last_price'))
                    if ltp <= 0:
                        ltp = _safe_float((qdata.get('ohlc') or {}).get('close'))
                    if spot_price <= 0:
                        pass  # spot already from WS; don't override
            except Exception as exc:
                log.warning('[DELTA CHECK] kite quote error: %s', exc)

    if ltp <= 0 or spot_price <= 0:
        return {}

    # ── Step 4: Black-Scholes Greeks ──────────────────────────────────────────
    T = _time_to_expiry(exp)
    r = _RISK_FREE_RATE
    q_yield = _DIVIDEND_YIELDS.get(und, _DEFAULT_DIVIDEND_YIELD)
    try:
        iv = _calc_iv(ltp, spot_price, stk, T, r, opt, q_yield)
        greeks = _calc_greeks(spot_price, stk, T, r, iv, opt, q_yield)
    except Exception as exc:
        log.warning('[DELTA CHECK] greeks calc error: %s', exc)
        return {}

    return {
        'delta': greeks['delta'],
        'ltp': ltp,
        'iv': round(iv * 100, 2),
        'symbol': symbol,
    }


# ── print combined CE / PE table ──────────────────────────────────────────────

def _print_combined_table(
    chain: dict[str, list[dict]],
    underlying: str,
    expiry: str,
    spot_price: float,
    leg_id: str,
) -> None:
    ce_by_strike = {r['strike']: r for r in chain.get('CE', [])}
    pe_by_strike = {r['strike']: r for r in chain.get('PE', [])}
    all_strikes  = sorted(set(ce_by_strike) | set(pe_by_strike))

    cache_key = (str(underlying or '').strip().upper(), str(expiry or '').strip()[:10])
    should_print_stdout = cache_key not in _FIRST_FULL_CHAIN_PRINTED
    sep = '[LIVE CHAIN] ' + '─' * 110

    def _emit(line: str) -> None:
        entry_print(line)
        if should_print_stdout:
            print(line, flush=True)

    _emit(
        f'\n[LIVE CHAIN] leg={leg_id}  {underlying}  expiry={expiry}  '
        f'spot={spot_price}  strikes={len(all_strikes)}'
    )
    _emit(sep)
    _emit(
        f'[LIVE CHAIN] {"CE_LTP":>9}  {"CE_IV%":>7}  {"CE_Delta":>9}  │'
        f'  {"STRIKE":>7}  │  {"PE_Delta":>9}  {"PE_IV%":>7}  {"PE_LTP":>9}'
    )
    _emit(sep)
    atm = _atm_from_spot(spot_price, underlying)
    for s in all_strikes:
        ce = ce_by_strike.get(s, {})
        pe = pe_by_strike.get(s, {})
        atm_marker = ' ←ATM' if int(s) == atm else ''
        _emit(
            f'[LIVE CHAIN] {_safe_float(ce.get("ltp")):>9.2f}  '
            f'{_safe_float(ce.get("iv")):>7.2f}  '
            f'{_safe_float(ce.get("delta")):>9.4f}  │'
            f'  {int(s):>7}  │  '
            f'{_safe_float(pe.get("delta")):>9.4f}  '
            f'{_safe_float(pe.get("iv")):>7.2f}  '
            f'{_safe_float(pe.get("ltp")):>9.2f}'
            f'{atm_marker}'
        )
    _emit(sep + '\n')
    if should_print_stdout:
        _FIRST_FULL_CHAIN_PRINTED.add(cache_key)


# ── strike selection from live chain ─────────────────────────────────────────

def select_strike_live(
    chain: dict[str, list[dict]],
    entry_kind: str,
    strike_param_raw: Any,
    option_type: str,
    position: str,
    spot_price: float,
    underlying: str,
    leg_id: str = '',
) -> dict | None:
    """
    Select a strike from the live chain based on entry_kind.
    Returns {'strike', 'ltp', 'token', 'symbol', 'meta'} or None.
    """
    opt      = option_type.upper()
    rows     = chain.get(opt, [])
    ce_rows  = chain.get('CE', [])
    pe_rows  = chain.get('PE', [])
    step     = _atm_step(underlying)
    atm      = _atm_from_spot(spot_price, underlying)
    sp       = _parse_sp(strike_param_raw)

    if not rows:
        log.warning('[LIVE SELECT] leg=%s no rows for opt=%s', leg_id, opt)
        return None

    # ── helpers ───────────────────────────────────────────────────────────────
    def _row_for_strike(target_strike: float) -> dict | None:
        if not rows:
            return None
        valid = [r for r in rows if _safe_float(r.get('ltp')) > 0]
        pool  = valid if valid else rows
        return min(pool, key=lambda r: abs(_safe_float(r.get('strike')) - target_strike))

    def _result(row: dict, meta: dict | None = None) -> dict:
        return {
            'strike': _safe_float(row.get('strike')),
            'ltp':    _safe_float(row.get('ltp')),
            'token':  str(row.get('token') or ''),
            'symbol': str(row.get('symbol') or ''),
            'iv':     _safe_float(row.get('iv')),
            'delta':  _safe_float(row.get('delta')),
            'meta':   meta or {},
        }

    # ── PremiumCloseToStraddle ────────────────────────────────────────────────
    if 'PremiumCloseToStraddle' in entry_kind:
        multiplier = _safe_float(sp.get('Multiplier') or 0.5)
        atm_ce = _find_atm_row(ce_rows, atm)
        atm_pe = _find_atm_row(pe_rows, atm)
        ce_ltp = _safe_float(atm_ce.get('ltp'))
        pe_ltp = _safe_float(atm_pe.get('ltp'))
        straddle = ce_ltp + pe_ltp
        if straddle <= 0:
            log.warning('[LIVE SELECT] leg=%s straddle=0 — skipping', leg_id)
            return None
        target = straddle * multiplier
        row = min(rows, key=lambda r: abs(_safe_float(r.get('ltp')) - target))
        entry_print(
            f'[LIVE SELECT] leg={leg_id} method=StraddlePct '
            f'atm={atm} ce={ce_ltp} pe={pe_ltp} straddle={round(straddle,2)} '
            f'target={round(target,2)} → strike={row.get("strike")} ltp={row.get("ltp")}'
        )
        return _result(row, {
            'atm_strike': atm, 'ce_atm_price': ce_ltp, 'pe_atm_price': pe_ltp,
            'straddle': round(straddle, 2), 'target': round(target, 2), 'multiplier': multiplier,
        })

    # ── StraddlePrice ─────────────────────────────────────────────────────────
    if 'StraddlePrice' in entry_kind:
        multiplier = _safe_float(sp.get('Multiplier') or 0.5)
        adjustment = str(sp.get('Adjustment') or 'AdjustmentType.Plus')
        is_plus    = 'Minus' not in adjustment
        atm_ce = _find_atm_row(ce_rows, atm)
        atm_pe = _find_atm_row(pe_rows, atm)
        straddle = _safe_float(atm_ce.get('ltp')) + _safe_float(atm_pe.get('ltp'))
        offset     = multiplier * straddle
        raw_strike = atm + offset if is_plus else atm - offset
        final_str  = int(round(raw_strike / step) * step)
        row = _row_for_strike(final_str)
        if not row:
            return None
        entry_print(
            f'[LIVE SELECT] leg={leg_id} method=StraddlePrice '
            f'straddle={round(straddle,2)} offset={round(offset,2)} '
            f'{"+" if is_plus else "-"} → strike={final_str}'
        )
        return _result(row, {
            'atm_strike':    atm,
            'ce_atm_price':  round(_safe_float(atm_ce.get('ltp')), 2),
            'pe_atm_price':  round(_safe_float(atm_pe.get('ltp')), 2),
            'straddle':      round(straddle, 2),
            'multiplier':    multiplier,
            'offset':        round(offset, 2),
            'adjustment':    '+' if is_plus else '-',
        })

    # ── SyntheticFuture ───────────────────────────────────────────────────────
    if 'SyntheticFuture' in entry_kind:
        import re as _re
        atm_ce = _find_atm_row(ce_rows, atm)
        atm_pe = _find_atm_row(pe_rows, atm)
        ce_ltp = _safe_float(atm_ce.get('ltp'))
        pe_ltp = _safe_float(atm_pe.get('ltp'))
        syn_future = atm - pe_ltp + ce_ltp
        syn_atm    = int(round(syn_future / step) * step)
        _sp_str  = str(strike_param_raw or '')
        m  = _re.search(r'OTM(\d+)', _sp_str)
        m2 = _re.search(r'ITM(\d+)', _sp_str)
        _raw_offset = int(m.group(1)) if m else (-int(m2.group(1)) if m2 else 0)
        # PE: OTM is below syn_atm, ITM is above syn_atm
        offset_n  = -_raw_offset if opt == 'PE' else _raw_offset
        final_str = syn_atm + offset_n * step
        row = _row_for_strike(final_str)
        if not row:
            return None
        _syn_label = f'OTM{abs(_raw_offset)}' if _raw_offset > 0 else (f'ITM{abs(_raw_offset)}' if _raw_offset < 0 else 'ATM')
        entry_print(
            f'[LIVE SELECT] leg={leg_id} method=SyntheticFuture+{_syn_label} '
            f'ce={ce_ltp} pe={pe_ltp} syn={round(syn_future,2)} '
            f'syn_atm={syn_atm} offset={offset_n} → strike={final_str}'
        )
        return _result(row, {
            'atm_strike': atm, 'ce_atm_price': ce_ltp, 'pe_atm_price': pe_ltp,
            'synthetic_future': round(syn_future, 2),
        })

    # ── AtmMultiplier ─────────────────────────────────────────────────────────
    if 'AtmMultiplier' in entry_kind:
        multiplier  = _safe_float(strike_param_raw) if not isinstance(strike_param_raw, dict) else 1.0
        raw_strike  = atm * multiplier
        final_str   = int(round(raw_strike / step) * step)
        row = _row_for_strike(final_str)
        if not row:
            entry_print(f'[LIVE SELECT] leg={leg_id} method=AtmMultiplier no chain row for strike={final_str} — skipped')
            return None
        actual_strike = _safe_float(row.get('strike'))
        if abs(actual_strike - final_str) > step:
            entry_print(
                f'[LIVE SELECT] leg={leg_id} method=AtmMultiplier '
                f'target={final_str} nearest={actual_strike} gap={abs(actual_strike - final_str)} > step={step} — skipped'
            )
            return None
        pct = round((multiplier - 1) * 100, 4)
        entry_print(f'[LIVE SELECT] leg={leg_id} method=AtmMultiplier atm={atm} mult={multiplier} pct={pct:+.4g}% → strike={final_str}')
        return _result(row, {'atm_strike': atm, 'multiplier': multiplier})

    # ── PremiumRange ──────────────────────────────────────────────────────────
    if 'PremiumRange' in entry_kind:
        lower = _safe_float(sp.get('LowerRange') or 0)
        upper = _safe_float(sp.get('UpperRange') or 0)
        mid   = (lower + upper) / 2
        valid = [r for r in rows if lower <= _safe_float(r.get('ltp')) <= upper]
        if not valid:
            entry_print(f'[LIVE SELECT] leg={leg_id} method=PremiumRange no strikes in [{lower},{upper}] — skipped')
            return None
        row = min(valid, key=lambda r: abs(_safe_float(r.get('ltp')) - mid))
        entry_print(f'[LIVE SELECT] leg={leg_id} method=PremiumRange [{lower},{upper}] mid={mid} → strike={row.get("strike")} ltp={row.get("ltp")}')
        return _result(row, {'lower_range': lower, 'upper_range': upper})

    # ── PremiumGEQ / PremiumLTE / Premium ─────────────────────────────────────
    if 'Premium' in entry_kind:
        target_val = _safe_float(strike_param_raw) if not isinstance(strike_param_raw, dict) else 0.0
        ek_lower   = entry_kind.lower()
        is_geq     = 'geq' in ek_lower
        is_lte     = 'lte' in ek_lower or 'leq' in ek_lower
        is_closest = not is_geq and not is_lte  # plain EntryByPremium → Closest Premium

        if is_closest:
            # pick the strike whose ltp is absolutely nearest to target (above OR below)
            below = sorted(
                [r for r in rows if _safe_float(r.get('ltp')) <= target_val],
                key=lambda r: _safe_float(r.get('ltp')), reverse=True
            )
            above = sorted(
                [r for r in rows if _safe_float(r.get('ltp')) >= target_val],
                key=lambda r: _safe_float(r.get('ltp'))
            )
            b = below[0] if below else None
            a = above[0] if above else None
            if not b and not a:
                entry_print(f'[LIVE SELECT] leg={leg_id} method=ClosestPremium target={target_val} — no strikes found, skipped')
                return None
            if not b:
                row = a
            elif not a:
                row = b
            else:
                diff_b = abs(_safe_float(b.get('ltp')) - target_val)
                diff_a = abs(_safe_float(a.get('ltp')) - target_val)
                row = b if diff_b <= diff_a else a  # on tie pick below (conservative)
            entry_print(f'[LIVE SELECT] leg={leg_id} method=ClosestPremium target={target_val} → strike={row.get("strike")} ltp={row.get("ltp")} diff={round(abs(_safe_float(row.get("ltp")) - target_val), 2)}')
        elif is_geq:
            candidates = sorted(
                [r for r in rows if _safe_float(r.get('ltp')) >= target_val],
                key=lambda r: _safe_float(r.get('ltp'))
            )
            if not candidates:
                entry_print(f'[LIVE SELECT] leg={leg_id} method=Premium no strike ≥ {target_val} — skipped')
                return None
            row = candidates[0]
            entry_print(f'[LIVE SELECT] leg={leg_id} method=Premium ≥{target_val} → strike={row.get("strike")} ltp={row.get("ltp")}')
        else:
            candidates = sorted(
                [r for r in rows if _safe_float(r.get('ltp')) <= target_val],
                key=lambda r: _safe_float(r.get('ltp')), reverse=True
            )
            if not candidates:
                entry_print(f'[LIVE SELECT] leg={leg_id} method=Premium no strike ≤ {target_val} — skipped')
                return None
            row = candidates[0]
            entry_print(f'[LIVE SELECT] leg={leg_id} method=Premium ≤{target_val} → strike={row.get("strike")} ltp={row.get("ltp")}')
        return _result(row)

    # ── DeltaRange ────────────────────────────────────────────────────────────
    if 'DeltaRange' in entry_kind:
        from features.delta_selector import select_delta_range, print_delta_chain_table  # type: ignore
        lower_pct = _safe_float(sp.get('LowerRange') or 0)
        upper_pct = _safe_float(sp.get('UpperRange') or 0)
        print_delta_chain_table(rows, underlying, '', opt, 'EntryByDeltaRange', leg_id, spot_price)
        chosen = select_delta_range(rows, lower_pct, upper_pct, opt, position, leg_id, spot_price)
        if not chosen:
            return None
        return _result(chosen, {'lower_pct': lower_pct, 'upper_pct': upper_pct,
                                 'selected_delta': _safe_float(chosen.get('delta'))})

    # ── Delta (closest) ───────────────────────────────────────────────────────
    if 'Delta' in entry_kind:
        from features.delta_selector import select_closest_delta, print_delta_chain_table  # type: ignore
        target_pct = _safe_float(strike_param_raw) if not isinstance(strike_param_raw, dict) else 50.0
        print_delta_chain_table(rows, underlying, '', opt, 'EntryByDelta', leg_id, spot_price)
        chosen = select_closest_delta(rows, target_pct, opt, leg_id)
        if not chosen:
            return None
        return _result(chosen, {'target_delta_pct': target_pct,
                                 'selected_delta': _safe_float(chosen.get('delta'))})

    # ── ATM / ATM offset (StrikeType.OTMn / ITMn) ────────────────────────────
    import re as _re
    sp_str   = str(strike_param_raw or '')
    m_otm    = _re.search(r'OTM(\d+)', sp_str)
    m_itm    = _re.search(r'ITM(\d+)', sp_str)
    _raw_offset = int(m_otm.group(1)) if m_otm else (-int(m_itm.group(1)) if m_itm else 0)
    # PE direction is reversed: OTM is below ATM, ITM is above ATM
    offset_n  = -_raw_offset if opt == 'PE' else _raw_offset
    final_str = atm + offset_n * step
    label = f'OTM{abs(_raw_offset)}' if _raw_offset > 0 else (f'ITM{abs(_raw_offset)}' if _raw_offset < 0 else 'ATM')

    # print available strikes near target so mismatch is easy to diagnose
    near = sorted(
        [_safe_float(r.get('strike')) for r in rows
         if abs(_safe_float(r.get('strike')) - final_str) <= step * 2],
    )
    entry_print(f'[LIVE SELECT] leg={leg_id} method={label} spot={spot_price} atm={atm} target={final_str} strikes_near_target={near}')

    row = _row_for_strike(final_str)
    if not row:
        return None
    actual_strike = _safe_float(row.get('strike'))
    if actual_strike != final_str:
        entry_print(
            f'[LIVE SELECT] leg={leg_id} method={label} WARNING: '
            f'target={final_str} NOT in chain → nearest={actual_strike} selected instead'
        )
    entry_print(f'[LIVE SELECT] leg={leg_id} method={label} → strike={actual_strike} ltp={_safe_float(row.get("ltp"))}')
    return _result(row, {'atm_strike': atm, 'offset': offset_n})
