"""
algo_backtest_event.py
──────────────────────
Single source of truth for ALL backtest market-data access.
Job: fetch spot price and option LTP from historical DB — nothing else.

Every backtest DB read for chain / spot data must go through this file.
execution_socket.py handles all trading logic (SL/TP/trail/overall/re-entry);
this file only supplies raw market data.

Public API
──────────
  Raw doc lookups (return full chain/spot document):
    get_latest_chain_doc(chain_col, underlying, expiry, strike, option_type, trade_date, market_cache) → dict
    get_chain_doc_at_time(chain_col, underlying, expiry, strike, option_type, snapshot_ts, market_cache) → dict
    get_chain_doc_by_token(chain_col, token, snapshot_ts) → dict
    get_spot_doc_at_time(index_spot_col, underlying, snapshot_ts, market_cache) → dict

  Price helpers (return float / list):
    get_spot_price(index_spot_col, underlying, snapshot_ts, market_cache) → float
    get_option_ltp(chain_col, underlying, expiry, strike, option_type, snapshot_ts, market_cache, fallback) → float
    get_open_legs_ltp_array(chain_col, open_legs, underlying, snapshot_ts, market_cache) → list[dict]
"""

from __future__ import annotations

import re
from typing import Any

from pymongo import DESCENDING

OPTION_CHAIN_COLLECTION = 'option_chain_historical_data'
INDEX_SPOT_COLLECTION   = 'option_chain_index_spot'
OPEN_LEG_STATUS = 1


# ─── Raw chain doc lookups ────────────────────────────────────────────────────

def get_latest_chain_doc(
    chain_col,
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    trade_date: str,
    market_cache: dict | None = None,
) -> dict:
    """Latest chain doc for a contract on trade_date (no timestamp filter)."""
    from features.spot_atm_utils import get_cached_chain_doc
    try:
        cached = get_cached_chain_doc(market_cache, underlying, expiry, strike, option_type)
        if cached:
            return cached
        doc = chain_col.find_one(
            {
                'underlying': underlying,
                'expiry': expiry,
                'strike': float(strike) if strike is not None else None,
                'type': option_type,
                'timestamp': {'$regex': f'^{trade_date}'},
            },
            sort=[('timestamp', DESCENDING)],
        )
        return doc or {}
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning('get_latest_chain_doc error: %s', exc)
        return {}


def get_chain_doc_at_time(
    chain_col,
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
) -> dict:
    """Chain doc closest to snapshot_ts (≤ snapshot_ts). Used by backtest minute tick."""
    from features.spot_atm_utils import get_cached_chain_doc
    try:
        cached = get_cached_chain_doc(market_cache, underlying, expiry, strike, option_type, snapshot_ts)
        if cached:
            return cached
        base: dict[str, Any] = {
            'underlying': underlying,
            'expiry': expiry,
            'strike': float(strike) if strike is not None else None,
            'type': option_type,
        }
        doc = chain_col.find_one({**base, 'timestamp': snapshot_ts})
        if not doc:
            doc = chain_col.find_one(
                {**base, 'timestamp': {'$lte': snapshot_ts}},
                sort=[('timestamp', DESCENDING)],
            )
        return doc or {}
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning('get_chain_doc_at_time error: %s', exc)
        return {}


def get_chain_doc_by_token(
    chain_col,
    token: str,
    snapshot_ts: str,
) -> dict:
    """Chain doc for a composite/exchange token at snapshot_ts, with minute-prefix fallback."""
    norm_token = str(token or '').strip()
    norm_ts    = str(snapshot_ts or '').strip()
    if not norm_token or not norm_ts:
        return {}
    try:
        variants: list[str] = []
        for candidate in [norm_ts, norm_ts.replace('T', ' ').rstrip('Z'), norm_ts.replace(' ', 'T').rstrip('Z')]:
            c = str(candidate or '').strip()
            if c and c not in variants:
                variants.append(c)

        for ts in variants:
            doc = chain_col.find_one({'token': norm_token, 'timestamp': ts})
            if doc:
                return doc

        for ts in variants:
            doc = chain_col.find_one(
                {'token': norm_token, 'timestamp': {'$lte': ts}},
                sort=[('timestamp', DESCENDING)],
            )
            if doc:
                return doc

        for ts in variants:
            prefix = ts[:16]
            if prefix:
                doc = chain_col.find_one(
                    {'token': norm_token, 'timestamp': {'$regex': '^' + re.escape(prefix)}},
                    sort=[('timestamp', DESCENDING)],
                )
                if doc:
                    return doc
        return {}
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning('get_chain_doc_by_token error token=%s ts=%s: %s', norm_token, norm_ts, exc)
        return {}


def get_spot_doc_at_time(
    index_spot_col,
    underlying: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
) -> dict:
    """Spot doc for underlying at snapshot_ts (≤ snapshot_ts)."""
    from features.spot_atm_utils import get_cached_spot_doc
    try:
        cached = get_cached_spot_doc(market_cache, underlying, snapshot_ts)
        if cached:
            return cached
        norm_ts = str(snapshot_ts or '').strip()
        variants: list[str] = []
        for candidate in [
            norm_ts,
            norm_ts.replace('T', ' ').rstrip('Z'),
            norm_ts.replace(' ', 'T').rstrip('Z'),
        ]:
            normalized = str(candidate or '').strip()
            if normalized and normalized not in variants:
                variants.append(normalized)

        doc = {}
        for ts in variants:
            doc = index_spot_col.find_one({'underlying': underlying, 'timestamp': ts})
            if doc:
                break

        if not doc:
            for ts in variants:
                doc = index_spot_col.find_one(
                    {'underlying': underlying, 'timestamp': {'$lte': ts}},
                    sort=[('timestamp', DESCENDING)],
                )
                if doc:
                    break

        if not doc:
            for ts in variants:
                prefix = ts[:16]
                if not prefix:
                    continue
                doc = index_spot_col.find_one(
                    {'underlying': underlying, 'timestamp': {'$regex': '^' + re.escape(prefix)}},
                    sort=[('timestamp', DESCENDING)],
                )
                if doc:
                    break

        # Algo-backtest: market open at 09:15 but first DB record may be 09:16.
        # If $lte found nothing, fetch the nearest future record ($gte).
        if not doc:
            for ts in variants:
                doc = index_spot_col.find_one(
                    {'underlying': underlying, 'timestamp': {'$gte': ts}},
                    sort=[('timestamp', 1)],
                )
                if doc:
                    break

        return doc or {}
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning('get_spot_doc_at_time error: %s', exc)
        return {}


# ─── Price helpers ────────────────────────────────────────────────────────────

def get_spot_price(
    index_spot_col,
    underlying: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
) -> float:
    """Spot price (float) for underlying at snapshot_ts."""
    from features.spot_atm_utils import safe_float
    doc = get_spot_doc_at_time(index_spot_col, underlying, snapshot_ts, market_cache)
    price = safe_float(doc.get('spot_price'))
    return price if price > 0 else 0.0


def get_option_ltp(
    chain_col,
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
    fallback: float = 0.0,
) -> float:
    """LTP (close price, float) for one option contract at snapshot_ts."""
    from features.spot_atm_utils import safe_float
    doc = get_chain_doc_at_time(chain_col, underlying, expiry, strike, option_type, snapshot_ts, market_cache)
    price = safe_float(doc.get('close'))
    return price if price > 0 else fallback


def get_open_legs_ltp_array(
    chain_col,
    open_legs: list[dict],
    underlying: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
) -> list[dict]:
    """
    Pre-fetch LTP for all open entered legs in one call.
    Returns [{leg_id, ltp, entry_price, expiry, strike, option_type}, ...].

    execution_socket._process_backtest_trade_tick() calls this once per trade
    tick so the inner event loop uses the in-memory map — no per-leg DB queries.
    """
    result: list[dict] = []
    for leg in open_legs:
        if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
            continue
        if not (leg.get('entry_trade') or {}):
            continue
        leg_id      = str(leg.get('id') or leg.get('leg_id') or '')
        expiry      = str(leg.get('expiry_date') or '')
        strike      = leg.get('strike')
        option_type = str(leg.get('option') or '')
        entry_price = _safe_float((leg.get('entry_trade') or {}).get('price'))
        fallback    = _safe_float(leg.get('last_saw_price'))

        ltp = get_option_ltp(
            chain_col, underlying, expiry, strike, option_type,
            snapshot_ts, market_cache=market_cache, fallback=fallback,
        )
        result.append({
            'leg_id':      leg_id,
            'ltp':         ltp,
            'entry_price': entry_price,
            'expiry':      expiry,
            'strike':      strike,
            'option_type': option_type,
        })
    return result


def get_spot_price_from_chain_col(
    chain_col,
    underlying: str,
    snapshot_ts: str,
) -> float:
    """
    Fallback for algo-backtest: read spot_price embedded in option_chain_historical_data.
    Used when option_chain_index_spot has no data for the backtest date.
    Only called for algo-backtest mode — never for live or fast-forward.
    """
    import logging
    _log = logging.getLogger(__name__)
    norm_ts = str(snapshot_ts or '').strip()
    variants: list[str] = []
    for candidate in [
        norm_ts,
        norm_ts.replace('T', ' ').rstrip('Z'),
        norm_ts.replace(' ', 'T').rstrip('Z'),
    ]:
        c = str(candidate or '').strip()
        if c and c not in variants:
            variants.append(c)
    try:
        for ts in variants:
            doc = chain_col.find_one(
                {'underlying': underlying, 'timestamp': ts, 'spot_price': {'$gt': 0}},
            )
            if doc:
                price = _safe_float(doc.get('spot_price'))
                if price > 0:
                    return price

        for ts in variants:
            doc = chain_col.find_one(
                {'underlying': underlying, 'timestamp': {'$lte': ts}, 'spot_price': {'$gt': 0}},
                sort=[('timestamp', DESCENDING)],
            )
            if doc:
                price = _safe_float(doc.get('spot_price'))
                if price > 0:
                    return price

        for ts in variants:
            prefix = ts[:16]
            if prefix:
                doc = chain_col.find_one(
                    {'underlying': underlying, 'timestamp': {'$regex': '^' + re.escape(prefix)}, 'spot_price': {'$gt': 0}},
                    sort=[('timestamp', DESCENDING)],
                )
                if doc:
                    price = _safe_float(doc.get('spot_price'))
                    if price > 0:
                        return price
    except Exception as exc:
        _log.warning('get_spot_price_from_chain_col error underlying=%s ts=%s: %s', underlying, snapshot_ts, exc)
    return 0.0


# ─── internal ─────────────────────────────────────────────────────────────────

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
