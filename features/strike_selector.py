"""
strike_selector.py
──────────────────
Reusable strike-selection logic for backtest, live-trade, and forward-test.

Public API
──────────
    resolve_expiry(chain_col, underlying, option_type, trade_date,
                   snapshot_timestamp=None) -> tuple[str, str | None]

    resolve_strike(chain_col, underlying, option_type, entry_kind,
                   strike_param_raw, position, spot_price, expiry,
                   trade_date, snapshot_timestamp=None,
                   market_cache=None, leg_id='') -> StrikeResult

StrikeResult fields
───────────────────
    strike       : float        resolved strike price
    entry_price  : float        option close price at that strike
    chain_doc    : dict | None  raw chain document (contains token, symbol, greeks)
    error        : str | None   None = success; non-None = failure reason (leg skipped)
    meta         : dict         extra calculation details (atm_strike, ce_atm_price, pe_atm_price, straddle, target, multiplier)

Supported entry_kind values
───────────────────────────
    EntryType.EntryByPremiumCloseToStraddle  ->  strike_param = {'Multiplier': 0.6, ...}
    EntryType.EntryByDeltaRange              ->  strike_param = {'LowerRange': 40, 'UpperRange': 70}
    EntryType.EntryByPremium (Geq/Lte)      ->  strike_param = float (plain premium target)
    anything else / ATM / offset            ->  strike_param = int offset from ATM (0 = ATM)
"""

from __future__ import annotations

import ast
import logging
from bisect import bisect_right
from dataclasses import dataclass, field
from typing import Any

from pymongo import DESCENDING

from features.spot_atm_utils import get_cached_chain_doc

log = logging.getLogger(__name__)


# ── helpers ──────────────────────────────────────────────────────────────────

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _is_sell(position_str: str) -> bool:
    return 'sell' in str(position_str or '').lower()


def _parse_sp_dict(raw) -> dict:
    """Parse strike_parameter that may be a dict or a string repr of a dict."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and '{' in raw:
        try:
            return ast.literal_eval(raw)
        except Exception:
            pass
    return {}


# ── result type ──────────────────────────────────────────────────────────────

@dataclass
class StrikeResult:
    strike: float = 0.0
    entry_price: float = 0.0
    chain_doc: dict | None = None   # resolved chain doc → use for token / symbol
    error: str | None = None        # None = success
    meta: dict = field(default_factory=dict)  # intermediate calculation values


# ── expiry resolver ───────────────────────────────────────────────────────────

def resolve_expiry(
    chain_col,
    underlying: str,
    option_type: str,
    trade_date: str,
    snapshot_timestamp: str | None = None,
    expiry_kind: str = 'ExpiryType.Weekly',
) -> tuple[str, str | None]:
    """
    Find the expiry >= trade_date that has data in the option chain, honoring
    expiry_kind (Weekly/NextWeekly/Monthly/NextMonthly) — same classification
    execution_socket._resolve_expiry_from_tokens uses for the live path, so a
    leg configured for e.g. Monthly doesn't silently corrupt to nearest-weekly
    here. Returns (expiry, error_reason). error_reason is None on success.
    """
    try:
        base_q = {
            'underlying': underlying,
            'type': option_type,
            'expiry': {'$gte': trade_date},
        }
        if snapshot_timestamp:
            raw_expiries = chain_col.distinct('expiry', {**base_q, 'timestamp': snapshot_timestamp})
            if not raw_expiries:
                raw_expiries = chain_col.distinct('expiry', {**base_q, 'timestamp': {'$lte': snapshot_timestamp}})
        else:
            raw_expiries = chain_col.distinct('expiry', {**base_q, 'timestamp': {'$regex': f'^{trade_date}'}})

        expiries = sorted({str(e)[:10] for e in (raw_expiries or []) if e})
        if not expiries:
            return '', 'no_expiry_data'

        kind = str(expiry_kind or 'ExpiryType.Weekly')
        if 'Monthly' in kind:
            monthly: list[str] = []
            from itertools import groupby
            for _month_key, group in groupby(expiries, key=lambda d: d[:7]):
                monthly.append(list(group)[-1])
            chosen = (monthly[1] if len(monthly) > 1 else monthly[0]) if 'NextMonthly' in kind else monthly[0]
        elif 'NextWeekly' in kind:
            chosen = expiries[1] if len(expiries) > 1 else expiries[0]
        else:
            chosen = expiries[0]
        return chosen, None
    except Exception as exc:
        return '', f'expiry_query_error:{exc}'


# ── chain fetch helpers ───────────────────────────────────────────────────────

def _chain_at_time(
    chain_col,
    underlying: str,
    expiry: str,
    strike: float,
    option_type: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
) -> dict:
    cached = get_cached_chain_doc(market_cache, underlying, expiry, strike, option_type, snapshot_ts)
    if cached:
        print(
            f'[CHAIN SOURCE] underlying={underlying} expiry={expiry} type={option_type} '
            f'strike={strike} source=market_cache snapshot={snapshot_ts}'
        )
        return cached
    print(
        f'[CHAIN SOURCE] underlying={underlying} expiry={expiry} type={option_type} '
        f'strike={strike} source=mongodb_query snapshot={snapshot_ts}'
    )
    base = {'underlying': underlying, 'expiry': expiry,
            'strike': float(strike), 'type': option_type}
    doc = chain_col.find_one({**base, 'timestamp': snapshot_ts})
    if not doc:
        doc = chain_col.find_one(
            {**base, 'timestamp': {'$lte': snapshot_ts}},
            sort=[('timestamp', DESCENDING)],
        )
    return doc or {}


def _chain_latest(
    chain_col,
    underlying: str,
    expiry: str,
    strike: float,
    option_type: str,
    trade_date: str,
    market_cache: dict | None = None,
) -> dict:
    cached = get_cached_chain_doc(market_cache, underlying, expiry, strike, option_type)
    if cached:
        print(
            f'[CHAIN SOURCE] underlying={underlying} expiry={expiry} type={option_type} '
            f'strike={strike} source=market_cache snapshot={trade_date}'
        )
        return cached
    print(
        f'[CHAIN SOURCE] underlying={underlying} expiry={expiry} type={option_type} '
        f'strike={strike} source=mongodb_query snapshot={trade_date}'
    )
    doc = chain_col.find_one(
        {'underlying': underlying, 'expiry': expiry,
         'strike': float(strike), 'type': option_type,
         'timestamp': {'$regex': f'^{trade_date}'}},
        sort=[('timestamp', DESCENDING)],
    )
    return doc or {}


# ── individual selectors ──────────────────────────────────────────────────────

def _select_premium_close_to_straddle(
    chain_col,
    underlying: str,
    option_type: str,
    expiry: str,
    strike_param_raw: Any,
    spot_price: float,
    trade_date: str,
    snapshot_timestamp: str | None,
    market_cache: dict | None,
    leg_id: str,
) -> StrikeResult:
    """
    EntryByPremiumCloseToStraddle
    strike_param = {'Multiplier': 0.6, 'StrikeKind': 'StrikeType.ATM'}

    1. Find ATM strike from spot_price
    2. straddle = CE_ATM_close + PE_ATM_close
    3. target = straddle * Multiplier
    4. Find strike whose close is CLOSEST to target
    """
    from features.backtest_engine import _resolve_strike

    sp = _parse_sp_dict(strike_param_raw)
    multiplier = _safe_float(sp.get('Multiplier') or 0.5)

    step = 50 if underlying.upper() == 'NIFTY' else 100
    atm_strike = _resolve_strike(spot_price, '0', 'CE', step)

    if snapshot_timestamp:
        ce_doc = _chain_at_time(chain_col, underlying, expiry, atm_strike, 'CE', snapshot_timestamp, market_cache)
        pe_doc = _chain_at_time(chain_col, underlying, expiry, atm_strike, 'PE', snapshot_timestamp, market_cache)
    else:
        ce_doc = _chain_latest(chain_col, underlying, expiry, atm_strike, 'CE', trade_date, market_cache)
        pe_doc = _chain_latest(chain_col, underlying, expiry, atm_strike, 'PE', trade_date, market_cache)

    ce_price = _safe_float((ce_doc or {}).get('close'))
    pe_price = _safe_float((pe_doc or {}).get('close'))
    straddle = ce_price + pe_price
    target = straddle * multiplier

    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=PremiumCloseToStraddle '
        f'atm={atm_strike} ce={ce_price} pe={pe_price} straddle={straddle} '
        f'multiplier={multiplier} target={target}'
    )

    if target <= 0:
        print(f'[STRIKE CALC FAILED] leg={leg_id} method=PremiumCloseToStraddle reason=straddle_or_multiplier_zero')
        return StrikeResult(error='straddle_premium_zero')

    def _closest(ts_filter: dict) -> dict | None:
        base = {'underlying': underlying, 'expiry': expiry, 'type': option_type, **ts_filter}
        below = chain_col.find_one({**base, 'close': {'$lte': target}}, sort=[('close', DESCENDING)])
        above = chain_col.find_one({**base, 'close': {'$gte': target}}, sort=[('close', 1)])
        if not below and not above:
            return None
        if not below:
            return above
        if not above:
            return below
        return below if abs(_safe_float(below.get('close')) - target) <= abs(_safe_float(above.get('close')) - target) else above

    if snapshot_timestamp:
        doc = _closest({'timestamp': snapshot_timestamp}) or _closest({'timestamp': {'$lte': snapshot_timestamp}})
    else:
        doc = _closest({'timestamp': {'$regex': f'^{trade_date}'}})

    if not doc:
        print(
            f'[STRIKE CALC FAILED] leg={leg_id} type={option_type} method=PremiumCloseToStraddle '
            f'target={target} expiry={expiry} reason=no_strike_found'
        )
        return StrikeResult(error='no_strike_for_straddle_premium')

    strike = _safe_float(doc.get('strike'))
    entry_price = _safe_float(doc.get('close'))
    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=PremiumCloseToStraddle '
        f'resolved_strike={strike} entry_price={entry_price} diff={round(abs(entry_price - target), 2)}'
    )
    return StrikeResult(
        strike=strike, entry_price=entry_price, chain_doc=doc,
        meta={
            'atm_strike':    atm_strike,
            'ce_atm_price':  round(ce_price, 2),
            'pe_atm_price':  round(pe_price, 2),
            'straddle':      round(straddle, 2),
            'target':        round(target, 2),
            'multiplier':    multiplier,
        },
    )


def _select_straddle_price(
    chain_col,
    underlying: str,
    option_type: str,
    expiry: str,
    strike_param_raw: Any,
    spot_price: float,
    trade_date: str,
    snapshot_timestamp: str | None,
    market_cache: dict | None,
    leg_id: str,
) -> StrikeResult:
    """
    EntryByStraddlePrice
    strike_param = {'Multiplier': 0.6, 'Adjustment': 'AdjustmentType.Plus', 'StrikeKind': 'StrikeType.ATM'}

    Formula:
      straddle = ATM CE close + ATM PE close
      offset   = Multiplier × straddle
      strike   = ATM Strike + offset  (Plus)
               = ATM Strike - offset  (Minus)
      → round to nearest step
    """
    from features.backtest_engine import _resolve_strike

    sp         = _parse_sp_dict(strike_param_raw)
    multiplier = _safe_float(sp.get('Multiplier') or 0.5)
    adjustment = str(sp.get('Adjustment') or 'AdjustmentType.Plus')
    is_plus    = 'Minus' not in adjustment

    step       = 50 if underlying.upper() == 'NIFTY' else 100
    atm_strike = _resolve_strike(spot_price, '0', 'CE', step)

    if snapshot_timestamp:
        ce_doc = _chain_at_time(chain_col, underlying, expiry, atm_strike, 'CE', snapshot_timestamp, market_cache)
        pe_doc = _chain_at_time(chain_col, underlying, expiry, atm_strike, 'PE', snapshot_timestamp, market_cache)
    else:
        ce_doc = _chain_latest(chain_col, underlying, expiry, atm_strike, 'CE', trade_date, market_cache)
        pe_doc = _chain_latest(chain_col, underlying, expiry, atm_strike, 'PE', trade_date, market_cache)

    ce_price = _safe_float((ce_doc or {}).get('close'))
    pe_price = _safe_float((pe_doc or {}).get('close'))
    straddle = ce_price + pe_price

    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=StraddlePrice '
        f'atm={atm_strike} ce={ce_price} pe={pe_price} straddle={straddle} '
        f'multiplier={multiplier} adjustment={"+" if is_plus else "-"}'
    )

    if straddle <= 0:
        print(f'[STRIKE CALC FAILED] leg={leg_id} method=StraddlePrice reason=straddle_zero')
        return StrikeResult(error='straddle_price_zero')

    offset     = multiplier * straddle
    raw_strike = atm_strike + offset if is_plus else atm_strike - offset
    final_str  = int(round(raw_strike / step) * step)

    if snapshot_timestamp:
        doc = _chain_at_time(chain_col, underlying, expiry, final_str, option_type, snapshot_timestamp, market_cache)
    else:
        doc = _chain_latest(chain_col, underlying, expiry, final_str, option_type, trade_date, market_cache)

    if not doc:
        print(f'[STRIKE CALC FAILED] leg={leg_id} method=StraddlePrice reason=no_chain_doc strike={final_str}')
        return StrikeResult(error='no_strike_for_straddle_price')

    entry_price = _safe_float(doc.get('close'))
    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=StraddlePrice '
        f'raw={raw_strike} resolved_strike={final_str} entry_price={entry_price}'
    )
    return StrikeResult(
        strike=final_str, entry_price=entry_price, chain_doc=doc,
        meta={
            'atm_strike':   atm_strike,
            'ce_atm_price': round(ce_price, 2),
            'pe_atm_price': round(pe_price, 2),
            'straddle':     round(straddle, 2),
            'multiplier':   multiplier,
            'offset':       round(offset, 2),
            'adjustment':   '+' if is_plus else '-',
        },
    )


def _fetch_chain_rows(
    chain_col,
    underlying: str,
    option_type: str,
    expiry: str,
    trade_date: str,
    snapshot_timestamp: str | None,
    market_cache: dict | None = None,
) -> list[dict]:
    """
    Fetch all chain rows for a given underlying/expiry/type at the snapshot time.
    Returns a list of dicts with strike, delta, close — used by delta_selector functions.
    Prefers market_cache (in-memory, O(1)) over a fresh MongoDB query.
    """
    und_norm = underlying.strip().upper()
    exp_norm = expiry.strip()[:10]
    opt_norm = option_type.strip().upper()

    if market_cache:
        chain_docs_map  = market_cache.get('chain_docs') or {}
        chain_ts_map    = market_cache.get('chain_timestamps') or {}
        rows: list[dict] = []
        for key, docs in chain_docs_map.items():
            k_und, k_exp, _k_strike, k_type = key
            if k_und != und_norm or k_exp != exp_norm or k_type != opt_norm:
                continue
            timestamps = chain_ts_map.get(key) or []
            if snapshot_timestamp:
                idx = bisect_right(timestamps, snapshot_timestamp) - 1
                doc = docs[idx] if idx >= 0 else None
            else:
                doc = docs[-1] if docs else None
            if doc:
                d = dict(doc)
                if 'ltp' not in d:
                    d['ltp'] = _safe_float(d.get('close'))
                rows.append(d)
        if rows:
            print(
                f'[CHAIN SOURCE] underlying={und_norm} expiry={exp_norm} type={opt_norm} '
                f'source=market_cache strikes={len(rows)} snapshot={snapshot_timestamp or trade_date}'
            )
            return rows
        print(
            f'[CHAIN SOURCE] underlying={und_norm} expiry={exp_norm} type={opt_norm} '
            f'source=market_cache_MISS → falling back to mongodb snapshot={snapshot_timestamp or trade_date}'
        )

    # Fall back to MongoDB query
    base_q = {'underlying': underlying, 'expiry': expiry, 'type': option_type}
    if snapshot_timestamp:
        docs = list(chain_col.find({**base_q, 'timestamp': snapshot_timestamp}))
        if not docs:
            latest_ts_doc = chain_col.find_one(
                {**base_q, 'timestamp': {'$lte': snapshot_timestamp}},
                sort=[('timestamp', DESCENDING)],
                projection={'timestamp': 1},
            )
            if latest_ts_doc:
                ts = latest_ts_doc['timestamp']
                docs = list(chain_col.find({**base_q, 'timestamp': ts}))
    else:
        docs = list(chain_col.find(
            {**base_q, 'timestamp': {'$regex': f'^{trade_date}'}},
        ))
    print(
        f'[CHAIN SOURCE] underlying={und_norm} expiry={exp_norm} type={opt_norm} '
        f'source=mongodb_query strikes={len(docs)} snapshot={snapshot_timestamp or trade_date}'
    )
    for d in docs:
        if 'ltp' not in d:
            d['ltp'] = _safe_float(d.get('close'))
    return docs


def _select_closest_delta(
    chain_col,
    underlying: str,
    option_type: str,
    expiry: str,
    strike_param_raw: Any,
    trade_date: str,
    snapshot_timestamp: str | None,
    leg_id: str,
    market_cache: dict | None = None,
) -> StrikeResult:
    """
    EntryByDelta — closest delta.
    strike_param = int/float (0–100, e.g. 50 → delta 0.50).
    Uses delta_selector.select_closest_delta (shared with live path).
    """
    from features.delta_selector import select_closest_delta

    target_pct = _safe_float(strike_param_raw)
    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=ClosestDelta '
        f'target={target_pct} expiry={expiry}'
    )

    rows = _fetch_chain_rows(chain_col, underlying, option_type, expiry, trade_date, snapshot_timestamp, market_cache)
    if not rows:
        print(f'[STRIKE CALC FAILED] leg={leg_id} method=ClosestDelta reason=no_chain_rows expiry={expiry}')
        return StrikeResult(error='no_chain_rows_for_delta')

    from features.delta_selector import print_delta_chain_table
    print_delta_chain_table(rows, underlying, expiry, option_type, 'EntryByDelta', leg_id)

    chosen = select_closest_delta(rows, target_pct, option_type, leg_id)
    if not chosen:
        print(f'[STRIKE CALC FAILED] leg={leg_id} method=ClosestDelta reason=no_valid_delta_row')
        return StrikeResult(error='no_strike_for_closest_delta')

    strike      = _safe_float(chosen.get('strike'))
    entry_price = _safe_float(chosen.get('close') or chosen.get('ltp'))
    sel_delta   = _safe_float(chosen.get('delta'))
    print(
        f'[STRIKE CALC] leg={leg_id} method=ClosestDelta '
        f'resolved_strike={strike} delta={sel_delta} entry_price={entry_price}'
    )
    return StrikeResult(
        strike=strike, entry_price=entry_price, chain_doc=chosen,
        meta={'target_delta_pct': target_pct, 'selected_delta': round(sel_delta, 4)},
    )


def _select_delta_range(
    chain_col,
    underlying: str,
    option_type: str,
    expiry: str,
    strike_param_raw: Any,
    position: str,
    trade_date: str,
    snapshot_timestamp: str | None,
    leg_id: str,
    market_cache: dict | None = None,
    spot_price: float = 0.0,
) -> StrikeResult:
    """
    EntryByDeltaRange — strike_param = {'LowerRange': 20, 'UpperRange': 40}.
    Uses delta_selector.select_delta_range (shared with live path).
    If no strike in range → returns error → leg entry is skipped.
    """
    from features.delta_selector import select_delta_range

    sp        = _parse_sp_dict(strike_param_raw)
    lower_pct = _safe_float(sp.get('LowerRange') or 0)
    upper_pct = _safe_float(sp.get('UpperRange') or 0)

    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=DeltaRange '
        f'range={lower_pct}%–{upper_pct}% position={position} expiry={expiry}'
    )

    rows = _fetch_chain_rows(chain_col, underlying, option_type, expiry, trade_date, snapshot_timestamp, market_cache)
    if not rows:
        print(f'[STRIKE CALC FAILED] leg={leg_id} method=DeltaRange reason=no_chain_rows expiry={expiry}')
        return StrikeResult(error='no_chain_rows_for_delta')

    from features.delta_selector import print_delta_chain_table
    print_delta_chain_table(rows, underlying, expiry, option_type, 'EntryByDeltaRange', leg_id)

    chosen = select_delta_range(rows, lower_pct, upper_pct, option_type, position, leg_id, spot_price)
    if not chosen:
        print(
            f'[STRIKE CALC FAILED] leg={leg_id} method=DeltaRange '
            f'range={lower_pct}%–{upper_pct}% expiry={expiry} reason=no_strike_in_delta_range — leg skipped'
        )
        return StrikeResult(error='no_strike_in_delta_range')

    strike      = _safe_float(chosen.get('strike'))
    entry_price = _safe_float(chosen.get('close') or chosen.get('ltp'))
    sel_delta   = _safe_float(chosen.get('delta'))
    is_sell_pos = _is_sell(position)
    print(
        f'[STRIKE CALC] leg={leg_id} method=DeltaRange '
        f'resolved_strike={strike} delta={sel_delta} entry_price={entry_price}'
    )
    return StrikeResult(
        strike=strike, entry_price=entry_price, chain_doc=chosen,
        meta={
            'lower_pct':      lower_pct,
            'upper_pct':      upper_pct,
            'selected_delta': round(sel_delta, 4),
            'position_side':  'sell' if is_sell_pos else 'buy',
        },
    )


def _select_premium_range(
    chain_col,
    underlying: str,
    option_type: str,
    expiry: str,
    strike_param_raw: Any,
    trade_date: str,
    snapshot_timestamp: str | None,
    leg_id: str,
) -> StrikeResult:
    """
    EntryByPremiumRange
    strike_param = {'LowerRange': 40, 'UpperRange': 80}
    Finds a strike where LowerRange <= close <= UpperRange.
    Picks the strike whose premium is closest to the midpoint of the range.
    Returns error if no strike found in range — entry is skipped.
    """
    sp    = _parse_sp_dict(strike_param_raw)
    lower = _safe_float(sp.get('LowerRange') or sp.get('lower') or 0)
    upper = _safe_float(sp.get('UpperRange') or sp.get('upper') or 0)

    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=PremiumRange '
        f'lower={lower} upper={upper} expiry={expiry}'
    )

    if lower <= 0 or upper <= 0 or lower >= upper:
        print(f'[STRIKE CALC FAILED] leg={leg_id} method=PremiumRange reason=invalid_range lower={lower} upper={upper}')
        return StrikeResult(error='premium_range_invalid')

    mid   = (lower + upper) / 2
    base_q = {
        'underlying': underlying,
        'expiry':     expiry,
        'type':       option_type,
        'close':      {'$gte': lower, '$lte': upper},
    }

    def _closest_to_mid(ts_filter: dict) -> dict | None:
        below = chain_col.find_one({**base_q, 'close': {'$gte': lower, '$lte': mid},   **ts_filter}, sort=[('close', DESCENDING)])
        above = chain_col.find_one({**base_q, 'close': {'$gte': mid,   '$lte': upper}, **ts_filter}, sort=[('close', 1)])
        if not below and not above:
            return None
        if not below:
            return above
        if not above:
            return below
        b_diff = abs(_safe_float(below.get('close')) - mid)
        a_diff = abs(_safe_float(above.get('close')) - mid)
        return below if b_diff <= a_diff else above

    if snapshot_timestamp:
        doc = _closest_to_mid({'timestamp': snapshot_timestamp})
        if not doc:
            doc = _closest_to_mid({'timestamp': {'$lte': snapshot_timestamp}})
    else:
        doc = _closest_to_mid({'timestamp': {'$regex': f'^{trade_date}'}})

    if not doc:
        print(
            f'[STRIKE CALC FAILED] leg={leg_id} type={option_type} method=PremiumRange '
            f'lower={lower} upper={upper} expiry={expiry} reason=no_strike_in_range'
        )
        return StrikeResult(error='no_strike_for_premium_range')

    strike      = _safe_float(doc.get('strike'))
    entry_price = _safe_float(doc.get('close'))
    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=PremiumRange '
        f'resolved_strike={strike} entry_price={entry_price} range=[{lower},{upper}]'
    )
    return StrikeResult(
        strike=strike, entry_price=entry_price, chain_doc=doc,
        meta={'lower_range': lower, 'upper_range': upper, 'mid_target': round(mid, 2)},
    )


def _select_premium(
    chain_col,
    underlying: str,
    option_type: str,
    expiry: str,
    strike_param: float,
    entry_kind: str,
    trade_date: str,
    snapshot_timestamp: str | None,
    leg_id: str,
) -> StrikeResult:
    """
    EntryByPremium — three modes based on entry_kind:
      plain EntryByPremium → Closest Premium: pick strike nearest to target (above OR below)
      Geq                  → close >= target, pick closest from above
      Lte                  → close <= target, pick closest from below
    """
    ek_lower = entry_kind.lower()
    is_geq = 'geq' in ek_lower
    is_lte = 'lte' in ek_lower or 'leq' in ek_lower
    is_closest = not is_geq and not is_lte  # pure EntryByPremium → Closest Premium

    base_q = {'underlying': underlying, 'expiry': expiry, 'type': option_type}

    if is_closest:
        print(
            f'[STRIKE CALC] leg={leg_id} type={option_type} method=ClosestPremium '
            f'target={strike_param} expiry={expiry}'
        )

        def _find_closest(ts_filter: dict) -> dict | None:
            below = chain_col.find_one(
                {**base_q, 'close': {'$lte': strike_param}, **ts_filter},
                sort=[('close', DESCENDING)],
            )
            above = chain_col.find_one(
                {**base_q, 'close': {'$gte': strike_param}, **ts_filter},
                sort=[('close', 1)],
            )
            if not below and not above:
                return None
            if not below:
                return above
            if not above:
                return below
            diff_below = abs(_safe_float(below.get('close')) - strike_param)
            diff_above = abs(_safe_float(above.get('close')) - strike_param)
            # on tie pick below (conservative — lower premium)
            return below if diff_below <= diff_above else above

        if snapshot_timestamp:
            doc = _find_closest({'timestamp': snapshot_timestamp})
            if not doc:
                doc = _find_closest({'timestamp': {'$lte': snapshot_timestamp}})
        else:
            doc = _find_closest({'timestamp': {'$regex': f'^{trade_date}'}})

        if not doc:
            print(
                f'[STRIKE CALC FAILED] leg={leg_id} type={option_type} method=ClosestPremium '
                f'target={strike_param} expiry={expiry} reason=no_strike_found'
            )
            return StrikeResult(error='no_strike_for_premium')

        strike = _safe_float(doc.get('strike'))
        entry_price = _safe_float(doc.get('close'))
        diff = round(abs(entry_price - strike_param), 2)
        print(
            f'[STRIKE CALC] leg={leg_id} type={option_type} method=ClosestPremium '
            f'resolved_strike={strike} entry_price={entry_price} diff={diff}'
        )
        return StrikeResult(strike=strike, entry_price=entry_price, chain_doc=doc)

    # Geq / Lte mode
    op = '$gte' if is_geq else '$lte'
    close_sort = [('close', 1)] if is_geq else [('close', DESCENDING)]
    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=Premium '
        f'op={op} target={strike_param} expiry={expiry}'
    )
    base_q_filtered = {**base_q, 'close': {op: strike_param}}
    if snapshot_timestamp:
        doc = chain_col.find_one({**base_q_filtered, 'timestamp': snapshot_timestamp}, sort=close_sort)
        if not doc:
            doc = chain_col.find_one(
                {**base_q_filtered, 'timestamp': {'$lte': snapshot_timestamp}},
                sort=close_sort,
            )
    else:
        doc = chain_col.find_one({**base_q_filtered, 'timestamp': {'$regex': f'^{trade_date}'}}, sort=close_sort)

    if not doc:
        print(
            f'[STRIKE CALC FAILED] leg={leg_id} type={option_type} method=Premium '
            f'op={op} target={strike_param} expiry={expiry} reason=no_strike_found'
        )
        return StrikeResult(error='no_strike_for_premium')

    strike = _safe_float(doc.get('strike'))
    entry_price = _safe_float(doc.get('close'))
    print(f'[STRIKE CALC] leg={leg_id} type={option_type} method=Premium resolved_strike={strike} entry_price={entry_price}')
    return StrikeResult(strike=strike, entry_price=entry_price, chain_doc=doc)


def _select_atm_multiplier(
    chain_col,
    underlying: str,
    option_type: str,
    expiry: str,
    multiplier: float,
    spot_price: float,
    trade_date: str,
    snapshot_timestamp: str | None,
    market_cache: dict | None,
    leg_id: str,
) -> StrikeResult:
    """
    EntryByAtmMultiplier
    strike_param = float multiplier (e.g. 1.005 = ATM + 0.5%, 0.99 = ATM - 1%)

    Formula:
      step         = 50 (NIFTY) or 100 (BANKNIFTY)
      atm_strike   = nearest step multiple to spot
      strike       = round(atm_strike × multiplier / step) × step
    """
    from features.backtest_engine import _resolve_strike

    step       = 50 if underlying.upper() == 'NIFTY' else 100
    atm_strike = _resolve_strike(spot_price, '0', 'CE', step)
    raw_strike = atm_strike * multiplier
    final_str  = int(round(raw_strike / step) * step)

    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=AtmMultiplier '
        f'atm={atm_strike} multiplier={multiplier} raw={round(raw_strike,2)} resolved={final_str}'
    )

    if snapshot_timestamp:
        doc = _chain_at_time(chain_col, underlying, expiry, final_str, option_type, snapshot_timestamp, market_cache)
    else:
        doc = _chain_latest(chain_col, underlying, expiry, final_str, option_type, trade_date, market_cache)

    if not doc:
        print(f'[STRIKE CALC FAILED] leg={leg_id} method=AtmMultiplier reason=no_chain_doc strike={final_str}')
        return StrikeResult(error='no_strike_for_atm_multiplier')

    entry_price = _safe_float(doc.get('close'))
    pct_change  = round((multiplier - 1) * 100, 4)
    print(
        f'[STRIKE CALC] leg={leg_id} method=AtmMultiplier '
        f'resolved_strike={final_str} pct={pct_change:+.4f}% entry_price={entry_price}'
    )
    return StrikeResult(
        strike=final_str, entry_price=entry_price, chain_doc=doc,
        meta={
            'atm_strike':  atm_strike,
            'multiplier':  multiplier,
            'pct_change':  pct_change,
            'raw_strike':  round(raw_strike, 2),
        },
    )


def _select_atm_offset(
    chain_col,
    underlying: str,
    option_type: str,
    expiry: str,
    strike_param: float,
    spot_price: float,
    trade_date: str,
    snapshot_timestamp: str | None,
    market_cache: dict | None,
    leg_id: str,
) -> StrikeResult:
    """
    ATM or fixed offset from ATM.
    strike_param = int offset (0 = ATM, 1 = 1 step OTM, -1 = 1 step ITM, etc.)
    """
    from features.backtest_engine import _resolve_strike

    step = 50 if underlying.upper() == 'NIFTY' else 100
    strike = _resolve_strike(spot_price, str(int(strike_param)), option_type, step)
    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=ATM/Offset '
        f'spot={spot_price} param={strike_param} step={step} resolved_strike={strike}'
    )

    if snapshot_timestamp:
        doc = _chain_at_time(chain_col, underlying, expiry, strike, option_type, snapshot_timestamp, market_cache)
    else:
        doc = _chain_latest(chain_col, underlying, expiry, strike, option_type, trade_date, market_cache)

    if not doc:
        print(
            f'[STRIKE CALC FAILED] leg={leg_id} type={option_type} method=ATM/Offset '
            f'strike={strike} expiry={expiry} reason=no_chain_doc'
        )
        return StrikeResult(error='no_chain_doc')

    entry_price = _safe_float(doc.get('close'))
    print(f'[STRIKE CALC] leg={leg_id} type={option_type} method=ATM/Offset resolved_strike={strike} close={entry_price}')
    return StrikeResult(strike=strike, entry_price=entry_price, chain_doc=doc)


# ── main public function ──────────────────────────────────────────────────────

def resolve_strike(
    chain_col,
    underlying: str,
    option_type: str,
    entry_kind: str,
    strike_param_raw: Any,
    position: str,
    spot_price: float,
    expiry: str,
    trade_date: str,
    snapshot_timestamp: str | None = None,
    market_cache: dict | None = None,
    leg_id: str = '',
) -> StrikeResult:
    """
    Resolve the strike and entry price for a pending leg.

    Parameters
    ----------
    chain_col          : MongoDB collection  (option_chain_historical_data)
    underlying         : 'NIFTY' | 'BANKNIFTY' | ...
    option_type        : 'CE' | 'PE'
    entry_kind         : EntryType string from strategy config
    strike_param_raw   : raw StrikeParameter (dict, string-repr of dict, or float/int)
    position           : 'PositionType.Sell' | 'PositionType.Buy'
    spot_price         : current underlying spot price
    expiry             : resolved expiry date string (use resolve_expiry first)
    trade_date         : 'YYYY-MM-DD'
    snapshot_timestamp : ISO timestamp for backtest / forward-test lookup
    market_cache       : optional preloaded cache dict
    leg_id             : for logging only

    Returns
    -------
    StrikeResult  — check .error first; None means success.
    """
    strike_param_float = _safe_float(strike_param_raw)

    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} entry_kind={entry_kind or "ATM"} '
        f'strike_parameter={strike_param_raw} underlying={underlying} '
        f'snapshot={snapshot_timestamp or trade_date}'
    )

    if 'PremiumCloseToStraddle' in entry_kind:
        result = _select_premium_close_to_straddle(
            chain_col, underlying, option_type, expiry,
            strike_param_raw, spot_price,
            trade_date, snapshot_timestamp, market_cache, leg_id,
        )

    elif 'DeltaRange' in entry_kind:
        result = _select_delta_range(
            chain_col, underlying, option_type, expiry,
            strike_param_raw, position,
            trade_date, snapshot_timestamp, leg_id,
            market_cache=market_cache,
            spot_price=spot_price,
        )

    elif 'Delta' in entry_kind:
        # EntryByDelta (closest delta) — checked after DeltaRange to avoid substring collision
        result = _select_closest_delta(
            chain_col, underlying, option_type, expiry,
            strike_param_raw,
            trade_date, snapshot_timestamp, leg_id,
            market_cache=market_cache,
        )

    elif 'StraddlePrice' in entry_kind:
        result = _select_straddle_price(
            chain_col, underlying, option_type, expiry,
            strike_param_raw, spot_price,
            trade_date, snapshot_timestamp, market_cache, leg_id,
        )

    elif 'AtmMultiplier' in entry_kind:
        result = _select_atm_multiplier(
            chain_col, underlying, option_type, expiry,
            _safe_float(strike_param_raw), spot_price,
            trade_date, snapshot_timestamp, market_cache, leg_id,
        )

    elif 'PremiumRange' in entry_kind:
        result = _select_premium_range(
            chain_col, underlying, option_type, expiry,
            strike_param_raw,
            trade_date, snapshot_timestamp, leg_id,
        )

    elif 'Premium' in entry_kind:
        result = _select_premium(
            chain_col, underlying, option_type, expiry,
            strike_param_float, entry_kind,
            trade_date, snapshot_timestamp, leg_id,
        )

    else:
        result = _select_atm_offset(
            chain_col, underlying, option_type, expiry,
            strike_param_float, spot_price,
            trade_date, snapshot_timestamp, market_cache, leg_id,
        )

    if result.error:
        return result

    if result.entry_price <= 0:
        print(
            f'[STRIKE CALC FAILED] leg={leg_id} type={option_type} '
            f'strike={result.strike} entry_price={result.entry_price} reason=entry_price_zero'
        )
        return StrikeResult(error='entry_price_zero')

    return result
