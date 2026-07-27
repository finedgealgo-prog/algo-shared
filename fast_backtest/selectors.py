"""
selectors.py
------------
Dynamic strike selection over an in-memory GroupArrays (one timestamp +
expiry + option_type slice of a DayChain). All lookups are numpy argmin
over a small array (typically <100 strikes) — no DataFrame filtering.
"""

from __future__ import annotations

import numpy as np

from .day_chain import DayChain, GroupArrays


def _pick(group: GroupArrays, idx: int) -> dict:
    delta = float(group.delta[idx]) if not np.isnan(group.delta[idx]) else None
    premium = float(group.premium[idx]) if not np.isnan(group.premium[idx]) else None
    return {"strike": float(group.strike[idx]), "delta": delta, "premium": premium}


def by_delta(group: GroupArrays | None, target_delta: float) -> dict | None:
    if group is None or group.delta.size == 0:
        return None
    diffs = np.abs(group.delta - target_delta)
    if np.all(np.isnan(diffs)):
        return None
    return _pick(group, int(np.nanargmin(diffs)))


def by_premium(group: GroupArrays | None, target_premium: float) -> dict | None:
    if group is None or group.premium.size == 0:
        return None
    diffs = np.abs(group.premium - target_premium)
    if np.all(np.isnan(diffs)):
        return None
    return _pick(group, int(np.nanargmin(diffs)))


def by_atm_strike(group: GroupArrays | None, spot_price: float) -> dict | None:
    if group is None or group.strike.size == 0:
        return None
    idx = int(np.argmin(np.abs(group.strike - spot_price)))
    return _pick(group, idx)


def straddle_premium(ce_group: GroupArrays | None, pe_group: GroupArrays | None, strike: float) -> float | None:
    if ce_group is None or pe_group is None:
        return None
    ce_idx = np.where(ce_group.strike == strike)[0]
    pe_idx = np.where(pe_group.strike == strike)[0]
    if ce_idx.size == 0 or pe_idx.size == 0:
        return None
    ce_p, pe_p = ce_group.premium[ce_idx[0]], pe_group.premium[pe_idx[0]]
    if np.isnan(ce_p) or np.isnan(pe_p):
        return None
    return float(ce_p + pe_p)


def by_straddle_premium_pct(chain: DayChain, timestamp: str, expiry: str,
                             pct: float, option_type: str) -> dict | None:
    """Find the strike whose premium is closest to pct% of the ATM straddle premium."""
    spot = chain.spot(timestamp)
    ce_group = chain.group(timestamp, expiry, "CE")
    pe_group = chain.group(timestamp, expiry, "PE")
    if spot is None or ce_group is None or pe_group is None:
        return None

    atm = by_atm_strike(ce_group, spot)
    if atm is None:
        return None

    straddle = straddle_premium(ce_group, pe_group, atm["strike"])
    if straddle is None:
        return None

    target_group = ce_group if option_type == "CE" else pe_group
    return by_premium(target_group, straddle * pct)
