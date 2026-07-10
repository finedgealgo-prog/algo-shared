"""
delta_selector.py
─────────────────
Pure delta-based strike selection logic.
Works on any list of chain rows — DB docs (backtest) or Kite live rows (live/forward).

Shared by:
  strike_selector.py   — backtest path  (rows fetched from option_chain_historical_data)
  kite_delta_chain.py  — live path      (rows computed from Kite Black-Scholes Greeks)

Row schema (each row is a dict):
  strike : float       — strike price
  delta  : float       — CE: positive (0–1), PE: negative (-1–0)
  close  : float       — option price (backtest)  OR
  ltp    : float       — option LTP   (live)
  token  : str         — broker token (live only, optional for backtest)
  symbol : str         — option symbol (optional)

All selection functions return the best matching row dict, or None if no match.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _is_sell(position: str) -> bool:
    return 'sell' in str(position or '').lower()


def _valid_rows(rows: list[dict]) -> list[dict]:
    """Filter rows that have a non-zero delta and a positive price."""
    out = []
    for r in rows:
        d = _safe_float(r.get('delta'))
        p = _safe_float(r.get('ltp') or r.get('close'))
        if d != 0.0 and p > 0:
            out.append(r)
    return out


# ── chain table printer ───────────────────────────────────────────────────────

def print_delta_chain_table(
    rows: list[dict],
    underlying: str,
    expiry: str,
    option_type: str,
    entry_kind: str,
    leg_id: str,
    spot_price: float = 0.0,
) -> None:
    """
    Print a formatted option chain table before delta strike selection.
    Called for BOTH backtest (rows from DB) and live (rows from Kite).
    Columns printed depend on what's available in the row dicts.
    """
    rows_sorted = sorted(rows, key=lambda r: _safe_float(r.get('strike')))
    sep = '[DELTA CHAIN] ' + '─' * 95

    has_iv    = any(r.get('iv')     is not None for r in rows_sorted)
    has_oi    = any(r.get('oi')     is not None for r in rows_sorted)
    has_theta = any(r.get('theta')  is not None for r in rows_sorted)

    print(
        f'\n[DELTA CHAIN] leg={leg_id} entry_kind={entry_kind} '
        f'underlying={underlying} expiry={expiry} type={option_type} '
        f'spot={spot_price} total_strikes={len(rows_sorted)}'
    )
    print(sep)

    header = f'[DELTA CHAIN] {"Strike":>8}  {"LTP/Close":>10}  {"Delta":>8}'
    if has_iv:
        header += f'  {"IV%":>8}'
    if has_theta:
        header += f'  {"Theta":>8}'
    if has_oi:
        header += f'  {"OI":>12}'
    print(header)
    print(sep)

    for r in rows_sorted:
        price = _safe_float(r.get('ltp') or r.get('close'))
        delta = _safe_float(r.get('delta'))
        line  = f'[DELTA CHAIN] {_safe_float(r.get("strike")):>8.0f}  {price:>10.2f}  {delta:>8.4f}'
        if has_iv:
            iv = _safe_float(r.get('iv'))
            line += f'  {iv:>8.2f}'
        if has_theta:
            theta = _safe_float(r.get('theta'))
            line += f'  {theta:>8.4f}'
        if has_oi:
            oi = int(r.get('oi') or 0)
            line += f'  {oi:>12}'
        print(line)

    print(sep + '\n')


# ── EntryByDelta ──────────────────────────────────────────────────────────────

def select_closest_delta(
    rows: list[dict],
    target_pct: float,   # 0–100 (e.g. 50 → delta 0.50)
    option_type: str,    # 'CE' or 'PE'
    leg_id: str = '',
) -> dict | None:
    """
    EntryByDelta — pick the strike whose delta is nearest to target_pct/100.

    PE deltas are negative internally; input is always a positive 0–100 value.
    If two strikes are equidistant, the lower-delta (more OTM) one is chosen.
    """
    target = target_pct / 100.0
    is_pe  = option_type.upper() == 'PE'
    if is_pe:
        target = -abs(target)

    valid = _valid_rows(rows)
    if not valid:
        log.warning('[DELTA SELECT] leg=%s no valid rows for ClosestDelta', leg_id)
        return None

    chosen = min(valid, key=lambda r: abs(_safe_float(r.get('delta')) - target))
    chosen_delta = _safe_float(chosen.get('delta'))
    print(
        f'[DELTA SELECT] leg={leg_id} method=ClosestDelta opt={option_type.upper()} '
        f'target={target:.4f} selected_strike={chosen.get("strike")} '
        f'delta={chosen_delta:.4f}'
    )
    return chosen


# ── EntryByDeltaRange ─────────────────────────────────────────────────────────

def select_delta_range(
    rows: list[dict],
    lower_pct: float,    # 0–100
    upper_pct: float,    # 0–100
    option_type: str,    # 'CE' or 'PE'
    position: str,       # 'PositionType.Sell' or 'PositionType.Buy'
    leg_id: str = '',
    spot_price: float = 0.0,
) -> dict | None:
    """
    EntryByDeltaRange — pick strike whose delta is in [lower_pct/100, upper_pct/100].

    Sell → primary: closest delta to upper bound (70); tiebreaker: nearest to ATM.
    Buy  → primary: closest delta to lower bound (40); tiebreaker: nearest to ATM.
    PE deltas are negative — range is automatically inverted.
    Returns None if no strike falls in range (leg should be skipped).
    """
    lower    = lower_pct / 100.0
    upper    = upper_pct / 100.0
    is_pe    = option_type.upper() == 'PE'
    sell_pos = _is_sell(position)
    valid    = _valid_rows(rows)

    if is_pe:
        # PE deltas: -upper ≤ delta ≤ -lower  (e.g. range 40–70 → -0.70 to -0.40)
        candidates = [r for r in valid if -upper <= _safe_float(r.get('delta')) <= -lower]
        # Sell → nearest to -upper (-0.70); Buy → nearest to -lower (-0.40)
        target_delta = -upper if sell_pos else -lower
    else:
        candidates = [r for r in valid if lower <= _safe_float(r.get('delta')) <= upper]
        # Sell → nearest to +upper (0.70); Buy → nearest to +lower (0.40)
        target_delta = upper if sell_pos else lower

    print(
        f'[DELTA SELECT] leg={leg_id} method=DeltaRange opt={option_type.upper()} '
        f'range={lower_pct}%–{upper_pct}% pos={"Sell" if sell_pos else "Buy"} '
        f'target_delta={target_delta:.4f} valid={len(valid)} candidates={len(candidates)}'
    )

    if not candidates:
        log.warning(
            '[DELTA SELECT] leg=%s no strikes in delta range %.0f%%–%.0f%% — entry skipped',
            leg_id, lower_pct, upper_pct,
        )
        return None

    # Exclude deep ITM anomalies (high IV causing unexpectedly low delta far from ATM).
    # Keep only strikes within 5% of spot; fall back to all candidates if none pass.
    if spot_price > 0:
        max_dist = spot_price * 0.05
        near = [r for r in candidates if abs(_safe_float(r.get('strike')) - spot_price) <= max_dist]
        if near:
            candidates = near

    # Primary: closest delta to target bound (upper for sell, lower for buy).
    # Tiebreaker: nearest to ATM when two strikes are equidistant in delta.
    candidates.sort(key=lambda r: (
        abs(_safe_float(r.get('delta')) - target_delta),
        abs(_safe_float(r.get('strike')) - spot_price) if spot_price > 0 else 0,
    ))

    chosen = candidates[0]
    print(
        f'[DELTA SELECT] leg={leg_id} selected strike={chosen.get("strike")} '
        f'delta={_safe_float(chosen.get("delta")):.4f} '
        f'price={_safe_float(chosen.get("ltp") or chosen.get("close"))}'
    )
    return chosen
