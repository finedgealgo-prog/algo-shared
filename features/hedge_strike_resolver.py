"""
hedge_strike_resolver.py
─────────────────────────
Resolves a single chain row (strike/ltp/delta/token/symbol) for the
Position Configuration panel's "Hedge Strike Type" config — used by
simulator_risk_monitor.py's Hedge Time Control execution to pick the
CE/PE strike for the basket's protective legs.

Operates on the exact row shape api.py's get_live_greeks_chain() returns
(chain["CE"]/chain["PE"]), the same shape features.delta_selector already
documents itself as working on.
"""

from __future__ import annotations

from typing import Any

from features.delta_selector import select_closest_delta


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _valid_premium_rows(rows: list[dict]) -> list[dict]:
    return [r for r in rows if _safe_float(r.get("ltp")) > 0]


def _select_closest_premium(rows: list[dict], target_premium: float) -> dict | None:
    valid = _valid_premium_rows(rows)
    if not valid or target_premium <= 0:
        return None
    return min(valid, key=lambda r: abs(_safe_float(r.get("ltp")) - target_premium))


def _select_by_strike_offset(
    rows: list[dict],
    option_type: str,
    strike_drop: str,
    atm_strike: float,
    strike_interval: float,
) -> dict | None:
    """
    strike_drop: "ATM", "OTM5", "ITM5", etc. — same vocabulary as the
    Hedge Strike Type dropdown (PaperTradeNew.tsx's ptnStrikeDrpOpts).
    OTM/ITM direction depends on option_type: for CE, OTM is above ATM and
    ITM is below; for PE it's the reverse.
    """
    valid = [r for r in rows if _safe_float(r.get("ltp")) > 0]
    if not valid or strike_interval <= 0:
        return None

    normalized = str(strike_drop or "ATM").strip().upper()
    offset = 0
    direction = 0  # +1 = OTM, -1 = ITM
    if normalized.startswith("OTM"):
        direction = 1
        offset = int(normalized[3:] or 0)
    elif normalized.startswith("ITM"):
        direction = -1
        offset = int(normalized[3:] or 0)
    # ATM (or unrecognized) -> offset stays 0, direction irrelevant.

    is_ce = str(option_type or "").strip().upper() == "CE"
    # CE: OTM = above spot (higher strikes); PE: OTM = below spot (lower strikes).
    sign = 1 if is_ce else -1
    target_strike = atm_strike + (sign * direction * offset * strike_interval)

    return min(valid, key=lambda r: abs(_safe_float(r.get("strike")) - target_strike))


def resolve_hedge_strike(
    chain_rows: list[dict],
    option_type: str,
    mode: str,
    value: float,
    strike_drop: str,
    atm_strike: float,
    strike_interval: float,
) -> dict | None:
    """
    chain_rows: chain["CE"] or chain["PE"] from get_live_greeks_chain(), already
    filtered to the option_type this call is resolving.
    mode: "delta" | "closest_premium" | "strike" (Hedge Strike Type's own mode field).
    value: the Hedge Strike Type numeric value — for "delta" this is the
    0-100 target select_closest_delta() expects (e.g. 20 -> delta 0.20),
    for "closest_premium" it's a target premium in rupees.
    Returns None (caller must skip + log, never place a half-resolved order)
    if the chain had no valid rows for this mode.
    """
    if mode == "delta":
        return select_closest_delta(chain_rows, value, option_type)
    if mode == "closest_premium":
        return _select_closest_premium(chain_rows, value)
    if mode == "strike":
        return _select_by_strike_offset(chain_rows, option_type, strike_drop, atm_strike, strike_interval)
    return None
