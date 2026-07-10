"""
overall_settings.py
────────────────────
All overall-level strategy controls live here.

Covered:
  ┌─ Overall SL            (MTM | PremiumPercentage)
  ├─ Overall Target        (MTM | PremiumPercentage)
  ├─ Overall Re-entry SL   (Immediate | ImmediateReverse | Momentum | MomentumReverse)
  ├─ Overall Re-entry Tgt  (Immediate | ImmediateReverse | Momentum | MomentumReverse)
  └─ [Future: Overall Trail SL, Lock & Trail]

Request JSON schema expected:

    "OverallSL": {
        "Type":  "OverallSLType.MTM",              // or PremiumPercentage | None
        "Value": 5000
    },

    "OverallTgt": {
        "Type":  "OverallTgtType.MTM",             // or PremiumPercentage | None
        "Value": 5000
    },

    "OverallReentrySL": {
        "Type":  "OverallReentryType.Immediate",   // or ImmediateReverse | Momentum | MomentumReverse | None
        "Value": {
            "ReentryCount": 3
        }
    },

    "OverallReentryTgt": {
        "Type":  "OverallReentryType.Immediate",   // or ImmediateReverse | Momentum | MomentumReverse | None
        "Value": {
            "ReentryCount": 3
        }
    }

Priority on same candle: Overall SL / Target fires before individual leg SL / Target.
When both Overall SL and Overall Target fire on the same candle, whichever time is earlier
wins; if equal, Overall SL takes priority (capital protection first).
"""

from __future__ import annotations

from typing import Optional, Tuple, List

try:
    from .debug_flags import debug_print
except ImportError:
    from debug_flags import debug_print

MAX_OVERALL_REENTRIES = 5


# ═══════════════════════════════════════════════════════════════════
# 1. PARSING HELPERS
# ═══════════════════════════════════════════════════════════════════

def parse_overall_sl(strategy: dict) -> Tuple[str, float]:
    """
    Returns (sl_type, sl_val).
      sl_type : "MTM" | "PremiumPercentage" | "None"
      sl_val  : float  (₹ amount for MTM, % value for PremiumPercentage)
    """
    cfg = strategy.get("OverallSL", {})
    t   = cfg.get("Type", "None")
    v   = float(cfg.get("Value", 0))
    if t == "None" or v <= 0:
        return "None", 0.0
    if "PremiumPercentage" in t or "Percentage" in t:
        return "PremiumPercentage", v
    return "MTM", v


def parse_overall_tgt(strategy: dict) -> Tuple[str, float]:
    """
    Returns (tgt_type, tgt_val).
      tgt_type : "MTM" | "PremiumPercentage" | "None"
      tgt_val  : float  (₹ amount for MTM, % value for PremiumPercentage)
    """
    cfg = strategy.get("OverallTgt", {})
    t   = cfg.get("Type", "None")
    v   = float(cfg.get("Value", 0))
    if t == "None" or v <= 0:
        return "None", 0.0
    if "PremiumPercentage" in t or "Percentage" in t:
        return "PremiumPercentage", v
    return "MTM", v


def _parse_overall_reentry(strategy: dict, key: str) -> Tuple[str, int]:
    """Shared parser for OverallReentrySL and OverallReentryTgt."""
    cfg = strategy.get(key, {})
    t = str(cfg.get("Type") or cfg.get("type") or "None").strip()
    if t == "None" or not t:
        return "None", 0

    raw_value = cfg.get("Value")
    if raw_value is None:
        raw_value = cfg.get("value")

    if isinstance(raw_value, dict):
        raw_count = raw_value.get("ReentryCount")
        if raw_count is None:
            raw_count = raw_value.get("count")
    elif raw_value is not None:
        raw_count = raw_value
    else:
        raw_count = cfg.get("Count")
        if raw_count is None:
            raw_count = cfg.get("count")

    try:
        count = min(int(raw_count or 0), MAX_OVERALL_REENTRIES)
    except Exception:
        count = 0

    normalized_type = t.replace(" ", "").lower()

    if "momentumreverse" in normalized_type:
        return "MomentumReverse", count
    if "momentum" in normalized_type:
        return "Momentum", count
    if "immediatereverse" in normalized_type or normalized_type.endswith("reverse"):
        return "ImmediateReverse", count
    if (
        "immediate" in normalized_type
        or "reasap" in normalized_type
        or "reentry" in normalized_type
        or "renetry" in normalized_type
    ):
        return "Immediate", count
    return "Immediate", count


def parse_overall_reentry_sl(strategy: dict) -> Tuple[str, int]:
    """
    Returns (reentry_type, reentry_count) for OverallReentrySL.

      reentry_type  : "Immediate" | "ImmediateReverse" |
                      "Momentum"  | "MomentumReverse"  | "None"
      reentry_count : max number of overall re-entries (capped at MAX_OVERALL_REENTRIES)

    For Momentum types, each leg's own LegMomentum config is used.
    """
    return _parse_overall_reentry(strategy, "OverallReentrySL")


def parse_overall_reentry_tgt(strategy: dict) -> Tuple[str, int]:
    """
    Returns (reentry_type, reentry_count) for OverallReentryTgt.

    Same type options as OverallReentrySL but triggered when profit target is hit.
    For Momentum types, each leg's own LegMomentum config is used.
    """
    return _parse_overall_reentry(strategy, "OverallReentryTgt")


def _build_cycle_sub_trades_for_overall_checks(
    idx,
    day: str,
    entry_time: str,
    exit_time: str,
    legs: list,
    expiries: list,
    step: int,
    lot_size: int,
    spot: float,
    idle_configs: dict = None,
) -> Tuple[list, float]:
    """
    Build the actual sub-trade path for one cycle using the same leg processing
    logic as the backtest engine. This keeps overall SL detection aligned with
    real trail SL, leg re-entry, lazy-leg momentum, and actual entry timing.
    """
    from .backtest_engine import (
        _resolve_expiry,
        _pick_strike,
        _find_momentum_entry,
        _add_one_minute,
        _process_leg,
    )
    from .lazy_leg import process_lazy_legs

    sub_trades: list = []
    total_entry_premium = 0.0

    for leg_num, leg in enumerate(legs, start=1):
        position     = "SELL" if "Sell" in leg["PositionType"] else "BUY"
        otype        = "CE"   if "CE"   in leg["InstrumentKind"] else "PE"
        expiry_kind  = leg.get("ExpiryKind", "ExpiryType.Weekly")
        entry_type   = leg.get("EntryType", "EntryType.EntryByStrikeType")
        strike_param = leg.get("StrikeParameter", "StrikeType.ATM")
        lots         = int(leg["LotConfig"]["Value"])
        sl_type      = leg["LegStopLoss"]["Type"]
        sl_val       = float(leg["LegStopLoss"]["Value"])
        tgt_type     = leg["LegTarget"]["Type"]
        tgt_val      = float(leg["LegTarget"]["Value"])
        momentum_type = leg.get("LegMomentum", {}).get("Type", "None")
        momentum_val  = float(leg.get("LegMomentum", {}).get("Value", 0))
        trail_sl      = leg.get("LegTrailSL", {})
        trail_type    = trail_sl.get("Type", "None")
        trail_x       = float(trail_sl.get("Value", {}).get("InstrumentMove", 0))
        trail_y       = float(trail_sl.get("Value", {}).get("StopLossMove", 0))

        re_sl  = leg.get("LegReentrySL", {})
        re_tp  = leg.get("LegReentryTP", {})
        reentry_sl_type = re_sl.get("Type", "None")
        reentry_tp_type = re_tp.get("Type", "None")
        re_sl_val = re_sl.get("Value", {})
        re_tp_val = re_tp.get("Value", {})
        reentry_sl_count = int(re_sl_val.get("ReentryCount", 0) if isinstance(re_sl_val, dict) else 0) \
            if reentry_sl_type != "None" else 0
        reentry_tp_count = int(re_tp_val.get("ReentryCount", 0) if isinstance(re_tp_val, dict) else 0) \
            if reentry_tp_type != "None" else 0
        reentry_sl_next_ref = re_sl_val.get("NextLegRef") if isinstance(re_sl_val, dict) else None
        reentry_tp_next_ref = re_tp_val.get("NextLegRef") if isinstance(re_tp_val, dict) else None

        expiry = _resolve_expiry(day, expiry_kind, expiries)
        if expiry is None:
            continue

        strike = _pick_strike(idx, day, entry_time, expiry, otype, spot, entry_type, strike_param, step)
        if strike is None:
            continue

        actual_entry_time = entry_time
        override_entry_px = None
        override_base_px = None

        if momentum_type != "None" and momentum_val > 0:
            if "Underlying" in momentum_type:
                base_px = idx.get_spot(day, entry_time)
            else:
                base_px = idx.get_close(day, entry_time, expiry, strike, otype)
            if base_px is None:
                continue

            mom_time, mom_px = _find_momentum_entry(
                idx, day, _add_one_minute(entry_time), exit_time,
                expiry, strike, otype, base_px, momentum_type, momentum_val,
            )
            if mom_time is None:
                continue
            actual_entry_time = mom_time
            override_entry_px = mom_px
            override_base_px = base_px
        else:
            if idx.get_close(day, entry_time, expiry, strike, otype) is None:
                continue

        result = _process_leg(
            idx, day, actual_entry_time, exit_time,
            expiry, strike, otype, position,
            sl_type, sl_val, tgt_type, tgt_val,
            reentry_sl_count, reentry_tp_count,
            reentry_sl_type, reentry_tp_type,
            lots, lot_size,
            entry_type, strike_param, step,
            momentum_type, momentum_val,
            override_entry_px=override_entry_px,
            override_base_px=override_base_px,
            strategy_entry_time=entry_time,
            trail_type=trail_type, trail_x=trail_x, trail_y=trail_y,
            reentry_sl_next_ref=reentry_sl_next_ref,
            reentry_tp_next_ref=reentry_tp_next_ref,
        )

        if result.get("sub_trades"):
            first_trade = result["sub_trades"][0]
            total_entry_premium += first_trade["entry_price"] * lots * lot_size

        for st in result.get("sub_trades", []):
            st["parent_leg_num"] = leg_num
            st["parent_leg_type"] = otype
            sub_trades.append(st)

        if idle_configs and result.get("next_leg_ref"):
            lazy_legs = process_lazy_legs(
                idx, day, exit_time, expiries,
                result["next_leg_ref"],
                result["next_leg_trigger_time"],
                idle_configs, lot_size, step,
            )
            for ll in lazy_legs:
                for st in ll.get("sub_trades", []):
                    st["lazy_leg_id"] = ll["id"]
                    st["parent_leg_num"] = leg_num
                    st["parent_leg_type"] = otype
                    sub_trades.append(st)

    return sub_trades, total_entry_premium


def _build_minute_pnl_from_sub_trades(idx, day: str, sub_trades: list, entry_time: str, exit_time: str) -> list:
    timeline = []
    for t in idx._all_times.get(day, []):
        if t <= entry_time or t > exit_time:
            continue

        combined = 0.0
        for st in sub_trades:
            st_entry = st.get("entry_time", "")
            st_exit = st.get("exit_time", "")
            if not st_entry or st_entry > t:
                continue

            if st_exit and st_exit <= t:
                combined += st.get("pnl", 0)
                continue

            cur_price = idx.get_close(day, t, st.get("_expiry", ""), st.get("strike"), st.get("option_type", ""))
            if cur_price is None:
                continue

            qty = st.get("_lots", 1) * st.get("_lot_size", 1)
            direction = -1 if st.get("entry_action", "SELL") == "SELL" else 1
            combined += (cur_price - st.get("entry_price", 0)) * qty * direction

        timeline.append((t, round(combined, 2)))
    return timeline


# ═══════════════════════════════════════════════════════════════════
# 2. OVERALL SL — DETECTION
# ═══════════════════════════════════════════════════════════════════

def _build_lazy_leg_state(idx, day, trigger_time, expiries, step, lot_size, idle_cfg, spot_at_trigger):
    """Build a leg_state dict for a lazy leg entering at trigger_time (approximation)."""
    from .backtest_engine import (
        _resolve_expiry, _pick_strike,
        _calc_trigger_price, _calc_pnl,
    )
    position    = "SELL" if "Sell" in idle_cfg["PositionType"] else "BUY"
    otype       = "CE"   if "CE"   in idle_cfg["InstrumentKind"] else "PE"
    expiry_kind = idle_cfg.get("ExpiryKind", "ExpiryType.Weekly")
    entry_type  = idle_cfg.get("EntryType",  "EntryType.EntryByStrikeType")
    strike_param= idle_cfg.get("StrikeParameter", "StrikeType.ATM")
    lots        = int(idle_cfg["LotConfig"]["Value"])
    sl_type     = idle_cfg["LegStopLoss"]["Type"]
    sl_val      = float(idle_cfg["LegStopLoss"]["Value"])
    tgt_type    = idle_cfg.get("LegTarget", {}).get("Type", "None")
    tgt_val     = float(idle_cfg.get("LegTarget", {}).get("Value", 0))

    expiry = _resolve_expiry(day, expiry_kind, expiries)
    if expiry is None:
        return None

    strike = _pick_strike(idx, day, trigger_time, expiry, otype, spot_at_trigger,
                          entry_type, strike_param, step)
    if strike is None:
        return None

    entry_price = idx.get_close(day, trigger_time, expiry, strike, otype)
    if entry_price is None:
        return None

    entry_spot = idx.get_spot(day, trigger_time) or 0.0
    sl_px  = _calc_trigger_price(entry_price, entry_spot, position, sl_type,  sl_val,  is_sl=True)
    tgt_px = _calc_trigger_price(entry_price, entry_spot, position, tgt_type, tgt_val, is_sl=False)

    # next lazy leg ref (chain support)
    re_sl      = idle_cfg.get("LegReentrySL", {})
    next_ref   = re_sl.get("Value", {}).get("NextLegRef") if re_sl.get("Type") == "ReentryType.NextLeg" else None

    return {
        "position":     position,
        "otype":        otype,
        "expiry":       expiry,
        "strike":       strike,
        "lots":         lots,
        "lot_size":     lot_size,
        "entry_price":  entry_price,
        "sl_px":        sl_px,
        "tgt_px":       tgt_px,
        "realized_pnl": None,
        "active_from":  trigger_time,   # only count MTM at candles > trigger_time
        "next_leg_ref": next_ref,
    }


def find_overall_sl_exit_time(
    idx,
    day: str,
    entry_time: str,
    exit_time: str,
    legs: list,
    expiries: list,
    step: int,
    lot_size: int,
    overall_sl_type: str,
    overall_sl_val: float,
    spot: float,
    idle_configs: dict = None,
    base_pnl_before_cycle: float = 0.0,
) -> Optional[str]:
    """
    Scan candles minute-by-minute to find when total strategy MTM P&L
    first hits the overall SL threshold.

    Includes lazy legs in the MTM calculation — when a main leg's SL fires
    and triggers a NextLeg (lazy leg), that lazy leg is added to the simulation
    from its trigger candle onwards so the overall SL threshold is computed
    on the full strategy MTM (main + lazy legs combined).
    """
    if overall_sl_type == "None" or overall_sl_val <= 0:
        return None

    sub_trades, total_entry_premium = _build_cycle_sub_trades_for_overall_checks(
        idx, day, entry_time, exit_time, legs, expiries, step, lot_size, spot, idle_configs,
    )
    if not sub_trades:
        return None

    # ── Threshold ─────────────────────────────────────────────────────────────
    if overall_sl_type == "PremiumPercentage":
        threshold = -(total_entry_premium * overall_sl_val / 100)
    else:
        threshold = -overall_sl_val

    for t, total_mtm in _build_minute_pnl_from_sub_trades(idx, day, sub_trades, entry_time, exit_time):
        day_total_mtm = round(base_pnl_before_cycle + total_mtm, 2)
        if day_total_mtm <= threshold:
            return t

    return None


# ═══════════════════════════════════════════════════════════════════
# 3. OVERALL TARGET — DETECTION
# ═══════════════════════════════════════════════════════════════════

def find_overall_tgt_exit_time(
    idx,
    day: str,
    entry_time: str,
    exit_time: str,
    legs: list,
    expiries: list,
    step: int,
    lot_size: int,
    overall_tgt_type: str,
    overall_tgt_val: float,
    spot: float,
    idle_configs: dict = None,
    base_pnl_before_cycle: float = 0.0,
) -> Optional[str]:
    """
    Scan candles minute-by-minute to find when total strategy MTM P&L
    first hits the overall Target threshold (total_pnl >= threshold).

    Logic
    ─────
    • Each leg contributes unrealized MTM PnL while it is still active.
    • When a leg's individual Target fires, its PnL is locked (realized).
    • When a leg's individual SL fires,    its PnL is locked (realized).
    • Returns the first candle time where total_pnl >= threshold, or None.

    Note: Momentum-entry legs are approximated as entering at strategy entry_time.
    """
    from .backtest_engine import (
        _resolve_expiry, _pick_strike,
        _calc_trigger_price, _calc_pnl,
    )

    if overall_tgt_type == "None" or overall_tgt_val <= 0:
        return None

    # ── Build per-leg state ───────────────────────────────────────────────────
    leg_states: list = []
    total_entry_premium = 0.0

    for leg in legs:
        position     = "SELL" if "Sell" in leg["PositionType"] else "BUY"
        otype        = "CE"   if "CE"   in leg["InstrumentKind"] else "PE"
        expiry_kind  = leg.get("ExpiryKind",      "ExpiryType.Weekly")
        entry_type   = leg.get("EntryType",       "EntryType.EntryByStrikeType")
        strike_param = leg.get("StrikeParameter", "StrikeType.ATM")
        lots         = int(leg["LotConfig"]["Value"])
        sl_type      = leg["LegStopLoss"]["Type"]
        sl_val       = float(leg["LegStopLoss"]["Value"])
        tgt_type_leg = leg["LegTarget"]["Type"]
        tgt_val_leg  = float(leg["LegTarget"]["Value"])

        re_sl    = leg.get("LegReentrySL", {})
        next_ref = re_sl.get("Value", {}).get("NextLegRef") if re_sl.get("Type") == "ReentryType.NextLeg" else None

        expiry = _resolve_expiry(day, expiry_kind, expiries)
        if expiry is None:
            return None

        strike = _pick_strike(idx, day, entry_time, expiry, otype, spot,
                              entry_type, strike_param, step)
        if strike is None:
            return None

        entry_price = idx.get_close(day, entry_time, expiry, strike, otype)
        if entry_price is None:
            return None

        entry_spot = idx.get_spot(day, entry_time) or 0.0
        sl_px  = _calc_trigger_price(entry_price, entry_spot, position,
                                     sl_type,     sl_val,     is_sl=True)
        tgt_px = _calc_trigger_price(entry_price, entry_spot, position,
                                     tgt_type_leg, tgt_val_leg, is_sl=False)

        leg_states.append({
            "position":     position,
            "otype":        otype,
            "expiry":       expiry,
            "strike":       strike,
            "lots":         lots,
            "lot_size":     lot_size,
            "entry_price":  entry_price,
            "sl_px":        sl_px,
            "tgt_px":       tgt_px,
            "realized_pnl": None,
            "active_from":  entry_time,
            "next_leg_ref": next_ref,
        })
        total_entry_premium += entry_price * lots * lot_size

    # ── Threshold (always positive) ───────────────────────────────────────────
    if overall_tgt_type == "PremiumPercentage":
        threshold = total_entry_premium * overall_tgt_val / 100
    else:
        threshold = overall_tgt_val

    # ── Scan minute-by-minute ─────────────────────────────────────────────────
    times = sorted(
        t for t in idx._all_times.get(day, [])
        if entry_time < t <= exit_time
    )

    for t in times:
        total_mtm = 0.0
        new_lazy_states = []

        for ls in leg_states:
            if t <= ls.get("active_from", entry_time):
                if ls["realized_pnl"] is not None:
                    total_mtm += ls["realized_pnl"]
                continue

            if ls["realized_pnl"] is not None:
                total_mtm += ls["realized_pnl"]
                continue

            cur_price = idx.get_close(day, t, ls["expiry"], ls["strike"], ls["otype"])
            if cur_price is None:
                continue

            # Lock PnL if individual SL fires
            if ls["sl_px"] is not None:
                sl_hit = (
                    (ls["position"] == "SELL" and cur_price >= ls["sl_px"]) or
                    (ls["position"] == "BUY"  and cur_price <= ls["sl_px"])
                )
                if sl_hit:
                    ls["realized_pnl"] = _calc_pnl(
                        ls["position"], ls["entry_price"], ls["sl_px"],
                        ls["lots"], ls["lot_size"],
                    )
                    total_mtm += ls["realized_pnl"]
                    if ls.get("next_leg_ref") and idle_configs and ls["next_leg_ref"] in idle_configs:
                        spot_now = idx.get_spot(day, t) or spot
                        lazy_state = _build_lazy_leg_state(
                            idx, day, t, expiries, step, lot_size,
                            idle_configs[ls["next_leg_ref"]], spot_now,
                        )
                        if lazy_state:
                            new_lazy_states.append(lazy_state)
                    continue

            # Lock PnL if individual Target fires
            if ls["tgt_px"] is not None:
                tgt_hit = (
                    (ls["position"] == "SELL" and cur_price <= ls["tgt_px"]) or
                    (ls["position"] == "BUY"  and cur_price >= ls["tgt_px"])
                )
                if tgt_hit:
                    ls["realized_pnl"] = _calc_pnl(
                        ls["position"], ls["entry_price"], ls["tgt_px"],
                        ls["lots"], ls["lot_size"],
                    )
                    total_mtm += ls["realized_pnl"]
                    continue

            # Active leg — unrealized MTM
            total_mtm += _calc_pnl(
                ls["position"], ls["entry_price"], cur_price,
                ls["lots"], ls["lot_size"],
            )

        leg_states.extend(new_lazy_states)

        day_total_mtm = round(base_pnl_before_cycle + total_mtm, 2)
        if day_total_mtm >= threshold:
            return t

    return None


def resolve_effective_exit(
    overall_sl_time: Optional[str],
    overall_tgt_time: Optional[str],
    exit_time: str,
) -> Tuple[Optional[str], Optional[str], str]:
    """
    Determine the effective exit time when both Overall SL and Overall Target
    are configured.

    Returns (overall_sl_exit_time, overall_tgt_exit_time, effective_exit).
      • effective_exit = whichever fires first (or exit_time if neither fires).
      • If both fire at the same candle, Overall SL wins (capital protection first).
    """
    if overall_sl_time and overall_tgt_time:
        if overall_sl_time <= overall_tgt_time:
            return overall_sl_time, overall_tgt_time, overall_sl_time
        else:
            return overall_sl_time, overall_tgt_time, overall_tgt_time
    elif overall_sl_time:
        return overall_sl_time, None, overall_sl_time
    elif overall_tgt_time:
        return None, overall_tgt_time, overall_tgt_time
    else:
        return None, None, exit_time


def resolve_all_exits(
    overall_sl_time: Optional[str],
    overall_tgt_time: Optional[str],
    lock_exit_time: Optional[str],
    eod_exit_time: str,
) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
    """
    Resolve all possible exit times (SL/TrailSL, Target, Lock/LockTrail) to a
    single effective_exit.

    Returns (overall_sl_time, overall_tgt_time, lock_exit_time, effective_exit).

    Priority on tie: SL wins over Target/Lock; Target wins over Lock.
    """
    # (time, priority) — lower priority number = fires first on tie
    candidates: list = []
    if overall_sl_time:
        candidates.append((overall_sl_time,  0))
    if overall_tgt_time:
        candidates.append((overall_tgt_time, 1))
    if lock_exit_time:
        candidates.append((lock_exit_time,   2))

    if not candidates:
        return overall_sl_time, overall_tgt_time, lock_exit_time, eod_exit_time

    candidates.sort(key=lambda x: (x[0], x[1]))
    effective_exit = candidates[0][0]
    return overall_sl_time, overall_tgt_time, lock_exit_time, effective_exit


# ═══════════════════════════════════════════════════════════════════
# 4. OVERALL RE-ENTRY ON SL
# ═══════════════════════════════════════════════════════════════════

def run_overall_reentry(
    idx,
    day: str,
    trigger_time: str,   # time at which overall SL fired (= new entry window start)
    exit_time: str,      # strategy EOD exit
    leg_configs: list,   # original ListOfLegConfigs from strategy
    expiries: list,
    step: int,
    lot_size: int,
    idle_configs: dict,  # IdleLegConfigs for lazy legs
    overall_sl_type: str,
    overall_sl_val: float,
    overall_tgt_type: str,
    overall_tgt_val: float,
    reentry_type: str,   # "Immediate"|"ImmediateReverse"|"Momentum"|"MomentumReverse"
    reentries_left: int,
    cycle_number: int,   # 1 = first re-entry, 2 = second, …
    base_pnl_before_cycle: float = 0.0,
) -> List[dict]:
    """
    Execute one overall re-entry cycle starting from trigger_time.

    Momentum types (Momentum / MomentumReverse):
      Each leg's own LegMomentum config is used — same momentum condition
      that was configured on that leg in the original strategy.
      If a leg has no LegMomentum, it enters immediately (like Immediate type).

    Returns a list of leg dicts (same shape as day_trade["legs"]) tagged
    with "overall_reentry_cycle": cycle_number.

    Recursion: if the new cycle itself hits overall SL AND reentries_left > 1,
    this function calls itself for the next cycle.
    """
    # Deferred imports to avoid circular dependency
    from .backtest_engine import (
        _process_leg, _pick_strike, _resolve_expiry,
        _add_one_minute, _find_momentum_entry,
        _calc_pnl,
    )
    from .lazy_leg import process_lazy_legs

    tag = f"[OverallReentry cycle={cycle_number} day={day}]"

    if trigger_time >= exit_time or reentries_left <= 0:
        debug_print(f"{tag} SKIP: trigger_time={trigger_time} exit_time={exit_time} reentries_left={reentries_left}")
        return []

    is_reverse  = "Reverse"  in reentry_type
    is_momentum = "Momentum" in reentry_type

    spot = idx.get_spot(day, trigger_time)
    if spot is None:
        debug_print(f"{tag} SKIP: no spot at trigger_time={trigger_time}")
        return []

    debug_print(f"{tag} type={reentry_type} spot={spot:.2f} reentries_left={reentries_left}")

    # ── Find effective exit for this cycle (may be shortened by another overall SL) ──
    cycle_sl_val = overall_sl_val * (cycle_number + 1) if overall_sl_type != "None" else overall_sl_val
    cycle_tgt_val = overall_tgt_val * (cycle_number + 1) if overall_tgt_type != "None" else overall_tgt_val
    next_overall_sl_time = find_overall_sl_exit_time(
        idx, day, trigger_time, exit_time,
        leg_configs, expiries, step, lot_size,
        overall_sl_type, cycle_sl_val, spot,
        idle_configs=idle_configs,
        base_pnl_before_cycle=base_pnl_before_cycle,
    )
    next_overall_tgt_time = find_overall_tgt_exit_time(
        idx, day, trigger_time, exit_time,
        leg_configs, expiries, step, lot_size,
        overall_tgt_type, cycle_tgt_val, spot,
        idle_configs=idle_configs,
        base_pnl_before_cycle=base_pnl_before_cycle,
    )
    next_overall_sl_time, next_overall_tgt_time, effective_exit = resolve_effective_exit(
        next_overall_sl_time, next_overall_tgt_time, exit_time
    )

    # ── Process each leg fresh ────────────────────────────────────────────────
    cycle_legs: List[dict] = []

    for leg_num, leg in enumerate(leg_configs, start=1):
        position     = "SELL" if "Sell" in leg["PositionType"] else "BUY"
        if is_reverse:
            position = "BUY" if position == "SELL" else "SELL"

        otype        = "CE"   if "CE"   in leg["InstrumentKind"] else "PE"
        expiry_kind  = leg.get("ExpiryKind",      "ExpiryType.Weekly")
        entry_type   = leg.get("EntryType",       "EntryType.EntryByStrikeType")
        strike_param = leg.get("StrikeParameter", "StrikeType.ATM")
        lots         = int(leg["LotConfig"]["Value"])
        sl_type      = leg["LegStopLoss"]["Type"]
        sl_val       = float(leg["LegStopLoss"]["Value"])
        tgt_type     = leg["LegTarget"]["Type"]
        tgt_val      = float(leg["LegTarget"]["Value"])
        trail_sl     = leg.get("LegTrailSL", {})
        trail_type   = trail_sl.get("Type", "None")
        trail_x      = float(trail_sl.get("Value", {}).get("InstrumentMove", 0))
        trail_y      = float(trail_sl.get("Value", {}).get("StopLossMove",   0))

        re_sl  = leg.get("LegReentrySL", {})
        re_tp  = leg.get("LegReentryTP", {})
        reentry_sl_type  = re_sl.get("Type", "None")
        reentry_tp_type  = re_tp.get("Type", "None")
        _re_sl_v = re_sl.get("Value", {})
        _re_tp_v = re_tp.get("Value", {})
        reentry_sl_count = int(_re_sl_v.get("ReentryCount", 0) if isinstance(_re_sl_v, dict) else 0) \
                           if reentry_sl_type != "None" else 0
        reentry_tp_count = int(_re_tp_v.get("ReentryCount", 0) if isinstance(_re_tp_v, dict) else 0) \
                           if reentry_tp_type != "None" else 0
        reentry_sl_next_ref = _re_sl_v.get("NextLegRef") if isinstance(_re_sl_v, dict) else None
        reentry_tp_next_ref = _re_tp_v.get("NextLegRef") if isinstance(_re_tp_v, dict) else None

        expiry = _resolve_expiry(day, expiry_kind, expiries)
        if expiry is None:
            debug_print(f"{tag} SKIP leg {leg.get('id')}: expiry not resolved")
            continue

        strike = _pick_strike(idx, day, trigger_time, expiry, otype, spot,
                              entry_type, strike_param, step)
        if strike is None:
            debug_print(f"{tag} SKIP leg {leg.get('id')}: strike not resolved")
            continue

        # ── Momentum wait — uses leg's own LegMomentum config ────────────────
        actual_entry_time  = trigger_time
        override_entry_px  = None
        override_base_px   = None

        # Read this leg's own momentum settings
        leg_mom_type = leg.get("LegMomentum", {}).get("Type",  "None")
        leg_mom_val  = float(leg.get("LegMomentum", {}).get("Value", 0))

        if is_momentum and leg_mom_type != "None" and leg_mom_val > 0:
            # Use leg's LegMomentum — same condition as the original strategy entry
            if "Underlying" in leg_mom_type:
                base_px = idx.get_spot(day, trigger_time)
            else:
                base_px = idx.get_close(day, trigger_time, expiry, strike, otype)

            if base_px is None:
                debug_print(f"{tag} SKIP leg {leg.get('id')}: no base_px for leg momentum")
                continue

            mom_time, mom_px = _find_momentum_entry(
                idx, day, _add_one_minute(trigger_time), effective_exit,
                expiry, strike, otype, base_px, leg_mom_type, leg_mom_val,
            )
            if mom_time is None:
                debug_print(f"{tag} SKIP leg {leg.get('id')}: leg momentum not triggered after re-entry")
                continue

            actual_entry_time = mom_time
            override_entry_px = mom_px
            override_base_px  = base_px

        else:
            # Immediate type OR Momentum type but leg has no LegMomentum → enter instantly
            if idx.get_close(day, trigger_time, expiry, strike, otype) is None:
                debug_print(f"{tag} SKIP leg {leg.get('id')}: no entry candle at trigger_time")
                continue

        # ── Run the leg ───────────────────────────────────────────────────────
        result = _process_leg(
            idx, day, actual_entry_time, effective_exit,
            expiry, strike, otype, position,
            sl_type, sl_val, tgt_type, tgt_val,
            reentry_sl_count, reentry_tp_count,
            reentry_sl_type, reentry_tp_type,
            lots, lot_size,
            entry_type, strike_param, step,
            override_entry_px=override_entry_px,
            override_base_px=override_base_px,
            strategy_entry_time=trigger_time,
            trail_type=trail_type, trail_x=trail_x, trail_y=trail_y,
            reentry_sl_next_ref=reentry_sl_next_ref,
            reentry_tp_next_ref=reentry_tp_next_ref,
        )

        for st in result["sub_trades"]:
            st["parent_leg_num"]        = leg_num
            st["parent_leg_type"]       = otype
            st["overall_reentry_cycle"] = cycle_number

        leg_dict = {
            "id":           leg["id"],
            "expiry":       expiry,
            "strike":       strike,
            "type":         otype,
            "position":     position,
            "entry_time":   result["entry_time"],
            "entry_price":  result["entry_price"],
            "exit_time":    result["exit_time"],
            "exit_price":   result["exit_price"],
            "exit_reason":  result["exit_reason"],
            "reentries":    result["reentries"],
            "lots":         lots,
            "lot_size":     lot_size,
            "pnl":          result["total_leg_pnl"],
            "sub_trades":   result["sub_trades"],
            "overall_reentry_cycle": cycle_number,
            "leg_num":      leg_num,
        }

        # ── Lazy legs (NextLeg triggered inside this cycle) ───────────────────
        if idle_configs and result.get("next_leg_ref"):
            lazy_legs = process_lazy_legs(
                idx, day, effective_exit, expiries,
                result["next_leg_ref"],
                result["next_leg_trigger_time"],
                idle_configs, lot_size, step,
            )
            for ll in lazy_legs:
                for st in ll.get("sub_trades", []):
                    st["lazy_leg_id"] = ll["id"]
                    st["parent_leg_num"]        = leg_num
                    st["parent_leg_type"]       = otype
                    st["overall_reentry_cycle"] = cycle_number
                    inner = st.get("reentry_type", "Initial")
                    st["reentry_type"] = (
                        f"Lazy({ll['id']})" if inner == "Initial"
                        else f"Lazy({ll['id']})/{inner}"
                    )
                    leg_dict["sub_trades"].append(st)
                leg_dict["pnl"] = round(leg_dict["pnl"] + ll["pnl"], 2)
                if ll["exit_time"] and ll["exit_time"] > leg_dict["exit_time"]:
                    leg_dict["exit_time"]   = ll["exit_time"]
                    leg_dict["exit_price"]  = ll["exit_price"]
                    leg_dict["exit_reason"] = ll["exit_reason"]

        cycle_legs.append(leg_dict)

    if not cycle_legs:
        return []

    if next_overall_sl_time:
        for leg in cycle_legs:
            for st in leg.get("sub_trades", []):
                if st.get("exit_time") == next_overall_sl_time and st.get("exit_reason") == "Time Exit":
                    st["exit_reason"] = "Overall SL"
            if leg.get("exit_time") == next_overall_sl_time and leg.get("exit_reason") == "Time Exit":
                leg["exit_reason"] = "Overall SL"

    if next_overall_tgt_time:
        for leg in cycle_legs:
            for st in leg.get("sub_trades", []):
                if st.get("exit_time") == next_overall_tgt_time and st.get("exit_reason") == "Time Exit":
                    st["exit_reason"] = "Overall Target"
            if leg.get("exit_time") == next_overall_tgt_time and leg.get("exit_reason") == "Time Exit":
                leg["exit_reason"] = "Overall Target"

    # ── If overall SL fires again in this cycle → recurse ────────────────────
    cycle_pnl = round(sum(leg.get("pnl", 0) for leg in cycle_legs), 2)
    next_base_pnl = round(base_pnl_before_cycle + cycle_pnl, 2)

    if next_overall_sl_time and effective_exit == next_overall_sl_time and reentries_left > 1:
        debug_print(f"{tag} overall SL fires again at {next_overall_sl_time} → cycle {cycle_number + 1}")
        next_cycle = run_overall_reentry(
            idx, day,
            trigger_time   = next_overall_sl_time,
            exit_time      = exit_time,
            leg_configs    = leg_configs,
            expiries       = expiries,
            step           = step,
            lot_size       = lot_size,
            idle_configs   = idle_configs,
            overall_sl_type  = overall_sl_type,
            overall_sl_val   = overall_sl_val,
            overall_tgt_type = overall_tgt_type,
            overall_tgt_val  = overall_tgt_val,
            reentry_type   = reentry_type,
            reentries_left = reentries_left - 1,
            cycle_number   = cycle_number + 1,
            base_pnl_before_cycle = next_base_pnl,
        )
        cycle_legs.extend(next_cycle)

    return cycle_legs


# ═══════════════════════════════════════════════════════════════════
# 5. OVERALL RE-ENTRY ON TARGET
# ═══════════════════════════════════════════════════════════════════

def run_overall_reentry_tgt(
    idx,
    day: str,
    trigger_time: str,    # time at which overall Target fired (= new entry window start)
    exit_time: str,       # strategy EOD exit
    leg_configs: list,    # original ListOfLegConfigs from strategy
    expiries: list,
    step: int,
    lot_size: int,
    idle_configs: dict,   # IdleLegConfigs for lazy legs
    overall_sl_type: str,
    overall_sl_val: float,
    overall_tgt_type: str,
    overall_tgt_val: float,
    reentry_type: str,    # "Immediate"|"ImmediateReverse"|"Momentum"|"MomentumReverse"
    reentries_left: int,
    cycle_number: int,    # 1 = first re-entry, 2 = second, …
    base_pnl_before_cycle: float = 0.0,
) -> List[dict]:
    """
    Execute one overall re-entry cycle starting from trigger_time after profit target hit.

    Full strategy reset for each cycle:
      • New ATM strike at current spot
      • New SL / Target thresholds
      • New momentum condition if applicable

    Momentum types (Momentum / MomentumReverse):
      Each leg's own LegMomentum config is used — same as the original strategy.
      If a leg has no LegMomentum, it enters immediately.

    Recursion: if the new cycle itself hits overall Target again AND reentries_left > 1,
    this function calls itself for the next cycle.
    If the new cycle hits overall SL first, no further target re-entry is triggered.

    Returns a list of leg dicts tagged with "overall_reentry_tgt_cycle": cycle_number.
    """
    from .backtest_engine import (
        _process_leg, _pick_strike, _resolve_expiry,
        _add_one_minute, _find_momentum_entry,
        _calc_pnl,
    )
    from .lazy_leg import process_lazy_legs

    tag = f"[OverallReentryTgt cycle={cycle_number} day={day}]"

    if trigger_time >= exit_time or reentries_left <= 0:
        debug_print(f"{tag} SKIP: trigger_time={trigger_time} exit_time={exit_time} reentries_left={reentries_left}")
        return []

    is_reverse  = "Reverse"  in reentry_type
    is_momentum = "Momentum" in reentry_type

    spot = idx.get_spot(day, trigger_time)
    if spot is None:
        debug_print(f"{tag} SKIP: no spot at trigger_time={trigger_time}")
        return []

    debug_print(f"{tag} type={reentry_type} spot={spot:.2f} reentries_left={reentries_left}")

    # ── Find effective exit for this cycle (SL or Target may fire, SL wins on tie) ──
    cycle_sl_val = overall_sl_val * (cycle_number + 1) if overall_sl_type != "None" else overall_sl_val
    cycle_tgt_val = overall_tgt_val * (cycle_number + 1) if overall_tgt_type != "None" else overall_tgt_val
    next_sl_time = find_overall_sl_exit_time(
        idx, day, trigger_time, exit_time,
        leg_configs, expiries, step, lot_size,
        overall_sl_type, cycle_sl_val, spot,
        idle_configs=idle_configs,
        base_pnl_before_cycle=base_pnl_before_cycle,
    )
    next_tgt_time = find_overall_tgt_exit_time(
        idx, day, trigger_time, exit_time,
        leg_configs, expiries, step, lot_size,
        overall_tgt_type, cycle_tgt_val, spot,
        idle_configs=idle_configs,
        base_pnl_before_cycle=base_pnl_before_cycle,
    )
    next_sl_time, next_tgt_time, effective_exit = resolve_effective_exit(
        next_sl_time, next_tgt_time, exit_time
    )

    # ── Process each leg fresh ────────────────────────────────────────────────
    cycle_legs: List[dict] = []

    for leg_num, leg in enumerate(leg_configs, start=1):
        position     = "SELL" if "Sell" in leg["PositionType"] else "BUY"
        if is_reverse:
            position = "BUY" if position == "SELL" else "SELL"

        otype        = "CE"   if "CE"   in leg["InstrumentKind"] else "PE"
        expiry_kind  = leg.get("ExpiryKind",      "ExpiryType.Weekly")
        entry_type   = leg.get("EntryType",       "EntryType.EntryByStrikeType")
        strike_param = leg.get("StrikeParameter", "StrikeType.ATM")
        lots         = int(leg["LotConfig"]["Value"])
        sl_type      = leg["LegStopLoss"]["Type"]
        sl_val       = float(leg["LegStopLoss"]["Value"])
        tgt_type     = leg["LegTarget"]["Type"]
        tgt_val      = float(leg["LegTarget"]["Value"])
        trail_sl     = leg.get("LegTrailSL", {})
        trail_type   = trail_sl.get("Type", "None")
        trail_x      = float(trail_sl.get("Value", {}).get("InstrumentMove", 0))
        trail_y      = float(trail_sl.get("Value", {}).get("StopLossMove",   0))

        re_sl  = leg.get("LegReentrySL", {})
        re_tp  = leg.get("LegReentryTP", {})
        reentry_sl_type  = re_sl.get("Type", "None")
        reentry_tp_type  = re_tp.get("Type", "None")
        _re_sl_v = re_sl.get("Value", {})
        _re_tp_v = re_tp.get("Value", {})
        reentry_sl_count = int(_re_sl_v.get("ReentryCount", 0) if isinstance(_re_sl_v, dict) else 0) \
                           if reentry_sl_type != "None" else 0
        reentry_tp_count = int(_re_tp_v.get("ReentryCount", 0) if isinstance(_re_tp_v, dict) else 0) \
                           if reentry_tp_type != "None" else 0
        reentry_sl_next_ref = _re_sl_v.get("NextLegRef") if isinstance(_re_sl_v, dict) else None
        reentry_tp_next_ref = _re_tp_v.get("NextLegRef") if isinstance(_re_tp_v, dict) else None

        expiry = _resolve_expiry(day, expiry_kind, expiries)
        if expiry is None:
            debug_print(f"{tag} SKIP leg {leg.get('id')}: expiry not resolved")
            continue

        strike = _pick_strike(idx, day, trigger_time, expiry, otype, spot,
                              entry_type, strike_param, step)
        if strike is None:
            debug_print(f"{tag} SKIP leg {leg.get('id')}: strike not resolved")
            continue

        # ── Momentum wait — uses leg's own LegMomentum config ────────────────
        actual_entry_time = trigger_time
        override_entry_px = None
        override_base_px  = None

        leg_mom_type = leg.get("LegMomentum", {}).get("Type",  "None")
        leg_mom_val  = float(leg.get("LegMomentum", {}).get("Value", 0))

        if is_momentum and leg_mom_type != "None" and leg_mom_val > 0:
            if "Underlying" in leg_mom_type:
                base_px = idx.get_spot(day, trigger_time)
            else:
                base_px = idx.get_close(day, trigger_time, expiry, strike, otype)

            if base_px is None:
                debug_print(f"{tag} SKIP leg {leg.get('id')}: no base_px for leg momentum")
                continue

            mom_time, mom_px = _find_momentum_entry(
                idx, day, _add_one_minute(trigger_time), effective_exit,
                expiry, strike, otype, base_px, leg_mom_type, leg_mom_val,
            )
            if mom_time is None:
                debug_print(f"{tag} SKIP leg {leg.get('id')}: leg momentum not triggered after target re-entry")
                continue

            actual_entry_time = mom_time
            override_entry_px = mom_px
            override_base_px  = base_px

        else:
            if idx.get_close(day, trigger_time, expiry, strike, otype) is None:
                debug_print(f"{tag} SKIP leg {leg.get('id')}: no entry candle at trigger_time")
                continue

        # ── Run the leg ───────────────────────────────────────────────────────
        result = _process_leg(
            idx, day, actual_entry_time, effective_exit,
            expiry, strike, otype, position,
            sl_type, sl_val, tgt_type, tgt_val,
            reentry_sl_count, reentry_tp_count,
            reentry_sl_type, reentry_tp_type,
            lots, lot_size,
            entry_type, strike_param, step,
            override_entry_px=override_entry_px,
            override_base_px=override_base_px,
            strategy_entry_time=trigger_time,
            trail_type=trail_type, trail_x=trail_x, trail_y=trail_y,
            reentry_sl_next_ref=reentry_sl_next_ref,
            reentry_tp_next_ref=reentry_tp_next_ref,
        )

        for st in result["sub_trades"]:
            st["parent_leg_num"]        = leg_num
            st["parent_leg_type"]       = otype
            st["overall_reentry_cycle"] = cycle_number

        leg_dict = {
            "id":           leg["id"],
            "expiry":       expiry,
            "strike":       strike,
            "type":         otype,
            "position":     position,
            "entry_time":   result["entry_time"],
            "entry_price":  result["entry_price"],
            "exit_time":    result["exit_time"],
            "exit_price":   result["exit_price"],
            "exit_reason":  result["exit_reason"],
            "reentries":    result["reentries"],
            "lots":         lots,
            "lot_size":     lot_size,
            "pnl":          result["total_leg_pnl"],
            "sub_trades":   result["sub_trades"],
            "overall_reentry_tgt_cycle": cycle_number,
            "leg_num":      leg_num,
        }

        # ── Lazy legs (NextLeg triggered inside this cycle) ───────────────────
        if idle_configs and result.get("next_leg_ref"):
            lazy_legs = process_lazy_legs(
                idx, day, effective_exit, expiries,
                result["next_leg_ref"],
                result["next_leg_trigger_time"],
                idle_configs, lot_size, step,
            )
            for ll in lazy_legs:
                for st in ll.get("sub_trades", []):
                    st["lazy_leg_id"] = ll["id"]
                    st["parent_leg_num"]        = leg_num
                    st["parent_leg_type"]       = otype
                    st["overall_reentry_cycle"] = cycle_number
                    inner = st.get("reentry_type", "Initial")
                    st["reentry_type"] = (
                        f"Lazy({ll['id']})" if inner == "Initial"
                        else f"Lazy({ll['id']})/{inner}"
                    )
                    leg_dict["sub_trades"].append(st)
                leg_dict["pnl"] = round(leg_dict["pnl"] + ll["pnl"], 2)
                if ll["exit_time"] and ll["exit_time"] > leg_dict["exit_time"]:
                    leg_dict["exit_time"]   = ll["exit_time"]
                    leg_dict["exit_price"]  = ll["exit_price"]
                    leg_dict["exit_reason"] = ll["exit_reason"]

        cycle_legs.append(leg_dict)

    if not cycle_legs:
        return []

    if next_sl_time:
        for leg in cycle_legs:
            for st in leg.get("sub_trades", []):
                if st.get("exit_time") == next_sl_time and st.get("exit_reason") == "Time Exit":
                    st["exit_reason"] = "Overall SL"
            if leg.get("exit_time") == next_sl_time and leg.get("exit_reason") == "Time Exit":
                leg["exit_reason"] = "Overall SL"

    if next_tgt_time:
        for leg in cycle_legs:
            for st in leg.get("sub_trades", []):
                if st.get("exit_time") == next_tgt_time and st.get("exit_reason") == "Time Exit":
                    st["exit_reason"] = "Overall Target"
            if leg.get("exit_time") == next_tgt_time and leg.get("exit_reason") == "Time Exit":
                leg["exit_reason"] = "Overall Target"

    # ── If overall Target fires again (and fired before SL) → recurse ─────────
    cycle_pnl = round(sum(leg.get("pnl", 0) for leg in cycle_legs), 2)
    next_base_pnl = round(base_pnl_before_cycle + cycle_pnl, 2)
    target_fired_first = (
        next_tgt_time is not None
        and effective_exit == next_tgt_time
        and reentries_left > 1
    )
    if target_fired_first:
        debug_print(f"{tag} overall Target fires again at {next_tgt_time} → cycle {cycle_number + 1}")
        next_cycle = run_overall_reentry_tgt(
            idx, day,
            trigger_time     = next_tgt_time,
            exit_time        = exit_time,
            leg_configs      = leg_configs,
            expiries         = expiries,
            step             = step,
            lot_size         = lot_size,
            idle_configs     = idle_configs,
            overall_sl_type  = overall_sl_type,
            overall_sl_val   = overall_sl_val,
            overall_tgt_type = overall_tgt_type,
            overall_tgt_val  = overall_tgt_val,
            reentry_type     = reentry_type,
            reentries_left   = reentries_left - 1,
            cycle_number     = cycle_number + 1,
            base_pnl_before_cycle = next_base_pnl,
        )
        cycle_legs.extend(next_cycle)

    return cycle_legs


# ═══════════════════════════════════════════════════════════════════
# 7. SHARED MTM SCAN HELPERS  (used by Lock / Trail SL)
# ═══════════════════════════════════════════════════════════════════

def _build_leg_scan_states(
    idx,
    day: str,
    entry_time: str,
    legs: list,
    expiries: list,
    step: int,
    lot_size: int,
    spot: float,
):
    """
    Build per-leg state dicts for minute-by-minute MTM scanning.
    Includes both SL and TGT trigger prices so the scan loop can lock
    realized PnL when either individual trigger fires.

    Returns (leg_states, total_entry_premium) or (None, None) on failure.
    """
    from .backtest_engine import _resolve_expiry, _pick_strike, _calc_trigger_price

    leg_states: list = []
    total_entry_premium = 0.0

    for leg in legs:
        position     = "SELL" if "Sell" in leg["PositionType"] else "BUY"
        otype        = "CE"   if "CE"   in leg["InstrumentKind"] else "PE"
        expiry_kind  = leg.get("ExpiryKind",      "ExpiryType.Weekly")
        entry_type   = leg.get("EntryType",       "EntryType.EntryByStrikeType")
        strike_param = leg.get("StrikeParameter", "StrikeType.ATM")
        lots         = int(leg["LotConfig"]["Value"])
        sl_type      = leg["LegStopLoss"]["Type"]
        sl_val       = float(leg["LegStopLoss"]["Value"])
        tgt_type     = leg["LegTarget"]["Type"]
        tgt_val      = float(leg["LegTarget"]["Value"])

        expiry = _resolve_expiry(day, expiry_kind, expiries)
        if expiry is None:
            return None, None

        strike = _pick_strike(idx, day, entry_time, expiry, otype, spot,
                              entry_type, strike_param, step)
        if strike is None:
            return None, None

        entry_price = idx.get_close(day, entry_time, expiry, strike, otype)
        if entry_price is None:
            return None, None

        entry_spot = idx.get_spot(day, entry_time) or 0.0
        sl_px  = _calc_trigger_price(entry_price, entry_spot, position,
                                     sl_type,  sl_val,  is_sl=True)
        tgt_px = _calc_trigger_price(entry_price, entry_spot, position,
                                     tgt_type, tgt_val, is_sl=False)

        leg_states.append({
            "position":     position,
            "otype":        otype,
            "expiry":       expiry,
            "strike":       strike,
            "lots":         lots,
            "lot_size":     lot_size,
            "entry_price":  entry_price,
            "sl_px":        sl_px,
            "tgt_px":       tgt_px,
            "realized_pnl": None,   # None = still active
        })
        total_entry_premium += entry_price * lots * lot_size

    return leg_states, total_entry_premium


def _iter_total_mtm(idx, day: str, entry_time: str, exit_time: str, leg_states: list):
    """
    Generator: yields (time_str, total_mtm) for each candle from entry+1 to exit.

    Per-leg PnL is locked (realized_pnl set) when individual SL or TGT fires.
    Mutates leg_states in place — do NOT reuse the same leg_states across
    multiple detection calls.
    """
    from .backtest_engine import _calc_pnl

    times = sorted(
        t for t in idx._all_times.get(day, [])
        if entry_time < t <= exit_time
    )

    for t in times:
        total_mtm = 0.0

        for ls in leg_states:
            if ls["realized_pnl"] is not None:
                total_mtm += ls["realized_pnl"]
                continue

            cur_price = idx.get_close(day, t, ls["expiry"], ls["strike"], ls["otype"])
            if cur_price is None:
                continue

            # Lock on individual SL
            if ls["sl_px"] is not None:
                sl_hit = (
                    (ls["position"] == "SELL" and cur_price >= ls["sl_px"]) or
                    (ls["position"] == "BUY"  and cur_price <= ls["sl_px"])
                )
                if sl_hit:
                    ls["realized_pnl"] = _calc_pnl(
                        ls["position"], ls["entry_price"], ls["sl_px"],
                        ls["lots"], ls["lot_size"],
                    )
                    total_mtm += ls["realized_pnl"]
                    continue

            # Lock on individual Target
            if ls["tgt_px"] is not None:
                tgt_hit = (
                    (ls["position"] == "SELL" and cur_price <= ls["tgt_px"]) or
                    (ls["position"] == "BUY"  and cur_price >= ls["tgt_px"])
                )
                if tgt_hit:
                    ls["realized_pnl"] = _calc_pnl(
                        ls["position"], ls["entry_price"], ls["tgt_px"],
                        ls["lots"], ls["lot_size"],
                    )
                    total_mtm += ls["realized_pnl"]
                    continue

            total_mtm += _calc_pnl(
                ls["position"], ls["entry_price"], cur_price,
                ls["lots"], ls["lot_size"],
            )

        yield t, total_mtm


# ═══════════════════════════════════════════════════════════════════
# 8. LOCK  /  LOCK AND TRAIL
# ═══════════════════════════════════════════════════════════════════

def parse_lock_and_trail(strategy: dict) -> Tuple[str, float, float, float, float]:
    """
    Returns (lock_type, trigger_profit, lock_profit, trail_for_every, trail_by).

      lock_type       : "Lock" | "LockAndTrail" | "None"
      trigger_profit  : total P&L level that activates the lock
      lock_profit     : P&L floor — exit when total P&L falls to or below this
      trail_for_every : (LockAndTrail only) raise the floor every X ₹ of extra profit
      trail_by        : (LockAndTrail only) ₹ to raise the floor per step

    JSON:
      "LockAndTrail": {"Type": "LockAndTrailType.Lock",
                       "Value": {"TriggerProfit": 10000, "LockProfit": 5000}}
      "LockAndTrail": {"Type": "LockAndTrailType.LockAndTrail",
                       "Value": {"TriggerProfit": 10000, "LockProfit": 5000,
                                 "TrailForEvery": 2000,  "TrailBy": 1000}}
    """
    cfg = strategy.get("LockAndTrail", {})
    t   = cfg.get("Type", "None")
    val = cfg.get("Value", {})

    if t == "None" or not isinstance(val, dict):
        return "None", 0.0, 0.0, 0.0, 0.0

    trigger = float(val.get("TriggerProfit", 0))
    lock    = float(val.get("LockProfit",    0))

    if trigger <= 0:
        return "None", 0.0, 0.0, 0.0, 0.0

    if "LockAndTrail" in t:
        trail_for_every = float(val.get("TrailForEvery", 0))
        trail_by        = float(val.get("TrailBy",        0))
        if trail_for_every <= 0 or trail_by <= 0:
            return "None", 0.0, 0.0, 0.0, 0.0
        return "LockAndTrail", trigger, lock, trail_for_every, trail_by

    if "Lock" in t:
        return "Lock", trigger, lock, 0.0, 0.0

    return "None", 0.0, 0.0, 0.0, 0.0


def find_lock_exit_time(
    idx,
    day: str,
    entry_time: str,
    exit_time: str,
    legs: list,
    expiries: list,
    step: int,
    lot_size: int,
    trigger_profit: float,
    lock_profit: float,
    spot: float,
) -> Optional[str]:
    """
    Lock feature exit detection.

    Logic:
      1. Scan minute-by-minute until total P&L >= trigger_profit → lock activated.
      2. Once locked, return the first candle where total P&L <= lock_profit.
    """
    if trigger_profit <= 0:
        return None

    leg_states, _ = _build_leg_scan_states(
        idx, day, entry_time, legs, expiries, step, lot_size, spot
    )
    if leg_states is None:
        return None

    lock_activated = False

    for t, total_mtm in _iter_total_mtm(idx, day, entry_time, exit_time, leg_states):
        if not lock_activated and total_mtm >= trigger_profit:
            lock_activated = True
        if lock_activated and total_mtm <= lock_profit:
            return t

    return None


def find_lock_trail_exit_time(
    idx,
    day: str,
    entry_time: str,
    exit_time: str,
    legs: list,
    expiries: list,
    step: int,
    lot_size: int,
    trigger_profit: float,
    lock_profit: float,
    trail_for_every: float,
    trail_by: float,
    spot: float,
) -> Optional[str]:
    """
    Lock and Trail feature exit detection.

    Logic:
      1. Once total P&L >= trigger_profit → floor = lock_profit (activated).
      2. For every trail_for_every additional profit above trigger, raise floor by trail_by.
      3. Return the first candle where total P&L falls below the current floor.

    Example:  trigger=10000, lock=5000, trail_for_every=2000, trail_by=1000
      P&L hits 10000 → floor = 5000
      P&L hits 12000 → floor = 6000
      P&L hits 14000 → floor = 7000
      P&L falls to 6800 → exit (below floor 7000)
    """
    if trigger_profit <= 0 or trail_for_every <= 0:
        return None

    leg_states, _ = _build_leg_scan_states(
        idx, day, entry_time, legs, expiries, step, lot_size, spot
    )
    if leg_states is None:
        return None

    lock_activated   = False
    lock_floor       = lock_profit
    next_trail_level = trigger_profit + trail_for_every

    for t, total_mtm in _iter_total_mtm(idx, day, entry_time, exit_time, leg_states):
        if not lock_activated:
            if total_mtm >= trigger_profit:
                lock_activated = True

        if lock_activated:
            # Trail the floor upward as profit climbs
            while total_mtm >= next_trail_level:
                lock_floor       += trail_by
                next_trail_level += trail_for_every

            if total_mtm <= lock_floor:
                return t

    return None


# ═══════════════════════════════════════════════════════════════════
# 9. OVERALL TRAIL SL
# ═══════════════════════════════════════════════════════════════════

def parse_overall_trail_sl(strategy: dict) -> Tuple[str, float, float]:
    """
    Returns (trail_type, trail_for_every, trail_by).

      trail_type      : "MTM" | "PremiumPercentage" | "None"
      trail_for_every : profit increment that triggers one trail step
      trail_by        : SL improvement per step (₹ for MTM, % for PremiumPercentage)

    Requires OverallSL to be configured; ignored otherwise.

    JSON:
      "OverallTrailSL": {"Type": "OverallTrailSLType.MTM",
                         "Value": {"TrailForEvery": 3000, "TrailBy": 1500}}
    """
    cfg = strategy.get("OverallTrailSL", {})
    t   = cfg.get("Type", "None")
    val = cfg.get("Value", {})

    if t == "None" or not isinstance(val, dict):
        return "None", 0.0, 0.0

    for_every = float(val.get("TrailForEvery", 0))
    by        = float(val.get("TrailBy",        0))

    if for_every <= 0 or by <= 0:
        return "None", 0.0, 0.0

    if "PremiumPercentage" in t or "Percentage" in t:
        return "PremiumPercentage", for_every, by
    return "MTM", for_every, by


def find_trail_sl_exit_time(
    idx,
    day: str,
    entry_time: str,
    exit_time: str,
    legs: list,
    expiries: list,
    step: int,
    lot_size: int,
    overall_sl_type: str,
    overall_sl_val: float,
    trail_sl_type: str,
    trail_for_every: float,
    trail_by: float,
    spot: float,
) -> Optional[str]:
    """
    Dynamic Overall Trail SL — improves (moves toward profit) as P&L rises.

    Supersedes find_overall_sl_exit_time when OverallTrailSL is configured.
    Requires OverallSL to be enabled.

    MTM example:
      overall_sl_val=10000 → initial threshold = -10000
      trail_for_every=3000, trail_by=1500
      P&L reaches  +3000 → threshold = -8500
      P&L reaches  +6000 → threshold = -7000
      P&L reaches  +9000 → threshold = -5500
      Eventually P&L reverses and hits -5500 → exit.

    PremiumPercentage mode:
      Both overall_sl_val and trail values are % of total entry premium.
    """
    if overall_sl_type == "None" or overall_sl_val <= 0:
        return None
    if trail_sl_type == "None" or trail_for_every <= 0 or trail_by <= 0:
        return None

    leg_states, total_entry_premium = _build_leg_scan_states(
        idx, day, entry_time, legs, expiries, step, lot_size, spot
    )
    if leg_states is None:
        return None

    # Convert to ₹ amounts for uniform comparison
    if overall_sl_type == "PremiumPercentage":
        sl_threshold    = -(total_entry_premium * overall_sl_val    / 100)
        trail_step      =   total_entry_premium * trail_for_every   / 100
        trail_step_size =   total_entry_premium * trail_by          / 100
    else:  # MTM
        sl_threshold    = -overall_sl_val
        trail_step      =  trail_for_every
        trail_step_size =  trail_by

    next_trail_trigger = trail_step  # first improvement fires at this P&L

    for t, total_mtm in _iter_total_mtm(idx, day, entry_time, exit_time, leg_states):
        # Improve the SL threshold as profit rises
        while total_mtm >= next_trail_trigger:
            sl_threshold       += trail_step_size   # less negative → toward profit
            next_trail_trigger += trail_step

        if total_mtm <= sl_threshold:
            return t

    return None
