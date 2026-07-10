"""
lazy_leg.py
───────────
Lazy Leg: a new independent leg triggered via ReentryType.NextLeg.

IdleLegConfigs schema (flat registry inside strategy JSON):
{
    "lazy1": { <full leg config> },
    "lazy2": { <full leg config> },
    ...
}

A main leg or lazy leg triggers a lazy leg by setting:
    "LegReentrySL": {"Type": "ReentryType.NextLeg", "Value": {"NextLegRef": "lazy1"}}
    "LegReentryTP": {"Type": "ReentryType.NextLeg", "Value": {"NextLegRef": "lazy2"}}

Chaining: lazy1 can also trigger lazy2 the same way.
Max depth = 10.
"""

try:
    from .debug_flags import debug_print
except ImportError:
    from debug_flags import debug_print


MAX_DEPTH = 10


def _parse_leg_config(cfg: dict) -> dict:
    re_sl = cfg.get("LegReentrySL", {})
    re_tp = cfg.get("LegReentryTP", {})
    reentry_sl_type = re_sl.get("Type", "None")
    reentry_tp_type = re_tp.get("Type", "None")
    trail_sl = cfg.get("LegTrailSL", {})

    return {
        "id":                   cfg.get("id", "lazy"),
        "position":             "SELL" if "Sell" in cfg.get("PositionType", "") else "BUY",
        "otype":                "CE"   if "CE"   in cfg.get("InstrumentKind", "") else "PE",
        "expiry_kind":          cfg.get("ExpiryKind",      "ExpiryType.Weekly"),
        "entry_type":           cfg.get("EntryType",       "EntryType.EntryByStrikeType"),
        "strike_param":         cfg.get("StrikeParameter", "StrikeType.ATM"),
        "lots":                 int(cfg["LotConfig"]["Value"]),
        "sl_type":              cfg["LegStopLoss"]["Type"],
        "sl_val":               float(cfg["LegStopLoss"]["Value"]),
        "tgt_type":             cfg["LegTarget"]["Type"],
        "tgt_val":              float(cfg["LegTarget"]["Value"]),
        "momentum_type":        cfg.get("LegMomentum", {}).get("Type",  "None"),
        "momentum_val":         float(cfg.get("LegMomentum", {}).get("Value", 0)),
        "trail_type":           trail_sl.get("Type", "None"),
        "trail_x":              float(trail_sl.get("Value", {}).get("InstrumentMove", 0)),
        "trail_y":              float(trail_sl.get("Value", {}).get("StopLossMove",   0)),
        "reentry_sl_type":      reentry_sl_type,
        "reentry_tp_type":      reentry_tp_type,
        "reentry_sl_count":     int(re_sl.get("Value", {}).get("ReentryCount", 0) if isinstance(re_sl.get("Value"), dict) else 0)
                                if reentry_sl_type not in ("None", "ReentryType.NextLeg") else 0,
        "reentry_tp_count":     int(re_tp.get("Value", {}).get("ReentryCount", 0) if isinstance(re_tp.get("Value"), dict) else 0)
                                if reentry_tp_type not in ("None", "ReentryType.NextLeg") else 0,
        "reentry_sl_next_ref":  re_sl.get("Value", {}).get("NextLegRef") if isinstance(re_sl.get("Value"), dict) else None,
        "reentry_tp_next_ref":  re_tp.get("Value", {}).get("NextLegRef") if isinstance(re_tp.get("Value"), dict) else None,
    }


def process_lazy_legs(idx, day: str, exit_time: str, expiries: list,
                      lazy_leg_id: str, trigger_time: str,
                      idle_leg_configs: dict, lot_size: int, step: int,
                      depth: int = 0) -> list:
    """
    Process a lazy leg by ID and recursively handle further chaining.

    lazy_leg_id   : key in idle_leg_configs to process
    trigger_time  : exit_time of the leg that triggered this lazy leg
    idle_leg_configs : flat dict {id → leg_config}

    Returns list of lazy leg dicts to append to day_trade["legs"].
    """
    # Deferred imports to avoid circular dependency
    from .backtest_engine import (
        _process_leg, _pick_strike, _resolve_expiry,
        _add_one_minute, _find_momentum_entry,
    )

    tag = f"[LazyLeg depth={depth} id={lazy_leg_id} day={day}]"

    if depth >= MAX_DEPTH:
        debug_print(f"{tag} SKIP: max depth reached")
        return []

    lazy_raw = idle_leg_configs.get(lazy_leg_id)
    if not lazy_raw:
        debug_print(f"{tag} SKIP: id not found in IdleLegConfigs")
        return []

    # Already past strategy exit time
    if trigger_time >= exit_time:
        debug_print(f"{tag} SKIP: trigger_time={trigger_time} >= exit_time={exit_time}")
        return []

    c = _parse_leg_config(lazy_raw)

    expiry = _resolve_expiry(day, c["expiry_kind"], expiries)
    if expiry is None:
        debug_print(f"{tag} SKIP: expiry not resolved")
        return []

    spot = idx.get_spot(day, trigger_time)
    if spot is None:
        debug_print(f"{tag} SKIP: no spot at trigger_time={trigger_time}")
        return []

    strike = _pick_strike(
        idx, day, trigger_time, expiry,
        c["otype"], spot, c["entry_type"], c["strike_param"], step,
    )
    if strike is None:
        debug_print(f"{tag} SKIP: strike not resolved, spot={spot}")
        return []

    debug_print(f"{tag} trigger_time={trigger_time} expiry={expiry} strike={strike} spot={spot:.2f}")

    # ── Simple Momentum (if configured on lazy leg) ───────────────────────────
    actual_entry_time = trigger_time
    override_entry_px = None
    override_base_px  = None

    if c["momentum_type"] != "None" and c["momentum_val"] > 0:
        if "Underlying" in c["momentum_type"]:
            base_px = idx.get_spot(day, trigger_time)
        else:
            base_px = idx.get_close(day, trigger_time, expiry, strike, c["otype"])
        if base_px is None:
            debug_print(f"{tag} SKIP: no base_px for momentum at trigger_time={trigger_time}")
            return []

        if "Up" in c["momentum_type"]:
            target_px = base_px * (1 + c["momentum_val"] / 100) if "Percentage" in c["momentum_type"] else base_px + c["momentum_val"]
        else:
            target_px = base_px * (1 - c["momentum_val"] / 100) if "Percentage" in c["momentum_type"] else base_px - c["momentum_val"]
        debug_print(f"{tag} momentum={c['momentum_type']} val={c['momentum_val']}% base_px={base_px:.2f} target={target_px:.2f} scan={_add_one_minute(trigger_time)}→{exit_time}")

        mom_time, mom_px = _find_momentum_entry(
            idx, day, _add_one_minute(trigger_time), exit_time,
            expiry, strike, c["otype"],
            base_px, c["momentum_type"], c["momentum_val"],
        )
        if mom_time is None:
            debug_print(f"{tag} SKIP: momentum target {target_px:.2f} NOT reached between {_add_one_minute(trigger_time)} and {exit_time}")
            return []
        debug_print(f"{tag} momentum achieved at {mom_time} px={mom_px}")

        actual_entry_time = mom_time
        override_entry_px = mom_px
        override_base_px  = base_px
    else:
        if idx.get_close(day, trigger_time, expiry, strike, c["otype"]) is None:
            return []

    # ── Run the lazy leg ──────────────────────────────────────────────────────
    result = _process_leg(
        idx, day, actual_entry_time, exit_time,
        expiry, strike, c["otype"], c["position"],
        c["sl_type"], c["sl_val"], c["tgt_type"], c["tgt_val"],
        c["reentry_sl_count"], c["reentry_tp_count"],
        c["reentry_sl_type"],  c["reentry_tp_type"],
        c["lots"], lot_size,
        c["entry_type"], c["strike_param"], step,
        c["momentum_type"], c["momentum_val"],
        override_entry_px=override_entry_px,
        override_base_px=override_base_px,
        strategy_entry_time=trigger_time,
        trail_type=c["trail_type"], trail_x=c["trail_x"], trail_y=c["trail_y"],
        reentry_sl_next_ref=c["reentry_sl_next_ref"],
        reentry_tp_next_ref=c["reentry_tp_next_ref"],
    )

    lazy_leg_dict = {
        "id":            c["id"],
        "expiry":        expiry,
        "strike":        strike,
        "type":          c["otype"],
        "position":      c["position"],
        "entry_time":    result["entry_time"],
        "entry_price":   result["entry_price"],
        "exit_time":     result["exit_time"],
        "exit_price":    result["exit_price"],
        "exit_reason":   result["exit_reason"],
        "reentries":     result["reentries"],
        "lots":          c["lots"],
        "lot_size":      lot_size,
        "pnl":           result["total_leg_pnl"],
        "sub_trades":    result["sub_trades"],
        "is_lazy_leg":   True,
        "lazy_depth":    depth + 1,
    }

    results = [lazy_leg_dict]

    # ── Chaining: lazy leg triggered another lazy leg via NextLeg ─────────────
    if result.get("next_leg_ref") and result.get("next_leg_trigger_time"):
        further = process_lazy_legs(
            idx, day, exit_time, expiries,
            result["next_leg_ref"],
            result["next_leg_trigger_time"],
            idle_leg_configs, lot_size, step,
            depth=depth + 1,
        )
        results.extend(further)

    return results
