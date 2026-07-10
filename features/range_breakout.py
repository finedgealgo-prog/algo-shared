"""
range_breakout.py
──────────────────
Range Breakout (ORB) and BTST ORB entry mechanisms.

Same-day ORB
────────────
  Range tracked between start_time and end_time on the same trading day.
  Breakout scan starts from end_time onward.

BTST ORB (Buy Today Sell Tomorrow Opening Range Breakout)
──────────────────────────────────────────────────────────
  Range is a single CONTINUOUS window spanning two trading days:
    • Day 1: range_start_time → end of Day 1 market
    • Day 2: market open → range_end_time
  Strike is selected at range_start_time on Day 1.
  Breakout scan starts from range_end_time on Day 2.

Supported types
───────────────
  Instrument     — same-day ORB, option price
  Underlying     — same-day ORB, spot price
  BTSTInstrument — cross-day ORB, option price
  BTSTUnderlying — cross-day ORB, spot price

Breakout conditions
───────────────────
  High — enter when watch-price crosses above range_high
  Low  — enter when watch-price crosses below range_low

⚠️  Range Breakout is mutually exclusive with Simple Momentum.
    When range breakout is configured, LegMomentum is ignored.

JSON config (strategy-level)
─────────────────────────────
    Same-day:
    "RangeBreakout": {
        "Type":      "RangeBreakoutType.Instrument",   // or Underlying
        "Condition": "RangeCondition.High",            // or Low
        "StartTime": {"Hour": 9,  "Minute": 16},
        "EndTime":   {"Hour": 9,  "Minute": 30}
    }

    BTST:
    "RangeBreakout": {
        "Type":      "RangeBreakoutType.BTSTUnderlying",  // or BTSTInstrument
        "Condition": "RangeCondition.High",
        "StartTime": {"Hour": 10, "Minute": 30},          // Day 1 start
        "EndTime":   {"Hour": 9,  "Minute": 30}           // Day 2 end
    }
"""

from __future__ import annotations

from typing import Optional, Tuple, List


# ═══════════════════════════════════════════════════════════════════
# 1. PARSING
# ═══════════════════════════════════════════════════════════════════

def parse_range_breakout(strategy: dict) -> Tuple[str, str, str, str, int, int]:
    """
    Returns (rb_type, condition, start_time, end_time, start_dte, end_dte).

      rb_type    : "Instrument" | "Underlying" |
                   "BTSTInstrument" | "BTSTUnderlying" |
                   "PositionalInstrument" | "PositionalUnderlying" | "None"
      condition  : "High" | "Low"
      start_time : "HH:MM"
      end_time   : "HH:MM"
      start_dte  : DTE of range-start day  (-1 for non-Positional types)
      end_dte    : DTE of range-end / trade day (-1 for non-Positional types)

    Positional JSON config:
      "RangeBreakout": {
          "Type":      "RangeBreakoutType.PositionalUnderlying",
          "Condition": "RangeCondition.High",
          "StartDTE":  1,
          "StartTime": {"Hour": 10, "Minute": 35},
          "EndDTE":    0,
          "EndTime":   {"Hour": 9,  "Minute": 35}
      }
    """
    cfg = strategy.get("RangeBreakout", {})
    t   = cfg.get("Type", "None")

    if t == "None":
        return "None", "High", "09:15", "09:30", -1, -1

    condition  = "Low" if "Low" in cfg.get("Condition", "High") else "High"

    st = cfg.get("StartTime", {})
    et = cfg.get("EndTime",   {})
    start_time = f"{int(st.get('Hour', 9)):02d}:{int(st.get('Minute', 15)):02d}"
    end_time   = f"{int(et.get('Hour', 9)):02d}:{int(et.get('Minute', 30)):02d}"

    if "PositionalUnderlying" in t:
        return ("PositionalUnderlying", condition, start_time, end_time,
                int(cfg.get("StartDTE", 1)), int(cfg.get("EndDTE", 0)))
    if "Positional" in t:
        return ("PositionalInstrument", condition, start_time, end_time,
                int(cfg.get("StartDTE", 1)), int(cfg.get("EndDTE", 0)))
    if "BTSTUnderlying" in t:
        return "BTSTUnderlying", condition, start_time, end_time, -1, -1
    if "BTST" in t:
        return "BTSTInstrument", condition, start_time, end_time, -1, -1
    if "Underlying" in t:
        return "Underlying", condition, start_time, end_time, -1, -1
    return "Instrument", condition, start_time, end_time, -1, -1


# ═══════════════════════════════════════════════════════════════════
# 2. RANGE COMPUTATION
# ═══════════════════════════════════════════════════════════════════

def compute_range(
    idx,
    day: str,
    start_time: str,
    end_time: str,
    rb_type: str,
    expiry: str     = "",
    strike: int     = 0,
    otype:  str     = "",
) -> Tuple[Optional[float], Optional[float]]:
    """
    Compute the High/Low of price during [start_time, end_time).

    For Instrument  → tracks option close prices
    For Underlying  → tracks spot close prices

    The window is [start_time, end_time) — end_time candle is NOT included
    (i.e. candles at 09:16, 09:17 … 09:29 for a 09:16→09:30 range).

    Returns (range_high, range_low) or (None, None) if no data exists.
    """
    times = sorted(
        t for t in idx._all_times.get(day, [])
        if start_time <= t < end_time
    )

    if not times:
        return None, None

    prices = []
    for t in times:
        if rb_type == "Underlying":
            px = idx.get_spot(day, t)
        else:
            px = idx.get_close(day, t, expiry, strike, otype)
        if px is not None:
            prices.append(px)

    if not prices:
        return None, None

    return max(prices), min(prices)


# ═══════════════════════════════════════════════════════════════════
# 3. BTST RANGE COMPUTATION  (cross-day)
# ═══════════════════════════════════════════════════════════════════

def compute_btst_range(
    prev_idx,           # DataIndex for Day 1 (loaded separately)
    curr_idx,           # DataIndex for Day 2 (current trading day)
    day1: str,          # "YYYY-MM-DD" — range start day
    day2: str,          # "YYYY-MM-DD" — range end / trade day
    start_time: str,    # range begins at this time on Day 1
    end_time: str,      # range ends  at this time on Day 2 (exclusive)
    rb_type: str,       # "BTSTInstrument" | "BTSTUnderlying"
    expiry: str     = "",
    strike: int     = 0,
    otype:  str     = "",
) -> Tuple[Optional[float], Optional[float]]:
    """
    Compute a single continuous price range spanning Day 1 and Day 2.

    Day 1 window: [start_time, end-of-day)  — from range_start to last candle
    Day 2 window: [market-open, end_time)   — from first candle to range_end (exclusive)

    For BTSTInstrument  → tracks option close prices
    For BTSTUnderlying  → tracks spot prices

    Returns (range_high, range_low) or (None, None) if no data in either window.
    """
    is_underlying = "Underlying" in rb_type
    prices: list  = []

    # ── Day 1: start_time → end of day ───────────────────────────────────────
    times_d1 = sorted(
        t for t in prev_idx._all_times.get(day1, [])
        if t >= start_time
    )
    for t in times_d1:
        px = (prev_idx.get_spot(day1, t) if is_underlying
              else prev_idx.get_close(day1, t, expiry, strike, otype))
        if px is not None:
            prices.append(px)

    # ── Day 2: market open → end_time (exclusive) ────────────────────────────
    times_d2 = sorted(
        t for t in curr_idx._all_times.get(day2, [])
        if t < end_time
    )
    for t in times_d2:
        px = (curr_idx.get_spot(day2, t) if is_underlying
              else curr_idx.get_close(day2, t, expiry, strike, otype))
        if px is not None:
            prices.append(px)

    if not prices:
        return None, None

    return max(prices), min(prices)


# ═══════════════════════════════════════════════════════════════════
# 4. POSITIONAL ORB  (multi-day DTE-based range)
# ═══════════════════════════════════════════════════════════════════

def compute_dte(day: str, expiry: str, trading_days: list) -> int:
    """
    Count trading days from `day` to `expiry` (exclusive `day`, inclusive `expiry`).

    DTE = 0  if day == expiry
    DTE = 1  if day is the immediately preceding trading day to expiry
    DTE = N  if N trading days exist strictly between day and expiry (inclusive expiry)
    """
    return sum(1 for d in trading_days if day < d <= expiry)


def find_day_by_dte(target_dte: int, expiry: str, trading_days: list) -> Optional[str]:
    """
    Find the trading day that has exactly `target_dte` DTE relative to `expiry`.

    Works by indexing the sorted list of days up to and including the expiry:
      DTE=0 → expiry day (last element)
      DTE=1 → day before expiry
      DTE=N → Nth day before expiry

    Returns None if target_dte is out of range (not enough trading days).
    """
    days_upto_expiry = sorted(d for d in trading_days if d <= expiry)
    if target_dte >= len(days_upto_expiry):
        return None
    return days_upto_expiry[-(target_dte + 1)]


def compute_positional_range(
    range_days_data: List[Tuple[str, object]],  # [(day_str, DataIndex), ...] sorted ascending
    start_time: str,         # range starts at this time on the FIRST day
    end_time: str,           # range ends at this time on the LAST day (exclusive)
    trade_day: str,          # the last day (= end_dte day)
    rb_type: str,            # "PositionalInstrument" | "PositionalUnderlying"
    expiry: str     = "",
    strike: int     = 0,
    otype:  str     = "",
) -> Tuple[Optional[float], Optional[float]]:
    """
    Compute a single continuous range spanning multiple trading days.

    Day windows:
      First day   : [start_time, end-of-day)  — from start_time to last candle
      Intermediate: [market-open, end-of-day) — full day (captures overnight gaps)
      Last day     : [market-open, end_time)  — from first candle to end_time (exclusive)

    For PositionalInstrument  → option close prices
    For PositionalUnderlying  → spot prices

    Returns (range_high, range_low) or (None, None) if no data.
    """
    if not range_days_data:
        return None, None

    is_underlying = "Underlying" in rb_type
    prices: list  = []
    first_day     = range_days_data[0][0]

    for rd, rd_idx in range_days_data:
        if rd == first_day:
            times = sorted(t for t in rd_idx._all_times.get(rd, []) if t >= start_time)
        elif rd == trade_day:
            times = sorted(t for t in rd_idx._all_times.get(rd, []) if t < end_time)
        else:
            times = sorted(rd_idx._all_times.get(rd, []))   # full intermediate day

        for t in times:
            px = (rd_idx.get_spot(rd, t) if is_underlying
                  else rd_idx.get_close(rd, t, expiry, strike, otype))
            if px is not None:
                prices.append(px)

    if not prices:
        return None, None

    return max(prices), min(prices)


# ═══════════════════════════════════════════════════════════════════
# 5. BREAKOUT ENTRY DETECTION
# ═══════════════════════════════════════════════════════════════════

def find_breakout_entry(
    idx,
    day: str,
    range_end_time: str,
    exit_time: str,
    expiry: str,
    strike: int,
    otype: str,
    rb_type: str,       # "Instrument" | "Underlying"
    condition: str,     # "High" | "Low"
    range_high: float,
    range_low:  float,
) -> Tuple[Optional[str], Optional[float]]:
    """
    Scan candles from range_end_time onward; return the first candle where
    the breakout condition is satisfied.

    Breakout check:
      High → watch-price crosses ABOVE range_high
      Low  → watch-price crosses BELOW range_low

    Watch price:
      Instrument  → option close price
      Underlying  → spot price

    Entry price returned is always the option close at the breakout candle
    (regardless of rb_type), since the trade entry is in the option.

    Returns (entry_time, entry_price) or (None, None) if no breakout.
    """
    times = sorted(
        t for t in idx._all_times.get(day, [])
        if range_end_time <= t <= exit_time
    )

    for t in times:
        # Price used for breakout check
        if rb_type == "Underlying":
            watch_px = idx.get_spot(day, t)
        else:
            watch_px = idx.get_close(day, t, expiry, strike, otype)

        if watch_px is None:
            continue

        breakout = (
            (condition == "High" and watch_px > range_high) or
            (condition == "Low"  and watch_px < range_low)
        )

        if not breakout:
            continue

        # Entry price = option close at breakout candle
        entry_price = idx.get_close(day, t, expiry, strike, otype)
        if entry_price is None:
            continue   # option data missing at this candle — keep scanning

        return t, entry_price

    return None, None
