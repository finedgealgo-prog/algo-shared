"""
monitoring_throttle.py
───────────────────────
Per-trade "how often do we re-evaluate SL/Target/entry conditions" gate,
driven by each trade's own execution_config_base.MonitoringMode
("ltp" | "mid_candle" | "candle_close") — replaces the old one-size-fits-
all-trades-in-a-mode throttle (e.g. FORWARD_TEST_CHECK_INTERVAL_SECONDS in
live_tick_dispatcher.py) with a per-trade interval so "ltp" and
"candle_close"/"mid_candle" trades can coexist in the same activation_mode.

Also tracks a separate "order placement is currently blocked" override per
trade: an order (entry, re-entry, or exit — any order_type, not just MPP)
that fails to place leaves nothing persisted, so the SAME condition is still
true on the next evaluation — but that next evaluation is normally gated by
the trade's configured MonitoringMode, which would mean a "Mid Candle"/
"on Candle Close" trade only retries a *failed* order every 30s/60s. A
failed placement isn't a "check less often" situation, it's "something
needs fixing (broker depth, connectivity) and should be retried soon" —
so mark_order_blocked()/mark_order_resolved() (called from
live_order_manager's entry/exit dispatch) override should_evaluate() to a
fast BLOCKED_RETRY_INTERVAL_SECONDS cadence until the order actually goes
through, independent of MonitoringMode.

In-memory only (module-level dicts, keyed by trade_id) — resets on process
restart, same as the fixed-interval throttle it replaces.
"""

from __future__ import annotations

import time

# Seconds between re-evaluations for each monitoring mode. "ltp" (0) means
# no throttle — evaluate on every tick, matching today's live/fast-forward
# default behaviour.
MONITORING_INTERVAL_SECONDS: dict[str, float] = {
    "ltp": 0.0,
    "mid_candle": 30.0,
    "candle_close": 60.0,
}

# Retry cadence while a trade has an order that failed to place (see module
# docstring) — deliberately independent of MONITORING_INTERVAL_SECONDS.
BLOCKED_RETRY_INTERVAL_SECONDS = 3.0

_last_evaluated_at: dict[str, float] = {}
_blocked_trade_ids: set[str] = set()


def mark_order_blocked(trade_id: str) -> None:
    """Call when an order placement attempt (entry/re-entry/exit, any
    order_type) fails for this trade — forces should_evaluate() onto the
    fast BLOCKED_RETRY_INTERVAL_SECONDS cadence, overriding whatever
    MonitoringMode this trade is configured with, until the next
    successful placement calls mark_order_resolved()."""
    if trade_id:
        _blocked_trade_ids.add(trade_id)


def mark_order_resolved(trade_id: str) -> None:
    """Call once an order for this trade places successfully — restores
    the trade's normal configured MonitoringMode cadence."""
    _blocked_trade_ids.discard(trade_id)


def should_evaluate(trade_id: str, monitoring_mode: str | None, *, default_mode: str = "ltp") -> bool:
    """
    True if `trade_id` is due for SL/Target/entry re-evaluation this tick.

    `monitoring_mode` is whatever's stored on the trade's
    execution_config_base.MonitoringMode (may be missing/unrecognized on
    older trades — falls back to `default_mode`, which the caller should
    pick to match today's real per-activation_mode behaviour: "ltp" for
    live/fast-forward, "mid_candle" for forward-test's existing 30s
    heartbeat). Ignored entirely while this trade has a blocked order —
    see mark_order_blocked().

    Has a side effect: on a True return, records `trade_id`'s last-
    evaluated timestamp so the next call can measure elapsed time.
    """
    mode = monitoring_mode if monitoring_mode in MONITORING_INTERVAL_SECONDS else default_mode
    interval = MONITORING_INTERVAL_SECONDS.get(mode, 0.0)
    if trade_id in _blocked_trade_ids:
        # Speed up to the fast retry cadence, but never SLOW DOWN a trade that's
        # already faster than that (e.g. "ltp" is already every-tick — blocking
        # it must not throttle it down to 3s).
        interval = min(interval, BLOCKED_RETRY_INTERVAL_SECONDS) if interval > 0 else 0.0
    if interval <= 0:
        return True
    now = time.monotonic()
    last = _last_evaluated_at.get(trade_id, 0.0)
    if now - last < interval:
        return False
    _last_evaluated_at[trade_id] = now
    return True
