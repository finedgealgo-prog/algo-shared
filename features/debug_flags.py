"""
Shared debug print toggle for backtest runtime logs.
Set DEBUG_PRINTS = True to enable verbose console output.
Set SHOW_ENTRY_LOGS = True to enable option chain / entry prints.
"""

import builtins as _builtins_df

DEBUG_PRINTS = True

# Controls: LIVE CHAIN, LIVE SELECT, STRIKE CALC, ENTRY MISS,
#           MOMENTUM PENDING, PENDING ENTRY, SNAPSHOT PRE-RESOLVE,
#           LIVE OPTION SUBSCRIBE, LIVE ENTRY SNAPSHOT, ENTRY KITE TOKEN
SHOW_ENTRY_LOGS = True

# Controls: BROKER TICK, BROKER GROUP, BROKER TRADE LOOP, MTM TOTAL,
#           KITE TICK STREAM, ENTRY CHECK, ENTRY MONITOR, ENTRY SKIP,
#           LiveEntryMonitor cache/subscribe prints
SHOW_RUNTIME_LOGS = True

# Suppressed for one tick cycle when all running trades are waiting for entry time.
# Set externally by the tick dispatcher; auto-reset after each tick.
suppress_runtime_logs = False

# Set True during a fast-forward tick; suppresses all prints except trade_event_print.
fast_forward_mode = False


def debug_print(*args, **kwargs):
    if DEBUG_PRINTS and not fast_forward_mode:
        _builtins_df.print(*args, **kwargs)


def entry_print(*args, **kwargs):
    if SHOW_ENTRY_LOGS and not fast_forward_mode:
        _builtins_df.print(*args, **kwargs)


def runtime_print(*args, **kwargs):
    if SHOW_RUNTIME_LOGS and not suppress_runtime_logs and not fast_forward_mode:
        _builtins_df.print(*args, **kwargs)


def trade_event_print(*args, **kwargs):
    """Always prints — used for actual trade entry / exit events."""
    _builtins_df.print(*args, **kwargs)
