"""
Entry & Exit Timing
Builds EntryIndicators and ExitIndicators for the AlgoTest request.

Actual request shape:
  "EntryIndicators": {
    "Type": "IndicatorTreeNodeType.OperandNode",
    "OperandType": "OperandType.And",
    "Value": [
      {
        "Type": "IndicatorTreeNodeType.DataNode",
        "Value": {
          "IndicatorName": "IndicatorType.TimeIndicator",
          "Parameters": { "Hour": 9, "Minute": 35 }
        }
      }
    ]
  }

Valid range:
  Entry : 09:16 – 15:28
  Exit  : 09:17 – 15:29
"""

import json
from datetime import time
from typing import Tuple


# ─── Validation ───────────────────────────────────────────────────────────────

ENTRY_MIN = time(9, 16)
ENTRY_MAX = time(15, 28)
EXIT_MIN  = time(9, 17)
EXIT_MAX  = time(15, 29)


def _validate(hour: int, minute: int, lo: time, hi: time, label: str):
    t = time(hour, minute)
    if not (lo <= t <= hi):
        raise ValueError(
            f"{label} {hour:02d}:{minute:02d} out of range "
            f"({lo.strftime('%H:%M')} – {hi.strftime('%H:%M')})"
        )


# ─── Core Builders ────────────────────────────────────────────────────────────

def _indicator_tree(hour: int, minute: int) -> dict:
    return {
        "Type": "IndicatorTreeNodeType.OperandNode",
        "OperandType": "OperandType.And",
        "Value": [
            {
                "Type": "IndicatorTreeNodeType.DataNode",
                "Value": {
                    "IndicatorName": "IndicatorType.TimeIndicator",
                    "Parameters": {"Hour": hour, "Minute": minute},
                },
            }
        ],
    }


def entry_time(hour: int, minute: int) -> dict:
    """
    Build EntryIndicators dict.
    e.g. entry_time(9, 35) → enter at 09:35
    """
    _validate(hour, minute, ENTRY_MIN, ENTRY_MAX, "Entry")
    return _indicator_tree(hour, minute)


def exit_time(hour: int, minute: int) -> dict:
    """
    Build ExitIndicators dict.
    e.g. exit_time(15, 15) → exit at 15:15
    """
    _validate(hour, minute, EXIT_MIN, EXIT_MAX, "Exit")
    return _indicator_tree(hour, minute)


# ─── Session Helper ───────────────────────────────────────────────────────────

class IntradaySession:
    """
    Pairs entry + exit time and exposes them as strategy fragment.

    Usage
    -----
    session = IntradaySession(9, 35, 15, 15)
    # or via helper:
    session = intraday_session((9, 35), (15, 15))

    strategy = {
        "Ticker": "NIFTY",
        **session.as_strategy_fragment,
        ...
    }
    """

    def __init__(self, entry_hour: int, entry_minute: int, exit_hour: int, exit_minute: int):
        _validate(entry_hour, entry_minute, ENTRY_MIN, ENTRY_MAX, "Entry")
        _validate(exit_hour,  exit_minute,  EXIT_MIN,  EXIT_MAX,  "Exit")

        if time(exit_hour, exit_minute) <= time(entry_hour, entry_minute):
            raise ValueError(
                f"Exit {exit_hour:02d}:{exit_minute:02d} must be after "
                f"entry {entry_hour:02d}:{entry_minute:02d}"
            )

        self.entry_hour   = entry_hour
        self.entry_minute = entry_minute
        self.exit_hour    = exit_hour
        self.exit_minute  = exit_minute

    @property
    def entry_indicators(self) -> dict:
        return entry_time(self.entry_hour, self.entry_minute)

    @property
    def exit_indicators(self) -> dict:
        return exit_time(self.exit_hour, self.exit_minute)

    @property
    def as_strategy_fragment(self) -> dict:
        """Returns {"EntryIndicators": ..., "ExitIndicators": ...}"""
        return {
            "EntryIndicators": self.entry_indicators,
            "ExitIndicators":  self.exit_indicators,
        }

    def __repr__(self):
        return (
            f"IntradaySession("
            f"entry={self.entry_hour:02d}:{self.entry_minute:02d}, "
            f"exit={self.exit_hour:02d}:{self.exit_minute:02d})"
        )


def intraday_session(entry: Tuple[int, int], exit_: Tuple[int, int]) -> IntradaySession:
    """
    Shorthand to create an IntradaySession.

    intraday_session(entry=(9, 35), exit_=(15, 15))
    """
    return IntradaySession(entry[0], entry[1], exit_[0], exit_[1])


# ─── Common Presets ───────────────────────────────────────────────────────────

class Presets:
    STANDARD      = intraday_session((9, 35),  (15, 15))   # default
    OPEN_TO_CLOSE = intraday_session((9, 20),  (15, 15))
    MID_SESSION   = intraday_session((10, 0),  (14, 30))
    MORNING_SCALP = intraday_session((9, 30),  (11, 0))
    AFTERNOON     = intraday_session((13, 0),  (15, 15))


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    session = intraday_session(entry=(9, 35), exit_=(15, 15))
    print(json.dumps(session.as_strategy_fragment, indent=2))
