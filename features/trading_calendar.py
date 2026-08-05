"""
trading_calendar.py
────────────────────
Shared NSE trading-day calendar helpers.

Holiday data already exists (MongoData.get_holidays() → market_holidays
collection), but "next/previous trading day" logic was previously duplicated
inline in execution_socket.py (_latest_trading_day, _previous_session_close)
and re-derived per backtest run in backtest_engine.py (_get_trading_days).
This module is the one shared place for that walk, used by BTST Range
Breakout's live session-rollover logic (Day 1 → Day 2 must be the next
*trading* session, not next calendar date).

Existing duplicated call sites are left as-is (working code, out of scope to
refactor here) — new call sites should use these functions.
"""

from __future__ import annotations

from datetime import date as _date, timedelta as _timedelta

# Comfortably covers any realistic holiday run (longest NSE gap is a few
# consecutive days around Diwali/year-end) while still bounding the loop.
_MAX_WALK_DAYS = 15


def is_trading_day(day: str, holidays: set) -> bool:
    """`day`: 'YYYY-MM-DD'. True if it's a weekday and not in `holidays`."""
    d = _date.fromisoformat(day[:10])
    return d.weekday() < 5 and day[:10] not in holidays


def next_valid_trading_day(current_date: str, holidays: set) -> str:
    """
    First trading day strictly AFTER `current_date`, skipping weekends and
    `holidays`. Never returns `current_date` itself.
    """
    cursor = _date.fromisoformat(current_date[:10]) + _timedelta(days=1)
    for _ in range(_MAX_WALK_DAYS):
        iso = cursor.isoformat()
        if cursor.weekday() < 5 and iso not in holidays:
            return iso
        cursor += _timedelta(days=1)
    return cursor.isoformat()


def previous_valid_trading_day(current_date: str, holidays: set) -> str:
    """
    First trading day strictly BEFORE `current_date`, skipping weekends and
    `holidays`. Never returns `current_date` itself.
    """
    cursor = _date.fromisoformat(current_date[:10]) - _timedelta(days=1)
    for _ in range(_MAX_WALK_DAYS):
        iso = cursor.isoformat()
        if cursor.weekday() < 5 and iso not in holidays:
            return iso
        cursor -= _timedelta(days=1)
    return cursor.isoformat()
