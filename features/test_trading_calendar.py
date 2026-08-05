"""
test_trading_calendar.py
──────────────────────────
Unit tests for trading_calendar.py (next/previous trading day, holiday
handling) — pure functions, no DB. Used by leg_range_monitor.py (Day1->Day2
rollover) and algo.trade/api.py's DTE portfolio-activation gate. Run with:

    python3 -m unittest shared/features/test_trading_calendar.py -v
"""

import unittest

from trading_calendar import (
    is_trading_day,
    next_valid_trading_day,
    previous_valid_trading_day,
)


class TradingCalendarTests(unittest.TestCase):
    def test_next_trading_day_skips_weekend(self):
        # Friday 2026-08-07 -> next trading day should be Monday 2026-08-10
        self.assertEqual(next_valid_trading_day("2026-08-07", set()), "2026-08-10")

    def test_next_trading_day_skips_weekend_matching_screenshot_case(self):
        # Friday 2025-08-08 -> Monday 2025-08-11, matching a real AlgoTest
        # BTST backtest report row (entry Fri, exit Mon).
        self.assertEqual(next_valid_trading_day("2025-08-08", set()), "2025-08-11")

    def test_previous_trading_day_skips_weekend(self):
        self.assertEqual(previous_valid_trading_day("2026-08-10", set()), "2026-08-07")

    def test_next_trading_day_skips_holiday(self):
        holidays = {"2026-08-11"}  # Tuesday holiday
        self.assertEqual(next_valid_trading_day("2026-08-10", holidays), "2026-08-12")

    def test_is_trading_day(self):
        self.assertTrue(is_trading_day("2026-08-10", set()))  # Monday
        self.assertFalse(is_trading_day("2026-08-08", set()))  # Saturday
        self.assertFalse(is_trading_day("2026-08-10", {"2026-08-10"}))  # holiday

    def test_never_returns_same_day(self):
        self.assertNotEqual(next_valid_trading_day("2026-08-10", set()), "2026-08-10")
        self.assertNotEqual(previous_valid_trading_day("2026-08-10", set()), "2026-08-10")


if __name__ == "__main__":
    unittest.main()
