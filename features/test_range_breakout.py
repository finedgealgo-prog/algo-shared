"""
test_range_breakout.py
────────────────────────
Unit tests for parse_leg_range_breakout() (per-leg BTST Range Breakout
config parsing) — pure function, no DB. Run with:

    python3 -m unittest shared/features/test_range_breakout.py -v
"""

import unittest

from range_breakout import parse_leg_range_breakout


class ParseLegRangeBreakoutTests(unittest.TestCase):
    def test_missing_config_defaults_to_none(self):
        rb_type, condition, start, end = parse_leg_range_breakout({})
        self.assertEqual(rb_type, "None")

    def test_btst_underlying(self):
        leg_cfg = {
            "LegRangeBreakout": {
                "Type": "RangeBreakoutType.BTSTUnderlying",
                "Condition": "RangeCondition.High",
                "StartTime": {"Hour": 14, "Minute": 30},
                "EndTime": {"Hour": 9, "Minute": 17},
            }
        }
        rb_type, condition, start, end = parse_leg_range_breakout(leg_cfg)
        self.assertEqual(rb_type, "BTSTUnderlying")
        self.assertEqual(condition, "High")
        self.assertEqual(start, "14:30")
        self.assertEqual(end, "09:17")

    def test_btst_instrument(self):
        leg_cfg = {
            "LegRangeBreakout": {
                "Type": "RangeBreakoutType.BTSTInstrument",
                "Condition": "RangeCondition.High",
                "StartTime": {"Hour": 14, "Minute": 30},
                "EndTime": {"Hour": 9, "Minute": 17},
            }
        }
        rb_type, condition, start, end = parse_leg_range_breakout(leg_cfg)
        self.assertEqual(rb_type, "BTSTInstrument")

    def test_same_day_underlying(self):
        leg_cfg = {
            "LegRangeBreakout": {
                "Type": "RangeBreakoutType.Underlying",
                "Condition": "RangeCondition.Low",
                "StartTime": {"Hour": 9, "Minute": 16},
                "EndTime": {"Hour": 9, "Minute": 30},
            }
        }
        rb_type, condition, start, end = parse_leg_range_breakout(leg_cfg)
        self.assertEqual(rb_type, "Underlying")
        self.assertEqual(condition, "Low")

    def test_same_day_instrument_default_type(self):
        # Type present but not "Underlying"/"BTST" -> falls through to Instrument
        leg_cfg = {"LegRangeBreakout": {"Type": "RangeBreakoutType.Instrument"}}
        rb_type, condition, start, end = parse_leg_range_breakout(leg_cfg)
        self.assertEqual(rb_type, "Instrument")

    def test_condition_defaults_to_high(self):
        leg_cfg = {"LegRangeBreakout": {"Type": "RangeBreakoutType.Underlying"}}
        _, condition, _, _ = parse_leg_range_breakout(leg_cfg)
        self.assertEqual(condition, "High")

    def test_no_dte_fields_leak_in(self):
        # Sanity: per-leg parser returns a 4-tuple, not the 6-tuple that
        # parse_range_breakout (strategy-level, Positional-capable) returns.
        result = parse_leg_range_breakout({})
        self.assertEqual(len(result), 4)


if __name__ == "__main__":
    unittest.main()
