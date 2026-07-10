"""
Leg Builder — Strike Selection & Leg Config
Builds one entry for ListOfLegConfigs / IdleLegConfigs.

Disabled value reference (from sample_backtest_request.json):
  LegStopLoss  disabled → {"Type": "None", "Value": 0}
  LegTarget    disabled → {"Type": "None", "Value": 0}
  LegMomentum  disabled → {"Type": "None", "Value": 0}
  LegTrailSL   disabled → {"Type": "None", "Value": {}}
  LegReentrySL disabled → {"Type": "None", "Value": {}}
  LegReentryTP disabled → {"Type": "None", "Value": {}}
"""

import json
import random
import string
from enum import Enum


# ─── Enums ────────────────────────────────────────────────────────────────────

class PositionType(str, Enum):
    BUY  = "PositionType.Buy"
    SELL = "PositionType.Sell"


class InstrumentKind(str, Enum):
    CE      = "LegType.CE"
    PE      = "LegType.PE"
    FUTURES = "LegType.Futures"


class ExpiryKind(str, Enum):
    WEEKLY       = "ExpiryType.Weekly"
    NEXT_WEEKLY  = "ExpiryType.NextWeekly"
    MONTHLY      = "ExpiryType.Monthly"
    NEXT_MONTHLY = "ExpiryType.NextMonthly"


class StrikeType(str, Enum):
    ITM20 = "StrikeType.ITM20"; ITM19 = "StrikeType.ITM19"; ITM18 = "StrikeType.ITM18"
    ITM17 = "StrikeType.ITM17"; ITM16 = "StrikeType.ITM16"; ITM15 = "StrikeType.ITM15"
    ITM14 = "StrikeType.ITM14"; ITM13 = "StrikeType.ITM13"; ITM12 = "StrikeType.ITM12"
    ITM11 = "StrikeType.ITM11"; ITM10 = "StrikeType.ITM10"; ITM9  = "StrikeType.ITM9"
    ITM8  = "StrikeType.ITM8";  ITM7  = "StrikeType.ITM7";  ITM6  = "StrikeType.ITM6"
    ITM5  = "StrikeType.ITM5";  ITM4  = "StrikeType.ITM4";  ITM3  = "StrikeType.ITM3"
    ITM2  = "StrikeType.ITM2";  ITM1  = "StrikeType.ITM1";  ATM   = "StrikeType.ATM"
    OTM1  = "StrikeType.OTM1";  OTM2  = "StrikeType.OTM2";  OTM3  = "StrikeType.OTM3"
    OTM4  = "StrikeType.OTM4";  OTM5  = "StrikeType.OTM5";  OTM6  = "StrikeType.OTM6"
    OTM7  = "StrikeType.OTM7";  OTM8  = "StrikeType.OTM8";  OTM9  = "StrikeType.OTM9"
    OTM10 = "StrikeType.OTM10"; OTM11 = "StrikeType.OTM11"; OTM12 = "StrikeType.OTM12"
    OTM13 = "StrikeType.OTM13"; OTM14 = "StrikeType.OTM14"; OTM15 = "StrikeType.OTM15"
    OTM16 = "StrikeType.OTM16"; OTM17 = "StrikeType.OTM17"; OTM18 = "StrikeType.OTM18"
    OTM19 = "StrikeType.OTM19"; OTM20 = "StrikeType.OTM20"; OTM21 = "StrikeType.OTM21"
    OTM22 = "StrikeType.OTM22"; OTM23 = "StrikeType.OTM23"; OTM24 = "StrikeType.OTM24"
    OTM25 = "StrikeType.OTM25"; OTM26 = "StrikeType.OTM26"; OTM27 = "StrikeType.OTM27"
    OTM28 = "StrikeType.OTM28"; OTM29 = "StrikeType.OTM29"; OTM30 = "StrikeType.OTM30"


class EntryType(str, Enum):
    STRIKE_TYPE              = "EntryType.EntryByStrikeType"
    PREMIUM_RANGE            = "EntryType.EntryByPremiumRange"
    CLOSEST_PREMIUM          = "EntryType.EntryByPremium"
    PREMIUM_GEQ              = "EntryType.EntryByPremiumGEQ"
    PREMIUM_LEQ              = "EntryType.EntryByPremiumLEQ"
    STRADDLE_WIDTH           = "EntryType.EntryByStraddlePrice"
    PCT_OF_ATM               = "EntryType.EntryByAtmMultiplier"
    SYNTHETIC_FUTURE         = "EntryType.EntryBySyntheticFuture"
    ATM_STRADDLE_PREMIUM_PCT = "EntryType.EntryByPremiumCloseToStraddle"
    CLOSEST_DELTA            = "EntryType.EntryByDelta"
    DELTA_RANGE              = "EntryType.EntryByDeltaRange"


class LotType(str, Enum):
    QUANTITY = "LotType.Quantity"


class LegTgtSLType(str, Enum):
    POINTS            = "LegTgtSLType.Points"
    UNDERLYING_POINTS = "LegTgtSLType.UnderlyingPoints"
    PERCENTAGE        = "LegTgtSLType.Percentage"
    UNDERLYING_PCT    = "LegTgtSLType.UnderlyingPercentage"


class TrailSLType(str, Enum):
    POINTS     = "TrailStopLossType.Points"
    PERCENTAGE = "TrailStopLossType.Percentage"


class MomentumType(str, Enum):
    POINTS_UP       = "MomentumType.PointsUp"
    POINTS_DOWN     = "MomentumType.PointsDown"
    PERCENTAGE_UP   = "MomentumType.PercentageUp"
    PERCENTAGE_DOWN = "MomentumType.PercentageDown"


class ReentryType(str, Enum):
    IMMEDIATE         = "ReentryType.Immediate"
    IMMEDIATE_REVERSE = "ReentryType.ImmediateReverse"
    LIKE_ORIGINAL     = "ReentryType.LikeOriginal"
    LIKE_ORIGINAL_REV = "ReentryType.LikeOriginalReverse"
    AT_COST           = "ReentryType.AtCost"
    AT_COST_REVERSE   = "ReentryType.AtCostReverse"
    NEXT_LEG          = "ReentryType.NextLeg"


class AdjustmentType(str, Enum):
    PLUS  = "AdjustmentType.Plus"
    MINUS = "AdjustmentType.Minus"


# ─── Disabled defaults (exact values from sample_backtest_request.json) ───────

DISABLED     = {"Type": "None", "Value": 0}   # StopLoss / Target / Momentum
DISABLED_OBJ = {"Type": "None", "Value": {}}  # TrailSL / ReentrySL / ReentryTP


# ─── Strike Parameter Builders ────────────────────────────────────────────────

def strike_by_type(strike: StrikeType = StrikeType.ATM):
    """→ "StrikeType.ATM" """
    return strike.value


def strike_by_premium_range(lower: float, upper: float):
    """→ {"LowerRange": 50, "UpperRange": 200}"""
    return {"LowerRange": lower, "UpperRange": upper}


def strike_by_closest_premium(premium: float):
    """→ 50"""
    return premium


def strike_by_premium_geq(premium: float):
    """→ 60  (premium >= 60)"""
    return premium


def strike_by_premium_leq(premium: float):
    """→ 40  (premium <= 40)"""
    return premium


def strike_by_straddle_width(
    multiplier: float = 0.5,
    adjustment: AdjustmentType = AdjustmentType.PLUS,
    strike_kind: StrikeType = StrikeType.ATM,
):
    """→ {"Multiplier": 0.4, "Adjustment": "AdjustmentType.Plus", "StrikeKind": "StrikeType.ATM"}"""
    return {
        "Multiplier": multiplier,
        "Adjustment": adjustment.value,
        "StrikeKind": strike_kind.value,
    }


def strike_by_pct_of_atm(pct: float, plus: bool = True):
    """→ 1.008 for +0.8%,  0.993 for -0.7%"""
    return 1 + (pct / 100) if plus else 1 - (pct / 100)


def strike_by_synthetic_future(strike: StrikeType = StrikeType.ATM):
    """→ "StrikeType.ITM18" """
    return strike.value


def strike_by_atm_straddle_premium_pct(pct: float, strike_kind: StrikeType = StrikeType.ATM):
    """→ {"Multiplier": 0.66, "StrikeKind": "StrikeType.ATM"}"""
    return {"Multiplier": pct / 100, "StrikeKind": strike_kind.value}


def strike_by_closest_delta(delta: float):
    """→ 36"""
    return delta


def strike_by_delta_range(lower: float, upper: float):
    """→ {"LowerRange": 30, "UpperRange": 70}"""
    return {"LowerRange": lower, "UpperRange": upper}


# ─── Sub-config Builders ──────────────────────────────────────────────────────

def lot_config(quantity: int = 1) -> dict:
    return {"Type": LotType.QUANTITY.value, "Value": quantity}


def stop_loss(sl_type: LegTgtSLType, value: float) -> dict:
    """e.g. stop_loss(LegTgtSLType.PERCENTAGE, 25)"""
    return {"Type": sl_type.value, "Value": value}


def target_profit(tp_type: LegTgtSLType, value: float) -> dict:
    """e.g. target_profit(LegTgtSLType.UNDERLYING_POINTS, 40)"""
    return {"Type": tp_type.value, "Value": value}


def trail_sl(trail_type: TrailSLType, instrument_move: float, sl_move: float) -> dict:
    """
    e.g. trail_sl(TrailSLType.PERCENTAGE, 4, 1)
    → {"Type": "TrailStopLossType.Percentage", "Value": {"InstrumentMove": 4, "StopLossMove": 1}}
    """
    return {
        "Type": trail_type.value,
        "Value": {"InstrumentMove": instrument_move, "StopLossMove": sl_move},
    }


def momentum(m_type: MomentumType, value: float) -> dict:
    """
    e.g. momentum(MomentumType.PERCENTAGE_DOWN, 8)
    → {"Type": "MomentumType.PercentageDown", "Value": 8}
    """
    return {"Type": m_type.value, "Value": value}


def reentry(re_type: ReentryType, count: int = 1, next_leg_ref: str = "") -> dict:
    """
    e.g.
      reentry(ReentryType.IMMEDIATE, count=2)
        → {"Type": "ReentryType.Immediate", "Value": {"ReentryCount": 2}}

      reentry(ReentryType.NEXT_LEG, next_leg_ref="lazy1")
        → {"Type": "ReentryType.NextLeg", "Value": {"NextLegRef": "lazy1"}}
    """
    if re_type == ReentryType.NEXT_LEG:
        return {"Type": re_type.value, "Value": {"NextLegRef": next_leg_ref}}
    return {"Type": re_type.value, "Value": {"ReentryCount": count}}


# ─── Leg Builder ──────────────────────────────────────────────────────────────

def _random_id(n: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def build_leg(
    position:         PositionType   = PositionType.SELL,
    instrument:       InstrumentKind = InstrumentKind.CE,
    expiry:           ExpiryKind     = ExpiryKind.WEEKLY,
    lots:             int            = 1,
    entry_type:       EntryType      = EntryType.STRIKE_TYPE,
    strike_parameter                 = None,
    leg_stoploss:     dict           = None,
    leg_target:       dict           = None,
    leg_trail_sl:     dict           = None,
    leg_momentum:     dict           = None,
    leg_reentry_sl:   dict           = None,
    leg_reentry_tp:   dict           = None,
    leg_id:           str            = None,
) -> dict:
    """
    Build one leg config dict for ListOfLegConfigs or IdleLegConfigs.

    Disabled defaults (from sample_backtest_request.json):
      leg_stoploss / leg_target / leg_momentum  → {"Type": "None", "Value": 0}
      leg_trail_sl / leg_reentry_sl / leg_reentry_tp → {"Type": "None", "Value": {}}
    """
    if strike_parameter is None:
        strike_parameter = strike_by_type(StrikeType.ATM)

    return {
        "id":              leg_id or _random_id(),
        "PositionType":    position.value,
        "LotConfig":       lot_config(lots),
        "LegStopLoss":     leg_stoploss   or DISABLED,
        "LegTarget":       leg_target     or DISABLED,
        "LegTrailSL":      leg_trail_sl   or DISABLED_OBJ,
        "LegMomentum":     leg_momentum   or DISABLED,
        "ExpiryKind":      expiry.value,
        "EntryType":       entry_type.value,
        "StrikeParameter": strike_parameter,
        "InstrumentKind":  instrument.value,
        "LegReentrySL":    leg_reentry_sl or DISABLED_OBJ,
        "LegReentryTP":    leg_reentry_tp or DISABLED_OBJ,
    }


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Matches leg 1 from sample_backtest_request.json exactly
    leg = build_leg(
        position=PositionType.SELL,
        instrument=InstrumentKind.CE,
        expiry=ExpiryKind.WEEKLY,
        lots=1,
        entry_type=EntryType.PREMIUM_RANGE,
        strike_parameter=strike_by_premium_range(50, 200),
        leg_stoploss=stop_loss(LegTgtSLType.PERCENTAGE, 25),
        leg_reentry_sl=reentry(ReentryType.IMMEDIATE, count=2),
        leg_reentry_tp=reentry(ReentryType.NEXT_LEG, next_leg_ref="lazy1"),
    )
    print(json.dumps(leg, indent=2))
