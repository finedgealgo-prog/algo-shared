"""
Re-Entry & Lazy Leg Feature
Builds reentry configs and manages IdleLegConfigs (lazy legs).

Re-entry types (from sample_backtest_request.json):
  RE-ASAP          → ReentryType.Immediate
  RE-ASAP Reverse  → ReentryType.ImmediateReverse
  RE-COST          → ReentryType.AtCost
  RE-COST Reverse  → ReentryType.AtCostReverse
  RE-MOMENTUM      → ReentryType.LikeOriginal
  RE-MOMENTUM Rev  → ReentryType.LikeOriginalReverse
  Lazy Leg         → ReentryType.NextLeg  (points to IdleLegConfigs key)

Lazy Leg (IdleLegConfigs) JSON shape:
  "IdleLegConfigs": {
    "lazy1": {
      "id": "lazy1",
      ...full leg config...
      "LegReentrySL": {"Type": "ReentryType.NextLeg", "Value": {"NextLegRef": "lazy2"}},
      "LegReentryTP": {"Type": "ReentryType.LikeOriginal", "Value": {"ReentryCount": 1}}
    },
    "lazy2": { ...another leg... }
  }

Chaining: lazy1 → lazy2 → ... (up to 10 levels)
"""

import json
try:
    from .leg_builder import (
        build_leg, ReentryType,
        PositionType, InstrumentKind, ExpiryKind, EntryType, StrikeType,
        LegTgtSLType, TrailSLType, MomentumType,
        stop_loss, target_profit, trail_sl, momentum,
        strike_by_type, strike_by_premium_geq, strike_by_premium_leq,
        strike_by_closest_premium, strike_by_premium_range,
    )
except ImportError:
    from leg_builder import (
        build_leg, ReentryType,
        PositionType, InstrumentKind, ExpiryKind, EntryType, StrikeType,
        LegTgtSLType, TrailSLType, MomentumType,
        stop_loss, target_profit, trail_sl, momentum,
        strike_by_type, strike_by_premium_geq, strike_by_premium_leq,
        strike_by_closest_premium, strike_by_premium_range,
    )


# ─── Re-entry Builders ────────────────────────────────────────────────────────

def re_asap(count: int = 1) -> dict:
    """
    RE-ASAP: Re-enter immediately at new ATM after SL/Target hit.
    → {"Type": "ReentryType.Immediate", "Value": {"ReentryCount": 2}}
    """
    return {"Type": ReentryType.IMMEDIATE.value, "Value": {"ReentryCount": count}}


def re_asap_reverse(count: int = 1) -> dict:
    """RE-ASAP but reverses the position direction."""
    return {"Type": ReentryType.IMMEDIATE_REVERSE.value, "Value": {"ReentryCount": count}}


def re_cost(count: int = 1) -> dict:
    """
    RE-COST: Re-enter original strike when price returns to entry price.
    → {"Type": "ReentryType.AtCost", "Value": {"ReentryCount": 1}}
    """
    return {"Type": ReentryType.AT_COST.value, "Value": {"ReentryCount": count}}


def re_cost_reverse(count: int = 1) -> dict:
    """RE-COST but reverses position direction."""
    return {"Type": ReentryType.AT_COST_REVERSE.value, "Value": {"ReentryCount": count}}


def re_momentum(count: int = 1) -> dict:
    """
    RE-MOMENTUM: Re-enter when momentum condition is met after SL/Target.
    Requires Simple Momentum to be enabled on the leg.
    → {"Type": "ReentryType.LikeOriginal", "Value": {"ReentryCount": 1}}
    """
    return {"Type": ReentryType.LIKE_ORIGINAL.value, "Value": {"ReentryCount": count}}


def re_momentum_reverse(count: int = 1) -> dict:
    """RE-MOMENTUM but reverses position direction."""
    return {"Type": ReentryType.LIKE_ORIGINAL_REV.value, "Value": {"ReentryCount": count}}


def re_lazy_leg(lazy_leg_name: str) -> dict:
    """
    Lazy Leg re-entry: activate a lazy leg when SL/Target hits.
    → {"Type": "ReentryType.NextLeg", "Value": {"NextLegRef": "lazy1"}}

    Parameters
    ----------
    lazy_leg_name : key in IdleLegConfigs  (e.g. "lazy1")
    """
    return {"Type": ReentryType.NEXT_LEG.value, "Value": {"NextLegRef": lazy_leg_name}}


# ─── Lazy Leg Registry ────────────────────────────────────────────────────────

class LazyLegRegistry:
    """
    Manages IdleLegConfigs — a dict of named lazy legs.

    Usage
    -----
    registry = LazyLegRegistry()

    registry.add("lazy1", build_leg(
        position=PositionType.SELL,
        instrument=InstrumentKind.CE,
        entry_type=EntryType.PREMIUM_GEQ,
        strike_parameter=strike_by_premium_geq(60),
        leg_stoploss=stop_loss(LegTgtSLType.POINTS, 60),
        leg_target=target_profit(LegTgtSLType.POINTS, 20),
        leg_momentum=momentum(MomentumType.PERCENTAGE_DOWN, 8),
        leg_reentry_sl=re_lazy_leg("lazy2"),        # chains to lazy2
        leg_reentry_tp=re_momentum(count=1),
    ))

    registry.add("lazy2", build_leg(...))

    idle_configs = registry.build()  # → IdleLegConfigs dict
    """

    MAX_LAZY_LEGS = 10

    def __init__(self):
        self._legs: dict[str, dict] = {}
        self._order: list[str] = []

    def add(self, name: str, leg_config: dict) -> "LazyLegRegistry":
        """
        Add a lazy leg.

        Parameters
        ----------
        name       : unique key (e.g. "lazy1"). Used as id and IdleLegConfigs key.
        leg_config : output of build_leg(). The id will be overwritten with name.
        """
        if len(self._legs) >= self.MAX_LAZY_LEGS:
            raise ValueError(f"Maximum {self.MAX_LAZY_LEGS} lazy legs allowed.")
        if name in self._legs:
            raise ValueError(f"Lazy leg '{name}' already exists.")

        # Force id = name (required by API)
        leg = dict(leg_config)
        leg["id"] = name
        self._legs[name] = leg
        self._order.append(name)
        return self

    def remove(self, name: str) -> "LazyLegRegistry":
        if name not in self._legs:
            raise KeyError(f"Lazy leg '{name}' not found.")
        self._legs.pop(name)
        self._order.remove(name)
        return self

    def build(self) -> dict:
        """Returns the IdleLegConfigs dict in insertion order."""
        return {name: self._legs[name] for name in self._order}

    def names(self) -> list:
        return list(self._order)

    def __len__(self):
        return len(self._legs)

    def __repr__(self):
        return f"LazyLegRegistry({self._order})"


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("Re-Entry & Lazy Leg Demo")
    print("=" * 65)

    # ── Re-entry types ────────────────────────────────────────────
    print("\n1. Re-entry builder outputs")
    print("-" * 45)
    print("RE-ASAP (2x):        ", re_asap(2))
    print("RE-ASAP Reverse (1x):", re_asap_reverse(1))
    print("RE-COST (1x):        ", re_cost(1))
    print("RE-MOMENTUM (1x):    ", re_momentum(1))
    print("Lazy Leg → lazy1:    ", re_lazy_leg("lazy1"))

    # ── Lazy Leg Registry — matches sample_backtest_request.json ──
    print("\n\n2. LazyLegRegistry (matching sample_backtest_request.json)")
    print("-" * 65)

    registry = LazyLegRegistry()

    # lazy1 — chains to lazy2 on SL, re-momentum on Target
    registry.add("lazy1", build_leg(
        position=PositionType.SELL,
        instrument=InstrumentKind.CE,
        expiry=ExpiryKind.WEEKLY,
        lots=1,
        entry_type=EntryType.PREMIUM_GEQ,
        strike_parameter=strike_by_premium_geq(60),
        leg_stoploss=stop_loss(LegTgtSLType.POINTS, 60),
        leg_target=target_profit(LegTgtSLType.POINTS, 20),
        leg_momentum=momentum(MomentumType.PERCENTAGE_DOWN, 8),
        leg_reentry_sl=re_lazy_leg("lazy2"),
        leg_reentry_tp=re_momentum(count=1),
    ))

    # lazy2 — standalone, RE-ASAP on SL
    registry.add("lazy2", build_leg(
        position=PositionType.SELL,
        instrument=InstrumentKind.CE,
        expiry=ExpiryKind.WEEKLY,
        lots=1,
        entry_type=EntryType.PREMIUM_LEQ,
        strike_parameter=strike_by_premium_leq(40),
        leg_stoploss=stop_loss(LegTgtSLType.POINTS, 40),
        leg_momentum=momentum(MomentumType.PERCENTAGE_DOWN, 10),
        leg_reentry_sl=re_asap(count=2),
    ))

    idle = registry.build()
    print(json.dumps(idle, indent=2))

    # ── How to use in a main leg ───────────────────────────────────
    print("\n\n3. Main leg referencing lazy1 via re_lazy_leg()")
    print("-" * 65)

    main_leg = build_leg(
        position=PositionType.SELL,
        instrument=InstrumentKind.CE,
        expiry=ExpiryKind.WEEKLY,
        lots=1,
        entry_type=EntryType.PREMIUM_RANGE,
        strike_parameter=strike_by_premium_range(50, 200),
        leg_stoploss=stop_loss(LegTgtSLType.PERCENTAGE, 25),
        leg_reentry_sl=re_asap(count=2),
        leg_reentry_tp=re_lazy_leg("lazy1"),   # ← triggers lazy1 on target hit
    )
    print(json.dumps(main_leg, indent=2))

    print(f"\n  Registry: {registry}  ({len(registry)} lazy legs)")
