"""
Overall Momentum Feature
Builds OverallMomentum config for the strategy-level entry condition.

What it does:
  Entry triggers ONLY when the combined premium of ALL legs moves
  by a specified amount (points or %) in a given direction.

Combined premium calculation:
  - All legs included (excluding lazy legs)
  - Each leg counts as 1 lot (lot size / ratio ignored)
  - Buy/Sell direction is respected

Four momentum types:
  POINTS_UP       → combined premium rises by N points
  POINTS_DOWN     → combined premium falls by N points
  PERCENTAGE_UP   → combined premium rises by N%
  PERCENTAGE_DOWN → combined premium falls by N%

JSON shape (from sample_backtest_request.json):
  "OverallMomentum": {"Type": "MomentumType.PercentageDown", "Value": 8}
  "OverallMomentum": {"Type": "None", "Value": 0}   ← disabled

Re-entry behaviour:
  - ASAP re-entry    → bypasses this condition
  - Momentum re-entry → respects this condition

Lazy legs:
  - Excluded from combined premium calculation
  - They retain their own independent Simple Momentum / ORB logic
"""

try:
    from .leg_builder import MomentumType
except ImportError:
    from leg_builder import MomentumType


# ─── Disabled default ─────────────────────────────────────────────────────────

DISABLED_MOMENTUM = {"Type": "None", "Value": 0}


# ─── Builder ──────────────────────────────────────────────────────────────────

def overall_momentum(m_type: MomentumType, value: float) -> dict:
    """
    Build OverallMomentum config.

    Parameters
    ----------
    m_type : MomentumType
        MomentumType.POINTS_UP       - combined premium rises by N points
        MomentumType.POINTS_DOWN     - combined premium falls by N points
        MomentumType.PERCENTAGE_UP   - combined premium rises by N%
        MomentumType.PERCENTAGE_DOWN - combined premium falls by N%
    value  : float
        Number of points or percentage

    Returns
    -------
    dict
        {"Type": "MomentumType.PercentageDown", "Value": 8}

    Examples
    --------
    overall_momentum(MomentumType.PERCENTAGE_DOWN, 8)
    overall_momentum(MomentumType.POINTS_UP, 10)
    overall_momentum(MomentumType.PERCENTAGE_UP, 5)
    overall_momentum(MomentumType.POINTS_DOWN, 15)
    """
    return {"Type": m_type.value, "Value": value}


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    print("=" * 60)
    print("Overall Momentum Feature Demo")
    print("=" * 60)

    print("\nDisabled (default when not set):")
    print(json.dumps(DISABLED_MOMENTUM, indent=2))

    print("\n1. Percentage Down — 8%")
    print("   Entry only when combined premium falls by 8%")
    result = overall_momentum(MomentumType.PERCENTAGE_DOWN, 8)
    print(json.dumps(result, indent=2))

    print("\n2. Percentage Up — 5%")
    print("   Entry only when combined premium rises by 5%")
    result = overall_momentum(MomentumType.PERCENTAGE_UP, 5)
    print(json.dumps(result, indent=2))

    print("\n3. Points Down — 15 pts")
    print("   Entry only when combined premium falls by 15 points")
    result = overall_momentum(MomentumType.POINTS_DOWN, 15)
    print(json.dumps(result, indent=2))

    print("\n4. Points Up — 10 pts")
    print("   Entry only when combined premium rises by 10 points")
    result = overall_momentum(MomentumType.POINTS_UP, 10)
    print(json.dumps(result, indent=2))

    print("\n" + "=" * 60)
    print("Iron Condor Example (from docs):")
    print("=" * 60)
    print("""
  Strategy: 4-leg Iron Condor
    Sell CE  ₹55  Sell PE  ₹55
    Buy  CE  ₹30  Buy  PE  ₹35
    Combined premium = 55 + 55 - 30 - 35 = ₹45

  Setting: overall_momentum(MomentumType.POINTS_UP, 10)
  → Entry triggers when premium reaches ₹55 (₹45 + ₹10)
""")
    print("Config:", json.dumps(overall_momentum(MomentumType.POINTS_UP, 10), indent=2))

    print("\n" + "=" * 60)
    print("All MomentumType options:")
    print("=" * 60)
    for m in MomentumType:
        print(f"  MomentumType.{m.name:<20} → \"{m.value}\"")
