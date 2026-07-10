"""
position_manager.py
────────────────────
Reusable SL / Target / Trail SL / Overall SL / Reentry logic.
Works identically for backtest, live-trade, and forward-test.

Public API — Leg level
──────────────────────
    calc_sl_price(entry_price, is_sell, sl_config)          → float | None
    calc_tp_price(entry_price, is_sell, tp_config)          → float | None
    is_sl_hit(current_price, sl_price, is_sell)             → bool
    is_tp_hit(current_price, tp_price, is_sell)             → bool
    update_trail_sl(entry_price, current_price,
                    current_sl, is_sell, trail_config)       → float
    get_trail_config(leg_cfg)                               → dict
    get_reentry_sl_config(leg_cfg)                          → dict
    get_reentry_tp_config(leg_cfg)                          → dict

Public API — Overall strategy level
─────────────────────────────────────
    parse_overall_sl(strategy_cfg)                          → (sl_type, sl_value)
    parse_overall_tgt(strategy_cfg)                         → (tgt_type, tgt_value)
    check_overall_sl(strategy_cfg, current_mtm)             → bool
    check_overall_tgt(strategy_cfg, current_mtm)            → bool
    parse_overall_trail_sl(strategy_cfg)                    → (trail_type, for_every, trail_by)
    update_overall_trail_sl(for_every, trail_by,
                            initial_sl, peak_mtm)           → new_sl_threshold
    parse_lock_and_trail(strategy_cfg)                      → LockAndTrailConfig
    check_lock_and_trail(lock_cfg, current_mtm, peak_mtm)   → (should_exit, floor)
    parse_overall_reentry_sl(strategy_cfg)                  → (reentry_type, count)
    parse_overall_reentry_tgt(strategy_cfg)                 → (reentry_type, count)

Public API — Reentry / Lazy leg builders
─────────────────────────────────────────
    build_reentry_action(leg_cfg, reentry_config,
                         triggered_by, now_ts,
                         existing_legs, idle_configs,
                         parent_leg_type)                   → ReentryAction | None
    ReentryAction fields:
        kind        : 'lazy' | 'immediate' | 'at_cost' | 'like_original'
        new_leg_id  : str
        new_leg     : dict   (ready to push to DB)
        description : str

Config field reference
──────────────────────
Leg-level fields (inside ListOfLegConfigs / IdleLegConfigs items):
    LegStopLoss   : {Type, Value}                    — Points | Percentage
    LegTarget     : {Type, Value}                    — Points | Percentage
    LegTrailSL    : {Type, Value:{InstrumentMove, StopLossMove}}
    LegMomentum   : {Type, Value}
    LegReentrySL  : {Type, Value}                    — Immediate | AtCost | LikeOriginal | NextLeg
    LegReentryTP  : {Type, Value}

Strategy-level fields:
    OverallSL           : {Type, Value}              — MTM | PremiumPercentage
    OverallTgt          : {Type, Value}
    OverallTrailSL      : {Type, Value:{TrailForEvery, TrailBy}}
    LockAndTrail        : {Type, Value:{ProfitReaches, LockProfit, IncreaseInProfitBy, TrailProfitBy}}
    OverallReentrySL    : {Type, Value:{ReentryCount}}
    OverallReentryTgt   : {Type, Value:{ReentryCount}}
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)


# ─── helpers ─────────────────────────────────────────────────────────────────

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _is_sell(position_str: str) -> bool:
    return 'sell' in str(position_str or '').lower()


# ═══════════════════════════════════════════════════════════════════════════════
# LEG-LEVEL SL / TARGET / TRAIL
# ═══════════════════════════════════════════════════════════════════════════════

def calc_sl_price(entry_price: float, is_sell: bool, sl_config: dict) -> float | None:
    """
    Compute absolute SL trigger price from entry_price and LegStopLoss config.

    Config format: {Type: "LegTgtSLType.Percentage" | "LegTgtSLType.Points", Value: float}

    Sell position: SL is ABOVE entry (loss if price goes up)
    Buy  position: SL is BELOW entry (loss if price goes down)
    """
    if not sl_config:
        return None
    sl_type  = str(sl_config.get('Type') or '')
    sl_value = _safe_float(sl_config.get('Value'))
    if 'None' in sl_type or sl_value <= 0:
        return None
    if 'Percentage' in sl_type:
        mult = (1 + sl_value / 100) if is_sell else (1 - sl_value / 100)
        return round(entry_price * mult, 2)
    if 'Points' in sl_type:
        return round(entry_price + sl_value if is_sell else entry_price - sl_value, 2)
    return None


def calc_tp_price(entry_price: float, is_sell: bool, tp_config: dict) -> float | None:
    """
    Compute absolute Target trigger price from entry_price and LegTarget config.

    Sell position: TP is BELOW entry (profit if price drops)
    Buy  position: TP is ABOVE entry (profit if price rises)
    """
    if not tp_config:
        return None
    tp_type  = str(tp_config.get('Type') or '')
    tp_value = _safe_float(tp_config.get('Value'))
    if 'None' in tp_type or tp_value <= 0:
        return None
    if 'Percentage' in tp_type:
        mult = (1 - tp_value / 100) if is_sell else (1 + tp_value / 100)
        return round(entry_price * mult, 2)
    if 'Points' in tp_type:
        return round(entry_price - tp_value if is_sell else entry_price + tp_value, 2)
    return None


def is_sl_hit(current_price: float, sl_price: float | None, is_sell: bool) -> bool:
    """True if current_price has crossed the SL threshold."""
    if sl_price is None:
        return False
    return current_price >= sl_price if is_sell else current_price <= sl_price


def is_tp_hit(current_price: float, tp_price: float | None, is_sell: bool) -> bool:
    """True if current_price has crossed the TP threshold."""
    if tp_price is None:
        return False
    return current_price <= tp_price if is_sell else current_price >= tp_price


def update_trail_sl(
    entry_price: float,
    current_price: float,
    current_sl: float,
    is_sell: bool,
    trail_config: dict,
    initial_sl: float | None = None,
) -> float:
    """
    Return updated SL price after applying Trail SL logic.

    LegTrailSL config: {Type: "TrailStopLossType.Points"|"TrailStopLossType.Percentage",
                        Value: {InstrumentMove: X, StopLossMove: Y}}

    Points mode:
        Every X pts the instrument moves in your favour → move SL by Y pts.
    Percentage mode:
        Every X% the instrument moves in your favour → move SL by Y%.
    """
    if not trail_config:
        return current_sl
    trail_type = str(trail_config.get('Type') or '')
    if 'None' in trail_type:
        return current_sl

    val = trail_config.get('Value') or {}
    x = _safe_float(val.get('InstrumentMove'))   # instrument must move X
    y = _safe_float(val.get('StopLossMove'))      # then SL moves Y

    if x <= 0 or y <= 0:
        return current_sl

    base_sl = _safe_float(initial_sl if initial_sl is not None else current_sl)
    if base_sl <= 0:
        return current_sl

    if 'Points' in trail_type:
        if is_sell:
            favorable = entry_price - current_price   # fell = good for sell
            if favorable > 0:
                steps  = int(favorable / x)
                new_sl = base_sl - steps * y
                return min(current_sl, round(new_sl, 2)) if new_sl < current_sl else current_sl
        else:
            favorable = current_price - entry_price   # rose = good for buy
            if favorable > 0:
                steps  = int(favorable / x)
                new_sl = base_sl + steps * y
                return max(current_sl, round(new_sl, 2)) if new_sl > current_sl else current_sl

    if 'Percentage' in trail_type:
        sl_step = entry_price * (y / 100)
        if is_sell:
            favorable_pct = (entry_price - current_price) / entry_price * 100
            if favorable_pct > 0:
                steps  = int(favorable_pct / x)
                new_sl = base_sl - steps * sl_step
                return min(current_sl, round(new_sl, 2)) if new_sl < current_sl else current_sl
        else:
            favorable_pct = (current_price - entry_price) / entry_price * 100
            if favorable_pct > 0:
                steps  = int(favorable_pct / x)
                new_sl = base_sl + steps * sl_step
                return max(current_sl, round(new_sl, 2)) if new_sl > current_sl else current_sl

    return current_sl


# ─── config accessors (handle both storage formats) ──────────────────────────

def get_trail_config(leg_cfg: dict) -> dict:
    """
    Extract TrailSL config from leg config.
    Checks LegTrailSL (standard) and LegStopLoss.Trail (alternate format).
    """
    trail = leg_cfg.get('LegTrailSL') or {}
    if not trail or str(trail.get('Type') or '') == 'None':
        # alternate: nested inside LegStopLoss
        trail = (leg_cfg.get('LegStopLoss') or {}).get('Trail') or {}
    return trail


def get_reentry_sl_config(leg_cfg: dict) -> dict:
    """
    Extract SL reentry config from leg config.
    Checks LegReentrySL (standard) and LegStopLoss.Reentry (alternate format).
    """
    re_cfg = leg_cfg.get('LegReentrySL') or {}
    if not re_cfg or str(re_cfg.get('Type') or '') == 'None':
        re_cfg = (leg_cfg.get('LegStopLoss') or {}).get('Reentry') or {}
    return re_cfg


def get_reentry_tp_config(leg_cfg: dict) -> dict:
    """
    Extract TP reentry config from leg config.
    Checks LegReentryTP (standard) and LegTarget.Reentry (alternate format).
    """
    re_cfg = leg_cfg.get('LegReentryTP') or {}
    if not re_cfg or str(re_cfg.get('Type') or '') == 'None':
        re_cfg = (leg_cfg.get('LegTarget') or {}).get('Reentry') or {}
    return re_cfg


# ═══════════════════════════════════════════════════════════════════════════════
# OVERALL SL / TARGET / TRAIL / LOCK & TRAIL
# ═══════════════════════════════════════════════════════════════════════════════

def parse_overall_sl(strategy_cfg: dict) -> tuple[str, float]:
    """
    Returns (sl_type, sl_value).
    sl_type: 'MTM' | 'PremiumPercentage' | 'None'
    sl_value: threshold (MTM = ₹ amount; PremiumPercentage = %)
    """
    cfg   = strategy_cfg.get('OverallSL') or {}
    stype = str(cfg.get('Type') or '')
    val   = _safe_float(cfg.get('Value'))
    if 'None' in stype or val <= 0:
        return 'None', 0.0
    if 'MTM' in stype:
        return 'MTM', val
    if 'PremiumPercentage' in stype or 'Premium' in stype:
        return 'PremiumPercentage', val
    return 'None', 0.0


def parse_overall_tgt(strategy_cfg: dict) -> tuple[str, float]:
    """Returns (tgt_type, tgt_value)."""
    cfg   = strategy_cfg.get('OverallTgt') or {}
    stype = str(cfg.get('Type') or '')
    val   = _safe_float(cfg.get('Value'))
    if 'None' in stype or val <= 0:
        return 'None', 0.0
    if 'MTM' in stype:
        return 'MTM', val
    if 'PremiumPercentage' in stype or 'Premium' in stype:
        return 'PremiumPercentage', val
    return 'None', 0.0


def check_overall_sl(strategy_cfg: dict, current_mtm: float) -> bool:
    """
    Returns True if current_mtm has crossed the overall SL threshold.

    current_mtm: total P&L across all legs (negative = loss).
    OverallSL.Value is the max loss in ₹ (positive number, e.g. 2500 means exit at -₹2500).
    """
    sl_type, sl_val = parse_overall_sl(strategy_cfg)
    if sl_type == 'None':
        return False
    if sl_type == 'MTM':
        return current_mtm <= -sl_val
    return False


def check_overall_tgt(strategy_cfg: dict, current_mtm: float) -> bool:
    """
    Returns True if current_mtm has reached the overall Target.

    current_mtm: total P&L (positive = profit).
    OverallTgt.Value is the profit target in ₹.
    """
    tgt_type, tgt_val = parse_overall_tgt(strategy_cfg)
    if tgt_type == 'None':
        return False
    if tgt_type == 'MTM':
        return current_mtm >= tgt_val
    return False


def parse_overall_trail_sl(strategy_cfg: dict) -> tuple[str, float, float]:
    """
    Returns (trail_type, for_every, trail_by).
    Config: OverallTrailSL = {Type, Value: {TrailForEvery, TrailBy}}

    Example: trail_type='MTM', for_every=3000, trail_by=1500
    → For every ₹3000 additional profit, move SL up by ₹1500.
    """
    cfg        = strategy_cfg.get('OverallTrailSL') or {}
    trail_type = str(cfg.get('Type') or '')
    if 'None' in trail_type:
        return 'None', 0.0, 0.0
    val        = cfg.get('Value') or {}
    for_every  = _safe_float(val.get('TrailForEvery'))
    trail_by   = _safe_float(val.get('TrailBy'))
    if for_every <= 0 or trail_by <= 0:
        return 'None', 0.0, 0.0
    return trail_type, for_every, trail_by


def update_overall_trail_sl(
    for_every: float,
    trail_by: float,
    initial_sl_value: float,
    peak_mtm: float,
) -> float:
    """
    Compute the current dynamic Overall SL threshold after trail logic.

    initial_sl_value : original OverallSL.Value (e.g. 2500 → exit at -₹2500)
    peak_mtm         : highest MTM P&L seen so far in this cycle
    for_every        : ₹ increment in profit that moves SL
    trail_by         : ₹ SL improves per for_every step

    Returns updated sl_threshold (positive number; exit when mtm <= -threshold).

    Example:
        initial_sl=2500, for_every=3000, trail_by=1500, peak_mtm=6000
        steps = int(6000 / 3000) = 2
        new SL threshold = max(0, 2500 - 2*1500) = 0   → SL is now at breakeven
    """
    if for_every <= 0 or peak_mtm <= 0:
        return initial_sl_value
    steps = int(peak_mtm / for_every)
    return max(0.0, round(initial_sl_value - steps * trail_by, 2))


# ─── Lock & Trail ─────────────────────────────────────────────────────────────

@dataclass
class LockAndTrailConfig:
    enabled:        bool  = False
    kind:           str   = 'None'         # 'Lock' | 'LockAndTrail'
    profit_reaches: float = 0.0            # activate when MTM >= this
    lock_profit:    float = 0.0            # floor: exit if MTM drops below this
    trail_for_every: float = 0.0           # raise floor every X ₹ of extra profit
    trail_by:       float = 0.0            # raise floor by Y ₹


def parse_lock_and_trail(strategy_cfg: dict) -> LockAndTrailConfig:
    """
    Parse LockAndTrail config.

    Config: {Type: "TrailingOption.Lock" | "TrailingOption.LockAndTrail",
             Value: {ProfitReaches, LockProfit, IncreaseInProfitBy, TrailProfitBy}}
    """
    cfg   = strategy_cfg.get('LockAndTrail') or {}
    stype = str(cfg.get('Type') or '')
    if 'None' in stype or not cfg:
        return LockAndTrailConfig()

    val             = cfg.get('Value') or {}
    profit_reaches  = _safe_float(val.get('ProfitReaches'))
    lock_profit     = _safe_float(val.get('LockProfit'))
    trail_for_every = _safe_float(val.get('IncreaseInProfitBy'))
    trail_by        = _safe_float(val.get('TrailProfitBy'))

    if 'LockAndTrail' in stype:
        return LockAndTrailConfig(
            enabled=True, kind='LockAndTrail',
            profit_reaches=profit_reaches, lock_profit=lock_profit,
            trail_for_every=trail_for_every, trail_by=trail_by,
        )
    if 'Lock' in stype:
        return LockAndTrailConfig(
            enabled=True, kind='Lock',
            profit_reaches=profit_reaches, lock_profit=lock_profit,
        )
    return LockAndTrailConfig()


def check_lock_and_trail(
    lock_cfg: LockAndTrailConfig,
    current_mtm: float,
    peak_mtm: float,
) -> tuple[bool, float]:
    """
    Check if Lock or LockAndTrail exit condition is triggered.

    Returns (should_exit, current_floor).

    Lock:
        1. Wait until current_mtm >= profit_reaches (lock activates)
        2. Exit if current_mtm drops below lock_profit

    LockAndTrail:
        1. Wait until current_mtm >= profit_reaches
        2. Floor = lock_profit + int((peak_mtm - profit_reaches) / trail_for_every) * trail_by
        3. Exit if current_mtm drops below floor
    """
    if not lock_cfg.enabled:
        return False, 0.0

    # Lock not yet activated
    if peak_mtm < lock_cfg.profit_reaches:
        return False, 0.0

    if lock_cfg.kind == 'Lock':
        floor = lock_cfg.lock_profit
        return current_mtm <= floor, floor

    if lock_cfg.kind == 'LockAndTrail':
        if lock_cfg.trail_for_every > 0:
            extra_profit = max(0.0, peak_mtm - lock_cfg.profit_reaches)
            steps = int(extra_profit / lock_cfg.trail_for_every)
            floor = lock_cfg.lock_profit + steps * lock_cfg.trail_by
        else:
            floor = lock_cfg.lock_profit
        return current_mtm <= floor, floor

    return False, 0.0


# ─── Overall Reentry ──────────────────────────────────────────────────────────

def parse_overall_reentry_sl(strategy_cfg: dict) -> tuple[str, int]:
    """
    Returns (reentry_type, count).
    reentry_type: 'Immediate' | 'ImmediateReverse' | 'Momentum' | 'MomentumReverse' | 'None'
    """
    return _parse_overall_reentry(strategy_cfg, 'OverallReentrySL')


def parse_overall_reentry_tgt(strategy_cfg: dict) -> tuple[str, int]:
    """Returns (reentry_type, count) for OverallReentryTgt."""
    return _parse_overall_reentry(strategy_cfg, 'OverallReentryTgt')


def _parse_overall_reentry(strategy_cfg: dict, key: str) -> tuple[str, int]:
    cfg = strategy_cfg.get(key) or {}
    stype = str(cfg.get('Type') or cfg.get('type') or '').strip()
    if not stype or 'None' in stype:
        return 'None', 0

    raw_value = cfg.get('Value')
    if raw_value is None:
        raw_value = cfg.get('value')

    count = 0
    if isinstance(raw_value, dict):
        count = _safe_int(
            raw_value.get('ReentryCount')
            if raw_value.get('ReentryCount') is not None
            else raw_value.get('count')
        )
    elif raw_value is not None:
        count = _safe_int(raw_value)
    elif cfg.get('Count') is not None or cfg.get('count') is not None:
        count = _safe_int(cfg.get('Count') if cfg.get('Count') is not None else cfg.get('count'))

    normalized_type = stype.replace(' ', '').lower()
    if 'likeoriginalreverse' in normalized_type:
        return 'LikeOriginalReverse', count
    if 'likeoriginal' in normalized_type:
        return 'LikeOriginal', count
    if 'momentumreverse' in normalized_type:
        return 'MomentumReverse', count
    if 'momentum' in normalized_type:
        return 'Momentum', count
    if 'immediatereverse' in normalized_type or normalized_type.endswith('reverse'):
        return 'ImmediateReverse', count
    if 'immediate' in normalized_type or 'reasap' in normalized_type or 'reentry' in normalized_type or 'renetry' in normalized_type:
        return 'Immediate', count

    for kind in ('LikeOriginalReverse', 'LikeOriginal', 'ImmediateReverse', 'Immediate', 'MomentumReverse', 'Momentum'):
        if kind.lower() in normalized_type:
            return kind, count
    return 'None', 0


# ═══════════════════════════════════════════════════════════════════════════════
# REENTRY / LAZY LEG BUILDERS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class ReentryAction:
    kind:        str    # 'lazy' | 'immediate' | 'at_cost' | 'like_original'
    new_leg_id:  str
    new_leg:     dict   = field(default_factory=dict)
    description: str    = ''


def build_reentry_action(
    leg_cfg: dict,
    reentry_config: dict,
    triggered_by: str,
    now_ts: str,
    existing_legs: list[dict],
    idle_configs: dict,
    parent_leg_type: str = '',
) -> ReentryAction | None:
    """
    Build a ReentryAction from a reentry config dict.

    Parameters
    ----------
    leg_cfg         : the leg config (from ListOfLegConfigs or IdleLegConfigs)
    reentry_config  : LegReentrySL / LegReentryTP value for this leg
    triggered_by    : leg id that triggered the reentry
    now_ts          : current ISO timestamp
    existing_legs   : current legs list (to check duplicates and count children)
    idle_configs    : strategy IdleLegConfigs dict
    parent_leg_type : leg_type of the parent (for naming)

    Returns None if no reentry should happen.
    """
    if not reentry_config:
        return None

    reentry_type  = str(reentry_config.get('Type') or '')
    reentry_value = reentry_config.get('Value')

    if 'None' in reentry_type or not reentry_type:
        return None

    existing_ids = {str(l.get('id') or '') for l in existing_legs if isinstance(l, dict)}
    child_count  = sum(
        1 for l in existing_legs
        if isinstance(l, dict) and str(l.get('triggered_by') or '') == triggered_by
    )

    # ── NextLeg → activate an idle (lazy) leg ────────────────────────────────
    if 'NextLeg' in reentry_type:
        lazy_ref = str((reentry_value or {}).get('NextLegRef') or reentry_value or '')
        lazy_cfg = idle_configs.get(lazy_ref)
        if not lazy_cfg:
            log.warning('Lazy leg ref %s not found in IdleLegConfigs', lazy_ref)
            return None
        if lazy_ref in existing_ids:
            log.info('Lazy leg %s already exists — skipping', lazy_ref)
            return None
        lazy_count  = sum(1 for l in existing_legs if isinstance(l, dict)
                          and str(l.get('triggered_by') or '') == triggered_by
                          and bool(l.get('is_lazy')))
        leg_type    = f'{parent_leg_type}-lazyleg_{lazy_count + 1}' if parent_leg_type else f'lazyleg_{lazy_count + 1}'
        new_leg     = _build_pending_leg_dict(lazy_ref, lazy_cfg, now_ts, triggered_by, leg_type, is_lazy=True)
        return ReentryAction(kind='lazy', new_leg_id=lazy_ref, new_leg=new_leg,
                             description=f'LazyLeg:{lazy_ref} queued leg_type={leg_type}')

    reentry_count = sum(1 for l in existing_legs if isinstance(l, dict)
                        and str(l.get('triggered_by') or '') == triggered_by
                        and not bool(l.get('is_lazy')))
    leg_type  = f'{parent_leg_type}-reentry_{reentry_count + 1}' if parent_leg_type else f'reentry_{reentry_count + 1}'
    orig_id   = str(leg_cfg.get('id') or triggered_by)
    ts_suffix = now_ts.replace(':', '').replace('T', '').replace('-', '')[:14]
    new_id    = f'{orig_id}_re_{ts_suffix}'

    # ── Immediate ─────────────────────────────────────────────────────────────
    if 'Immediate' in reentry_type:
        count = _safe_int((reentry_value or {}).get('ReentryCount') if isinstance(reentry_value, dict) else reentry_value)
        if count <= 0:
            return None
        new_leg = _build_pending_leg_dict(new_id, leg_cfg, now_ts, triggered_by, leg_type)
        new_leg['reentry_count_remaining'] = count - 1
        new_leg['reentry_type']            = 'Immediate'
        return ReentryAction(kind='immediate', new_leg_id=new_id, new_leg=new_leg,
                             description=f'Reentry:Immediate queued ({count}x) leg_type={leg_type}')

    # ── AtCost ────────────────────────────────────────────────────────────────
    if 'AtCost' in reentry_type:
        new_leg = _build_pending_leg_dict(new_id, leg_cfg, now_ts, triggered_by, leg_type)
        new_leg['reentry_type']            = 'AtCost'
        new_leg['reentry_count_remaining'] = _safe_int(reentry_value)
        return ReentryAction(kind='at_cost', new_leg_id=new_id, new_leg=new_leg,
                             description=f'Reentry:AtCost queued leg_type={leg_type}')

    # ── LikeOriginal (Momentum) ───────────────────────────────────────────────
    if 'LikeOriginal' in reentry_type:
        count = _safe_int(reentry_value)
        if count <= 0:
            return None
        new_leg = _build_pending_leg_dict(new_id, leg_cfg, now_ts, triggered_by, leg_type)
        new_leg['reentry_type']            = 'LikeOriginal'
        new_leg['reentry_count_remaining'] = count - 1
        return ReentryAction(kind='like_original', new_leg_id=new_id, new_leg=new_leg,
                             description=f'Reentry:LikeOriginal queued leg_type={leg_type}')

    return None


def _build_pending_leg_dict(
    leg_id: str,
    leg_cfg: dict,
    now_ts: str,
    triggered_by: str,
    leg_type: str,
    is_lazy: bool = False,
) -> dict:
    """
    Build a pending leg dict ready to be pushed to algo_trades.legs.
    This is the DB-agnostic version (no db calls; caller pushes to DB).
    """
    option_type = str(
        leg_cfg.get('InstrumentKind') or leg_cfg.get('option') or 'CE'
    ).replace('LegType.', '')
    position = str(leg_cfg.get('PositionType') or leg_cfg.get('position') or 'PositionType.Sell')
    return {
        'id':                      leg_id,
        'status':                  1,             # OPEN
        'option':                  option_type,
        'position':                position,
        'expiry_kind':             str(leg_cfg.get('ExpiryKind') or leg_cfg.get('expiry_kind') or ''),
        'strike_parameter':        leg_cfg.get('StrikeParameter') or leg_cfg.get('strike_parameter'),
        'entry_kind':              str(leg_cfg.get('EntryType') or leg_cfg.get('entry_kind') or ''),
        'entry_trade':             None,
        'exit_trade':              None,
        'quantity':                0,
        'lot_config_value':        _safe_int((leg_cfg.get('LotConfig') or {}).get('Value') or leg_cfg.get('lot_config_value') or 1),
        'last_saw_price':          0.0,
        'current_sl_price':        None,
        'strike':                  None,
        'expiry_date':             None,
        'token':                   None,
        'symbol':                  None,
        'triggered_by':            triggered_by,
        'queued_at':               now_ts,
        'is_lazy':                 is_lazy,
        'is_reentered_leg':        not is_lazy,
        'leg_type':                leg_type,
        'transactions':            {},
        'current_transaction_id':  None,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE: full leg exit check
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class LegCheckResult:
    sl_hit:       bool  = False
    tp_hit:       bool  = False
    new_sl_price: float = 0.0     # updated trail SL (same as input if no trail)
    exit_reason:  str   = ''      # 'stoploss' | 'target' | ''


def check_leg_exit(
    entry_price: float,
    current_price: float,
    current_sl: float | None,
    is_sell: bool,
    leg_cfg: dict,
) -> LegCheckResult:
    """
    One-stop function: check SL hit, TP hit, and update trail SL for a live leg.

    Parameters
    ----------
    entry_price   : original entry price of the leg
    current_price : latest market price
    current_sl    : stored SL price from DB (None = compute fresh from config)
    is_sell       : True for sell positions
    leg_cfg       : leg config dict (from ListOfLegConfigs / IdleLegConfigs)

    Returns LegCheckResult with sl_hit, tp_hit, new_sl_price, exit_reason.
    """
    sl_config  = leg_cfg.get('LegStopLoss') or {}
    tp_config  = leg_cfg.get('LegTarget')   or {}
    trail_cfg  = get_trail_config(leg_cfg)

    initial_sl = calc_sl_price(entry_price, is_sell, sl_config)
    sl_price = current_sl or initial_sl
    tp_price = calc_tp_price(entry_price, is_sell, tp_config)

    # Apply trail SL update
    new_sl = sl_price
    if sl_price and trail_cfg:
        new_sl = update_trail_sl(entry_price, current_price, sl_price, is_sell, trail_cfg, initial_sl=initial_sl)

    if is_sl_hit(current_price, new_sl, is_sell):
        return LegCheckResult(sl_hit=True, new_sl_price=new_sl or 0.0, exit_reason='stoploss')

    if is_tp_hit(current_price, tp_price, is_sell):
        return LegCheckResult(tp_hit=True, new_sl_price=new_sl or 0.0, exit_reason='target')

    return LegCheckResult(new_sl_price=new_sl or 0.0)
