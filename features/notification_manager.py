"""
notification_manager.py
────────────────────────
Records every strategy event into the `algo_trade_notification` collection.

One document per event. Provides a complete audit trail to verify that
every feature (entry, SL, target, trail SL, reentry, overall SL/target)
worked correctly.

Event types
───────────
    entry_taken              — leg entered: strike choice, sl, target, overall sl/tgt
    simple_momentum_armed    — leg is waiting for simple momentum trigger
    simple_momentum_triggered — simple momentum condition matched; leg entry can be taken
    sl_hit                   — leg SL triggered
    target_hit               — leg Target triggered
    trail_sl_changed         — trail SL price updated
    reentry_queued           — reentry / lazy-leg queued after SL or target
    overall_sl_hit           — strategy-level overall SL crossed
    overall_target_hit       — strategy-level overall target crossed
    overall_trail_sl_changed — overall dynamic SL threshold updated
    lock_and_trail_exit      — LockAndTrail / Lock profit floor triggered exit
    overall_reentry_queued   — original legs re-queued after OverallReentrySL/Tgt
    force_exit               — leg closed at exit_time, overall_sl, overall_target, or lock_and_trail
    entry_blocked            — pending leg could not be entered this tick (missing expiry/
                                strike/chain-price/spot, or an unexpected resolution error)

Document structure (every event)
──────────────────────────────────
    _id              : auto ObjectId
    strategy_id      : str
    trade_id         : str
    leg_id           : str | None
    event_type       : str
    timestamp        : str  (ISO)
    trade_date       : str  (YYYY-MM-DD)
    strategy_name    : str
    ticker           : str
    data             : dict  (event-specific fields — see below)

data fields per event_type
───────────────────────────
entry_taken:
    strike, option_type, expiry, position,
    entry_kind, strike_parameter,
    entry_price, spot_at_entry,
    momentum_type, momentum_value,
    momentum_base_price, momentum_target_price,
    sl_price, sl_type, sl_value,
    tp_price, tp_type, tp_value,
    trail_sl_config,
    overall_sl_type, overall_sl_value,
    overall_tgt_type, overall_tgt_value

simple_momentum_armed:
    strike, option_type, expiry, position,
    momentum_type, momentum_value,
    base_price, target_price, spot_price

simple_momentum_triggered:
    strike, option_type, expiry, position,
    momentum_type, momentum_value,
    base_price, target_price, current_price, spot_price

sl_hit:
    strike, option_type, entry_price,
    exit_price, sl_price, pnl

target_hit:
    strike, option_type, entry_price,
    exit_price, tp_price, pnl

trail_sl_changed:
    strike, option_type, entry_price,
    current_price, old_sl, new_sl

reentry_queued:
    triggered_by_leg_id, reentry_type,
    new_leg_id, reentry_kind, reason (sl/target)

overall_sl_hit:
    overall_sl_type, overall_sl_value,
    current_mtm, legs_pnl

overall_target_hit:
    overall_tgt_type, overall_tgt_value,
    current_mtm, legs_pnl

overall_trail_sl_changed:
    old_sl_threshold, new_sl_threshold,
    peak_mtm, current_mtm

lock_and_trail_exit:
    floor, current_mtm, legs_pnl

overall_reentry_queued:
    reason, reentry_type, reentry_count, legs_queued

force_exit:
    strike, option_type, entry_price,
    exit_price, exit_reason, pnl
    (exit_reason: exit_time | overall_sl | overall_target | lock_and_trail)

entry_blocked:
    reason (e.g. expiry_missing, chain_empty, strike_missing, resolve_exception),
    message, option_type, expiry_kind, strike_parameter
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

NOTIFICATION_COLLECTION = 'algo_trade_notification'


# ─── helpers ─────────────────────────────────────────────────────────────────

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _trade_date_from_ts(ts: str) -> str:
    """Extract YYYY-MM-DD from an ISO or datetime string."""
    return str(ts or '')[:10]


# ─── core writer ─────────────────────────────────────────────────────────────

def _build_what_happened(event_type: str, data: dict) -> str:
    """Build a human-readable 'what happened' description for the event."""
    d = data
    et = event_type

    if et == 'entry_taken':
        pos = str(d.get('position') or '').replace('PositionType.', '')
        momentum = ''
        if d.get('momentum_type') and d.get('momentum_type') != 'None':
            momentum = (
                f", Simple Momentum {d.get('momentum_type')} {d.get('momentum_value')} "
                f"(base {d.get('momentum_base_price')} -> trigger {d.get('momentum_target_price')})"
            )
        trail = ''
        if d.get('trail_sl_type') and d.get('trail_sl_type') != 'None':
            trail = f". Trail SL: every {d.get('trail_instrument_move')} pts move -> SL shifts {d.get('trail_sl_move')} pts"
        tp = ''
        if d.get('tp_type') and d.get('tp_type') != 'None':
            tp = f", TP set @ {d.get('tp_price')}"
        return (
            f"{pos} {d.get('strike')} {d.get('option_type')} @ {d.get('entry_price')}, "
            f"spot {d.get('spot_at_entry')}, "
            f"SL set @ {d.get('sl_price')}{tp}{momentum}{trail}"
        )

    if et == 'simple_momentum_armed':
        return (
            f"Simple Momentum armed for {d.get('strike')} {d.get('option_type')}. "
            f"Rule: {d.get('momentum_type')} {d.get('momentum_value')}. "
            f"Base {d.get('base_price')}, trigger {d.get('target_price')}, spot {d.get('spot_price')}."
        )

    if et == 'simple_momentum_triggered':
        return (
            f"Simple Momentum triggered for {d.get('strike')} {d.get('option_type')}. "
            f"Current price {d.get('current_price')} crossed trigger {d.get('target_price')} "
            f"(base {d.get('base_price')}, rule {d.get('momentum_type')} {d.get('momentum_value')})."
        )

    if et == 'sl_hit':
        return (
            f"Stop Loss triggered. Exited {d.get('strike')} {d.get('option_type')} "
            f"@ {d.get('exit_price')} (SL was {d.get('sl_price')}, entry {d.get('entry_price')}). "
            f"Leg P&L: {d.get('pnl')}"
        )

    if et == 'target_hit':
        return (
            f"Target reached. Exited {d.get('strike')} {d.get('option_type')} "
            f"@ {d.get('exit_price')} (TP {d.get('tp_price')}, entry {d.get('entry_price')}). "
            f"Leg P&L: {d.get('pnl')}"
        )

    if et == 'trail_sl_changed':
        instr   = d.get('trail_instrument_move', '?')
        sl_move = d.get('trail_sl_move', '?')
        moved   = abs(_safe_float(d.get('sl_moved_by')))
        steps   = round(moved / _safe_float(sl_move, 1)) if _safe_float(sl_move) > 0 else 1
        return (
            f"Trail SL moved from {d.get('old_sl')} to {d.get('new_sl')} "
            f"after favorable move {moved}. "
            f"Rule: every {instr} favorable move, SL shifts by {sl_move}. "
            f"Steps reached: {steps}."
        )

    if et == 'reentry_queued':
        return (
            f"Re-entry queued ({d.get('reentry_kind') or d.get('reentry_type')}) "
            f"from leg {d.get('triggered_by_leg_id')}. "
            f"New leg: {d.get('new_leg_id')}. Reason: {d.get('reason')}."
        )

    if et == 'overall_sl_hit':
        return (
            f"Overall Stop Loss hit. MTM reached {d.get('current_mtm')} "
            f"(limit: {d.get('overall_sl_type')} {d.get('overall_sl_value')}, "
            f"cycle {d.get('cycle_number')}/{(int(d.get('configured_reentry_count') or 0) + 1)}). "
            f"All open legs closed."
        )

    if et == 'overall_target_hit':
        return (
            f"Overall Target reached. MTM {d.get('current_mtm')} "
            f"(target: {d.get('overall_tgt_type')} {d.get('overall_tgt_value')}, "
            f"cycle {d.get('cycle_number')}/{(int(d.get('configured_reentry_count') or 0) + 1)}). "
            f"All open legs closed."
        )

    if et == 'overall_trail_sl_changed':
        return (
            f"Overall Trail SL tightened from -{d.get('old_sl_threshold')} "
            f"to -{d.get('new_sl_threshold')}. "
            f"Peak MTM: {d.get('peak_mtm')}, current MTM: {d.get('current_mtm')}."
        )

    if et == 'lock_and_trail_exit':
        return (
            f"Lock & Trail exit triggered. MTM {d.get('current_mtm')} "
            f"dropped below profit floor {d.get('floor')}. All open legs closed."
        )

    if et == 'overall_reentry_queued':
        legs = ', '.join(d.get('legs_queued') or [])
        return (
            f"Overall re-entry after {d.get('reason')} "
            f"({d.get('reentry_type')}, total count: {d.get('reentry_count')}, "
            f"done: {d.get('completed_reentries')}, next cycle: {d.get('next_cycle_number')}). "
            f"Next Overall SL: {d.get('next_overall_sl_value') or 0}, "
            f"Next Overall Target: {d.get('next_overall_tgt_value') or 0}. "
            f"New legs: {legs or '—'}."
        )

    if et == 'force_exit':
        return (
            f"Force exit ({d.get('exit_reason')}). "
            f"Closed {d.get('strike')} {d.get('option_type')} "
            f"@ {d.get('exit_price')}. Entry: {d.get('entry_price')}. "
            f"Leg P&L: {d.get('pnl')}"
        )

    if et == 'entry_blocked':
        return (
            f"Entry could not be taken for {d.get('option_type') or '?'} "
            f"({d.get('strike_parameter') or d.get('expiry_kind') or ''}). "
            f"Reason: {d.get('reason')}. {d.get('message') or ''}"
        ).strip()

    return ''





def _push_notification(db, doc: dict) -> None:
    """Compute what_happened, then insert one notification document."""
    try:
        doc['what_happened'] = _build_what_happened(doc.get('event_type', ''), doc.get('data') or {})
        db[NOTIFICATION_COLLECTION].insert_one(doc)
    except Exception as exc:
        log.error('notification_manager insert error: %s', exc)


def _base(
    strategy_id: str,
    trade_id: str,
    event_type: str,
    timestamp: str,
    strategy_name: str = '',
    ticker: str = '',
    leg_id: str | None = None,
) -> dict:
    return {
        'strategy_id':   strategy_id,
        'trade_id':      trade_id,
        'leg_id':        leg_id,
        'event_type':    event_type,
        'timestamp':     timestamp,
        'trade_date':    _trade_date_from_ts(timestamp),
        'strategy_name': strategy_name,
        'ticker':        ticker,
        'data':          {},
    }


def _trade_meta(trade: dict) -> dict:
    """Extract common meta fields from a trade dict."""
    return {
        'strategy_id':   str(trade.get('strategy_id') or ''),
        'strategy_name': str(trade.get('name') or ''),
        'ticker':        str(
            (trade.get('config') or {}).get('Ticker')
            or (trade.get('strategy') or {}).get('Ticker')
            or trade.get('ticker') or ''
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC EVENT RECORDERS
# ═══════════════════════════════════════════════════════════════════════════════

def record_entry_taken(
    db,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
    timestamp: str,
    overall_sl_type: str = 'None',
    overall_sl_value: float = 0.0,
    overall_tgt_type: str = 'None',
    overall_tgt_value: float = 0.0,
) -> None:
    """
    Record that a leg entry was taken.
    Captures strike selection details, SL, target, trail, and overall SL/tgt.
    """
    meta = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')

    entry_trade = leg.get('entry_trade') or {}
    sl_config   = leg_cfg.get('LegStopLoss') or {}
    tp_config   = leg_cfg.get('LegTarget')   or {}
    trail_cfg   = leg_cfg.get('LegTrailSL')  or {}

    entry_price = _safe_float(entry_trade.get('price'))
    sl_value    = _safe_float(sl_config.get('Value'))
    tp_value    = _safe_float(tp_config.get('Value'))

    # compute absolute SL and TP price for display
    is_sell = 'sell' in str(leg.get('position') or '').lower()
    from features.position_manager import calc_sl_price, calc_tp_price
    sl_price = calc_sl_price(entry_price, is_sell, sl_config)
    tp_price = calc_tp_price(entry_price, is_sell, tp_config)

    doc = _base(
        meta['strategy_id'], trade_id, 'entry_taken', timestamp,
        meta['strategy_name'], meta['ticker'], leg_id=str(leg.get('id') or ''),
    )
    doc['data'] = {
        # --- Strike info ---
        'strike':           leg.get('strike'),
        'option_type':      str(leg.get('option') or ''),
        'expiry':           str(leg.get('expiry_date') or ''),
        'position':         str(leg.get('position') or ''),
        'entry_kind':       str(leg.get('entry_kind') or ''),
        'strike_parameter': leg.get('strike_parameter'),
        'entry_price':      entry_price,
        'spot_at_entry':    _safe_float(entry_trade.get('underlying_at_trade') or entry_trade.get('underlying_trigger_price')),
        'momentum_type':    str((leg_cfg.get('LegMomentum') or {}).get('Type') or 'None'),
        'momentum_value':   _safe_float((leg_cfg.get('LegMomentum') or {}).get('Value')),
        'momentum_base_price': _safe_float(leg.get('momentum_base_price')),
        'momentum_target_price': _safe_float(leg.get('momentum_target_price')),

        # --- SL ---
        'sl_price':         sl_price,
        'sl_type':          str(sl_config.get('Type') or 'None'),
        'sl_value':         sl_value,

        # --- Target ---
        'tp_price':         tp_price,
        'tp_type':          str(tp_config.get('Type') or 'None'),
        'tp_value':         tp_value,

        # --- Trail SL ---
        'trail_sl_type':    str(trail_cfg.get('Type') or 'None'),
        'trail_instrument_move': _safe_float((trail_cfg.get('Value') or {}).get('InstrumentMove')),
        'trail_sl_move':         _safe_float((trail_cfg.get('Value') or {}).get('StopLossMove')),

        # --- Overall strategy SL / Target ---
        'overall_sl_type':   overall_sl_type,
        'overall_sl_value':  overall_sl_value,
        'overall_tgt_type':  overall_tgt_type,
        'overall_tgt_value': overall_tgt_value,
    }
    _push_notification(db, doc)


def record_entry_blocked(
    db,
    trade: dict,
    leg_id: str,
    reason: str,
    message: str,
    timestamp: str,
    option_type: str = '',
    expiry_kind: str = '',
    strike_parameter: Any = None,
) -> None:
    """
    Record that a pending leg's entry could not be taken this tick.
    One document per (leg_id, reason) per _DEDUP_WINDOW-ish burst is NOT
    enforced here — callers are expected to only call this on state change
    (new reason, or first occurrence) to avoid flooding the history with a
    duplicate row every retry tick.
    """
    meta = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')
    doc = _base(
        meta['strategy_id'], trade_id, 'entry_blocked', timestamp,
        meta['strategy_name'], meta['ticker'], leg_id=leg_id,
    )
    doc['data'] = {
        'reason':           reason,
        'message':          message,
        'option_type':      option_type,
        'expiry_kind':      expiry_kind,
        'strike_parameter': strike_parameter,
    }
    _push_notification(db, doc)


def record_simple_momentum_armed(
    db,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
    timestamp: str,
    base_price: float,
    target_price: float,
    spot_price: float,
) -> None:
    meta = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')
    momentum_cfg = leg_cfg.get('LegMomentum') or {}

    doc = _base(
        meta['strategy_id'], trade_id, 'simple_momentum_armed', timestamp,
        meta['strategy_name'], meta['ticker'], leg_id=str(leg.get('id') or ''),
    )
    doc['data'] = {
        'strike':         leg.get('strike'),
        'option_type':    str(leg.get('option') or ''),
        'expiry':         str(leg.get('expiry_date') or ''),
        'position':       str(leg.get('position') or ''),
        'momentum_type':  str(momentum_cfg.get('Type') or 'None'),
        'momentum_value': _safe_float(momentum_cfg.get('Value')),
        'base_price':     round(base_price, 2),
        'target_price':   round(target_price, 2),
        'spot_price':     round(spot_price, 2),
    }
    _push_notification(db, doc)


def record_simple_momentum_triggered(
    db,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
    timestamp: str,
    base_price: float,
    target_price: float,
    current_price: float,
    spot_price: float,
) -> None:
    meta = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')
    momentum_cfg = leg_cfg.get('LegMomentum') or {}

    doc = _base(
        meta['strategy_id'], trade_id, 'simple_momentum_triggered', timestamp,
        meta['strategy_name'], meta['ticker'], leg_id=str(leg.get('id') or ''),
    )
    doc['data'] = {
        'strike':         leg.get('strike'),
        'option_type':    str(leg.get('option') or ''),
        'expiry':         str(leg.get('expiry_date') or ''),
        'position':       str(leg.get('position') or ''),
        'momentum_type':  str(momentum_cfg.get('Type') or 'None'),
        'momentum_value': _safe_float(momentum_cfg.get('Value')),
        'base_price':     round(base_price, 2),
        'target_price':   round(target_price, 2),
        'current_price':  round(current_price, 2),
        'spot_price':     round(spot_price, 2),
    }
    _push_notification(db, doc)


def upsert_simple_momentum_feature_status(
    db,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
    timestamp: str,
    base_price: float,
    target_price: float,
    current_price: float | None = None,
    status: str = 'pending',
    enabled: bool = True,
) -> None:
    meta = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')
    leg_id = _resolve_feature_leg_id(db, trade_id, str(leg.get('_id') or leg.get('id') or ''))
    if not trade_id or not leg_id:
        return

    col = db[LEG_FEATURE_STATUS_COLLECTION]
    now = timestamp or _lfs_now()
    momentum_cfg = leg_cfg.get('LegMomentum') or {}
    momentum_type = str(momentum_cfg.get('Type') or 'None')
    momentum_value = _safe_float(momentum_cfg.get('Value'))
    current_value = _safe_float(current_price if current_price is not None else leg.get('last_saw_price'))
    description = (
        f"Simple Momentum active: {momentum_type} {momentum_value}. "
        f"Base price: {_format_rupee(base_price)}. "
        f"Trigger price: {_format_rupee(target_price)}. "
        f"Current price: {_format_rupee(current_value)}."
    )

    try:
        col.update_one(
            {'trade_id': trade_id, 'leg_id': leg_id, 'feature': 'simpleMomentum'},
            {'$set': {
                'strategy_id': meta['strategy_id'],
                'strategy_name': meta['strategy_name'],
                'ticker': meta['ticker'],
                'trade_date': _trade_date_from_ts(now),
                'feature': 'simpleMomentum',
                'enabled': enabled,
                'status': status,
                'entry_price': _safe_float(base_price),
                'trigger_price': round(target_price, 2),
                'trigger_type': momentum_type,
                'trigger_value': momentum_value,
                'trigger_description': description,
                'current_option_price': round(current_value, 2),
                'updated_at': now,
                'triggered_at': now if status == 'triggered' else None,
                'triggered_price': round(current_value, 2) if status == 'triggered' else None,
            }, '$setOnInsert': {
                'trade_id': trade_id,
                'leg_id': leg_id,
                'created_at': now,
                'disabled_at': None,
                'disabled_reason': None,
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error('upsert_simple_momentum_feature_status error leg=%s: %s', leg_id, exc)


def record_sl_hit(
    db,
    trade: dict,
    leg: dict,
    timestamp: str,
    exit_price: float,
    sl_price: float,
) -> None:
    meta     = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')
    entry_price = _safe_float((leg.get('entry_trade') or {}).get('price'))
    quantity    = int(leg.get('quantity') or 0) * max(1, int(leg.get('lot_size') or 1))
    is_sell     = 'sell' in str(leg.get('position') or '').lower()
    pnl = (entry_price - exit_price) * quantity if is_sell else (exit_price - entry_price) * quantity

    doc = _base(
        meta['strategy_id'], trade_id, 'sl_hit', timestamp,
        meta['strategy_name'], meta['ticker'], leg_id=str(leg.get('id') or ''),
    )
    doc['data'] = {
        'strike':       leg.get('strike'),
        'option_type':  str(leg.get('option') or ''),
        'expiry':       str(leg.get('expiry_date') or ''),
        'position':     str(leg.get('position') or ''),
        'entry_price':  entry_price,
        'exit_price':   exit_price,
        'sl_price':     sl_price,
        'pnl':          round(pnl, 2),
    }
    _push_notification(db, doc)


def record_target_hit(
    db,
    trade: dict,
    leg: dict,
    timestamp: str,
    exit_price: float,
    tp_price: float,
) -> None:
    meta     = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')
    entry_price = _safe_float((leg.get('entry_trade') or {}).get('price'))
    quantity    = int(leg.get('quantity') or 0) * max(1, int(leg.get('lot_size') or 1))
    is_sell     = 'sell' in str(leg.get('position') or '').lower()
    pnl = (entry_price - exit_price) * quantity if is_sell else (exit_price - entry_price) * quantity

    doc = _base(
        meta['strategy_id'], trade_id, 'target_hit', timestamp,
        meta['strategy_name'], meta['ticker'], leg_id=str(leg.get('id') or ''),
    )
    doc['data'] = {
        'strike':       leg.get('strike'),
        'option_type':  str(leg.get('option') or ''),
        'expiry':       str(leg.get('expiry_date') or ''),
        'position':     str(leg.get('position') or ''),
        'entry_price':  entry_price,
        'exit_price':   exit_price,
        'tp_price':     tp_price,
        'pnl':          round(pnl, 2),
    }
    _push_notification(db, doc)


def record_trail_sl_changed(
    db,
    trade: dict,
    leg: dict,
    timestamp: str,
    old_sl: float,
    new_sl: float,
    current_price: float,
    trail_config: dict | None = None,
) -> None:
    meta     = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')
    entry_price = _safe_float((leg.get('entry_trade') or {}).get('price'))

    tc = trail_config or {}
    trail_val = tc.get('Value') or {}
    if not isinstance(trail_val, dict):
        trail_val = {}

    doc = _base(
        meta['strategy_id'], trade_id, 'trail_sl_changed', timestamp,
        meta['strategy_name'], meta['ticker'], leg_id=str(leg.get('id') or ''),
    )
    doc['data'] = {
        'strike':               leg.get('strike'),
        'option_type':          str(leg.get('option') or ''),
        'expiry':               str(leg.get('expiry_date') or ''),
        'position':             str(leg.get('position') or ''),
        'entry_price':          entry_price,
        'current_price':        current_price,
        'old_sl':               round(old_sl, 2),
        'new_sl':               round(new_sl, 2),
        'sl_moved_by':          round(old_sl - new_sl, 2),
        'trail_instrument_move': _safe_float(trail_val.get('InstrumentMove')),
        'trail_sl_move':         _safe_float(trail_val.get('StopLossMove')),
    }
    _push_notification(db, doc)


def record_reentry_queued(
    db,
    trade: dict,
    timestamp: str,
    triggered_by_leg_id: str,
    reentry_kind: str,
    new_leg_id: str,
    reentry_type: str,
    reason: str,
) -> None:
    """reason: 'sl' | 'target' | 'overall_sl' | 'overall_target'"""
    meta     = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')

    doc = _base(
        meta['strategy_id'], trade_id, 'reentry_queued', timestamp,
        meta['strategy_name'], meta['ticker'],
        leg_id=triggered_by_leg_id,
    )
    doc['data'] = {
        'triggered_by_leg_id': triggered_by_leg_id,
        'new_leg_id':          new_leg_id,
        'reentry_kind':        reentry_kind,        # lazy | immediate | at_cost | like_original
        'reentry_type':        reentry_type,        # raw Type string from config
        'reason':              reason,              # sl | target | overall_sl | overall_target
    }
    _push_notification(db, doc)


def record_overall_sl_hit(
    db,
    trade: dict,
    timestamp: str,
    overall_sl_type: str,
    overall_sl_value: float,
    current_mtm: float,
    legs_pnl: list[dict] | None = None,
    cycle_number: int | None = None,
    configured_reentry_count: int | None = None,
) -> None:
    meta     = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')

    doc = _base(
        meta['strategy_id'], trade_id, 'overall_sl_hit', timestamp,
        meta['strategy_name'], meta['ticker'],
    )
    doc['data'] = {
        'overall_sl_type':  overall_sl_type,
        'overall_sl_value': overall_sl_value,
        'current_mtm':      round(current_mtm, 2),
        'legs_pnl':         legs_pnl or [],
        'cycle_number':     int(cycle_number or 1),
        'configured_reentry_count': int(configured_reentry_count or 0),
    }
    _push_notification(db, doc)


def record_overall_target_hit(
    db,
    trade: dict,
    timestamp: str,
    overall_tgt_type: str,
    overall_tgt_value: float,
    current_mtm: float,
    legs_pnl: list[dict] | None = None,
    cycle_number: int | None = None,
    configured_reentry_count: int | None = None,
) -> None:
    meta     = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')

    doc = _base(
        meta['strategy_id'], trade_id, 'overall_target_hit', timestamp,
        meta['strategy_name'], meta['ticker'],
    )
    doc['data'] = {
        'overall_tgt_type':  overall_tgt_type,
        'overall_tgt_value': overall_tgt_value,
        'current_mtm':       round(current_mtm, 2),
        'legs_pnl':          legs_pnl or [],
        'cycle_number':      int(cycle_number or 1),
        'configured_reentry_count': int(configured_reentry_count or 0),
    }
    _push_notification(db, doc)


def record_overall_trail_sl_changed(
    db,
    trade: dict,
    timestamp: str,
    old_sl_threshold: float,
    new_sl_threshold: float,
    peak_mtm: float,
    current_mtm: float,
) -> None:
    meta     = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')

    doc = _base(
        meta['strategy_id'], trade_id, 'overall_trail_sl_changed', timestamp,
        meta['strategy_name'], meta['ticker'],
    )
    doc['data'] = {
        'old_sl_threshold': round(old_sl_threshold, 2),
        'new_sl_threshold': round(new_sl_threshold, 2),
        'improved_by':      round(old_sl_threshold - new_sl_threshold, 2),
        'peak_mtm':         round(peak_mtm, 2),
        'current_mtm':      round(current_mtm, 2),
    }
    _push_notification(db, doc)


def record_lock_and_trail_exit(
    db,
    trade: dict,
    timestamp: str,
    floor: float,
    current_mtm: float,
    legs_pnl: list[dict] | None = None,
) -> None:
    """Record a LockAndTrail (Lock or LockAndTrail) strategy exit."""
    meta     = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')

    doc = _base(
        meta['strategy_id'], trade_id, 'lock_and_trail_exit', timestamp,
        meta['strategy_name'], meta['ticker'],
    )
    doc['data'] = {
        'floor':       round(floor, 2),
        'current_mtm': round(current_mtm, 2),
        'legs_pnl':    legs_pnl or [],
    }
    _push_notification(db, doc)


def record_overall_reentry_queued(
    db,
    trade: dict,
    timestamp: str,
    reason: str,
    reentry_type: str,
    count: int,
    legs_queued: list[str],
    completed_reentries: int | None = None,
    next_cycle_number: int | None = None,
    next_overall_sl_value: float | None = None,
    next_overall_tgt_value: float | None = None,
) -> None:
    """
    Record that all original legs were re-queued after OverallReentrySL or OverallReentryTgt.

    reason : 'overall_sl' | 'overall_target'
    """
    meta     = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')

    doc = _base(
        meta['strategy_id'], trade_id, 'overall_reentry_queued', timestamp,
        meta['strategy_name'], meta['ticker'],
    )
    doc['data'] = {
        'reason':        reason,
        'reentry_type':  reentry_type,
        'reentry_count': count,
        'legs_queued':   legs_queued,
        'completed_reentries': int(completed_reentries or 0),
        'next_cycle_number': int(next_cycle_number or 1),
        'remaining_reentries': max(0, int(count or 0) - int(completed_reentries or 0)),
        'next_overall_sl_value': round(_safe_float(next_overall_sl_value), 2),
        'next_overall_tgt_value': round(_safe_float(next_overall_tgt_value), 2),
    }
    _push_notification(db, doc)


def record_force_exit(
    db,
    trade: dict,
    leg: dict,
    timestamp: str,
    exit_price: float,
    exit_reason: str = 'exit_time',
) -> None:
    meta     = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')
    entry_price = _safe_float((leg.get('entry_trade') or {}).get('price'))
    quantity    = int(leg.get('quantity') or 0) * max(1, int(leg.get('lot_size') or 1))
    is_sell     = 'sell' in str(leg.get('position') or '').lower()
    pnl = (entry_price - exit_price) * quantity if is_sell else (exit_price - entry_price) * quantity

    doc = _base(
        meta['strategy_id'], trade_id, 'force_exit', timestamp,
        meta['strategy_name'], meta['ticker'], leg_id=str(leg.get('id') or ''),
    )
    doc['data'] = {
        'strike':       leg.get('strike'),
        'option_type':  str(leg.get('option') or ''),
        'expiry':       str(leg.get('expiry_date') or ''),
        'position':     str(leg.get('position') or ''),
        'entry_price':  entry_price,
        'exit_price':   exit_price,
        'exit_reason':  exit_reason,
        'pnl':          round(pnl, 2),
    }
    _push_notification(db, doc)


# ═══════════════════════════════════════════════════════════════════════════════
# LEG FEATURE STATUS TRACKER
# ═══════════════════════════════════════════════════════════════════════════════
#
# Collection: algo_leg_feature_status
#
# Tracks the CURRENT status of each feature (SL / Target / TrailSL) per leg.
# This is a live state table — not an event log — so records are UPDATED
# (not appended) when a feature triggers or is disabled.
#
# Lifecycle:
#   entry_taken  →  status=pending  (one record per enabled feature)
#   SL hit       →  sl record: status=triggered
#                   other records (target, trailSL): status=disabled
#   target hit   →  target record: status=triggered
#                   other records (sl, trailSL): status=disabled
#   force_exit / overall_sl / overall_target / lock_and_trail
#                →  all remaining pending records: status=disabled
#
# When re-entry or lazy leg opens and enters, a NEW set of pending records
# is created for the new leg_id.
# ───────────────────────────────────────────────────────────────────────────────

LEG_FEATURE_STATUS_COLLECTION = 'algo_leg_feature_status'


def _lfs_now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _lfs_today() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def _resolve_feature_leg_id(db, trade_id: str, leg_id: str) -> str:
    resolved_trade_id = str(trade_id or '').strip()
    resolved_leg_id = str(leg_id or '').strip()
    if not resolved_trade_id or not resolved_leg_id:
        return resolved_leg_id

    try:
        feature_col = db[LEG_FEATURE_STATUS_COLLECTION]
        existing_feature = feature_col.find_one(
            {'trade_id': resolved_trade_id, 'leg_id': resolved_leg_id},
            {'_id': 1},
        )
        if existing_feature:
            return resolved_leg_id

        history_col = db['algo_trade_positions_history']
        history_doc = history_col.find_one(
            {
                'trade_id': resolved_trade_id,
                '$or': [
                    {'_id': resolved_leg_id},
                    {'leg_id': resolved_leg_id, 'exit_trade': None},
                ],
            },
            {'_id': 1},
        )
        if history_doc and history_doc.get('_id') is not None:
            return str(history_doc.get('_id'))
    except Exception:
        pass
    return resolved_leg_id


def _format_rupee(value) -> str:
    numeric = _safe_float(value)
    return f'₹{round(numeric, 2)}'


def _build_trail_step_reference_text(
    entry_price: float,
    initial_sl_price: float | None,
    current_sl_price: float | None,
    instr_move: float,
    sl_move: float,
    is_sell: bool,
    trail_type: str,
    current_step: int = 0,
    prefix: str = 'Trail SL active',
) -> str:
    entry_value = _safe_float(entry_price)
    base_sl_value = _safe_float(initial_sl_price)
    step_index = max(0, int(current_step or 0)) + 1

    if 'Percentage' in str(trail_type or ''):
        trigger_ltp = (
            entry_value * (1 - (step_index * instr_move / 100.0))
            if is_sell else
            entry_value * (1 + (step_index * instr_move / 100.0))
        )
        sl_step = entry_value * (sl_move / 100.0)
        next_sl_price = (
            base_sl_value - (step_index * sl_step)
            if is_sell else
            base_sl_value + (step_index * sl_step)
        )
    else:
        trigger_ltp = (
            entry_value - (step_index * instr_move)
            if is_sell else
            entry_value + (step_index * instr_move)
        )
        next_sl_price = (
            base_sl_value - (step_index * sl_move)
            if is_sell else
            base_sl_value + (step_index * sl_move)
        )

    if current_sl_price is None or base_sl_value <= 0:
        return (
            f'{prefix}: for every favorable move from entry. '
            f'Example: if LTP reaches {_format_rupee(trigger_ltp)}, SL will move.'
        )

    direction_text = 'falls to' if is_sell else 'rises to'
    return (
        f'{prefix}: if LTP {direction_text} {_format_rupee(trigger_ltp)}, '
        f'SL will move from {_format_rupee(current_sl_price)} to {_format_rupee(next_sl_price)}. '
        f'For every next favorable move of {instr_move} {"%" if "Percentage" in str(trail_type or "") else "points"}, '
        f'SL shifts by {sl_move} {"%" if "Percentage" in str(trail_type or "") else "points"} again.'
    )


def _parse_strike_type_offset(sp_raw) -> tuple[int, str]:
    """Parse 'StrikeType.OTM2' → (+2, 'OTM2'), 'StrikeType.ITM3' → (-3, 'ITM3'), ATM → (0, 'ATM')."""
    import re as _re
    s = str(sp_raw or '')
    m = _re.search(r'OTM(\d+)', s)
    if m:
        n = int(m.group(1))
        return n, f'OTM{n}'
    m = _re.search(r'ITM(\d+)', s)
    if m:
        n = int(m.group(1))
        return -n, f'ITM{n}'
    return 0, 'ATM'


def _fetch_atm_ce_pe_prices(db, underlying: str, expiry: str, atm_strike, entry_ts: str) -> tuple:
    """Query option_chain_historical_data for ATM CE and PE close prices at entry time.

    Query strategy (tries each in order until a doc is found):
      1. Exact timestamp match
      2. $lte ISO timestamp on same calendar date as entry_ts
      3. Date-prefix regex on entry_ts date
      4. $lte expiry date (fallback for backtests where entry_ts is the run-date
         but chain data lives on the historical simulation date)
    """
    try:
        col        = db['option_chain_historical_data']
        strike_val = float(atm_strike)
        trade_date = str(entry_ts)[:10]
        base       = {'underlying': underlying, 'expiry': expiry, 'strike': strike_val}

        def _fetch(option_type: str):
            q = {**base, 'type': option_type}
            # 1. exact match
            doc = col.find_one({**q, 'timestamp': entry_ts})
            if doc:
                return doc
            # 2. $lte ISO timestamp (T-format, same day)
            iso_ts = entry_ts.replace(' ', 'T').rstrip('Z')
            doc = col.find_one(
                {**q, 'timestamp': {'$lte': iso_ts, '$gte': trade_date}},
                sort=[('timestamp', -1)],
            )
            if doc:
                return doc
            # 3. date-prefix regex on entry day
            doc = col.find_one(
                {**q, 'timestamp': {'$regex': f'^{trade_date}'}},
                sort=[('timestamp', -1)],
            )
            if doc:
                return doc
            # 4. any data up to and including expiry date (handles backtest
            #    where entry_ts is today but chain data is on the sim date)
            return col.find_one(
                {**q, 'timestamp': {'$lte': expiry}},
                sort=[('timestamp', -1)],
            )

        ce_doc = _fetch('CE')
        pe_doc = _fetch('PE')
        ce_p   = _safe_float((ce_doc or {}).get('close')) or None
        pe_p   = _safe_float((pe_doc or {}).get('close')) or None
        return ce_p, pe_p
    except Exception:
        return None, None


def _build_leg_entry_description(
    leg: dict,
    leg_cfg: dict,
    entry_trade: dict,
    entry_price: float,
    is_sell: bool,
    underlying: str = '',
    db=None,
    now: str = '',
) -> str:
    """Build a step-by-step trigger_description for the leg_entry audit row."""
    import ast as _ast

    position_str = 'Sell' if is_sell else 'Buy'
    strike       = entry_trade.get('strike') or leg.get('strike') or '?'
    option_type  = str(leg.get('option') or entry_trade.get('option_type') or '').strip()
    spot         = _safe_float(
        entry_trade.get('spot_price')
        or entry_trade.get('underlying_at_trade')
        or entry_trade.get('underlying_trigger_price')
        or leg.get('spot_at_queue')
    )
    expiry       = str(entry_trade.get('expiry') or leg.get('expiry_date') or '')[:10]
    # sl_op: SL moves against position → sell SL goes UP (+), buy SL goes DOWN (−)
    sl_op        = '+' if is_sell else '−'
    # trail_op: favorable move direction → sell price drops (−), buy price rises (+)
    trail_op     = '−' if is_sell else '+'

    lines = [
        f"New leg entered: {position_str} {strike} {option_type} @ {entry_price}"
        f" (spot: {spot}, expiry: {expiry})."
    ]

    # ── Strike selection ──────────────────────────────────────────────────────
    entry_kind = str(leg_cfg.get('EntryType') or leg.get('entry_kind') or '').strip()
    sp_raw     = leg_cfg.get('StrikeParameter') or leg.get('strike_parameter') or ''

    def _parse(raw) -> dict:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str) and '{' in raw:
            try:
                return _ast.literal_eval(raw)
            except Exception:
                pass
        return {}

    # ATM step to approximate ATM strike from spot
    _step_map  = {'NIFTY': 50, 'BANKNIFTY': 100, 'FINNIFTY': 50, 'MIDCPNIFTY': 25, 'SENSEX': 100, 'BANKEX': 100}
    atm_step   = _step_map.get(underlying.upper(), 100)
    atm_strike = int(round(spot / atm_step) * atm_step) if spot else '?'

    if 'PremiumCloseToStraddle' in entry_kind:
        sp         = _parse(sp_raw)
        multiplier = _safe_float(sp.get('Multiplier') or 0.5)
        smeta      = entry_trade.get('strike_meta') or {}
        ce_p       = smeta.get('ce_atm_price')
        pe_p       = smeta.get('pe_atm_price')
        atm_s      = smeta.get('atm_strike') or atm_strike

        # Fallback: query DB when strike_meta wasn't stored in entry_trade
        if (ce_p is None or pe_p is None) and db is not None:
            entry_ts = str(
                entry_trade.get('traded_timestamp')
                or entry_trade.get('trigger_timestamp')
                or now
            )
            _expiry = str(entry_trade.get('expiry') or leg.get('expiry_date') or '')[:10]
            ce_p, pe_p = _fetch_atm_ce_pe_prices(db, underlying, _expiry, atm_s, entry_ts)

        if ce_p is not None and pe_p is not None:
            straddle_v = round(ce_p + pe_p, 2)
            target_v   = round(straddle_v * multiplier, 2)
            lines += [
                f"Strike selection: Straddle % (multiplier: {multiplier}) → closest premium to straddle target.",
                f"  Step 1 → ATM strike from spot {spot} = {atm_s} (nearest {atm_step}-pt step)",
                f"  Step 2 → Straddle = {atm_s} CE @ {ce_p} + {atm_s} PE @ {pe_p} = {straddle_v}",
                f"  Step 3 → Target premium = {straddle_v} × {multiplier} = {target_v}",
                f"  Step 4 → Find strike with premium closest to {target_v} → {strike} {option_type} @ {entry_price}",
            ]
        else:
            lines += [
                f"Strike selection: Straddle % (multiplier: {multiplier}) → closest premium to straddle target.",
                f"  Step 1 → ATM strike from spot {spot} = {atm_s} (nearest {atm_step}-pt step)",
                f"  Step 2 → Straddle = CE close ({atm_s} CE) + PE close ({atm_s} PE)",
                f"  Step 3 → Target premium = Straddle × {multiplier}",
                f"  Step 4 → Find strike with premium closest to target → {strike} {option_type} @ {entry_price}",
            ]

    elif 'AtmMultiplier' in entry_kind:
        multiplier  = _safe_float(sp_raw) if not isinstance(sp_raw, dict) else 1.0
        pct_change  = round((multiplier - 1) * 100, 4)
        pct_display = f'{pct_change:+.4g}%'.replace('+', '+').replace('-', '−')
        adj_sym     = '+' if pct_change >= 0 else '−'
        abs_pct     = abs(pct_change)
        pct_of_atm  = round(abs_pct / 100 * atm_strike, 2)
        raw_strike  = round(atm_strike * multiplier, 2)
        final_str   = int(round(raw_strike / atm_step) * atm_step)
        lines += [
            f"Strike selection: % of ATM ({pct_display}, multiplier: {multiplier}).",
            f"  Step 1 → ATM strike from spot {spot} = {atm_strike} (nearest {atm_step}-pt step)",
            f"  Step 2 → {abs_pct}% of ATM = {abs_pct}/100 × {atm_strike} = {pct_of_atm}",
            f"  Step 3 → Strike = ATM {adj_sym} {pct_of_atm} = {atm_strike} {adj_sym} {pct_of_atm} = {raw_strike}",
            f"           (OR: ATM × multiplier = {atm_strike} × {multiplier} = {raw_strike})",
            f"  Step 4 → Rounded to nearest {atm_step}-pt step → {final_str}",
            f"  Result → {strike} {option_type} entered @ {entry_price}",
        ]

    elif 'StraddlePrice' in entry_kind:
        sp         = _parse(sp_raw)
        multiplier = _safe_float(sp.get('Multiplier') or 0.5)
        adjustment = str(sp.get('Adjustment') or 'AdjustmentType.Plus')
        is_plus    = 'Minus' not in adjustment
        adj_sym    = '+' if is_plus else '−'

        # Get CE/PE prices from meta or DB fallback
        smeta  = entry_trade.get('strike_meta') or {}
        ce_p   = smeta.get('ce_atm_price')
        pe_p   = smeta.get('pe_atm_price')
        if (ce_p is None or pe_p is None) and db is not None:
            entry_ts = str(
                entry_trade.get('traded_timestamp')
                or entry_trade.get('trigger_timestamp')
                or now
            )
            _expiry = str(entry_trade.get('expiry') or leg.get('expiry_date') or '')[:10]
            ce_p, pe_p = _fetch_atm_ce_pe_prices(db, underlying, _expiry, atm_strike, entry_ts)

        if ce_p is not None and pe_p is not None:
            straddle_v  = round(ce_p + pe_p, 2)
            offset_v    = round(multiplier * straddle_v, 2)
            raw_strike  = round(atm_strike + offset_v if is_plus else atm_strike - offset_v, 2)
            final_str   = int(round(raw_strike / atm_step) * atm_step)
            lines += [
                f"Strike selection: Straddle Width (multiplier: {multiplier}, adjustment: {adj_sym}).",
                f"  Step 1 → ATM strike from spot {spot} = {atm_strike} (nearest {atm_step}-pt step)",
                f"  Step 2 → Straddle = {atm_strike} CE @ {ce_p} + {atm_strike} PE @ {pe_p} = {straddle_v}",
                f"  Step 3 → Offset = Straddle × Multiplier = {straddle_v} × {multiplier} = {offset_v}",
                f"  Step 4 → Strike = ATM {adj_sym} Offset = {atm_strike} {adj_sym} {offset_v} = {raw_strike}"
                f"  → rounded to {final_str} (nearest {atm_step}-pt step)",
                f"  Result → {strike} {option_type} entered @ {entry_price}",
            ]
        else:
            lines += [
                f"Strike selection: Straddle Width (multiplier: {multiplier}, adjustment: {adj_sym}).",
                f"  Step 1 → ATM strike from spot {spot} = {atm_strike} (nearest {atm_step}-pt step)",
                f"  Step 2 → Straddle = ATM CE close + ATM PE close",
                f"  Step 3 → Offset = Straddle × {multiplier}",
                f"  Step 4 → Strike = ATM {adj_sym} Offset → rounded to nearest {atm_step}-pt step",
                f"  Result → {strike} {option_type} entered @ {entry_price}",
            ]

    elif 'SyntheticFuture' in entry_kind:
        offset, offset_label = _parse_strike_type_offset(sp_raw)
        # Fetch ATM CE/PE prices to compute synthetic future
        ce_p, pe_p = None, None
        smeta = entry_trade.get('strike_meta') or {}
        ce_p  = smeta.get('ce_atm_price')
        pe_p  = smeta.get('pe_atm_price')
        if (ce_p is None or pe_p is None) and db is not None:
            entry_ts = str(
                entry_trade.get('traded_timestamp')
                or entry_trade.get('trigger_timestamp')
                or now
            )
            _expiry = str(entry_trade.get('expiry') or leg.get('expiry_date') or '')[:10]
            ce_p, pe_p = _fetch_atm_ce_pe_prices(db, underlying, _expiry, atm_strike, entry_ts)

        if ce_p is not None and pe_p is not None:
            syn_future = round(atm_strike - pe_p + ce_p, 2)
            syn_atm    = int(round(syn_future / atm_step) * atm_step)
            # PE: OTM is below syn_atm, ITM is above → reverse offset direction
            _is_pe       = option_type.upper() == 'PE'
            actual_offset = -offset if _is_pe else offset
            if offset == 0:
                final_strike = syn_atm
                step4        = f"  Step 4 → ATM of Synthetic Future {syn_future} = {syn_atm} (nearest {atm_step}-pt step) → Strike {final_strike}"
            else:
                final_strike = syn_atm + actual_offset * atm_step
                _dir         = '+' if actual_offset > 0 else '−'
                step4        = f"  Step 4 → {offset_label} of Synthetic Future: {syn_atm} {_dir} {abs(offset)}×{atm_step} = {final_strike}"
            lines += [
                f"Strike selection: Synthetic Future ({offset_label}).",
                f"  Step 1 → Spot {spot} → ATM strike = {atm_strike} (nearest {atm_step}-pt step)",
                f"  Step 2 → ATM CE @ {atm_strike} = {ce_p}   |   ATM PE @ {atm_strike} = {pe_p}",
                f"  Step 3 → Synthetic Future = {atm_strike} − {pe_p} + {ce_p} = {syn_future}",
                f"           Formula: ATM Strike − ATM PE + ATM CE",
                step4,
                f"  Result → {strike} {option_type} entered @ {entry_price}",
            ]
        else:
            lines += [
                f"Strike selection: Synthetic Future ({offset_label}).",
                f"  Step 1 → Spot {spot} → ATM strike = {atm_strike} (nearest {atm_step}-pt step)",
                f"  Step 2 → ATM CE + ATM PE prices at entry time",
                f"  Step 3 → Synthetic Future = ATM Strike − ATM PE + ATM CE",
                f"  Step 4 → {offset_label} of Synthetic Future → Strike {strike}",
                f"  Result → {strike} {option_type} entered @ {entry_price}",
            ]

    elif 'DeltaRange' in entry_kind:
        sp        = _parse(sp_raw)
        lower_pct = _safe_float(sp.get('LowerRange') or 0)
        upper_pct = _safe_float(sp.get('UpperRange') or 0)
        smeta     = entry_trade.get('strike_meta') or {}
        sel_delta = smeta.get('selected_delta')
        pos_side  = smeta.get('position_side') or ('sell' if is_sell else 'buy')
        pick_rule = 'highest delta (least OTM, closest to ATM)' if pos_side == 'sell' else 'lowest delta (most OTM)'
        lines += [
            f"Strike selection: Delta Range ({lower_pct}% ≤ delta ≤ {upper_pct}%).",
            f"  Range   → {lower_pct}/100 = {lower_pct/100:.2f}  to  {upper_pct}/100 = {upper_pct/100:.2f}",
            f"  Rule    → {'Sell' if pos_side == 'sell' else 'Buy'} position → pick {pick_rule}.",
            f"  Result  → {strike} {option_type} @ {entry_price}"
            + (f"  |  delta = {sel_delta}" if sel_delta is not None else ''),
        ]

    elif 'Delta' in entry_kind:
        target_pct = _safe_float(sp_raw) if not isinstance(sp_raw, dict) else 0.0
        smeta      = entry_trade.get('strike_meta') or {}
        sel_delta  = smeta.get('selected_delta')
        lines += [
            f"Strike selection: Closest Delta (target: {target_pct}).",
            f"  Target  → {target_pct}/100 = {target_pct/100:.2f} delta.",
            f"  Method  → Scan all {option_type} strikes, find one whose delta is nearest to {target_pct/100:.2f}.",
            f"  Result  → {strike} {option_type} @ {entry_price}"
            + (f"  |  delta = {sel_delta}  (closest to {target_pct/100:.2f})" if sel_delta is not None else ''),
        ]

    elif 'PremiumRange' in entry_kind:
        sp    = _parse(sp_raw)
        lower = _safe_float(sp.get('LowerRange') or sp.get('lower') or 0)
        upper = _safe_float(sp.get('UpperRange') or sp.get('upper') or 0)
        mid   = round((lower + upper) / 2, 2) if lower and upper else '?'
        lines += [
            f"Strike selection: Premium Range ({lower} ≤ premium ≤ {upper}).",
            f"  Method → Find all strikes where option close is between {lower} and {upper}.",
            f"  Target  → Pick strike closest to midpoint = ({lower} + {upper}) / 2 = {mid}.",
            f"  Result  → {strike} {option_type} @ {entry_price}  (condition: {lower} ≤ {entry_price} ≤ {upper} ✓)",
        ]

    elif 'Premium' in entry_kind:
        ek_lower   = entry_kind.lower()
        is_geq     = 'geq' in ek_lower
        is_lte     = 'lte' in ek_lower or 'leq' in ek_lower
        is_closest = not is_geq and not is_lte
        target_val = _safe_float(sp_raw) if not isinstance(sp_raw, dict) else 0.0
        if is_closest:
            diff = round(abs(entry_price - target_val), 2)
            lines += [
                f"Strike selection: Closest Premium to {target_val}.",
                f"  Method → Find strike whose premium is nearest to {target_val} (checks both above & below).",
                f"  Result → {strike} {option_type} @ {entry_price}  (diff = {diff} from target {target_val})",
            ]
        else:
            direction = '≥' if is_geq else '≤'
            sort_word = 'closest above' if is_geq else 'closest below'
            lines += [
                f"Strike selection: Premium {direction} {target_val}.",
                f"  Method → Find strike where option close {direction} {target_val} → pick {sort_word} target.",
                f"  Result → {strike} {option_type} @ {entry_price}  (condition: {entry_price} {direction} {target_val} ✓)",
            ]

    else:
        import re as _re2
        _sp_str    = str(sp_raw or '')
        _m_otm     = _re2.search(r'OTM(\d+)', _sp_str)
        _m_itm     = _re2.search(r'ITM(\d+)', _sp_str)
        _raw_off   = int(_m_otm.group(1)) if _m_otm else (-int(_m_itm.group(1)) if _m_itm else 0)
        # PE: OTM is below ATM, ITM is above ATM
        _offset_n  = -_raw_off if option_type.upper() == 'PE' else _raw_off
        _label     = f'OTM{abs(_raw_off)}' if _raw_off > 0 else (f'ITM{abs(_raw_off)}' if _raw_off < 0 else 'ATM')

        if _raw_off == 0:
            lines += [
                f"Strike selection: ATM.",
                f"  Method → Nearest 50-pt rounded strike to spot {spot} = {atm_strike}.",
                f"  Result → {strike} {option_type} | Entry premium = {entry_price}",
            ]
        else:
            _direction = '+' if _offset_n > 0 else '−'
            _abs_n     = abs(_raw_off)
            _computed  = atm_strike + _offset_n * atm_step
            lines += [
                f"Strike selection: {_label}.",
                f"  Method → ATM ({atm_strike}) {_direction} {_abs_n} step × {atm_step} pts = {_computed}.",
                f"  Result → {strike} {option_type} | Entry premium = {entry_price}",
            ]

    # ── SL formula ────────────────────────────────────────────────────────────
    sl_cfg           = leg_cfg.get('LegStopLoss') or {}
    sl_type          = str(sl_cfg.get('Type') or '')
    sl_val           = _safe_float(sl_cfg.get('Value'))
    sl_trigger_price = None
    if 'None' not in sl_type and sl_type and sl_val > 0:
        from features.position_manager import calc_sl_price
        sl_trigger_price = calc_sl_price(entry_price, is_sell, sl_cfg)
        sl_kind          = 'Percentage' if 'Percentage' in sl_type else 'Points'
        rise_fall        = 'rises' if is_sell else 'falls'
        if sl_kind == 'Percentage':
            factor = round(1 + sl_val / 100 if is_sell else 1 - sl_val / 100, 6)
            lines += [
                f"SL @ {sl_trigger_price} ({sl_kind} {sl_val}%)",
                f"  Formula → {entry_price} × {factor}  =  {sl_trigger_price}",
                f"  Trigger → SL fires when price {rise_fall} to {sl_trigger_price}",
            ]
        else:
            lines += [
                f"SL @ {sl_trigger_price} (Points {sl_val})",
                f"  Formula → {entry_price} {sl_op} {sl_val}  =  {sl_trigger_price}",
                f"  Trigger → SL fires when price {rise_fall} to {sl_trigger_price}",
            ]

    # ── Trail SL formula ──────────────────────────────────────────────────────
    trail_cfg  = leg_cfg.get('LegTrailSL') or {}
    trail_type = str(trail_cfg.get('Type') or '')
    trail_val  = trail_cfg.get('Value') or {}
    instr_move = _safe_float(trail_val.get('InstrumentMove') if isinstance(trail_val, dict) else 0)
    sl_move    = _safe_float(trail_val.get('StopLossMove') if isinstance(trail_val, dict) else 0)
    if 'None' not in trail_type and trail_type and instr_move > 0:
        move_word = 'drops' if is_sell else 'rises'
        if is_sell:
            step1_ltp = round(entry_price - instr_move, 2)
            step2_ltp = round(entry_price - 2 * instr_move, 2)
        else:
            step1_ltp = round(entry_price + instr_move, 2)
            step2_ltp = round(entry_price + 2 * instr_move, 2)

        if sl_trigger_price is not None:
            step1_sl = round(sl_trigger_price - sl_move if is_sell else sl_trigger_price + sl_move, 2)
            step2_sl = round(sl_trigger_price - 2 * sl_move if is_sell else sl_trigger_price + 2 * sl_move, 2)
            sl_ref   = str(sl_trigger_price)
        else:
            step1_sl = '?'
            step2_sl = '?'
            sl_ref   = '?'

        lines += [
            f"Trail SL: every {instr_move} pts favorable move → SL shifts {sl_move} pts.",
            f"  How it works → price {move_word} {instr_move} pts from entry → SL moves {sl_move} pts in your favor.",
            f"  Step 1 → LTP {move_word} to {step1_ltp}  ({entry_price} {trail_op} {instr_move})"
            f"  →  SL: {sl_ref} {trail_op} {sl_move} = {step1_sl}",
            f"  Step 2 → LTP {move_word} to {step2_ltp}"
            f"  →  SL: {step1_sl} {trail_op} {sl_move} = {step2_sl}  (repeats every {instr_move} pts)",
        ]

    return '\n'.join(lines)


def record_leg_features_at_entry(
    db,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
    timestamp: str,
    feature_leg_id: str | None = None,
) -> None:
    """
    At entry time: create one status record per ENABLED feature for this leg.

    Features tracked:
      sl      – LegStopLoss (if Type != None and Value > 0)
      target  – LegTarget   (if Type != None and Value > 0)
      trailSL – LegTrailSL  (if Type != None and InstrumentMove > 0)

    Inserts into algo_leg_feature_status with status='pending'.
    Skips features that are disabled (Type=None or value=0).
    Also inserts a leg_entry audit row (status='disabled') with full entry details.
    """
    col = db[LEG_FEATURE_STATUS_COLLECTION]

    meta     = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')
    leg_id   = str(feature_leg_id or leg.get('_id') or leg.get('id') or '')
    today    = _lfs_today()
    now      = timestamp or _lfs_now()

    entry_trade = leg.get('entry_trade') or {}
    entry_price = _safe_float(entry_trade.get('price'))
    is_sell     = 'sell' in str(leg.get('position') or '').lower()

    from features.position_manager import calc_sl_price, calc_tp_price

    docs = []

    # ── SL ────────────────────────────────────────────────────────────────────
    sl_cfg  = leg_cfg.get('LegStopLoss') or {}
    sl_type = str(sl_cfg.get('Type') or '')
    sl_val  = _safe_float(sl_cfg.get('Value'))
    if 'None' not in sl_type and sl_type and sl_val > 0:
        sl_trigger_price = calc_sl_price(entry_price, is_sell, sl_cfg)
        sl_kind = 'Percentage' if 'Percentage' in sl_type else 'Points'
        if is_sell:
            description = (
                f"SL triggers when price rises above ₹{sl_trigger_price} "
                f"({sl_kind}: {sl_val}{'%' if sl_kind == 'Percentage' else ' pts'} from ₹{entry_price})"
            )
        else:
            description = (
                f"SL triggers when price falls below ₹{sl_trigger_price} "
                f"({sl_kind}: {sl_val}{'%' if sl_kind == 'Percentage' else ' pts'} from ₹{entry_price})"
            )
        docs.append({
            'strategy_id':      meta['strategy_id'],
            'strategy_name':    meta['strategy_name'],
            'ticker':           meta['ticker'],
            'trade_id':         trade_id,
            'leg_id':           leg_id,
            'trade_date':       today,
            'feature':          'sl',
            'enabled':          True,
            'status':           'pending',
            'entry_price':      entry_price,
            'trigger_price':    sl_trigger_price,
            'order_limit_price': sl_trigger_price,
            'trigger_type':     sl_kind,
            'trigger_value':    sl_val,
            'trigger_description': description,
            'trail_config':     None,
            'current_sl_price': sl_trigger_price,
            'initial_sl_price': sl_trigger_price,
            'position_side':    'sell' if is_sell else 'buy',
            'created_at':       now,
            'updated_at':       now,
            'triggered_at':     None,
            'triggered_price':  None,
            'disabled_at':      None,
            'disabled_reason':  None,
        })

    # ── Target ────────────────────────────────────────────────────────────────
    tp_cfg  = leg_cfg.get('LegTarget') or {}
    tp_type = str(tp_cfg.get('Type') or '')
    tp_val  = _safe_float(tp_cfg.get('Value'))
    if 'None' not in tp_type and tp_type and tp_val > 0:
        tp_trigger_price = calc_tp_price(entry_price, is_sell, tp_cfg)
        tp_kind = 'Percentage' if 'Percentage' in tp_type else 'Points'
        if is_sell:
            description = (
                f"Target triggers when price falls below ₹{tp_trigger_price} "
                f"({tp_kind}: {tp_val}{'%' if tp_kind == 'Percentage' else ' pts'} from ₹{entry_price})"
            )
        else:
            description = (
                f"Target triggers when price rises above ₹{tp_trigger_price} "
                f"({tp_kind}: {tp_val}{'%' if tp_kind == 'Percentage' else ' pts'} from ₹{entry_price})"
            )
        docs.append({
            'strategy_id':      meta['strategy_id'],
            'strategy_name':    meta['strategy_name'],
            'ticker':           meta['ticker'],
            'trade_id':         trade_id,
            'leg_id':           leg_id,
            'trade_date':       today,
            'feature':          'target',
            'enabled':          True,
            'status':           'pending',
            'entry_price':      entry_price,
            'trigger_price':    tp_trigger_price,
            'trigger_type':     tp_kind,
            'trigger_value':    tp_val,
            'trigger_description': description,
            'trail_config':     None,
            'current_sl_price': None,
            'position_side':    'sell' if is_sell else 'buy',
            'created_at':       now,
            'updated_at':       now,
            'triggered_at':     None,
            'triggered_price':  None,
            'disabled_at':      None,
            'disabled_reason':  None,
        })

    # ── Trail SL ──────────────────────────────────────────────────────────────
    trail_cfg  = leg_cfg.get('LegTrailSL') or {}
    trail_type = str(trail_cfg.get('Type') or '')
    trail_val  = trail_cfg.get('Value') or {}
    instr_move = _safe_float(trail_val.get('InstrumentMove') if isinstance(trail_val, dict) else 0)
    sl_move    = _safe_float(trail_val.get('StopLossMove') if isinstance(trail_val, dict) else 0)
    if 'None' not in trail_type and trail_type and instr_move > 0:
        trail_kind = 'Percentage' if 'Percentage' in trail_type else 'Points'
        current_sl_price = docs[0]['current_sl_price'] if docs else None
        is_sell_position = 'sell' in str(leg.get('position') or '').lower()
        reference_text = _build_trail_step_reference_text(
            entry_price=entry_price,
            initial_sl_price=current_sl_price,
            current_sl_price=current_sl_price,
            instr_move=instr_move,
            sl_move=sl_move,
            is_sell=is_sell_position,
            trail_type=trail_type,
            current_step=0,
        )
        description = (
            f"Trail SL active: every {instr_move} {trail_kind.lower()} favorable move → "
            f"SL shifts {sl_move} {trail_kind.lower()}. "
            f"Initial SL: {_format_rupee(current_sl_price) if current_sl_price is not None else '—'}. "
            f"{reference_text}"
        )
        docs.append({
            'strategy_id':      meta['strategy_id'],
            'strategy_name':    meta['strategy_name'],
            'ticker':           meta['ticker'],
            'trade_id':         trade_id,
            'leg_id':           leg_id,
            'trade_date':       today,
            'feature':          'trailSL',
            'enabled':          True,
            'status':           'pending',
            'entry_price':      entry_price,
            'trigger_price':    None,            # trail SL has no fixed trigger price
            'trigger_type':     trail_kind,
            'trigger_value':    instr_move,
            'trigger_description': description,
            'trail_config':     {
                'type':           trail_type,
                'instrument_move': instr_move,
                'sl_move':         sl_move,
            },
            'current_sl_price': current_sl_price,  # same initial SL as the SL feature
            'initial_sl_price': current_sl_price,
            'position_side':    'sell' if is_sell_position else 'buy',
            'created_at':       now,
            'updated_at':       now,
            'triggered_at':     None,
            'triggered_price':  None,
            'disabled_at':      None,
            'disabled_reason':  None,
        })

    if docs:
        try:
            col.insert_many(docs)
        except Exception as exc:
            log.error('record_leg_features_at_entry error leg=%s: %s', leg_id, exc)

    # ── Leg entry audit row (always inserted, status=disabled) ───────────────
    try:
        entry_description = _build_leg_entry_description(
            leg=leg, leg_cfg=leg_cfg, entry_trade=entry_trade,
            entry_price=entry_price, is_sell=is_sell,
            underlying=str(meta.get('ticker') or ''),
            db=db,
            now=now,
        )
        col.insert_one({
            'strategy_id':         meta['strategy_id'],
            'strategy_name':       meta['strategy_name'],
            'ticker':              meta['ticker'],
            'trade_id':            trade_id,
            'leg_id':              leg_id,
            'trade_date':          today,
            'feature':             'leg_entry',
            'enabled':             False,
            'status':              'disabled',
            'entry_price':         entry_price,
            'trigger_price':       None,
            'trigger_type':        None,
            'trigger_value':       None,
            'trigger_description': entry_description,
            'trail_config':        None,
            'current_sl_price':    None,
            'initial_sl_price':    None,
            'position_side':       'sell' if is_sell else 'buy',
            'created_at':          now,
            'updated_at':          now,
            'triggered_at':        None,
            'triggered_price':     None,
            'disabled_at':         now,
            'disabled_reason':     'entry_audit',
        })
    except Exception as exc:
        log.error('leg_entry audit row error leg=%s: %s', leg_id, exc)


def refresh_leg_feature_status_at_fill(
    db,
    trade_id: str,
    leg_id: str,
    fill_price: float,
    leg_cfg: dict,
    is_sell: bool,
    sl_price: float,
    tp_price: float,
    now: str,
) -> None:
    """
    Called after broker entry fill is confirmed (postback).
    Rebuilds trigger_price, current_sl_price, initial_sl_price AND trigger_description
    for sl / target / trailSL / leg_entry feature rows using the actual fill price.
    Only touches active/pending rows (skips triggered/disabled).
    """
    from features.position_manager import calc_sl_price, calc_tp_price  # type: ignore

    col   = db[LEG_FEATURE_STATUS_COLLECTION]
    query = {
        'trade_id': trade_id,
        'leg_id':   leg_id,
        'status':   {'$nin': ['triggered', 'disabled', 'completed', 'cancelled']},
    }

    sl_cfg   = leg_cfg.get('LegStopLoss') or {}
    sl_type  = str(sl_cfg.get('Type') or '')
    sl_val   = _safe_float(sl_cfg.get('Value'))
    tp_cfg   = leg_cfg.get('LegTarget') or {}
    tp_type  = str(tp_cfg.get('Type') or '')
    tp_val   = _safe_float(tp_cfg.get('Value'))
    trail_cfg  = leg_cfg.get('LegTrailSL') or {}
    trail_type = str(trail_cfg.get('Type') or '')
    trail_val  = trail_cfg.get('Value') or {}
    instr_move = _safe_float(trail_val.get('InstrumentMove') if isinstance(trail_val, dict) else 0)
    sl_move    = _safe_float(trail_val.get('StopLossMove') if isinstance(trail_val, dict) else 0)

    # ── SL ────────────────────────────────────────────────────────────────────
    if sl_price > 0 and 'None' not in sl_type and sl_val > 0:
        sl_kind = 'Percentage' if 'Percentage' in sl_type else 'Points'
        if is_sell:
            sl_desc = (
                f"SL triggers when price rises above ₹{sl_price} "
                f"({sl_kind}: {sl_val}{'%' if sl_kind == 'Percentage' else ' pts'} from ₹{fill_price})"
            )
        else:
            sl_desc = (
                f"SL triggers when price falls below ₹{sl_price} "
                f"({sl_kind}: {sl_val}{'%' if sl_kind == 'Percentage' else ' pts'} from ₹{fill_price})"
            )
        try:
            col.update_many(
                {**query, 'feature': 'sl'},
                {'$set': {
                    'entry_price':         fill_price,
                    'trigger_price':       sl_price,
                    'order_limit_price':   sl_price,
                    'current_sl_price':    sl_price,
                    'initial_sl_price':    sl_price,
                    'trigger_description': sl_desc,
                    'updated_at':          now,
                }},
            )
        except Exception as _e:
            log.warning('refresh_feature sl error leg=%s: %s', leg_id, _e)

    # ── Target ────────────────────────────────────────────────────────────────
    if tp_price > 0 and 'None' not in tp_type and tp_val > 0:
        tp_kind = 'Percentage' if 'Percentage' in tp_type else 'Points'
        if is_sell:
            tp_desc = (
                f"Target triggers when price falls below ₹{tp_price} "
                f"({tp_kind}: {tp_val}{'%' if tp_kind == 'Percentage' else ' pts'} from ₹{fill_price})"
            )
        else:
            tp_desc = (
                f"Target triggers when price rises above ₹{tp_price} "
                f"({tp_kind}: {tp_val}{'%' if tp_kind == 'Percentage' else ' pts'} from ₹{fill_price})"
            )
        try:
            col.update_many(
                {**query, 'feature': 'target'},
                {'$set': {
                    'entry_price':         fill_price,
                    'trigger_price':       tp_price,
                    'trigger_description': tp_desc,
                    'updated_at':          now,
                }},
            )
        except Exception as _e:
            log.warning('refresh_feature target error leg=%s: %s', leg_id, _e)

    # ── Trail SL ──────────────────────────────────────────────────────────────
    if sl_price > 0 and 'None' not in trail_type and instr_move > 0:
        trail_kind = 'Percentage' if 'Percentage' in trail_type else 'Points'
        reference_text = _build_trail_step_reference_text(
            entry_price=fill_price,
            initial_sl_price=sl_price,
            current_sl_price=sl_price,
            instr_move=instr_move,
            sl_move=sl_move,
            is_sell=is_sell,
            trail_type=trail_type,
            current_step=0,
        )
        trail_desc = (
            f"Trail SL active: every {instr_move} {trail_kind.lower()} favorable move → "
            f"SL shifts {sl_move} {trail_kind.lower()}. "
            f"Initial SL: {_format_rupee(sl_price)}. "
            f"{reference_text}"
        )
        try:
            col.update_many(
                {**query, 'feature': {'$in': ['trailSL', 'trail_sl', 'trailing_sl']}},
                {'$set': {
                    'entry_price':         fill_price,
                    'current_sl_price':    sl_price,
                    'initial_sl_price':    sl_price,
                    'trigger_price':       sl_price,
                    'trigger_description': trail_desc,
                    'updated_at':          now,
                }},
            )
        except Exception as _e:
            log.warning('refresh_feature trailSL error leg=%s: %s', leg_id, _e)

    # ── leg_entry audit row ───────────────────────────────────────────────────
    try:
        col.update_many(
            {'trade_id': trade_id, 'leg_id': leg_id, 'feature': 'leg_entry'},
            {'$set': {'entry_price': fill_price, 'updated_at': now}},
        )
    except Exception as _e:
        log.warning('refresh_feature leg_entry error leg=%s: %s', leg_id, _e)

    print(
        f'[FEATURE STATUS REFRESHED] trade={trade_id} leg={leg_id} '
        f'fill={fill_price} sl={sl_price} tp={tp_price}'
    )


def trigger_leg_feature(
    db,
    trade_id: str,
    leg_id: str,
    feature: str,
    triggered_price: float,
    timestamp: str,
) -> None:
    """
    Mark one feature (sl | target | trailSL) as triggered for this leg.
    Called immediately when SL/Target fires.
    """
    col = db[LEG_FEATURE_STATUS_COLLECTION]
    now = timestamp or _lfs_now()
    feature_leg_id = _resolve_feature_leg_id(db, trade_id, leg_id)
    try:
        col.update_one(
            {'trade_id': trade_id, 'leg_id': feature_leg_id, 'feature': feature, 'status': 'pending'},
            {'$set': {
                'status':          'triggered',
                'triggered_at':    now,
                'triggered_price': round(triggered_price, 2),
                'updated_at':      now,
            }},
        )
    except Exception as exc:
        log.error('trigger_leg_feature error leg=%s feature=%s: %s', feature_leg_id, feature, exc)


def disable_leg_features(
    db,
    trade_id: str,
    leg_id: str,
    except_feature: str | None = None,
    reason: str = 'leg_closed',
    timestamp: str | None = None,
) -> None:
    """
    Disable all PENDING feature records for this leg, except the one that triggered.

    Called immediately after SL/Target/force_exit fires to mark remaining
    features as irrelevant (leg is now closed).

    Parameters
    ----------
    except_feature : feature name to skip (already marked as 'triggered')
    reason         : 'sl_triggered' | 'target_triggered' | 'force_exit' |
                     'overall_sl' | 'overall_target' | 'lock_and_trail'
    """
    col = db[LEG_FEATURE_STATUS_COLLECTION]
    now = timestamp or _lfs_now()
    feature_leg_id = _resolve_feature_leg_id(db, trade_id, leg_id)
    query: dict = {
        'trade_id': trade_id,
        'leg_id':   feature_leg_id,
        'status':   'pending',
    }
    if except_feature:
        query['feature'] = {'$ne': except_feature}
    try:
        col.update_many(
            query,
            {'$set': {
                'status':          'disabled',
                'enabled':         False,
                'disabled_at':     now,
                'disabled_reason': reason,
                'updated_at':      now,
            }},
        )
    except Exception as exc:
        log.error('disable_leg_features error leg=%s reason=%s: %s', feature_leg_id, reason, exc)


def rotate_trail_sl_record(
    db,
    trade_id: str,
    leg_id: str,
    old_sl_price: float,
    new_sl_price: float,
    current_option_price: float,
    trail_config: dict,
    timestamp: str,
) -> None:
    """
    Called every time Trail SL moves a step (instrument moved X pts → SL shifts Y pts).

    Instead of updating the existing records in place, we:
      1. Disable the current pending SL + trailSL records
         (reason: 'trail_sl_moved', capturing the old_sl_price)
      2. Insert NEW pending SL + trailSL records with the updated trigger price

    This creates a step-by-step audit trail in algo_leg_feature_status so that
    every trail SL movement is individually verifiable — critical for real-money trading.

    Example audit trail for a leg with TrailSL(instrument_move=10, sl_move=5):
      Step 0 (entry):      SL=₹185 pending, trailSL pending
      Step 1 (moved +10): SL=₹185 disabled, trailSL disabled
                           SL=₹180 pending (step=1), trailSL pending (step=1)
      Step 2 (moved +10): SL=₹180 disabled, trailSL disabled
                           SL=₹175 pending (step=2), trailSL pending (step=2)
      SL hit @ ₹175:       SL=₹175 triggered, trailSL disabled
    """
    col = db[LEG_FEATURE_STATUS_COLLECTION]
    now = timestamp or _lfs_now()
    feature_leg_id = _resolve_feature_leg_id(db, trade_id, leg_id)

    # ── 1. Fetch the current pending SL record to copy meta fields ────────────
    current_sl_rec = col.find_one(
        {'trade_id': trade_id, 'leg_id': feature_leg_id, 'feature': 'sl', 'status': 'pending'}
    )
    current_trail_rec = col.find_one(
        {'trade_id': trade_id, 'leg_id': feature_leg_id, 'feature': 'trailSL', 'status': 'pending'}
    )

    if not current_sl_rec and not current_trail_rec:
        # No pending records to rotate (leg may already be closed)
        return

    # How many trail steps have happened already?
    step = col.count_documents({
        'trade_id':        trade_id,
        'leg_id':          feature_leg_id,
        'feature':         'sl',
        'disabled_reason': 'trail_sl_moved',
    }) + 1  # this rotation becomes step N

    # ── 2. Disable current pending SL + trailSL records ──────────────────────
    try:
        col.update_many(
            {
                'trade_id': trade_id,
                'leg_id':   feature_leg_id,
                'feature':  {'$in': ['sl', 'trailSL']},
                'status':   'pending',
            },
            {'$set': {
                'status':           'disabled',
                'enabled':          False,
                'disabled_at':      now,
                'disabled_reason':  'trail_sl_moved',
                'old_sl_price':     round(old_sl_price, 2),   # capture before rotation
                'updated_at':       now,
            }},
        )
    except Exception as exc:
        log.error('rotate_trail_sl_record disable error leg=%s: %s', feature_leg_id, exc)
        return

    # ── 3. Build new pending records with updated SL price ────────────────────
    # Carry forward meta from the old record (or fallback to empty strings)
    base = current_sl_rec or current_trail_rec or {}
    strategy_id   = base.get('strategy_id', '')
    strategy_name = base.get('strategy_name', '')
    ticker        = base.get('ticker', '')
    entry_price   = base.get('entry_price', 0.0)
    trade_date    = base.get('trade_date', _lfs_today())
    trigger_type  = base.get('trigger_type', 'Points')
    trigger_value = base.get('trigger_value', 0.0)
    initial_sl_price = _safe_float(base.get('initial_sl_price') or old_sl_price)
    position_side = str(base.get('position_side') or '').lower()
    if position_side in {'sell', 'buy'}:
        is_sell = position_side == 'sell'
    else:
        existing_desc = str((current_sl_rec or current_trail_rec or {}).get('trigger_description') or '').lower()
        if 'rises above' in existing_desc or 'falls to' in existing_desc:
            is_sell = True
        elif 'falls below' in existing_desc or 'rises to' in existing_desc:
            is_sell = False
        else:
            is_sell = new_sl_price > entry_price if entry_price else True

    trail_val      = trail_config.get('Value') or {}
    if not isinstance(trail_val, dict):
        trail_val = {}
    instr_move = _safe_float(trail_val.get('InstrumentMove'))
    sl_move    = _safe_float(trail_val.get('StopLossMove'))
    trail_type = str(trail_config.get('Type') or '')
    trail_kind = 'Percentage' if 'Percentage' in trail_type else 'Points'

    new_docs = []

    # New SL record
    if is_sell:
        sl_description = (
            f"[Trail step {step}] SL triggers when price rises above ₹{new_sl_price} "
            f"(moved from ₹{old_sl_price} after {instr_move}-pt instrument move)"
        )
    else:
        sl_description = (
            f"[Trail step {step}] SL triggers when price falls below ₹{new_sl_price} "
            f"(moved from ₹{old_sl_price} after {instr_move}-pt instrument move)"
        )
    new_docs.append({
        'strategy_id':      strategy_id,
        'strategy_name':    strategy_name,
        'ticker':           ticker,
        'trade_id':         trade_id,
        'leg_id':           feature_leg_id,
        'trade_date':       trade_date,
        'feature':          'sl',
        'enabled':          True,
        'status':           'pending',
        'trail_step':       step,
        'entry_price':      entry_price,
        'trigger_price':    round(new_sl_price, 2),
        'order_limit_price': round(new_sl_price, 2),
        'previous_sl_price': round(old_sl_price, 2),
        'trigger_type':     trigger_type,
        'trigger_value':    trigger_value,
        'trigger_description': sl_description,
        'trail_config':     None,
        'current_sl_price': round(new_sl_price, 2),
        'initial_sl_price': round(initial_sl_price, 2),
        'position_side':    'sell' if is_sell else 'buy',
        'current_option_price': round(current_option_price, 2),
        'created_at':       now,
        'updated_at':       now,
        'triggered_at':     None,
        'triggered_price':  None,
        'disabled_at':      None,
        'disabled_reason':  None,
        'old_sl_price':     None,
    })

    # New trailSL record
    trail_description = (
        f"[Trail step {step}] Trail SL active: every {instr_move} {trail_kind.lower()} "
        f"favorable move → SL shifts {sl_move} {trail_kind.lower()}. "
        f"Current SL: {_format_rupee(new_sl_price)} (prev: {_format_rupee(old_sl_price)}). "
        f"Current option price: {_format_rupee(current_option_price)}. "
        f"{_build_trail_step_reference_text(entry_price, initial_sl_price, new_sl_price, instr_move, sl_move, is_sell, trail_type, step, prefix='Next trail step')}"
    )
    new_docs.append({
        'strategy_id':      strategy_id,
        'strategy_name':    strategy_name,
        'ticker':           ticker,
        'trade_id':         trade_id,
        'leg_id':           feature_leg_id,
        'trade_date':       trade_date,
        'feature':          'trailSL',
        'enabled':          True,
        'status':           'pending',
        'trail_step':       step,
        'entry_price':      entry_price,
        'trigger_price':    None,
        'order_limit_price': round(new_sl_price, 2),
        'previous_sl_price': round(old_sl_price, 2),
        'trigger_type':     trail_kind,
        'trigger_value':    instr_move,
        'trigger_description': trail_description,
        'trail_config':     {
            'type':            trail_type,
            'instrument_move': instr_move,
            'sl_move':         sl_move,
        },
        'current_sl_price': round(new_sl_price, 2),
        'initial_sl_price': round(initial_sl_price, 2),
        'position_side':    'sell' if is_sell else 'buy',
        'current_option_price': round(current_option_price, 2),
        'created_at':       now,
        'updated_at':       now,
        'triggered_at':     None,
        'triggered_price':  None,
        'disabled_at':      None,
        'disabled_reason':  None,
        'old_sl_price':     None,
    })

    try:
        col.insert_many(new_docs)
        log.info(
            'Trail SL rotated leg=%s step=%d: ₹%s → ₹%s',
            feature_leg_id, step, old_sl_price, new_sl_price,
        )
    except Exception as exc:
        log.error('rotate_trail_sl_record insert error leg=%s: %s', feature_leg_id, exc)


def upsert_broker_feature_status(
    db,
    *,
    trade: dict,
    user_id: str,
    broker: str,
    activation_mode: str,
    feature: str,
    trigger_value: float,
    current_mtm: float,
    timestamp: str,
    status: str = 'triggered',
) -> None:
    """
    Store broker-level SL/Target tracking rows in algo_leg_feature_status.
    Broker-level events are audit events, so we always insert a fresh row
    instead of updating an older broker/day record in place.
    """
    col = db[LEG_FEATURE_STATUS_COLLECTION]
    now = timestamp or _lfs_now()
    trade_date = _trade_date_from_ts(now) or _lfs_today()
    resolved_trade = trade or {}
    resolved_feature = str(feature or '').strip() or 'broker_event'
    broker_id = str(broker or '').strip()
    broker_leg_id = f'__broker__:{broker_id}'

    try:
        payload = {
            'strategy_id': str(
                resolved_trade.get('strategy_id')
                or ((resolved_trade.get('strategy') or {}).get('_id') or '')
            ),
            'strategy_name': str(
                resolved_trade.get('name')
                or resolved_trade.get('strategy_name')
                or ((resolved_trade.get('strategy') or {}).get('name') or '')
            ),
            'ticker': str(
                resolved_trade.get('ticker')
                or ((resolved_trade.get('config') or {}).get('Ticker') or '')
                or ((resolved_trade.get('strategy') or {}).get('Ticker') or '')
            ),
            'trade_id': str(resolved_trade.get('_id') or ''),
            'trade_date': trade_date,
            'leg_id': broker_leg_id,
            'feature': resolved_feature,
            'enabled': False,
            'status': status,
            'entry_price': None,
            'trigger_price': round(_safe_float(trigger_value), 2),
            'trigger_type': 'MTM',
            'trigger_value': round(_safe_float(trigger_value), 2),
            'trigger_description': (
                f'Broker-level {resolved_feature} triggered at MTM '
                f'{round(_safe_float(current_mtm), 2)}.'
            ),
            'trail_config': None,
            'current_sl_price': None,
            'initial_sl_price': None,
            'position_side': 'broker',
            'created_at': now,
            'updated_at': now,
            'triggered_at': now,
            'triggered_price': round(_safe_float(current_mtm), 2),
            'disabled_at': now,
            'disabled_reason': resolved_feature,
            'user_id': str(user_id or '').strip(),
            'broker': broker_id,
            'activation_mode': str(activation_mode or '').strip(),
            'current_mtm': round(_safe_float(current_mtm), 2),
            'source_event': 'broker_level_trigger',
        }
        result = col.insert_one(payload)
        print('[BROKER FEATURE STATUS INSERTED]', {
            'id': str(result.inserted_id),
            'broker': broker_id,
            'feature': resolved_feature,
            'trade_id': payload['trade_id'],
            'strategy_name': payload['strategy_name'],
            'timestamp': now,
        })
    except Exception as exc:
        log.error('upsert_broker_feature_status error broker=%s feature=%s: %s', broker_id, resolved_feature, exc)


def upsert_recost_feature_status(
    db,
    trade: dict,
    leg: dict,
    timestamp: str,
    cost_price: float,
    exit_price: float,
    status: str = 'pending',
    recost_leg_id: str | None = None,
) -> None:
    """
    Track AtCost (RE-COST) reentry status in algo_leg_feature_status.

    status='pending'    → SL/Target hit; waiting for price to return to cost_price
    status='triggered'  → Price returned to cost_price; re-entry entry taken
    status='disabled'   → Cancelled (e.g. trade exited before price returned)

    Key: (trade_id, recost_leg_id, feature='reCost')
    recost_leg_id is the inline pending reentry leg's own id (e.g. "0nmwoa90_re_2025-11-03094000"),
    so _attach_leg_feature_statuses can attach this row to the correct inline leg object.
    """
    meta     = _trade_meta(trade)
    trade_id = str(trade.get('_id') or '')
    leg_id   = str(recost_leg_id or leg.get('id') or '')

    col = db[LEG_FEATURE_STATUS_COLLECTION]
    now = timestamp or _lfs_now()

    position    = str(leg.get('position') or '')
    is_sell     = 'sell' in position.lower()
    option_type = str(leg.get('option') or '')
    strike      = leg.get('strike')

    if status == 'pending':
        direction = 'falls back to' if is_sell else 'rises back to'
        description = (
            f"RE-COST pending: {strike} {option_type} exited @ \u20b9{round(_safe_float(exit_price), 2)}. "
            f"Waiting for price to {direction} \u20b9{round(_safe_float(cost_price), 2)} "
            f"(original entry cost) to re-enter same strike."
        )
    elif status == 'triggered':
        description = (
            f"RE-COST triggered: price returned to \u20b9{round(_safe_float(cost_price), 2)} "
            f"for {strike} {option_type}. Re-entry taken."
        )
    else:  # disabled
        description = (
            f"RE-COST disabled for {strike} {option_type}. "
            f"Waiting cost \u20b9{round(_safe_float(cost_price), 2)} not reached."
        )

    try:
        col.update_one(
            {'trade_id': trade_id, 'leg_id': leg_id, 'feature': 'reCost'},
            {'$set': {
                'strategy_id':    meta['strategy_id'],
                'strategy_name':  meta['strategy_name'],
                'ticker':         meta['ticker'],
                'trade_date':     _trade_date_from_ts(now),
                'feature':        'reCost',
                'enabled':        status == 'pending',
                'status':         status,
                'entry_price':    round(_safe_float(cost_price), 2),
                'trigger_price':  round(_safe_float(cost_price), 2),
                'trigger_type':   'AtCost',
                'trigger_value':  round(_safe_float(cost_price), 2),
                'trigger_description': description,
                'exit_price':     round(_safe_float(exit_price), 2),
                'strike':         strike,
                'option_type':    option_type,
                'position':       position,
                'updated_at':     now,
                'triggered_at':   now if status == 'triggered' else None,
                'triggered_price': round(_safe_float(cost_price), 2) if status == 'triggered' else None,
                'disabled_at':    now if status == 'disabled' else None,
                'disabled_reason': 'cost_not_reached' if status == 'disabled' else None,
            }, '$setOnInsert': {
                'trade_id':       trade_id,
                'leg_id':         leg_id,
                'created_at':     now,
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error('upsert_recost_feature_status error leg=%s: %s', leg_id, exc)


def disable_all_trade_notifications(
    db,
    trade_id: str,
    reason: str = 'overall_exit',
    timestamp: str | None = None,
) -> None:
    """
    Disable ALL active/pending algo_leg_feature_status records for a trade.

    Called when overall SL or overall target is hit so that every open
    notification (simpleMomentum, sl, target, trailSL, momentum_pending, etc.)
    is marked disabled before the new cycle begins or the trade ends.

    Unlike _disable_trade_feature_rows_for_new_cycle (which only targets
    enabled=True records), this function disables any record whose status
    is still 'pending', 'active', or 'armed' regardless of the enabled flag.
    """
    col = db[LEG_FEATURE_STATUS_COLLECTION]
    now = timestamp or _lfs_now()
    normalized_trade_id = str(trade_id or '').strip()
    if not normalized_trade_id:
        return
    try:
        col.update_many(
            {
                'trade_id': normalized_trade_id,
                'status': {'$in': ['pending', 'active', 'armed']},
            },
            {'$set': {
                'status':          'disabled',
                'enabled':         False,
                'disabled_at':     now,
                'disabled_reason': str(reason or 'overall_exit'),
                'updated_at':      now,
            }},
        )
    except Exception as exc:
        log.error('disable_all_trade_notifications error trade=%s: %s', normalized_trade_id, exc)
