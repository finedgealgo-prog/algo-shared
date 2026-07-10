"""
simulator_adjustment_tracker.py
────────────────────────────────
Tracks orders placed by the SimulatorRiskMonitor's upper/lower adjustment
fire path — completely separate from features/live_order_manager.py's
broker_orders collection (that one is algo_trades strategy builder only).

Collection: simulator_adjustment_orders
  One doc per leg placed in a single adjustment fire.
  Keyed by (adjustment_doc_id, order_id) — never by trade_id/leg_id
  which don't exist in the simulator path.

Public API (used by simulator_risk_monitor.py only):
  save_adjustment_order_results(db, adjustment_doc_id, broker_id, underlying,
      trigger_condition, order_results, positions)
      → saves placed orders to collection, returns count saved

  poll_and_update(db, broker_id) -> list[tuple[str, str, str]]
      → fetches all OPEN orders for this broker_id, calls broker.orders(),
        updates statuses, returns list of (adjustment_doc_id, broker_id,
        trigger_condition) groups where ALL orders are now terminal
        (ready for breach-clear).

  get_orders_for_adjustment(db, adjustment_doc_id) -> list[dict]
      → read-only query for the API endpoint / UI display.

  delete_for_adjustment(db, adjustment_doc_id)
      → cleanup after breach is cleared.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

log = logging.getLogger(__name__)

_COL = 'simulator_adjustment_orders'
_IST = timezone(timedelta(hours=5, minutes=30))
_TERMINAL = {'COMPLETE', 'REJECTED', 'CANCELLED', 'ERROR'}


def _now() -> str:
    return datetime.now(_IST).strftime('%Y-%m-%dT%H:%M:%S')


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


# ── Broker adapter resolution ─────────────────────────────────────────────────

def _resolve_broker_adapter(db, broker_id: str):
    """
    Returns (adapter, broker_type_str) for the given broker_id.
    Mirrors the broker-selection logic in api.py::simulator_place_manual_order
    exactly — Dhan via kite_market_config, FlatTrade/Kite via broker_configuration.
    Returns (None, '') if the broker_id isn't recognised or credentials missing.
    """
    raw_db = db._db
    # ── Dhan ──────────────────────────────────────────────────────────────────
    dhan_cfg = raw_db['kite_market_config'].find_one({'broker': 'dhan'}) or {}
    if broker_id and broker_id == str(dhan_cfg.get('_id') or '').strip():
        client_id = str(dhan_cfg.get('user_id') or dhan_cfg.get('dhan_client_id') or '').strip()
        token = str(dhan_cfg.get('access_token') or '').strip()
        if not client_id or not token:
            return None, ''
        try:
            from features.dhan_broker import get_dhan_instance
            adapter = get_dhan_instance(db, client_id, token)
            return adapter, 'dhan'
        except Exception as exc:
            log.debug('[ADJ TRACKER] dhan adapter error: %s', exc)
            return None, ''

    # ── FlatTrade / Kite ──────────────────────────────────────────────────────
    try:
        from bson import ObjectId
        doc = raw_db['broker_configuration'].find_one({'_id': ObjectId(broker_id)})
    except Exception:
        return None, ''
    if not doc:
        return None, ''
    broker_name = str(doc.get('broker_name') or doc.get('name') or '').strip().lower()
    if 'flattrade' in broker_name:
        try:
            from features.flattrade_broker import get_flattrade_instance
            adapter = get_flattrade_instance(str(doc.get('user_id') or ''), str(doc.get('access_token') or ''))
            return adapter, 'flattrade'
        except Exception as exc:
            log.debug('[ADJ TRACKER] flattrade adapter error: %s', exc)
            return None, ''
    if 'zerodha' in broker_name or 'kite' in broker_name:
        try:
            from features.kite_broker import get_kite_instance
            adapter = get_kite_instance(str(doc.get('_id') or ''), db)
            return adapter, 'kite'
        except Exception as exc:
            log.debug('[ADJ TRACKER] kite adapter error: %s', exc)
            return None, ''
    return None, ''


_DHAN_STATUS_MAP = {
    'TRADED': 'COMPLETE', 'REJECTED': 'REJECTED', 'CANCELLED': 'CANCELLED',
    'PENDING': 'OPEN', 'TRANSIT': 'OPEN', 'OPEN': 'OPEN',
}


def _normalise_order_status(raw_status: str, broker_type: str) -> str:
    """Map broker-specific status strings to our canonical set: OPEN / COMPLETE / REJECTED / CANCELLED."""
    s = str(raw_status or '').strip().upper()
    if broker_type == 'dhan':
        return _DHAN_STATUS_MAP.get(s, 'OPEN')
    # FlatTrade and Kite already return Kite-shaped statuses
    if s in ('COMPLETE', 'FILLED'):
        return 'COMPLETE'
    if s in ('REJECTED',):
        return 'REJECTED'
    if s in ('CANCELLED', 'CANCELED'):
        return 'CANCELLED'
    return 'OPEN'


# ── Public API ────────────────────────────────────────────────────────────────

def save_adjustment_order_results(
    db,
    adjustment_doc_id: str,
    broker_id: str,
    underlying: str,
    trigger_condition: str,
    order_results: list[dict],   # per-leg dicts from simulator_place_manual_order
    positions: list[dict],       # original adjustment positions (for metadata)
) -> int:
    """
    Insert one simulator_adjustment_orders doc per successfully placed leg.
    order_results[i] corresponds to positions[i] (same index order as
    simulator_place_manual_order's asyncio.gather output).
    Returns count of rows saved.
    """
    now = _now()
    docs = []
    for result, pos in zip(order_results, positions):
        if result.get('status') != 'success':
            continue
        order_id = str(result.get('order_id') or '').strip()
        if not order_id:
            continue
        leg_meta = result.get('leg') or {}
        docs.append({
            'adjustment_doc_id': adjustment_doc_id,
            'broker_id': broker_id,
            'underlying': underlying,
            'trigger_condition': trigger_condition,
            'order_id': order_id,
            'symbol': str(leg_meta.get('symbol') or pos.get('symbol') or ''),
            'option_type': str(pos.get('option_type') or '').upper(),
            'strike': _safe_float(pos.get('strike')),
            'expiry': str(pos.get('expiry') or ''),
            'order_side': str(leg_meta.get('side') or pos.get('side') or '').upper(),
            'tag': str(pos.get('tag') or 'EXIT').upper(),
            'quantity': _safe_int(pos.get('qty') or pos.get('lots') or leg_meta.get('quantity') or 0),
            'price': _safe_float(leg_meta.get('price') or pos.get('entry_price')),
            'status': 'OPEN',
            'fill_price': 0.0,
            'fill_qty': 0,
            'rejection_reason': '',
            'placed_at': now,
            'updated_at': now,
        })
    if docs:
        try:
            db._db[_COL].insert_many(docs)
        except Exception as exc:
            log.error('[ADJ TRACKER] save_adjustment_order_results error: %s', exc)
    return len(docs)


def get_orders_for_adjustment(db, adjustment_doc_id: str) -> list[dict]:
    """Read-only — for the API endpoint and UI display."""
    docs = list(db._db[_COL].find({'adjustment_doc_id': adjustment_doc_id}).sort('placed_at', -1))
    for d in docs:
        d['_id'] = str(d['_id'])
    return docs


def delete_for_adjustment(db, adjustment_doc_id: str) -> None:
    """Called after breach-clear completes — remove all tracking rows for this adjustment."""
    try:
        db._db[_COL].delete_many({'adjustment_doc_id': adjustment_doc_id})
    except Exception as exc:
        log.debug('[ADJ TRACKER] delete_for_adjustment error adj=%s: %s', adjustment_doc_id, exc)


def poll_and_update(db, broker_id: str) -> list[tuple[str, str, str]]:
    """
    1. Find all OPEN simulator_adjustment_orders for this broker_id.
    2. Call broker.orders() once — match each record by order_id.
    3. Bulk-update statuses in DB.
    4. Return list of (adjustment_doc_id, broker_id, trigger_condition) groups
       where every order is now terminal → caller should clear the breach.
    Returns [] if broker adapter is unavailable or no OPEN orders exist.
    """
    raw_db = db._db
    open_records = list(raw_db[_COL].find({'broker_id': broker_id, 'status': 'OPEN'}))
    if not open_records:
        return []

    adapter, broker_type = _resolve_broker_adapter(db, broker_id)
    if adapter is None:
        log.debug('[ADJ TRACKER] poll_and_update: no adapter for broker=%s', broker_id)
        return []

    # Fetch ALL orders from broker once — avoids one API call per order_id
    try:
        all_broker_orders: list[dict] = adapter.orders() or []
    except Exception as exc:
        log.warning('[ADJ TRACKER] broker.orders() error broker=%s: %s', broker_id, exc)
        return []

    # Build lookup: order_id -> broker order dict
    broker_order_by_id: dict[str, dict] = {}
    for bo in all_broker_orders:
        oid = str(bo.get('order_id') or '').strip()
        if oid:
            broker_order_by_id[oid] = bo

    now = _now()
    # Group records by adjustment fire: (adjustment_doc_id, trigger_condition)
    groups: dict[tuple[str, str], list[dict]] = {}
    updated_records: list[dict] = []

    for rec in open_records:
        oid = str(rec.get('order_id') or '').strip()
        bo = broker_order_by_id.get(oid)
        if bo is None:
            # Not in broker's order list yet — still OPEN
            updated_records.append({**rec, 'status': 'OPEN'})
        else:
            raw_s = str(bo.get('status') or bo.get('order_status') or '').strip()
            norm_s = _normalise_order_status(raw_s, broker_type)
            fill_price = _safe_float(bo.get('fill_price') or bo.get('average_price') or bo.get('avgprc') or 0)
            fill_qty = _safe_int(bo.get('fill_qty') or bo.get('filled_quantity') or bo.get('fillshares') or 0)
            rejection_reason = str(bo.get('rejection_reason') or bo.get('remarks') or '') if norm_s in ('REJECTED', 'CANCELLED') else ''
            updated_records.append({
                **rec,
                'status': norm_s,
                'fill_price': fill_price,
                'fill_qty': fill_qty,
                'rejection_reason': rejection_reason,
                'updated_at': now,
            })

        grp_key = (str(rec.get('adjustment_doc_id') or ''), str(rec.get('trigger_condition') or ''))
        groups.setdefault(grp_key, []).append(rec)

    # Bulk update
    for rec in updated_records:
        if rec.get('status') != 'OPEN' or rec.get('updated_at') == now:
            try:
                from bson import ObjectId
                raw_db[_COL].update_one(
                    {'_id': ObjectId(str(rec['_id']))},
                    {'$set': {
                        'status': rec['status'],
                        'fill_price': rec.get('fill_price', 0.0),
                        'fill_qty': rec.get('fill_qty', 0),
                        'rejection_reason': rec.get('rejection_reason', ''),
                        'updated_at': now,
                    }},
                )
            except Exception as exc:
                log.debug('[ADJ TRACKER] update_one error: %s', exc)

    # Determine which groups are fully terminal
    completed_groups: list[tuple[str, str, str]] = []
    for (adj_doc_id, trigger_cond), grp_recs in groups.items():
        if not adj_doc_id:
            continue
        # Re-check current DB state (includes just-updated records)
        all_for_group = list(raw_db[_COL].find({'adjustment_doc_id': adj_doc_id}))
        if all_for_group and all(str(r.get('status') or '') in _TERMINAL for r in all_for_group):
            completed_groups.append((adj_doc_id, broker_id, trigger_cond))

    return completed_groups
