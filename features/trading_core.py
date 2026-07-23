"""
trading_core.py
══════════════════════════════════════════════════════════════════════════════
Heart of the trading system — reusable core engine.

This module contains ALL trading logic, independent of transport (WebSocket /
HTTP / CLI).  Every execution mode imports from here:

    • algo_backtest      – execution_socket._backtest_minute_tick
    • strategy_backtest  – single-strategy historical replay
    • portfolio_backtest – multi-strategy historical replay
    • fast_forward       – accelerated simulation (skip non-event ticks)
    • live_trade         – execution_socket._live_minute_tick

Architecture
────────────
                         trading_core.py
  ┌──────────────────────────────────────────────────────────────────┐
  │  §1  Imports & Types                                             │
  │  §2  Constants & Status Codes                                    │
  │  §3  Utility Helpers          (safe_float, is_sell, timestamps)  │
  │  §4  Market Data Helpers      (chain lookup, spot lookup)        │
  │  §5  Strike & Expiry Selection (WEEKLY/MONTH/ATM/OTM/ITM)       │
  │  §6  Leg Entry & Position History                                │
  │  §7  Feature Status Management (algo_leg_feature_status)         │
  │  §8  MTM / PnL Computation                                       │
  │  §9  Leg-Level SL / Target / Trail SL                            │
  │  §10 Overall SL / Target / Trail SL / LockAndTrail               │
  │  §11 Broker-Level SL / Target  (algo_borker_stoploss_settings)   │
  │  §12 Simple Momentum Engine                                      │
  │  §13 Re-entry Engine          (SL reentry, Target reentry)       │
  │  §14 Lazy Leg Engine          (pending legs, lazy entry)         │
  │  §15 Square-Off Helpers                                          │
  │  §16 Tick Processor           (per-minute core loop)             │
  │  §17 Entry Processor          (pending leg resolution)           │
  └──────────────────────────────────────────────────────────────────┘

Usage
─────
    from features.trading_core import (
        TickContext,
        process_tick,          # §16 — main per-minute loop
        process_pending_entries,  # §17 — entry resolution
        compute_strategy_mtm,  # §8  — PnL snapshot
        square_off_trade,      # §15 — manual/event square-off
    )

    ctx = TickContext(db=db, trade_date='2025-11-03',
                      now_ts='2025-11-03T09:18:00',
                      activation_mode='algo-backtest')
    result = process_tick(ctx, running_trades)

All functions accept a MongoData `db` instance so the caller controls the
database connection — no module-level singletons.
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger(__name__)

SHOW_PRINT_STATEMENT = False

# ──────────────────────────────────────────────────────────────────────────────
# §1  IMPORTS & TYPES
# ──────────────────────────────────────────────────────────────────────────────

# MongoData wrapper  (thin pymongo helper — db._db is the raw pymongo Database)
from features.mongo_data import MongoData  # type: ignore
import builtins as _builtins_tc
import features.debug_flags as _debug_flags_tc
from features.debug_flags import runtime_print, trade_event_print

def print(*_a, **_kw):  # suppress plain print() in fast-forward mode
    if not _debug_flags_tc.fast_forward_mode:
        _builtins_tc.print(*_a, **_kw)

# Market data cache helpers (shared with execution_socket.py)
from features.spot_atm_utils import (     # type: ignore
    get_cached_chain_doc,
    get_cached_spot_doc,
    preload_market_data_cache,
    clear_market_data_cache,
    build_entry_spot_snapshots,
)

# Position-manager: pure math, no DB side-effects
from features.position_manager import (   # type: ignore
    # ── leg-level SL / TP ────────────────────────────────────────────────────
    calc_sl_price,           # compute SL price from leg_cfg + entry_price
    calc_tp_price,           # compute TP price from leg_cfg + entry_price
    is_sl_hit,               # bool: current_price hit SL threshold
    is_tp_hit,               # bool: current_price hit TP threshold
    check_leg_exit,          # unified: returns LegCheckResult(event, price)
    # ── trailing SL ──────────────────────────────────────────────────────────
    get_trail_config,        # parse LegTrailSL config from leg_cfg
    update_trail_sl,         # recalculate SL when price moves favourably
    # ── reentry ──────────────────────────────────────────────────────────────
    get_reentry_sl_config,   # reentry config after SL hit
    get_reentry_tp_config,   # reentry config after TP hit
    # ── overall SL / target ──────────────────────────────────────────────────
    parse_overall_sl,        # extract overall SL (type + value) from strategy_cfg
    parse_overall_tgt,       # extract overall target from strategy_cfg
    check_overall_sl,        # bool: trade MTM hit overall SL
    check_overall_tgt,       # bool: trade MTM hit overall target
    # ── overall reentry ──────────────────────────────────────────────────────
    parse_overall_reentry_sl,   # reentry config after overall SL hit
    parse_overall_reentry_tgt,  # reentry config after overall target hit
    # ── overall trail SL ─────────────────────────────────────────────────────
    parse_overall_trail_sl,  # extract OverallTrailSL from strategy_cfg
    update_overall_trail_sl, # recalculate overall SL threshold
    # ── lock and trail ───────────────────────────────────────────────────────
    parse_lock_and_trail,    # extract LockAndTrail from strategy_cfg
    check_lock_and_trail,    # bool + floor: LockAndTrail exit check
    # ── types ────────────────────────────────────────────────────────────────
    LockAndTrailConfig,
    ReentryAction,
    LegCheckResult,
)

# ──────────────────────────────────────────────────────────────────────────────
# §2  CONSTANTS & STATUS CODES
# ──────────────────────────────────────────────────────────────────────────────

#: Leg status stored in algo_trades.legs[].status
OPEN_LEG_STATUS   = 1   # leg is open / active
CLOSED_LEG_STATUS = 0   # leg has been exited

#: Trade status stored in algo_trades.trade_status
TRADE_STATUS_RUNNING    = 1
TRADE_STATUS_SQUARED_OFF = 2

#: algo_trades.status strings
RUNNING_STATUS        = 'StrategyStatus.Live_Running'
SQUARED_OFF_STATUS    = 'StrategyStatus.SquaredOff'
BACKTEST_IMPORT_STATUS = 'StrategyStatus.Import'

#: MongoDB collection names
COL_ALGO_TRADES     = 'algo_trades'
COL_POSITIONS_HIST  = 'algo_trade_positions_history'
COL_LEG_FEATURES    = 'algo_leg_feature_status'
COL_NOTIFICATIONS   = 'algo_trade_notification'
COL_OPTION_CHAIN    = 'option_chain_historical_data'
COL_SPOT            = 'option_chain_index_spot'
COL_BROKER_SL       = 'algo_borker_stoploss_settings'
COL_INDIA_VIX       = 'india_vix'

#: Synthetic leg_id used for trade-level (overall) feature records
OVERALL_LEG_ID = '__overall__'


# ──────────────────────────────────────────────────────────────────────────────
# §2b  TICK CONTEXT  — carries all per-tick state (replaces scattered globals)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class TickContext:
    """
    Immutable-ish context object passed into every core function.

    Centralises: db, trade_date, now_ts, activation_mode, market_cache.
    This replaces the scattered parameter lists in execution_socket.py
    and makes every function testable without a running server.

    Example
    -------
        ctx = TickContext(
            db=db,
            trade_date='2025-11-03',
            now_ts='2025-11-03T09:18:00',
            activation_mode='algo-backtest',
        )
        result = process_tick(ctx, running_trades)
    """
    db:              MongoData
    trade_date:      str
    now_ts:          str                       # ISO timestamp of current candle
    activation_mode: str = 'algo-backtest'
    market_cache:    dict | None = None        # pre-loaded chain/spot cache

    # ── mutable state collected during the tick (filled by process_tick) ─────
    trade_mtm_map:     dict = field(default_factory=dict)  # trade_id → float MTM
    hit_trade_ids:     list = field(default_factory=list)  # IDs hit this tick
    hit_ltp_snapshots: dict = field(default_factory=dict)  # trade_id → leg LTP list
    actions_taken:     list = field(default_factory=list)  # audit strings


@dataclass
class TickResult:
    """
    Returned by process_tick().
    Frontend / websocket layer reads this to decide what to broadcast.
    """
    actions_taken:     list[str]         # human-readable audit log
    hit_trade_ids:     list[str]         # strategies whose overall/broker SL-TGT fired
    hit_ltp_snapshots: dict[str, list]   # {trade_id: [{leg_id, ltp, entry_price, pnl}]}
    open_positions:    list[dict]        # snapshot of all open positions this tick
    checked_at:        str               # now_ts echoed back


# ──────────────────────────────────────────────────────────────────────────────
# §3  UTILITY HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def safe_float(value: Any, default: float = 0.0) -> float:
    """
    Safely convert any value to float.
    Returns `default` on None, empty string, or conversion failure.

    Used everywhere a config value might be None or a string number.
    """
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    """
    Safely convert any value to int (via float to handle '75.0').
    Returns `default` on failure.
    """
    if value is None:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def is_sell(position_str: str) -> bool:
    """
    Returns True if the position string indicates a SELL (short) position.
    Handles: 'PositionType.Sell', 'sell', 'SHORT', etc.
    """
    return 'sell' in str(position_str or '').lower()


def parse_timestamp(value: Any) -> datetime | None:
    """
    Parse ISO timestamp strings into datetime objects.
    Accepts: 'YYYY-MM-DDTHH:MM:SS', 'YYYY-MM-DD HH:MM:SS', 'YYYY-MM-DDTHH:MM'.

    Returns None if parsing fails — callers must handle None gracefully.
    """
    raw = str(value or '').strip()
    if not raw:
        return None
    normalized = raw.replace(' ', 'T')
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M'):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def format_timestamp(dt: datetime) -> str:
    """Format datetime to canonical 'YYYY-MM-DDTHH:MM:SS' string."""
    return dt.strftime('%Y-%m-%dT%H:%M:%S')


def now_iso() -> str:
    """Current UTC time in ISO format ('YYYY-MM-DDTHH:MM:SSZ')."""
    return datetime.now(timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%dT%H:%M:%SZ')


def build_trade_query(
    trade_date: str,
    *,
    activation_mode: str | None = None,
    statuses: list[str] | None = None,
    include_squared_off: bool = False,
) -> dict:
    """
    Build a MongoDB query for algo_trades.

    Parameters
    ----------
    trade_date:           'YYYY-MM-DD'  (filters creation_ts with regex)
    activation_mode:      'algo-backtest' | 'live' | 'forward-test'
    statuses:             list of status strings; omit for default (Live_Running)
    include_squared_off:  if True, omits trade_status filter so both 1 and 2 are included

    Returns a pymongo-compatible filter dict.
    """
    query: dict[str, Any] = {}
    if not include_squared_off:
        query['trade_status'] = TRADE_STATUS_RUNNING
    normalized_date = str(trade_date or '').strip()
    if normalized_date:
        query['creation_ts'] = {'$regex': f'^{re.escape(normalized_date)}'}
    if activation_mode:
        query['activation_mode'] = str(activation_mode).strip()
    normalized_statuses = [str(s).strip() for s in (statuses or []) if str(s or '').strip()]
    if len(normalized_statuses) == 1:
        query['status'] = normalized_statuses[0]
    elif normalized_statuses:
        query['status'] = {'$in': normalized_statuses}
    return query


def normalize_expiry(raw_expiry: str) -> str:
    """
    Normalize expiry_date to 'YYYY-MM-DD' format.
    History collection may store '2025-11-06 15:30:00'; strip to date-only.
    """
    raw = str(raw_expiry or '').strip()
    return raw[:10] if ' ' in raw else raw


# ──────────────────────────────────────────────────────────────────────────────
# §4  MARKET DATA HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def make_option_token(underlying: str, expiry: str, strike: Any, option_type: str) -> str:
    """
    Build a composite token string for an option contract.

    Example:
        make_option_token('NIFTY', '2025-11-04', 24500, 'CE')
        → 'NIFTY_2025-11-04_24500_CE'

    Used as cache key and subscription identifier across all execution modes.
    """
    strike_str = str(int(float(strike))) if strike is not None else '0'
    return f'{underlying}_{expiry}_{strike_str}_{option_type}'


def get_chain_at_time(
    db: MongoData,
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    now_ts: str,
    market_cache: dict | None = None,
) -> dict | None:
    """
    Fetch option chain document at a specific historical timestamp.

    Backtest:      read from the preloaded market_cache.
    Live/forward:  delegate to get_cached_chain_doc(db._db, ...) so the
                   broker-specific active token lookup (Kite/Dhan) can
                   supply live LTP, token, symbol, and IV before entry.

    Used by:
      - Strike resolution at entry time
      - SL / TP price calculation
      - Backtest tick processing
    """
    try:
        data_source = market_cache if market_cache else db._db
        return get_cached_chain_doc(
            data_source,
            underlying,
            expiry,
            strike,
            option_type,
            now_ts,
            cache=market_cache,
        )
    except Exception as exc:
        log.warning('get_chain_at_time error underlying=%s strike=%s: %s', underlying, strike, exc)
        return None


def get_chain_by_token_at_time(
    db: MongoData,
    token: str,
    now_ts: str,
    market_cache: dict | None = None,
) -> dict | None:
    """
    Fetch option chain document using a composite token string.

    Token format: 'UNDERLYING_EXPIRY_STRIKE_OPTIONTYPE'
    Example: 'NIFTY_2025-11-04_24500_CE'

    Used during square-off when we already have the token from position history.
    """
    parts = token.split('_')
    if len(parts) < 4:
        return None
    underlying   = parts[0]
    option_type  = parts[-1]
    strike       = parts[-2]
    expiry       = '_'.join(parts[1:-2])
    return get_chain_at_time(db, underlying, expiry, strike, option_type, now_ts, market_cache)


def get_spot_at_time(
    db: MongoData,
    underlying: str,
    now_ts: str,
    market_cache: dict | None = None,
) -> float:
    """
    Fetch index spot price at a specific timestamp.

    Returns 0.0 if not found.

    Used by:
      - Strike selection (ATM calculation needs current spot)
      - Upper/Lower adjustment level checks
    """
    try:
        doc = get_cached_spot_doc(
            db._db,
            underlying=underlying,
            timestamp=now_ts,
            cache=market_cache,
        )
        if not doc:
            return 0.0
        return safe_float(
            doc.get('close') or doc.get('ltp') or doc.get('last_price') or doc.get('price')
        )
    except Exception as exc:
        log.warning('get_spot_at_time error underlying=%s: %s', underlying, exc)
        return 0.0


def get_vix_at_time(
    db: MongoData,
    now_ts: str,
    market_cache: dict | None = None,
) -> float:
    """
    Return India VIX value at the given timestamp.

    Backtest:      binary-search in market_cache['vix_docs'] (pre-loaded from india_vix).
    Live/fast-fwd: Kite WebSocket LTP (token 264969) → fallback to india_vix DB query.
    Returns 0.0 if not found.
    """
    try:
        # Backtest: use pre-loaded cache
        if market_cache:
            from features.spot_atm_utils import _find_latest_snapshot  # type: ignore
            doc = _find_latest_snapshot(
                market_cache.get('vix_docs') or [],
                market_cache.get('vix_timestamps') or [],
                now_ts,
            )
            return safe_float((doc or {}).get('close'))

        # Live / fast-forward: Kite WebSocket LTP first (same method as spot price)
        try:
            from features.spot_atm_utils import INDIA_VIX_KITE_TOKEN  # type: ignore
            from features.broker_gateway import get_broker_ltp_map  # type: ignore
            ltp_map = get_broker_ltp_map() or {}
            vix_ltp = safe_float(ltp_map.get(str(INDIA_VIX_KITE_TOKEN)))
            if vix_ltp > 0:
                return vix_ltp
        except Exception:
            pass

        # Fallback: DB query
        query: dict = {}
        if now_ts:
            query['timestamp'] = {'$lte': now_ts}
        doc = db._db[COL_INDIA_VIX].find_one(query, sort=[('timestamp', -1)])
        return safe_float((doc or {}).get('close'))
    except Exception as exc:
        log.warning('get_vix_at_time error: %s', exc)
        return 0.0


def resolve_chain_price(chain_doc: dict | None) -> float:
    """
    Extract the best available price from a chain document.

    Priority: close → ltp → current_price → price → last_price
    Returns 0.0 if chain_doc is None or all fields are missing/zero.

    This is the single authoritative price-extraction function — all
    leg-level SL/TP/entry computations should use this.
    """
    if not chain_doc:
        return 0.0
    for field_name in ('close', 'ltp', 'current_price', 'price', 'last_price'):
        val = safe_float(chain_doc.get(field_name))
        if val > 0:
            return val
    return 0.0


def normalize_chain_fields(chain_doc: dict) -> dict:
    """
    Ensure all standard price fields on a chain document are populated.

    Sets ltp, close, current_price, price, last_price to the best available
    value so downstream code can use any field without extra None checks.
    """
    best = resolve_chain_price(chain_doc)
    for f in ('ltp', 'close', 'current_price', 'price', 'last_price'):
        if not chain_doc.get(f):
            chain_doc[f] = best
    return chain_doc


def preload_market_cache(
    db: MongoData,
    trade_date: str,
    trade_records: list[dict],
) -> dict:
    """
    Pre-load all option chain + spot data for a trade_date into memory.

    Call once per trade_date before the tick loop to make all chain/spot
    lookups O(1) in-memory instead of per-tick MongoDB queries.

    Returns a market_cache dict that should be passed into TickContext.
    """
    try:
        underlyings = {
            str((t.get('config') or {}).get('Ticker') or t.get('ticker') or '')
            for t in trade_records
        } - {''}
        cache: dict = {}
        for underlying in underlyings:
            preload_market_data_cache(
                db._db,
                underlying=underlying,
                trade_date=trade_date,
                cache=cache,
            )
        return cache
    except Exception as exc:
        log.warning('preload_market_cache error date=%s: %s', trade_date, exc)
        return {}


# ──────────────────────────────────────────────────────────────────────────────
# §5  STRIKE & EXPIRY SELECTION
# ──────────────────────────────────────────────────────────────────────────────
#
# Expiry types supported:
#   WEEKLY      → current week's expiry (nearest expiry from now_ts)
#   NEXT_WEEK   → next week's expiry
#   MONTH       → current month's expiry (last Thursday / configured day)
#   NEXT_MONTH  → next month's expiry
#
# Strike types supported:
#   ATM         → At-the-money (nearest strike to spot)
#   OTM         → Out-of-the-money  (+N strikes for CE, -N strikes for PE)
#   ITM         → In-the-money      (-N strikes for CE, +N strikes for PE)
#
# Both resolve through features.strike_selector which uses the option_chain
# collection to find the actual listed strike nearest to the required value.
# ─────────────────────────────────────────────────────────────────────────────

def resolve_leg_expiry(
    db: MongoData,
    leg_cfg: dict,
    underlying: str,
    now_ts: str,
    market_cache: dict | None = None,
) -> str | None:
    """
    Resolve the expiry date string for a pending leg.

    Reads ExpiryKind (WEEKLY / NEXT_WEEK / MONTH / NEXT_MONTH) from leg_cfg
    and returns the matching expiry date as 'YYYY-MM-DD'.

    Returns None if resolution fails — caller should skip entry for this tick.

    Called by:
      - resolve_pending_leg_entry  (§17)
      - process_momentum_legs      (§12)
    """
    try:
        from features.backtest_engine import _resolve_expiry  # type: ignore
        expiry_kind = str(
            leg_cfg.get('ExpiryKind')
            or leg_cfg.get('expiry_kind')
            or leg_cfg.get('ExpiryType')
            or 'ExpiryType.Weekly'
        ).strip()
        trade_date = now_ts[:10]

        # Get expiries list: prefer market_cache, then active_option_tokens, then option chain
        expiries: list[str] = []
        if market_cache:
            expiries = list(
                (market_cache.get('expiries_by_underlying') or {}).get(underlying) or []
            )
        if not expiries:
            try:
                raw = db._db['active_option_tokens'].distinct(
                    'expiry',
                    {'instrument': underlying, 'expiry': {'$gte': trade_date}},
                )
                expiries = sorted(str(e)[:10] for e in raw if e)
            except Exception:
                pass
        if not expiries:
            try:
                from features.spot_atm_utils import get_kite_expiries  # type: ignore
                expiries = get_kite_expiries(underlying, trade_date)
            except Exception:
                pass
        if not expiries:
            chain_col = db._db['option_chain_historical_data']
            raw = chain_col.distinct(
                'expiry',
                {'underlying': underlying, 'expiry': {'$gte': trade_date}},
            )
            expiries = sorted(str(e)[:10] for e in raw if e)

        return _resolve_expiry(trade_date, expiry_kind, expiries)
    except Exception as exc:
        log.warning('resolve_leg_expiry error underlying=%s: %s', underlying, exc)
        return None


def resolve_leg_strike(
    db: MongoData,
    leg_cfg: dict,
    underlying: str,
    expiry: str,
    option_type: str,
    spot_price: float,
    now_ts: str,
    market_cache: dict | None = None,
) -> int | None:
    """
    Resolve the strike price for a pending leg.

    Reads StrikeParameter (ATM / OTM / ITM) and StrikeValue from leg_cfg,
    then finds the nearest listed strike in the option chain.

    Parameters
    ----------
    spot_price:   current index spot at entry time (from get_spot_at_time)

    Returns an integer strike price, or None on failure.

    Called by:
      - resolve_pending_leg_entry  (§17)
      - process_momentum_legs      (§12)
    """
    try:
        from features.strike_selector import resolve_strike  # type: ignore
        strike_param_raw = str(
            leg_cfg.get('StrikeParameter')
            or leg_cfg.get('strike_parameter')
            or leg_cfg.get('StrikeType')
            or 'ATM'
        ).strip()
        entry_kind = str(
            leg_cfg.get('EntryType')
            or leg_cfg.get('entry_kind')
            or leg_cfg.get('entry_type')
            or ''
        ).strip()
        position = str(
            leg_cfg.get('PositionType')
            or leg_cfg.get('position')
            or 'PositionType.Sell'
        ).strip()
        chain_col = db._db['option_chain_historical_data']
        trade_date = str(now_ts or '')[:10]
        result = resolve_strike(
            chain_col,
            underlying=underlying,
            option_type=option_type,
            entry_kind=entry_kind,
            strike_param_raw=strike_param_raw,
            position=position,
            spot_price=spot_price,
            expiry=expiry,
            trade_date=trade_date,
            snapshot_timestamp=now_ts,
            market_cache=market_cache,
        )
        return int(result.strike) if result and result.strike else None
    except Exception as exc:
        log.warning('resolve_leg_strike error underlying=%s expiry=%s: %s', underlying, expiry, exc)
        return None


# ──────────────────────────────────────────────────────────────────────────────
# §6  LEG ENTRY & POSITION HISTORY
# ──────────────────────────────────────────────────────────────────────────────

def build_exit_trade_payload(
    exit_price: float,
    exit_reason: str,
    now_ts: str,
    exit_iv: float | None = None,
    exit_vix: float | None = None,
) -> dict:
    """
    Build the exit_trade dict stored inside algo_trades.legs[].exit_trade
    and algo_trade_positions_history.exit_trade.

    exit_reason choices:
      'stoploss'   – leg-level SL hit
      'target'     – leg-level TP hit
      'overall_sl' – trade-level overall SL hit
      'overall_target' – trade-level overall target hit
      'exit_time'  – time-based forced exit
      'squared_off'– manual or broker-level square-off
    """
    payload = {
        'trigger_timestamp':  now_ts,
        'trigger_price':      exit_price,
        'price':              exit_price,
        'traded_timestamp':   now_ts,
        'exchange_timestamp': now_ts,
        'exit_reason':        exit_reason,
    }
    if exit_iv is not None:
        payload['exit_iv'] = exit_iv
    if exit_vix is not None:
        payload['exit_vix'] = exit_vix
    return payload


def close_leg_in_db(
    db: MongoData,
    trade_id: str,
    leg_index: int,
    exit_price: float,
    exit_reason: str,
    now_ts: str,
    leg_id: str = '',
    exit_iv: float | None = None,
    exit_vix: float | None = None,
) -> None:
    """
    Mark a leg as CLOSED in algo_trades and update its position history record.

    Steps:
      1. Sets legs[leg_index].status = CLOSED_LEG_STATUS (0)
      2. Sets legs[leg_index].exit_trade with price + reason + timestamps
      3. Calls update_position_history_exit() to mirror the exit in history

    Called by every exit path: SL hit, TP hit, exit_time, square-off.
    """
    exit_payload = build_exit_trade_payload(exit_price, exit_reason, now_ts, exit_iv=exit_iv, exit_vix=exit_vix)
    try:
        db._db[COL_ALGO_TRADES].update_one(
            {'_id': trade_id},
            {'$set': {
                f'legs.{leg_index}.status':      CLOSED_LEG_STATUS,
                f'legs.{leg_index}.exit_reason': exit_reason,
                f'legs.{leg_index}.exit_trade':  exit_payload,
                f'legs.{leg_index}.last_saw_price': exit_price,
            }},
        )
    except Exception as exc:
        log.error('close_leg_in_db error trade=%s leg=%s: %s', trade_id, leg_index, exc)
    update_position_history_exit(db, trade_id, leg_index, exit_price, exit_reason, now_ts, leg_id=leg_id, exit_iv=exit_iv, exit_vix=exit_vix)


def update_position_history_exit(
    db: MongoData,
    trade_id: str,
    leg_index: int,
    exit_price: float,
    exit_reason: str,
    now_ts: str,
    leg_id: str = '',
    exit_iv: float | None = None,
    exit_vix: float | None = None,
) -> None:
    """
    Write exit_trade data to algo_trade_positions_history.

    Finds the history record by leg_id (preferred) or trade_id + array index,
    sets exit_trade, updates last_saw_price and display fields.

    Called exclusively from close_leg_in_db — not called directly.
    """
    exit_payload = build_exit_trade_payload(exit_price, exit_reason, now_ts, exit_iv=exit_iv, exit_vix=exit_vix)
    try:
        if leg_id:
            db._db[COL_POSITIONS_HIST].update_one(
                {'trade_id': trade_id, 'leg_id': leg_id},
                {'$set': {
                    'exit_trade':     exit_payload,
                    'last_saw_price': exit_price,
                    'status':         CLOSED_LEG_STATUS,
                }},
            )
        else:
            db._db[COL_POSITIONS_HIST].update_one(
                {'trade_id': trade_id},
                {'$set': {
                    'exit_trade':     exit_payload,
                    'last_saw_price': exit_price,
                    'status':         CLOSED_LEG_STATUS,
                }},
                sort=[('_id', 1)],
            )
    except Exception as exc:
        log.error('update_position_history_exit error trade=%s leg=%s: %s', trade_id, leg_id, exc)


def resolve_trade_leg_configs(trade: dict) -> dict[str, dict]:
    """
    Merge ListOfLegConfigs + IdleLegConfigs into a single dict keyed by leg_id.

    This is the canonical way to get the config for any leg by its id,
    used by every SL/TP/Trail/Reentry check to read feature settings.

    IdleLegConfigs can be either:
      - A dict  {"callLeg1": {...}, "callLeg2": {...}}  (standard live/FF format)
      - A list  [{"id": "callLeg1", ...}, ...]          (legacy backtest format)

    Returns {} if trade has no leg configs.
    """
    merged: dict[str, dict] = {}

    # Handle list-format configs (ListOfLegConfigs is always a list)
    for cfg_list in (
        trade.get('ListOfLegConfigs') or [],
        (trade.get('strategy') or {}).get('ListOfLegConfigs') or [],
    ):
        for cfg in (cfg_list or []):
            if not isinstance(cfg, dict):
                continue
            leg_id = str(cfg.get('id') or '').strip()
            if leg_id:
                merged[leg_id] = cfg

    # Handle IdleLegConfigs — supports both dict {"callLeg1": {...}} and list [{id: ...}] formats
    for idle_src in (
        trade.get('IdleLegConfigs'),
        (trade.get('strategy') or {}).get('IdleLegConfigs'),
        (trade.get('config') or {}).get('IdleLegConfigs'),
    ):
        if isinstance(idle_src, dict):
            for k, v in idle_src.items():
                if isinstance(v, dict) and k:
                    merged[str(k)] = v
        elif isinstance(idle_src, list):
            for cfg in idle_src:
                if not isinstance(cfg, dict):
                    continue
                leg_id = str(cfg.get('id') or '').strip()
                if leg_id:
                    merged[leg_id] = cfg

    return merged


def resolve_leg_cfg(leg_id: str, leg: dict, all_leg_configs: dict[str, dict]) -> dict:
    """
    Get the leg config dict for a specific leg.

    Resolution priority (mirrors execution_socket._resolve_leg_cfg):
      1. Exact id match
      2. triggered_by field  (parent-leg direct reentries)
      3. lazy_leg_ref field  (momentum lazy legs)
      4. _re_ base-id prefix (reentry legs like leg1_re_20260416...)
      5. Longest dash-prefix match (momentum lazy leg composite ids)
      6. Fall back to the leg document itself

    Returns {} if not found — callers must treat missing config as
    "feature disabled".
    """
    # 1. Exact match
    cfg = all_leg_configs.get(leg_id)
    if cfg:
        return cfg

    # 2. triggered_by
    triggered_by = str(leg.get('triggered_by') or '').strip()
    if triggered_by:
        cfg = all_leg_configs.get(triggered_by)
        if cfg:
            return cfg

    # 3. lazy_leg_ref
    lazy_ref = str(leg.get('lazy_leg_ref') or '').strip()
    if lazy_ref:
        cfg = all_leg_configs.get(lazy_ref)
        if cfg:
            return cfg

    # 4. _re_ base-id prefix
    if '_re_' in leg_id:
        base = leg_id.split('_re_', 1)[0].strip()
        if base:
            cfg = all_leg_configs.get(base)
            if cfg:
                return cfg

    # 5. Longest dash-prefix match
    best_key = ''
    for k in all_leg_configs:
        if leg_id.startswith(k + '-') and len(k) > len(best_key):
            best_key = k
    if best_key:
        return all_leg_configs[best_key]

    return {}


def build_pending_leg(
    leg_id: str,
    leg_cfg: dict,
    trade: dict,
    now_ts: str,
    triggered_by: str = '',
    *,
    leg_type: str = '',
) -> dict:
    """
    Construct a new pending (lazy) leg dict to push into algo_trades.legs.

    The leg has no entry_trade yet — it will be filled by
    resolve_pending_leg_entry() (§17) when entry conditions are met.

    Fields set:
      id, status=OPEN, entry_trade=None, is_lazy=True,
      triggered_by, leg_type, ExpiryKind, StrikeParameter, etc.
    """
    _instrument_raw = str(leg_cfg.get('InstrumentKind') or '')
    _instrument_kind = _instrument_raw.split('.')[-1] if '.' in _instrument_raw else _instrument_raw
    return {
        'id':              leg_id,
        'status':          OPEN_LEG_STATUS,
        'entry_trade':     None,
        'exit_trade':      None,
        'is_lazy':         True,
        'triggered_by':    triggered_by,
        'leg_type':        leg_type or leg_id,
        'option':          str(leg_cfg.get('OptionType') or leg_cfg.get('option') or _instrument_kind or 'CE'),
        'position':        str(leg_cfg.get('Position') or leg_cfg.get('position') or 'PositionType.Sell'),
        'quantity':        safe_int(leg_cfg.get('Quantity') or leg_cfg.get('quantity') or 1),
        'lot_size':        safe_int(leg_cfg.get('LotSize') or leg_cfg.get('lot_size') or 1),
        'expiry_kind':     str(leg_cfg.get('ExpiryKind') or leg_cfg.get('ExpiryType') or 'WEEKLY'),
        'strike_parameter': str(leg_cfg.get('StrikeParameter') or leg_cfg.get('StrikeType') or 'ATM'),
        'strike_value':    safe_float(leg_cfg.get('StrikeValue') or leg_cfg.get('strike_value') or 0),
        'created_at':      now_ts,
        'parent_trade_id': str(trade.get('_id') or ''),
    }


def push_new_leg_in_db(db: MongoData, trade_id: str, leg: dict) -> bool:
    """
    Append a new pending leg dict to algo_trades.legs array.

    Returns True on success, False on error.
    Called by _handle_reentry (§13) and queue_original_legs (§14).
    """
    leg_id = str((leg or {}).get('id') or '').strip()
    if not leg_id:
        return False
    try:
        result = db._db[COL_ALGO_TRADES].update_one(
            {
                '_id': trade_id,
                'legs': {'$not': {'$elemMatch': {'id': leg_id}}},
            },
            {'$push': {'legs': leg}},
        )
        return bool(result.modified_count)
    except Exception as exc:
        log.error('push_new_leg_in_db error trade=%s: %s', trade_id, exc)
        return False


# ──────────────────────────────────────────────────────────────────────────────
# §7  FEATURE STATUS MANAGEMENT  (algo_leg_feature_status)
# ──────────────────────────────────────────────────────────────────────────────
#
# Feature status records track the state of every feature (SL, Target,
# TrailSL, Momentum, Overall SL/Target) per leg per trade.
#
# Each record:
#   trade_id, leg_id, feature, enabled, status, trigger_value,
#   triggered_at, current_mtm, disabled_reason
#
# Special leg_id '__overall__' is used for trade-level (overall) features.
# ─────────────────────────────────────────────────────────────────────────────

def set_overall_feature_state(
    db: MongoData,
    trade_id: str,
    feature: str,
    *,
    enabled: bool,
    status: str,
    now_ts: str,
    current_mtm: float = 0.0,
    disabled_reason: str = '',
) -> None:
    """
    Update a single overall feature record (overall_sl or overall_target).

    feature:         'overall_sl' | 'overall_target'
    enabled:         True = still watching; False = fired or disabled
    status:          'pending' | 'triggered' | 'disabled'
    disabled_reason: 'overall_sl_hit' | 'overall_target_hit' | 'cycle_completed'

    Called by sync_overall_feature_status() and when SL/Target fires.
    """
    try:
        db._db[COL_LEG_FEATURES].update_one(
            {
                'trade_id': trade_id,
                'leg_id':   OVERALL_LEG_ID,
                'feature':  feature,
            },
            {'$set': {
                'enabled':         enabled,
                'status':          status,
                'timestamp':       now_ts,
                'current_mtm':     round(current_mtm, 2),
                'disabled_reason': disabled_reason,
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error('set_overall_feature_state error trade=%s feature=%s: %s', trade_id, feature, exc)


def sync_overall_feature_status(
    db: MongoData,
    trade: dict,
    now_ts: str,
    *,
    current_mtm: float,
    overall_sl_done: int,
    overall_tgt_done: int,
) -> None:
    """
    Upsert overall_sl and overall_target feature records for a trade.

    Creates 'pending' records on first call, updates current_mtm and
    reentry cycle counts on subsequent calls.

    Called every tick for trades with overall SL/Target configured.
    Ensures the feature audit trail is always up-to-date.
    """
    strategy_cfg = trade.get('strategy') or trade.get('config') or {}
    _osl_type, _osl_val   = parse_overall_sl(strategy_cfg)
    _otgt_type, _otgt_val = parse_overall_tgt(strategy_cfg)

    if _osl_type != 'None' and _osl_val:
        set_overall_feature_state(
            db, str(trade.get('_id') or ''), 'overall_sl',
            enabled=True, status='pending', now_ts=now_ts,
            current_mtm=current_mtm,
        )
    if _otgt_type != 'None' and _otgt_val:
        set_overall_feature_state(
            db, str(trade.get('_id') or ''), 'overall_target',
            enabled=True, status='pending', now_ts=now_ts,
            current_mtm=current_mtm,
        )


def disable_feature_records_for_cycle(
    db: MongoData,
    trade_id: str,
    *,
    reason: str,
    now_ts: str,
) -> None:
    """
    Disable ALL feature records for a trade at the end of a reentry cycle.

    Called when overall SL or overall Target fires and the trade starts
    a new reentry cycle.  Ensures stale feature records don't block the
    next cycle's checks.
    """
    try:
        db._db[COL_LEG_FEATURES].update_many(
            {'trade_id': trade_id, 'enabled': True},
            {'$set': {
                'enabled':         False,
                'disabled_reason': reason,
                'disabled_at':     now_ts,
            }},
        )
    except Exception as exc:
        log.error('disable_feature_records_for_cycle error trade=%s: %s', trade_id, exc)


# ──────────────────────────────────────────────────────────────────────────────
# §8  MTM / PnL COMPUTATION
# ──────────────────────────────────────────────────────────────────────────────

def compute_strategy_mtm(
    db: MongoData,
    trade_id: str,
    now_ts: str,
    open_positions: list[dict] | None = None,
) -> tuple[float, list[dict]]:
    """
    Compute total MTM (mark-to-market) PnL for a trade at now_ts.

    Algorithm
    ---------
    1. Load all legs from algo_trade_positions_history for trade_id.
    2. For each entered leg:
       - If exit_trade exists and exit_dt <= now_ts → use exit_price (realized)
       - Else → use current_price from open_positions or last_saw_price (unrealized)
    3. PnL formula:
       SELL: (entry_price - current_price) × quantity × lot_size
       BUY:  (current_price - entry_price) × quantity × lot_size
    4. Sum all leg PnLs → total_mtm

    Returns
    -------
    (total_mtm, legs_pnl_snapshot)
    where legs_pnl_snapshot = [{leg_id, pnl, quantity, ltp, entry_price}]

    Used by:
      - Backtest tick (§16) for overall SL/Target check
      - Broker-level SL check (§11)
      - LTP-snapshot capture before square-off
    """
    normalized_id = str(trade_id or '').strip()
    if not normalized_id:
        return 0.0, []

    # Index open_positions by leg_id for O(1) LTP lookup
    ltp_by_leg: dict[str, dict] = {}
    for pos in (open_positions or []):
        leg_id = str((pos or {}).get('leg_id') or '').strip()
        if leg_id:
            ltp_by_leg[leg_id] = dict(pos)

    history_docs = list(db._db[COL_POSITIONS_HIST].find(
        {'trade_id': normalized_id},
        {
            '_id': 1, 'leg_id': 1, 'id': 1,
            'position': 1, 'quantity': 1, 'lot_size': 1,
            'entry_trade': 1, 'exit_trade': 1, 'last_saw_price': 1,
            'strike': 1, 'option': 1, 'option_type': 1, 'token': 1, 'symbol': 1,
        },
    ))

    now_dt      = parse_timestamp(now_ts)
    total_mtm   = 0.0
    legs_snapshot: list[dict] = []

    if SHOW_PRINT_STATEMENT:
        print('[MTM INPUT]', {
            'trade_id': normalized_id,
            'timestamp': now_ts,
            'history_docs': len(history_docs),
            'open_positions': len(open_positions or []),
            'open_leg_ids': [str((pos or {}).get('leg_id') or '') for pos in (open_positions or [])],
        })

    for doc in history_docs:
        entry_trade = doc.get('entry_trade') if isinstance(doc.get('entry_trade'), dict) else {}
        if not entry_trade:
            continue   # pending leg — not yet entered, no PnL

        leg_id       = str(doc.get('leg_id') or doc.get('id') or doc.get('_id') or '').strip()
        entry_price  = safe_float(entry_trade.get('price') or entry_trade.get('trigger_price'))
        lot_size     = safe_int(doc.get('lot_size'), 1)
        lots         = safe_int(doc.get('quantity') or entry_trade.get('quantity'))
        qty          = max(0, lots) * max(1, lot_size)

        if entry_price <= 0 or qty <= 0 or not leg_id:
            continue

        sell         = is_sell(str(doc.get('position') or ''))
        exit_trade   = doc.get('exit_trade') if isinstance(doc.get('exit_trade'), dict) else None
        exit_ts      = str((exit_trade or {}).get('traded_timestamp') or (exit_trade or {}).get('trigger_timestamp') or '').strip()
        exit_dt      = parse_timestamp(exit_ts)

        if exit_trade and (not now_dt or not exit_dt or exit_dt <= now_dt):
            # Leg already exited at or before now_ts → realized PnL
            current_price = safe_float(exit_trade.get('price') or exit_trade.get('trigger_price'))
        else:
            # Leg still open → unrealized PnL from live LTP or last_saw_price
            open_pos      = ltp_by_leg.get(leg_id) or {}
            current_price = safe_float(
                open_pos.get('current_price')
                or open_pos.get('ltp')
                or doc.get('last_saw_price')
            )

        pnl = ((entry_price - current_price) if sell else (current_price - entry_price)) * qty
        total_mtm += pnl
        if SHOW_PRINT_STATEMENT:
            print('[MTM LEG]', {
                'trade_id': normalized_id,
                'leg_id': leg_id,
                'strike': doc.get('strike'),
                'option_type': str(doc.get('option') or doc.get('option_type') or ''),
                'token': str(doc.get('token') or ''),
                'symbol': str(doc.get('symbol') or ''),
                'entry_price': entry_price,
                'current_price': current_price,
                'qty': qty,
                'sell': sell,
                'used_open_position': bool(ltp_by_leg.get(leg_id)),
                'last_saw_price': safe_float(doc.get('last_saw_price')),
                'pnl': round(pnl, 2),
            })
        legs_snapshot.append({
            'leg_id':      leg_id,
            'pnl':         round(pnl, 2),
            'quantity':    qty,
            'ltp':         current_price,
            'entry_price': entry_price,
        })

    runtime_print('[MTM TOTAL]', {
        'trade_id': normalized_id,
        'timestamp': now_ts,
        'current_mtm': round(total_mtm, 2),
    })
    return round(total_mtm, 2), legs_snapshot


def get_pending_leg_feature_rows(
    db: MongoData,
    *,
    strategy_id: str,
    trade_id: str,
    leg_id: str,
    leg_doc: dict | None = None,
) -> dict[str, dict]:
    normalized_strategy_id = str(
        (leg_doc or {}).get('strategy_id')
        or strategy_id
        or ''
    ).strip()
    normalized_trade_id = str(trade_id or '').strip()
    normalized_leg_id = str(leg_id or '').strip()
    if not normalized_trade_id or not normalized_leg_id:
        return {}
    base_query: dict[str, Any] = {
        'trade_id': normalized_trade_id,
        'leg_id': normalized_leg_id,
        'enabled': True,
        'status': 'pending',
    }

    queries: list[dict[str, Any]] = []
    if normalized_strategy_id:
        queries.append({**base_query, 'strategy_id': normalized_strategy_id})
    queries.append(base_query)

    try:
        for query in queries:
            feature_map: dict[str, dict] = {}
            for row in db._db[COL_LEG_FEATURES].find(query):
                feature_key = str(row.get('feature') or '').strip()
                if feature_key:
                    feature_map[feature_key] = row
            if feature_map:
                return feature_map
    except Exception as exc:
        log.warning('get_pending_leg_feature_rows error trade=%s leg=%s: %s', normalized_trade_id, normalized_leg_id, exc)
    return {}


# ──────────────────────────────────────────────────────────────────────────────
# §9  LEG-LEVEL SL / TARGET / TRAIL SL
# ──────────────────────────────────────────────────────────────────────────────
#
# Per-leg feature checks run inside the legs loop of process_tick().
# Each function is pure (no DB side-effects) — callers decide what to do
# with the result and write to DB via close_leg_in_db().
# ─────────────────────────────────────────────────────────────────────────────

def check_leg_sl(
    leg_cfg: dict,
    entry_price: float,
    current_price: float,
    stored_sl: float | None,
    is_sell_position: bool,
) -> tuple[bool, float]:
    """
    Check if the current price has hit the leg's stop-loss.

    Returns (sl_hit: bool, sl_price: float).

    Delegates to position_manager.is_sl_hit() and calc_sl_price().
    The stored_sl (from DB) takes priority over a freshly computed one —
    this preserves manual SL updates and trail-SL moves.
    """
    sl_price = stored_sl if stored_sl else calc_sl_price(entry_price, is_sell_position, leg_cfg.get('LegStopLoss') or {})
    if not sl_price:
        return False, 0.0
    hit = is_sl_hit(current_price, sl_price, is_sell_position)
    return hit, sl_price


def check_leg_target(
    leg_cfg: dict,
    entry_price: float,
    current_price: float,
    stored_tp: float | None,
    is_sell_position: bool,
) -> tuple[bool, float]:
    """
    Check if the current price has hit the leg's take-profit target.

    Returns (tp_hit: bool, tp_price: float).
    """
    tp_price = stored_tp if stored_tp else calc_tp_price(entry_price, is_sell_position, leg_cfg.get('LegTarget') or {})
    if not tp_price:
        return False, 0.0
    hit = is_tp_hit(current_price, tp_price, is_sell_position)
    return hit, tp_price


def compute_next_trail_sl(
    leg_cfg: dict,
    entry_price: float,
    current_price: float,
    stored_sl: float,
    is_sell_position: bool,
) -> float:
    """
    Compute the new trail-SL price if the current price moved favourably.

    Returns new_sl_price — if it equals stored_sl, no update is needed.
    Returns stored_sl unchanged if trail is not configured or conditions not met.

    Delegates to position_manager.update_trail_sl().
    """
    trail_cfg = get_trail_config(leg_cfg)
    if not trail_cfg:
        return stored_sl
    return update_trail_sl(entry_price, current_price, stored_sl, is_sell_position, trail_cfg) or stored_sl


def update_leg_sl_in_db(
    db: MongoData,
    trade_id: str,
    leg_index: int,
    new_sl_price: float,
    current_price: float,
    leg_id: str = '',
) -> None:
    """
    Persist a new SL price after a trail-SL move or manual SL update.

    Updates:
      - algo_trades.legs[leg_index].current_sl_price
      - algo_trades.legs[leg_index].last_saw_price
      - algo_trade_positions_history.current_sl_price  (by leg_id)

    Called only when new_sl_price != stored_sl (to avoid redundant writes).
    """
    try:
        db._db[COL_ALGO_TRADES].update_one(
            {'_id': trade_id},
            {'$set': {
                f'legs.{leg_index}.current_sl_price':  new_sl_price,
                f'legs.{leg_index}.last_saw_price':    current_price,
            }},
        )
        if leg_id:
            db._db[COL_POSITIONS_HIST].update_one(
                {'trade_id': trade_id, 'leg_id': leg_id},
                {'$set': {'current_sl_price': new_sl_price, 'last_saw_price': current_price}},
            )
    except Exception as exc:
        log.error('update_leg_sl_in_db error trade=%s leg=%s: %s', trade_id, leg_id, exc)


# ──────────────────────────────────────────────────────────────────────────────
# §10  OVERALL SL / TARGET / TRAIL SL / LOCK-AND-TRAIL  (per trade)
# ──────────────────────────────────────────────────────────────────────────────
#
# These checks operate on the total trade MTM, not individual legs.
#
# Priority order (same as execution_socket.py):
#   1. Overall SL        → close all legs, optional reentry
#   2. Lock-and-Trail    → close all legs, no reentry
#   3. Overall Trail SL  → adjust threshold (no close unless SL breached)
#   4. Overall Target    → close all legs, optional reentry
# ─────────────────────────────────────────────────────────────────────────────

def resolve_overall_cycle_value(base_value: float, completed_reentries: int) -> float:
    """
    Compute the effective overall SL/Target for the current reentry cycle.

    Formula: base_value × (completed_reentries + 1)

    Example: base=1000, cycle 0 → 1000, cycle 1 → 2000, cycle 2 → 3000
    This prevents cascading losses across reentry cycles.
    """
    normalized = safe_float(base_value)
    if normalized <= 0:
        return 0.0
    cycle_index = max(0, safe_int(completed_reentries)) + 1
    return round(normalized * cycle_index, 2)


def check_overall_sl_hit(
    strategy_cfg: dict,
    current_mtm: float,
    dynamic_sl_threshold: float | None,
    sl_reentry_done: int,
) -> bool:
    """
    Returns True if the trade's total MTM has hit the overall stop-loss.

    Checks both the cycle-adjusted SL and the dynamic (trail-adjusted) threshold.

    Parameters
    ----------
    strategy_cfg:         trade.strategy or trade.config
    current_mtm:          total trade PnL this tick (from compute_strategy_mtm)
    dynamic_sl_threshold: current trail-SL threshold (from algo_trades.current_overall_sl_threshold)
    sl_reentry_done:      number of SL reentry cycles already completed
    """
    osl_type, osl_val = parse_overall_sl(strategy_cfg)
    if osl_type == 'None' or not osl_val:
        return False
    effective_sl  = resolve_overall_cycle_value(osl_val, sl_reentry_done)
    dyn_sl        = safe_float(dynamic_sl_threshold, effective_sl or osl_val)
    return (effective_sl > 0 and current_mtm <= -effective_sl) or (dyn_sl > 0 and current_mtm <= -dyn_sl)


def check_overall_target_hit(
    strategy_cfg: dict,
    current_mtm: float,
    tgt_reentry_done: int,
) -> bool:
    """
    Returns True if the trade's total MTM has hit the overall target.

    Parameters
    ----------
    tgt_reentry_done: number of target reentry cycles already completed
    """
    otgt_type, otgt_val = parse_overall_tgt(strategy_cfg)
    if otgt_type == 'None' or not otgt_val:
        return False
    effective_tgt = resolve_overall_cycle_value(otgt_val, tgt_reentry_done)
    return effective_tgt > 0 and current_mtm >= effective_tgt


def compute_next_overall_trail_sl(
    strategy_cfg: dict,
    current_overall_sl: float,
    base_sl: float,
    peak_mtm: float,
) -> float:
    """
    Compute the new overall trail-SL threshold after a peak MTM update.

    Uses OverallTrailSL config from strategy_cfg.
    Returns new_threshold — if equal to current_overall_sl, no update needed.

    Called in the "elif open_leg_ids" branch after lock-and-trail is checked.
    """
    trail_type, for_every, trail_by = parse_overall_trail_sl(strategy_cfg)
    if trail_type == 'None' or for_every <= 0:
        return current_overall_sl
    new_threshold = update_overall_trail_sl(for_every, trail_by, base_sl, peak_mtm)
    return new_threshold if new_threshold < current_overall_sl else current_overall_sl


# ──────────────────────────────────────────────────────────────────────────────
# §11  BROKER-LEVEL SL / TARGET  (algo_borker_stoploss_settings)
# ──────────────────────────────────────────────────────────────────────────────
#
# Broker-level SL/Target aggregates MTM across ALL trades belonging to the
# same broker account (identified by user_id + broker + activation_mode).
#
# Total broker MTM = Σ open_trade_mtm + Σ closed_trade_realized_pnl
# (closed PnL is fetched via compute_strategy_mtm with exit_trade prices)
#
# When fired:
#   1. All open legs closed via square_off_trade()
#   2. All pending trades under the broker set to SquaredOff
#   3. algo_borker_stoploss_settings.status → 0  (disable further checks)
# ─────────────────────────────────────────────────────────────────────────────

def get_broker_sl_settings(
    db: MongoData,
    user_id: str,
    broker: str,
    activation_mode: str,
) -> dict | None:
    """
    Fetch broker-level SL settings from algo_borker_stoploss_settings.

    Query: {user_id, broker, activation_mode, status: 1}
    Returns the settings document or None if not found / disabled.

    Fields used:
      StopLoss  – total broker MTM floor  (e.g. -5000)
      Target    – total broker MTM ceiling (e.g. +3000)
      status    – 1=active, 0=disabled (set to 0 after hit)
    """
    if not user_id or not broker or not activation_mode:
        return None
    try:
        return db._db[COL_BROKER_SL].find_one({
            'user_id':         user_id,
            'broker':          broker,
            'activation_mode': activation_mode,
            'status':          1,
        })
    except Exception as exc:
        log.warning('get_broker_sl_settings error broker=%s: %s', broker, exc)
        return None


def disable_broker_sl_settings(
    db: MongoData,
    user_id: str,
    broker: str,
    activation_mode: str,
) -> None:
    """
    Set algo_borker_stoploss_settings.status = 0 after broker SL/Target fires.

    This prevents re-triggering on subsequent ticks.
    To re-enable: manually set status back to 1 in the UI/database.
    """
    try:
        db._db[COL_BROKER_SL].update_one(
            {
                'user_id':         user_id,
                'broker':          broker,
                'activation_mode': activation_mode,
                'status':          1,
            },
            {'$set': {'status': 0}},
        )
    except Exception as exc:
        log.warning('disable_broker_sl_settings error broker=%s: %s', broker, exc)


def compute_broker_group_mtm(
    db: MongoData,
    user_id: str,
    broker: str,
    trade_date: str,
    now_ts: str,
    running_trade_mtm_map: dict[str, float],
) -> tuple[float, float, float]:
    """
    Compute total MTM for all trades under a broker account on trade_date.

    Total = running_mtm (live) + closed_mtm (realized from history)

    Running trades use the pre-computed _trade_mtm_map from the current tick.
    Closed/squared-off trades call compute_strategy_mtm() to get final PnL.

    Returns (total_mtm, open_mtm, closed_mtm)

    This ensures a new position opened at 09:40 (open_mtm = -590) is combined
    with an earlier position that hit target at 09:33 (closed_mtm = +1700)
    → total = +1110, which is checked against broker Target of +1000.
    """
    open_mtm   = 0.0
    closed_mtm = 0.0
    try:
        all_trades = list(db._db[COL_ALGO_TRADES].find({
            'user_id':     user_id,
            'broker':      broker,
            'creation_ts': {'$regex': f'^{re.escape(trade_date)}'},
        }))
    except Exception as exc:
        log.warning('compute_broker_group_mtm query error: %s', exc)
        return 0.0, 0.0, 0.0

    for t in all_trades:
        tid = str(t.get('_id') or '').strip()
        if not tid:
            continue
        if tid in running_trade_mtm_map:
            open_mtm += running_trade_mtm_map[tid]
        else:
            try:
                c_mtm, _ = compute_strategy_mtm(db, tid, now_ts)
                closed_mtm += c_mtm
            except Exception as exc:
                log.warning('compute_broker_group_mtm closed PnL error trade=%s: %s', tid, exc)

    return round(open_mtm + closed_mtm, 2), round(open_mtm, 2), round(closed_mtm, 2)


def check_broker_sl_target(
    ctx: TickContext,
    running_trades: list[dict],
    strategy_map: dict[str, dict],
    open_trades_list: list[dict],
) -> tuple[list[str], dict[str, list]]:
    """
    Broker-level SL / Target / LockAndTrail check — run once per tick.

    Algorithm per broker group (user_id + broker + activation_mode):
      1. Fetch algo_borker_stoploss_settings (status=1).
      2. Compute total broker MTM = open (live) + closed (realized).
      3. Print [BROKER SL/TGT CHECK] every tick.
      4. StopLoss check  → MTM <= -StopLoss       → fire SL HIT
      5. Target check    → MTM >= Target           → fire TARGET HIT
      6. LockAndTrail    (if LockAndTrail field present in settings):
           a. Activate lock when MTM >= InstrumentMove → floor = StopLossMove
           b. OverallTrailSL (if not null):
                For every InstrumentMove increase above activation point,
                trail the floor up by StopLossMove.
           c. Print [BROKER LOCK CHECK] every tick after activation.
           d. Exit when MTM < current_lock_floor   → fire LOCK AND TRAIL HIT

    State for LockAndTrail is stored back in algo_borker_stoploss_settings:
      lock_activated    (bool)   – True after profit first crossed InstrumentMove
      current_lock_floor (float) – current exit floor (trails up, never down)
      lock_peak_mtm     (float)  – highest MTM seen since lock activated

    Returns (hit_trade_ids, hit_ltp_snapshots) for execute-orders emit.
    """
    hit_trade_ids:     list[str]       = []
    hit_ltp_snapshots: dict[str, list] = {}

    # ── Group running trades by (user_id, broker, activation_mode) ───────
    groups: dict[str, list[dict]] = {}
    for t in running_trades:
        _uid  = str(t.get('user_id') or '').strip()
        _bkr  = str(t.get('broker') or '').strip()
        _mode = str(t.get('activation_mode') or ctx.activation_mode).strip()
        if not _uid or not _bkr or not _mode:
            continue
        groups.setdefault(f'{_uid}|{_bkr}|{_mode}', []).append(t)

    for gkey, group in groups.items():
        _uid, _bkr, _mode = gkey.split('|', 2)

        # ── Broker group border — easy cross-verification in logs ─────────
        runtime_print(
            f'\n{"━" * 65}\n'
            f'  [BROKER GROUP]  broker={_bkr}  |  mode={_mode}  |  user={_uid}\n'
            f'{"━" * 65}'
        )

        settings = get_broker_sl_settings(ctx.db, _uid, _bkr, _mode)
        if not settings:
            continue

        sl_val   = settings.get('StopLoss')
        tgt_val  = settings.get('Target')
        lat_cfg  = settings.get('LockAndTrail') or {}   # LockAndTrail config
        trail_cfg = settings.get('OverallTrailSL')      # None = disabled

        # Skip if no feature is configured
        if not sl_val and not tgt_val and not lat_cfg and not trail_cfg:
            continue

        total_mtm, open_mtm, closed_mtm = compute_broker_group_mtm(
            ctx.db, _uid, _bkr, ctx.trade_date, ctx.now_ts, ctx.trade_mtm_map
        )

        # Identify currently open trades in this group
        open_trades = [
            t for t in group
            if str(t.get('_id') or '') in ctx.trade_mtm_map
        ]

        # ── OverallTrailSL standalone (Case A) ────────────────────────────
        # When LockAndTrail is null but OverallTrailSL is present:
        # For every `InstrumentMove` increase in profit → reduce StopLoss by `StopLossMove`
        # e.g. StopLoss=3700, InstrumentMove=100, StopLossMove=50
        #      profit 100 → effective_sl=3650, profit 200 → 3600, etc.
        effective_sl_val = sl_val  # default: use original StopLoss unchanged
        _settings_id = settings.get('_id')

        if trail_cfg and not lat_cfg and sl_val:
            _sl_trail_every = safe_float((trail_cfg or {}).get('InstrumentMove') or 0)
            _sl_trail_by    = safe_float((trail_cfg or {}).get('StopLossMove') or 0)
            if _sl_trail_every > 0 and _sl_trail_by > 0:
                # Reset sl trail state when config changes or new run date
                _sl_sig = f"{ctx.trade_date}|{sl_val}|{_sl_trail_every}|{_sl_trail_by}"
                _stored_sl_sig = settings.get('sl_settings_sig') or ''
                if _stored_sl_sig != _sl_sig:
                    _sl_peak_mtm     = 0.0
                    effective_sl_val = sl_val
                    try:
                        ctx.db._db[COL_BROKER_SL].update_one(
                            {'_id': _settings_id},
                            {'$set': {
                                'sl_settings_sig': _sl_sig,
                                'sl_peak_mtm':     0.0,
                                'effective_sl':    sl_val,
                            }},
                        )
                    except Exception as exc:
                        log.warning('[SL TRAIL STATE RESET] save error: %s', exc)
                    print('[SL TRAIL STATE RESET]', {
                        'timestamp': ctx.now_ts, 'broker': _bkr, 'mode': _mode,
                        'reason': 'settings changed or new run date',
                        'old_sig': _stored_sl_sig, 'new_sig': _sl_sig,
                    })
                else:
                    _sl_peak_mtm = safe_float(settings.get('sl_peak_mtm') or 0)
                _sl_state_upd: dict = {}
                if total_mtm > _sl_peak_mtm:
                    _sl_peak_mtm = total_mtm
                    _sl_state_upd['sl_peak_mtm'] = total_mtm
                _steps    = int(_sl_peak_mtm / _sl_trail_every) if _sl_peak_mtm >= _sl_trail_every else 0
                _new_eff  = round(safe_float(sl_val) - _steps * _sl_trail_by, 2)
                _prev_eff = safe_float(settings.get('effective_sl') or sl_val)
                # Only tighten the SL (move it lower), never loosen it
                effective_sl_val = min(_new_eff, _prev_eff)
                if effective_sl_val < _prev_eff:
                    _sl_state_upd['effective_sl'] = effective_sl_val
                    print('[BROKER SL TRAIL UPDATE]', {
                        'timestamp':   ctx.now_ts,
                        'broker':      _bkr,
                        'mode':        _mode,
                        'broker_mtm':  total_mtm,
                        'original_sl': sl_val,
                        'old_eff_sl':  _prev_eff,
                        'new_eff_sl':  effective_sl_val,
                        'sl_peak_mtm': _sl_peak_mtm,
                        'trail_steps': _steps,
                        'trail_every': _sl_trail_every,
                        'trail_by':    _sl_trail_by,
                    })
                if _sl_state_upd and _settings_id is not None:
                    try:
                        ctx.db._db[COL_BROKER_SL].update_one(
                            {'_id': _settings_id}, {'$set': _sl_state_upd}
                        )
                    except Exception as exc:
                        log.warning('[SL TRAIL STATE] save error: %s', exc)
                print('[BROKER SL TRAIL CHECK]', {
                    'timestamp':     ctx.now_ts,
                    'broker':        _bkr,
                    'mode':          _mode,
                    'broker_mtm':    total_mtm,
                    'original_sl':   sl_val,
                    'effective_sl':  effective_sl_val,
                    'sl_peak_mtm':   _sl_peak_mtm,
                    'trail_steps':   _steps,
                    'trail_every':   _sl_trail_every,
                    'trail_by':      _sl_trail_by,
                })

        sl_rem  = round(total_mtm + safe_float(effective_sl_val), 2)  if effective_sl_val  else None
        tgt_rem = round(safe_float(tgt_val) - total_mtm, 2) if tgt_val else None
        sl_status  = 'HIT' if (effective_sl_val and total_mtm <= -safe_float(effective_sl_val)) else 'active'
        tgt_status = 'HIT' if (tgt_val and total_mtm >= safe_float(tgt_val)) else 'active'

        # ── Print SL/TGT check every tick ────────────────────────────────
        print('[BROKER SL/TGT CHECK]', {
            'timestamp':     ctx.now_ts,
            'user_id':       _uid,
            'broker':        _bkr,
            'mode':          _mode,
            'open_mtm':      open_mtm,
            'closed_mtm':    closed_mtm,
            'broker_mtm':    total_mtm,
            'broker_sl':     effective_sl_val,
            'original_sl':   sl_val if effective_sl_val != sl_val else None,
            'sl_remaining':  sl_rem,
            'sl_status':     sl_status,
            'broker_target': tgt_val,
            'tgt_remaining': tgt_rem,
            'tgt_status':    tgt_status,
            'open_trades':   len(open_trades),
            'total_trades':  len(group),
        })

        # ── Shared fire helper ────────────────────────────────────────────
        def _fire(reason: str, lock_floor: float = 0.0) -> None:
            """Close all open trades + disable settings + collect hit IDs."""
            trade_ids = [str(t.get('_id') or '') for t in open_trades]
            print(f'[BROKER {reason}]', {
                'timestamp':           ctx.now_ts,
                'broker':              _bkr,
                'user_id':             _uid,
                'mode':                _mode,
                'broker_mtm':          total_mtm,
                'lock_floor':          lock_floor if lock_floor else None,
                'squaring_off_trades': trade_ids,
            })
            try:
                from features.notification_manager import upsert_broker_feature_status  # type: ignore
                _feature_name = 'broker_sl' if 'SL' in str(reason or '').upper() else 'broker_target'
                _trigger_value = safe_float(effective_sl_val) if _feature_name == 'broker_sl' else safe_float(tgt_val)
                upsert_broker_feature_status(
                    ctx.db._db,
                    trade=(group[0] if group else {}),
                    user_id=_uid,
                    broker=_bkr,
                    activation_mode=_mode,
                    feature=_feature_name,
                    trigger_value=_trigger_value,
                    current_mtm=total_mtm,
                    timestamp=ctx.now_ts,
                )
            except Exception as exc:
                log.warning('[BROKER %s] feature status upsert error: %s', reason, exc)
            # Step 1 — close open-leg trades at current LTP
            for t in open_trades:
                fresh = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': t['_id']}) or t
                square_off_trade(ctx.db, fresh, ctx.now_ts, market_cache=ctx.market_cache)
                trade_event_print(f'  [BROKER {reason}] trade={str(t.get("_id") or "")[:16]} closed')
            # Step 2 — bulk-close all pending/import trades under this broker
            try:
                res = ctx.db._db[COL_ALGO_TRADES].update_many(
                    {'broker': _bkr, 'user_id': _uid, 'activation_mode': _mode, 'active_on_server': True},
                    {'$set': {'active_on_server': False, 'status': SQUARED_OFF_STATUS, 'trade_status': TRADE_STATUS_SQUARED_OFF}},
                )
                print(f'  [BROKER {reason}] group close matched={res.matched_count} modified={res.modified_count}')
            except Exception as exc:
                log.warning('[BROKER %s] bulk close error: %s', reason, exc)
            # Step 3 — disable settings (status → 0) so next tick skips
            disable_broker_sl_settings(ctx.db, _uid, _bkr, _mode)
            print(f'  [BROKER {reason}] settings disabled user={_uid[:8]}.. broker={_bkr[:8]}..')
            ctx.actions_taken.append(
                f'broker_{reason.lower().replace(" ", "_")} broker={_bkr} mtm={total_mtm}'
            )
            # Collect hit IDs + LTP snapshots for execute-orders emit
            for t in group:
                tid = str(t.get('_id') or '').strip()
                if tid and tid not in hit_trade_ids:
                    hit_trade_ids.append(tid)
                    hit_ltp_snapshots[tid] = list(
                        (strategy_map.get(tid) or {}).get('open_positions') or []
                    )

        # ── 1. StopLoss check ─────────────────────────────────────────────
        # Uses effective_sl_val which may be trailed lower than original sl_val
        if effective_sl_val and total_mtm <= -safe_float(effective_sl_val):
            _fire('SL HIT')
            continue

        # ── 2. Target check ───────────────────────────────────────────────
        if tgt_val and total_mtm >= safe_float(tgt_val):
            _fire('TARGET HIT')
            continue

        # ── 3. LockAndTrail check ─────────────────────────────────────────
        # Only runs if LockAndTrail config is present in settings.
        if not lat_cfg:
            continue

        lat_instrument_move = safe_float(lat_cfg.get('InstrumentMove') or 0)  # e.g. 600
        lat_stop_loss_move  = safe_float(lat_cfg.get('StopLossMove') or 0)    # e.g. 400
        if not lat_instrument_move or not lat_stop_loss_move:
            continue

        # OverallTrailSL — None means disabled (only plain lock, no trail)
        trail_every = safe_float((trail_cfg or {}).get('InstrumentMove') or 0)  # e.g. 100
        trail_by    = safe_float((trail_cfg or {}).get('StopLossMove') or 0)    # e.g. 20
        has_trail   = bool(trail_cfg and trail_every > 0 and trail_by > 0)

        _settings_id  = settings.get('_id')
        _state_updates: dict = {}

        # ── Settings signature: reset state when user changes config mid-run ──
        # Covers both (a) new backtest date and (b) live settings edit.
        # Fingerprint = trade_date + all config values that affect lock logic.
        _lock_sig = (
            f"{ctx.trade_date}|"
            f"{lat_instrument_move}|{lat_stop_loss_move}|"
            f"{trail_every}|{trail_by}|"
            f"{sl_val}|{tgt_val}"
        )
        _stored_lock_sig    = settings.get('lock_settings_sig') or ''
        _was_lock_activated = bool(settings.get('lock_activated') or False)
        if _stored_lock_sig != _lock_sig:
            if _was_lock_activated:
                # Lock already activated — NEVER reset back to False.
                # Only update the signature; preserve activation state and floor.
                lock_activated     = True
                current_lock_floor = safe_float(settings.get('current_lock_floor') or 0)
                lock_peak_mtm      = safe_float(settings.get('lock_peak_mtm') or 0)
                _state_updates['lock_settings_sig'] = _lock_sig
                print('[LOCK SIG UPDATED - ACTIVATION PRESERVED]', {
                    'timestamp':   ctx.now_ts,
                    'broker':      _bkr,
                    'mode':        _mode,
                    'reason':      'settings changed but lock already activated — not resetting',
                    'lock_floor':  current_lock_floor,
                    'old_sig':     _stored_lock_sig,
                    'new_sig':     _lock_sig,
                })
            else:
                # Lock not yet activated — safe to reset for the new config
                lock_activated     = False
                current_lock_floor = 0.0
                lock_peak_mtm      = 0.0
                _state_updates.update({
                    'lock_settings_sig':  _lock_sig,
                    'lock_activated':     False,
                    'current_lock_floor': 0.0,
                    'lock_peak_mtm':      0.0,
                })
                print('[LOCK STATE RESET]', {
                    'timestamp':  ctx.now_ts,
                    'broker':     _bkr,
                    'mode':       _mode,
                    'reason':     'settings changed or new run date',
                    'old_sig':    _stored_lock_sig,
                    'new_sig':    _lock_sig,
                })
        else:
            lock_activated     = _was_lock_activated
            current_lock_floor = safe_float(settings.get('current_lock_floor') or 0)
            lock_peak_mtm      = safe_float(settings.get('lock_peak_mtm') or 0)

            # Safety guard: lock_floor must never decrease once set.
            # If DB stored a stale/missing floor, reconstruct from peak.
            if lock_activated and lock_peak_mtm > lat_instrument_move:
                if has_trail and trail_every > 0:
                    _pa = lock_peak_mtm - lat_instrument_move
                    _steps_so_far = int(_pa / trail_every) if _pa >= trail_every else 0
                    _implied_floor = round(lat_stop_loss_move + _steps_so_far * trail_by, 2)
                else:
                    _implied_floor = lat_stop_loss_move
                if _implied_floor > current_lock_floor:
                    current_lock_floor = _implied_floor
                    _state_updates['current_lock_floor'] = _implied_floor

        # ── 3a. Activate lock when MTM first crosses InstrumentMove ──────
        if not lock_activated:
            if total_mtm >= lat_instrument_move:
                lock_activated     = True
                current_lock_floor = lat_stop_loss_move
                lock_peak_mtm      = total_mtm
                _state_updates.update({
                    'lock_activated':      True,
                    'current_lock_floor':  lat_stop_loss_move,
                    'lock_peak_mtm':       total_mtm,
                    'lock_activated_at':   ctx.now_ts,
                    'lock_activation_mtm': total_mtm,
                })
                print('[LOCK ACTIVATED]', {
                    'timestamp':         ctx.now_ts,
                    'broker':            _bkr,
                    'mode':              _mode,
                    'broker_mtm':        total_mtm,
                    'instrument_move':   lat_instrument_move,
                    'lock_floor_set_to': lat_stop_loss_move,
                    'trail_enabled':     has_trail,
                })
            else:
                # Lock not yet activated — print full settings snapshot for cross-verification
                print('[BROKER LOCK CHECK]', {
                    'timestamp':         ctx.now_ts,
                    'broker':            _bkr,
                    'mode':              _mode,
                    'broker_mtm':        total_mtm,
                    'lock_activated':    False,
                    # ── LockAndTrail config (from DB) ──
                    'activate_at':       lat_instrument_move,
                    'lock_profit':       lat_stop_loss_move,
                    'remaining_to_lock': round(lat_instrument_move - total_mtm, 2),
                    # ── OverallTrailSL config (from DB) ──
                    'trail_enabled':     has_trail,
                    'trail_every':       trail_every if has_trail else None,
                    'trail_by':          trail_by    if has_trail else None,
                    # ── StopLoss / Target (from DB) ──
                    'broker_sl':         sl_val,
                    'broker_target':     tgt_val,
                    # ── Signature: changes when settings edited mid-run ──
                    'settings_sig':      _lock_sig,
                })
                continue

        # ── 3b. Update peak MTM ───────────────────────────────────────────
        if total_mtm > lock_peak_mtm:
            lock_peak_mtm = total_mtm
            _state_updates['lock_peak_mtm'] = total_mtm

        # ── 3c. Trail lock floor if OverallTrailSL is configured ─────────
        if has_trail:
            # For every `trail_every` increase above the activation point,
            # raise the floor by `trail_by`.
            # Formula: new_floor = stop_loss_move + floor((peak - instrument_move) / trail_every) × trail_by
            profit_above = lock_peak_mtm - lat_instrument_move
            trail_steps  = int(profit_above / trail_every) if profit_above >= trail_every else 0
            new_floor    = round(lat_stop_loss_move + trail_steps * trail_by, 2)
            if new_floor > current_lock_floor:
                print('[LOCK TRAIL UPDATE]', {
                    'timestamp':       ctx.now_ts,
                    'broker':          _bkr,
                    'mode':            _mode,
                    'broker_mtm':      total_mtm,
                    'lock_peak_mtm':   lock_peak_mtm,
                    'old_lock_floor':  current_lock_floor,
                    'new_lock_floor':  new_floor,
                    'trail_steps':     trail_steps,
                    'trail_every':     trail_every,
                    'trail_by':        trail_by,
                })
                current_lock_floor = new_floor
                _state_updates['current_lock_floor'] = new_floor

        # ── Persist state changes ─────────────────────────────────────────
        # lock_peak_mtm and current_lock_floor use $max so they are
        # monotonically non-decreasing — stale DB reads can never roll them back.
        if _state_updates and _settings_id is not None:
            try:
                _max_fields: dict = {}
                _set_fields: dict  = {}
                for _k, _v in _state_updates.items():
                    if _k in ('lock_peak_mtm', 'current_lock_floor'):
                        _max_fields[_k] = _v
                    else:
                        _set_fields[_k] = _v
                _mongo_op: dict = {}
                if _max_fields:
                    _mongo_op['$max'] = _max_fields
                if _set_fields:
                    _mongo_op['$set'] = _set_fields
                if _mongo_op:
                    ctx.db._db[COL_BROKER_SL].update_one(
                        {'_id': _settings_id},
                        _mongo_op,
                    )
            except Exception as exc:
                log.warning('[LOCK STATE] save error: %s', exc)

        # ── Print lock status every tick ──────────────────────────────────
        print('[BROKER LOCK CHECK]', {
            'timestamp':         ctx.now_ts,
            'broker':            _bkr,
            'mode':              _mode,
            'broker_mtm':        total_mtm,
            'lock_activated':    lock_activated,
            # ── Lock state ──
            'lock_floor':        current_lock_floor,
            'lock_peak_mtm':     lock_peak_mtm,
            'remaining_to_exit': round(total_mtm - current_lock_floor, 2),
            # ── LockAndTrail config (from DB) ──
            'activate_at':       lat_instrument_move,
            'lock_profit':       lat_stop_loss_move,
            # ── OverallTrailSL config (from DB) ──
            'trail_enabled':     has_trail,
            'trail_every':       trail_every if has_trail else None,
            'trail_by':          trail_by    if has_trail else None,
            # ── StopLoss / Target (from DB) ──
            'broker_sl':         sl_val,
            'broker_target':     tgt_val,
            # ── Signature: changes when settings edited mid-run ──
            'settings_sig':      _lock_sig,
        })

        # ── 3d. Exit when MTM drops below lock floor ──────────────────────
        if total_mtm < current_lock_floor:
            _fire('LOCK AND TRAIL HIT', lock_floor=current_lock_floor)

    return hit_trade_ids, hit_ltp_snapshots


# ──────────────────────────────────────────────────────────────────────────────
# §12  SIMPLE MOMENTUM ENGINE
# ──────────────────────────────────────────────────────────────────────────────
#
# Simple Momentum: a leg only enters after its underlying price moves by
# a configured amount from a reference price.
#
# Types:
#   PercentageUp    – trigger when price rises N% from base
#   PercentageDown  – trigger when price falls N% from base
#   PointsUp        – trigger when price rises N points from base
#   PointsDown      – trigger when price falls N points from base
#
# Flow:
#   1. Leg is created as pending (is_lazy=True).
#   2. On first tick: set base_price = current spot/option price.
#   3. Every tick: check if current_price reached target_price.
#   4. On trigger: proceed with normal entry (resolve_pending_leg_entry).
# ─────────────────────────────────────────────────────────────────────────────

def has_momentum_config(leg_cfg: dict) -> bool:
    """
    Returns True if the leg has a LegMomentum config with Type != 'None'
    and a positive Value.

    Used to decide whether to arm momentum tracking for a pending leg.
    """
    momentum = (leg_cfg or {}).get('LegMomentum') or {}
    if not isinstance(momentum, dict):
        return False
    m_type  = str(momentum.get('Type') or 'None').strip()
    m_value = safe_float(momentum.get('Value') or 0)
    return m_type != 'None' and m_value > 0


def compute_momentum_target(
    momentum_type: str,
    base_price: float,
    momentum_value: float,
) -> float | None:
    """
    Compute the momentum trigger price from base_price.

    momentum_type:  'PercentageUp' | 'PercentageDown' | 'PointsUp' | 'PointsDown'
    momentum_value: percentage or points
    base_price:     price when momentum tracking was armed

    Returns the target price, or None if inputs are invalid.
    """
    if not base_price or not momentum_value:
        return None
    m = str(momentum_type or '').strip()
    if 'PercentageUp' in m:
        return round(base_price * (1 + momentum_value / 100), 2)
    if 'PercentageDown' in m:
        return round(base_price * (1 - momentum_value / 100), 2)
    if 'PointsUp' in m:
        return round(base_price + momentum_value, 2)
    if 'PointsDown' in m:
        return round(base_price - momentum_value, 2)
    return None


def is_momentum_triggered(
    momentum_type: str,
    current_price: float,
    target_price: float,
) -> bool:
    """
    Returns True if current_price has reached the momentum target.

    Direction is inferred from momentum_type:
      Up variants   → triggered when current_price >= target_price
      Down variants → triggered when current_price <= target_price
    """
    if not current_price or not target_price:
        return False
    m = str(momentum_type or '').strip()
    if 'Up' in m:
        return current_price >= target_price
    if 'Down' in m:
        return current_price <= target_price
    return False


# ──────────────────────────────────────────────────────────────────────────────
# §13  RE-ENTRY ENGINE
# ──────────────────────────────────────────────────────────────────────────────
#
# After a leg-level or overall SL/Target fires, the reentry engine may open
# new positions according to the strategy's reentry configuration.
#
# Reentry types:
#   NextLeg      – push a pre-configured "next leg" (lazy/pending)
#   Immediate    – re-enter same strike/expiry immediately
#   AtCost       – wait until underlying returns to entry cost before re-entering
#   LikeOriginal – re-enter N times with original leg parameters
#
# Overall reentry types (after overall SL/Target):
#   Same as above but applied to ALL trade legs simultaneously.
# ─────────────────────────────────────────────────────────────────────────────

def handle_leg_reentry(
    db: MongoData,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
    exit_event: str,
    now_ts: str,
) -> str | None:
    """
    Process reentry config after a leg-level SL or Target hit.

    Parameters
    ----------
    exit_event:  'stoploss' | 'target'

    Returns a human-readable result string for the actions_taken log,
    or None if reentry is not configured / already exhausted.

    Called immediately after close_leg_in_db() when SL/TP fires.
    """
    reentry_cfg = (
        get_reentry_sl_config(leg_cfg) if exit_event == 'stoploss'
        else get_reentry_tp_config(leg_cfg)
    )
    if not reentry_cfg:
        return None

    trade_id = str(trade.get('_id') or '')
    leg_id   = str(leg.get('id') or '')

    # Read the reentry kind straight off the raw config — NOT via
    # build_reentry_action(), whose (leg_cfg, reentry_config, triggered_by,
    # now_ts, existing_legs, idle_configs, parent_leg_type='') signature does
    # not match a 3-positional-arg call and has no verified caller anywhere
    # in the codebase (only this one, which would TypeError on first use).
    re_type = str(reentry_cfg.get('Type') or '')
    re_kind = (
        'lazy'         if 'NextLeg'   in re_type else
        'immediate'    if 'Immediate' in re_type else
        'at_cost'      if 'AtCost'    in re_type else
        'like_original'
    )

    # Bounded reentry count: read THIS (closing) leg's own remaining budget,
    # not the static config's ReentryCount — the static config is the same
    # value on every generation (resolve_leg_cfg always walks back to it for
    # SL/Target/Trail to keep applying), so gating on it directly would make
    # reentries unbounded across generations. Mirrors
    # execution_socket._reentry_budget_remaining (the live-path equivalent).
    configured_count = safe_int((reentry_cfg.get('Value') or {}).get('ReentryCount'))
    remaining = leg.get('reentry_count_remaining')
    budget = safe_int(remaining) if remaining is not None else configured_count
    if budget <= 0:
        return None

    if re_kind == 'lazy':
        # Push the pre-configured "next leg" (NextLegRef) as a pending leg
        next_leg_id  = str((reentry_cfg.get('Value') or {}).get('NextLegRef') or '')
        next_leg_cfg = (resolve_trade_leg_configs(trade) or {}).get(next_leg_id) or {}
        if next_leg_id and next_leg_cfg:
            new_leg = build_pending_leg(next_leg_id, next_leg_cfg, trade, now_ts,
                                        triggered_by=leg_id, leg_type=f'{leg_id}-lazyleg_1')
            new_leg['reentry_count_remaining'] = budget - 1
            push_new_leg_in_db(db, trade_id, new_leg)
    elif re_kind in ('immediate', 'at_cost', 'like_original'):
        # Re-queue original leg config as a fresh pending leg
        new_leg_id = f'{leg_id}-reentry-{now_ts.replace(":", "").replace("-", "").replace("T", "")[:14]}'
        new_leg = build_pending_leg(new_leg_id, leg_cfg, trade, now_ts,
                                    triggered_by=leg_id, leg_type=f'{leg_id}-reentry')
        new_leg['reentry_count_remaining'] = budget - 1
        push_new_leg_in_db(db, trade_id, new_leg)

    return f'reentry({re_kind}) queued after {exit_event} on leg={leg_id}'


def requeue_all_legs_for_overall_reentry(
    db: MongoData,
    trade: dict,
    trade_id: str,
    strategy_cfg: dict,
    now_ts: str,
    reentry_type: str = '',
) -> list[str]:
    """
    Re-queue ALL original leg configs as new pending legs for overall reentry.

    Called after overall SL or overall Target fires and the trade is configured
    for reentry (parse_overall_reentry_sl / parse_overall_reentry_tgt returns
    count > 0).

    reentry_type: ore_type / ort_type from parse_overall_reentry_sl/tgt
                  (e.g. 'LikeOriginal'). When 'LikeOriginal', new legs get
                  reentry_type='LikeOriginal' to match leg-level behaviour.

    Returns list of new leg_ids pushed.
    """
    original_cfgs = (
        strategy_cfg.get('ListOfLegConfigs')
        or list((resolve_trade_leg_configs(trade) or {}).values())
    )
    new_ids: list[str] = []
    ts_sfx = now_ts.replace(':', '').replace('T', '').replace('-', '')[:14]
    existing = {str(l.get('id') or '') for l in (trade.get('legs') or []) if isinstance(l, dict)}
    is_like_original = 'LikeOriginal' in reentry_type

    def _leg_has_momentum(leg_cfg: dict) -> bool:
        mom = leg_cfg.get('LegMomentum') or {}
        mom_type = str(mom.get('Type') or 'None')
        mom_val = safe_float(mom.get('Value'))
        return 'None' not in mom_type and mom_val > 0

    for cfg in original_cfgs:
        if not isinstance(cfg, dict):
            continue
        orig_id = str(cfg.get('id') or '')
        if not orig_id:
            continue
        new_id   = f'{orig_id}-ore-{ts_sfx}'
        leg_type = f'{orig_id}-overall_reentry'
        if new_id in existing:
            continue

        if is_like_original and _leg_has_momentum(cfg):
            # LikeOriginal + LegMomentum → same method as NextLeg+momentum:
            # queue to algo_leg_feature_status, momentum_pending flow enters it
            mom      = cfg.get('LegMomentum') or {}
            option_raw = str(cfg.get('InstrumentKind') or '')
            option_type = option_raw.split('.')[-1] if '.' in option_raw else option_raw
            mom_doc = {
                'trade_id':        trade_id,
                'leg_id':          new_id,
                'lazy_leg_ref':    orig_id,
                'parent_leg_id':   orig_id,
                'feature':         'momentum_pending',
                'enabled':         True,
                'status':          'active',
                'underlying':      str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '').strip().upper(),
                'option':          option_type,
                'expiry_kind':     str(cfg.get('ExpiryKind') or 'ExpiryType.Weekly'),
                'strike_parameter': str(cfg.get('StrikeParameter') or 'StrikeType.ATM'),
                'entry_kind':      str(cfg.get('EntryType') or ''),
                'position':        str(cfg.get('PositionType') or ''),
                'lot_config_value': max(1, int((cfg.get('LotConfig') or {}).get('Value') or 1)),
                'momentum_type':   str(mom.get('Type') or 'None'),
                'momentum_value':  safe_float(mom.get('Value')),
                'triggered_by':    f'overall_{orig_id}',
                'leg_type':        leg_type,
                'queued_at':       now_ts,
                'is_reentered_leg': True,
                'reentry_type':    'LikeOriginal',
            }
            try:
                db._db['algo_leg_feature_status'].insert_one(mom_doc)
                new_ids.append(new_id)
            except Exception as _e:
                log.warning('overall reentry momentum_pending insert error leg=%s: %s', orig_id, _e)
            continue

        new_leg = build_pending_leg(new_id, cfg, trade, now_ts,
                                    triggered_by=f'overall_{orig_id}',
                                    leg_type=leg_type)
        new_leg['is_reentered_leg'] = True
        new_leg['lazy_leg_ref']     = orig_id
        new_leg['parent_leg_id']    = orig_id
        if is_like_original:
            # LikeOriginal, no LegMomentum → pending leg, enters on next tick
            new_leg['reentry_type'] = 'LikeOriginal'
        else:
            # Immediate (and others): bypass momentum gate, enter directly
            new_leg['skip_momentum_check'] = True
        push_new_leg_in_db(db, trade_id, new_leg)
        new_ids.append(new_id)

    return new_ids


# ──────────────────────────────────────────────────────────────────────────────
# §14  LAZY LEG ENGINE  (pending legs)
# ──────────────────────────────────────────────────────────────────────────────
#
# A "lazy leg" is a pending leg with entry_trade=None.
# It waits in algo_trades.legs[] until entry conditions are met on some tick.
#
# Entry conditions (checked in order):
#   1. entry_time  – not before the strategy's configured entry_time
#   2. Expiry      – resolve_leg_expiry must succeed
#   3. Strike      – resolve_leg_strike must succeed
#   4. Chain price – must be > 0 (contract must exist in option_chain)
#   5. Momentum    – if LegMomentum configured, trigger price must be reached
# ─────────────────────────────────────────────────────────────────────────────

def queue_original_legs_if_needed(
    db: MongoData,
    trade: dict,
    now_ts: str,
) -> bool:
    """
    On the first tick after a trade goes Live_Running, push all original
    leg configs as pending legs if algo_trades.legs is empty.

    This bootstraps the lazy leg system — after this call, the normal
    pending-leg entry loop (§17) picks them up.

    Returns True if legs were queued, False if already present or no configs.
    """
    # live / fast-forward use algo_leg_feature_status for entry tracking.
    # Full leg dicts must never be pushed to legs[] — only string history IDs.
    activation_mode = str(trade.get('activation_mode') or '').strip()
    if activation_mode in {'live', 'fast-forward', 'forward-test'}:
        return False

    existing_legs = [l for l in (trade.get('legs') or []) if isinstance(l, dict)]
    if existing_legs:
        return False

    strategy_cfg = trade.get('strategy') or trade.get('config') or {}
    leg_configs  = strategy_cfg.get('ListOfLegConfigs') or []
    if not leg_configs:
        return False

    trade_id = str(trade.get('_id') or '')
    queued   = 0
    for cfg in leg_configs:
        if not isinstance(cfg, dict):
            continue
        leg_id = str(cfg.get('id') or '').strip()
        if not leg_id:
            continue
        new_leg = build_pending_leg(leg_id, cfg, trade, now_ts)
        if push_new_leg_in_db(db, trade_id, new_leg):
            queued += 1

    if queued:
        log.info('queue_original_legs trade=%s queued=%d', trade_id, queued)
    return queued > 0


def get_pending_legs(trade: dict) -> list[tuple[int, dict]]:
    """
    Return list of (index, leg_dict) for all pending (unentried) legs.

    A leg is pending if entry_trade is None/missing AND status == OPEN.
    Returns index so callers can update legs[index] by position.
    """
    result = []
    for idx, leg in enumerate(trade.get('legs') or []):
        if not isinstance(leg, dict):
            continue
        if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
            continue
        if isinstance(leg.get('entry_trade'), dict):
            continue   # already entered
        result.append((idx, leg))
    return result


def get_open_legs(trade: dict, history_docs: list[dict] | None = None) -> list[dict]:
    """
    Return all open (entered, not yet exited) legs for a trade.

    Merges legs from algo_trades.legs (dict entries) with any legs that
    have been moved to algo_trade_positions_history (string refs).

    If history_docs is None the caller must pass them; this function
    does NOT query the DB to keep it pure.
    """
    leg_ids_seen: set[str] = set()
    result: list[dict] = []

    for leg in (trade.get('legs') or []):
        if not isinstance(leg, dict):
            continue
        if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
            continue
        if not isinstance(leg.get('entry_trade'), dict):
            continue  # pending, not entered
        leg_id = str(leg.get('id') or '')
        if leg_id:
            leg_ids_seen.add(leg_id)
        result.append(leg)

    for hdoc in (history_docs or []):
        if int(hdoc.get('status') or 0) != OPEN_LEG_STATUS:
            continue
        h_leg_id = str(hdoc.get('leg_id') or hdoc.get('id') or '')
        if h_leg_id in leg_ids_seen:
            continue
        result.append(hdoc)

    return result


# ──────────────────────────────────────────────────────────────────────────────
# §15  SQUARE-OFF HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def mark_trade_squared_off(db: MongoData, trade_id: str) -> None:
    """
    Set algo_trades status to SquaredOff when all legs are confirmed closed.

    Sets: status=SquaredOff, trade_status=2, active_on_server=False.
    Does NOT close any legs — call close_leg_in_db() for each leg first.
    """
    try:
        db._db[COL_ALGO_TRADES].update_one(
            {'_id': trade_id},
            {'$set': {
                'active_on_server': False,
                'status':           SQUARED_OFF_STATUS,
                'trade_status':     TRADE_STATUS_SQUARED_OFF,
            }},
        )
    except Exception as exc:
        log.error('mark_trade_squared_off error trade=%s: %s', trade_id, exc)


def square_off_trade(
    db: MongoData,
    trade_rec: dict,
    exit_timestamp: str,
    *,
    market_cache: dict | None = None,
) -> bool:
    """
    Square off ALL open legs in a trade at current LTP.

    Algorithm:
      1. Collect open legs from algo_trades.legs[] (dict entries).
      2. Collect open legs from algo_trade_positions_history (string refs).
      3. For each open leg: fetch chain doc at exit_timestamp, get LTP.
      4. Call close_leg_in_db() at that LTP with reason='squared_off'.
      5. If all legs closed → call mark_trade_squared_off().

    Returns True if trade was marked SquaredOff, False otherwise.

    Used by:
      - Manual square-off from frontend (squared-off socket message)
      - Overall SL/Target hit → _bt_close_remaining()
      - Broker-level SL/Target hit → check_broker_sl_target()
    """
    t_id = str((trade_rec or {}).get('_id') or '').strip()
    if not t_id:
        return False

    algo_col   = db._db[COL_ALGO_TRADES]
    history_col = db._db[COL_POSITIONS_HIST]
    underlying = str(
        trade_rec.get('ticker')
        or (trade_rec.get('config') or {}).get('Ticker')
        or ''
    )
    activation_mode = str(trade_rec.get('activation_mode') or '').strip()
    all_open_closed = True

    def _live_ltp(token: str) -> float:
        """Get current LTP from Dhan ticker for live/fast-forward modes."""
        if activation_mode not in {'live', 'fast-forward', 'forward-test'} or not token:
            return 0.0
        try:
            from features.live_monitor_socket import _get_active_ticker_manager
            return float(_get_active_ticker_manager().ltp_map.get(token) or 0)
        except Exception:
            return 0.0

    # ── Close legs stored as dicts in algo_trades.legs[] ─────────────────
    for idx, leg in enumerate(trade_rec.get('legs') or []):
        if not isinstance(leg, dict):
            continue
        if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
            continue
        if not isinstance(leg.get('entry_trade'), dict):
            continue  # pending, not yet entered

        leg_id      = str(leg.get('id') or '')
        token       = str(leg.get('token') or '')
        expiry      = normalize_expiry(str(leg.get('expiry_date') or ''))
        strike      = leg.get('strike')
        option_type = str(leg.get('option') or '')

        exit_price = _live_ltp(token)
        if not exit_price:
            chain_doc = None
            if token:
                chain_doc = get_chain_by_token_at_time(db, token, exit_timestamp, market_cache)
            if not chain_doc and underlying and expiry and strike and option_type:
                chain_doc = get_chain_at_time(db, underlying, expiry, strike, option_type, exit_timestamp, market_cache)
            exit_price = resolve_chain_price(chain_doc)
        if not exit_price:
            exit_price = safe_float((leg.get('entry_trade') or {}).get('price'))

        close_leg_in_db(db, t_id, idx, exit_price, 'squared_off', exit_timestamp, leg_id=leg_id)
        trade_event_print(f'  [SQUARE OFF] leg={leg_id} token={token} price={exit_price} mode={activation_mode}')

    # ── Close legs already in position history (string refs) ─────────────
    history_open = list(history_col.find({'trade_id': t_id, 'status': OPEN_LEG_STATUS}))
    for hdoc in history_open:
        leg_id      = str(hdoc.get('leg_id') or hdoc.get('id') or '')
        token       = str(hdoc.get('token') or '')
        expiry      = normalize_expiry(str(hdoc.get('expiry_date') or ''))
        strike      = hdoc.get('strike')
        option_type = str(hdoc.get('option') or hdoc.get('option_type') or '')

        exit_price = _live_ltp(token)
        if not exit_price:
            chain_doc = None
            if token:
                chain_doc = get_chain_by_token_at_time(db, token, exit_timestamp, market_cache)
            if not chain_doc and underlying and expiry and strike and option_type:
                chain_doc = get_chain_at_time(db, underlying, expiry, strike, option_type, exit_timestamp, market_cache)
            exit_price = resolve_chain_price(chain_doc)
        if not exit_price:
            exit_price = safe_float((hdoc.get('entry_trade') or {}).get('price'))

        # History legs don't have an array index — use leg_id match
        try:
            history_col.update_one(
                {'trade_id': t_id, 'leg_id': leg_id},
                {'$set': {
                    'exit_trade':     build_exit_trade_payload(exit_price, 'squared_off', exit_timestamp),
                    'last_saw_price': exit_price,
                    'status':         CLOSED_LEG_STATUS,
                }},
            )
        except Exception as exc:
            log.error('square_off_trade history update error leg=%s: %s', leg_id, exc)
            all_open_closed = False

        trade_event_print(f'  [SQUARE OFF] closed leg trade={t_id} leg={leg_id} price={exit_price} ts={exit_timestamp}')

    # ── Verify all legs are now closed ────────────────────────────────────
    refreshed   = algo_col.find_one({'_id': t_id}) or {}
    pending_legs = [
        l for l in (refreshed.get('legs') or [])
        if isinstance(l, dict)
        and int(l.get('status') or 0) == OPEN_LEG_STATUS
        and not isinstance(l.get('exit_trade'), dict)
    ]
    remaining_open_history = list(history_col.find({'trade_id': t_id, 'status': OPEN_LEG_STATUS}))

    if all_open_closed and not pending_legs and not remaining_open_history:
        mark_trade_squared_off(db, t_id)
        trade_event_print(f'[SQUARE OFF] trade={t_id} marked SquaredOff')
        return True

    print(
        f'[SQUARE OFF] trade={t_id} not marked SquaredOff '
        f'pending_exit_legs={len(pending_legs)} '
        f'remaining_history_open_legs={len(remaining_open_history)}'
    )
    return False


# ──────────────────────────────────────────────────────────────────────────────
# §16  TICK PROCESSOR  — main per-minute core loop
# ──────────────────────────────────────────────────────────────────────────────
#
# process_tick() is the heart of the engine.
# It runs once per candle minute for ALL running trades.
#
# Per-trade flow:
#   1. Load open legs (from algo_trades.legs + position history).
#   2. For each open leg:
#        a. Get LTP from chain at now_ts.
#        b. Check leg SL → close + reentry if hit.
#        c. Check leg Target → close + reentry if hit.
#        d. Check Trail SL → update SL if moved.
#   3. After legs loop:
#        a. Compute trade MTM (compute_strategy_mtm).
#        b. Sync overall feature status records.
#        c. Check overall SL → close all + optional reentry.
#        d. Check LockAndTrail → close all if floor breached.
#        e. Check overall Trail SL → update threshold.
#        f. Check overall Target → close all + optional reentry.
# 4. After trades loop:
#        a. Broker-level SL/Target check (check_broker_sl_target).
#
# This function is MODE-AGNOSTIC — it works identically for
# backtest, forward-test, and live execution.
# ─────────────────────────────────────────────────────────────────────────────

def process_tick(
    ctx: TickContext,
    running_trades: list[dict],
) -> TickResult:
    """
    Core per-minute tick processor.

    Runs all feature checks for every open trade and returns a TickResult
    that the caller uses to:
      - Broadcast position updates to the frontend.
      - Emit hit-strategy details to execute-orders socket.
      - Persist audit entries.

    Parameters
    ----------
    ctx:             TickContext with db, trade_date, now_ts, activation_mode.
    running_trades:  list of trade documents from algo_trades (pre-loaded by caller).

    Returns TickResult(actions_taken, hit_trade_ids, hit_ltp_snapshots,
                        open_positions, checked_at).

    Side effects (DB writes):
      - close_leg_in_db      when SL/TP/exit_time fires
      - update_leg_sl_in_db  when trail SL moves
      - push_new_leg_in_db   when reentry legs are queued
      - DB updates for overall SL/Target/LockAndTrail
      - disable_broker_sl_settings when broker SL/Target fires
    """
    open_positions: list[dict]       = []
    strategy_map:   dict[str, dict]  = {}

    now_dt = parse_timestamp(ctx.now_ts)

    for trade in running_trades:
        trade_id   = str(trade.get('_id') or '')
        underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        strategy_cfg     = trade.get('strategy') or trade.get('config') or {}
        all_leg_configs  = resolve_trade_leg_configs(trade)

        # Exit time check — skip all feature checks after configured exit time
        raw_exit_time  = str(trade.get('exit_time') or '')
        exit_time_hhmm = raw_exit_time[11:16] if len(raw_exit_time) >= 16 else raw_exit_time[:5]
        now_hhmm       = ctx.now_ts[11:16] if len(ctx.now_ts) >= 16 else ''
        past_exit      = bool(exit_time_hhmm and now_hhmm and now_hhmm >= exit_time_hhmm)

        # Load legs (dict entries + position history string refs)
        legs = [l for l in (trade.get('legs') or []) if isinstance(l, dict)]
        dict_leg_ids = {str(l.get('id') or '') for l in legs if l.get('id')}
        if any(isinstance(item, str) for item in (trade.get('legs') or [])):
            hist_col = ctx.db._db[COL_POSITIONS_HIST]
            for hdoc in hist_col.find({'trade_id': trade_id}):
                if not isinstance(hdoc.get('entry_trade'), dict) or hdoc.get('exit_trade'):
                    continue
                h_leg_id = str(hdoc.get('leg_id') or hdoc.get('id') or '')
                if not h_leg_id or h_leg_id in dict_leg_ids:
                    continue
                hdoc['expiry_date'] = normalize_expiry(str(hdoc.get('expiry_date') or ''))
                hdoc['id'] = h_leg_id
                hdoc.setdefault('status', OPEN_LEG_STATUS)
                legs.append(hdoc)
        elif not legs:
            hist_col = ctx.db._db[COL_POSITIONS_HIST]
            for hdoc in hist_col.find({'trade_id': trade_id}):
                if not isinstance(hdoc.get('entry_trade'), dict) or hdoc.get('exit_trade'):
                    continue
                h_leg_id = str(hdoc.get('leg_id') or hdoc.get('id') or '')
                if not h_leg_id or h_leg_id in dict_leg_ids:
                    continue
                hdoc['expiry_date'] = normalize_expiry(str(hdoc.get('expiry_date') or ''))
                hdoc['id'] = h_leg_id
                hdoc.setdefault('status', OPEN_LEG_STATUS)
                legs.append(hdoc)

        # print('[BROKER TICK LEGS]', {
        #     'mode': ctx.activation_mode,
        #     'trade_id': trade_id,
        #     'timestamp': ctx.now_ts,
        #     'trade_legs_raw': len(trade.get('legs') or []),
        #     'loaded_open_legs': len(legs),
        #     'leg_ids': [str((leg or {}).get('id') or '') for leg in legs if isinstance(leg, dict)],
        # })

        # ── Per-leg loop ──────────────────────────────────────────────────
        for leg_index, leg in enumerate(legs):
            entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
            if not entry_trade or leg.get('exit_trade'):
                continue  # pending or already closed

            leg_id      = str(leg.get('id') or '')
            leg_cfg     = resolve_leg_cfg(leg_id, leg, all_leg_configs)
            entry_price = safe_float(entry_trade.get('price') or entry_trade.get('trigger_price'))
            lot_size    = safe_int(leg.get('lot_size'), 1)
            lots        = safe_int(leg.get('quantity') or entry_trade.get('quantity'))
            qty         = max(0, lots) * max(1, lot_size)
            expiry      = normalize_expiry(str(leg.get('expiry_date') or ''))
            strike      = leg.get('strike')
            option_type = str(leg.get('option') or '')
            sell_pos    = is_sell(str(leg.get('position') or ''))

            # ── Force-exit at exit_time ───────────────────────────────────
            if past_exit:
                chain_doc  = get_chain_at_time(ctx.db, underlying, expiry, strike, option_type, ctx.now_ts, ctx.market_cache)
                exit_price = resolve_chain_price(chain_doc) or entry_price
                _exit_iv  = safe_float((chain_doc or {}).get('iv')) or None
                _exit_vix = get_vix_at_time(ctx.db, ctx.now_ts, ctx.market_cache) or None
                close_leg_in_db(ctx.db, trade_id, leg_index, exit_price, 'exit_time', ctx.now_ts, leg_id=leg_id, exit_iv=_exit_iv, exit_vix=_exit_vix)
                ctx.actions_taken.append(f'{trade_id}/{leg_id}: exit_time @ {exit_price}')
                continue

            # ── Get current LTP ───────────────────────────────────────────
            chain_doc     = get_chain_at_time(ctx.db, underlying, expiry, strike, option_type, ctx.now_ts, ctx.market_cache)
            current_price = resolve_chain_price(chain_doc)
            if not current_price:
                continue  # no data for this candle

            _tick_iv  = safe_float((chain_doc or {}).get('iv')) or None
            _tick_vix = get_vix_at_time(ctx.db, ctx.now_ts, ctx.market_cache) or None

            pnl = ((entry_price - current_price) if sell_pos else (current_price - entry_price)) * qty

            # ── SL check ─────────────────────────────────────────────────
            stored_sl = safe_float(leg.get('current_sl_price') or leg.get('sl_price') or 0) or None
            sl_hit, sl_price = check_leg_sl(leg_cfg, entry_price, current_price, stored_sl, sell_pos)
            if sl_hit:
                close_leg_in_db(ctx.db, trade_id, leg_index, current_price, 'stoploss', ctx.now_ts, leg_id=leg_id, exit_iv=_tick_iv, exit_vix=_tick_vix)
                ctx.actions_taken.append(f'{trade_id}/{leg_id}: SL hit @ {current_price}')
                result = handle_leg_reentry(ctx.db, trade, leg, leg_cfg, 'stoploss', ctx.now_ts)
                if result:
                    ctx.actions_taken.append(result)
                continue

            # ── Target check ──────────────────────────────────────────────
            stored_tp = safe_float(leg.get('current_tp_price') or leg.get('tp_price') or 0) or None
            tp_hit, tp_price = check_leg_target(leg_cfg, entry_price, current_price, stored_tp, sell_pos)
            if tp_hit:
                close_leg_in_db(ctx.db, trade_id, leg_index, current_price, 'target', ctx.now_ts, leg_id=leg_id, exit_iv=_tick_iv, exit_vix=_tick_vix)
                ctx.actions_taken.append(f'{trade_id}/{leg_id}: TP hit @ {current_price}')
                result = handle_leg_reentry(ctx.db, trade, leg, leg_cfg, 'target', ctx.now_ts)
                if result:
                    ctx.actions_taken.append(result)
                continue

            # ── Trail SL update ───────────────────────────────────────────
            if stored_sl:
                new_sl = compute_next_trail_sl(leg_cfg, entry_price, current_price, stored_sl, sell_pos)
                if new_sl != stored_sl:
                    update_leg_sl_in_db(ctx.db, trade_id, leg_index, new_sl, current_price, leg_id=leg_id)
                    ctx.actions_taken.append(f'{trade_id}/{leg_id}: trail SL {stored_sl}→{new_sl}')

            # ── Accumulate open position snapshot ────────────────────────
            strat_entry = strategy_map.setdefault(trade_id, {
                'trade_id': trade_id, 'open_positions': [], 'total_pnl': 0.0,
            })
            strat_entry['open_positions'].append({
                'leg_id':      leg_id,
                'pnl':         round(pnl, 2),
                'ltp':         current_price,
                'current_price': current_price,
                'entry_price': entry_price,
                'quantity':    qty,
            })
            strat_entry['total_pnl'] = round(strat_entry['total_pnl'] + pnl, 2)
            open_positions.append({'trade_id': trade_id, 'leg_id': leg_id, 'ltp': current_price, 'pnl': round(pnl, 2)})

        # ── Overall SL / Target / Trail / LockAndTrail ────────────────────
        if not past_exit:
            strat_entry = strategy_map.get(trade_id)
            open_leg_ids = {p['leg_id'] for p in (strat_entry or {}).get('open_positions', [])}
            current_mtm, legs_snapshot = compute_strategy_mtm(
                ctx.db, trade_id, ctx.now_ts,
                (strat_entry or {}).get('open_positions', []),
            )
            ctx.trade_mtm_map[trade_id] = current_mtm

            peak_mtm  = max(safe_float(trade.get('peak_mtm'), current_mtm), current_mtm)
            ore_done  = safe_int(trade.get('overall_sl_reentry_done'))
            ort_done  = safe_int(trade.get('overall_tgt_reentry_done'))
            dyn_sl    = safe_float(trade.get('current_overall_sl_threshold'))
            eff_sl    = safe_float(resolve_overall_cycle_value(parse_overall_sl(strategy_cfg)[1], ore_done))
            eff_tgt   = safe_float(resolve_overall_cycle_value(parse_overall_tgt(strategy_cfg)[1], ort_done))
            last_overall_event_at = str(trade.get('last_overall_event_at') or '').strip().replace(' ', 'T')[:19]
            last_overall_event_reason = str(trade.get('last_overall_event_reason') or '').strip()
            skip_same_tick_overall = bool(
                open_leg_ids
                and last_overall_event_at
                and last_overall_event_at == str(ctx.now_ts or '').strip()[:19]
                and last_overall_event_reason in {'overall_sl', 'overall_target'}
            )

            sync_overall_feature_status(ctx.db, trade, ctx.now_ts,
                                        current_mtm=current_mtm,
                                        overall_sl_done=ore_done,
                                        overall_tgt_done=ort_done)

            if SHOW_PRINT_STATEMENT:
                print('[OVERALL CHECK]', {
                    'mode':           ctx.activation_mode,
                    'trade_id':       trade_id,
                    'timestamp':      ctx.now_ts,
                    'current_mtm':    round(current_mtm, 2),
                    'overall_sl':     eff_sl,
                    'overall_target': eff_tgt,
                    'dynamic_sl':     dyn_sl,
                })
                print('[OVERALL SL DEBUG]', {
                    'mode': ctx.activation_mode,
                    'trade_id': trade_id,
                    'timestamp': ctx.now_ts,
                    'current_mtm': round(current_mtm, 2),
                    'base_overall_sl': round(safe_float(parse_overall_sl(strategy_cfg)[1]), 2),
                    'reentry_done': ore_done,
                    'cycle_number': ore_done + 1,
                    'cycle_overall_sl': round(eff_sl, 2),
                    'dynamic_sl_threshold': round(safe_float(dyn_sl), 2),
                    'checked_stoploss': round(safe_float(dyn_sl or eff_sl), 2),
                    'would_hit_cycle_sl': bool(eff_sl > 0 and current_mtm <= -eff_sl),
                    'would_hit_dynamic_sl': bool(dyn_sl > 0 and current_mtm <= -dyn_sl),
                    'skip_same_tick_overall': skip_same_tick_overall,
                    'last_overall_event_at': last_overall_event_at,
                    'last_overall_event_reason': last_overall_event_reason,
                })

            if skip_same_tick_overall:
                ctx.actions_taken.append(
                    f'{trade_id}: skipped same-tick overall recheck after {last_overall_event_reason}'
                )
                continue

            # Helper: close all open legs and return True
            def _close_all(reason: str) -> None:
                for p in list((strat_entry or {}).get('open_positions', [])):
                    _lid = p.get('leg_id', '')
                    for i, l in enumerate(trade.get('legs') or []):
                        if isinstance(l, dict) and str(l.get('id') or '') == _lid:
                            close_leg_in_db(ctx.db, trade_id, i, p.get('ltp', 0), reason, ctx.now_ts, leg_id=_lid)
                # Also close history legs (match by entry_trade present + no exit_trade)
                for hdoc in ctx.db._db[COL_POSITIONS_HIST].find({'trade_id': trade_id}):
                    if not isinstance(hdoc.get('entry_trade'), dict) or hdoc.get('exit_trade'):
                        continue
                    h_lid = str(hdoc.get('leg_id') or '')
                    ltp   = safe_float(hdoc.get('last_saw_price'))
                    ctx.db._db[COL_POSITIONS_HIST].update_one(
                        {'trade_id': trade_id, 'leg_id': h_lid},
                        {'$set': {
                            'exit_trade':     build_exit_trade_payload(ltp, reason, ctx.now_ts),
                            'status':         CLOSED_LEG_STATUS,
                            'last_saw_price': ltp,
                        }},
                    )

            # ── 1. Overall SL ─────────────────────────────────────────────
            osl_hit = (eff_sl > 0 and current_mtm <= -eff_sl) or (dyn_sl > 0 and current_mtm <= -dyn_sl)
            if osl_hit and not open_leg_ids:
                osl_hit = False  # no open legs to close — guard against duplicate events

            if osl_hit:
                _close_all('overall_sl')
                ctx.actions_taken.append(f'{trade_id}: overall SL hit mtm={current_mtm}')
                ctx.hit_trade_ids.append(trade_id)
                ctx.hit_ltp_snapshots[trade_id] = list((strategy_map.get(trade_id) or {}).get('open_positions') or [])
                # Reentry
                ore_type, ore_count = parse_overall_reentry_sl(strategy_cfg)
                if ore_type != 'None' and ore_count > 0 and ore_done < ore_count:
                    next_cycle_sl = resolve_overall_cycle_value(
                        parse_overall_sl(strategy_cfg)[1],
                        ore_done + 1,
                    )
                    ctx.db._db[COL_ALGO_TRADES].update_one(
                        {'_id': trade_id},
                        {'$set': {
                            'overall_sl_reentry_done': ore_done + 1,
                            'peak_mtm': 0.0,
                            'current_overall_sl_threshold': next_cycle_sl,
                            'last_overall_event_at': ctx.now_ts,
                            'last_overall_event_reason': 'overall_sl',
                        }},
                    )
                    refreshed_trade = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': trade_id}) or trade
                    print('[OVERALL REENTRY PREPARE]', {
                        'mode': ctx.activation_mode,
                        'trade_id': trade_id,
                        'reason': 'overall_sl',
                        'timestamp': ctx.now_ts,
                        'base_overall_sl': round(safe_float(parse_overall_sl(strategy_cfg)[1]), 2),
                        'updated_reentry_done': ore_done + 1,
                        'updated_cycle_number': ore_done + 2,
                        'updated_overall_sl': round(safe_float(next_cycle_sl), 2),
                        'current_overall_sl_threshold': round(safe_float((refreshed_trade or {}).get('current_overall_sl_threshold')), 2),
                        'reentry_type': ore_type,
                    })
                    new_ids = requeue_all_legs_for_overall_reentry(
                        ctx.db, refreshed_trade, trade_id, strategy_cfg, ctx.now_ts,
                        reentry_type=ore_type,
                    )
                    if new_ids:
                        refreshed_trade = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': trade_id}) or refreshed_trade
                        immediate_entries = process_pending_entries(ctx, [refreshed_trade])
                        print('[OVERALL REENTRY ENTRY CHECK]', {
                            'mode': ctx.activation_mode,
                            'trade_id': trade_id,
                            'reason': 'overall_sl',
                            'timestamp': ctx.now_ts,
                            'queued_leg_ids': new_ids,
                            'immediate_entries': len(immediate_entries),
                            'entry_condition': 'same_tick_process_pending_entries',
                            'updated_overall_sl': round(safe_float(next_cycle_sl), 2),
                            'current_overall_sl_threshold': round(safe_float((refreshed_trade or {}).get('current_overall_sl_threshold')), 2),
                            'check_message': 'reentry uses updated overall SL before immediate entry attempt',
                        })
                        ctx.db._db[COL_ALGO_TRADES].update_one(
                            {'_id': trade_id},
                            {'$set': {'last_overall_event_at': ctx.now_ts, 'last_overall_event_reason': 'overall_sl'}},
                        )
                        ctx.actions_taken.append(
                            f'{trade_id}: overall SL reentry queued legs={new_ids} '
                            f'immediate_entries={len(immediate_entries)}'
                        )
                    else:
                        square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                else:
                    square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                continue

            # ── 2. Overall Target ─────────────────────────────────────────
            if open_leg_ids and eff_tgt > 0 and current_mtm >= eff_tgt:
                _close_all('overall_target')
                ctx.actions_taken.append(f'{trade_id}: overall Target hit mtm={current_mtm}')
                ctx.hit_trade_ids.append(trade_id)
                ctx.hit_ltp_snapshots[trade_id] = list((strategy_map.get(trade_id) or {}).get('open_positions') or [])
                ort_type, ort_count = parse_overall_reentry_tgt(strategy_cfg)
                if ort_type != 'None' and ort_count > 0 and ort_done < ort_count:
                    reset_sl_threshold = resolve_overall_cycle_value(
                        parse_overall_sl(strategy_cfg)[1],
                        ore_done,
                    )
                    ctx.db._db[COL_ALGO_TRADES].update_one(
                        {'_id': trade_id},
                        {'$set': {
                            'overall_tgt_reentry_done': ort_done + 1,
                            'peak_mtm': 0.0,
                            'current_overall_sl_threshold': reset_sl_threshold,
                            'last_overall_event_at': ctx.now_ts,
                            'last_overall_event_reason': 'overall_target',
                        }},
                    )
                    refreshed_trade = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': trade_id}) or trade
                    print('[OVERALL REENTRY PREPARE]', {
                        'mode': ctx.activation_mode,
                        'trade_id': trade_id,
                        'reason': 'overall_target',
                        'timestamp': ctx.now_ts,
                        'base_overall_sl': round(safe_float(parse_overall_sl(strategy_cfg)[1]), 2),
                        'updated_reentry_done': ort_done + 1,
                        'updated_cycle_number': ort_done + 2,
                        'updated_overall_sl': round(safe_float(reset_sl_threshold), 2),
                        'current_overall_sl_threshold': round(safe_float((refreshed_trade or {}).get('current_overall_sl_threshold')), 2),
                        'reentry_type': ort_type,
                    })
                    new_ids = requeue_all_legs_for_overall_reentry(
                        ctx.db, refreshed_trade, trade_id, strategy_cfg, ctx.now_ts,
                        reentry_type=ort_type,
                    )
                    if new_ids:
                        refreshed_trade = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': trade_id}) or refreshed_trade
                        immediate_entries = process_pending_entries(ctx, [refreshed_trade])
                        print('[OVERALL REENTRY ENTRY CHECK]', {
                            'mode': ctx.activation_mode,
                            'trade_id': trade_id,
                            'reason': 'overall_target',
                            'timestamp': ctx.now_ts,
                            'queued_leg_ids': new_ids,
                            'immediate_entries': len(immediate_entries),
                            'entry_condition': 'same_tick_process_pending_entries',
                            'updated_overall_sl': round(safe_float(reset_sl_threshold), 2),
                            'current_overall_sl_threshold': round(safe_float((refreshed_trade or {}).get('current_overall_sl_threshold')), 2),
                            'check_message': 'reentry uses updated overall SL before immediate entry attempt',
                        })
                        ctx.db._db[COL_ALGO_TRADES].update_one(
                            {'_id': trade_id},
                            {'$set': {'last_overall_event_at': ctx.now_ts, 'last_overall_event_reason': 'overall_target'}},
                        )
                        ctx.actions_taken.append(
                            f'{trade_id}: overall Target reentry queued legs={new_ids} '
                            f'immediate_entries={len(immediate_entries)}'
                        )
                    else:
                        square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                else:
                    square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                continue

            # ── 3. LockAndTrail ───────────────────────────────────────────
            if open_leg_ids:
                lock_cfg = parse_lock_and_trail(strategy_cfg)
                lock_exit, lock_floor = check_lock_and_trail(lock_cfg, current_mtm, peak_mtm)
                if lock_exit:
                    _close_all('lock_and_trail')
                    square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                    ctx.actions_taken.append(f'{trade_id}: LockAndTrail exit mtm={current_mtm} floor={lock_floor}')
                    continue

            # ── 4. Overall Trail SL threshold update ──────────────────────
            if open_leg_ids:
                new_thresh = compute_next_overall_trail_sl(strategy_cfg, dyn_sl or eff_sl, safe_float(parse_overall_sl(strategy_cfg)[1]), peak_mtm)
                if new_thresh != (dyn_sl or eff_sl):
                    ctx.db._db[COL_ALGO_TRADES].update_one(
                        {'_id': trade_id},
                        {'$set': {'peak_mtm': peak_mtm, 'current_overall_sl_threshold': new_thresh}},
                    )
                    ctx.actions_taken.append(f'{trade_id}: overall trail SL {dyn_sl}→{new_thresh}')
                elif peak_mtm > safe_float(trade.get('peak_mtm')):
                    ctx.db._db[COL_ALGO_TRADES].update_one({'_id': trade_id}, {'$set': {'peak_mtm': peak_mtm}})

        elif past_exit:
            mark_trade_squared_off(ctx.db, trade_id)

    # ── Broker-level SL/Target check ─────────────────────────────────────
    broker_hit_ids, broker_ltp_snaps = check_broker_sl_target(
        ctx, running_trades, strategy_map, []
    )
    for tid in broker_hit_ids:
        if tid not in ctx.hit_trade_ids:
            ctx.hit_trade_ids.append(tid)
    ctx.hit_ltp_snapshots.update(broker_ltp_snaps)

    return TickResult(
        actions_taken=ctx.actions_taken,
        hit_trade_ids=ctx.hit_trade_ids,
        hit_ltp_snapshots=ctx.hit_ltp_snapshots,
        open_positions=open_positions,
        checked_at=ctx.now_ts,
    )


# ──────────────────────────────────────────────────────────────────────────────
# §17  ENTRY PROCESSOR  — pending leg resolution
# ──────────────────────────────────────────────────────────────────────────────
#
# resolve_pending_leg_entry() attempts to fill a single pending leg on the
# current candle.  It is called for every pending leg every tick.
#
# Entry conditions checked in order:
#   1. entry_time  – not before strategy.entry_time (HH:MM)
#   2. Expiry      – resolve_leg_expiry() must succeed
#   3. Strike      – resolve_leg_strike() must succeed
#   4. Chain price – must be > 0
#   5. Momentum    – if configured, trigger must be reached first
#
# On success:
#   - entry_trade is set on the leg in algo_trades
#   - algo_trade_positions_history record is created
#   - SL/TP feature records are seeded (via notification_manager)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_pending_leg_entry(
    ctx: TickContext,
    trade: dict,
    leg: dict,
    leg_index: int,
) -> dict | None:
    """
    Attempt to enter a single pending leg at the current candle.

    Returns the entry_trade dict if entry succeeded, None otherwise.

    The caller (process_pending_entries or _execute_backtest_entries) should:
      - On success: push entry data to DB + create position history record.
      - On None: skip this leg and try again next tick.

    This function is READ-ONLY with respect to the DB — it only computes
    whether entry should happen and what the entry price would be.
    The caller writes the actual DB updates.
    """
    trade_id    = str(trade.get('_id') or '')
    leg_id      = str(leg.get('id') or '')
    all_configs = resolve_trade_leg_configs(trade)
    leg_cfg     = resolve_leg_cfg(leg_id, leg, all_configs)
    underlying  = str(
        (trade.get('config') or {}).get('Ticker')
        or (trade.get('strategy') or {}).get('Ticker')
        or trade.get('ticker')
        or ''
    )
    option_type = str(leg.get('option') or leg_cfg.get('OptionType') or 'CE')
    sell_pos    = is_sell(str(leg.get('position') or leg_cfg.get('Position') or ''))

    print(
        f'[ENTRY CHECK] trade_id={trade_id} '
        f'leg_id={leg_id or "-"} '
        f'mode={ctx.activation_mode} '
        f'now_ts={ctx.now_ts} '
        f'underlying={underlying or "-"} '
        f'option_type={option_type or "-"} '
        f'leg_index={leg_index}'
    )

    # 1. Entry time gate
    _entry_time_raw = str(
        (trade.get('config') or {}).get('entry_time')
        or trade.get('entry_time')
        or ''
    ).strip()
    entry_time = _entry_time_raw[11:16] if len(_entry_time_raw) >= 16 else _entry_time_raw[:5]
    if entry_time and ctx.now_ts[11:16] < entry_time:
        print(
            f'[ENTRY SKIP] trade_id={trade_id} '
            f'leg_id={leg_id or "-"} '
            f'reason=too_early '
            f'entry_time={entry_time} '
            f'current_time={ctx.now_ts[11:16]}'
        )
        return {
            '__skip__': True,
            'reason': 'too_early',
            'message': f'Waiting for entry time {entry_time} (current {ctx.now_ts[11:16]})',
            'is_blocking': False,
        }

    # 2. Resolve expiry
    expiry = resolve_leg_expiry(ctx.db, leg_cfg, underlying, ctx.now_ts, ctx.market_cache)
    if not expiry:
        print(
            f'[ENTRY SKIP] trade_id={trade_id} '
            f'leg_id={leg_id or "-"} '
            f'reason=expiry_missing '
            f'underlying={underlying or "-"} '
            f'now_ts={ctx.now_ts}'
        )
        return {
            '__skip__': True,
            'reason': 'expiry_missing',
            'message': f'Could not resolve option expiry for {underlying or "underlying"}',
            'is_blocking': True,
        }

    # 3. Resolve spot & strike
    spot = get_spot_at_time(ctx.db, underlying, ctx.now_ts, ctx.market_cache)
    if not spot:
        print(
            f'[ENTRY SKIP] trade_id={trade_id} '
            f'leg_id={leg_id or "-"} '
            f'reason=spot_missing '
            f'underlying={underlying or "-"} '
            f'expiry={expiry}'
        )
        return {
            '__skip__': True,
            'reason': 'spot_missing',
            'message': f'Spot price for {underlying or "underlying"} not available yet',
            'is_blocking': True,
        }

    strike = resolve_leg_strike(ctx.db, leg_cfg, underlying, expiry, option_type, spot, ctx.now_ts, ctx.market_cache)
    if not strike:
        print(
            f'[ENTRY SKIP] trade_id={trade_id} '
            f'leg_id={leg_id or "-"} '
            f'reason=strike_missing '
            f'underlying={underlying or "-"} '
            f'expiry={expiry} '
            f'spot={spot} '
            f'option_type={option_type or "-"}'
        )
        return {
            '__skip__': True,
            'reason': 'strike_missing',
            'message': f'Could not resolve strike for {underlying or "underlying"} {option_type} (spot {spot})',
            'is_blocking': True,
        }

    # 4. Fetch chain doc → entry price
    chain_doc = normalize_chain_fields(
        get_chain_at_time(ctx.db, underlying, expiry, strike, option_type, ctx.now_ts, ctx.market_cache) or {}
    )
    entry_price = resolve_chain_price(chain_doc)
    if not entry_price:
        print(
            f'[ENTRY SKIP] trade_id={trade_id} '
            f'leg_id={leg_id or "-"} '
            f'reason=chain_price_missing '
            f'underlying={underlying or "-"} '
            f'expiry={expiry} '
            f'strike={strike} '
            f'option_type={option_type or "-"} '
            f'chain_keys={list((chain_doc or {}).keys())}'
        )
        return {
            '__skip__': True,
            'reason': 'chain_price_missing',
            'message': f'Option price not available for {underlying or "underlying"} {strike} {option_type} (expiry {expiry})',
            'is_blocking': True,
        }  # contract not listed / no data

    # 5. Momentum gate (if configured)
    if has_momentum_config(leg_cfg):
        momentum_cfg    = leg_cfg.get('LegMomentum') or {}
        momentum_type   = str(momentum_cfg.get('Type') or '').strip()
        momentum_value  = safe_float(momentum_cfg.get('Value') or 0)
        stored_base     = safe_float(leg.get('momentum_base_price') or 0)
        stored_target   = safe_float(leg.get('momentum_target_price') or 0)

        if not stored_base:
            # Arm momentum — store base price for first time
            # (return None this tick; caller will write base_price; trigger next tick)
            print(
                f'[ENTRY WAIT] trade_id={trade_id} '
                f'leg_id={leg_id or "-"} '
                f'reason=momentum_base_missing '
                f'base_price={entry_price} '
                f'momentum_type={momentum_type or "-"} '
                f'momentum_value={momentum_value}'
            )
            return {'__arm_momentum__': True, 'base_price': entry_price, 'momentum_type': momentum_type, 'momentum_value': momentum_value}

        target_price = stored_target or compute_momentum_target(momentum_type, stored_base, momentum_value)
        if not is_momentum_triggered(momentum_type, entry_price, target_price or 0):
            print(
                f'[ENTRY WAIT] trade_id={trade_id} '
                f'leg_id={leg_id or "-"} '
                f'reason=momentum_not_triggered '
                f'momentum_type={momentum_type or "-"} '
                f'base_price={stored_base} '
                f'target_price={target_price} '
                f'current_price={entry_price}'
            )
            return {
                '__skip__': True,
                'reason': 'momentum_not_triggered',
                'message': f'Waiting for momentum trigger ({momentum_type} {momentum_value}, current {entry_price} vs target {target_price})',
                'is_blocking': False,
            }  # waiting for momentum trigger

    # ── Entry approved — build entry_trade ───────────────────────────────
    lot_size  = safe_int(leg_cfg.get('LotSize') or leg.get('lot_size') or 1)
    quantity  = safe_int(leg_cfg.get('Quantity') or leg.get('quantity') or 1)
    entry_iv  = safe_float((chain_doc or {}).get('iv')) or None
    entry_vix = get_vix_at_time(ctx.db, ctx.now_ts, ctx.market_cache) or None

    print(f'[STRIKE CALC] leg={leg_id} type={option_type} spot={spot} strike={strike} close={entry_price} iv={entry_iv} vix={entry_vix}')
    log.warning(
        '[ENTRY IV/VIX] leg=%s mode=%s chain_doc_iv=%s entry_iv=%s entry_vix=%s chain_doc_keys=%s',
        leg_id, ctx.activation_mode,
        safe_float((chain_doc or {}).get('iv')),
        entry_iv, entry_vix,
        list((chain_doc or {}).keys()),
    )

    return {
        'trigger_timestamp':  ctx.now_ts,
        'trigger_price':      entry_price,
        'price':              entry_price,
        'traded_timestamp':   ctx.now_ts,
        'exchange_timestamp': ctx.now_ts,
        'spot_price':         spot,
        'expiry':             expiry,
        'strike':             strike,
        'option_type':        option_type,
        'quantity':           quantity,
        'lot_size':           lot_size,
        'entry_iv':           entry_iv,
        'entry_vix':          entry_vix,
        'ltp':                entry_price,
        'chain_timestamp':    str((chain_doc or {}).get('timestamp') or ctx.now_ts),
        'token':              str((chain_doc or {}).get('token') or ''),
        'instrument_token':   str((chain_doc or {}).get('token') or ''),
        'symbol':             str((chain_doc or {}).get('symbol') or ''),
        'exchange':           str((chain_doc or {}).get('exchange') or ''),
    }


def _build_position_history_doc_for_entry(trade: dict, leg: dict) -> dict | None:
    entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
    entry_timestamp = str(
        entry_trade.get('traded_timestamp')
        or entry_trade.get('trigger_timestamp')
        or ''
    ).strip()
    if not entry_timestamp:
        return None

    strategy_cfg = trade.get('strategy') or trade.get('config') or {}
    return {
        'trade_id': str(trade.get('_id') or ''),
        'strategy_id': str(trade.get('strategy_id') or ''),
        'strategy_name': str(trade.get('name') or ''),
        'group_name': str(((trade.get('portfolio') or {}).get('group_name') or '')),
        'ticker': str(strategy_cfg.get('Ticker') or trade.get('ticker') or ''),
        'creation_ts': str(trade.get('creation_ts') or ''),
        'entry_timestamp': entry_timestamp,
        'history_type': 'position_entry',
        'created_at': now_iso(),
        'leg_id': str(leg.get('id') or ''),
        'id': str(leg.get('id') or ''),
        'status': int(leg.get('status') or OPEN_LEG_STATUS),
        'token': leg.get('token'),
        'symbol': leg.get('symbol'),
        'quantity': safe_int(leg.get('quantity')),
        'lot_size': safe_int(leg.get('lot_size') or leg.get('quantity')),
        'position': leg.get('position'),
        'option': leg.get('option'),
        'expiry_date': str(leg.get('expiry_date') or '')[:10] or None,
        'strike': leg.get('strike'),
        'last_saw_price': safe_float(leg.get('last_saw_price')),
        'entry_trade': entry_trade,
        'exit_trade': leg.get('exit_trade') if leg.get('exit_trade') is not None else None,
        'is_reentered_leg': bool(leg.get('is_reentered_leg') or leg.get('triggered_by')),
        'transactions': leg.get('transactions') or {},
        'current_transaction_id': leg.get('current_transaction_id'),
        'initial_sl_value': leg.get('initial_sl_value', leg.get('current_sl_price')),
        'display_sl_value': leg.get('display_sl_value', leg.get('current_sl_price')),
        'display_target_value': leg.get('display_target_value'),
        'leg_type': str(leg.get('leg_type') or ''),
        'lot_config_value': safe_int(leg.get('lot_config_value') or 1),
        'momentum_base_price': safe_float(leg.get('momentum_base_price')) or None,
        'momentum_target_price': safe_float(leg.get('momentum_target_price')) or None,
        'momentum_reference_set_at': str(leg.get('momentum_reference_set_at') or ''),
        'momentum_triggered_at': str(leg.get('momentum_triggered_at') or ''),
    }


def _store_position_history_for_entry(
    db: MongoData,
    trade: dict,
    leg: dict,
) -> tuple[bool, str | None]:
    history_doc = _build_position_history_doc_for_entry(trade, leg)
    if not history_doc:
        return False, None

    history_col = db._db[COL_POSITIONS_HIST]
    duplicate_query = {
        'trade_id': history_doc['trade_id'],
        'leg_id': history_doc['leg_id'],
        'entry_timestamp': history_doc['entry_timestamp'],
    }
    existing = history_col.find_one(duplicate_query, {'_id': 1})
    if existing:
        existing_id = str(existing.get('_id') or '').strip() or None
        return False, existing_id

    result = history_col.insert_one(history_doc)
    inserted_id = str(result.inserted_id)
    try:
        history_col.update_one(
            {'_id': result.inserted_id},
            {'$set': {'id': inserted_id}},
        )
    except Exception as exc:
        log.warning('position history id sync error trade=%s leg=%s: %s', history_doc['trade_id'], history_doc['leg_id'], exc)

    leg_id = str(leg.get('id') or '').strip()
    try:
        db._db[COL_ALGO_TRADES].update_one(
            {'_id': history_doc['trade_id']},
            {
                '$pull': {'legs': {'id': leg_id}},
                '$push': {'legs': inserted_id},
            },
        )
    except Exception as exc:
        log.error('position history leg replace error trade=%s leg=%s: %s', history_doc['trade_id'], leg_id, exc)
        return False, None

    print(
        f'[POSITION HISTORY STORE] trade={history_doc["trade_id"]} '
        f'leg={history_doc["leg_id"]} history_id={inserted_id}'
    )
    return True, inserted_id


def _record_entry_skip_reason(
    ctx: TickContext,
    trade_id: str,
    leg_id: str,
    reason: str,
    message: str,
) -> None:
    """
    Persist why a pending leg's entry couldn't be taken this tick, so the
    strategy row in the UI can surface it. Only called for blocking reasons
    (missing expiry/spot/strike/chain price) — waiting states are not stored.
    """
    try:
        ctx.db._db[COL_ALGO_TRADES].update_one(
            {'_id': trade_id},
            {'$set': {
                'entry_error': {
                    'leg_id':  leg_id,
                    'reason':  reason,
                    'message': message,
                    'at':      ctx.now_ts,
                },
            }},
        )
    except Exception as exc:
        log.warning('entry skip reason write error trade=%s leg=%s: %s', trade_id, leg_id, exc)
        return

    try:
        from features.execution_socket import mark_execute_order_dirty_from_trade_id  # type: ignore
        mark_execute_order_dirty_from_trade_id(ctx.db, trade_id)
    except Exception:
        pass


def process_pending_entries(
    ctx: TickContext,
    running_trades: list[dict],
) -> list[dict]:
    """
    Attempt to fill all pending legs across all running trades.

    Iterates every running trade, finds pending legs via get_pending_legs(),
    calls resolve_pending_leg_entry() for each.

    On success for each leg:
      - Writes entry_trade to algo_trades.legs[index]
      - Creates position history record in algo_trade_positions_history
      - Seeds feature status records via notification_manager

    Returns list of entry records (for broadcast / audit trail).

    Called before process_tick() in the backtest/live loop so that
    newly entered legs are immediately visible to the SL/Target checks.
    """
    entries_executed: list[dict] = []

    for trade in running_trades:
        trade_id    = str(trade.get('_id') or '')
        underlying  = str(
            (trade.get('config') or {}).get('Ticker')
            or (trade.get('strategy') or {}).get('Ticker')
            or trade.get('ticker')
            or ''
        )
        print(
            f'[ENTRY FLOW] trade_id={trade_id} '
            f'mode={ctx.activation_mode} '
            f'now_ts={ctx.now_ts} '
            f'underlying={underlying or "-"} '
            f'legs_count={len(trade.get("legs") or [])}'
        )

        # Bootstrap: queue original legs only when legs array is completely empty.
        # String IDs (history refs) count as existing — do not re-bootstrap.
        if not trade.get('legs'):
            queue_original_legs_if_needed(ctx.db, trade, ctx.now_ts)
            # Reload trade to get the newly pushed legs
            trade = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': trade_id}) or trade
            print(
                f'[ENTRY FLOW] trade_id={trade_id} '
                f'state=legs_bootstrapped '
                f'legs_count={len(trade.get("legs") or [])}'
            )

        pending_legs = get_pending_legs(trade)
        print(
            f'[ENTRY FLOW] trade_id={trade_id} '
            f'state=pending_legs_loaded '
            f'pending_count={len(pending_legs)}'
        )

        for leg_index, leg in pending_legs:
            result = resolve_pending_leg_entry(ctx, trade, leg, leg_index)
            if result is None:
                continue

            # Skip with a reason — persist reasons that represent a real
            # blocker (missing expiry/spot/strike/chain price) so the UI can
            # show why entry hasn't been taken. Waiting states (too_early,
            # momentum not yet triggered) are expected and not persisted.
            if result.get('__skip__'):
                if result.get('is_blocking'):
                    _record_entry_skip_reason(
                        ctx, trade_id, str(leg.get('id') or ''),
                        str(result.get('reason') or ''), str(result.get('message') or ''),
                    )
                continue

            # Handle momentum arming (set base_price, defer actual entry)
            if result.get('__arm_momentum__'):
                try:
                    ctx.db._db[COL_ALGO_TRADES].update_one(
                        {'_id': trade_id},
                        {'$set': {
                            f'legs.{leg_index}.momentum_base_price':  result['base_price'],
                            f'legs.{leg_index}.momentum_type':        result['momentum_type'],
                            f'legs.{leg_index}.momentum_value':       result['momentum_value'],
                            f'legs.{leg_index}.momentum_reference_set_at': ctx.now_ts,
                        }},
                    )
                    print(f'[MOMENTUM ARMED] leg={str(leg.get("id") or "")} base={result["base_price"]}')
                except Exception as exc:
                    log.error('momentum arm error leg=%s: %s', leg.get('id'), exc)
                continue

            entry_trade = result
            leg_id      = str(leg.get('id') or '')

            # Write entry_trade to algo_trades. Only clear entry_error if it was
            # reported against THIS leg — a sibling leg that's still blocked must
            # keep its error visible, not have it wiped by an unrelated leg's
            # successful entry.
            try:
                update_ops: dict = {
                    '$set': {
                        f'legs.{leg_index}.entry_trade':    entry_trade,
                        f'legs.{leg_index}.expiry_date':    entry_trade.get('expiry'),
                        f'legs.{leg_index}.strike':         entry_trade.get('strike'),
                        f'legs.{leg_index}.last_saw_price': entry_trade.get('price'),
                        f'legs.{leg_index}.token':          entry_trade.get('token') or entry_trade.get('instrument_token'),
                        f'legs.{leg_index}.symbol':         entry_trade.get('symbol'),
                        f'legs.{leg_index}.exchange':       entry_trade.get('exchange'),
                    },
                }
                _current_trade_doc = ctx.db._db[COL_ALGO_TRADES].find_one(
                    {'_id': trade_id}, {'entry_error': 1},
                ) or {}
                if str((_current_trade_doc.get('entry_error') or {}).get('leg_id') or '') == leg_id:
                    update_ops['$unset'] = {'entry_error': ''}
                ctx.db._db[COL_ALGO_TRADES].update_one({'_id': trade_id}, update_ops)
            except Exception as exc:
                log.error('write entry_trade error trade=%s leg=%s: %s', trade_id, leg_id, exc)
                continue

            # Seed position history + feature status records
            try:
                from features.notification_manager import record_entry_taken, record_leg_features_at_entry  # type: ignore
                record_entry_taken(ctx.db._db, trade, leg, entry_trade, ctx.now_ts)
                record_leg_features_at_entry(ctx.db._db, trade, leg, entry_trade, ctx.now_ts)
            except Exception as exc:
                log.warning('record_entry_taken error leg=%s: %s', leg_id, exc)

            try:
                refreshed_trade = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': trade_id}) or trade
                refreshed_legs = [
                    item for item in (refreshed_trade.get('legs') or [])
                    if isinstance(item, dict)
                ]
                refreshed_leg = next(
                    (item for item in refreshed_legs if str(item.get('id') or '') == leg_id),
                    None,
                )
                if refreshed_leg:
                    _store_position_history_for_entry(ctx.db, refreshed_trade, refreshed_leg)
            except Exception as exc:
                log.error('position history store error trade=%s leg=%s: %s', trade_id, leg_id, exc)

            entries_executed.append({
                'trade_id':         trade_id,
                'leg_id':           leg_id,
                'entry_price':      entry_trade.get('price'),
                'ltp':              entry_trade.get('ltp') or entry_trade.get('price'),
                'strike':           entry_trade.get('strike'),
                'expiry':           entry_trade.get('expiry'),
                'option_type':      entry_trade.get('option_type'),
                'entry_iv':         entry_trade.get('entry_iv'),
                'entry_vix':        entry_trade.get('entry_vix'),
                'token':            entry_trade.get('token'),
                'instrument_token': entry_trade.get('instrument_token') or entry_trade.get('token'),
                'symbol':           entry_trade.get('symbol'),
                'exchange':         entry_trade.get('exchange'),
                'chain_timestamp':  entry_trade.get('chain_timestamp'),
                'timestamp':        ctx.now_ts,
            })
            print(f'[POSITION ENTRY] trade={trade_id} leg={leg_id} strike={entry_trade.get("strike")} price={entry_trade.get("price")} ts={ctx.now_ts}')

        if not any(str(item.get('trade_id') or '') == trade_id for item in entries_executed):
            print(
                f'[ENTRY FLOW] trade_id={trade_id} '
                f'state=no_entries_taken '
                f'pending_count={len(pending_legs)} '
                f'now_ts={ctx.now_ts}'
            )

    return entries_executed


# ──────────────────────────────────────────────────────────────────────────────
# §18  BROKER TICK PROCESSOR  — live trade & fast-forward via broker WebSocket
# ──────────────────────────────────────────────────────────────────────────────
#
# When activation_mode is 'live' or 'fast-forward', LTP data comes from the
# broker's WebSocket (Zerodha KiteTicker, Angel SmartWebSocket, etc.) instead
# of option_chain_historical_data.
#
# Broker WS emits on every price change → we get a broker_ltp_map:
#   { composite_token: ltp }
#   composite_token = make_option_token(underlying, expiry, strike, option_type)
#   Example: 'NIFTY_2025-11-04_24500_CE' → 156.50
#
# process_broker_tick() is identical to process_tick() in §16 except:
#   - current_price  = broker_ltp_map.get(token)          ← broker WS (live/ff)
#   - fallback       = resolve_chain_price(chain_doc)     ← DB if token missing
#
# Usage:
#   # On each broker WS tick:
#   broker_map = {tick['token']: tick['last_price'] for tick in broker_ticks}
#   ctx = TickContext(db=db, trade_date=trade_date, now_ts=now_iso(),
#                    activation_mode='live')
#   result = process_broker_tick(ctx, running_trades, broker_map)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_broker_ltp(
    broker_ltp_map: dict[str, float],
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    leg: dict | None = None,
) -> float:
    """
    Look up current LTP from broker_ltp_map using composite token key.

    Try order:
      1. leg.token (e.g. 'NSE_20251104_24500_CE' — broker's own token string)
      2. make_option_token(underlying, expiry, strike, option_type)
         (our canonical composite key: 'NIFTY_2025-11-04_24500_CE')
      3. Return 0.0 if not found — caller will fall back to DB chain doc.

    Parameters
    ----------
    broker_ltp_map: dict emitted from broker WS on_ticks, normalised to
                    {token_str: ltp_float} by the caller.
    leg:            optional leg dict — may have a broker token stored as leg.token
    """
    def _get_cached_socket_ltp(token_value: str) -> float:
        normalized_token = str(token_value or '').strip()
        if not normalized_token:
            return 0.0
        try:
            from features.broker_gateway import broker_ticker_manager, _active_broker  # type: ignore
            # For Dhan: only use WS price if it arrived in the current tick (broker_ltp_map)
            # or was received within the last 5 minutes — otherwise it's a stale initial-subscription price
            if _active_broker() == 'dhan':
                import time as _time
                try:
                    ltp_ts = broker_ticker_manager.ltp_ts_map.get(normalized_token)
                    if ltp_ts:
                        from datetime import datetime as _dt
                        tick_age = (_time.time() -
                                    _dt.fromisoformat(ltp_ts).timestamp())
                        if tick_age > 300:  # older than 5 minutes = stale
                            return 0.0
                except Exception:
                    return 0.0  # no timestamp = no fresh tick; don't use stale
            return safe_float(broker_ticker_manager.get_ltp(normalized_token))
        except Exception:
            return 0.0

    if not broker_ltp_map:
        broker_ltp_map = {}
    # 1. Try stored broker token (e.g. numeric converted to string, or exchange token)
    if leg:
        stored_tok = str(leg.get('token') or '').strip()
        if stored_tok:
            if stored_tok in broker_ltp_map:
                return safe_float(broker_ltp_map[stored_tok])
            cached_ltp = _get_cached_socket_ltp(stored_tok)
            if cached_ltp > 0:
                print('[BROKER LTP CACHE HIT]', {
                    'token': stored_tok,
                    'underlying': underlying,
                    'expiry': expiry,
                    'strike': strike,
                    'option_type': option_type,
                    'ltp': cached_ltp,
                })
                return cached_ltp
    # 2. Try composite canonical key
    composite = make_option_token(underlying, expiry, strike, option_type)
    if composite in broker_ltp_map:
        return safe_float(broker_ltp_map[composite])
    return 0.0


def process_broker_tick(
    ctx: TickContext,
    running_trades: list[dict],
    broker_ltp_map: dict[str, float],
) -> TickResult:
    """
    Broker-tick processor for live trade and fast-forward modes.

    Identical to process_tick() (§16) in all SL/Target/Trail/Overall/Broker
    logic, but LTP source is broker_ltp_map instead of option_chain DB.

    Parameters
    ----------
    ctx:            TickContext — db, trade_date, now_ts, activation_mode.
    running_trades: list of trade docs from algo_trades (pre-loaded by caller).
    broker_ltp_map: { token_str: ltp_float } — from broker WS on_ticks callback.
                    token_str can be the broker's own token or our composite key.

    LTP resolution per leg:
      1. broker_ltp_map[leg.token]  → direct broker token match
      2. broker_ltp_map[make_option_token(...)]  → composite key match
      3. Fallback: resolve_chain_price(get_chain_at_time(...))  → DB query
         (used when broker hasn't emitted a tick for that token yet, or
          when running in fast-forward mode with sparse broker data)

    Returns TickResult — same structure as process_tick().
    """
    open_positions: list[dict]      = []
    strategy_map:   dict[str, dict] = {}
    now_time = ctx.now_ts[11:16] if len(ctx.now_ts) >= 16 else ''
    chain_col = ctx.db._db[COL_OPTION_CHAIN]

    for trade in running_trades:
        try:
            from features.execution_socket import (
                _populate_legs_from_history,
                _process_backtest_trade_tick,
            )
            enriched_rows = _populate_legs_from_history(ctx.db, [dict(trade)])
            if enriched_rows:
                trade = enriched_rows[0]
        except Exception as exc:
            log.warning('process_broker_tick enrich error trade=%s: %s', str((trade or {}).get('_id') or ''), exc)

        trade_id         = str(trade.get('_id') or '')
        strategy_id      = str(trade.get('strategy_id') or '')
        underlying       = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        strategy_cfg     = trade.get('strategy') or trade.get('config') or {}
        all_leg_configs  = resolve_trade_leg_configs(trade)
        runtime_print('[BROKER TRADE LOOP]', {
            'mode': ctx.activation_mode,
            'trade_id': trade_id,
            'strategy_id': strategy_id,
            'timestamp': ctx.now_ts,
            'raw_legs': len(trade.get('legs') or []),
            'raw_leg_types': [type(item).__name__ for item in (trade.get('legs') or [])[:10]],
            'raw_leg_values': [str(item)[:40] for item in (trade.get('legs') or [])[:10]],
            'underlying': underlying,
        })

        # Exit time check
        raw_exit_time  = str(trade.get('exit_time') or '')
        exit_time_hhmm = raw_exit_time[11:16] if len(raw_exit_time) >= 16 else raw_exit_time[:5]
        now_hhmm       = ctx.now_ts[11:16] if len(ctx.now_ts) >= 16 else ''
        past_exit      = bool(exit_time_hhmm and now_hhmm and now_hhmm >= exit_time_hhmm)

        # Mirror the generic tick path: load all trade history docs and decide in
        # Python which legs are still open. Some forward/live history rows do not
        # persist `exit_trade: None` exactly, so a strict Mongo filter can hide
        # active legs and block feature evaluation entirely.
        hist_col = ctx.db._db[COL_POSITIONS_HIST]
        legs: list[dict] = []
        for hdoc in hist_col.find({'trade_id': trade_id}):
            if not isinstance(hdoc.get('entry_trade'), dict) or hdoc.get('exit_trade'):
                continue
            # Skip legs where broker order is still pending or was rejected
            # MTM / SL / TP must only run on confirmed filled positions
            _lifecycle = str((hdoc.get('entry_trade') or {}).get('entry_lifecycle_status') or '').strip()
            if _lifecycle in {'order_open', 'entry_failed'}:
                continue
            h_leg_id = str(hdoc.get('leg_id') or hdoc.get('id') or '')
            if not h_leg_id:
                continue
            hdoc['expiry_date'] = normalize_expiry(str(hdoc.get('expiry_date') or ''))
            hdoc['id'] = h_leg_id
            hdoc['status'] = OPEN_LEG_STATUS
            legs.append(hdoc)

        if SHOW_PRINT_STATEMENT:
            print('[BROKER LEGS RESOLVED]', {
                'mode': ctx.activation_mode,
                'trade_id': trade_id,
                'timestamp': ctx.now_ts,
                'dict_legs': len([l for l in (trade.get('legs') or []) if isinstance(l, dict)]),
                'string_refs': len([l for l in (trade.get('legs') or []) if isinstance(l, str)]),
                'loaded_legs': len(legs),
                'loaded_leg_ids': [str((leg or {}).get('id') or '') for leg in legs[:10] if isinstance(leg, dict)],
            })
        ltp_map: dict[str, float] = {}
        _is_live_mode = ctx.activation_mode in {'live', 'fast-forward', 'forward-test'}
        for leg in legs:
            leg_id = str(leg.get('id') or '')
            if not leg_id:
                continue
            expiry = normalize_expiry(str(leg.get('expiry_date') or ''))
            strike = leg.get('strike')
            option_type = str(leg.get('option') or '')
            current_price = resolve_broker_ltp(broker_ltp_map, underlying, expiry, strike, option_type, leg)
            if not current_price:
                if _is_live_mode:
                    # In live/fast-forward: NEVER fall back to historical chain data for SL/TP checks.
                    # A stale historical price (e.g. 345.65) would immediately trigger SL and exit
                    # a position that just entered. Skip this leg until WS streams a real LTP.
                    print(
                        f'[LIVE LTP MISSING] leg={leg_id} token={str(leg.get("token") or "")} '
                        f'underlying={underlying} strike={strike} option={option_type} '
                        f'— skipping SL/TP check until live LTP arrives'
                    )
                    continue
                chain_doc = get_chain_at_time(
                    ctx.db,
                    underlying,
                    expiry,
                    strike,
                    option_type,
                    ctx.now_ts,
                    ctx.market_cache,
                )
                current_price = resolve_chain_price(chain_doc)
            if current_price:
                ltp_map[leg_id] = current_price
            else:
                if SHOW_PRINT_STATEMENT:
                    print('[BROKER LEG SKIP]', {
                        'mode': ctx.activation_mode,
                        'trade_id': trade_id,
                        'leg_id': leg_id,
                        'token': str(leg.get('token') or ''),
                        'symbol': str(leg.get('symbol') or ''),
                        'underlying': underlying,
                        'expiry': expiry,
                        'strike': strike,
                        'option_type': option_type,
                        'reason': 'ltp_missing_before_shared_tick',
                    })

        if SHOW_PRINT_STATEMENT:
            print('[BROKER SHARED TICK]', {
                'mode': ctx.activation_mode,
                'trade_id': trade_id,
                'timestamp': ctx.now_ts,
                'legs': len(legs),
                'ltp_map_size': len(ltp_map),
                'uses_shared_backtest_tick': True,
            })
        tick_result = _process_backtest_trade_tick(
            db=ctx.db,
            trade=trade,
            legs=legs,
            ltp_map=ltp_map,
            now_ts=ctx.now_ts,
            now_time=now_time,
            chain_col=chain_col,
            market_cache=ctx.market_cache,
            past_exit=past_exit,
            all_leg_configs=all_leg_configs,
            underlying=underlying,
            trade_date=ctx.trade_date,
            activation_mode=ctx.activation_mode,
        )
        open_positions.extend(tick_result.get('open_positions') or [])
        if tick_result.get('strategy_entry'):
            strategy_map[trade_id] = tick_result['strategy_entry']
        ctx.actions_taken.extend(tick_result.get('actions_taken') or [])
        if tick_result.get('hit_trade_id'):
            if tick_result['hit_trade_id'] not in ctx.hit_trade_ids:
                ctx.hit_trade_ids.append(tick_result['hit_trade_id'])
            ctx.hit_ltp_snapshots[tick_result['hit_trade_id']] = list(
                tick_result.get('hit_ltp_snapshot') or []
            )
        ctx.trade_mtm_map[trade_id] = safe_float(tick_result.get('trade_mtm'))

    # ── Broker-level SL/Target check ─────────────────────────────────────
    broker_hit_ids, broker_ltp_snaps = check_broker_sl_target(
        ctx, running_trades, strategy_map, []
    )
    for tid in broker_hit_ids:
        if tid not in ctx.hit_trade_ids:
            ctx.hit_trade_ids.append(tid)
    ctx.hit_ltp_snapshots.update(broker_ltp_snaps)

    return TickResult(
        actions_taken=ctx.actions_taken,
        hit_trade_ids=ctx.hit_trade_ids,
        hit_ltp_snapshots=ctx.hit_ltp_snapshots,
        open_positions=open_positions,
        checked_at=ctx.now_ts,
    )


# ──────────────────────────────────────────────────────────────────────────────
# END OF trading_core.py
#
# How to use across execution modes:
#
#   from features.trading_core import TickContext, process_tick, process_pending_entries
#
#   # 1. Pre-load market cache once per trade_date
#   records = load_running_trades(db, trade_date, activation_mode)
#   cache   = preload_market_cache(db, trade_date, records)
#
#   # 2. For each candle minute (backtest loop or live timer):
#   for now_ts in candle_timestamps:
#       ctx    = TickContext(db=db, trade_date=trade_date,
#                            now_ts=now_ts, activation_mode=mode,
#                            market_cache=cache)
#
#       # 2a. Try to enter pending legs
#       entries = process_pending_entries(ctx, records)
#
#       # 2b. Reload after entries (new legs may have entered)
#       records = load_running_trades(db, trade_date, activation_mode)
#       broadcast(result)
# ──────────────────────────────────────────────────────────────────────────────
