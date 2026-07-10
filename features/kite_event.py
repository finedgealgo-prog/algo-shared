"""
kite_event.py
─────────────
Broker WebSocket tick processing for live-trade and fast-forward modes.

Parallel to algo_backtest_event.py (backtest data layer) — this file owns
all Kite / broker-tick normalisation and the live-tick event dispatcher.

Public API
──────────
  build_broker_ltp_map(broker_ticks) → dict[str, float]
  broker_live_tick(db, trade_date, now_ts, broker_ltp_map, activation_mode, running_trades) → dict

kite_ticker.py calls build_broker_ltp_map + broker_live_tick on every on_ticks event.
execution_socket.py is NOT imported here — no circular dependency.
"""

from __future__ import annotations

from features.mongo_data import MongoData
import features.debug_flags as _debug_flags
from features.debug_flags import runtime_print


# ─── Constants (mirror execution_socket.py) ───────────────────────────────────

RUNNING_STATUS  = 'StrategyStatus.Live_Running'
OPEN_LEG_STATUS = 1


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _make_token(underlying: str, expiry: str, strike, option_type: str) -> str:
    """Composite token key: NIFTY_2025-11-04_24500_CE"""
    try:
        strike_str = str(int(float(strike))) if strike not in (None, '') else ''
    except (TypeError, ValueError):
        strike_str = str(strike or '')
    return f"{underlying}_{expiry}_{strike_str}_{option_type}"


# ─── Public API ───────────────────────────────────────────────────────────────

def build_broker_ltp_map(broker_ticks: list[dict]) -> dict[str, float]:
    """
    Normalise a list of raw broker tick dicts to { token_str: last_price }.

    Handles multiple broker tick formats:
      Zerodha KiteTicker:  {'instrument_token': 12345, 'last_price': 156.5, ...}
      Angel SmartWS:       {'tk': '12345', 'lp': '156.50', ...}
      Our own tick format: {'token': 'NIFTY_2025-11-04_24500_CE', 'ltp': 156.5}

    Returns dict with ALL available key variants so the caller can look up
    any leg regardless of which token format is stored in the leg document.
    """
    result: dict[str, float] = {}
    for tick in (broker_ticks or []):
        if not isinstance(tick, dict):
            continue
        ltp = _safe_float(tick.get('ltp') or tick.get('last_price') or tick.get('lp') or 0)
        # Our format / already normalised
        tok = str(tick.get('token') or '').strip()
        if tok and ltp:
            result[tok] = ltp
        # Zerodha numeric instrument_token (stored as int or str)
        instr = str(tick.get('instrument_token') or '').strip()
        if instr and ltp:
            result[instr] = ltp
        # Angel SmartWS 'tk' key
        angel_tk = str(tick.get('tk') or '').strip()
        if angel_tk and ltp:
            result[angel_tk] = ltp
    return result


def broker_live_tick(
    db: MongoData,
    trade_date: str,
    now_ts: str,
    broker_ltp_map: dict[str, float],
    activation_mode: str = 'live',
    running_trades: list[dict] | None = None,
) -> dict:
    """
    Called on every broker WebSocket tick in live-trade and fast-forward modes.

    Parameters
    ----------
    db:              MongoData instance.
    trade_date:      'YYYY-MM-DD' — today's date for live, past date for fast-forward.
    now_ts:          ISO timestamp of this tick ('YYYY-MM-DDTHH:MM:SS').
    broker_ltp_map:  { token_str: ltp } normalised from broker WS on_ticks.
                     Use build_broker_ltp_map(raw_ticks) to prepare this.
    activation_mode: 'live' | 'fast-forward'.
    running_trades:  pre-loaded list of algo_trades docs; if None, loads from DB.

    Returns
    -------
    dict with keys:
      actions_taken      – list of audit strings
      checked_at         – now_ts echoed back
      hit_trade_ids      – list of trade_ids whose overall/broker SL-TGT fired
      hit_ltp_snapshots  – {trade_id: [{leg_id, ltp, entry_price, pnl}]}
      open_positions     – [{trade_id, leg_id, ltp, pnl}] for broadcast
      subscribe_tokens   – tokens currently active → subscribe to broker WS
    """
    from features.trading_core import (  # type: ignore
        TickContext,
        process_broker_tick,
    )
    from features.execution_socket import _build_trade_query

    if running_trades is None:
        _trade_query = _build_trade_query(
            trade_date,
            activation_mode=activation_mode,
            statuses=[RUNNING_STATUS],
        )
        running_trades = list(db._db['algo_trades'].find(_trade_query))

    ctx = TickContext(
        db=db,
        trade_date=trade_date,
        now_ts=now_ts,
        activation_mode=activation_mode,
    )

    # Suppress runtime prints for this tick if all trades are waiting for entry time
    _now_hhmm = now_ts[11:16] if len(now_ts) >= 16 else ''
    _all_waiting = bool(running_trades) and all(
        (lambda _et: bool(_et and _now_hhmm and _now_hhmm < _et))(
            (lambda _raw: _raw[11:16] if len(_raw) >= 16 else _raw[:5])(
                str((t.get('config') or {}).get('entry_time') or t.get('entry_time') or '')
            )
        )
        for t in running_trades
    )
    _debug_flags.fast_forward_mode = (activation_mode in ('fast-forward', 'forward-test'))
    if not _debug_flags.fast_forward_mode:
        _debug_flags.suppress_runtime_logs = _all_waiting

    result = process_broker_tick(ctx, running_trades, broker_ltp_map)

    _debug_flags.fast_forward_mode = False
    _debug_flags.suppress_runtime_logs = False

    # Build subscribe_tokens from all open legs
    subscribe_tokens: list[str] = []
    for trade in running_trades:
        underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        for leg in (trade.get('legs') or []):
            if not isinstance(leg, dict):
                continue
            if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
                continue
            if not isinstance(leg.get('entry_trade'), dict):
                continue
            tok = str(leg.get('token') or '').strip()
            if not tok:
                tok = _make_token(
                    underlying,
                    str(leg.get('expiry_date') or ''),
                    leg.get('strike'),
                    str(leg.get('option') or ''),
                )
            if tok and tok not in subscribe_tokens:
                subscribe_tokens.append(tok)

    # runtime_print('[BROKER TICK]', {
    #     'mode':            activation_mode,
    #     'timestamp':       now_ts,
    #     'ticks_received':  len(broker_ltp_map),
    #     'running_trades':  len(running_trades),
    #     'subscribe_count': len(subscribe_tokens),
    #     'actions':         result.actions_taken,
    #     'hit_trades':      result.hit_trade_ids,
    # })

    return {
        'actions_taken':     result.actions_taken,
        'checked_at':        result.checked_at,
        'hit_trade_ids':     result.hit_trade_ids,
        'hit_ltp_snapshots': result.hit_ltp_snapshots,
        'open_positions':    result.open_positions,
        'subscribe_tokens':  subscribe_tokens,
    }
