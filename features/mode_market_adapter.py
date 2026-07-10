"""
mode_market_adapter.py
──────────────────────
Single import boundary for mode-specific market-data adapters.

Why this file exists
────────────────────
execution_socket.py is the shared orchestration layer. It should not grow
direct knowledge of which mode file owns which price-fetch logic.

This registry keeps that mapping in one place:
  algo-backtest -> algo_backtest_event
  live          -> live_event
  fast-forward  -> fast_forward_event
"""

from __future__ import annotations

from typing import Any

from features import algo_backtest_event, fast_forward_event, live_event


def resolve_market_event_adapter(activation_mode: str | None = None):
    normalized_mode = str(activation_mode or 'algo-backtest').strip() or 'algo-backtest'
    if normalized_mode == 'live':
        return live_event
    if normalized_mode in ('fast-forward', 'forward-test'):
        return fast_forward_event
    return algo_backtest_event


def get_latest_chain_doc(
    chain_col,
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    trade_date: str,
    market_cache: dict | None = None,
    activation_mode: str | None = None,
) -> dict:
    adapter = resolve_market_event_adapter(activation_mode)
    return adapter.get_latest_chain_doc(
        chain_col, underlying, expiry, strike, option_type, trade_date, market_cache,
    )


def get_chain_doc_at_time(
    chain_col,
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
    activation_mode: str | None = None,
) -> dict:
    adapter = resolve_market_event_adapter(activation_mode)
    return adapter.get_chain_doc_at_time(
        chain_col, underlying, expiry, strike, option_type, snapshot_ts, market_cache,
    )


def get_chain_doc_by_token(
    chain_col,
    token: str,
    snapshot_ts: str,
    activation_mode: str | None = None,
) -> dict:
    adapter = resolve_market_event_adapter(activation_mode)
    return adapter.get_chain_doc_by_token(chain_col, token, snapshot_ts)


def get_spot_doc_at_time(
    index_spot_col,
    underlying: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
    activation_mode: str | None = None,
) -> dict:
    adapter = resolve_market_event_adapter(activation_mode)
    return adapter.get_spot_doc_at_time(index_spot_col, underlying, snapshot_ts, market_cache)


def get_spot_price(
    index_spot_col,
    underlying: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
    activation_mode: str | None = None,
) -> float:
    adapter = resolve_market_event_adapter(activation_mode)
    return adapter.get_spot_price(index_spot_col, underlying, snapshot_ts, market_cache)


def get_option_ltp(
    chain_col,
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
    fallback: float = 0.0,
    activation_mode: str | None = None,
) -> float:
    adapter = resolve_market_event_adapter(activation_mode)
    return adapter.get_option_ltp(
        chain_col, underlying, expiry, strike, option_type, snapshot_ts, market_cache, fallback,
    )


def get_open_legs_ltp_array(
    chain_col,
    open_legs: list[dict],
    underlying: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
    activation_mode: str | None = None,
) -> list[dict]:
    adapter = resolve_market_event_adapter(activation_mode)
    return adapter.get_open_legs_ltp_array(
        chain_col, open_legs, underlying, snapshot_ts, market_cache,
    )


def resolve_entry_price_for_mode(
    db,
    trade: dict,
    token: str,
    symbol: str,
    fallback_price: float,
) -> tuple[float, str]:
    adapter = resolve_market_event_adapter(str(trade.get('activation_mode') or '').strip())
    resolver = getattr(adapter, 'resolve_fast_forward_entry_price', None)
    if callable(resolver):
        return resolver(db, trade, token, symbol, fallback_price)
    return fallback_price, 'default'


def resolve_pending_entry_snapshot_for_mode(
    db,
    trade: dict,
    leg_cfg: dict,
    *,
    now_ts: str,
) -> dict:
    # Both live and fast-forward use the same live resolver.
    # Order placement is the only difference — handled separately in execution_socket.py.
    activation_mode = str(trade.get('activation_mode') or '').strip()
    if activation_mode in ('live', 'fast-forward', 'forward-test'):
        resolver = getattr(live_event, 'resolve_live_pending_entry_snapshot', None)
        if callable(resolver):
            return resolver(db, trade, leg_cfg, now_ts=now_ts) or {}
    return {}


def resolve_entry_execution_payload_for_mode(
    db,
    trade: dict,
    leg: dict,
    *,
    now_ts: str,
) -> dict:
    # Both live and fast-forward use the same live resolver.
    # Order placement is the only difference — handled separately in execution_socket.py.
    activation_mode = str(trade.get('activation_mode') or '').strip()
    if activation_mode in ('live', 'fast-forward', 'forward-test'):
        resolver = getattr(live_event, 'resolve_live_entry_execution_payload', None)
        if callable(resolver):
            return resolver(db, trade, leg, now_ts=now_ts) or {}
    return {}


__all__ = [
    'resolve_market_event_adapter',
    'get_latest_chain_doc',
    'get_chain_doc_at_time',
    'get_chain_doc_by_token',
    'get_spot_doc_at_time',
    'get_spot_price',
    'get_option_ltp',
    'get_open_legs_ltp_array',
    'resolve_entry_price_for_mode',
    'resolve_pending_entry_snapshot_for_mode',
    'resolve_entry_execution_payload_for_mode',
]
