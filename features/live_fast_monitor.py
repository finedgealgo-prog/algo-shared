"""
live_fast_monitor.py
────────────────────
Single supervisor for live + fast-forward modes.

This is not the hot execution path.
Its job is to:
  - keep one global monitor loop for both active modes
  - refresh active strategy snapshots every second
  - feed the runtime registry so the tick dispatcher only does necessary work

Live order execution still happens on the dedicated live tick worker.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from features.mongo_data import MongoData
from features.runtime_mode_registry import runtime_mode_registry

log = logging.getLogger(__name__)

SUPPORTED_MODES = ('live', 'fast-forward', 'forward-test')

# Modes that share the fast-forward tick-driven quote engine (same execution
# behaviour as fast-forward, just tracked as a distinct bucket/identity so the
# Forward Test and Fast Forward pages only ever see their own records).
FAST_FORWARD_LIKE_MODES = ('fast-forward', 'forward-test')

# Forward Test checks SL/target/lazy-leg/entry conditions on a slow heartbeat
# instead of every 0.25s supervisor tick — see the matching constant/comment
# in live_tick_dispatcher.py for the tick-driven half of this same throttle.
FORWARD_TEST_QUOTE_CYCLE_INTERVAL_SECONDS = 30.0
IST = timezone(timedelta(hours=5, minutes=30))

# Set True to show verbose monitor loop print statements
SHOW_MONITOR_LOGS = False


def _now_iso() -> str:
    return datetime.now(IST).strftime('%Y-%m-%dT%H:%M:%S')


def _normalize_trade_date(value: str | None = None) -> str:
    normalized = str(value or '').strip()
    if normalized:
        return normalized
    return _now_iso()[:10]


def _load_mode_records(db: MongoData, trade_date: str, activation_mode: str) -> list[dict[str, Any]]:
    from features.execution_socket import _load_running_trade_records, _resolve_trade_leg_configs

    records = _load_running_trade_records(db, trade_date, activation_mode=activation_mode)
    result: list[dict[str, Any]] = []
    for item in records:
        trade_id = str(item.get('_id') or '')
        full_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or item
        try:
            open_legs = int(
                db._db['algo_trade_positions_history'].count_documents(
                    {'trade_id': trade_id, 'status': 1},
                )
            )
        except Exception:
            open_legs = 0
        total_legs = len(_resolve_trade_leg_configs(full_trade) or {})
        result.append({
            '_id': str(item.get('_id') or ''),
            'activation_mode': str(item.get('activation_mode') or activation_mode),
            'name': str(item.get('name') or ''),
            'ticker': str(item.get('ticker') or ((item.get('config') or {}).get('Ticker') or '')),
            'entry_time': str(item.get('entry_time') or ''),
            'exit_time': str(item.get('exit_time') or ''),
            'group_name': str(((item.get('portfolio') or {}).get('group_name')) or ''),
            'group_id': str(((item.get('portfolio') or {}).get('group_id')) or ''),
            'user_id': str(item.get('user_id') or ''),
            'open_legs': open_legs,
            'total_legs': total_legs,
        })
    return result


def _has_fast_forward_quote_trades(db: MongoData, trade_date: str, activation_mode: str = 'fast-forward') -> bool:
    from features.execution_socket import _load_running_trade_records
    from features.fast_forward_event import should_use_fast_forward_quote

    records = _load_running_trade_records(db, trade_date, activation_mode=activation_mode)
    for record in records:
        full_trade = db._db['algo_trades'].find_one({'_id': str(record.get('_id') or '')}) or record
        if should_use_fast_forward_quote(full_trade):
            return True
    return False


def _should_run_fast_forward_quote_cycle(now_ts: str, ticker_tick_count: int) -> bool:
    # Quote-enabled fast-forward should keep progressing regardless of whether
    # live ticks are flowing. Duplicate entry attempts are naturally blocked by
    # history/pending-entry checks in the common execution layer.
    return True


class _LiveFastMonitorSupervisor:
    def __init__(self) -> None:
        self._running = False
        self._task: asyncio.Task | None = None
        self.trade_date = ''
        self.started_at = ''
        self.last_tick_at = ''
        self._quote_cycle_last_run_monotonic: dict[str, float] = {}

    def start(self, trade_date: str = '') -> None:
        normalized_trade_date = _normalize_trade_date(trade_date)
        if self._running:
            if self.trade_date == normalized_trade_date:
                return
            self.stop()
        self.trade_date = normalized_trade_date
        self.started_at = _now_iso()
        self.last_tick_at = ''
        self._quote_cycle_last_run_monotonic = {}
        self._running = True
        runtime_mode_registry.enable()
        self._task = asyncio.create_task(self._run())
        # Start DB change watcher so every insert/update/delete in the three
        # trading collections automatically emits to the owning user's socket.
        try:
            from features.db_change_watcher import db_change_watcher
            db_change_watcher.start(trade_date=normalized_trade_date)
        except Exception as _dw_exc:
            log.warning('[LIVE+FF MONITOR] db_change_watcher start error: %s', _dw_exc)
        print(
            f'[LIVE+FF MONITOR] started '
            f'trade_date={self.trade_date}'
        )

    def stop(self) -> None:
        was_running = self._running
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None
        runtime_mode_registry.disable()
        try:
            from features.db_change_watcher import db_change_watcher
            db_change_watcher.stop()
        except Exception:
            pass
        if was_running:
            print('[LIVE+FF MONITOR] stopped')

    def get_status(self) -> dict[str, Any]:
        snapshot = runtime_mode_registry.snapshot()
        return {
            'running': self._running,
            'trade_date': self.trade_date,
            'started_at': self.started_at,
            'last_tick_at': self.last_tick_at,
            'last_refresh_at': snapshot.get('last_refresh_at') or '',
            'counts': snapshot.get('counts') or {},
            'records_by_mode': snapshot.get('records_by_mode') or {},
        }

    async def _run(self) -> None:
        db = MongoData()
        _poll_tick = 0
        try:
            from features.live_order_manager import restore_sl_order_registry
            restore_sl_order_registry(db)
        except Exception as _re:
            log.warning('[SL REGISTRY RESTORE] error: %s', _re)
        try:
            while self._running:
                now_ts = _now_iso()
                _poll_tick += 1
                current_hhmm = now_ts[11:16] if len(now_ts) >= 16 else ''
                ticker_tick_count = 0
                try:
                    from features.broker_gateway import broker_ticker_manager as ticker_manager  # type: ignore
                    ticker_tick_count = int(ticker_manager.tick_count or 0)
                except Exception:
                    ticker_tick_count = 0
                records_by_mode: dict[str, list[dict[str, Any]]] = {}
                for activation_mode in SUPPORTED_MODES:
                    records_by_mode[activation_mode] = _load_mode_records(
                        db,
                        self.trade_date,
                        activation_mode,
                    )
                runtime_mode_registry.update(
                    records_by_mode=records_by_mode,
                    refreshed_at=now_ts,
                )
                self.last_tick_at = now_ts
                try:
                    if records_by_mode.get('live') or records_by_mode.get('fast-forward') or records_by_mode.get('forward-test'):
                        from features.live_event import sync_live_open_position_subscriptions
                        synced = sync_live_open_position_subscriptions(self.trade_date)
                        if synced:
                            if SHOW_MONITOR_LOGS:
                                print(
                                    '[LIVE+FF TOKEN SYNC] '
                                    f'trade_date={self.trade_date} '
                                    f'synced={synced}'
                                )
                except Exception as exc:
                    log.warning('[LIVE+FF TOKEN SYNC] error: %s', exc)
                if SHOW_MONITOR_LOGS:
                    print(
                        '[LIVE+FF MONITOR] '
                        f'trade_date={self.trade_date} '
                        f'live={len(records_by_mode.get("live") or [])} '
                        f'fast_forward={len(records_by_mode.get("fast-forward") or [])} '
                        f'ticker_tick_count={ticker_tick_count}'
                    )
                    for activation_mode in SUPPORTED_MODES:
                        for record in (records_by_mode.get(activation_mode) or []):
                            _raw_et = str(record.get('entry_time') or '').strip()
                            _et_hhmm = _raw_et[11:16] if len(_raw_et) >= 16 else _raw_et[:5]
                            _open = int(record.get('open_legs') or 0)
                            _total = int(record.get('total_legs') or 0)
                            if _open > 0:
                                _entry_status = 'entered'
                            elif _et_hhmm and current_hhmm and current_hhmm < _et_hhmm:
                                _entry_status = f'waiting — entry_at={_et_hhmm} now={current_hhmm}'
                            elif _et_hhmm:
                                _entry_status = f'ready_to_enter — entry_at={_et_hhmm} now={current_hhmm}'
                            else:
                                _entry_status = 'no_entry_time'
                            print(
                                '[LIVE+FF CHECK] '
                                f'mode={activation_mode} '
                                f'group={str(record.get("group_name") or "-")} '
                                f'strategy={str(record.get("name") or "-")} '
                                f'entry_time={_et_hhmm or "--:--"} '
                                f'current_time={current_hhmm or "--:--"} '
                                f'legs={_open}/{_total} '
                                f'status={_entry_status}'
                            )
                try:
                    live_records = records_by_mode.get('live') or []
                    if live_records:
                        from features.kite_event import broker_live_tick
                        from features.broker_gateway import broker_ticker_manager as ticker_manager  # type: ignore
                        from features.live_tick_dispatcher import _run_entries_for_mode

                        if SHOW_MONITOR_LOGS:
                            print(
                                '[LIVE AUTO CYCLE] '
                                f'trade_date={self.trade_date} '
                                f'timestamp={now_ts} '
                                f'ticker_tick_count={ticker_tick_count} '
                                f'records={len(live_records)}'
                            )
                        _run_entries_for_mode(
                            db,
                            self.trade_date,
                            'live',
                            current_hhmm,
                            now_ts,
                        )
                        broker_live_tick(
                            db,
                            self.trade_date,
                            now_ts,
                            dict(ticker_manager.ltp_map or {}),
                            activation_mode='live',
                        )
                        # Entry fill detection: primary = postback URL.
                        # Fallback poll every 8 ticks (~2 s) only while pending entry
                        # orders exist. Stops automatically once SL is placed (order
                        # deregistered from active set after fill confirmed).
                        if _poll_tick % 8 == 0:
                            try:
                                from features.live_order_manager import (
                                    poll_pending_order_fills,
                                    has_pending_entry_orders,
                                )
                                if has_pending_entry_orders():
                                    poll_pending_order_fills(db)
                            except Exception as _pe:
                                log.debug('[ORDER POLL] error: %s', _pe)
                        # Position sync every 120 ticks (~30 s) — detects externally
                        # closed positions (manual exit in broker terminal, etc.)
                        if _poll_tick % 120 == 0:
                            try:
                                from features.live_order_manager import sync_open_leg_positions
                                sync_open_leg_positions(db)
                            except Exception as _se:
                                log.debug('[POSITION SYNC] error: %s', _se)

                    for _ff_mode in FAST_FORWARD_LIKE_MODES:
                        ff_mode_records = records_by_mode.get(_ff_mode) or []
                        if not ff_mode_records:
                            continue
                        if _ff_mode == 'forward-test':
                            _now_monotonic = time.monotonic()
                            _last_run = self._quote_cycle_last_run_monotonic.get(_ff_mode, 0.0)
                            if _now_monotonic - _last_run < FORWARD_TEST_QUOTE_CYCLE_INTERVAL_SECONDS:
                                continue
                            self._quote_cycle_last_run_monotonic[_ff_mode] = _now_monotonic
                            print(
                                f'[FORWARD-TEST {int(FORWARD_TEST_QUOTE_CYCLE_INTERVAL_SECONDS)}s CHECK] '
                                f'heartbeat SL/target/lazy-leg/entry check running '
                                f'trade_date={self.trade_date} now_ts={now_ts} '
                                f'records={len(ff_mode_records)}'
                            )
                        has_quote_trades = _has_fast_forward_quote_trades(db, self.trade_date, _ff_mode)
                        if (
                            has_quote_trades
                            or _should_run_fast_forward_quote_cycle(now_ts, ticker_tick_count)
                        ):
                            from features.live_tick_dispatcher import _run_entries_for_mode
                            from features.broker_gateway import broker_ticker_manager as ticker_manager  # type: ignore

                            if SHOW_MONITOR_LOGS:
                                print(
                                    '[FAST-FORWARD QUOTE CYCLE] '
                                    f'mode={_ff_mode} '
                                    f'trade_date={self.trade_date} '
                                    f'timestamp={now_ts} '
                                    f'ticker_tick_count={ticker_tick_count} '
                                    f'quote_trades={has_quote_trades} '
                                    f'records={len(ff_mode_records)} '
                                    f'ticker_status={ticker_manager.status} '
                                    f'spot_keys={list((ticker_manager.spot_map or {}).keys())[:10]} '
                                    f'ltp_count={len(ticker_manager.ltp_map or {})}'
                                )
                            _run_entries_for_mode(
                                db,
                                self.trade_date,
                                _ff_mode,
                                current_hhmm,
                                now_ts,
                            )
                except Exception as exc:
                    log.warning('[FAST-FORWARD QUOTE CYCLE] error: %s', exc)
                # ── Broadcast Kite LTP → update channel (per-user filtered) ──────────
                # Only send each user the tokens they have subscribed to
                # (active strategy spot tokens + open leg tokens).
                # This prevents broadcasting all 1000+ broker tokens to every client.
                try:
                    from features.live_monitor_socket import (
                        _get_active_ticker_manager,
                        _SPOT_TOKEN_BY_UNDERLYING,
                        _build_message as _ltp_build_message,
                    )
                    from features.execution_socket import (
                        _broadcast_user_channel_message,
                        get_update_user_subscribed_tokens,
                    )
                    from features.broker_gateway import BROKER_VIX_TOKEN as _VIX_TOKEN_ID  # type: ignore

                    _tm = _get_active_ticker_manager()
                    _spot_token_set = set(_SPOT_TOKEN_BY_UNDERLYING.values())
                    _spot_ltp_list = [
                        {
                            'token': _SPOT_TOKEN_BY_UNDERLYING.get(und, ''),
                            'ltp': float(ltp),
                            'underlying': und,
                            'option_type': 'SPOT',
                            'timestamp': now_ts,
                        }
                        for und, ltp in _tm.spot_map.items()
                        if ltp and float(ltp) > 0
                    ]
                    _full_option_ltp_map = {
                        tok: float(ltp)
                        for tok, ltp in _tm.ltp_map.items()
                        if tok not in _spot_token_set and ltp and float(ltp) > 0
                    }
                    _vix_ltp = float(_tm.ltp_map.get(str(_VIX_TOKEN_ID), 0) or 0)
                    _vix_ltp_list = [{
                        'token': 'NSE_00',
                        'underlying': 'INDIA VIX',
                        'option_type': 'SPOT',
                        'ltp': _vix_ltp,
                        'timestamp': now_ts,
                    }] if _vix_ltp > 0 else []

                    _user_token_map = get_update_user_subscribed_tokens()
                    _spot_parts = '  '.join(
                        f'{s["underlying"]}={s["ltp"]:.2f}' for s in _spot_ltp_list
                    ) or 'no spot'
                    for _uid, _user_tokens in _user_token_map.items():
                        _user_option_ltp = [
                            {'token': tok, 'ltp': ltp, 'timestamp': now_ts}
                            for tok, ltp in _full_option_ltp_map.items()
                            if tok in _user_tokens
                        ]
                        _user_payload = _ltp_build_message(
                            'ltp_update',
                            'Live LTP tick',
                            {
                                'trade_date': now_ts[:10],
                                'listen_time': current_hhmm,
                                'listen_timestamp': now_ts,
                                'ltp': _spot_ltp_list + _user_option_ltp + _vix_ltp_list,
                                'spot_map': dict(_tm.spot_map),
                                'broker_status': _tm.status,
                                'mode': 'fast-forward',
                            },
                        )
                        await _broadcast_user_channel_message(_uid, 'update', _user_payload)
                        if SHOW_MONITOR_LOGS:
                            print(
                                f'[FF LTP EMIT]  {now_ts}'
                                f'  |  user: {_uid}'
                                f'  |  spot: {_spot_parts}'
                                f'  |  option tokens: {len(_user_option_ltp)}'
                                f'  |  vix: {_vix_ltp:.2f}' if _vix_ltp > 0 else ''
                            )
                except Exception as _ltp_exc:
                    log.debug('[FF LTP EMIT] error: %s', _ltp_exc)

                await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error('[LIVE+FF MONITOR] fatal error: %s', exc)
            self._running = False
        finally:
            try:
                db.close()
            except Exception:
                pass
            if not self._running:
                runtime_mode_registry.disable()
            print('[LIVE+FF MONITOR] exited')


live_fast_monitor_supervisor = _LiveFastMonitorSupervisor()
