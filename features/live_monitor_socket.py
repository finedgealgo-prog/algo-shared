"""
live_monitor_socket.py
──────────────────────
Server-side background loop for live and fast-forward trade monitoring.

Started by:  GET /live/start
Stopped by:  GET /live/stop

Once started, runs every second — independent of any WebSocket connection.
Broadcasts two events to all connected clients on the 'executions' channel:

  live_tick             → LTP snapshot from kite_ticker (in-memory, no DB)
  live_strategy_update  → Active strategy details from DB
                          (activation_mode='live', active_on_server=True, trade_status=1)

Zero contact with execution_socket.py internals.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from bson import ObjectId

from features.mongo_data import MongoData
from features.telegram_notifier import notify_admin, notify_both

IST = timezone(timedelta(hours=5, minutes=30))

log = logging.getLogger(__name__)

_RUNNING_STATUS  = 'StrategyStatus.Live_Running'
_OPEN_LEG_STATUS = 1
_SPOT_TOKEN_BY_UNDERLYING = {
    'NIFTY': '256265',
    'BANKNIFTY': '260105',
    'FINNIFTY': '257801',
    'SENSEX': '265',
    'MIDCPNIFTY': '288009',
}
_QUOTE_INSTRUMENT_BY_UNDERLYING = {
    'NIFTY': 'NSE:NIFTY 50',
    'BANKNIFTY': 'NSE:NIFTY BANK',
    'FINNIFTY': 'NSE:NIFTY FIN SERVICE',
    'SENSEX': 'BSE:SENSEX',
    'MIDCPNIFTY': 'NSE:NIFTY MID SELECT',
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(IST).strftime('%Y-%m-%dT%H:%M:%S')


def _build_message(message_type: str, message: str, data: Any = None) -> str:
    payload: dict[str, Any] = {
        'type':        message_type,
        'message':     message,
        'server_time': _now_iso(),
    }
    if data is not None:
        payload['data'] = data
    return json.dumps(payload)


def _get_active_ticker_manager():
    """
    Prefer the mock ticker when it is active so websocket consumers receive
    mock LTP updates without needing a separate API poll.
    """
    from features.mock_ticker import mock_ticker_manager
    if mock_ticker_manager.status in ('running', 'connecting') and mock_ticker_manager._ticker:
        return mock_ticker_manager

    from features.broker_gateway import broker_ticker_manager  # type: ignore
    return broker_ticker_manager


def _load_live_strategies(db: MongoData, trade_date: str, activation_mode: str) -> list[dict]:
    from features.execution_socket import _resolve_trade_leg_configs

    query: dict[str, Any] = {
        'activation_mode': activation_mode,
        'active_on_server': True,
        'trade_status':     1,
        'status':           _RUNNING_STATUS,
    }
    if trade_date:
        query['creation_ts'] = {'$regex': f'^{trade_date}'}

    print(
        f'[LIVE MONITOR QUERY] '
        f'activation_mode={query.get("activation_mode")} | '
        f'active_on_server={query.get("active_on_server")} | '
        f'trade_status={query.get("trade_status")} | '
        f'status={query.get("status")} | '
        f'creation_ts=^{trade_date}'
    )
    records = []
    for item in db._db['algo_trades'].find(query).sort('creation_ts', 1):
        trade_id = str(item.get('_id') or '')
        try:
            open_legs = list(
                db._db['algo_trade_positions_history'].find(
                    {'trade_id': trade_id, 'status': _OPEN_LEG_STATUS},
                    {'_id': 1},
                )
            )
        except Exception:
            open_legs = []
        total_legs = len(_resolve_trade_leg_configs(item) or {})
        records.append({
            '_id':        str(item.get('_id') or ''),
            'name':       str(item.get('name') or ''),
            'ticker':     str(
                item.get('ticker')
                or (item.get('config') or {}).get('Ticker')
                or ''
            ),
            'entry_time': str(item.get('entry_time') or ''),
            'exit_time':  str(item.get('exit_time') or ''),
            'group_name': str((item.get('portfolio') or {}).get('group_name') or ''),
            'open_legs':  len(open_legs),
            'total_legs': total_legs,
        })
    return records


def _subscribe_token_to_kite(token_str: str) -> None:
    """Subscribe a single option token to the active ticker (live or mock)."""
    try:
        _tm = _get_active_ticker_manager()
        if not _tm._ticker or _tm.status != 'running':
            return
        normalized_token = str(token_str).strip()
        if not normalized_token:
            return
        subscribe_token = int(normalized_token) if _tm.__class__.__name__ != '_MockTickerManager' else normalized_token
        _tm._ticker.subscribe([subscribe_token])
        _tm._ticker.set_mode(_tm._ticker.MODE_LTP, [subscribe_token])
        print(f'[TICKER SUBSCRIBE] token={normalized_token} source={_tm.__class__.__name__}')
    except Exception as exc:
        log.debug('ticker subscribe error token=%s: %s', token_str, exc)


def _get_live_spot_price(underlying: str) -> tuple[float, str]:
    normalized_underlying = str(underlying or '').strip().upper()
    if not normalized_underlying:
        return 0.0, ''
    try:
        _tm = _get_active_ticker_manager()
        spot_token = _SPOT_TOKEN_BY_UNDERLYING.get(normalized_underlying, '')
        if spot_token:
            token_price = float(_tm.ltp_map.get(spot_token) or 0)
            if token_price > 0:
                return token_price, spot_token
        spot_price = float(_tm.get_spot(normalized_underlying) or 0)
        if spot_price > 0:
            return spot_price, spot_token
    except Exception as exc:
        log.debug('live spot read error underlying=%s: %s', normalized_underlying, exc)
    return 0.0, _SPOT_TOKEN_BY_UNDERLYING.get(normalized_underlying, '')


def _get_live_option_ltp(token: str) -> float:
    normalized_token = str(token or '').strip()
    if not normalized_token:
        return 0.0
    try:
        _tm = _get_active_ticker_manager()
        return float(_tm.ltp_map.get(normalized_token) or 0)
    except Exception as exc:
        log.debug('live option ltp read error token=%s: %s', normalized_token, exc)
        return 0.0


def _is_truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value or '').strip().lower()
    return normalized in {'1', 'true', 'yes', 'y', 'on'}


def _should_use_quote(trade_doc: dict, activation_mode: str) -> bool:
    if str(activation_mode or '').strip() not in ('fast-forward', 'forward-test'):
        return False
    if 'get_quote' in trade_doc:
        return _is_truthy_flag(trade_doc.get('get_quote'))
    strategy_cfg = trade_doc.get('strategy') or {}
    config_cfg = trade_doc.get('config') or {}
    if 'get_quote' in strategy_cfg:
        return _is_truthy_flag(strategy_cfg.get('get_quote'))
    return _is_truthy_flag(config_cfg.get('get_quote'))


def _get_quote_access_token(db: MongoData, trade_doc: dict) -> str:
    broker_id = str(trade_doc.get('broker') or '').strip()
    if broker_id:
        try:
            broker_doc = db._db['broker_configuration'].find_one(
                {'_id': ObjectId(broker_id)},
                {'access_token': 1},
            ) or {}
            access_token = str(broker_doc.get('access_token') or '').strip()
            if access_token:
                return access_token
        except Exception as exc:
            log.debug('quote broker token lookup error broker=%s: %s', broker_id, exc)
    try:
        from features.broker_gateway import BROKER_CONFIG_COLLECTION  # type: ignore
        market_cfg = db._db[BROKER_CONFIG_COLLECTION].find_one(
            {'enabled': True},
            {'access_token': 1},
        ) or {}
        return str(market_cfg.get('access_token') or '').strip()
    except Exception as exc:
        log.debug('quote market token lookup error: %s', exc)
        return ''


def _get_quote_spot_price(db: MongoData, trade_doc: dict, underlying: str) -> tuple[float, str]:
    normalized_underlying = str(underlying or '').strip().upper()
    instrument = _QUOTE_INSTRUMENT_BY_UNDERLYING.get(normalized_underlying, '')
    if not instrument:
        return 0.0, ''
    access_token = _get_quote_access_token(db, trade_doc)
    if not access_token:
        return 0.0, instrument
    try:
        from features.broker_gateway import get_broker_rest_client_with_token as get_kite_instance  # type: ignore

        kite = get_kite_instance(access_token)
        quote_map = kite.quote([instrument]) or {}
        quote_doc = quote_map.get(instrument) or {}
        for key in ('last_price', 'last_trade_price'):
            price = float(quote_doc.get(key) or 0)
            if price > 0:
                return price, instrument
        ohlc = quote_doc.get('ohlc') if isinstance(quote_doc.get('ohlc'), dict) else {}
        for key in ('close', 'open'):
            price = float(ohlc.get(key) or 0)
            if price > 0:
                return price, instrument
    except Exception as exc:
        log.debug('quote spot fetch error underlying=%s instrument=%s: %s', normalized_underlying, instrument, exc)
    return 0.0, instrument


def _resolve_entry_spot_price(
    db: MongoData,
    trade_doc: dict,
    underlying: str,
    activation_mode: str,
    now_ts: str,
) -> tuple[float, float, str, str]:
    normalized_underlying = str(underlying or '').strip().upper()
    use_quote = _should_use_quote(trade_doc, activation_mode)
    quote_spot_price = 0.0
    quote_instrument = ''

    if use_quote:
        quote_spot_price, quote_instrument = _get_quote_spot_price(
            db,
            trade_doc,
            normalized_underlying,
        )
        if quote_spot_price > 0:
            return quote_spot_price, quote_spot_price, quote_instrument, 'kite_quote'

    live_spot_price, spot_token = _get_live_spot_price(normalized_underlying)
    if live_spot_price > 0:
        return live_spot_price, quote_spot_price, spot_token, 'ticker_spot'

    return 0.0, quote_spot_price, spot_token if 'spot_token' in locals() else '', 'unavailable'


def _execute_live_entry(
    db: MongoData,
    trade_doc: dict,
    leg_entries: list[dict],
    now_ts: str,
) -> None:
    from features.execution_socket import apply_resolved_live_entries

    applied = apply_resolved_live_entries(db, trade_doc, leg_entries, now_ts)
    print(
        f'[LIVE ENTRY APPLY] trade={str(trade_doc.get("_id") or "")} '
        f'applied={len(applied)}'
    )


async def _broadcast(message: str) -> None:
    """Send to all sockets connected to the 'executions' channel."""
    try:
        from features.execution_socket import broadcast_to_channel
        await broadcast_to_channel('executions', message)
    except Exception as exc:
        log.debug('broadcast error: %s', exc)


# ─── Background loop ──────────────────────────────────────────────────────────

class _LiveMonitorLoop:
    def __init__(self) -> None:
        self._running   = False
        self._task: asyncio.Task | None = None
        self.trade_date = ''
        self.activation_mode = 'live'
        self.started_at = ''
        self.last_tick_at = ''

    # ── called from async FastAPI endpoint ────────────────────────────────────
    def start(self, trade_date: str = '', activation_mode: str = 'live') -> None:
        normalized_trade_date = trade_date or _now_iso()[:10]
        normalized_mode = str(activation_mode or 'live').strip() or 'live'
        if self._running:
            if self.trade_date == normalized_trade_date and self.activation_mode == normalized_mode:
                log.info('[LIVE MONITOR LOOP] already running')
                return
            self.stop()
        self.trade_date      = normalized_trade_date
        self.activation_mode = normalized_mode
        self.started_at      = _now_iso()
        self.last_tick_at    = ''
        self._running        = True
        self._task           = asyncio.create_task(self._run())
        # Start DB change watcher so every write in the three trading collections
        # automatically marks the owning user's execute-orders socket dirty.
        try:
            from features.db_change_watcher import db_change_watcher
            db_change_watcher.start(trade_date=normalized_trade_date)
        except Exception as _dw_exc:
            log.warning('[LIVE MONITOR LOOP] db_change_watcher start error: %s', _dw_exc)
        print(
            f'[LIVE MONITOR LOOP] started | '
            f'trade_date={self.trade_date} mode={self.activation_mode}'
        )

    def stop(self) -> None:
        was_running = self._running
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None
        try:
            from features.db_change_watcher import db_change_watcher
            db_change_watcher.stop()
        except Exception:
            pass
        if was_running:
            print('[LIVE MONITOR LOOP] stopped')

    def get_status(self) -> dict[str, Any]:
        return {
            'running': self._running,
            'trade_date': self.trade_date,
            'activation_mode': self.activation_mode,
            'started_at': self.started_at,
            'last_tick_at': self.last_tick_at,
        }

    @property
    def running(self) -> bool:
        return self._running

    # ── background coroutine ──────────────────────────────────────────────────
    async def _run(self) -> None:
        db = MongoData()
        try:
            while self._running:
                now_ts      = _now_iso()
                self.last_tick_at = now_ts
                listen_hhmm = now_ts[11:16] if len(now_ts) >= 16 else ''

                # ── Event 1: live_tick  (in-memory, no DB) ────────────────────
                try:
                    _tm = _get_active_ticker_manager()
                    await _broadcast(_build_message(
                        'live_tick',
                        'Live market tick',
                        {
                            'timestamp':       now_ts,
                            'ltp_map':         dict(_tm.ltp_map),
                            'spot_map':        dict(_tm.spot_map),
                            'tick_count':      _tm.tick_count,
                            'broker_status':   _tm.status,
                            'mode':            self.activation_mode,
                        },
                    ))
                except Exception as exc:
                    log.error('live_tick error: %s', exc)
                    notify_admin('entry_logic_error', f'live_tick broadcast error: {exc}')

                # ── Event 1b: ltp_update → update channel (fast-forward / live dashboards) ──
                try:
                    _tm = _get_active_ticker_manager()
                    spot_token_set = set(_SPOT_TOKEN_BY_UNDERLYING.values())
                    spot_ltp_list = [
                        {
                            'token': _SPOT_TOKEN_BY_UNDERLYING.get(underlying, ''),
                            'ltp': float(ltp),
                            'underlying': underlying,
                            'option_type': 'SPOT',
                            'timestamp': now_ts,
                        }
                        for underlying, ltp in _tm.spot_map.items()
                        if ltp and float(ltp) > 0
                    ]
                    option_ltp_list = [
                        {
                            'token': token,
                            'ltp': float(ltp),
                            'timestamp': now_ts,
                        }
                        for token, ltp in _tm.ltp_map.items()
                        if token not in spot_token_set and ltp and float(ltp) > 0
                    ]
                    from features.broker_gateway import BROKER_VIX_TOKEN as _VIX_TOKEN_ID  # type: ignore
                    _vix_ltp = float(_tm.ltp_map.get(str(_VIX_TOKEN_ID), 0) or 0)
                    vix_ltp_list = [{
                        'token': 'NSE_00',
                        'underlying': 'INDIA VIX',
                        'option_type': 'SPOT',
                        'ltp': _vix_ltp,
                        'timestamp': now_ts,
                    }] if _vix_ltp > 0 else []
                    from features.execution_socket import broadcast_to_channel
                    await broadcast_to_channel('update', _build_message(
                        'ltp_update',
                        'Live LTP tick',
                        {
                            'trade_date':       now_ts[:10],
                            'listen_time':      listen_hhmm,
                            'listen_timestamp': now_ts,
                            'ltp':              spot_ltp_list + option_ltp_list + vix_ltp_list,
                            'spot_map':         dict(_tm.spot_map),
                            'broker_status':    _tm.status,
                            'mode':             self.activation_mode,
                        },
                    ))
                    spot_parts = '  '.join(
                        f'{item["underlying"]} = {item["ltp"]:.2f}'
                        for item in spot_ltp_list
                    ) or 'no spot data'
                    print(
                        f'[EMIT → update channel]  {now_ts}'
                        f'  |  broker: {_tm.status}'
                        f'  |  spot: {spot_parts}'
                        f'  |  option tokens: {len(option_ltp_list)}'
                    )
                except Exception as exc:
                    log.error('ltp_update broadcast error: %s', exc)
                    notify_admin('entry_logic_error', f'ltp_update broadcast error: {exc}')

                # ── Event 2: live_strategy_update  (DB query) ─────────────────
                try:
                    records = _load_live_strategies(
                        db, self.trade_date, self.activation_mode
                    )
                    for rec in records:
                        rec['listen_time'] = listen_hhmm

                    print(
                        f'[LIVE MONITOR LOOP] {now_ts} '
                        f'strategies={len(records)}'
                    )

                    for rec in records:
                        entry_raw   = str(rec.get('entry_time') or '')
                        entry_hhmm  = entry_raw[11:16] if len(entry_raw) >= 16 else entry_raw[:5]
                        ticker_name = rec.get('ticker') or ''
                        has_pending = rec.get('open_legs', 0) == 0
                        trade_doc = db._db['algo_trades'].find_one({'_id': rec['_id']}) or {}
                        use_quote = _should_use_quote(trade_doc, self.activation_mode)
                        if use_quote:
                            from features.spot_atm_utils import resolve_atm_price
                            quote_spot_price, quote_instrument = _get_quote_spot_price(
                                db,
                                trade_doc,
                                ticker_name,
                            )
                            quote_atm_price = resolve_atm_price(ticker_name, quote_spot_price) if quote_spot_price > 0 else 0
                            print(
                                f'  [QUOTE SPOT SNAPSHOT] '
                                f'mode={self.activation_mode} | '
                                f'ticker={ticker_name} | '
                                f'quote_enabled={use_quote} | '
                                f'instrument={quote_instrument or "NOT_FOUND"} | '
                                f'spot_price={quote_spot_price} | '
                                f'atm_price={quote_atm_price}'
                            )

                        print(
                            f'  group={rec["group_name"]} | '
                            f'strategy={rec["name"]} | '
                            f'ticker={ticker_name} | '
                            f'entry={entry_hhmm} | '
                            f'current={listen_hhmm} | '
                            f'open_legs={rec["open_legs"]}/{rec["total_legs"]}'
                        )

                        # Entry condition: current_time >= entry_time and legs not yet entered.
                        # is_paused: a sibling leg's entry already failed for this strategy —
                        # don't take any further entries until someone clears it.
                        if (
                            listen_hhmm and entry_hhmm and listen_hhmm >= entry_hhmm and has_pending
                            and not (trade_doc or {}).get('is_paused')
                        ):
                            from features.spot_atm_utils import resolve_atm_price
                            from features.backtest_engine import _resolve_expiry, _resolve_strike, STRIKE_STEPS

                            spot_price, quote_spot_price, spot_token, spot_source = _resolve_entry_spot_price(
                                db,
                                trade_doc,
                                ticker_name,
                                self.activation_mode,
                                now_ts,
                            )
                            atm_price  = resolve_atm_price(ticker_name, spot_price) if spot_price > 0 else 0
                            print(
                                f'  [ENTRY MARKET SNAPSHOT] '
                                f'mode={self.activation_mode} | '
                                f'ticker={ticker_name} | '
                                f'spot_source={spot_source} | '
                                f'spot_token={spot_token or "NOT_FOUND"} | '
                                f'spot_price={spot_price} | '
                                f'atm_price={atm_price}'
                            )
                            print(
                                f'  [ENTRY CONDITION MET] '
                                f'mode={self.activation_mode} | '
                                f'ticker={ticker_name} | '
                                f'entry_time={entry_hhmm} | '
                                f'current_time={listen_hhmm} | '
                                f'spot_price={spot_price} | '
                                f'atm_price={atm_price}'
                            )
                            rec['entry_condition_met'] = True
                            rec['spot_price']          = spot_price
                            rec['atm_price']           = atm_price

                            # Resolve strike and expiry per leg using same logic as algo-backtest
                            leg_entries: list[dict] = []
                            try:
                                if trade_doc:
                                    if use_quote and quote_spot_price > 0:
                                        spot_price = quote_spot_price
                                        atm_price = resolve_atm_price(ticker_name, spot_price)

                                    # Support both config.LegConfigs (dict) and strategy.ListOfLegConfigs (list)
                                    leg_configs: dict = (trade_doc.get('config') or {}).get('LegConfigs') or {}
                                    if not leg_configs:
                                        for i, lc in enumerate((trade_doc.get('strategy') or {}).get('ListOfLegConfigs') or []):
                                            if isinstance(lc, dict):
                                                leg_configs[str(lc.get('id') or i)] = lc

                                    # Get available expiries from active_option_tokens (live instrument data)
                                    step = STRIKE_STEPS.get(ticker_name.upper(), 50)
                                    expiries = sorted([
                                        str(e) for e in db._db['active_option_tokens'].distinct(
                                            'expiry',
                                            {'instrument': ticker_name.upper(), 'expiry': {'$gte': self.trade_date}},
                                        ) if e
                                    ])

                                    leg_entries: list[dict] = []
                                    for leg_id, leg_cfg in leg_configs.items():
                                        contract     = leg_cfg.get('ContractType') or {}
                                        option_raw   = contract.get('Option') or ''
                                        if not option_raw:
                                            inst       = str(leg_cfg.get('InstrumentKind') or '')
                                            option_raw = inst.split('.')[-1] if '.' in inst else inst
                                        expiry_kind  = contract.get('Expiry') or str(leg_cfg.get('ExpiryKind') or '')
                                        strike_param = contract.get('StrikeParameter') or str(leg_cfg.get('StrikeParameter') or 'StrikeType.ATM')

                                        expiry = _resolve_expiry(self.trade_date, expiry_kind, expiries)
                                        strike = _resolve_strike(spot_price, strike_param, option_raw, step) if spot_price > 0 else 0

                                        # Fetch kite token from active_option_tokens
                                        token_doc = db._db['active_option_tokens'].find_one({
                                            'instrument':  ticker_name.upper(),
                                            'expiry':      expiry,
                                            'strike':      strike,
                                            'option_type': option_raw.upper(),
                                        })
                                        token  = str((token_doc or {}).get('token') or '')
                                        symbol = str((token_doc or {}).get('symbol') or '')

                                        # Subscribe token to kite and get live LTP
                                        if token:
                                            _subscribe_token_to_kite(token)
                                        ltp = _get_live_option_ltp(token) if token else 0.0

                                        print(
                                            f'    [LEG ENTRY MARKET] id={leg_id} | '
                                            f'mode={self.activation_mode} | '
                                            f'option={option_raw} | '
                                            f'expiry={expiry} | '
                                            f'strike={strike} | '
                                            f'token={token or "NOT_FOUND"} | '
                                            f'symbol={symbol or "-"} | '
                                            f'spot_token={spot_token or "NOT_FOUND"} | '
                                            f'spot_price={spot_price} | '
                                            f'atm_price={atm_price} | '
                                            f'ltp={ltp}'
                                        )

                                        if token and expiry and strike:
                                            leg_entries.append({
                                                'leg_id':     leg_id,
                                                'option':     option_raw,
                                                'expiry':     expiry,
                                                'strike':     strike,
                                                'token':      token,
                                                'symbol':     symbol,
                                                'ltp':        ltp,
                                                'spot_price': spot_price,
                                                # Lot size synced from Dhan's instrument feed for this
                                                # exact contract — apply_resolved_live_entries prefers
                                                # this over the static lot_sizes table when present.
                                                'lot_size':   (token_doc or {}).get('lot_size'),
                                            })

                            except Exception as _leg_exc:
                                log.error('leg resolve error trade=%s: %s', rec.get('_id'), _leg_exc)
                                notify_admin('entry_logic_error', f'leg resolve error trade={rec.get("_id")}: {_leg_exc}', {'trade_id': str(rec.get('_id') or '')})
                                leg_entries = []

                            # Execute live entry across 3 tables — separate try so a genuinely
                            # unexpected failure here (as opposed to a leg-resolution error above)
                            # is reported as "unknown", per the explicit both-admin-and-user case.
                            if leg_entries:
                                try:
                                    _execute_live_entry(db, trade_doc, leg_entries, now_ts)
                                except Exception as _entry_exc:
                                    log.error('_execute_live_entry unknown error trade=%s: %s', rec.get('_id'), _entry_exc)
                                    notify_both(
                                        'unknown_entry_error',
                                        f'Unexpected error taking entry for trade={rec.get("_id")}: {_entry_exc}',
                                        {'trade_id': str(rec.get('_id') or '')},
                                    )

                    await _broadcast(_build_message(
                        'live_strategy_update',
                        'Live strategy update',
                        {
                            'timestamp':       now_ts,
                            'listen_time':     listen_hhmm,
                            'trade_date':      self.trade_date,
                            'activation_mode': self.activation_mode,
                            'records':         records,
                            'count':           len(records),
                        },
                    ))
                except Exception as exc:
                    log.error('live_strategy_update error: %s', exc)
                    notify_admin('entry_logic_error', f'live_strategy_update error: {exc}')

                await asyncio.sleep(1)

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error('[LIVE MONITOR LOOP] fatal error: %s', exc)
            self._running = False
        finally:
            try:
                db.close()
            except Exception:
                pass
            print('[LIVE MONITOR LOOP] exited')


# Singleton — imported by api.py
live_monitor_loop = _LiveMonitorLoop()
