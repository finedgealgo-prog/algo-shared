"""
leg_range_monitor.py — per-leg BTST Range Breakout live watcher
──────────────────────────────────────────────────────────────────
Follows the exact same template as live_entry_monitor.py / live_exit_monitor.py:
an asyncio loop, ticking every second, started from FastAPI @startup and
stopped on @shutdown, re-querying Mongo fresh every cycle (no in-memory-only
state) so a mid-cycle process restart just resumes from whatever was last
persisted.

Unlike the scrapped v1 (btst_range_monitor.py, strategy-level), this is
keyed PER LEG — matching the real AlgoTest "Range Breakout BTST" feature:
each option leg (CE, PE, ...) independently tracks its own range (spot or
its own option premium) and enters on its own breakout, completely
decoupled from any other leg on the same strategy. See
range_breakout.py::parse_leg_range_breakout and each ListOfLegConfigs[i]'s
"LegRangeBreakout" key.

What this module does NOT do: it never calls a broker or places an order,
and (per the corrected exit-timing model — range spans Day1->Day2, but
entry AND exit both happen on Day2, there is no Day3) it never touches
exit_time either. On breakout it calls
trading_core.queue_leg_range_breakout_entry(), which queues a normal
'pending_entry' algo_leg_feature_status row — the already-running
execution_socket._process_momentum_pending_feature_legs() picks it up and
enters through the exact same order path every other live leg uses. Once
entered, the leg becomes an ordinary open leg governed by the strategy's
own existing exit/SL/target machinery — nothing further for this monitor
to do for that leg.

Lifecycle
─────────
  start(loop)   ← called from FastAPI @startup (alongside live_entry_monitor)
  stop()        ← called from FastAPI @shutdown

Scope (Phase 2 — see plan doc): trigger_source Underlying/BTSTUnderlying
only (spot price). Instrument/BTSTInstrument (the option's own premium —
AlgoTest's actual default) is Phase 3, added as a second price-source
branch in _update_leg_range/_check_leg_breakout without changing this
module's state machine.
"""

from __future__ import annotations

import asyncio
import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

from features.trading_calendar import next_valid_trading_day, is_trading_day
from features.range_breakout import parse_leg_range_breakout

log = logging.getLogger(__name__)

RUNNING_STATUS = 'StrategyStatus.Live_Running'
CACHE_TTL_SECONDS = 5.0

MARKET_OPEN_HHMM = '09:15'
MARKET_CLOSE_HHMM = '15:30'

_ACTIVE_STATES = {
    'LegRangeState.CollectingDay1',
    'LegRangeState.WaitingForNextSession',
    'LegRangeState.CollectingDay2',
    'LegRangeState.RangeFrozen',
    'LegRangeState.WaitingForBreakout',
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _now_ist_ts() -> str:
    return (_now_utc() + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%dT%H:%M:%S')


class LegRangeMonitor:
    """Asyncio-based per-leg BTST Range Breakout watcher, ticking every second."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._running: bool = False
        self._task: asyncio.Task | None = None
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='leg-range')
        self._db = None

        self._trades_cache: list[dict] = []
        self._cache_loaded_at: float = 0.0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._running:
            print('[LegRangeMonitor] already running — start() ignored')
            return

        self._loop = loop
        self._running = True
        self._trades_cache = []
        self._cache_loaded_at = 0.0

        if self._db is None:
            from features.mongo_data import MongoData
            self._db = MongoData()

        if self._task and not self._task.done():
            self._task.cancel()

        self._task = loop.create_task(self._monitor_loop(), name='leg-range-monitor')
        print('[LegRangeMonitor] started — checking every second')

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None
        self._trades_cache = []
        self._cache_loaded_at = 0.0
        print('[LegRangeMonitor] stopped')

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _monitor_loop(self) -> None:
        while self._running:
            try:
                await self._loop.run_in_executor(self._executor, self._tick)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception('[LegRangeMonitor] loop error: %s', exc)
            await asyncio.sleep(1.0)

    def _tick(self) -> None:
        now_ts = _now_ist_ts()
        now_hhmm = now_ts[11:16]
        today = now_ts[:10]

        cache_age = time.monotonic() - self._cache_loaded_at
        if cache_age >= CACHE_TTL_SECONDS or not self._trades_cache:
            self._reload_range_leg_trades_cache()

        for trade in self._trades_cache:
            try:
                self._ensure_cycles_exist(trade, today)
            except Exception as exc:
                log.exception('[LegRangeMonitor] ensure_cycles error trade=%s: %s', trade.get('_id'), exc)

        try:
            active_cycles = list(self._db._db['algo_leg_range_cycles'].find(
                {'leg_range_state': {'$in': list(_ACTIVE_STATES)}}
            ))
        except Exception as exc:
            log.exception('[LegRangeMonitor] load active cycles error: %s', exc)
            return

        for cycle in active_cycles:
            try:
                self._advance_cycle(cycle, now_ts, now_hhmm, today)
            except Exception as exc:
                log.exception('[LegRangeMonitor] advance_cycle error cycle=%s: %s', cycle.get('_id'), exc)

    # ── Discover legs with range breakout enabled ───────────────────────────────

    def _reload_range_leg_trades_cache(self) -> None:
        try:
            trades = list(self._db._db['algo_trades'].find({
                'trade_status': 1,
                'activation_mode': {'$in': ['live', 'forward-test', 'fast-forward']},
                'status': RUNNING_STATUS,
            }))
            self._trades_cache = trades
            self._cache_loaded_at = time.monotonic()
        except Exception as exc:
            log.exception('[LegRangeMonitor] _reload_range_leg_trades_cache error: %s', exc)

    def _ensure_cycles_exist(self, trade: dict, today: str) -> None:
        """
        For every leg on this trade whose LegRangeBreakout.Type != "None",
        create an algo_leg_range_cycles doc if one doesn't already exist
        (unique index on trade_id+leg_id+cycle_day1 is the concurrency
        backstop — a racing insert simply fails with DuplicateKeyError and
        is ignored).

        Skips legs that already have an open position or an in-flight
        pending_entry — this monitor only owns the range-BUILDING phase;
        once entered, the leg is out of its scope entirely.
        """
        from features.trading_core import resolve_trade_leg_configs

        trade_id = str(trade.get('_id') or '')
        strategy_id = str(trade.get('strategy_id') or trade_id)
        underlying = str(
            (trade.get('strategy') or trade.get('config') or {}).get('Ticker')
            or trade.get('ticker') or ''
        ).strip().upper()
        if not underlying:
            return

        already_entered_leg_ids = {
            str(leg.get('id') or leg) if isinstance(leg, dict) else str(leg)
            for leg in (trade.get('legs') or [])
        }
        try:
            already_queued_leg_ids = {
                str(doc.get('leg_id') or '')
                for doc in self._db._db['algo_leg_feature_status'].find(
                    {
                        'trade_id': trade_id,
                        'feature': 'pending_entry',
                        'status': {'$in': ['active', 'triggered', 'processing']},
                    },
                    {'leg_id': 1},
                )
            }
        except Exception:
            already_queued_leg_ids = set()

        leg_configs = resolve_trade_leg_configs(trade)
        for leg_id, leg_cfg in leg_configs.items():
            if leg_id in already_entered_leg_ids or leg_id in already_queued_leg_ids:
                continue

            rb_type, condition, start_hhmm, end_hhmm = parse_leg_range_breakout(leg_cfg)
            if rb_type == 'None':
                continue

            existing = self._db._db['algo_leg_range_cycles'].find_one({
                'trade_id': trade_id,
                'leg_id': leg_id,
                'leg_range_state': {'$nin': ['LegRangeState.Entered', 'LegRangeState.Cancelled', 'LegRangeState.Error']},
            })
            if existing:
                continue

            holidays = self._db.get_holidays()
            spans_next_day = 'BTST' in rb_type
            day1 = today if is_trading_day(today, holidays) else next_valid_trading_day(today, holidays)
            day2 = next_valid_trading_day(day1, holidays) if spans_next_day else day1

            cycle_doc = {
                'trade_id': trade_id,
                'leg_id': leg_id,
                'strategy_id': strategy_id,
                'activation_mode': str(trade.get('activation_mode') or 'live'),
                'underlying': underlying,
                'option_type': str(leg_cfg.get('InstrumentKind') or '').split('.')[-1] or 'CE',
                'rb_type': rb_type,
                'condition': condition,
                'expiry_kind': leg_cfg.get('ExpiryKind') or 'ExpiryType.Weekly',
                'entry_kind': leg_cfg.get('EntryType') or 'EntryType.EntryByStrikeType',
                'strike_parameter': leg_cfg.get('StrikeParameter') or 'StrikeType.ATM',
                'position': leg_cfg.get('PositionType') or 'PositionType.Sell',
                'lot_config_value': int((leg_cfg.get('LotConfig') or {}).get('Value') or 1),
                'cycle_day1': day1,
                'cycle_day2': day2,
                'range_start_hhmm': start_hhmm,
                'range_end_hhmm': end_hhmm,
                'strike': None, 'expiry_date': None, 'token': None, 'symbol': None,
                'range_high': None, 'range_low': None, 'range_frozen': False,
                'leg_range_state': 'LegRangeState.CollectingDay1',
                'breakout_timestamp': None, 'breakout_price': None, 'skip_reason': None,
                'created_at': _now_ist_ts(), 'updated_at': _now_ist_ts(),
            }
            try:
                self._db._db['algo_leg_range_cycles'].insert_one(cycle_doc)
                print(f'[LegRangeMonitor] cycle created trade={trade_id} leg={leg_id} '
                      f'type={rb_type} day1={day1} day2={day2}')
            except Exception as exc:
                if 'duplicate key' not in str(exc).lower():
                    log.warning('[LegRangeMonitor] cycle insert error trade=%s leg=%s: %s', trade_id, leg_id, exc)

    # ── Advance one cycle's state ──────────────────────────────────────────────

    def _advance_cycle(self, cycle: dict, now_ts: str, now_hhmm: str, today: str) -> None:
        state = cycle.get('leg_range_state')

        if state == 'LegRangeState.CollectingDay1':
            self._collect_day1(cycle, now_ts, now_hhmm, today)
        elif state == 'LegRangeState.WaitingForNextSession':
            self._wait_for_next_session(cycle, today)
        elif state == 'LegRangeState.CollectingDay2':
            self._collect_day2(cycle, now_ts, now_hhmm, today)
        elif state == 'LegRangeState.RangeFrozen':
            self._arm_breakout_watch(cycle)
        elif state == 'LegRangeState.WaitingForBreakout':
            self._check_breakout(cycle, now_ts)

    def _collect_day1(self, cycle: dict, now_ts: str, now_hhmm: str, today: str) -> None:
        day1 = cycle['cycle_day1']
        day2 = cycle['cycle_day2']
        same_day = day1 == day2

        if today != day1:
            self._db._db['algo_leg_range_cycles'].update_one(
                {'_id': cycle['_id']},
                {'$set': {'leg_range_state': 'LegRangeState.WaitingForNextSession', 'updated_at': now_ts}},
            )
            return

        if now_hhmm < cycle['range_start_hhmm']:
            return  # not yet inside the collection window

        # Same-day (non-BTST) ORB: freeze at range_end_hhmm today instead of
        # rolling to a next session.
        if same_day and now_hhmm >= cycle['range_end_hhmm']:
            self._freeze_range(cycle, now_ts)
            return
        if not same_day and now_hhmm >= MARKET_CLOSE_HHMM:
            self._db._db['algo_leg_range_cycles'].update_one(
                {'_id': cycle['_id']},
                {'$set': {'leg_range_state': 'LegRangeState.WaitingForNextSession', 'updated_at': now_ts}},
            )
            return

        self._update_leg_range(cycle, now_ts)

    def _wait_for_next_session(self, cycle: dict, today: str) -> None:
        day2 = cycle['cycle_day2']
        if today >= day2:
            self._db._db['algo_leg_range_cycles'].find_one_and_update(
                {'_id': cycle['_id'], 'leg_range_state': 'LegRangeState.WaitingForNextSession'},
                {'$set': {'leg_range_state': 'LegRangeState.CollectingDay2', 'updated_at': _now_ist_ts()}},
            )

    def _collect_day2(self, cycle: dict, now_ts: str, now_hhmm: str, today: str) -> None:
        if today < cycle['cycle_day2'] or now_hhmm < MARKET_OPEN_HHMM:
            return

        if now_hhmm >= cycle['range_end_hhmm']:
            self._freeze_range(cycle, now_ts)
            return

        self._update_leg_range(cycle, now_ts)

    def _update_leg_range(self, cycle: dict, now_ts: str) -> None:
        price = self._get_price(cycle, now_ts)
        if price is None or price <= 0:
            return

        current_high = cycle.get('range_high')
        current_low = cycle.get('range_low')
        new_high = price if current_high is None else max(current_high, price)
        new_low = price if current_low is None else min(current_low, price)

        self._db._db['algo_leg_range_cycles'].update_one(
            {'_id': cycle['_id']},
            {'$set': {'range_high': new_high, 'range_low': new_low, 'updated_at': now_ts}},
        )

    def _freeze_range(self, cycle: dict, now_ts: str) -> None:
        range_high = cycle.get('range_high')
        range_low = cycle.get('range_low')
        from_state = cycle['leg_range_state']  # CollectingDay1 (same-day) or CollectingDay2 (BTST)

        if range_high is None or range_low is None:
            self._db._db['algo_leg_range_cycles'].find_one_and_update(
                {'_id': cycle['_id'], 'leg_range_state': from_state},
                {'$set': {
                    'leg_range_state': 'LegRangeState.Cancelled',
                    'skip_reason': 'no_price_data_in_range_window',
                    'updated_at': now_ts,
                }},
            )
            return

        # CAS on current state — atomic "freeze"; a racing second worker's
        # update simply matches 0 docs.
        self._db._db['algo_leg_range_cycles'].find_one_and_update(
            {'_id': cycle['_id'], 'leg_range_state': from_state},
            {'$set': {
                'range_frozen': True,
                'leg_range_state': 'LegRangeState.RangeFrozen',
                'updated_at': now_ts,
            }},
        )
        print(f'[LegRangeMonitor] range frozen trade={cycle["trade_id"]} leg={cycle["leg_id"]} '
              f'high={range_high} low={range_low}')

    def _arm_breakout_watch(self, cycle: dict) -> None:
        self._db._db['algo_leg_range_cycles'].find_one_and_update(
            {'_id': cycle['_id'], 'leg_range_state': 'LegRangeState.RangeFrozen'},
            {'$set': {'leg_range_state': 'LegRangeState.WaitingForBreakout', 'updated_at': _now_ist_ts()}},
        )

    def _check_breakout(self, cycle: dict, now_ts: str) -> None:
        price = self._get_price(cycle, now_ts)
        if price is None or price <= 0:
            return

        range_high = cycle['range_high']
        range_low = cycle['range_low']
        condition = cycle['condition']

        breakout = (
            (condition == 'High' and price > range_high) or
            (condition == 'Low' and price < range_low)
        )
        if not breakout:
            return

        # Atomic CAS to Entered-pending — only the worker whose update
        # actually matches WaitingForBreakout proceeds to queue the entry.
        result = self._db._db['algo_leg_range_cycles'].find_one_and_update(
            {'_id': cycle['_id'], 'leg_range_state': 'LegRangeState.WaitingForBreakout'},
            {'$set': {
                'leg_range_state': 'LegRangeState.Entered',
                'breakout_timestamp': now_ts,
                'breakout_price': price,
                'updated_at': now_ts,
            }},
        )
        if result is None:
            return  # another worker already won this transition

        print(f'[LegRangeMonitor] breakout trade={cycle["trade_id"]} leg={cycle["leg_id"]} '
              f'condition={condition} price={price}')

        from features.trading_core import queue_leg_range_breakout_entry
        stamped_cycle = self._db._db['algo_leg_range_cycles'].find_one({'_id': cycle['_id']})
        queue_leg_range_breakout_entry(self._db, stamped_cycle, now_ts)

    # ── Price source ─────────────────────────────────────────────────────────

    # Virtual user_id for Kite token subscriptions owned by this monitor's
    # option-premium tracking — mirrors live_entry_monitor.py's
    # _KITE_USER_ID_OPTION pattern (a distinct namespace so unsubscribing
    # elsewhere never touches these tokens).
    _KITE_USER_ID_LEG_RANGE = '__leg_range_option__'

    def _get_price(self, cycle: dict, now_ts: str):
        """
        Underlying/BTSTUnderlying -> spot price (Phase 2).
        Instrument/BTSTInstrument -> the leg's own option premium (Phase 3,
        AlgoTest's actual default — "we track the ATM put option itself,
        not the underlying"). The contract (strike/expiry/token/symbol) is
        resolved once, at range_start, and frozen onto the cycle doc so it
        never changes mid-cycle even if spot drifts across strike
        boundaries afterward.
        """
        rb_type = cycle.get('rb_type', '')
        if 'Underlying' in rb_type:
            return self._get_spot_price(cycle['underlying'], now_ts)

        if not cycle.get('token'):
            self._resolve_and_freeze_contract(cycle)
            return None  # contract not resolved yet — retry next tick

        return self._get_option_ltp(cycle['token'])

    def _resolve_and_freeze_contract(self, cycle: dict) -> None:
        """
        Resolve this leg's own option contract (strike/expiry) at the
        current spot, look up its Kite token, subscribe it, and freeze all
        four fields onto the cycle doc — mirrors the existing strike-lock-
        once pattern used elsewhere in the live system (e.g.
        live_entry_monitor.py's pre-subscribe flow) so the same contract
        stays in force for the rest of the Day1->Day2 window regardless of
        later spot movement.
        """
        from features.spot_atm_utils import get_kite_expiries, get_strike_step  # type: ignore
        from features.backtest_engine import _resolve_expiry, _resolve_strike  # type: ignore
        from features.broker_gateway import broker_register_user_tokens as register_user_tokens, broker_is_configured as is_configured  # type: ignore

        if not is_configured():
            return

        underlying = cycle['underlying']
        day1 = cycle['cycle_day1']
        spot = self._get_spot_price(underlying, _now_ist_ts())
        if spot <= 0:
            return

        expiries = get_kite_expiries(underlying, day1)
        if not expiries:
            return
        expiry = _resolve_expiry(day1, cycle.get('expiry_kind') or 'ExpiryType.Weekly', expiries)
        if not expiry:
            return

        step = get_strike_step(underlying)
        strike = _resolve_strike(spot, cycle.get('strike_parameter') or 'StrikeType.ATM', cycle['option_type'], step)

        doc = self._db._db['active_option_tokens'].find_one(
            {
                'instrument': underlying,
                'expiry': expiry,
                'strike': float(strike),
                'option_type': cycle['option_type'],
            },
            {'_id': 0, 'token': 1, 'tokens': 1, 'symbol': 1},
        ) or {}
        token = str(doc.get('token') or doc.get('tokens') or '').strip()
        if not token or not token.isdigit():
            return  # not found yet — retry next tick, e.g. before market data loads

        self._db._db['algo_leg_range_cycles'].update_one(
            {'_id': cycle['_id'], 'token': None},
            {'$set': {
                'strike': strike,
                'expiry_date': expiry,
                'token': token,
                'symbol': str(doc.get('symbol') or '').strip(),
                'updated_at': _now_ist_ts(),
            }},
        )
        try:
            register_user_tokens(self._KITE_USER_ID_LEG_RANGE, [int(token)])
        except Exception as exc:
            log.warning('[LegRangeMonitor] token subscribe error token=%s: %s', token, exc)
        print(f'[LegRangeMonitor] contract frozen trade={cycle["trade_id"]} leg={cycle["leg_id"]} '
              f'strike={strike} expiry={expiry} token={token}')

    def _get_option_ltp(self, token: str) -> float | None:
        from features.broker_gateway import get_broker_ltp_map as get_ltp_map  # type: ignore

        ltp_map = get_ltp_map()
        ltp = float(ltp_map.get(str(token), 0.0))
        return ltp if ltp > 0 else None

    def _get_spot_price(self, underlying: str, now_ts: str) -> float:
        from features.spot_atm_utils import KITE_INDEX_TOKENS as INDEX_TOKENS  # type: ignore
        from features.broker_gateway import get_broker_ltp_map as get_ltp_map  # type: ignore

        idx_tok = INDEX_TOKENS.get(underlying.upper())
        if idx_tok:
            ltp_map = get_ltp_map()
            kite_spot = float(ltp_map.get(str(idx_tok), 0.0))
            if kite_spot > 0:
                return kite_spot

        try:
            doc = self._db._db['option_chain_index_spot'].find_one(
                {'underlying': underlying.upper(), 'timestamp': {'$lte': now_ts}},
                sort=[('timestamp', -1)],
            )
            if doc:
                val = doc.get('spot_price') or doc.get('close') or doc.get('ltp') or 0
                return float(val) if val else 0.0
        except Exception as exc:
            log.warning('[LegRangeMonitor] spot DB lookup error: %s', exc)

        return 0.0


# ─── Module-level singleton ────────────────────────────────────────────────────

_monitor: LegRangeMonitor | None = None


def get_monitor() -> LegRangeMonitor:
    global _monitor
    if _monitor is None:
        _monitor = LegRangeMonitor()
    return _monitor


def start(loop: asyncio.AbstractEventLoop) -> None:
    get_monitor().start(loop)


def stop() -> None:
    if _monitor:
        _monitor.stop()
