"""
live_entry_monitor.py — Live Strategy Entry Monitor
────────────────────────────────────────────────────
Checks all Live_Running strategies every second from the moment the monitor
starts. Prints a countdown for each waiting strategy and triggers entry
using the same process_pending_entries() used by algo-backtest.

Entry price comes from live Kite LTP (kite_broker_ws) — the same kite
ticker socket that emits continuously is used as the price source.

Print format (every second, per strategy):
  [ENTRY MONITOR] strategy=X  entry_time=09:35  current=09:33  diff=00:01:15  status=waiting_for_entry
  [ENTRY TRIGGER] strategy=X  entry_time=09:35  elapsed=00:00:03  → taking entry now
  [LIVE LTP ENTRY] leg=L1  token=12345678  db_price=145.2  → kite_ltp=147.5
  [ENTRY SUCCESS]  strategy=X  entries=2
    [LEG ENTRY] leg=L1  strike=24500  price=147.5

Architecture
────────────
  • Runs on the same event loop as FastAPI / LiveMonitorService.
  • Entry logic: process_pending_entries() from trading_core — identical to
    algo-backtest (same file, same function).
  • LTP source: kite_broker_ws.get_ltp_map() — the shared kite WebSocket
    that is already running; no second connection.
  • Token subscription: option tokens are pre-subscribed 5 minutes before
    entry time so Kite has time to start sending ticks for those tokens.

Lifecycle
─────────
  start(loop)   ← called from FastAPI @startup (alongside LiveMonitorService)
  stop()        ← called from FastAPI @shutdown
  attach_kite_listener()  ← called after POST /kite/config
"""

from __future__ import annotations

import asyncio
import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)


def _trace_stdout(message: str) -> None:
    """Print entry-monitor traces immediately to the Python terminal."""
    runtime_print(message, flush=True)

# ─── Constants ────────────────────────────────────────────────────────────────

RUNNING_STATUS        = 'StrategyStatus.Live_Running'
OPEN_LEG_STATUS       = 1
CACHE_TTL_SECONDS     = 5.0       # reload live trades from DB every 5s
PRE_SUBSCRIBE_MINUTES = 5         # subscribe option tokens this many min before entry

# Index token mapping lives in spot_atm_utils (shared across all modules)
from features.spot_atm_utils import KITE_INDEX_TOKENS as INDEX_TOKENS  # type: ignore
from features.debug_flags import runtime_print

# Virtual user_id used for kite subscriptions owned by this monitor
_KITE_USER_ID_INDEX  = '__live_entry_index__'
_KITE_USER_ID_OPTION = '__live_entry_option__'


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _now_ist_ts() -> str:
    """Current IST as ISO string: 'YYYY-MM-DDTHH:MM:SS'."""
    return (_now_utc() + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%dT%H:%M:%S')


def _now_ist_hhmm() -> str:
    """Current IST as 'HH:MM'."""
    return _now_ist_ts()[11:16]


def _now_ist_seconds() -> int:
    """Current IST time as total seconds since midnight."""
    ts = _now_ist_ts()
    h, m, s = int(ts[11:13]), int(ts[14:16]), int(ts[17:19])
    return h * 3600 + m * 60 + s


def _hhmm_to_seconds(hhmm: str) -> int:
    """'HH:MM' → total seconds since midnight."""
    try:
        parts = str(hhmm or '').strip().split(':')
        return int(parts[0]) * 3600 + int(parts[1]) * 60
    except Exception:
        return -1


def _format_seconds(total_seconds: int) -> str:
    """Format |seconds| as 'HH:MM:SS'."""
    total_seconds = abs(int(total_seconds))
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f'{h:02d}:{m:02d}:{s:02d}'


def _extract_entry_hhmm(raw: str) -> str:
    """Extract HH:MM from entry_time which may be full ISO or just HH:MM."""
    raw = str(raw or '').strip()
    if len(raw) >= 16:
        return raw[11:16]   # 'YYYY-MM-DDTHH:MM...'
    return raw[:5]           # 'HH:MM'


def _resolve_option_type(cfg: dict) -> str:
    option_type = str(cfg.get('InstrumentKind') or '').replace('LegType.', '').upper()
    return option_type if option_type in ('CE', 'PE') else 'CE'


# ─── Service ──────────────────────────────────────────────────────────────────

class LiveEntryMonitor:
    """
    Asyncio-based entry monitor that runs every second.

    Responsibilities:
    1. Every second: load Live_Running strategies, print countdown per trade.
    2. 5 min before entry: pre-subscribe option tokens to Kite.
    3. At entry_time: call process_pending_entries() with live Kite LTP.
    4. After entry: subscribe entered-leg tokens to Kite for SL/TP monitoring.
    """

    def __init__(self) -> None:
        self._loop:    asyncio.AbstractEventLoop | None = None
        self._running: bool = False
        self._task:    asyncio.Task | None = None
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='live-entry')

        # Persistent DB connection (opened in start())
        self._db = None

        # Track trades we've already triggered entry for (avoid double-entry)
        self._entered_trade_ids: set[str] = set()

        # Track (trade_id, option_type) pairs already pre-subscribed
        self._pre_subscribed: set[tuple] = set()

        # Track trade_ids already logged as fast-forward (option-chain entry
        # path) so the [ENTRY SKIP] line prints once per trade instead of
        # every second for as long as that strategy stays active.
        self._fast_forward_skip_logged: set[str] = set()

        # In-memory trade cache with TTL
        self._trades_cache:    list[dict] = []
        self._cache_loaded_at: float = 0.0

        self._lock = threading.Lock()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._running:
            runtime_print('[LiveEntryMonitor] already running — start() ignored')
            return

        self._loop    = loop
        self._running = True

        # ── Reset all state for a clean start from current time ───────────────
        with self._lock:
            self._entered_trade_ids.clear()
            self._pre_subscribed.clear()
            self._fast_forward_skip_logged.clear()
        self._trades_cache    = []
        self._cache_loaded_at = 0.0   # force immediate reload on first tick

        # Re-open DB connection (may have been closed by a previous stop())
        if self._db is None:
            from features.mongo_data import MongoData
            self._db = MongoData()
            runtime_print('[LiveEntryMonitor] DB connection opened')

        # Cancel any stale task left from a previous stop()
        if self._task and not self._task.done():
            self._task.cancel()

        self._task = loop.create_task(
            self._monitor_loop(), name='live-entry-monitor'
        )
        runtime_print('[LiveEntryMonitor] started — checking every second from current time')

        # Subscribe Kite index tokens immediately (for live spot price)
        self._subscribe_index_tokens()
        # Subscribe tokens for any already-entered open legs (restart recovery)
        self._subscribe_existing_open_leg_tokens()

    def stop(self) -> None:
        """
        Stop the entry monitor cleanly.
        Clears all in-memory state so the next start() is a fresh run.
        """
        self._running = False

        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None

        # Clear runtime state — fresh start when start() is called again
        with self._lock:
            self._entered_trade_ids.clear()
            self._pre_subscribed.clear()
            self._fast_forward_skip_logged.clear()
        self._trades_cache    = []
        self._cache_loaded_at = 0.0

        # Close DB
        if self._db:
            try:
                self._db.close()
            except Exception:
                pass
            self._db = None

        runtime_print('[LiveEntryMonitor] stopped — state cleared')

    def attach_kite_listener(self) -> None:
        """Re-subscribe index tokens after Kite credentials are updated."""
        self._subscribe_index_tokens()
        self._subscribe_existing_open_leg_tokens()

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _monitor_loop(self) -> None:
        runtime_print('[LiveEntryMonitor] loop started — waiting for Live_Running strategies')
        while self._running:
            try:
                await self._loop.run_in_executor(
                    self._executor, self._check_all_strategies
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception('[LiveEntryMonitor] loop error: %s', exc)
            await asyncio.sleep(1.0)
        runtime_print('[LiveEntryMonitor] loop exited')

    # ── Core: check every strategy every second ───────────────────────────────

    def _check_all_strategies(self) -> None:
        """
        Called every second in thread pool.
        For each live / fast-forward running strategy:
          - Print countdown if waiting for entry time
          - Trigger entry when entry_time <= now
        """
        now_ts     = _now_ist_ts()
        now_sec    = _now_ist_seconds()
        now_hhmm   = now_ts[11:16]
        trade_date = now_ts[:10]

        # ── Reload trade cache if stale ───────────────────────────────────────
        cache_age = time.monotonic() - self._cache_loaded_at
        if cache_age >= CACHE_TTL_SECONDS or not self._trades_cache:
            self._reload_cache(trade_date)

        trades = self._trades_cache
        if not trades:
            # Print once every 30s so we know the monitor is alive but idle
            if int(time.monotonic()) % 30 == 0:
                runtime_print(
                    f'[ENTRY MONITOR]  no active live/fast-forward strategies found'
                    f'  ({now_hhmm} IST)'
                )
            return

        for trade in trades:
            try:
                self._process_trade(trade, trade_date, now_ts, now_sec, now_hhmm)
            except Exception as exc:
                trade_id = str(trade.get('_id') or '')
                log.exception('[LiveEntryMonitor] trade=%s error: %s', trade_id, exc)

    def _process_trade(
        self,
        trade: dict,
        trade_date: str,
        now_ts: str,
        now_sec: int,
        now_hhmm: str,
    ) -> None:
        trade_id      = str(trade.get('_id') or '')
        strategy_name = str(trade.get('name') or trade.get('strategy_name') or '')
        activation_mode = str(trade.get('activation_mode') or '').strip().lower()

        # Fast-forward/forward-test entries must happen only after execution_socket
        # resolves the option chain and queues/enters the pending legs. If this
        # monitor also triggers entry, the same parent legs can be bootstrapped twice.
        if activation_mode in ('fast-forward', 'forward-test'):
            # Log this once per trade, not every second — the reason never
            # changes for as long as the strategy stays active, so repeating
            # it endlessly is just noise (the trade itself is correctly being
            # entered via execution_socket's option-chain path, not skipped).
            with self._lock:
                already_logged = trade_id in self._fast_forward_skip_logged
                if not already_logged:
                    self._fast_forward_skip_logged.add(trade_id)
            if not already_logged:
                _trace_stdout(
                    f'[ENTRY SKIP]  strategy={strategy_name}  trade_id={trade_id}  '
                    f'reason=fast_forward_uses_option_chain_entry_path'
                )
            return

        raw_entry     = str(trade.get('entry_time') or '')
        entry_hhmm    = _extract_entry_hhmm(raw_entry)

        if not entry_hhmm or len(entry_hhmm) < 5:
            _trace_stdout(
                f'[ENTRY SKIP]  strategy={strategy_name}  trade_id={trade_id}  '
                f'reason=invalid_entry_time  raw_entry={raw_entry or "-"}'
            )
            return

        entry_sec = _hhmm_to_seconds(entry_hhmm)
        if entry_sec < 0:
            _trace_stdout(
                f'[ENTRY SKIP]  strategy={strategy_name}  trade_id={trade_id}  '
                f'reason=entry_time_parse_failed  entry_time={entry_hhmm}'
            )
            return

        _trace_stdout(
            f'[ENTRY CHECK]  strategy={strategy_name}  trade_id={trade_id}  '
            f'entry_time={entry_hhmm}  current={now_hhmm}'
        )

        diff_sec = entry_sec - now_sec   # positive = still waiting, negative = elapsed

        if diff_sec > 0:
            # ── Waiting for entry ─────────────────────────────────────────────
            diff_str = _format_seconds(diff_sec)
            _trace_stdout(
                f'[ENTRY MONITOR]  '
                f'strategy={strategy_name}  '
                f'entry_time={entry_hhmm}  '
                f'current={now_hhmm}  '
                f'diff={diff_str}  '
                f'status=waiting_for_entry'
            )

            # Pre-subscribe option tokens 5 min before entry
            if diff_sec <= PRE_SUBSCRIBE_MINUTES * 60:
                self._pre_subscribe_option_tokens(trade, trade_date, now_ts)

        else:
            # ── Entry time reached ────────────────────────────────────────────
            open_history_count = self._db._db['algo_trade_positions_history'].count_documents(
                {'trade_id': trade_id, 'status': OPEN_LEG_STATUS}
            )

            # Skip if we already triggered entry for this trade
            with self._lock:
                if trade_id in self._entered_trade_ids:
                    _trace_stdout(
                        f'[ENTRY SKIP]  strategy={strategy_name}  trade_id={trade_id}  '
                        f'reason=already_marked_entered'
                    )
                    _trace_stdout(
                        f'[ENTRY EVENT CHECK]  strategy={strategy_name}  trade_id={trade_id}  '
                        f'open_history_legs={open_history_count}  '
                        f'next_handler={"LiveMonitorService" if open_history_count > 0 else "waiting_for_history"}'
                    )
                    return

            # Skip if DB already has entries for this trade
            already = open_history_count > 0
            if already:
                with self._lock:
                    self._entered_trade_ids.add(trade_id)
                _trace_stdout(
                    f'[ENTRY SKIP]  strategy={strategy_name}  trade_id={trade_id}  '
                    f'reason=open_position_history_exists'
                )
                _trace_stdout(
                    f'[ENTRY EVENT CHECK]  strategy={strategy_name}  trade_id={trade_id}  '
                    f'open_history_legs={open_history_count}  '
                    f'next_handler=LiveMonitorService'
                )
                return

            elapsed_str = _format_seconds(abs(diff_sec))
            _trace_stdout(
                f'[ENTRY TRIGGER]  '
                f'strategy={strategy_name}  '
                f'entry_time={entry_hhmm}  '
                f'current={now_hhmm}  '
                f'elapsed={elapsed_str}  '
                f'→ taking entry now'
            )

            self._take_entry(trade, trade_date, now_ts)

    # ── Pre-subscribe option tokens ───────────────────────────────────────────

    def _resolve_trade_entry_tokens(
        self,
        trade: dict,
        trade_date: str,
        now_ts: str,
    ) -> list[dict]:
        underlying = str(
            (trade.get('strategy') or trade.get('config') or {}).get('Ticker')
            or trade.get('ticker') or ''
        ).strip().upper()
        if not underlying:
            return []

        strategy_cfg = trade.get('strategy') or trade.get('config') or {}
        leg_configs = strategy_cfg.get('ListOfLegConfigs') or []
        if not leg_configs:
            return []

        spot = self._get_spot_price(underlying, now_ts)
        if spot <= 0:
            _trace_stdout(
                f'[ENTRY TOKEN RESOLVE] strategy={str(trade.get("name") or "")} '
                f'underlying={underlying} spot=UNAVAILABLE'
            )
            return []

        from features.spot_atm_utils import get_kite_expiries, get_strike_step  # type: ignore
        from features.backtest_engine import _resolve_expiry, _resolve_strike  # type: ignore

        expiries = get_kite_expiries(underlying, trade_date)
        if not expiries:
            _trace_stdout(
                f'[ENTRY TOKEN RESOLVE] strategy={str(trade.get("name") or "")} '
                f'underlying={underlying} expiries=UNAVAILABLE'
            )
            return []

        resolved: list[dict] = []
        for cfg in leg_configs:
            if not isinstance(cfg, dict):
                continue

            leg_cfg_id = str(cfg.get('id') or '').strip()
            if not leg_cfg_id:
                continue

            option_type = _resolve_option_type(cfg)
            expiry_kind = str(cfg.get('ExpiryKind') or 'ExpiryType.Weekly')
            expiry = _resolve_expiry(trade_date, expiry_kind, expiries) if expiries else None
            if not expiry:
                continue

            step = get_strike_step(underlying)
            strike = _resolve_strike(
                spot,
                str(cfg.get('StrikeParameter') or 'StrikeType.ATM'),
                option_type,
                step,
            )

            doc = self._db._db['active_option_tokens'].find_one(
                {
                    'instrument': underlying,
                    'expiry': expiry,
                    'strike': float(strike),
                    'option_type': option_type,
                },
                {'_id': 0, 'token': 1, 'tokens': 1, 'symbol': 1},
            ) or {}
            token = str(doc.get('token') or doc.get('tokens') or '').strip()
            if not token or not token.isdigit():
                _trace_stdout(
                    f'[ENTRY TOKEN RESOLVE] strategy={str(trade.get("name") or "")}  '
                    f'leg={leg_cfg_id} {underlying} {option_type} expiry={expiry} '
                    f'strike={strike} token=NOT_FOUND'
                )
                continue

            resolved.append({
                'leg_id': leg_cfg_id,
                'underlying': underlying,
                'option_type': option_type,
                'expiry': expiry,
                'strike': strike,
                'token': token,
                'symbol': str(doc.get('symbol') or '').strip(),
            })

        return resolved

    def _pre_subscribe_option_tokens(
        self, trade: dict, trade_date: str, now_ts: str
    ) -> None:
        """
        Resolve option instrument tokens from Kite instruments cache and
        subscribe them so LTP is ready when entry_time arrives.

        Uses strategy.ListOfLegConfigs (always present) — NOT trade.legs
        (which is empty until entry time) and NOT option_chain_historical_data
        (live/fast-forward never reads from that collection).
        """
        from features.broker_gateway import broker_register_user_tokens as register_user_tokens, broker_is_configured as is_configured  # type: ignore

        if not is_configured():
            return

        trade_id = str(trade.get('_id') or '')
        tokens_to_sub: list[int] = []
        resolved_tokens = self._resolve_trade_entry_tokens(trade, trade_date, now_ts)

        for item in resolved_tokens:
            leg_cfg_id = str(item.get('leg_id') or '').strip()
            raw_tok = str(item.get('token') or '').strip()
            if not leg_cfg_id or not raw_tok.isdigit():
                continue

            sub_key = (trade_id, leg_cfg_id)
            with self._lock:
                if sub_key in self._pre_subscribed:
                    continue

            tokens_to_sub.append(int(raw_tok))
            with self._lock:
                self._pre_subscribed.add(sub_key)
            _trace_stdout(
                f'[ENTRY MONITOR]  pre-subscribe '
                f'strategy={str(trade.get("name") or "")}  '
                f'{str(item.get("underlying") or "")} {str(item.get("option_type") or "")} '
                f'expiry={str(item.get("expiry") or "")} strike={str(item.get("strike") or "")} '
                f'token={raw_tok} source=active_option_tokens'
            )

        if tokens_to_sub:
            try:
                register_user_tokens(_KITE_USER_ID_OPTION, tokens_to_sub)
                _trace_stdout(
                    f'[ENTRY TOKEN SUBSCRIBED]  strategy={str(trade.get("name") or "")}  '
                    f'tokens={",".join(str(tok) for tok in tokens_to_sub)}'
                )
            except Exception as exc:
                log.warning('[LiveEntryMonitor] pre-subscribe register error: %s', exc)

    # ── Take entry ────────────────────────────────────────────────────────────

    def _take_entry(self, trade: dict, trade_date: str, now_ts: str) -> None:
        """
        Execute entry using the same process_pending_entries() as algo-backtest.
        Entry price = live Kite LTP (passed via TickContext.live_ltp_map).
        Falls back to DB option chain close price if kite LTP not yet available.
        """
        from features.trading_core import TickContext, process_pending_entries  # type: ignore
        from features.broker_gateway import broker_register_user_tokens as register_user_tokens, broker_wait_for_tokens_ltp as wait_for_tokens_ltp  # type: ignore

        trade_id      = str(trade.get('_id') or '')
        strategy_name = str(trade.get('name') or '')

        # ── Early spot-availability guard ─────────────────────────────────────
        # If the Kite WS hasn't received an index tick yet, spot=0 → all leg
        # entries inside process_pending_entries will log UNAVAILABLE.
        # Return early and let the monitor retry next second instead.
        underlying = str(
            (trade.get('strategy') or trade.get('config') or {}).get('Ticker')
            or trade.get('ticker') or ''
        ).strip().upper()
        if underlying:
            spot = self._get_spot_price(underlying, now_ts)
            if spot <= 0:
                _trace_stdout(
                    f'[ENTRY PENDING]  strategy={strategy_name}  '
                    f'underlying={underlying}  spot=UNAVAILABLE — retry next second'
                )
                return

        # ── Momentum pre-queue ────────────────────────────────────────────────
        # process_pending_entries arms momentum only when legs are already
        # in algo_trades.legs (DB-backed path). In the live snapshot path
        # (legs not yet queued), it skips with momentum_pending_requires_persisted_leg.
        # Fix: if any leg has LegMomentum config, queue legs to DB first so
        # the normal DB momentum-arm flow runs on the same or next tick.
        # queue_original_legs_if_needed is idempotent — no-op if already queued.
        from features.trading_core import (  # type: ignore
            has_momentum_config, queue_original_legs_if_needed, resolve_trade_leg_configs,
        )
        _all_cfgs = resolve_trade_leg_configs(trade)
        if any(has_momentum_config(cfg) for cfg in _all_cfgs.values()):
            queued = queue_original_legs_if_needed(self._db, trade, now_ts)
            if queued:
                _trace_stdout(
                    f'[ENTRY MOMENTUM]  strategy={strategy_name}  '
                    f'legs queued to DB for momentum arming — will arm next tick'
                )

        resolved_tokens = self._resolve_trade_entry_tokens(trade, trade_date, now_ts)
        trigger_tokens = [
            int(str(item.get('token') or '').strip())
            for item in resolved_tokens
            if str(item.get('token') or '').strip().isdigit()
        ]
        if trigger_tokens:
            register_user_tokens(_KITE_USER_ID_OPTION, trigger_tokens)
            ready_ltp = wait_for_tokens_ltp(trigger_tokens, timeout_seconds=2.5)
            _trace_stdout(
                f'[ENTRY TOKEN READY] strategy={strategy_name}  '
                f'requested={len(trigger_tokens)} ready={len(ready_ltp)}  '
                f'tokens={",".join(str(tok) for tok in trigger_tokens)}'
            )

        ctx = TickContext(
            db              = self._db,
            trade_date      = trade_date,
            now_ts          = now_ts,
            activation_mode = 'live',
            market_cache    = None,          # live DB queries for chain/spot
        )

        # Re-fetch trade from DB to get the latest leg state
        fresh_trade = self._db._db['algo_trades'].find_one({'_id': trade_id}) or trade

        try:
            entries = process_pending_entries(ctx, [fresh_trade])
        except Exception as exc:
            log.exception(
                '[LiveEntryMonitor] process_pending_entries error trade=%s: %s',
                trade_id, exc
            )
            return

        if entries:
            with self._lock:
                self._entered_trade_ids.add(trade_id)

            runtime_print(f'[ENTRY SUCCESS]  strategy={strategy_name}  entries={len(entries)}')
            for e in entries:
                runtime_print(
                    f'  [LEG ENTRY]  leg={e.get("leg_id")}  '
                    f'strike={e.get("strike")}  '
                    f'expiry={e.get("expiry")}  '
                    f'price={e.get("entry_price")}'
                )

            # Subscribe newly entered option tokens to Kite for live SL/TP
            self._sync_entry_tokens(entries)

            # Expire trade cache so LiveMonitorService picks up the new open legs
            self._cache_loaded_at = 0.0

        else:
            runtime_print(
                f'[ENTRY PENDING]  strategy={strategy_name}  '
                f'no entries taken (price not available / legs already queued)  '
                f'— will retry next second'
            )

    # ── Subscribe entered-leg tokens to Kite ──────────────────────────────────

    def _sync_entry_tokens(self, entries: list[dict]) -> None:
        """
        After entry, subscribe the resolved option instrument tokens to Kite
        so that LiveMonitorService receives their ticks for SL/TP checks.
        """
        from features.broker_gateway import broker_register_user_tokens as register_user_tokens, broker_is_configured as is_configured  # type: ignore

        if not is_configured():
            return

        tokens: list[int] = []
        for e in entries:
            raw = str(
                e.get('instrument_token')
                or e.get('token')
                or ''
            ).strip()
            if raw.isdigit():
                tokens.append(int(raw))

        if tokens:
            try:
                register_user_tokens(_KITE_USER_ID_OPTION, tokens)
                runtime_print(
                    f'[LiveEntryMonitor]  subscribed {len(tokens)} entry token(s) '
                    f'to Kite for live SL/TP monitoring'
                )
            except Exception as exc:
                log.warning('[LiveEntryMonitor] entry token subscribe error: %s', exc)

    # ── Index token subscription ──────────────────────────────────────────────

    def _subscribe_index_tokens(self) -> None:
        """
        Subscribe major index tokens to Kite so live spot prices are always
        available in the LTP map before entry time.
        """
        from features.broker_gateway import broker_register_user_tokens as register_user_tokens, broker_is_configured as is_configured  # type: ignore

        if not is_configured():
            runtime_print('[LiveEntryMonitor] Kite not configured — index tokens not subscribed yet')
            return

        tokens = list(INDEX_TOKENS.values())
        try:
            register_user_tokens(_KITE_USER_ID_INDEX, tokens)
            runtime_print(
                f'[LiveEntryMonitor]  index tokens subscribed: '
                f'{list(INDEX_TOKENS.keys())}'
            )
        except Exception as exc:
            log.warning('[LiveEntryMonitor] index token subscribe error: %s', exc)

    # ── Bootstrap existing open leg tokens ───────────────────────────────────

    def _subscribe_existing_open_leg_tokens(self) -> None:
        """
        On startup / Kite re-attach: find all already-entered open legs across
        live + fast-forward strategies and subscribe their Kite instrument tokens.

        Flow (reliable across restarts):
          1. Query algo_trades for all active live/fast-forward running trades.
          2. Walk each trade's legs[] array:
               - dict leg  → take leg['id'] (original config ID, e.g. "7f8w4jjv")
               - string/ObjectId ref → points to algo_trade_positions_history._id
          3. Query algo_trade_positions_history:
               - by leg_id IN config_ids  AND exit_trade=null
               - by _id   IN object_id refs AND exit_trade=null
          4. Collect token from those history docs and subscribe.
        """
        from features.broker_gateway import broker_register_user_tokens as register_user_tokens, broker_is_configured as is_configured  # type: ignore
        from bson import ObjectId  # type: ignore

        if not is_configured():
            _trace_stdout('[ENTRY MONITOR] open-leg bootstrap skipped: Kite not configured')
            return

        if not self._db:
            _trace_stdout('[ENTRY MONITOR] open-leg bootstrap skipped: DB not ready')
            return

        try:
            # Step 1 — active trades
            active_trades = list(self._db._db['algo_trades'].find(
                {
                    'trade_status': 1,
                    'activation_mode': {'$in': ['live', 'fast-forward', 'forward-test']},
                    'status': RUNNING_STATUS,
                },
                {'_id': 1, 'legs': 1},
            ))

            if not active_trades:
                _trace_stdout('[ENTRY MONITOR] open-leg bootstrap: no active live/fast-forward trades')
                return

            hist_col = self._db._db['algo_trade_positions_history']
            tokens: list[int] = []
            seen:   set[int]  = set()

            for trade in active_trades:
                trade_id = str(trade.get('_id') or '').strip()
                legs     = trade.get('legs') or []

                # Step 2 — split legs into config-id refs vs ObjectId refs
                config_leg_ids:  list[str]      = []
                history_obj_ids: list[ObjectId] = []

                for leg in legs:
                    if isinstance(leg, dict):
                        lid = str(leg.get('id') or '').strip()
                        if lid:
                            config_leg_ids.append(lid)
                    elif isinstance(leg, (str, ObjectId)):
                        try:
                            history_obj_ids.append(ObjectId(str(leg).strip()))
                        except Exception:
                            pass

                if not config_leg_ids and not history_obj_ids:
                    continue

                # Step 3 — build $or query and fetch open history docs
                or_clauses: list[dict] = []
                if config_leg_ids:
                    or_clauses.append({
                        'trade_id': trade_id,
                        'leg_id':   {'$in': config_leg_ids},
                        'exit_trade': None,
                    })
                if history_obj_ids:
                    or_clauses.append({
                        '_id':        {'$in': history_obj_ids},
                        'exit_trade': None,
                    })

                query = {'$or': or_clauses} if len(or_clauses) > 1 else or_clauses[0]
                open_docs = list(hist_col.find(
                    query,
                    {'token': 1, 'instrument_token': 1, 'entry_trade': 1, 'leg_id': 1},
                ))

                # Step 4 — collect token
                for doc in open_docs:
                    entry_trade = doc.get('entry_trade') if isinstance(doc.get('entry_trade'), dict) else {}
                    raw_tok = str(
                        doc.get('token')
                        or doc.get('instrument_token')
                        or entry_trade.get('token')
                        or entry_trade.get('instrument_token')
                        or ''
                    ).strip()
                    if raw_tok.isdigit():
                        itok = int(raw_tok)
                        if itok not in seen:
                            seen.add(itok)
                            tokens.append(itok)
                            _trace_stdout(
                                f'[ENTRY MONITOR] open-leg bootstrap: '
                                f'trade={trade_id} leg_id={str(doc.get("leg_id") or "-")} '
                                f'token={itok}'
                            )

            if tokens:
                register_user_tokens(_KITE_USER_ID_OPTION, tokens)
                _trace_stdout(
                    f'[ENTRY MONITOR] open-leg bootstrap: subscribed {len(tokens)} token(s) '
                    f'tokens={",".join(str(t) for t in tokens)}'
                )
            else:
                _trace_stdout('[ENTRY MONITOR] open-leg bootstrap: no open leg tokens found')

        except Exception as exc:
            log.warning('[LiveEntryMonitor] _subscribe_existing_open_leg_tokens error: %s', exc)

    # ── Spot price helper ─────────────────────────────────────────────────────

    def _get_spot_price(self, underlying: str, now_ts: str) -> float:
        """
        Get current spot price.
        Priority: live Kite index LTP → DB option_chain_index_spot.
        """
        from features.broker_gateway import get_broker_ltp_map as get_ltp_map  # type: ignore

        # 1. Try Kite index LTP (fastest, real-time)
        idx_tok = INDEX_TOKENS.get(underlying.upper())
        if idx_tok:
            ltp_map = get_ltp_map()
            kite_spot = float(ltp_map.get(str(idx_tok), 0.0))
            if kite_spot > 0:
                return kite_spot

        # 2. Fallback: DB query
        try:
            doc = self._db._db['option_chain_index_spot'].find_one(
                {'underlying': underlying.upper(), 'timestamp': {'$lte': now_ts}},
                sort=[('timestamp', -1)],
            )
            if doc:
                val = doc.get('spot_price') or doc.get('close') or doc.get('ltp') or 0
                return float(val) if val else 0.0
        except Exception as exc:
            log.warning('[LiveEntryMonitor] spot DB lookup error: %s', exc)

        return 0.0

    # ── Cache reload ──────────────────────────────────────────────────────────

    def _reload_cache(self, trade_date: str) -> None:
        """
        Reload active strategies from DB into in-memory cache.
        Queries both 'live' and 'fast-forward' activation modes in one shot.
        Does NOT filter by creation_ts date so strategies created on earlier
        dates (e.g. imported / carry-forward) are still picked up.
        """
        try:
            # Build query directly — supports both modes in one $in query.
            # trade_status:1 = active row, status = running strategy.
            query: dict = {
                'trade_status': 1,
                'activation_mode': {'$in': ['live', 'fast-forward', 'forward-test']},
                'status': RUNNING_STATUS,
            }
            trades = list(self._db._db['algo_trades'].find(query))
            self._trades_cache    = trades
            self._cache_loaded_at = time.monotonic()

            runtime_print(
                f'[LiveEntryMonitor]  cache loaded: {len(trades)} active trade(s) '
                f'(live + fast-forward)  trade_date={trade_date}'
            )
            for t in trades:
                entry_raw = str(t.get('entry_time') or '')
                entry_hhmm = entry_raw[11:16] if len(entry_raw) >= 16 else entry_raw[:5]
                runtime_print(
                    f'  [TRADE] strategy={str(t.get("name") or "")}  '
                    f'mode={str(t.get("activation_mode") or "")}  '
                    f'entry_time={entry_hhmm or "-"}'
                )
        except Exception as exc:
            log.exception('[LiveEntryMonitor] _reload_cache error: %s', exc)

    # ── Time helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _today_ist() -> str:
        return (_now_utc() + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d')


# ─── Module-level singleton ────────────────────────────────────────────────────

_monitor: LiveEntryMonitor | None = None


def get_monitor() -> LiveEntryMonitor:
    global _monitor
    if _monitor is None:
        _monitor = LiveEntryMonitor()
    return _monitor


def start(loop: asyncio.AbstractEventLoop) -> None:
    get_monitor().start(loop)


def stop() -> None:
    if _monitor:
        _monitor.stop()


def attach_kite_listener() -> None:
    """Call this after Kite credentials are updated to subscribe index tokens."""
    if _monitor:
        _monitor.attach_kite_listener()
