"""
kite_ticker.py
──────────────
Manages the KiteTicker WebSocket lifecycle for live trading.

Default spot tokens (always subscribed):
  256265  → NIFTY 50
  260105  → NIFTY BANK
  257801  → NIFTY FIN SERVICE (FINNIFTY)
  265     → SENSEX
  288009  → NIFTY MIDCAP SELECT

On every tick:
  - ltp_map updated (token → last_price)
  - Spot ticks → written to option_chain_index_spot (entry logic reads from there)
  - broker_live_tick() → SL / TG / Trail / Exit (same as backtest)
  - Every new minute → _execute_backtest_entries() → entry logic (same as backtest)
"""

from __future__ import annotations

import threading
import logging
from datetime import datetime

from kiteconnect import KiteTicker
from features.kite_event import build_broker_ltp_map
from features.live_tick_dispatcher import live_tick_dispatcher
from features.mongo_data import MongoData
from features.debug_flags import runtime_print

logger = logging.getLogger(__name__)

KITE_MAX_OPTION_TOKENS = 3000
ACTIVATION_MODE        = "live"

# ── Well-known Kite instrument tokens for spot indices ────────────────────────
# These are permanent Kite tokens — subscribe by default so spot price is always live
SPOT_TOKENS: dict[int, str] = {
    256265: "NIFTY",
    260105: "BANKNIFTY",
    257801: "FINNIFTY",
    265:    "SENSEX",
    288009: "MIDCPNIFTY",
}

# India VIX token — subscribed separately so LTP is live in ltp_map but
# VIX is NOT written to option_chain_index_spot (it is not a tradeable spot).
INDIA_VIX_TOKEN_ID = 264969


class _TickerManager:
    def __init__(self):
        self._ticker:       KiteTicker | None = None
        self._lock        = threading.Lock()
        self._last_minute = ""
        self._stopped     = False          # flag checked inside _on_ticks to abort early
        self._listeners: list = []

        self.ltp_map:    dict[str, float] = {}   # token_str → last_price
        self.spot_map:   dict[str, float] = {}   # underlying → spot_price (NIFTY, BANKNIFTY…)
        self.status:     str = "stopped"
        self.error_msg:  str = ""
        self.started_at: str = ""
        self.tick_count: int = 0
        self.subscribed_tokens: set[str] = set()
        self.token_labels: dict[str, str] = {}

    # ── public ───────────────────────────────────────────────────────────────

    def start(self, db):
        with self._lock:
            if self.status == "running":
                return {"ok": False, "message": "Already running"}

            cfg = db["kite_market_config"].find_one({"enabled": True})
            if not cfg:
                self.status    = "error"
                self.error_msg = "No enabled kite_market_config found in MongoDB"
                print("[KITE TICKER START FAIL] reason=no_enabled_kite_market_config")
                return {"ok": False, "message": self.error_msg}

            api_key      = (cfg.get("api_key") or "").strip()
            access_token = (cfg.get("access_token") or "").strip()
            login_time   = str(cfg.get("login_time") or "").strip()
            cfg_id       = str(cfg.get("_id") or "").strip()

            if not api_key or not access_token:
                self.status    = "error"
                self.error_msg = "api_key or access_token missing in kite_market_config"
                print(
                    f"[KITE TICKER START FAIL] reason=missing_credentials "
                    f"config_id={cfg_id or '-'} has_api_key={bool(api_key)} "
                    f"has_access_token={bool(access_token)} login_time={login_time or '-'}"
                )
                return {"ok": False, "message": self.error_msg}

            print(
                f"[KITE TICKER START] config_id={cfg_id or '-'} "
                f"login_time={login_time or '-'} api_key_tail={api_key[-4:] if len(api_key) >= 4 else api_key} "
                f"token_len={len(access_token)}"
            )

            # ── Subscribe only spot/index instrument tokens at startup ─────
            # Option strikes are subscribed lazily only when an entry actually
            # needs that contract.
            spot_token_ids = list(SPOT_TOKENS.keys())
            option_tokens: list[int] = []
            all_tokens     = spot_token_ids + [INDIA_VIX_TOKEN_ID]

            print(
                f"[KITE TICKER] subscribing "
                f"spot_tokens={len(spot_token_ids)} "
                f"option_tokens={len(option_tokens)} "
                f"total={len(all_tokens)}"
            )

            self.ltp_map      = {}
            self.spot_map     = {}
            self.tick_count   = 0
            self.subscribed_tokens = {str(token_id) for token_id in spot_token_ids}
            self.token_labels = {str(token_id): label for token_id, label in SPOT_TOKENS.items()}
            self.error_msg    = ""
            self._last_minute = ""
            self._stopped     = False
            self.status       = "connecting"
            self.started_at   = datetime.now().isoformat()

            ticker = KiteTicker(api_key, access_token)
            self._ticker = ticker

            def _on_ticks(ws, ticks):
                # ── Guard: stop() called → ignore all incoming ticks ─────
                if self._stopped:
                    return

                now_dt     = datetime.now()
                now_ts     = now_dt.strftime("%Y-%m-%dT%H:%M:%S")
                trade_date = now_ts[:10]
                now_minute = now_ts[:16]
                listen_time = now_ts[11:16]

                # ── 1. Update ltp_map + handle spot tokens ────────────────
                spot_ticks_received = []
                for tick in ticks:
                    token_int = tick.get("instrument_token")
                    token_str = str(token_int or "")
                    lp = tick.get("last_price") or tick.get("last_traded_price")
                    if not token_str or lp is None:
                        continue

                    lp = float(lp)
                    self.ltp_map[token_str] = lp

                    # Spot token → update spot_map + write to option_chain_index_spot
                    if token_int in SPOT_TOKENS:
                        underlying = SPOT_TOKENS[token_int]
                        self.spot_map[underlying] = lp
                        spot_ticks_received.append((underlying, lp, now_ts))

                self.tick_count += len(ticks)
                current_ltp_map = dict(self.ltp_map)
                current_spot_map = dict(self.spot_map)
                listeners = list(self._listeners)
                if self.tick_count <= len(ticks):
                    first_tick_tokens = [
                        str(tick.get("instrument_token") or "")
                        for tick in ticks[:10]
                        if str(tick.get("instrument_token") or "").strip()
                    ]
                    print(
                        f"[KITE FIRST TICKS] batch_size={len(ticks)} "
                        f"tokens={first_tick_tokens or ['-']} "
                        f"spot_map={current_spot_map}"
                    )
                try:
                    from features.runtime_mode_registry import runtime_mode_registry
                    active_modes = [
                        mode for mode in ("live", "fast-forward", "forward-test")
                        if runtime_mode_registry.has_active_mode(mode)
                    ]
                except Exception:
                    active_modes = []

                if active_modes and ticks:
                    spot_labels = [
                        self.token_labels.get(str(token_id), str(token_id))
                        for token_id in SPOT_TOKENS.keys()
                    ]
                    option_labels = sorted([
                        self.token_labels.get(token, token)
                        for token in self.subscribed_tokens
                        if token not in {str(token_id) for token_id in SPOT_TOKENS.keys()}
                    ])
                    tick_parts: list[str] = []
                    for tick in ticks[:12]:
                        token_str = str(tick.get("instrument_token") or "")
                        if not token_str:
                            continue
                        last_price = tick.get("last_price") or tick.get("last_traded_price")
                        if last_price is None:
                            continue
                        tick_label = self.token_labels.get(token_str, token_str)
                        tick_parts.append(f"{tick_label}({token_str})={float(last_price):.2f}")
                    runtime_print(
                        "[KITE TICK STREAM] "
                        f"modes={','.join(active_modes)} "
                        f"subscribed_total={len(self.subscribed_tokens)} "
                        f"spot_subscribed={len(SPOT_TOKENS)} "
                        f"option_subscribed={max(0, len(self.subscribed_tokens) - len(SPOT_TOKENS))} "
                        f"spot_tokens=[{', '.join(spot_labels)}] "
                        f"option_tokens=[{', '.join(option_labels[:12]) if option_labels else '-'}] "
                        f"tick_batch=[{'; '.join(tick_parts) if tick_parts else '-'}]"
                    )

                # ── 2. Offload ALL heavy work to dedicated workers ──────────
                # The broker on_ticks callback must stay thin so real live
                # orders are not delayed by DB work, fast-forward work, or
                # minute-entry processing.
                try:
                    broker_ltp_map = build_broker_ltp_map(ticks)
                    live_tick_dispatcher.dispatch_tick(
                        trade_date=trade_date,
                        now_ts=now_ts,
                        now_minute=now_minute,
                        listen_time=listen_time,
                        broker_ltp_map=broker_ltp_map,
                        spot_ticks_received=spot_ticks_received,
                    )
                except Exception as exc:
                    logger.error("tick dispatch error: %s", exc)

                if listeners:
                    tick_payload = {
                        "timestamp": now_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
                        "ltp_map": current_ltp_map,
                        "spot_map": current_spot_map,
                        "changed_ltp_map": {
                            str(tick.get("instrument_token") or ""): float(
                                tick.get("last_price") or tick.get("last_traded_price") or 0
                            )
                            for tick in ticks
                            if str(tick.get("instrument_token") or "").strip()
                            and (tick.get("last_price") is not None or tick.get("last_traded_price") is not None)
                        },
                        "tick_count": self.tick_count,
                        "status": self.status,
                    }
                    for listener in listeners:
                        try:
                            listener(tick_payload)
                        except Exception as exc:
                            logger.warning("ticker listener error: %s", exc)

            def _on_connect(ws, response):
                logger.info("KiteTicker connected")
                self.status = "running"
                ws.subscribe(all_tokens)
                ws.set_mode(ws.MODE_LTP, all_tokens)
                print(
                    f"[KITE TICKER] connected and subscribed "
                    f"total={len(all_tokens)} tokens "
                    f"response={response}"
                )

                # Start per-minute option chain writer so bar-replay has fresh DB data
                try:
                    from features.option_chain_minute_writer import start_option_chain_minute_writer
                    start_option_chain_minute_writer()
                    print("[KITE TICKER] option_chain_minute_writer started")
                except Exception as _exc:
                    logger.warning("option_chain_minute_writer start error: %s", _exc)

                try:
                    from features.live_event import sync_live_open_position_subscriptions
                    from features.fast_forward_event import sync_fast_forward_open_position_subscriptions

                    current_trade_date = datetime.now().strftime("%Y-%m-%d")
                    live_subscribed = sync_live_open_position_subscriptions(current_trade_date)
                    ff_subscribed = sync_fast_forward_open_position_subscriptions(current_trade_date)
                    print(
                        f"[KITE TICKER OPEN POSITIONS] "
                        f"trade_date={current_trade_date} "
                        f"live_subscribed={live_subscribed} "
                        f"fast_forward_subscribed={ff_subscribed}"
                    )
                except Exception as exc:
                    logger.error("open position subscribe sync error: %s", exc)

                # ── Print active live strategies at connect time ──────────
                try:
                    from features.execution_socket import _load_running_trade_records
                    _db = MongoData()
                    now_ts     = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                    trade_date = now_ts[:10]
                    records    = _load_running_trade_records(
                        _db, trade_date, activation_mode=ACTIVATION_MODE
                    )
                    print(
                        f"[LIVE MONITOR] connected | trade_date={trade_date} | "
                        f"active_strategies={len(records)}"
                    )
                    for rec in records:
                        group_name    = (rec.get("portfolio") or {}).get("group_name") or ""
                        strategy_name = rec.get("name") or ""
                        entry_time    = str(rec.get("entry_time") or "")
                        exit_time     = str(rec.get("exit_time") or "")
                        ticker        = rec.get("ticker") or ""
                        leg_count     = len(
                            (rec.get("strategy") or {}).get("ListOfLegConfigs") or []
                        )
                        print(
                            f"[LIVE STRATEGY] "
                            f"group={group_name} | "
                            f"strategy={strategy_name} | "
                            f"ticker={ticker} | "
                            f"entry_time={entry_time} | "
                            f"exit_time={exit_time} | "
                            f"legs={leg_count}"
                        )
                    _db.close()
                except Exception as exc:
                    logger.error("on_connect strategy print error: %s", exc)

            def _on_close(ws, code, reason):
                logger.info("KiteTicker closed: %s %s", code, reason)
                self.status = "stopped"
                print(
                    f"[KITE TICKER CLOSED] code={code} reason={reason} "
                    f"tick_count={self.tick_count} ltp_count={len(self.ltp_map)} spot_count={len(self.spot_map)}"
                )

            def _on_error(ws, code, reason):
                logger.error("KiteTicker error: %s %s", code, reason)
                self.status    = "error"
                self.error_msg = f"{code}: {reason}"
                print(
                    f"[KITE TICKER ERROR] code={code} reason={reason} "
                    f"tick_count={self.tick_count} subscribed={len(self.subscribed_tokens)}"
                )

            def _on_reconnect(ws, attempts_count):
                logger.info("KiteTicker reconnecting... attempt %s", attempts_count)
                self.status = "connecting"
                print(
                    f"[KITE TICKER RECONNECT] attempt={attempts_count} "
                    f"tick_count={self.tick_count} error={self.error_msg or '-'}"
                )

            ticker.on_ticks     = _on_ticks
            ticker.on_connect   = _on_connect
            ticker.on_close     = _on_close
            ticker.on_error     = _on_error
            ticker.on_reconnect = _on_reconnect

            # threaded=True → KiteTicker manages its own internal thread
            # This allows close() + stop_retry() to fully shut it down
            ticker.connect(threaded=True)

            return {
                "ok":             True,
                "message":        "KiteTicker starting",
                "spot_tokens":    len(spot_token_ids),
                "option_tokens":  len(option_tokens),
                "total_tokens":   len(all_tokens),
                "started_at":     self.started_at,
            }

    def stop(self):
        with self._lock:
            # 1. Set flag first — _on_ticks ignores any in-flight ticks immediately
            self._stopped = True

            try:
                from features.option_chain_minute_writer import stop_option_chain_minute_writer
                stop_option_chain_minute_writer()
            except Exception:
                pass

            if self._ticker:
                try:
                    self._ticker.stop_retry()   # no auto-reconnect
                except Exception:
                    pass
                try:
                    self._ticker.close()         # close WebSocket connection
                except Exception:
                    pass
                self._ticker = None

            self.status       = "stopped"
            self.ltp_map      = {}
            self.spot_map     = {}
            self.tick_count   = 0
            self.subscribed_tokens = set()
            self.token_labels = {}
            self.error_msg    = ""
            self.started_at   = ""
            self._last_minute = ""

            print("[KITE TICKER] stopped")
            return {"ok": True, "message": "KiteTicker stopped"}

    def restart(self, db):
        self.stop()
        return self.start(db)

    def get_status(self) -> dict:
        return {
            "status":     self.status,
            "tick_count": self.tick_count,
            "ltp_count":  len(self.ltp_map),
            "spot_map":   self.spot_map,
            "started_at": self.started_at,
            "error":      self.error_msg,
        }

    def get_ltp(self, token: str) -> float | None:
        return self.ltp_map.get(str(token))

    def get_spot(self, underlying: str) -> float | None:
        return self.spot_map.get(underlying.upper())

    def register_option_token(self, token: str, label: str = "") -> None:
        normalized_token = str(token or "").strip()
        if not normalized_token:
            return
        self.subscribed_tokens.add(normalized_token)
        if str(label or "").strip():
            self.token_labels[normalized_token] = str(label).strip()

    def add_tick_listener(self, listener) -> None:
        if not callable(listener):
            return
        with self._lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def remove_tick_listener(self, listener) -> None:
        with self._lock:
            self._listeners = [item for item in self._listeners if item is not listener]


# Singleton
ticker_manager = _TickerManager()
