"""
central_tick_client.py
──────────────────────
Drop-in replacement for broker_ticker_manager when running in central-tick mode.

One Dhan/Kite WS connection lives only in algo.websocket.
algo.trade and algo.simulator connect HERE via /ws/internal-ticks to receive
the same tick stream. Each service keeps a local ltp_map + spot_map in-process
(no network hop for SL/TP checks) and calls live_tick_dispatcher.dispatch_tick()
exactly like the real ticker would — so execution_socket.py and
simulator_risk_monitor.py are untouched.

Subscribe/unsubscribe requests go to algo.websocket's REST endpoints, which
aggregate all service requests into ONE subscription set sent to the broker.

API rate-limit calls (Dhan /marketfeed/quote) still go through each service's
own broker_gateway.dhan_quote_post — the rate gate is per-process but these
REST calls are infrequent (option chain loads, not tick-level). If cross-process
rate gating is ever needed, route those calls through algo.websocket's /quotes
proxy (see ws_main.py) instead.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from threading import Thread
from typing import Any

import requests

log = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

_HUB_DEFAULT = "http://localhost:8003"


class _CentralTickHandle:
    """
    Mimics kite/dhan's raw ticker object for callers that reach into
    ticker_manager._ticker directly (e.g. live_event._subscribe_live_option_token).
    """
    MODE_LTP = "ltp"

    def __init__(self, client: "CentralTickClient") -> None:
        self._client = client

    def subscribe(self, token_ids: list) -> None:
        self._client.subscribe_tokens([str(t) for t in token_ids])

    def set_mode(self, mode: Any, token_ids: list) -> None:
        pass  # mode is chosen by the hub when it forwards to the broker

    def __bool__(self) -> bool:
        return self._client.status == "running"


class CentralTickClient:
    """
    Full broker_ticker_manager-compatible interface backed by the central hub
    instead of a direct broker connection.

    Properties / methods that callers use:
      .ltp_map            dict[token_str, float]  — kept live from WS stream
      .spot_map           dict[underlying, float] — kept live from WS stream
      .status             str                     — "connecting" | "running" | "error" | "stopped"
      .tick_count         int
      .started_at         str
      .error_msg          str
      .subscribed_tokens  set[str]
      ._ticker            _CentralTickHandle      — for code that calls ._ticker.subscribe()
      .start(db)          → connects to hub WS in background thread
      .stop()             → disconnects
      .restart(db)        → stop + start
      .get_ltp(token)     → float | None
      .get_spot(ul)       → float | None
      .get_status()       → dict
      .subscribe_tokens(ids, exchange)  → POST to hub /ticker/subscribe
      .register_option_token(token, label)  → no-op (hub manages labels)
      .add_tick_listener(fn) / .remove_tick_listener(fn)
    """

    def __init__(self, hub_base: str = _HUB_DEFAULT) -> None:
        self._hub_http = hub_base.rstrip("/")
        self._hub_ws   = (
            hub_base.rstrip("/")
            .replace("http://", "ws://")
            .replace("https://", "wss://")
        )
        self.ltp_map:           dict[str, float] = {}
        self.spot_map:          dict[str, float] = {}
        self.oi_map:            dict[str, float] = {}
        # Best (level-0) bid/ask, relayed from the hub's changed_bid_map/changed_ask_map —
        # see ws_main.py's _InternalTickHub.broadcast and dhan_ticker.py's depth parsing.
        # Only ever non-empty for F&O option legs (RESP_FULL packets).
        self.bid_map:           dict[str, float] = {}
        self.ask_map:           dict[str, float] = {}
        self.ltp_ts_map:        dict[str, str]   = {}   # token -> ISO ts of last tick, mirrors dhan_ticker
        self._listeners:        list             = []
        self.status:            str              = "stopped"
        self.tick_count:        int              = 0
        self.started_at:        str              = ""
        self.error_msg:         str              = ""
        self.subscribed_tokens: set[str]         = set()
        self.chain_subscribed_tokens: set[str]   = set()
        self._running:          bool             = False
        self._ticker                             = _CentralTickHandle(self)
        self._lock                               = threading.Lock()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self, db: Any) -> None:
        if self._running:
            return
        self._running    = True
        self.status      = "connecting"
        self.started_at  = datetime.now(IST).isoformat()
        self.tick_count  = 0
        Thread(
            target=lambda: asyncio.run(self._ws_loop()),
            daemon=True,
            name="central_tick_ws",
        ).start()
        log.info("[CentralTick] started → %s/ws/internal-ticks", self._hub_ws)

    def stop(self) -> None:
        self._running = False
        self.status   = "stopped"

    def restart(self, db: Any) -> None:
        self.stop()
        time.sleep(0.3)
        self.start(db)

    # ── Interface ─────────────────────────────────────────────────────────────

    def get_ltp(self, token: str) -> float | None:
        return self.ltp_map.get(str(token))

    def get_spot(self, underlying: str) -> float | None:
        return self.spot_map.get(str(underlying or "").upper())

    def get_status(self) -> dict:
        return {
            "status":     self.status,
            "tick_count": self.tick_count,
            "ltp_count":  len(self.ltp_map),
            "spot_map":   dict(self.spot_map),
            "started_at": self.started_at,
            "error":      self.error_msg,
            "mode":       "central",
            "hub":        self._hub_http,
        }

    def add_tick_listener(self, listener: Any) -> None:
        with self._lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    # alias used by some older callers
    add_listener = add_tick_listener

    def remove_tick_listener(self, listener: Any) -> None:
        with self._lock:
            self._listeners = [l for l in self._listeners if l is not listener]

    def subscribe_tokens(self, token_ids: list, exchange: str = "NSE_FNO") -> None:
        str_ids = [str(t) for t in token_ids if str(t).strip()]
        new     = [t for t in str_ids if t not in self.subscribed_tokens]
        if not new:
            return
        self.subscribed_tokens.update(new)
        try:
            requests.post(
                f"{self._hub_http}/ticker/subscribe",
                json={"tokens": new, "exchange": exchange},
                timeout=5.0,
            )
            log.info("[CentralTick] subscribed %d tokens via hub", len(new))
        except Exception as exc:
            log.warning("[CentralTick] subscribe POST failed: %s", exc)

    def register_option_token(self, token: str, label: str = "") -> None:
        normalized = str(token or "").strip()
        if normalized:
            self.subscribed_tokens.add(normalized)

    def warm_chain_tokens(self, token_ids: list, exchange: str = "NSE_FNO") -> None:
        """
        Forward to algo.websocket's chain-feed connection pool (see
        dhan_ticker.py) instead of trying to warm a chain locally — this
        process never opens its own broker WS, so there's no local
        dhan_ticker_manager with real credentials to warm against. Same
        fire-and-forget POST pattern as subscribe_tokens(), deduped locally
        so repeat fetch_full_chain() calls for an already-warm chain don't
        re-POST every time.
        """
        str_ids = [str(t) for t in token_ids if str(t).strip()]
        new     = [t for t in str_ids if t not in self.chain_subscribed_tokens]
        if not new:
            return
        self.chain_subscribed_tokens.update(new)
        try:
            requests.post(
                f"{self._hub_http}/ticker/warm-chain",
                json={"tokens": new, "exchange": exchange},
                timeout=5.0,
            )
            log.info("[CentralTick] warmed %d chain tokens via hub", len(new))
        except Exception as exc:
            log.warning("[CentralTick] warm-chain POST failed: %s", exc)

    # ── Internal WS loop ─────────────────────────────────────────────────────

    async def _ws_loop(self) -> None:
        import websockets  # type: ignore

        url = f"{self._hub_ws}/ws/internal-ticks"
        while self._running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self.status    = "running"
                    self.error_msg = ""
                    log.info("[CentralTick] connected to %s", url)
                    async for raw in ws:
                        if not self._running:
                            break
                        self._on_raw(raw)
            except Exception as exc:
                self.status    = "error"
                self.error_msg = str(exc)
                log.warning("[CentralTick] WS error: %s — retry in 2s", exc)
                if self._running:
                    await asyncio.sleep(2)

        self.status = "stopped"

    def _on_raw(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except Exception:
            return
        if msg.get("type") != "tick":
            return

        data         = msg.get("data") or {}
        changed_ltp: dict[str, float] = data.get("changed_ltp_map") or {}
        spot_upd:    dict[str, float] = data.get("spot_map")         or {}
        # Additive — a hub that doesn't send these yet (old process, not restarted) just
        # yields {}, so bid_map/ask_map/oi_map simply stay empty like before this change.
        changed_oi:  dict[str, float] = data.get("changed_oi_map")  or {}
        changed_bid: dict[str, float] = data.get("changed_bid_map") or {}
        changed_ask: dict[str, float] = data.get("changed_ask_map") or {}
        now_ts       = str(data.get("now_ts") or datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"))
        trade_date   = now_ts[:10]
        now_minute   = now_ts[:16]
        listen_time  = now_ts[11:16]

        # ── 1. Update local maps (in-process, sub-ms reads for SL/TP) ─────────
        if changed_ltp:
            self.ltp_map.update(changed_ltp)
            # Mirrors dhan_ticker's ltp_ts_map — without this, get_broker_rest_
            # quotes() treats every central-tick-sourced price as "no timestamp
            # → stale" and discards it, forcing a REST call on every poll.
            for _tok in changed_ltp:
                self.ltp_ts_map[_tok] = now_ts
        if spot_upd:
            self.spot_map.update(spot_upd)
        if changed_oi:
            self.oi_map.update(changed_oi)
        if changed_bid:
            self.bid_map.update(changed_bid)
        if changed_ask:
            self.ask_map.update(changed_ask)
        self.tick_count += len(changed_ltp)

        # ── 2. Dispatch to live_tick_dispatcher — FIRST PRIORITY ──────────────
        #    spot_ticks_received=[] because algo.websocket already persists
        #    spot data via its own dispatch_tick call.
        if changed_ltp:
            try:
                from features.live_tick_dispatcher import live_tick_dispatcher
                from features.runtime_mode_registry import runtime_mode_registry
                if (
                    runtime_mode_registry.has_active_mode("live") or
                    runtime_mode_registry.has_active_mode("fast-forward") or
                    runtime_mode_registry.has_active_mode("forward-test")
                ):
                    live_tick_dispatcher.dispatch_tick(
                        trade_date=trade_date,
                        now_ts=now_ts,
                        now_minute=now_minute,
                        listen_time=listen_time,
                        broker_ltp_map=dict(changed_ltp),
                        spot_ticks_received=[],
                    )
            except Exception as exc:
                log.error("[CentralTick] dispatch error: %s", exc)

        # ── 3. Notify listeners (display/monitoring) ──────────────────────────
        with self._lock:
            listeners = list(self._listeners)
        if listeners:
            payload = {
                "timestamp":       now_ts,
                "ltp_map":         dict(self.ltp_map),
                "spot_map":        dict(self.spot_map),
                "changed_ltp_map": changed_ltp,
                "changed_oi_map":  changed_oi,
                "changed_bid_map": changed_bid,
                "changed_ask_map": changed_ask,
                "tick_count":      self.tick_count,
                "status":          self.status,
            }
            for listener in listeners:
                try:
                    listener(payload)
                except Exception as exc:
                    log.warning("[CentralTick] listener error: %s", exc)
