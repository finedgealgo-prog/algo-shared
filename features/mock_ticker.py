"""
mock_ticker.py
──────────────
MockTicker  — drop-in replacement for kiteconnect.KiteTicker.
              Identical interface; reads historical data from MongoDB instead of
              connecting to Zerodha's WebSocket.

_MockTickerManager — mirrors _TickerManager in kite_ticker.py.
                     Same public attributes (ltp_map, spot_map, status, …)
                     so live_monitor_socket.py can import either manager with
                     zero code change.

How it works
────────────
1. Call mock_ticker_manager.set_mock_time("2025-11-03T09:15:00") once.
2. Call mock_ticker_manager.start(db)  — starts the background thread.
   • on_connect fires → ws.subscribe(all_tokens) + ws.set_mode(ws.MODE_FULL, …)
     (exact same code as kite_ticker._on_connect)
   • every real second:
       - advances mock_time by 1 minute
       - queries option_chain_historical_data + option_chain_index_spot
       - calls on_ticks(ws, ticks)  — exact same handler as kite_ticker._on_ticks
3. Call mock_ticker_manager.stop() to shut down.

Switching to live Kite on Monday
─────────────────────────────────
In live_monitor_socket.py change only:
  from features.mock_ticker import mock_ticker_manager as _tm
  →
  from features.kite_ticker import ticker_manager as _tm
Everything else — ltp_map, spot_map, status, subscribe flow — stays identical.
"""

from __future__ import annotations

import time
import threading
import logging
import json
from pathlib import Path
from datetime import datetime, timedelta

from features.mongo_data import MongoData

logger = logging.getLogger(__name__)

STATE_FILE = Path(__file__).resolve().parent / "mock_ticker_state.json"

# ── Spot token map for mock data ──────────────────────────────────────────────
# option_chain_index_spot.token → underlying name
# MockTicker builds this dynamically from the DB; _MockTickerManager reads it
# to populate spot_map (mirrors the SPOT_TOKENS dict in kite_ticker.py).
MOCK_SPOT_TOKENS: dict[str, str] = {}   # populated at start-time from DB


def _get_active_runtime_modes() -> list[str]:
    try:
        from features.runtime_mode_registry import runtime_mode_registry
        active_modes = [
            mode for mode in ("live", "fast-forward", "forward-test")
            if runtime_mode_registry.has_active_mode(mode)
        ]
        if active_modes:
            return active_modes
    except Exception:
        pass
    return ["live", "fast-forward", "forward-test"]


# ─────────────────────────────────────────────────────────────────────────────
# MockTicker  (drop-in for kiteconnect.KiteTicker)
# ─────────────────────────────────────────────────────────────────────────────

class MockTicker:
    """
    Behaves exactly like kiteconnect.KiteTicker but reads from MongoDB.

    Usage (identical to KiteTicker):
        ticker = MockTicker(mock_time="2025-11-03T09:15:00")
        ticker.on_ticks   = _on_ticks
        ticker.on_connect = _on_connect
        ticker.on_close   = _on_close
        ticker.on_error   = _on_error
        ticker.on_reconnect = _on_reconnect
        ticker.connect(threaded=True)
        # later:
        ticker.subscribe([...])
        ticker.set_mode(ticker.MODE_FULL, [...])
        ticker.stop_retry()
        ticker.close()
    """

    MODE_LTP   = "ltp"
    MODE_QUOTE = "quote"
    MODE_FULL  = "full"

    def __init__(self, mock_time: str) -> None:
        """
        mock_time: ISO string "YYYY-MM-DDTHH:MM:SS" — simulation start time.
        """
        self.on_ticks:     object = None
        self.on_connect:   object = None
        self.on_close:     object = None
        self.on_error:     object = None
        self.on_reconnect: object = None

        self._mock_time         = datetime.fromisoformat(mock_time[:19])
        self._subscribed:       set[str] = set()
        self._thread:           threading.Thread | None = None
        self._stopped           = False

        # populated during _tick_loop; used by _MockTickerManager._on_ticks
        # to detect spot ticks (mirrors SPOT_TOKENS in kite_ticker.py)
        self._spot_token_map:   dict[str, str] = {}   # token_str → underlying

    # ── KiteTicker interface ──────────────────────────────────────────────────

    def connect(self, threaded: bool = False) -> None:
        """Start the mock feed. If threaded=True the loop runs in a daemon thread."""
        self._stopped = False
        if self.on_connect:
            self.on_connect(self, {})
        if threaded:
            self._thread = threading.Thread(
                target=self._tick_loop, daemon=True, name="mock_ticker_feed"
            )
            self._thread.start()
        else:
            self._tick_loop()

    def subscribe(self, tokens: list) -> None:
        """Add tokens to the active subscription — called from on_connect."""
        for tok in tokens:
            self._subscribed.add(str(tok).strip())
        print(f"[MOCK TICKER] subscribed {len(tokens)} tokens | total={len(self._subscribed)}")

    def set_mode(self, mode: str, tokens: list) -> None:
        """No-op — MockTicker always returns full data regardless of mode."""
        pass

    def close(self) -> None:
        self._stopped = True
        if self.on_close:
            self.on_close(self, 0, "mock closed")

    def stop_retry(self) -> None:
        self._stopped = True

    # ── Internal tick loop ────────────────────────────────────────────────────

    def _tick_loop(self) -> None:
        db = MongoData()
        try:
            while not self._stopped:
                ticks = self._fetch_ticks(db)
                if ticks and callable(self.on_ticks):
                    self.on_ticks(self, ticks)

                if not self._stopped:
                    self._mock_time += timedelta(minutes=1)

                time.sleep(1)

        except Exception as exc:
            logger.error("[MockTicker] tick loop error: %s", exc)
            if callable(self.on_error):
                self.on_error(self, 0, str(exc))
        finally:
            try:
                db.close()
            except Exception:
                pass
            print("[MockTicker] feed thread exited")

    def _fetch_ticks(self, db: MongoData) -> list[dict]:
        now_ts = self._mock_time.strftime("%Y-%m-%dT%H:%M:%S")
        ticks: list[dict] = []

        # ── Spot / index data ─────────────────────────────────────────────
        try:
            spot_docs = list(db._db["option_chain_index_spot"].find(
                {"timestamp": now_ts},
                {"_id": 0, "token": 1, "underlying": 1, "spot_price": 1},
            ))
            for doc in spot_docs:
                token      = str(doc.get("token") or "").strip()
                underlying = str(doc.get("underlying") or "").strip().upper()
                ltp        = float(doc.get("spot_price") or 0)
                if not token or ltp <= 0:
                    continue
                # Build spot_token_map so _MockTickerManager can detect spot ticks
                self._spot_token_map[token] = underlying
                ticks.append({
                    "instrument_token":    token,
                    "last_price":          ltp,
                    "mode":                "ltp",
                    "tradable":            True,
                    "timestamp":           now_ts,
                    # extra key consumed by kite_event.build_broker_ltp_map
                    "token":               token,
                })
        except Exception as exc:
            logger.error("[MockTicker] spot fetch error: %s", exc)

        # ── Option chain data ─────────────────────────────────────────────
        try:
            subscribed_list = list(self._subscribed)
            if subscribed_list:
                option_docs = list(db._db["option_chain_historical_data"].find(
                    {
                        "timestamp": now_ts,
                        "token":     {"$in": subscribed_list},
                    },
                    {
                        "_id": 0,
                        "token": 1, "close": 1, "oi": 1,
                        "underlying": 1, "strike": 1, "type": 1, "expiry": 1,
                    },
                ))
                for doc in option_docs:
                    token = str(doc.get("token") or "").strip()
                    ltp   = float(doc.get("close") or 0)
                    oi    = int(doc.get("oi") or 0)
                    if not token or ltp <= 0:
                        continue
                    ticks.append({
                        # Kite MODE_FULL layout
                        "instrument_token":     token,
                        "last_price":           ltp,
                        "last_traded_quantity": 0,
                        "average_traded_price": ltp,
                        "volume_traded":        0,
                        "total_buy_quantity":   0,
                        "total_sell_quantity":  0,
                        "ohlc": {
                            "open":  ltp,
                            "high":  ltp,
                            "low":   ltp,
                            "close": ltp,
                        },
                        "change":           0.0,
                        "last_trade_time":  now_ts,
                        "oi":               oi,
                        "oi_day_high":      oi,
                        "oi_day_low":       oi,
                        "timestamp":        now_ts,
                        "tradable":         True,
                        "mode":             "full",
                        # extra key for build_broker_ltp_map
                        "token":            token,
                    })
        except Exception as exc:
            logger.error("[MockTicker] option fetch error: %s", exc)

        return ticks


# ─────────────────────────────────────────────────────────────────────────────
# _MockTickerManager  (mirrors _TickerManager in kite_ticker.py)
# ─────────────────────────────────────────────────────────────────────────────

class _MockTickerManager:
    """
    Public interface identical to kite_ticker._TickerManager.
    Attributes: ltp_map, spot_map, status, error_msg, started_at, tick_count
    Methods:    start(db), stop(), restart(db), get_status(), get_ltp(), get_spot()
    Extra:      set_mock_time(time_str)  — call BEFORE start()
    """

    def __init__(self) -> None:
        self._ticker:       MockTicker | None = None
        self._lock          = threading.Lock()
        self._last_minute   = ""
        self._stopped       = False
        self._mock_time_str = ""       # ISO string — set by set_mock_time()
        self._listeners: list = []

        self.ltp_map:    dict[str, float] = {}
        self.spot_map:   dict[str, float] = {}
        self.status:     str = "stopped"
        self.error_msg:  str = ""
        self.started_at: str = ""
        self.tick_count: int = 0
        self.mock_current_time: str = ""
        self._load_state()

    def _save_state(self) -> None:
        state = {
            "mock_time_str": self._mock_time_str,
            "mock_current_time": self.mock_current_time,
            "status": self.status,
            "started_at": self.started_at,
            "saved_at": datetime.now().isoformat(),
        }
        try:
            STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning("mock: failed to persist state: %s", exc)

    def _load_state(self) -> None:
        try:
            if not STATE_FILE.exists():
                return
            state = json.loads(STATE_FILE.read_text(encoding="utf-8") or "{}")
            saved_time = str(state.get("mock_current_time") or state.get("mock_time_str") or "").strip()
            if saved_time:
                self._mock_time_str = saved_time[:19]
                self.mock_current_time = saved_time[:19]
            self.status = "stopped"
        except Exception as exc:
            logger.warning("mock: failed to load persisted state: %s", exc)

    # ── Extra method (not on _TickerManager) ─────────────────────────────────

    def set_mock_time(self, time_str: str) -> dict:
        """
        Set the simulation start time — call this BEFORE start().
        Accepted formats:
          "HH:MM"               → today's date is used
          "YYYY-MM-DDTHH:MM"    → full datetime
          "YYYY-MM-DDTHH:MM:SS" → full ISO timestamp
        """
        ts = (time_str or "").strip()
        if not ts:
            return {"ok": False, "message": "time_str required"}
        try:
            if "T" in ts:
                dt = datetime.fromisoformat(ts[:19])
            elif ":" in ts:
                today = datetime.now().strftime("%Y-%m-%d")
                dt    = datetime.fromisoformat(f"{today}T{ts[:5]}:00")
            else:
                return {"ok": False, "message": f"Unknown format: {ts}"}
            self._mock_time_str     = dt.strftime("%Y-%m-%dT%H:%M:%S")
            self.mock_current_time  = self._mock_time_str
            self._save_state()
            return {"ok": True, "mock_time": self._mock_time_str}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    # ── Mirrors _TickerManager.start() ───────────────────────────────────────

    def start(self, db) -> dict:
        with self._lock:
            if self.status == "running":
                return {"ok": False, "message": "Already running"}
            if not self._mock_time_str:
                return {"ok": False, "message": "Call set_mock_time() before start()"}

            # Accept both MongoData instance and raw pymongo db
            raw_db = db._db if hasattr(db, "_db") else db

            # ── Load option tokens (same as kite_ticker.py) ───────────────
            token_docs    = list(raw_db["active_option_tokens"].find({}, {"token": 1, "_id": 0}))
            option_tokens = []
            for doc in token_docs:
                try:
                    tok = str(doc.get("token") or "").strip()
                    if tok:
                        option_tokens.append(tok)
                except Exception:
                    pass
            option_tokens = option_tokens[:3000]

            # ── Load spot tokens from option_chain_index_spot ─────────────
            spot_token_ids: list[str] = []
            try:
                spot_docs = list(raw_db["option_chain_index_spot"].find(
                    {}, {"token": 1, "underlying": 1, "_id": 0}
                ))
                for doc in spot_docs:
                    tok = str(doc.get("token") or "").strip()
                    und = str(doc.get("underlying") or "").strip().upper()
                    if tok and und:
                        MOCK_SPOT_TOKENS[tok] = und
                        if tok not in spot_token_ids:
                            spot_token_ids.append(tok)
            except Exception as exc:
                logger.warning("mock: failed to load spot tokens: %s", exc)

            all_tokens = spot_token_ids + option_tokens

            print(
                f"[MOCK TICKER] subscribing "
                f"spot_tokens={len(spot_token_ids)} "
                f"option_tokens={len(option_tokens)} "
                f"total={len(all_tokens)}"
            )

            self.ltp_map      = {}
            self.spot_map     = {}
            self.tick_count   = 0
            self.error_msg    = ""
            self._last_minute = ""
            self._stopped     = False
            self.status       = "connecting"
            self.started_at   = datetime.now().isoformat()
            self._save_state()

            ticker = MockTicker(mock_time=self._mock_time_str)
            self._ticker = ticker

            # ── _on_ticks — mirrors kite_ticker._on_ticks exactly ─────────
            def _on_ticks(ws, ticks):
                if self._stopped:
                    return

                # Update mock_current_time from the ticker
                self.mock_current_time = ws._mock_time.strftime("%Y-%m-%dT%H:%M:%S")
                self._mock_time_str = self.mock_current_time
                self._save_state()

                now_ts      = self.mock_current_time
                trade_date  = now_ts[:10]
                now_minute  = now_ts[:16]
                listen_time = now_ts[11:16]

                # ── 1. Update ltp_map and spot_map ────────────────────────
                for tick in ticks:
                    token_str = str(tick.get("instrument_token") or "").strip()
                    lp = tick.get("last_price") or tick.get("last_traded_price")
                    if not token_str or lp is None:
                        continue
                    lp = float(lp)
                    self.ltp_map[token_str] = lp

                    # Spot token detection via MockTicker's spot_token_map
                    underlying = ws._spot_token_map.get(token_str)
                    if underlying:
                        self.spot_map[underlying] = lp
                        print(
                            f"[MOCK SPOT] underlying={underlying} "
                            f"spot_price={lp} timestamp={now_ts}"
                        )

                self.tick_count += len(ticks)
                current_ltp_map = dict(self.ltp_map)
                current_spot_map = dict(self.spot_map)
                listeners = list(self._listeners)
                active_modes = _get_active_runtime_modes()

                # ── 2. SL / TG / Trail / Exit — every tick ────────────────
                try:
                    from features.kite_event import build_broker_ltp_map, broker_live_tick
                    _db = MongoData()
                    broker_ltp_map = build_broker_ltp_map(ticks)
                    for activation_mode in active_modes:
                        broker_live_tick(
                            _db,
                            trade_date,
                            now_ts,
                            broker_ltp_map,
                            activation_mode=activation_mode,
                        )
                    _db.close()
                except Exception as exc:
                    logger.error("mock broker_live_tick error: %s", exc)

                if listeners:
                    tick_payload = {
                        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
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
                            logger.warning("mock ticker listener error: %s", exc)

                # ── 3. Entry — once per new mock minute ───────────────────
                if now_minute != self._last_minute:
                    self._last_minute = now_minute
                    try:
                        from features.execution_socket import (
                            _load_running_trade_records,
                            _execute_backtest_entries,
                            _sync_entered_legs_to_history,
                            _validate_trade_leg_storage,
                            build_entry_spot_snapshots,
                        )
                        _db = MongoData()
                        for activation_mode in active_modes:
                            records = _load_running_trade_records(
                                _db, trade_date, activation_mode=activation_mode
                            )
                            print(
                                f"[MOCK TICKER] timestamp={now_ts} "
                                f"mode={activation_mode} "
                                f"active_strategies={len(records)}"
                            )
                            if not records:
                                continue
                            build_entry_spot_snapshots(_db, records, listen_time, now_ts)
                            entries_executed = _execute_backtest_entries(
                                _db, records, listen_time, now_ts
                            )
                            if entries_executed:
                                synced_ids: dict[str, dict] = {}
                                for e in entries_executed:
                                    if not e.get("entered"):
                                        continue
                                    tid = str(e.get("trade_id") or "").strip()
                                    if tid:
                                        synced_ids[tid] = {"_id": tid}
                                if synced_ids:
                                    _sync_entered_legs_to_history(
                                        _db, list(synced_ids.values())
                                    )
                                    for tid in synced_ids:
                                        _validate_trade_leg_storage(_db, tid)
                        _db.close()
                    except Exception as exc:
                        logger.error("mock entry error at %s: %s", now_minute, exc)

            def _on_connect(ws, response):
                logger.info("MockTicker connected")
                self.status = "running"
                ws.subscribe(all_tokens)
                ws.set_mode(ws.MODE_FULL, all_tokens)
                print(
                    f"[MOCK TICKER] connected and subscribed "
                    f"total={len(all_tokens)} tokens | "
                    f"mock_time={self._mock_time_str}"
                )

            def _on_close(ws, code, reason):
                logger.info("MockTicker closed: %s %s", code, reason)
                self.status = "stopped"
                self._save_state()

            def _on_error(ws, code, reason):
                logger.error("MockTicker error: %s %s", code, reason)
                self.status    = "error"
                self.error_msg = f"{code}: {reason}"
                self._save_state()

            def _on_reconnect(ws, attempts_count):
                logger.info("MockTicker reconnecting... attempt %s", attempts_count)
                self.status = "connecting"
                self._save_state()

            ticker.on_ticks     = _on_ticks
            ticker.on_connect   = _on_connect
            ticker.on_close     = _on_close
            ticker.on_error     = _on_error
            ticker.on_reconnect = _on_reconnect

            ticker.connect(threaded=True)

            return {
                "ok":            True,
                "message":       "Mock ticker starting",
                "mock_time":     self._mock_time_str,
                "spot_tokens":   len(spot_token_ids),
                "option_tokens": len(option_tokens),
                "total_tokens":  len(all_tokens),
                "started_at":    self.started_at,
            }

    # ── Mirrors _TickerManager.stop() ────────────────────────────────────────

    def stop(self) -> dict:
        with self._lock:
            self._stopped = True

            if self._ticker:
                try:
                    self._ticker.stop_retry()
                except Exception:
                    pass
                try:
                    self._ticker.close()
                except Exception:
                    pass
                self._ticker = None

            self.status       = "stopped"
            self.ltp_map      = {}
            self.spot_map     = {}
            self.tick_count   = 0
            self.error_msg    = ""
            self.started_at   = ""
            self._last_minute = ""
            if self.mock_current_time:
                self._mock_time_str = self.mock_current_time
            self._save_state()

            print("[MOCK TICKER] stopped")
            return {"ok": True, "message": "Mock ticker stopped"}

    def restart(self, db) -> dict:
        self.stop()
        return self.start(db)

    def get_status(self) -> dict:
        return {
            "status":            self.status,
            "tick_count":        self.tick_count,
            "ltp_count":         len(self.ltp_map),
            "spot_map":          dict(self.spot_map),
            "started_at":        self.started_at,
            "error":             self.error_msg,
            "mock_time":         self.mock_current_time,
            "subscribed_tokens": len(self._ticker._subscribed) if self._ticker else 0,
        }

    def get_ltp(self, token: str) -> float | None:
        return self.ltp_map.get(str(token))

    def get_spot(self, underlying: str) -> float | None:
        return self.spot_map.get(underlying.upper())

    def add_tick_listener(self, listener) -> None:
        if not callable(listener):
            return
        with self._lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def remove_tick_listener(self, listener) -> None:
        with self._lock:
            self._listeners = [item for item in self._listeners if item is not listener]


# Singleton — imported by api.py
mock_ticker_manager = _MockTickerManager()
