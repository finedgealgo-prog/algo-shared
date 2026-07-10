from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from features.mock_ticker import mock_ticker_manager
from features.mongo_data import MongoData

log = logging.getLogger(__name__)
IST = timezone(timedelta(hours=5, minutes=30))

mock_kite_socket_router = APIRouter()

MODE_LTP = "ltp"
MODE_QUOTE = "quote"
MODE_FULL = "full"
VALID_MODES = {MODE_LTP, MODE_QUOTE, MODE_FULL}


def _now_iso() -> str:
    return datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    return str(value)


@dataclass
class _MockKiteSocketSession:
    websocket: WebSocket
    session_id: str
    subscribed_tokens: set[str] = field(default_factory=set)
    default_tokens: set[str] = field(default_factory=set)
    token_modes: dict[str, str] = field(default_factory=dict)
    last_emitted_time: str = ""
    closed: bool = False
    task: asyncio.Task | None = None

    @property
    def all_tokens(self) -> set[str]:
        return set(self.default_tokens) | set(self.subscribed_tokens)

    def set_mode(self, mode: str, tokens: list[str]) -> None:
        normalized_mode = (mode or "").strip().lower()
        if normalized_mode not in VALID_MODES:
            return
        for token in tokens:
            normalized_token = str(token or "").strip()
            if normalized_token:
                self.token_modes[normalized_token] = normalized_mode

    def get_mode(self, token: str) -> str:
        return self.token_modes.get(str(token or "").strip(), MODE_LTP)


class _MockKiteSocketHub:
    def __init__(self) -> None:
        self._sessions: dict[str, _MockKiteSocketSession] = {}
        self._lock = asyncio.Lock()

    async def register(self, websocket: WebSocket) -> _MockKiteSocketSession:
        await websocket.accept()
        default_tokens = await asyncio.to_thread(self._load_default_tokens)
        session = _MockKiteSocketSession(
            websocket=websocket,
            session_id=uuid.uuid4().hex,
            default_tokens=default_tokens,
        )
        session.set_mode(MODE_LTP, list(default_tokens))
        async with self._lock:
            self._sessions[session.session_id] = session
        print(
            f"[MOCK DEFAULT TOKENS] session={session.session_id} "
            f"count={len(default_tokens)} tokens={sorted(default_tokens)}"
        )
        session.task = asyncio.create_task(self._emit_loop(session))
        await self._send_message(
            session,
            "message",
            {
                "message": "mock kite socket connected",
                "session_id": session.session_id,
                "default_tokens": sorted(default_tokens),
                "supported_actions": ["subscribe", "unsubscribe", "mode"],
                "supported_modes": sorted(VALID_MODES),
            },
        )
        return session

    async def unregister(self, session: _MockKiteSocketSession) -> None:
        session.closed = True
        if session.task and not session.task.done():
            session.task.cancel()
            try:
                await session.task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                log.debug("mock socket task close error session=%s: %s", session.session_id, exc)
        async with self._lock:
            self._sessions.pop(session.session_id, None)

    async def handle_client_message(self, session: _MockKiteSocketSession, raw_message: str) -> None:
        try:
            payload = json.loads(raw_message or "{}")
        except Exception:
            await self._send_message(session, "error", "invalid JSON payload")
            return

        action = str(payload.get("a") or "").strip().lower()
        value = payload.get("v")

        if action == "subscribe":
            tokens = self._normalize_token_list(value)
            for token in tokens:
                session.subscribed_tokens.add(token)
            session.set_mode(MODE_LTP, tokens)
            await self._send_message(
                session,
                "message",
                {
                    "message": "tokens subscribed",
                    "subscribed": tokens,
                    "total_subscribed": len(session.subscribed_tokens),
                },
            )
            return

        if action == "unsubscribe":
            tokens = self._normalize_token_list(value)
            for token in tokens:
                session.subscribed_tokens.discard(token)
                if token not in session.default_tokens:
                    session.token_modes.pop(token, None)
            await self._send_message(
                session,
                "message",
                {
                    "message": "tokens unsubscribed",
                    "unsubscribed": tokens,
                    "total_subscribed": len(session.subscribed_tokens),
                },
            )
            return

        if action == "mode":
            mode_value = value if isinstance(value, list) else []
            mode = str(mode_value[0] if len(mode_value) > 0 else "").strip().lower()
            tokens = self._normalize_token_list(mode_value[1] if len(mode_value) > 1 else [])
            if mode not in VALID_MODES:
                await self._send_message(session, "error", f"unsupported mode: {mode}")
                return
            session.set_mode(mode, tokens)
            await self._send_message(
                session,
                "message",
                {
                    "message": "mode updated",
                    "mode": mode,
                    "tokens": tokens,
                },
            )
            return

        await self._send_message(session, "error", f"unsupported action: {action}")

    async def _emit_loop(self, session: _MockKiteSocketSession) -> None:
        try:
            while not session.closed:
                current_time = str(mock_ticker_manager.mock_current_time or "").strip()
                is_running = mock_ticker_manager.status in ("running", "connecting")
                if not is_running or not current_time:
                    await asyncio.sleep(1)
                    continue

                if current_time == session.last_emitted_time:
                    await asyncio.sleep(0.2)
                    continue

                subscribed_tokens = session.all_tokens
                if not subscribed_tokens:
                    session.last_emitted_time = current_time
                    await asyncio.sleep(1)
                    continue

                print(
                    f"[MOCK SUBSCRIBED TOKENS] listen_time={current_time[11:16]} "
                    f"count={len(subscribed_tokens)} tokens={sorted(subscribed_tokens)}"
                )

                ticks = await asyncio.to_thread(
                    self._load_ticks,
                    current_time,
                    subscribed_tokens,
                    dict(session.token_modes),
                )
                if ticks:
                    await session.websocket.send_text(json.dumps({
                        "type": "ticks",
                        "timestamp": current_time,
                        "data": ticks,
                    }, default=_json_default))
                session.last_emitted_time = current_time
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("mock emit loop error session=%s: %s", session.session_id, exc)
            try:
                await self._send_message(session, "error", str(exc))
            except Exception:
                pass

    async def _send_message(self, session: _MockKiteSocketSession, message_type: str, data: Any) -> None:
        await session.websocket.send_text(json.dumps({
            "type": message_type,
            "data": data,
            "server_time": _now_iso(),
        }, default=_json_default))

    def _normalize_token_list(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        tokens: list[str] = []
        for item in value:
            token = str(item or "").strip()
            if token and token not in tokens:
                tokens.append(token)
        return tokens

    def _load_default_tokens(self) -> set[str]:
        db = MongoData()
        try:
            default_tokens: set[str] = set()

            spot_tokens = db._db["option_chain_index_spot"].distinct("token")
            default_tokens.update(
                str(token or "").strip()
                for token in spot_tokens
                if str(token or "").strip()
            )

            option_docs = db._db["active_option_tokens"].find({}, {"token": 1, "_id": 0})
            for doc in option_docs:
                token = str((doc or {}).get("token") or "").strip()
                if token:
                    default_tokens.add(token)

            return default_tokens
        except Exception as exc:
            log.warning("mock default token load error: %s", exc)
            return set()
        finally:
            try:
                db.close()
            except Exception:
                pass

    def _load_ticks(self, current_time: str, subscribed_tokens: set[str], token_modes: dict[str, str]) -> list[dict]:
        db = MongoData()
        try:
            token_list = [str(token or "").strip() for token in subscribed_tokens if str(token or "").strip()]
            if not token_list:
                return []

            token_set = set(token_list)
            ticks: list[dict] = []

            spot_docs = list(db._db["option_chain_index_spot"].find(
                {"timestamp": current_time, "token": {"$in": token_list}},
                {"_id": 0, "timestamp": 1, "underlying": 1, "spot_price": 1, "token": 1},
            ))
            for doc in spot_docs:
                token = str(doc.get("token") or "").strip()
                if not token:
                    continue
                ticks.append(self._build_index_tick(doc, token_modes.get(token, MODE_LTP)))
                token_set.discard(token)

            if token_set:
                option_docs = list(db._db["option_chain_historical_data"].find(
                    {"timestamp": current_time, "token": {"$in": list(token_set)}},
                    {
                        "_id": 0,
                        "close": 1,
                        "delta": 1,
                        "expiry": 1,
                        "gamma": 1,
                        "iv": 1,
                        "oi": 1,
                        "rho": 1,
                        "strike": 1,
                        "theta": 1,
                        "timestamp": 1,
                        "token": 1,
                        "type": 1,
                        "underlying": 1,
                        "vega": 1,
                    },
                ))
                for doc in option_docs:
                    token = str(doc.get("token") or "").strip()
                    if not token:
                        continue
                    ticks.append(self._build_option_tick(doc, token_modes.get(token, MODE_LTP)))

            return ticks
        finally:
            try:
                db.close()
            except Exception:
                pass

    def _build_index_tick(self, doc: dict, mode: str) -> dict:
        token = str(doc.get("token") or "").strip()
        price = float(doc.get("spot_price") or 0)
        timestamp = str(doc.get("timestamp") or "").strip()
        ohlc = {
            "open": price,
            "high": price,
            "low": price,
            "close": price,
        }
        tick = {
            "tradable": False,
            "mode": mode,
            "instrument_token": token,
            "last_price": price,
            "ohlc": ohlc,
            "change": 0.0,
            "token": token,
            "timestamp": timestamp,
            "underlying": str(doc.get("underlying") or "").strip().upper(),
        }
        if mode == MODE_LTP:
            return {
                "tradable": False,
                "mode": mode,
                "instrument_token": token,
                "last_price": price,
                "token": token,
                "timestamp": timestamp,
                "underlying": str(doc.get("underlying") or "").strip().upper(),
            }
        if mode == MODE_FULL:
            tick["exchange_timestamp"] = timestamp
        return tick

    def _build_option_tick(self, doc: dict, mode: str) -> dict:
        token = str(doc.get("token") or "").strip()
        ltp = float(doc.get("close") or 0)
        oi = int(doc.get("oi") or 0)
        timestamp = str(doc.get("timestamp") or "").strip()
        if mode == MODE_LTP:
            return {
                "tradable": True,
                "mode": mode,
                "instrument_token": token,
                "last_price": ltp,
                "token": token,
                "timestamp": timestamp,
                "underlying": str(doc.get("underlying") or "").strip().upper(),
                "strike": doc.get("strike"),
                "type": str(doc.get("type") or "").strip().upper(),
                "expiry": str(doc.get("expiry") or "").strip(),
            }

        tick = {
            "tradable": True,
            "mode": mode,
            "instrument_token": token,
            "last_price": ltp,
            "last_traded_quantity": 0,
            "average_traded_price": ltp,
            "volume_traded": 0,
            "total_buy_quantity": 0,
            "total_sell_quantity": 0,
            "ohlc": {
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp,
            },
            "change": 0.0,
            "token": token,
            "timestamp": timestamp,
            "underlying": str(doc.get("underlying") or "").strip().upper(),
            "strike": doc.get("strike"),
            "type": str(doc.get("type") or "").strip().upper(),
            "expiry": str(doc.get("expiry") or "").strip(),
            "oi": oi,
            "iv": doc.get("iv"),
            "delta": doc.get("delta"),
            "gamma": doc.get("gamma"),
            "theta": doc.get("theta"),
            "vega": doc.get("vega"),
            "rho": doc.get("rho"),
        }
        if mode == MODE_FULL:
            tick["last_trade_time"] = timestamp
            tick["oi"] = oi
            tick["oi_day_high"] = oi
            tick["oi_day_low"] = oi
            tick["exchange_timestamp"] = timestamp
            tick["depth"] = {"buy": [], "sell": []}
        return tick

    def get_status(self) -> dict:
        return {
            "connections": len(self._sessions),
            "mock_status": mock_ticker_manager.status,
            "mock_time": mock_ticker_manager.mock_current_time,
        }


mock_kite_socket_hub = _MockKiteSocketHub()


@mock_kite_socket_router.get("/mock/socket/status")
async def mock_socket_status():
    return mock_kite_socket_hub.get_status()


@mock_kite_socket_router.get("/mock/socket/set-time")
async def mock_socket_set_time(time: str = Query(default="")):
    if mock_ticker_manager.status in ("running", "connecting"):
        return {
            "ok": False,
            "message": "Stop mock ticker before changing socket time",
            "mock_time": mock_ticker_manager.mock_current_time,
        }
    result = mock_ticker_manager.set_mock_time(time)
    return result


@mock_kite_socket_router.websocket("/ws/mock/kite")
async def mock_kite_socket(websocket: WebSocket):
    session = await mock_kite_socket_hub.register(websocket)
    try:
        while True:
            raw_message = await websocket.receive_text()
            await mock_kite_socket_hub.handle_client_message(session, raw_message)
    except WebSocketDisconnect:
        pass
    finally:
        await mock_kite_socket_hub.unregister(session)
