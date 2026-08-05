"""
alert_events_socket.py
────────────────────────
Push channel that tells the algo-admin chart page the instant one of its
live price/trendline/indicator alerts has actually fired — so the "Alert
Triggered" popup, the alerts-list row status, and once_only removal are all
driven by the same backend detection that already does the real work
(alert_checker.py's tick-accurate poll, independent of any open browser tab),
instead of the frontend re-deriving the same crossing from on-screen bars and
occasionally firing/delivering it a second time itself.

Reuses execution_socket.py's existing per-user room registry and JWT-as-
first-message auth handshake (_register_user_websocket / _broadcast_user_
channel_message / _ws_authenticate / _build_message) rather than standing up
a second connection-management implementation — those helpers are already
generic over an arbitrary channel name, this just uses its own ("chart-
alerts") instead of "executions"/"execute-orders"/"update".

mark_alert_fired() is the sync half: alert_checker.py's _fire_alert runs on
a worker thread (via asyncio.to_thread), so it can't await a broadcast
directly. It just appends to PENDING_ALERT_EVENTS (plain dict/list mutation,
GIL-safe) — same shape as execution_socket.py's mark_strategy_activity /
PENDING_STRATEGY_ACTIVITIES, which solves the identical thread-to-asyncio
handoff for SL/TP/reentry activity events.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from features.execution_socket import (
    CONNECTED_USER_CHANNEL_WEBSOCKETS,
    _broadcast_user_channel_message,
    _build_message,
    _register_user_websocket,
    _unregister_user_websocket,
    _ws_authenticate,
)

log = logging.getLogger(__name__)

ALERT_EVENTS_CHANNEL = "chart-alerts"
POLL_TIMEOUT_SECONDS = 1.0

# user_id -> list of fired-alert payload dicts queued since the last flush.
PENDING_ALERT_EVENTS: dict[str, list[dict]] = {}

router = APIRouter()


def mark_alert_fired(user_id: str, payload: dict) -> None:
    """Sync — called from alert_checker.py's _fire_alert (worker thread)."""
    uid = str(user_id or "").strip()
    if not uid:
        return
    PENDING_ALERT_EVENTS.setdefault(uid, []).append(payload)
    log.info("[alert_events_socket] TEMP DIAG queued alert_id=%s for user=%s (pending now: %d)",
              payload.get("alert_id"), uid, len(PENDING_ALERT_EVENTS[uid]))


async def _flush_pending_alert_events(user_id: str) -> None:
    events = PENDING_ALERT_EVENTS.pop(user_id, None)
    if not events:
        return
    message = _build_message("alert_fired", "Chart alert triggered", {"events": events})
    room_key = f"{user_id}:{ALERT_EVENTS_CHANNEL}"
    sockets_in_room = len(CONNECTED_USER_CHANNEL_WEBSOCKETS.get(room_key) or [])
    delivered = await _broadcast_user_channel_message(user_id, ALERT_EVENTS_CHANNEL, message)
    log.info("[alert_events_socket] TEMP DIAG flush user=%s events=%d sockets_registered=%d delivered=%d",
              user_id, len(events), sockets_in_room, delivered)


@router.websocket("/ws/alert-events")
async def alert_events_socket(websocket: WebSocket) -> None:
    await websocket.accept()
    user_id = await _ws_authenticate(websocket)
    if not user_id:
        return

    _register_user_websocket(ALERT_EVENTS_CHANNEL, user_id, websocket)
    log.info("[alert_events_socket] TEMP DIAG registered user=%s", user_id)
    await websocket.send_text(_build_message(
        "connection_established",
        "Alert events websocket connected",
        {"channel": ALERT_EVENTS_CHANNEL, "user_id": user_id},
    ))
    # Anything that fired between the alert being armed and this socket
    # actually connecting (e.g. page refresh mid-fire) is still delivered
    # on the very first poll below rather than lost.
    await _flush_pending_alert_events(user_id)

    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=POLL_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                pass
            await _flush_pending_alert_events(user_id)
    except WebSocketDisconnect as exc:
        log.info("[alert_events_socket] TEMP DIAG disconnected user=%s code=%s reason=%s", user_id, getattr(exc, "code", None), getattr(exc, "reason", None))
        return
    except Exception:
        log.exception("[alert_events_socket] connection error for user %s", user_id)
    finally:
        _unregister_user_websocket(websocket)
