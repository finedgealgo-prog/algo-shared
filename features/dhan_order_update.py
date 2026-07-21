"""
Persistent client for Dhan's Live Order Update WebSocket (wss://api-order-update.dhan.co) —
pushes this account's order status changes (COMPLETE/REJECTED/CANCELLED/TRIGGER_PENDING/OPEN)
the instant Dhan emits them. Built so the Order Pad / Orderbook can drop their old poll-
GET-/broker/orders-every-4s loop in favor of a true push, the same way dhan_ticker.py already
replaced REST-polling for LTP. Mirrors dhan_ticker.py's connection-lifecycle conventions
(websocket-client, daemon thread, exponential-backoff reconnect, listener/broadcast fan-out)
— a second, narrower Dhan WS feed, same pattern.
"""

import json
import logging
import ssl
import threading
import time

logger = logging.getLogger(__name__)

_ORDER_UPDATE_WS_URL = "wss://api-order-update.dhan.co"

# Dhan's own order-update "Status" values → the same COMPLETE/OPEN/REJECTED/CANCELLED/
# TRIGGER_PENDING vocabulary dhan_broker.py's REST orders() mapping already uses, so
# callers never have to juggle two different status vocabularies for the same order.
_STATUS_MAP = {
    "TRADED": "COMPLETE",
    "COMPLETE": "COMPLETE",
    "REJECTED": "REJECTED",
    "CANCELLED": "CANCELLED",
    "EXPIRED": "CANCELLED",
    "PART_TRADED": "OPEN",
    "PENDING": "OPEN",
    "TRANSIT": "OPEN",
    "TRIGGER_PENDING": "TRIGGER_PENDING",
}


class _DhanOrderUpdateManager:
    def __init__(self):
        self.order_status_map: dict[str, dict] = {}
        self.status = "stopped"
        self.error_msg = ""
        self._stopped = True
        self._started = False
        self._lock = threading.Lock()
        self._listeners: list = []

    # ── listener registration — same seam dhan_ticker.py's add_tick_listener uses to
    # bridge this thread's callbacks into a asyncio event loop for browser pushes ──
    def add_update_listener(self, listener) -> None:
        with self._lock:
            self._listeners.append(listener)

    def remove_update_listener(self, listener) -> None:
        with self._lock:
            if listener in self._listeners:
                self._listeners.remove(listener)

    def get_status(self, order_id: str) -> dict | None:
        return self.order_status_map.get(str(order_id or "").strip())

    def start(self, db) -> dict:
        with self._lock:
            if self._started:
                return {"ok": False, "message": "Already running"}

            cfg = db["kite_market_config"].find_one({"broker": "dhan", "enabled": True})
            if not cfg:
                self.status = "error"
                self.error_msg = "No enabled dhan config in kite_market_config"
                return {"ok": False, "message": self.error_msg}

            client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
            access_token = str(cfg.get("access_token") or "").strip()
            if not client_id or not access_token:
                self.status = "error"
                self.error_msg = "user_id or access_token missing in kite_market_config (broker=dhan)"
                return {"ok": False, "message": self.error_msg}

            self._started = True
            self._stopped = False
            self.status = "connecting"

        threading.Thread(
            target=self._run_ws,
            args=(client_id, access_token),
            daemon=True,
            name="dhan_order_update_ws",
        ).start()
        return {"ok": True, "message": "Starting"}

    def stop(self) -> None:
        self._stopped = True
        self._started = False

    def _run_ws(self, client_id: str, access_token: str) -> None:
        try:
            import websocket  # websocket-client package
        except ImportError:
            logger.error("[DHAN ORDER UPDATE] websocket-client not installed")
            self.status = "error"
            self.error_msg = "websocket-client not installed"
            self._started = False
            return

        def _on_open(ws):
            login_msg = {
                "LoginReq": {"MsgCode": 42, "ClientId": client_id, "Token": access_token},
                "UserType": "SELF",
            }
            ws.send(json.dumps(login_msg))
            self.status = "running"
            self.error_msg = ""
            print("[DHAN ORDER UPDATE] connected", flush=True)

        def _on_message(ws, message):
            try:
                payload = json.loads(message)
            except Exception:
                return
            if payload.get("Type") != "order_alert":
                return
            data = payload.get("Data") or {}
            order_no = str(data.get("OrderNo") or "").strip()
            if not order_no:
                return
            raw_status = str(data.get("Status") or "").strip().upper()
            entry = {
                "order_id": order_no,
                "status": _STATUS_MAP.get(raw_status, raw_status or "OPEN"),
                "filled_quantity": data.get("TradedQty"),
                "average_price": data.get("AvgTradedPrice"),
                "updated_at": data.get("LastUpdatedTime"),
            }
            with self._lock:
                self.order_status_map[order_no] = entry
                listeners = list(self._listeners)
            for listener in listeners:
                try:
                    listener(entry)
                except Exception as exc:
                    logger.warning("[DHAN ORDER UPDATE] listener error: %s", exc)

        def _on_error(ws, error):
            logger.warning("[DHAN ORDER UPDATE] error: %s", error)

        def _on_close(ws, code, msg):
            print(f"[DHAN ORDER UPDATE] closed code={code} msg={msg}", flush=True)

        retry_delay = 5
        while not self._stopped:
            try:
                ws_app = websocket.WebSocketApp(
                    _ORDER_UPDATE_WS_URL,
                    on_open=_on_open,
                    on_message=_on_message,
                    on_error=_on_error,
                    on_close=_on_close,
                )
                ws_app.run_forever(ping_interval=0, sslopt={"cert_reqs": ssl.CERT_NONE}, reconnect=0)
            except Exception as exc:
                logger.error("[DHAN ORDER UPDATE] run_forever error: %s", exc)
            if self._stopped:
                break
            self.status = "reconnecting"
            print(f"[DHAN ORDER UPDATE] disconnected — reconnecting in {retry_delay}s...", flush=True)
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)
        self.status = "stopped"
        self._started = False


dhan_order_update_manager = _DhanOrderUpdateManager()
