"""
Client pool for Dhan's Live Order Update WebSocket (wss://api-order-update.dhan.co) — pushes
an account's order status changes (COMPLETE/REJECTED/CANCELLED/TRIGGER_PENDING/OPEN) the
instant Dhan emits them. Built so the Order Pad / Orderbook can drop poll-GET-/broker/
orders-every-4s loops in favor of a true push, the same way dhan_ticker.py already replaced
REST-polling for LTP. Mirrors dhan_ticker.py's connection-lifecycle conventions (websocket-
client, daemon thread, exponential-backoff reconnect, listener/broadcast fan-out).

One connection per Dhan account (client_id), not one for the whole app — kite_market_config
already stores a separate Dhan doc per app_user_id (see broker_accounts.py's
get_market_broker_accounts_for_user), and the Order Pad already places Dhan orders through
whichever specific kite_market_config._id the user picked (see algo.order/api.py's
_simulator_place_manual_order_core: broker_id == that doc's _id) — so a single global
connection was only ever correct by coincidence of today's single-account deployment.
Connections are started on demand (ensure_started) and auto-closed after IDLE_TIMEOUT_SECONDS
with no order activity, so the connection count tracks accounts actually trading right now,
not every account that ever existed.
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


class _DhanOrderUpdateConnection:
    """One WS connection for one Dhan account (client_id)."""

    def __init__(self, client_id: str, access_token: str):
        self.client_id = client_id
        self.access_token = access_token
        self.order_status_map: dict[str, dict] = {}
        self.status = "stopped"
        self.error_msg = ""
        self._stopped = True
        self._started = False
        self._lock = threading.Lock()
        self._listeners: list = []
        self._last_active = time.time()

    def touch(self) -> None:
        self._last_active = time.time()

    def idle_seconds(self) -> float:
        return time.time() - self._last_active

    def add_update_listener(self, listener) -> None:
        with self._lock:
            self._listeners.append(listener)

    def remove_update_listener(self, listener) -> None:
        with self._lock:
            if listener in self._listeners:
                self._listeners.remove(listener)

    def get_status(self, order_id: str) -> dict | None:
        return self.order_status_map.get(str(order_id or "").strip())

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            self._stopped = False
            self.status = "connecting"
        threading.Thread(
            target=self._run_ws,
            daemon=True,
            name=f"dhan_order_update_ws:{self.client_id}",
        ).start()

    def stop(self) -> None:
        self._stopped = True
        self._started = False

    def _run_ws(self) -> None:
        try:
            import websocket  # websocket-client package
        except ImportError:
            logger.error("[DHAN ORDER UPDATE] websocket-client not installed")
            self.status = "error"
            self.error_msg = "websocket-client not installed"
            self._started = False
            return

        client_id = self.client_id
        access_token = self.access_token

        def _on_open(ws):
            login_msg = {
                "LoginReq": {"MsgCode": 42, "ClientId": client_id, "Token": access_token},
                "UserType": "SELF",
            }
            ws.send(json.dumps(login_msg))
            self.status = "running"
            self.error_msg = ""
            print(f"[DHAN ORDER UPDATE] connected client_id={client_id}", flush=True)

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
            self.touch()
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
            logger.warning("[DHAN ORDER UPDATE] client_id=%s error: %s", client_id, error)

        def _on_close(ws, code, msg):
            print(f"[DHAN ORDER UPDATE] client_id={client_id} closed code={code} msg={msg}", flush=True)

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
                logger.error("[DHAN ORDER UPDATE] client_id=%s run_forever error: %s", client_id, exc)
            if self._stopped:
                break
            self.status = "reconnecting"
            print(f"[DHAN ORDER UPDATE] client_id={client_id} disconnected — reconnecting in {retry_delay}s...", flush=True)
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)
        self.status = "stopped"
        self._started = False


class _DhanOrderUpdatePool:
    """Per-account connection pool. ensure_started is cheap to call repeatedly — it's a
    no-op once that account's connection is already up, so callers (order placement, the
    browser-facing WS route) can call it defensively on every use without worrying about
    spawning duplicate connections."""

    IDLE_TIMEOUT_SECONDS = 900  # 15 min with no order activity on this account → disconnect

    def __init__(self):
        self._connections: dict[str, _DhanOrderUpdateConnection] = {}
        self._lock = threading.Lock()
        self._sweeper_started = False

    def ensure_started(self, client_id: str, access_token: str) -> "_DhanOrderUpdateConnection | None":
        client_id = str(client_id or "").strip()
        access_token = str(access_token or "").strip()
        if not client_id or not access_token:
            return None
        self._ensure_sweeper()
        with self._lock:
            conn = self._connections.get(client_id)
            if conn is None or conn.status in ("stopped", "error"):
                conn = _DhanOrderUpdateConnection(client_id, access_token)
                self._connections[client_id] = conn
                conn.start()
        conn.touch()
        return conn

    def get_connection(self, client_id: str) -> "_DhanOrderUpdateConnection | None":
        return self._connections.get(str(client_id or "").strip())

    def _ensure_sweeper(self) -> None:
        with self._lock:
            if self._sweeper_started:
                return
            self._sweeper_started = True
        threading.Thread(target=self._sweep_loop, daemon=True, name="dhan_order_update_sweeper").start()

    def _sweep_loop(self) -> None:
        while True:
            time.sleep(60)
            with self._lock:
                idle_conns = [
                    (cid, conn) for cid, conn in self._connections.items()
                    if conn.idle_seconds() > self.IDLE_TIMEOUT_SECONDS
                ]
                for cid, _conn in idle_conns:
                    self._connections.pop(cid, None)
            # stop() outside the lock — WebSocketApp teardown shouldn't hold it.
            for cid, conn in idle_conns:
                conn.stop()
            if idle_conns:
                print(f"[DHAN ORDER UPDATE] swept idle accounts: {[cid for cid, _ in idle_conns]}", flush=True)


dhan_order_update_pool = _DhanOrderUpdatePool()
