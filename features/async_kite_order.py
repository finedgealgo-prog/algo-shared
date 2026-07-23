"""
async_kite_order.py
────────────────────
True async (httpx) order placement against Kite Connect's REST API —
bypasses the official `kiteconnect` SDK (which is built on blocking
`requests`) so an entry-processing event loop can have thousands of order
placements in flight at once without any of them queuing behind a bounded
thread pool.

Scope — READ THIS BEFORE WIRING THIS INTO A LIVE PATH:
This module replicates ONLY the raw "place an already-fully-formed order"
HTTP call — i.e. the exact equivalent of kiteconnect's
KiteConnect.place_order(), traced line-for-line from
site-packages/kiteconnect/connect.py (place_order + _request, see comments
below for the exact source mapping). It deliberately does NOT replicate the
~150 lines of order-construction business logic that
live_order_manager.place_live_entry_order() runs before calling
kite.place_order() today — MPP bid/ask protection pricing, tick-size
rounding, FlatTrade's SL-MKT→SL-LMT conversion, same-option conflict
detection. That logic is real-money-critical and has NOT been ported here.

Until that logic is ported (a separate, carefully-tested piece of work),
this module is NOT a drop-in replacement for place_live_entry_order and
must not be called from the live order-entry path. It exists so the async
entry engine has a correct, ready-to-use primitive for when that porting
work happens, and so its exact behavior (headers/body/error-mapping) is
documented and testable in isolation today.

Source mapping (kiteconnect==4.2.0, connect.py):
  URL      : urljoin(root, "/orders/{variety}")           connect.py:867-873
  Headers  : X-Kite-Version: 3, User-Agent, Authorization  connect.py:876-884
             Authorization: "token {api_key}:{access_token}"
  Method   : POST, form-encoded body (NOT json)             connect.py:890-903
  Success  : response json()["data"]["order_id"]            connect.py:911-929
  Error    : json()["status"] == "error" or "error_type"
             present → raise mapped exception                connect.py:920-927
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

log = logging.getLogger(__name__)

_KITE_ROOT = "https://api.kite.trade"
_KITE_HEADER_VERSION = "3"
_USER_AGENT = "Option-algo-async-kite/1.0"
_DEFAULT_TIMEOUT_SECONDS = 7.0


class AsyncKiteOrderError(Exception):
    """Raised when Kite's API returns an error response for an order call."""

    def __init__(self, message: str, error_type: str = "", status_code: int = 0):
        super().__init__(message)
        self.error_type = error_type
        self.status_code = status_code


# One shared httpx.AsyncClient for connection pooling across all order
# placements — created lazily, bound to whichever event loop first uses it
# (matches the "one dedicated loop for the async entry engine" design).
_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT_SECONDS)
    return _client


async def async_place_order(
    api_key: str,
    access_token: str,
    *,
    variety: str,
    exchange: str,
    tradingsymbol: str,
    transaction_type: str,
    quantity: int,
    product: str,
    order_type: str,
    price: float | None = None,
    trigger_price: float | None = None,
    validity: str | None = None,
    disclosed_quantity: int | None = None,
    tag: str | None = None,
) -> str:
    """
    Place an order against Kite Connect's REST API directly (async).

    Params mirror kiteconnect.KiteConnect.place_order()'s signature exactly
    (only the fields this codebase's order-construction logic actually
    uses are exposed — extend if a caller needs more).

    Returns the order_id string on success.
    Raises AsyncKiteOrderError on a Kite-reported error (mirrors the
    exception kiteconnect itself would raise, so callers can catch a single
    type instead of the SDK's per-error-type exception hierarchy).
    Raises httpx.HTTPError subclasses on network-level failures (timeout,
    connection error) — same class of failure the SDK's `requests` call
    would surface as a requests exception.
    """
    params: dict[str, Any] = {
        "exchange": exchange,
        "tradingsymbol": tradingsymbol,
        "transaction_type": transaction_type,
        "quantity": quantity,
        "product": product,
        "order_type": order_type,
    }
    if price is not None:
        params["price"] = price
    if trigger_price is not None:
        params["trigger_price"] = trigger_price
    if validity is not None:
        params["validity"] = validity
    if disclosed_quantity is not None:
        params["disclosed_quantity"] = disclosed_quantity
    if tag is not None:
        params["tag"] = tag

    headers = {
        "X-Kite-Version": _KITE_HEADER_VERSION,
        "User-Agent": _USER_AGENT,
        "Authorization": f"token {api_key}:{access_token}",
    }
    url = f"{_KITE_ROOT}/orders/{variety}"

    client = _get_client()
    resp = await client.post(url, data=params, headers=headers)

    content_type = resp.headers.get("content-type", "")
    if "json" not in content_type:
        raise AsyncKiteOrderError(
            f"Unknown Content-Type ({content_type}) with response: {resp.text[:300]}"
        )

    try:
        data = resp.json()
    except ValueError as exc:
        raise AsyncKiteOrderError(f"Couldn't parse JSON response: {resp.text[:300]}") from exc

    if data.get("status") == "error" or data.get("error_type"):
        raise AsyncKiteOrderError(
            str(data.get("message") or "unknown Kite error"),
            error_type=str(data.get("error_type") or ""),
            status_code=resp.status_code,
        )

    return str((data.get("data") or {}).get("order_id") or "")


async def aclose() -> None:
    """Call on process shutdown to release the pooled httpx connections."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
