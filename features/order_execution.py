"""
Single shared broker-order-placement function — the "one parent code path" every
order placement (live entry, live exit, protection SL/TP, manual square-off,
rejection square-off, and the manual paper-trade order pad at
`/trade/positions/place-order`) funnels through, regardless of broker.

`broker` must already be a resolved adapter exposing the uniform interface shared
by KiteConnect, FlatTradeAdapter, and DhanAdapter: `place_order(**kwargs) -> str`.
Resolving which broker/account to use is the caller's job (see
`live_order_manager.get_broker_for_trade` for the live-engine side) — this function
only owns the mechanics of the call itself, so it has no opinion on credential
storage and works identically for every broker.
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable

from features.telegram_notifier import notify_user

log = logging.getLogger(__name__)


async def place_legs_hedge_ordered(
    legs: list,
    place_one_leg: Callable[[Any], Awaitable[dict]],
) -> list[dict]:
    """
    Places every BUY leg first (as one concurrent batch), then every SELL leg (as a
    second concurrent batch) — instead of firing every leg at once regardless of
    side. For a hedged basket (e.g. buy the protective leg, sell the other), this
    gives the broker a real BUY order already in the pipe before the SELL leg
    arrives, rather than both sides landing simultaneously.

    Deliberately does NOT wait for the BUY leg to actually FILL before placing the
    SELL leg — only for its placement call (including the immediate status check
    in place_broker_order) to return. Waiting for a fill could stall the SELL leg
    indefinitely on a slow-filling BUY, or skip it entirely if the BUY errors out.
    Per the same reasoning, a failed/rejected BUY leg does not block the SELL leg
    from being placed — every leg gets a placement attempt regardless of how any
    other leg in the basket fared.

    `legs` must each expose a `.side` attribute ("BUY"/"SELL"); `place_one_leg` is
    the caller's own per-leg coroutine (already broker-specific — Dhan/FlatTrade/
    Kite each build their own). Returns results in the same order as `legs`, not
    placement order, so callers keep matching by index/leg_id unaffected by this
    reordering.
    """
    indexed = list(enumerate(legs))
    buy_batch = [(i, leg) for i, leg in indexed if str(getattr(leg, 'side', '')).upper() == 'BUY']
    sell_batch = [(i, leg) for i, leg in indexed if str(getattr(leg, 'side', '')).upper() != 'BUY']

    results: dict[int, dict] = {}
    if buy_batch:
        buy_results = await asyncio.gather(*(place_one_leg(leg) for _, leg in buy_batch))
        for (i, _), r in zip(buy_batch, buy_results):
            results[i] = r
    if sell_batch:
        sell_results = await asyncio.gather(*(place_one_leg(leg) for _, leg in sell_batch))
        for (i, _), r in zip(sell_batch, sell_results):
            results[i] = r
    return [results[i] for i in range(len(legs))]


def place_broker_order(
    broker,
    *,
    tradingsymbol: str,
    exchange: str,
    transaction_type: str,
    quantity: int,
    order_type: str,
    product: str,
    variety: str = 'regular',
    price: float = 0.0,
    trigger_price: float = 0.0,
    validity: str = 'DAY',
    context: dict | None = None,
    broker_kwargs: dict | None = None,
    check_status: bool = True,
) -> dict:
    """
    Returns {"order_id": str, "status": "success"|"error", "message": str, "raw": Any}.
    Never raises — any broker exception or an empty order_id on apparent success is
    reported back as status="error" instead. Fires a Telegram notification to the
    user on any failure (order-execution failures are the trader's money, not a
    backend bug, so they go to the user bucket — see telegram_notifier.py).

    check_status=False skips the post-placement broker.orders() call below —
    a second full order-book REST round trip per leg, on top of place_order()'s
    own. Callers where a human is waiting on the response (e.g. the Order Pad)
    should pass False for near-instant placement; broker_status just comes back
    "UNKNOWN" and the real fill status is picked up later via the normal
    positions/order-book refresh. Live entry/exit and the SL/TP monitor keep the
    default (True) since they act on broker_status immediately.

    broker_kwargs: opaque extra kwargs forwarded verbatim to broker.place_order() —
    this function stays broker-agnostic on purpose and must never name a specific
    broker's own concepts (e.g. Dhan's security_id/exchange_segment) in its own
    signature. Only the caller (which already knows which broker it's placing
    through) and that broker's own adapter file know what belongs in this dict;
    Kite/FlatTrade callers simply never populate it, so it's a no-op for them.
    """
    ctx = dict(context or {})
    if broker is None:
        message = 'No broker session available for this order'
        log.error('[ORDER] %s context=%s', message, ctx)
        notify_user('order_placement_failed', message, ctx)
        return {'order_id': '', 'status': 'error', 'message': message, 'raw': None}

    try:
        raw_order_id = broker.place_order(
            tradingsymbol=tradingsymbol,
            exchange=exchange,
            transaction_type=transaction_type,
            quantity=int(quantity),
            order_type=order_type,
            product=product,
            variety=variety,
            price=price,
            trigger_price=trigger_price,
            validity=validity,
            **(broker_kwargs or {}),
        )
    except Exception as exc:
        message = str(exc)
        log.error('[ORDER FAILED] %s context=%s', message, ctx)
        notify_user('order_placement_failed', message, {**ctx, 'symbol': tradingsymbol})
        return {'order_id': '', 'status': 'error', 'message': message, 'raw': None}

    order_id = str(raw_order_id or '').strip()
    if not order_id:
        message = 'Broker returned success but no order_id'
        log.error('[ORDER FAILED] %s context=%s', message, ctx)
        notify_user('order_placement_failed', message, {**ctx, 'symbol': tradingsymbol})
        return {'order_id': '', 'status': 'error', 'message': message, 'raw': raw_order_id}

    # A returned order_id only means the broker ACCEPTED the order for processing —
    # not that it filled. Check the order book right away for this order_id's real,
    # current status (COMPLETE/OPEN/REJECTED/CANCELLED/TRIGGER PENDING — Kite-shaped
    # terms all three adapters already report in) so the caller isn't told "success"
    # for an order that's actually still pending or was rejected moments later.
    # broker_status stays "UNKNOWN" (not a failure) if this check itself errors —
    # the placement already succeeded; a status-check hiccup shouldn't overturn that.
    broker_status = 'UNKNOWN'
    broker_status_detail: dict = {}
    if check_status:
        try:
            book = broker.orders()
            match = next((o for o in book if str(o.get('order_id') or '') == order_id), None)
            if match:
                broker_status = str(match.get('status') or 'UNKNOWN').upper()
                broker_status_detail = {
                    'average_price': match.get('average_price'),
                    'filled_quantity': match.get('filled_quantity'),
                    'status_message': match.get('status_message') or '',
                }
        except Exception as exc:
            log.debug('[ORDER STATUS CHECK] failed order_id=%s: %s', order_id, exc)

    if broker_status == 'REJECTED':
        message = str(broker_status_detail.get('status_message') or 'Order rejected by broker after placement.')
        log.error('[ORDER REJECTED] order_id=%s context=%s', order_id, ctx)
        notify_user('order_placement_failed', message, {**ctx, 'symbol': tradingsymbol, 'order_id': order_id})

    return {
        'order_id': order_id,
        'status': 'success',
        'broker_status': broker_status,
        'message': '',
        'raw': raw_order_id,
        **broker_status_detail,
    }
