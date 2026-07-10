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

import logging

from features.telegram_notifier import notify_user

log = logging.getLogger(__name__)


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
) -> dict:
    """
    Returns {"order_id": str, "status": "success"|"error", "message": str, "raw": Any}.
    Never raises — any broker exception or an empty order_id on apparent success is
    reported back as status="error" instead. Fires a Telegram notification to the
    user on any failure (order-execution failures are the trader's money, not a
    backend bug, so they go to the user bucket — see telegram_notifier.py).
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

    return {'order_id': order_id, 'status': 'success', 'message': '', 'raw': raw_order_id}
