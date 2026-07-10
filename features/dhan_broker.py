"""
Wraps Dhan's v2 REST order API with a KiteConnect/FlatTradeAdapter-compatible surface:
  place_order(**params) → order_id str
  orders()              → list[dict] in Kite field names
  cancel_order(variety, order_id)
  quote(symbols)         → dict in Kite depth format

place_order() is extracted from the existing, working Dhan order-placement logic in
api.py's `/trade/positions/place-order` endpoint (the one piece of Dhan order handling
already exercised by the manual paper-trade order pad). orders()/cancel_order() are new
— Dhan order-book polling has no prior implementation anywhere in this codebase to lift
from, so the field mapping below is best-effort against Dhan's documented v2 API shape
and should get one manual smoke-test pass (place a small live order, confirm orders()
reports its fill correctly) before any live strategy is allowed to rely on it unattended.
"""

import logging

import requests

log = logging.getLogger(__name__)

_DHAN_API_BASE = 'https://api.dhan.co/v2'

_ORDER_TYPE_TO_DHAN = {'LIMIT': 'LIMIT', 'MARKET': 'MARKET', 'SL': 'STOP_LOSS', 'SL-M': 'STOP_LOSS_MARKET'}

# Dhan orderStatus → Kite-shaped status used by poll_pending_order_fills()/live_order_manager.py.
_DHAN_STATUS_TO_KITE = {
    'TRADED': 'COMPLETE',
    'REJECTED': 'REJECTED',
    'CANCELLED': 'CANCELLED',
    'EXPIRED': 'CANCELLED',
    'PART_TRADED': 'OPEN',
    'PENDING': 'OPEN',
    'TRANSIT': 'OPEN',
}


class DhanAdapter:
    def __init__(self, db, client_id: str, access_token: str):
        self.db = db
        self.client_id = client_id
        self.access_token = access_token

    def _headers(self) -> dict:
        return {
            'access-token': self.access_token,
            'client-id': self.client_id,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

    def _resolve_security(self, tradingsymbol: str, exchange: str) -> tuple[str, str]:
        doc = self.db._db['active_option_tokens'].find_one({'broker': 'dhan', 'symbol': tradingsymbol}) or {}
        security_id = str(doc.get('token') or doc.get('tokens') or '').strip()
        exchange_segment = str(doc.get('ws_segment') or '').strip().upper()
        if not exchange_segment:
            exchange_segment = 'BSE_FNO' if str(exchange or '').upper() == 'BSE' else 'NSE_FNO'
        return security_id, exchange_segment

    # ── place_order ──────────────────────────────────────────────────────────

    def place_order(
        self,
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
    ) -> str:
        security_id, exchange_segment = self._resolve_security(tradingsymbol, exchange)
        if not security_id:
            raise Exception(f'Dhan security_id not found for symbol={tradingsymbol}')

        dhan_order_type = _ORDER_TYPE_TO_DHAN.get(order_type, 'LIMIT')
        payload: dict = {
            'dhanClientId': self.client_id,
            'transactionType': 'BUY' if transaction_type == 'BUY' else 'SELL',
            'exchangeSegment': exchange_segment,
            'productType': 'INTRADAY' if product == 'MIS' else 'MARGIN',
            'orderType': dhan_order_type,
            'validity': str(validity or 'DAY').upper(),
            'tradingSymbol': tradingsymbol,
            'securityId': security_id,
            'quantity': int(quantity),
            'price': round(float(price or 0), 2) if dhan_order_type in ('LIMIT', 'STOP_LOSS') else 0,
        }
        if dhan_order_type in ('STOP_LOSS', 'STOP_LOSS_MARKET') and trigger_price:
            payload['triggerPrice'] = round(float(trigger_price), 2)

        resp = requests.post(f'{_DHAN_API_BASE}/orders', json=payload, headers=self._headers(), timeout=10)
        if resp.status_code not in (200, 201):
            raise Exception(f'Dhan PlaceOrder failed: HTTP {resp.status_code} {resp.text[:300]}')
        data = resp.json() if resp.text else {}
        return str(data.get('orderId') or data.get('order_id') or '')

    # ── cancel_order ─────────────────────────────────────────────────────────

    def cancel_order(self, variety: str = 'regular', order_id: str = '') -> str:
        resp = requests.delete(f'{_DHAN_API_BASE}/orders/{order_id}', headers=self._headers(), timeout=10)
        if resp.status_code not in (200, 202):
            raise Exception(f'Dhan CancelOrder failed: HTTP {resp.status_code} {resp.text[:300]}')
        return order_id

    # ── orders ───────────────────────────────────────────────────────────────

    def orders(self) -> list:
        """Return order book as list of Kite-shaped dicts. Unverified — see module docstring."""
        try:
            resp = requests.get(f'{_DHAN_API_BASE}/orders', headers=self._headers(), timeout=10)
        except Exception as exc:
            log.error('Dhan orders() request error: %s', exc)
            return []
        if resp.status_code != 200:
            log.warning('Dhan orders() non-200 status=%s body=%s', resp.status_code, resp.text[:300])
            return []
        try:
            rows = resp.json() if resp.text else []
        except Exception:
            return []
        if not isinstance(rows, list):
            return []

        out = []
        for o in rows:
            if not isinstance(o, dict):
                continue
            raw_status = str(o.get('orderStatus') or '').strip().upper()
            out.append({
                'order_id': str(o.get('orderId') or ''),
                'status': _DHAN_STATUS_TO_KITE.get(raw_status, 'OPEN'),
                'average_price': float(o.get('averageTradedPrice') or 0),
                'price': float(o.get('price') or 0),
                'trigger_price': float(o.get('triggerPrice') or 0),
                'filled_quantity': int(o.get('filledQty') or 0),
                'quantity': int(o.get('quantity') or 0),
                'tradingsymbol': str(o.get('tradingSymbol') or ''),
                'exchange': str(o.get('exchangeSegment') or ''),
                'transaction_type': str(o.get('transactionType') or ''),
                'product': 'MIS' if str(o.get('productType') or '').upper() == 'INTRADAY' else 'NRML',
                'last_price': float(o.get('averageTradedPrice') or o.get('price') or 0),
                'status_message': str(o.get('omsErrorDescription') or ''),
                'status_message_raw': str(o.get('omsErrorDescription') or ''),
            })
        return out

    # ── quote ────────────────────────────────────────────────────────────────

    def quote(self, symbols: list) -> dict:
        """
        Kite-shaped quote dict, last_price only — Dhan's quote feed isn't pulled for
        bid/ask depth in this codebase yet, so depth comes back empty. Callers
        (_get_bid_ask / _get_aggressive_exit_price in live_order_manager.py) already
        fall back to last_price when depth is empty, so this degrades safely.
        """
        from features.broker_gateway import get_broker_rest_quotes

        token_map: dict[str, str] = {}
        for sym_key in symbols or []:
            parts = sym_key.split(':', 1)
            tsym = parts[1] if len(parts) == 2 else parts[0]
            doc = self.db._db['active_option_tokens'].find_one({'broker': 'dhan', 'symbol': tsym}) or {}
            token = str(doc.get('token') or '').strip()
            if token:
                token_map[token] = sym_key
        if not token_map:
            return {}

        quotes = get_broker_rest_quotes(list(token_map.keys()), self.db._db)
        result: dict = {}
        for token, sym_key in token_map.items():
            q = quotes.get(token) or {}
            lp = float(q.get('ltp') or 0)
            result[sym_key] = {
                'last_price': lp,
                'upper_circuit': 0.0,
                'lower_circuit': 0.0,
                'depth': {'buy': [], 'sell': []},
            }
        return result


def get_dhan_instance(db, client_id: str, access_token: str) -> DhanAdapter | None:
    if not client_id or not access_token:
        return None
    return DhanAdapter(db, client_id, access_token)


def _is_dhan_doc(doc: dict) -> bool:
    """Return True if a broker_configuration doc belongs to Dhan.

    Mirrors flattrade_broker._is_flattrade_doc — without this, get_broker_for_trade
    falls through to its Kite branch for any non-flattrade doc, so a per-trade
    Dhan account gets wrapped as a Kite client with a Dhan-format access_token
    (wrong auth entirely) and every order placement attempt fails.
    """
    name = str(doc.get("name") or "").lower()
    icon = str(doc.get("broker_icon") or "").lower()
    return "dhan" in name or "dhan" in icon
