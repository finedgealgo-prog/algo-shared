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
import time

import requests

log = logging.getLogger(__name__)

_DHAN_API_BASE = 'https://api.dhan.co/v2'

# Module-level, process-wide session — every place_order()/orders()/etc. call reuses its
# connection pool instead of doing a fresh TCP+TLS handshake to api.dhan.co per request
# (the plain `requests.post(...)` module-level calls this replaced each opened/closed
# their own connection). Handshake overhead is a meaningful slice of order-placement
# latency on a REST call this size, and DhanAdapter itself is re-instantiated per request
# (see get_dhan_instance) so only a module-level session — not a per-instance one — persists
# connections across separate order placements.
_session = requests.Session()

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

# Dhan orderType → display label for the Orderbook UI.
_DHAN_ORDER_TYPE_DISPLAY = {
    'LIMIT': 'Limit',
    'MARKET': 'Market',
    'STOP_LOSS': 'SL',
    'STOP_LOSS_MARKET': 'SL-M',
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
        security_id: str = '',
        exchange_segment: str = '',
    ) -> str:
        # Prefer a security_id/exchange_segment the caller already resolved unambiguously
        # (by strike+expiry+option_type) over re-deriving it here from tradingsymbol alone —
        # Dhan's own tradingSymbol convention (e.g. "NIFTY-Jul2026-24200-PE") doesn't encode
        # which week's contract it is, so active_option_tokens has one row per weekly expiry
        # all sharing that identical symbol string, and _resolve_security's symbol-only lookup
        # can return any one of them — including an already-expired week's security_id.
        if not security_id or not exchange_segment:
            security_id, exchange_segment = self._resolve_security(tradingsymbol, exchange)
        if not security_id:
            raise Exception(f'Dhan security_id not found for symbol={tradingsymbol}')

        dhan_order_type = _ORDER_TYPE_TO_DHAN.get(order_type, 'LIMIT')
        # Fields always present in Dhan's own documented sample payload — disclosedQuantity,
        # triggerPrice, and afterMarketOrder were previously omitted/conditional here (triggerPrice
        # only sent for SL orders), unlike Dhan's sample which always sends all three (0/false when
        # not applicable). DH-905 is a generic Input_Exception bucket ("missing required fields, bad
        # values for parameters etc.") per Dhan's own docs, so a request shape gap here can plausibly
        # surface as an unrelated-looking error (e.g. "Invalid SecurityId") instead of naming the
        # actually-missing field.
        payload: dict = {
            'dhanClientId': self.client_id,
            'correlationId': f'order_{int(time.time() * 1000)}',
            'transactionType': 'BUY' if transaction_type == 'BUY' else 'SELL',
            'exchangeSegment': exchange_segment,
            'productType': 'INTRADAY' if product == 'MIS' else 'MARGIN',
            'orderType': dhan_order_type,
            'validity': str(validity or 'DAY').upper(),
            'securityId': security_id,
            'quantity': int(quantity),
            'disclosedQuantity': 0,
            'price': round(float(price or 0), 2) if dhan_order_type in ('LIMIT', 'STOP_LOSS') else 0,
            'triggerPrice': round(float(trigger_price or 0), 2),
            'afterMarketOrder': False,
        }
        print(f'[DHAN PLACE_ORDER] payload={payload}', flush=True)

        resp = _session.post(f'{_DHAN_API_BASE}/orders', json=payload, headers=self._headers(), timeout=10)
        print(f'[DHAN PLACE_ORDER] response status={resp.status_code} body={resp.text[:500]}', flush=True)
        if resp.status_code not in (200, 201):
            raise Exception(f'Dhan PlaceOrder failed: HTTP {resp.status_code} {resp.text[:300]}')
        data = resp.json() if resp.text else {}
        return str(data.get('orderId') or data.get('order_id') or '')

    # ── cancel_order ─────────────────────────────────────────────────────────

    def cancel_order(self, variety: str = 'regular', order_id: str = '') -> str:
        resp = _session.delete(f'{_DHAN_API_BASE}/orders/{order_id}', headers=self._headers(), timeout=10)
        if resp.status_code not in (200, 202):
            raise Exception(f'Dhan CancelOrder failed: HTTP {resp.status_code} {resp.text[:300]}')
        return order_id

    # ── modify_order ─────────────────────────────────────────────────────────
    # Was missing entirely — broker_modify_order (api.py) called this for every
    # broker generically; on Dhan it hit AttributeError instead of modifying
    # anything. exchange/tradingsymbol accepted only to match the shared
    # adapter signature (FlatTradeAdapter.modify_order) — Dhan's PUT /orders/
    # {id} identifies the order purely by orderId, no symbol needed.

    def modify_order(
        self,
        variety: str = 'regular',   # noqa: ignored — Dhan has no variety concept
        order_id: str = '',
        order_type: str = 'LIMIT',
        quantity: int | None = None,
        price: float = 0.0,
        trigger_price: float = 0.0,
        validity: str = 'DAY',
        exchange: str = '',
        tradingsymbol: str = '',
    ) -> str:
        dhan_order_type = _ORDER_TYPE_TO_DHAN.get(order_type, 'LIMIT')
        payload: dict = {
            'dhanClientId': self.client_id,
            'orderId': str(order_id),
            'orderType': dhan_order_type,
            'validity': str(validity or 'DAY').upper(),
            'price': round(float(price or 0), 2) if dhan_order_type in ('LIMIT', 'STOP_LOSS') else 0,
            'triggerPrice': round(float(trigger_price or 0), 2),
            'disclosedQuantity': 0,
        }
        if quantity is not None:
            payload['quantity'] = int(quantity)
        resp = _session.put(f'{_DHAN_API_BASE}/orders/{order_id}', json=payload, headers=self._headers(), timeout=10)
        if resp.status_code not in (200, 202):
            raise Exception(f'Dhan ModifyOrder failed: HTTP {resp.status_code} {resp.text[:300]}')
        data = resp.json() if resp.text else {}
        return str(data.get('orderId') or order_id)

    # ── orders ───────────────────────────────────────────────────────────────

    def orders(self) -> list:
        """Return order book as list of Kite-shaped dicts. Unverified — see module docstring."""
        try:
            resp = _session.get(f'{_DHAN_API_BASE}/orders', headers=self._headers(), timeout=10)
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
                # security_id + exchange (already exchangeSegment above) are what
                # /trade/positions/repeat-order needs to re-place this exact contract
                # via Dhan's live feed (see _fetch_dhan_market_data) without re-parsing
                # underlying/expiry/strike out of tradingSymbol — Dhan's own tradingSymbol
                # doesn't reliably encode which week's contract it is (see _resolve_security's
                # docstring above), but securityId always identifies the exact instrument.
                'security_id': str(o.get('securityId') or ''),
                'transaction_type': str(o.get('transactionType') or ''),
                'product': 'MIS' if str(o.get('productType') or '').upper() == 'INTRADAY' else 'NRML',
                'last_price': float(o.get('averageTradedPrice') or o.get('price') or 0),
                'status_message': str(o.get('omsErrorDescription') or ''),
                'status_message_raw': str(o.get('omsErrorDescription') or ''),
                'order_type': _DHAN_ORDER_TYPE_DISPLAY.get(str(o.get('orderType') or '').upper(), str(o.get('orderType') or '')),
                # createTime is Dhan's "order placed at" timestamp (documented format
                # "YYYY-MM-DD HH:MM:SS"); updateTime is its last-activity fallback for
                # older orders whose createTime Dhan sometimes omits.
                'order_time': str(o.get('createTime') or o.get('updateTime') or ''),
            })
        return out

    # ── quote ────────────────────────────────────────────────────────────────

    def quote(self, symbols: list) -> dict:
        """
        Kite-shaped quote dict with REAL depth (price+quantity per level), not just
        last_price. Previously always returned 'depth': {'buy': [], 'sell': []} — every
        MPP order for a Dhan-executed live/Advanced-execution leg (_get_bid_ask/
        _get_market_depth/_walk_depth_for_qty in live_order_manager.py, all broker-agnostic
        and unchanged, they just call broker.quote()) silently aborted with "no live depth,
        order NOT placed" because of that stub, even though the same walked-depth+quantity
        pricing already worked for Dhan via the manual Order Pad flow
        (_resolve_mpp_price/_fetch_dhan_market_data in algo.simulator/api.py and
        algo.trade/api.py) — this ports that same WS-first/REST-fallback depth fetch here so
        the live/webhook execution path gets it too, without touching either of those.

        WS-first (dhan_ticker_manager.bid_map/ask_map/bid_qty_map/ask_qty_map — instant,
        no REST, but only ever level-0/best price+qty, same limit the WS binary parser has
        everywhere else in this codebase). REST fallback (/marketfeed/quote, rate-gated via
        dhan_quote_post_blocking) for anything missing from WS — returns the full depth
        Dhan sends (typically 5 levels each side), letting _walk_depth_for_qty walk past
        level 0 for a quantity bigger than the top level alone holds.
        """
        from features.broker_gateway import dhan_quote_post_blocking
        from features.dhan_ticker import dhan_ticker_manager as _dtm

        # sym_key ("EXCH:TRADINGSYMBOL") -> (security_id, exchange_segment)
        resolved: dict[str, tuple[str, str]] = {}
        for sym_key in symbols or []:
            parts = sym_key.split(':', 1)
            exch, tsym = (parts[0], parts[1]) if len(parts) == 2 else ('', parts[0])
            doc = self.db._db['active_option_tokens'].find_one({'broker': 'dhan', 'symbol': tsym}) or {}
            token = str(doc.get('token') or doc.get('tokens') or '').strip()
            if not token:
                continue
            segment = str(doc.get('ws_segment') or '').strip().upper() or ('BSE_FNO' if exch.upper() == 'BSE' else 'NSE_FNO')
            resolved[sym_key] = (token, segment)
        if not resolved:
            return {}

        result: dict = {}
        missing_by_segment: dict[str, list[int]] = {}
        for sym_key, (token, segment) in resolved.items():
            ws_bid = float(_dtm.bid_map.get(token) or 0)
            ws_ask = float(_dtm.ask_map.get(token) or 0)
            if ws_bid > 0 or ws_ask > 0:
                result[sym_key] = {
                    'last_price': float(_dtm.ltp_map.get(token) or 0),
                    'upper_circuit': 0.0,
                    'lower_circuit': 0.0,
                    'depth': {
                        'buy':  [{'price': ws_bid, 'quantity': int(_dtm.bid_qty_map.get(token) or 0)}] if ws_bid > 0 else [],
                        'sell': [{'price': ws_ask, 'quantity': int(_dtm.ask_qty_map.get(token) or 0)}] if ws_ask > 0 else [],
                    },
                }
            else:
                try:
                    missing_by_segment.setdefault(segment, []).append(int(token))
                except ValueError:
                    continue

        if missing_by_segment:
            try:
                resp = dhan_quote_post_blocking(missing_by_segment, self.access_token, self.client_id, timeout=10.0)
                if resp is not None and resp.status_code == 200:
                    raw = resp.json()
                    data = raw.get('data') or raw
                    token_to_sym_key = {tok: sk for sk, (tok, _seg) in resolved.items()}
                    for segment_data in data.values():
                        if not isinstance(segment_data, dict):
                            continue
                        for tok, info in segment_data.items():
                            sym_key = token_to_sym_key.get(str(tok))
                            if not sym_key or not isinstance(info, dict):
                                continue
                            depth = info.get('depth') or {}

                            def _levels(raw_levels: list) -> list[dict]:
                                return [
                                    {'price': float(lvl.get('price') or 0), 'quantity': int(lvl.get('quantity') or 0)}
                                    for lvl in (raw_levels or []) if float(lvl.get('price') or 0) > 0
                                ]

                            result[sym_key] = {
                                'last_price': float(info.get('last_price') or 0),
                                'upper_circuit': 0.0,
                                'lower_circuit': 0.0,
                                'depth': {'buy': _levels(depth.get('buy')), 'sell': _levels(depth.get('sell'))},
                            }
            except Exception as exc:
                log.warning('[DHAN QUOTE] depth REST fetch failed: %s', exc)

        # Any symbol still unresolved (WS empty AND REST missed/failed) gets an
        # empty-depth entry — callers already treat that as "no live depth", not a KeyError.
        for sym_key in resolved:
            result.setdefault(sym_key, {'last_price': 0.0, 'upper_circuit': 0.0, 'lower_circuit': 0.0, 'depth': {'buy': [], 'sell': []}})
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
