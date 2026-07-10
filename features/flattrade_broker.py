"""
flattrade_broker.py
───────────────────
FlatTrade REST API adapter.

Provides a KiteConnect-compatible interface so live_order_manager.py
can use either Kite or FlatTrade without any extra branching.

broker_configuration document for FlatTrade:
  {
    "name":         "Broker.FlatTrade",
    "broker_icon":  "flattrade.svg",
    "broker_type":  "live",
    "user_id":      "<FlatTrade client ID>",
    "access_token": "<jKey session token>",
  }

Login flow:
  1. GET  /broker/flattrade/login?broker_doc_id=<id>
         → redirect to https://auth.flattrade.in/?app_key=<API_KEY>&state=<session_id>
  2. FlatTrade redirects back with ?code=<request_code>&state=<session_id>
  3. GET  /broker/flattrade/redirect?code=<code>&state=<session_id>
         → exchange request_code for jKey → save to broker_configuration
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_ROOT / ".env")
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

log = logging.getLogger(__name__)

FLATTRADE_API_KEY    = os.getenv("FLATTRADE_API_KEY", "").strip()
FLATTRADE_API_SECRET = os.getenv("FLATTRADE_API_SECRET", "").strip()

_AUTH_URL    = "https://auth.flattrade.in/"
_TOKEN_URL   = "https://authapi.flattrade.in/trade/apitoken"
_BASE_URL    = "https://piconnect.flattrade.in/PiConnectAPI"


# ── Symbol conversion ─────────────────────────────────────────────────────────

def _to_flattrade_symbol(kite_symbol: str, exchange: str) -> str:
    """
    Convert Kite trading symbol to FlatTrade symbol format.

    Kite  : NIFTY26APR23850CE  →  {name}{YY}{MMM}{strike}{CE|PE}
    FlatTrade: NIFTY28APR26C23850  →  {name}{DD}{MMM}{YY}{C|P}{strike}
    """
    sym = str(kite_symbol or '').strip()
    if not sym or str(exchange or '').upper() not in ('NFO', 'BFO'):
        return sym

    if sym.endswith('CE'):
        ft_type = 'C'
    elif sym.endswith('PE'):
        ft_type = 'P'
    else:
        return sym  # equity / index — no conversion needed

    try:
        from features.spot_atm_utils import _load_kite_instruments  # type: ignore
        from datetime import datetime as _dt

        cache = _load_kite_instruments()
        for (_name, exp_str, _strike, _opt), inst in cache.items():
            if str(inst.get('symbol') or '').strip() == sym:
                dt = _dt.strptime(exp_str[:10], '%Y-%m-%d')
                day = dt.strftime('%d')
                mon = dt.strftime('%b').upper()
                yr  = dt.strftime('%y')
                strike_int = int(_strike) if float(_strike).is_integer() else _strike
                ft_sym = f'{_name}{day}{mon}{yr}{ft_type}{strike_int}'
                print(f'[FLATTRADE SYMBOL] kite={sym} → flattrade={ft_sym}')
                return ft_sym
    except Exception as exc:
        log.warning('[FLATTRADE SYMBOL] conversion error symbol=%s: %s', sym, exc)

    log.warning('[FLATTRADE SYMBOL] not found in kite instruments cache: %s — using as-is', sym)
    return sym


_TSYM_OPTION_RE = re.compile(r'^([A-Z]+)(\d{2})([A-Z]{3})(\d{2})([CP])(\d+(?:\.\d+)?)$')


def parse_flattrade_tsym(tsym: str) -> dict | None:
    """
    Inverse of _to_flattrade_symbol — pulls FlatTrade's options tsym format
    "{NAME}{DD}{MMM}{YY}{C|P}{strike}" (e.g. "NIFTY28APR26C23850") back into
    {underlying, expiry: "YYYY-MM-DD", strike, option_type: "CE"|"PE"}.

    Returns None for anything that isn't an options contract in this exact
    shape (futures, equity, unrecognized) so callers can skip it safely.
    """
    from datetime import datetime as _dt

    match = _TSYM_OPTION_RE.match(str(tsym or '').strip().upper())
    if not match:
        return None
    name, day, mon, yr, cp, strike = match.groups()
    try:
        expiry = _dt.strptime(f'{day}{mon}{yr}', '%d%b%y').strftime('%Y-%m-%d')
    except ValueError:
        return None
    return {
        'underlying': name,
        'expiry': expiry,
        'strike': float(strike),
        'option_type': 'CE' if cp == 'C' else 'PE',
    }


# ── Auth helpers ──────────────────────────────────────────────────────────────

def get_login_url(state: str = "", api_key: str = "") -> str:
    key = api_key or FLATTRADE_API_KEY
    if not key:
        log.error("FlatTrade login URL requested but api_key is missing")
    url = f"{_AUTH_URL}?app_key={key}"
    if state:
        url += f"&state={state}"
    return url


def _session_token(session: dict) -> str:
    return str(
        session.get("token")
        or session.get("access_token")
        or session.get("jKey")
        or session.get("jkey")
        or session.get("susertoken")
        or ""
    ).strip()


def _session_user_id(session: dict) -> str:
    return str(
        session.get("clientid")
        or session.get("uid")
        or session.get("actid")
        or session.get("user_id")
        or session.get("client")
        or ""
    ).strip()


def generate_session(request_code: str, api_key: str = "", api_secret: str = "") -> dict:
    """Exchange request_code for jKey/session token."""
    key    = api_key    or FLATTRADE_API_KEY
    secret = api_secret or FLATTRADE_API_SECRET
    if not key or not secret:
        raise ValueError("FlatTrade api_key / api_secret missing (set in broker configuration or .env)")

    checksum = hashlib.sha256(f"{key}{request_code}{secret}".encode()).hexdigest()
    resp = requests.post(
        _TOKEN_URL,
        json={"api_key": key, "request_code": request_code, "api_secret": checksum},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("stat") == "Not_Ok" or not _session_token(data):
        log.error("FlatTrade token exchange failed: %s", data.get("emsg", data))
        raise ValueError(f"FlatTrade session error: {data.get('emsg', data)}")
    return data


def save_flattrade_session(db, broker_doc_id: str, session: dict) -> None:
    """Persist jKey and login time into broker_configuration."""
    from bson import ObjectId
    from datetime import datetime, timezone
    token = _session_token(session)
    user_id = _session_user_id(session)
    if not token:
        raise ValueError(f"FlatTrade login response did not include a session token. Keys: {sorted(session.keys())}")
    db["broker_configuration"].update_one(
        {"_id": ObjectId(broker_doc_id)},
        {"$set": {
            "access_token": token,
            "user_id":      user_id,
            "user_name":    user_id,
            "login_time":   datetime.now(timezone.utc).isoformat(),
        }},
    )


def get_stored_access_token(db, broker_doc_id: str) -> str | None:
    from bson import ObjectId
    doc = db["broker_configuration"].find_one(
        {"_id": ObjectId(broker_doc_id)},
        {"access_token": 1},
    )
    return (doc or {}).get("access_token")


def validate_session(user_id: str, access_token: str) -> tuple[bool, str]:
    """
    Validate a FlatTrade session token with a lightweight authenticated call.
    """
    adapter = get_flattrade_instance(user_id=user_id, access_token=access_token)
    if adapter is None:
        return False, "FlatTrade user_id or access_token missing"

    try:
        result = adapter._post("OrderBook", {"uid": adapter.user_id})
        if isinstance(result, dict) and str(result.get("stat") or "").strip().lower() == "not_ok":
            return False, str(result.get("emsg") or "FlatTrade session invalid")
        return True, "FlatTrade session valid"
    except Exception as exc:
        return False, str(exc)


# ── FlatTrade adapter ─────────────────────────────────────────────────────────

class FlatTradeAdapter:
    """
    Wraps FlatTrade Noren REST API with a KiteConnect-compatible surface:
      place_order(**params) → order_id str
      orders()              → list[dict] in Kite field names
      cancel_order(variety, order_id)
      quote(symbols)        → dict in Kite depth format
    """

    def __init__(self, user_id: str, jkey: str):
        self.user_id = user_id
        self.jkey    = jkey

    def _post(self, endpoint: str, data: dict) -> object:
        url = f"{_BASE_URL}/{endpoint}"
        payload_data = dict(data)
        if payload_data.get("tsym"):
            payload_data["tsym"] = quote(str(payload_data["tsym"]), safe="")
        body = f"jData={json.dumps(payload_data, separators=(',', ':'))}&jKey={self.jkey}"
        print(f'[FLATTRADE RAW REQUEST] endpoint={endpoint} jData={json.dumps(payload_data, indent=2)}')
        resp = requests.post(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            raise requests.HTTPError(
                f"{exc}; FlatTrade response: {resp.text[:500]}"
            ) from exc
        return resp.json()

    # ── place_order ──────────────────────────────────────────────────────────

    def place_order(
        self,
        tradingsymbol: str,
        exchange: str,
        transaction_type: str,      # 'BUY' / 'SELL'
        quantity: int,
        order_type: str,             # 'LIMIT' / 'MARKET' / 'SL' / 'SL-M'
        product: str,                # 'NRML' / 'MIS'
        variety: str = "regular",    # ignored — FlatTrade has no variety concept
        price: float = 0.0,
        trigger_price: float = 0.0,
        validity: str = "DAY",
    ) -> str:
        _prctyp = {
            "LIMIT":   "LMT",
            "MARKET":  "MKT",
            "SL":      "SL-LMT",
            "SL-M":    "SL-MKT",
        }.get(order_type, "LMT")

        ft_symbol = _to_flattrade_symbol(tradingsymbol, exchange)

        body: dict = {
            "uid":         self.user_id,
            "actid":       self.user_id,
            "exch":        exchange,
            "tsym":        ft_symbol,
            "qty":         str(int(quantity)),
            "prc":         str(round(float(price or 0), 2)),
            "dscqty":      "0",
            "prd":         "I" if product == "MIS" else "M",
            "trantype":    "B" if transaction_type == "BUY" else "S",
            "prctyp":      _prctyp,
            "ret":         str(validity or "DAY").upper(),
            "ordersource": "API",
        }
        if _prctyp in ("SL-LMT", "SL-MKT") and trigger_price:
            body["trgprc"] = str(round(float(trigger_price), 2))

        result = self._post("PlaceOrder", body)
        if not isinstance(result, dict) or result.get("stat") != "Ok":
            raise Exception(
                f"FlatTrade PlaceOrder failed: {(result or {}).get('emsg', result)}"
            )
        return str(result.get("norenordno") or "")

    # ── orders ───────────────────────────────────────────────────────────────

    def orders(self) -> list:
        """Return order book as list of Kite-shaped dicts."""
        result = self._post("OrderBook", {"uid": self.user_id})
        if not isinstance(result, list):
            return []

        _status_map = {
            "COMPLETE":        "COMPLETE",
            "OPEN":            "OPEN",
            "TRIGGER_PENDING": "TRIGGER_PENDING",
            "REJECTED":        "REJECTED",
            "CANCELLED":       "CANCELLED",
        }
        out = []
        for o in result:
            raw_status = str(o.get("status") or "").upper()
            out.append({
                "order_id":           str(o.get("norenordno") or ""),
                "status":             _status_map.get(raw_status, raw_status),
                "average_price":      float(o.get("avgprc") or o.get("flprc") or 0),
                "price":              float(o.get("prc") or 0),
                "trigger_price":      float(o.get("trgprc") or 0),
                "filled_quantity":    int(o.get("fillshares") or 0),
                "quantity":           int(o.get("qty") or 0),
                "tradingsymbol":      str(o.get("tsym") or ""),
                "exchange":           str(o.get("exch") or ""),
                "transaction_type":   "BUY" if o.get("trantype") == "B" else "SELL",
                "product":            "MIS" if o.get("prd") == "I" else "NRML",
                "last_price":         float(o.get("lp") or 0),
                "status_message":     str(o.get("rejreason") or ""),
                "status_message_raw": str(o.get("rejreason") or ""),
            })
        return out

    # ── modify_order ─────────────────────────────────────────────────────────

    def modify_order(
        self,
        variety: str = "regular",   # noqa: ignored — Flattrade has no variety concept
        order_id: str = "",
        order_type: str = "LIMIT",
        quantity: int | None = None,
        price: float = 0.0,
        trigger_price: float = 0.0,
        validity: str = "DAY",
        exchange: str = "",
        tradingsymbol: str = "",
    ) -> str:
        _prctyp = {
            "LIMIT": "LMT",
            "MARKET": "MKT",
            "SL":     "SL-LMT",
            "SL-M":   "SL-MKT",
        }.get(order_type, "LMT")

        ft_symbol = _to_flattrade_symbol(tradingsymbol, exchange) if tradingsymbol and exchange else tradingsymbol

        body: dict = {
            "norenordno": order_id,
            "uid":        self.user_id,
            "exch":       str(exchange or "NFO").upper(),
            "tsym":       ft_symbol,
            "prc":        str(round(float(price or 0), 2)),
            "prctyp":     _prctyp,
            "ret":        str(validity or "DAY").upper(),
        }
        if quantity is not None:
            body["qty"] = str(int(quantity))
        if _prctyp in ("SL-LMT", "SL-MKT") and trigger_price:
            body["trgprc"] = str(round(float(trigger_price), 2))

        result = self._post("ModifyOrder", body)
        if not isinstance(result, dict) or result.get("stat") != "Ok":
            raise Exception(
                f"FlatTrade ModifyOrder failed: {(result or {}).get('emsg', result)}"
            )
        return str(result.get("result") or order_id)

    # ── positions ────────────────────────────────────────────────────────────

    def positions(self) -> list:
        """Return net positions as list of Kite-shaped dicts.
        qty is negative for short (SELL) positions, positive for long (BUY).
        """
        result = self._post("PositionBook", {"uid": self.user_id, "actid": self.user_id})
        if not isinstance(result, list):
            return []
        out = []
        for p in result:
            try:
                net_qty = int(float(p.get("netqty") or "0"))
            except (ValueError, TypeError):
                net_qty = 0
            out.append({
                "tradingsymbol": str(p.get("tsym") or ""),
                "exchange":      str(p.get("exch") or ""),
                "quantity":      net_qty,
                "average_price": float(p.get("netavgprc") or 0),
                "last_price":    float(p.get("lp") or 0),
                "product":       "MIS" if p.get("prd") == "I" else "NRML",
            })
        return out

    def raw_positions(self) -> list:
        """
        Same PositionBook call as positions(), but returns FlatTrade's rows
        unmodified instead of collapsing them into the lossy Kite-shaped dict
        above — callers that need native fields (tsym, netqty, netavgprc, lp,
        exch, prd) for cross-broker merging should use this instead.
        """
        result = self._post("PositionBook", {"uid": self.user_id, "actid": self.user_id})
        if isinstance(result, list):
            return result
        if isinstance(result, dict) and str(result.get("stat") or "").strip().lower() == "not_ok":
            # An empty PositionBook also comes back in this same {stat: Not_Ok}
            # shape (e.g. "no data"), so only raise when emsg actually points at
            # a session/auth problem — anything else is tolerated as "no
            # positions", same as the list branch above.
            emsg = str(result.get("emsg") or "").strip()
            if "session" in emsg.lower() or "invalid" in emsg.lower() or "login" in emsg.lower() or "token" in emsg.lower():
                raise Exception(emsg or "FlatTrade session invalid")
        return []

    # ── cancel_order ─────────────────────────────────────────────────────────

    def cancel_order(self, variety: str = "regular", order_id: str = "") -> str:
        result = self._post("CancelOrder", {
            "norenordno": order_id,
            "uid":        self.user_id,
        })
        if not isinstance(result, dict) or result.get("stat") != "Ok":
            raise Exception(
                f"FlatTrade CancelOrder failed: {(result or {}).get('emsg', result)}"
            )
        return str(result.get("result") or order_id)

    # ── quote ────────────────────────────────────────────────────────────────

    def quote(self, symbols: list) -> dict:
        """
        Get depth/bid-ask for a list of 'EXCHANGE:SYMBOL' strings.
        Returns Kite-compatible dict.

        FlatTrade GetQuotes works by tsym (tradingsymbol). Returns bp1/sp1
        as best bid/ask prices.
        """
        result = {}
        for sym_key in (symbols or []):
            parts = sym_key.split(":", 1)
            exch  = parts[0] if len(parts) == 2 else "NFO"
            tsym  = parts[1] if len(parts) == 2 else parts[0]
            try:
                q = self._post("GetQuotes", {
                    "uid":  self.user_id,
                    "exch": exch,
                    "tsym": tsym,
                })
                if isinstance(q, dict) and q.get("stat") == "Ok":
                    bp = float(q.get("bp1") or 0)
                    sp = float(q.get("sp1") or 0)
                    lp = float(q.get("lp")  or 0)
                    uc = float(q.get("uc")  or 0)
                    lc = float(q.get("lc")  or 0)
                    result[sym_key] = {
                        "last_price":    lp,
                        "upper_circuit": uc,
                        "lower_circuit": lc,
                        "depth": {
                            "buy":  [{"price": bp, "quantity": int(q.get("bq1") or 0)}],
                            "sell": [{"price": sp, "quantity": int(q.get("sq1") or 0)}],
                        },
                    }
            except Exception as exc:
                log.debug("FlatTrade quote error sym=%s: %s", sym_key, exc)
        return result


# ── Factory ───────────────────────────────────────────────────────────────────

def get_flattrade_instance(user_id: str, access_token: str) -> FlatTradeAdapter | None:
    if not user_id or not access_token:
        return None
    return FlatTradeAdapter(user_id=user_id, jkey=access_token)


def _is_flattrade_doc(doc: dict) -> bool:
    """Return True if broker_configuration doc belongs to FlatTrade.
    Uses name only — icon field can be wrong in DB (both may share same icon).
    """
    name = str(doc.get("name") or "").lower()
    return "flattrade" in name
