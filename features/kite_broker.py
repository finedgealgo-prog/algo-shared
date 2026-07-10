"""
kite_broker.py
──────────────
Kite Connect broker integration.

Login flow:
  1. GET  /broker/kite/login-url  → redirect user to Zerodha login page
  2. Zerodha redirects back with ?request_token=xxx
  3. POST /broker/kite/callback   → exchange request_token for access_token
  4. access_token stored in MongoDB broker_configuration collection
"""

from __future__ import annotations

import os
import hashlib
from datetime import datetime, timezone

from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()

KITE_API_KEY    = os.getenv("KITE_API_KEY", "")
KITE_API_SECRET = os.getenv("KITE_API_SECRET", "")


def get_kite_instance(access_token: str = None) -> KiteConnect:
    kite = KiteConnect(api_key=KITE_API_KEY)
    if access_token:
        kite.set_access_token(access_token)
    return kite


def get_login_url() -> str:
    kite = get_kite_instance()
    return kite.login_url()


def generate_session(request_token: str) -> dict:
    """Exchange request_token for access_token. Returns session dict."""
    kite = get_kite_instance()
    session = kite.generate_session(request_token, api_secret=KITE_API_SECRET)
    return session


def sync_kite_access_token_by_credentials(
    db, api_key: str, api_secret: str, access_token: str, login_time: str,
    *, skip_collection: str = "",
) -> None:
    """
    Mirror a freshly-issued Kite access_token into every OTHER store that
    holds this same (api_key, api_secret) pair.

    `broker_configuration` (trade execution, used by the Positions page /
    live order placement) and `kite_market_config` (market-data feed) each
    keep their own independent copy of a Kite login. When both happen to be
    registered with the same Zerodha api_key+api_secret, they describe the
    same account — without this, logging in through only one flow leaves the
    other one's access_token silently stale until its own next login.
    `skip_collection` is the collection the caller already wrote directly,
    so it's not redundantly re-matched against itself.
    """
    api_key = str(api_key or "").strip()
    api_secret = str(api_secret or "").strip()
    if not api_key or not api_secret or not access_token:
        return
    set_fields = {"access_token": access_token, "login_time": login_time}
    if skip_collection != "broker_configuration":
        db["broker_configuration"].update_many(
            {"api_key": api_key, "api_secret": api_secret},
            {"$set": set_fields},
        )
    if skip_collection != "kite_market_config":
        db["kite_market_config"].update_many(
            {"broker": "kite", "api_key": api_key, "api_secret": api_secret},
            {"$set": set_fields},
        )


def save_kite_session(db, broker_doc_id: str, session: dict):
    """Persist access_token and login time into broker_configuration, and
    mirror it to kite_market_config when they share the same api_key/api_secret
    (see sync_kite_access_token_by_credentials)."""
    from bson import ObjectId
    access_token = session.get("access_token")
    login_time = datetime.now(timezone.utc).isoformat()
    doc = db["broker_configuration"].find_one(
        {"_id": ObjectId(broker_doc_id)}, {"api_key": 1, "api_secret": 1},
    ) or {}
    db["broker_configuration"].update_one(
        {"_id": ObjectId(broker_doc_id)},
        {"$set": {
            "access_token":  access_token,
            "login_time":    login_time,
            "user_id":       session.get("user_id"),
            "user_name":     session.get("user_name"),
        }},
    )
    sync_kite_access_token_by_credentials(
        db, doc.get("api_key"), doc.get("api_secret"), access_token, login_time,
        skip_collection="broker_configuration",
    )


def get_stored_access_token(db, broker_doc_id: str) -> str | None:
    """Load access_token from broker_configuration for a given broker doc."""
    from bson import ObjectId
    doc = db["broker_configuration"].find_one(
        {"_id": ObjectId(broker_doc_id)},
        {"access_token": 1},
    )
    return (doc or {}).get("access_token")


def get_option_instrument_token(
    kite: KiteConnect,
    name: str,           # e.g. "NIFTY", "BANKNIFTY"
    expiry,              # datetime.date object
    strike: float,
    option_type: str,    # "CE" or "PE"
    exchange: str = "NFO",
) -> int | None:
    """Return instrument_token for a specific option contract."""
    instruments = kite.instruments(exchange)
    for inst in instruments:
        if (
            inst["name"] == name
            and inst["instrument_type"] == option_type.upper()
            and inst["strike"] == strike
            and inst["expiry"] == expiry
        ):
            return inst["instrument_token"]
    return None


def get_option_historical_data(
    access_token: str,
    name: str,           # e.g. "NIFTY", "BANKNIFTY"
    expiry,              # datetime.date object
    strike: float,
    option_type: str,    # "CE" or "PE"
    from_date=None,      # datetime.date, defaults to today
    to_date=None,        # datetime.date, defaults to today
    interval: str = "minute",
    exchange: str = "NFO",
) -> list:
    """
    Fetch OHLCV candles for an option contract.

    Returns list of dicts:
      [{"date": datetime, "open": float, "high": float,
        "low": float, "close": float, "volume": int}, ...]
    """
    from datetime import date

    kite = get_kite_instance(access_token)

    if from_date is None:
        from_date = date.today()
    if to_date is None:
        to_date = date.today()

    token = get_option_instrument_token(kite, name, expiry, strike, option_type, exchange)
    if token is None:
        raise ValueError(
            f"Instrument not found: {name} {strike}{option_type} expiry={expiry} on {exchange}"
        )

    data = kite.historical_data(
        instrument_token=token,
        from_date=from_date,
        to_date=to_date,
        interval=interval,
    )
    return data
