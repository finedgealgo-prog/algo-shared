"""
active_option_tokens_cache.py
──────────────────────────────
Whole-collection, once-a-day in-memory cache for active_option_tokens.

active_option_tokens is re-seeded once/day (contract master); ~99.6% of it is
already expiry >= today at any point during the day (123,340 of 123,802 docs
measured live). Several hot paths were re-querying Mongo for the same handful
of (instrument, expiry, strike, option_type) / token lookups on every tick of
a 1s loop (live_monitor_socket's entry-trigger monitor, dhan_ticker's
subscribe-segment resolution) — see the missing-index investigation that
turned up mongod at 150% CPU driven largely by these two repeat patterns.

Rather than cache each lookup lazily key-by-key, load the whole current+future
slice ONCE per process (one bulk query) and rebuild it once a day (IST trade
date rollover) — every read after that is a plain dict lookup, no per-key TTL
bookkeeping, no partial-cache cold-start cost for the first query of a new key.

Not a cross-process cache (each of the 5 services holds its own copy in
memory) — that's fine here since the source data changes at most once/day and
every service already independently queries Mongo directly today.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))


def _today_str() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


_lock = threading.Lock()
_state: dict = {
    "date": None,                    # IST date string this snapshot was built for
    "expiries_by_instrument": {},    # instrument -> sorted [expiry, ...] (>= that day's trade date)
    "doc_by_contract": {},           # (instrument, expiry, strike, option_type) -> full doc
    "segment_by_broker_token": {},   # (broker, token) -> ws_segment
}


def _refresh_if_stale(db) -> None:
    today = _today_str()
    if _state["date"] == today:
        return
    with _lock:
        if _state["date"] == today:  # lost the race, another thread already refreshed
            return
        expiries_by_instrument: dict[str, set] = {}
        doc_by_contract: dict = {}
        segment_by_broker_token: dict = {}
        cursor = db._db["active_option_tokens"].find(
            {"expiry": {"$gte": today}},
            {"instrument": 1, "expiry": 1, "strike": 1, "option_type": 1,
             "token": 1, "ws_segment": 1, "broker": 1, "symbol": 1, "lot_size": 1, "_id": 0},
        )
        for doc in cursor:
            instrument = str(doc.get("instrument") or "")
            expiry = str(doc.get("expiry") or "")
            broker = str(doc.get("broker") or "")
            token = str(doc.get("token") or "")
            if instrument and expiry:
                expiries_by_instrument.setdefault(instrument, set()).add(expiry)
                strike = doc.get("strike")
                option_type = str(doc.get("option_type") or "")
                if strike is not None and option_type:
                    doc_by_contract[(instrument, expiry, strike, option_type)] = doc
            if broker and token:
                segment_by_broker_token[(broker, token)] = str(doc.get("ws_segment") or "")
        _state["expiries_by_instrument"] = {k: sorted(v) for k, v in expiries_by_instrument.items()}
        _state["doc_by_contract"] = doc_by_contract
        _state["segment_by_broker_token"] = segment_by_broker_token
        _state["date"] = today


def get_expiries(db, instrument: str) -> list[str]:
    _refresh_if_stale(db)
    return _state["expiries_by_instrument"].get(str(instrument), [])


def get_token_doc(db, instrument: str, expiry: str, strike, option_type: str) -> dict:
    _refresh_if_stale(db)
    return _state["doc_by_contract"].get((str(instrument), str(expiry), strike, str(option_type)), {})


def get_ws_segments_bulk(db, broker: str, tokens: list[str], default_segment: str) -> dict[str, str]:
    _refresh_if_stale(db)
    by_token = _state["segment_by_broker_token"]
    return {t: (by_token.get((broker, t)) or default_segment) for t in tokens}
