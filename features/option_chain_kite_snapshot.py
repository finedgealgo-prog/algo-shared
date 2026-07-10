"""
option_chain_kite_snapshot.py
──────────────────────────────
Fetch option chain snapshot from Kite historical_data API.
Used as fallback when option_chain_historical_data has no entry for the
requested date (e.g. fast-forward / live trades in the current year).

Strategy: fetch the **full trading-day OHLCV** for all ATM-range tokens once,
cache by (underlying, date) in process memory, then look up the close price
at any timestamp in O(log n) without a second Kite call.

First call for a date: ~20-30 s (Kite fetch, 240 tokens, 30 workers, complete).
Subsequent calls for the same date: <10 ms (in-memory lookup).
"""
from __future__ import annotations

import bisect
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Lock
from typing import Any

log = logging.getLogger(__name__)

SPOT_COL   = "option_chain_index_spot"
TOKENS_COL = "active_option_tokens"

# ── in-process day cache ──────────────────────────────────────────────────────
# key  → "NIFTY:2026-05-26"
# val  → { token_int: {"timestamps": [...], "closes": [...]} }
_day_cache: dict[str, dict[int, dict]] = {}
_build_lock = Lock()   # prevents concurrent cold-start builds for the same key


# ── helpers ───────────────────────────────────────────────────────────────────

def _init_kite():
    from features.kite_broker_ws import (
        get_common_credentials, is_configured, load_credentials_from_db,
    )
    from features.kite_broker import get_kite_instance
    from features.mongo_data import MongoData

    if not is_configured():
        _db = MongoData()
        try:
            load_credentials_from_db(_db)
        finally:
            _db.close()

    if not is_configured():
        raise RuntimeError("Kite access token not configured")

    _, access_token = get_common_credentials()
    return get_kite_instance(access_token)


def _parse_ts(ts_str: str) -> datetime:
    raw = str(ts_str or "").strip().replace(" ", "T").rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return datetime.now().replace(second=0, microsecond=0)


def _fetch_full_day(kite, token_int: int, from_dt: datetime, to_dt: datetime) -> dict:
    """Return {timestamps: [...], closes: [...]} for token_int over the full day."""
    try:
        candles = kite.historical_data(
            instrument_token=token_int,
            from_date=from_dt,
            to_date=to_dt,
            interval="minute",
        )
        timestamps, closes = [], []
        for c in candles:
            dt = c.get("date")
            # Kite returns timezone-aware datetime (IST = UTC+5:30)
            # strftime gives the wall-clock time in the object's own timezone
            ts = dt.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dt, datetime) else str(dt or "")
            if ts:
                timestamps.append(ts)
                closes.append(float(c.get("close") or 0))
        return {"timestamps": timestamps, "closes": closes}
    except Exception as exc:
        log.debug("[oc_kite_snap] token=%d fetch error: %s", token_int, exc)
        return {"timestamps": [], "closes": []}


def _close_at_ts(series: dict, target_ts: str) -> float:
    """Binary-search the series for the last close at or before target_ts."""
    timestamps = series.get("timestamps", [])
    closes     = series.get("closes",     [])
    if not timestamps:
        return 0.0
    idx = bisect.bisect_right(timestamps, target_ts) - 1
    if idx < 0:
        # target is before the first candle — forward-fill with first close
        return closes[0] if closes else 0.0
    return closes[idx] if idx < len(closes) else 0.0


# ── day-level cache builder ───────────────────────────────────────────────────

def _build_day_cache(
    underlying: str,
    date_part: str,
    token_int_list: list[int],
    max_workers: int = 30,
) -> dict[int, dict]:
    """
    Fetch full trading-day candles for every token.
    Waits for ALL futures (wait=True) so the cache is always complete —
    no token is abandoned with an empty series.
    First call takes 20-30 s; all subsequent calls are instant from cache.
    """
    try:
        kite = _init_kite()
    except Exception as exc:
        log.warning("[oc_kite_snap] Kite init failed: %s", exc)
        return {}

    from_dt = datetime.strptime(f"{date_part}T09:15:00", "%Y-%m-%dT%H:%M:%S")
    to_dt   = datetime.strptime(f"{date_part}T15:30:00", "%Y-%m-%dT%H:%M:%S")
    # Cap to_dt at now so we don't ask Kite for future candles
    to_dt = min(to_dt, datetime.now().replace(second=0, microsecond=0))

    result: dict[int, dict] = {}

    # wait=True (default in __exit__): block until every thread finishes
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_tok = {
            pool.submit(_fetch_full_day, kite, t, from_dt, to_dt): t
            for t in token_int_list
        }
        for fut, tok in future_to_tok.items():
            try:
                result[tok] = fut.result()
            except Exception:
                result[tok] = {"timestamps": [], "closes": []}

    zeros = sum(1 for s in result.values() if not s.get("timestamps"))
    log.info(
        "[oc_kite_snap] day cache built: %s %s  tokens=%d  zeros=%d",
        underlying, date_part, len(result), zeros,
    )
    return result


# ── public API ────────────────────────────────────────────────────────────────

def get_option_chain_kite_snapshot(
    db,
    underlying: str,
    norm_ts: str,
    *,
    atm_range: int = 1000,
    max_expiries: int = 3,
    max_workers: int = 30,
    timeout_s: float = 60.0,   # noqa: ARG001 — kept for API compat
) -> dict[str, Any]:
    """
    Return an option chain snapshot for *underlying* at *norm_ts*.
    First call per (underlying, date) fetches from Kite (slow, complete).
    Subsequent calls are served from the in-process cache (fast).
    """
    ul        = str(underlying or "").strip().upper()
    date_part = norm_ts[:10]
    cache_key = f"{ul}:{date_part}"

    # ── 1. Spot price ($lte lookup — always finds nearest) ────────────────────
    spot_doc = db._db[SPOT_COL].find_one(
        {"underlying": ul, "timestamp": {"$lte": norm_ts}},
        {"_id": 0, "spot_price": 1},
        sort=[("timestamp", -1)],
    )
    spot = float((spot_doc or {}).get("spot_price") or 0)

    # ── 2. Token metadata from active_option_tokens ───────────────────────────
    expiries_available = sorted(
        db._db[TOKENS_COL].distinct(
            "expiry",
            {"instrument": ul, "expiry": {"$gte": date_part}},
        )
    )[:max_expiries]

    if not expiries_available:
        return _empty_response(ul, spot, norm_ts)

    strike_filter: dict[str, Any] = (
        {"strike": {"$gte": spot - atm_range, "$lte": spot + atm_range}}
        if spot else {}
    )
    token_docs = list(db._db[TOKENS_COL].find(
        {"instrument": ul, "expiry": {"$in": expiries_available}, **strike_filter},
        {"_id": 0, "expiry": 1, "strike": 1, "option_type": 1, "token": 1},
    ))
    if not token_docs:
        return _empty_response(ul, spot, norm_ts)

    token_int_map: dict[int, dict] = {}
    for doc in token_docs:
        try:
            token_int_map[int(doc["token"])] = doc
        except (ValueError, KeyError):
            pass

    # ── 3. Build / reuse day cache (double-checked lock prevents duplicate builds) ─
    if cache_key not in _day_cache:
        with _build_lock:
            if cache_key not in _day_cache:
                log.info("[oc_kite_snap] cache MISS %s — fetching from Kite (full day) …", cache_key)
                _day_cache[cache_key] = _build_day_cache(
                    ul, date_part, list(token_int_map.keys()), max_workers=max_workers,
                )
            else:
                log.debug("[oc_kite_snap] cache HIT (lock) %s", cache_key)
    else:
        log.debug("[oc_kite_snap] cache HIT %s", cache_key)

    day_data = _day_cache[cache_key]

    # ── 4. Look up close at norm_ts for each token ────────────────────────────
    expiry_set: set[str] = set()
    option_chain: list[dict] = []
    grouped: dict[str, dict] = {}

    for tok_int, doc in token_int_map.items():
        close    = _close_at_ts(day_data.get(tok_int, {}), norm_ts)
        expiry   = str(doc.get("expiry") or "")[:10]
        strike   = float(doc.get("strike") or 0)
        opt_type = str(doc.get("option_type") or "").upper()
        if not expiry or not strike or opt_type not in ("CE", "PE"):
            continue

        expiry_set.add(expiry)
        row: dict[str, Any] = {
            "underlying": ul, "expiry": expiry,
            "strike": strike, "type": opt_type,
            "token": str(doc.get("token") or "").strip(),
            "close": close, "iv": None, "delta": None,
            "oi": 0, "spot_price": spot, "timestamp": norm_ts,
        }
        option_chain.append(row)
        sk = str(int(strike)) if float(strike) == int(float(strike)) else str(strike)
        grouped.setdefault(expiry, {}).setdefault(sk, {"CE": None, "PE": None})[opt_type] = row

    return {
        "instrument":           ul,
        "expiries":             sorted(expiry_set),
        "expiry_count":         len(expiry_set),
        "total_contracts":      len(option_chain),
        "source":               "kite_snapshot",
        "option_chain":         option_chain,
        "grouped_option_chain": grouped,
        "spot_price":           spot,
        "timestamp":            norm_ts,
    }


def _empty_response(underlying: str, spot: float, ts: str) -> dict:
    return {
        "instrument": underlying, "expiries": [], "expiry_count": 0,
        "total_contracts": 0, "source": "kite_snapshot_empty",
        "option_chain": [], "grouped_option_chain": {},
        "spot_price": spot, "timestamp": ts,
    }


def backfill_today_to_db(
    db,
    underlying: str,
    *,
    atm_range: int = 1000,
    max_expiries: int = 3,
    max_workers: int = 30,
) -> dict:
    """
    Fetch the full trading-day OHLCV (from Kite) for *underlying* and write
    every minute's close into option_chain_historical_data.

    Returns a summary dict with counts.  Call once per day to backfill so
    bar-replay can read from DB instead of hitting the Kite fallback.
    """
    ul        = str(underlying or "").strip().upper()
    date_part = datetime.now().strftime("%Y-%m-%d")
    cache_key = f"{ul}:{date_part}"

    # ── build (or reuse) the in-process day cache ─────────────────────────────
    spot_doc = db._db[SPOT_COL].find_one(
        {"underlying": ul},
        {"_id": 0, "spot_price": 1},
        sort=[("timestamp", -1)],
    )
    spot = float((spot_doc or {}).get("spot_price") or 0)

    expiries_available = sorted(
        db._db[TOKENS_COL].distinct(
            "expiry",
            {"instrument": ul, "expiry": {"$gte": date_part}},
        )
    )[:max_expiries]

    if not expiries_available:
        return {"ok": False, "error": "no active expiries"}

    strike_filter: dict[str, Any] = (
        {"strike": {"$gte": spot - atm_range, "$lte": spot + atm_range}} if spot else {}
    )
    token_docs = list(db._db[TOKENS_COL].find(
        {"instrument": ul, "expiry": {"$in": expiries_available}, **strike_filter},
        {"_id": 0, "expiry": 1, "strike": 1, "option_type": 1, "token": 1},
    ))

    token_int_map: dict[int, dict] = {}
    for doc in token_docs:
        try:
            token_int_map[int(doc["token"])] = doc
        except (ValueError, KeyError):
            pass

    if not token_int_map:
        return {"ok": False, "error": "no tokens found"}

    if cache_key not in _day_cache:
        with _build_lock:
            if cache_key not in _day_cache:
                log.info("[oc_kite_snap] backfill: building day cache %s …", cache_key)
                _day_cache[cache_key] = _build_day_cache(
                    ul, date_part, list(token_int_map.keys()), max_workers=max_workers,
                )

    day_data = _day_cache[cache_key]

    # ── expand every timestamp in the cache into DB rows ─────────────────────
    from pymongo import UpdateOne

    # Collect all unique minute timestamps across all tokens
    all_timestamps: set[str] = set()
    for series in day_data.values():
        all_timestamps.update(series.get("timestamps", []))

    ops: list = []
    oc_col = db._db["option_chain_historical_data"]

    for ts in sorted(all_timestamps):
        for tok_int, doc in token_int_map.items():
            series   = day_data.get(tok_int, {})
            close    = _close_at_ts(series, ts)
            if not close:
                continue
            expiry   = str(doc.get("expiry") or "")[:10]
            strike   = float(doc.get("strike") or 0)
            opt_type = str(doc.get("option_type") or "").upper()
            if not expiry or not strike or opt_type not in ("CE", "PE"):
                continue
            ops.append(UpdateOne(
                {"underlying": ul, "expiry": expiry, "strike": strike,
                 "type": opt_type, "timestamp": ts},
                {"$set": {"underlying": ul, "expiry": expiry, "strike": strike,
                           "type": opt_type, "timestamp": ts,
                           "token": str(tok_int),
                           "close": close, "oi": 0,
                           "iv": None, "delta": None,
                           "source": "kite_backfill"}},
                upsert=True,
            ))

            # flush in batches of 2000
            if len(ops) >= 2_000:
                oc_col.bulk_write(ops, ordered=False)
                ops = []

    if ops:
        oc_col.bulk_write(ops, ordered=False)

    total_timestamps = len(all_timestamps)
    total_tokens     = len(token_int_map)
    log.info(
        "[oc_kite_snap] backfill done: %s  timestamps=%d  tokens=%d",
        ul, total_timestamps, total_tokens,
    )
    return {
        "ok": True, "underlying": ul, "date": date_part,
        "timestamps_written": total_timestamps,
        "tokens": total_tokens,
    }


def clear_day_cache(underlying: str | None = None, date_part: str | None = None) -> None:
    """Clear one or all day-cache entries (call after a server restart or manual refresh)."""
    if underlying and date_part:
        _day_cache.pop(f"{underlying.upper()}:{date_part}", None)
    elif underlying:
        for k in list(_day_cache):
            if k.startswith(f"{underlying.upper()}:"):
                del _day_cache[k]
    else:
        _day_cache.clear()
