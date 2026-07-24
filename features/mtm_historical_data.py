"""
mtm_historical_data.py
──────────────────────
Fetch per-minute OHLCV candle data for given tokens.

Endpoint:
    GET /algo/mtm/historical-data
        ?tokens=NSE_2025110409674,NSE_54815
        &candle=2025-11-03T11:10:21
        &activation_mode=algo-backtest

Response format (matches algo-historical-data.json):
    {
      "NSE_2025110409674": {
        "timestamp": ["2025-11-03T09:15:00", ..., "2025-11-03T11:10:00"],
        "open":   [100.0, ...],
        "high":   [100.0, ...],
        "low":    [100.0, ...],
        "close":  [100.0, ...],
        "volume": [0, ...]
      }
    }

Returns candles from 09:15 up to and including the candle timestamp.

Modes:
  algo-backtest → query option_chain_historical_data by token field; open=high=low=close
  fast-forward  → Kite historical_data API; real OHLCV, minute interval
  live          → Kite historical_data API; real OHLCV, minute interval
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

log = logging.getLogger(__name__)

OPTION_CHAIN_COLLECTION = "option_chain_historical_data"
SPOT_COLLECTION = "option_chain_index_spot"
VIX_SPOT_TOKEN = "NSE_00"

# Underlying/VIX names that can appear in the same tokens= list as option
# legs (get_mtm_historical_data_range) — these live in option_chain_index_spot,
# not option_chain_historical_data, and are looked up by name, not by a
# broker-specific numeric token.
_UNDERLYING_NAMES = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}
_VIX_ALIASES = {"INDIA_VIX", "INDIAVIX"}
_INDEX_NAMES = _UNDERLYING_NAMES | _VIX_ALIASES

# Dhan security IDs for index underlyings/VIX (broker_gateway.py's
# _DHAN_INDEX_TOKENS, duplicated here as strings since Dhan's REST API takes
# securityId as a string, not an int like Kite's instrument_token).
_DHAN_INDEX_TOKENS: dict[str, str] = {
    "NIFTY": "13", "BANKNIFTY": "25", "SENSEX": "51",
    "FINNIFTY": "27", "MIDCPNIFTY": "11915",
    "INDIA_VIX": "20225", "INDIAVIX": "20225",
}
# active_option_tokens.instrument_type -> Dhan's SEM_INSTRUMENT_NAME code
# (dhan_token_sync.py backfills 'index' from OPTIDX rows; only index-underlying
# options exist in this codebase today, 'stock' is provided for completeness).
_DHAN_LEG_INSTRUMENT_MAP: dict[str, str] = {"index": "OPTIDX", "stock": "OPTSTK"}


# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_candle_datetime(candle: str) -> tuple[str, str]:
    """
    Parse candle string → (trade_date YYYY-MM-DD, candle_ts YYYY-MM-DDTHH:MM:SS).
    Strips timezone offset.  Falls back to today at 15:30.
    """
    raw = str(candle or "").strip()
    for sep in ("+", "Z"):
        if sep in raw:
            raw = raw.split(sep)[0].strip()

    if "T" in raw:
        date_part, time_part = raw.split("T", 1)
        hm = time_part[:5]          # 'HH:MM'
        try:
            parsed_time = datetime.strptime(hm, "%H:%M")
            if (parsed_time.hour, parsed_time.minute) > (15, 30):
                hm = "15:30"
        except ValueError:
            hm = "15:30"
        candle_ts = f"{date_part}T{hm}:00"
    else:
        date_part = raw[:10] if len(raw) >= 10 else date.today().isoformat()
        candle_ts = f"{date_part}T15:30:00"

    if len(date_part) != 10:
        date_part = date.today().isoformat()
        candle_ts = f"{date_part}T15:30:00"

    return date_part, candle_ts


def _normalize_token(raw: str) -> str:
    return str(raw or "").strip().upper()


def _numeric_part(token_key: str) -> str:
    """'NSE_54812' → '54812';  '54812' → '54812'."""
    raw = str(token_key or "").strip()
    return raw.split("_", 1)[-1] if "_" in raw else raw


def _normalize_range_ts(raw: str, *, is_end: bool = False) -> str:
    """
    Pad a caller-supplied ISO timestamp out to 'YYYY-MM-DDTHH:MM:SS'.
    Accepts 'YYYY-MM-DD', 'YYYY-MM-DDTHH:MM', or already-full timestamps
    (extra offset/Z suffix stripped). Missing time-of-day defaults to
    market open (09:15) for a start bound, market close (15:30) for an end
    bound — same defaulting '15:30 cap' idea as _parse_candle_datetime.
    """
    ts = str(raw or "").strip()
    for sep in ("+", "Z"):
        if sep in ts:
            ts = ts.split(sep)[0].strip()
    if "T" not in ts and " " in ts:
        ts = ts.replace(" ", "T", 1)  # some callers send 'YYYY-MM-DD HH:MM:SS'

    default_time = "15:30:00" if is_end else "09:15:00"
    if "T" not in ts:
        date_part = ts[:10] if len(ts) >= 10 else date.today().isoformat()
        return f"{date_part}T{default_time}"

    date_part, time_part = ts.split("T", 1)
    if len(date_part) != 10:
        date_part = date.today().isoformat()
    if len(time_part) == 5:          # 'HH:MM'
        time_part = f"{time_part}:00"
    elif len(time_part) != 8:        # not 'HH:MM:SS' either
        time_part = default_time
    return f"{date_part}T{time_part}"


# ── backtest path ─────────────────────────────────────────────────────────────

def _fetch_backtest_historical(
    db,
    token_keys: list[str],
    from_ts: str,
    to_ts: str,
) -> dict:
    """
    Query option_chain_historical_data by token field directly.
    Fetch candles from from_ts up to to_ts (inclusive).
    open = high = low = close  (backtest only stores close price).
    """
    result: dict[str, dict] = {}

    for token_key in token_keys:
        query: dict[str, Any] = {
            "token": token_key,
            "timestamp": {
                "$gte": from_ts,
                "$lte": to_ts,
            },
        }

        try:
            docs = list(
                db._db[OPTION_CHAIN_COLLECTION].find(
                    query,
                    {"_id": 0, "timestamp": 1, "close": 1, "oi": 1},
                ).sort("timestamp", 1)
            )
        except Exception as exc:
            log.warning("[mtm_historical] backtest query error token=%s: %s", token_key, exc)
            continue

        if not docs:
            log.info("[mtm_historical] no docs found for token=%s date=%s..%s", token_key, from_ts, to_ts)
            continue

        timestamps, closes, ois = [], [], []
        for doc in docs:
            ts = str(doc.get("timestamp") or "").strip()
            close = float(doc.get("close") or 0.0)
            oi = int(doc.get("oi") or 0)
            if ts:
                timestamps.append(ts)
                closes.append(close)
                ois.append(oi)

        result[token_key] = {
            "timestamp": timestamps,
            "open":   closes,
            "high":   closes,
            "low":    closes,
            "close":  closes,
            "volume": ois,
        }

    return result


# ── live / fast-forward path ──────────────────────────────────────────────────

def _init_kite():
    from features.broker_gateway import get_broker_credentials as get_common_credentials, broker_is_configured as is_configured, load_broker_credentials_from_db as load_credentials_from_db, get_broker_rest_client_with_token as get_kite_instance
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


def _ensure_broker_credentials_loaded(db) -> None:
    """Same 'load from DB if not already cached in-memory' dance _init_kite()
    does for Kite — needed before get_broker_credentials() for Dhan too, or a
    fresh process returns an empty access_token instead of the real one."""
    from features.broker_gateway import broker_is_configured, load_broker_credentials_from_db
    if not broker_is_configured():
        load_broker_credentials_from_db(db)


def _fetch_dhan_leg_historical(
    db,
    token_keys: list[str],
    from_dt: datetime,
    to_dt: datetime,
) -> dict:
    """
    Dhan REST fallback for option-leg tokens — no dhanhq SDK dependency
    (this box doesn't have it installed; candle_fetch.fetch_dhan_intraday_candles
    hits POST /v2/charts/intraday directly, same call already proven working
    for scanner backfills). exchange_segment/instrument per token come from
    active_option_tokens (same lookup dhan_broker.py's place_order path uses
    to resolve a security_id) since Dhan's API needs those alongside the
    securityId itself.
    """
    from features.broker_gateway import get_broker_credentials
    from features.candle_fetch import fetch_dhan_intraday_candles

    _ensure_broker_credentials_loaded(db)
    result: dict[str, dict] = {}
    _, access_token = get_broker_credentials()
    if not access_token:
        log.warning("[mtm_historical] Dhan access_token not configured")
        return result

    segments: dict[str, tuple[str, str]] = {}
    for doc in db._db["active_option_tokens"].find(
        {"broker": "dhan", "token": {"$in": token_keys}},
        {"_id": 0, "token": 1, "ws_segment": 1, "instrument_type": 1, "exchange": 1},
    ):
        token = str(doc.get("token") or "").strip()
        if not token:
            continue
        segment = str(doc.get("ws_segment") or "").strip().upper() or (
            "BSE_FNO" if str(doc.get("exchange") or "").upper() == "BSE" else "NSE_FNO"
        )
        instrument = _DHAN_LEG_INSTRUMENT_MAP.get(str(doc.get("instrument_type") or "").strip().lower(), "OPTIDX")
        segments[token] = (segment, instrument)

    for token_key in token_keys:
        seg_instrument = segments.get(token_key)
        if not seg_instrument:
            log.warning("[mtm_historical] no active_option_tokens doc for Dhan token=%s", token_key)
            continue
        exchange_segment, instrument = seg_instrument

        try:
            candles = fetch_dhan_intraday_candles(
                access_token, token_key, exchange_segment, instrument, "1", from_dt, to_dt,
            )
        except Exception as exc:
            log.warning("[mtm_historical] Dhan intraday error token=%s: %s", token_key, exc)
            continue

        timestamps, opens, highs, lows, closes, volumes = [], [], [], [], [], []
        for candle in candles:
            dt = candle.get("date")
            if not isinstance(dt, datetime):
                continue
            ist_dt = dt + timedelta(hours=5, minutes=30)  # Dhan intraday timestamps are UTC instants
            timestamps.append(ist_dt.strftime("%Y-%m-%dT%H:%M:%S"))
            opens.append(float(candle.get("open") or 0.0))
            highs.append(float(candle.get("high") or 0.0))
            lows.append(float(candle.get("low") or 0.0))
            closes.append(float(candle.get("close") or 0.0))
            volumes.append(int(candle.get("volume") or 0))

        if timestamps:
            result[token_key] = {
                "timestamp": timestamps,
                "open":   opens,
                "high":   highs,
                "low":    lows,
                "close":  closes,
                "volume": volumes,
            }

    return result


def _fetch_kite_historical(
    db,
    token_keys: list[str],
    from_ts: str,
    to_ts: str,
) -> dict:
    """
    Real OHLCV, minute interval, from from_ts up to to_ts — broker fallback
    used once the DB has nothing for a token. Branches on whichever broker is
    actually active: Dhan's REST intraday endpoint (see _fetch_dhan_leg_historical)
    when Dhan is active, Kite's own historical_data() otherwise.
    """
    try:
        from_dt = datetime.strptime(from_ts, "%Y-%m-%dT%H:%M:%S")
        to_dt   = datetime.strptime(to_ts, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        from_dt = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
        to_dt   = datetime.now().replace(second=0, microsecond=0)

    from features.broker_gateway import _active_broker  # type: ignore
    if _active_broker() == "dhan":
        return _fetch_dhan_leg_historical(db, token_keys, from_dt, to_dt)

    result: dict[str, dict] = {}

    try:
        kite = _init_kite()
    except Exception as exc:
        log.warning("[mtm_historical] Kite init error: %s", exc)
        return result

    for token_key in token_keys:
        numeric_str = _numeric_part(token_key)
        try:
            numeric_token = int(numeric_str)
        except (ValueError, TypeError):
            log.warning("[mtm_historical] cannot parse numeric token from %s", token_key)
            continue

        try:
            candles = kite.historical_data(
                instrument_token=numeric_token,
                from_date=from_dt,
                to_date=to_dt,
                interval="minute",
            )
        except Exception as exc:
            log.warning("[mtm_historical] Kite error token=%s: %s", token_key, exc)
            continue

        timestamps, opens, highs, lows, closes, volumes = [], [], [], [], [], []
        for candle in candles:
            dt = candle.get("date")
            ts = dt.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dt, datetime) else str(dt or "")
            timestamps.append(ts)
            opens.append(float(candle.get("open") or 0.0))
            highs.append(float(candle.get("high") or 0.0))
            lows.append(float(candle.get("low") or 0.0))
            closes.append(float(candle.get("close") or 0.0))
            volumes.append(int(candle.get("volume") or 0))

        result[token_key] = {
            "timestamp": timestamps,
            "open":   opens,
            "high":   highs,
            "low":    lows,
            "close":  closes,
            "volume": volumes,
        }

    return result


# ── underlying/VIX names (get_mtm_historical_data_range only) ─────────────────

def _fetch_index_spot_historical(
    db,
    index_keys: list[str],
    from_ts: str,
    to_ts: str,
) -> dict:
    """
    DB path for underlying/VIX names (e.g. 'NIFTY', 'INDIA_VIX') passed in
    the same tokens= list as option legs — these live in option_chain_index_spot,
    not option_chain_historical_data, and only carry a close price (no OHLC),
    so open=high=low=close / volume=0 here — same convention the option-leg
    backtest path uses for its own close-only source.
    """
    result: dict[str, dict] = {}
    col = db._db[SPOT_COLLECTION]
    time_q = {"timestamp": {"$gte": from_ts, "$lte": to_ts}}

    for key in index_keys:
        is_vix = key in _VIX_ALIASES
        query = {**time_q, "token": VIX_SPOT_TOKEN} if is_vix else {**time_q, "underlying": key, "token": {"$ne": VIX_SPOT_TOKEN}}
        try:
            docs = list(
                col.find(query, {"_id": 0, "timestamp": 1, "close": 1, "spot_price": 1})
                .sort("timestamp", 1)
            )
        except Exception as exc:
            log.warning("[mtm_historical] index-spot query error key=%s: %s", key, exc)
            continue

        timestamps, closes = [], []
        for doc in docs:
            ts = str(doc.get("timestamp") or "").strip()
            if not ts:
                continue
            timestamps.append(ts)
            closes.append(float(doc.get("close") or doc.get("spot_price") or 0.0))

        if timestamps:
            result[key] = {
                "timestamp": timestamps,
                "open":   closes,
                "high":   closes,
                "low":    closes,
                "close":  closes,
                "volume": [0] * len(closes),
            }

    return result


def _fetch_dhan_index_historical(
    db,
    index_keys: list[str],
    from_dt: datetime,
    to_dt: datetime,
) -> dict:
    """Dhan REST fallback for underlying/VIX names — same
    candle_fetch.fetch_dhan_intraday_candles call as _fetch_dhan_leg_historical,
    resolving each name to its Dhan index security_id (IDX_I / INDEX)."""
    from features.broker_gateway import get_broker_credentials
    from features.candle_fetch import fetch_dhan_intraday_candles

    _ensure_broker_credentials_loaded(db)
    result: dict[str, dict] = {}
    _, access_token = get_broker_credentials()
    if not access_token:
        log.warning("[mtm_historical] Dhan access_token not configured (index path)")
        return result

    for key in index_keys:
        security_id = _DHAN_INDEX_TOKENS.get(key)
        if not security_id:
            log.warning("[mtm_historical] no Dhan index token for %s", key)
            continue

        try:
            candles = fetch_dhan_intraday_candles(access_token, security_id, "IDX_I", "INDEX", "1", from_dt, to_dt)
        except Exception as exc:
            log.warning("[mtm_historical] Dhan index error key=%s: %s", key, exc)
            continue

        timestamps, opens, highs, lows, closes, volumes = [], [], [], [], [], []
        for candle in candles:
            dt = candle.get("date")
            if not isinstance(dt, datetime):
                continue
            ist_dt = dt + timedelta(hours=5, minutes=30)  # Dhan intraday timestamps are UTC instants
            timestamps.append(ist_dt.strftime("%Y-%m-%dT%H:%M:%S"))
            opens.append(float(candle.get("open") or 0.0))
            highs.append(float(candle.get("high") or 0.0))
            lows.append(float(candle.get("low") or 0.0))
            closes.append(float(candle.get("close") or 0.0))
            volumes.append(int(candle.get("volume") or 0))

        if timestamps:
            result[key] = {
                "timestamp": timestamps,
                "open":   opens,
                "high":   highs,
                "low":    lows,
                "close":  closes,
                "volume": volumes,
            }

    return result


def _fetch_index_kite_historical(
    db,
    index_keys: list[str],
    from_ts: str,
    to_ts: str,
) -> dict:
    """Broker fallback for underlying/VIX names — Dhan REST when Dhan is
    active (see _fetch_dhan_index_historical), else resolves each name to
    Kite's numeric index instrument token before calling historical_data (the
    same generic call _fetch_kite_historical makes for option legs)."""
    try:
        from_dt = datetime.strptime(from_ts, "%Y-%m-%dT%H:%M:%S")
        to_dt   = datetime.strptime(to_ts, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return {}

    from features.broker_gateway import _active_broker  # type: ignore
    if _active_broker() == "dhan":
        return _fetch_dhan_index_historical(db, index_keys, from_dt, to_dt)

    from features.spot_atm_utils import KITE_INDEX_TOKENS, INDIA_VIX_KITE_TOKEN

    result: dict[str, dict] = {}

    try:
        kite = _init_kite()
    except Exception as exc:
        log.warning("[mtm_historical] Kite init error (index path): %s", exc)
        return result

    for key in index_keys:
        numeric_token = INDIA_VIX_KITE_TOKEN if key in _VIX_ALIASES else KITE_INDEX_TOKENS.get(key, 0)
        if not numeric_token:
            log.warning("[mtm_historical] no Kite index token for %s", key)
            continue

        try:
            candles = kite.historical_data(
                instrument_token=numeric_token,
                from_date=from_dt,
                to_date=to_dt,
                interval="minute",
            )
        except Exception as exc:
            log.warning("[mtm_historical] Kite index error key=%s: %s", key, exc)
            continue

        timestamps, opens, highs, lows, closes, volumes = [], [], [], [], [], []
        for candle in candles:
            dt = candle.get("date")
            ts = dt.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dt, datetime) else str(dt or "")
            timestamps.append(ts)
            opens.append(float(candle.get("open") or 0.0))
            highs.append(float(candle.get("high") or 0.0))
            lows.append(float(candle.get("low") or 0.0))
            closes.append(float(candle.get("close") or 0.0))
            volumes.append(int(candle.get("volume") or 0))

        if timestamps:
            result[key] = {
                "timestamp": timestamps,
                "open":   opens,
                "high":   highs,
                "low":    lows,
                "close":  closes,
                "volume": volumes,
            }

    return result


# ── leg cache (get_mtm_historical_data_range only) ─────────────────────────────
#
# Dhan enforces one global, cross-process ~1.05s minimum between ANY two REST
# calls (broker_gateway.wait_for_dhan_slot) — with N leg tokens needing a live
# fetch that's N seconds no matter what, and it can't be parallelized (they'd
# just queue on the same shared clock) or shortened (bypassing Dhan's limit
# risks the account getting rate-blocked). The only lever left is not needing
# the live call at all on repeat opens — cached separately from
# option_chain_historical_data, whose (underlying, expiry, strike, type,
# timestamp) unique key is the real backtest/IV ground truth other features
# read; writing live-fetched candles into it risks colliding with or
# poisoning that data instead.

_MTM_LEG_CACHE_COLLECTION = "mtm_leg_intraday_cache"
_MTM_LEG_CACHE_FRESHNESS_SECONDS = 90
_mtm_leg_cache_index_ensured = False


def _ensure_mtm_leg_cache_index(db) -> None:
    global _mtm_leg_cache_index_ensured
    if _mtm_leg_cache_index_ensured:
        return
    try:
        db._db[_MTM_LEG_CACHE_COLLECTION].create_index([("token", 1), ("timestamp", 1)], unique=True, background=True)
    except Exception as exc:
        log.warning("[mtm_historical] leg-cache index ensure failed: %s", exc)
    _mtm_leg_cache_index_ensured = True


def _cache_fresh_enough(last_ts: str, to_ts: str) -> bool:
    try:
        last_dt = datetime.strptime(last_ts, "%Y-%m-%dT%H:%M:%S")
        to_dt = datetime.strptime(to_ts, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return False
    return 0 <= (to_dt - last_dt).total_seconds() <= _MTM_LEG_CACHE_FRESHNESS_SECONDS


def _fetch_leg_cache(db, token_keys: list[str], from_ts: str, to_ts: str) -> dict:
    """Close-only cache (all the MTM chart plots) — a hit only counts if its
    last candle is within _MTM_LEG_CACHE_FRESHNESS_SECONDS of to_ts, so a
    click doesn't silently serve a stale/truncated series forever."""
    result: dict[str, dict] = {}
    col = db._db[_MTM_LEG_CACHE_COLLECTION]
    for token_key in token_keys:
        docs = list(col.find(
            {"token": token_key, "timestamp": {"$gte": from_ts, "$lte": to_ts}},
            {"_id": 0, "timestamp": 1, "close": 1},
        ).sort("timestamp", 1))
        if not docs or not _cache_fresh_enough(docs[-1]["timestamp"], to_ts):
            continue
        closes = [float(d["close"]) for d in docs]
        result[token_key] = {
            "timestamp": [d["timestamp"] for d in docs],
            "open":   closes,
            "high":   closes,
            "low":    closes,
            "close":  closes,
            "volume": [0] * len(closes),
        }
    return result


def _persist_leg_cache(db, fetched: dict) -> None:
    from pymongo import UpdateOne

    _ensure_mtm_leg_cache_index(db)
    ops = []
    for token_key, series in fetched.items():
        for ts, close in zip(series.get("timestamp") or [], series.get("close") or []):
            ops.append(UpdateOne({"token": token_key, "timestamp": ts}, {"$set": {"close": close}}, upsert=True))
    if not ops:
        return
    try:
        db._db[_MTM_LEG_CACHE_COLLECTION].bulk_write(ops, ordered=False)
    except Exception as exc:
        log.warning("[mtm_historical] leg-cache bulk-write failed (%d ops): %s", len(ops), exc)


# ── public API ────────────────────────────────────────────────────────────────

def get_mtm_historical_data(
    db,
    tokens: str,
    candle: str,
    activation_mode: str,
) -> dict:
    """
    tokens          – comma-separated 'NSE_2025110409674,NSE_54815'
    candle          – ISO timestamp '2025-11-03T11:10:21'
    activation_mode – 'algo-backtest' | 'fast-forward' | 'live'

    Returns OHLCV candles from 09:15 up to (and including) the candle minute.
    """
    normalized_mode = str(activation_mode or "").strip().lower()
    trade_date, candle_ts = _parse_candle_datetime(candle)
    market_open_ts = f"{trade_date}T09:15:00"

    token_keys = [_normalize_token(t) for t in str(tokens or "").split(",") if t.strip()]
    if not token_keys:
        return {}

    # Prefer DB-backed minute history whenever it exists. This keeps the API
    # stable even when callers omit activation_mode or accidentally send the
    # wrong mode for backtest / fast-forward execution pages.
    db_result = _fetch_backtest_historical(db, token_keys, market_open_ts, candle_ts)
    if db_result:
        return db_result

    if normalized_mode == "algo-backtest":
        return {}

    return _fetch_kite_historical(db, token_keys, market_open_ts, candle_ts)


def get_mtm_historical_data_range(
    db,
    tokens: str,
    start_dt: str,
    end_dt: str,
) -> dict:
    """
    tokens   – comma-separated leg tokens AND/OR underlying/VIX names, e.g.
               '63926,63919,63923,63924,NIFTY' — one call covers both, same
               as prices.algotest.in/historical's single tokens= list.
    start_dt – ISO timestamp, inclusive lower bound, e.g. '2026-07-24T09:15'
    end_dt   – ISO timestamp, inclusive upper bound, e.g. '2026-07-24T15:30'

    Explicit-range twin of get_mtm_historical_data (which always starts at
    market open and ends at one "candle" instant, and only handles option
    legs) — mirrors prices.algotest.in/historical's ?start_dt&end_dt contract:
    one endpoint, one tokens list (legs + underlying/VIX names mixed freely),
    one flat per-key OHLCV response. DB is tried first per key; anything
    still missing after that falls back to Kite — independently per key, so
    one source succeeding never hides another key that needs the other path.
    """
    all_keys = [_normalize_token(t) for t in str(tokens or "").split(",") if t.strip()]
    if not all_keys:
        return {}

    from_ts = _normalize_range_ts(start_dt, is_end=False)
    to_ts = _normalize_range_ts(end_dt, is_end=True)

    option_keys = [k for k in all_keys if k not in _INDEX_NAMES]
    index_keys  = [k for k in all_keys if k in _INDEX_NAMES]

    result: dict = {}
    if option_keys:
        result.update(_fetch_backtest_historical(db, option_keys, from_ts, to_ts))
    if index_keys:
        result.update(_fetch_index_spot_historical(db, index_keys, from_ts, to_ts))

    missing = [k for k in all_keys if k not in result]
    if missing:
        missing_option = [k for k in missing if k not in _INDEX_NAMES]
        missing_index  = [k for k in missing if k in _INDEX_NAMES]

        if missing_option:
            cached = _fetch_leg_cache(db, missing_option, from_ts, to_ts)
            result.update(cached)
            missing_option = [k for k in missing_option if k not in cached]

        if missing_option:
            fetched = _fetch_kite_historical(db, missing_option, from_ts, to_ts)
            _persist_leg_cache(db, fetched)
            result.update(fetched)

        if missing_index:
            result.update(_fetch_index_kite_historical(db, missing_index, from_ts, to_ts))

    return result
