"""
spot_historical_data.py
───────────────────────
Fetch per-minute spot price + VIX history from option_chain_index_spot.

Endpoint:
    GET /algo/spot/historical-data
        ?underlying=NIFTY
        &candle=2025-11-03T15:30:00
        &activation_mode=algo-backtest

Response (same shape as mtm/historical-data):
    {
      "NSE_01": { "timestamp": [...], "close": [...] },
      "NSE_00": { "timestamp": [...], "close": [...] }
    }
"""
from __future__ import annotations

import logging
from datetime import date as _date, datetime

log = logging.getLogger(__name__)

SPOT_COL  = "option_chain_index_spot"
VIX_TOKEN = "NSE_00"


def _set_underlying_series(result: dict, underlying: str, token: str, series: dict) -> None:
    ul = str(underlying or "").strip().upper()
    token_key = str(token or "").strip() or f"SPOT_{ul}"
    alias_key = f"SPOT_{ul}"
    result[token_key] = series
    result[alias_key] = series


def _parse_candle(candle: str) -> tuple[str, str]:
    raw = str(candle or "").strip()
    for sep in ("+", "Z"):
        if sep in raw:
            raw = raw.split(sep)[0].strip()
    if "T" in raw:
        date_part, time_part = raw.split("T", 1)
        hm = time_part[:5]
        try:
            parsed_time = datetime.strptime(hm, "%H:%M")
            if (parsed_time.hour, parsed_time.minute) > (15, 30):
                hm = "15:30"
        except ValueError:
            hm = "15:30"
        candle_ts = f"{date_part}T{hm}:00"
    else:
        date_part = raw[:10] if len(raw) >= 10 else _date.today().isoformat()
        candle_ts = f"{date_part}T15:30:00"
    return date_part, candle_ts


def _to_series(docs: list) -> dict:
    timestamps, closes = [], []
    for doc in docs:
        ts = str(doc.get("timestamp") or "").strip()
        if ts:
            timestamps.append(ts)
            # prefer new 'close' field; fall back to legacy 'spot_price'
            price = doc.get("close") or doc.get("spot_price") or 0.0
            closes.append(float(price))
    return {"timestamp": timestamps, "close": closes}


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


def _kite_candles_to_series(candles: list) -> dict:
    timestamps, closes = [], []
    for c in candles:
        dt = c.get("date")
        ts = dt.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dt, datetime) else str(dt or "")
        if ts:
            timestamps.append(ts)
            closes.append(float(c.get("close") or 0.0))
    return {"timestamp": timestamps, "close": closes}


def _fetch_kite_spot_vix(underlying: str, trade_date: str, candle_ts: str) -> dict:
    """Fetch spot + VIX candles from Kite historical_data API (live/fast-forward)."""
    from features.spot_atm_utils import KITE_INDEX_TOKENS, INDIA_VIX_KITE_TOKEN

    ul = underlying.strip().upper()
    spot_kite_token = KITE_INDEX_TOKENS.get(ul, 0)
    if not spot_kite_token:
        log.warning("[spot_hist] no Kite token for underlying=%s", ul)
        return {}

    try:
        kite = _init_kite()
    except Exception as exc:
        log.warning("[spot_hist] Kite init error: %s", exc)
        return {}

    from_dt = datetime.strptime(f"{trade_date}T09:15:00", "%Y-%m-%dT%H:%M:%S")
    to_dt   = datetime.strptime(candle_ts,                 "%Y-%m-%dT%H:%M:%S")
    result: dict = {}

    # Spot candles
    try:
        spot_candles = kite.historical_data(spot_kite_token, from_dt, to_dt, "minute")
        series = _kite_candles_to_series(spot_candles)
        if series["timestamp"]:
            result[f"SPOT_{ul}"] = series
            log.info("[spot_hist] Kite spot %s rows=%d", ul, len(series["timestamp"]))
    except Exception as exc:
        log.warning("[spot_hist] Kite spot fetch error underlying=%s: %s", ul, exc)

    # VIX candles
    try:
        vix_candles = kite.historical_data(INDIA_VIX_KITE_TOKEN, from_dt, to_dt, "minute")
        vix_series = _kite_candles_to_series(vix_candles)
        if vix_series["timestamp"]:
            result[VIX_TOKEN] = vix_series
            log.info("[spot_hist] Kite VIX rows=%d", len(vix_series["timestamp"]))
    except Exception as exc:
        log.warning("[spot_hist] Kite VIX fetch error: %s", exc)

    return result


def _normalize_range_ts(raw: str, *, is_end: bool = False) -> str:
    """Pad a caller-supplied ISO timestamp out to 'YYYY-MM-DDTHH:MM:SS'."""
    ts = str(raw or "").strip()
    for sep in ("+", "Z"):
        if sep in ts:
            ts = ts.split(sep)[0].strip()
    if "T" not in ts and " " in ts:
        ts = ts.replace(" ", "T", 1)  # some callers send 'YYYY-MM-DD HH:MM:SS'

    default_time = "15:30:00" if is_end else "09:15:00"
    if "T" not in ts:
        date_part = ts[:10] if len(ts) >= 10 else _date.today().isoformat()
        return f"{date_part}T{default_time}"

    date_part, time_part = ts.split("T", 1)
    if len(date_part) != 10:
        date_part = _date.today().isoformat()
    if len(time_part) == 5:          # 'HH:MM'
        time_part = f"{time_part}:00"
    elif len(time_part) != 8:        # not 'HH:MM:SS' either
        time_part = default_time
    return f"{date_part}T{time_part}"


def _fetch_spot_range(
    db,
    underlying: str,
    from_ts: str,
    to_ts: str,
    *,
    kite_fallback: bool,
) -> dict:
    """
    Returns { underlying_token: series, "NSE_00": vix_series } for [from_ts, to_ts].
    Shared body for both the candle-based and explicit-range public entry points.
    """
    time_q = {"timestamp": {"$gte": from_ts, "$lte": to_ts}}
    col = db._db[SPOT_COL]
    result: dict = {}

    # ── Underlying spot (NIFTY → NSE_01, BANKNIFTY → NSE_02, …) ────────────
    ul = underlying.strip().upper()
    spot_docs = list(
        col.find(
            {**time_q, "underlying": ul, "token": {"$ne": VIX_TOKEN}},
            {"_id": 0, "timestamp": 1, "close": 1, "spot_price": 1, "token": 1},
        ).sort("timestamp", 1)
    )
    has_spot = bool(spot_docs)
    if has_spot:
        token = str(spot_docs[0].get("token") or f"SPOT_{ul}")
        _set_underlying_series(result, ul, token, _to_series(spot_docs))
        log.info("[spot_hist] %s token=%s rows=%d", ul, token, len(spot_docs))
    else:
        log.warning("[spot_hist] no data for underlying=%s range=%s..%s", ul, from_ts, to_ts)

    # ── India VIX (NSE_00) ──────────────────────────────────────────────────
    vix_docs = list(
        col.find(
            {**time_q, "token": VIX_TOKEN},
            {"_id": 0, "timestamp": 1, "close": 1, "spot_price": 1},
        ).sort("timestamp", 1)
    )
    has_vix = bool(vix_docs)
    if has_vix:
        result[VIX_TOKEN] = _to_series(vix_docs)
        log.info("[spot_hist] VIX rows=%d", len(vix_docs))
    else:
        log.warning("[spot_hist] no VIX data for range=%s..%s", from_ts, to_ts)

    if not kite_fallback or (has_spot and has_vix):
        return result

    trade_date = from_ts[:10]
    kite_result = _fetch_kite_spot_vix(underlying, trade_date, to_ts)
    if not has_spot:
        for key, series in kite_result.items():
            if key != VIX_TOKEN:
                _set_underlying_series(result, ul, key, series)
    if not has_vix and VIX_TOKEN in kite_result:
        result[VIX_TOKEN] = kite_result[VIX_TOKEN]
    return result


def get_spot_historical_data(
    db,
    underlying: str,
    candle: str,
    activation_mode: str = "algo-backtest",
) -> dict:
    """
    Returns { underlying_token: series, "NSE_00": vix_series }
    Both series span 09:15 → candle timestamp on the trade date.

    live / fast-forward → Kite historical_data API (real OHLCV)
    algo-backtest       → DB option_chain_index_spot
    """
    date_part, candle_ts = _parse_candle(candle)
    market_open = f"{date_part}T09:15:00"
    normalized_mode = str(activation_mode or "").strip().lower()
    return _fetch_spot_range(db, underlying, market_open, candle_ts, kite_fallback=normalized_mode != "algo-backtest")


def get_spot_historical_data_range(
    db,
    underlying: str,
    start_dt: str,
    end_dt: str,
) -> dict:
    """
    Explicit-range twin of get_spot_historical_data — takes [start_dt, end_dt]
    directly instead of "market open through one candle instant", mirroring
    prices.algotest.in/historical's ?start_dt&end_dt contract. Always tries
    DB first, then falls back to Kite (no algo-backtest-only short-circuit).
    """
    from_ts = _normalize_range_ts(start_dt, is_end=False)
    to_ts = _normalize_range_ts(end_dt, is_end=True)
    return _fetch_spot_range(db, underlying, from_ts, to_ts, kite_fallback=True)
