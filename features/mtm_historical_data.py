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
from datetime import date, datetime
from typing import Any

log = logging.getLogger(__name__)

OPTION_CHAIN_COLLECTION = "option_chain_historical_data"


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


# ── backtest path ─────────────────────────────────────────────────────────────

def _fetch_backtest_historical(
    db,
    token_keys: list[str],
    trade_date: str,
    candle_ts: str,
) -> dict:
    """
    Query option_chain_historical_data by token field directly.
    Fetch candles from 09:15 up to candle_ts.
    open = high = low = close  (backtest only stores close price).
    """
    result: dict[str, dict] = {}
    market_open_ts = f"{trade_date}T09:15:00"

    for token_key in token_keys:
        query: dict[str, Any] = {
            "token": token_key,
            "timestamp": {
                "$gte": market_open_ts,
                "$lte": candle_ts,
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
            log.info("[mtm_historical] no docs found for token=%s date=%s..%s", token_key, market_open_ts, candle_ts)
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


def _fetch_kite_historical(
    token_keys: list[str],
    trade_date: str,
    candle_ts: str,
) -> dict:
    """
    Call Kite historical_data (minute interval) from 09:15 up to candle_ts.
    """
    result: dict[str, dict] = {}

    try:
        kite = _init_kite()
    except Exception as exc:
        log.warning("[mtm_historical] Kite init error: %s", exc)
        return result

    try:
        from_dt = datetime.strptime(f"{trade_date}T09:15:00", "%Y-%m-%dT%H:%M:%S")
        to_dt   = datetime.strptime(candle_ts, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        from_dt = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
        to_dt   = datetime.now().replace(second=0, microsecond=0)

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

    token_keys = [_normalize_token(t) for t in str(tokens or "").split(",") if t.strip()]
    if not token_keys:
        return {}

    # Prefer DB-backed minute history whenever it exists. This keeps the API
    # stable even when callers omit activation_mode or accidentally send the
    # wrong mode for backtest / fast-forward execution pages.
    db_result = _fetch_backtest_historical(db, token_keys, trade_date, candle_ts)
    if db_result:
        return db_result

    if normalized_mode == "algo-backtest":
        return {}

    return _fetch_kite_historical(token_keys, trade_date, candle_ts)
