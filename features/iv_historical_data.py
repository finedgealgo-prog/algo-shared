"""
iv_historical_data.py
─────────────────────
Fetch per-minute price + IV + Delta history for option leg tokens
from option_chain_historical_data.

Endpoint:
    GET /algo/option-chain/historical-iv
        ?tokens=NSE_2025110484996,NSE_2025110460049
        &candle=2025-11-03T15:30:00
        &activation_mode=algo-backtest

Response:
    {
      "NSE_2025110484996": {
        "timestamp": [...],
        "close":     [...],
        "iv":        [...],
        "delta":     [...],
        "oi":        [...]
      }
    }
"""
from __future__ import annotations

import logging
from datetime import date as _date, datetime

log = logging.getLogger(__name__)

OC_COL = "option_chain_historical_data"


def _parse_candle(candle: str) -> tuple[str, str]:
    raw = str(candle or "").strip()
    for sep in ("+", "Z"):
        if sep in raw:
            raw = raw.split(sep)[0].strip()
    if "T" in raw:
        date_part, time_part = raw.split("T", 1)
        candle_ts = f"{date_part}T{time_part[:5]}:00"
    else:
        date_part = raw[:10] if len(raw) >= 10 else _date.today().isoformat()
        candle_ts = f"{date_part}T15:30:00"
    return date_part, candle_ts


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


def _resolve_token_meta(db, token_key: str) -> dict:
    """
    Resolve underlying, expiry, strike, option_type from a token.
    Supports compound format 'NIFTY_2025-11-04_24500_CE' and numeric Kite tokens.
    Returns {} if not resolvable.
    """
    raw = str(token_key or "").strip()

    # Compound format: NIFTY_2025-11-04_24500_CE
    parts = raw.split("_")
    if len(parts) >= 4 and parts[-1].upper() in ("CE", "PE"):
        try:
            return {
                "underlying":  parts[0].upper(),
                "expiry":      "_".join(parts[1:-2]),
                "strike":      float(parts[-2]),
                "option_type": parts[-1].upper(),
            }
        except (ValueError, IndexError):
            pass

    # Numeric Kite token — look up in active_option_tokens
    numeric = raw.split("_")[-1] if "_" in raw else raw
    if numeric.isdigit() and db is not None:
        try:
            doc = db._db["active_option_tokens"].find_one(
                {"$or": [{"token": numeric}, {"tokens": numeric}]},
                {"instrument": 1, "expiry": 1, "strike": 1, "option_type": 1},
            ) or {}
            if doc.get("instrument"):
                return {
                    "underlying":  str(doc["instrument"]).upper(),
                    "expiry":      str(doc.get("expiry") or ""),
                    "strike":      float(doc.get("strike") or 0),
                    "option_type": str(doc.get("option_type") or "CE").upper(),
                }
        except Exception:
            pass

    return {}


def _fetch_kite_iv(db, token_list: list[str], trade_date: str, candle_ts: str) -> dict:
    """
    For live/fast-forward: fetch option candles from Kite, spot from Kite,
    then calculate IV per minute using Black-Scholes.
    """
    from features.spot_atm_utils import KITE_INDEX_TOKENS, _calculate_live_iv
    from features.span_margin import RISK_FREE_RATE

    try:
        kite = _init_kite()
    except Exception as exc:
        log.warning("[iv_hist] Kite init error: %s", exc)
        return {}

    from_dt = datetime.strptime(f"{trade_date}T09:15:00", "%Y-%m-%dT%H:%M:%S")
    to_dt   = datetime.strptime(candle_ts,                 "%Y-%m-%dT%H:%M:%S")
    result: dict = {}

    # Pre-fetch spot candles per underlying (cache to avoid duplicate API calls)
    spot_cache: dict[str, dict[str, float]] = {}  # underlying → {ts: close}

    def _get_spot_map(underlying: str) -> dict[str, float]:
        if underlying in spot_cache:
            return spot_cache[underlying]
        kite_token = KITE_INDEX_TOKENS.get(underlying, 0)
        if not kite_token:
            spot_cache[underlying] = {}
            return {}
        try:
            candles = kite.historical_data(kite_token, from_dt, to_dt, "minute")
            m: dict[str, float] = {}
            for c in candles:
                dt = c.get("date")
                ts = dt.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dt, datetime) else str(dt or "")
                if ts:
                    m[ts] = float(c.get("close") or 0.0)
            spot_cache[underlying] = m
            return m
        except Exception as exc:
            log.warning("[iv_hist] Kite spot fetch %s: %s", underlying, exc)
            spot_cache[underlying] = {}
            return {}

    for token_key in token_list:
        meta = _resolve_token_meta(db, token_key)
        if not meta:
            log.warning("[iv_hist] cannot resolve metadata for token=%s", token_key)
            continue

        underlying  = meta["underlying"]
        expiry      = meta["expiry"]
        strike      = meta["strike"]
        option_type = meta["option_type"]

        # Numeric Kite token for this contract
        numeric = token_key.split("_")[-1] if "_" in token_key else token_key
        if not numeric.isdigit():
            # Try active_option_tokens for the Kite token
            try:
                tok_doc = db._db["active_option_tokens"].find_one({
                    "instrument":  underlying,
                    "expiry":      expiry,
                    "strike":      strike,
                    "option_type": option_type,
                }, {"token": 1, "tokens": 1}) or {}
                numeric = str(tok_doc.get("token") or tok_doc.get("tokens") or "").strip()
            except Exception:
                numeric = ""

        if not numeric or not numeric.isdigit():
            log.warning("[iv_hist] no numeric Kite token for %s", token_key)
            continue

        try:
            opt_candles = kite.historical_data(int(numeric), from_dt, to_dt, "minute")
        except Exception as exc:
            log.warning("[iv_hist] Kite option fetch token=%s: %s", numeric, exc)
            continue

        spot_map = _get_spot_map(underlying)

        timestamps, closes, ivs, deltas, ois = [], [], [], [], []
        for c in opt_candles:
            dt = c.get("date")
            ts = dt.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dt, datetime) else str(dt or "")
            if not ts:
                continue
            ltp  = float(c.get("close") or 0.0)
            spot = spot_map.get(ts, 0.0)
            iv   = _calculate_live_iv(spot, strike, expiry, ltp, option_type) if spot > 0 and ltp > 0 else 0.0
            timestamps.append(ts)
            closes.append(ltp)
            ivs.append(round(iv * 100, 4))   # store as percentage (e.g. 15.2 for 15.2%)
            deltas.append(0.0)               # delta requires BS calculation — extendable
            ois.append(int(c.get("volume") or 0))

        if timestamps:
            result[token_key] = {
                "timestamp": timestamps,
                "close":     closes,
                "iv":        ivs,
                "delta":     deltas,
                "oi":        ois,
            }
            log.info("[iv_hist] Kite token=%s rows=%d", token_key, len(timestamps))

    return result


def get_iv_historical_data(
    db,
    tokens: str,
    candle: str,
    activation_mode: str = "algo-backtest",
) -> dict:
    """
    Returns price + IV + Delta series for each token.
    tokens is a comma-separated string.

    live / fast-forward → Kite historical_data + Black-Scholes IV calculation
    algo-backtest       → DB option_chain_historical_data
    """
    date_part, candle_ts = _parse_candle(candle)
    token_list = [t.strip() for t in tokens.split(",") if t.strip()]
    if not token_list:
        return {}

    if str(activation_mode or "").strip() in ("live", "fast-forward", "forward-test"):
        return _fetch_kite_iv(db, token_list, date_part, candle_ts)

    market_open = f"{date_part}T09:15:00"
    result: dict = {}
    col = db._db[OC_COL]

    for token_key in token_list:
        docs = list(
            col.find(
                {
                    "token": token_key,
                    "timestamp": {"$gte": market_open, "$lte": candle_ts},
                },
                {"_id": 0, "timestamp": 1, "close": 1, "iv": 1, "delta": 1, "oi": 1},
            ).sort("timestamp", 1)
        )
        if not docs:
            continue

        timestamps, closes, ivs, deltas, ois = [], [], [], [], []
        for doc in docs:
            ts = str(doc.get("timestamp") or "").strip()
            if not ts:
                continue
            timestamps.append(ts)
            closes.append(float(doc.get("close") or 0.0))
            ivs.append(float(doc.get("iv") or 0.0))
            deltas.append(float(doc.get("delta") or 0.0))
            ois.append(int(doc.get("oi") or 0))

        result[token_key] = {
            "timestamp": timestamps,
            "close":     closes,
            "iv":        ivs,
            "delta":     deltas,
            "oi":        ois,
        }
        log.info("[iv_hist] token=%s rows=%d", token_key, len(timestamps))

    return result
