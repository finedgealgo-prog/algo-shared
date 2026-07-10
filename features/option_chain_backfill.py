"""
option_chain_backfill.py
─────────────────────────
Backfills a full trading day's option chain into MongoDB.

What it writes
──────────────
1. option_chain_index_spot  — OHLCV per minute for ALL indices + India VIX
     token     : standardised key  (NSE_01, NSE_00, …)
     kite_token: actual Kite instrument token  (256265, 264969, …)
2. option_chain_historical_data — one row per
     (underlying, expiry, strike, type, timestamp)
     fields: open, high, low, close, volume, oi, iv, delta, gamma, theta, vega

Usage
─────
GET /algo/option-chain/backfill-today/NIFTY   → single instrument
GET /algo/option-chain/backfill-today/all     → all instruments at once
GET /algo/option-chain/backfill-status
"""
from __future__ import annotations

import bisect
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from threading import Event, Lock, Thread

log = logging.getLogger(__name__)

# ── token maps ────────────────────────────────────────────────────────────────
_KITE_SPOT_TOKEN = {
    "NIFTY":      256265,
    "BANKNIFTY":  260105,
    "FINNIFTY":   257801,
    "SENSEX":     265,
    "MIDCPNIFTY": 288009,
}
_KITE_VIX_TOKEN = 264969
# BSE instruments that need string-symbol lookup (no integer token in Kite)
_KITE_SPOT_SYMBOL = {
    "BANKEX": "BSE:BANKEX",
}
_DYNAMIC_SPOT_TOKEN_CACHE: dict[str, int] = {}
_SAFE_MAX_WORKERS = 8

# ── Kite rate limiter (token bucket: max 5 req/s sustained) ──────────────────
class _KiteRateLimiter:
    def __init__(self, rps: float = 5.0):
        self._interval = 1.0 / rps
        self._lock = Lock()
        self._next_allowed = 0.0

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            if wait > 0:
                time.sleep(wait)
            self._next_allowed = time.monotonic() + self._interval

_kite_rate_limiter = _KiteRateLimiter(rps=5.0)

_NSE_TOKEN = {
    "NIFTY":      "NSE_01",
    "INDIAVIX":   "NSE_00",
    "SENSEX":     "NSE_02",
    "BANKNIFTY":  "NSE_03",
    "BANKEX":     "NSE_04",
    "FINNIFTY":   "NSE_05",
    "MIDCPNIFTY": "NSE_06",
}

# ── Greeks constants ──────────────────────────────────────────────────────────
_IST       = timezone(timedelta(hours=5, minutes=30))
_RISK_FREE = 0.068
_MIN_T     = 1 / (365 * 24 * 60)
_DIV_Q     = {
    "NIFTY": 0.012, "BANKNIFTY": 0.005, "FINNIFTY": 0.015,
    "SENSEX": 0.012, "MIDCPNIFTY": 0.008,
}


# ── Black-Scholes ─────────────────────────────────────────────────────────────

def _ncdf(x):  return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))
def _npdf(x):  return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

def _bs(S, K, T, r, sigma, opt, q=0.0):
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if opt == "CE" else (K - S))
    sqT = math.sqrt(T)
    d1  = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqT)
    d2  = d1 - sigma * sqT
    if opt == "CE":
        return S*math.exp(-q*T)*_ncdf(d1) - K*math.exp(-r*T)*_ncdf(d2)
    return K*math.exp(-r*T)*_ncdf(-d2) - S*math.exp(-q*T)*_ncdf(-d1)

def _calc_iv(ltp, S, K, T, r, opt, q=0.0):
    if ltp <= 0 or S <= 0 or K <= 0 or T <= 0:  return 0.0
    if ltp < max(0.0, (S-K) if opt=="CE" else (K-S)):  return 0.0
    lo, hi = 1e-5, 20.0
    for _ in range(120):
        mid = (lo + hi) * 0.5
        p   = _bs(S, K, T, r, mid, opt, q)
        if abs(p - ltp) < 0.001:  return mid
        lo, hi = (mid, hi) if p < ltp else (lo, mid)
    return (lo + hi) * 0.5

def _calc_greeks(S, K, T, r, sigma, opt, q=0.0):
    if sigma <= 0 or T <= 0 or S <= 0 or K <= 0:
        return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}
    sqT = math.sqrt(T); eqT = math.exp(-q*T); erT = math.exp(-r*T)
    d1  = (math.log(S/K) + (r-q+0.5*sigma*sigma)*T) / (sigma*sqT)
    d2  = d1 - sigma*sqT; nd1 = _npdf(d1)
    gamma = eqT * nd1 / (S * sigma * sqT)
    vega  = S * eqT * nd1 * sqT / 100.0
    if opt == "CE":
        delta = eqT * _ncdf(d1)
        theta = (-(S*nd1*sigma*eqT)/(2*sqT) + q*S*eqT*_ncdf(d1)  - r*K*erT*_ncdf(d2))  / 365.0
    else:
        delta = eqT * (_ncdf(d1) - 1.0)
        theta = (-(S*nd1*sigma*eqT)/(2*sqT) - q*S*eqT*_ncdf(-d1) + r*K*erT*_ncdf(-d2)) / 365.0
    return {"delta": round(delta,4), "gamma": round(gamma,6),
            "theta": round(theta,4), "vega":  round(vega, 4)}

def _tte(expiry_str, at_ts_str):
    try:
        exp = datetime.fromisoformat(expiry_str[:10]).replace(hour=15, minute=30, tzinfo=_IST)
        ref = datetime.fromisoformat(at_ts_str[:19]).replace(tzinfo=_IST)
        return max(_MIN_T, (exp - ref).total_seconds() / (365.0 * 86400))
    except Exception:
        return _MIN_T


# ── numpy-vectorized Greeks (fast path) ──────────────────────────────────────

try:
    import numpy as _np
    from scipy.special import ndtr as _ndtr
    _NUMPY_OK = True
except ImportError:
    _NUMPY_OK = False


def _bs_price_vec(S: float, K, T, r: float, sigma, is_call, q: float):
    """Vectorized BS price for arrays K, T, sigma, is_call (bool array)."""
    sqT  = _np.sqrt(T)
    d1   = (_np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqT)
    d2   = d1 - sigma * sqT
    eqT  = _np.exp(-q * T)
    erT  = _np.exp(-r * T)
    Nd1  = _ndtr(d1);  Nnd1 = _ndtr(-d1)
    Nd2  = _ndtr(d2);  Nnd2 = _ndtr(-d2)
    ce   = S * eqT * Nd1  - K * erT * Nd2
    pe   = K * erT * Nnd2 - S * eqT * Nnd1
    return _np.where(is_call, ce, pe)


def _calc_iv_vec(closes, S: float, K, T, r: float, is_call, q: float):
    """Vectorized IV via 60-step numpy bisection. Returns iv array (0 where invalid)."""
    n     = len(closes)
    ivs   = _np.zeros(n)
    valid = (closes > 0) & (K > 0) & (T > 0)
    if not valid.any():
        return ivs
    C_v, K_v, T_v, ic_v = closes[valid], K[valid], T[valid], is_call[valid]
    lo = _np.full(valid.sum(), 1e-5)
    hi = _np.full(valid.sum(), 20.0)
    for _ in range(60):
        mid = (lo + hi) * 0.5
        p   = _bs_price_vec(S, K_v, T_v, r, mid, ic_v, q)
        lo  = _np.where(p < C_v, mid, lo)
        hi  = _np.where(p < C_v, hi, mid)
    ivs[valid] = (lo + hi) * 0.5
    return ivs


def _calc_greeks_vec(S: float, K, T, r: float, sigma, is_call, q: float):
    """Returns (delta, gamma, theta, vega) numpy arrays."""
    sqT  = _np.sqrt(T)
    eqT  = _np.exp(-q * T);   erT = _np.exp(-r * T)
    d1   = (_np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqT)
    d2   = d1 - sigma * sqT
    nd1  = _np.exp(-0.5 * d1 ** 2) / _np.sqrt(2.0 * _np.pi)
    Nd1  = _ndtr(d1);  Nd2 = _ndtr(d2)
    gamma = eqT * nd1 / (S * sigma * sqT)
    vega  = S * eqT * nd1 * sqT / 100.0
    delta = _np.where(is_call, eqT * Nd1, eqT * (Nd1 - 1.0))
    theta = _np.where(
        is_call,
        (-(S * nd1 * sigma * eqT) / (2 * sqT) + q * S * eqT * Nd1  - r * K * erT * Nd2)  / 365.0,
        (-(S * nd1 * sigma * eqT) / (2 * sqT) - q * S * eqT * _ndtr(-d1) + r * K * erT * _ndtr(-d2)) / 365.0,
    )
    return (
        _np.round(delta, 4), _np.round(gamma, 6),
        _np.round(theta, 4), _np.round(vega,  4),
    )


# ── status tracker ────────────────────────────────────────────────────────────

class _BackfillStatus:
    def __init__(self):
        self._lock  = Lock()
        self.status = "idle"
        self.progress = ""
        self.summary: dict = {}

    def update(self, status, progress="", summary=None):
        with self._lock:
            self.status   = status
            self.progress = progress
            if summary is not None:
                self.summary = summary

    def get(self):
        with self._lock:
            return {"status": self.status, "progress": self.progress,
                    "summary": dict(self.summary)}

_status = _BackfillStatus()
_stop_event = Event()


def _stop_requested() -> bool:
    return _stop_event.is_set()


def _resolve_spot_kite_token(kite, underlying: str) -> int:
    ul = str(underlying or "").strip().upper()
    if ul in _KITE_SPOT_TOKEN:
        return int(_KITE_SPOT_TOKEN[ul])
    cached = _DYNAMIC_SPOT_TOKEN_CACHE.get(ul)
    if cached:
        return int(cached)
    for segment in ("NSE", "BSE"):
        try:
            for inst in kite.instruments(segment):
                tradingsymbol = str(inst.get("tradingsymbol") or "").strip().upper()
                name = str(inst.get("name") or "").strip().upper()
                if tradingsymbol == ul or name == ul:
                    token = int(inst.get("instrument_token") or 0)
                    if token > 0:
                        _DYNAMIC_SPOT_TOKEN_CACHE[ul] = token
                        return token
        except Exception as exc:
            log.debug("[backfill] dynamic spot token lookup failed underlying=%s segment=%s error=%s", ul, segment, exc)
    return 0


def _find_catchup_start_dt(db, instruments: list[str], trade_date: str) -> tuple[datetime, str | None]:
    day_start = f"{trade_date}T09:15:00"
    day_end = f"{trade_date}T23:59:59"
    latest_points: list[str] = []

    for ul in instruments:
        doc = db._db["option_chain_historical_data"].find_one(
            {"underlying": ul, "timestamp": {"$gte": day_start, "$lte": day_end}},
            {"_id": 0, "timestamp": 1},
            sort=[("timestamp", -1)],
        )
        if doc and doc.get("timestamp"):
            latest_points.append(str(doc["timestamp"]))

    for ul in [*instruments, "INDIAVIX"]:
        doc = db._db["option_chain_index_spot"].find_one(
            {"underlying": ul, "timestamp": {"$gte": day_start, "$lte": day_end}},
            {"_id": 0, "timestamp": 1},
            sort=[("timestamp", -1)],
        )
        if doc and doc.get("timestamp"):
            latest_points.append(str(doc["timestamp"]))

    if not latest_points:
        return datetime.strptime(day_start, "%Y-%m-%dT%H:%M:%S"), None

    base_ts = min(latest_points)
    start_dt = datetime.fromisoformat(base_ts[:19]) + timedelta(minutes=1)
    return start_dt, base_ts


# ── Kite helpers ───────────────────────────────────────────────────────────────

def _init_kite():
    from features.kite_broker_ws import get_common_credentials, is_configured, load_credentials_from_db
    from features.kite_broker import get_kite_instance
    from features.mongo_data import MongoData
    if not is_configured():
        _db = MongoData()
        try:    load_credentials_from_db(_db)
        finally: _db.close()
    if not is_configured():
        raise RuntimeError("Kite not configured")
    _, tok = get_common_credentials()
    return get_kite_instance(tok)


def _fetch_ohlcv(kite, token_int, from_dt, to_dt, *, retries: int = 3, retry_sleep_s: float = 0.35):
    """Returns {ts_str: {open, high, low, close, volume, oi}}.
    Acquires global rate limiter before each Kite call.
    On 429/rate-limit errors sleeps 5 s before retry.
    """
    last_exc = None
    for attempt in range(1, max(1, retries) + 1):
        if _stop_requested():
            return {}
        _kite_rate_limiter.acquire()          # ← global 5 req/s cap
        try:
            candles = kite.historical_data(
                instrument_token=token_int,
                from_date=from_dt, to_date=to_dt, interval="minute",
            )
            result = {}
            for c in candles:
                dt = c.get("date")
                ts = dt.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dt, datetime) else str(dt or "")
                if ts:
                    result[ts] = {
                        "open":   float(c.get("open")   or 0),
                        "high":   float(c.get("high")   or 0),
                        "low":    float(c.get("low")    or 0),
                        "close":  float(c.get("close")  or 0),
                        "volume": int(c.get("volume")   or 0),
                        "oi":     int(c.get("oi")       or 0),
                    }
            if result or attempt >= max(1, retries):
                if not result:
                    log.warning("[backfill] token=%d returned empty OHLCV after %d attempt(s)", token_int, attempt)
                return result
        except Exception as exc:
            last_exc = exc
            err_str = str(exc).lower()
            is_rate_limit = "too many" in err_str or "429" in err_str or "rate limit" in err_str
            if is_rate_limit:
                sleep_s = 5.0 * attempt          # 5 s, 10 s, 15 s …
                log.warning("[backfill] token=%d rate-limited (attempt %d), sleeping %.0fs",
                            token_int, attempt, sleep_s)
                time.sleep(sleep_s)
            else:
                log.debug("[backfill] token=%d attempt=%d error: %s", token_int, attempt, exc)
                if attempt < max(1, retries):
                    if _stop_requested():
                        return {}
                    time.sleep(retry_sleep_s * attempt)
    if last_exc is not None:
        log.warning("[backfill] token=%d failed after %d attempt(s): %s", token_int, retries, last_exc)
    return {}


# ── main backfill logic ────────────────────────────────────────────────────────

def _write_spot_ohlcv_to_db(db, underlying: str, ohlcv: dict, kite_tok_int: int | None) -> int:
    """Upsert spot OHLCV rows to option_chain_index_spot. Returns rows written."""
    from pymongo import UpdateOne
    if not ohlcv:
        return 0
    ul        = underlying.upper()
    nse_tok   = _NSE_TOKEN.get(ul, f"SPOT_{ul}")
    kite_tok  = str(kite_tok_int or "")
    spot_col  = db._db["option_chain_index_spot"]
    ops = []
    for ts, c in ohlcv.items():
        ops.append(UpdateOne(
            {"underlying": ul, "timestamp": ts},
            {"$set": {
                "underlying": ul,  "timestamp": ts,
                "token":      nse_tok, "kite_token": kite_tok,
                "open":    c["open"],  "high":   c["high"],
                "low":     c["low"],   "close":  c["close"],
                "volume":  c["volume"],"oi":     c["oi"],
                "spot_price": c["close"],   # backward compat
                "source":  "kite_backfill",
            }},
            upsert=True,
        ))
    if ops:
        spot_col.bulk_write(ops, ordered=False)
    return len(ops)


def _write_option_chain_to_db(
    db, ul: str, token_map: dict, ohlcv_map: dict,
    spot_ts_sorted: list, spot_closes: list,
    status_cb,
) -> int:
    """Calculate Greeks and bulk-write option chain rows. Returns rows written."""
    from pymongo import UpdateOne
    q_yield = _DIV_Q.get(ul, 0.01)
    oc_col  = db._db["option_chain_historical_data"]
    ops: list = []
    total = 0
    duplicate_keys = 0
    seen_row_keys: set[tuple[str, str, float, str, str]] = set()

    all_ts: set[str] = set(spot_ts_sorted)
    for s in ohlcv_map.values():
        all_ts.update(s.keys())
    time_grid = sorted(spot_ts_sorted) if spot_ts_sorted else sorted(all_ts)

    first_candle_by_token: dict[int, dict] = {}
    last_seen_candle_by_token: dict[int, dict] = {}
    for tok_int, series in ohlcv_map.items():
        if not series:
            continue
        first_ts = min(series.keys())
        first_candle_by_token[tok_int] = series.get(first_ts) or {}

    def _spot_at(ts: str) -> float:
        if not spot_ts_sorted: return 0.0
        idx = bisect.bisect_right(spot_ts_sorted, ts) - 1
        return spot_closes[idx if idx >= 0 else 0] if spot_closes else 0.0

    tok_list = list(token_map.items())   # stable order for numpy indexing

    for ts in time_grid:
        if _stop_requested():
            status_cb(f"[{ul}] Stop requested while writing option chain")
            break
        spot = _spot_at(ts)

        # ── collect candle data for all tokens at this timestamp ──────────
        rows: list[dict] = []
        for tok_int, doc in tok_list:
            series       = ohlcv_map.get(tok_int, {}) or {}
            exact_candle = series.get(ts) or {}
            if exact_candle:
                last_seen_candle_by_token[tok_int] = exact_candle
            candle   = exact_candle or last_seen_candle_by_token.get(tok_int) or first_candle_by_token.get(tok_int) or {}
            close    = float(candle.get("close") or 0)
            oi       = int(candle.get("volume") or candle.get("oi") or 0)
            if close <= 0 and oi == 0:
                continue
            expiry   = str(doc.get("expiry") or "")[:10]
            strike   = float(doc.get("strike") or 0)
            opt_type = str(doc.get("option_type") or "").upper()
            token_str = str(tok_int)
            if not expiry or not strike or opt_type not in ("CE", "PE"):
                continue
            row_key = (token_str, ts, expiry, strike, opt_type)
            if row_key in seen_row_keys:
                duplicate_keys += 1
                continue
            seen_row_keys.add(row_key)
            rows.append({
                "tok": token_str, "expiry": expiry, "strike": strike,
                "type": opt_type, "candle": candle, "close": close, "oi": oi,
            })

        if not rows:
            continue

        # ── vectorized IV + Greeks (numpy fast path) ──────────────────────
        n = len(rows)
        if _NUMPY_OK and spot > 0 and n > 0:
            closes_a  = _np.array([r["close"]  for r in rows], dtype=_np.float64)
            strikes_a = _np.array([r["strike"] for r in rows], dtype=_np.float64)
            is_call_a = _np.array([r["type"] == "CE" for r in rows])
            T_a       = _np.array([_tte(r["expiry"], ts) for r in rows], dtype=_np.float64)

            valid_mask = (closes_a > 0) & (strikes_a > 0) & (T_a > 0)
            ivs_a = _np.zeros(n)
            if valid_mask.any():
                ivs_a[valid_mask] = _calc_iv_vec(
                    closes_a[valid_mask], spot,
                    strikes_a[valid_mask], T_a[valid_mask],
                    _RISK_FREE, is_call_a[valid_mask], q_yield,
                )
            iv_valid = ivs_a > 0
            deltas_a = _np.zeros(n); gammas_a = _np.zeros(n)
            thetas_a = _np.zeros(n); vegas_a  = _np.zeros(n)
            if iv_valid.any():
                d, g, th, ve = _calc_greeks_vec(
                    spot,
                    strikes_a[iv_valid], T_a[iv_valid],
                    _RISK_FREE, ivs_a[iv_valid], is_call_a[iv_valid], q_yield,
                )
                deltas_a[iv_valid] = d
                gammas_a[iv_valid] = g
                thetas_a[iv_valid] = th
                vegas_a[iv_valid]  = ve
        else:
            # scalar fallback (no numpy)
            ivs_a    = [0.0] * n
            deltas_a = [0.0] * n; gammas_a = [0.0] * n
            thetas_a = [0.0] * n; vegas_a  = [0.0] * n
            if spot > 0:
                for i, r in enumerate(rows):
                    iv = _calc_iv(r["close"], spot, r["strike"],
                                  _tte(r["expiry"], ts), _RISK_FREE, r["type"], q_yield)
                    ivs_a[i] = iv
                    if iv > 0:
                        g = _calc_greeks(spot, r["strike"], _tte(r["expiry"], ts),
                                         _RISK_FREE, iv, r["type"], q_yield)
                        deltas_a[i] = g["delta"]; gammas_a[i] = g["gamma"]
                        thetas_a[i] = g["theta"]; vegas_a[i]  = g["vega"]

        # ── build mongo ops ────────────────────────────────────────────────
        for i, r in enumerate(rows):
            iv    = float(ivs_a[i])
            delta = float(deltas_a[i]); gamma = float(gammas_a[i])
            theta = float(thetas_a[i]); vega  = float(vegas_a[i])
            c     = r["candle"]
            ops.append(UpdateOne(
                {"underlying": ul, "token": r["tok"], "expiry": r["expiry"],
                 "strike": r["strike"], "type": r["type"], "timestamp": ts},
                {"$set": {
                    "underlying": ul, "expiry": r["expiry"], "strike": r["strike"],
                    "type": r["type"], "timestamp": ts, "token": r["tok"],
                    "open":  float(c.get("open")  or 0),
                    "high":  float(c.get("high")  or 0),
                    "low":   float(c.get("low")   or 0),
                    "close": r["close"], "oi": r["oi"],
                    "iv":    round(iv * 100, 4) if iv else None,
                    "delta": delta or None, "gamma": gamma or None,
                    "theta": theta or None, "vega":  vega  or None,
                    "source": "kite_backfill",
                }},
                upsert=True,
            ))
            total += 1
        if len(ops) >= 2_000:
            oc_col.bulk_write(ops, ordered=False)
            ops = []
            status_cb(f"[{ul}] Written {total:,} option rows…")

    if ops:
        oc_col.bulk_write(ops, ordered=False)
    if duplicate_keys:
        log.warning("[backfill] %s skipped %d duplicate option row keys", ul, duplicate_keys)
    return total


def _run_backfill(
    instruments: list[str],
    date_str: str,
    max_days_ahead: int,
    workers: int,
    expiry_filter: str | None = None,
    sync_from_last: bool = False,
) -> None:
    from features.mongo_data import MongoData
    label = "+".join(instruments)
    expiry_msg = f" expiry={expiry_filter}" if expiry_filter else ""
    _status.update("running", f"Starting backfill for {label} on {date_str}{expiry_msg}")
    _stop_event.clear()

    try:
        kite    = _init_kite()
        market_open_dt = datetime.strptime(f"{date_str}T09:15:00", "%Y-%m-%dT%H:%M:%S")
        market_close_dt = datetime.strptime(f"{date_str}T15:30:00", "%Y-%m-%dT%H:%M:%S")
        now_floor = datetime.now().replace(second=0, microsecond=0)
        from_dt = market_open_dt
        latest_synced_ts: str | None = None

        if sync_from_last:
            db = MongoData()
            try:
                from_dt, latest_synced_ts = _find_catchup_start_dt(db, instruments, date_str)
            finally:
                db.close()
            if from_dt < market_open_dt:
                from_dt = market_open_dt

        to_dt = min(market_close_dt, now_floor)
        if from_dt > to_dt:
            summary = {
                "date": date_str,
                "expiry_filter": expiry_filter or "all",
                "instruments": instruments,
                "from_ts": from_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                "to_ts": to_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                "latest_synced_ts": latest_synced_ts or "",
                "message": "Already up to date",
            }
            _status.update("done", "No new minutes to sync", summary)
            return

        # ── 1. Spot OHLCV — ALL indices + India VIX ───────────────────────────
        _status.update("running", "Fetching all index spots + India VIX from Kite…")
        all_spot_ohlcv: dict[str, dict] = {}   # underlying → {ts: candle}

        # Always fetch all known index spots
        spot_fetch_targets = [(ul, _resolve_spot_kite_token(kite, ul)) for ul in instruments]
        spot_fetch_targets = [(ul, tok) for ul, tok in spot_fetch_targets if tok]
        spot_fetch_targets.append(("INDIAVIX", _KITE_VIX_TOKEN))
        with ThreadPoolExecutor(max_workers=len(spot_fetch_targets)) as pool:
            spot_futures = {
                pool.submit(_fetch_ohlcv, kite, kite_tok, from_dt, to_dt): ul
                for ul, kite_tok in spot_fetch_targets
            }
            for fut in as_completed(spot_futures):
                if _stop_requested():
                    _status.update("stopped", "Stop requested during spot fetch")
                    return
                ul = spot_futures[fut]
                try:    all_spot_ohlcv[ul] = fut.result()
                except Exception: all_spot_ohlcv[ul] = {}
                log.info("[backfill] spot %s: %d candles", ul, len(all_spot_ohlcv[ul]))

        # ── 2. Write all spot data to DB ──────────────────────────────────────
        _status.update("running", "Writing all index spots and India VIX to DB…")
        db = MongoData()
        try:
            spot_summary = {}
            for ul_name, ohlcv in all_spot_ohlcv.items():
                kite_tok = (_KITE_VIX_TOKEN if ul_name == "INDIAVIX"
                            else _KITE_SPOT_TOKEN.get(ul_name))
                n = _write_spot_ohlcv_to_db(db, ul_name, ohlcv, kite_tok)
                spot_summary[ul_name] = n
            log.info("[backfill] spot written: %s", spot_summary)
        finally:
            db.close()

        # ── 3. Option tokens for each requested instrument ─────────────────────
        _status.update("running", "Loading option tokens from DB…")
        db = MongoData()
        try:
            if expiry_filter:
                expiry_query: str | dict = expiry_filter
            elif max_days_ahead > 0:
                cutoff = (datetime.fromisoformat(date_str) + timedelta(days=max_days_ahead)).strftime("%Y-%m-%d")
                expiry_query = {"$gte": date_str, "$lte": cutoff}
            else:
                expiry_query = {"$gte": date_str}

            token_map_by_inst: dict[str, dict[int, dict]] = {}
            expiries_by_inst:  dict[str, list] = {}
            for ul in instruments:
                exps = sorted(db._db["active_option_tokens"].distinct(
                    "expiry", {"instrument": ul, "expiry": expiry_query}
                ))
                docs = list(db._db["active_option_tokens"].find(
                    {"instrument": ul, "expiry": {"$in": exps}},
                    {"_id": 0, "instrument": 1, "expiry": 1, "strike": 1,
                     "option_type": 1, "token": 1},
                ))
                tmap: dict[int, dict] = {}
                for doc in docs:
                    try: tmap[int(doc["token"])] = doc
                    except (ValueError, KeyError): pass
                token_map_by_inst[ul] = tmap
                expiries_by_inst[ul]  = exps
                log.info("[backfill] %s: %d tokens, %d expiries", ul, len(tmap), len(exps))
        finally:
            db.close()

        # ── 4. Parallel OHLCV fetch for all option tokens ─────────────────────
        all_tokens: dict[int, dict] = {}
        for tmap in token_map_by_inst.values():
            all_tokens.update(tmap)

        total_tokens = len(all_tokens)
        if total_tokens == 0:
            avail: dict[str, list] = {}
            db2 = MongoData()
            try:
                for ul in instruments:
                    avail[ul] = sorted(db2._db["active_option_tokens"].distinct("expiry", {"instrument": ul}))
            finally:
                db2.close()
            msg = (
                f"No tokens found for {instruments} expiry_filter={expiry_filter!r}. "
                f"Available expiries: {avail}"
            )
            log.warning("[backfill] %s", msg)
            _status.update("error", msg, {"available_expiries": avail})
            return

        _status.update("running", f"Fetching OHLCV for {total_tokens} option tokens from Kite…")
        ohlcv_map: dict[int, dict] = {}
        done = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_fetch_ohlcv, kite, tok, from_dt, to_dt): tok
                       for tok in all_tokens}
            for fut in as_completed(futures):
                if _stop_requested():
                    _status.update("stopped", "Stop requested during option fetch")
                    return
                tok = futures[fut]
                try:    ohlcv_map[tok] = fut.result()
                except Exception: ohlcv_map[tok] = {}
                done += 1
                if done % 100 == 0 or done == total_tokens:
                    _status.update("running", f"Fetched {done}/{total_tokens} option tokens…")

        empty_tokens = [tok for tok, candles in ohlcv_map.items() if not candles]
        if empty_tokens:
            log.warning("[backfill] %d option tokens returned empty OHLCV", len(empty_tokens))
            _status.update("running", f"Fetched all tokens; {len(empty_tokens)} token(s) returned empty OHLCV")
            serial_fixed = 0
            for idx, tok in enumerate(empty_tokens, start=1):
                if _stop_requested():
                    _status.update("stopped", f"Stop requested during serial retry; fixed {serial_fixed}")
                    return
                try:
                    retried = _fetch_ohlcv(kite, tok, from_dt, to_dt, retries=5, retry_sleep_s=0.8)
                except Exception:
                    retried = {}
                if retried:
                    ohlcv_map[tok] = retried
                    serial_fixed += 1
                if idx % 25 == 0 or idx == len(empty_tokens):
                    _status.update("running", f"Serial retry checked {idx}/{len(empty_tokens)} empty token(s); fixed {serial_fixed}")
            empty_tokens = [tok for tok, candles in ohlcv_map.items() if not candles]
            if empty_tokens:
                log.warning("[backfill] %d option tokens still empty after serial retry", len(empty_tokens))

        # ── 5. Calculate Greeks + write option chain per instrument ───────────
        total_option_rows = 0
        for ul in instruments:
            if _stop_requested():
                _status.update("stopped", f"Stop requested before writing {ul}")
                return
            tmap = token_map_by_inst.get(ul, {})
            if not tmap:
                log.info("[backfill] %s: no option tokens, skipping chain", ul)
                continue

            _status.update("running", f"Writing option chain for {ul}…")
            ul_ohlcv_sub = {tok: ohlcv_map[tok] for tok in tmap if tok in ohlcv_map}

            spot_ohlcv = all_spot_ohlcv.get(ul, {})
            spts = sorted(spot_ohlcv.keys())
            spcs = [spot_ohlcv[t]["close"] for t in spts]

            db = MongoData()
            try:
                n = _write_option_chain_to_db(
                    db, ul, tmap, ul_ohlcv_sub, spts, spcs,
                    lambda msg: _status.update("running", msg),
                )
                total_option_rows += n
                log.info("[backfill] %s option chain rows written: %d", ul, n)
            finally:
                db.close()

        if _stop_requested():
            _status.update("stopped", "Backfill stop requested")
            return

        summary = {
            "date": date_str,
            "expiry_filter": expiry_filter or "all",
            "instruments": instruments,
            "from_ts": from_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "to_ts": to_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "latest_synced_ts": latest_synced_ts or "",
            "spot_written": spot_summary,
            "option_tokens": total_tokens,
            "empty_option_tokens": len(empty_tokens),
            "option_rows_written": total_option_rows,
            "expiries_by_inst": {ul: len(v) for ul, v in expiries_by_inst.items()},
        }
        log.info("[backfill] DONE %s", summary)
        _status.update("done", "Backfill complete", summary)

    except Exception as exc:
        log.error("[backfill] ERROR: %s", exc, exc_info=True)
        _status.update("error", str(exc))


# ── public API ────────────────────────────────────────────────────────────────

ALL_INSTRUMENTS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY", "BANKEX"]


def start_backfill(
    underlying: str,           # "NIFTY" | "SENSEX" | ... | "all"
    date_str: str | None = None,
    max_days_ahead: int = 0,   # 0 = all expiries
    workers: int = 8,
    expiry_filter: str | None = None,
    sync_from_last: bool = False,
) -> dict:
    if _status.get()["status"] == "running":
        return {"status": "already_running", "progress": _status.get()["progress"]}

    # "all" → run for every instrument that has option tokens; spot for all 5 indices
    if underlying.lower() == "all":
        instruments = ALL_INSTRUMENTS
    else:
        instruments = [underlying.upper()]

    ds = date_str or datetime.now().strftime("%Y-%m-%d")
    # If expiry is a past date and no explicit date was given, fetch data FOR that expiry date.
    # (Options expired yesterday can't be fetched using today's date range.)
    if not date_str and expiry_filter:
        expiry_date = expiry_filter[:10]
        if expiry_date < ds:
            ds = expiry_date
    safe_workers = max(1, min(int(workers or _SAFE_MAX_WORKERS), _SAFE_MAX_WORKERS))
    Thread(
        target=_run_backfill,
        args=(instruments, ds, max_days_ahead, safe_workers, expiry_filter.strip() if expiry_filter else None, sync_from_last),
        daemon=True, name="option_chain_backfill",
    ).start()
    return {"status": "started", "instruments": instruments, "date": ds,
            "max_days_ahead": max_days_ahead or "all", "workers": safe_workers,
            "expiry_filter": expiry_filter.strip() if expiry_filter else "all",
            "sync_from_last": bool(sync_from_last)}


def get_backfill_status() -> dict:
    return _status.get()


# ── MTM-path backfill (uses DB spot + parallel Kite option OHLCV) ─────────────

def _get_db_spot_series(db, underlying: str, date_str: str) -> tuple[list[str], list[float]]:
    """
    Return (sorted_timestamps, spot_closes) from option_chain_index_spot for the date.
    Works for both kite_backfill (minute bars) and kite_live (second-level ticks).
    """
    day_start = f"{date_str}T09:00:00"
    day_end   = f"{date_str}T15:30:59"
    docs = list(db._db["option_chain_index_spot"].find(
        {"underlying": underlying, "timestamp": {"$gte": day_start, "$lte": day_end}},
        {"_id": 0, "timestamp": 1, "spot_price": 1, "close": 1},
    ).sort("timestamp", 1))
    ts_list: list[str] = []
    close_list: list[float] = []
    for doc in docs:
        ts    = str(doc.get("timestamp") or "")
        price = float(doc.get("spot_price") or doc.get("close") or 0)
        if ts and price > 0:
            ts_list.append(ts)
            close_list.append(price)
    return ts_list, close_list


def _run_backfill_greeks(
    instruments: list[str],
    date_str: str,
    expiry_filter: str | None,
    workers: int,
) -> None:
    """
    MTM-path backfill:
      1. Spot price   → option_chain_index_spot DB  (falls back to Kite historical)
      2. Option OHLCV → Kite historical (parallel, same tokens as active_option_tokens)
      3. Greeks       → Black-Scholes per minute
      4. Write        → option_chain_historical_data

    Designed for instruments (like SENSEX) where Kite historical spot token may
    return empty but live-tick spot is already stored in option_chain_index_spot.
    """
    from features.mongo_data import MongoData

    label = "+".join(instruments)
    expiry_msg = f" expiry={expiry_filter}" if expiry_filter else ""
    _status.update("running", f"[greeks] Starting {label} on {date_str}{expiry_msg}")
    _stop_event.clear()

    try:
        kite = _init_kite()
        market_open_dt  = datetime.strptime(f"{date_str}T09:15:00", "%Y-%m-%dT%H:%M:%S")
        market_close_dt = datetime.strptime(f"{date_str}T15:30:00", "%Y-%m-%dT%H:%M:%S")
        to_dt = min(market_close_dt, datetime.now().replace(second=0, microsecond=0))

        # ── 1. Spot from DB; fallback to Kite historical ──────────────────────
        _status.update("running", "[greeks] Loading spot prices…")
        db = MongoData()
        try:
            all_spot_ts:    dict[str, list[str]]   = {}
            all_spot_close: dict[str, list[float]] = {}
            for ul in instruments:
                ts_list, close_list = _get_db_spot_series(db, ul, date_str)
                if ts_list:
                    log.info("[backfill_greeks] %s spot from DB: %d ticks", ul, len(ts_list))
                    all_spot_ts[ul]    = ts_list
                    all_spot_close[ul] = close_list
                else:
                    log.info("[backfill_greeks] %s spot not in DB, will fetch from Kite", ul)
        finally:
            db.close()

        # Kite fetch for instruments that had no DB spot
        missing_spot = [ul for ul in instruments if ul not in all_spot_ts]
        if missing_spot:
            _status.update("running", f"[greeks] Fetching spot from Kite for {missing_spot}…")
            spot_targets = [(ul, _resolve_spot_kite_token(kite, ul)) for ul in missing_spot]
            spot_targets = [(ul, tok) for ul, tok in spot_targets if tok]
            spot_targets.append(("INDIAVIX", _KITE_VIX_TOKEN))
            with ThreadPoolExecutor(max_workers=max(1, len(spot_targets))) as pool:
                futs = {
                    pool.submit(_fetch_ohlcv, kite, tok, market_open_dt, to_dt): ul
                    for ul, tok in spot_targets
                }
                for fut in as_completed(futs):
                    ul = futs[fut]
                    try:
                        ohlcv = fut.result()
                    except Exception:
                        ohlcv = {}
                    spts   = sorted(ohlcv.keys())
                    sclose = [ohlcv[t]["close"] for t in spts]
                    all_spot_ts[ul]    = spts
                    all_spot_close[ul] = sclose
                    # Also write spot to DB for future runs
                    if ul in missing_spot and ohlcv:
                        kite_tok = _KITE_VIX_TOKEN if ul == "INDIAVIX" else _KITE_SPOT_TOKEN.get(ul)
                        db2 = MongoData()
                        try:
                            _write_spot_ohlcv_to_db(db2, ul, ohlcv, kite_tok)
                        finally:
                            db2.close()
                    log.info("[backfill_greeks] %s spot from Kite: %d bars", ul, len(spts))

        # ── 2. Option tokens from DB ──────────────────────────────────────────
        _status.update("running", "[greeks] Loading option tokens…")
        expiry_query: str | dict = expiry_filter if expiry_filter else {"$gte": date_str}
        db = MongoData()
        try:
            token_map_by_inst: dict[str, dict[int, dict]] = {}
            expiries_by_inst:  dict[str, list[str]]       = {}
            for ul in instruments:
                exps = sorted(db._db["active_option_tokens"].distinct(
                    "expiry", {"instrument": ul, "expiry": expiry_query}
                ))
                docs = list(db._db["active_option_tokens"].find(
                    {"instrument": ul, "expiry": {"$in": exps}},
                    {"_id": 0, "expiry": 1, "strike": 1, "option_type": 1, "token": 1},
                ))
                tmap: dict[int, dict] = {}
                for doc in docs:
                    try:
                        tmap[int(doc["token"])] = doc
                    except (ValueError, KeyError):
                        pass
                token_map_by_inst[ul] = tmap
                expiries_by_inst[ul]  = exps
                log.info("[backfill_greeks] %s: %d tokens, expiries=%s", ul, len(tmap), exps)
        finally:
            db.close()

        # ── 3. Parallel OHLCV fetch for all option tokens ─────────────────────
        all_tokens: dict[int, dict] = {}
        for tmap in token_map_by_inst.values():
            all_tokens.update(tmap)
        total_tokens = len(all_tokens)

        if total_tokens == 0:
            avail: dict[str, list] = {}
            db2 = MongoData()
            try:
                for ul in instruments:
                    avail[ul] = sorted(db2._db["active_option_tokens"].distinct("expiry", {"instrument": ul}))
            finally:
                db2.close()
            msg = (
                f"No tokens found for {instruments} expiry_filter={expiry_filter!r}. "
                f"Available expiries: {avail}"
            )
            log.warning("[backfill_greeks] %s", msg)
            _status.update("error", msg, {"available_expiries": avail})
            return

        # ── 3. Batch fetch: DB first, Kite only for missing tokens ───────────────
        _MTM_BATCH = 1500
        token_list = list(all_tokens.keys())
        batches = [token_list[i : i + _MTM_BATCH] for i in range(0, total_tokens, _MTM_BATCH)]
        ohlcv_map: dict[int, dict] = {}
        day_start_ts = f"{date_str}T09:15:00"
        day_end_ts   = to_dt.strftime("%Y-%m-%dT%H:%M:%S")

        for b_idx, batch in enumerate(batches, 1):
            if _stop_requested():
                _status.update("stopped", "[greeks] Stop requested during batch fetch")
                return
            _status.update("running",
                f"[greeks] Batch {b_idx}/{len(batches)}: checking DB for {len(batch)} tokens…")

            # ── 3a. Which tokens already have OHLCV in DB for this date? ─────
            db = MongoData()
            try:
                existing_tok_strs: set[str] = set(
                    db._db["option_chain_historical_data"].distinct(
                        "token",
                        {"token": {"$in": [str(t) for t in batch]},
                         "timestamp": {"$gte": day_start_ts, "$lte": day_end_ts}},
                    )
                )
                # Load full OHLCV series from DB for tokens that already exist
                if existing_tok_strs:
                    for doc in db._db["option_chain_historical_data"].find(
                        {"token": {"$in": list(existing_tok_strs)},
                         "timestamp": {"$gte": day_start_ts, "$lte": day_end_ts}},
                        {"_id": 0, "token": 1, "timestamp": 1,
                         "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "oi": 1},
                    ).sort("timestamp", 1):
                        tok_int = int(doc["token"])
                        ts = doc["timestamp"]
                        if tok_int not in ohlcv_map:
                            ohlcv_map[tok_int] = {}
                        c = float(doc.get("close") or 0)
                        ohlcv_map[tok_int][ts] = {
                            "open":   float(doc.get("open")   or c),
                            "high":   float(doc.get("high")   or c),
                            "low":    float(doc.get("low")    or c),
                            "close":  c,
                            "volume": int(doc.get("volume") or doc.get("oi") or 0),
                            "oi":     int(doc.get("oi")     or 0),
                        }
            finally:
                db.close()

            # ── 3b. Kite fetch for tokens NOT in DB ───────────────────────────
            kite_batch = [t for t in batch if str(t) not in existing_tok_strs]
            if kite_batch:
                _status.update("running",
                    f"[greeks] Batch {b_idx}/{len(batches)}: "
                    f"fetching {len(kite_batch)} tokens from Kite "
                    f"({len(existing_tok_strs)} already in DB)…")
                kite_done = 0
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futs = {
                        pool.submit(_fetch_ohlcv, kite, tok, market_open_dt, to_dt): tok
                        for tok in kite_batch
                    }
                    for fut in as_completed(futs):
                        if _stop_requested():
                            _status.update("stopped", "[greeks] Stop requested during Kite fetch")
                            return
                        tok = futs[fut]
                        try:
                            ohlcv_map[tok] = fut.result()
                        except Exception:
                            ohlcv_map[tok] = {}
                        kite_done += 1
                        if kite_done % 100 == 0 or kite_done == len(kite_batch):
                            _status.update("running",
                                f"[greeks] Batch {b_idx}: Kite {kite_done}/{len(kite_batch)}…")
            else:
                log.info("[backfill_greeks] batch %d: all %d tokens from DB cache", b_idx, len(batch))

        empty_count = sum(1 for v in ohlcv_map.values() if not v)
        if empty_count:
            log.warning("[backfill_greeks] %d tokens returned empty OHLCV", empty_count)

        # ── 4. Greeks + write per instrument ─────────────────────────────────
        total_rows = 0
        for ul in instruments:
            if _stop_requested():
                _status.update("stopped", f"[greeks] Stop before writing {ul}")
                return
            tmap = token_map_by_inst.get(ul, {})
            if not tmap:
                log.info("[backfill_greeks] %s: no tokens, skipping", ul)
                continue

            spts   = all_spot_ts.get(ul, [])
            spcs   = all_spot_close.get(ul, [])
            ul_ohlcv = {tok: ohlcv_map.get(tok, {}) for tok in tmap}

            _status.update("running", f"[greeks] Writing option chain for {ul}…")
            db = MongoData()
            try:
                n = _write_option_chain_to_db(
                    db, ul, tmap, ul_ohlcv, spts, spcs,
                    lambda msg: _status.update("running", msg),
                )
                total_rows += n
                log.info("[backfill_greeks] %s: %d rows written", ul, n)
            finally:
                db.close()

        if _stop_requested():
            _status.update("stopped", "[greeks] Backfill stopped")
            return

        summary = {
            "mode": "greeks_backfill",
            "date": date_str,
            "expiry_filter": expiry_filter or "all",
            "instruments": instruments,
            "to_ts": to_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "option_tokens": total_tokens,
            "empty_option_tokens": empty_count,
            "option_rows_written": total_rows,
            "expiries_by_inst": {ul: len(v) for ul, v in expiries_by_inst.items()},
        }
        log.info("[backfill_greeks] DONE %s", summary)
        _status.update("done", "[greeks] Backfill complete", summary)

    except Exception as exc:
        log.error("[backfill_greeks] ERROR: %s", exc, exc_info=True)
        _status.update("error", str(exc))


def start_backfill_greeks(
    underlying: str,
    date_str: str | None = None,
    workers: int = 8,
    expiry_filter: str | None = None,
) -> dict:
    """
    MTM-path Greeks backfill.
    Always saves ALL expiries for the instrument on the given date.
    Spot is read from DB first; falls back to Kite historical if DB has no data.
    """
    if _status.get()["status"] == "running":
        return {"status": "already_running", "progress": _status.get()["progress"]}

    instruments = ALL_INSTRUMENTS if underlying.lower() == "all" else [underlying.upper()]
    ds = date_str or datetime.now().strftime("%Y-%m-%d")
    safe_workers = max(1, min(int(workers or _SAFE_MAX_WORKERS), _SAFE_MAX_WORKERS))

    Thread(
        target=_run_backfill_greeks,
        args=(instruments, ds, None, safe_workers),   # expiry_filter always None — all expiries
        daemon=True, name="option_chain_backfill_greeks",
    ).start()
    return {
        "status": "started", "mode": "greeks_backfill",
        "instruments": instruments, "date": ds,
        "workers": safe_workers,
        "expiry_filter": expiry_filter.strip() if expiry_filter else "all",
    }


def stop_backfill() -> dict:
    current = _status.get()
    if current.get("status") != "running":
        return {"status": "not_running", "progress": current.get("progress") or ""}
    _stop_event.set()
    _status.update("stopping", "Stop requested")
    return {"status": "stopping", "progress": "Stop requested"}


# ── Quote-snapshot: bulk kite.quote() → DB in seconds ─────────────────────────

def _execute_quote_snapshot(
    kite,
    instruments: list[str],
    expiry_filter: str | None,
    now_ts: str,
    *,
    stop_check=None,
) -> dict:
    """
    Core quote-snapshot logic. Called by both the manual endpoint and the live scheduler.

    Returns summary dict:
      { option_tokens, quotes_fetched, option_rows_written, expiries_by_inst, spot_by_inst }

    stop_check: optional callable → bool; if returns True the run aborts early.
    """
    from features.mongo_data import MongoData
    from pymongo import UpdateOne

    date_str = now_ts[:10]
    _stop = stop_check or (lambda: False)

    # ── 1. Option tokens from DB ──────────────────────────────────────────────
    expiry_query: str | dict = expiry_filter if expiry_filter else {"$gte": date_str}
    db = MongoData()
    try:
        all_tokens: dict[str, dict] = {}
        token_map_by_inst: dict[str, dict[str, dict]] = {}
        expiries_by_inst:  dict[str, list[str]] = {}
        for ul in instruments:
            exps = sorted(db._db["active_option_tokens"].distinct(
                "expiry", {"instrument": ul, "expiry": expiry_query}
            ))
            docs = list(db._db["active_option_tokens"].find(
                {"instrument": ul, "expiry": {"$in": exps}},
                {"_id": 0, "expiry": 1, "strike": 1, "option_type": 1, "token": 1},
            ))
            tmap: dict[str, dict] = {}
            for doc in docs:
                tok_str = str(doc.get("token") or "").strip()
                if tok_str:
                    tmap[tok_str] = doc
            token_map_by_inst[ul] = tmap
            expiries_by_inst[ul]  = exps
            all_tokens.update(tmap)
    finally:
        db.close()

    total_tokens = len(all_tokens)
    if total_tokens == 0:
        log.warning("[quote_snap] no tokens for instruments=%s expiry=%s", instruments, expiry_filter)
        return {"option_tokens": 0, "quotes_fetched": 0, "option_rows_written": 0,
                "expiries_by_inst": expiries_by_inst, "spot_by_inst": {}}

    # ── 2. Spot prices (all requested indices + India VIX always) ─────────────
    spot_by_inst: dict[str, float] = {}

    # Integer-token instruments (NSE indices + VIX)
    _int_tok_map: dict[str, int] = {
        ul: _KITE_SPOT_TOKEN[ul] for ul in instruments if ul in _KITE_SPOT_TOKEN
    }
    _int_tok_map["INDIAVIX"] = _KITE_VIX_TOKEN   # always fetch VIX

    try:
        spot_q = kite.quote(list(_int_tok_map.values())) or {}
        # Response keys are trading symbols; look up price via instrument_token in values
        tok_to_price: dict[int, float] = {}
        for _sym, q_val in spot_q.items():
            tok_int = int(q_val.get("instrument_token") or 0)
            price   = float(q_val.get("last_price") or 0)
            if tok_int and price:
                tok_to_price[tok_int] = price
        for ul, kite_tok in _int_tok_map.items():
            price = tok_to_price.get(kite_tok, 0)
            if price > 0:
                spot_by_inst[ul] = price
    except Exception as exc:
        log.warning("[quote_snap] spot quote (int tokens) error: %s", exc)

    # Symbol-based instruments (BSE indices like BANKEX)
    _sym_map: dict[str, str] = {
        ul: _KITE_SPOT_SYMBOL[ul] for ul in instruments if ul in _KITE_SPOT_SYMBOL
    }
    if _sym_map:
        try:
            sym_q = kite.quote(list(_sym_map.values())) or {}
            for ul, sym in _sym_map.items():
                price = float((sym_q.get(sym) or {}).get("last_price") or 0)
                if price > 0:
                    spot_by_inst[ul] = price
        except Exception as exc:
            log.warning("[quote_snap] spot quote (symbols) error: %s", exc)

    log.info("[quote_snap] spots fetched: %s", {ul: round(s, 2) for ul, s in spot_by_inst.items()})

    # Save all spots (indices + VIX) to option_chain_index_spot
    if spot_by_inst:
        db = MongoData()
        try:
            spot_ops = [
                UpdateOne(
                    {"underlying": ul, "timestamp": now_ts},
                    {"$set": {
                        "underlying": ul, "timestamp": now_ts,
                        "token":      _NSE_TOKEN.get(ul, f"SPOT_{ul}"),
                        "kite_token": str(_int_tok_map.get(ul) or _KITE_SPOT_TOKEN.get(ul) or ""),
                        "close": spot, "spot_price": spot,
                        "source": "kite_quote_snap",
                    }},
                    upsert=True,
                )
                for ul, spot in spot_by_inst.items()
            ]
            db._db["option_chain_index_spot"].bulk_write(spot_ops, ordered=False)
            log.info("[quote_snap] spot saved: %s", {ul: round(s, 2) for ul, s in spot_by_inst.items()})
        finally:
            db.close()

    # ── 3. Bulk quote option tokens (500 per Kite call) ───────────────────────
    token_list = list(all_tokens.keys())
    quote_map: dict[str, dict] = {}

    for batch_start in range(0, total_tokens, 500):
        if _stop():
            break
        batch_int = [int(t) for t in token_list[batch_start: batch_start + 500] if t]
        try:
            batch_q = kite.quote(batch_int) or {}
            for _sym, q_val in batch_q.items():
                tok_str = str(q_val.get("instrument_token") or "").strip()
                if tok_str:
                    quote_map[tok_str] = q_val
        except Exception as exc:
            log.warning("[quote_snap] quote batch i=%d error: %s", batch_start, exc)

    log.info("[quote_snap] quotes=%d/%d ts=%s", len(quote_map), total_tokens, now_ts)

    # ── 4. Greeks + write per instrument ─────────────────────────────────────
    total_rows = 0
    for ul in instruments:
        if _stop():
            break
        tmap = token_map_by_inst.get(ul, {})
        spot = spot_by_inst.get(ul, 0.0)
        if not tmap:
            continue

        q_yield = _DIV_Q.get(ul, 0.01)

        rows: list[dict] = []
        for tok_str, doc in tmap.items():
            q_val    = quote_map.get(tok_str) or {}
            ltp      = float(q_val.get("last_price") or 0)
            if ltp == 0:
                ltp  = float((q_val.get("ohlc") or {}).get("close") or 0)
            oi       = int(q_val.get("oi") or 0)
            expiry   = str(doc.get("expiry") or "")[:10]
            strike   = float(doc.get("strike") or 0)
            opt_type = str(doc.get("option_type") or "").upper()
            if not expiry or not strike or opt_type not in ("CE", "PE"):
                continue
            rows.append({"tok": tok_str, "expiry": expiry, "strike": strike,
                         "type": opt_type, "close": ltp, "oi": oi})

        if not rows:
            continue

        # vectorized Greeks
        n = len(rows)
        if _NUMPY_OK and spot > 0:
            closes_a  = _np.array([r["close"]  for r in rows], dtype=_np.float64)
            strikes_a = _np.array([r["strike"] for r in rows], dtype=_np.float64)
            is_call_a = _np.array([r["type"] == "CE" for r in rows])
            T_a       = _np.array([_tte(r["expiry"], now_ts) for r in rows], dtype=_np.float64)
            valid     = (closes_a > 0) & (strikes_a > 0) & (T_a > 0)
            ivs_a     = _np.zeros(n)
            if valid.any():
                ivs_a[valid] = _calc_iv_vec(
                    closes_a[valid], spot, strikes_a[valid], T_a[valid],
                    _RISK_FREE, is_call_a[valid], q_yield,
                )
            iv_ok = ivs_a > 0
            deltas_a = _np.zeros(n); gammas_a = _np.zeros(n)
            thetas_a = _np.zeros(n); vegas_a  = _np.zeros(n)
            if iv_ok.any():
                d, g, th, ve = _calc_greeks_vec(
                    spot, strikes_a[iv_ok], T_a[iv_ok],
                    _RISK_FREE, ivs_a[iv_ok], is_call_a[iv_ok], q_yield,
                )
                deltas_a[iv_ok] = d;  gammas_a[iv_ok] = g
                thetas_a[iv_ok] = th; vegas_a[iv_ok]  = ve
        else:
            ivs_a = [0.0]*n; deltas_a=[0.0]*n; gammas_a=[0.0]*n
            thetas_a=[0.0]*n; vegas_a=[0.0]*n
            if spot > 0:
                for i, r in enumerate(rows):
                    iv = _calc_iv(r["close"], spot, r["strike"],
                                  _tte(r["expiry"], now_ts), _RISK_FREE, r["type"], q_yield)
                    ivs_a[i] = iv
                    if iv > 0:
                        g = _calc_greeks(spot, r["strike"], _tte(r["expiry"], now_ts),
                                         _RISK_FREE, iv, r["type"], q_yield)
                        deltas_a[i] = g["delta"]; gammas_a[i] = g["gamma"]
                        thetas_a[i] = g["theta"]; vegas_a[i]  = g["vega"]

        ops: list = []
        db = MongoData()
        try:
            for i, r in enumerate(rows):
                iv = float(ivs_a[i]); delta = float(deltas_a[i]); gamma = float(gammas_a[i])
                theta = float(thetas_a[i]); vega = float(vegas_a[i])
                ops.append(UpdateOne(
                    {"underlying": ul, "token": r["tok"], "timestamp": now_ts},
                    {"$set": {
                        "underlying": ul, "expiry": r["expiry"],
                        "strike": r["strike"], "type": r["type"],
                        "timestamp": now_ts, "token": r["tok"],
                        "open": r["close"], "high": r["close"],
                        "low":  r["close"], "close": r["close"],
                        "oi":   r["oi"],
                        "iv":    round(iv * 100, 4) if iv else None,
                        "delta": delta or None, "gamma": gamma or None,
                        "theta": theta or None, "vega":  vega  or None,
                        "source": "kite_quote_snap",
                    }},
                    upsert=True,
                ))
            if ops:
                db._db["option_chain_historical_data"].bulk_write(ops, ordered=False)
                total_rows += len(ops)
                log.info("[quote_snap] %s: %d rows @ %s", ul, len(ops), now_ts)
        finally:
            db.close()

    return {
        "option_tokens":       total_tokens,
        "quotes_fetched":      len(quote_map),
        "option_rows_written": total_rows,
        "expiries_by_inst":    {ul: len(v) for ul, v in expiries_by_inst.items()},
        "spot_by_inst":        spot_by_inst,
    }


def _run_quote_snapshot_to_db(
    instruments: list[str],
    expiry_filter: str | None,
) -> None:
    """Manual one-shot quote snapshot (runs in background thread, updates _status)."""
    label = "+".join(instruments)
    _status.update("running", f"[quote_snap] Starting {label}")
    _stop_event.clear()

    now_ts = datetime.now().replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")

    try:
        kite = _init_kite()
        summary = _execute_quote_snapshot(
            kite, instruments, expiry_filter, now_ts,
            stop_check=_stop_requested,
        )
        summary.update({"mode": "quote_snapshot", "timestamp": now_ts,
                        "instruments": instruments, "expiry_filter": expiry_filter or "all"})
        log.info("[quote_snap] done: %s", summary)
        _status.update("done", "[quote_snap] Snapshot complete", summary)
    except Exception as exc:
        log.error("[quote_snap] ERROR: %s", exc, exc_info=True)
        _status.update("error", str(exc))


def start_quote_snapshot(
    underlying: str,
    expiry_filter: str | None = None,
) -> dict:
    """
    Fast one-shot snapshot using kite.quote() bulk API (500 tokens per call).
    Stores current LTP + Greeks for all option tokens to option_chain_historical_data.
    Completes in ~5-30 s. Poll /algo/option-chain/backfill-status for progress.
    """
    if _status.get()["status"] == "running":
        return {"status": "already_running", "progress": _status.get()["progress"]}

    instruments = ALL_INSTRUMENTS if underlying.lower() == "all" else [underlying.upper()]
    Thread(
        target=_run_quote_snapshot_to_db,
        args=(instruments, expiry_filter.strip() if expiry_filter else None),
        daemon=True, name="option_chain_quote_snapshot",
    ).start()
    return {
        "status":        "started",
        "mode":          "quote_snapshot",
        "instruments":   instruments,
        "expiry_filter": expiry_filter.strip() if expiry_filter else "all",
    }


# ── Live snapshot scheduler (runs every minute during market hours) ─────────────

_MARKET_OPEN  = (9, 15)
_MARKET_CLOSE = (15, 30)


class _LiveSchedulerStatus:
    def __init__(self):
        self._lock      = Lock()
        self.running    = False
        self.run_count  = 0
        self.err_count  = 0
        self.last_ts    = ""
        self.last_rows  = 0
        self.next_ts    = ""
        self.instruments: list[str] = []

    def get(self) -> dict:
        with self._lock:
            return {
                "running":           self.running,
                "instruments":       list(self.instruments),
                "run_count":         self.run_count,
                "error_count":       self.err_count,
                "last_snapshot_ts":  self.last_ts,
                "last_rows_written": self.last_rows,
                "next_scheduled_ts": self.next_ts,
            }


_live_sched        = _LiveSchedulerStatus()
_sched_stop        = Event()
_sched_thread: Thread | None = None


def _live_snapshot_loop(instruments: list[str], expiry_filter: str | None) -> None:
    """Background loop: quote-snapshot every minute during market hours (9:15–15:30)."""
    log.info("[live_sched] started instruments=%s", instruments)
    with _live_sched._lock:
        _live_sched.running     = True
        _live_sched.instruments = list(instruments)
        _live_sched.run_count   = 0
        _live_sched.err_count   = 0

    try:
        kite = _init_kite()
    except Exception as exc:
        log.error("[live_sched] Kite init failed: %s", exc)
        with _live_sched._lock:
            _live_sched.running = False
        return

    try:
        while not _sched_stop.is_set():
            now        = datetime.now()
            h, m       = now.hour, now.minute
            in_market  = _MARKET_OPEN <= (h, m) <= _MARKET_CLOSE

            # next whole-minute boundary
            next_min = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
            with _live_sched._lock:
                _live_sched.next_ts = next_min.strftime("%Y-%m-%dT%H:%M:%S")

            if in_market:
                now_ts = now.replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")
                log.info("[live_sched] running snapshot ts=%s", now_ts)
                try:
                    result = _execute_quote_snapshot(
                        kite, instruments, expiry_filter, now_ts,
                        stop_check=_sched_stop.is_set,
                    )
                    with _live_sched._lock:
                        _live_sched.run_count += 1
                        _live_sched.last_ts    = now_ts
                        _live_sched.last_rows  = result.get("option_rows_written", 0)
                    log.info("[live_sched] done ts=%s rows=%d", now_ts, _live_sched.last_rows)
                except Exception as exc:
                    log.error("[live_sched] snapshot error: %s", exc, exc_info=True)
                    with _live_sched._lock:
                        _live_sched.err_count += 1
            else:
                log.debug("[live_sched] outside market hours (%02d:%02d)", h, m)

            # sleep until the next minute boundary (or stop signal)
            sleep_s = max(1.0, (next_min - datetime.now()).total_seconds())
            _sched_stop.wait(timeout=sleep_s)

    finally:
        with _live_sched._lock:
            _live_sched.running = False
        log.info("[live_sched] stopped")


def start_live_snapshot(
    underlying: str = "all",
    expiry_filter: str | None = None,
) -> dict:
    """
    Start the every-minute live snapshot scheduler.
    Runs _execute_quote_snapshot at each minute boundary during market hours (9:15–15:30).
    Use 'all' for all instruments. Stop with stop_live_snapshot().
    """
    global _sched_thread

    if _live_sched.running:
        return {"status": "already_running", **_live_sched.get()}

    instruments = ALL_INSTRUMENTS if underlying.lower() == "all" else [underlying.upper()]
    _sched_stop.clear()

    _sched_thread = Thread(
        target=_live_snapshot_loop,
        args=(instruments, expiry_filter.strip() if expiry_filter else None),
        daemon=True, name="live_snapshot_scheduler",
    )
    _sched_thread.start()

    return {
        "status":        "started",
        "instruments":   instruments,
        "expiry_filter": expiry_filter.strip() if expiry_filter else "all",
        "info":          "Runs every minute at market hours 09:15–15:30. Poll /algo/option-chain/live-snapshot/status",
    }


def stop_live_snapshot() -> dict:
    if not _live_sched.running:
        return {"status": "not_running"}
    _sched_stop.set()
    return {"status": "stopping", **_live_sched.get()}


def get_live_snapshot_status() -> dict:
    return _live_sched.get()
