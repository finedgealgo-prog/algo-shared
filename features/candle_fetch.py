"""Broker-agnostic OHLCV candle fetching — Dhan and Kite.

Callers pass in an already-authenticated client/access-token (this module
never loads credentials itself, so it has no dependency on any feature
module and can be imported from anywhere without circular-import risk).

Verbatim copy of algo.scanner/common/historical_data.py — that copy is
scanner-private (algo.scanner/common/ is real files, not symlinked into any
other service), so features/chart_data.py (which needs these fetchers but
must be importable from any service, not just scanner) gets its own copy
here in the already-shared features/ package instead of reaching into
scanner's private common/ module. Scanner's own copy is untouched and keeps
serving scanner's other callers exactly as before.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any

import requests

from features.broker_gateway import wait_for_dhan_slot
from features.mongo_data import MongoData

HISTORICAL_INTERVAL = "day"
HISTORICAL_CHUNK_DAYS = 2000       # Kite allows up to 2000 days per call for day interval
DHAN_HISTORICAL_CHUNK_DAYS = 365   # Dhan allows up to 1 year per call
DHAN_INTRADAY_CHUNK_DAYS = 90      # Dhan /v2/charts/intraday hard limit, any interval

# resolution (TradingView resolution string) -> (native interval string, aggregate
# factor, max days per request). 30/120/240/360/480/720 aren't native intervals
# on either broker (Kite has no 2h/4h/6h/8h/12h candle, Dhan has no 30min) so
# they're built by aggregating the nearest native interval's candles in
# aggregate_intraday_candles. "1" is each broker's own native 1-minute
# interval (Kite: "minute", already used elsewhere in this codebase — see
# shared/features/kite_broker.py; Dhan: "1", per its own intraday API's
# documented native intervals 1/5/15/25/60). "3" is native on Kite
# ("3minute", a standard documented Zerodha interval) but not on Dhan (whose
# native intervals stop at 1/5/15/25/60), so Dhan's "3" is built the same
# way "30" already is — aggregating a faster native interval by a factor.
KITE_INTRADAY_RESOLUTION_MAP: dict[str, tuple[str, int, int]] = {
    "1": ("minute", 1, 60),
    "3": ("3minute", 1, 100),
    "5": ("5minute", 1, 100),
    "15": ("15minute", 1, 200),
    "30": ("30minute", 1, 200),
    "60": ("60minute", 1, 400),
    "120": ("60minute", 2, 400),
    "240": ("60minute", 4, 400),
    "360": ("60minute", 6, 400),
    "480": ("60minute", 8, 400),
    "720": ("60minute", 12, 400),
}

DHAN_INTRADAY_RESOLUTION_MAP: dict[str, tuple[str, int, int]] = {
    "1": ("1", 1, DHAN_INTRADAY_CHUNK_DAYS),
    "3": ("1", 3, DHAN_INTRADAY_CHUNK_DAYS),
    "5": ("5", 1, DHAN_INTRADAY_CHUNK_DAYS),
    "15": ("15", 1, DHAN_INTRADAY_CHUNK_DAYS),
    "30": ("15", 2, DHAN_INTRADAY_CHUNK_DAYS),
    "60": ("60", 1, DHAN_INTRADAY_CHUNK_DAYS),
    "120": ("60", 2, DHAN_INTRADAY_CHUNK_DAYS),
    "240": ("60", 4, DHAN_INTRADAY_CHUNK_DAYS),
    "360": ("60", 6, DHAN_INTRADAY_CHUNK_DAYS),
    "480": ("60", 8, DHAN_INTRADAY_CHUNK_DAYS),
    "720": ("60", 12, DHAN_INTRADAY_CHUNK_DAYS),
}

# Running count of real broker API hits this process has made — purely for the
# [DHAN]/[KITE] log lines below, callers may also read these to log their own
# "N hits so far" context.
FEED_API_CALL_COUNTS: dict[str, int] = {"dhan": 0, "kite": 0}

_INDEX_DAILY_CACHE_COLLECTION = "scanner_index_chart_cache"
_index_daily_cache_index_ensured = False


def format_day_span(from_str: str, to_str: str) -> str:
    try:
        days = (datetime.strptime(to_str, "%Y-%m-%d") - datetime.strptime(from_str, "%Y-%m-%d")).days + 1
        return f"{days}d"
    except ValueError:
        return "?d"


def post_dhan_chart_request(
    url: str,
    body: dict[str, Any],
    access_token: str,
    *,
    _retry: int = 6,
) -> dict[str, Any]:
    """POST to a Dhan /v2/charts/* endpoint, retrying on rate-limit responses.

    DH-904 = explicit rate limit; DH-905 = sometimes rate limit in disguise.
    Shared by the daily and intraday candle fetchers below — only the request
    body and the resulting timestamp interpretation differ between them.
    """
    headers = {"access-token": access_token, "Content-Type": "application/json"}
    rate_limit_codes = {"DH-904", "DH-905"}
    from_str = str(body.get("fromDate", "?"))
    to_str = str(body.get("toDate", "?"))
    span = format_day_span(from_str, to_str)
    last_err_json: dict[str, Any] = {}
    extra_backoff = 0.0
    for attempt in range(1, _retry + 1):
        # Funnel onto the same shared clock as every other Dhan REST caller
        # (live quotes, option-chain backfills, ...) instead of pacing
        # ourselves in isolation. Dhan's historical/intraday endpoints share
        # the account-wide ~1 req/sec budget with those callers, so a burst
        # of chart requests here used to blow past the limit and get
        # DH-904/905'd even though this function's own calls were spaced out.
        wait_for_dhan_slot()
        if extra_backoff:
            time.sleep(extra_backoff)
        FEED_API_CALL_COUNTS["dhan"] += 1
        print(
            f"[DHAN] API hit #{FEED_API_CALL_COUNTS['dhan']} (attempt {attempt}/{_retry}) "
            f"interval={body.get('interval', 'EOD')} range={from_str}->{to_str} ({span})",
            flush=True,
        )
        resp = requests.post(url, json=body, headers=headers, timeout=30)
        if resp.status_code == 429:
            print(f"[DHAN] 429 (attempt {attempt}/{_retry})", flush=True)
            extra_backoff = min(8.0, extra_backoff * 2 or 2.0)
            continue
        if resp.status_code == 400:
            try:
                err_json = resp.json()
            except Exception:
                err_json = {}
            err_code = err_json.get("errorCode", "")
            if err_code in rate_limit_codes:
                last_err_json = err_json
                print(f"[DHAN] {err_code} rate-limit (attempt {attempt}/{_retry}) — raw={err_json}", flush=True)
                extra_backoff = min(8.0, extra_backoff * 2 or 2.0)
                continue
            raise Exception(f"400 — {err_json}")
        if not resp.ok:
            try:
                err_detail = resp.json()
            except Exception:
                err_detail = resp.text[:200]
            raise Exception(f"{resp.status_code} {resp.reason} — {err_detail}")
        return resp.json()
    raise Exception(f"Dhan rate-limit (DH-904/905) after {_retry} retries for url={url} — last raw error: {last_err_json}")


def candles_from_dhan_response(data: dict[str, Any], *, ist_offset: bool) -> list[dict[str, Any]]:
    opens = data.get("open") or []
    highs = data.get("high") or []
    lows = data.get("low") or []
    closes = data.get("close") or []
    volumes = data.get("volume") or []
    timestamps = data.get("timestamp") or []
    candles = []
    for i in range(len(timestamps)):
        dt = datetime.utcfromtimestamp(timestamps[i])
        if ist_offset:
            # Dhan's daily timestamps mark midnight IST — shift to midnight UTC of
            # the same trading day so strftime("%Y-%m-%d") lands on the right date.
            dt = dt + timedelta(hours=5, minutes=30)
        candles.append({
            "date": dt,
            "open": opens[i] if i < len(opens) else 0.0,
            "high": highs[i] if i < len(highs) else 0.0,
            "low": lows[i] if i < len(lows) else 0.0,
            "close": closes[i] if i < len(closes) else 0.0,
            "volume": volumes[i] if i < len(volumes) else 0,
        })
    return candles


def fetch_dhan_daily_candles(
    access_token: str,
    security_id: str,
    exchange_segment: str,
    instrument: str,
    from_date: datetime,
    to_date: datetime,
    *,
    _retry: int = 6,
) -> list[dict[str, Any]]:
    body = {
        "securityId": str(security_id),
        "exchangeSegment": exchange_segment,
        "instrument": instrument,
        "expiryCode": 0,
        "fromDate": from_date.strftime("%Y-%m-%d"),
        "toDate": to_date.strftime("%Y-%m-%d"),
    }
    data = post_dhan_chart_request("https://api.dhan.co/v2/charts/historical", body, access_token, _retry=_retry)
    return candles_from_dhan_response(data, ist_offset=True)


def fetch_dhan_intraday_candles(
    access_token: str,
    security_id: str,
    exchange_segment: str,
    instrument: str,
    interval: str,
    from_date: datetime,
    to_date: datetime,
    *,
    _retry: int = 6,
) -> list[dict[str, Any]]:
    """Minute candles from Dhan's intraday feed (native intervals: 1, 5, 15, 25, 60).

    Unlike the daily endpoint, intraday timestamps are genuine UTC instants —
    no IST-midnight offset needed, the time-of-day itself matters here.
    """
    body = {
        "securityId": str(security_id),
        "exchangeSegment": exchange_segment,
        "instrument": instrument,
        "interval": str(interval),
        "expiryCode": 0,
        "fromDate": from_date.strftime("%Y-%m-%d"),
        "toDate": to_date.strftime("%Y-%m-%d"),
    }
    data = post_dhan_chart_request("https://api.dhan.co/v2/charts/intraday", body, access_token, _retry=_retry)
    return candles_from_dhan_response(data, ist_offset=False)


def aggregate_intraday_candles(candles: list[dict[str, Any]], factor: int) -> list[dict[str, Any]]:
    """Groups consecutive native-interval candles into `factor`-sized buckets.

    Buckets reset at every trading-day boundary so a 4h/8h bar never spans two
    sessions — the last bucket of a day is simply shorter when the session
    length doesn't divide evenly by `factor` (matches how exchanges behave).
    """
    if factor <= 1:
        return candles

    buckets: list[list[dict[str, Any]]] = []
    current_day: str | None = None
    current_bucket: list[dict[str, Any]] = []

    for candle in candles:
        date_value = candle.get("date")
        if not isinstance(date_value, datetime):
            continue
        day_key = date_value.strftime("%Y-%m-%d")
        if day_key != current_day or len(current_bucket) >= factor:
            if current_bucket:
                buckets.append(current_bucket)
            current_bucket = []
            current_day = day_key
        current_bucket.append(candle)

    if current_bucket:
        buckets.append(current_bucket)

    return [
        {
            "date": bucket[0]["date"],
            "open": bucket[0]["open"],
            "high": max(c["high"] for c in bucket),
            "low": min(c["low"] for c in bucket),
            "close": bucket[-1]["close"],
            "volume": sum(c.get("volume") or 0 for c in bucket),
        }
        for bucket in buckets
    ]


def fetch_dhan_daily_index_candles_cached(
    security_id: str,
    access_token: str,
    symbol: str,
    from_date: datetime,
    to_date: datetime,
) -> list[dict[str, Any]]:
    """Daily index bars for Dhan, backed by a Mongo cache.

    /v2/charts/historical (EOD) rejects every IDX_I request with DH-905 — it
    simply doesn't support index instruments — so daily bars are derived by
    aggregating /v2/charts/intraday candles instead, capped at that endpoint's
    90-day window. That makes a chart's first load page backward through
    several real Dhan calls. Closed trading days are cached forever per
    (symbol, day) here — including a `has_data: False` placeholder on market
    holidays, so a missing weekday isn't mistaken for a gap on the next
    request — and today's still-open session is never cached, always live.
    """
    global _index_daily_cache_index_ensured
    db = MongoData()._db
    collection = db[_INDEX_DAILY_CACHE_COLLECTION]
    if not _index_daily_cache_index_ensured:
        collection.create_index([("i_symbol", 1), ("day", 1)], unique=True)
        _index_daily_cache_index_ensured = True

    today_key = datetime.utcnow().strftime("%Y-%m-%d")
    expected_days = {
        day.strftime("%Y-%m-%d")
        for day in (from_date + timedelta(days=n) for n in range((to_date - from_date).days + 1))
        if day.weekday() < 5 and day.strftime("%Y-%m-%d") != today_key
    }

    cached_docs = list(collection.find(
        {"i_symbol": symbol, "day": {"$gte": from_date.strftime("%Y-%m-%d"), "$lte": to_date.strftime("%Y-%m-%d")}},
        {"_id": 0},
    ))
    cached_days = {doc["day"] for doc in cached_docs}
    if expected_days and expected_days.issubset(cached_days):
        return [
            {
                "date": datetime.strptime(doc["day"], "%Y-%m-%d"),
                "open": doc["open"], "high": doc["high"], "low": doc["low"], "close": doc["close"],
                "volume": doc.get("volume", 0),
            }
            for doc in sorted(cached_docs, key=lambda d: d["day"])
            if doc.get("has_data", True)
        ]

    raw_candles = fetch_dhan_intraday_candles(access_token, security_id, "IDX_I", "INDEX", "60", from_date, to_date)
    daily = aggregate_intraday_candles(raw_candles, 1000)
    daily_by_day = {candle["date"].strftime("%Y-%m-%d"): candle for candle in daily}

    for day_key in expected_days:
        candle = daily_by_day.get(day_key)
        doc = (
            {"open": candle["open"], "high": candle["high"], "low": candle["low"], "close": candle["close"],
             "volume": candle.get("volume", 0), "has_data": True}
            if candle else {"has_data": False}
        )
        collection.update_one({"i_symbol": symbol, "day": day_key}, {"$set": doc}, upsert=True)

    return daily


def fetch_kite_daily_candles(
    kite,
    instrument_token: int,
    from_date: datetime,
    to_date: datetime,
) -> list[dict[str, Any]]:
    FEED_API_CALL_COUNTS["kite"] += 1
    span = format_day_span(from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"))
    print(
        f"[KITE] API hit #{FEED_API_CALL_COUNTS['kite']} interval=day "
        f"range={from_date.date()}->{to_date.date()} ({span})",
        flush=True,
    )
    candles = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_date,
        to_date=to_date,
        interval=HISTORICAL_INTERVAL,
    )
    return candles if isinstance(candles, list) else []


def fetch_kite_intraday_candles(
    kite,
    instrument_token: int,
    interval: str,
    from_date: datetime,
    to_date: datetime,
) -> list[dict[str, Any]]:
    FEED_API_CALL_COUNTS["kite"] += 1
    span = format_day_span(from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"))
    print(
        f"[KITE] API hit #{FEED_API_CALL_COUNTS['kite']} interval={interval} "
        f"range={from_date.date()}->{to_date.date()} ({span})",
        flush=True,
    )
    candles = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_date,
        to_date=to_date,
        interval=interval,
    )
    return candles if isinstance(candles, list) else []
