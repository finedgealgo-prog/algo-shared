"""Chart symbol-search + OHLCV history — the data layer behind chart_api.py's
/v1/symbol_search and /v1/symbol_historical_chart, and behind alert_checker's
indicator-bars evaluation.

Ported from algo.scanner/scanner/service.py's search_symbol_universe /
get_symbol_historical_chart_bars / get_index_historical_chart_bars, which
only chart_api.py and alert_checker.py actually needed from that module —
everything else in scanner/service.py (scoring, portfolios, EOD sync, ...) is
genuinely scanner-only and stays there untouched. Living here in the
already-shared features/ package (instead of scanner's private scanner/
package) is what lets chart_api.py — and alert_checker.py's indicator-bars
path — be mounted/imported from any service, not just algo.scanner.

This is a parallel copy, not a refactor of the scanner original: scanner's
own get_index_historical_chart_bars keeps serving scanner's own 2 other
callers (common/router.py, scanner/router.py) exactly as before, with its
own separate in-memory cache. Two intentional consequences of that:
  - The index-alias / Dhan-security-id tables below are duplicated from
    scanner/service.py's copy. If a new index is ever added there, add it
    here too — same tradeoff every algo.trade/algo.simulator/algo.order
    already makes for their own local kite_market_config broker-selection
    helpers (see e.g. algo.simulator/api.py).
  - Each process that imports this module gets its own independent
    in-memory chunk cache — no cross-process cache sharing, same as before
    (scanner's own cache was never shared with any other process either).
"""

from __future__ import annotations

import calendar
import re
import threading
import time
from datetime import datetime, timedelta
from typing import Any

from features.candle_fetch import (
    DHAN_INTRADAY_RESOLUTION_MAP as _DHAN_INTRADAY_RESOLUTION_MAP,
    HISTORICAL_CHUNK_DAYS,
    DHAN_HISTORICAL_CHUNK_DAYS,
    DHAN_INTRADAY_CHUNK_DAYS,
    KITE_INTRADAY_RESOLUTION_MAP as _KITE_INTRADAY_RESOLUTION_MAP,
    aggregate_intraday_candles as _aggregate_intraday_candles,
    fetch_dhan_daily_candles as _fetch_dhan_daily_candles,
    fetch_dhan_daily_index_candles_cached as _fetch_dhan_daily_index_candles_cached,
    fetch_dhan_intraday_candles as _fetch_dhan_intraday_candles,
    fetch_kite_daily_candles as _fetch_kite_daily_candles,
    fetch_kite_intraday_candles as _fetch_kite_intraday_candles,
)
from features.dhan_token_sync import _get_dhan_commodity_master as _load_dhan_commodity_master
from features.mongo_data import MongoData
from features.spot_atm_utils import KITE_INDEX_TOKENS

STOCKS_COLLECTION = "scanner_stocks_list"

# Dhan security IDs for indices (used when broker=dhan), verified against
# Dhan's scrip master (https://images.dhan.co/api-data/api-scrip-master.csv), segment IDX_I.
# Duplicated from scanner/service.py — see module docstring.
DHAN_INDEX_SECURITY_IDS: dict[str, int] = {
    "NIFTY": 13, "BANKNIFTY": 25, "SENSEX": 51,
    "FINNIFTY": 27, "MIDCPNIFTY": 11915,
    "NIFTY100": 17, "NIFTY200": 18, "NIFTY500": 19,
    "NIFTYMIDCAP50": 20, "NIFTYMIDCAP100": 37,
    "NIFTYNXT50": 38, "NIFTYSMLCAP100": 5,
    "INDIA_VIX": 21,
}

SCANNER_INDEX_ALIASES: dict[str, tuple[str, str]] = {
    "GOLDBEES": ("GOLDBEES", "gold_bees"),
    "GOLD_BEES": ("GOLDBEES", "gold_bees"),
    "NIFTY": ("NIFTY", "nifty_50"),
    "GOLDBEES-EQ": ("GOLDBEES", "gold_bees"),
    "NIFTY50": ("NIFTY", "nifty_50"),
    "NIFTY 50": ("NIFTY", "nifty_50"),
    "NIFTY_50": ("NIFTY", "nifty_50"),
    "NIFTY500": ("NIFTY500", "nifty_500"),
    "NIFTY 500": ("NIFTY500", "nifty_500"),
    "NIFTY_500": ("NIFTY500", "nifty_500"),
    "NIFTYMIDCAP100": ("NIFTYMIDCAP100", "nifty_midcap_100"),
    "NIFTY MIDCAP100": ("NIFTYMIDCAP100", "nifty_midcap_100"),
    "NIFTY_MIDCAP_100": ("NIFTYMIDCAP100", "nifty_midcap_100"),
    "NIFTY_MIDCAP_50": ("NIFTYMIDCAP50", "nifty_midcap_50"),
    "NIFTYMIDCAP50": ("NIFTYMIDCAP50", "nifty_midcap_50"),
    "NIFTY MIDCAP50": ("NIFTYMIDCAP50", "nifty_midcap_50"),
    "NIFTY100": ("NIFTY100", "nifty_100"),
    "NIFTY 100": ("NIFTY100", "nifty_100"),
    "NIFTY_100": ("NIFTY100", "nifty_100"),
    "NIFTYNXT50": ("NIFTYNXT50", "nifty_next_50"),
    "NIFTY NXT50": ("NIFTYNXT50", "nifty_next_50"),
    "NIFTY_NEXT_50": ("NIFTYNXT50", "nifty_next_50"),
    "NIFTYSMLCAP100": ("NIFTYSMLCAP100", "nifty_smallcap_100"),
    "NIFTY SMLCAP100": ("NIFTYSMLCAP100", "nifty_smallcap_100"),
    "NIFTY_SMALLCAP_50": ("NIFTYSMLCAP100", "nifty_smallcap_50"),
    "NIFTY_SMALLCAP_100": ("NIFTYSMLCAP100", "nifty_smallcap_100"),
    "NIFTY_SMLCAP_100": ("NIFTYSMLCAP100", "nifty_smallcap_100"),
    "NIFTYSMALLCAP100": ("NIFTYSMLCAP100", "nifty_smallcap_100"),
    "NIFTY200": ("NIFTY200", "nifty_200"),
    "NIFTY 200": ("NIFTY200", "nifty_200"),
    "NIFTY_200": ("NIFTY200", "nifty_200"),
    "BANKNIFTY": ("BANKNIFTY", "nifty_bank"),
    "NIFTY BANK": ("BANKNIFTY", "nifty_bank"),
    "NIFTY_BANK": ("BANKNIFTY", "nifty_bank"),
    "FINNIFTY": ("FINNIFTY", "nifty_fin_service"),
    "NIFTY FIN SERVICE": ("FINNIFTY", "nifty_fin_service"),
    "NIFTY_FIN_SERVICE": ("FINNIFTY", "nifty_fin_service"),
    "MIDCPNIFTY": ("MIDCPNIFTY", "nifty_mid_select"),
    "NIFTY MID SELECT": ("MIDCPNIFTY", "nifty_mid_select"),
    "NIFTY_MID_SELECT": ("MIDCPNIFTY", "nifty_mid_select"),
    "SENSEX": ("SENSEX", "sensex"),
    "INDIA VIX": ("INDIA_VIX", "india_vix"),
    "INDIA_VIX": ("INDIA_VIX", "india_vix"),
    "INDIAVIX": ("INDIA_VIX", "india_vix"),
}

DEFAULT_SCANNER_INDEXES: list[dict[str, Any]] = [
    {"lookup_symbol": "GOLDBEES", "raw_symbol": "GOLDBEES-EQ", "normalized_symbol": "gold_bees", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTY", "raw_symbol": "NIFTY50", "normalized_symbol": "nifty_50", "fetch_mode": "token"},
    {"lookup_symbol": "NIFTY500", "raw_symbol": "NIFTY 500", "normalized_symbol": "nifty_500", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTYMIDCAP100", "raw_symbol": "NIFTY MIDCAP 100", "normalized_symbol": "nifty_midcap_100", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTYMIDCAP50", "raw_symbol": "NIFTY MIDCAP 50", "normalized_symbol": "nifty_midcap_50", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTY100", "raw_symbol": "NIFTY 100", "normalized_symbol": "nifty_100", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTYNXT50", "raw_symbol": "NIFTY NEXT 50", "normalized_symbol": "nifty_next_50", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTYSMLCAP100", "raw_symbol": "NIFTY SMLCAP 100", "normalized_symbol": "nifty_smallcap_100", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTY200", "raw_symbol": "NIFTY 200", "normalized_symbol": "nifty_200", "fetch_mode": "nse"},
    {"lookup_symbol": "BANKNIFTY", "raw_symbol": "BANKNIFTY", "normalized_symbol": "nifty_bank", "fetch_mode": "token"},
    {"lookup_symbol": "FINNIFTY", "raw_symbol": "FINNIFTY", "normalized_symbol": "nifty_fin_service", "fetch_mode": "token"},
    {"lookup_symbol": "MIDCPNIFTY", "raw_symbol": "MIDCPNIFTY", "normalized_symbol": "nifty_mid_select", "fetch_mode": "token"},
    {"lookup_symbol": "SENSEX", "raw_symbol": "SENSEX", "normalized_symbol": "sensex", "fetch_mode": "token"},
    {"lookup_symbol": "INDIA_VIX", "raw_symbol": "INDIA VIX", "normalized_symbol": "india_vix", "fetch_mode": "token"},
]

_index_history_chunk_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_INDEX_HISTORY_CACHE_TTL_SECONDS = 20.0
_index_history_chunk_inflight_lock = threading.Lock()
_index_history_chunk_inflight: dict[str, dict[str, Any]] = {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if isinstance(value, str):
            value = value.strip().replace(",", "").replace("%", "")
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    if numeric != numeric or numeric in (float("inf"), float("-inf")):  # NaN/inf, no numpy dependency needed here
        return default
    return numeric


def _epoch_millis(value: datetime) -> int:
    """Correct epoch-millis for both naive-UTC datetimes (Dhan) and
    tz-aware Asia/Kolkata datetimes (Kite's historical_data SDK) — using
    calendar.timegm on a tz-aware value would silently ignore its tzinfo
    and misread local wall-clock digits as UTC, shifting intraday bars by
    5:30h.
    """
    if value.tzinfo is not None:
        return int(value.timestamp() * 1000)
    return calendar.timegm(value.timetuple()) * 1000


def _load_kite_credentials() -> tuple[str, str]:
    db = MongoData()._db
    doc = db["kite_market_config"].find_one(
        {"broker": "kite"},
        {"api_key": 1, "access_token": 1},
    ) or {}
    api_key = str(doc.get("api_key") or "").strip()
    access_token = str(doc.get("access_token") or "").strip()
    return api_key, access_token


def _build_kite_client_from_config():
    from kiteconnect import KiteConnect  # type: ignore

    api_key, access_token = _load_kite_credentials()
    if not access_token:
        raise ValueError("Kite access token not configured.")
    if not api_key:
        raise ValueError("Kite api key not configured.")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def _load_dhan_credentials() -> tuple[str, str]:
    """Returns (client_id, access_token) for Dhan from kite_market_config."""
    db = MongoData()._db
    cfg = db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
    client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
    access_token = str(cfg.get("access_token") or "").strip()
    return client_id, access_token


def _load_dhan_credentials_any() -> tuple[str, str]:
    """Dhan (client_id, access_token) regardless of which broker is currently
    marked active — MCX commodities have no Kite path anywhere in this
    codebase, so commodity bars always go through Dhan even when Kite is the
    app's active broker, and can't gate on kite_market_config.enabled the way
    _load_dhan_credentials does.
    """
    db = MongoData()._db
    cfg = db["kite_market_config"].find_one({"broker": "dhan"}) or {}
    client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
    access_token = str(cfg.get("access_token") or "").strip()
    return client_id, access_token


def _get_active_market_data_broker() -> str:
    """"kite" or "dhan" — whichever is marked enabled in kite_market_config.

    The chart feed should follow whichever broker the user has actually logged
    into (BrokerLogin page sets this), not a hardcoded choice — Kite and Dhan
    use different instrument-id schemes and have different per-interval date
    range limits, so the right one has to be picked before fetching.
    """
    db = MongoData()._db
    enabled_doc = db["kite_market_config"].find_one({"enabled": True}, {"_id": 0, "broker": 1})
    broker = str((enabled_doc or {}).get("broker") or "").strip().lower()
    return broker if broker in {"kite", "dhan"} else "dhan"


def _resolve_stock_kite_token(stock: dict[str, Any]) -> tuple[str, str]:
    symbol = str(stock.get("symbol") or stock.get("tradingsymbol") or "").strip().upper()
    raw_token = str(
        stock.get("kite_token")
        or stock.get("token")
        or stock.get("tokens")
        or stock.get("instrument_token")
        or stock.get("exchange_token")
        or stock.get("code")
        or ""
    ).strip()
    try:
        token = str(int(float(raw_token))) if raw_token else ""
    except (ValueError, TypeError):
        token = raw_token
    return symbol, token


def _resolve_stock_dhan_security_id(stock: dict[str, Any]) -> tuple[str, str]:
    """Returns (symbol, nse_security_id) for a stock record. NSE only."""
    symbol = str(stock.get("symbol") or stock.get("tradingsymbol") or "").strip().upper()

    def _clean(val: Any) -> str:
        raw = str(val or "").strip()
        try:
            return str(int(float(raw))) if raw else ""
        except (ValueError, TypeError):
            return raw

    nse_id = _clean(stock.get("dhan_security_id"))
    return symbol, nse_id


def search_symbol_universe(query: str, limit: int = 30) -> list[dict[str, Any]]:
    """Combined index + stock + commodity list for the chart's TradingView
    "Symbol Search" popup. Empty query returns a curated default (every
    chartable index, then stocks alphabetically, then MCX commodities); a
    non-empty query substring-matches across all three, indices ranked first.

    `symbol` in each result is the ticker the chart's datafeed/backend uses
    going forward — for indices that's the same normalized form
    get_index_historical_chart_bars already expects (e.g. "nifty_50").
    """
    query_norm = str(query or "").strip().lower()
    limit = max(1, min(int(limit or 30), 100))

    def matches(*fields: str) -> bool:
        if not query_norm:
            return True
        return any(query_norm in str(field or "").lower() for field in fields)

    results: list[dict[str, Any]] = []

    for entry in DEFAULT_SCANNER_INDEXES:
        label = str(entry.get("raw_symbol") or entry.get("lookup_symbol") or "")
        if matches(label, str(entry.get("lookup_symbol") or "")):
            results.append({
                "symbol": entry["normalized_symbol"],
                "full_name": label,
                "description": label,
                "exchange": "NSE",
                "type": "index",
            })

    if len(results) < limit:
        db = MongoData()._db
        stock_filter: dict[str, Any] = {}
        if query_norm:
            pattern = re.escape(query_norm)
            stock_filter = {"$or": [
                {"symbol": {"$regex": pattern, "$options": "i"}},
                {"tradingsymbol": {"$regex": pattern, "$options": "i"}},
                {"company_name": {"$regex": pattern, "$options": "i"}},
            ]}
        seen_stock_symbols: set[str] = set()
        for row in db[STOCKS_COLLECTION].find(
            stock_filter,
            {"_id": 0, "symbol": 1, "tradingsymbol": 1, "company_name": 1, "exchange": 1},
        ).sort("symbol", 1).limit((limit - len(results)) * 3):
            symbol = str(row.get("symbol") or row.get("tradingsymbol") or "").strip().upper()
            if not symbol or symbol in seen_stock_symbols:
                continue
            seen_stock_symbols.add(symbol)
            results.append({
                "symbol": symbol,
                "full_name": str(row.get("company_name") or symbol),
                "description": symbol,
                "exchange": str(row.get("exchange") or "NSE"),
                "type": "stock",
            })
            if len(results) >= limit:
                break

    if len(results) < limit:
        try:
            commodity_master = _load_dhan_commodity_master()
        except Exception:
            commodity_master = {}
        for underlying in sorted(commodity_master.keys()):
            if len(results) >= limit:
                break
            if matches(underlying):
                results.append({
                    "symbol": underlying,
                    "full_name": f"{underlying} (MCX)",
                    "description": underlying,
                    "exchange": "MCX",
                    "type": "commodity",
                })

    return results[:limit]


def get_index_historical_chart_bars(
    i_symbol: str,
    from_ts: int | None = None,
    to_ts: int | None = None,
    resolution: str = "1D",
) -> dict[str, Any]:
    """OHLCV bars for a scanner index, sourced live from whichever broker
    (Kite or Dhan) is currently marked enabled in kite_market_config.

    Important: this endpoint intentionally fetches only one broker-sized chunk
    per request — the max date span depends on both the active broker and the
    interval (Kite: 100/200/400/2000 days for 5min/15-30min/60min/day; Dhan:
    90 days flat for any intraday interval, 365 for daily — see
    _KITE_INTRADAY_RESOLUTION_MAP / _DHAN_INTRADAY_RESOLUTION_MAP /
    *_HISTORICAL_CHUNK_DAYS). The frontend pages backwards/forwards with
    continued requests for adjacent ranges instead of asking for multi-year
    history in one call. Weekly/monthly bars are derived by the frontend from
    daily bars, not fetched here — neither broker has a native week/month
    intraday-style interval worth a round trip for.
    """
    symbol = str(i_symbol or "").strip().lower()
    if not symbol:
        raise ValueError("i_symbol is required.")
    resolution = str(resolution or "1D").strip()

    alias_name = next(
        (broker_symbol for broker_symbol, normalized_symbol in SCANNER_INDEX_ALIASES.values() if normalized_symbol == symbol),
        "",
    )
    if not alias_name:
        raise ValueError(f"No index alias configured for i_symbol={symbol}")

    now_utc = datetime.utcnow()
    from_date = datetime.utcfromtimestamp(from_ts) if from_ts is not None else datetime(2018, 1, 1)
    to_date = datetime.utcfromtimestamp(to_ts) if to_ts is not None else now_utc
    if from_date > to_date:
        raise ValueError("'from' must be less than or equal to 'to'.")

    broker = _get_active_market_data_broker()
    is_intraday = resolution in _KITE_INTRADAY_RESOLUTION_MAP or resolution in _DHAN_INTRADAY_RESOLUTION_MAP

    if not is_intraday:
        if broker == "kite":
            max_chunk_days = HISTORICAL_CHUNK_DAYS
        else:
            # Dhan's /v2/charts/historical (EOD) endpoint rejects every IDX_I
            # request with DH-905 regardless of params/date-range — confirmed
            # by direct testing, it simply doesn't support index instruments,
            # only equities/derivatives. So daily index bars are derived below
            # from /v2/charts/intraday instead, which is capped at 90 days.
            max_chunk_days = DHAN_INTRADAY_CHUNK_DAYS
    elif broker == "kite":
        _, _, max_chunk_days = _KITE_INTRADAY_RESOLUTION_MAP[resolution]
    else:
        _, _, max_chunk_days = _DHAN_INTRADAY_RESOLUTION_MAP[resolution]

    max_chunk_span = timedelta(days=max_chunk_days - 1)
    effective_from = from_date
    effective_to = to_date
    if (effective_to - effective_from) > max_chunk_span:
        effective_from = max(from_date, effective_to - max_chunk_span)

    cache_key = f"{symbol}:{resolution}:{broker}:{int(effective_from.timestamp())}:{int(effective_to.timestamp())}"
    cached = _index_history_chunk_cache.get(cache_key)
    if cached and (time.time() - cached[0]) <= _INDEX_HISTORY_CACHE_TTL_SECONDS:
        return cached[1]

    wait_event: threading.Event | None = None
    wait_record: dict[str, Any] | None = None
    is_request_leader = False
    with _index_history_chunk_inflight_lock:
        inflight = _index_history_chunk_inflight.get(cache_key)
        if inflight:
            wait_record = inflight
            wait_event = inflight["event"]
        else:
            wait_event = threading.Event()
            _index_history_chunk_inflight[cache_key] = {"event": wait_event, "result": None, "error": None}
            is_request_leader = True

    if not is_request_leader:
        wait_event.wait()
        if wait_record and wait_record.get("error") is not None:
            raise wait_record["error"]
        if wait_record and wait_record.get("result") is not None:
            return wait_record["result"]
        cached = _index_history_chunk_cache.get(cache_key)
        if cached and (time.time() - cached[0]) <= _INDEX_HISTORY_CACHE_TTL_SECONDS:
            return cached[1]
        raise Exception("Historical chart request finished without cache result.")

    try:
        if broker == "kite":
            instrument_token = int(KITE_INDEX_TOKENS.get(alias_name) or 0)
            if not instrument_token:
                raise ValueError(f"Kite instrument token not configured for i_symbol={symbol}")
            kite = _build_kite_client_from_config()
            if is_intraday:
                native_interval, factor, _ = _KITE_INTRADAY_RESOLUTION_MAP[resolution]
                candles = _fetch_kite_intraday_candles(kite, instrument_token, native_interval, effective_from, effective_to)
                candles = _aggregate_intraday_candles(candles, factor)
            else:
                candles = _fetch_kite_daily_candles(kite, instrument_token, effective_from, effective_to)
        else:
            dhan_security_id = str(DHAN_INDEX_SECURITY_IDS.get(alias_name, 0) or "").strip()
            if not dhan_security_id or dhan_security_id == "0":
                raise ValueError(f"Dhan security id not configured for i_symbol={symbol}")
            client_id, access_token = _load_dhan_credentials()
            if not access_token:
                raise ValueError("Active Dhan access token not configured.")
            if not client_id:
                raise ValueError("Active Dhan client id not configured.")
            if is_intraday:
                native_interval, factor, _ = _DHAN_INTRADAY_RESOLUTION_MAP[resolution]
                candles = _fetch_dhan_intraday_candles(
                    access_token, dhan_security_id, "IDX_I", "INDEX", native_interval, effective_from, effective_to
                )
                candles = _aggregate_intraday_candles(candles, factor)
            else:
                candles = _fetch_dhan_daily_index_candles_cached(
                    dhan_security_id, access_token, symbol, effective_from, effective_to
                )

        if is_intraday:
            bars = [
                {
                    "time": _epoch_millis(candle["date"]),
                    "open": _safe_float(candle.get("open")),
                    "high": _safe_float(candle.get("high")),
                    "low": _safe_float(candle.get("low")),
                    "close": _safe_float(candle.get("close")),
                    "volume": _safe_float(candle.get("volume")),
                }
                for candle in sorted(
                    (c for c in candles if isinstance(c.get("date"), datetime)),
                    key=lambda c: c["date"],
                )
            ]
        else:
            unique_by_day: dict[str, dict[str, Any]] = {}
            for candle in candles:
                date_value = candle.get("date")
                if not isinstance(date_value, datetime):
                    continue
                day_key = date_value.strftime("%Y-%m-%d")
                unique_by_day[day_key] = candle

            bars = [
                {
                    "time": calendar.timegm(datetime.strptime(day_key, "%Y-%m-%d").timetuple()) * 1000,
                    "open": _safe_float(candle.get("open")),
                    "high": _safe_float(candle.get("high")),
                    "low": _safe_float(candle.get("low")),
                    "close": _safe_float(candle.get("close")),
                    "volume": _safe_float(candle.get("volume")),
                }
                for day_key, candle in sorted(unique_by_day.items(), key=lambda item: item[0])
            ]

        result = {
            "status": "success",
            "i_symbol": symbol,
            "resolution": resolution,
            "broker": broker,
            "bars": bars,
            "range": {
                "from": int(effective_from.timestamp()),
                "to": int(effective_to.timestamp()),
            },
            "partial": effective_from != from_date or effective_to != to_date,
        }
        _index_history_chunk_cache[cache_key] = (time.time(), result)
        with _index_history_chunk_inflight_lock:
            inflight = _index_history_chunk_inflight.get(cache_key)
            if inflight:
                inflight["result"] = result
        return result
    except Exception as exc:
        with _index_history_chunk_inflight_lock:
            inflight = _index_history_chunk_inflight.get(cache_key)
            if inflight:
                inflight["error"] = exc
        raise
    finally:
        with _index_history_chunk_inflight_lock:
            inflight = _index_history_chunk_inflight.get(cache_key)
            if inflight:
                inflight["event"].set()
                _index_history_chunk_inflight.pop(cache_key, None)


def get_symbol_historical_chart_bars(
    symbol: str,
    symbol_type: str | None,
    from_ts: int | None = None,
    to_ts: int | None = None,
    resolution: str = "1D",
) -> dict[str, Any]:
    """Generalized OHLCV bars for any chart symbol-search result. Indices
    delegate straight to get_index_historical_chart_bars (left completely
    unchanged — it already has 3 other callers, including the live
    alert_checker indicator loop, that must keep working exactly as before).
    Stocks and commodities are resolved here directly since neither has an
    i_symbol alias in SCANNER_INDEX_ALIASES; this duplicates a small amount
    of the chunk-day-limit/caching/bar-formatting logic from
    get_index_historical_chart_bars rather than refactoring that function,
    to avoid any risk to its existing callers.
    """
    symbol = str(symbol or "").strip()
    if not symbol:
        raise ValueError("symbol is required.")
    symbol_type = str(symbol_type or "index").strip().lower()
    resolution = str(resolution or "1D").strip()

    if symbol_type == "index":
        return get_index_historical_chart_bars(symbol, from_ts=from_ts, to_ts=to_ts, resolution=resolution)

    if symbol_type not in ("stock", "commodity"):
        raise ValueError(f"Unsupported symbol_type={symbol_type}")

    now_utc = datetime.utcnow()
    from_date = datetime.utcfromtimestamp(from_ts) if from_ts is not None else datetime(2018, 1, 1)
    to_date = datetime.utcfromtimestamp(to_ts) if to_ts is not None else now_utc
    if from_date > to_date:
        raise ValueError("'from' must be less than or equal to 'to'.")

    is_intraday = resolution in _KITE_INTRADAY_RESOLUTION_MAP or resolution in _DHAN_INTRADAY_RESOLUTION_MAP
    # MCX has no Kite path anywhere in this codebase — commodities always go
    # through Dhan regardless of which broker the app currently has active.
    broker = "dhan" if symbol_type == "commodity" else _get_active_market_data_broker()

    if not is_intraday:
        max_chunk_days = HISTORICAL_CHUNK_DAYS if broker == "kite" else DHAN_HISTORICAL_CHUNK_DAYS
    elif broker == "kite":
        _, _, max_chunk_days = _KITE_INTRADAY_RESOLUTION_MAP[resolution]
    else:
        _, _, max_chunk_days = _DHAN_INTRADAY_RESOLUTION_MAP[resolution]

    max_chunk_span = timedelta(days=max_chunk_days - 1)
    effective_from = from_date
    effective_to = to_date
    if (effective_to - effective_from) > max_chunk_span:
        effective_from = max(from_date, effective_to - max_chunk_span)

    cache_key = f"{symbol_type}:{symbol.upper()}:{resolution}:{broker}:{int(effective_from.timestamp())}:{int(effective_to.timestamp())}"
    cached = _index_history_chunk_cache.get(cache_key)
    if cached and (time.time() - cached[0]) <= _INDEX_HISTORY_CACHE_TTL_SECONDS:
        return cached[1]

    wait_event: threading.Event | None = None
    wait_record: dict[str, Any] | None = None
    is_request_leader = False
    with _index_history_chunk_inflight_lock:
        inflight = _index_history_chunk_inflight.get(cache_key)
        if inflight:
            wait_record = inflight
            wait_event = inflight["event"]
        else:
            wait_event = threading.Event()
            _index_history_chunk_inflight[cache_key] = {"event": wait_event, "result": None, "error": None}
            is_request_leader = True

    if not is_request_leader:
        wait_event.wait()
        if wait_record and wait_record.get("error") is not None:
            raise wait_record["error"]
        if wait_record and wait_record.get("result") is not None:
            return wait_record["result"]
        cached = _index_history_chunk_cache.get(cache_key)
        if cached and (time.time() - cached[0]) <= _INDEX_HISTORY_CACHE_TTL_SECONDS:
            return cached[1]
        raise Exception("Historical chart request finished without cache result.")

    try:
        if symbol_type == "stock":
            db = MongoData()._db
            stock = db[STOCKS_COLLECTION].find_one(
                {"$or": [{"symbol": symbol.upper()}, {"tradingsymbol": symbol.upper()}]}
            )
            if not stock:
                raise ValueError(f"Unknown stock symbol={symbol}")
            if broker == "kite":
                _, instrument_token = _resolve_stock_kite_token(stock)
                if not instrument_token:
                    raise ValueError(f"Kite instrument token not configured for symbol={symbol}")
                kite = _build_kite_client_from_config()
                if is_intraday:
                    native_interval, factor, _ = _KITE_INTRADAY_RESOLUTION_MAP[resolution]
                    candles = _fetch_kite_intraday_candles(kite, int(instrument_token), native_interval, effective_from, effective_to)
                    candles = _aggregate_intraday_candles(candles, factor)
                else:
                    candles = _fetch_kite_daily_candles(kite, int(instrument_token), effective_from, effective_to)
            else:
                _, dhan_security_id = _resolve_stock_dhan_security_id(stock)
                if not dhan_security_id:
                    raise ValueError(f"Dhan security id not configured for symbol={symbol}")
                _, access_token = _load_dhan_credentials_any()
                if not access_token:
                    raise ValueError("Active Dhan access token not configured.")
                if is_intraday:
                    native_interval, factor, _ = _DHAN_INTRADAY_RESOLUTION_MAP[resolution]
                    candles = _fetch_dhan_intraday_candles(
                        access_token, dhan_security_id, "NSE_EQ", "EQUITY", native_interval, effective_from, effective_to
                    )
                    candles = _aggregate_intraday_candles(candles, factor)
                else:
                    candles = _fetch_dhan_daily_candles(
                        access_token, dhan_security_id, "NSE_EQ", "EQUITY", effective_from, effective_to
                    )
        else:  # commodity — always Dhan, front-month FUTCOM contract
            commodity_master = _load_dhan_commodity_master()
            contracts = [
                c for c in commodity_master.get(symbol.upper(), []) if c.get("opt_type") == "FUT"
            ]
            front_month = min(contracts, key=lambda c: c.get("expiry") or "9999-99-99", default=None)
            if not front_month:
                raise ValueError(f"No MCX futures contract found for symbol={symbol}")
            dhan_security_id = str(front_month.get("sec_id") or "").strip()
            if not dhan_security_id:
                raise ValueError(f"Dhan security id missing for commodity contract symbol={symbol}")
            _, access_token = _load_dhan_credentials_any()
            if not access_token:
                raise ValueError("Active Dhan access token not configured.")
            if is_intraday:
                native_interval, factor, _ = _DHAN_INTRADAY_RESOLUTION_MAP[resolution]
                candles = _fetch_dhan_intraday_candles(
                    access_token, dhan_security_id, "MCX_COMM", "FUTCOM", native_interval, effective_from, effective_to
                )
                candles = _aggregate_intraday_candles(candles, factor)
            else:
                candles = _fetch_dhan_daily_candles(
                    access_token, dhan_security_id, "MCX_COMM", "FUTCOM", effective_from, effective_to
                )

        if is_intraday:
            bars = [
                {
                    "time": _epoch_millis(candle["date"]),
                    "open": _safe_float(candle.get("open")),
                    "high": _safe_float(candle.get("high")),
                    "low": _safe_float(candle.get("low")),
                    "close": _safe_float(candle.get("close")),
                    "volume": _safe_float(candle.get("volume")),
                }
                for candle in sorted(
                    (c for c in candles if isinstance(c.get("date"), datetime)),
                    key=lambda c: c["date"],
                )
            ]
        else:
            unique_by_day: dict[str, dict[str, Any]] = {}
            for candle in candles:
                date_value = candle.get("date")
                if not isinstance(date_value, datetime):
                    continue
                day_key = date_value.strftime("%Y-%m-%d")
                unique_by_day[day_key] = candle

            bars = [
                {
                    "time": calendar.timegm(datetime.strptime(day_key, "%Y-%m-%d").timetuple()) * 1000,
                    "open": _safe_float(candle.get("open")),
                    "high": _safe_float(candle.get("high")),
                    "low": _safe_float(candle.get("low")),
                    "close": _safe_float(candle.get("close")),
                    "volume": _safe_float(candle.get("volume")),
                }
                for day_key, candle in sorted(unique_by_day.items(), key=lambda item: item[0])
            ]

        result = {
            "status": "success",
            "i_symbol": symbol,
            "symbol_type": symbol_type,
            "resolution": resolution,
            "broker": broker,
            "bars": bars,
            "range": {
                "from": int(effective_from.timestamp()),
                "to": int(effective_to.timestamp()),
            },
            "partial": effective_from != from_date or effective_to != to_date,
        }
        _index_history_chunk_cache[cache_key] = (time.time(), result)
        with _index_history_chunk_inflight_lock:
            inflight = _index_history_chunk_inflight.get(cache_key)
            if inflight:
                inflight["result"] = result
        return result
    except Exception as exc:
        with _index_history_chunk_inflight_lock:
            inflight = _index_history_chunk_inflight.get(cache_key)
            if inflight:
                inflight["error"] = exc
        raise
    finally:
        with _index_history_chunk_inflight_lock:
            inflight = _index_history_chunk_inflight.get(cache_key)
            if inflight:
                inflight["event"].set()
                _index_history_chunk_inflight.pop(cache_key, None)
