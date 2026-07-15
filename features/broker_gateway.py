"""
broker_gateway.py
─────────────────
Single broker abstraction layer — THE ONE FILE that changes when switching brokers.

Architecture:
  • All market-data and trading operations across the system import from here.
  • Never import kite_* / dhan_* directly from business-logic files.
  • Active broker is read from kite_market_config (enabled=true, broker=kite|dhan).
  • To add a new broker: create <broker>_ticker.py + <broker>_broker_ws.py +
    <broker>_broker.py with the same public function signatures, then wire them
    in the routing sections below.

Current supported brokers: kite (Zerodha), dhan (DhanHQ)
"""

from __future__ import annotations

import os
import threading
import time

from features.debug_flags import debug_print

# ── Active broker detection ───────────────────────────────────────────────────
# Every process (algo.trade, algo.simulator, algo.scanner, algo.websocket) has
# its own copy of this module-level cache. It used to be cached forever after
# the first read ("restart to switch"), which meant flipping the enabled
# broker in kite_market_config only ever took effect in whichever single
# process happened to call reset_broker_cache() (algo.trade's broker-login
# endpoints) — every other process kept dialing the old broker (e.g. Kite,
# with a dead/expired token → repeated 403s) until it was manually restarted.
# A short TTL instead means every process re-checks Mongo on its own within
# a bounded window, so a broker switch reaches all of them without needing a
# coordinated restart.
_broker_cache: list[str] = []
_broker_cache_at = 0.0
_broker_cache_ttl = 20.0  # seconds
_broker_lock  = threading.Lock()


def _active_broker() -> str:
    """Returns 'kite' or 'dhan' based on kite_market_config enabled record."""
    global _broker_cache_at
    now = time.monotonic()
    if _broker_cache and (now - _broker_cache_at) < _broker_cache_ttl:
        return _broker_cache[0]
    with _broker_lock:
        now = time.monotonic()
        if _broker_cache and (now - _broker_cache_at) < _broker_cache_ttl:
            return _broker_cache[0]
        try:
            from features.mongo_data import MongoData  # type: ignore
            _db = MongoData()
            try:
                cfg = _db._db['kite_market_config'].find_one({'enabled': True}) or {}
                name = str(cfg.get('broker') or 'kite').strip().lower()
                print(f'[ACTIVE BROKER] resolved={name!r} from kite_market_config enabled doc (pid={os.getpid()})')
            finally:
                _db.close()
        except Exception:
            # Transient DB hiccup — keep serving the last known-good value
            # instead of collapsing to 'kite' and flipping brokers on a blip.
            name = _broker_cache[0] if _broker_cache else 'kite'
        _broker_cache[:] = [name]
        _broker_cache_at = now
        return name


def get_active_broker_token_status() -> tuple[bool, str]:
    """
    (is_valid, message) for the kite_market_config record currently marked
    enabled=True. Checks token presence and, where the broker stores one
    (Dhan's expiry_time — tokens expire ~24h after login), whether it has
    already expired. Kite has no stored expiry — its session is verified
    live by the TokenException handling at each kite.quote() call site
    instead, so this is presence-only for kite.

    Without this, a missing/expired token just makes every downstream quote
    call fail silently and the option chain renders as ltp=0 for every
    strike — indistinguishable from "broker has no data right now".
    """
    from features.mongo_data import MongoData  # type: ignore
    from datetime import datetime as _dt

    broker = _active_broker()
    _db = MongoData()
    try:
        cfg = _db._db['kite_market_config'].find_one({'broker': broker, 'enabled': True}) or {}
    finally:
        _db.close()

    if not cfg:
        return False, f"No {broker.capitalize()} broker is enabled in kite_market_config — please login from Broker Login."

    access_token = str(cfg.get('access_token') or '').strip()
    if not access_token:
        return False, f"{broker.capitalize()} is not logged in — please login from Broker Login."

    expiry_time = str(cfg.get('expiry_time') or '').strip()
    if expiry_time:
        try:
            if _dt.fromisoformat(expiry_time) <= _dt.now():
                return False, f"{broker.capitalize()} session expired — please re-login from Broker Login."
        except ValueError:
            pass

    return True, ""


def reset_broker_cache() -> None:
    """Call if broker config changes at runtime (e.g. switching between kite↔dhan)."""
    with _broker_lock:
        _broker_cache.clear()


# ── Ticker manager proxy ──────────────────────────────────────────────────────
# Delegates attribute/method access to the correct broker ticker at runtime.

class _BrokerTickerProxy:
    """
    Proxy that routes to kite_ticker or dhan_ticker based on active broker.

    In central-tick mode (set_central_client called at service startup),
    all routing goes to the CentralTickClient instead — one broker WS
    connection lives in algo.websocket and all other services share it.
    Call set_central_client(None) to revert to direct broker mode.
    """

    _central: object = None  # CentralTickClient instance when in central mode

    @property
    def _delegate(self):
        if self._central is not None:
            return self._central
        if _active_broker() == 'dhan':
            from features.dhan_ticker import dhan_ticker_manager  # type: ignore
            return dhan_ticker_manager
        from features.kite_ticker import ticker_manager  # type: ignore
        return ticker_manager

    def set_central_client(self, client) -> None:
        """
        Switch this proxy to use a CentralTickClient instead of a direct
        broker connection. Call from algo.trade / algo.simulator startup
        AFTER removing _auto_start_ticker from on_startup so the service
        does not also open its own broker WS.
        Pass None to revert to direct broker mode.
        """
        type(self)._central = client

    @property
    def ltp_map(self):           return self._delegate.ltp_map

    @property
    def spot_map(self):          return self._delegate.spot_map

    @property
    def status(self):            return self._delegate.status

    @property
    def tick_count(self):        return self._delegate.tick_count

    @property
    def started_at(self):        return self._delegate.started_at

    @property
    def error_msg(self):         return self._delegate.error_msg

    @property
    def subscribed_tokens(self): return self._delegate.subscribed_tokens

    @property
    def oi_map(self):
        # Kite's ticker doesn't track OI the same way Dhan's does — empty
        # dict there, real data on Dhan (direct or via CentralTickClient).
        return getattr(self._delegate, 'oi_map', {})

    @property
    def bid_map(self):
        # Best (level-0) bid, F&O legs only (RESP_FULL packets) — see dhan_ticker.py's
        # depth parsing and CentralTickClient's changed_bid_map relay. Empty on Kite
        # (no equivalent tracked) or for any token never subscribed in Full mode.
        return getattr(self._delegate, 'bid_map', {})

    @property
    def ask_map(self):
        return getattr(self._delegate, 'ask_map', {})

    @property
    def ltp_ts_map(self):
        return getattr(self._delegate, 'ltp_ts_map', {})

    @property
    def prev_close_map(self):
        # Previous trading day's close, from Dhan's dedicated RESP_PREV_CLOSE
        # (packet code 6) WS packets — see dhan_ticker.py. Empty on Kite (no
        # equivalent tracked); callers should keep their Mongo-based fallback
        # for the Kite path / until this map has warmed up post-connect.
        return getattr(self._delegate, 'prev_close_map', {})

    @property
    def _ticker(self):           return self._delegate._ticker

    def get_ltp(self, token):            return self._delegate.get_ltp(token)
    def get_spot(self, underlying):      return self._delegate.get_spot(underlying)
    def get_status(self):                return self._delegate.get_status()
    def add_tick_listener(self, l):      return self._delegate.add_tick_listener(l)
    def remove_tick_listener(self, l):   return self._delegate.remove_tick_listener(l)
    def register_option_token(self, token, label=""):
        return self._delegate.register_option_token(token, label)
    def subscribe_tokens(self, ids, exchange='NSE_FNO'):
        return self._delegate.subscribe_tokens(ids, exchange)
    def warm_chain_tokens(self, ids, exchange='NSE_FNO'):
        # Dhan-only (chain-feed connection pool, see dhan_ticker.py). No-op
        # for Kite — fetch_full_chain's Kite path always REST-fetches the
        # whole chain in one batched call anyway, no per-token warm needed.
        fn = getattr(self._delegate, 'warm_chain_tokens', None)
        if fn is not None:
            return fn(ids, exchange)
    def start(self, db):                 return self._delegate.start(db)
    def stop(self):                      return self._delegate.stop()
    def restart(self, db):               return self._delegate.restart(db)


broker_ticker_manager = _BrokerTickerProxy()


# ── Credential / WebSocket functions — broker-routed ─────────────────────────
# All functions have the same signature as kite_broker_ws equivalents.

def get_broker_ltp_map() -> dict[str, float]:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import get_ltp_map  # type: ignore
        return get_ltp_map()
    from features.kite_broker_ws import get_ltp_map  # type: ignore
    return get_ltp_map()


_REST_QUOTE_CACHE: dict[str, tuple[float, dict]] = {}  # token → (epoch, result)
_REST_QUOTE_CACHE_TTL = 3.0  # seconds — avoid 429 rate limit

# token → last-seen-good {"ltp", "oi"}. Dhan's REST quote endpoint is rate
# limited to ~1 req/sec per account; any concurrent caller (other open tabs/
# pages polling the same broker) can push a burst over that limit and Dhan
# returns 429. Previously that silently fell through to ltp=0 ("unavailable"),
# which the UI showed as a literal 0.00 LTP. Never evict this — a stale-but-
# real price is always a better fallback than 0.
_LAST_GOOD_QUOTE: dict[str, dict] = {}

# ── Global Dhan /marketfeed/quote rate gate ───────────────────────────────────
# This app has *several* independent callers of Dhan's quote endpoint —
# get_broker_rest_quotes() below, api.py's _fetch_dhan_market_data() (option
# chain + stock spot), execution_socket.py's _fetch_dhan_index_quotes()
# (index spot/change%), and live_quote_socket.py's background 3s REST-refresh
# loop. Each one used to call Dhan directly with no idea any of the others
# existed, so any two firing within Dhan's ~1 req/sec-per-account window
# would 429 each other — confirmed live: an isolated test script calling the
# exact same endpoint with the exact same tokens got real data every time,
# while the same call made *through* the running server (competing with the
# live-quote WS's background refresh loop) got 429'd consistently. Every
# caller now funnels through dhan_quote_post() so the whole app shares one
# clock instead of each call site guessing with its own ad-hoc sleep.
_DHAN_QUOTE_LOCK = threading.Lock()
_dhan_quote_last_call_at = 0.0
_DHAN_QUOTE_MIN_INTERVAL = 1.05  # seconds


def wait_for_dhan_slot(min_interval: float = _DHAN_QUOTE_MIN_INTERVAL) -> None:
    """
    Block until the shared Dhan rate-gate clock has a free slot, then claim it.

    Same lock/clock as dhan_quote_post() above, but blocking instead of
    skip-on-miss — for callers that need the call to eventually succeed
    (e.g. historical-data backfills) rather than callers that are fine
    falling back to a cached value (live quote polling). Use this so
    non-quote Dhan REST calls line up on the *same* clock as the quote
    pollers instead of running their own independent sleep() and 429-ing
    each other, which is the exact failure mode this lock was built for.
    """
    global _dhan_quote_last_call_at
    while True:
        with _DHAN_QUOTE_LOCK:
            now = time.monotonic()
            remaining = min_interval - (now - _dhan_quote_last_call_at)
            if remaining <= 0:
                _dhan_quote_last_call_at = now
                return
        time.sleep(remaining)


def _dhan_quote_http(req_body: dict, access_token: str, client_id: str, timeout: float):
    import requests as _req
    return _req.post(
        "https://api.dhan.co/v2/marketfeed/quote",
        headers={
            "access-token": access_token,
            "client-id": client_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=req_body,
        timeout=timeout,
    )


def dhan_quote_post(req_body: dict, access_token: str, client_id: str, timeout: float = 15.0):
    """
    POST to Dhan's /v2/marketfeed/quote, globally throttled across every
    caller in this process. Returns None (never raises, never blocks) if
    called too soon after the last call anywhere in the app — callers
    already fall back to their own last-good cache for that case, which
    costs nothing and is strictly better than burning the shared rate-limit
    budget on a call likely to 429 anyway.
    """
    global _dhan_quote_last_call_at
    with _DHAN_QUOTE_LOCK:
        now = time.monotonic()
        if now - _dhan_quote_last_call_at < _DHAN_QUOTE_MIN_INTERVAL:
            return None
        _dhan_quote_last_call_at = now

    return _dhan_quote_http(req_body, access_token, client_id, timeout)


def dhan_quote_post_blocking(req_body: dict, access_token: str, client_id: str, timeout: float = 15.0):
    """
    Same call as dhan_quote_post(), but waits for the shared rate-gate slot
    instead of skipping when busy — for callers where a skip means "this
    request just lost its data", not "fall back to a cache". A request that
    fetches the index spot then immediately fetches the whole option chain
    (see api.py's get_live_greeks_chain) makes two dhan_quote_post() calls
    within the same handler, microseconds apart — far under the 1.05s gate —
    so the second one always lost the race and the entire chain rendered as
    ltp=0 on every single load, not just under real rate-limit contention.
    """
    wait_for_dhan_slot()
    return _dhan_quote_http(req_body, access_token, client_id, timeout)


def get_broker_rest_quotes(
    token_ids: list[str],
    db,
    ws_segments: dict[str, str] | None = None,
) -> dict[str, dict]:
    """
    Return {token_str: {"ltp": float, "oi": int}} using:
      - Dhan WS ltp_map / oi_map where available
      - Dhan REST /marketfeed/quote fallback for missing tokens
      - Handles both NSE_FNO and BSE_FNO (SENSEX/BANKEX)
    Kite: returns {} (caller uses kite_quote_map for LTP+OI).
    """
    if _active_broker() != 'dhan':
        return {}
    # Read through broker_ticker_manager (the proxy defined in this file),
    # NOT a direct dhan_ticker_manager import — in central-tick mode
    # (algo.trade/algo.simulator), the real ticks live in CentralTickClient,
    # not in this process's own (never-started, empty) dhan_ticker_manager.
    # Importing dhan_ticker_manager directly here made every WS lookup miss
    # in those processes, forcing a REST call on every single poll.
    _dtm = broker_ticker_manager

    result: dict[str, dict] = {}

    def _fill_last_good(res: dict[str, dict]) -> dict[str, dict]:
        # Last resort for any token this call couldn't price (most often a
        # 429 burst, see _LAST_GOOD_QUOTE doc above): reuse the last real
        # price we ever saw for it instead of surfacing ltp=0 ("unavailable").
        for t in token_ids:
            if t not in res or not res[t].get("ltp"):
                cached = _LAST_GOOD_QUOTE.get(t)
                if cached:
                    res[t] = cached
        return res

    ws_ltp = _dtm.ltp_map
    ws_oi  = _dtm.oi_map
    ws_ts  = _dtm.ltp_ts_map

    import time as _time
    from datetime import datetime as _dt
    _now_epoch = _time.time()

    # Determine new (un-subscribed) tokens before the loop so prints can be gated.
    _subscribed = set(_dtm.subscribed_tokens or [])
    _not_subscribed = [t for t in token_ids if t not in _subscribed]

    # Populate from WS first — skip stale ticks older than 5 minutes
    for t in token_ids:
        raw_ltp = float(ws_ltp.get(t) or 0)
        oi      = int(ws_oi.get(t) or 0)
        ltp     = raw_ltp
        ts_str  = ws_ts.get(t)
        tick_age_sec = None
        if raw_ltp > 0:
            if ts_str:
                try:
                    tick_age_sec = _now_epoch - _dt.fromisoformat(ts_str).timestamp()
                    if tick_age_sec > 300:
                        ltp = 0.0  # stale — refresh via REST
                except Exception:
                    ltp = 0.0  # can't verify freshness → treat as stale
            else:
                ltp = 0.0  # no timestamp → initial subscription price, treat as stale
        if _not_subscribed:
            debug_print(f'[REST_QUOTES_DEBUG] token={t} ws_ltp={raw_ltp} ws_ts={ts_str} tick_age={tick_age_sec} using_ws_ltp={ltp}')
        if ltp > 0 or oi > 0:
            result[t] = {"ltp": ltp, "oi": oi}
            if ltp > 0:
                _LAST_GOOD_QUOTE[t] = {"ltp": ltp, "oi": oi}

    # REST fallback for tokens missing LTP (BSE_FNO never gets WS ticks after hours)
    missing = [t for t in token_ids if t not in result or result[t]["ltp"] == 0]
    if _not_subscribed:
        debug_print(
            f'[REST_QUOTES_DEBUG] missing_tokens={missing}  will_call_rest={bool(missing)}'
            f'  |  requested={list(token_ids)}  subscribed_count={len(_subscribed)}'
            f'  |  not_in_subscribed={_not_subscribed}'
        )

    _segs = ws_segments or {}

    # Subscribe any never-seen tokens to the WS so the *next* poll resolves
    # straight from ltp_map (in-memory, no network round trip, no rate limit)
    # instead of hitting Dhan's REST endpoint again. This is fire-and-forget —
    # ticks arrive asynchronously, so it can't help resolve THIS call, but it's
    # what makes repeated polling for the same legs converge to near-zero
    # latency instead of one REST call every poll forever.
    if _not_subscribed:
        try:
            new_by_segment: dict[str, list[str]] = {}
            for t in _not_subscribed:
                new_by_segment.setdefault(_segs.get(t, "NSE_FNO").upper(), []).append(t)
            for segment, tokens in new_by_segment.items():
                _dtm.subscribe_tokens(tokens, segment)
        except Exception as exc:
            debug_print(f'[REST_QUOTES_DEBUG] WS subscribe error: {exc}')

    if not missing:
        return _fill_last_good(result)

    # Serve from REST cache to avoid 429 rate limit
    import time as _tc
    _cache_now = _tc.time()
    still_missing = []
    for t in missing:
        cached = _REST_QUOTE_CACHE.get(t)
        if cached and (_cache_now - cached[0]) < _REST_QUOTE_CACHE_TTL:
            result[t] = cached[1]
        else:
            still_missing.append(t)
    missing = still_missing

    if not missing:
        return _fill_last_good(result)

    # Group by segment dynamically (not just NSE_FNO/BSE_FNO) so a caller can
    # fold IDX_I (index/spot) tokens into this same batch — one Dhan REST call
    # covering every leg + the underlying's spot price, instead of a separate
    # sequential call per segment (which is what previously forced a blind
    # sleep between them to dodge back-to-back 429s).
    missing_by_segment: dict[str, list[str]] = {}
    for t in missing:
        missing_by_segment.setdefault(_segs.get(t, "NSE_FNO").upper(), []).append(t)

    try:
        cfg = db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
        access_token = str(cfg.get("access_token") or "").strip()
        client_id    = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
        if not access_token:
            if _not_subscribed:
                debug_print(f'[REST_QUOTES_DEBUG] no access_token in db — REST skipped')
            return _fill_last_good(result)

        def _to_int(tok: str) -> int | None:
            """Strip exchange prefix and convert to int: 'NSE_54808' → 54808."""
            n = tok.split("_", 1)[-1] if "_" in tok else tok
            try:
                return int(n)
            except (ValueError, TypeError):
                return None

        # Build reverse map: numeric_str → original prefixed token (e.g. "54808" → "NSE_54808")
        _numeric_to_original: dict[str, str] = {}
        for t in missing:
            n = t.split("_", 1)[-1] if "_" in t else t
            _numeric_to_original.setdefault(n, t)

        # Dhan caps /marketfeed/quote at ~1000 ids per segment per request.
        # A caller pooling tokens across many sessions/strategies (see
        # live_quote_socket.py's hub-wide union) can realistically exceed
        # that, and one oversized request body either gets rejected outright
        # or silently truncated — chunk every segment to _BATCH and issue one
        # request per chunk-index, packing all segments for that index into
        # the same request (Dhan's cap is per-segment, not per-request, so
        # this still covers every segment in as few round trips as possible).
        _BATCH = 500
        _max_len = max((len(ids) for ids in missing_by_segment.values()), default=0)
        _num_batches = (_max_len + _BATCH - 1) // _BATCH if _max_len else 0

        # Tokens never seen before (just-warmed chain, e.g. a newly opened
        # expiry tab) have no _LAST_GOOD_QUOTE to fall back on — for them a
        # skipped call means this row renders ltp=0 to the user, not "serve
        # slightly-stale cache". A steady-state refresh for already-known
        # tokens can safely skip-on-busy (WS/cache already has a real price),
        # but the first-ever fetch for a chain must wait for its rate-gate
        # slot instead of giving up, same reasoning as dhan_quote_post_blocking's
        # docstring. Without this, switching to a rarely-viewed expiry while
        # another chain is actively polling (and thus usually holding the
        # shared 1-req/sec slot) reliably lost the race and showed ltp=0.
        _has_never_seen = bool(set(missing) & set(_not_subscribed))

        for _batch_idx in range(_num_batches):
            req_body: dict = {
                segment: [
                    v for t in ids[_batch_idx * _BATCH:(_batch_idx + 1) * _BATCH]
                    if t and (v := _to_int(t)) is not None
                ]
                for segment, ids in missing_by_segment.items()
            }
            req_body = {segment: ids for segment, ids in req_body.items() if ids}
            if not req_body:
                continue

            if _not_subscribed:
                debug_print(f'[REST_QUOTES_DEBUG] calling Dhan REST batch={_batch_idx} req_body={req_body}')
            resp = (
                dhan_quote_post_blocking(req_body, access_token, client_id, timeout=10.0)
                if _has_never_seen
                else dhan_quote_post(req_body, access_token, client_id, timeout=10.0)
            )
            if resp is None:
                # Globally throttled — some other caller in this process hit
                # Dhan within the last ~1.05s. Remaining batches (if any)
                # would hit the same gate immediately, so stop here rather
                # than spin through them for nothing — they, and this one,
                # fall back to last-good below; a later refresh cycle picks
                # up wherever this one left off.
                if _not_subscribed:
                    debug_print('[REST_QUOTES_DEBUG] skipped — global Dhan quote rate gate')
                break
            if _not_subscribed:
                debug_print(f'[REST_QUOTES_DEBUG] REST status={resp.status_code} response={resp.text[:500]}')

            if resp.status_code == 200:
                raw = resp.json()
                data = raw.get("data") or raw
                _rest_epoch = _tc.time()
                for exch in req_body.keys():
                    for tok, v in (data.get(exch) or {}).items():
                        if not isinstance(v, dict):
                            continue
                        ltp = float(v.get("last_price") or 0)
                        oi  = int(v.get("oi") or 0)
                        tok_str = str(tok)
                        for _key in (tok_str, _numeric_to_original.get(tok_str, tok_str)):
                            existing = result.get(_key, {"ltp": 0, "oi": 0})
                            entry = {
                                "ltp": ltp if ltp > 0 else existing["ltp"],
                                "oi":  oi  if oi  > 0 else existing["oi"],
                            }
                            result[_key] = entry
                            _REST_QUOTE_CACHE[_key] = (_rest_epoch, entry)
                            if entry["ltp"] > 0:
                                _LAST_GOOD_QUOTE[_key] = entry
            else:
                # Most commonly a 429 — Dhan rate-limits /marketfeed/quote to
                # roughly 1 req/sec per account, and any other open page/tab
                # polling the same broker can burn that allowance. Don't let a
                # rate-limited response wipe out tokens we've already priced —
                # fall through to the _LAST_GOOD_QUOTE backfill below instead.
                if _not_subscribed:
                    debug_print(f'[REST_QUOTES_DEBUG] REST non-200 status={resp.status_code} — falling back to last-good cache')
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("[BROKER REST QUOTES] %s", exc)
        if _not_subscribed:
            debug_print(f'[REST_QUOTES_DEBUG] REST exception: {exc}')
        from features.telegram_notifier import notify_admin
        notify_admin('ltp_fetch_error', f'get_broker_rest_quotes Dhan REST call failed: {exc}')

    result = _fill_last_good(result)
    still_unpriced = [t for t in token_ids if not result.get(t, {}).get("ltp")]
    if still_unpriced:
        from features.telegram_notifier import notify_admin
        notify_admin(
            'ltp_fetch_error',
            f'{len(still_unpriced)} token(s) have no LTP after REST fallback and no cached last-good price',
            {'tokens': ','.join(still_unpriced[:10])},
        )
    if _not_subscribed:
        debug_print(f'[REST_QUOTES_DEBUG] final_result={result}')
    return result


_REST_DEPTH_CACHE: dict[str, tuple[float, dict]] = {}  # token → (epoch, {"bid","ask","prev_close"})
_REST_DEPTH_CACHE_TTL = 3.0  # seconds — matches _REST_QUOTE_CACHE_TTL / dhan rate gate cadence


def get_broker_rest_depth(
    token_ids: list[str],
    db,
    ws_segments: dict[str, str] | None = None,
) -> dict[str, dict]:
    """
    Return {token_str: {"bid": float, "ask": float, "prev_close": float}} —
    top-of-book depth + previous day's close premium, sourced from Dhan's
    REST /marketfeed/quote. This is the only place this app ever sees depth —
    the binary WS feed parsed in dhan_ticker.py's _handle_binary only decodes
    LTP/OI from the Full packet, never its depth block, so depth can't come
    from ws_ltp/ws_oi the way get_broker_rest_quotes' LTP does.
    Kite: returns {} — no depth wiring for that broker yet.

    Every call fetches fresh for tokens not already cached (no WS shortcut,
    unlike get_broker_rest_quotes) — but every token is cached for
    _REST_DEPTH_CACHE_TTL so repeat callers within the same short window
    (the live-greeks-chain broadcaster polls every 2s per open chain) share
    one Dhan REST round trip instead of one each, respecting the same
    dhan_quote_post() rate gate every other REST caller in this file uses.
    """
    if _active_broker() != 'dhan':
        return {}

    result: dict[str, dict] = {}
    now = time.time()
    missing = []
    for t in token_ids:
        cached = _REST_DEPTH_CACHE.get(t)
        if cached and (now - cached[0]) < _REST_DEPTH_CACHE_TTL:
            result[t] = cached[1]
        else:
            missing.append(t)
    if not missing:
        return result

    _segs = ws_segments or {}

    def _to_int(tok: str) -> int | None:
        n = tok.split("_", 1)[-1] if "_" in tok else tok
        try:
            return int(n)
        except (ValueError, TypeError):
            return None

    _numeric_to_original: dict[str, str] = {}
    for t in missing:
        n = t.split("_", 1)[-1] if "_" in t else t
        _numeric_to_original.setdefault(n, t)

    missing_by_segment: dict[str, list[str]] = {}
    for t in missing:
        missing_by_segment.setdefault(_segs.get(t, "NSE_FNO").upper(), []).append(t)

    try:
        cfg = db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
        access_token = str(cfg.get("access_token") or "").strip()
        client_id    = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
        if not access_token:
            return result

        # Dhan caps /marketfeed/quote at ~1000 ids per segment per request —
        # same chunking as get_broker_rest_quotes above.
        _BATCH = 500
        _max_len = max((len(ids) for ids in missing_by_segment.values()), default=0)
        _num_batches = (_max_len + _BATCH - 1) // _BATCH if _max_len else 0

        for _batch_idx in range(_num_batches):
            req_body: dict = {
                segment: [
                    v for t in ids[_batch_idx * _BATCH:(_batch_idx + 1) * _BATCH]
                    if t and (v := _to_int(t)) is not None
                ]
                for segment, ids in missing_by_segment.items()
            }
            req_body = {segment: ids for segment, ids in req_body.items() if ids}
            if not req_body:
                continue

            # _blocking, not the skip-if-busy dhan_quote_post used above: this
            # call always fires microseconds after get_broker_rest_quotes'
            # own dhan_quote_post within the same _fetch_full_chain_from_dhan
            # pass, i.e. always inside the other call's 1.05s gate window —
            # skipping here would mean depth silently never resolves. Same
            # fix dhan_quote_post_blocking's docstring already describes for
            # fetch_full_chain's own back-to-back spot+chain calls.
            resp = dhan_quote_post_blocking(req_body, access_token, client_id, timeout=10.0)
            if resp.status_code != 200:
                continue

            raw = resp.json()
            data = raw.get("data") or raw
            _epoch = time.time()
            for exch in req_body.keys():
                for tok, v in (data.get(exch) or {}).items():
                    if not isinstance(v, dict):
                        continue
                    depth = v.get("depth") or {}
                    buy_levels = depth.get("buy") or []
                    sell_levels = depth.get("sell") or []
                    bid = float((buy_levels[0] or {}).get("price") or 0) if buy_levels else 0.0
                    ask = float((sell_levels[0] or {}).get("price") or 0) if sell_levels else 0.0
                    prev_close = float((v.get("ohlc") or {}).get("close") or 0)
                    tok_str = str(tok)
                    entry = {"bid": bid, "ask": ask, "prev_close": prev_close}
                    for _key in (tok_str, _numeric_to_original.get(tok_str, tok_str)):
                        result[_key] = entry
                        _REST_DEPTH_CACHE[_key] = (_epoch, entry)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("[BROKER REST DEPTH] %s", exc)

    return result


def get_broker_oi_map(
    token_ids: list[str],
    db,
    ws_segments: dict[str, str] | None = None,
) -> dict[str, int]:
    """Backward-compat wrapper — returns only OI. Use get_broker_rest_quotes for LTP+OI."""
    quotes = get_broker_rest_quotes(token_ids, db, ws_segments)
    return {t: v["oi"] for t, v in quotes.items() if v["oi"] > 0}



def get_broker_credentials() -> tuple[str, str]:
    """(api_key, access_token) for Kite  |  (client_id, access_token) for Dhan."""
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import get_common_credentials  # type: ignore
        return get_common_credentials()
    from features.kite_broker_ws import get_common_credentials  # type: ignore
    return get_common_credentials()


def get_broker_api_key() -> str:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import get_common_api_key  # type: ignore
        return get_common_api_key()
    from features.kite_broker_ws import get_common_api_key  # type: ignore
    return get_common_api_key()


def broker_is_configured() -> bool:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import is_configured  # type: ignore
        return is_configured()
    from features.kite_broker_ws import is_configured  # type: ignore
    return is_configured()


def load_broker_credentials_from_db(db) -> bool:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import load_credentials_from_db  # type: ignore
        return load_credentials_from_db(db)
    from features.kite_broker_ws import load_credentials_from_db  # type: ignore
    return load_credentials_from_db(db)


def set_broker_credentials(key: str, access_token: str) -> None:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import set_common_credentials  # type: ignore
        set_common_credentials(key, access_token)
    else:
        from features.kite_broker_ws import set_common_credentials  # type: ignore
        set_common_credentials(key, access_token)


def save_broker_access_token(db, access_token: str) -> None:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import save_access_token_to_db  # type: ignore
        save_access_token_to_db(db, access_token)
    else:
        from features.kite_broker_ws import save_access_token_to_db  # type: ignore
        save_access_token_to_db(db, access_token)


def save_broker_credentials_to_db(db, key: str, access_token: str) -> None:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import save_credentials_to_db  # type: ignore
        save_credentials_to_db(db, key, access_token)
    else:
        from features.kite_broker_ws import save_credentials_to_db  # type: ignore
        save_credentials_to_db(db, key, access_token)


def get_broker_ws_login_url() -> str:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import get_login_url  # type: ignore
        return get_login_url()
    from features.kite_broker_ws import get_login_url  # type: ignore
    return get_login_url()


def broker_generate_access_token(request_token: str) -> str:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import generate_access_token  # type: ignore
        return generate_access_token(request_token)
    from features.kite_broker_ws import generate_access_token  # type: ignore
    return generate_access_token(request_token)


def broker_validate_access_token(access_token: str = '') -> bool:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import validate_access_token  # type: ignore
        return validate_access_token(access_token)
    from features.kite_broker_ws import validate_access_token  # type: ignore
    return validate_access_token(access_token)


def broker_extract_instrument_tokens(positions: list[dict]) -> list[int]:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import extract_instrument_tokens  # type: ignore
        return extract_instrument_tokens(positions)
    from features.kite_broker_ws import extract_instrument_tokens  # type: ignore
    return extract_instrument_tokens(positions)


def broker_register_user_tokens(user_id: str, tokens: list) -> bool:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import register_user_tokens  # type: ignore
        return register_user_tokens(user_id, tokens)
    from features.kite_broker_ws import register_user_tokens  # type: ignore
    return register_user_tokens(user_id, tokens)


def broker_unregister_user(user_id: str) -> None:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import unregister_user  # type: ignore
        unregister_user(user_id)
    else:
        from features.kite_broker_ws import unregister_user  # type: ignore
        unregister_user(user_id)


def broker_refresh_user_tokens(user_id: str, new_tokens: list) -> bool:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import refresh_user_tokens  # type: ignore
        return refresh_user_tokens(user_id, new_tokens)
    from features.kite_broker_ws import refresh_user_tokens  # type: ignore
    return refresh_user_tokens(user_id, new_tokens)


def broker_add_tick_listener(listener) -> bool:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import add_tick_listener  # type: ignore
        return add_tick_listener(listener)
    from features.kite_broker_ws import add_tick_listener  # type: ignore
    return add_tick_listener(listener)


def broker_remove_tick_listener(listener) -> None:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import remove_tick_listener  # type: ignore
        remove_tick_listener(listener)
    else:
        from features.kite_broker_ws import remove_tick_listener  # type: ignore
        remove_tick_listener(listener)


def broker_wait_for_tokens_ltp(
    tokens: list,
    timeout_seconds: float = 2.0,
) -> dict[str, float]:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import wait_for_tokens_ltp  # type: ignore
        return wait_for_tokens_ltp(tokens, timeout_seconds)
    from features.kite_broker_ws import wait_for_tokens_ltp  # type: ignore
    return wait_for_tokens_ltp(tokens, timeout_seconds)


def broker_stop_all() -> None:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import stop_all  # type: ignore
        stop_all()
    else:
        from features.kite_broker_ws import stop_all  # type: ignore
        stop_all()


# ── REST client / OAuth ───────────────────────────────────────────────────────

def get_broker_rest_client_with_token(access_token: str):
    """
    Return a broker REST client pre-configured with a specific access_token.
    Kite: KiteConnect(api_key).set_access_token(token)
    Dhan: dhanhq.dhanhq(client_id, token)
    """
    if _active_broker() == 'dhan':
        try:
            from features.dhan_broker_ws import get_common_api_key  # type: ignore
            import dhanhq  # type: ignore
            client_id = get_common_api_key() or ''
            return dhanhq.dhanhq(client_id, str(access_token or '').strip())
        except Exception:
            return None
    from features.kite_broker import get_kite_instance  # type: ignore
    return get_kite_instance(access_token)


def broker_get_login_url() -> str:
    # No Dhan-active gate here: Dhan doesn't use this OAuth redirect flow at
    # all (it has its own consent-link flow), so these three functions are
    # only ever exercised for Kite — gating them on "is Dhan the globally
    # active market-data broker" only broke a user's ability to log into
    # their own Kite account whenever Dhan happened to be active elsewhere.
    from features.kite_broker import get_login_url  # type: ignore
    return get_login_url()


def broker_generate_session(request_token: str):
    from features.kite_broker import generate_session  # type: ignore
    return generate_session(request_token)


def save_broker_session(db, broker_doc_id: str, session: dict) -> None:
    from features.kite_broker import save_kite_session  # type: ignore
    save_kite_session(db, broker_doc_id, session)


def get_stored_broker_access_token(db) -> str:
    if _active_broker() == 'dhan':
        try:
            raw = db._db if hasattr(db, '_db') else db
            cfg = raw['kite_market_config'].find_one({'broker': 'dhan', 'enabled': True}) or {}
            return str(cfg.get('access_token') or '').strip()
        except Exception:
            return ''
    from features.kite_broker import get_stored_access_token  # type: ignore
    return get_stored_access_token(db)


# ── Broker-specific constants ─────────────────────────────────────────────────
# Lazy dicts/objects so the correct values are used regardless of when imported.

_KITE_INDEX_TOKENS: dict[str, int] = {
    'NIFTY': 256265, 'BANKNIFTY': 260105, 'SENSEX': 265,
    'FINNIFTY': 257801, 'MIDCPNIFTY': 288009, 'INDIA_VIX': 264969,
}
_DHAN_INDEX_TOKENS: dict[str, int] = {
    'NIFTY': 13, 'BANKNIFTY': 25, 'SENSEX': 51,
    'FINNIFTY': 27, 'MIDCPNIFTY': 11915, 'INDIA_VIX': 20225,
}


class _LazyBrokerTokenDict(dict):
    """
    Resolves to the active broker's index tokens on every access — NOT a
    load-once cache. _active_broker() is itself already cached (a single
    DB read, then a list lookup), so re-checking it here is cheap, and it
    closes a real failure mode: if this dict's first-ever access happened
    before _active_broker() had resolved to 'dhan' (e.g. a transient/early
    call), the old load-once version locked in Kite's token IDs for the
    rest of the process — every later caller would keep getting Kite
    security IDs fed into Dhan REST quote calls (NSE_FNO segment), which
    Dhan correctly rejects/returns empty for, with no obvious error.
    """
    def _load(self) -> None:
        self.clear()
        self.update(_DHAN_INDEX_TOKENS if _active_broker() == 'dhan' else _KITE_INDEX_TOKENS)

    def get(self, key, default=None):  # type: ignore[override]
        self._load(); return super().get(key, default)

    def __getitem__(self, key):
        self._load(); return super().__getitem__(key)

    def __contains__(self, key):  # type: ignore[override]
        self._load(); return super().__contains__(key)

    def __iter__(self):
        self._load(); return super().__iter__()

    def items(self):   self._load(); return super().items()    # type: ignore[override]
    def keys(self):    self._load(); return super().keys()     # type: ignore[override]
    def values(self):  self._load(); return super().values()   # type: ignore[override]


class _LazyVIXToken:
    """Scalar proxy for BROKER_VIX_TOKEN — resolves to correct value on first use."""
    def _val(self) -> int:
        return 20225 if _active_broker() == 'dhan' else 264969
    def __int__(self):          return self._val()
    def __index__(self):        return self._val()
    def __str__(self):          return str(self._val())
    def __repr__(self):         return repr(self._val())
    def __eq__(self, other):    return self._val() == other  # type: ignore[override]
    def __hash__(self):         return hash(self._val())
    def __format__(self, spec): return format(self._val(), spec)


BROKER_INDEX_TOKENS: dict = _LazyBrokerTokenDict()
BROKER_VIX_TOKEN         = _LazyVIXToken()
BROKER_CONFIG_COLLECTION: str = 'kite_market_config'


def get_broker_index_token(underlying: str) -> int:
    return BROKER_INDEX_TOKENS.get(str(underlying or '').strip().upper(), 0)


# ── Market data: instruments, expiries, contracts, quotes ─────────────────────
# Lazy imports prevent circular dependency with spot_atm_utils
# (spot_atm_utils imports broker_gateway for credentials + LTP).

def load_broker_instruments(force: bool = False) -> dict:
    if _active_broker() == 'dhan':
        from features.spot_atm_utils import _load_dhan_instruments  # type: ignore
        return _load_dhan_instruments(force=force)
    from features.spot_atm_utils import _load_kite_instruments  # type: ignore
    return _load_kite_instruments(force=force)


def get_broker_expiries(underlying: str, from_date: str, *, force_refresh: bool = False) -> list[str]:
    if _active_broker() == 'dhan':
        from features.spot_atm_utils import get_dhan_expiries  # type: ignore
        return get_dhan_expiries(underlying, from_date, force_refresh=force_refresh)
    from features.spot_atm_utils import get_kite_expiries  # type: ignore
    return get_kite_expiries(underlying, from_date, force_refresh=force_refresh)


def list_broker_option_contracts(underlying: str, expiry: str, *, force_refresh: bool = False) -> list[dict]:
    if _active_broker() == 'dhan':
        from features.spot_atm_utils import list_dhan_option_contracts  # type: ignore
        return list_dhan_option_contracts(underlying, expiry, force_refresh=force_refresh)
    from features.spot_atm_utils import list_kite_option_contracts  # type: ignore
    return list_kite_option_contracts(underlying, expiry, force_refresh=force_refresh)


def get_broker_chain_doc(underlying: str, expiry: str, strike: float, option_type: str) -> dict:
    if _active_broker() == 'dhan':
        from features.spot_atm_utils import get_dhan_chain_doc  # type: ignore
        return get_dhan_chain_doc(underlying, expiry, strike, option_type)
    from features.spot_atm_utils import get_kite_chain_doc  # type: ignore
    return get_kite_chain_doc(underlying, expiry, strike, option_type)


def fetch_broker_quotes_for_expiry(underlying: str, expiry: str) -> dict[str, float]:
    if _active_broker() == 'dhan':
        from features.spot_atm_utils import fetch_dhan_quotes_for_expiry  # type: ignore
        return fetch_dhan_quotes_for_expiry(underlying, expiry)
    from features.spot_atm_utils import fetch_kite_quotes_for_expiry  # type: ignore
    return fetch_kite_quotes_for_expiry(underlying, expiry)


def get_broker_rest_client(db=None):
    """
    Return a broker REST client configured from credentials.
    Kite: KiteConnect  |  Dhan: dhanhq instance
    """
    try:
        broker = _active_broker()
        if broker == 'dhan':
            from features.dhan_broker_ws import (  # type: ignore
                get_common_credentials, load_credentials_from_db,
            )
            if db is not None:
                load_credentials_from_db(db)
            client_id, access_token = get_common_credentials()
            if not client_id or not access_token:
                return None
            import dhanhq  # type: ignore
            return dhanhq.dhanhq(client_id, access_token)
        else:
            if db is not None:
                from features.kite_delta_chain import _get_kite_credentials  # type: ignore
                api_key, access_token = _get_kite_credentials(db)
            else:
                api_key, access_token = get_broker_credentials()
            if not api_key or not access_token:
                return None
            from kiteconnect import KiteConnect  # type: ignore
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            return kite
    except Exception:
        return None


# ── Black-Scholes helpers (broker-agnostic math) ──────────────────────────────

def get_bs_helpers():
    """
    Returns (_calc_iv, _calc_greeks, _time_to_expiry,
             _RISK_FREE_RATE, _DIVIDEND_YIELDS, _DEFAULT_DIVIDEND_YIELD).
    Math is broker-agnostic — only data inputs differ per broker.
    """
    from features.kite_delta_chain import (  # type: ignore
        _calc_iv, _calc_greeks, _time_to_expiry,
        _RISK_FREE_RATE, _DIVIDEND_YIELDS, _DEFAULT_DIVIDEND_YIELD,
    )
    return (
        _calc_iv, _calc_greeks, _time_to_expiry,
        _RISK_FREE_RATE, _DIVIDEND_YIELDS, _DEFAULT_DIVIDEND_YIELD,
    )


def get_broker_credentials_from_db(db=None) -> tuple[str, str]:
    """
    (api_key/client_id, access_token) from in-memory cache or DB.
    """
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import (  # type: ignore
            get_common_credentials, load_credentials_from_db,
        )
        if db is not None:
            load_credentials_from_db(db)
        return get_common_credentials()
    from features.kite_delta_chain import _get_kite_credentials  # type: ignore
    return _get_kite_credentials(db)
