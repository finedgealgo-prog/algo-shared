"""
instrument_ref_manager.py
──────────────────────────
Cross-process reference counting for "who currently wants live ticks for
this instrument/token" — chart viewers, running strategies, alerts, etc.

Why this exists: neither live_quote_socket.py's per-session subscribed_tokens
nor dhan_ticker_manager.subscribed_tokens ever shrinks today (Dhan's
unsubscribe is a no-op — see dhan_ticker.py's _DhanCompatTicker.unsubscribe).
This module is the shared source of truth for "is anyone still watching X",
so callers (chain-pool admission control, chart attach/detach) can make that
decision without needing a real broker-side unsubscribe.

Backed by one Redis ZSET per key: `instrument_ref:{key}`, member =
"{ref_type}:{ref_id}" (e.g. "chart:<session_id>", "strategy:<trade_id>"),
score = last-heartbeat unix epoch. TTL-based expiry is the crash-safety net
for callers that disconnect without ever calling release() (tab crash,
network drop) — explicit release() stays the fast, immediate path.

Redis already runs locally for this project (see backtest_engine.py's
_get_redis() for the same lazy-connect pattern reused here).
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

_KEY_PREFIX = "instrument_ref:"
DEFAULT_TTL_SECONDS = 45.0

_redis_client = None  # lazy, one connection per process


def _get_redis():
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    import redis
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    r.ping()
    _redis_client = r
    return _redis_client


def _zkey(key: str) -> str:
    return f"{_KEY_PREFIX}{key}"


def _member(ref_type: str, ref_id: str) -> str:
    return f"{ref_type}:{ref_id}"


def acquire(key: str, ref_type: str, ref_id: str) -> None:
    """Register (or heartbeat-refresh) interest in `key` from (ref_type, ref_id).
    Idempotent — safe to call repeatedly as a heartbeat, not just on first attach."""
    key = str(key or "").strip()
    if not key:
        return
    try:
        _get_redis().zadd(_zkey(key), {_member(ref_type, ref_id): time.time()})
    except Exception as exc:
        logger.warning("[instrument_ref] acquire error key=%s ref=%s:%s: %s", key, ref_type, ref_id, exc)


def release(key: str, ref_type: str, ref_id: str) -> None:
    """Explicit detach — symbol switch, unsubscribe, session close, strategy exit."""
    key = str(key or "").strip()
    if not key:
        return
    try:
        _get_redis().zrem(_zkey(key), _member(ref_type, ref_id))
    except Exception as exc:
        logger.warning("[instrument_ref] release error key=%s ref=%s:%s: %s", key, ref_type, ref_id, exc)


def refcount(key: str, ttl_seconds: float = DEFAULT_TTL_SECONDS) -> int:
    """Prune stale (unheartbeated) members, then return the live count."""
    key = str(key or "").strip()
    if not key:
        return 0
    zkey = _zkey(key)
    try:
        r = _get_redis()
        r.zremrangebyscore(zkey, "-inf", time.time() - ttl_seconds)
        return r.zcard(zkey)
    except Exception as exc:
        logger.warning("[instrument_ref] refcount error key=%s: %s", key, exc)
        return 0


def active_keys(ttl_seconds: float = DEFAULT_TTL_SECONDS) -> list[str]:
    """All instrument/token keys that currently have at least one live (non-expired) ref."""
    try:
        r = _get_redis()
        keys = [k[len(_KEY_PREFIX):] for k in r.scan_iter(match=f"{_KEY_PREFIX}*")]
    except Exception as exc:
        logger.warning("[instrument_ref] active_keys scan error: %s", exc)
        return []
    return [k for k in keys if refcount(k, ttl_seconds) > 0]


def oldest_heartbeat(key: str) -> float | None:
    """Unix epoch of the least-recently-refreshed ref on `key`, or None if empty.
    Used by eviction to pick which instrument to drop first when at capacity."""
    key = str(key or "").strip()
    if not key:
        return None
    try:
        result = _get_redis().zrange(_zkey(key), 0, 0, withscores=True)
        return result[0][1] if result else None
    except Exception as exc:
        logger.warning("[instrument_ref] oldest_heartbeat error key=%s: %s", key, exc)
        return None
