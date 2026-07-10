"""
dhan_broker_ws.py
─────────────────
Dhan credential management + WebSocket subscription interface.

Mirrors kite_broker_ws.py API exactly — broker_gateway.py imports from
here when Dhan is the active broker. No other file should import this
directly; always go through broker_gateway.

Credentials are stored in kite_market_config collection:
  {broker: "dhan", enabled: true, user_id: "...", access_token: "..."}
"""

from __future__ import annotations

import threading
import time
import logging

log = logging.getLogger(__name__)

# ── In-memory credential cache ────────────────────────────────────────────────
_cred_lock     = threading.Lock()
_client_id:     str = ""
_access_token:  str = ""


# ── Credentials ───────────────────────────────────────────────────────────────

def set_common_credentials(client_id: str, access_token: str) -> None:
    global _client_id, _access_token
    with _cred_lock:
        _client_id    = str(client_id or "").strip()
        _access_token = str(access_token or "").strip()


def get_common_credentials() -> tuple[str, str]:
    with _cred_lock:
        return _client_id, _access_token


def get_common_api_key() -> str:
    """Returns client_id (Dhan equivalent of api_key)."""
    with _cred_lock:
        return _client_id


def is_configured() -> bool:
    with _cred_lock:
        return bool(_client_id and _access_token)


def load_credentials_from_db(db) -> bool:
    """
    Load Dhan credentials from kite_market_config (broker=dhan, enabled=true).
    Returns True if credentials were found and loaded.
    """
    try:
        raw = db._db if hasattr(db, "_db") else db
        cfg = raw["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
        client_id    = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
        access_token = str(cfg.get("access_token") or "").strip()
        if client_id and access_token:
            set_common_credentials(client_id, access_token)
            return True
        return False
    except Exception as exc:
        log.warning("[dhan_broker_ws] load_credentials_from_db error: %s", exc)
        return False


def save_access_token_to_db(db, access_token: str) -> None:
    try:
        raw = db._db if hasattr(db, "_db") else db
        raw["kite_market_config"].update_one(
            {"broker": "dhan", "enabled": True},
            {"$set": {"access_token": str(access_token or "").strip()}},
        )
        with _cred_lock:
            global _access_token
            _access_token = str(access_token or "").strip()
    except Exception as exc:
        log.warning("[dhan_broker_ws] save_access_token_to_db error: %s", exc)


def save_credentials_to_db(db, client_id: str, access_token: str) -> None:
    try:
        raw = db._db if hasattr(db, "_db") else db
        raw["kite_market_config"].update_one(
            {"broker": "dhan"},
            {"$set": {
                "broker":       "dhan",
                "enabled":      True,
                "user_id":      str(client_id or "").strip(),
                "access_token": str(access_token or "").strip(),
            }},
            upsert=True,
        )
        set_common_credentials(client_id, access_token)
    except Exception as exc:
        log.warning("[dhan_broker_ws] save_credentials_to_db error: %s", exc)


def get_login_url() -> str:
    """Dhan does not use OAuth redirect flow — returns empty string."""
    return ""


def generate_access_token(request_token: str) -> str:
    """Not applicable for Dhan (no OAuth token exchange)."""
    raise NotImplementedError("Dhan does not use request_token OAuth flow")


def validate_access_token(access_token: str = "") -> bool:
    """Validate by calling Dhan profile API."""
    try:
        import requests as _req  # type: ignore
        tok = access_token or _access_token
        if not tok:
            return False
        resp = _req.get(
            "https://api.dhan.co/v2/profile",
            headers={"access-token": tok, "Content-Type": "application/json"},
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False


# ── LTP map ───────────────────────────────────────────────────────────────────

def get_ltp_map() -> dict[str, float]:
    try:
        from features.dhan_ticker import dhan_ticker_manager
        return dict(dhan_ticker_manager.ltp_map or {})
    except Exception:
        return {}


# ── Tick listeners ────────────────────────────────────────────────────────────

def add_tick_listener(listener) -> bool:
    try:
        from features.dhan_ticker import dhan_ticker_manager
        dhan_ticker_manager.add_tick_listener(listener)
        return True
    except Exception:
        return False


def remove_tick_listener(listener) -> None:
    try:
        from features.dhan_ticker import dhan_ticker_manager
        dhan_ticker_manager.remove_tick_listener(listener)
    except Exception:
        pass


# ── Token subscription management ────────────────────────────────────────────
# Dhan has no per-user concept — subscriptions go directly to the WS feed.
# register_user_tokens / refresh_user_tokens just subscribe the tokens to Dhan WS.

def extract_instrument_tokens(positions: list[dict]) -> list[int]:
    """
    Extract Dhan security IDs from position dicts.
    Dhan security IDs are numeric strings, returned as ints for API compatibility.
    """
    result: list[int] = []
    seen: set[int] = set()
    for pos in (positions or []):
        tok_raw = pos.get("token") or pos.get("instrument_token") or pos.get("security_id")
        if not tok_raw:
            continue
        try:
            tok_int = int(str(tok_raw).strip())
            if tok_int and tok_int not in seen:
                seen.add(tok_int)
                result.append(tok_int)
        except (ValueError, TypeError):
            pass
    return result


def register_user_tokens(user_id: str, tokens: list) -> bool:
    """Subscribe tokens to Dhan WS feed (user_id is informational only)."""
    try:
        from features.dhan_ticker import dhan_ticker_manager
        str_ids = [str(t) for t in (tokens or []) if t]
        if str_ids:
            dhan_ticker_manager.subscribe_tokens(str_ids, exchange="NSE_FNO")
        return True
    except Exception as exc:
        log.warning("[dhan_broker_ws] register_user_tokens error: %s", exc)
        return False


def unregister_user(user_id: str) -> None:
    """No-op for Dhan — no per-user subscription tracking needed."""
    pass


def refresh_user_tokens(user_id: str, new_tokens: list) -> bool:
    """Re-subscribe tokens (same as register for Dhan)."""
    return register_user_tokens(user_id, new_tokens)


def wait_for_tokens_ltp(
    tokens: list,
    timeout_seconds: float = 2.0,
) -> dict[str, float]:
    """
    Wait until all given tokens have LTP in dhan_ticker_manager.ltp_map.
    Returns the ltp_map subset for those tokens.
    """
    try:
        from features.dhan_ticker import dhan_ticker_manager
        str_toks = [str(t) for t in (tokens or []) if t]
        if not str_toks:
            return {}
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            ltp_map = dhan_ticker_manager.ltp_map
            if all(ltp_map.get(t) for t in str_toks):
                return {t: float(ltp_map[t]) for t in str_toks if ltp_map.get(t)}
            time.sleep(0.05)
        ltp_map = dhan_ticker_manager.ltp_map
        return {t: float(ltp_map[t]) for t in str_toks if ltp_map.get(t)}
    except Exception:
        return {}


def stop_all() -> None:
    try:
        from features.dhan_ticker import dhan_ticker_manager
        dhan_ticker_manager.stop()
    except Exception:
        pass
