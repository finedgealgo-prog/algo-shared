"""
Proxy for Sensibull's public "Verified PnL / Hall of Fame" leaderboard
(https://web.sensibull.com/verified-pnl/hall-of-fame) — an opt-in feature
where Sensibull users choose to publicly share their live P&L.

The underlying oxide.sensibull.com endpoint requires a short-lived anonymous
session token (`access_token` cookie, ~hours-long expiry) that Sensibull's
own web app mints client-side; there is no documented way to obtain one
server-side. So the token is stored in Mongo and refreshed manually via
`set_hall_of_fame_token()` (see the admin-only /simulator/sensibull-hall-of-
fame/token endpoint) whenever Sensibull's endpoint starts returning 401 —
this proxy cannot self-refresh it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests

_HOF_URL = "https://oxide.sensibull.com/v1/compute/vbys/hall_of_fame/live_pnl"
_POSITIONS_SNAPSHOT_URL = "https://oxide.sensibull.com/v1/compute/verified_by_sensibull/live_positions/snapshot/{word_hash}"
_FAVORITES_URL = "https://oxide.sensibull.com/v1/compute/1/vbys/user/favorites"
_SETTINGS_DOC_ID = "sensibull_hall_of_fame_token"
_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _resolve_db(db: Any):
    return db._db if hasattr(db, "_db") else db


def _error_for_response(response: "requests.Response") -> dict | None:
    """Shared 401/429/other-non-2xx mapping for both Sensibull endpoints below."""
    if response.status_code == 401:
        return {
            "ok": False,
            "detail": "Sensibull session expired. Please update the token.",
            "token_expired": True,
        }
    if response.status_code == 429:
        # Surfaced as-is, no retry here — a retry loop against a rate limit
        # is exactly the "keep hammering it" behavior this proxy is meant to
        # avoid. The caller just waits and reloads manually.
        return {
            "ok": False,
            "detail": "Sensibull is rate-limiting this request. Please wait a bit before retrying.",
            "rate_limited": True,
        }
    if not response.ok:
        return {"ok": False, "detail": f"Sensibull API error ({response.status_code})"}
    return None


def get_hall_of_fame_token(db: Any) -> str:
    raw_db = _resolve_db(db)
    doc = raw_db["app_settings"].find_one({"_id": _SETTINGS_DOC_ID}) or {}
    return str(doc.get("access_token") or "").strip()


def set_hall_of_fame_token(db: Any, access_token: str) -> None:
    raw_db = _resolve_db(db)
    raw_db["app_settings"].update_one(
        {"_id": _SETTINGS_DOC_ID},
        {"$set": {"access_token": str(access_token or "").strip(), "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


def get_hall_of_fame_token_status(db: Any) -> dict:
    """Lets the admin UI show "token last updated Xh ago" without exposing the token itself."""
    raw_db = _resolve_db(db)
    doc = raw_db["app_settings"].find_one({"_id": _SETTINGS_DOC_ID}) or {}
    token = str(doc.get("access_token") or "").strip()
    updated_at = doc.get("updated_at")
    return {
        "configured": bool(token),
        "updated_at": updated_at.isoformat() if isinstance(updated_at, datetime) else None,
    }


def fetch_hall_of_fame(
    db: Any,
    page: int = 1,
    per_page: int = 10,
    sort_by: str = "pl_desc",
    search_term: str = "",
    is_following: bool = False,
) -> dict:
    token = get_hall_of_fame_token(db)
    if not token:
        return {
            "ok": False,
            "detail": "Sensibull hall-of-fame token not configured.",
            "token_missing": True,
            "results": [],
            "total_pages": 0,
        }

    try:
        response = requests.post(
            _HOF_URL,
            headers={
                "accept": "application/json, text/plain, */*",
                "content-type": "application/json",
                "origin": "https://web.sensibull.com",
                "referer": "https://web.sensibull.com/",
                "user-agent": _USER_AGENT,
            },
            cookies={"access_token": token},
            json={
                "sort_by": sort_by,
                "page": page,
                "per_page": per_page,
                "search_term": search_term,
                "is_following": is_following,
            },
            timeout=10,
        )
    except Exception as exc:
        return {"ok": False, "detail": f"Request to Sensibull failed: {exc}", "results": [], "total_pages": 0}

    error = _error_for_response(response)
    if error is not None:
        return {**error, "results": [], "total_pages": 0}

    try:
        payload = response.json()
    except Exception as exc:
        return {"ok": False, "detail": f"Invalid response from Sensibull: {exc}", "results": [], "total_pages": 0}

    if not payload.get("success"):
        return {"ok": False, "detail": "Sensibull API returned an unsuccessful response.", "results": [], "total_pages": 0}

    data = payload.get("payload") or {}
    return {
        "ok": True,
        "results": data.get("results") or [],
        "total_pages": int(data.get("total_pages") or 0),
        "page": page,
        "per_page": per_page,
    }


def fetch_live_positions_snapshot(db: Any, word_hash: str) -> dict:
    """
    A specific trader's live positions — the detail view reached by clicking
    a row on the hall-of-fame leaderboard (`profile.word_hash` from
    fetch_hall_of_fame's results is the slug this takes).
    """
    token = get_hall_of_fame_token(db)
    if not token:
        return {"ok": False, "detail": "Sensibull hall-of-fame token not configured.", "token_missing": True}

    normalized_word_hash = str(word_hash or "").strip()
    if not normalized_word_hash:
        return {"ok": False, "detail": "Missing trader identifier."}

    try:
        response = requests.get(
            _POSITIONS_SNAPSHOT_URL.format(word_hash=normalized_word_hash),
            headers={
                "accept": "application/json, text/plain, */*",
                "origin": "https://web.sensibull.com",
                "referer": "https://web.sensibull.com/",
                "user-agent": _USER_AGENT,
            },
            cookies={"access_token": token},
            timeout=10,
        )
    except Exception as exc:
        return {"ok": False, "detail": f"Request to Sensibull failed: {exc}"}

    error = _error_for_response(response)
    if error is not None:
        return error

    try:
        payload = response.json()
    except Exception as exc:
        return {"ok": False, "detail": f"Invalid response from Sensibull: {exc}"}

    if not payload.get("success"):
        return {"ok": False, "detail": "Sensibull API returned an unsuccessful response."}

    snapshot = (payload.get("payload") or {}).get("position_snapshot_data") or {}
    return {
        "ok": True,
        "created_at": snapshot.get("created_at") or "",
        "total_profit": snapshot.get("total_profit"),
        "roi": snapshot.get("roi"),
        "total_capital": snapshot.get("total_capital"),
        "underlyings": snapshot.get("data") or [],
    }


def fetch_hall_of_fame_favorites(db: Any) -> dict:
    """
    Traders the Sensibull account behind our stored token has starred as a
    favorite on web.sensibull.com — a flat, unpaginated list (unlike the main
    leaderboard), each entry shaped `{profile: {word_hash, full_name,
    twitter_profile, followers_count, ...}, snapshot: {total_profit, roi,
    created_at} | null}`. `profile.word_hash` is the same identifier the main
    leaderboard's `profile.word_hash` uses, so the frontend can cross-reference
    the two (e.g. to badge a leaderboard row as already-favorited) purely off
    that shared key.
    """
    token = get_hall_of_fame_token(db)
    if not token:
        return {"ok": False, "detail": "Sensibull hall-of-fame token not configured.", "token_missing": True, "results": []}

    try:
        response = requests.get(
            _FAVORITES_URL,
            headers={
                "accept": "application/json, text/plain, */*",
                "origin": "https://web.sensibull.com",
                "referer": "https://web.sensibull.com/",
                "user-agent": _USER_AGENT,
            },
            cookies={"access_token": token},
            timeout=10,
        )
    except Exception as exc:
        return {"ok": False, "detail": f"Request to Sensibull failed: {exc}", "results": []}

    error = _error_for_response(response)
    if error is not None:
        return {**error, "results": []}

    try:
        payload = response.json()
    except Exception as exc:
        return {"ok": False, "detail": f"Invalid response from Sensibull: {exc}", "results": []}

    if not payload.get("success"):
        return {"ok": False, "detail": "Sensibull API returned an unsuccessful response.", "results": []}

    return {"ok": True, "results": payload.get("payload") or []}


_APP_FAVORITES_COLLECTION = "hall_of_fame_favorites"


def list_app_favorites(db: Any, user_id: str) -> dict:
    """
    Traders *this app's* logged-in user has starred from our own Hall of Fame page —
    stored in our own DB, independent of fetch_hall_of_fame_favorites above (Sensibull's
    own account-level favorites, which we can only read, never set — there is no
    documented "add favorite" API for that). Denormalized at favorite-time (see
    add_app_favorite) so the Favourite tab renders without a fresh Sensibull round trip
    and stays populated even for a trader who has since scrolled off the current
    leaderboard page/search/sort.
    """
    raw_db = _resolve_db(db)
    docs = raw_db[_APP_FAVORITES_COLLECTION].find({"user_id": str(user_id)}).sort("created_at", -1)
    results = [
        {
            "word_hash": d.get("word_hash", ""),
            "name": d.get("name", ""),
            "user_name": d.get("user_name", ""),
            "profile_image_url": d.get("profile_image_url", ""),
            "followers_count": d.get("followers_count"),
            "live_since": d.get("live_since"),
            "total_profit": d.get("total_profit"),
            "roi": d.get("roi"),
            "total_capital": d.get("total_capital"),
        }
        for d in docs
    ]
    return {"ok": True, "results": results}


def add_app_favorite(db: Any, user_id: str, entry: dict) -> dict:
    word_hash = str(entry.get("word_hash") or "").strip()
    if not word_hash:
        return {"ok": False, "detail": "Missing word_hash"}
    raw_db = _resolve_db(db)
    raw_db[_APP_FAVORITES_COLLECTION].update_one(
        {"user_id": str(user_id), "word_hash": word_hash},
        {
            "$set": {
                "user_id": str(user_id),
                "word_hash": word_hash,
                "name": str(entry.get("name") or ""),
                "user_name": str(entry.get("user_name") or ""),
                "profile_image_url": str(entry.get("profile_image_url") or ""),
                "followers_count": entry.get("followers_count"),
                "live_since": str(entry.get("live_since") or ""),
                "total_profit": entry.get("total_profit"),
                "roi": entry.get("roi"),
                "total_capital": entry.get("total_capital"),
            },
            "$setOnInsert": {"created_at": datetime.now(timezone.utc)},
        },
        upsert=True,
    )
    return {"ok": True}


def remove_app_favorite(db: Any, user_id: str, word_hash: str) -> dict:
    raw_db = _resolve_db(db)
    raw_db[_APP_FAVORITES_COLLECTION].delete_one({"user_id": str(user_id), "word_hash": str(word_hash)})
    return {"ok": True}
