"""
auth.py
───────
App-user authentication (separate from broker login).

  • Login identifier : mobile number, stored as plain 10-digit string;
                        country code is kept in its own field (default +91)
  • Password          : bcrypt hashed, never stored/returned in plain text
  • Access token      : JWT (HS256), signed with JWT_SECRET_KEY from .env

MongoDB collection: user_details
  {mobile, country_code, name, email, password, referral_code, created_at, is_active}
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

_IST = ZoneInfo("Asia/Kolkata")


def _next_expiry_ist() -> datetime:
    """Next 07:30 AM IST — token always expires at market-open prep time."""
    now_ist = datetime.now(_IST)
    target  = now_ist.replace(hour=7, minute=30, second=0, microsecond=0)
    if now_ist >= target:
        target += timedelta(days=1)
    return target.astimezone(timezone.utc)
from typing import Any, Optional

import bcrypt
import jwt
from fastapi import HTTPException, Header
from pydantic import BaseModel


class RegisterIn(BaseModel):
    mobile: str
    name: str
    email: str
    password: str
    referral_code: Optional[str] = None


class LoginIn(BaseModel):
    mobile: str
    password: str


JWT_SECRET_KEY    = os.getenv("JWT_SECRET_KEY", "")
JWT_ALGORITHM      = "HS256"
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "10080"))  # default 7 days

MOBILE_RE = re.compile(r"^[6-9]\d{9}$")
EMAIL_RE  = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

USERS_COLLECTION    = "user_details"
DEFAULT_COUNTRY_CODE = "+91"

_ANONYMOUS_USER_STUB: dict[str, Any] = {
    "_id": None, "mobile": "", "country_code": DEFAULT_COUNTRY_CODE,
    "name": "Auth Disabled", "email": "", "referral_code": None, "created_at": "",
}


def _env_flag_enabled(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def normalize_mobile(raw: str) -> str:
    """Strip spaces/+91/leading 0, validate 10-digit Indian mobile, return 'XXXXXXXXXX'."""
    digits = re.sub(r"\D", "", raw or "")
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    digits = digits.lstrip("0")
    if not MOBILE_RE.match(digits):
        raise HTTPException(status_code=400, detail="Enter a valid 10-digit mobile number")
    return digits


def normalize_email(raw: str) -> str:
    email = (raw or "").strip().lower()
    if not EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail="Enter a valid email address")
    return email


def hash_password(password: str) -> str:
    if not password or len(password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except (ValueError, TypeError):
        return False


def create_access_token(user_id: str, mobile: str) -> str:
    if not JWT_SECRET_KEY:
        raise HTTPException(status_code=500, detail="JWT_SECRET_KEY is not configured")
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "mobile": mobile,
        "iat": now,
        "exp": _next_expiry_ist(),
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> dict[str, Any]:
    try:
        return jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Session expired, please login again")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid authentication token")


def public_user(doc: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(doc.get("_id")),
        "mobile": doc.get("mobile"),
        "country_code": doc.get("country_code") or DEFAULT_COUNTRY_CODE,
        "name": doc.get("name"),
        "email": doc.get("email"),
        "referral_code": doc.get("referral_code"),
        "created_at": doc.get("created_at"),
        "is_admin": bool(doc.get("is_admin")),
        "is_active": bool(doc.get("is_active", True)),
        # Set from the Profile page's "Link Telegram" flow — see /auth/telegram-username
        # and features.telegram_notifier (set_pending_telegram_username,
        # telegram_linking_poll_loop, notify_user_for), which every service
        # (trade/simulator/scanner/chart) routes per-user notifications through.
        # telegram_linked only flips true once the user has actually messaged the
        # bot and the poll loop matched their username to a real chat_id.
        "telegram_username": doc.get("telegram_username") or "",
        "telegram_linked": bool(doc.get("telegram_linked")),
    }


def register_user(db, payload: dict[str, Any]) -> dict[str, Any]:
    mobile = normalize_mobile(payload.get("mobile", ""))
    email  = normalize_email(payload.get("email", ""))
    name   = (payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name is required")
    referral_code = (payload.get("referral_code") or "").strip() or None
    password_hash  = hash_password(payload.get("password", ""))

    col = db._db[USERS_COLLECTION]
    if col.find_one({"mobile": mobile}, {"_id": 1}):
        raise HTTPException(status_code=409, detail="Mobile number is already registered")
    if col.find_one({"email": email}, {"_id": 1}):
        raise HTTPException(status_code=409, detail="Email is already registered")

    doc = {
        "mobile": mobile,
        "country_code": DEFAULT_COUNTRY_CODE,
        "name": name,
        "email": email,
        "password": password_hash,
        "referral_code": referral_code,
        "is_active": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    result = col.insert_one(doc)
    doc["_id"] = result.inserted_id
    return doc


def authenticate_user(db, mobile_raw: str, password: str) -> dict[str, Any]:
    col = db._db[USERS_COLLECTION]
    # Try raw lookup first — handles special accounts (e.g. admin) whose stored
    # mobile doesn't survive normalize_mobile (leading zero, non-standard format).
    doc = col.find_one({"mobile": (mobile_raw or "").strip()})
    if not doc:
        mobile = normalize_mobile(mobile_raw)
        doc = col.find_one({"mobile": mobile})
    if not doc or not verify_password(password, doc.get("password", "")):
        raise HTTPException(status_code=401, detail="Invalid mobile number or password")
    if not doc.get("is_active", True):
        raise HTTPException(status_code=403, detail="Account is disabled")
    return doc


def _authenticate_request(authorization: Optional[str]) -> dict[str, Any]:
    import time as _t  # TEMPORARY DIAGNOSTIC — remove with the prints below
    _t0 = _t.perf_counter()
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing authentication token")
    token = authorization.split(" ", 1)[1].strip()
    claims = decode_access_token(token)
    print(f"[TIMING auth] decode_access_token done t={_t.perf_counter()-_t0:.3f}s")

    from bson import ObjectId
    from features.mongo_data import MongoData

    db = MongoData()
    print(f"[TIMING auth] MongoData() instance ready t={_t.perf_counter()-_t0:.3f}s")
    col = db._db[USERS_COLLECTION]
    try:
        doc = col.find_one({"_id": ObjectId(claims["sub"])})
        print(f"[TIMING auth] find_one user done t={_t.perf_counter()-_t0:.3f}s")
    except Exception as exc:
        print(f"[TIMING auth] find_one user FAILED: {exc} t={_t.perf_counter()-_t0:.3f}s")
        doc = None
    if not doc:
        raise HTTPException(status_code=401, detail="User not found")
    return doc


def get_current_user(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    """FastAPI dependency — reads 'Authorization: Bearer <token>', returns the user doc.

    AUTH_ENFORCEMENT_ENABLED is a kill switch (default off) for every route that
    Depends() on this — flip it on once the simulator/trade endpoints are ready
    to require a logged-in session again; until then every caller resolves to
    an anonymous stub instead of 401ing.
    """
    if not _env_flag_enabled("AUTH_ENFORCEMENT_ENABLED", default=False):
        return _ANONYMOUS_USER_STUB
    return _authenticate_request(authorization)


def require_current_user(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    """Same as get_current_user but ignores AUTH_ENFORCEMENT_ENABLED — always
    requires a valid, non-expired Bearer token.

    Use this (instead of get_current_user) on routes being migrated to real
    auth one at a time, so they start enforcing immediately without flipping
    the still-off global kill switch that every other Depends(get_current_user)
    route depends on.
    """
    return _authenticate_request(authorization)
