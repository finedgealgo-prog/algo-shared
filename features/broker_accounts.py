"""
broker_accounts.py
───────────────────
Per-app-user view over `broker_configuration` — "which broker accounts does
this user have connected, and is each one's session still good right now."

`broker_configuration.user_id` is the BROKER's own account/client code (e.g.
FlatTrade's "FT056897", read by live_order_manager.get_broker_for_trade and
the FlatTrade OAuth redirect's lookup-by-client-id fallback) — never repurpose
it. `app_user_id` is the separate field for "which app user owns this broker
connection," following the same `_resolve_app_user_id()` convention already
used on algo_trade/strategy documents in api.py.
"""

from __future__ import annotations

from datetime import datetime, timezone

from bson import ObjectId

# Single hardcoded app-user id — this admin app has no real login/session
# system of its own yet; every document that needs an "owning user" stamps
# this same value (see api.py's _resolve_app_user_id, the original source
# of this constant).
DEFAULT_APP_USER_ID = "69dcf52711877c164638d2a7"


def clear_broker_configuration_access_token(db_handle, broker_doc_id: str) -> None:
    if not broker_doc_id:
        return
    db_handle["broker_configuration"].update_one(
        {"_id": ObjectId(broker_doc_id)},
        {"$set": {
            "access_token": "",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }},
    )


def validate_broker_configuration_session(item: dict, db_handle) -> tuple[bool, bool, str]:
    """Returns (is_logged_in, session_expired, message)."""
    access_token = str(item.get("access_token") or "").strip()
    broker_doc_id = str(item.get("_id") or "").strip()
    broker_name = str(item.get("broker_name") or item.get("name") or "").strip().lower()
    user_id = str(item.get("user_id") or "").strip()
    print(
        "[BROKER SESSION VALIDATE]",
        {
            "broker_doc_id": broker_doc_id,
            "broker_name": broker_name,
            "user_id": user_id,
            "has_access_token": bool(access_token),
        },
        flush=True,
    )
    if not access_token:
        print(
            "[BROKER SESSION VALIDATE] missing access_token",
            {"broker_doc_id": broker_doc_id, "broker_name": broker_name},
            flush=True,
        )
        return False, False, ""

    try:
        if "zerodha" in broker_name or "kite" in broker_name:
            from kiteconnect import KiteConnect  # type: ignore

            api_key = str(item.get("api_key") or "").strip()
            if not api_key:
                print(
                    "[BROKER SESSION VALIDATE] missing api_key",
                    {"broker_doc_id": broker_doc_id, "broker_name": broker_name},
                    flush=True,
                )
                raise ValueError("Kite api_key missing in broker configuration")
            print(
                "[BROKER SESSION VALIDATE] calling kite.profile()",
                {
                    "broker_doc_id": broker_doc_id,
                    "broker_name": broker_name,
                    "user_id": user_id,
                    "api_key": api_key,
                },
                flush=True,
            )
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            profile_response = kite.profile()
            print(
                "[KITE PROFILE DEBUG]",
                {
                    "broker_doc_id": broker_doc_id,
                    "broker_name": broker_name,
                    "user_id": user_id,
                    "profile_response": profile_response,
                },
                flush=True,
            )
            return True, False, ""

        if "flattrade" in broker_name:
            from features.flattrade_broker import validate_session as validate_flattrade_session

            ok, message = validate_flattrade_session(user_id=user_id, access_token=access_token)
            if ok:
                return True, False, ""
            raise ValueError(message or "FlatTrade session invalid")

        return True, False, ""
    except Exception as exc:
        print(
            "[BROKER SESSION VALIDATE ERROR]",
            {
                "broker_doc_id": broker_doc_id,
                "broker_name": broker_name,
                "user_id": user_id,
                "error": str(exc),
            },
            flush=True,
        )
        return False, True, str(exc)


def _login_url_for(broker_name: str, broker_doc_id: str) -> str:
    name = str(broker_name or "").lower()
    if "flattrade" in name:
        return f"/broker/flattrade/login?broker_doc_id={broker_doc_id}"
    if "zerodha" in name or "kite" in name:
        return f"/broker/kite/login?broker_doc_id={broker_doc_id}"
    return ""


def get_broker_accounts_for_user(db, app_user_id: str, broker_type: str | None = None) -> list[dict]:
    """
    Every broker_configuration doc owned by app_user_id, each with a live
    session-validity check (reuses validate_broker_configuration_session —
    the same check /broker-configurations already runs per row) and a
    ready-to-use login_url for whichever ones aren't currently logged in.
    """
    db_handle = db._db if hasattr(db, "_db") else db
    query: dict = {"app_user_id": str(app_user_id or "").strip()}
    if broker_type:
        query["broker_type"] = broker_type

    accounts: list[dict] = []
    for doc in db_handle["broker_configuration"].find(query):
        broker_doc_id = str(doc.get("_id") or "")
        broker_name = str(doc.get("broker_name") or doc.get("name") or "").strip()
        is_logged_in, _expired, message = validate_broker_configuration_session(doc, db_handle)
        accounts.append({
            "_id": broker_doc_id,
            "broker_name": broker_name,
            "account_id": str(doc.get("user_id") or "").strip(),
            "access_token": str(doc.get("access_token") or "").strip(),
            "is_logged_in": is_logged_in,
            "login_url": "" if is_logged_in else _login_url_for(broker_name, broker_doc_id),
            "message": message,
        })
    return accounts


def get_market_broker_accounts_for_user(db, app_user_id: str, broker: str | None = None) -> list[dict]:
    """
    Same idea as get_broker_accounts_for_user(), but over `kite_market_config`
    (Dhan/Kite market-data credentials) instead of `broker_configuration`
    (FlatTrade/Zerodha trade-execution credentials) — they're separate
    collections today, each scoped by the same app_user_id.
    """
    db_handle = db._db if hasattr(db, "_db") else db
    query: dict = {"app_user_id": str(app_user_id or "").strip()}
    if broker:
        query["broker"] = broker

    accounts: list[dict] = []
    for doc in db_handle["kite_market_config"].find(query):
        broker_doc_id = str(doc.get("_id") or "")
        broker_name = str(doc.get("broker") or "").strip()
        access_token = str(doc.get("access_token") or "").strip()

        is_logged_in = False
        if access_token:
            if broker_name == "dhan":
                from features.dhan_broker_ws import validate_access_token as validate_dhan_token
                is_logged_in = validate_dhan_token(access_token)
            elif broker_name == "kite":
                from features.kite_broker_ws import validate_access_token as validate_kite_token
                is_logged_in = validate_kite_token(access_token)
            else:
                is_logged_in = True

        login_url = ""
        if not is_logged_in:
            login_url = {"dhan": "/broker/dhan/login", "kite": "/broker/kite/login"}.get(broker_name, "")

        accounts.append({
            "_id": broker_doc_id,
            "broker_name": broker_name,
            "account_id": str(doc.get("user_id") or "").strip(),
            "access_token": access_token,
            "is_logged_in": is_logged_in,
            "login_url": login_url,
            "message": "",
        })
    return accounts
