"""
dhan_totp_login.py
────────────────────
Daily unattended Dhan access_token refresh via Dhan's documented TOTP login
endpoint (auth.dhan.co/app/generateAccessToken) — no OAuth consent/browser
step needed, unlike the /broker/dhan/generate-consent flow in algo.trade's
api.py. Requires TOTP to already be enabled on the Dhan account.

client_id and the TOTP secret are read from kite_market_config's own dhan
doc (user_id / totp fields — the same doc the OAuth consent flow already
populates), not from env, so there's one less place to keep in sync. Only
DHAN_PIN stays in shared/.env — Dhan's kite_market_config schema has no PIN
field, and a login PIN is more sensitive than a TOTP seed alone.

Token is valid 24h from generation, so this is meant to run once daily
(cron hitting GET /broker/dhan/totp-login before market open, or this
module run directly — see __main__ below) to keep the live feed
(dhan_ticker.py / dhan_broker_ws.py) and order APIs (dhan_broker.py)
working without a manual login every morning. Production's crontab runs
it at 07:31 IST (02:01 UTC — the box runs Etc/UTC) against algo-trade on
:8000.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

import pyotp
import requests

log = logging.getLogger(__name__)

_GENERATE_TOKEN_URL = "https://auth.dhan.co/app/generateAccessToken"
_NOTIFY_MOBILE = "7667662644"


def _notify_totp_result(db, ok: bool, message: str) -> None:
    """Telegram-notify the account owner on every run, success or failure —
    an unattended 07:31 IST cron failure otherwise goes unnoticed until
    someone happens to check. No auto-retry on failure by design; this is
    just the "go look at this" signal — next attempt is tomorrow's cron or
    a manual click on Monitor's Relogin button."""
    try:
        from features.telegram_notifier import notify_user_for  # type: ignore
        user_doc = db._db["user_details"].find_one({"mobile": _NOTIFY_MOBILE})
        event_type = "DHAN_TOTP_LOGIN" if ok else "DHAN_TOTP_LOGIN_FAILED"
        notify_user_for(user_doc or _NOTIFY_MOBILE, event_type, message)
    except Exception:
        log.exception("[dhan totp login] telegram notify failed")


def refresh_dhan_token_via_totp() -> dict:
    """Generate a fresh Dhan access_token from PIN+TOTP and persist it to
    kite_market_config (broker=dhan) — the same collection/fields the OAuth
    consent callback writes, so every existing Dhan consumer picks it up
    with no further changes."""
    pin = os.environ.get("DHAN_PIN", "").strip()

    from features.mongo_data import MongoData  # type: ignore
    from features.dhan_broker_ws import save_credentials_to_db  # type: ignore
    from features.broker_gateway import reset_broker_cache  # type: ignore

    login_time = datetime.now().isoformat()
    mirrored = 0
    db = MongoData()
    try:
        market_cfg = db._db["kite_market_config"].find_one({"broker": "dhan"}) or {}
        client_id = str(market_cfg.get("user_id") or market_cfg.get("dhan_client_id") or "").strip()
        secret = str(market_cfg.get("totp") or "").strip()
        api_key = str(market_cfg.get("api_key") or "").strip()
        api_secret = str(market_cfg.get("api_secret") or "").strip()

        if not client_id or not secret:
            message = "kite_market_config (broker=dhan) is missing user_id and/or totp"
            _notify_totp_result(db, False, message)
            return {"ok": False, "message": message}
        if not pin:
            message = "DHAN_PIN must be set in shared/.env"
            _notify_totp_result(db, False, message)
            return {"ok": False, "message": message}

        totp_code = pyotp.TOTP(secret).now()

        try:
            resp = requests.post(
                _GENERATE_TOKEN_URL,
                params={"dhanClientId": client_id, "pin": pin, "totp": totp_code},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.exception("[dhan totp login] generateAccessToken request failed")
            message = f"generateAccessToken error: {exc}"
            _notify_totp_result(db, False, message)
            return {"ok": False, "message": message}

        access_token = str(data.get("accessToken") or "").strip()
        if not access_token:
            message = f"No accessToken in response: {data}"
            _notify_totp_result(db, False, message)
            return {"ok": False, "message": message}

        expiry_time = str(data.get("expiryTime") or "").strip()
        resolved_client_id = str(data.get("dhanClientId") or client_id).strip()

        save_credentials_to_db(db, resolved_client_id, access_token)
        db._db["kite_market_config"].update_one(
            {"broker": "dhan"},
            {"$set": {"login_time": login_time, "expiry_time": expiry_time}},
        )

        # Mirror into broker_configuration (live order placement) — it keeps its
        # own independent copy of the Dhan login, separate from kite_market_config
        # (feed). Same account when registered with the same api_key/api_secret;
        # without this, order placement keeps using yesterday's expired token
        # until its own next login. Matches kite_broker.py's
        # sync_kite_access_token_by_credentials pattern for Kite.
        if api_key and api_secret:
            result = db._db["broker_configuration"].update_many(
                {"api_key": api_key, "api_secret": api_secret, "broker_type": "live"},
                {"$set": {"access_token": access_token, "login_time": login_time}},
            )
            mirrored = result.matched_count

        success_message = f"Dhan access token refreshed via TOTP. Valid until {expiry_time}."
        _notify_totp_result(db, True, success_message)
    finally:
        db.close()

    reset_broker_cache()
    log.info(
        "[dhan totp login] token refreshed client_id=%s expiry=%s mirrored_to_broker_configuration=%d",
        resolved_client_id, expiry_time, mirrored,
    )
    return {
        "ok": True,
        "message": success_message,
        "client_id": resolved_client_id,
        "expiry_time": expiry_time,
        "mirrored_to_broker_configuration": mirrored,
    }


if __name__ == "__main__":
    # Standalone entry point for cron, e.g. run from algo.trade/ so the
    # `features` package (symlinked to ../shared/features) resolves:
    #   venv/bin/python -m features.dhan_totp_login
    import pathlib
    from dotenv import load_dotenv

    load_dotenv(pathlib.Path(__file__).resolve().parent / ".env")
    logging.basicConfig(level=logging.INFO)
    result = refresh_dhan_token_via_totp()
    print(result)
    import time
    time.sleep(3)  # let the Telegram notify's daemon thread finish before the process exits
    raise SystemExit(0 if result.get("ok") else 1)
