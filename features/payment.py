"""
payment.py
──────────
Razorpay payment gateway + user subscription management.

Endpoints (registered in api.py via payment_router):
  GET  /auth/subscriptions   → list current user's subscriptions
  POST /payment/create-order → create a Razorpay order
  POST /payment/verify       → verify signature, activate subscription

MongoDB collection: user_subscriptions
  { user_id, feature, plan_id, plan_name, status, validity_days,
    created_at, expires_at, razorpay_order_id, razorpay_payment_id }
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from features.mongo_data import MongoData
from features import auth as app_auth
from features.telegram_notifier import notify_user_for

# No prefix baked in here — each service's entrypoint mounts this same router
# under its own path (e.g. /algo on algo.trade, /simulator on algo.simulator)
# via app.include_router(payment_router, prefix=...), so every service calls
# the exact same handler code under a different URL.
payment_router = APIRouter()

_KEY_ID     = os.getenv("RAZORPAY_KEY_ID", "")
_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")

SUBSCRIPTIONS_COL  = "user_subscriptions"
FEATURE_PLANS_COL  = "feature_subscription_plans"
PAYMENT_ORDERS_COL = "payment_orders"

# Features whose plans are catalogued in FEATURE_PLANS_COL and therefore get
# server-side price/plan validation in create_order/verify_payment below.
# "simulator" deliberately excluded — it already has its own richer,
# purpose-built catalog (sim_subscription_plans, see algo.simulator/api.py)
# and its Plans.tsx tab is legacy/superseded; it keeps today's trust-the-
# client behavior unchanged rather than being folded into this generic one.
_CATALOGED_FEATURES = ("trade", "scanner", "signal", "all")


# ── Pydantic models ───────────────────────────────────────────────────────────

class CreateOrderIn(BaseModel):
    plan_id: str
    plan_name: str
    feature: str
    amount_inr: int        # total including GST
    validity_days: int


class VerifyPaymentIn(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str
    plan_id: str
    plan_name: str
    feature: str
    validity_days: int


class AdminGrantIn(BaseModel):
    user_id: str
    feature: str
    plan_id: str
    plan_name: str
    validity_days: int


class AdminFeaturePlanIn(BaseModel):
    plan_id: Optional[str] = None  # omitted/blank on create -> derived from feature+label
    feature: str                    # "trade" | "scanner" | "signal" | "all"
    tier: str = "starter"           # "starter" | "advanced" | "pro"
    label: str
    validity_days: int = 30
    price_inr: int = 0
    save_percent: Optional[int] = None
    effective_per_month: Optional[int] = None
    includes: list[str] = []
    excludes: list[str] = []
    sort_order: int = 0
    is_active: bool = True


# ── Feature plan catalogue (Trade / Scanner / Signal / All-Access) ─────────────
# Mirrors the frontend's hardcoded TRADE_PLANS/SCANNER_PLANS/SIGNAL_PLANS/
# ALL_PLANS (previously duplicated — and drifted — between Plans.tsx and
# Admin/UserView.tsx). Seeded from Plans.tsx's values (the public-facing
# prices), which both files now read from instead of hardcoding their own.
_FEATURE_PLAN_DEFAULTS: list[dict[str, Any]] = [
    # ── Trade ──
    {"plan_id": "trade-starter", "feature": "trade", "tier": "starter", "label": "Starter",
     "validity_days": 30, "price_inr": 499, "save_percent": None, "effective_per_month": None,
     "includes": ["Forward Test & Live Trade", "Live Option Chain", "MTM & Group MTM Graph",
                  "Broker Login integration", "Access unlimited times within validity"],
     "excludes": ["Algo Backtest runs"], "sort_order": 1, "is_active": True},
    {"plan_id": "trade-advanced", "feature": "trade", "tier": "advanced", "label": "Advanced",
     "validity_days": 180, "price_inr": 2399, "save_percent": 20, "effective_per_month": 400,
     "includes": ["Forward Test & Live Trade", "Live Option Chain", "MTM & Group MTM Graph",
                  "Broker Login integration", "Access unlimited times within validity"],
     "excludes": ["Algo Backtest runs"], "sort_order": 2, "is_active": True},
    {"plan_id": "trade-pro", "feature": "trade", "tier": "pro", "label": "Pro",
     "validity_days": 360, "price_inr": 3999, "save_percent": 33, "effective_per_month": 333,
     "includes": ["Forward Test & Live Trade", "Live Option Chain", "MTM & Group MTM Graph",
                  "Broker Login integration", "Access unlimited times within validity",
                  "Algo Backtest runs included"],
     "excludes": [], "sort_order": 3, "is_active": True},
    # ── Scanner ──
    {"plan_id": "scanner-starter", "feature": "scanner", "tier": "starter", "label": "Starter",
     "validity_days": 30, "price_inr": 349, "save_percent": None, "effective_per_month": None,
     "includes": ["EOD Score Scanner", "Scanner Portfolio", "Multi-Strategy analysis",
                  "Access unlimited times within validity"],
     "excludes": ["Scanner Backtest runs", "Combined Backtest"], "sort_order": 1, "is_active": True},
    {"plan_id": "scanner-advanced", "feature": "scanner", "tier": "advanced", "label": "Advanced",
     "validity_days": 180, "price_inr": 1599, "save_percent": 24, "effective_per_month": 267,
     "includes": ["EOD Score Scanner", "Scanner Portfolio", "Multi-Strategy analysis",
                  "Scanner Backtest runs", "Access unlimited times within validity"],
     "excludes": ["Combined Backtest"], "sort_order": 2, "is_active": True},
    {"plan_id": "scanner-pro", "feature": "scanner", "tier": "pro", "label": "Pro",
     "validity_days": 360, "price_inr": 2599, "save_percent": 38, "effective_per_month": 217,
     "includes": ["EOD Score Scanner", "Scanner Portfolio", "Multi-Strategy analysis",
                  "Scanner Backtest runs", "Combined Backtest", "Access unlimited times within validity"],
     "excludes": [], "sort_order": 3, "is_active": True},
    # ── Signal ──
    {"plan_id": "signal-starter", "feature": "signal", "tier": "starter", "label": "Starter",
     "validity_days": 30, "price_inr": 299, "save_percent": None, "effective_per_month": None,
     "includes": ["Quantman Signals", "Signal Strategy Builder", "Real-time signal alerts",
                  "Access unlimited times within validity"],
     "excludes": [], "sort_order": 1, "is_active": True},
    {"plan_id": "signal-advanced", "feature": "signal", "tier": "advanced", "label": "Advanced",
     "validity_days": 180, "price_inr": 1299, "save_percent": 28, "effective_per_month": 217,
     "includes": ["Quantman Signals", "Signal Strategy Builder", "Real-time signal alerts",
                  "Access unlimited times within validity"],
     "excludes": [], "sort_order": 2, "is_active": True},
    {"plan_id": "signal-pro", "feature": "signal", "tier": "pro", "label": "Pro",
     "validity_days": 360, "price_inr": 1999, "save_percent": 44, "effective_per_month": 167,
     "includes": ["Quantman Signals", "Signal Strategy Builder", "Real-time signal alerts",
                  "Priority signal delivery", "Access unlimited times within validity"],
     "excludes": [], "sort_order": 3, "is_active": True},
    # ── All-Access ──
    {"plan_id": "all-starter", "feature": "all", "tier": "starter", "label": "Starter",
     "validity_days": 30, "price_inr": 999, "save_percent": None, "effective_per_month": None,
     "includes": ["Simulator + Trade + Scanner + Signal", "All features unlocked",
                  "Access unlimited times within validity", "Data available since 1st Jan '21"],
     "excludes": ["Backtest runs (buy individual add-on)"], "sort_order": 1, "is_active": True},
    {"plan_id": "all-advanced", "feature": "all", "tier": "advanced", "label": "Advanced",
     "validity_days": 180, "price_inr": 4499, "save_percent": 25, "effective_per_month": 750,
     "includes": ["Simulator + Trade + Scanner + Signal", "All features unlocked",
                  "Access unlimited times within validity", "Data available since 1st Jan '21"],
     "excludes": ["Backtest runs (buy individual add-on)"], "sort_order": 2, "is_active": True},
    {"plan_id": "all-pro", "feature": "all", "tier": "pro", "label": "Pro",
     "validity_days": 360, "price_inr": 7999, "save_percent": 33, "effective_per_month": 667,
     "includes": ["Simulator + Trade + Scanner + Signal", "All features unlocked",
                  "All Backtest runs included", "Access unlimited times within validity",
                  "Data available since 1st Jan '21", "Priority support"],
     "excludes": [], "sort_order": 3, "is_active": True},
]


def _seed_feature_plans_if_empty() -> None:
    col = MongoData()._db[FEATURE_PLANS_COL]
    if col.count_documents({}) == 0:
        col.insert_many([dict(p) for p in _FEATURE_PLAN_DEFAULTS])
        return
    # Backfill defaults added after this collection was first seeded — inserts
    # whole new plan_ids and fills keys missing on existing docs, but never
    # overwrites an admin's edits to a plan that already exists.
    for default in _FEATURE_PLAN_DEFAULTS:
        existing = col.find_one({"plan_id": default["plan_id"]}, {"_id": 0})
        if existing is None:
            col.insert_one(dict(default))
            continue
        missing = {k: v for k, v in default.items() if k not in existing}
        if missing:
            col.update_one({"plan_id": default["plan_id"]}, {"$set": missing})


def _resolve_authoritative_order(
    feature: str, plan_id: str, client_amount_inr: int, client_validity_days: int
) -> tuple[str, int, int]:
    """
    For catalogued features (trade/scanner/signal/all), looks up the real plan
    from feature_subscription_plans and returns (plan_name, amount_inr,
    validity_days) computed server-side — the client-supplied amount_inr AND
    validity_days are both ignored here to close a tampering gap (a modified
    request body could previously set any price *or* claim a much longer
    validity for the price of a short one). Raises 404 if plan_id isn't a real
    plan for that feature. Non-catalogued features (e.g. "simulator") pass the
    client's values through unchanged — see _CATALOGED_FEATURES.
    """
    if feature not in _CATALOGED_FEATURES:
        return "", client_amount_inr, client_validity_days
    _seed_feature_plans_if_empty()
    plan = MongoData()._db[FEATURE_PLANS_COL].find_one({"plan_id": plan_id, "feature": feature})
    if not plan:
        raise HTTPException(status_code=404, detail=f"Unknown plan '{plan_id}' for feature '{feature}'")
    gst = round(plan["price_inr"] * 0.18)
    return plan["label"], plan["price_inr"] + gst, plan["validity_days"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _require_keys() -> None:
    if not _KEY_ID or not _KEY_SECRET:
        raise HTTPException(status_code=503, detail="Payment gateway not configured")


def _verify_signature(order_id: str, payment_id: str, signature: str) -> bool:
    msg      = f"{order_id}|{payment_id}".encode()
    expected = hmac.new(_KEY_SECRET.encode(), msg, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


# ── Routes ────────────────────────────────────────────────────────────────────

@payment_router.get("/auth/subscriptions")
def get_subscriptions(current_user: dict = Depends(app_auth.require_current_user)):
    user_id = str(current_user["_id"])
    db      = MongoData()._db
    now     = datetime.now(timezone.utc).isoformat()

    docs   = list(db[SUBSCRIPTIONS_COL].find({"user_id": user_id}))
    result = []
    for d in docs:
        status     = d.get("status", "active")
        expires_at = d.get("expires_at")
        if status == "active" and expires_at and expires_at < now:
            status = "expired"
            db[SUBSCRIPTIONS_COL].update_one({"_id": d["_id"]}, {"$set": {"status": "expired"}})
        result.append({
            "id":         str(d["_id"]),
            "feature":    d.get("feature"),
            "plan_id":    d.get("plan_id"),
            "plan_name":  d.get("plan_name"),
            "status":     status,
            "expires_at": expires_at,
            "created_at": d.get("created_at"),
        })
    return result


@payment_router.post("/payment/create-order")
def create_order(payload: CreateOrderIn, current_user: dict = Depends(app_auth.require_current_user)):
    _require_keys()
    # For trade/scanner/signal/all, the price is looked up server-side from
    # feature_subscription_plans — payload.amount_inr/plan_name are NOT
    # trusted for these (see _resolve_authoritative_order). "simulator" is
    # exempted and keeps trusting the client, unchanged.
    plan_name, amount_inr, validity_days = _resolve_authoritative_order(
        payload.feature, payload.plan_id, payload.amount_inr, payload.validity_days
    )
    if not plan_name:
        plan_name = payload.plan_name
    amount_paise = amount_inr * 100
    receipt      = f"{payload.feature}-{payload.plan_id}-{str(current_user['_id'])[:8]}"

    resp = requests.post(
        "https://api.razorpay.com/v1/orders",
        auth=(_KEY_ID, _KEY_SECRET),
        json={"amount": amount_paise, "currency": "INR", "receipt": receipt},
        timeout=10,
    )
    if not resp.ok:
        raise HTTPException(status_code=502, detail="Failed to create payment order")

    order = resp.json()

    # Audit-trail record — verify_payment reads this back for catalogued
    # features instead of trusting the verify call's own payload, closing the
    # gap where a client could pay for a cheap plan but claim a pricier one
    # at verify time (the signature only proves payment authenticity, not
    # which plan was actually paid for).
    MongoData()._db[PAYMENT_ORDERS_COL].insert_one({
        "order_id":       order["id"],
        "user_id":        str(current_user["_id"]),
        "feature":        payload.feature,
        "plan_id":        payload.plan_id,
        "plan_name":      plan_name,
        "validity_days":  validity_days,
        "amount_inr":     amount_inr,
        "created_at":     datetime.now(timezone.utc).isoformat(),
    })

    return {
        "order_id": order["id"],
        "amount":   order["amount"],
        "currency": order["currency"],
        "key_id":   _KEY_ID,
    }


@payment_router.post("/payment/verify")
def verify_payment(payload: VerifyPaymentIn, current_user: dict = Depends(app_auth.require_current_user)):
    _require_keys()
    if not _verify_signature(payload.razorpay_order_id, payload.razorpay_payment_id, payload.razorpay_signature):
        raise HTTPException(status_code=400, detail="Payment verification failed")

    user_id    = str(current_user["_id"])
    db         = MongoData()._db

    # Catalogued features: trust the order record created at create-order
    # time (looked up by order_id+user_id), not this call's own payload —
    # otherwise a client could create an order for a cheap plan, pay for it
    # legitimately, then call verify claiming a different plan_id/
    # validity_days to get upgraded for the cheap price.
    plan_id, plan_name, validity_days = payload.plan_id, payload.plan_name, payload.validity_days
    if payload.feature in _CATALOGED_FEATURES:
        order_doc = db[PAYMENT_ORDERS_COL].find_one({"order_id": payload.razorpay_order_id, "user_id": user_id})
        if not order_doc:
            raise HTTPException(status_code=400, detail="No matching order found for this payment")
        plan_id, plan_name, validity_days = order_doc["plan_id"], order_doc["plan_name"], order_doc["validity_days"]

    now        = datetime.now(timezone.utc)
    expires_at = (now + timedelta(days=validity_days)).isoformat()

    db[SUBSCRIPTIONS_COL].update_many(
        {"user_id": user_id, "feature": payload.feature, "status": "active"},
        {"$set": {"status": "cancelled"}},
    )
    db[SUBSCRIPTIONS_COL].insert_one({
        "user_id":             user_id,
        "feature":             payload.feature,
        "plan_id":             plan_id,
        "plan_name":           plan_name,
        "status":              "active",
        "validity_days":       validity_days,
        "created_at":          now.isoformat(),
        "expires_at":          expires_at,
        "razorpay_order_id":   payload.razorpay_order_id,
        "razorpay_payment_id": payload.razorpay_payment_id,
    })

    _send_purchase_notification(current_user, payload, plan_name, validity_days, expires_at)

    return {"success": True, "expires_at": expires_at}


_FEATURE_LABELS: dict[str, str] = {
    "simulator": "Simulator",
    "trade":     "Algo Trade",
    "scanner":   "Scanner",
    "signal":    "Signal",
    "all":       "All-Access",
}


@payment_router.get("/admin/subscriptions")
def admin_list_subscriptions(plan_id: Optional[str] = None):
    """
    Admin: every user_subscriptions row (Trade/Scanner/Signal/All-Access —
    Simulator has its own equivalent at /simulator/admin/subscriptions, see
    algo.simulator/api.py), joined with the subscriber's name/email. This is
    the "which user has which plan" view for the generic feature-plan
    catalog, same shape as the Simulator one so ActiveSubscriptions.tsx can
    merge both. Optional ?plan_id= filters to one plan's subscribers (used by
    the "N active users" badge on the Feature Plans list).
    """
    db = MongoData()._db
    query: dict[str, Any] = {"plan_id": plan_id} if plan_id else {}
    docs = list(db[SUBSCRIPTIONS_COL].find(query).sort("expires_at", -1))

    from bson import ObjectId as _ObjectId
    users_col = db[app_auth.USERS_COLLECTION]

    rows = []
    for doc in docs:
        user_id_raw = doc.get("user_id")
        user_doc = None
        try:
            user_doc = users_col.find_one({"_id": _ObjectId(str(user_id_raw))}, {"name": 1, "email": 1, "mobile": 1})
        except Exception:
            pass
        rows.append({
            "user_id":      str(user_id_raw),
            "user_name":    (user_doc or {}).get("name"),
            "user_email":   (user_doc or {}).get("email"),
            "user_mobile":  (user_doc or {}).get("mobile"),
            "plan_id":      doc.get("plan_id"),
            "plan_name":    doc.get("plan_name"),
            "status":       doc.get("status"),
            "billing":      None,
            "starts_at":    doc.get("created_at"),
            "expires_at":   doc.get("expires_at"),
            "reference_by": doc.get("reference_by"),
        })
    return rows


@payment_router.get("/admin/user-subscriptions/{user_id}")
def admin_get_user_subscriptions(user_id: str):
    """Admin: list all subscriptions for a given user_id. No auth required — keep off public deployments."""
    from bson import ObjectId
    db  = MongoData()._db
    now = datetime.now(timezone.utc).isoformat()
    try:
        ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user_id")

    docs   = list(db[SUBSCRIPTIONS_COL].find({"user_id": user_id}))
    result = []
    for d in docs:
        status     = d.get("status", "active")
        expires_at = d.get("expires_at")
        if status == "active" and expires_at and expires_at < now:
            status = "expired"
            db[SUBSCRIPTIONS_COL].update_one({"_id": d["_id"]}, {"$set": {"status": "expired"}})
        result.append({
            "id":           str(d["_id"]),
            "feature":      d.get("feature"),
            "plan_id":      d.get("plan_id"),
            "plan_name":    d.get("plan_name"),
            "status":       status,
            "validity_days": d.get("validity_days"),
            "expires_at":   expires_at,
            "created_at":   d.get("created_at"),
            "reference_by": d.get("reference_by"),
        })
    return result


@payment_router.post("/admin/grant-subscription")
def admin_grant_subscription(payload: AdminGrantIn):
    """Admin: instantly activate a subscription for any user — no payment required.
    Marks reference_by='admin' so it can be distinguished from paid subscriptions.
    No auth required — keep off public deployments."""
    from bson import ObjectId
    db = MongoData()._db
    try:
        oid = ObjectId(payload.user_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user_id")

    user = db[app_auth.USERS_COLLECTION].find_one({"_id": oid})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    now        = datetime.now(timezone.utc)
    expires_at = (now + timedelta(days=payload.validity_days)).isoformat()

    db[SUBSCRIPTIONS_COL].update_many(
        {"user_id": payload.user_id, "feature": payload.feature, "status": "active"},
        {"$set": {"status": "cancelled"}},
    )
    result = db[SUBSCRIPTIONS_COL].insert_one({
        "user_id":       payload.user_id,
        "feature":       payload.feature,
        "plan_id":       payload.plan_id,
        "plan_name":     payload.plan_name,
        "status":        "active",
        "validity_days": payload.validity_days,
        "created_at":    now.isoformat(),
        "expires_at":    expires_at,
        "reference_by":  "admin",
    })
    return {"success": True, "id": str(result.inserted_id), "expires_at": expires_at}


@payment_router.delete("/admin/subscription/{subscription_id}")
def admin_cancel_subscription(subscription_id: str):
    """Admin: cancel/remove a subscription by ID. No auth required — keep off public deployments."""
    from bson import ObjectId
    db = MongoData()._db
    try:
        oid = ObjectId(subscription_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid subscription_id")
    result = db[SUBSCRIPTIONS_COL].update_one({"_id": oid}, {"$set": {"status": "cancelled"}})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return {"success": True}


@payment_router.patch("/admin/user/{user_id}/status")
def admin_toggle_user_status(user_id: str, body: dict):
    """Admin: set a user's is_active flag. Body: {is_active: bool}. No auth — keep off public deployments."""
    from bson import ObjectId
    db = MongoData()._db
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user_id")
    is_active = bool(body.get("is_active", True))
    result = db[app_auth.USERS_COLLECTION].update_one({"_id": oid}, {"$set": {"is_active": is_active}})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    return {"success": True, "is_active": is_active}


# ── Feature plan catalogue CRUD (Trade / Scanner / Signal / All-Access) ────────
# Same shape as algo.simulator's /simulator/admin/subscription-plans CRUD —
# public read-only list here, admin-only create/update/delete, no auth (same
# convention as every other admin endpoint in this router).

@payment_router.get("/feature-plans")
def list_feature_plans(feature: Optional[str] = None):
    """Public — active plans only. Powers Plans.tsx and the Add Plan modal."""
    _seed_feature_plans_if_empty()
    query: dict[str, Any] = {"is_active": True}
    if feature:
        query["feature"] = feature
    col = MongoData()._db[FEATURE_PLANS_COL]
    return list(col.find(query, {"_id": 0}).sort("sort_order", 1))


@payment_router.get("/admin/feature-plans")
def admin_list_feature_plans():
    """Admin: full catalogue including inactive plans, plus a live
    active_subscribers count from user_subscriptions per plan_id."""
    _seed_feature_plans_if_empty()
    db = MongoData()._db
    plans = list(db[FEATURE_PLANS_COL].find({}, {"_id": 0}).sort([("feature", 1), ("sort_order", 1)]))

    counts_by_plan: dict[str, int] = {}
    for row in db[SUBSCRIPTIONS_COL].aggregate([
        {"$match": {"status": "active"}},
        {"$group": {"_id": "$plan_id", "count": {"$sum": 1}}},
    ]):
        counts_by_plan[row["_id"]] = row["count"]

    for plan in plans:
        plan["active_subscribers"] = counts_by_plan.get(plan["plan_id"], 0)
    return plans


@payment_router.post("/admin/feature-plans")
def admin_create_feature_plan(body: AdminFeaturePlanIn):
    col = MongoData()._db[FEATURE_PLANS_COL]
    raw_id  = (body.plan_id or f"{body.feature}-{body.label}").strip().lower()
    plan_id = re.sub(r"[^a-z0-9]+", "-", raw_id).strip("-")
    if not plan_id:
        raise HTTPException(status_code=400, detail="plan_id or (feature + label) is required")
    if col.find_one({"plan_id": plan_id}):
        raise HTTPException(status_code=409, detail=f"Plan '{plan_id}' already exists")
    doc = body.dict()
    doc["plan_id"] = plan_id
    col.insert_one(doc)
    return {"ok": True, "plan_id": plan_id}


@payment_router.put("/admin/feature-plans/{plan_id}")
def admin_update_feature_plan(plan_id: str, body: AdminFeaturePlanIn):
    col = MongoData()._db[FEATURE_PLANS_COL]
    doc = body.dict()
    doc["plan_id"] = plan_id
    result = col.update_one({"plan_id": plan_id}, {"$set": doc}, upsert=False)
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail=f"Plan '{plan_id}' not found")
    return {"ok": True}


@payment_router.delete("/admin/feature-plans/{plan_id}")
def admin_delete_feature_plan(plan_id: str):
    db = MongoData()._db
    active_count = db[SUBSCRIPTIONS_COL].count_documents({"plan_id": plan_id, "status": "active"})
    if active_count > 0:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete '{plan_id}' — {active_count} user(s) are currently active on this plan. Move them to another plan first.",
        )
    result = db[FEATURE_PLANS_COL].delete_one({"plan_id": plan_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail=f"Plan '{plan_id}' not found")
    return {"ok": True}


# ── AlgoTrade Subscription (dedicated collections) ────────────────────────────
# Own catalog + own user-subscription collections — same architectural
# pattern as algo.simulator's sim_subscription_plans/sim_user_subscriptions
# (dedicated catalog, dedicated per-user rows, admin CRUD, grant endpoint),
# but NOT the same literal collections — those are Simulator-specific with a
# completely different document shape (advanced_slots, TV alert limits,
# etc.); reusing them would collide with Simulator's own data.
#
# Each of the 3 tiers (live_trade/fast_forward/forward_test) is its own,
# independent credit pool — unlike Simulator's single-active-plan-per-user
# model, a user can hold an active Live Trade AND an active Forward Test
# subscription at the same time. So "cancel the previous active one" below
# is scoped by (user_id, tier), never by the whole user.

ALGOTRADE_PLANS_COL = "algotrade_subscription_plans"
ALGOTRADE_SUBS_COL  = "algotrade_user_subscriptions"
ALGOTRADE_WALLET_COL = "algotrade_wallet"
ALGOTRADE_WALLET_PACKAGES_COL = "algotrade_wallet_packages"

# "Add credits to your Wallet" popup — one universal, non-expiring credit
# balance per user, spent on all 3 AlgoTrade tiers' access and on
# deploy-limit purchases (replaces the old per-tier expiring credit model).
# base_credits is the pre-bonus amount shown struck through; credits is what
# actually lands in the wallet (== base_credits for Starter, which has no bonus).
_ALGOTRADE_WALLET_PACKAGE_DEFAULTS: list[dict[str, Any]] = [
    {"package_id": "starter", "label": "Starter", "price_inr": 499,
     "base_credits": 500, "credits": 500, "most_popular": False, "best_value": False,
     "sort_order": 1, "is_active": True},
    {"package_id": "explorer", "label": "Explorer", "price_inr": 999,
     "base_credits": 1000, "credits": 1100, "most_popular": True, "best_value": False,
     "sort_order": 2, "is_active": True},
    {"package_id": "pro", "label": "Pro", "price_inr": 2499,
     "base_credits": 2500, "credits": 3000, "most_popular": False, "best_value": False,
     "sort_order": 3, "is_active": True},
    {"package_id": "advanced", "label": "Advanced", "price_inr": 5999,
     "base_credits": 6000, "credits": 7500, "most_popular": False, "best_value": False,
     "sort_order": 4, "is_active": True},
    {"package_id": "ultimate", "label": "Ultimate", "price_inr": 14999,
     "base_credits": 15000, "credits": 20000, "most_popular": False, "best_value": True,
     "sort_order": 5, "is_active": True},
]

# Credits spent per +1 strategy slot, buy-limit only supports these 2 tiers
# (matches the "Algo Trading Plan" popup, which has no Forward-Test-tier card
# distinct from the fast_forward one actually used by the /forward-test page).
ALGOTRADE_BUY_LIMIT_RATES: dict[str, int] = {"live_trade": 100, "fast_forward": 50, "forward_test": 50}


_ALGOTRADE_PLAN_DEFAULTS: list[dict[str, Any]] = [
    {"plan_id": "algotrade-live-trade", "tier": "live_trade", "label": "Live Trade",
     "validity_days": 30, "price_inr": 70, "save_percent": None, "effective_per_month": None,
     "includes": ["1 credit = 1 strategy activation", "Real-time tick-by-tick data updates",
                  "SL / Target / legs monitored on every tick", "Real broker execution",
                  "Best for precision, low-latency execution"],
     "excludes": [], "sort_order": 1, "is_active": True},
    {"plan_id": "algotrade-fast-forward", "tier": "fast_forward", "label": "Fast-Forward",
     "validity_days": 30, "price_inr": 65, "save_percent": None, "effective_per_month": None,
     "includes": ["1 credit = 1 strategy activation", "Tick-by-tick simulated data updates",
                  "SL / Target / legs monitored on every tick"],
     "excludes": [], "sort_order": 2, "is_active": True},
    {"plan_id": "algotrade-forward-test", "tier": "forward_test", "label": "Forward Test",
     "validity_days": 30, "price_inr": 50, "save_percent": None, "effective_per_month": None,
     "includes": ["Checked once every 30 seconds", "SL / Target / lazy legs evaluated on the latest LTP each cycle"],
     "excludes": [], "sort_order": 3, "is_active": True},
]


class AlgoTradePlanUpdateIn(BaseModel):
    price_inr: int
    validity_days: int
    save_percent: Optional[int] = None
    effective_per_month: Optional[int] = None
    includes: list[str] = []
    excludes: list[str] = []
    sort_order: int = 0
    is_active: bool = True


class AlgoTradeBuyLimitIn(BaseModel):
    live_trade_qty: int = 0
    fast_forward_qty: int = 0
    forward_test_qty: int = 0


def _seed_algotrade_plans_if_empty() -> None:
    col = MongoData()._db[ALGOTRADE_PLANS_COL]
    if col.count_documents({}) > 0:
        return
    # One-time migration: these 3 rows used to live in the generic
    # feature_subscription_plans collection (feature="algotrade") before this
    # dedicated collection existed. Carry over whatever the admin already
    # edited there instead of overwriting with fresh defaults, then retire
    # those rows from the shared collection so it's not a second source of
    # truth for them.
    old_col = MongoData()._db[FEATURE_PLANS_COL]
    migrated = list(old_col.find({"feature": "algotrade"}, {"_id": 0, "feature": 0}))
    if migrated:
        col.insert_many(migrated)
        old_col.delete_many({"feature": "algotrade"})
        return
    col.insert_many([dict(p) for p in _ALGOTRADE_PLAN_DEFAULTS])


@payment_router.get("/algotrade/subscription/plans")
def algotrade_subscription_plans():
    """Public — no auth. Active plans only."""
    _seed_algotrade_plans_if_empty()
    col = MongoData()._db[ALGOTRADE_PLANS_COL]
    return list(col.find({"is_active": True}, {"_id": 0}).sort("sort_order", 1))


@payment_router.get("/algotrade/admin/subscription-plans")
def admin_list_algotrade_plans():
    """Admin: full catalogue (including inactive), plus a live
    active_subscribers count per plan_id from algotrade_user_subscriptions."""
    _seed_algotrade_plans_if_empty()
    db = MongoData()._db
    plans = list(db[ALGOTRADE_PLANS_COL].find({}, {"_id": 0}).sort("sort_order", 1))

    counts_by_plan: dict[str, int] = {}
    for row in db[ALGOTRADE_SUBS_COL].aggregate([
        {"$match": {"status": "active"}},
        {"$group": {"_id": "$plan_id", "count": {"$sum": 1}}},
    ]):
        counts_by_plan[row["_id"]] = row["count"]

    for plan in plans:
        plan["active_subscribers"] = counts_by_plan.get(plan["plan_id"], 0)
    return plans


@payment_router.put("/algotrade/admin/subscription-plans/{plan_id}")
def admin_update_algotrade_plan(plan_id: str, body: AlgoTradePlanUpdateIn):
    """Admin: edit price/validity/includes/excludes/status for one of the 3
    fixed AlgoTrade tiers. tier and label are each row's permanent identity
    and aren't editable here — there's no create/delete for this catalog."""
    _seed_algotrade_plans_if_empty()
    col = MongoData()._db[ALGOTRADE_PLANS_COL]
    result = col.update_one({"plan_id": plan_id}, {"$set": body.dict()})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail=f"Plan '{plan_id}' not found")
    return {"ok": True}


def _algotrade_wallet_balance(db, user_id: str) -> int:
    """The universal AlgoTrade wallet balance — one non-expiring number per
    user, spent on all 3 tiers' access and on deploy-limit purchases. Not
    scoped per tier (replaces the old per-tier credit-batch model).
    Returns 0 while the wallet is admin-deactivated, even though the credit
    count is still retained on the doc for history display."""
    d = db[ALGOTRADE_WALLET_COL].find_one({"user_id": user_id})
    if not d or not d.get("is_active", True):
        return 0
    return int(d["credits"])


def _algotrade_wallet_add(db, user_id: str, amount: int) -> int:
    """Granting credits also reactivates a previously-removed wallet."""
    existing = db[ALGOTRADE_WALLET_COL].find_one({"user_id": user_id})
    if existing:
        db[ALGOTRADE_WALLET_COL].update_one({"_id": existing["_id"]}, {"$inc": {"credits": amount}, "$set": {"is_active": True}})
        return int(existing["credits"]) + amount
    db[ALGOTRADE_WALLET_COL].insert_one({"user_id": user_id, "credits": amount, "is_active": True})
    return amount


def _algotrade_wallet_debit(db, user_id: str, amount: int) -> None:
    """Caller must have already checked _algotrade_wallet_balance(...) >=
    amount — this does not itself validate sufficiency."""
    db[ALGOTRADE_WALLET_COL].update_one({"user_id": user_id}, {"$inc": {"credits": -amount}})


def _seed_algotrade_wallet_packages_if_empty() -> None:
    col = MongoData()._db[ALGOTRADE_WALLET_PACKAGES_COL]
    if col.count_documents({}) > 0:
        return
    col.insert_many([dict(p) for p in _ALGOTRADE_WALLET_PACKAGE_DEFAULTS])


@payment_router.get("/algotrade/wallet/packages")
def algotrade_wallet_packages():
    """Public — no auth. Active wallet top-up packages, "Add credits to your
    Wallet" popup. Credits bought here never expire and aren't tier-scoped."""
    _seed_algotrade_wallet_packages_if_empty()
    col = MongoData()._db[ALGOTRADE_WALLET_PACKAGES_COL]
    return list(col.find({"is_active": True}, {"_id": 0}).sort("sort_order", 1))


class AlgoTradeWalletPackageUpdateIn(BaseModel):
    price_inr: int
    base_credits: int
    credits: int
    most_popular: bool = False
    best_value: bool = False
    sort_order: int = 0
    is_active: bool = True


@payment_router.get("/algotrade/admin/wallet/packages")
def admin_list_algotrade_wallet_packages():
    """Admin: full wallet package catalogue, including inactive rows."""
    _seed_algotrade_wallet_packages_if_empty()
    col = MongoData()._db[ALGOTRADE_WALLET_PACKAGES_COL]
    return list(col.find({}, {"_id": 0}).sort("sort_order", 1))


@payment_router.put("/algotrade/admin/wallet/packages/{package_id}")
def admin_update_algotrade_wallet_package(package_id: str, body: AlgoTradeWalletPackageUpdateIn):
    """Admin: edit price/credits/badges/status for one wallet top-up package.
    package_id and label are each row's permanent identity and aren't editable
    here — there's no create/delete for this catalog."""
    _seed_algotrade_wallet_packages_if_empty()
    col = MongoData()._db[ALGOTRADE_WALLET_PACKAGES_COL]
    result = col.update_one({"package_id": package_id}, {"$set": body.dict()})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail=f"Package '{package_id}' not found")
    return {"ok": True}


@payment_router.get("/algotrade/wallet/balance")
def algotrade_wallet_balance_me(current_user: dict = Depends(app_auth.require_current_user)):
    """Auth required. Current user's universal AlgoTrade wallet balance."""
    return {"balance": _algotrade_wallet_balance(MongoData()._db, str(current_user["_id"]))}


class AlgoTradeWalletGrantIn(BaseModel):
    user_id: str
    credits: int


@payment_router.get("/algotrade/admin/wallet/balance/{user_id}")
def admin_get_algotrade_wallet_balance(user_id: str):
    """Admin: this user's current AlgoTrade wallet balance, plus the raw
    credits + is_active flag so the admin UI can still show a removed
    wallet's credit count in its Expired & Cancelled history row."""
    d = MongoData()._db[ALGOTRADE_WALLET_COL].find_one({"user_id": user_id})
    credits = int(d["credits"]) if d else 0
    is_active = d.get("is_active", True) if d else True
    return {"balance": credits if is_active else 0, "credits": credits, "is_active": is_active}


@payment_router.post("/algotrade/admin/wallet/grant")
def admin_grant_algotrade_wallet(payload: AlgoTradeWalletGrantIn):
    """Admin: add credits to a user's AlgoTrade wallet — no payment required.
    Real Razorpay checkout for "Buy Credits" isn't wired yet, so this is the
    only way credits enter the wallet for now. No auth — keep off public
    deployments."""
    from bson import ObjectId
    db = MongoData()._db
    try:
        ObjectId(payload.user_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user_id")
    if payload.credits <= 0:
        raise HTTPException(status_code=400, detail="credits must be positive")
    new_balance = _algotrade_wallet_add(db, payload.user_id, payload.credits)
    return {"ok": True, "new_balance": new_balance}


@payment_router.delete("/algotrade/admin/wallet/balance/{user_id}")
def admin_deactivate_algotrade_wallet(user_id: str):
    """Admin: deactivate this user's AlgoTrade Wallet. Unlike a hard delete,
    the credit count is kept on the doc — the balance goes to 0 (unusable)
    but the admin UI still shows the row, marked Inactive, in the Expired &
    Cancelled history tab instead of it vanishing. Granting new credits
    (admin_grant_algotrade_wallet) reactivates it."""
    from bson import ObjectId
    db = MongoData()._db
    try:
        ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user_id")
    result = db[ALGOTRADE_WALLET_COL].update_one({"user_id": user_id}, {"$set": {"is_active": False}})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Wallet not found")
    return {"ok": True, "new_balance": 0}


def _algotrade_tier_balance(db, user_id: str, tier: str) -> int:
    """Sum of credits across this user's active, non-expired
    algotrade_user_subscriptions batches for one tier — how many strategy
    slots they've bought and haven't lapsed yet. Flips newly-expired rows to
    status="expired" along the way so they stop counting immediately."""
    now = datetime.now(timezone.utc).isoformat()
    total = 0
    for d in db[ALGOTRADE_SUBS_COL].find({"user_id": user_id, "tier": tier, "status": "active"}):
        if d.get("expires_at") and d["expires_at"] < now:
            db[ALGOTRADE_SUBS_COL].update_one({"_id": d["_id"]}, {"$set": {"status": "expired"}})
            continue
        total += int(d.get("credits") or 1)
    return total


@payment_router.get("/algotrade/subscription/buy-limit-rates")
def algotrade_buy_limit_rates():
    """Public — no auth. Credits-per-slot for each tier's buy-limit purchase —
    the "Algo Trading Plan" popup fetches this instead of hardcoding a copy,
    so it can never drift from what algotrade_buy_limit actually charges."""
    return ALGOTRADE_BUY_LIMIT_RATES


# Tier name (as stored in algotrade_user_subscriptions / used by the credit-balance side)
# → activation_mode (as stored on algo_trades documents / used by the deploy-limit side).
# Kept here, not in algo.trade/api.py, since this is the lower-level module the trade
# service imports from — api.py's own ACTIVATION_MODE_TO_ALGOTRADE_TIER is just this
# mapping inverted, for the one place that needs to go the other direction.
ALGOTRADE_TIER_TO_ACTIVATION_MODE = {"live_trade": "live", "fast_forward": "fast-forward", "forward_test": "forward-test"}


_IST = timezone(timedelta(hours=5, minutes=30))


def _algotrade_currently_running(db, user_id: str, activation_mode: str) -> int:
    """How many of this user's algo_trades are actually running right now for one
    activation_mode — trade_status=1 (TRADE_STATUS_RUNNING) and active_on_server=True
    together, since square-off flips both at once (see trading_core.py's
    square_off_strategy). Shared by algotrade_my_tier_balances' balance_* fields below
    and algo.trade/api.py's _algotrade_deploy_status, so the two can never drift on what
    counts as "currently deployed" for the same tier.

    Scoped to today (IST) — same creation_ts-prefix convention api.py's
    list_algo_trades/_default_runtime_trade_date already use for live/fast-forward/
    forward-test. These 3 modes are re-armed fresh every trading day, so a doc left
    active_on_server=True from a previous day (server restart, missed EOD square-off,
    etc.) must not still count against today's slot — without this, a live tier's
    balance never recovers even though nothing from a prior day is actually running."""
    query: dict = {
        "user_id": user_id,
        "activation_mode": activation_mode,
        "trade_status": 1,
        "active_on_server": True,
    }
    if activation_mode in {"live", "fast-forward", "forward-test"}:
        today = datetime.now(_IST).strftime("%Y-%m-%d")
        query["creation_ts"] = {"$regex": f"^{re.escape(today)}"}
    return db["algo_trades"].count_documents(query)


@payment_router.get("/algotrade/subscription/my-tier-balances")
def algotrade_my_tier_balances(current_user: dict = Depends(app_auth.require_current_user)):
    """Auth required. Each tier's current strategy-slot balance — bought via
    buy-limit, each purchase its own 30-day batch (see algotrade_buy_limit).
    0 means this tier has never been bought, or every batch has expired.

    Also includes balance_<tier>: how many *more* slots are actually free right now
    (total bought minus currently-running, floored at 0) — the number the Portfolio
    Activation page and the Fast-Forward/Live-Trade/Forward-Test stat bars actually
    want to show/check against, instead of each doing its own separate deploy-status
    round-trip per tier to work it out client-side."""
    db = MongoData()._db
    user_id = str(current_user["_id"])
    tiers = ("live_trade", "fast_forward", "forward_test")
    result = {tier: _algotrade_tier_balance(db, user_id, tier) for tier in tiers}
    for tier in tiers:
        running = _algotrade_currently_running(db, user_id, ALGOTRADE_TIER_TO_ACTIVATION_MODE[tier])
        result[f"balance_{tier}"] = max(0, result[tier] - running)
    return result


@payment_router.post("/algotrade/subscription/buy-limit")
def algotrade_buy_limit(payload: AlgoTradeBuyLimitIn, current_user: dict = Depends(app_auth.require_current_user)):
    """Auth required. User-initiated: spend wallet credits to buy strategy
    slots for a tier (the "Algo Trading Plan" popup) — 100 credits/slot for
    Live Trade, 50/slot for Fast-Forward. Cost is checked against the single
    universal wallet balance (either the whole purchase succeeds or none of
    it does), but what you get is tier-scoped and expiring: each purchase
    inserts its own 30-day batch into algotrade_user_subscriptions (the same
    table the old admin-granted tier credits used), so "Strategies deployed"
    naturally drops back to 0 a month after the last top-up, same as any
    other subscription batch here."""
    user_id = str(current_user["_id"])
    db = MongoData()._db

    purchases = [
        (tier, qty)
        for tier, qty in (
            ("live_trade", payload.live_trade_qty),
            ("fast_forward", payload.fast_forward_qty),
            ("forward_test", payload.forward_test_qty),
        )
        if qty > 0
    ]
    if not purchases:
        raise HTTPException(status_code=400, detail="Nothing to purchase.")

    total_cost = sum(ALGOTRADE_BUY_LIMIT_RATES[tier] * qty for tier, qty in purchases)
    balance = _algotrade_wallet_balance(db, user_id)
    if balance < total_cost:
        raise HTTPException(
            status_code=400,
            detail=f"Insufficient credits: need {total_cost}, have {balance}.",
        )

    _algotrade_wallet_debit(db, user_id, total_cost)

    _seed_algotrade_plans_if_empty()
    plans_by_tier = {p["tier"]: p for p in db[ALGOTRADE_PLANS_COL].find({})}
    now = datetime.now(timezone.utc)
    validity_days = 30
    expires_at = (now + timedelta(days=validity_days)).isoformat()
    for tier, qty in purchases:
        plan = plans_by_tier.get(tier, {})
        db[ALGOTRADE_SUBS_COL].insert_one({
            "user_id":       user_id,
            "plan_id":       plan.get("plan_id", f"algotrade-{tier.replace('_', '-')}"),
            "plan_name":     plan.get("label", tier),
            "tier":          tier,
            "status":        "active",
            "credits":       qty,
            "validity_days": validity_days,
            "created_at":    now.isoformat(),
            "expires_at":    expires_at,
            "reference_by":  "wallet_purchase",
        })

    return {
        "ok": True,
        "tier_balances": {tier: _algotrade_tier_balance(db, user_id, tier) for tier, _ in purchases},
        "wallet_balance": _algotrade_wallet_balance(db, user_id),
        "expires_at": expires_at,
    }


def _send_purchase_notification(
    user: dict, payload: VerifyPaymentIn, plan_name: str, validity_days: int, expires_at: str
) -> None:
    """plan_name/validity_days are passed separately (not read off `payload`)
    since verify_payment resolves them from the trusted order record for
    catalogued features — the notification must reflect what was actually
    paid for, not whatever the verify call's payload claimed."""
    feature_label = _FEATURE_LABELS.get(payload.feature, payload.feature.title())
    try:
        expiry_display = datetime.fromisoformat(expires_at).strftime("%d %b %Y")
    except Exception:
        expiry_display = expires_at[:10]

    message = (
        f"✅ Plan Activated!\n\n"
        f"Feature   : {feature_label}\n"
        f"Plan      : {plan_name}\n"
        f"Validity  : {validity_days} days\n"
        f"Valid Till: {expiry_display}\n\n"
        f"Payment ID: {payload.razorpay_payment_id}"
    )
    notify_user_for(user, "PLAN_PURCHASED", message, category="algo")
