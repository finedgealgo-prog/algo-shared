"""
sim_plans.py
────────────
Single source of truth for "which sim_subscription_plans doc applies to this
user" — resolve_user_plan() joins sim_user_subscriptions.plan_id (the plan's
_id as a string) to sim_subscription_plans._id, falls back to the "free" slug
when the user has no active subscription.

Factored out so both api.py's endpoints (strategy/advanced-slot limits,
/simulator/subscription/my-plan) and shared/features/simulator_risk_monitor.py
(the live auto-exit engine) resolve a user's plan the exact same way. Two
independent copies of this join previously drifted — the plan doc's identity
moved from a hand-picked "plan_id" slug string to the real Mongo _id, and one
copy kept matching on the old field, silently treating every paying user as
Free.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from bson import ObjectId

from features.mongo_data import MongoData

IST = timezone(timedelta(hours=5, minutes=30))

# Mirrors api.py's SIM_SUB_STATUS_* — stored as an int on sim_user_subscriptions.
SIM_SUB_STATUS_INACTIVE = 0
SIM_SUB_STATUS_ACTIVE = 1
SIM_SUB_STATUS_EXPIRED = 2
SIM_SUB_STATUS_CANCELLED = 3

_mongo = MongoData()


def _sim_user_or_filter(user_id: Any) -> dict:
    ids: list[Any] = [user_id]
    try:
        ids.append(ObjectId(user_id))
    except Exception:
        pass
    return {"user_id": {"$in": ids}}


def _looks_like_object_id(value: Any) -> bool:
    try:
        ObjectId(str(value))
        return True
    except Exception:
        return False


def _sim_find_plan(value: Optional[str]) -> Optional[dict]:
    """Resolves a plan reference by its real _id (the normal case) or by
    slug (the "free" fallback string used when there's no active subscription)."""
    if not value:
        return None
    plans_col = _mongo._db["sim_subscription_plans"]
    if _looks_like_object_id(value):
        doc = plans_col.find_one({"_id": ObjectId(value)})
        if doc:
            return doc
    return plans_col.find_one({"slug": value})


def _sim_sub_effective_status(sub_doc: Optional[dict]) -> int:
    """Cancelled always wins over expires_at; otherwise active vs expired is
    just a straight expires_at-vs-now comparison."""
    if not sub_doc:
        return SIM_SUB_STATUS_INACTIVE
    if sub_doc.get("status") == SIM_SUB_STATUS_CANCELLED:
        return SIM_SUB_STATUS_CANCELLED
    expires_at = sub_doc.get("expires_at")
    now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
    if expires_at and expires_at < now_str:
        return SIM_SUB_STATUS_EXPIRED
    return SIM_SUB_STATUS_ACTIVE


def resolve_user_plan(user_id: Any) -> dict:
    """
    Returns the caller's effective sim_subscription_plans doc: their active
    subscription's plan if one exists and isn't expired/cancelled, else the
    Free plan. Never raises — falls back to `{}` only if even the Free plan
    doc is missing (shouldn't happen once seeded).
    """
    subs_col = _mongo._db["sim_user_subscriptions"]
    sub_doc = subs_col.find_one(_sim_user_or_filter(user_id), sort=[("_id", -1)]) if user_id is not None else None
    is_active_sub = _sim_sub_effective_status(sub_doc) == SIM_SUB_STATUS_ACTIVE
    plan_ref = sub_doc["plan_id"] if (sub_doc and is_active_sub) else "free"
    return _sim_find_plan(plan_ref) or _sim_find_plan("free") or {}


def normalize_execution_mode(value: Any) -> str:
    """Canonicalizes a simulator_strategy doc's execution_mode: "advanced"
    stays "advanced"; anything else (including the legacy "normal" value used
    before this field was renamed, or a missing field) reads as "regular"."""
    return "advanced" if str(value or "").strip().lower() == "advanced" else "regular"
