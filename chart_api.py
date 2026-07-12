"""
chart_api.py
────────────
Chart domain — TradingView chart state, price/trendline + indicator alerts.
Backs the frontend's /simulator/full-chart page (Chart.tsx).

Runs mounted on algo.scanner's process/port (8002), not as its own uvicorn
service: scanner already imports the `scanner` package that alert_checker's
indicator path needs (scanner.service.get_index_historical_chart_bars), and
scanner does no per-tick heavy work, so chart/alert checks stay fast without
adding a 5th process. See algo.scanner/scanner_main.py for the mount point
(chart_api.py is symlinked there) and algo.scanner/scanner/service.py for the
search/historical-bars functions backing the two /v1/symbol_* routes below.

Live price for the chart itself comes from the shared /ws/live-quotes hub
(features/live_quote_socket.py, served by algo.websocket on 8003) — the
frontend already connects there directly (see commonApiBase.ts/WS_API_BASE),
so this module never opens its own broker connection. alert_checker's
price/trendline loop reads option_chain_index_spot, a collection algo.websocket
keeps fresh via its own dispatch_tick — also no separate connection needed
here.

Endpoints (mounted with prefix="/v1" — every API this page's Chart.tsx calls
lives under this one un-domain-prefixed path, deliberately not "/scanner" or
"/chart", so the frontend doesn't expose either backend module's name):
  GET    /v1/chart-state                       layout + resolution
  PUT    /v1/chart-state
  GET    /v1/alerts                             list price/trendline + indicator alerts
  PUT    /v1/alerts/{alert_id}                  create/edit one alert
  DELETE /v1/alerts/{alert_id}
  POST   /v1/indicator-alert-monitor/start       manual on/off — heavier than
  POST   /v1/indicator-alert-monitor/stop        the always-on price/trendline
  GET    /v1/indicator-alert-monitor/status      loop, see alert_checker.py
  GET    /v1/symbol_search                      index/stock/commodity search (see scanner/service.py)
  GET    /v1/symbol_historical_chart             OHLCV bars for any of the above
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from pymongo import MongoClient

from features import auth as app_auth
from features.alert_checker import (
    is_alert_checker_running,
    is_indicator_alert_monitor_running,
    start_alert_checker_monitor,
    stop_alert_checker_monitor,
    start_indicator_alert_monitor,
    stop_indicator_alert_monitor,
)
from features.chart_data import (
    get_symbol_historical_chart_bars,
    search_symbol_universe,
)

router = APIRouter(prefix="/v1")

_mongo_client      = MongoClient("mongodb://localhost:27017/")
_stock_db          = _mongo_client["stock_data"]
_tv_chart_state_col = _stock_db["tv_chart_state"]
_tv_alerts_col      = _stock_db["tv_alerts"]
# Same "stock_data" Mongo DB algo.simulator's shared MongoData() points at —
# read directly rather than importing algo.simulator/api.py, since this module
# is mounted on algo.scanner's process (not algo.simulator's) and the two
# don't share a Python import path. Only used for the plan-limit checks below
# (tv_max_alerts_per_strategy / tv_max_indicator_conditions).
_sim_webhooks_col = _stock_db["simulator_webhooks"]
_sim_subs_col     = _stock_db["sim_user_subscriptions"]
_sim_plans_col    = _stock_db["sim_subscription_plans"]

IST = timezone(timedelta(hours=5, minutes=30))

_WEBHOOK_URL_ID_RE = re.compile(r"/webhook/tv/alert/([a-fA-F0-9]{24})")


def _resolve_plan_for_user(user_id: Any) -> dict:
    """
    Minimal, chart-service-local copy of algo.simulator's plan resolution
    (_sim_resolve_plan_and_advanced_slots / _sim_sub_effective_status) — falls
    back to the "free" plan when there's no active subscription, matching
    that module's own logic exactly (cancelled always wins over expiry).
    """
    ids = [user_id]
    try:
        ids.append(ObjectId(user_id))
    except Exception:
        pass
    sub_doc = _sim_subs_col.find_one({"user_id": {"$in": ids}}, sort=[("_id", -1)])
    is_active = False
    if sub_doc:
        if sub_doc.get("status") == 3:  # SIM_SUB_STATUS_CANCELLED — always wins
            is_active = False
        else:
            expires_at = sub_doc.get("expires_at")
            now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
            is_active = not (expires_at and expires_at < now_str)
    plan_id = sub_doc["plan_id"] if (sub_doc and is_active) else "free"
    plan = None
    try:
        plan = _sim_plans_col.find_one({"_id": ObjectId(plan_id)})
    except Exception:
        pass
    if not plan:
        plan = _sim_plans_col.find_one({"slug": plan_id}) or _sim_plans_col.find_one({"slug": "free"})
    return plan or {}


def _resolve_webhook_scope(webhook_url: str) -> Optional[dict]:
    """Extracts the webhook id from a pasted simulator webhook URL and looks up
    which strategy (and user) it belongs to. None if the URL doesn't match or
    the webhook no longer exists."""
    match = _WEBHOOK_URL_ID_RE.search(webhook_url or "")
    if not match:
        return None
    try:
        webhook_id = ObjectId(match.group(1))
    except Exception:
        return None
    doc = _sim_webhooks_col.find_one({"_id": webhook_id})
    if not doc:
        return None
    # simulator_webhooks.user_id is stored as a raw ObjectId (see
    # algo.simulator/api.py's webhook-creation insert_one — current_user["_id"]
    # is never cast to str there). Cast here so callers never persist an
    # ObjectId onto a tv_alerts doc, which pydantic can't JSON-serialize back
    # out of GET /alerts.
    owner_id = doc.get("user_id")
    return {"strategy_id": doc.get("strategy_id"), "user_id": str(owner_id) if owner_id is not None else None}


class ChartStateIn(BaseModel):
    page_id: str = "simulator_chart_workspace"
    symbol: str = "nifty_50"
    layout: Any = None
    resolution: Optional[str] = None


class ChartAlertIn(BaseModel):
    page_id: str = "simulator_chart_workspace"
    symbol: str = "nifty_50"
    alert: Dict[str, Any]


def _get_required_user_id(current_user: dict) -> str:
    user_id = str(current_user.get("_id") or "").strip()
    if not user_id:
        raise HTTPException(status_code=401, detail="Authenticated user is required")
    return user_id


@router.get("/chart-state")
async def get_chart_state(
    page_id: str = Query(default="simulator_chart_workspace"),
    symbol: str = Query(default="nifty_50"),
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    """Layout + resolution only — each alert is its own document in
    tv_alerts (see GET /alerts), not nested in this doc anymore."""
    try:
        user_id = _get_required_user_id(current_user)
        doc = _tv_chart_state_col.find_one(
            {"user_id": user_id, "page_id": page_id, "symbol": symbol},
            {"_id": 0, "layout": 1, "resolution": 1, "updated_at": 1, "page_id": 1, "symbol": 1},
        )
        return {
            "status": "success",
            "state": doc
            or {
                "page_id": page_id,
                "symbol": symbol,
                "layout": None,
                "resolution": None,
                "updated_at": None,
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.put("/chart-state")
async def save_chart_state(
    body: ChartStateIn,
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    try:
        user_id = _get_required_user_id(current_user)
        normalized_page_id = str(body.page_id or "simulator_chart_workspace").strip() or "simulator_chart_workspace"
        normalized_symbol = str(body.symbol or "nifty_50").strip() or "nifty_50"
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        _tv_chart_state_col.update_one(
            {"user_id": user_id, "page_id": normalized_page_id, "symbol": normalized_symbol},
            {
                "$set": {
                    "user_id": user_id,
                    "page_id": normalized_page_id,
                    "symbol": normalized_symbol,
                    "layout": body.layout,
                    "resolution": body.resolution,
                    "updated_at": now_str,
                }
            },
            upsert=True,
        )
        return {"status": "success", "updated_at": now_str}
    except HTTPException:
        raise
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/alerts")
async def list_chart_alerts(
    page_id: str = Query(default="simulator_chart_workspace"),
    symbol: str = Query(default="nifty_50"),
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    """Every alert is its own tv_alerts document — one new record per
    alert created, never a shared array field rewritten on every save."""
    try:
        user_id = _get_required_user_id(current_user)
        docs = list(_tv_alerts_col.find({"user_id": user_id, "page_id": page_id, "symbol": symbol}, {"_id": 0}))
        # Defensive: alerts saved before the _resolve_webhook_scope fix above
        # may still have a raw ObjectId sitting in webhook_owner_id — pydantic
        # can't serialize that, so stringify it here rather than 500ing on
        # every pre-existing webhook-linked alert.
        for doc in docs:
            owner_id = doc.get("webhook_owner_id")
            if isinstance(owner_id, ObjectId):
                doc["webhook_owner_id"] = str(owner_id)
        return {"status": "success", "alerts": docs}
    except HTTPException:
        raise
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.put("/alerts/{alert_id}")
async def save_chart_alert(
    alert_id: str,
    body: ChartAlertIn,
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    """Upserts exactly one alert document, scoped to this user — creating
    a new alert and editing an existing one both land here, keyed on the
    alert's own client-generated id (tv_alerts_id_uq).

    Two plan-gated checks run before the upsert:
    - tv_max_indicator_conditions: caps how many indicator-kind entries (the
      primary condition plus any chained additionalConditions) a single alert
      can carry, checked against the alert's own creator's plan.
    - tv_max_alerts_per_strategy: caps how many active alerts can point their
      Webhook URL at the same simulator strategy, checked against the plan of
      whoever *owns* that webhook (which may be a different user than the one
      creating this alert)."""
    try:
        user_id = _get_required_user_id(current_user)
        normalized_page_id = str(body.page_id or "simulator_chart_workspace").strip() or "simulator_chart_workspace"
        normalized_symbol = str(body.symbol or "nifty_50").strip() or "nifty_50"
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        doc = dict(body.alert)

        source_type = str(doc.get("sourceType") or "")

        indicator_count = 1 if source_type == "indicator" else 0
        for entry in (doc.get("additionalConditions") or []):
            if isinstance(entry, dict) and str(entry.get("kind") or "") == "indicator":
                indicator_count += 1

        # Resolved once and reused by both checks below — indicator conditions
        # count regardless of the alert's own active flag (an inactive alert
        # can still be edited back to a too-large condition list), while the
        # price/trendline caps only care about *active* alerts (mirrors the
        # webhook tv_max_alerts_per_strategy check further down).
        creator_plan = _resolve_plan_for_user(user_id) if (indicator_count > 0 or source_type in ("price", "trendline")) else {}

        if indicator_count > 0:
            max_indicator_conditions = creator_plan.get("tv_max_indicator_conditions")
            if max_indicator_conditions not in (None, -1) and indicator_count > int(max_indicator_conditions):
                creator_plan_name = creator_plan.get("plan_name") or "current"
                return {
                    "status": "error",
                    "message": (
                        f"Your {creator_plan_name} plan allows up to {max_indicator_conditions} "
                        f"indicator condition{'s' if max_indicator_conditions != 1 else ''} per alert "
                        f"(this one has {indicator_count}). Remove one, or upgrade/buy more."
                    ),
                }

        # Price Alerts / Trendline Alerts count caps (tv_price_alerts_count /
        # tv_trendline_alerts_count) — same count_documents + exclude-self-by-id
        # convention as the webhook-scoped tv_max_alerts_per_strategy check
        # below, just scoped by creator + sourceType instead of by webhook.
        if source_type in ("price", "trendline") and doc.get("active", True):
            count_field = "tv_price_alerts_count" if source_type == "price" else "tv_trendline_alerts_count"
            limit = creator_plan.get(count_field)
            if limit not in (None, -1):
                used = _tv_alerts_col.count_documents({
                    "id": {"$ne": alert_id},
                    "user_id": user_id,
                    "active": True,
                    "sourceType": source_type,
                })
                if used >= int(limit):
                    creator_plan_name = creator_plan.get("plan_name") or "current"
                    label = "Price" if source_type == "price" else "Trendline"
                    return {
                        "status": "error",
                        "message": (
                            f"Your {creator_plan_name} plan allows up to {int(limit)} active {label} "
                            f"alert{'s' if int(limit) != 1 else ''} (you already have {used}). "
                            "Disable one, or upgrade/buy more."
                        ),
                    }

        webhook_url = str(doc.get("webhookUrl") or "").strip()
        webhook_strategy_id = None
        webhook_owner_id = None
        if doc.get("webhookEnabled") and webhook_url:
            scope = _resolve_webhook_scope(webhook_url)
            if not scope:
                return {
                    "status": "error",
                    "message": "That webhook URL doesn't look valid — generate one from Paper Trade first.",
                }
            webhook_strategy_id = scope.get("strategy_id")
            webhook_owner_id = scope.get("user_id")
            webhook_plan = _resolve_plan_for_user(webhook_owner_id)
            webhook_plan_name = webhook_plan.get("plan_name") or "current"
            if webhook_plan.get("trade_generate_webhook_mode") != "enabled":
                return {
                    "status": "error",
                    "message": f"Webhook Trading isn't available on the {webhook_plan_name} plan this webhook belongs to.",
                }
            limit = webhook_plan.get("tv_max_alerts_per_strategy")
            if doc.get("active", True) and limit not in (None, -1):
                scope_filter: dict[str, Any] = {"id": {"$ne": alert_id}, "active": True}
                if webhook_strategy_id:
                    scope_filter["webhook_strategy_id"] = webhook_strategy_id
                else:
                    # Not-yet-saved-strategy draft webhook — no strategy_id yet,
                    # so scope by (owner, still-draft) instead, same convention
                    # algo.simulator uses for these (see simulator_pt_webhook_usage).
                    scope_filter["webhook_strategy_id"] = None
                    scope_filter["webhook_owner_id"] = webhook_owner_id
                used = _tv_alerts_col.count_documents(scope_filter)
                if used >= int(limit):
                    return {
                        "status": "error",
                        "message": (
                            f"This strategy already has {used}/{int(limit)} active alerts on the "
                            f"{webhook_plan_name} plan. Disable one of the existing alerts on it, "
                            "or upgrade/buy more, before creating another."
                        ),
                    }

        doc["webhook_strategy_id"] = webhook_strategy_id
        doc["webhook_owner_id"] = webhook_owner_id
        doc["id"] = alert_id
        doc["user_id"] = user_id
        doc["page_id"] = normalized_page_id
        doc["symbol"] = normalized_symbol
        doc["updated_at"] = now_str
        _tv_alerts_col.update_one(
            {"id": alert_id, "user_id": user_id},
            {"$set": doc},
            upsert=True,
        )
        return {"status": "success", "updated_at": now_str}
    except HTTPException:
        raise
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.delete("/alerts/{alert_id}")
async def delete_chart_alert(
    alert_id: str,
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    try:
        user_id = _get_required_user_id(current_user)
        _tv_alerts_col.delete_one({"id": alert_id, "user_id": user_id})
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.post("/indicator-alert-monitor/start")
async def indicator_alert_monitor_start(
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    return start_indicator_alert_monitor()


@router.post("/indicator-alert-monitor/stop")
async def indicator_alert_monitor_stop(
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    return await stop_indicator_alert_monitor()


@router.get("/indicator-alert-monitor/status")
async def indicator_alert_monitor_status(
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    return {"status": "success", "running": is_indicator_alert_monitor_running()}


@router.api_route("/alert-checker/start", methods=["GET", "POST"])
async def alert_checker_start(
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    return start_alert_checker_monitor()


@router.api_route("/alert-checker/stop", methods=["GET", "POST"])
async def alert_checker_stop(
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    return await stop_alert_checker_monitor()


@router.get("/alert-checker/status")
async def alert_checker_status(
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    return {"status": "success", "running": is_alert_checker_running()}


@router.get("/symbol_search")
async def chart_symbol_search(
    q: str = Query(default=""),
    limit: int = Query(default=30, ge=1, le=100),
) -> dict[str, Any]:
    try:
        return {"status": "success", "items": search_symbol_universe(q, limit=limit)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/symbol_historical_chart")
async def chart_symbol_historical_chart(
    symbol: str = Query(..., description="Ticker from /v1/symbol_search, e.g. nifty_50, RELIANCE, GOLD"),
    symbol_type: str = Query(default="index", description="index | stock | commodity"),
    from_ts: Optional[int] = Query(default=None, alias="from"),
    to_ts: Optional[int] = Query(default=None, alias="to"),
    resolution: str = Query(default="1D", description="5/15/30/60/240/480 (minutes) or 1D"),
) -> dict[str, Any]:
    try:
        return get_symbol_historical_chart_bars(
            symbol, symbol_type, from_ts=from_ts, to_ts=to_ts, resolution=resolution
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


async def start_chart_background_loops() -> None:
    """
    Call once from algo.scanner's startup hook. Starts the always-on
    price/trendline alert loop (2s Mongo poll, see alert_checker.py) as a
    background task. The heavier indicator-condition loop stays manual —
    started/stopped via /v1/indicator-alert-monitor/{start,stop}, same
    on-demand pattern the Simulator Monitor already uses.
    """
    start_alert_checker_monitor()
