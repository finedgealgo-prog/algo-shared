"""
alert_checker.py
─────────────────
Background loop that evaluates the chart price/trendline alerts the
algo-admin frontend stores in `tv_alerts` (one document per alert — see
GET/PUT/DELETE /simulator/alerts) against the live spot price landing in
`option_chain_index_spot` (written by live_tick_dispatcher's
spot_tick_writer from the Kite ticker) — and fires each alert's webhook
the moment its condition is met, independent of whether any browser tab
with the chart open is still alive.

Ports the exact crossing / trigger-mode / trendline-value / webhook-retry
math from algo-admin's src/pages/Simulator/Chart.tsx (didCrossLevel,
isLevelTouched, getAlertPriceAtTime, deliverWebhookAlert) so server-side
and client-side evaluation agree on when an alert has actually fired.
One meaningful difference: the frontend evaluates bar-by-bar (previous
candle's close vs current candle's high/low); this loop instead compares
two consecutive live polls of the spot price directly — there's no
"candle" on the backend, so each poll is treated like the frontend
treats a new bar.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any

import requests

from features.indicator_alerts import (
    evaluate_indicator_condition,
    evaluate_price_condition,
    get_indicator_lookback_seconds,
    seconds_until_next_bar_close,
)
from features.chart_data import get_index_historical_chart_bars
from features.mongo_data import MongoData
from features.telegram_notifier import notify_user_for

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 2.0
ONCE_PER_MINUTE_COOLDOWN_MS = 60_000
# Indicator-condition alerts (Supertrend/MACD/MA Cross/RSI/Stochastic) are
# checked by their own scheduler loop (start_indicator_alert_scheduler_loop),
# not this 2s price/trendline poll — it sleeps until each active resolution's
# next actual bar-close instead of polling on a fixed cadence. This is just
# the ceiling on that sleep, so a freshly created alert on an otherwise-idle
# resolution is noticed within one clamp window instead of waiting on some
# unrelated resolution's boundary.
INDICATOR_SCHEDULER_MAX_SLEEP_SECONDS = 60.0

WEBHOOK_TIMEOUT_SECONDS = 8.0
WEBHOOK_MAX_ATTEMPTS = 3
WEBHOOK_RETRY_DELAY_SECONDS = [1.0, 2.0]

ALERTS_COLLECTION = "tv_alerts"
SPOT_COLLECTION = "option_chain_index_spot"

# Same exclusion as Chart.tsx's isUnimplementedDirection — these need
# channel geometry or rate-of-change/trend-state history neither engine
# actually computes yet. Selectable in the UI, never evaluated here.
_UNIMPLEMENTED_DIRECTIONS = {
    "enter_channel",
    "exit_channel",
    "inside_channel",
    "outside_channel",
    "moving_up",
    "moving_down",
    "moving_up_percent",
    "moving_down_percent",
    "rising_to_falling",
    "falling_to_rising",
}

# tv_chart_state's `symbol` is the frontend's internal slug (Chart.tsx's
# SYMBOL constant, currently always "nifty_50") — map to the `underlying`
# key option_chain_index_spot is keyed by (see live_tick_dispatcher.py's
# NSE_TOKEN_BY_UNDERLYING).
_SYMBOL_TO_UNDERLYING = {
    "nifty_50": "NIFTY",
    "nifty": "NIFTY",
    "banknifty": "BANKNIFTY",
    "bank_nifty": "BANKNIFTY",
    "finnifty": "FINNIFTY",
    "midcpnifty": "MIDCPNIFTY",
    "sensex": "SENSEX",
}


def _is_valid_webhook_url(url: str | None) -> bool:
    return bool(url) and (url.startswith("http://") or url.startswith("https://"))


def _resolve_message_placeholders(message: str, trigger_price: float) -> str:
    return re.sub(r"\{\{\s*close\s*\}\}", f"{trigger_price:.2f}", message or "", flags=re.IGNORECASE)


def _deliver_webhook(url: str, body: str) -> dict[str, Any]:
    """Python port of Chart.tsx's deliverWebhookAlert — same retry count,
    same backoff delays, same non-retry-on-4xx rule, same response capture."""
    if not _is_valid_webhook_url(url):
        return {"ok": False, "status": None, "responseText": "Invalid webhook URL."}

    try:
        json.loads(body)
        content_type = "application/json"
    except (ValueError, TypeError):
        content_type = "text/plain"

    last_status: int | None = None
    last_text = ""
    for attempt in range(WEBHOOK_MAX_ATTEMPTS):
        try:
            response = requests.post(
                url,
                data=body.encode("utf-8"),
                headers={"Content-Type": content_type},
                timeout=WEBHOOK_TIMEOUT_SECONDS,
            )
            last_status = response.status_code
            last_text = response.text
            if response.ok:
                return {"ok": True, "status": response.status_code, "responseText": last_text}
            if response.status_code < 500:
                return {"ok": False, "status": response.status_code, "responseText": last_text}
        except requests.RequestException as exc:
            last_text = str(exc)
        if attempt < WEBHOOK_MAX_ATTEMPTS - 1:
            time.sleep(WEBHOOK_RETRY_DELAY_SECONDS[attempt])
    return {"ok": False, "status": last_status, "responseText": last_text}


def _get_trendline_price_at_time(points: list[dict] | None, t: float) -> float | None:
    if not points or len(points) < 2:
        return None
    start, end = points[0], points[-1]
    start_time, end_time = start.get("time"), end.get("time")
    if start_time is None or end_time is None or start_time == end_time:
        return None
    slope = (end["price"] - start["price"]) / (end_time - start_time)
    return start["price"] + slope * (t - start_time)


def _is_time_inside_alert_line(points: list[dict] | None, line_mode: str | None, t: float) -> bool:
    if not points or len(points) < 2:
        return False
    start_time = points[0].get("time")
    end_time = points[-1].get("time")
    if start_time is None or end_time is None:
        return False
    if line_mode == "extended":
        return True
    if line_mode == "ray_left":
        return t <= end_time
    if line_mode == "ray_right":
        return t >= start_time
    return start_time <= t <= end_time


def _chain_needs_bar_close_engine(alert: dict) -> bool:
    """Mirrors Chart.tsx's chainNeedsBarCloseEngine — true the moment a
    chain needs "every condition checked against the same closed bar"
    instead of this module's tick-based price/trendline engine: either the
    primary or an additionalConditions row is an indicator (indicators
    only exist as bars), or there's more than one condition at all (this
    module's tick checker only ever evaluates an alert's own primary, so a
    price-or-trendline-only chain with an ANDed condition would otherwise
    have that second condition silently ignored)."""
    if alert.get("sourceType") == "indicator":
        return True
    return bool(alert.get("additionalConditions"))


def _get_alert_price_at_time(alert: dict, t: float) -> float | None:
    if alert.get("sourceType") == "trendline":
        points = alert.get("linePoints")
        if not _is_time_inside_alert_line(points, alert.get("lineMode"), t):
            return None
        return _get_trendline_price_at_time(points, t)
    price = alert.get("price")
    return float(price) if isinstance(price, (int, float)) else None


def _did_cross_level(prev_price: float, curr_price: float, prev_t: float, curr_t: float, alert: dict) -> str | None:
    """Tick-level analog of didCrossLevel — prev/curr are two consecutive
    live spot samples instead of two consecutive completed bars."""
    direction = alert.get("direction")
    if direction in _UNIMPLEMENTED_DIRECTIONS:
        return None

    prev_level = _get_alert_price_at_time(alert, prev_t)
    curr_level = _get_alert_price_at_time(alert, curr_t)
    if prev_level is None or curr_level is None:
        return None

    crossed_above = prev_price < prev_level and curr_price >= curr_level
    crossed_below = prev_price > prev_level and curr_price <= curr_level

    if direction == "crosses_above":
        return "up" if crossed_above else None
    if direction == "crosses_below":
        return "down" if crossed_below else None
    # "Greater Than"/"Less Than" are a plain state check, not a crossing
    # transition — matches Chart.tsx's didCrossLevel comment verbatim.
    if direction == "greater_than":
        return "up" if curr_price > curr_level else None
    if direction == "less_than":
        return "down" if curr_price < curr_level else None
    if crossed_above:
        return "up"
    if crossed_below:
        return "down"
    return None


def evaluate_trendline_condition(
    direction: str, line_points: list[dict] | None, line_mode: str | None, bars: list[dict]
) -> tuple[bool, float] | None:
    """Mirrors Chart.tsx's evaluateTrendlineCondition. Defined here rather
    than in indicator_alerts.py (alongside evaluate_price_condition/
    evaluate_indicator_condition) because it needs this module's own
    trendline geometry helpers (_did_cross_level/_get_alert_price_at_time)
    — pulling those the other way would be a circular import, since this
    module already imports from indicator_alerts.py. For a trendline-kind
    chain entry mixed into an AND chain that needs the bar-close engine
    (an indicator condition anywhere, or just a second condition of any
    kind) — reuses the same tick-level crossing math against two
    consecutive bar closes instead of two consecutive live ticks."""
    n = len(bars)
    if n < 2:
        return None
    last, prev = n - 1, n - 2
    bar_time = float(bars[last]["time"])
    prev_close = bars[prev].get("close")
    curr_close = bars[last].get("close")
    if not isinstance(prev_close, (int, float)) or not isinstance(curr_close, (int, float)):
        return None
    pseudo_alert = {"sourceType": "trendline", "direction": direction, "linePoints": line_points, "lineMode": line_mode}
    crossed = _did_cross_level(float(prev_close), float(curr_close), float(bars[prev]["time"]), float(bars[last]["time"]), pseudo_alert)
    return crossed is not None, bar_time


def _is_level_touched(price: float, t: float, alert: dict) -> str | None:
    direction = alert.get("direction")
    if direction in _UNIMPLEMENTED_DIRECTIONS:
        return None
    level = _get_alert_price_at_time(alert, t)
    if level is None:
        return None
    touched_up = price >= level
    touched_down = price <= level
    if direction in ("crosses_below", "less_than"):
        return "down" if touched_down else None
    if direction in ("crosses_above", "greater_than"):
        return "up" if touched_up else None
    if touched_up:
        return "up"
    if touched_down:
        return "down"
    return None


class _AlertChecker:
    """Holds the only state this loop needs across polls: each underlying's
    last-seen price/time (for edge-triggered crossing) — everything else
    (armedFromBarTime, once_only consumption, once_per_minute cooldown) is
    read straight from the stored alert document each cycle, so a server
    restart only ever loses one poll's worth of "previous price" context,
    never an alert's own state."""

    def __init__(self) -> None:
        self._previous_sample: dict[str, tuple[float, float]] = {}

    def run_cycle(self) -> None:
        db = MongoData()._db
        alerts_col = db[ALERTS_COLLECTION]
        spot_col = db[SPOT_COLLECTION]

        alerts = list(alerts_col.find({"active": True}))
        if not alerts:
            return

        now_ms = time.time() * 1000

        needed_underlyings = {
            _SYMBOL_TO_UNDERLYING.get(str(alert.get("symbol") or "").lower(), str(alert.get("symbol") or "").upper())
            for alert in alerts
        }
        current_price_by_underlying: dict[str, float] = {}
        for underlying in needed_underlyings:
            latest = spot_col.find_one({"underlying": underlying}, sort=[("timestamp", -1)])
            close = latest.get("close") if latest else None
            if isinstance(close, (int, float)):
                current_price_by_underlying[underlying] = float(close)

        alerts_by_symbol: dict[str, list[dict]] = {}
        for alert in alerts:
            alerts_by_symbol.setdefault(str(alert.get("symbol") or ""), []).append(alert)

        for symbol, symbol_alerts in alerts_by_symbol.items():
            symbol_key = symbol.lower()
            underlying = _SYMBOL_TO_UNDERLYING.get(symbol_key, symbol_key.upper())
            curr_price = current_price_by_underlying.get(underlying)
            if curr_price is None:
                continue

            prev_price, prev_t = self._previous_sample.get(underlying, (None, None))
            self._previous_sample[underlying] = (curr_price, now_ms)
            if prev_price is None:
                # First sample for this underlying since this process
                # started — nothing to compare against yet.
                continue

            self._check_alerts(symbol_alerts, prev_price, curr_price, prev_t, now_ms)

    def get_active_indicator_resolutions(self) -> set[str]:
        """Distinct BASE resolution values (indicatorResolution) across
        every active alert — only the alert's own base/chart resolution
        drives scheduling, same as real TradingView alerts, which only
        ever re-evaluate at the bar closes of the chart timeframe they
        were created from. A condition on a different (typically slower)
        interval doesn't get its own wake — it's simply read fresh (and so
        implicitly "holds" its last confirmed value) at every one of these
        base-cadence wakes; see check_indicator_alerts."""
        db = MongoData()._db
        alerts_col = db[ALERTS_COLLECTION]
        values = alerts_col.distinct("indicatorResolution", {"active": True, "indicatorResolution": {"$ne": None}})
        return {value for value in values if value}

    def _fetch_indicator_bars(self, symbol: str, resolution: str) -> list[dict]:
        try:
            lookback = get_indicator_lookback_seconds(resolution)
            to_ts = int(time.time())
            result = get_index_historical_chart_bars(symbol, from_ts=to_ts - lookback, to_ts=to_ts, resolution=resolution)
            return result.get("bars") or []
        except Exception:
            logger.exception("[alert_checker] failed to fetch %s/%s bars for indicator alert", symbol, resolution)
            return []

    @staticmethod
    def _evaluate_chain_entry(
        entry: dict, symbol: str, resolution: str, bars: list[dict], evaluation_by_tuple: dict
    ) -> tuple[bool, float] | None:
        """Evaluates one condition-chain entry (the primary one, reshaped by
        the caller, or an additionalConditions row) — indicator-kind via
        evaluate_indicator_condition, price-kind via evaluate_price_condition
        — caching by a tuple key so an entry shared across many alerts (or
        shared between one alert's own primary/extra rows) is still only
        computed once per cycle."""
        kind = entry.get("kind")
        if kind == "indicator":
            indicator_name = entry.get("indicatorName")
            condition = entry.get("indicatorCondition")
            if not (indicator_name and condition):
                return None
            raw_value = entry.get("value")
            threshold = None
            if raw_value not in (None, ""):
                try:
                    threshold = float(raw_value)
                except (TypeError, ValueError):
                    threshold = None
            key = (symbol, resolution, "indicator", indicator_name, condition, threshold)
            if key not in evaluation_by_tuple:
                evaluation_by_tuple[key] = evaluate_indicator_condition(indicator_name, condition, bars, threshold)
            return evaluation_by_tuple[key]
        if kind == "price":
            direction = entry.get("direction")
            value = entry.get("value")
            if direction is None or value is None:
                return None
            try:
                value = float(value)
            except (TypeError, ValueError):
                return None
            key = (symbol, resolution, "price", direction, value)
            if key not in evaluation_by_tuple:
                evaluation_by_tuple[key] = evaluate_price_condition(direction, value, bars)
            return evaluation_by_tuple[key]
        if kind == "trendline":
            direction = entry.get("direction")
            if direction is None:
                return None
            # linePoints isn't hashable into evaluation_by_tuple's cache
            # key, but a trendline's exact two anchor points are unique to
            # one alert anyway — no cross-alert sharing to dedup here.
            return evaluate_trendline_condition(direction, entry.get("linePoints"), entry.get("lineMode"), bars)
        return None

    @staticmethod
    def _effective_entry_resolution(entry: dict, base_resolution: str) -> str:
        """A condition entry's own interval when it picked one, else the
        chain's base indicatorResolution — mirrors Chart.tsx's Interval
        dropdown "Same as chart" fallback. Price/trendline entries have no
        interval of their own (only indicator conditions get one) so they
        always evaluate on the chain's base resolution."""
        if entry.get("kind") == "indicator":
            return entry.get("resolution") or base_resolution
        return base_resolution

    def check_indicator_alerts(self) -> None:
        """Called by start_indicator_alert_scheduler_loop whenever a
        subscribed resolution's bar-close boundary is hit. Queries via the
        indicatorResolution/active index (see mongo_data.ensure_core_
        indexes) instead of scanning every doc, then dedups twice: bars
        fetched once per (symbol, resolution), each chain entry evaluated
        once per (symbol, resolution, kind, ...) tuple — so cost depends on
        how many distinct setups exist, not on how many alerts/users share
        them. Covers both indicator-primary alerts and a price-primary
        alert that's had an indicator condition ANDed onto it (and vice
        versa). Each chain entry evaluates against its own effective
        resolution (mirrors Chart.tsx's checkIndicatorAlerts) — a chain no
        longer needs every condition to share one resolution, only to all
        be independently true at once."""
        db = MongoData()._db
        alerts_col = db[ALERTS_COLLECTION]
        alerts = list(alerts_col.find({"active": True, "indicatorResolution": {"$ne": None}}))
        if not alerts:
            return

        bars_by_key: dict[tuple[str, str], list[dict]] = {}
        evaluation_by_tuple: dict[tuple, tuple[bool, float] | None] = {}

        for alert in alerts:
            if alert.get("isReplayTest"):
                continue

            symbol = str(alert.get("symbol") or "")
            alert_id = alert.get("id")
            base_resolution = alert.get("indicatorResolution")
            if not (alert_id and base_resolution):
                continue

            source_type = alert.get("sourceType")
            if source_type == "indicator":
                primary_entry = {
                    "kind": "indicator",
                    "indicatorName": alert.get("indicatorName"),
                    "indicatorCondition": alert.get("indicatorCondition"),
                    "value": alert.get("indicatorValue"),
                    "resolution": base_resolution,
                }
            elif source_type == "trendline":
                primary_entry = {
                    "kind": "trendline",
                    "direction": alert.get("direction"),
                    "linePoints": alert.get("linePoints"),
                    "lineMode": alert.get("lineMode"),
                }
            else:
                primary_entry = {"kind": "price", "direction": alert.get("direction"), "value": alert.get("price")}
            chain = [primary_entry, *(alert.get("additionalConditions") or [])]
            resolved_chain = [
                (entry, self._effective_entry_resolution(entry, base_resolution)) for entry in chain
            ]

            evaluations: list[tuple[bool, float] | None] = []
            bars_missing = False
            for entry, resolution in resolved_chain:
                bar_key = (symbol, resolution)
                if bar_key not in bars_by_key:
                    bars_by_key[bar_key] = self._fetch_indicator_bars(symbol, resolution)
                bars = bars_by_key[bar_key]
                if len(bars) < 2:
                    bars_missing = True
                    break
                evaluations.append(self._evaluate_chain_entry(entry, symbol, resolution, bars, evaluation_by_tuple))
            if bars_missing:
                continue
            if any(evaluation is None or not evaluation[0] for evaluation in evaluations):
                continue

            # Every condition is independently true right now, each read
            # fresh off its own resolution's latest confirmed bar (so a
            # condition on a slower interval just keeps "holding" its last
            # confirmed true/false between its own closes). But the fire
            # time/dedup and displayed price are pinned to the alert's own
            # BASE resolution instead of any one condition's interval, same
            # as real TradingView alerts: they only ever re-evaluate at the
            # bar closes of the chart timeframe the alert itself was
            # created from, so this alert fires once per base-resolution
            # bar close for as long as every condition keeps holding true —
            # not just once per the fastest individual condition's own bar
            # close.
            base_bars = bars_by_key.get((symbol, base_resolution))
            if not base_bars:
                continue
            bar_time = base_bars[-1].get("time")
            if not isinstance(bar_time, (int, float)):
                continue

            last_signal = alert.get("lastIndicatorSignalBarTime")
            if isinstance(last_signal, (int, float)) and bar_time <= last_signal:
                continue

            trigger_price = base_bars[-1].get("close")
            if not isinstance(trigger_price, (int, float)):
                trigger_price = alert.get("price") or 0.0

            field_updates = {"lastIndicatorSignalBarTime": bar_time}
            if alert.get("triggerMode") == "once_only":
                field_updates["active"] = False

            self._fire_alert(alert, "indicator", float(trigger_price), field_updates)
            self._persist_update(alert_id, field_updates)

        if evaluation_by_tuple:
            logger.info(
                "[alert_checker] indicator check: %d distinct evaluations across %d alerts",
                len(evaluation_by_tuple), len(alerts),
            )

    def _check_alerts(
        self, alerts: list[dict], prev_price: float, curr_price: float, prev_t: float, now_ms: float
    ) -> None:
        for alert in alerts:
            alert_id = alert.get("id")
            if not alert_id or not alert.get("active"):
                continue
            # Created while scrubbing Replay on the frontend (Chart.tsx sets
            # isReplayTest at creation) — that's testing scaffolding against
            # historical bars, not a real live alert; firing its webhook
            # against actual market price here would be spurious.
            if alert.get("isReplayTest"):
                continue
            # Any chain with more than one condition (an indicator
            # anywhere, or simply a "+ Add condition" row of any kind) is
            # handled by check_indicator_alerts on its own resolution-
            # locked bar fetch instead, since "every condition matched on
            # the same bar" needs one well-defined timeframe — this tick
            # checker only ever evaluates the primary by itself.
            if _chain_needs_bar_close_engine(alert):
                continue
            armed_from = alert.get("armedFromBarTime") or 0
            if now_ms <= armed_from:
                continue

            trigger_mode = alert.get("triggerMode")
            direction: str | None = None

            if trigger_mode == "once_per_minute":
                touched = _is_level_touched(curr_price, now_ms, alert)
                if touched:
                    last_fired = alert.get("lastTriggeredAt") or 0
                    if now_ms - last_fired >= ONCE_PER_MINUTE_COOLDOWN_MS:
                        direction = touched
            else:
                direction = _did_cross_level(prev_price, curr_price, prev_t, now_ms, alert)

            if not direction:
                continue

            trigger_price = _get_alert_price_at_time(alert, now_ms)
            if trigger_price is None:
                trigger_price = alert.get("price")

            field_updates = {"lastTriggeredAt": now_ms}
            if trigger_mode == "once_only":
                field_updates["active"] = False

            self._fire_alert(alert, direction, trigger_price, field_updates)
            self._persist_update(alert_id, field_updates)

    def _fire_alert(
        self, alert: dict, direction: str, trigger_price: float, field_updates: dict
    ) -> None:
        alert_name = alert.get("name") or "Alert"
        price_str = f"{float(trigger_price):.2f}" if trigger_price is not None else ""
        arrow = "↑" if direction == "up" else "↓" if direction == "down" else "→"
        source_type = alert.get("sourceType") or "price"
        condition_label = {
            "crosses_above": "Crossing Up",
            "crosses_below": "Crossing Down",
            "crosses_either": "Crossing",
            "greater_than": "Greater Than",
            "less_than": "Less Than",
        }.get(alert.get("direction") or "", alert.get("direction") or "")
        symbol_label = str(alert.get("symbol") or "").upper()
        line_kind = "Trendline" if source_type == "trendline" else "Price"

        # ── Telegram notification ──────────────────────────────────────────
        # Sent whenever notifyInApp is True (the default) and the user has
        # linked their Telegram account — so alerts reach them even when the
        # chart tab is closed.
        if alert.get("notifyInApp", True):
            user_id = str(alert.get("user_id") or "").strip()
            tg_message = (
                f"{arrow} {alert_name}\n"
                f"{symbol_label} · {line_kind} · {condition_label}\n"
                f"Price: {price_str}"
            )
            notify_user_for(
                user_id or None,
                "CHART ALERT",
                tg_message,
                context={"symbol": symbol_label, "price": price_str},
                category="chart",
            )

        # ── Webhook ────────────────────────────────────────────────────────
        webhook_enabled = bool(alert.get("webhookEnabled"))
        webhook_url = alert.get("webhookUrl") or ""
        if not webhook_enabled or not webhook_url:
            logger.info("[alert_checker] %s triggered (%s) — no webhook configured", alert_name, direction)
            return

        message = alert.get("message") or f"{alert_name} triggered"
        body = _resolve_message_placeholders(message, float(trigger_price) if trigger_price is not None else 0.0)
        result = _deliver_webhook(webhook_url, body)

        field_updates["lastWebhookOk"] = result["ok"]
        field_updates["lastWebhookStatus"] = result["status"]
        field_updates["lastWebhookResponse"] = (result["responseText"] or "")[:2000]

        if result["ok"]:
            logger.info("[alert_checker] %s triggered (%s) — webhook delivered to %s", alert_name, direction, webhook_url)
        else:
            logger.error(
                "[alert_checker] %s triggered (%s) — webhook FAILED (%s) to %s: %s",
                alert_name, direction, result["status"], webhook_url, result["responseText"],
            )

    def _persist_update(self, alert_id: str, field_updates: dict) -> None:
        db = MongoData()._db
        alerts_col = db[ALERTS_COLLECTION]
        try:
            alerts_col.update_one({"id": alert_id}, {"$set": field_updates})
        except Exception:
            logger.exception("[alert_checker] failed to persist trigger state for alert %s", alert_id)


_checker = _AlertChecker()


async def start_alert_checker_loop() -> None:
    """Call once from a FastAPI startup hook — runs forever for the life of
    the process, same shape as api.py's other startup background loops
    (e.g. _auto_daily_scanner_snapshot)."""
    while True:
        try:
            await asyncio.to_thread(_checker.run_cycle)
        except Exception:
            logger.exception("[alert_checker] check cycle failed")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def start_indicator_alert_scheduler_loop() -> None:
    """Call once from a FastAPI startup hook, alongside start_alert_checker_
    loop — runs forever, but unlike that fixed 2s poll this sleeps until the
    next real bar-close across whichever resolutions currently have an
    active indicator alert (computed fresh each wake from
    get_active_indicator_resolutions), clamped to
    INDICATOR_SCHEDULER_MAX_SLEEP_SECONDS so a brand-new alert on an
    otherwise-idle resolution is still noticed promptly. Checking every
    currently-active resolution on each wake (not just whichever one's
    boundary was nearest) is deliberately simpler than tracking "which
    resolution is due" — it's safe because lastIndicatorSignalBarTime
    already guards against re-firing a bar that hasn't changed."""
    while True:
        sleep_seconds = INDICATOR_SCHEDULER_MAX_SLEEP_SECONDS
        try:
            resolutions = await asyncio.to_thread(_checker.get_active_indicator_resolutions)
            if resolutions:
                now = time.time()
                sleep_seconds = min(
                    min(seconds_until_next_bar_close(r, now) for r in resolutions),
                    INDICATOR_SCHEDULER_MAX_SLEEP_SECONDS,
                )
        except Exception:
            logger.exception("[indicator_alert_scheduler] failed to compute next wake time")

        await asyncio.sleep(max(sleep_seconds, 1.0))

        try:
            await asyncio.to_thread(_checker.check_indicator_alerts)
        except Exception:
            logger.exception("[indicator_alert_scheduler] check cycle failed")


# Manual on/off control for the price/trendline alert checker loop.
# Auto-started at scanner startup (_auto_start_alert_checker in api.py) but
# also controllable via /v1/alert-checker/{start,stop,status} so the checker
# can be paused without restarting the whole process.
_alert_checker_task: asyncio.Task | None = None


def is_alert_checker_running() -> bool:
    return _alert_checker_task is not None and not _alert_checker_task.done()


def start_alert_checker_monitor() -> dict[str, Any]:
    global _alert_checker_task
    if is_alert_checker_running():
        return {"status": "success", "running": True, "message": "Alert checker is already running."}
    _alert_checker_task = asyncio.create_task(start_alert_checker_loop())
    logger.info("[alert_checker] monitor started manually")
    return {"status": "success", "running": True, "message": "Alert checker started."}


async def stop_alert_checker_monitor() -> dict[str, Any]:
    global _alert_checker_task
    task = _alert_checker_task
    if task is None or task.done():
        _alert_checker_task = None
        return {"status": "success", "running": False, "message": "Alert checker is already stopped."}
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _alert_checker_task = None
    logger.info("[alert_checker] monitor stopped manually")
    return {"status": "success", "running": False, "message": "Alert checker stopped."}


# Manual on/off control for the indicator-alert scheduler — unlike
# start_alert_checker_loop's price/trendline path (always on, started once
# from api.py's startup hook), this one is NOT started automatically. A
# human starts/stops it from the monitor page (see signal_builder/router.py's
# /signal/indicator-alert-monitor/{start,stop,status}), the same on-demand
# pattern simulator/api_server.py's /monitor/{start,stop,status} already
# uses for the Simulator Monitor.
_indicator_monitor_task: asyncio.Task | None = None


def is_indicator_alert_monitor_running() -> bool:
    return _indicator_monitor_task is not None and not _indicator_monitor_task.done()


def start_indicator_alert_monitor() -> dict[str, Any]:
    global _indicator_monitor_task
    if is_indicator_alert_monitor_running():
        return {"status": "success", "running": True, "message": "Indicator alert monitor is already running."}
    _indicator_monitor_task = asyncio.create_task(start_indicator_alert_scheduler_loop())
    logger.info("[indicator_alert_scheduler] monitor started")
    return {"status": "success", "running": True, "message": "Indicator alert monitor started."}


async def stop_indicator_alert_monitor() -> dict[str, Any]:
    global _indicator_monitor_task
    task = _indicator_monitor_task
    if task is None or task.done():
        _indicator_monitor_task = None
        return {"status": "success", "running": False, "message": "Indicator alert monitor is already stopped."}
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _indicator_monitor_task = None
    logger.info("[indicator_alert_scheduler] monitor stopped")
    return {"status": "success", "running": False, "message": "Indicator alert monitor stopped."}
