"""
live_quote_socket.py
─────────────────────
Frontend-facing WebSocket that streams live LTP for an arbitrary, client-declared
set of instrument tokens — independent of any algo_trades record.

Why this exists instead of reusing /ws/update (execution_socket.py):
/ws/update's `subscribe_tokens` is built entirely from running trade records
(_build_subscribe_tokens), so it only ever covers tokens belonging to an active
algo trade. A manually-built basket (e.g. the paper-trade builder's "New Position"
legs, picked straight from the option chain) is pure client-side state that's
never persisted as a trade record until the user actually places the order, so it
has no token coverage there. This module reads broker_gateway.broker_ticker_manager
(the same kite/dhan-routed ltp_map every other live feature reads) and lets any
client subscribe to whichever tokens it currently cares about.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

log = logging.getLogger(__name__)
IST = timezone(timedelta(hours=5, minutes=30))
EMIT_INTERVAL_SECONDS = 0.5
REST_REFRESH_INTERVAL_SECONDS = 1.5
# Paid-plan-only MTM broadcast (see "auth" action / _handle_auth) — a session's
# strategy list + mode is a one-time-ish Mongo read, not a per-tick one, so this
# just governs how stale that cache is allowed to get before re-reading it.
STRATEGY_CACHE_REFRESH_SECONDS = 75.0
# "Normal" mode strategies (the default — no advanced-slot execution_mode set)
# batch on this cadence instead of emitting on every tick like "advanced" ones do.
NORMAL_MTM_BATCH_SECONDS = 30.0
_SIMULATOR_STRATEGY_INDEX_ENSURED = False


def _ensure_simulator_strategy_index(db) -> None:
    """
    Same index api.py's own copy ensures (idx_simulator_strategy_user_v1) — this
    process (algo.websocket) may be the first one to ever run
    _refresh_session_strategies, so it can't assume api.py already created it.
    create_index is a cheap no-op once the index exists, and index creation
    itself is collection-level, not per-process, so whichever process gets
    here first is the one that actually creates it.
    """
    global _SIMULATOR_STRATEGY_INDEX_ENSURED
    if _SIMULATOR_STRATEGY_INDEX_ENSURED:
        return
    try:
        db._db["simulator_strategy"].create_index(
            [("user_id", 1), ("all_exited", 1)],
            name="idx_simulator_strategy_user_v1",
        )
    except Exception:
        pass
    _SIMULATOR_STRATEGY_INDEX_ENSURED = True


# Always included in _refresh_underlying_quotes' instrument set, regardless
# of whether any open strategy/subscribed token currently references them —
# standalone surfaces with no strategy of their own (e.g. the simulator's
# bare chart page) still need a live NIFTY spot tick to show something. Also
# backs the algo-trade pages' shared index ticker bar (Live/Fast-Forward/
# Forward-Test), which shows all six of these regardless of what the user
# has open, so all six stay warm unconditionally rather than only appearing
# once some strategy happens to reference them.
ALWAYS_TRACKED_UNDERLYINGS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY", "INDIAVIX"}

live_quote_socket_router = APIRouter()


def _now_iso() -> str:
    return datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")


@dataclass
class _LiveQuoteSession:
    websocket: WebSocket
    session_id: str
    subscribed_tokens: set[str] = field(default_factory=set)
    last_sent: dict[str, float] = field(default_factory=dict)
    # Underlying (index/stock) spot broadcast is global, not opt-in per
    # session — see _collect_changed_underlyings — so this tracks "last
    # spot_price this session was sent" the same way last_sent does for
    # option tokens, just keyed by instrument name instead.
    last_sent_underlying: dict[str, float] = field(default_factory=dict)
    closed: bool = False
    task: asyncio.Task | None = None
    # ── Per-user MTM broadcast (opt-in via the "auth" action — sessions that never
    # send it behave exactly as before, so every other page already on this socket
    # is unaffected). Advanced-mode strategies (execution_mode: "advanced" on the
    # simulator_strategy doc) get tick-driven MTM in the same 0.5s loop as raw LTP;
    # everything else (the default) batches on its own 30s per-session cadence
    # instead, so this one socket serves both without a second connection. ──
    user_id: str | None = None
    mtm_legs: dict[str, list[dict]] = field(default_factory=dict)   # strategy_id -> resolved open legs
    mtm_mode: dict[str, str] = field(default_factory=dict)          # strategy_id -> "advanced" | "regular"
    last_sent_mtm: dict[str, float] = field(default_factory=dict)   # strategy_id -> last emitted total MTM
    last_strategy_refresh: float = 0.0
    last_normal_batch: float = 0.0


class _LiveQuoteHub:
    def __init__(self) -> None:
        self._sessions: dict[str, _LiveQuoteSession] = {}
        self._lock = asyncio.Lock()
        # Fallback for tokens broker_ticker_manager.ltp_map has nothing for
        # (just-subscribed, illiquid, or off-peak — the WS tick simply never
        # arrives) — see _refresh_missing_via_rest. Keyed by token, shared
        # across every session so two clients watching the same token only
        # cost one REST call, not one each.
        self._rest_ltp_cache: dict[str, float] = {}
        self._rest_refresh_task: asyncio.Task | None = None
        # instrument (e.g. "NIFTY", "BSE") → {spot_price, change_pct, change_points, ...}
        # for every underlying with at least one open paper-trade strategy,
        # across *every* portfolio — refreshed on the same cadence as
        # _rest_ltp_cache above (_rest_refresh_loop), broadcast to every
        # connected session regardless of that session's own option-token
        # subscriptions (see _collect_changed_underlyings). This is what
        # makes "watch every open strategy's underlying move, system-wide"
        # possible without each client having to know in advance which
        # instruments to ask for.
        self._underlying_quote_cache: dict[str, dict] = {}

    def _ensure_rest_refresh_started(self) -> None:
        if self._rest_refresh_task is None or self._rest_refresh_task.done():
            self._rest_refresh_task = asyncio.create_task(self._rest_refresh_loop())

    async def register(self, websocket: WebSocket) -> _LiveQuoteSession:
        await websocket.accept()
        session = _LiveQuoteSession(websocket=websocket, session_id=uuid.uuid4().hex)
        async with self._lock:
            self._sessions[session.session_id] = session
        session.task = asyncio.create_task(self._emit_loop(session))
        self._ensure_rest_refresh_started()
        await self._send_message(session, "message", {
            "message": "live quote socket connected",
            "session_id": session.session_id,
        })
        return session

    async def unregister(self, session: _LiveQuoteSession) -> None:
        session.closed = True
        if session.task and not session.task.done():
            session.task.cancel()
            try:
                await session.task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                log.debug("live quote task close error session=%s: %s", session.session_id, exc)
        async with self._lock:
            self._sessions.pop(session.session_id, None)

    async def handle_client_message(self, session: _LiveQuoteSession, raw_message: str) -> None:
        try:
            payload = json.loads(raw_message or "{}")
        except Exception:
            return
        action = str(payload.get("action") or "").strip().lower()
        tokens = [str(t or "").strip() for t in (payload.get("tokens") or []) if str(t or "").strip()]

        if action == "resolve":
            await self._handle_resolve(session, payload)
            return

        if action == "auth":
            await self._handle_auth(session, payload)
            return

        if action == "unsubscribe":
            for token in tokens:
                session.subscribed_tokens.discard(token)
                session.last_sent.pop(token, None)
            return

        if action == "subscribe":
            new_tokens = [t for t in tokens if t not in session.subscribed_tokens]
            session.subscribed_tokens.update(tokens)
        elif action == "replace":
            # The basket's leg set changes as a whole on every add/remove/expiry-change, so the
            # client just resends its current full token list rather than diffing client-side.
            new_tokens = [t for t in tokens if t not in session.subscribed_tokens]
            removed_tokens = session.subscribed_tokens - set(tokens)
            session.subscribed_tokens = set(tokens)
            for token in removed_tokens:
                session.last_sent.pop(token, None)
        else:
            return

        if new_tokens:
            await asyncio.to_thread(self._ensure_broker_subscribed, new_tokens)

    async def _handle_resolve(self, session: _LiveQuoteSession, payload: dict) -> None:
        """
        Resolve {instrument, expiry, strike, option_type} → token via active_option_tokens,
        subscribe it, and return its current ltp (if already live) — all in one round trip.

        Deliberately not /live-greeks-chain: that endpoint builds the *entire* chain's
        Greeks plus a Dhan REST quote call (~1.5s) just to hand back one row. A contract lookup
        is a single indexed Mongo query (idx_active_option_contract_v2) — sub-5ms — since all
        this needs is the token; the socket's own ltp_map already carries the live price once
        subscribed.
        """
        request_id = str(payload.get("request_id") or "").strip()
        instrument = str(payload.get("instrument") or "").strip().upper()
        expiry = str(payload.get("expiry") or "").strip()[:10]
        option_type = str(payload.get("option_type") or "").strip().upper()
        # A futures contract has no strike — always stored as 0.0 (see
        # _sync_dhan_index_future_tokens), so a FUT resolve request doesn't need
        # one supplied at all; CE/PE still require a real strike.
        is_future = option_type == "FUT"
        try:
            strike = float(payload.get("strike"))
        except (TypeError, ValueError):
            strike = 0.0 if is_future else None

        if not instrument or not expiry or option_type not in ("CE", "PE", "FUT") or strike is None:
            await self._send_message(session, "resolve_error", {
                "request_id": request_id,
                "message": "instrument, expiry and option_type are required (strike too, unless option_type is FUT)",
            })
            return

        contract = await asyncio.to_thread(self._lookup_contract, instrument, expiry, strike, option_type)
        if not contract:
            await self._send_message(session, "resolve_error", {
                "request_id": request_id,
                "instrument": instrument,
                "expiry": expiry,
                "strike": strike,
                "option_type": option_type,
                "message": "contract not found",
            })
            return

        token = contract["token"]
        session.subscribed_tokens.add(token)
        await asyncio.to_thread(self._ensure_broker_subscribed, [token])

        from features.broker_gateway import broker_ticker_manager
        ltp = broker_ticker_manager.get_ltp(token)

        await self._send_message(session, "resolved", {
            "request_id": request_id,
            "instrument": instrument,
            "expiry": expiry,
            "strike": strike,
            "option_type": option_type,
            "token": token,
            "symbol": contract.get("symbol") or "",
            "ltp": float(ltp) if ltp else None,
        })

    async def _handle_auth(self, session: _LiveQuoteSession, payload: dict) -> None:
        """
        Opt-in identity for the MTM broadcast — every other action above works
        the same with or without this ever being sent, so pages that only want
        raw LTP (the vast majority of this socket's callers) are untouched.
        Paid-plan only by convention (the frontend never opens this socket at
        all for free plan — see isFreePlan gates on useLiveQuoteSocket), but
        nothing here re-checks that server-side since a free user simply has
        no socket to send this on in the first place.
        """
        token = str(payload.get("token") or "").strip()
        if not token:
            return
        try:
            from features.auth import decode_access_token
            claims = decode_access_token(token)
            user_id = str(claims.get("sub") or "").strip()
        except Exception:
            log.debug("live quote auth: invalid token for session=%s", session.session_id)
            return
        if not user_id:
            return
        session.user_id = user_id
        session.last_strategy_refresh = time.monotonic()
        await asyncio.to_thread(self._refresh_session_strategies, session)

    def _refresh_session_strategies(self, session: _LiveQuoteSession) -> None:
        """
        One Mongo read for this session's whole open-strategy set, not one per
        tick — _collect_mtm_updates below only ever does in-memory dict lookups
        against what this populates, same principle as ltp_map/last_sent above.
        Re-run on STRATEGY_CACHE_REFRESH_SECONDS, not on every emit loop tick.
        """
        if not session.user_id:
            return
        from features.mongo_data import MongoData
        db = MongoData()
        try:
            _ensure_simulator_strategy_index(db)
            docs = db._db["simulator_strategy"].find(
                {
                    "$or": [{"user_id": session.user_id}, {"user_id": {"$exists": False}}],
                    "all_exited": {"$ne": True},
                },
                {"positions": 1, "execution_mode": 1},
            )
            legs_by_strategy: dict[str, list[dict]] = {}
            mode_by_strategy: dict[str, str] = {}
            for doc in docs:
                strategy_id = str(doc.get("_id"))
                legs: list[dict] = []
                for position in (doc.get("positions") or []):
                    if not isinstance(position, dict) or position.get("exited"):
                        continue
                    pos_token = str(position.get("token") or "")
                    if not pos_token:
                        continue
                    legs.append({
                        "token": pos_token,
                        "entry": float(position.get("entry_price") or 0),
                        "side": "S" if str(position.get("type") or "").strip().lower().startswith("s") else "B",
                        "lots": float(position.get("lots") or 1),
                        "lot_size": float(position.get("lot_size") or 1),
                    })
                if legs:
                    legs_by_strategy[strategy_id] = legs
                    # Regular-mode strategies (the default, no execution_mode set, a
                    # legacy "normal" value, or anything besides the literal
                    # "advanced") batch on the 30s cadence below; only an
                    # Advanced-slot strategy emits every tick.
                    mode_by_strategy[strategy_id] = (
                        "advanced" if str(doc.get("execution_mode") or "").lower() == "advanced" else "regular"
                    )
            session.mtm_legs = legs_by_strategy
            session.mtm_mode = mode_by_strategy
        except Exception as exc:
            log.warning("live quote strategy refresh error user=%s: %s", session.user_id, exc)
        finally:
            db.close()

    def _compute_strategy_mtm(self, legs: list[dict], ltp_map: dict) -> float | None:
        """Pure in-memory — no I/O, safe to call every tick. None means no leg has a
        live price yet (nothing to emit), not "MTM is zero"."""
        total = 0.0
        has_price = False
        for leg in legs:
            ltp = ltp_map.get(leg["token"])
            if not ltp:
                continue
            has_price = True
            unit_pnl = (ltp - leg["entry"]) if leg["side"] == "B" else (leg["entry"] - ltp)
            total += unit_pnl * leg["lots"] * leg["lot_size"]
        return total if has_price else None

    def _collect_mtm_updates(self, session: _LiveQuoteSession, mode: str, force: bool) -> list[dict]:
        from features.broker_gateway import broker_ticker_manager
        ltp_map = broker_ticker_manager.ltp_map or {}
        changed: list[dict] = []
        for strategy_id, legs in session.mtm_legs.items():
            if session.mtm_mode.get(strategy_id, "regular") != mode:
                continue
            mtm = self._compute_strategy_mtm(legs, ltp_map)
            if mtm is None:
                continue
            mtm = round(mtm, 2)
            if not force and session.last_sent_mtm.get(strategy_id) == mtm:
                continue
            session.last_sent_mtm[strategy_id] = mtm
            changed.append({"strategy_id": strategy_id, "mtm": mtm})
        return changed

    def _lookup_contract(self, instrument: str, expiry: str, strike: float, option_type: str) -> dict | None:
        from features.mongo_data import MongoData
        from features.broker_gateway import _active_broker
        db = MongoData()
        try:
            doc = db._db["active_option_tokens"].find_one(
                {
                    "instrument": instrument,
                    "expiry": expiry,
                    "strike": strike,
                    "option_type": option_type,
                    "broker": _active_broker(),
                },
                {"_id": 0, "token": 1, "tokens": 1, "symbol": 1},
            )
        finally:
            db.close()
        if not doc:
            return None
        token = str(doc.get("token") or doc.get("tokens") or "").strip()
        if not token:
            return None
        return {"token": token, "symbol": str(doc.get("symbol") or "")}

    def _ensure_broker_subscribed(self, tokens: list[str]) -> None:
        try:
            from features.live_event import _subscribe_live_option_token
        except Exception as exc:
            log.debug("live quote subscribe import error: %s", exc)
            return
        for token in tokens:
            try:
                _subscribe_live_option_token(token)
            except Exception as exc:
                log.debug("live quote subscribe error token=%s: %s", token, exc)

    async def _emit_loop(self, session: _LiveQuoteSession) -> None:
        try:
            while not session.closed:
                if session.subscribed_tokens:
                    changed = self._collect_changed_ltp(session)
                    if changed:
                        await session.websocket.send_text(json.dumps({
                            "type": "ltp_update",
                            "data": changed,
                            "server_time": _now_iso(),
                        }))
                # Underlying broadcast is unconditional — every connected
                # session gets every open strategy's instrument move, not
                # just the legs it explicitly subscribed to (see
                # _collect_changed_underlyings).
                changed_underlyings = self._collect_changed_underlyings(session)
                if changed_underlyings:
                    await session.websocket.send_text(json.dumps({
                        "type": "underlying_update",
                        "data": changed_underlyings,
                        "server_time": _now_iso(),
                    }))

                if session.user_id:
                    # Isolated from the raw ltp_update/underlying_update above on purpose —
                    # any failure here (bad leg data, a Mongo blip) must never kill this
                    # session's whole emit loop, which is what an uncaught exception here
                    # would otherwise do (this whole loop only has one outer try/except).
                    try:
                        now = time.monotonic()
                        if now - session.last_strategy_refresh >= STRATEGY_CACHE_REFRESH_SECONDS:
                            session.last_strategy_refresh = now
                            await asyncio.to_thread(self._refresh_session_strategies, session)

                        # Advanced-mode: same cadence as raw LTP above, diff-emit only —
                        # this *is* "tick by tick" (evaluated every tick, sent only when
                        # the computed value actually moved, same as _collect_changed_ltp).
                        advanced_changes = self._collect_mtm_updates(session, "advanced", force=False)
                        if advanced_changes:
                            await session.websocket.send_text(json.dumps({
                                "type": "mtm_update",
                                "mode": "advanced",
                                "data": advanced_changes,
                                "server_time": _now_iso(),
                            }))

                        # Regular-mode (the default plan/execution_mode): unconditional
                        # batch every 30s per session, not diff-based — a flat MTM is
                        # still "fresh as of this batch".
                        if now - session.last_normal_batch >= NORMAL_MTM_BATCH_SECONDS:
                            session.last_normal_batch = now
                            normal_changes = self._collect_mtm_updates(session, "regular", force=True)
                            if normal_changes:
                                await session.websocket.send_text(json.dumps({
                                    "type": "mtm_update",
                                    "mode": "regular",
                                    "data": normal_changes,
                                    "server_time": _now_iso(),
                                }))
                    except Exception as exc:
                        log.warning("live quote mtm emit error session=%s user=%s: %s", session.session_id, session.user_id, exc)

                await asyncio.sleep(EMIT_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("live quote emit loop error session=%s: %s", session.session_id, exc)

    def _collect_changed_ltp(self, session: _LiveQuoteSession) -> list[dict]:
        from features.broker_gateway import broker_ticker_manager
        ltp_map = broker_ticker_manager.ltp_map or {}
        changed: list[dict] = []
        for token in session.subscribed_tokens:
            ltp = ltp_map.get(token)
            ltp_float = float(ltp) if ltp else 0.0
            if ltp_float <= 0:
                # No WS tick has ever landed for this token (just subscribed,
                # illiquid, or off-peak) — _rest_refresh_loop's periodic
                # REST fallback below is the only other source; this hot
                # 0.5s loop stays a pure dict lookup either way, no I/O here.
                ltp_float = self._rest_ltp_cache.get(token, 0.0)
            if ltp_float <= 0 or session.last_sent.get(token) == ltp_float:
                continue
            session.last_sent[token] = ltp_float
            changed.append({"token": token, "ltp": ltp_float})
        return changed

    def _collect_changed_underlyings(self, session: _LiveQuoteSession) -> list[dict]:
        """Pure dict lookup, no I/O — same shape as _collect_changed_ltp, just
        reading the hub-wide cache _refresh_underlying_quotes keeps warm."""
        changed: list[dict] = []
        for instrument, quote in self._underlying_quote_cache.items():
            spot_price = float(quote.get("spot_price") or 0)
            if spot_price <= 0 or session.last_sent_underlying.get(instrument) == spot_price:
                continue
            session.last_sent_underlying[instrument] = spot_price
            changed.append({
                "instrument": instrument,
                "spot_price": spot_price,
                "change_pct": quote.get("change_pct"),
                "change_points": quote.get("change_points"),
            })
        return changed

    async def _rest_refresh_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(REST_REFRESH_INTERVAL_SECONDS)
                try:
                    await self._refresh_missing_via_rest()
                except Exception as exc:
                    log.warning("live quote REST refresh error: %s", exc)
                try:
                    await self._refresh_underlying_quotes()
                except Exception as exc:
                    log.warning("live quote underlying refresh error: %s", exc)
        except asyncio.CancelledError:
            pass

    async def _refresh_underlying_quotes(self) -> None:
        """
        Spot price (+ change%) for every instrument currently relevant to
        *any* connected client — not scoped to one session's subscriptions,
        since this is meant to be a shared, always-on feed for system-wide
        monitoring (see _collect_changed_underlyings). Three sources, unioned:

          1. ALWAYS_TRACKED_UNDERLYINGS — kept warm unconditionally so a
             standalone surface with no strategy/token of its own (the
             simulator's bare chart page, say) still gets a live tick
             instead of silently getting nothing until something else in
             the system happens to be watching the same instrument.
          2. Every open paper-trade strategy's instrument, across *every*
             portfolio (covers PortfolioNew.tsx even before any of its legs
             have been subscribed as option tokens).
          3. Whichever instruments the option tokens *currently pooled
             across every session* (the same union _refresh_missing_via_rest
             already builds) belong to — covers real broker positions
             (Positions.tsx) and PaperTradeNew.tsx's legs without needing a
             separate Dhan /positions REST call: the tokens are already
             flowing through this socket, so resolving token → instrument is
             a single indexed Mongo lookup, no extra broker API hit at all.

        Reuses features.execution_socket._fetch_dhan_index_quotes, which
        already prices both indices (IDX_I) and individual F&O stocks
        (NSE_EQ, see its stock-equity branch) in one batched call with its
        own persistent last-good fallback — same function
        /simulator/paper-trade's underlying-quotes endpoint and
        /live-greeks-chain both already rely on, so this stays
        consistent with every other surface instead of inventing a fourth
        way to price an underlying.
        """
        from features.mongo_data import MongoData
        from features.execution_socket import _fetch_dhan_index_quotes

        async with self._lock:
            subscribed_tokens = set()
            for s in self._sessions.values():
                subscribed_tokens |= s.subscribed_tokens

        db = MongoData()
        try:
            instruments = set(ALWAYS_TRACKED_UNDERLYINGS)
            instruments |= {
                str(doc.get("instrument") or "").strip().upper()
                for doc in db._db["simulator_strategy"].find(
                    {"all_exited": {"$ne": True}},
                    {"_id": 0, "instrument": 1},
                )
                if str(doc.get("instrument") or "").strip()
            }
            if subscribed_tokens:
                instruments |= {
                    str(doc.get("instrument") or "").strip().upper()
                    for doc in db._db["active_option_tokens"].find(
                        {"token": {"$in": list(subscribed_tokens)}},
                        {"_id": 0, "instrument": 1},
                    )
                    if str(doc.get("instrument") or "").strip()
                }
            quotes = await asyncio.to_thread(_fetch_dhan_index_quotes, db, instruments)
        finally:
            db.close()
        if quotes:
            self._underlying_quote_cache.update(quotes)

    async def _refresh_missing_via_rest(self) -> None:
        """
        broker_ticker_manager.ltp_map is purely passive — it only ever has a
        value for a token once the broker's own WS feed has sent at least one
        tick for it. A just-subscribed or thinly-traded contract can sit with
        nothing in ltp_map indefinitely, and _collect_changed_ltp on its own
        has no way to notice or do anything about that (by design — it's a
        0.5s hot loop, no I/O allowed in it). This is the active counterpart:
        every few seconds, find subscribed tokens still missing a live tick
        and ask the broker directly via the same get_broker_rest_quotes
        every other simulator surface already uses this session (WS-first,
        REST fallback, proper NSE_FNO/BSE_FNO segment routing).
        """
        from features.broker_gateway import broker_ticker_manager, get_broker_rest_quotes, _active_broker
        if _active_broker() != "dhan":
            return  # Kite path: caller-side kite_quote_map covers this elsewhere, untouched here.

        async with self._lock:
            all_tokens = set()
            for s in self._sessions.values():
                all_tokens |= s.subscribed_tokens
        if not all_tokens:
            return

        ltp_map = broker_ticker_manager.ltp_map or {}
        missing = [t for t in all_tokens if not ltp_map.get(t)]
        if not missing:
            return

        from features.mongo_data import MongoData
        db = MongoData()
        try:
            segment_by_token = {
                str(row.get("token") or row.get("tokens") or "").strip(): str(row.get("ws_segment") or "NSE_FNO").strip().upper()
                for row in db._db["active_option_tokens"].find(
                    {"broker": "dhan", "token": {"$in": missing}},
                    {"_id": 0, "token": 1, "tokens": 1, "ws_segment": 1},
                )
            }
            quotes = await asyncio.to_thread(get_broker_rest_quotes, missing, db._db, segment_by_token)
        finally:
            db.close()

        for token, info in quotes.items():
            ltp = float((info or {}).get("ltp") or 0)
            if ltp > 0:
                self._rest_ltp_cache[token] = ltp

    async def _send_message(self, session: _LiveQuoteSession, message_type: str, data: Any) -> None:
        await session.websocket.send_text(json.dumps({
            "type": message_type,
            "data": data,
            "server_time": _now_iso(),
        }))

    def get_status(self) -> dict:
        return {"connections": len(self._sessions)}


live_quote_hub = _LiveQuoteHub()


@live_quote_socket_router.get("/live-quotes/status")
async def live_quote_status():
    return live_quote_hub.get_status()


@live_quote_socket_router.websocket("/ws/live-quotes")
async def live_quote_socket(websocket: WebSocket):
    session = await live_quote_hub.register(websocket)
    try:
        while True:
            raw_message = await websocket.receive_text()
            await live_quote_hub.handle_client_message(session, raw_message)
    except WebSocketDisconnect:
        pass
    finally:
        await live_quote_hub.unregister(session)
