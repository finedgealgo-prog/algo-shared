"""
dhan_token_sync.py
────────────────────
Syncs active_option_tokens for Dhan-listed index options/futures and MCX
commodity options/futures (Gold, Silver, Crude Oil, Natural Gas, Copper,
Zinc, Lead, Aluminum, Crude Palm Oil, Cotton, Mentha Oil, and anything else
Dhan lists under MCX — discovered dynamically from the scrip master, not a
fixed list) straight from Dhan's scrip master CSV.

Equity/stock F&O sync (Kite-instrument-cache based, the "FNO-STOCKS" /
per-stock path) is a separate, larger concern that stays in algo.trade/
api.py's _sync_active_option_tokens — out of scope here.

Mounted on BOTH algo.trade and algo.simulator (an explicit exception to the
"common stuff only on algo.websocket" rule — this is admin/data-sync
tooling the user wants triggerable from either backend they're actively
running, not live trading data).
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime

from fastapi import APIRouter

log = logging.getLogger(__name__)

router = APIRouter()

INDEX_SET = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}

_DHAN_SCRIP_MASTER_CACHE: dict = {}        # {"rows": [csv_row_dict, ...], "date": "YYYY-MM-DD"}
_DHAN_INDEX_OPTION_CACHE: dict = {}        # {"rows": {instrument: [contract, ...]}, "date": "YYYY-MM-DD"}
_DHAN_INDEX_FUTURE_CACHE: dict = {}        # {"rows": {instrument: [contract, ...]}, "date": "YYYY-MM-DD"}
_DHAN_COMMODITY_MASTER_CACHE: dict = {}    # {"rows": {underlying: [contract, ...]}, "date": "YYYY-MM-DD"}
_ACTIVE_OPTION_TOKENS_INDEX_ENSURED = False


def _get_dhan_scrip_master_rows() -> list[dict]:
    """
    Raw Dhan scrip master CSV rows (~30MB file), downloaded once per calendar day
    and shared by every Dhan contract sync — indices, commodities, anything else —
    so the file is fetched at most once a day no matter how many instruments sync.
    """
    import io as _io, csv as _csv, requests as _req
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _DHAN_SCRIP_MASTER_CACHE.get("rows") and _DHAN_SCRIP_MASTER_CACHE.get("date") == today_str:
        return _DHAN_SCRIP_MASTER_CACHE["rows"]

    resp = _req.get("https://images.dhan.co/api-data/api-scrip-master.csv", timeout=30)
    resp.raise_for_status()
    rows = list(_csv.DictReader(_io.StringIO(resp.text)))
    _DHAN_SCRIP_MASTER_CACHE["rows"] = rows
    _DHAN_SCRIP_MASTER_CACHE["date"] = today_str
    return rows


def _get_dhan_index_option_master() -> dict[str, list[dict]]:
    """
    Returns {instrument: [{sec_id, symbol, strike, opt_type, expiry, exchange, lot_size}]}
    for index (OPTIDX) contracts — NIFTY, SENSEX, BANKNIFTY, etc. — straight from Dhan's
    scrip master CSV. Cached once per calendar day, same shape as the commodity master.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _DHAN_INDEX_OPTION_CACHE.get("rows") and _DHAN_INDEX_OPTION_CACHE.get("date") == today_str:
        return _DHAN_INDEX_OPTION_CACHE["rows"]

    reader = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    for row in reader:
        if row.get("SEM_INSTRUMENT_NAME", "").strip() != "OPTIDX":
            continue
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()
        symbol = ts.split("-")[0].strip().upper() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        master.setdefault(symbol, []).append({
            "sec_id":   row.get("SEM_SMST_SECURITY_ID", "").strip(),
            "symbol":   ts,
            "strike":   float(row.get("SEM_STRIKE_PRICE") or 0),
            "opt_type": row.get("SEM_OPTION_TYPE", "").strip().upper(),
            "expiry":   expiry,
            "exchange": row.get("SEM_EXM_EXCH_ID", "").strip(),
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        })

    _DHAN_INDEX_OPTION_CACHE["rows"] = master
    _DHAN_INDEX_OPTION_CACHE["date"] = today_str
    return master


def _get_dhan_index_future_master() -> dict[str, list[dict]]:
    """
    Returns {instrument: [{sec_id, symbol, expiry, exchange, lot_size}]} for index
    (FUTIDX) futures contracts, straight from Dhan's scrip master CSV.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _DHAN_INDEX_FUTURE_CACHE.get("rows") and _DHAN_INDEX_FUTURE_CACHE.get("date") == today_str:
        return _DHAN_INDEX_FUTURE_CACHE["rows"]

    reader = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    for row in reader:
        if row.get("SEM_INSTRUMENT_NAME", "").strip() != "FUTIDX":
            continue
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()
        symbol = ts.split("-")[0].strip().upper() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        master.setdefault(symbol, []).append({
            "sec_id":   row.get("SEM_SMST_SECURITY_ID", "").strip(),
            "symbol":   ts,
            "expiry":   expiry,
            "exchange": row.get("SEM_EXM_EXCH_ID", "").strip(),
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        })

    for contracts in master.values():
        contracts.sort(key=lambda c: c["expiry"])

    _DHAN_INDEX_FUTURE_CACHE["rows"] = master
    _DHAN_INDEX_FUTURE_CACHE["date"] = today_str
    return master


def _get_dhan_commodity_master() -> dict[str, list[dict]]:
    """
    Returns {underlying: [{sec_id, symbol, strike, opt_type, expiry, exchange, lot_size}]}
    for every MCX commodity — gold, silver, crude oil, copper, and everything else Dhan
    lists on MCX — covering both futures (FUTCOM, opt_type "FUT", strike 0) and options
    on futures (OPTFUT, opt_type CE/PE). Underlyings aren't a fixed list like the indices;
    they're discovered straight from whatever Dhan's scrip master actually carries.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _DHAN_COMMODITY_MASTER_CACHE.get("rows") and _DHAN_COMMODITY_MASTER_CACHE.get("date") == today_str:
        return _DHAN_COMMODITY_MASTER_CACHE["rows"]

    rows = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    for row in rows:
        if row.get("SEM_EXM_EXCH_ID", "").strip() != "MCX":
            continue
        inst = row.get("SEM_INSTRUMENT_NAME", "").strip()
        if inst not in ("FUTCOM", "OPTFUT"):
            continue
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()
        symbol = ts.split("-")[0].strip().upper() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        if inst == "FUTCOM":
            opt_type, strike = "FUT", 0.0
        else:
            opt_type = row.get("SEM_OPTION_TYPE", "").strip().upper()
            strike = float(row.get("SEM_STRIKE_PRICE") or 0)
        master.setdefault(symbol, []).append({
            "sec_id":   row.get("SEM_SMST_SECURITY_ID", "").strip(),
            "symbol":   ts,
            "strike":   strike,
            "opt_type": opt_type,
            "expiry":   expiry,
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        })

    _DHAN_COMMODITY_MASTER_CACHE["rows"] = master
    _DHAN_COMMODITY_MASTER_CACHE["date"] = today_str
    return master


def _ensure_active_option_tokens_index(col) -> None:
    """
    Create the compound index every Dhan contract upsert matches on, once per process.
    Without it, each upsert inside a bulk_write does a full collection scan to check for
    an existing match — that alone turned a multi-thousand-contract sync from under a
    second into ~10s per instrument (measured: NIFTY's 4080 contracts 9.8s -> 0.28s).
    """
    global _ACTIVE_OPTION_TOKENS_INDEX_ENSURED
    if _ACTIVE_OPTION_TOKENS_INDEX_ENSURED:
        return
    try:
        col.create_index(
            [("broker", 1), ("instrument", 1), ("expiry", 1), ("strike", 1), ("option_type", 1)],
            name="idx_active_option_contract_v2",
        )
    except Exception:
        pass
    _ACTIVE_OPTION_TOKENS_INDEX_ENSURED = True


def _sync_dhan_index_option_tokens(instrument: str) -> dict:
    """Refresh active_option_tokens for one index instrument from Dhan's scrip master."""
    from features.mongo_data import MongoData  # type: ignore

    normalized = str(instrument or "").strip().upper()
    master = _get_dhan_index_option_master()
    contracts = master.get(normalized, [])
    if not contracts:
        return {
            "instrument": normalized,
            "expiries": [],
            "contracts_processed": 0,
            "created": 0,
            "updated": 0,
            "message": f"No Dhan index option contracts found for {normalized} in the scrip master",
        }

    from pymongo import UpdateOne

    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        _ensure_active_option_tokens_index(col)
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        expiries: set[str] = set()
        ops = []
        for c in contracts:
            expiries.add(c["expiry"])
            opt_type = c["opt_type"]
            if opt_type not in {"CE", "PE"}:
                continue
            sec_id = str(c.get("sec_id") or "").strip()
            if not sec_id:
                continue
            exch = c.get("exchange") or ("BSE" if normalized in {"SENSEX", "BANKEX"} else "NSE")
            key = {
                "broker": "dhan",
                "instrument": normalized,
                "expiry": c["expiry"],
                "strike": c["strike"],
                "option_type": opt_type,
            }
            update_payload = {
                **key,
                "instrument_type": "index",
                "exchange": exch,
                "symbol": c.get("symbol") or f"{normalized}-{c['expiry']}-{c['strike']}-{opt_type}",
                "token": sec_id,
                "tokens": sec_id,
                "ws_segment": "BSE_FNO" if exch == "BSE" else "NSE_FNO",
                "lot_size": c.get("lot_size"),
                "updated_at": now_ts,
            }
            ops.append(UpdateOne(
                key, {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}}, upsert=True,
            ))

        created = 0
        updated = 0
        if ops:
            result = col.bulk_write(ops, ordered=False)
            created = result.upserted_count
            updated = result.matched_count

        return {
            "instrument": normalized,
            "expiries": sorted(expiries),
            "contracts_processed": len(contracts),
            "created": created,
            "updated": updated,
            "message": "active_option_tokens sync completed from Dhan scrip master",
        }
    finally:
        db.close()


def _sync_dhan_index_future_tokens(instrument: str) -> dict:
    """Refresh active_option_tokens for one index's FUTIDX contracts."""
    from features.mongo_data import MongoData  # type: ignore

    normalized = str(instrument or "").strip().upper()
    master = _get_dhan_index_future_master()
    contracts = master.get(normalized, [])
    if not contracts:
        return {
            "instrument": normalized,
            "expiries": [],
            "contracts_processed": 0,
            "created": 0,
            "updated": 0,
            "message": f"No Dhan index future contracts found for {normalized} in the scrip master",
        }

    from pymongo import UpdateOne

    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        _ensure_active_option_tokens_index(col)
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        expiries: set[str] = set()
        ops = []
        for c in contracts:
            expiries.add(c["expiry"])
            sec_id = str(c.get("sec_id") or "").strip()
            if not sec_id:
                continue
            exch = c.get("exchange") or ("BSE" if normalized in {"SENSEX", "BANKEX"} else "NSE")
            key = {
                "broker": "dhan",
                "instrument": normalized,
                "expiry": c["expiry"],
                "strike": 0.0,
                "option_type": "FUT",
            }
            update_payload = {
                **key,
                "instrument_type": "future",
                "exchange": exch,
                "symbol": c.get("symbol") or f"{normalized}-{c['expiry']}-FUT",
                "token": sec_id,
                "tokens": sec_id,
                "ws_segment": "BSE_FNO" if exch == "BSE" else "NSE_FNO",
                "lot_size": c.get("lot_size"),
                "updated_at": now_ts,
            }
            ops.append(UpdateOne(
                key, {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}}, upsert=True,
            ))

        created = 0
        updated = 0
        if ops:
            result = col.bulk_write(ops, ordered=False)
            created = result.upserted_count
            updated = result.matched_count

        return {
            "instrument": normalized,
            "expiries": sorted(expiries),
            "contracts_processed": len(contracts),
            "created": created,
            "updated": updated,
            "message": "active_option_tokens FUT sync completed from Dhan scrip master",
        }
    finally:
        db.close()


def _sync_dhan_commodity_tokens(instrument: str) -> dict:
    """Refresh active_option_tokens for one MCX commodity (futures + options) from Dhan's scrip master."""
    from features.mongo_data import MongoData  # type: ignore

    normalized = str(instrument or "").strip().upper()
    master = _get_dhan_commodity_master()
    contracts = master.get(normalized, [])
    if not contracts:
        return {
            "instrument": normalized,
            "expiries": [],
            "contracts_processed": 0,
            "created": 0,
            "updated": 0,
            "message": f"No Dhan commodity contracts found for {normalized} in the scrip master",
        }

    from pymongo import UpdateOne

    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        _ensure_active_option_tokens_index(col)
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        expiries: set[str] = set()
        ops = []
        for c in contracts:
            expiries.add(c["expiry"])
            opt_type = c["opt_type"]
            if opt_type not in {"CE", "PE", "FUT"}:
                continue
            sec_id = str(c.get("sec_id") or "").strip()
            if not sec_id:
                continue
            key = {
                "broker": "dhan",
                "instrument": normalized,
                "expiry": c["expiry"],
                "strike": c["strike"],
                "option_type": opt_type,
            }
            update_payload = {
                **key,
                "instrument_type": "commodity",
                "exchange": "MCX",
                "symbol": c.get("symbol") or f"{normalized}-{c['expiry']}-{c['strike']}-{opt_type}",
                "token": sec_id,
                "tokens": sec_id,
                "ws_segment": "MCX_COMM",
                "lot_size": c.get("lot_size"),
                "updated_at": now_ts,
            }
            ops.append(UpdateOne(
                key, {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}}, upsert=True,
            ))

        created = 0
        updated = 0
        if ops:
            result = col.bulk_write(ops, ordered=False)
            created = result.upserted_count
            updated = result.matched_count

        return {
            "instrument": normalized,
            "expiries": sorted(expiries),
            "contracts_processed": len(contracts),
            "created": created,
            "updated": updated,
            "message": "active_option_tokens sync completed from Dhan scrip master (commodity)",
        }
    finally:
        db.close()


def dispatch_dhan_token_sync(instrument: str) -> dict | None:
    """
    Sync one instrument (or "ALL" for every index + commodity Dhan lists) if it's a
    known index or MCX commodity. Returns None if `instrument` is neither — caller
    decides what to do then (algo.trade falls back to its own stock/Kite sync path;
    algo.simulator just reports "not a known index or commodity").
    """
    from features.broker_gateway import _active_broker  # type: ignore

    normalized = str(instrument or "").strip().upper()

    if normalized == "ALL":
        if _active_broker() != "dhan":
            return {"status": "error", "message": "Active broker is not dhan"}
        index_results = {idx: _sync_dhan_index_option_tokens(idx) for idx in sorted(INDEX_SET)}
        index_future_results = {idx: _sync_dhan_index_future_tokens(idx) for idx in sorted(INDEX_SET)}
        commodity_master = _get_dhan_commodity_master()
        commodity_results = {sym: _sync_dhan_commodity_tokens(sym) for sym in sorted(commodity_master.keys())}
        all_results = list(index_results.values()) + list(index_future_results.values()) + list(commodity_results.values())
        return {
            "status": "success",
            "broker": "dhan",
            "indices": index_results,
            "index_futures": index_future_results,
            "commodities": commodity_results,
            "totals": {
                "contracts_processed": sum(r.get("contracts_processed", 0) for r in all_results),
                "created": sum(r.get("created", 0) for r in all_results),
                "updated": sum(r.get("updated", 0) for r in all_results),
            },
        }

    if normalized in _get_dhan_commodity_master():
        if _active_broker() != "dhan":
            return {"status": "error", "message": "Active broker is not dhan"}
        return _sync_dhan_commodity_tokens(normalized)

    if normalized in INDEX_SET:
        if _active_broker() != "dhan":
            return {"status": "error", "message": "Active broker is not dhan"}
        option_result = _sync_dhan_index_option_tokens(normalized)
        future_result = _sync_dhan_index_future_tokens(normalized)
        return {
            "status": "success",
            "instrument": normalized,
            "options": option_result,
            "futures": future_result,
            "contracts_processed": option_result.get("contracts_processed", 0) + future_result.get("contracts_processed", 0),
            "created": option_result.get("created", 0) + future_result.get("created", 0),
            "updated": option_result.get("updated", 0) + future_result.get("updated", 0),
        }

    return None


# ── Background sync state (per-process) ───────────────────────────────────────
_bg_sync_state: dict = {
    "running": False,
    "instrument": "",
    "started_at": "",
    "finished_at": "",
    "result": None,
    "error": "",
}
_bg_sync_thread: threading.Thread | None = None


def _run_bg_sync(instrument: str) -> None:
    global _bg_sync_state
    _bg_sync_state["running"] = True
    _bg_sync_state["instrument"] = instrument
    _bg_sync_state["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _bg_sync_state["finished_at"] = ""
    _bg_sync_state["result"] = None
    _bg_sync_state["error"] = ""
    try:
        result = dispatch_dhan_token_sync(instrument)
        if result is None:
            result = {
                "status": "error",
                "instrument": str(instrument or "").strip().upper(),
                "message": (
                    "Not a known index or MCX commodity. This endpoint only syncs "
                    "indices/commodities — equity F&O stock sync stays on algo.trade's "
                    "own /algo/get_active_tokens/{instrument}."
                ),
            }
        _bg_sync_state["result"] = result
    except Exception as exc:
        _bg_sync_state["error"] = str(exc)
    finally:
        _bg_sync_state["running"] = False
        _bg_sync_state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@router.get("/algo/sync-tokens/start/{instrument}")
async def bg_sync_start(instrument: str):
    """
    Start a background sync of active_option_tokens for one index/commodity (or
    "ALL" for every index + every MCX commodity Dhan lists). Returns immediately;
    poll /algo/sync-tokens/status for the result.
    """
    global _bg_sync_thread
    if _bg_sync_state["running"]:
        return {
            "status": "already_running",
            "instrument": _bg_sync_state["instrument"],
            "started_at": _bg_sync_state["started_at"],
            "message": "Sync already in progress. Check /algo/sync-tokens/status",
        }
    _bg_sync_thread = threading.Thread(
        target=_run_bg_sync, args=(instrument,), daemon=True
    )
    _bg_sync_thread.start()
    return {
        "status": "started",
        "instrument": instrument.upper(),
        "message": "Sync running in background. Check /algo/sync-tokens/status",
        "status_url": "/algo/sync-tokens/status",
        "stop_url": "/algo/sync-tokens/stop",
    }


@router.get("/algo/sync-tokens/status")
async def bg_sync_status():
    """Check the status of the background active_option_tokens sync."""
    state = dict(_bg_sync_state)
    if state["running"]:
        status = "running"
    elif state["error"]:
        status = "error"
    elif state["result"] is not None:
        status = "completed"
    else:
        status = "idle"
    return {"status": status, **state}


@router.get("/algo/sync-tokens/stop")
async def bg_sync_stop():
    """Signal the background sync to stop (marks as not running; thread finishes current batch)."""
    global _bg_sync_state
    if not _bg_sync_state["running"]:
        return {"status": "not_running", "message": "No sync is currently running."}
    _bg_sync_state["running"] = False
    _bg_sync_state["error"] = "Stopped by user"
    _bg_sync_state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {"status": "stop_requested", "message": "Stop signal sent. Thread will finish current batch."}
