"""
chain_snapshot.py
------------------
Parquet-backed twin of algo.websocket/historical_data_router.py's
paper-trade backtest-replay chain snapshot endpoints (that file is
untouched — this reads shared/parquet_data/ instead of MongoDB's
option_chain_historical_data). Same response contract: instrument,
expiry, expiries, spot_price, pricing_spot, previous_close, change_pct,
change_points, atm_strike, strike_interval, india_vix, lot_size,
chain: {CE, PE}.

Limitations (minimal engine, by design):
  - india_vix is always 0 — the option_chain_new export this reads from
    doesn't carry a VIX series the way option_chain_index_spot does.
  - lot_size comes from a hardcoded table, not the lot_sizes collection
    (lot sizes rarely change within one dataset's span).
  - "token" isn't in the Parquet schema (option_chain_new has no token
    field), so the synthesized `symbol` string is reused as the token.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from pathlib import Path

import polars as pl

from .day_chain import DayChain
from .engine import FastBacktestEngine

LOT_SIZE_DEFAULTS = {
    "NIFTY": 75, "BANKNIFTY": 15, "FINNIFTY": 40,
    "MIDCPNIFTY": 120, "SENSEX": 10, "BANKEX": 15,
}


def _pivot_ts(chain: DayChain, requested):
    at_or_before = [t for t in chain.timestamps if t <= requested]
    if at_or_before:
        return at_or_before[-1]
    after = [t for t in chain.timestamps if t >= requested]
    return after[0] if after else None


def _safe_float(arr, i: int) -> float:
    v = float(arr[i])
    return v if v == v else 0.0  # NaN guard


def get_historical_chain(parquet_root: str | Path, instrument: str,
                          timestamp: str, expiry: str = "") -> dict:
    normalized = instrument.strip().upper()
    norm_ts = timestamp.strip().replace(" ", "T").rstrip("Z")
    req_date = norm_ts[:10]

    day_path = Path(parquet_root) / normalized / req_date / "data.parquet"
    if not day_path.exists():
        return {"status": "error", "message": f"No historical data for {normalized} on {req_date}."}

    chain = DayChain.load(day_path)
    requested = datetime.fromisoformat(norm_ts)
    pivot_ts = _pivot_ts(chain, requested)
    if pivot_ts is None:
        return {"status": "error", "message": f"No historical data for {normalized} on {req_date}."}

    expiries_sorted = chain.expiries_at(pivot_ts)
    if not expiries_sorted:
        return {"status": "error", "message": f"No option chain rows for {normalized} at {pivot_ts}."}

    req_expiry = (expiry or "").strip()[:10]
    if req_expiry and req_expiry in expiries_sorted:
        live_expiry = req_expiry
    else:
        future = [e for e in expiries_sorted if e >= req_date]
        live_expiry = future[0] if future else expiries_sorted[-1]

    spot_price = chain.spot(pivot_ts) or 0.0

    # Every expiry live at this instant, built in the same pass (DayChain.group()
    # is an O(1) dict hit + numpy slice, no extra Parquet reads) — so switching
    # which expiry the frontend is looking at never needs another HTTP round trip.
    chains_out: dict[str, dict] = {}
    for exp in expiries_sorted:
        built = _build_expiry_chain(chain, pivot_ts, exp, normalized, spot_price)
        if built is not None:
            chains_out[exp] = built

    if live_expiry not in chains_out:
        return {"status": "error", "message": f"No option chain rows for {normalized} {live_expiry} at {pivot_ts}."}

    # previous close: last spot tick of the previous day with a Parquet export
    engine = FastBacktestEngine(parquet_root, normalized)
    all_days = engine.trading_days()
    previous_close = 0.0
    if req_date in all_days:
        idx = all_days.index(req_date)
        if idx > 0:
            prev_path = Path(parquet_root) / normalized / all_days[idx - 1] / "data.parquet"
            prev_chain = DayChain.load(prev_path)
            if prev_chain.timestamps:
                previous_close = prev_chain.spot(prev_chain.timestamps[-1]) or 0.0

    change_pct = round((spot_price - previous_close) / previous_close * 100, 2) if previous_close else 0.0
    change_points = round(spot_price - previous_close, 2) if previous_close else 0.0

    live = chains_out[live_expiry]
    return {
        "status": "success",
        "instrument": normalized,
        "expiry": live_expiry,
        "expiries": expiries_sorted,
        "spot_price": round(spot_price, 2),
        "pricing_spot": round(spot_price, 2),
        "previous_close": round(previous_close, 2),
        "change_pct": change_pct,
        "change_points": change_points,
        "atm_strike": live["atm_strike"],
        "strike_interval": live["strike_interval"],
        "india_vix": 0.0,
        "lot_size": LOT_SIZE_DEFAULTS.get(normalized, 75),
        "chain": live["chain"],
        "chains": chains_out,
        "timestamp": pivot_ts.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def _build_expiry_chain(chain: DayChain, pivot_ts, expiry: str, normalized: str, spot_price: float) -> dict | None:
    ce_group = chain.group(pivot_ts, expiry, "CE")
    pe_group = chain.group(pivot_ts, expiry, "PE")
    if ce_group is None and pe_group is None:
        return None

    chain_out: dict[str, list[dict]] = {"CE": [], "PE": []}
    all_strikes: set[float] = set()
    for opt_type, group in (("CE", ce_group), ("PE", pe_group)):
        if group is None:
            continue
        for i in range(group.strike.size):
            strike = float(group.strike[i])
            all_strikes.add(strike)
            strike_label = int(strike) if strike == int(strike) else strike
            symbol = f"{normalized}{expiry.replace('-', '')}{strike_label}{opt_type}"
            chain_out[opt_type].append({
                "strike": strike_label,
                "ltp": _safe_float(group.premium, i),
                "iv": round(_safe_float(group.iv, i) * 100, 2),
                "delta": _safe_float(group.delta, i),
                "gamma": _safe_float(group.gamma, i),
                "theta": _safe_float(group.theta, i),
                "vega": _safe_float(group.vega, i),
                "oi": int(_safe_float(group.oi, i)),
                "token": symbol,
                "symbol": symbol,
            })
    chain_out["CE"].sort(key=lambda x: float(x["strike"]))
    chain_out["PE"].sort(key=lambda x: float(x["strike"]))

    strikes_sorted = sorted(all_strikes)
    strike_interval = 0.0
    if len(strikes_sorted) >= 2:
        diffs = [strikes_sorted[i + 1] - strikes_sorted[i] for i in range(len(strikes_sorted) - 1)]
        strike_interval = float(Counter(diffs).most_common(1)[0][0])

    atm_strike = 0.0
    if strikes_sorted and spot_price > 0:
        atm_strike = min(strikes_sorted, key=lambda s: abs(s - spot_price))
    elif strikes_sorted:
        atm_strike = strikes_sorted[len(strikes_sorted) // 2]

    return {
        "expiry": expiry,
        "atm_strike": int(atm_strike) if atm_strike == int(atm_strike) else atm_strike,
        "strike_interval": int(strike_interval) if strike_interval == int(strike_interval) else strike_interval,
        "chain": chain_out,
    }


def get_expiry_dates(parquet_root: str | Path, instrument: str) -> dict:
    """
    Every distinct `expiry` date that appears anywhere in this underlying's
    exported Parquet data — i.e. the real calendar dates that are actual
    option expiries, straight from the data, not a "Tuesday = expiry"
    assumption. Used to mark expiry days on the replay date picker.
    """
    normalized = instrument.strip().upper()
    glob_path = str(Path(parquet_root) / normalized / "*" / "data.parquet")
    try:
        expiries = (
            pl.scan_parquet(glob_path)
            .select(pl.col("expiry").cast(pl.Utf8))
            .unique()
            .collect()["expiry"]
            .to_list()
        )
    except Exception:
        expiries = []

    if not expiries:
        return {"status": "error", "message": f"No historical data found for {normalized}."}

    return {"status": "success", "instrument": normalized, "expiry_dates": sorted(expiries)}


def get_trading_days(parquet_root: str | Path, instrument: str) -> dict:
    """Every calendar date this underlying has an exported Parquet file for —
    lets the replay toolbar's day-step buttons jump to the actual next/previous
    trading day instead of a naive calendar +1/-1 (which would land on weekends
    or data-less holidays)."""
    normalized = instrument.strip().upper()
    days = FastBacktestEngine(parquet_root, normalized).trading_days()
    if not days:
        return {"status": "error", "message": f"No historical data found for {normalized}."}
    return {"status": "success", "instrument": normalized, "trading_days": days}


def get_latest_date(parquet_root: str | Path, instrument: str) -> dict:
    normalized = instrument.strip().upper()
    engine = FastBacktestEngine(parquet_root, normalized)
    days = engine.trading_days()
    if not days:
        return {"status": "error", "message": f"No historical data found for {normalized}."}

    latest = days[-1]
    chain = DayChain.load(Path(parquet_root) / normalized / latest / "data.parquet")
    early = [t for t in chain.timestamps if t.strftime("%H:%M") <= "09:35"]
    ts = early[0] if early else (chain.timestamps[0] if chain.timestamps else None)
    if ts is None:
        return {"status": "error", "message": f"No historical data found for {normalized}."}
    return {"status": "success", "date": latest, "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S")}
