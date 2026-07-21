"""
iron_condor_v2_backtest.py
───────────────────────────
Standalone backtest engine for the "V2 Condor" strategy: a next-week-expiry
delta-based Iron Condor with an IV-Rank/ADX entry filter, skew-adjusted put
strike, capital-based position sizing, hybrid roll-adjustment logic (roll the
challenged side out, roll the safe side closer by half the distance), a two
step profit ladder, and hard delta/loss circuit breakers.

Data source: option_chain_historical_data (per-minute close/delta/iv per
strike) and option_chain_index_spot (per-minute spot) — both already carry
live Greeks computed upstream, so this module never re-derives Black-Scholes
itself; it only consumes `delta` / `iv` fields as stored.

Known data-driven simplifications (see run_backtest docstring):
  - IV Rank is seeded with a weekly-sampled ATM-IV lookback (default 90
    calendar days) fetched once before start_date, same idea as the ADX
    warm-up below. If Mongo has no data that far back (e.g. backtest starts
    right at the beginning of the collection's history), the lookback comes
    back empty and the filter is simply not applied until at least
    `min_iv_samples` real samples have accumulated — a handful of readings
    can't tell you whether IV is "high" or "low", so we don't pretend they
    can.
  - ADX(14) is seeded with a 30-calendar-day spot lookback fetched once
    before start_date so it is valid from week 1.
  - The "event week" skip (RBI/Budget/Fed) is not implemented — there is no
    economic calendar collection in Mongo.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any, Optional

from features.mongo_data import MongoData
from features.delta_selector import select_closest_delta

OC_COL = "option_chain_historical_data"
SPOT_COL = "option_chain_index_spot"

CHAIN_PROJECTION = {
    "_id": 0, "timestamp": 1, "strike": 1, "type": 1, "close": 1,
    "delta": 1, "iv": 1,
}
SPOT_PROJECTION = {"_id": 0, "timestamp": 1, "spot_price": 1}


# ── small pure helpers ──────────────────────────────────────────────────────

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _parse_date(s: str) -> date:
    return datetime.strptime(s[:10], "%Y-%m-%d").date()


def _add_days(d: date, n: int) -> date:
    return d + timedelta(days=n)


def _find_row(rows: list[dict], strike: float, otype: str) -> Optional[dict]:
    for r in rows:
        if r.get("type") == otype and abs(_safe_float(r.get("strike")) - strike) < 0.01:
            return r
    return None


def _nearest_strike_row(rows: list[dict], otype: str, target_strike: float) -> Optional[dict]:
    candidates = [r for r in rows if r.get("type") == otype]
    if not candidates:
        return None
    return min(candidates, key=lambda r: abs(_safe_float(r.get("strike")) - target_strike))


def _nearest_atm_row(rows: list[dict], otype: str, spot: float) -> Optional[dict]:
    """Nearest-to-spot row with a usable quote — excludes illiquid/stale ticks
    (iv or close <= 0) so a single bad tick can't corrupt the IV history."""
    candidates = [
        r for r in rows
        if r.get("type") == otype and _safe_float(r.get("iv")) > 0 and _safe_float(r.get("close")) > 0
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda r: abs(_safe_float(r.get("strike")) - spot))


# ── ADX(14) from daily spot OHLC (Wilder's smoothing, pure python) ─────────

def _daily_ohlc_from_spot_ticks(ticks: list[tuple[str, float]]) -> dict[str, dict]:
    """ticks: list of (timestamp_str, spot_price) → {date: {high, low, close}}"""
    by_date: dict[str, list[float]] = {}
    for ts, price in ticks:
        d = ts[:10]
        by_date.setdefault(d, []).append(price)
    out = {}
    for d, prices in by_date.items():
        out[d] = {"high": max(prices), "low": min(prices), "close": prices[-1]}
    return out


def _compute_adx_series(daily_bars: dict[str, dict], period: int = 14) -> dict[str, float]:
    """Wilder ADX(period) over a chronologically sorted daily bar dict."""
    dates = sorted(daily_bars.keys())
    if len(dates) < period + 1:
        return {}

    trs, plus_dms, minus_dms = [], [], []
    for i in range(1, len(dates)):
        prev = daily_bars[dates[i - 1]]
        cur = daily_bars[dates[i]]
        tr = max(
            cur["high"] - cur["low"],
            abs(cur["high"] - prev["close"]),
            abs(cur["low"] - prev["close"]),
        )
        up_move = cur["high"] - prev["high"]
        down_move = prev["low"] - cur["low"]
        plus_dm = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm = down_move if (down_move > up_move and down_move > 0) else 0.0
        trs.append(tr)
        plus_dms.append(plus_dm)
        minus_dms.append(minus_dm)

    if len(trs) < period:
        return {}

    smoothed_tr = sum(trs[:period])
    smoothed_pdm = sum(plus_dms[:period])
    smoothed_mdm = sum(minus_dms[:period])

    adx_by_date: dict[str, float] = {}
    dx_values: list[float] = []

    def _dx(tr_s, pdm_s, mdm_s) -> float:
        if tr_s <= 0:
            return 0.0
        pdi = 100.0 * pdm_s / tr_s
        mdi = 100.0 * mdm_s / tr_s
        denom = pdi + mdi
        return 100.0 * abs(pdi - mdi) / denom if denom > 0 else 0.0

    dx_values.append(_dx(smoothed_tr, smoothed_pdm, smoothed_mdm))
    idx_date = dates[period]  # dates[0] has no TR; trs[i] corresponds to dates[i+1]

    for i in range(period, len(trs)):
        smoothed_tr = smoothed_tr - (smoothed_tr / period) + trs[i]
        smoothed_pdm = smoothed_pdm - (smoothed_pdm / period) + plus_dms[i]
        smoothed_mdm = smoothed_mdm - (smoothed_mdm / period) + minus_dms[i]
        dx_values.append(_dx(smoothed_tr, smoothed_pdm, smoothed_mdm))
        idx_date = dates[i + 1]
        if len(dx_values) >= period:
            adx_by_date[idx_date] = sum(dx_values[-period:]) / period

    return adx_by_date


# ── main engine ─────────────────────────────────────────────────────────────

class IronCondorV2Backtest:
    def __init__(
        self,
        underlying: str = "NIFTY",
        capital: float = 1_000_000.0,
        risk_pct: float = 2.0,
        entry_time: str = "09:30:00",
        entry_weekday: int = 0,          # Monday
        short_delta_ce: float = 16.0,
        short_delta_pe: float = 13.0,     # skew-adjusted, slightly tighter than CE
        hedge_delta: float = 5.0,
        adjust_delta_pct: float = 30.0,
        emergency_delta_pct: float = 45.0,
        max_adjustments: int = 2,
        target_partial_pct: float = 25.0,  # % of initial credit -> close 50% of lots
        target_full_pct: float = 50.0,     # % of initial credit -> close rest
        max_loss_multiple: float = 1.5,    # x initial credit -> full exit
        iv_rank_min: float = 30.0,
        adx_max: float = 25.0,
        adx_half_size: float = 20.0,
        min_iv_samples: int = 8,          # need this many IV readings before iv-rank filter kicks in
        time_exit_dte: int = 0,           # 0 = exit on expiry day itself
        time_exit_time: str = "15:15:00",
    ):
        self.underlying = underlying.upper()
        self.capital = capital
        self.risk_pct = risk_pct
        self.entry_time = entry_time
        self.entry_weekday = entry_weekday
        self.short_delta_ce = short_delta_ce
        self.short_delta_pe = short_delta_pe
        self.hedge_delta = hedge_delta
        self.adjust_delta_pct = adjust_delta_pct
        self.emergency_delta_pct = emergency_delta_pct
        self.max_adjustments = max_adjustments
        self.target_partial_pct = target_partial_pct
        self.target_full_pct = target_full_pct
        self.max_loss_multiple = max_loss_multiple
        self.iv_rank_min = iv_rank_min
        self.adx_max = adx_max
        self.adx_half_size = adx_half_size
        self.min_iv_samples = min_iv_samples
        self.time_exit_dte = time_exit_dte
        self.time_exit_time = time_exit_time

        self.db = MongoData()
        self._daily_bars: dict[str, dict] = {}   # date -> {high, low, close} spot
        self._iv_history: list[float] = []        # chronological ATM IV samples

    # ── data loading ────────────────────────────────────────────────────

    def _prime_adx_lookback(self, start_date: str) -> None:
        lookback_start = _add_days(_parse_date(start_date), -30).isoformat()
        ticks = list(self.db._db[SPOT_COL].find(
            {"underlying": self.underlying,
             "timestamp": {"$gte": f"{lookback_start}T00:00:00", "$lt": f"{start_date}T00:00:00"}},
            SPOT_PROJECTION,
        ))
        pairs = [(t["timestamp"], _safe_float(t.get("spot_price"))) for t in ticks if t.get("spot_price")]
        self._daily_bars.update(_daily_ohlc_from_spot_ticks(pairs))

    def _sample_atm_iv(self, monday: str) -> Optional[float]:
        """Lightweight single-day ATM-IV read (entry-time snapshot only, no
        full-week load) — used only to seed the IV-rank lookback history."""
        expiries = self._next_expiries(monday)
        if len(expiries) < 2:
            return None
        expiry = expiries[1]
        rows = list(self.db._db[OC_COL].find(
            {"underlying": self.underlying, "expiry": expiry,
             "timestamp": {"$gte": f"{monday}T00:00:00", "$lte": f"{monday}T23:59:59"}},
            CHAIN_PROJECTION,
        ))
        if not rows:
            return None
        by_ts: dict[str, list[dict]] = {}
        for r in rows:
            by_ts.setdefault(r["timestamp"], []).append(r)
        entry_ts = next((t for t in sorted(by_ts.keys()) if t[11:19] >= self.entry_time), None)
        if entry_ts is None:
            return None
        entry_rows = by_ts[entry_ts]
        spot_doc = self.db._db[SPOT_COL].find_one(
            {"underlying": self.underlying, "timestamp": {"$gte": entry_ts}},
            SPOT_PROJECTION, sort=[("timestamp", 1)],
        )
        spot = _safe_float(spot_doc.get("spot_price")) if spot_doc else 0.0
        if spot <= 0:
            return None
        atm_ce = _nearest_atm_row(entry_rows, "CE", spot)
        atm_pe = _nearest_atm_row(entry_rows, "PE", spot)
        if not atm_ce or not atm_pe:
            return None
        return (_safe_float(atm_ce.get("iv")) + _safe_float(atm_pe.get("iv"))) / 2.0

    def _prime_iv_lookback(self, start_date: str, lookback_days: int = 90) -> None:
        cur = _add_days(_parse_date(start_date), -lookback_days)
        end = _add_days(_parse_date(start_date), -1)
        while cur <= end:
            if cur.weekday() == self.entry_weekday:
                iv = self._sample_atm_iv(cur.isoformat())
                if iv is not None and iv > 0:
                    self._iv_history.append(iv)
            cur = _add_days(cur, 1)

    def _load_week(self, week_start: str, week_end: str, expiry: str) -> tuple[dict, list[str], dict, list[str]]:
        chain_rows = list(self.db._db[OC_COL].find(
            {"underlying": self.underlying, "expiry": expiry,
             "timestamp": {"$gte": f"{week_start}T00:00:00", "$lte": f"{week_end}T23:59:59"}},
            CHAIN_PROJECTION,
        ))
        chain_by_ts: dict[str, list[dict]] = {}
        for r in chain_rows:
            chain_by_ts.setdefault(r["timestamp"], []).append(r)
        chain_ts_sorted = sorted(chain_by_ts.keys())

        spot_rows = list(self.db._db[SPOT_COL].find(
            {"underlying": self.underlying,
             "timestamp": {"$gte": f"{week_start}T00:00:00", "$lte": f"{week_end}T23:59:59"}},
            SPOT_PROJECTION,
        ))
        spot_by_ts = {r["timestamp"]: _safe_float(r.get("spot_price")) for r in spot_rows if r.get("spot_price")}
        spot_ts_sorted = sorted(spot_by_ts.keys())

        # accumulate daily OHLC bars for ADX as we go (expanding window)
        pairs = [(ts, p) for ts, p in spot_by_ts.items()]
        self._daily_bars.update(_daily_ohlc_from_spot_ticks(pairs))

        return chain_by_ts, chain_ts_sorted, spot_by_ts, spot_ts_sorted

    def _next_expiries(self, monday: str) -> list[str]:
        expiries = self.db._db[OC_COL].distinct(
            "expiry",
            {"underlying": self.underlying,
             "timestamp": {"$gte": f"{monday}T00:00:00", "$lte": f"{monday}T23:59:59"}},
        )
        return sorted(e for e in expiries if e >= monday)

    # ── entry filters ───────────────────────────────────────────────────

    def _current_adx(self, before_date: str) -> Optional[float]:
        series = _compute_adx_series(self._daily_bars, period=14)
        prior_dates = sorted(d for d in series if d < before_date)
        if not prior_dates:
            return None
        return series[prior_dates[-1]]

    def _iv_rank(self, current_iv: float) -> Optional[float]:
        if len(self._iv_history) < self.min_iv_samples:
            return None
        lo, hi = min(self._iv_history), max(self._iv_history)
        if hi <= lo:
            return 50.0
        return 100.0 * (current_iv - lo) / (hi - lo)

    # ── strike selection ────────────────────────────────────────────────

    def _select_legs(self, rows: list[dict]) -> Optional[dict]:
        ce_rows = [r for r in rows if r.get("type") == "CE"]
        pe_rows = [r for r in rows if r.get("type") == "PE"]
        short_ce = select_closest_delta(ce_rows, self.short_delta_ce, "CE", leg_id="short_ce")
        hedge_ce = select_closest_delta(ce_rows, self.hedge_delta, "CE", leg_id="hedge_ce")
        short_pe = select_closest_delta(pe_rows, self.short_delta_pe, "PE", leg_id="short_pe")
        hedge_pe = select_closest_delta(pe_rows, self.hedge_delta, "PE", leg_id="hedge_pe")
        if not all([short_ce, hedge_ce, short_pe, hedge_pe]):
            return None
        return {"short_ce": short_ce, "hedge_ce": hedge_ce, "short_pe": short_pe, "hedge_pe": hedge_pe}

    @staticmethod
    def _leg_credit(short_row: dict, hedge_row: dict) -> float:
        return _safe_float(short_row.get("close")) - _safe_float(hedge_row.get("close"))

    # ── one trade simulation ────────────────────────────────────────────

    def _simulate_trade(self, monday: str, expiry: str, capital_risk_amount: float,
                         adx: Optional[float]) -> tuple[Optional[dict], Optional[float]]:
        """Returns (trade_or_None, atm_iv_sample_or_None). The IV sample is
        returned even when the trade itself is skipped/invalid so `run()` can
        still feed the IV-rank history from every week that had usable data."""
        chain_by_ts, chain_ts, spot_by_ts, spot_ts = self._load_week(monday, expiry, expiry)
        if not chain_ts or not spot_ts:
            return None, None

        entry_ts = next((t for t in chain_ts if t[:10] == monday and t[11:19] >= self.entry_time), None)
        if entry_ts is None:
            return None, None
        entry_spot_ts = next((t for t in spot_ts if t >= entry_ts), spot_ts[-1] if spot_ts else None)
        spot = spot_by_ts.get(entry_spot_ts, 0.0)
        if spot <= 0:
            return None, None

        entry_rows = chain_by_ts[entry_ts]
        legs = self._select_legs(entry_rows)
        if legs is None:
            return None, None

        # ATM IV sample for iv-rank history (nearest CE/PE to spot)
        atm_ce = _nearest_atm_row(entry_rows, "CE", spot)
        atm_pe = _nearest_atm_row(entry_rows, "PE", spot)
        atm_iv = (_safe_float(atm_ce.get("iv")) + _safe_float(atm_pe.get("iv"))) / 2.0 if atm_ce and atm_pe else 0.0
        atm_iv_sample = atm_iv if atm_iv > 0 else None

        credit = self._leg_credit(legs["short_ce"], legs["hedge_ce"]) + self._leg_credit(legs["short_pe"], legs["hedge_pe"])
        if credit <= 0:
            return None, atm_iv_sample

        width_ce = abs(_safe_float(legs["hedge_ce"]["strike"]) - _safe_float(legs["short_ce"]["strike"]))
        width_pe = abs(_safe_float(legs["short_pe"]["strike"]) - _safe_float(legs["hedge_pe"]["strike"]))
        max_loss_points = max(width_ce, width_pe) - credit
        lot_size = self.db.get_lot_size(monday, self.underlying)

        max_loss_rupees_per_lot = max(max_loss_points, 1.0) * lot_size
        lots = max(1, math.floor(capital_risk_amount / max_loss_rupees_per_lot)) if max_loss_rupees_per_lot > 0 else 1

        open_lots = lots
        cash_flow = credit * lot_size * lots  # premium received at entry
        adjustments_used = 0
        partial_done = False
        exit_reason = None
        exit_ts = entry_ts
        events: list[dict] = []

        remaining_ts = [t for t in chain_ts if t >= entry_ts]
        for ts in remaining_ts:
            rows = chain_by_ts.get(ts)
            if not rows:
                continue
            cur_short_ce = _find_row(rows, legs["short_ce"]["strike"], "CE")
            cur_hedge_ce = _find_row(rows, legs["hedge_ce"]["strike"], "CE")
            cur_short_pe = _find_row(rows, legs["short_pe"]["strike"], "PE")
            cur_hedge_pe = _find_row(rows, legs["hedge_pe"]["strike"], "PE")
            if not all([cur_short_ce, cur_hedge_ce, cur_short_pe, cur_hedge_pe]):
                continue

            close_cost_points = self._leg_credit(cur_short_ce, cur_hedge_ce) + self._leg_credit(cur_short_pe, cur_hedge_pe)
            mtm_pnl_rupees = cash_flow - close_cost_points * lot_size * open_lots
            mtm_pnl_points_per_lot = mtm_pnl_rupees / (lot_size * open_lots) if open_lots else 0.0

            cur_date = ts[:10]
            cur_time = ts[11:19]
            days_to_expiry = (_parse_date(expiry) - _parse_date(cur_date)).days

            # 1) time exit
            if days_to_expiry <= self.time_exit_dte and cur_time >= self.time_exit_time:
                cash_flow -= close_cost_points * lot_size * open_lots
                open_lots = 0
                exit_reason = "time_exit"
                exit_ts = ts
                break

            # 2) profit ladder
            if not partial_done and mtm_pnl_points_per_lot >= (self.target_partial_pct / 100.0) * credit:
                close_lots = open_lots // 2 or (1 if open_lots > 1 else 0)
                if close_lots > 0:
                    cash_flow -= close_cost_points * lot_size * close_lots
                    open_lots -= close_lots
                    partial_done = True
                    events.append({"ts": ts, "action": "partial_target", "lots_closed": close_lots})

            if mtm_pnl_points_per_lot >= (self.target_full_pct / 100.0) * credit:
                cash_flow -= close_cost_points * lot_size * open_lots
                open_lots = 0
                exit_reason = "target_full"
                exit_ts = ts
                break

            # 3) emergency delta exit
            ce_delta = abs(_safe_float(cur_short_ce.get("delta")))
            pe_delta = abs(_safe_float(cur_short_pe.get("delta")))
            if ce_delta * 100 >= self.emergency_delta_pct or pe_delta * 100 >= self.emergency_delta_pct:
                cash_flow -= close_cost_points * lot_size * open_lots
                open_lots = 0
                exit_reason = "emergency_delta"
                exit_ts = ts
                break

            # 4) max loss exit
            if mtm_pnl_points_per_lot <= -self.max_loss_multiple * credit:
                cash_flow -= close_cost_points * lot_size * open_lots
                open_lots = 0
                exit_reason = "max_loss"
                exit_ts = ts
                break

            # 5) adjustment trigger
            breached_ce = ce_delta * 100 >= self.adjust_delta_pct
            breached_pe = pe_delta * 100 >= self.adjust_delta_pct
            if breached_ce or breached_pe:
                if adjustments_used >= self.max_adjustments:
                    cash_flow -= close_cost_points * lot_size * open_lots
                    open_lots = 0
                    exit_reason = "breach_after_max_adjustments"
                    exit_ts = ts
                    break

                challenged_type = "CE" if breached_ce else "PE"
                safe_type = "PE" if breached_ce else "CE"
                challenged_target_delta = self.short_delta_ce if challenged_type == "CE" else self.short_delta_pe
                safe_target_delta = self.short_delta_pe if safe_type == "PE" else self.short_delta_ce

                cur_challenged_short = cur_short_ce if challenged_type == "CE" else cur_short_pe
                cur_challenged_hedge = cur_hedge_ce if challenged_type == "CE" else cur_hedge_pe
                cur_safe_short = cur_short_pe if challenged_type == "CE" else cur_short_ce
                cur_safe_hedge = cur_hedge_pe if challenged_type == "CE" else cur_hedge_ce

                old_challenged_close_cost = self._leg_credit(cur_challenged_short, cur_challenged_hedge)
                old_safe_close_cost = self._leg_credit(cur_safe_short, cur_safe_hedge)

                # roll challenged side OUT — re-establish target delta fresh
                new_challenged_short = select_closest_delta(rows, challenged_target_delta, challenged_type, leg_id="roll_challenged_short")
                new_challenged_hedge = select_closest_delta(rows, self.hedge_delta, challenged_type, leg_id="roll_challenged_hedge")

                # roll safe side closer by HALF the distance to a fresh target strike
                fresh_safe_short = select_closest_delta(rows, safe_target_delta, safe_type, leg_id="fresh_safe_short")
                if fresh_safe_short is not None:
                    midpoint = (_safe_float(cur_safe_short["strike"]) + _safe_float(fresh_safe_short["strike"])) / 2.0
                    new_safe_short = _nearest_strike_row(rows, safe_type, midpoint)
                    safe_width = abs(_safe_float(cur_safe_short["strike"]) - _safe_float(cur_safe_hedge["strike"]))
                    # CE hedge sits further OTM (higher strike); PE hedge further OTM (lower strike)
                    hedge_target = (
                        _safe_float(new_safe_short["strike"]) + safe_width if safe_type == "CE"
                        else _safe_float(new_safe_short["strike"]) - safe_width
                    )
                    new_safe_hedge = _nearest_strike_row(rows, safe_type, hedge_target)
                else:
                    new_safe_short, new_safe_hedge = cur_safe_short, cur_safe_hedge

                if not all([new_challenged_short, new_challenged_hedge, new_safe_short, new_safe_hedge]):
                    # can't roll (illiquid/no data) — fall through to next tick
                    continue

                new_challenged_credit = self._leg_credit(new_challenged_short, new_challenged_hedge)
                new_safe_credit = self._leg_credit(new_safe_short, new_safe_hedge)

                cash_flow += (
                    (new_challenged_credit - old_challenged_close_cost)
                    + (new_safe_credit - old_safe_close_cost)
                ) * lot_size * open_lots

                if challenged_type == "CE":
                    legs["short_ce"], legs["hedge_ce"] = new_challenged_short, new_challenged_hedge
                    legs["short_pe"], legs["hedge_pe"] = new_safe_short, new_safe_hedge
                else:
                    legs["short_pe"], legs["hedge_pe"] = new_challenged_short, new_challenged_hedge
                    legs["short_ce"], legs["hedge_ce"] = new_safe_short, new_safe_hedge

                adjustments_used += 1
                events.append({
                    "ts": ts, "action": "adjustment", "n": adjustments_used,
                    "challenged_side": challenged_type,
                    "new_challenged_strike": _safe_float(new_challenged_short["strike"]),
                    "new_safe_strike": _safe_float(new_safe_short["strike"]),
                })

        else:
            # loop completed without break — close at last available tick
            if open_lots > 0 and remaining_ts:
                last_rows = chain_by_ts.get(remaining_ts[-1])
                if last_rows:
                    cur_short_ce = _find_row(last_rows, legs["short_ce"]["strike"], "CE")
                    cur_hedge_ce = _find_row(last_rows, legs["hedge_ce"]["strike"], "CE")
                    cur_short_pe = _find_row(last_rows, legs["short_pe"]["strike"], "PE")
                    cur_hedge_pe = _find_row(last_rows, legs["hedge_pe"]["strike"], "PE")
                    if all([cur_short_ce, cur_hedge_ce, cur_short_pe, cur_hedge_pe]):
                        close_cost_points = self._leg_credit(cur_short_ce, cur_hedge_ce) + self._leg_credit(cur_short_pe, cur_hedge_pe)
                        cash_flow -= close_cost_points * lot_size * open_lots
                exit_reason = "expiry_close"
                exit_ts = remaining_ts[-1]

        trade = {
            "entry_date": monday,
            "expiry": expiry,
            "entry_spot": spot,
            "initial_credit_points": round(credit, 2),
            "lots": lots,
            "lot_size": lot_size,
            "capital_at_risk": round(capital_risk_amount, 2),
            "iv_rank_at_entry": None,  # filled in by run() once it knows the iv-rank history
            "adx_at_entry": round(adx, 2) if adx is not None else None,
            "adjustments_used": adjustments_used,
            "exit_reason": exit_reason or "expiry_close",
            "exit_date": exit_ts[:10],
            "exit_time": exit_ts[11:19],
            "pnl_rupees": round(cash_flow, 2),
            "events": events,
        }
        return trade, atm_iv_sample

    # ── metrics ─────────────────────────────────────────────────────────

    @staticmethod
    def _compute_metrics(trades: list[dict]) -> dict:
        if not trades:
            return {
                "total_trades": 0, "win_rate": 0.0, "avg_win": 0.0, "avg_loss": 0.0,
                "gross_profit": 0.0, "gross_loss": 0.0, "profit_factor": 0.0,
                "total_net_pnl": 0.0, "max_drawdown": 0.0, "sharpe_like": 0.0,
            }
        pnls = [t["pnl_rupees"] for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))

        equity = 0.0
        peak = 0.0
        max_dd = 0.0
        for p in pnls:
            equity += p
            peak = max(peak, equity)
            max_dd = max(max_dd, peak - equity)

        mean_pnl = sum(pnls) / len(pnls)
        variance = sum((p - mean_pnl) ** 2 for p in pnls) / len(pnls) if len(pnls) > 1 else 0.0
        std = math.sqrt(variance)
        sharpe_like = (mean_pnl / std * math.sqrt(52)) if std > 0 else 0.0

        return {
            "total_trades": len(trades),
            "win_rate": round(100.0 * len(wins) / len(trades), 2),
            "avg_win": round(gross_profit / len(wins), 2) if wins else 0.0,
            "avg_loss": round(gross_loss / len(losses), 2) if losses else 0.0,
            "gross_profit": round(gross_profit, 2),
            "gross_loss": round(gross_loss, 2),
            "profit_factor": round(gross_profit / gross_loss, 2) if gross_loss > 0 else 0.0,
            "total_net_pnl": round(sum(pnls), 2),
            "max_drawdown": round(max_dd, 2),
            "sharpe_like": round(sharpe_like, 2),
        }

    # ── public entry point ──────────────────────────────────────────────

    def run(self, start_date: str, end_date: str) -> dict:
        self._prime_adx_lookback(start_date)
        self._prime_iv_lookback(start_date)
        holidays = self.db.get_holidays()

        trades: list[dict] = []
        skipped: list[dict] = []

        cur = _parse_date(start_date)
        end = _parse_date(end_date)
        while cur <= end:
            if cur.weekday() != self.entry_weekday:
                cur = _add_days(cur, 1)
                continue
            date_str = cur.isoformat()
            if date_str in holidays:
                cur = _add_days(cur, 1)
                continue

            try:
                expiries = self._next_expiries(date_str)
                if len(expiries) < 2:
                    skipped.append({"date": date_str, "reason": "no_next_week_expiry"})
                    cur = _add_days(cur, 1)
                    continue
                expiry = expiries[1]

                adx = self._current_adx(date_str)
                # cheap ATM-iv probe reused inside _simulate_trade; do a lightweight
                # pre-check using ADX only here, final iv_rank filter applied once
                # the entry candle (and its ATM iv) is loaded inside _simulate_trade.
                allowed_by_adx = adx is None or adx <= self.adx_max
                if not allowed_by_adx:
                    skipped.append({"date": date_str, "reason": "trending_adx", "adx": round(adx, 2)})
                    cur = _add_days(cur, 1)
                    continue

                size_mult = 0.5 if (adx is not None and adx >= self.adx_half_size) else 1.0
                risk_amount = self.capital * (self.risk_pct / 100.0) * size_mult

                trade, atm_iv_sample = self._simulate_trade(date_str, expiry, risk_amount, adx)
                if atm_iv_sample is not None:
                    self._iv_history.append(atm_iv_sample)

                if trade is None:
                    skipped.append({"date": date_str, "reason": "no_data_or_no_valid_strikes"})
                    cur = _add_days(cur, 1)
                    continue

                iv_rank = self._iv_rank(atm_iv_sample) if atm_iv_sample is not None else None
                if iv_rank is not None and iv_rank < self.iv_rank_min:
                    skipped.append({"date": date_str, "reason": "iv_rank_low", "iv_rank": round(iv_rank, 2)})
                    cur = _add_days(cur, 1)
                    continue

                trade["iv_rank_at_entry"] = round(iv_rank, 2) if iv_rank is not None else None
                trades.append(trade)
            except Exception as exc:  # noqa: BLE001 — keep one bad week from killing the run
                skipped.append({"date": date_str, "reason": "error", "detail": str(exc)})

            cur = _add_days(cur, 1)

        metrics = self._compute_metrics(trades)
        return {
            "underlying": self.underlying,
            "start_date": start_date,
            "end_date": end_date,
            "capital": self.capital,
            "risk_pct": self.risk_pct,
            "trades": trades,
            "skipped_weeks": skipped,
            "metrics": metrics,
        }


def run_backtest(
    underlying: str,
    start_date: str,
    end_date: str,
    capital: float = 1_000_000.0,
    risk_pct: float = 2.0,
    **engine_kwargs,
) -> dict:
    engine = IronCondorV2Backtest(underlying=underlying, capital=capital, risk_pct=risk_pct, **engine_kwargs)
    return engine.run(start_date, end_date)
