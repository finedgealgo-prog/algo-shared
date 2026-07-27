"""
strategy_runner.py
-------------------
Minimal strategy execution on top of DayChain + selectors, reading the
saved_strategies.full_config.strategy shape used elsewhere in this repo.

Deliberately does NOT replicate backtest_engine.py's full feature set —
by design (see conversation): a separate, small engine so nothing in the
existing MongoDB-backed backtest path is touched.

Supports:
  - Entry / exit: fixed time only (IndicatorType.TimeIndicator)
  - Strike selection per leg: StrikeType.ATM / Delta / Premium
  - Per-leg stop loss: LegTgtSLType.UnderlyingPoints
  - Overall stop loss / target: OverallTgtSLType.MTM

Not supported yet (ignored if present in the strategy config):
  re-entries, lazy legs, adjustments, momentum, lock-and-trail,
  BTST/positional, range breakout, condition-based entry/exit.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Callable

import numpy as np

from . import selectors
from .day_chain import DayChain
from .engine import FastBacktestEngine

log = logging.getLogger("fast_backtest.strategy_runner")

DEFAULT_LOT_SIZE = {"NIFTY": 75, "BANKNIFTY": 35, "FINNIFTY": 65, "MIDCPNIFTY": 140}

OnProgress = Callable[[int, int, str], None]


def _time_from_indicators(indicators: dict | None) -> tuple[int, int] | None:
    for node in (indicators or {}).get("Value") or []:
        val = node.get("Value") or {}
        if val.get("IndicatorName") == "IndicatorType.TimeIndicator":
            params = val.get("Parameters") or {}
            return int(params.get("Hour", 0)), int(params.get("Minute", 0))
    return None


def _nearest_ts(chain: DayChain, date: str, hour: int, minute: int):
    y, m, d = (int(p) for p in date.split("-"))
    target = datetime(y, m, d, hour, minute)
    later = [t for t in chain.timestamps if t >= target]
    if later:
        return later[0]
    return chain.timestamps[-1] if chain.timestamps else None


def _fmt_ts(ts) -> str:
    return ts.strftime("%H:%M")


def _leg_direction(position: str, option_type: str) -> int:
    """Sign of the underlying move that is ADVERSE to this leg (+1 = up hurts, -1 = down hurts)."""
    is_sell = "Sell" in position
    is_ce = option_type == "CE"
    if is_sell:
        return 1 if is_ce else -1
    return -1 if is_ce else 1


def _leg_pnl(leg: dict, px: float) -> float:
    if "Sell" in leg["position"]:
        return (leg["entry_price"] - px) * leg["lots"] * leg["lot_size"]
    return (px - leg["entry_price"]) * leg["lots"] * leg["lot_size"]


def _resolve_strike(chain: DayChain, ts, expiry: str, option_type: str,
                     strike_param: str, strike_value) -> dict | None:
    group = chain.group(ts, expiry, option_type)
    if group is None:
        return None
    if strike_param.endswith("ATM"):
        spot = chain.spot(ts)
        if spot is None:
            return None
        return selectors.by_atm_strike(group, spot)
    if strike_param.endswith("Delta"):
        target = float(strike_value or 0)
        target = target if option_type == "CE" else -abs(target)
        return selectors.by_delta(group, target)
    if strike_param.endswith("Premium"):
        return selectors.by_premium(group, float(strike_value or 0))
    return None


def _premium_at(chain: DayChain, ts, expiry: str, option_type: str, strike: float) -> float | None:
    group = chain.group(ts, expiry, option_type)
    if group is None:
        return None
    idx = np.where(group.strike == strike)[0]
    if idx.size == 0:
        return None
    val = float(group.premium[idx[0]])
    return val if val == val else None  # NaN check


def _close_remaining(leg_states: list[dict], chain: DayChain, ts, default_reason: str) -> None:
    for leg in leg_states:
        if leg["exit_price"] is not None:
            continue
        px = _premium_at(chain, ts, leg["expiry"], leg["option_type"], leg["strike"])
        if px is None:
            px = leg["entry_price"]
        leg["_closed_pnl"] = _leg_pnl(leg, px)
        leg["exit_price"] = px
        leg["exit_time"] = _fmt_ts(ts)
        leg["exit_reason"] = leg["exit_reason"] or default_reason


def _open_legs(chain: DayChain, entry_ts, expiry: str, legs_cfg: list[dict], lot_size: int) -> list[dict]:
    leg_states = []
    for leg_cfg in legs_cfg:
        option_type = "CE" if str(leg_cfg.get("InstrumentKind", "")).endswith("CE") else "PE"
        strike_param = leg_cfg.get("StrikeParameter", "StrikeType.ATM")
        strike_value = leg_cfg.get("StrikeValue")
        picked = _resolve_strike(chain, entry_ts, expiry, option_type, strike_param, strike_value)
        if picked is None or picked.get("premium") is None:
            continue

        position = leg_cfg.get("PositionType", "PositionType.Sell")
        sl_cfg = leg_cfg.get("LegStopLoss") or {}
        sl_points = (
            float(sl_cfg.get("Value", 0) or 0)
            if sl_cfg.get("Type") == "LegTgtSLType.UnderlyingPoints" else None
        )

        leg_states.append({
            "option_type": option_type,
            "expiry": expiry,
            "strike": picked["strike"],
            "entry_price": picked["premium"],
            "entry_time": _fmt_ts(entry_ts),
            "position": position,
            "lots": float((leg_cfg.get("LotConfig") or {}).get("Value", 1) or 1),
            "lot_size": lot_size,
            "sl_points": sl_points,
            "direction": _leg_direction(position, option_type),
            "exit_price": None,
            "exit_time": None,
            "exit_reason": None,
        })
    return leg_states


def _run_day(chain: DayChain, date: str, legs_cfg: list[dict],
             entry_h: int, entry_m: int, exit_h: int, exit_m: int,
             overall_sl_val: float | None, overall_tgt_val: float | None,
             lot_size: int) -> dict | None:
    entry_ts = _nearest_ts(chain, date, entry_h, entry_m)
    exit_ts = _nearest_ts(chain, date, exit_h, exit_m)
    if entry_ts is None or exit_ts is None:
        return None

    expiries = chain.expiries_at(entry_ts)
    if not expiries:
        return None
    expiry = expiries[0]  # nearest live expiry

    leg_states = _open_legs(chain, entry_ts, expiry, legs_cfg, lot_size)
    if not leg_states:
        return None

    entry_spot = chain.spot(entry_ts) or 0.0
    day_timestamps = [t for t in chain.timestamps if entry_ts <= t <= exit_ts]

    day_exit_reason = None
    for ts in day_timestamps:
        spot = chain.spot(ts)
        running_mtm = 0.0
        any_open = False

        for leg in leg_states:
            if leg["exit_price"] is not None:
                running_mtm += leg["_closed_pnl"]
                continue
            any_open = True
            px = _premium_at(chain, ts, leg["expiry"], leg["option_type"], leg["strike"])
            if px is None:
                continue
            leg_pnl = _leg_pnl(leg, px)

            if leg["sl_points"] is not None and spot is not None:
                adverse_move = (spot - entry_spot) * leg["direction"]
                if adverse_move >= leg["sl_points"]:
                    leg["_closed_pnl"] = leg_pnl
                    leg["exit_price"] = px
                    leg["exit_time"] = _fmt_ts(ts)
                    leg["exit_reason"] = "Leg SL"
                    continue

            running_mtm += leg_pnl

        if not any_open:
            break
        if overall_sl_val is not None and running_mtm <= -abs(overall_sl_val):
            day_exit_reason = "Overall SL"
            _close_remaining(leg_states, chain, ts, day_exit_reason)
            break
        if overall_tgt_val is not None and running_mtm >= abs(overall_tgt_val):
            day_exit_reason = "Overall Target"
            _close_remaining(leg_states, chain, ts, day_exit_reason)
            break

    _close_remaining(leg_states, chain, exit_ts, day_exit_reason or "Time Exit")

    legs_out = []
    total_pnl = 0.0
    for leg in leg_states:
        pnl = leg.get("_closed_pnl", 0.0)
        total_pnl += pnl
        legs_out.append({
            "lots": leg["lots"], "lot_size": leg["lot_size"], "position": leg["position"],
            "type": leg["option_type"], "strike": leg["strike"],
            "entry_time": leg["entry_time"], "exit_time": leg["exit_time"],
            "entry_price": round(leg["entry_price"], 2),
            "exit_price": round(leg["exit_price"], 2) if leg["exit_price"] is not None else None,
            "pnl": round(pnl, 2), "exit_reason": leg["exit_reason"],
        })

    return {
        "date": date, "entry_time": _fmt_ts(entry_ts), "exit_time": _fmt_ts(exit_ts),
        "total_pnl": round(total_pnl, 2), "legs": legs_out,
    }


def run_strategy(strategy: dict, start_date: str, end_date: str, parquet_root: str | Path,
                  on_progress: OnProgress | None = None) -> list[dict]:
    underlying = strategy["Ticker"]
    legs_cfg = strategy["ListOfLegConfigs"]

    entry_hm = _time_from_indicators(strategy.get("EntryIndicators"))
    exit_hm = _time_from_indicators(strategy.get("ExitIndicators"))
    if not entry_hm or not exit_hm:
        raise ValueError("fast_backtest only supports time-based EntryIndicators/ExitIndicators for now")
    entry_h, entry_m = entry_hm
    exit_h, exit_m = exit_hm

    overall_sl = strategy.get("OverallSL") or {}
    overall_tgt = strategy.get("OverallTgt") or {}
    overall_sl_val = float(overall_sl["Value"]) if overall_sl.get("Type") == "OverallTgtSLType.MTM" else None
    overall_tgt_val = float(overall_tgt["Value"]) if overall_tgt.get("Type") == "OverallTgtSLType.MTM" else None

    lot_size = DEFAULT_LOT_SIZE.get(underlying, 75)

    engine = FastBacktestEngine(parquet_root, underlying)
    days = [d for d in engine.trading_days() if start_date <= d <= end_date]
    total = len(days)

    trades: list[dict] = []
    for day_idx, date in enumerate(days):
        path = Path(parquet_root) / underlying / date / "data.parquet"
        chain = DayChain.load(path)
        trade = _run_day(chain, date, legs_cfg, entry_h, entry_m, exit_h, exit_m,
                          overall_sl_val, overall_tgt_val, lot_size)
        if trade:
            trades.append(trade)
        if on_progress:
            on_progress(day_idx + 1, total, date)

    return trades
