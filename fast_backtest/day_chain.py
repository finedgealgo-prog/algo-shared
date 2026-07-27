"""
day_chain.py
------------
Loads one trading day's option chain Parquet file into RAM and indexes it
by (timestamp, expiry, option_type) as (start, end) offsets into flat numpy
arrays, so a lookup is a dict hit + a numpy slice (view, no copy) instead of
a Polars DataFrame filter.

This relies on the Parquet being sorted by
(trade_date, timestamp, expiry, option_type, strike) at export time
(see parquet_export.py SORT_COLUMNS) — under that order, every
(timestamp, expiry, option_type) group occupies one contiguous row block,
so group boundaries can be computed with a single vectorized group_by
instead of building a numpy array per group in a Python loop (which was
tried first and was slower than a naive per-lookup filter — see git
history / benchmark.py for the comparison).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl


@dataclass
class GroupArrays:
    strike: np.ndarray
    delta: np.ndarray
    premium: np.ndarray
    oi: np.ndarray
    iv: np.ndarray


class DayChain:
    """One trading day's option chain, held in RAM for the duration of that day's backtest step."""

    def __init__(self, trade_date: str, underlying: str,
                 index: dict[tuple, tuple[int, int]],
                 strike: np.ndarray, delta: np.ndarray, premium: np.ndarray,
                 oi: np.ndarray, iv: np.ndarray,
                 spot_by_ts: dict, expiries_by_ts: dict) -> None:
        self.trade_date = trade_date
        self.underlying = underlying
        self._index = index
        self._strike = strike
        self._delta = delta
        self._premium = premium
        self._oi = oi
        self._iv = iv
        self._spot_by_ts = spot_by_ts
        self._expiries_by_ts = expiries_by_ts
        self.timestamps = sorted(spot_by_ts.keys())

    @classmethod
    def load(cls, parquet_path: str | Path) -> "DayChain":
        df = pl.read_parquet(parquet_path)
        trade_date = str(df["trade_date"][0])
        underlying = str(df["underlying"][0])

        grouped = df.group_by(["timestamp", "expiry", "option_type"], maintain_order=True).agg(pl.len().alias("n"))
        lengths = grouped["n"].to_numpy()
        offsets = np.concatenate(([0], np.cumsum(lengths)))

        ts_keys = grouped["timestamp"].to_list()
        expiry_keys = grouped["expiry"].cast(pl.Utf8).to_list()
        type_keys = grouped["option_type"].to_list()

        index: dict[tuple, tuple[int, int]] = {}
        expiries_by_ts: dict = {}
        for i, (ts, exp, opt) in enumerate(zip(ts_keys, expiry_keys, type_keys)):
            index[(ts, exp, opt)] = (int(offsets[i]), int(offsets[i + 1]))
            expiries_by_ts.setdefault(ts, set()).add(exp)
        expiries_by_ts = {ts: sorted(exps) for ts, exps in expiries_by_ts.items()}

        strike = df["strike"].to_numpy()
        delta = df["delta"].to_numpy()
        premium = df["close"].to_numpy()
        oi = df["oi"].to_numpy()
        iv = df["iv"].to_numpy()

        spot_map = df.group_by("timestamp", maintain_order=True).agg(pl.col("spot_price").first())
        spot_by_ts = dict(spot_map.iter_rows())

        return cls(trade_date, underlying, index, strike, delta, premium, oi, iv, spot_by_ts, expiries_by_ts)

    def spot(self, timestamp) -> float | None:
        return self._spot_by_ts.get(timestamp)

    def expiries_at(self, timestamp) -> list[str]:
        return self._expiries_by_ts.get(timestamp, [])

    def group(self, timestamp, expiry: str, option_type: str) -> GroupArrays | None:
        bounds = self._index.get((timestamp, expiry, option_type))
        if bounds is None:
            return None
        start, end = bounds
        return GroupArrays(
            strike=self._strike[start:end],
            delta=self._delta[start:end],
            premium=self._premium[start:end],
            oi=self._oi[start:end],
            iv=self._iv[start:end],
        )
