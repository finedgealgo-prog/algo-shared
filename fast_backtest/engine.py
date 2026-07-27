"""
engine.py
---------
Iterates trading days for one underlying, loading each day's Parquet
partition into a DayChain exactly once, handing it to a strategy callback,
then dropping it before moving to the next day. Multiple strategies can
share the same loaded DayChain within one on_day call.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Callable

from .day_chain import DayChain

log = logging.getLogger("fast_backtest.engine")

OnDay = Callable[[str, DayChain], None]


class FastBacktestEngine:
    def __init__(self, parquet_root: str | Path, underlying: str) -> None:
        self.parquet_root = Path(parquet_root)
        self.underlying = underlying

    def trading_days(self) -> list[str]:
        base = self.parquet_root / self.underlying
        return sorted(p.parent.name for p in base.glob("*/data.parquet"))

    def run(self, on_day: OnDay) -> list[tuple[str, float, float]]:
        """Runs on_day(date, chain) once per trading day. Returns [(date, load_s, process_s), ...]."""
        stats = []
        for date in self.trading_days():
            path = self.parquet_root / self.underlying / date / "data.parquet"

            t0 = time.perf_counter()
            chain = DayChain.load(path)
            load_s = time.perf_counter() - t0

            t0 = time.perf_counter()
            on_day(date, chain)
            process_s = time.perf_counter() - t0

            stats.append((date, load_s, process_s))
            del chain
        return stats
