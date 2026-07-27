"""
benchmark.py
------------
Runs the same "N dynamic strike lookups per day" workload as the earlier
naive-Polars-filter benchmark, but through DayChain + selectors (numpy
argmin over pre-indexed arrays). Prints load/query breakdown and
extrapolates to a 5-year, 4-index backtest for comparison.

Usage:
    python -m shared.fast_backtest.benchmark --root shared/parquet_data --underlying NIFTY
"""

from __future__ import annotations

import argparse
import random
import time
from pathlib import Path

from . import selectors
from .day_chain import DayChain
from .engine import FastBacktestEngine

TRADING_DAYS_PER_YEAR = 250
YEARS = 5
INDICES = 4


def simulate_day(chain: DayChain, trades_per_day: int) -> None:
    if not chain.timestamps:
        return
    for _ in range(trades_per_day):
        ts = random.choice(chain.timestamps)
        expiries = chain.expiries_at(ts)
        if not expiries:
            continue
        expiry = random.choice(expiries)
        option_type = random.choice(["CE", "PE"])
        group = chain.group(ts, expiry, option_type)
        if random.random() < 0.5:
            selectors.by_delta(group, 0.30)
        else:
            selectors.by_premium(group, 250.0)


def run(root: Path, underlying: str, trades_per_day: int = 50, seed: int = 42) -> None:
    random.seed(seed)
    engine = FastBacktestEngine(root, underlying)
    days = engine.trading_days()
    if not days:
        print(f"no parquet files found under {root / underlying}")
        return

    stats = engine.run(lambda date, chain: simulate_day(chain, trades_per_day))

    load_time = sum(s[1] for s in stats)
    query_time = sum(s[2] for s in stats)
    total = load_time + query_time
    n_days = len(stats)
    per_day = total / n_days

    print(f"days benchmarked: {n_days} ({underlying})")
    print(f"load time total:  {load_time:.3f}s ({load_time / n_days * 1000:.2f} ms/day)")
    print(f"query time total: {query_time:.3f}s ({query_time / n_days * 1000:.2f} ms/day, {trades_per_day} lookups/day)")
    print(f"total time:       {total:.3f}s ({per_day * 1000:.2f} ms/day)")

    projected_days = TRADING_DAYS_PER_YEAR * YEARS
    single_index_5y = per_day * projected_days
    all_index_5y = single_index_5y * INDICES

    print(f"\nprojected {YEARS}y single-index ({projected_days} days): {single_index_5y:.2f}s")
    print(f"projected {YEARS}y x {INDICES} indices (sequential):        {all_index_5y:.2f}s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark fast_backtest dynamic strike selection")
    parser.add_argument("--root", default="parquet_data")
    parser.add_argument("--underlying", default="NIFTY")
    parser.add_argument("--trades-per-day", type=int, default=50)
    args = parser.parse_args()
    run(Path(args.root), args.underlying, args.trades_per_day)


if __name__ == "__main__":
    main()
