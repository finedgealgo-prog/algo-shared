"""
export_optimized_parquet.py
-----------------------------
One-time preprocessing export following the architecture in
Documents/option-algo/exd.txt: reads option_chain_new (MongoDB) for one
month and writes a Hive-partitioned Parquet dataset —
symbol/year/month/expiry/strike_bucket — with efficient dtypes
(Float32/Int32/Categorical) and a precomputed atm_strike column, so a
backtest query can prune to ATM +/-N strikes and one expiry without
scanning the full chain.

Writes into parquet_data/optimized/ — separate from the existing
parquet_data/{INSTRUMENT}/{date}/data.parquet layout that
fast_backtest/chain_snapshot.py reads (that day-based layout is untouched).

Usage:
    python export_optimized_parquet.py --month 2026-06 --underlying NIFTY
"""

from __future__ import annotations

import argparse
import calendar
import logging
import time
from datetime import date as date_cls, timedelta
from pathlib import Path

import polars as pl
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "stock_data"
DEFAULT_COLLECTION = "option_chain_new"

STRIKE_INTERVAL = 50
STRIKE_BUCKET_SIZE = 1000

PROJECTION = {
    "_id": 0, "timestamp": 1, "underlying": 1, "expiry": 1, "strike": 1,
    "type": 1, "close": 1, "oi": 1, "iv": 1, "delta": 1, "gamma": 1,
    "theta": 1, "vega": 1, "rho": 1, "spot_price": 1,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("export_optimized_parquet")


def daterange(start: date_cls, end: date_cls):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def fetch_month(client: MongoClient, collection: str, underlying: str, year: int, month: int) -> pl.DataFrame:
    last_day = calendar.monthrange(year, month)[1]
    start, end = date_cls(year, month, 1), date_cls(year, month, last_day)

    coll = client[DB_NAME][collection]
    frames: list[pl.DataFrame] = []
    for d in daterange(start, end):
        day_str = d.isoformat()
        query = {
            "underlying": underlying,
            "timestamp": {"$gte": f"{day_str}T00:00:00", "$lte": f"{day_str}T23:59:59"},
        }
        docs = list(coll.find(query, PROJECTION))
        if not docs:
            continue
        frames.append(pl.DataFrame(docs))
        log.info("fetched %s: %d rows", day_str, len(docs))

    return pl.concat(frames, how="vertical") if frames else pl.DataFrame()


def transform(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.col("timestamp").str.to_datetime("%Y-%m-%dT%H:%M:%S"),
        pl.col("expiry").str.to_date(),
        pl.col("type").alias("option_type"),
        pl.col("strike").round(0).cast(pl.Int32),
        pl.col("close").cast(pl.Float32),
        pl.col("oi").fill_null(0).cast(pl.Int64),
        pl.col("iv").cast(pl.Float32),
        pl.col("delta").cast(pl.Float32),
        pl.col("gamma").cast(pl.Float32),
        pl.col("theta").cast(pl.Float32),
        pl.col("vega").cast(pl.Float32),
        pl.col("rho").cast(pl.Float32),
        pl.col("spot_price").cast(pl.Float32),
    ).drop("type")

    df = df.with_columns(
        pl.col("timestamp").dt.date().alias("trade_date"),
        (pl.col("strike") // STRIKE_BUCKET_SIZE * STRIKE_BUCKET_SIZE).cast(pl.Int32).alias("strike_bucket"),
        (((pl.col("spot_price") + STRIKE_INTERVAL / 2) / STRIKE_INTERVAL).floor() * STRIKE_INTERVAL)
        .cast(pl.Int32)
        .alias("atm_strike"),
    )
    return df.with_columns(pl.col("option_type").cast(pl.Categorical)).sort(
        ["expiry", "strike_bucket", "timestamp", "strike", "option_type"]
    )


def write_partitions(df: pl.DataFrame, out_root: Path, underlying: str, year: int, month: int) -> dict:
    base = out_root / f"symbol={underlying}" / f"year={year:04d}" / f"month={month:02d}"
    written_files = 0
    written_rows = 0
    written_bytes = 0
    t0 = time.time()

    for (expiry, bucket), part in df.group_by(["expiry", "strike_bucket"], maintain_order=True):
        target_dir = base / f"expiry={expiry.isoformat()}" / f"strike_bucket={bucket}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / "part-000.parquet"
        # underlying/expiry/strike_bucket are already encoded in the folder path
        # (symbol=/expiry=/strike_bucket=) — hive_partitioning=True reconstructs
        # them on read, so storing them again as physical columns would just
        # duplicate the same value across every row in the partition.
        part_to_write = part.drop([c for c in ("underlying", "expiry", "strike_bucket") if c in part.columns])
        part_to_write.write_parquet(target_file, compression="zstd", statistics=True, row_group_size=100_000)
        written_files += 1
        written_rows += part.height
        written_bytes += target_file.stat().st_size

    return {
        "files": written_files,
        "rows": written_rows,
        "bytes": written_bytes,
        "write_seconds": round(time.time() - t0, 2),
    }


def write_atm_index(df: pl.DataFrame, out_root: Path, underlying: str, year: int, month: int) -> Path:
    index_df = (
        df.select(["timestamp", "trade_date", "spot_price", "atm_strike"])
        .unique(subset=["timestamp"])
        .sort("timestamp")
    )
    target_dir = out_root / "_atm_index" / f"symbol={underlying}" / f"year={year:04d}" / f"month={month:02d}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "atm_index.parquet"
    index_df.write_parquet(target_file, compression="zstd", statistics=True)
    return target_file


def export_month(underlying: str, year: int, month: int, out_root: Path, collection: str = DEFAULT_COLLECTION) -> None:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        t0 = time.time()
        raw = fetch_month(client, collection, underlying, year, month)
        if raw.is_empty():
            log.warning("no data found for %s %04d-%02d", underlying, year, month)
            return
        fetch_seconds = time.time() - t0
        log.info("fetched %d raw rows in %.2fs", raw.height, fetch_seconds)

        df = transform(raw)
        stats = write_partitions(df, out_root, underlying, year, month)
        index_path = write_atm_index(df, out_root, underlying, year, month)

        log.info(
            "done: %s %04d-%02d -> %d rows, %d partition files, %.1f MB (%.2fs write) + atm index %s",
            underlying, year, month, stats["rows"], stats["files"],
            stats["bytes"] / 1_048_576, stats["write_seconds"], index_path,
        )
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Export option_chain_new to Hive-partitioned optimized Parquet")
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--underlying", default="NIFTY")
    parser.add_argument("--out", default=str(Path(__file__).resolve().parent / "parquet_data" / "optimized"))
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)
    args = parser.parse_args()

    year, month = (int(p) for p in args.month.split("-"))
    export_month(args.underlying, year, month, Path(args.out), args.collection)


if __name__ == "__main__":
    main()
