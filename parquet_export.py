"""
parquet_export.py
------------------
Export option_chain documents from MongoDB into partitioned, ZSTD-compressed
Parquet files optimized for fast backtest reads.

Usage:
    python parquet_export.py --date 2025-11-03
    python parquet_export.py --date 2025-11-03 --underlying NIFTY --out ./parquet_data
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
DEFAULT_COLLECTION = "option_chain"

PROJECTION = {
    "_id": 0, "timestamp": 1, "underlying": 1, "expiry": 1, "strike": 1,
    "type": 1, "close": 1, "oi": 1, "iv": 1, "delta": 1, "gamma": 1,
    "theta": 1, "vega": 1, "rho": 1, "spot_price": 1,
}

SORT_COLUMNS = ["trade_date", "timestamp", "expiry", "option_type", "strike"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("parquet_export")


def fetch_day(client: MongoClient, date: str, underlying: str, collection: str) -> list[dict]:
    query = {
        "underlying": underlying,
        "timestamp": {"$gte": f"{date}T00:00:00", "$lte": f"{date}T23:59:59"},
    }
    return list(client[DB_NAME][collection].find(query, PROJECTION))


def to_dataframe(docs: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(docs)
    df = df.with_columns(
        pl.col("type").alias("option_type"),
        pl.col("timestamp").str.slice(0, 10).str.to_date().alias("trade_date"),
        pl.col("timestamp").str.to_datetime(),
        pl.col("expiry").str.to_date(),
    ).drop("type")
    return df.sort(SORT_COLUMNS)


def write_partition(df: pl.DataFrame, out_dir: Path, underlying: str, date: str) -> Path:
    target_dir = out_dir / underlying / date
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "data.parquet"
    df.write_parquet(target_file, compression="zstd", statistics=True, row_group_size=50_000)
    return target_file


def validate(target_file: Path, expected_rows: int) -> bool:
    written = pl.scan_parquet(target_file).select(pl.len()).collect().item()
    ok = written == expected_rows
    (log.info if ok else log.error)(
        "validate: expected=%d written=%d match=%s", expected_rows, written, ok
    )
    return ok


def export_day(client: MongoClient, date: str, underlying: str | None, out_dir: Path,
                collection: str = DEFAULT_COLLECTION) -> None:
    coll = client[DB_NAME][collection]
    underlyings = [underlying] if underlying else coll.distinct(
        "underlying", {"timestamp": {"$gte": f"{date}T00:00:00", "$lte": f"{date}T23:59:59"}}
    )
    if not underlyings:
        log.warning("no data found for date=%s underlying=%s", date, underlying)
        return

    for u in underlyings:
        t0 = time.time()
        docs = fetch_day(client, date, u, collection)
        if not docs:
            log.warning("no rows for %s on %s", u, date)
            continue
        df = to_dataframe(docs)
        target = write_partition(df, out_dir, u, date)
        validate(target, len(docs))
        log.info("%s %s: %d rows -> %s (%.2fs)", u, date, len(docs), target, time.time() - t0)


def daterange(start: str, end: str):
    start_d = date_cls.fromisoformat(start)
    end_d = date_cls.fromisoformat(end)
    d = start_d
    while d <= end_d:
        yield d.isoformat()
        d += timedelta(days=1)


def export_range(start: str, end: str, underlying: str | None, out_dir: Path,
                  collection: str = DEFAULT_COLLECTION) -> None:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    t0 = time.time()
    day_count = 0
    for date in daterange(start, end):
        coll = client[DB_NAME][collection]
        if coll.count_documents({"timestamp": {"$gte": f"{date}T00:00:00", "$lte": f"{date}T23:59:59"}}, limit=1) == 0:
            continue
        export_day(client, date, underlying, out_dir, collection)
        day_count += 1
    client.close()
    log.info("range %s..%s: %d trading days exported (%.2fs)", start, end, day_count, time.time() - t0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export MongoDB option_chain data to partitioned Parquet")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", help="Single trade date, YYYY-MM-DD")
    group.add_argument("--month", help="Calendar month, YYYY-MM")
    group.add_argument("--start", help="Range start date, YYYY-MM-DD (use with --end)")
    parser.add_argument("--end", help="Range end date, YYYY-MM-DD (use with --start)")
    parser.add_argument("--underlying", default=None, help="Underlying symbol (default: all found)")
    parser.add_argument("--out", default="parquet_data", help="Output root directory")
    parser.add_argument("--collection", default=DEFAULT_COLLECTION, help="MongoDB collection name")
    args = parser.parse_args()

    if args.date:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        export_day(client, args.date, args.underlying, Path(args.out), args.collection)
        client.close()
    elif args.month:
        year, month = (int(p) for p in args.month.split("-"))
        last_day = calendar.monthrange(year, month)[1]
        start = f"{year:04d}-{month:02d}-01"
        end = f"{year:04d}-{month:02d}-{last_day:02d}"
        export_range(start, end, args.underlying, Path(args.out), args.collection)
    else:
        if not args.end:
            parser.error("--start requires --end")
        export_range(args.start, args.end, args.underlying, Path(args.out), args.collection)


if __name__ == "__main__":
    main()
