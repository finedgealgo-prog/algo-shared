"""
export_historical_parquet.py
-----------------------------
Export scanner_stock_historical_data / scanner_index_historical_data from
MongoDB into local Parquet files that parquet_store.py reads for fast
backtest/score access (see algo.scanner/scanner/backtest_engine.py,
algo.scanner/scanner/service.py). Lives in shared/ (like features/,
chart_api.py, fast_backtest/) so any service can symlink it in the same
way algo.scanner does; symlink `export_historical_parquet.py` into a
service directory to use it there too.

Usage:
    python export_historical_parquet.py
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import pandas as pd
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "stock_data"
COL_STOCKS = "scanner_stock_historical_data"
COL_INDEX = "scanner_index_historical_data"

PARQUET_DIR = Path(__file__).resolve().parent / "parquet_data" / "scanner_historical"
STOCK_PARQUET_PATH = PARQUET_DIR / "scanner_stock_historical_data.parquet"
INDEX_PARQUET_PATH = PARQUET_DIR / "scanner_index_historical_data.parquet"

STOCK_NUMERIC_COLUMNS = [
    "ch_opening_price", "ch_high_price", "ch_low_price",
    "ch_closing_price", "ch_volume", "ch_previous_cls_price",
]
INDEX_NUMERIC_COLUMNS = [
    "i_open_price", "i_high_price", "i_low_price",
    "i_close_price", "i_volume", "i_previous_close", "i_ch",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("export_historical_parquet")


def _write_atomic(df: pd.DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    df.to_parquet(tmp, engine="pyarrow", compression="zstd", index=False)
    os.replace(tmp, target)


def export_stock_history(client: MongoClient) -> int:
    t0 = time.time()
    docs = list(client[DB_NAME][COL_STOCKS].find({}, {"_id": 0}))
    if not docs:
        log.warning("no documents found in %s", COL_STOCKS)
        return 0

    df = pd.DataFrame(docs)
    df["ch_timestamp"] = pd.to_datetime(df["ch_timestamp"], errors="coerce")
    for col in STOCK_NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["h_symbol", "ch_timestamp"])
    df = df.sort_values(["h_symbol", "ch_timestamp"]).reset_index(drop=True)

    _write_atomic(df, STOCK_PARQUET_PATH)
    log.info("%s: %d rows -> %s (%.2fs)", COL_STOCKS, len(df), STOCK_PARQUET_PATH, time.time() - t0)
    return len(df)


def export_index_history(client: MongoClient) -> int:
    t0 = time.time()
    docs = list(client[DB_NAME][COL_INDEX].find({}, {"_id": 0}))
    if not docs:
        log.warning("no documents found in %s", COL_INDEX)
        return 0

    df = pd.DataFrame(docs)
    df["ih_timestamp"] = pd.to_datetime(df["ih_timestamp"], errors="coerce")
    for col in INDEX_NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["i_symbol", "ih_timestamp"])
    df = df.sort_values(["i_symbol", "ih_timestamp"]).reset_index(drop=True)

    _write_atomic(df, INDEX_PARQUET_PATH)
    log.info("%s: %d rows -> %s (%.2fs)", COL_INDEX, len(df), INDEX_PARQUET_PATH, time.time() - t0)
    return len(df)


def export_all() -> dict:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        stock_rows = export_stock_history(client)
        index_rows = export_index_history(client)
    finally:
        client.close()
    return {"stock_rows": stock_rows, "index_rows": index_rows}


if __name__ == "__main__":
    result = export_all()
    log.info("done: %s", result)
