"""
parquet_store.py
-----------------
Cached read-side access to the Parquet files produced by
export_historical_parquet.py, standing in for direct MongoDB reads of
scanner_stock_historical_data / scanner_index_historical_data. Lives in
shared/ (like features/, chart_api.py, fast_backtest/) so any service can
symlink it in the same way algo.scanner does.

Run `python export_historical_parquet.py` first to produce the files.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

PARQUET_DIR = Path(__file__).resolve().parent / "parquet_data" / "scanner_historical"
STOCK_PARQUET_PATH = PARQUET_DIR / "scanner_stock_historical_data.parquet"
INDEX_PARQUET_PATH = PARQUET_DIR / "scanner_index_historical_data.parquet"

_stock_df: pd.DataFrame | None = None
_stock_mtime: float | None = None
_index_df: pd.DataFrame | None = None
_index_mtime: float | None = None


def _missing_file_error(path: Path) -> FileNotFoundError:
    return FileNotFoundError(
        f"{path} not found. Run `python export_historical_parquet.py` "
        "(in shared/) to generate it first."
    )


def _get_stock_df() -> pd.DataFrame:
    global _stock_df, _stock_mtime
    if not STOCK_PARQUET_PATH.exists():
        raise _missing_file_error(STOCK_PARQUET_PATH)
    mtime = os.path.getmtime(STOCK_PARQUET_PATH)
    if _stock_df is None or mtime != _stock_mtime:
        _stock_df = pd.read_parquet(STOCK_PARQUET_PATH, engine="pyarrow")
        _stock_mtime = mtime
    return _stock_df


def _get_index_df() -> pd.DataFrame:
    global _index_df, _index_mtime
    if not INDEX_PARQUET_PATH.exists():
        raise _missing_file_error(INDEX_PARQUET_PATH)
    mtime = os.path.getmtime(INDEX_PARQUET_PATH)
    if _index_df is None or mtime != _index_mtime:
        _index_df = pd.read_parquet(INDEX_PARQUET_PATH, engine="pyarrow")
        _index_mtime = mtime
    return _index_df


def load_index_daily(symbol, start, end) -> pd.DataFrame:
    df = _get_index_df()
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()

    sub = df[
        (df["i_symbol"] == symbol)
        & (df["ih_timestamp"] >= start_ts)
        & (df["ih_timestamp"] <= end_ts)
    ]
    cols = ["i_symbol", "ih_timestamp", "i_open_price", "i_high_price", "i_low_price", "i_close_price"]
    sub = sub[cols].sort_values("ih_timestamp").set_index("ih_timestamp")
    return sub


def load_stocks_bulk(symbols, start, end, min_price=None, max_price=None) -> pd.DataFrame:
    def safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    min_price = safe_float(min_price) or None
    max_price = safe_float(max_price) or None

    df = _get_stock_df()
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()

    sub = df[
        df["h_symbol"].isin(symbols)
        & (df["ch_timestamp"] >= start_ts)
        & (df["ch_timestamp"] <= end_ts)
    ]
    if sub.empty:
        return sub

    sub = (
        sub[["h_symbol", "ch_timestamp", "ch_opening_price", "ch_high_price", "ch_low_price", "ch_closing_price"]]
        .groupby(["h_symbol", "ch_timestamp"], as_index=False)
        .agg(
            {
                "ch_opening_price": "first",
                "ch_high_price": "max",
                "ch_low_price": "min",
                "ch_closing_price": "last",
            }
        )
    )

    if min_price is not None or max_price is not None:
        latest = sub.groupby("h_symbol", sort=False)["ch_closing_price"].last().reset_index()
        if min_price is not None:
            latest = latest[latest["ch_closing_price"] >= min_price]
        if max_price is not None:
            latest = latest[latest["ch_closing_price"] <= max_price]
        sub = sub[sub["h_symbol"].isin(latest["h_symbol"].tolist())]

    sub = sub.sort_values(["h_symbol", "ch_timestamp"]).set_index("ch_timestamp")
    return sub


def load_stock_closes(symbols, hist_start, hist_end) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()

    df = _get_stock_df()
    start_ts = pd.Timestamp(hist_start).normalize()
    end_ts = pd.Timestamp(hist_end).normalize()

    sub = df[
        df["h_symbol"].isin(symbols)
        & (df["ch_timestamp"] >= start_ts)
        & (df["ch_timestamp"] <= end_ts)
    ][["h_symbol", "ch_timestamp", "ch_closing_price"]].copy()

    sub = sub.dropna(subset=["h_symbol", "ch_timestamp", "ch_closing_price"])
    sub = sub.sort_values(["h_symbol", "ch_timestamp"]).reset_index(drop=True)
    return sub
