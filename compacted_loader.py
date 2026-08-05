"""
compacted_loader.py
---------------------
Query planner + Polars loader for the month-level compacted/ dataset
produced by compact_nifty_parquet.py (symbol/year/month folder partitions;
expiry and strike_bucket kept as physical columns, not folder partitions).

The planner resolves the exact monthly part-*.parquet files a request needs
before calling scan_parquet — no wildcard glob across unrelated years or
months (see prequnt-prompt1.txt "Required Query Planner").

expiry_type resolution (current_week/next_week/monthly) is calendar-driven
off the actual distinct expiry values present in the data — never a
hardcoded "Thursday" assumption (prequnt-prompt1.txt explicitly warns
against that, since holiday-shifted expiries would break it).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal

import polars as pl

DEFAULT_COLUMNS = (
    "timestamp", "expiry", "strike", "option_type", "close",
    "delta", "iv", "spot_price", "atm_strike",
)

ExpiryType = Literal["explicit", "current_week", "next_week", "monthly"]

_CALENDAR_COLUMN = {
    "current_week": "current_weekly_expiry",
    "next_week": "next_weekly_expiry",
    "monthly": "monthly_expiry",
}


@dataclass(frozen=True)
class BacktestDataRequest:
    symbol: str
    start_date: date
    end_date: date
    expiry_type: ExpiryType = "explicit"
    expiries: tuple[date, ...] = ()  # used when expiry_type == "explicit"; empty = all expiries in range
    candidate_strikes: int = 20
    strike_interval: int = 50
    required_columns: tuple[str, ...] = DEFAULT_COLUMNS


def _months_in_range(start: date, end: date) -> list[tuple[int, int]]:
    months = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


class CompactedMarketDataLoader:
    def __init__(self, root: Path | str):
        self.root = Path(root)

    def _resolve_files(self, symbol: str, start: date, end: date) -> list[Path]:
        files: list[Path] = []
        for y, m in _months_in_range(start, end):
            month_dir = self.root / f"symbol={symbol}" / f"year={y:04d}" / f"month={m:02d}"
            if month_dir.exists():
                files.extend(sorted(month_dir.glob("part-*.parquet")))
        return files

    def build_expiry_calendar(self, symbol: str, start_date: date, end_date: date,
                               lookahead_days: int = 45) -> pl.DataFrame:
        """trade_date -> current_weekly_expiry / next_weekly_expiry / monthly_expiry,
        derived from the actual expiries present in the data (with a lookahead window
        so the last few trading days in range can still resolve a next-week/monthly
        expiry that falls just past end_date)."""
        lookahead_end = end_date + timedelta(days=lookahead_days)
        files = self._resolve_files(symbol, start_date, lookahead_end)
        if not files:
            return pl.DataFrame(schema={
                "trade_date": pl.Date, "current_weekly_expiry": pl.Date,
                "next_weekly_expiry": pl.Date, "monthly_expiry": pl.Date,
            })

        calendar_lf = pl.scan_parquet([str(f) for f in files], parallel="auto", rechunk=False, low_memory=False)
        all_expiries = sorted(calendar_lf.select("expiry").unique().collect()["expiry"].to_list())
        trade_dates = sorted(
            calendar_lf.filter(
                (pl.col("trade_date") >= start_date) & (pl.col("trade_date") <= end_date)
            ).select("trade_date").unique().collect()["trade_date"].to_list()
        )

        monthly_expiries: dict[tuple[int, int], date] = {}
        for e in all_expiries:
            key = (e.year, e.month)
            if key not in monthly_expiries or e > monthly_expiries[key]:
                monthly_expiries[key] = e
        monthly_sorted = sorted(monthly_expiries.values())

        rows = []
        for td in trade_dates:
            upcoming = [e for e in all_expiries if e >= td]
            upcoming_monthly = [e for e in monthly_sorted if e >= td]
            rows.append({
                "trade_date": td,
                "current_weekly_expiry": upcoming[0] if upcoming else None,
                "next_weekly_expiry": upcoming[1] if len(upcoming) > 1 else None,
                "monthly_expiry": upcoming_monthly[0] if upcoming_monthly else None,
            })

        return pl.DataFrame(rows, schema={
            "trade_date": pl.Date, "current_weekly_expiry": pl.Date,
            "next_weekly_expiry": pl.Date, "monthly_expiry": pl.Date,
        })

    def explain_query(self, request: BacktestDataRequest) -> dict:
        files = self._resolve_files(request.symbol, request.start_date, request.end_date)
        total_bytes = sum(f.stat().st_size for f in files)
        estimated_rows = None
        if files:
            estimated_rows = self._base_lazy(request, files).select(pl.len()).collect().item()
        return {
            "symbol": request.symbol,
            "date_range": [request.start_date.isoformat(), request.end_date.isoformat()],
            "expiry_type": request.expiry_type,
            "months_selected": len(_months_in_range(request.start_date, request.end_date)),
            "files_selected": len(files),
            "file_paths": [str(f) for f in files],
            "expiries_filter": [e.isoformat() for e in request.expiries] if (request.expiry_type == "explicit" and request.expiries) else "resolved_per_trade_date" if request.expiry_type != "explicit" else "all",
            "candidate_strikes": request.candidate_strikes,
            "strike_window_points": request.candidate_strikes * request.strike_interval,
            "estimated_compressed_mb": round(total_bytes / 1_048_576, 2),
            "estimated_row_count": estimated_rows,
            "required_columns": list(request.required_columns),
        }

    def _base_lazy(self, request: BacktestDataRequest, files: list[Path]) -> pl.LazyFrame:
        strike_window = request.candidate_strikes * request.strike_interval
        lf = pl.scan_parquet([str(f) for f in files], parallel="auto", rechunk=False, low_memory=False)
        lf = lf.filter(
            (pl.col("timestamp") >= datetime.combine(request.start_date, datetime.min.time()))
            & (pl.col("timestamp") < datetime.combine(request.end_date, datetime.min.time()).replace(hour=23, minute=59, second=59))
            & (pl.col("strike") >= pl.col("atm_strike") - strike_window)
            & (pl.col("strike") <= pl.col("atm_strike") + strike_window)
        )

        if request.expiry_type == "explicit":
            if request.expiries:
                lf = lf.filter(pl.col("expiry").is_in(list(request.expiries)))
        else:
            calendar = self.build_expiry_calendar(request.symbol, request.start_date, request.end_date)
            calendar_col = _CALENDAR_COLUMN[request.expiry_type]
            lf = lf.join(calendar.lazy(), on="trade_date", how="inner").filter(
                pl.col("expiry") == pl.col(calendar_col)
            )

        select_cols = list(request.required_columns)
        return lf.select(select_cols)

    def load(self, request: BacktestDataRequest) -> pl.DataFrame:
        files = self._resolve_files(request.symbol, request.start_date, request.end_date)
        if not files:
            return pl.DataFrame()
        return self._base_lazy(request, files).collect()

    def iter_months(self, request: BacktestDataRequest):
        """Yields (year, month, DataFrame) — one monthly batch at a time, so a caller
        can carry strategy state across months without materializing the full range."""
        for y, m in _months_in_range(request.start_date, request.end_date):
            month_dir = self.root / f"symbol={request.symbol}" / f"year={y:04d}" / f"month={m:02d}"
            files = sorted(month_dir.glob("part-*.parquet")) if month_dir.exists() else []
            if not files:
                continue
            yield y, m, self._base_lazy(request, files).collect()
