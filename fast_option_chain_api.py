"""
fast_option_chain_api.py
--------------------------
HTTP API for the Parquet-based paper-trade backtest-replay chain snapshot.
Same response contract as algo.websocket/historical_data_router.py's
/simulator/paper-trade/historical-chain* endpoints (that file is
untouched) — mounted under /simulator/fast-paper-trade/* instead, reading
shared/parquet_data/ (option_chain_new-sourced) via
shared/fast_backtest/chain_snapshot.py.

Mounted the same way as chart_api.py / fast_backtest_api.py: symlinked
into each service dir and included from that service's *_main.py.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

from fast_backtest.chain_snapshot import get_expiry_dates, get_historical_chain, get_latest_date, get_trading_days

router = APIRouter(prefix="/simulator/fast-paper-trade")

PARQUET_ROOT = Path(__file__).resolve().parent / "parquet_data"


@router.get("/historical-chain/{instrument}")
def historical_chain(instrument: str,
                      timestamp: str = Query(..., description="ISO timestamp, e.g. 2025-11-03T09:16:00"),
                      expiry: str = Query(default="")):
    if len(timestamp.strip()) < 10:
        raise HTTPException(status_code=400, detail="timestamp is required, e.g. 2025-11-03T09:16:00")
    return get_historical_chain(PARQUET_ROOT, instrument, timestamp, expiry)


@router.get("/historical-chain-latest-date/{instrument}")
def historical_chain_latest_date(instrument: str):
    return get_latest_date(PARQUET_ROOT, instrument)


@router.get("/expiry-dates/{instrument}")
def expiry_dates(instrument: str):
    return get_expiry_dates(PARQUET_ROOT, instrument)


@router.get("/trading-days/{instrument}")
def trading_days(instrument: str):
    return get_trading_days(PARQUET_ROOT, instrument)
