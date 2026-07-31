"""
export_by_date.py
------------------
Simple script version of parquet_export.py — just edit the variables below
and run it. No command-line flags needed.

    python export_by_date.py
"""

from pathlib import Path

from pymongo import MongoClient

from parquet_export import MONGO_URI, export_day

# ──────────────── EDIT THESE ────────────────
DATE = "2026-06-01"           # trade date to export, YYYY-MM-DD
UNDERLYING = "NIFTY"          # or None to export every underlying found on that date
COLLECTION = "option_chain_new"
OUT_DIR = "parquet_data"
# ──────────────────────────────────────────────

if __name__ == "__main__":
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    export_day(client, DATE, UNDERLYING, Path(OUT_DIR), COLLECTION)
    client.close()
