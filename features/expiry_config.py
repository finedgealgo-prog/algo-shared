"""
expiry_config.py
────────────────
Historical expiry-day configuration for Indian index derivatives.

Each instrument has a list of date-range → weekday mappings reflecting
exchange-mandated expiry day changes over time.

Supported instruments
─────────────────────
  NIFTY      : Thursday → Tuesday  (from 2025-09-01)
  BANKNIFTY  : Thursday → Wednesday (from 2023-09-04) → Tuesday (from 2025-09-01)
  FINNIFTY   : Tuesday  (default)
  MIDCPNIFTY : Monday   (default)
  SENSEX     : Tuesday  (from 2023-06-13) → Thursday (from 2025-09-01)

MongoDB collection: expiry_day_config
──────────────────────────────────────
  {underlying, from_date, to_date, weekday}
  Query: find one where underlying matches AND from_date <= date <= to_date

Usage
─────
  from expiry_config import get_expiry_weekday, seed_expiry_config

  weekday = get_expiry_weekday("NIFTY", "2025-10-01")   # → "Tuesday"
  weekday = get_expiry_weekday("NIFTY", "2024-01-15")   # → "Thursday"
"""

from typing import Optional

# ═══════════════════════════════════════════════════════════════════
# 1. IN-MEMORY RULES  (source of truth — seed DB from these)
# ═══════════════════════════════════════════════════════════════════

# Each entry: (from_date_inclusive, to_date_inclusive, weekday_name)
EXPIRY_RULES: dict[str, list[tuple[str, str, str]]] = {
    "NIFTY": [
        ("2000-01-01", "2025-08-31", "Thursday"),
        ("2025-09-01", "2099-12-31", "Tuesday"),
    ],
    "BANKNIFTY": [
        ("2000-01-01", "2023-09-03", "Thursday"),
        ("2023-09-04", "2025-08-31", "Wednesday"),
        ("2025-09-01", "2099-12-31", "Tuesday"),
    ],
    "FINNIFTY": [
        ("2000-01-01", "2099-12-31", "Tuesday"),
    ],
    "MIDCPNIFTY": [
        ("2000-01-01", "2099-12-31", "Monday"),
    ],
    "SENSEX": [
        ("2023-06-13", "2025-08-31", "Tuesday"),
        ("2025-09-01", "2099-12-31", "Thursday"),
    ],
    "BANKEX": [
        ("2023-06-13", "2099-12-31", "Monday"),
    ],
}

_DEFAULT_WEEKDAY = "Thursday"   # fallback for any instrument not listed above


# ═══════════════════════════════════════════════════════════════════
# 2. IN-MEMORY LOOKUP
# ═══════════════════════════════════════════════════════════════════

def get_expiry_weekday_from_rules(rules: list, date: str) -> str:
    """
    Lookup weekday from a pre-loaded rules list (from DB or in-memory).

    `rules` is a list of (from_date, to_date, weekday) tuples.
    Used during backtest — rules loaded once, this called per trading day.

    Returns "Thursday" as default if no rule matches.
    """
    for from_dt, to_dt, weekday in rules:
        if from_dt <= date <= to_dt:
            return weekday
    return _DEFAULT_WEEKDAY


def get_expiry_weekday(underlying: str, date: str) -> str:
    """
    Return the expiry weekday name for `underlying` on `date` (YYYY-MM-DD).

    Uses the in-memory EXPIRY_RULES dict — no DB call needed for backtests.
    Returns "Thursday" as default for unknown instruments.

    Examples
    ────────
      get_expiry_weekday("NIFTY", "2024-06-01")   → "Thursday"
      get_expiry_weekday("NIFTY", "2025-09-15")   → "Tuesday"
      get_expiry_weekday("BANKNIFTY", "2024-01-10")  → "Wednesday"
    """
    rules = EXPIRY_RULES.get(underlying, [])
    for from_dt, to_dt, weekday in rules:
        if from_dt <= date <= to_dt:
            return weekday
    return _DEFAULT_WEEKDAY


# ═══════════════════════════════════════════════════════════════════
# 3. MONGODB SEED  (run once to populate DB from in-memory rules)
# ═══════════════════════════════════════════════════════════════════

def seed_expiry_config(db) -> int:
    """
    Seed the `expiry_day_config` MongoDB collection from EXPIRY_RULES.

    Idempotent — drops existing docs for listed instruments before inserting.

    Parameters
    ──────────
      db : MongoData instance (must have ._db attribute pointing to pymongo DB)

    Returns number of documents inserted.
    """
    coll = db._db["expiry_day_config"]

    docs = []
    for underlying, rules in EXPIRY_RULES.items():
        coll.delete_many({"underlying": underlying})   # clear stale docs
        for from_dt, to_dt, weekday in rules:
            docs.append({
                "underlying": underlying,
                "from_date":  from_dt,
                "to_date":    to_dt,
                "weekday":    weekday,
            })

    if docs:
        coll.insert_many(docs)
        # Index for fast range queries
        coll.create_index([("underlying", 1), ("from_date", 1), ("to_date", 1)])

    return len(docs)


# ═══════════════════════════════════════════════════════════════════
# 4. MONGODB LOOKUP  (alternative to in-memory for live systems)
# ═══════════════════════════════════════════════════════════════════

def get_expiry_weekday_from_db(db, underlying: str, date: str) -> str:
    """
    Fetch expiry weekday from MongoDB `expiry_day_config` collection.

    Falls back to in-memory lookup if no DB document is found.
    Use this in live/paper-trading systems where DB is the authority.
    """
    coll = db._db["expiry_day_config"]
    doc  = coll.find_one({
        "underlying": underlying,
        "from_date":  {"$lte": date},
        "to_date":    {"$gte": date},
    })
    if doc:
        return doc["weekday"]
    return get_expiry_weekday(underlying, date)   # fallback to in-memory
