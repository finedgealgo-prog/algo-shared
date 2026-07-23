"""
async_mongo.py
───────────────
Async (motor) MongoDB client — used ONLY by the new async entry-processing
path (async_entry_engine.py), which currently runs for fast-forward mode
only. Every other part of the codebase keeps using the synchronous
MongoData/pymongo client (mongo_data.py) — this is a deliberately separate,
parallel client, not a replacement.

Why separate instead of migrating mongo_data.py itself: MongoData/pymongo is
used in 85+ files and 500+ call sites across this repo. A system-wide swap
to motor is not a scoped, safely-testable change. Running a small async
client alongside the existing sync one — same URI, same database, same
collections — lets the new low-latency entry path adopt async without
touching (or risking) anything else.

Connection pool is sized independently from mongo_data.py's pymongo pool
(see MongoData.__init__, maxPoolSize=150) since the two clients don't share
a pool — keep the two roughly in the same ballpark so neither starves the
DB server of total connections when both are under load at once.
"""

from __future__ import annotations

import threading

from motor.motor_asyncio import AsyncIOMotorClient

from features.mongo_data import MONGO_URI, DB_NAME  # reuse the exact same URI/DB as the sync client

_client_lock = threading.Lock()
_client: AsyncIOMotorClient | None = None


def get_async_client() -> AsyncIOMotorClient:
    """
    Returns the process-wide singleton AsyncIOMotorClient.

    Motor clients must be created on (and only used from) the event loop
    that will run their operations — this is fine here because the async
    entry engine owns one dedicated event loop for the process lifetime
    (see async_entry_engine.py's _get_loop()), and this client is only ever
    touched from coroutines running on that loop.
    """
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                _client = AsyncIOMotorClient(
                    MONGO_URI,
                    serverSelectionTimeoutMS=5000,
                    appname="option-algo-async",
                    maxPoolSize=150,
                    minPoolSize=10,
                )
    return _client


def get_async_db():
    """Returns the async database handle — same DB_NAME as the sync client."""
    return get_async_client()[DB_NAME]
