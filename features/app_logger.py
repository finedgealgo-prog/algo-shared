"""
app_logger.py
─────────────
Centralised logging setup for the algo trading backend.

Two log files (daily rotating, kept for 30 days):
  logs/app.log      – all INFO+ messages from every module
  logs/error.log    – WARNING+ only (quick scan for failures)

Call setup_logging() once at app startup (api.py lifespan).
After that, every module's standard `logging.getLogger(__name__)` call
automatically writes to both file and console.

DB-activity helpers
-------------------
  log_db_write(collection, operation, doc_id, extra)  → structured DB log
  log_db_error(collection, operation, error, extra)   → structured DB error log

Import from anywhere:
  from features.app_logger import log_db_write, log_db_error
"""

from __future__ import annotations

import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path
from typing import Any

# ── Log directory (relative to this file → backend/logs/) ─────────────────────
_LOG_DIR = Path(__file__).resolve().parent.parent / "logs"

# ── Module-level DB activity logger ───────────────────────────────────────────
_db_log = logging.getLogger("db_activity")


def setup_logging() -> None:
    """
    Call once at app startup.
    Configures root logger + db_activity logger with file + console handlers.
    """
    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        fmt="%(asctime)s  %(levelname)-8s  %(name)-35s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── app.log — all INFO+ ───────────────────────────────────────────────────
    app_handler = logging.handlers.TimedRotatingFileHandler(
        filename=str(_LOG_DIR / "app.log"),
        when="midnight",
        backupCount=30,
        encoding="utf-8",
        utc=False,
    )
    app_handler.setLevel(logging.INFO)
    app_handler.setFormatter(fmt)

    # ── error.log — WARNING+ only ─────────────────────────────────────────────
    err_handler = logging.handlers.TimedRotatingFileHandler(
        filename=str(_LOG_DIR / "error.log"),
        when="midnight",
        backupCount=30,
        encoding="utf-8",
        utc=False,
    )
    err_handler.setLevel(logging.WARNING)
    err_handler.setFormatter(fmt)

    # ── db_activity.log — all DB operations ──────────────────────────────────
    db_handler = logging.handlers.TimedRotatingFileHandler(
        filename=str(_LOG_DIR / "db_activity.log"),
        when="midnight",
        backupCount=30,
        encoding="utf-8",
        utc=False,
    )
    db_handler.setLevel(logging.DEBUG)
    db_handler.setFormatter(fmt)

    # ── console handler (INFO+) ───────────────────────────────────────────────
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt)

    # ── Root logger ───────────────────────────────────────────────────────────
    root = logging.getLogger()
    if root.handlers:
        root.handlers.clear()
    root.setLevel(logging.DEBUG)
    root.addHandler(app_handler)
    root.addHandler(err_handler)
    root.addHandler(console_handler)

    # ── DB activity logger (separate file) ───────────────────────────────────
    _db_log.setLevel(logging.DEBUG)
    _db_log.addHandler(db_handler)
    _db_log.propagate = True   # also flows to app.log

    # Suppress noisy third-party loggers
    for noisy in ("pymongo", "urllib3", "websockets", "asyncio", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logging.getLogger(__name__).info(
        "Logging initialised — log dir: %s", _LOG_DIR
    )


# ── Public helpers ─────────────────────────────────────────────────────────────

def log_db_write(
    collection: str,
    operation: str,
    doc_id: Any = None,
    extra: dict | None = None,
) -> None:
    """
    Log a DB write operation (insert / update / delete / upsert).

    Usage:
        log_db_write("algo_trades", "update_one", trade_id, {"reason": "exit_time"})
    """
    parts = [f"col={collection}", f"op={operation}"]
    if doc_id is not None:
        parts.append(f"id={doc_id}")
    if extra:
        for k, v in extra.items():
            parts.append(f"{k}={v}")
    _db_log.info("[DB WRITE]  %s", "  ".join(parts))


def log_db_error(
    collection: str,
    operation: str,
    error: Exception,
    doc_id: Any = None,
    extra: dict | None = None,
) -> None:
    """
    Log a DB error with full traceback context.

    Usage:
        log_db_error("algo_trades", "update_one", exc, trade_id)
    """
    parts = [f"col={collection}", f"op={operation}"]
    if doc_id is not None:
        parts.append(f"id={doc_id}")
    if extra:
        for k, v in extra.items():
            parts.append(f"{k}={v}")
    _db_log.error("[DB ERROR]  %s  error=%s", "  ".join(parts), error, exc_info=True)
