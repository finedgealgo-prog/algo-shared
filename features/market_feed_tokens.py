"""
market_feed_tokens.py
─────────────────────
DB-backed spot index token registry for all brokers.
No tokens are hardcoded in code — everything lives in MongoDB collection
`market_feed_tokens`.

Document structure:
  {
    "broker":     "kite" | "dhan",
    "underlying": "NIFTY",
    "token":      "256265",     ← broker-specific token / security ID
    "exchange":   "NSE",
    "type":       "spot" | "vix"
  }

Seed data (inserted once at startup if collection is empty for a broker):
  Kite : NIFTY=256265  BANKNIFTY=260105  FINNIFTY=257801
         SENSEX=265    MIDCPNIFTY=288009  VIX=264969
  Dhan : NIFTY=13      BANKNIFTY=25      FINNIFTY=27
         SENSEX=51     MIDCPNIFTY=11915  VIX=20225
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)

COLLECTION = "market_feed_tokens"

# ── Default seed data ─────────────────────────────────────────────────────────
_SEED: dict[str, list[dict]] = {
    "kite": [
        {"underlying": "NIFTY",      "token": "256265", "exchange": "NSE", "type": "spot"},
        {"underlying": "BANKNIFTY",  "token": "260105", "exchange": "NSE", "type": "spot"},
        {"underlying": "FINNIFTY",   "token": "257801", "exchange": "NSE", "type": "spot"},
        {"underlying": "SENSEX",     "token": "265",    "exchange": "BSE", "type": "spot"},
        {"underlying": "MIDCPNIFTY", "token": "288009", "exchange": "NSE", "type": "spot"},
        {"underlying": "BANKEX",     "token": "274441", "exchange": "BSE", "type": "spot"},
        {"underlying": "INDIAVIX",   "token": "264969", "exchange": "NSE", "type": "vix"},
    ],
    "dhan": [
        {"underlying": "NIFTY",      "token": "13",     "exchange": "IDX_I", "type": "spot"},
        {"underlying": "BANKNIFTY",  "token": "25",     "exchange": "IDX_I", "type": "spot"},
        {"underlying": "FINNIFTY",   "token": "27",     "exchange": "IDX_I", "type": "spot"},
        {"underlying": "SENSEX",     "token": "51",     "exchange": "IDX_I", "type": "spot"},
        {"underlying": "MIDCPNIFTY", "token": "11915",  "exchange": "IDX_I", "type": "spot"},
        {"underlying": "BANKEX",     "token": "69",     "exchange": "IDX_I", "type": "spot"},
        {"underlying": "INDIAVIX",   "token": "20225",  "exchange": "IDX_I", "type": "vix"},
    ],
}


def ensure_seeded(db) -> None:
    """Insert default tokens for each broker if collection is empty for that broker."""
    col = db[COLLECTION]
    for broker, rows in _SEED.items():
        if col.count_documents({"broker": broker}) == 0:
            docs = [{"broker": broker, **r} for r in rows]
            col.insert_many(docs)
            logger.info("[market_feed_tokens] seeded %d tokens for broker=%s", len(docs), broker)


def get_spot_tokens(db, broker: str) -> dict[str, str]:
    """
    Returns {token: underlying_name} for all spot-type tokens of a broker.
    e.g. {"256265": "NIFTY", "260105": "BANKNIFTY", ...}
    """
    ensure_seeded(db)
    col  = db[COLLECTION]
    docs = col.find({"broker": broker, "type": "spot"}, {"token": 1, "underlying": 1, "_id": 0})
    return {str(d["token"]): str(d["underlying"]).upper() for d in docs if d.get("token")}


def get_vix_token(db, broker: str) -> str:
    """Returns the VIX token/security_id string for a broker."""
    ensure_seeded(db)
    col = db[COLLECTION]
    doc = col.find_one({"broker": broker, "type": "vix"}, {"token": 1, "_id": 0}) or {}
    return str(doc.get("token") or "")


def get_all_spot_token_ids(db, broker: str) -> list[str]:
    """Returns flat list of token strings for spot + vix (for WebSocket subscription)."""
    ensure_seeded(db)
    col  = db[COLLECTION]
    docs = col.find({"broker": broker}, {"token": 1, "_id": 0})
    return [str(d["token"]) for d in docs if d.get("token")]


def get_active_feed_broker(db) -> str:
    """
    Returns the active market feed broker ('kite' or 'dhan').
    Pass either a MongoData instance or a raw pymongo db.
    """
    try:
        raw = db._db if hasattr(db, '_db') else db
        cfg = raw['kite_market_config'].find_one({"enabled": True}, {"broker": 1}) or {}
        return str(cfg.get("broker") or "kite").strip().lower()
    except Exception:
        return "kite"


def active_token_broker_filter(db) -> dict:
    """Returns {'broker': 'kite'} or {'broker': 'dhan'} for active_option_tokens queries."""
    return {"broker": get_active_feed_broker(db)}


def get_vix_token_id() -> str:
    """
    Convenience: returns VIX token for the currently active broker.
    Falls back to Kite VIX token if DB unavailable.
    """
    try:
        from features.mongo_data import MongoData
        _db = MongoData()
        cfg = _db._db["kite_market_config"].find_one({"enabled": True}, {"broker": 1}) or {}
        broker = str(cfg.get("broker") or "kite").lower()
        token  = get_vix_token(_db._db, broker)
        _db.close()
        return token
    except Exception:
        return "264969"  # kite fallback only if DB is unreachable
