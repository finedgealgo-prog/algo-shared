"""
SPAN parameter loader — NSE + BSE (all index + stock options).

How it works (priority order):
  1. Kite basket margin API    — most accurate, needs valid session
  2. Local SPAN file           — place downloaded .zip/.spn in backend/data/span/
                                 (download from NSE/BSE website manually, every ~5 days)
  3. Calibrated DEFAULTS       — always available, covers all major indices

To refresh:
  1. Download NSEFO_SPAN_DDMMMYYYY.zip from NSE website manually
  2. Download BSEFO_SPAN_DDMMMYYYY.zip from BSE website manually
  3. Place both in:  backend/data/span/
  4. Call GET /algo/span/refresh  →  system auto-loads latest files in that folder

Direct web download is not used — NSE/BSE now restrict SPAN file downloads
to registered member portals.
"""
from __future__ import annotations

import os
import re
import glob
import zipfile
import logging
from typing import Optional

log = logging.getLogger(__name__)

# ─── NSE SPAN parameters (from NSE Clearing official page) ───────────────────
# PSR  = 6σ × √2, capped at stated % of underlying price
# VSR  = 25% of annualized EWMA volatility, min 4% (index) / 10% (stock)
# Calendar spread charge = % of far-month contract value (stored as decimal)
#   inter_month_pct: 0.0175 = 1.75% for index, 0.022 = 2.2% for stocks
# SOMC calibrated from live Kite data (Apr-May 2026)
DEFAULTS: dict[str, dict] = {
    "NIFTY":      {"psr_pct": 0.093, "vsr": 0.04, "somc": 21000, "inter_month_pct": 0.0175},
    "BANKNIFTY":  {"psr_pct": 0.093, "vsr": 0.04, "somc": 45000, "inter_month_pct": 0.0175},
    "FINNIFTY":   {"psr_pct": 0.093, "vsr": 0.04, "somc": 16000, "inter_month_pct": 0.0175},
    "MIDCPNIFTY": {"psr_pct": 0.093, "vsr": 0.04, "somc": 14000, "inter_month_pct": 0.0175},
    "SENSEX":     {"psr_pct": 0.093, "vsr": 0.04, "somc": 21000, "inter_month_pct": 0.0175},
    "BANKEX":     {"psr_pct": 0.093, "vsr": 0.04, "somc": 45000, "inter_month_pct": 0.0175},
}
# Stock options: PSR = 14.2%, VSR min 10%, calendar charge = 2.2%
_STOCK_FALLBACK = {"psr_pct": 0.142, "vsr": 0.10, "somc": 10000, "inter_month_pct": 0.022}

# ─── Local SPAN file directory ────────────────────────────────────────────────
_SPAN_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "span")

# ─── In-memory cache ──────────────────────────────────────────────────────────
_nse_cache: dict[str, dict] = {}
_bse_cache: dict[str, dict] = {}
_nse_date:  str = ""
_bse_date:  str = ""


def get_params(underlying: str) -> dict:
    """
    Return SPAN params for any underlying.
    Lookup order:
      1. NSE/BSE file cache (if file was loaded via /span/upload or /span/refresh)
      2. MongoDB span_params collection (quarterly updated by user)
      3. Hardcoded DEFAULTS (index) / _STOCK_FALLBACK (stocks)
    """
    key = underlying.upper()
    p = (
        _nse_cache.get(key)
        or _bse_cache.get(key)
        or _db_cache.get(key)
        or DEFAULTS.get(key)
        or _STOCK_FALLBACK
    )
    if "inter_month_pct" not in p and "inter_month" in p:
        p = dict(p)
        p["inter_month_pct"] = _STOCK_FALLBACK["inter_month_pct"]
    return p


# ─── MongoDB cache (loaded at startup, refreshed on /span/refresh) ────────────
_db_cache: dict[str, dict] = {}


def load_from_db() -> int:
    """Load SPAN params from MongoDB span_params collection into memory."""
    global _db_cache
    try:
        from features.mongo_data import MongoData
        db = MongoData()
        docs = list(db._db["span_params"].find({}, {"_id": 0}))
        db.close()
        _db_cache = {d["underlying"].upper(): d for d in docs if d.get("underlying")}
        log.info("SPAN params loaded from DB: %d underlyings", len(_db_cache))
        return len(_db_cache)
    except Exception as exc:
        log.warning("SPAN DB load failed: %s", exc)
        return 0


def save_defaults_to_db() -> int:
    """
    Seed MongoDB span_params with DEFAULTS if collection is empty.
    Called at startup — safe to run multiple times (upsert).
    """
    try:
        from features.mongo_data import MongoData
        from datetime import datetime
        db = MongoData()
        col = db._db["span_params"]
        count = 0
        for underlying, params in DEFAULTS.items():
            col.update_one(
                {"underlying": underlying},
                {"$setOnInsert": {
                    "underlying": underlying,
                    "exchange":   "NSE" if underlying not in ("SENSEX", "BANKEX") else "BSE",
                    **params,
                    "source":     "defaults",
                    "updated_at": datetime.utcnow().strftime("%Y-%m-%d"),
                }},
                upsert=True,
            )
            count += 1
        db.close()
        load_from_db()
        log.info("SPAN defaults seeded to DB: %d records", count)
        return count
    except Exception as exc:
        log.warning("SPAN DB seed failed: %s", exc)
        return 0


def get_cache_date() -> str:
    dates = [d for d in [_nse_date, _bse_date] if d]
    return ", ".join(dates) if dates else ""


def is_loaded() -> bool:
    return bool(_nse_cache or _bse_cache)


# ─── Public fetch (reads local files only) ───────────────────────────────────

def fetch_span_file(*_args, **_kwargs) -> bool:
    """
    Refresh SPAN params:
      1. Load from MongoDB span_params (quarterly updated by user)
      2. Load from any files in backend/data/span/ (overrides DB values)
    Returns True if any source loaded successfully.
    """
    span_dir = os.path.abspath(_SPAN_DIR)
    os.makedirs(span_dir, exist_ok=True)

    # DB is always loaded first as the base
    db_count = load_from_db()

    # File-based overrides DB (more up-to-date when file is present)
    nse_ok = _load_latest(span_dir, "NSEFO_SPAN", "NSE")
    bse_ok = _load_latest(span_dir, "BSEFO_SPAN", "BSE")

    return bool(db_count or nse_ok or bse_ok)


def _load_latest(span_dir: str, prefix: str, exchange: str) -> bool:
    """Find the most recent matching file in span_dir and load it."""
    patterns = [
        f"{prefix}_*.zip", f"{prefix}_*.spn",          # NSE: NSEFO_SPAN_*.zip
        "BSERISK*.XML", "BSERISK*.xml",                  # BSE: BSERISK20260522-00.XML
    ] if exchange == "BSE" else [
        f"{prefix}_*.zip", f"{prefix}_*.spn",
    ]

    candidates = []
    for pat in patterns:
        candidates += glob.glob(os.path.join(span_dir, pat))
    candidates = sorted(set(candidates), reverse=True)

    for path in candidates:
        fname = os.path.basename(path)
        # Extract date from filename
        m = (re.search(r"BSERISK(\d{8})", fname, re.IGNORECASE) or
             re.search(r"_(\d{2}[A-Z]{3}\d{4})", fname, re.IGNORECASE))
        date_str = m.group(1).upper() if m else fname

        try:
            ext = os.path.splitext(fname)[1].lower()

            if ext == ".zip":
                with zipfile.ZipFile(path) as zf:
                    inner = next((n for n in zf.namelist()
                                  if n.lower().endswith((".spn", ".xml"))), None)
                    if not inner:
                        continue
                    raw = zf.read(inner).decode("latin-1", errors="ignore")
                    inner_ext = os.path.splitext(inner)[1].lower()
            elif ext == ".xml":
                with open(path, encoding="latin-1", errors="ignore") as f:
                    raw = f.read()
                inner_ext = ".xml"
            else:
                with open(path, encoding="latin-1", errors="ignore") as f:
                    raw = f.read()
                inner_ext = ".spn"

            parsed = (
                _parse_bse_xml(raw) if inner_ext == ".xml"
                else _parse_spn(raw, exchange)
            )
            if not parsed:
                continue

            global _nse_cache, _bse_cache, _nse_date, _bse_date
            if exchange == "NSE":
                _nse_cache, _nse_date = parsed, date_str
            else:
                _bse_cache, _bse_date = parsed, date_str

            log.info("%s SPAN loaded from %s: %d underlyings", exchange, fname, len(parsed))
            return True

        except Exception as exc:
            log.warning("%s: failed to read %s: %s", exchange, fname, exc)
            continue

    return False


# ─── .spn parser ─────────────────────────────────────────────────────────────

def _parse_spn(content: str, exchange: str) -> dict[str, dict]:
    """
    Parse NSE/BSE SPAN .spn file (CME SPAN 4, NSCCL/ICCL variant).

    Record type "81" = Combined Commodity (one row per underlying/stock).
    Fixed-width ASCII fields (0-indexed byte positions):
      [0:2]   "81"
      [2:7]   exchange acronym
      [7:17]  underlying symbol (index or stock ticker)
      [17:20] currency
      [20:27] PSR absolute (rupees per unit)
      [27:34] VSR (decimal × 10000)
      [34:41] weight
      [41:48] settlement price
      [48:55] SOMC flag/rate
      [55:62] SOMC per lot (rupees)
      [62:69] inter-month spread charge per lot (rupees)
    """
    results: dict[str, dict] = {}

    for line in content.splitlines():
        if len(line) < 20 or not line.startswith("81"):
            continue
        try:
            underlying = line[7:17].strip().upper()
            # Keep letters + & only (e.g. "M&M", "L&TFH" are valid NSE tickers)
            if not underlying or not re.fullmatch(r"[A-Z&]+", underlying):
                continue

            def _f(s: int, e: int) -> Optional[float]:
                seg = line[s:e].strip() if len(line) >= e else ""
                try:
                    return float(seg) if seg else None
                except ValueError:
                    return None

            psr_raw   = _f(20, 27)
            vsr_raw   = _f(27, 34)
            somc_raw  = _f(55, 62)
            inter_raw = _f(62, 69)

            psr   = psr_raw   if psr_raw   and 100    < psr_raw   < 100_000  else None
            somc  = somc_raw  if somc_raw  and 1_000  < somc_raw  < 2_000_000 else None
            inter = inter_raw if inter_raw and 0      < inter_raw < 200_000   else None

            vsr = None
            if vsr_raw is not None:
                vsr = (vsr_raw / 10000) if vsr_raw > 1 else vsr_raw
                if not (0.005 < vsr < 0.5):
                    vsr = None

            base = DEFAULTS.get(underlying, _STOCK_FALLBACK)
            results[underlying] = {
                "psr_abs":     psr,
                "psr_pct":     base.get("psr_pct",     _STOCK_FALLBACK["psr_pct"]),
                "vsr":         vsr   or base.get("vsr",         0.04),
                "somc":        somc  or base.get("somc",        _STOCK_FALLBACK["somc"]),
                "inter_month": inter or base.get("inter_month", _STOCK_FALLBACK["inter_month"]),
                "from_file":   True,
                "exchange":    exchange,
            }
        except Exception:
            continue

    return results


# ─── BSE XML parser ───────────────────────────────────────────────────────────

# BSE pfCode → standard underlying name used in our system
_BSE_CODE_MAP: dict[str, str] = {
    "BSX":  "SENSEX",
    "BKX":  "BANKEX",
    "SX50": "BSESENSEX50",
    "BIT":  "BSEIT",
}

def _parse_bse_xml(content: str) -> dict[str, dict]:
    """
    Parse BSE SPAN XML file (BSERISK*.XML, SPAN 4 XML format).

    Structure:
      <phyPf>
        <pfCode>BSX</pfCode>        ← underlying code
        <name>BSE 30 SENSEX</name>
        <phy>
          <scanRate>
            <priceScan>6996.21</priceScan>  ← PSR absolute (rupees)
            <volScan>0.04</volScan>          ← VSR decimal
          </scanRate>
        </phy>
      </phyPf>
    """
    import xml.etree.ElementTree as ET

    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        log.warning("BSE XML parse error: %s", exc)
        return {}

    results: dict[str, dict] = {}

    for pf in root.iter("phyPf"):
        try:
            code_el = pf.find("pfCode")
            if code_el is None:
                continue
            bse_code = (code_el.text or "").strip().upper()

            # Map known BSE index codes; keep all others as-is for stock options
            underlying = _BSE_CODE_MAP.get(bse_code, bse_code)
            if not underlying or not re.fullmatch(r"[A-Z0-9&]+", underlying):
                continue

            phy  = pf.find("phy")
            if phy is None:
                continue
            scan = phy.find("scanRate")
            if scan is None:
                continue

            psr_text = (scan.findtext("priceScan") or "").strip()
            vsr_text = (scan.findtext("volScan")   or "").strip()

            psr = float(psr_text) if psr_text else None
            vsr = float(vsr_text) if vsr_text else None

            if psr is None or psr <= 0:
                continue
            if vsr is not None and not (0.005 < vsr < 0.5):
                vsr = None

            base = DEFAULTS.get(underlying, _STOCK_FALLBACK)
            results[underlying] = {
                "psr_abs":         psr,
                "psr_pct":         base.get("psr_pct",         _STOCK_FALLBACK["psr_pct"]),
                "vsr":             vsr  or base.get("vsr",             0.04),
                "somc":            base.get("somc",            _STOCK_FALLBACK["somc"]),
                "inter_month_pct": base.get("inter_month_pct", _STOCK_FALLBACK["inter_month_pct"]),
                "from_file":       True,
                "exchange":        "BSE",
            }
        except Exception:
            continue

    return results
