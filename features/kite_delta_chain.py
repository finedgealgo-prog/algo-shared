"""
kite_delta_chain.py
───────────────────
Fetch and log the live Kite option chain with Greeks (delta, IV, theta, gamma)
for delta-based entry types: EntryByDelta and EntryByDeltaRange.

Kite's quote() API does not return Greeks, so they are calculated using the
Black-Scholes model: IV is derived from the option LTP via bisection, then
delta/gamma/theta are computed from IV.

Public API:
  fetch_and_log_delta_option_chain(...)        — log only (backward compat)
  fetch_log_and_select_delta_strike(...)       — fetch → log → select in one pass
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone, timedelta
from typing import Any

log = logging.getLogger(__name__)

_DELTA_ENTRY_TYPES = frozenset({
    'EntryType.EntryByDelta',
    'EntryType.EntryByDeltaRange',
})

_IST = timezone(timedelta(hours=5, minutes=30))
_RISK_FREE_RATE = 0.068          # India 91-day T-bill ≈ 6.8% p.a.
_MIN_T = 1 / (365 * 24 * 60)    # 1 minute minimum to avoid div-by-zero

# Continuous dividend yields per index — same as Kite/Sensibull reference values
_DIVIDEND_YIELDS: dict[str, float] = {
    'NIFTY':      0.012,   # ~1.2%
    'BANKNIFTY':  0.005,   # ~0.5%
    'FINNIFTY':   0.015,   # ~1.5%
    'SENSEX':     0.012,   # ~1.2%
    'MIDCPNIFTY': 0.008,   # ~0.8%
}
_DEFAULT_DIVIDEND_YIELD = 0.01


# ── Black-Scholes helpers ─────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _bs_price(S: float, K: float, T: float, r: float, sigma: float, opt: str, q: float = 0.0) -> float:
    """Black-Scholes-Merton option price with continuous dividend yield q."""
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if opt == 'CE' else (K - S))
    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    exp_qT = math.exp(-q * T)
    exp_rT = math.exp(-r * T)
    if opt == 'CE':
        return S * exp_qT * _norm_cdf(d1) - K * exp_rT * _norm_cdf(d2)
    return K * exp_rT * _norm_cdf(-d2) - S * exp_qT * _norm_cdf(-d1)


def _calc_iv(ltp: float, S: float, K: float, T: float, r: float, opt: str, q: float = 0.0) -> float:
    """Implied volatility via bisection.  Returns annual vol (e.g. 0.15 = 15%)."""
    if ltp <= 0 or S <= 0 or K <= 0 or T <= 0:
        return 0.0
    intrinsic = max(0.0, (S - K) if opt == 'CE' else (K - S))
    if ltp < intrinsic:
        return 0.0
    lo, hi = 1e-5, 20.0    # 0.001 % to 2000 % annual vol
    for _ in range(120):
        mid = (lo + hi) * 0.5
        price = _bs_price(S, K, T, r, mid, opt, q)
        if abs(price - ltp) < 0.001:
            return mid
        if price < ltp:
            lo = mid
        else:
            hi = mid
    return (lo + hi) * 0.5


def _calc_greeks(S: float, K: float, T: float, r: float, sigma: float, opt: str, q: float = 0.0) -> dict:
    """Return delta, gamma, theta, vega — Black-Scholes-Merton with dividend yield q."""
    if sigma <= 0 or T <= 0 or S <= 0 or K <= 0:
        return {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0}
    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    nd1 = _norm_pdf(d1)
    exp_rT = math.exp(-r * T)
    exp_qT = math.exp(-q * T)

    gamma = exp_qT * nd1 / (S * sigma * sqrt_T)
    vega  = S * exp_qT * nd1 * sqrt_T / 100.0   # per 1% change in IV

    if opt == 'CE':
        delta = exp_qT * _norm_cdf(d1)
        theta = (
            -(S * nd1 * sigma * exp_qT) / (2.0 * sqrt_T)
            + q * S * exp_qT * _norm_cdf(d1)
            - r * K * exp_rT * _norm_cdf(d2)
        ) / 365.0
    else:
        delta = exp_qT * (_norm_cdf(d1) - 1.0)
        theta = (
            -(S * nd1 * sigma * exp_qT) / (2.0 * sqrt_T)
            - q * S * exp_qT * _norm_cdf(-d1)
            + r * K * exp_rT * _norm_cdf(-d2)
        ) / 365.0

    return {
        'delta': round(delta, 4),
        'gamma': round(gamma, 6),
        'theta': round(theta, 4),
        'vega':  round(vega,  4),
    }


def _time_to_expiry(expiry_str: str) -> float:
    """Fraction of a year from now (IST) to market close (15:30) on expiry_str."""
    try:
        exp_day = datetime.fromisoformat(expiry_str[:10])
        exp_close = exp_day.replace(hour=15, minute=30, tzinfo=_IST)
        now_ist   = datetime.now(_IST)
        secs = (exp_close - now_ist).total_seconds()
        return max(_MIN_T, secs / (365.0 * 86400))
    except Exception:
        return _MIN_T


# ── credential helper ─────────────────────────────────────────────────────────

def _get_kite_credentials(db) -> tuple[str, str]:
    """(api_key, access_token) — prefers kite_broker_ws cache, falls back to DB."""
    try:
        from features.kite_broker_ws import get_common_credentials, is_configured  # type: ignore
        if is_configured():
            return get_common_credentials()
    except Exception:
        pass
    try:
        doc = db._db['kite_market_config'].find_one({'enabled': True}) or {}
        api_key      = str(doc.get('api_key')      or '').strip()
        access_token = str(doc.get('access_token') or '').strip()
        if api_key and access_token:
            return api_key, access_token
    except Exception as exc:
        log.warning('[DELTA CHAIN] kite_market_config read error: %s', exc)
    return '', ''


def _parse_strike_param_dict(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        import ast
        try:
            val = ast.literal_eval(raw)
            if isinstance(val, dict):
                return val
        except Exception:
            pass
    return {}


def _is_sell_position(position: str) -> bool:
    return str(position or '').strip().lower() in {'sell', 'short', 'positiontype.sell', 'positiontype.short'}


# ── core fetch + compute + log (returns rows with token included) ─────────────

def _fetch_compute_and_log(
    *,
    db,
    underlying: str,
    expiry: str,
    option_type: str,
    entry_kind: str,
    leg_id: str,
    spot_price: float,
) -> list[dict]:
    """
    Fetch live quotes, compute Black-Scholes Greeks, print the chain table.
    Returns a list of row dicts (each includes 'token', 'symbol', 'strike',
    'ltp', 'delta').  Returns [] on any fatal error.
    """
    api_key, access_token = _get_kite_credentials(db)
    if not api_key or not access_token:
        print(f'[DELTA CHAIN] leg={leg_id} no Kite credentials — skipping chain fetch')
        return []

    # ── resolve spot price if not passed ─────────────────────────────────────
    if spot_price <= 0:
        try:
            from features.kite_broker_ws import get_ltp_map  # type: ignore
            ltp_map = get_ltp_map() or {}
            _spot_tokens = {
                'NIFTY': '256265', 'BANKNIFTY': '260105',
                'FINNIFTY': '257801', 'SENSEX': '265', 'MIDCPNIFTY': '288009',
            }
            spot_price = float(ltp_map.get(_spot_tokens.get(underlying, ''), 0) or 0)
        except Exception:
            pass

    T = _time_to_expiry(expiry)
    r = _RISK_FREE_RATE

    # ── get instrument list ───────────────────────────────────────────────────
    instruments: list[tuple[float, int, str]] = []  # (strike, token, symbol)
    try:
        from features.spot_atm_utils import _load_kite_instruments
        cache = _load_kite_instruments()
        for (name, exp, stk, typ), info in cache.items():
            if name == underlying and exp == expiry and typ == option_type:
                instruments.append((float(stk), int(info['token']), str(info.get('symbol', ''))))
    except Exception as exc:
        log.warning('[DELTA CHAIN] leg=%s instrument cache error: %s', leg_id, exc)

    # fallback: call kite.instruments() directly
    if not instruments:
        try:
            import datetime as _dt
            from kiteconnect import KiteConnect  # type: ignore
            _k = KiteConnect(api_key=api_key)
            _k.set_access_token(access_token)
            exp_date = _dt.date.fromisoformat(expiry)
            for seg in ('NFO', 'BFO'):
                try:
                    raw = _k.instruments(seg) or []
                except Exception:
                    continue
                for inst in raw:
                    if (
                        str(inst.get('name') or '').strip().upper() == underlying
                        and str(inst.get('instrument_type') or '').strip().upper() == option_type
                        and inst.get('expiry') == exp_date
                    ):
                        instruments.append((
                            float(inst.get('strike') or 0),
                            int(inst.get('instrument_token') or 0),
                            str(inst.get('tradingsymbol') or ''),
                        ))
        except Exception as exc:
            log.warning('[DELTA CHAIN] leg=%s instruments() fallback error: %s', leg_id, exc)

    if not instruments:
        print(
            f'[DELTA CHAIN] leg={leg_id} underlying={underlying} expiry={expiry} '
            f'type={option_type} — no instruments found, skipping'
        )
        return []

    # ── fetch quotes (LTP, OI, volume) via Kite REST ─────────────────────────
    from kiteconnect import KiteConnect  # type: ignore
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    tokens = [tok for _stk, tok, _sym in instruments]
    token_to_quote: dict[str, dict] = {}
    for i in range(0, len(tokens), 500):
        try:
            quotes = kite.quote(tokens[i:i + 500]) or {}
            for _sym, q in quotes.items():
                t = str(q.get('instrument_token') or '').strip()
                if t:
                    token_to_quote[t] = q
        except Exception as exc:
            log.warning('[DELTA CHAIN] leg=%s quote batch[%d] error: %s', leg_id, i, exc)

    # ── compute Greeks via Black-Scholes ─────────────────────────────────────
    rows: list[dict] = []
    for stk, tok, sym in instruments:
        q   = token_to_quote.get(str(tok)) or {}
        ltp = float(q.get('last_price') or 0)
        oi  = int(q.get('oi') or 0)
        vol = int(q.get('volume') or 0)

        if spot_price > 0 and ltp > 0:
            iv     = _calc_iv(ltp, spot_price, stk, T, r, option_type)
            greeks = _calc_greeks(spot_price, stk, T, r, iv, option_type)
        else:
            iv     = 0.0
            greeks = {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0}

        rows.append({
            'strike': stk,
            'token':  str(tok),
            'symbol': sym,
            'ltp':    ltp,
            'iv':     round(iv * 100, 2),
            'delta':  greeks['delta'],
            'gamma':  greeks['gamma'],
            'theta':  greeks['theta'],
            'vega':   greeks['vega'],
            'oi':     oi,
            'volume': vol,
        })

    rows.sort(key=lambda r: r['strike'])

    # ── print formatted table ─────────────────────────────────────────────────
    sep = '[DELTA CHAIN] ' + '-' * 105
    print(
        f'\n[DELTA CHAIN LOG] leg={leg_id} entry_kind={entry_kind} '
        f'underlying={underlying} expiry={expiry} type={option_type} '
        f'spot={spot_price} T_years={round(T, 6)} total_strikes={len(rows)}'
    )
    print(sep)
    print(
        f'[DELTA CHAIN] {"Strike":>8}  {"LTP":>8}  {"Delta":>8}  '
        f'{"IV%":>8}  {"Theta":>8}  {"Gamma":>10}  {"Vega":>8}  {"OI":>12}  {"Volume":>10}  Symbol'
    )
    print(sep)
    for r in rows:
        print(
            f'[DELTA CHAIN] {r["strike"]:>8.0f}  {r["ltp"]:>8.2f}  {r["delta"]:>8.4f}  '
            f'{r["iv"]:>8.2f}  {r["theta"]:>8.4f}  {r["gamma"]:>10.6f}  '
            f'{r["vega"]:>8.4f}  {r["oi"]:>12}  {r["volume"]:>10}  {r["symbol"]}'
        )
    print(sep + '\n')

    return rows


def _select_from_rows(
    rows: list[dict],
    *,
    entry_kind: str,
    strike_param_raw: Any,
    option_type: str,
    position: str,
    leg_id: str,
    spot_price: float = 0.0,
) -> dict:
    """
    Select a strike from pre-computed rows based on delta range or closest delta.
    Delegates to delta_selector (shared with backtest path in strike_selector.py).
    Returns the chosen row dict or {}.
    """
    from features.delta_selector import select_closest_delta, select_delta_range

    sp = _parse_strike_param_dict(strike_param_raw)

    if 'DeltaRange' in entry_kind:
        lower_pct = float(sp.get('LowerRange') or 0)
        upper_pct = float(sp.get('UpperRange') or 0)
        chosen = select_delta_range(rows, lower_pct, upper_pct, option_type, position, leg_id, spot_price)
    else:
        # EntryByDelta — closest delta
        target_pct = float(sp.get('DeltaValue') or sp.get('Value') or strike_param_raw or 0)
        chosen = select_closest_delta(rows, target_pct, option_type, leg_id)

    return chosen or {}


# ── public API ────────────────────────────────────────────────────────────────

def fetch_and_log_delta_option_chain(
    *,
    db,
    trade: dict,  # noqa: ARG001 — kept for future broker-specific token lookup
    underlying: str,
    expiry: str,
    option_type: str,
    entry_kind: str,
    leg_id: str = '',
    spot_price: float = 0.0,
) -> None:
    """
    Fetch live Kite option chain, compute Greeks via Black-Scholes, and log it.

    Runs ONLY for EntryByDelta and EntryByDeltaRange entry types.
    Safe to call unconditionally — silently no-ops for other entry types.
    """
    if entry_kind not in _DELTA_ENTRY_TYPES:
        return
    if not underlying or not expiry:
        return

    try:
        _fetch_compute_and_log(
            db=db,
            underlying=str(underlying).strip().upper(),
            expiry=str(expiry).strip()[:10],
            option_type=str(option_type).strip().upper(),
            entry_kind=entry_kind,
            leg_id=leg_id,
            spot_price=float(spot_price or 0),
        )
    except Exception as exc:
        log.warning('[DELTA CHAIN] leg=%s error: %s', leg_id, exc)


def fetch_log_and_select_delta_strike(
    *,
    db,
    underlying: str,
    expiry: str,
    option_type: str,
    entry_kind: str,
    strike_param_raw: Any,
    position: str = '',
    leg_id: str = '',
    spot_price: float = 0.0,
) -> dict:
    """
    Fetch live option chain → log table → select strike by delta in one pass.

    Order: fetch quotes → compute Greeks → print chain → select strike.
    Returns {'strike', 'token', 'symbol', 'ltp', 'delta'} or {} on failure.
    """
    if entry_kind not in _DELTA_ENTRY_TYPES:
        return {}
    if not underlying or not expiry:
        return {}

    opt = str(option_type or '').strip().upper()
    exp = str(expiry or '').strip()[:10]
    und = str(underlying or '').strip().upper()

    try:
        rows = _fetch_compute_and_log(
            db=db,
            underlying=und,
            expiry=exp,
            option_type=opt,
            entry_kind=entry_kind,
            leg_id=leg_id,
            spot_price=float(spot_price or 0),
        )
    except Exception as exc:
        log.warning('[DELTA CHAIN] leg=%s fetch error: %s', leg_id, exc)
        return {}

    if not rows:
        return {}

    try:
        return _select_from_rows(
            rows,
            entry_kind=entry_kind,
            strike_param_raw=strike_param_raw,
            option_type=opt,
            position=position,
            leg_id=leg_id,
            spot_price=float(spot_price or 0),
        )
    except Exception as exc:
        log.warning('[DELTA CHAIN] leg=%s select error: %s', leg_id, exc)
        return {}
