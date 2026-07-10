"""
kite_broker_ws.py
─────────────────
Shared Kite (Zerodha) WebSocket service — ONE common account for all users.

Architecture:
  • Single KiteTicker connection shared by every user on the platform.
  • Credentials (api_key, access_token) stored in MongoDB → kite_market_config.
  • All users' open-position tokens merged into one subscription set.
  • Per-user filtering: each user's WebSocket only receives LTP for their own tokens.

MongoDB document (collection: kite_market_config):
  {
    "api_key":      "abc123",
    "access_token": "xyz789",   ← update this daily via POST /kite/config
    "enabled":      true
  }

Public API:
  set_common_credentials(api_key, access_token)   ← call from API endpoint
  load_credentials_from_db(db)                    ← call on app startup / daily refresh
  get_common_api_key()                            ← returns api_key or ''
  register_user_tokens(user_id, tokens)           ← uses stored common key
  unregister_user(user_id)
  refresh_user_tokens(user_id, new_tokens)
  add_tick_listener(listener)
  remove_tick_listener(listener)
  get_ltp_map()
  extract_instrument_tokens(positions)
  stop_all()

Tick listener signature:
  def listener(ltp_map: dict[str, float]) -> None
  where ltp_map = { str(instrument_token): last_price }
"""

from __future__ import annotations

import logging
import re
import threading
import time
from collections import defaultdict
from typing import Callable

from features.debug_flags import runtime_print

log = logging.getLogger(__name__)

_KITE_MONTH_MAP = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
    '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6,
    '7': 7, '8': 8, '9': 9, 'O': 10, 'N': 11, 'D': 12,
}
_KITE_MONTHLY_RE = re.compile(r'^([A-Z]+)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+(?:\.\d+)?)(CE|PE)$')
_KITE_WEEKLY_RE = re.compile(r'^([A-Z]+)(\d{2})([1-9OND])(\d{1,2})(\d+(?:\.\d+)?)(CE|PE)$')


def _trace_stdout(message: str) -> None:
    """Print websocket/runtime diagnostics immediately to backend stdout."""
    runtime_print(message, flush=True)

KITE_CONFIG_COLLECTION = 'kite_market_config'

# ─── Hardcoded app credentials (permanent — do not expire) ───────────────────
# These are the Kite Connect app credentials.
# access_token is generated daily via OAuth and stored in MongoDB.
DEFAULT_API_KEY    = 'h283rtgbdfbdwr3f'
DEFAULT_API_SECRET = '52b40zgix98vbjxstkp633rxy98eabhy'

# ─── Type alias ───────────────────────────────────────────────────────────────
TickListener = Callable[[dict[str, float]], None]


# ─── Common credentials store ─────────────────────────────────────────────────

_cred_lock = threading.Lock()
_common_api_key: str = DEFAULT_API_KEY
_common_api_secret: str = DEFAULT_API_SECRET
_common_access_token: str = ''
_token_label_cache_lock = threading.Lock()
_token_label_cache_day = ''
_token_label_cache: dict[str, str] = {}


def _ist_today() -> str:
    from datetime import datetime, timezone, timedelta
    return (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d')


def _build_token_label_cache() -> dict[str, str]:
    """
    Best-effort token -> human-readable label cache.

    Priority:
      1. Hardcoded index token names
      2. Kite instruments trading symbols for option tokens
    """
    global _token_label_cache_day, _token_label_cache

    today = _ist_today()
    with _token_label_cache_lock:
        if _token_label_cache_day == today and _token_label_cache:
            return _token_label_cache

        labels: dict[str, str] = {
            '256265': 'NIFTY',
            '260105': 'BANKNIFTY',
            '265': 'SENSEX',
            '257801': 'FINNIFTY',
            '288009': 'MIDCPNIFTY',
        }

        try:
            from features.spot_atm_utils import _load_kite_instruments  # type: ignore

            for _key, inst in (_load_kite_instruments() or {}).items():
                token = str(inst.get('token') or '').strip()
                symbol = str(inst.get('symbol') or '').strip()
                if token and symbol and token not in labels:
                    labels[token] = symbol
        except Exception:
            pass

        _token_label_cache = labels
        _token_label_cache_day = today
        return _token_label_cache


def _describe_token(token: str) -> str:
    token_str = str(token or '').strip()
    if not token_str:
        return '-'
    labels = _build_token_label_cache()
    return labels.get(token_str, token_str)


def set_common_credentials(api_key: str, access_token: str) -> None:
    """
    Set the common Kite credentials at runtime.
    Call this from the API endpoint when access_token is updated daily.
    Restarts the KiteTicker connection if credentials changed.
    """
    global _common_api_key, _common_access_token
    api_key = str(api_key or '').strip()
    access_token = str(access_token or '').strip()
    if not api_key or not access_token:
        log.warning('[kite_broker_ws] set_common_credentials: empty credentials ignored')
        return
    with _cred_lock:
        changed = (api_key != _common_api_key or access_token != _common_access_token)
        _common_api_key = api_key
        _common_access_token = access_token

    if changed and _manager is not None:
        log.info('[kite_broker_ws] credentials updated — restarting KiteTicker')
        _manager.restart(api_key, access_token)

    log.info('[kite_broker_ws] common credentials set api_key=%s', api_key)


def load_credentials_from_db(db) -> bool:
    """
    Load api_key + api_secret + access_token from MongoDB kite_market_config.
    Falls back to hardcoded DEFAULT_API_KEY / DEFAULT_API_SECRET if not in DB.
    Returns True if a usable access_token was found and set.
    """
    global _common_api_key, _common_api_secret
    try:
        doc = db._db[KITE_CONFIG_COLLECTION].find_one({'enabled': True})
        if not doc:
            doc = db._db[KITE_CONFIG_COLLECTION].find_one({})

        # api_key / secret: prefer DB, fallback to hardcoded defaults
        api_key = str((doc or {}).get('api_key') or '').strip() or DEFAULT_API_KEY
        api_secret = str((doc or {}).get('api_secret') or '').strip() or DEFAULT_API_SECRET
        access_token = str((doc or {}).get('access_token') or '').strip()

        with _cred_lock:
            _common_api_key = api_key
            _common_api_secret = api_secret

        if access_token:
            set_common_credentials(api_key, access_token)
            return True

        log.warning('[kite_broker_ws] no access_token in DB — login required')
        return False
    except Exception as exc:
        log.error('[kite_broker_ws] load_credentials_from_db error: %s', exc)
        return False


def save_access_token_to_db(db, access_token: str) -> None:
    """Upsert only the access_token (called after daily OAuth login)."""
    try:
        api_key = DEFAULT_API_KEY
        with _cred_lock:
            api_key = _common_api_key or DEFAULT_API_KEY
        db._db[KITE_CONFIG_COLLECTION].update_one(
            {},
            {'$set': {
                'api_key': api_key,
                'api_secret': _common_api_secret or DEFAULT_API_SECRET,
                'access_token': access_token,
                'enabled': True,
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error('[kite_broker_ws] save_access_token_to_db error: %s', exc)


def save_credentials_to_db(db, api_key: str, access_token: str) -> None:
    """Upsert full credentials into kite_market_config."""
    try:
        db._db[KITE_CONFIG_COLLECTION].update_one(
            {},
            {'$set': {'api_key': api_key, 'access_token': access_token, 'enabled': True}},
            upsert=True,
        )
    except Exception as exc:
        log.error('[kite_broker_ws] save_credentials_to_db error: %s', exc)


# ─── OAuth helpers ────────────────────────────────────────────────────────────

def get_login_url() -> str:
    """Return the Kite Connect login URL for the configured api_key."""
    with _cred_lock:
        api_key = _common_api_key or DEFAULT_API_KEY
    return f'https://kite.zerodha.com/connect/login?api_key={api_key}&v=3'


def generate_access_token(request_token: str) -> str:
    """
    Exchange request_token (from Kite OAuth callback) for access_token.
    Uses api_key + api_secret stored in memory.
    Returns the access_token string, or raises on failure.
    """
    try:
        from kiteconnect import KiteConnect  # type: ignore
    except ImportError:
        raise RuntimeError('kiteconnect not installed: pip install kiteconnect')

    with _cred_lock:
        api_key = _common_api_key or DEFAULT_API_KEY
        api_secret = _common_api_secret or DEFAULT_API_SECRET

    kite = KiteConnect(api_key=api_key)
    session = kite.generate_session(request_token, api_secret=api_secret)
    access_token = str(session.get('access_token') or '').strip()
    if not access_token:
        raise RuntimeError('generate_session returned no access_token')
    return access_token


def validate_access_token(access_token: str = '') -> bool:
    """
    Check if the given (or currently stored) access_token is still valid
    by calling the Kite profile API.  Returns True if valid.
    """
    try:
        from kiteconnect import KiteConnect  # type: ignore
    except ImportError:
        return False

    with _cred_lock:
        api_key = _common_api_key or DEFAULT_API_KEY
        tok = access_token or _common_access_token

    if not tok:
        return False
    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(tok)
        kite.profile()   # raises exception if token invalid
        return True
    except Exception:
        return False


def parse_kite_tradingsymbol(tradingsymbol: str) -> dict | None:
    """
    Kite's options symbol grammar differs between monthly and weekly
    contracts (3-letter month vs single-char month+day), so instead of
    hand-rolling a regex this does a reverse lookup against
    spot_atm_utils._load_kite_instruments()'s cache — built from Kite's own
    instrument dump — for the entry whose 'symbol' matches. Same technique
    flattrade_broker._to_flattrade_symbol() uses, just inverted.

    Returns {underlying, expiry: "YYYY-MM-DD", strike, option_type} or None
    if the symbol isn't found in today's cached instrument dump (e.g. Kite
    instruments were never loaded because Dhan is the active feed broker).
    """
    sym = str(tradingsymbol or '').strip()
    if not sym:
        return None
    try:
        from features.spot_atm_utils import _load_kite_instruments  # type: ignore
        cache = _load_kite_instruments()
        for (name, exp_str, strike, opt), inst in cache.items():
            if str(inst.get('symbol') or '').strip() == sym:
                return {'underlying': name, 'expiry': exp_str, 'strike': strike, 'option_type': opt}
    except Exception:
        pass

    monthly_match = _KITE_MONTHLY_RE.match(sym)
    if monthly_match:
        underlying, year_2, month_code, strike_raw, option_type = monthly_match.groups()
        month = _KITE_MONTH_MAP.get(month_code)
        if month:
            try:
                from calendar import monthrange
                from datetime import date

                year = 2000 + int(year_2)
                day = monthrange(year, month)[1]
                strike = float(strike_raw)
                strike_value = int(strike) if strike.is_integer() else strike
                expiry = date(year, month, day).isoformat()
                return {
                    'underlying': underlying,
                    'expiry': expiry,
                    'strike': strike_value,
                    'option_type': option_type,
                }
            except Exception:
                pass

    weekly_match = _KITE_WEEKLY_RE.match(sym)
    if weekly_match:
        underlying, year_2, month_code, day_raw, strike_raw, option_type = weekly_match.groups()
        month = _KITE_MONTH_MAP.get(month_code)
        if month:
            try:
                from datetime import date

                year = 2000 + int(year_2)
                day = int(day_raw)
                strike = float(strike_raw)
                strike_value = int(strike) if strike.is_integer() else strike
                expiry = date(year, month, day).isoformat()
                return {
                    'underlying': underlying,
                    'expiry': expiry,
                    'strike': strike_value,
                    'option_type': option_type,
                }
            except Exception:
                pass
    return None


def get_common_api_key() -> str:
    with _cred_lock:
        return _common_api_key or DEFAULT_API_KEY


def get_common_credentials() -> tuple[str, str]:
    with _cred_lock:
        return (_common_api_key or DEFAULT_API_KEY), _common_access_token


def is_configured() -> bool:
    """True only if we have a valid access_token (api_key always has a default)."""
    with _cred_lock:
        return bool(_common_access_token)


# ─── Singleton KiteWSManager ──────────────────────────────────────────────────

class _KiteWSManager:
    """
    ONE KiteTicker connection for the common Kite account.
    Thread-safe. Token subscriptions are reference-counted per user.
    """

    def __init__(self, api_key: str, access_token: str) -> None:
        self._api_key = api_key
        self._access_token = access_token
        self._lock = threading.Lock()
        self._tick_cv = threading.Condition(self._lock)

        # instrument_token (int) → set of user_ids that need it
        self._token_refs: dict[int, set[str]] = defaultdict(set)
        # user_id → set of instrument_tokens they registered
        self._user_tokens: dict[str, set[int]] = defaultdict(set)

        # Latest full ltp_map: str(token) → last_price
        self._ltp_map: dict[str, float] = {}

        # Tick listeners
        self._listeners: list[TickListener] = []

        # KiteTicker (lazy-started on first subscription)
        self._ticker = None
        self._ticker_started = False
        self._ws_connected = False
        self._pending_subscribe: set[int] = set()

    # ── Token registration ────────────────────────────────────────────────

    def add_user_tokens(self, user_id: str, tokens: list[int]) -> None:
        if not tokens:
            return
        with self._lock:
            newly_needed: list[int] = []
            for tok in tokens:
                itok = int(tok)
                if not self._token_refs[itok]:
                    newly_needed.append(itok)
                self._token_refs[itok].add(user_id)
                self._user_tokens[user_id].add(itok)

        if newly_needed:
            self._ensure_ticker_started()
            self._subscribe(newly_needed)
            log.info('[KiteWSManager] user=%s +%d token(s): %s', user_id, len(newly_needed), newly_needed)
            _trace_stdout(
                f'[KITE TOKEN REGISTER] user={user_id} added={len(newly_needed)} '
                f'tokens={",".join(str(tok) for tok in newly_needed)}'
            )

    def remove_user(self, user_id: str) -> None:
        with self._lock:
            user_toks = set(self._user_tokens.pop(user_id, set()))
            no_longer_needed: list[int] = []
            for tok in user_toks:
                self._token_refs[tok].discard(user_id)
                if not self._token_refs[tok]:
                    del self._token_refs[tok]
                    no_longer_needed.append(tok)

        if no_longer_needed:
            self._unsubscribe(no_longer_needed)
            log.info('[KiteWSManager] user=%s -%d token(s): %s', user_id, len(no_longer_needed), no_longer_needed)
            _trace_stdout(
                f'[KITE TOKEN UNREGISTER] user={user_id} removed={len(no_longer_needed)} '
                f'tokens={",".join(str(tok) for tok in no_longer_needed)}'
            )

    def refresh_user_tokens(self, user_id: str, new_tokens: list[int]) -> None:
        """Replace this user's token set atomically."""
        with self._lock:
            old_toks = set(self._user_tokens.get(user_id, set()))
            new_toks = {int(t) for t in new_tokens}

            to_add = new_toks - old_toks
            to_remove = old_toks - new_toks

            for tok in to_remove:
                self._token_refs[tok].discard(user_id)
                if not self._token_refs[tok]:
                    del self._token_refs[tok]
            for tok in to_add:
                self._token_refs[tok].add(user_id)

            if new_toks:
                self._user_tokens[user_id] = new_toks
            else:
                self._user_tokens.pop(user_id, None)

            no_longer_needed = [t for t in to_remove if t not in self._token_refs]
            newly_needed = list(to_add)

        if no_longer_needed:
            self._unsubscribe(no_longer_needed)
        if newly_needed:
            self._ensure_ticker_started()
            self._subscribe(newly_needed)

        if to_add or to_remove:
            log.info('[KiteWSManager] refresh user=%s +%d -%d', user_id, len(to_add), len(to_remove))
            _trace_stdout(
                f'[KITE TOKEN REFRESH] user={user_id} add={len(to_add)} remove={len(to_remove)} '
                f'add_tokens={",".join(str(tok) for tok in sorted(to_add)) or "-"} '
                f'remove_tokens={",".join(str(tok) for tok in sorted(to_remove)) or "-"}'
            )

    # ── Listeners ─────────────────────────────────────────────────────────

    def add_listener(self, listener: TickListener) -> None:
        with self._lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def remove_listener(self, listener: TickListener) -> None:
        with self._lock:
            try:
                self._listeners.remove(listener)
            except ValueError:
                pass

    # ── LTP access ────────────────────────────────────────────────────────

    def get_ltp_map(self) -> dict[str, float]:
        with self._lock:
            return dict(self._ltp_map)

    def wait_for_tokens_ltp(
        self,
        tokens: list[int] | list[str],
        timeout_seconds: float = 2.0,
    ) -> dict[str, float]:
        token_keys = {
            str(int(tok)).strip()
            for tok in (tokens or [])
            if str(tok).strip()
        }
        if not token_keys:
            return {}

        deadline = time.monotonic() + max(0.0, float(timeout_seconds or 0.0))
        with self._tick_cv:
            while True:
                ready = {
                    tok: float(self._ltp_map.get(tok, 0.0))
                    for tok in token_keys
                    if float(self._ltp_map.get(tok, 0.0) or 0.0) > 0
                }
                if ready:
                    return ready

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return ready
                self._tick_cv.wait(timeout=remaining)

    # ── KiteTicker lifecycle ──────────────────────────────────────────────

    def _ensure_ticker_started(self) -> None:
        if self._ticker_started:
            return
        self._start_ticker(self._api_key, self._access_token)

    def _start_ticker(self, api_key: str, access_token: str) -> None:
        try:
            from kiteconnect import KiteTicker  # type: ignore
        except ImportError:
            log.error('[KiteWSManager] kiteconnect not installed: pip install kiteconnect')
            return

        ticker = KiteTicker(api_key, access_token)
        ticker.on_ticks = self._on_ticks
        ticker.on_connect = self._on_connect
        ticker.on_close = self._on_close
        ticker.on_error = self._on_error
        ticker.on_reconnect = self._on_reconnect
        ticker.on_noreconnect = self._on_noreconnect
        ticker.connect(threaded=True)
        self._ticker = ticker
        self._ticker_started = True
        self._ws_connected = False
        self._api_key = api_key
        self._access_token = access_token
        log.info('[KiteWSManager] KiteTicker connected')
        _trace_stdout('[KITE WS START] KiteTicker client started')

    def restart(self, api_key: str, access_token: str) -> None:
        """Stop current ticker and start fresh with new credentials."""
        self.stop_ticker()
        with self._lock:
            all_tokens = list(self._token_refs.keys())
        if all_tokens:
            self._start_ticker(api_key, access_token)
            # _on_connect will re-subscribe all tokens

    def stop_ticker(self) -> None:
        if self._ticker:
            try:
                self._ticker.close()
            except Exception:
                pass
            self._ticker = None
            self._ticker_started = False
            self._ws_connected = False
            self._pending_subscribe.clear()
        log.info('[KiteWSManager] stopped')
        _trace_stdout('[KITE WS STOP] KiteTicker stopped')

    def _subscribe(self, tokens: list[int]) -> None:
        if not tokens:
            return
        if not self._ticker:
            _trace_stdout(
                f'[KITE WS SUBSCRIBE SKIP] ticker_ready={"yes" if self._ticker else "no"} '
                f'tokens={",".join(str(tok) for tok in tokens) if tokens else "-"}'
            )
            return
        if not self._ws_connected:
            with self._lock:
                self._pending_subscribe.update(int(tok) for tok in tokens)
            _trace_stdout(
                f'[KITE WS SUBSCRIBE QUEUED] count={len(tokens)} '
                f'tokens={",".join(str(tok) for tok in tokens)} reason=socket_not_connected'
            )
            return
        try:
            self._ticker.subscribe(tokens)
            self._ticker.set_mode(self._ticker.MODE_LTP, tokens)
            _trace_stdout(
                f'[KITE WS SUBSCRIBE] count={len(tokens)} '
                f'tokens={",".join(str(tok) for tok in tokens)} mode=LTP'
            )
        except Exception as exc:
            log.warning('[KiteWSManager] subscribe error: %s', exc)
            _trace_stdout(f'[KITE WS SUBSCRIBE ERROR] error={exc}')

    def _unsubscribe(self, tokens: list[int]) -> None:
        if not self._ticker or not tokens:
            return
        try:
            self._ticker.unsubscribe(tokens)
            _trace_stdout(
                f'[KITE WS UNSUBSCRIBE] count={len(tokens)} '
                f'tokens={",".join(str(tok) for tok in tokens)}'
            )
        except Exception as exc:
            log.warning('[KiteWSManager] unsubscribe error: %s', exc)
            _trace_stdout(f'[KITE WS UNSUBSCRIBE ERROR] error={exc}')

    # ── KiteTicker callbacks ──────────────────────────────────────────────

    def _on_connect(self, ws, response) -> None:
        log.info('[KiteWSManager] connected')
        with self._lock:
            self._ws_connected = True
            all_tokens = list(self._token_refs.keys())
            pending_tokens = list(self._pending_subscribe)
            self._pending_subscribe.clear()
        subscribe_tokens = sorted({int(tok) for tok in (all_tokens + pending_tokens)})
        _trace_stdout(
            f'[KITE WS CONNECTED] subscribed_token_refs={len(all_tokens)} '
            f'tokens={",".join(str(tok) for tok in subscribe_tokens) or "-"}'
        )
        if subscribe_tokens:
            self._subscribe(subscribe_tokens)

    def _on_close(self, ws, code, reason) -> None:
        log.warning('[KiteWSManager] closed code=%s reason=%s', code, reason)
        self._ws_connected = False
        _trace_stdout(f'[KITE WS CLOSED] code={code} reason={reason}')

    def _on_error(self, ws, code, reason) -> None:
        log.error('[KiteWSManager] error code=%s reason=%s', code, reason)
        _trace_stdout(f'[KITE WS ERROR] code={code} reason={reason}')

    def _on_reconnect(self, ws, attempts_count) -> None:
        log.info('[KiteWSManager] reconnecting attempt=%s', attempts_count)
        _trace_stdout(f'[KITE WS RECONNECT] attempt={attempts_count}')

    def _on_noreconnect(self, ws) -> None:
        log.error('[KiteWSManager] max reconnects reached')
        _trace_stdout('[KITE WS NORECONNECT] max reconnects reached')

    def _on_ticks(self, ws, ticks: list[dict]) -> None:
        """
        KiteTicker tick format (MODE_LTP):
            [{'instrument_token': 12345678, 'last_price': 156.5, ...}, ...]
        """
        if not ticks:
            return

        update: dict[str, float] = {}
        for tick in ticks:
            tok = tick.get('instrument_token')
            ltp = tick.get('last_price')
            if tok is not None and ltp is not None:
                try:
                    update[str(int(tok))] = float(ltp)
                except (TypeError, ValueError):
                    pass

        if not update:
            return

        with self._lock:
            self._ltp_map.update(update)
            current_map = dict(self._ltp_map)
            listeners = list(self._listeners)
            self._tick_cv.notify_all()

        _trace_stdout(
            f'[KITE WS TICK] count={len(update)} '
            f'ltp={", ".join(f"{_describe_token(tok)}({tok})={price}" for tok, price in sorted(update.items()))}'
        )

        for listener in listeners:
            try:
                listener(current_map)
            except Exception as exc:
                log.warning('[KiteWSManager] listener error: %s', exc)


# ─── Singleton instance ────────────────────────────────────────────────────────

_manager_lock = threading.Lock()
_manager: _KiteWSManager | None = None


def _get_manager() -> _KiteWSManager | None:
    return _manager


def _get_or_create_manager() -> _KiteWSManager | None:
    global _manager
    api_key, access_token = get_common_credentials()
    if not api_key or not access_token:
        log.warning('[kite_broker_ws] no common credentials set — call set_common_credentials() first')
        return None
    with _manager_lock:
        if _manager is None:
            _manager = _KiteWSManager(api_key, access_token)
    return _manager


# ─── Public API ───────────────────────────────────────────────────────────────

def register_user_tokens(user_id: str, tokens: list[int]) -> bool:
    """
    Register open-position tokens for `user_id` on the shared Kite connection.
    Returns True if successfully registered.
    """
    if not user_id or not tokens:
        _trace_stdout(
            f'[KITE REGISTER SKIP] user={user_id or "-"} tokens='
            f'{",".join(str(tok) for tok in tokens) if tokens else "-"}'
        )
        return False
    mgr = _get_or_create_manager()
    if not mgr:
        _trace_stdout(
            f'[KITE REGISTER FAILED] user={user_id} reason=manager_unavailable '
            f'tokens={",".join(str(tok) for tok in tokens)}'
        )
        return False
    mgr.add_user_tokens(user_id, tokens)
    _trace_stdout(
        f'[KITE REGISTER OK] user={user_id} tokens={",".join(str(tok) for tok in tokens)}'
    )
    return True


def unregister_user(user_id: str) -> None:
    """Remove all token registrations for `user_id`. Call on disconnect."""
    mgr = _get_manager()
    if mgr:
        mgr.remove_user(user_id)


def refresh_user_tokens(user_id: str, new_tokens: list[int]) -> bool:
    """
    Replace this user's token set with `new_tokens`.
    Subscribes new tokens, unsubscribes ones no longer needed by anyone.
    Returns True if successfully updated.
    """
    if not user_id:
        return False
    mgr = _get_or_create_manager()
    if not mgr:
        return False
    mgr.refresh_user_tokens(user_id, new_tokens)
    return True


def add_tick_listener(listener: TickListener) -> bool:
    """
    Register a callback fired on every Kite tick.
    Signature: listener(ltp_map: dict[str, float])
    Runs in KiteTicker's thread — use call_soon_threadsafe for asyncio.
    Returns True if registered.
    """
    mgr = _get_or_create_manager()
    if not mgr:
        _trace_stdout('[KITE LISTENER FAILED] reason=manager_unavailable')
        return False
    mgr.add_listener(listener)
    _trace_stdout(f'[KITE LISTENER ADDED] listener={getattr(listener, "__name__", "anonymous")}')
    return True


def remove_tick_listener(listener: TickListener) -> None:
    """Unregister a tick listener."""
    mgr = _get_manager()
    if mgr:
        mgr.remove_listener(listener)
        _trace_stdout(f'[KITE LISTENER REMOVED] listener={getattr(listener, "__name__", "anonymous")}')


def get_ltp_map() -> dict[str, float]:
    """Current ltp_map snapshot. Returns {} if not connected."""
    mgr = _get_manager()
    return mgr.get_ltp_map() if mgr else {}


def wait_for_tokens_ltp(tokens: list[int] | list[str], timeout_seconds: float = 2.0) -> dict[str, float]:
    """Wait briefly for the shared Kite socket to publish LTP for any token."""
    mgr = _get_manager()
    return mgr.wait_for_tokens_ltp(tokens, timeout_seconds=timeout_seconds) if mgr else {}


def stop_all() -> None:
    """Stop KiteTicker. Call on app shutdown."""
    global _manager
    with _manager_lock:
        mgr = _manager
        _manager = None
    if mgr:
        mgr.stop_ticker()


# ─── Helper ───────────────────────────────────────────────────────────────────

def extract_instrument_tokens(positions: list[dict]) -> list[int]:
    """
    Extract Zerodha numeric instrument_tokens from open-position / subscribe_tokens records.
    Skips composite string tokens like 'NIFTY_2025-11-04_24500_CE'.
    Returns deduplicated list of int tokens.
    """
    seen: set[int] = set()
    result: list[int] = []
    for pos in (positions or []):
        raw = str(pos.get('token') or pos.get('instrument_token') or '').strip()
        if raw.isdigit():
            itok = int(raw)
            if itok not in seen:
                seen.add(itok)
                result.append(itok)
    return result
