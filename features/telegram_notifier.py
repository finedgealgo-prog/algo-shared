"""
Fire-and-forget Telegram notifications — shared by every service (algo.trade,
algo.simulator, algo.scanner, algo.chart, algo.websocket) so each one only
has to call notify_admin/notify_user/notify_user_for instead of reimplementing
its own sender.

Bot credentials live in Mongo, not env vars — collection `finedge_telegram_bot`,
one document per *category* (`common`, `simulator`, `chart`, `algo`, `scanner`, ...)
so each service can eventually get its own bot/chat instead of all sharing one.
Every notify_*() call takes an optional `category` kwarg (defaults to `common`,
today's single shared bot) and looks up that category's doc via _get_bot_config():

  bot_token        – bot token from @BotFather for this category
  admin_chat_id     – chat_id that receives backend/infra-class errors
  user_chat_id      – fallback chat_id for order/trade-class messages,
                        used until a user sets their own (see notify_user_for)
  enabled           – per-category on/off, defaults to True if the doc exists

A category with no doc of its own falls back to the `common` doc, so new
categories work immediately (shared bot) until someone gives them their own
via set_bot_config(). TELEGRAM_NOTIFICATIONS_ENABLED stays an env var — a
single instant kill-switch across every category, same on/off convention as
LIVE_ORDER_STATUS, that doesn't require touching Mongo to flip.

notify_user_for() is the per-user path: it reads telegram_chat_id off the
user's own user_details doc (set from their Profile page) and sends there
instead of the category's shared user_chat_id — falls back to that shared
chat if the user hasn't set their own yet, so nothing breaks for users who
never configured it.

This module never raises and never blocks the caller — every send happens on a
daemon thread with a short timeout, so a slow/unreachable Telegram API can't
stall order placement or the monitor/poll loops that call into this.
"""

import asyncio
import logging
import os
import threading
import time

import requests

log = logging.getLogger(__name__)

_TELEGRAM_API_BASE = 'https://api.telegram.org'
_SEND_TIMEOUT_SECONDS = 5
_DEDUP_WINDOW_SECONDS = 45
_GETUPDATES_LONGPOLL_SECONDS = 25
_BOT_STATE_COLLECTION = 'telegram_bot_state'
_BOT_CONFIG_COLLECTION = 'finedge_telegram_bot'
_CONFIG_CACHE_TTL_SECONDS = 30

DEFAULT_CATEGORY = 'common'

_dedup_lock = threading.Lock()
_last_sent_at: dict[str, float] = {}
_config_cache_lock = threading.Lock()
_config_cache: dict[str, tuple[float, dict]] = {}
_cached_bot_username: dict[str, str] = {}


def _env_flag_enabled(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, '')).strip().lower()
    if not raw:
        return default
    return raw in {'1', 'true', 'yes', 'on'}


def _notifications_enabled() -> bool:
    return _env_flag_enabled('TELEGRAM_NOTIFICATIONS_ENABLED', default=False)


def _format_message(event_type: str, message: str, context: dict | None) -> str:
    lines = [f'[{event_type}]', message.strip()]
    if context:
        context_lines = ', '.join(f'{k}={v}' for k, v in context.items() if v not in (None, ''))
        if context_lines:
            lines.append(context_lines)
    return '\n'.join(lines)


def _is_duplicate(dedup_key: str) -> bool:
    now = time.monotonic()
    with _dedup_lock:
        last = _last_sent_at.get(dedup_key)
        if last is not None and (now - last) < _DEDUP_WINDOW_SECONDS:
            return True
        _last_sent_at[dedup_key] = now
        return False


def _send_sync(token: str, chat_id: str, text: str) -> None:
    if not token or not chat_id:
        log.warning('[TELEGRAM] skipped — bot token or chat_id not configured. text=%s', text)
        return
    try:
        resp = requests.post(
            f'{_TELEGRAM_API_BASE}/bot{token}/sendMessage',
            json={'chat_id': chat_id, 'text': text},
            timeout=_SEND_TIMEOUT_SECONDS,
        )
        if resp.status_code != 200:
            log.error('[TELEGRAM] send failed status=%s body=%s', resp.status_code, resp.text[:300])
    except Exception as exc:
        log.error('[TELEGRAM] send error: %s', exc)


# ── Bot config (Mongo-backed, per category) ────────────────────────────────

def _get_bot_config(category: str = DEFAULT_CATEGORY) -> dict:
    """Cached for _CONFIG_CACHE_TTL_SECONDS so the frequent fire-and-forget
    notify_*() calls don't hit Mongo on every call. Falls back to the
    `common` category's doc if this category has no doc of its own yet."""
    now = time.monotonic()
    with _config_cache_lock:
        cached = _config_cache.get(category)
        if cached and (now - cached[0]) < _CONFIG_CACHE_TTL_SECONDS:
            return cached[1]
    from features.mongo_data import MongoData
    col = MongoData()._db[_BOT_CONFIG_COLLECTION]
    doc = col.find_one({'_id': category}) or {}
    if not doc and category != DEFAULT_CATEGORY:
        doc = col.find_one({'_id': DEFAULT_CATEGORY}) or {}
    with _config_cache_lock:
        _config_cache[category] = (now, doc)
    return doc


def set_bot_config(
    category: str = DEFAULT_CATEGORY,
    bot_token: str | None = None,
    admin_chat_id: str | None = None,
    user_chat_id: str | None = None,
    enabled: bool | None = None,
) -> None:
    """Create/update one category's bot config doc. Used by the one-off setup
    script and any future admin UI for managing bots per service."""
    from features.mongo_data import MongoData
    update = {}
    if bot_token is not None:
        update['bot_token'] = bot_token.strip()
    if admin_chat_id is not None:
        update['admin_chat_id'] = admin_chat_id.strip()
    if user_chat_id is not None:
        update['user_chat_id'] = user_chat_id.strip()
    if enabled is not None:
        update['enabled'] = bool(enabled)
    if not update:
        return
    MongoData()._db[_BOT_CONFIG_COLLECTION].update_one({'_id': category}, {'$set': update}, upsert=True)
    with _config_cache_lock:
        _config_cache.pop(category, None)


def _dispatch(category: str, chat_kind: str, event_type: str, message: str, context: dict | None) -> bool:
    """Returns whether a send was actually queued (notifications enabled,
    category configured, token+chat_id both present) — callers that fire
    blind (the ~30 existing notify_admin/notify_user call sites) ignore this,
    but admin_send_telegram_message uses it to report real delivery instead
    of guessing from a user's telegram_linked flag."""
    text = _format_message(event_type, message, context)
    log.info('[TELEGRAM %s:%s] %s', category, chat_kind, text.replace('\n', ' | '))
    if not _notifications_enabled():
        return False
    config = _get_bot_config(category)
    if not config or config.get('enabled') is False:
        return False
    token = str(config.get('bot_token') or '').strip()
    chat_id = str(config.get(f'{chat_kind}_chat_id') or '').strip()
    if not token or not chat_id:
        log.warning('[TELEGRAM] skipped — bot token or %s chat_id not configured for category "%s"', chat_kind, category)
        return False
    dedup_key = f'{category}:{chat_kind}:{event_type}:{(context or {}).get("trade_id", "")}:{(context or {}).get("leg_id", "")}'
    if _is_duplicate(dedup_key):
        return True
    threading.Thread(target=_send_sync, args=(token, chat_id, text), daemon=True).start()
    return True


def notify_admin(event_type: str, message: str, context: dict | None = None, category: str = DEFAULT_CATEGORY) -> bool:
    """Backend/infra-class errors — LTP fetch, leg-resolution logic, broker-unreachable polling."""
    return _dispatch(category, 'admin', event_type, message, context)


def notify_user(event_type: str, message: str, context: dict | None = None, category: str = DEFAULT_CATEGORY) -> bool:
    """Order/trade-class errors — broker rejected an order, strategy paused, etc."""
    return _dispatch(category, 'user', event_type, message, context)


def notify_both(event_type: str, message: str, context: dict | None = None, category: str = DEFAULT_CATEGORY) -> bool:
    """Genuinely unclassifiable failures — sent to both admin and user."""
    admin_sent = notify_admin(event_type, message, context, category)
    user_sent = notify_user(event_type, message, context, category)
    return admin_sent or user_sent


def _resolve_user_chat_id(user: dict | str | None) -> str:
    """Accepts either a user_details doc (used as-is if it already carries
    telegram_chat_id) or a bare user_id, in which case this does the Mongo
    lookup itself — callers that already have the doc in hand (e.g. an
    endpoint's `current_user`) skip the extra round trip for free."""
    if isinstance(user, dict):
        chat_id = str(user.get('telegram_chat_id') or '').strip()
        if chat_id:
            return chat_id
        user_id = user.get('_id')
    else:
        user_id = user
    if not user_id:
        return ''
    try:
        from bson import ObjectId
        from features.auth import USERS_COLLECTION
        from features.mongo_data import MongoData
        oid = user_id if isinstance(user_id, ObjectId) else ObjectId(str(user_id))
        doc = MongoData()._db[USERS_COLLECTION].find_one({'_id': oid}, {'telegram_chat_id': 1})
        return str((doc or {}).get('telegram_chat_id') or '').strip()
    except Exception:
        return ''


def notify_user_for(
    user: dict | str | None,
    event_type: str,
    message: str,
    context: dict | None = None,
    category: str = DEFAULT_CATEGORY,
) -> tuple[bool, bool]:
    """Per-user Telegram notification — e.g. a "Generate Webhook URL" position
    actually executing (PaperTradeNew.tsx / simulator_pt_create_new_strategy_webhook),
    where the result should reach the specific user who generated that link, not
    the category's shared user_chat_id every notify_user() call goes to.
    `user` is whatever the caller already has on hand — a user_details doc
    (e.g. current_user from app_auth.get_current_user) or a bare user_id string.
    Falls back to the shared notify_user() chat if this user has no
    telegram_chat_id set in their profile yet, so nothing silently goes nowhere.

    Returns (sent, used_own_chat) — sent is False if notifications are
    disabled or no token/chat_id is configured at all (own or fallback), so
    callers like admin_send_telegram_message can report real delivery
    instead of guessing from telegram_linked.
    """
    chat_id = _resolve_user_chat_id(user)
    if not chat_id:
        return notify_user(event_type, message, context, category), False
    text = _format_message(event_type, message, context)
    log.info('[TELEGRAM %s user:%s] %s', category, chat_id, text.replace('\n', ' | '))
    if not _notifications_enabled():
        return False, True
    config = _get_bot_config(category)
    if not config or config.get('enabled') is False:
        return False, True
    token = str(config.get('bot_token') or '').strip()
    if not token:
        log.warning('[TELEGRAM] skipped — no bot_token configured for category "%s"', category)
        return False, True
    dedup_key = f'{category}:user:{chat_id}:{event_type}:{(context or {}).get("trade_id", "")}:{(context or {}).get("leg_id", "")}'
    if _is_duplicate(dedup_key):
        return True, True
    threading.Thread(target=_send_sync, args=(token, chat_id, text), daemon=True).start()
    return True, True


# ── Telegram username linking ───────────────────────────────────────────────
# Profile page flow (algotest-style): user types their Telegram @username,
# we store it; Telegram's Bot API can't message someone who has never
# messaged the bot first (privacy restriction on their side), so the user
# then sends one message to the bot — _telegram_linking_poll_loop below
# (long-polling getUpdates, no public webhook URL needed) matches the
# sender's username against a pending record and captures their chat_id.
# Linking always uses the `common` category's bot — that's the one users see
# and message from their Profile page, regardless of which category ends up
# sending them a given alert.

def get_bot_username(category: str = DEFAULT_CATEGORY) -> str:
    """Cached for the process lifetime per category — a bot's username never
    changes, so this only ever calls Telegram once per category."""
    if category in _cached_bot_username:
        return _cached_bot_username[category]
    token = str(_get_bot_config(category).get('bot_token') or '').strip()
    if not token:
        return ''
    try:
        resp = requests.get(f'{_TELEGRAM_API_BASE}/bot{token}/getMe', timeout=_SEND_TIMEOUT_SECONDS)
        username = str((resp.json().get('result') or {}).get('username') or '').strip()
        _cached_bot_username[category] = username
        return username
    except Exception:
        log.exception('[TELEGRAM] getMe failed — bot username unavailable')
        return ''


def set_pending_telegram_username(user_id, username: str) -> None:
    """Profile page's "Link Telegram" submit — stores the username to match
    against incoming messages; doesn't touch any existing telegram_chat_id
    until the match actually happens (re-linking only takes effect once the
    user messages the bot again under the new username)."""
    from bson import ObjectId
    from features.auth import USERS_COLLECTION
    from features.mongo_data import MongoData
    normalized = username.strip().lstrip('@')
    oid = user_id if isinstance(user_id, ObjectId) else ObjectId(str(user_id))
    MongoData()._db[USERS_COLLECTION].update_one(
        {'_id': oid},
        {'$set': {
            'telegram_username': normalized,
            'telegram_username_lower': normalized.lower(),
            'telegram_linked': False,
        }},
    )


def _get_update_offset(state_col) -> int:
    doc = state_col.find_one({'_id': 'update_offset'}) or {}
    return int(doc.get('value') or 0)


def _set_update_offset(state_col, offset: int) -> None:
    state_col.update_one({'_id': 'update_offset'}, {'$set': {'value': offset}}, upsert=True)


def _process_telegram_update(update: dict, users_col, token: str) -> None:
    message = update.get('message') or update.get('edited_message') or {}
    sender = message.get('from') or {}
    username = str(sender.get('username') or '').strip().lower()
    chat = message.get('chat') or {}
    chat_id = chat.get('id')
    if not username or chat_id is None:
        return
    user_doc = users_col.find_one({'telegram_username_lower': username})
    if not user_doc:
        return
    users_col.update_one(
        {'_id': user_doc['_id']},
        {'$set': {'telegram_chat_id': str(chat_id), 'telegram_linked': True}},
    )
    _send_sync(token, str(chat_id), "✅ Telegram linked to FinEdgeAlgo — your trade and strategy notifications will arrive here from now on.")


async def telegram_linking_poll_loop() -> None:
    """Runs for the life of the process (started once, from algo.trade's
    startup — see _auto_start_telegram_linking) — long-polls getUpdates so no
    public HTTPS webhook URL is needed for local/dev. The update offset is
    persisted in Mongo (telegram_bot_state) so a restart resumes from where
    it left off instead of replaying Telegram's whole backlog or permanently
    skipping whatever arrived while the process was down. Always polls the
    `common` category's bot — see module docstring."""
    token = str(_get_bot_config(DEFAULT_CATEGORY).get('bot_token') or '').strip()
    if not token:
        log.warning('[TELEGRAM linking] no bot_token configured for "%s" category — username-linking loop not starting', DEFAULT_CATEGORY)
        return

    from features.auth import USERS_COLLECTION
    from features.mongo_data import MongoData
    db = MongoData()
    state_col = db._db[_BOT_STATE_COLLECTION]
    users_col = db._db[USERS_COLLECTION]
    offset = _get_update_offset(state_col)

    while True:
        try:
            resp = await asyncio.to_thread(
                requests.get,
                f'{_TELEGRAM_API_BASE}/bot{token}/getUpdates',
                params={'offset': offset, 'timeout': _GETUPDATES_LONGPOLL_SECONDS, 'allowed_updates': '["message"]'},
                timeout=_GETUPDATES_LONGPOLL_SECONDS + 10,
            )
            results = (resp.json() or {}).get('result') or []
            for update in results:
                offset = max(offset, int(update.get('update_id') or 0) + 1)
                try:
                    _process_telegram_update(update, users_col, token)
                except Exception:
                    log.exception('[TELEGRAM linking] failed processing update=%s', update.get('update_id'))
            if results:
                _set_update_offset(state_col, offset)
        except Exception:
            log.exception('[TELEGRAM linking] poll iteration failed')
            await asyncio.sleep(5)
