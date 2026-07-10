"""
CLI for managing Mongo-backed Telegram bot configs (finedge_telegram_bot
collection — see features/telegram_notifier.py). Run from anywhere with
`python3 shared/manage_telegram_bots.py <command>` (this file's own directory
holds the `features` package, so no extra PYTHONPATH setup is needed).

Commands:
  list                                            show every category's config
  chat-ids --category common                      poll getUpdates, print recent
                                                    senders' usernames + chat_ids
  set --category common --admin-chat-id X          upsert admin/user chat_id
      [--user-chat-id Y] [--bot-token T]           and/or bot_token for a category
"""

import argparse
import sys

import requests

from features.telegram_notifier import (
    _BOT_CONFIG_COLLECTION,
    _TELEGRAM_API_BASE,
    _get_bot_config,
    set_bot_config,
)
from features.mongo_data import MongoData


def cmd_list(_args) -> None:
    col = MongoData()._db[_BOT_CONFIG_COLLECTION]
    docs = list(col.find())
    if not docs:
        print('No telegram bot configs yet.')
        return
    for doc in docs:
        token = doc.get('bot_token') or ''
        masked = f'{token[:8]}...{token[-4:]}' if token else '(none)'
        print(f"- {doc['_id']}: token={masked} admin_chat_id={doc.get('admin_chat_id') or '(none)'} "
              f"user_chat_id={doc.get('user_chat_id') or '(none)'} enabled={doc.get('enabled', True)}")


def cmd_chat_ids(args) -> None:
    token = str(_get_bot_config(args.category).get('bot_token') or '').strip()
    if not token:
        print(f'No bot_token set for category "{args.category}". Run `set --category {args.category} --bot-token ...` first.')
        sys.exit(1)
    resp = requests.get(f'{_TELEGRAM_API_BASE}/bot{token}/getUpdates', params={'timeout': 0}, timeout=10)
    results = (resp.json() or {}).get('result') or []
    if not results:
        print(f'No messages yet — open Telegram, find @{_bot_username(token)} and send it any message, then re-run this.')
        return
    seen = {}
    for update in results:
        message = update.get('message') or update.get('edited_message') or {}
        chat = message.get('chat') or {}
        sender = message.get('from') or {}
        seen[chat.get('id')] = (sender.get('username') or sender.get('first_name') or '(unknown)', chat.get('type'))
    for chat_id, (who, chat_type) in seen.items():
        print(f'- chat_id={chat_id}  from=@{who}  type={chat_type}')


def _bot_username(token: str) -> str:
    try:
        resp = requests.get(f'{_TELEGRAM_API_BASE}/bot{token}/getMe', timeout=10)
        return str((resp.json().get('result') or {}).get('username') or '?')
    except Exception:
        return '?'


def cmd_set(args) -> None:
    set_bot_config(
        category=args.category,
        bot_token=args.bot_token,
        admin_chat_id=args.admin_chat_id,
        user_chat_id=args.user_chat_id,
        enabled=None if args.enabled is None else (args.enabled == 'true'),
    )
    print(f'Updated "{args.category}".')
    cmd_list(args)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)

    sub.add_parser('list').set_defaults(func=cmd_list)

    p_chat_ids = sub.add_parser('chat-ids')
    p_chat_ids.add_argument('--category', default='common')
    p_chat_ids.set_defaults(func=cmd_chat_ids)

    p_set = sub.add_parser('set')
    p_set.add_argument('--category', default='common')
    p_set.add_argument('--bot-token')
    p_set.add_argument('--admin-chat-id')
    p_set.add_argument('--user-chat-id')
    p_set.add_argument('--enabled', choices=['true', 'false'])
    p_set.set_defaults(func=cmd_set)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
