"""
One-off check: is TELEGRAM_BOT_TOKEN valid, and what's your chat id?

1. Calls getMe to confirm the token works (prints the bot's own username).
2. Calls getUpdates to list recent messages sent to the bot, so you can read off the chat id
   of whoever/wherever sent them.

If step 2 prints nothing, Telegram has no updates queued for this bot yet - open a chat with it
on Telegram, send it any message (e.g. "hi"), then re-run this script.
"""

import os

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
BASE_URL = f'https://api.telegram.org/bot{TOKEN}'


def main():
    if not TOKEN:
        print('TELEGRAM_BOT_TOKEN not set in .env')
        return

    me = requests.get(f'{BASE_URL}/getMe', timeout=10).json()
    if not me.get('ok'):
        print(f'Bot token is invalid: {me}')
        return
    bot = me['result']
    print(f"Bot token OK - @{bot['username']} ({bot['first_name']})")

    updates = requests.get(f'{BASE_URL}/getUpdates', timeout=10).json()
    if not updates.get('ok'):
        print(f'getUpdates failed: {updates}')
        return

    results = updates['result']
    if not results:
        print(
            '\nNo updates yet. Open a chat with the bot on Telegram (or add it to a group), '
            'send it any message, then re-run this script.'
        )
        return

    print('\nRecent chats that have messaged this bot:')
    seen = set()
    for update in results:
        message = update.get('message') or update.get('channel_post')
        if not message:
            continue
        chat = message['chat']
        if chat['id'] in seen:
            continue
        seen.add(chat['id'])
        who = chat.get('username') or chat.get('title') or chat.get('first_name') or ''
        print(f"  chat_id={chat['id']}  type={chat['type']}  {who}")


if __name__ == '__main__':
    main()
