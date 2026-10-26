"""
token_check.py - 09:36 sanity check of the broker tokens pulled from GitHub at 09:35.

The tokens are generated elsewhere and committed to the repo; this only checks that what landed on
the box actually works, before hedge_buyer.py (09:37) and the straddles (09:44) depend on them:
  - AliceBlue: aliceblue_token.json userSession -> /limits/ (validity) + /profile/ (details)
  - Zerodha:   zerodha_token.json access_token  -> /user/profile
The result for both goes to Telegram as one message. Exits non-zero if either token is bad.
"""

import json
import os
import sys

import requests
from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(HERE, '.env'))

ALICEBLUE_TOKEN_FILE = os.path.join(HERE, 'aliceblue_token.json')
ALICEBLUE_BASE_URL = 'https://a3.aliceblueonline.com/open-api/od/v1'
ZERODHA_TOKEN_FILE = os.path.join(HERE, 'zerodha_token.json')
ZERODHA_BASE_URL = 'https://api.kite.trade'
ZERODHA_API_KEY = os.getenv('ZERODHA_API_KEY')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
REQUEST_TIMEOUT = 10
SENSITIVE_KEYS = ('pan', 'mobile', 'phone', 'bank', 'account', 'aadhaar', 'dob', 'address')


def _mask(key, value):
    if any(s in key.lower() for s in SENSITIVE_KEYS) and isinstance(value, str) and len(value) > 4:
        return '*' * (len(value) - 4) + value[-4:]
    return value


def _format_fields(d):
    """Scalar fields only (lists/dicts like exchanges/products are noise here), sensitive ones masked."""
    return [f'  {k}: {_mask(k, v)}' for k, v in d.items() if isinstance(v, (str, int, float, bool)) and v != '']


def check_aliceblue():
    with open(ALICEBLUE_TOKEN_FILE) as f:
        token = json.load(f)
    headers = {'Authorization': f"Bearer {token['userSession']}", 'Content-Type': 'application/json'}

    limits = requests.get(f'{ALICEBLUE_BASE_URL}/limits/', headers=headers, timeout=REQUEST_TIMEOUT)
    if not limits.ok:
        return False, [f'session INVALID ({limits.status_code}): {limits.text[:200]}']
    lines = [f"client: {token.get('clientId')}"]

    profile = requests.get(f'{ALICEBLUE_BASE_URL}/profile/', headers=headers, timeout=REQUEST_TIMEOUT)
    data = profile.json() if profile.ok else {}
    result = data.get('result') if data.get('status') == 'Ok' else None
    if isinstance(result, list):
        result = result[0] if result else None
    if isinstance(result, dict):
        lines += _format_fields(result)
    else:                                       # session works (limits OK) - just no profile details
        lines.append(f'profile unavailable ({profile.status_code}): {profile.text[:200]}')
    return True, lines


def check_zerodha():
    with open(ZERODHA_TOKEN_FILE) as f:
        token = json.load(f)
    headers = {'Authorization': f"token {ZERODHA_API_KEY}:{token['access_token']}", 'X-Kite-Version': '3'}
    resp = requests.get(f'{ZERODHA_BASE_URL}/user/profile', headers=headers, timeout=REQUEST_TIMEOUT)
    if not resp.ok:
        return False, [f'access token INVALID ({resp.status_code}): {resp.text[:200]}']
    return True, [f"login_time (token file): {token.get('login_time')}"] + _format_fields(resp.json()['data'])


def telegram_send(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print('TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set in .env', file=sys.stderr)
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage',
            json={'chat_id': TELEGRAM_CHAT_ID, 'text': message},
            timeout=REQUEST_TIMEOUT,
        )
    except Exception as exc:
        print(f'Telegram send failed: {exc}', file=sys.stderr)


def main():
    all_ok = True
    sections = []
    for name, check in (('AliceBlue', check_aliceblue), ('Zerodha', check_zerodha)):
        try:
            ok, lines = check()
        except Exception as exc:                # missing/garbled token file, network error, ...
            ok, lines = False, [f'check failed: {exc}']
        all_ok &= ok
        sections.append(f"{'✅' if ok else '❌'} {name}\n" + '\n'.join(lines))

    header = 'Token check: all OK' if all_ok else 'Token check: FAILED - fix before 09:37'
    message = header + '\n\n' + '\n\n'.join(sections)
    print(message)
    telegram_send(message)
    sys.exit(0 if all_ok else 1)


if __name__ == '__main__':
    main()
