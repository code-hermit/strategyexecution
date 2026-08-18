"""
Login to Zerodha (Kite Connect).

Open the printed login URL, log in, and Kite redirects to your app's redirect URL with a
`request_token` query param - paste just that value in when prompted here.

Kite's /session/token exchange needs a checksum: the SHA-256 hash of api_key + request_token +
api_secret (same shape as AliceBlue's checksum in aliceblue_token_generation.py, different inputs).
"""

import hashlib
import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()

ZERODHA_API_KEY = os.getenv('ZERODHA_API_KEY')
ZERODHA_API_SECRET = os.getenv('ZERODHA_API_SECRET')

LOGIN_URL = f'https://kite.zerodha.com/connect/login?v=3&api_key={ZERODHA_API_KEY}'
TOKEN_URL = 'https://api.kite.trade/session/token'
TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'zerodha_token.json')

print(f'Log in here, then copy the request_token from the redirect URL:\n{LOGIN_URL}')
request_token = input('Enter the request_token from the redirect URL: ').strip()

checksum = hashlib.sha256((ZERODHA_API_KEY + request_token + ZERODHA_API_SECRET).encode()).hexdigest()

resp = requests.post(TOKEN_URL, data={
    'api_key': ZERODHA_API_KEY,
    'request_token': request_token,
    'checksum': checksum,
})
rj = resp.json()

if rj.get('status') != 'success' or 'access_token' not in rj.get('data', {}):
    raise RuntimeError(f'Zerodha token exchange failed: {rj}')

with open(TOKEN_FILE, 'w') as f:
    json.dump(rj['data'], f, indent=2)
print(f'Saved to {TOKEN_FILE}')
