"""
Login to Finvasia (Shoonya OAuth) - run once a day before stock_range_reversal.py.

Open the printed URL, log in (password + TOTP); the browser is redirected to your app's redirect URL
with ?code=... - paste that code (or the whole redirected URL) here. The code is exchanged for an
access token with checksum = SHA-256 of client_id + secret_code + code.

.env: FINVASIA_CLIENT_ID (usually <userid>_U), FINVASIA_SECRET_CODE, optional FINVASIA_USER_ID.
The API requests must come from the static IP registered on the Shoonya API key page.
"""

import hashlib
import json
import os
from urllib.parse import parse_qs, urlparse

import dotenv
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
dotenv.load_dotenv(os.path.join(HERE, '.env'))

FINVASIA_CLIENT_ID = os.getenv('FINVASIA_CLIENT_ID')
FINVASIA_SECRET_CODE = os.getenv('FINVASIA_SECRET_CODE')
FINVASIA_USER_ID = os.getenv('FINVASIA_USER_ID') or FINVASIA_CLIENT_ID.removesuffix('_U')
LOGIN_URL = (f'https://api.shoonya.com/OAuthlogin/investor-entry-level/login'
             f'?api_key={FINVASIA_CLIENT_ID}&route_to={FINVASIA_USER_ID}')
TOKEN_URL = 'https://api.shoonya.com/NorenWClientAPI/GenAcsTok'

print(f'Log in here, then copy the code from the redirect URL:\n{LOGIN_URL}')
entered = input('Enter the code (or the full redirected URL): ').strip()
auth_code = parse_qs(urlparse(entered).query).get('code', [entered])[0]

checksum = hashlib.sha256((FINVASIA_CLIENT_ID + FINVASIA_SECRET_CODE + auth_code).encode()).hexdigest()
resp = requests.post(TOKEN_URL, data='jData=' + json.dumps({'code': auth_code, 'checksum': checksum,
                                                            'uid': FINVASIA_USER_ID}), timeout=15)
rj = resp.json()
if 'access_token' not in rj:
    raise SystemExit(f'Finvasia GenAcsTok failed: {rj}')

with open(os.path.join(HERE, 'finvasia_token.json'), 'w') as f:
    json.dump({'access_token': rj['access_token'], 'refresh_token': rj.get('refresh_token'),
               'uid': rj.get('USERID') or FINVASIA_USER_ID, 'actid': rj.get('actid') or FINVASIA_USER_ID}, f, indent=2)
print('Saved to finvasia_token.json')
