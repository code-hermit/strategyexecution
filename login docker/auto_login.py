"""
Headless-Chrome auto login for Zerodha, AliceBlue and Finvasia - runs once when the container
starts, drives each broker's OAuth login page (user id / password / TOTP), catches the redirect,
does the same code -> token exchange as zerodha_generate_access_token.py /
aliceblue_token_generation.py / finvasia_token_generation.py, and commits the resulting
zerodha_token.json / aliceblue_token.json / finvasia_token.json to the GitHub repo through the
REST contents API (no git inside the container).

Each broker gets its own fresh browser and is independent - one failing login doesn't stop the
others, but the container exits non-zero if any failed. A failed login leaves a screenshot +
page source in DEBUG_DIR so a changed login page is easy to diagnose.

The login pages are filled heuristically rather than with hard-coded selectors: on every step the
visible empty inputs are classified (password type -> password, otp/totp/number/tel -> TOTP,
mpin -> MPIN, anything else -> user id), filled, and submitted, until the browser hits the
redirect URL carrying the broker's code param. That survives most cosmetic page redesigns.

Env (see .env.example): GITHUB_PAT, GITHUB_REPO, GITHUB_BRANCH, plus per broker the API keys the
existing token scripts already use and <BROKER>_PASSWORD / <BROKER>_TOTP_SECRET.
"""

import base64
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
import time
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import pyotp
import requests
from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('auto_login')
# Lambda's runtime installs its own root handler first, which makes basicConfig a no-op (root stays at WARNING).
log.setLevel(logging.INFO)

# Matches KEY=value, KEY='value' or KEY="value", whether entries are separated by newlines or (when
# the Lambda console flattens a pasted .env) by spaces. A leading '#' marks a commented-out entry.
ENV_PAIR_RE = re.compile(r"""(#\s*)?\b([A-Za-z_][A-Za-z0-9_]*)\s*=(?:\s*'([^']*)'|\s*"([^"]*)"|([^\s'"]*))""")


def load_pasted_env(name='secrets'):
    """Loads a whole .env pasted into the single Lambda env var `name` into os.environ, so the rest
    of the script just uses os.getenv. Variables set individually on the function win over it, and
    empty entries are skipped so they don't mask a default (e.g. the Dockerfile's BROKERS)."""
    for comment, key, single, double, bare in ENV_PAIR_RE.findall(os.getenv(name, '')):
        value = single or double or bare
        if not comment and value:
            os.environ.setdefault(key, value)


load_pasted_env()

# In Lambda the only writable path is /tmp (and HOME isn't writable either - Chrome needs one).
IN_LAMBDA = bool(os.getenv('AWS_LAMBDA_FUNCTION_NAME'))
if IN_LAMBDA:
    os.environ['HOME'] = '/tmp'
DEBUG_DIR = os.getenv('DEBUG_DIR', '/tmp/debug' if IN_LAMBDA else '/app/debug')
LOGIN_TIMEOUT = int(os.getenv('LOGIN_TIMEOUT', '90'))
GITHUB_PAT = os.getenv('GITHUB_PAT')
GITHUB_REPO = os.getenv('GITHUB_REPO', 'code-hermit/strategyexecution')
GITHUB_BRANCH = os.getenv('GITHUB_BRANCH', 'main')
# Folder inside the repo the token files go to - '' is the repo root, where they live today.
GITHUB_DIR = os.getenv('GITHUB_DIR', '').strip('/')

OTP_WORDS = ('otp', 'totp', '2fa', 'authenticator', 'app code')
SUBMIT_WORDS = ('login', 'log in', 'sign in', 'continue', 'next', 'submit', 'verify', 'proceed')
AVOID_WORDS = ('open account', 'guest', 'fast login', 'forgot', 'unblock', 'create', 'sign up', 'signup', 'register')


def env(name, default=None):
    value = os.getenv(name, default)
    if not value:
        raise RuntimeError(f'{name} is not set')
    return value


def fresh_totp(secret):
    # Don't type a code with only a couple of seconds left - it can expire before the form round-trips.
    remaining = 30 - time.time() % 30
    if remaining < 4:
        time.sleep(remaining + 0.5)
    return pyotp.TOTP(secret.replace(' ', '')).now()


# ---------------------------------------------------------------------------------------------
# Browser
# ---------------------------------------------------------------------------------------------

def new_driver():
    opts = webdriver.ChromeOptions()
    opts.binary_location = os.getenv('CHROME_BIN', '/usr/bin/chromium')
    for arg in ('--headless=new', '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu',
                '--window-size=1366,900', '--lang=en-US'):
        opts.add_argument(arg)
    if IN_LAMBDA:
        # Lambda has no /dev/shm, no zygote-friendly sandbox and a read-only filesystem outside /tmp.
        profile = tempfile.mkdtemp(dir='/tmp')
        for arg in ('--single-process', '--no-zygote', f'--user-data-dir={profile}',
                    f'--data-path={profile}/data', f'--disk-cache-dir={profile}/cache',
                    '--crash-dumps-dir=/tmp'):
            opts.add_argument(arg)
    # Performance log = every request the page made, so a redirect that bounces straight on to
    # another page is still caught even if current_url has already moved past it.
    opts.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    driver = webdriver.Chrome(service=Service(os.getenv('CHROMEDRIVER', '/usr/bin/chromedriver')), options=opts)
    # Some login pages refuse "HeadlessChrome" user agents.
    ua = driver.execute_script('return navigator.userAgent').replace('HeadlessChrome', 'Chrome')
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {'userAgent': ua})
    return driver


def find_redirect(driver, param, redirect_prefix=None):
    """Returns the parsed query of the first page navigation carrying `param`, else None."""
    urls = [driver.current_url]
    for entry in driver.get_log('performance'):
        msg = json.loads(entry['message'])['message']
        # Only top-level document navigations - the page's own XHR/API calls can carry a "code" too.
        if msg.get('method') == 'Network.requestWillBeSent' and msg['params'].get('type') == 'Document':
            urls.append(msg['params']['request']['url'])
    for url in urls:
        if redirect_prefix and not url.startswith(redirect_prefix):
            continue
        query = parse_qs(urlparse(url).query)
        if query.get(param, [''])[0]:
            return query
    return None


def visible_inputs(driver):
    out = []
    for el in driver.find_elements(By.TAG_NAME, 'input'):
        try:
            typ = (el.get_attribute('type') or 'text').lower()
            if typ in ('text', 'password', 'number', 'tel', 'email') and el.is_displayed() and el.is_enabled():
                out.append(el)
        except StaleElementReferenceException:
            pass
    return out


def classify(el):
    typ = (el.get_attribute('type') or 'text').lower()
    desc = ' '.join(el.get_attribute(a) or '' for a in
                    ('id', 'name', 'placeholder', 'aria-label', 'label', 'autocomplete')).lower()
    if 'mpin' in desc:
        return 'mpin'
    if any(w in desc for w in OTP_WORDS) or typ in ('number', 'tel') or el.get_attribute('maxlength') == '1':
        return 'totp'
    if typ == 'password':
        return 'password'
    return 'user_id'


def click_submit(driver, fallback_el):
    """Clicks the form's real submit button. Ranked rather than first-match: AliceBlue's page has
    "Open Account" as its first type=submit button and "Fast Login" / "Login as Guest?" beside the
    real "Next", so: exact label ("next", "login") > label containing one > any type=submit, and
    never a button whose label names a different flow."""
    best, best_rank = None, 0
    for el in driver.find_elements(By.CSS_SELECTOR, 'button, input[type=submit]'):
        try:
            if not (el.is_displayed() and el.is_enabled()):
                continue
            text = (el.text or el.get_attribute('value') or '').strip().lower()
            if any(w in text for w in AVOID_WORDS):
                continue
            if text in SUBMIT_WORDS:
                rank = 3
            elif any(w in text for w in SUBMIT_WORDS):
                rank = 2
            elif el.get_attribute('type') == 'submit':
                rank = 1
            else:
                continue
            if rank > best_rank:
                best, best_rank = el, rank
        except (StaleElementReferenceException, WebDriverException):
            pass
    if best is not None:
        try:
            best.click()
            return
        except (StaleElementReferenceException, WebDriverException):
            pass
    try:
        fallback_el.send_keys(Keys.ENTER)
    except (StaleElementReferenceException, WebDriverException):
        # Field already gone - the page auto-submitted (Kite does this on the 6th TOTP digit).
        pass


def login(driver, url, param, creds, redirect_prefix=None):
    """Fills the login form step by step until the browser lands on the redirect URL carrying
    `param`. `creds` maps field kind (user_id/password/totp/mpin) to a value; totp is the secret,
    turned into a code only when the TOTP field actually shows up."""
    driver.get(url)
    deadline = time.time() + LOGIN_TIMEOUT
    while time.time() < deadline:
        query = find_redirect(driver, param, redirect_prefix)
        if query:
            return query

        empty = [el for el in visible_inputs(driver) if not el.get_attribute('value')]
        if not empty:
            time.sleep(0.5)
            continue

        otp_boxes = [el for el in empty if el.get_attribute('maxlength') == '1']
        last = None
        try:
            if len(otp_boxes) >= 4:
                # Split one-digit-per-box OTP widget.
                code = fresh_totp(creds['totp'])
                for box, digit in zip(otp_boxes, code):
                    box.send_keys(digit)
                last = otp_boxes[-1]
            else:
                for el in empty:
                    kind = classify(el)
                    if not creds.get(kind):
                        raise RuntimeError(f'login page asked for "{kind}" but no value is configured for it')
                    el.send_keys(fresh_totp(creds['totp']) if kind == 'totp' else creds[kind])
                    last = el
        except StaleElementReferenceException:
            continue  # page re-rendered under us - look again
        click_submit(driver, last)
        time.sleep(1.5)
    raise TimeoutError(f'no redirect with "{param}" within {LOGIN_TIMEOUT}s (stuck at {driver.current_url})')


def dump_debug(driver, broker):
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    try:
        driver.save_screenshot(os.path.join(DEBUG_DIR, f'{broker}_{stamp}.png'))
        with open(os.path.join(DEBUG_DIR, f'{broker}_{stamp}.html'), 'w') as f:
            f.write(driver.page_source)
    except Exception:
        log.exception(f'could not save debug output for {broker}')


# ---------------------------------------------------------------------------------------------
# Brokers - each returns (file name, token dict) in the same shape the trading scripts read
# ---------------------------------------------------------------------------------------------

def zerodha(driver):
    api_key, api_secret = env('ZERODHA_API_KEY'), env('ZERODHA_API_SECRET')
    query = login(driver, f'https://kite.zerodha.com/connect/login?v=3&api_key={api_key}', 'request_token',
                  {'user_id': env('ZERODHA_USER_ID'), 'password': env('ZERODHA_PASSWORD'),
                   'totp': env('ZERODHA_TOTP_SECRET')},
                  os.getenv('ZERODHA_REDIRECT_URL'))
    request_token = query['request_token'][0]
    checksum = hashlib.sha256((api_key + request_token + api_secret).encode()).hexdigest()
    rj = requests.post('https://api.kite.trade/session/token', timeout=15, data={
        'api_key': api_key, 'request_token': request_token, 'checksum': checksum}).json()
    if rj.get('status') != 'success' or 'access_token' not in rj.get('data', {}):
        raise RuntimeError(f'Zerodha token exchange failed: {rj}')
    return 'zerodha_token.json', rj['data']


def aliceblue(driver):
    user_id, app_secret = env('ALICEBLUE_USER_ID'), env('ALICEBLUE_APP_SECRET')
    app_code = env('ALICEBLUE_APP_CODE', '4IxfSoqtdk')
    query = login(driver, f'https://ant.aliceblueonline.com/?appcode={app_code}', 'authCode',
                  {'user_id': user_id, 'password': env('ALICEBLUE_PASSWORD'),
                   'totp': env('ALICEBLUE_TOTP_SECRET'), 'mpin': os.getenv('ALICEBLUE_MPIN')},
                  os.getenv('ALICEBLUE_REDIRECT_URL'))
    auth_code = query['authCode'][0]
    checksum = hashlib.sha256((user_id + auth_code + app_secret).encode()).hexdigest()
    rj = requests.post('https://a3.aliceblueonline.com/open-api/od/v1/vendor/getUserDetails',
                       json={'checkSum': checksum}, timeout=15).json()
    if not rj.get('userSession'):
        raise RuntimeError(f'AliceBlue getUserDetails failed: {rj}')
    return 'aliceblue_token.json', rj


def finvasia(driver):
    client_id, secret_code = env('FINVASIA_CLIENT_ID'), env('FINVASIA_SECRET_CODE')
    user_id = os.getenv('FINVASIA_USER_ID') or client_id.removesuffix('_U')
    query = login(driver, f'https://api.shoonya.com/OAuthlogin/investor-entry-level/login'
                          f'?api_key={client_id}&route_to={user_id}', 'code',
                  {'user_id': user_id, 'password': env('FINVASIA_PASSWORD'),
                   'totp': env('FINVASIA_TOTP_SECRET')},
                  os.getenv('FINVASIA_REDIRECT_URL'))
    auth_code = query['code'][0]
    checksum = hashlib.sha256((client_id + secret_code + auth_code).encode()).hexdigest()
    # Must come from the static IP registered on the Shoonya API key page.
    rj = requests.post('https://api.shoonya.com/NorenWClientAPI/GenAcsTok', timeout=15,
                       data='jData=' + json.dumps({'code': auth_code, 'checksum': checksum, 'uid': user_id})).json()
    if 'access_token' not in rj:
        raise RuntimeError(f'Finvasia GenAcsTok failed: {rj}')
    return 'finvasia_token.json', {'access_token': rj['access_token'], 'refresh_token': rj.get('refresh_token'),
                                   'uid': rj.get('USERID') or user_id, 'actid': rj.get('actid') or user_id}


BROKERS = {'zerodha': zerodha, 'aliceblue': aliceblue, 'finvasia': finvasia}


# ---------------------------------------------------------------------------------------------
# GitHub
# ---------------------------------------------------------------------------------------------

def push_to_github(file_name, data):
    path = f'{GITHUB_DIR}/{file_name}' if GITHUB_DIR else file_name
    url = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{path}'
    headers = {'Authorization': f'Bearer {GITHUB_PAT}', 'Accept': 'application/vnd.github+json',
               'X-GitHub-Api-Version': '2022-11-28'}
    content = base64.b64encode(json.dumps(data, indent=2).encode()).decode()
    for attempt in range(3):
        # Updating an existing file needs its current blob sha; a new file has none.
        r = requests.get(url, headers=headers, params={'ref': GITHUB_BRANCH}, timeout=15)
        if r.status_code not in (200, 404):
            r.raise_for_status()
        body = {'message': f'{file_name} auto login {datetime.now():%Y-%m-%d %H:%M}',
                'branch': GITHUB_BRANCH, 'content': content}
        if r.status_code == 200:
            body['sha'] = r.json()['sha']
        r = requests.put(url, headers=headers, json=body, timeout=15)
        if r.status_code in (200, 201):
            return
        # 409 = branch moved between the GET and the PUT (another token commit landed) - retry with the new sha.
        if r.status_code != 409 or attempt == 2:
            raise RuntimeError(f'GitHub PUT {path} failed: {r.status_code} {r.text}')
        time.sleep(1)


def telegram_send(message):
    token, chat_id = os.getenv('TELEGRAM_BOT_TOKEN'), os.getenv('TELEGRAM_CHAT_ID')
    if not token or not chat_id:
        return
    try:
        requests.post(f'https://api.telegram.org/bot{token}/sendMessage',
                      json={'chat_id': chat_id, 'text': message}, timeout=10)
    except Exception:  # never let a Telegram hiccup break the run
        log.exception('Telegram alert failed')


def run():
    """Logs in to every broker in BROKERS, sends one Telegram summary; returns the names that failed."""
    selected = [b.strip().lower() for b in os.getenv('BROKERS', ','.join(BROKERS)).split(',') if b.strip()]
    lines, failed = [], []
    try:
        env('GITHUB_PAT')
    except RuntimeError as exc:
        telegram_send(f'❌ Auto login not run: {exc}')
        raise
    os.makedirs(DEBUG_DIR, exist_ok=True)
    for name in selected:
        if name not in BROKERS:
            log.error(f'unknown broker "{name}" in BROKERS')
            lines.append(f'❌ {name}: unknown broker')
            failed.append(name)
            continue
        log.info(f'{name}: logging in')
        driver = None
        try:
            driver = new_driver()
            file_name, data = BROKERS[name](driver)
            push_to_github(file_name, data)
            log.info(f'{name}: {file_name} pushed to {GITHUB_REPO}@{GITHUB_BRANCH}')
            # Zerodha/Finvasia call it access_token, AliceBlue userSession.
            token = data.get('access_token') or data.get('userSession')
            lines.append(f'✅ {name}: {file_name} pushed\n{token}')
        except Exception as exc:
            log.exception(f'{name}: failed')
            if driver:
                dump_debug(driver, name)
            lines.append(f'❌ {name}: {str(exc).splitlines()[0][:200] if str(exc) else type(exc).__name__}')
            failed.append(name)
        finally:
            if driver:
                driver.quit()

    if failed:
        log.error(f'failed: {", ".join(failed)}')
    else:
        log.info('all logins done')
    telegram_send(f'{"⚠️" if failed else "🔑"} Auto login {datetime.now():%d %b %H:%M}\n' + '\n'.join(lines))
    return failed


def lambda_handler(event, context):
    # Raise on failure so the invocation counts in the function's Errors metric (CloudWatch alarm),
    # same as aws_lambda_ssm_runner.py.
    failed = run()
    if failed:
        raise RuntimeError(f'auto login failed for: {", ".join(failed)}')
    return {'status': 'ok', 'brokers': os.getenv('BROKERS', ','.join(BROKERS))}


def main():
    sys.exit(1 if run() else 0)


if __name__ == '__main__':
    main()
