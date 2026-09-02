"""
One shared process holding the one Zerodha ticker websocket connection every execution script
reads from, instead of each script (exec_rsv_cont.py, sensex_option_buying_twhf.py, any future
one) opening and maintaining its own. Two reasons that matters: fewer live websocket connections
against the same account (Kite caps concurrent ticker connections per API key), and every script
gets the exact same in-sync view of a given instrument's price rather than N independently-drifting
feeds. Writes every tick straight to Redis (SET zerodha:ltp:<instrument_token> {"price","ts"}), so
any script - this process included - reads prices as a plain, fast Redis GET. See
zerodha_ltp_client.py for the read side (Redis first, REST fallback if Redis or this service is
down) and register_subscription() (the write side scripts use to ask this service to start
streaming a token).

At startup, proactively subscribes to the FULL current-week option chain (every CE/PE) for both
NIFTY (NFO) and SENSEX (BFO), plus both underlyings' own spot index - see UNDERLYINGS/
_bootstrap_subscriptions below. At this scale (a few hundred instruments total) that's simpler
than tracking exactly which strikes are "needed" right now, and it means a strategy script never
waits on any registration lag for a strike it hasn't touched yet - every ATM/OTM/ITM strike for
both underlyings is already streaming the moment this service comes up. On top of that, this
service ALSO polls the zerodha:subscriptions Redis SET every SUBSCRIPTION_POLL_SECONDS for
anything a script has separately registered (via zerodha_ltp_client.register_subscription()) -
covers next week's expiry, a different underlying, or anything else outside the two chains above.
Nothing is ever unsubscribed intraday - the tick volume for this many instruments is still trivial,
not worth the bookkeeping to prune.

Hand-rolled against Kite's published binary tick protocol (LTP mode only - 8-byte packets: bytes
0-3 instrument_token uint32, bytes 4-7 last_traded_price uint32 / KITE_TICK_PRICE_DIVISOR) rather
than the kiteconnect SDK's KiteTicker: that SDK's dependency chain (Twisted/autobahn/cryptography,
the last needing a Rust build toolchain) failed to install where this was built; websocket-client
is a single pure-Python, zero-transitive-dependency package. A binary message that doesn't parse
as an LTP tick (the server's periodic 1-byte heartbeat, in particular) is silently ignored, not
treated as an error - a heartbeat isn't a price update.

Auth reuses the same zerodha_token.json every other script here already generates via
zerodha_generate_access_token.py - this file doesn't do its own login flow, just reads that token
and fails loudly (same as every other script) if it's missing or expired.

Run standalone, once per trading day, independent of and started before the strategy scripts:
    python3 zerodha_ticker_service.py
Logs to zerodha_ticker_service.log and stdout; a WARNING+ log record is pushed to Telegram if
TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are set in .env, same convention as exec_rsv_cont.py.
"""

import csv
import io
import json
import logging
import os
import struct
import sys
import threading
import time as time_module
from datetime import datetime, time as dtime

import requests
import websocket
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

LOG_FILE = os.path.join(os.path.dirname(__file__), 'zerodha_ticker_service.log')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_TIMEOUT = 10


def _telegram_send(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage',
            json={'chat_id': TELEGRAM_CHAT_ID, 'text': message}, timeout=TELEGRAM_TIMEOUT,
        )
    except Exception as exc:
        print(f'Telegram alert failed: {exc}', file=sys.stderr)


class TelegramHandler(logging.Handler):
    def emit(self, record):
        if getattr(record, 'no_telegram', False):
            return
        try:
            _telegram_send(self.format(record))
        except Exception as exc:
            print(f'TelegramHandler.emit failed: {exc}', file=sys.stderr)


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout), TelegramHandler()],
)
log = logging.getLogger(__name__)

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    log.warning('TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set in .env - Telegram alerts disabled', extra={'no_telegram': True})

try:
    import redis
except ImportError:
    log.critical('redis package not installed - this service has nothing to write to, exiting. pip install redis.')
    raise

from zerodha_ltp_client import REDIS_DB, REDIS_HOST, REDIS_LTP_KEY_PREFIX, REDIS_PORT, REDIS_SUBSCRIPTIONS_KEY

SUBSCRIPTION_POLL_SECONDS = 2  # how often to check zerodha:subscriptions for tokens to add
RUN_UNTIL = dtime(15, 30)  # exits for the day shortly after every strategy script's own EXIT_TIME

# Both underlyings this repo's strategy scripts trade - matches CFG in exec_rsv_cont.py and
# sensex_option_buying_twhf.py's own SYMBOL/exchange constants.
UNDERLYINGS = {
    'NIFTY': dict(options_exchange='NFO', spot_exchange='NSE', spot_tradingsymbol='NIFTY 50'),
    'SENSEX': dict(options_exchange='BFO', spot_exchange='BSE', spot_tradingsymbol='SENSEX'),
}

# ── Zerodha auth (REST, once at startup - reused for the websocket URL's query string) ──────────
ZERODHA_API_KEY = os.getenv('ZERODHA_API_KEY')
ZERODHA_TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'zerodha_token.json')
ZERODHA_BASE_URL = 'https://api.kite.trade'
REQUEST_TIMEOUT = 10


def _zerodha_headers(access_token):
    return {'Authorization': f'token {ZERODHA_API_KEY}:{access_token}', 'X-Kite-Version': '3'}


def _load_zerodha_token():
    with open(ZERODHA_TOKEN_FILE) as f:
        return json.load(f)['access_token']


def _zerodha_token_is_valid(access_token):
    resp = requests.get(f'{ZERODHA_BASE_URL}/user/profile', headers=_zerodha_headers(access_token), timeout=REQUEST_TIMEOUT)
    return resp.ok


def _valid_zerodha_token():
    if not os.path.exists(ZERODHA_TOKEN_FILE):
        raise RuntimeError('No Zerodha access token found - run zerodha_generate_access_token.py to log in')
    access_token = _load_zerodha_token()
    if not _zerodha_token_is_valid(access_token):
        raise RuntimeError('Zerodha access token expired - run zerodha_generate_access_token.py to log in again')
    return access_token


try:
    ZERODHA_ACCESS_TOKEN = _valid_zerodha_token()
except Exception:
    log.critical('Zerodha auth failed', exc_info=True)
    raise

# ── Redis (writes only - the read side is zerodha_ltp_client.py) ────────────────────────────────
_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
try:
    _redis.ping()
except Exception:
    log.critical(f'Cannot reach Redis at {REDIS_HOST}:{REDIS_PORT} - this service has nothing to write to, exiting', exc_info=True)
    raise

# ── Kite ticker websocket (v3, LTP mode, hand-rolled - see module docstring) ────────────────────
KITE_WS_URL = 'wss://ws.kite.trade'
KITE_TICK_PRICE_DIVISOR = 100  # equities/index/F&O segments - what every instrument here is

_subscribed_tokens = set()
_ws_app = None
_ws_lock = threading.Lock()


def _parse_ltp_ticks(raw):
    if not raw or len(raw) < 2:
        return []
    try:
        num_packets = struct.unpack('>H', raw[0:2])[0]
        offset = 2
        ticks = []
        for _ in range(num_packets):
            if offset + 2 > len(raw):
                break
            length = struct.unpack('>H', raw[offset:offset + 2])[0]
            offset += 2
            packet = raw[offset:offset + length]
            offset += length
            if length < 8:
                continue
            token = struct.unpack('>I', packet[0:4])[0]
            ltp = struct.unpack('>I', packet[4:8])[0] / KITE_TICK_PRICE_DIVISOR
            ticks.append((token, ltp))
        return ticks
    except (struct.error, TypeError, IndexError):
        # struct.error: truncated/malformed binary frame. TypeError: `raw` wasn't bytes-like after
        # all (shouldn't happen now that _ws_on_message filters non-binary frames, but fail closed
        # rather than crash the ticker loop if that guard is ever bypassed). IndexError: a slice
        # came up short in a way struct.unpack's own length check didn't catch.
        return []


WS_SUBSCRIBE_CHUNK_SIZE = 200  # tokens per subscribe/mode message - the full NIFTY+SENSEX weekly
# chain can be several hundred instruments; chunking keeps each websocket text frame modest rather
# than sending one message with everything in it.


def _ws_send_subscribe(ws, tokens):
    for i in range(0, len(tokens), WS_SUBSCRIBE_CHUNK_SIZE):
        chunk = tokens[i:i + WS_SUBSCRIBE_CHUNK_SIZE]
        ws.send(json.dumps({'a': 'subscribe', 'v': chunk}))
        ws.send(json.dumps({'a': 'mode', 'v': ['ltp', chunk]}))


def _ws_on_open(ws):
    log.info('Kite ticker websocket connected', extra={'no_telegram': True})
    with _ws_lock:
        tokens = list(_subscribed_tokens)
    if tokens:
        _ws_send_subscribe(ws, tokens)


def _ws_on_message(ws, message):
    # Kite's ticker sends both binary tick frames (what _parse_ltp_ticks understands) and
    # occasional text/JSON frames (connection acks, postbacks, error notices) on the same socket -
    # websocket-client hands those to this callback as `str`, not `bytes`. There's no tick data to
    # extract from a text frame, so skip it rather than let struct.unpack blow up on it below.
    if not isinstance(message, (bytes, bytearray)):
        log.debug(f'non-binary websocket message (ignored, not tick data): {message!r}')
        return

    now = time_module.time()
    pipe = _redis.pipeline(transaction=False)
    wrote_any = False
    for token, ltp in _parse_ltp_ticks(message):
        pipe.set(f'{REDIS_LTP_KEY_PREFIX}{token}', json.dumps({'price': ltp, 'ts': now}))
        wrote_any = True
    if wrote_any:
        try:
            pipe.execute()
        except Exception as exc:
            log.warning(f'Redis write failed ({exc}) - dropping this tick batch', extra={'no_telegram': True})


def _ws_on_error(ws, error):
    log.warning(f'Kite ticker websocket error: {error}', extra={'no_telegram': True})


def _ws_on_close(ws, code, reason):
    log.warning(f'Kite ticker websocket closed ({code} {reason}) - reconnecting', extra={'no_telegram': True})


def _start_kite_ws():
    global _ws_app
    url = f'{KITE_WS_URL}?api_key={ZERODHA_API_KEY}&access_token={ZERODHA_ACCESS_TOKEN}'
    _ws_app = websocket.WebSocketApp(
        url, on_open=_ws_on_open, on_message=_ws_on_message, on_error=_ws_on_error, on_close=_ws_on_close,
    )
    thread = threading.Thread(
        target=lambda: _ws_app.run_forever(reconnect=5, ping_interval=30, ping_timeout=10),
        daemon=True, name='kite-ticker',
    )
    thread.start()
    return thread


def _fetch_current_week_option_tokens(underlying, options_exchange):
    """Every CE/PE instrument_token for `underlying`'s current-week expiry on `options_exchange` -
    same current-week filtering every strategy script's own _load_zerodha_current_week_options
    already uses, just returning tokens for ALL strikes instead of one at a time."""
    resp = requests.get(f'{ZERODHA_BASE_URL}/instruments/{options_exchange}', headers=_zerodha_headers(ZERODHA_ACCESS_TOKEN), timeout=30)
    resp.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(resp.text)))
    today = datetime.now().date()
    opts = [
        row for row in rows
        if row['name'] == underlying and row['instrument_type'] in ('CE', 'PE') and row['expiry']
        and datetime.strptime(row['expiry'], '%Y-%m-%d').date() >= today
    ]
    if not opts:
        log.warning(f'No {underlying} option instruments found on Zerodha {options_exchange}', extra={'no_telegram': True})
        return []
    current_week_expiry = min(row['expiry'] for row in opts)
    opts = [row for row in opts if row['expiry'] == current_week_expiry]
    return [int(row['instrument_token']) for row in opts]


def _fetch_spot_token(spot_exchange, spot_tradingsymbol):
    resp = requests.get(f'{ZERODHA_BASE_URL}/instruments/{spot_exchange}', headers=_zerodha_headers(ZERODHA_ACCESS_TOKEN), timeout=30)
    resp.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(resp.text)))
    for row in rows:
        if row.get('segment') == 'INDICES' and row.get('tradingsymbol') == spot_tradingsymbol:
            return int(row['instrument_token'])
    raise RuntimeError(f'{spot_tradingsymbol} index instrument_token not found on Zerodha {spot_exchange} dump')


def _bootstrap_subscriptions():
    """Proactively subscribes to every current-week NIFTY and SENSEX CE/PE, plus both underlyings'
    spot indices - see module docstring for why this runs up front rather than waiting on scripts
    to register individual strikes. Failures here are logged, not fatal - whichever underlying's
    fetch fails just falls back to the ordinary register-driven path for its instruments (and the
    caller alerts loudly either way, since this is meant to cover everything)."""
    tokens = set()
    for underlying, cfg in UNDERLYINGS.items():
        try:
            option_tokens = _fetch_current_week_option_tokens(underlying, cfg['options_exchange'])
            tokens.update(option_tokens)
            log.info(f'{underlying}: {len(option_tokens)} current-week option instrument(s) on {cfg["options_exchange"]}', extra={'no_telegram': True})
        except Exception:
            log.warning(f'{underlying}: failed to fetch current-week option chain - will rely on register_subscription() for these instead', exc_info=True)

        try:
            spot_token = _fetch_spot_token(cfg['spot_exchange'], cfg['spot_tradingsymbol'])
            tokens.add(spot_token)
            log.info(f'{underlying}: spot instrument_token={spot_token}', extra={'no_telegram': True})
        except Exception:
            log.warning(f'{underlying}: failed to fetch spot instrument_token - will rely on register_subscription() for it instead', exc_info=True)

    with _ws_lock:
        _subscribed_tokens.update(tokens)
    if tokens:
        try:
            _redis.sadd(REDIS_SUBSCRIPTIONS_KEY, *tokens)
        except Exception as exc:
            log.warning(f'Could not record bootstrap tokens in {REDIS_SUBSCRIPTIONS_KEY} ({exc}) - harmless, they are already subscribed either way', extra={'no_telegram': True})
    log.info(f'Bootstrap subscriptions: {len(tokens)} instrument(s) total across {list(UNDERLYINGS)}', extra={'no_telegram': True})
    return tokens


def _subscription_poll_loop():
    """Every SUBSCRIPTION_POLL_SECONDS, subscribes to any token in the Redis subscriptions set
    that isn't already subscribed - see zerodha_ltp_client.register_subscription()."""
    while True:
        try:
            wanted = {int(t) for t in _redis.smembers(REDIS_SUBSCRIPTIONS_KEY)}
        except Exception as exc:
            log.warning(f'Could not read {REDIS_SUBSCRIPTIONS_KEY} from Redis ({exc})', extra={'no_telegram': True})
            wanted = set()

        with _ws_lock:
            new_tokens = list(wanted - _subscribed_tokens)
            _subscribed_tokens.update(new_tokens)

        if new_tokens and _ws_app is not None and getattr(_ws_app, 'sock', None) is not None and _ws_app.sock.connected:
            log.info(f'Subscribing to {len(new_tokens)} new instrument token(s): {new_tokens}', extra={'no_telegram': True})
            _ws_send_subscribe(_ws_app, new_tokens)

        time_module.sleep(SUBSCRIPTION_POLL_SECONDS)


def main():
    log.info('Zerodha ticker service starting', extra={'no_telegram': True})
    _bootstrap_subscriptions()  # populates _subscribed_tokens BEFORE connecting, so _ws_on_open's
    # very first subscribe already covers the full NIFTY+SENSEX chain - see module docstring
    _start_kite_ws()
    poll_thread = threading.Thread(target=_subscription_poll_loop, daemon=True, name='sub-poll')
    poll_thread.start()

    while datetime.now().time() < RUN_UNTIL:
        time_module.sleep(30)
    log.info(f'{RUN_UNTIL} reached - ticker service exiting for the day', extra={'no_telegram': True})


if __name__ == '__main__':
    main()
