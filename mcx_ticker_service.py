"""
MCX counterpart of zerodha_ticker_service.py: one shared process holding a Zerodha ticker websocket
for MCX gold (GOLDM by default - see UNDERLYINGS), writing every tick to the SAME Redis keys
(SET zerodha:ltp:<instrument_token> {"price","ts"}) that zerodha_ltp_client.py reads - so
exec_rsv_goldm.py (or any other MCX script) gets Redis-fast prices with no change on its side, and
falls back to REST exactly as before if this service is down. Separate from
zerodha_ticker_service.py because MCX runs on a different clock: that service exits at 15:30, MCX
trades until 23:30 (23:55 when US daylight saving is off).

What it tracks, resolved at startup from Kite's MCX instrument dump (once per trading day - MCX
contracts don't change intraday):
  - the nearest-expiry option chain (every CE/PE strike) for each underlying;
  - that chain's UNDERLYING FUTURE - the nearest FUT expiring on/after the chain's expiry. MCX
    options are options on futures and expire ~a week before their future, so this is not always
    the nearest FUT (e.g. 29 Oct GOLDM options sit on the 5 Nov future while the 5 Oct future is
    still trading) - same pairing exec_rsv_goldm.py and the backtest use;
  - the nearest FUT itself too, when that's a different (expiring-soon) contract - it's still the
    most traded gold future until it expires, cheap to stream, useful to watch.
The resolved set is published to Redis as JSON under mcx:chain:<UNDERLYING> (expiries,
tradingsymbols, instrument tokens, strikes) for any script that wants the current contracts
without downloading the dump itself, and logged at startup.

Like zerodha_ticker_service.py, it ALSO polls the shared zerodha:subscriptions Redis SET every
SUBSCRIPTION_POLL_SECONDS for tokens scripts register via zerodha_ltp_client.register_subscription()
- but only picks up tokens that are MCX instruments (per today's MCX dump), so it never opens a
second stream for NSE/BSE tokens the main ticker service already covers. Nothing is unsubscribed
intraday.

Same hand-rolled Kite v3 binary LTP protocol (8-byte packets, price / 100 - MCX uses the same
divisor as the equity/F&O segments) and websocket-client transport as zerodha_ticker_service.py -
see its docstring for why not the kiteconnect SDK. Two services = two ticker connections on the
same API key, within Kite's per-key limit (3).

Auth reuses zerodha_token.json (zerodha_generate_access_token.py), failing loudly if missing/expired.

Run standalone once per MCX trading day, before the MCX strategy scripts (e.g. ~08:55 or any time
before exec_rsv_goldm.py's 15:14 warm-up; cron starts it at 15:14, exec_rsv_goldm.py at 15:14:30
- and exec_rsv_goldm.py waits for this to be running before it starts):
    python3 mcx_ticker_service.py
Logs to mcx_ticker_service.log and stdout; WARNING+ records go to Telegram if
TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are set in .env.
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

LOG_FILE = os.path.join(os.path.dirname(__file__), 'mcx_ticker_service.log')
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

SUBSCRIPTION_POLL_SECONDS = 2  # how often to check zerodha:subscriptions for MCX tokens to add
RUN_UNTIL = dtime(23, 58)  # after MCX's latest close (23:55 when US DST is off)
EXCHANGE = 'MCX'
UNDERLYINGS = ('GOLDM',)  # Kite 'name' column - add e.g. 'GOLD' to stream the big contract too
REDIS_CHAIN_KEY_PREFIX = 'mcx:chain:'  # mcx:chain:<UNDERLYING> -> JSON of today's resolved contracts

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

# ── Kite ticker websocket (v3, LTP mode, hand-rolled - see zerodha_ticker_service.py) ───────────
KITE_WS_URL = 'wss://ws.kite.trade'
KITE_TICK_PRICE_DIVISOR = 100  # MCX uses the same divisor as equity/F&O (only CDS/BCD differ)

_subscribed_tokens = set()
_mcx_tokens = set()  # every instrument_token in today's MCX dump - filter for registered tokens
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
        return []


WS_SUBSCRIBE_CHUNK_SIZE = 200


def _ws_send_subscribe(ws, tokens):
    for i in range(0, len(tokens), WS_SUBSCRIBE_CHUNK_SIZE):
        chunk = tokens[i:i + WS_SUBSCRIBE_CHUNK_SIZE]
        ws.send(json.dumps({'a': 'subscribe', 'v': chunk}))
        ws.send(json.dumps({'a': 'mode', 'v': ['ltp', chunk]}))


def _ws_on_open(ws):
    log.info('Kite ticker websocket connected (MCX)', extra={'no_telegram': True})
    with _ws_lock:
        tokens = list(_subscribed_tokens)
    if tokens:
        _ws_send_subscribe(ws, tokens)


def _ws_on_message(ws, message):
    if not isinstance(message, (bytes, bytearray)):  # acks/notices, not tick data
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
        daemon=True, name='kite-ticker-mcx',
    )
    thread.start()
    return thread


# ── Contract resolution ─────────────────────────────────────────────────────────────────────────
def _fetch_mcx_instruments():
    resp = requests.get(f'{ZERODHA_BASE_URL}/instruments/{EXCHANGE}', headers=_zerodha_headers(ZERODHA_ACCESS_TOKEN), timeout=30)
    resp.raise_for_status()
    return list(csv.DictReader(io.StringIO(resp.text)))


def _contract(row):
    return dict(
        instrument_token=int(row['instrument_token']), tradingsymbol=row['tradingsymbol'],
        expiry=row['expiry'], instrument_type=row['instrument_type'],
        strike=float(row['strike']), lot_size=int(row['lot_size']), tick_size=float(row['tick_size']),
    )


def resolve_chain(rows, underlying):
    """Today's contracts for `underlying`: the nearest-expiry option chain, the future that chain is
    written on (nearest FUT expiring on/after the chain's expiry), and the nearest FUT overall.
    Returns a JSON-serialisable dict - see module docstring."""
    today = datetime.now().strftime('%Y-%m-%d')
    live = [r for r in rows if r['name'] == underlying and r['expiry'] and r['expiry'] >= today]
    futs = sorted((r for r in live if r['instrument_type'] == 'FUT'), key=lambda r: r['expiry'])
    opts = [r for r in live if r['instrument_type'] in ('CE', 'PE')]
    if not futs:
        raise RuntimeError(f'No live {underlying} futures on Zerodha {EXCHANGE}')

    chain = dict(underlying=underlying, resolved_at=datetime.now().isoformat(timespec='seconds'),
                 near_future=_contract(futs[0]), option_expiry=None, underlying_future=None, options=[])
    if not opts:
        log.warning(f'{underlying}: no live option chain on Zerodha {EXCHANGE} - streaming futures only')
        return chain

    option_expiry = min(r['expiry'] for r in opts)
    underlying_fut = next((r for r in futs if r['expiry'] >= option_expiry), None)
    if underlying_fut is None:
        log.warning(f'{underlying}: no future expiring on/after option expiry {option_expiry} - option chain has no underlying to track')
    chain.update(
        option_expiry=option_expiry,
        underlying_future=_contract(underlying_fut) if underlying_fut else None,
        options=sorted((_contract(r) for r in opts if r['expiry'] == option_expiry),
                       key=lambda c: (c['strike'], c['instrument_type'])),
    )
    return chain


def _chain_tokens(chain):
    tokens = {chain['near_future']['instrument_token']}
    if chain['underlying_future']:
        tokens.add(chain['underlying_future']['instrument_token'])
    tokens.update(c['instrument_token'] for c in chain['options'])
    return tokens


def _bootstrap_subscriptions():
    """Resolves and subscribes every UNDERLYINGS chain + futures, and publishes each resolved chain
    to Redis. A failure for one underlying is logged (and alerted), not fatal - the register-driven
    path still covers whatever a script asks for."""
    rows = _fetch_mcx_instruments()
    _mcx_tokens.update(int(r['instrument_token']) for r in rows)

    tokens = set()
    for underlying in UNDERLYINGS:
        try:
            chain = resolve_chain(rows, underlying)
        except Exception:
            log.warning(f'{underlying}: failed to resolve MCX contracts - relying on register_subscription() for it instead', exc_info=True)
            continue
        chain_tokens = _chain_tokens(chain)
        tokens.update(chain_tokens)

        uf = chain['underlying_future']
        log.info(
            f"{underlying}: near future {chain['near_future']['tradingsymbol']} ({chain['near_future']['expiry']}), "
            f"option chain {chain['option_expiry']} ({len(chain['options'])} contracts) on "
            f"{uf['tradingsymbol'] + ' (' + uf['expiry'] + ')' if uf else 'no underlying future'} - "
            f'{len(chain_tokens)} instrument(s)',
            extra={'no_telegram': True},
        )
        try:
            _redis.set(f'{REDIS_CHAIN_KEY_PREFIX}{underlying}', json.dumps(chain))
        except Exception as exc:
            log.warning(f'Could not publish {underlying} chain to Redis ({exc})', extra={'no_telegram': True})

    with _ws_lock:
        _subscribed_tokens.update(tokens)
    if tokens:
        try:
            _redis.sadd(REDIS_SUBSCRIPTIONS_KEY, *tokens)
        except Exception as exc:
            log.warning(f'Could not record bootstrap tokens in {REDIS_SUBSCRIPTIONS_KEY} ({exc}) - harmless, they are already subscribed either way', extra={'no_telegram': True})
    log.info(f'Bootstrap subscriptions: {len(tokens)} MCX instrument(s) across {list(UNDERLYINGS)}', extra={'no_telegram': True})
    return tokens


def _subscription_poll_loop():
    """Every SUBSCRIPTION_POLL_SECONDS, subscribes to any MCX token in the shared Redis
    subscriptions set that isn't already subscribed (non-MCX tokens are left to
    zerodha_ticker_service.py)."""
    while True:
        try:
            wanted = {int(t) for t in _redis.smembers(REDIS_SUBSCRIPTIONS_KEY)} & _mcx_tokens
        except Exception as exc:
            log.warning(f'Could not read {REDIS_SUBSCRIPTIONS_KEY} from Redis ({exc})', extra={'no_telegram': True})
            wanted = set()

        with _ws_lock:
            new_tokens = list(wanted - _subscribed_tokens)
            _subscribed_tokens.update(new_tokens)

        if new_tokens and _ws_app is not None and getattr(_ws_app, 'sock', None) is not None and _ws_app.sock.connected:
            log.info(f'Subscribing to {len(new_tokens)} new MCX instrument token(s): {new_tokens}', extra={'no_telegram': True})
            _ws_send_subscribe(_ws_app, new_tokens)

        time_module.sleep(SUBSCRIPTION_POLL_SECONDS)


def main():
    log.info('MCX ticker service starting', extra={'no_telegram': True})
    if not _bootstrap_subscriptions():
        log.error('No MCX instruments resolved at startup - only register_subscription() tokens will stream')
    _start_kite_ws()
    poll_thread = threading.Thread(target=_subscription_poll_loop, daemon=True, name='sub-poll-mcx')
    poll_thread.start()

    while datetime.now().time() < RUN_UNTIL:
        time_module.sleep(30)
    log.info(f'{RUN_UNTIL} reached - MCX ticker service exiting for the day', extra={'no_telegram': True})


if __name__ == '__main__':
    main()
