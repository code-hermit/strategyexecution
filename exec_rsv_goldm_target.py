"""
Live execution of "mcx trading/rsv_goldm_target_nocp.py" on MCX GOLDMINI (GOLDM): a short ATM
straddle with a per-straddle profit target and stoploss and NO checkpoints. Market data from
Zerodha Kite (Redis feed from mcx_ticker_service.py, REST fallback), orders through AliceBlue -
same split and the same broker plumbing as exec_rsv_goldm.py (copied, not imported - every live
script in this folder is self-contained).

Rules (CFG below; backtested as --target-rs 2000 --stoploss-rs 8000 at 2 lots = 100 / 400 points
per unit, which is 1000 / 4000 Rs at 1 lot):
  - ENTRY_TIME (15:15): short the ATM CE + PE (ATM = strike nearest the option chain's underlying
    future). One straddle at a time.
  - Every poll (POLL_INTERVAL_SECONDS), the open straddle's P&L - both legs, entry fill vs live
    LTP - is checked:  >= +target_rs -> close both legs (TARGET);  <= -stoploss_rs -> close both
    legs (STOPLOSS). Both are checked here, by this process: there is NO resting stop order at the
    broker (a per-leg broker stop can't express a straddle-level stop - it would fire on ordinary
    one-sided moves the backtest never exits on). If this process is down, nothing stops the legs:
    run it under process_monitor and watch the heartbeats.
  - After a straddle closes, the next one starts (checked every poll):
      * immediately at the new ATM if the ATM strike differs from the ATM at the close;
      * at the same strike once the ATM straddle premium has reversed by reentry_rs from its value
        at the close - back UP after a TARGET (premium had fallen), back DOWN after a STOPLOSS.
  - daily_loss_limit_rs (realized + unrealized) closes everything and stops for the day.
  - EXIT_TIME (23:00): close whatever is open. NRML (LONGTERM) positions are never auto-squared-off
    by the broker, so this square-off is the only thing closing them.

Rs amounts are for CFG's lots: Rs = points x lots x UNITS_PER_LOT (GOLDM is quoted per 10 g and a
lot is 100 g, so 1 point = Rs 10 per lot; AliceBlue's MCX order quantity is in lots).

Orders are tagged 'rtG' + option (C/P) + role (E entry / X exit) + HHMM, e.g. 'rtGCE1515'. Do NOT run
this alongside exec_rsv_goldm.py on the same account: AliceBlue's positions endpoint can't tell the
two strategies' GOLDM legs apart.

On startup with GOLDM positions already open on the current chain (mid-day restart), they are
adopted as the current straddle, entry prices reconstructed from the order book (LTP fallback).

Run (DRY_RUN=false in the env or .env to place real orders; anything else simulates fills at LTP):
    python exec_rsv_goldm_target.py [GOLDM] [weekday codes, e.g. mtwhf]
Logs to exec_rsv_goldm_target_<SYMBOL>.log and stdout; lifecycle events and WARNING+ to Telegram.
"""

import csv
import io
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time as time_module
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time as dtime, timedelta, timezone
from typing import NamedTuple

import requests
from dotenv import load_dotenv

import zerodha_ltp_client

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# ── Logging / alerts ─────────────────────────────────────────────────────────────────────────────
_ARGV_SYMBOL = sys.argv[1].upper() if len(sys.argv) > 1 and sys.argv[1] else 'GOLDM'

LOG_FILE = os.path.join(os.path.dirname(__file__), f'exec_rsv_goldm_target_{_ARGV_SYMBOL}.log')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_TIMEOUT = 10

log = logging.getLogger(f'exec_rsv_goldm_target.{_ARGV_SYMBOL}')
log.setLevel(logging.INFO)
log.propagate = False
_formatter = logging.Formatter(f'%(asctime)s %(levelname)s [{_ARGV_SYMBOL} target] %(message)s')
for _handler in (logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)):
    _handler.setFormatter(_formatter)
    log.addHandler(_handler)


def _telegram_send(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage',
            json={'chat_id': TELEGRAM_CHAT_ID, 'text': message},
            timeout=TELEGRAM_TIMEOUT,
        )
    except Exception as exc:  # never let a Telegram hiccup break the strategy
        print(f'Telegram alert failed: {exc}', file=sys.stderr)


class TelegramHandler(logging.Handler):
    """Safety net: any WARNING+ log record gets pushed to Telegram automatically. Records from
    alert() itself are skipped (marked via `_alerted`) since alert() already sends them. Records
    marked extra={'no_telegram': True} are skipped too (throttled repeated failures)."""

    def emit(self, record):
        try:
            if getattr(record, '_alerted', False) or getattr(record, 'no_telegram', False):
                return
            _telegram_send(f'[{record.levelname}] {self.format(record)}')
        except Exception:
            self.handleError(record)


_telegram_handler = TelegramHandler(level=logging.WARNING)
_telegram_handler.setFormatter(logging.Formatter('%(message)s'))
log.addHandler(_telegram_handler)

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    log.warning('TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set in .env - Telegram alerts disabled')

_last_event = {'text': 'not started yet', 'at': None}


def alert(message, level=logging.INFO):
    """Log + always push to Telegram exactly once - use for events the user actually wants
    pinged about."""
    _last_event['text'] = message
    _last_event['at'] = datetime.now()
    try:
        log.log(level, message, extra={'_alerted': True})
    except Exception as exc:
        print(f'alert() logging failed: {exc}', file=sys.stderr)
    try:
        _telegram_send(message)
    except Exception as exc:
        print(f'alert() Telegram send failed: {exc}', file=sys.stderr)


FAILURE_ALERT_EVERY = 10


def _alert_failure_throttled(message, failure_count, level=logging.ERROR, every=FAILURE_ALERT_EVERY):
    try:
        if failure_count == 1 or failure_count % every == 0:
            alert(message, level=level)
        else:
            log.log(level, message, extra={'no_telegram': True})
    except Exception as exc:
        print(f'_alert_failure_throttled failed: {exc}', file=sys.stderr)


def _log_uncaught_exception(exc_type, exc_value, exc_tb):
    log.critical('Uncaught exception', exc_info=(exc_type, exc_value, exc_tb))


sys.excepthook = _log_uncaught_exception


def _log_and_exit_on_signal(signum, frame):
    log.critical(f'Received signal {signal.Signals(signum).name} ({signum}) - exiting')
    sys.exit(1)


for _sig in (signal.SIGTERM, signal.SIGHUP):
    signal.signal(_sig, _log_and_exit_on_signal)

DRY_RUN = os.getenv('DRY_RUN', 'true').lower() != 'false'  # set DRY_RUN=false to place real orders

# ── Strategy config ──────────────────────────────────────────────────────────────────────────────
WARMUP_TIME = dtime(15, 14)  # a minute ahead of ENTRY_TIME, so instrument/contract caches and the
# Redis feed are hot by the entry snapshot - no orders and no recorded prices during warm-up.
ENTRY_TIME = dtime(15, 15)
EXIT_TIME = dtime(23, 0)  # the backtest's exit - 30 min inside MCX's 23:30 close
POLL_INTERVAL_SECONDS = 5  # target/stop/re-entry/loss-limit check cadence (the backtest checks once a minute)
HEARTBEAT_INTERVAL = timedelta(minutes=30)
WARMUP_POLL_SECONDS = 2
RECONCILE_INTERVAL = timedelta(minutes=5)  # how often broker positions are compared with what this process thinks it holds

OPTION_TYPES = ('CE', 'PE')
DAY_CODE_TO_WEEKDAY = {'m': 'Monday', 't': 'Tuesday', 'w': 'Wednesday', 'h': 'Thursday', 'f': 'Friday'}
UNITS_PER_LOT = 10  # GOLDM: price per 10 g, lot 100 g -> 1 point = Rs 10 per lot

# Rs amounts are for `lots` lots. 1000 / 4000 / 500 / 5000 at 1 lot = the backtest's
# 2000 / 8000 / 1000 / 10000 at 2 lots (100 / 400 / 50 / 500 points per unit).
CFG = {
    'GOLDM': dict(
        strike_interval=500, lots=1, aliceblue_exchange='MCX', zerodha_options_exchange='MCX',
        target_rs=1000, stoploss_rs=4000, reentry_rs=500, daily_loss_limit_rs=5000,
    ),
}


def _parse_trade_weekdays(codes):
    """Compact weekday-code string -> set of weekday names, e.g. 'th' -> {'Tuesday', 'Thursday'}.
    None/empty trades every weekday. Codes: m/t/w/h/f."""
    if not codes:
        return set(DAY_CODE_TO_WEEKDAY.values())
    weekdays = set()
    for code in codes.lower():
        weekday = DAY_CODE_TO_WEEKDAY.get(code)
        if weekday is None:
            raise ValueError(f"unknown weekday code {code!r} in {codes!r} - use any combination of {''.join(DAY_CODE_TO_WEEKDAY)}")
        weekdays.add(weekday)
    return weekdays


def _today_str():
    return datetime.now().strftime('%Y-%m-%d')


def atm_strike(spot, strike_interval):
    return round(spot / strike_interval) * strike_interval


def _rs(points, cfg):
    return points * cfg['lots'] * UNITS_PER_LOT


def _points(rs, cfg):
    return rs / (cfg['lots'] * UNITS_PER_LOT)


# ── Zerodha (REST, Kite Connect) - market data only ─────────────────────────────────────────────
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

_zerodha_options_cache = {}  # symbol -> {'date', 'options'}
_zerodha_future_cache = {}  # symbol -> {'date', 'future'} - the chain's underlying future


def _load_zerodha_option_chain(symbol, cfg):
    """The nearest-expiry CE/PE chain for `symbol` from Kite's instrument dump, plus (cached
    alongside it in _zerodha_future_cache) the future that chain is written on: the nearest FUT
    expiring on/after the chain's expiry - NOT simply the nearest FUT, which in the week between an
    option expiry and its future's expiry is the old month's contract. Cached per calendar day."""
    today = _today_str()
    cached = _zerodha_options_cache.get(symbol)
    if cached and cached['date'] == today:
        return cached['options']

    resp = requests.get(
        f"{ZERODHA_BASE_URL}/instruments/{cfg['zerodha_options_exchange']}",
        headers=_zerodha_headers(ZERODHA_ACCESS_TOKEN), timeout=30,
    )
    resp.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(resp.text)))
    today_date = datetime.now().date()
    opts = [
        row for row in rows
        if row['name'] == symbol and row['instrument_type'] in OPTION_TYPES and row['expiry']
        and datetime.strptime(row['expiry'], '%Y-%m-%d').date() >= today_date
    ]
    if not opts:
        raise RuntimeError(f"No {symbol} option instruments found on Zerodha {cfg['zerodha_options_exchange']}")
    chain_expiry = min(row['expiry'] for row in opts)
    opts = [row for row in opts if row['expiry'] == chain_expiry]

    futs = sorted(
        (row for row in rows
         if row['name'] == symbol and row['instrument_type'] == 'FUT' and row['expiry'] >= chain_expiry),
        key=lambda row: row['expiry'],
    )
    if not futs:
        raise RuntimeError(f"No {symbol} future expiring on/after {chain_expiry} on Zerodha {cfg['zerodha_options_exchange']}")
    future = futs[0]
    log.info(f"{symbol} option chain {chain_expiry} ({len(opts)} contracts), underlying future {future['tradingsymbol']} (expiry {future['expiry']})")

    _zerodha_options_cache[symbol] = {'date': today, 'options': opts}
    _zerodha_future_cache[symbol] = {'date': today, 'future': future}
    return opts


def _zerodha_option_row(zerodha_options, strike, option_type):
    for row in zerodha_options:
        if int(float(row['strike'])) == strike and row['instrument_type'] == option_type:
            return row
    raise KeyError(f'no Zerodha instrument found for strike={strike} type={option_type}')


def _zerodha_quote_ltp(instrument_keys):
    """instrument_keys like ['MCX:GOLDM26OCT149000CE']. Returns key -> last_price. REST only - the
    hot path is zerodha_ltp_client.py's shared Redis cache; this is its fallback."""
    resp = requests.get(
        f'{ZERODHA_BASE_URL}/quote/ltp', headers=_zerodha_headers(ZERODHA_ACCESS_TOKEN),
        params=[('i', k) for k in instrument_keys], timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()['data']
    return {k: float(v['last_price']) for k, v in data.items()}


def get_spot_ltp(symbol, cfg):
    """Live LTP of the option chain's underlying future (see _load_zerodha_option_chain) - what
    ATM is measured off. Redis first (mcx_ticker_service.py), REST fallback."""
    _load_zerodha_option_chain(symbol, cfg)
    future = _zerodha_future_cache[symbol]['future']
    key = f"{cfg['zerodha_options_exchange']}:{future['tradingsymbol']}"
    token = int(future['instrument_token'])
    zerodha_ltp_client.register_subscription(token)
    return zerodha_ltp_client.get_ltp(token, rest_fetch=lambda: _zerodha_quote_ltp([key])[key], log=log)


# ── AliceBlue (REST, v3 open-api) - order placement only ────────────────────────────────────────
ALICEBLUE_TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'aliceblue_token.json')
ALICEBLUE_BASE_URL = 'https://a3.aliceblueonline.com/open-api/od/v1'
ALICEBLUE_CONTRACT_MASTER_URL = 'https://v2api.aliceblueonline.com/restpy/static/contract_master/V2/{exchange}'
LIMIT_OFFSET_PCT = 0.05  # entry SELL limit this far below LTP (marketable); exits start here and chase
FILL_POLL_TIMEOUT = 10
ALICEBLUE_PRODUCT = 'LONGTERM'  # = NRML - AliceBlue blocks MIS/INTRADAY on MCX GOLDM options (see exec_rsv_goldm.py)
TERMINAL_ORDER_STATUSES = {'complete', 'rejected', 'cancelled'}
_ALICEBLUE_EMPTY_RESULT_STATUSES = {'EC920'}
EXIT_CHASE_WAIT_SECONDS = 2
EXIT_CHASE_OFFSET_STEP = 0.05
EXIT_CHASE_MAX_OFFSET_PCT = 0.15  # under AliceBlue's ~20%-from-LTP MCX price band


def _status(o):
    """Order status, lowercased, with every cancel spelling ('CANCELED', 'Cancelled', ...) mapped to
    'cancelled' - see exec_rsv_goldm.py."""
    status = str(o.get('orderStatus', '')).strip().lower()
    return 'cancelled' if status.startswith('cancel') else status


def _aliceblue_headers(session_id):
    return {'Authorization': f'Bearer {session_id}', 'Content-Type': 'application/json'}


def _load_aliceblue_session():
    with open(ALICEBLUE_TOKEN_FILE) as f:
        return json.load(f)['userSession']


def _aliceblue_session_is_valid(session_id):
    resp = requests.get(f'{ALICEBLUE_BASE_URL}/limits/', headers=_aliceblue_headers(session_id), timeout=REQUEST_TIMEOUT)
    return resp.ok


def _valid_aliceblue_session():
    if not os.path.exists(ALICEBLUE_TOKEN_FILE):
        raise RuntimeError('No AliceBlue session found - run aliceblue_token_generation.py to log in')
    session_id = _load_aliceblue_session()
    if not _aliceblue_session_is_valid(session_id):
        raise RuntimeError('AliceBlue session expired - run aliceblue_token_generation.py to log in again')
    return session_id


try:
    ALICEBLUE_SESSION_ID = _valid_aliceblue_session()
except Exception:
    log.critical('AliceBlue auth failed', exc_info=True)
    raise


def _aliceblue_result(path, data):
    if data.get('status') == 'Ok':
        return data['result']
    if data.get('status') in _ALICEBLUE_EMPTY_RESULT_STATUSES:
        return []
    raise RuntimeError(f'AliceBlue request to {path} failed: {data}')


def _aliceblue_get(path):
    resp = requests.get(ALICEBLUE_BASE_URL + path, headers=_aliceblue_headers(ALICEBLUE_SESSION_ID), timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return _aliceblue_result(path, resp.json())


def _aliceblue_post(path, payload):
    resp = requests.post(
        ALICEBLUE_BASE_URL + path, json=payload, headers=_aliceblue_headers(ALICEBLUE_SESSION_ID), timeout=REQUEST_TIMEOUT,
    )
    if not resp.ok:
        raise RuntimeError(f'AliceBlue POST {path} failed ({resp.status_code}): {resp.text}')
    return _aliceblue_result(path, resp.json())


class Instrument(NamedTuple):
    token: int
    symbol: str
    name: str
    lot_size: int
    tick_size: float
    exchange: str


_aliceblue_contracts_cache = {}  # symbol -> {'date', 'contracts'}


def _load_aliceblue_contracts(symbol, cfg):
    """The nearest-expiry CE/PE contracts for `symbol` from AliceBlue's contract master (token /
    lot size / tick size for orders). Cached per calendar day."""
    today = _today_str()
    cached = _aliceblue_contracts_cache.get(symbol)
    if cached and cached['date'] == today:
        return cached['contracts']

    exchange = cfg['aliceblue_exchange']
    resp = requests.get(ALICEBLUE_CONTRACT_MASTER_URL.format(exchange=exchange), timeout=30)
    resp.raise_for_status()
    today_date = datetime.now().date()
    opts = [
        c for c in resp.json()[exchange]
        if c['symbol'] == symbol and c['option_type'] in OPTION_TYPES
        and datetime.fromtimestamp(c['expiry_date'] / 1000, tz=timezone.utc).date() >= today_date
    ]
    if not opts:
        raise RuntimeError(f'No {symbol} contracts found on AliceBlue {exchange}')
    nearest = min(c['expiry_date'] for c in opts)
    contracts = [c for c in opts if c['expiry_date'] == nearest]

    _aliceblue_contracts_cache[symbol] = {'date': today, 'contracts': contracts}
    return contracts


def _to_instrument(contract):
    return Instrument(
        token=int(contract['token']), symbol=contract['symbol'],
        name=contract['trading_symbol'], lot_size=int(contract['lot_size']),
        tick_size=float(contract['tick_size']), exchange=contract['exch'],
    )


def _round_to_tick(price, tick_size):
    return round(round(price / tick_size) * tick_size, 2)


def _order_book():
    return _aliceblue_get('/orders/book')


BROKER_FETCH_MIN_GAP_SECONDS = 2.0  # order book and /positions each read at most once every 2s per process


class _GatedFetch:
    """A broker endpoint read shared by every caller in this process, fetched at most once every
    BROKER_FETCH_MIN_GAP_SECONDS - see exec_rsv_goldm.py for the full rationale. Fails fast."""

    def __init__(self, name, fetch_fn):
        self._name = name
        self._fetch_fn = fetch_fn
        self._lock = threading.Lock()
        self._data = None
        self._data_at = None
        self._attempt_at = None

    def get(self, fresh_after=None):
        with self._lock:
            now = time_module.time()
            if fresh_after is None:
                fresh_after = now - BROKER_FETCH_MIN_GAP_SECONDS
            if self._data_at is not None and self._data_at >= fresh_after:
                return self._data
            if self._attempt_at is not None:
                wait = self._attempt_at + BROKER_FETCH_MIN_GAP_SECONDS - now
                if wait > 0:
                    time_module.sleep(wait)
            self._attempt_at = time_module.time()
            data = self._fetch_fn()
            self._data, self._data_at = data, self._attempt_at
            return data


_order_book_fetch = _GatedFetch('order book', _order_book)
_positions_fetch = _GatedFetch('positions', lambda: _aliceblue_get('/positions'))


def _get_order_book(fresh_after=None):
    return _order_book_fetch.get(fresh_after)


def _read_order_book_or_none(label):
    try:
        return _get_order_book(fresh_after=time_module.time())
    except Exception as exc:
        log.warning(f'{label}: order book read failed ({exc}) - retrying', extra={'no_telegram': True})
        return None


def _find_order(order_book, order_id):
    for o in order_book or []:
        if o.get('brokerOrderId') == order_id:
            return o
    return None


# ── Order tags ──────────────────────────────────────────────────────────────────────────────────
# 'rt' + underlying + option (C/P) + role (E entry / X exit) + leg id (HHMM opened) - distinct from
# exec_rsv_goldm.py's 'rv...' tags.
_TAG_UNDERLYING = {'GOLDM': 'G'}
TAG_PREFIX = 'rt' + _TAG_UNDERLYING.get(_ARGV_SYMBOL, _ARGV_SYMBOL[:1])
TAG_ENTRY, TAG_EXIT = 'E', 'X'


def _new_leg_id():
    return datetime.now().strftime('%H%M')


def _order_tag(opt, role, leg_id):
    tag = f'{TAG_PREFIX}{opt[0]}{role}{leg_id or "0000"}'
    assert len(tag) < 10, tag
    return tag


def _tag_of(o):
    for key in ('remarks', 'orderTag'):
        if key in o:
            tag = str(o.get(key) or '').strip()
            return '' if tag in ('', '--', 'NA') else tag
    return None


def _counts_toward_position(o):
    """Our own orders plus untagged (manual) ones - orders tagged by another strategy are left out."""
    tag = _tag_of(o)
    return not tag or tag.lower().startswith(TAG_PREFIX.lower())


# ── Orders ──────────────────────────────────────────────────────────────────────────────────────
def _place_order(transaction_type, instrument, quantity, order_type, price='0', trigger_price=None, order_tag=None):
    payload = [{
        'exchange': instrument.exchange,
        'instrumentId': str(instrument.token),
        'transactionType': transaction_type,
        'quantity': quantity,
        'product': ALICEBLUE_PRODUCT,
        'orderComplexity': 'REGULAR',
        'orderType': order_type,
        'validity': 'DAY',
        'price': str(price),
        'slTriggerPrice': str(trigger_price) if trigger_price is not None else '',
        'orderTag': order_tag or '',
    }]
    result = _aliceblue_post('/orders/placeorder', payload)
    return result[0] if isinstance(result, list) and len(result) == 1 else result


def _cancel_order(broker_order_id):
    return _aliceblue_post('/orders/cancel', {'brokerOrderId': broker_order_id})


def _modify_order(broker_order_id, quantity, order_type, price, trigger_price=None):
    payload = {
        'brokerOrderId': broker_order_id,
        'quantity': quantity,
        'orderType': order_type,
        'price': str(price),
        'slTriggerPrice': str(trigger_price) if trigger_price is not None else '',
        'validity': 'DAY',
    }
    return _aliceblue_post('/orders/modify', payload)


def _wait_for_fill_price(broker_order_id):
    deadline = time_module.time() + FILL_POLL_TIMEOUT
    while time_module.time() < deadline:
        book = _read_order_book_or_none(f'fill poll {broker_order_id}')
        o = _find_order(book, broker_order_id)
        if o is not None:
            status = _status(o)
            if status == 'rejected':
                raise RuntimeError(f'order {broker_order_id} rejected: {o.get("rejectionReason")}')
            if status == 'complete':
                return float(o.get('averageTradedPrice') or 0)
    raise TimeoutError(f'order {broker_order_id} not filled within {FILL_POLL_TIMEOUT}s')


def _poll_order_status(broker_order_id, deadline):
    while time_module.time() < deadline:
        book = _read_order_book_or_none(f'exit poll {broker_order_id}')
        o = _find_order(book, broker_order_id)
        if o is not None:
            status = _status(o)
            if status == 'rejected':
                raise RuntimeError(f'order {broker_order_id} rejected: {o.get("rejectionReason")}')
            if status == 'complete':
                return float(o.get('averageTradedPrice') or 0)
    return None


CANCEL_CONFIRM_SECONDS = 6


def _cancel_and_confirm(order_id, label):
    """Cancel `order_id`, then read the order book until it shows terminal. Returns (status,
    fill_price) - 'complete' means it filled before the cancel landed."""
    try:
        _cancel_order(order_id)
    except Exception as exc:
        log.warning(f'{label}: cancel of order {order_id} failed ({exc}) - checking the order book')
    deadline = time_module.time() + CANCEL_CONFIRM_SECONDS
    while True:
        book = _read_order_book_or_none(f'{label} cancel check')
        o = _find_order(book, order_id) if book is not None else None
        status = _status(o) if o else None
        if status == 'complete':
            return status, float(o.get('averageTradedPrice') or 0) or None
        if status in TERMINAL_ORDER_STATUSES or time_module.time() >= deadline:
            return status, None


def _exit_chase_fill(instrument, transaction_type, quantity, get_fresh_ltp, order_tag):
    """Exit LIMIT order, repriced wider every EXIT_CHASE_WAIT_SECONDS until it fills (modify in
    place; cancel+place-new once a modify fails) - see exec_rsv_goldm.py."""
    sign = 1 if transaction_type == 'BUY' else -1
    offset_pct = LIMIT_OFFSET_PCT
    ltp = get_fresh_ltp()
    if ltp is None:
        raise RuntimeError(f'{instrument.name}: no LTP available to place exit order')
    price = _round_to_tick(ltp * (1 + sign * offset_pct), instrument.tick_size)
    order = _place_order(transaction_type, instrument, quantity, 'LIMIT', price=str(price), order_tag=order_tag)
    order_no = order.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} exit order rejected: {order}')

    attempt = 1
    modify_broken = False
    while True:
        fill_price = _poll_order_status(order_no, time_module.time() + EXIT_CHASE_WAIT_SECONDS)
        if fill_price is not None:
            if attempt > 1:
                log.info(f'{instrument.name} exit filled on attempt {attempt} at offset {offset_pct:.0%}')
            return fill_price

        attempt += 1
        offset_pct = min(offset_pct + EXIT_CHASE_OFFSET_STEP, EXIT_CHASE_MAX_OFFSET_PCT)
        ltp = get_fresh_ltp()
        if ltp is None:
            raise RuntimeError(f'{instrument.name}: no LTP available to reprice exit order')
        price = _round_to_tick(ltp * (1 + sign * offset_pct), instrument.tick_size)

        if not modify_broken:
            log.warning(f'{instrument.name} exit not filled within {EXIT_CHASE_WAIT_SECONDS}s (attempt {attempt}, offset {offset_pct:.0%}) - modifying to {price}')
            try:
                _modify_order(order_no, quantity, 'LIMIT', price)
                continue
            except Exception as exc:
                modify_broken = True
                log.warning(f'{instrument.name}: modify of order {order_no} failed ({exc}) - cancel+place-new from now on')

        status, fill_price = _cancel_and_confirm(order_no, f'{instrument.name} exit')
        if status == 'complete':
            return fill_price
        if status not in TERMINAL_ORDER_STATUSES:
            log.warning(f'{instrument.name}: exit order {order_no} still {status!r} after cancel - polling it again, not placing another')
            continue
        order = _place_order(transaction_type, instrument, quantity, 'LIMIT', price=str(price), order_tag=order_tag)
        order_no = order.get('brokerOrderId')
        if not order_no:
            raise RuntimeError(f'{instrument.name} exit order rejected: {order}')


def get_open_legs(contracts_by_token, fresh_after=None):
    """token -> position dict for open (nonzero net qty) positions on the current chain's contracts."""
    positions = _positions_fetch.get(fresh_after)
    return {
        int(p['instrumentId']): p for p in positions
        if int(p['instrumentId']) in contracts_by_token and int(p.get('netQuantity', 0)) != 0
    }


# ── Retry/backoff for read-only calls ───────────────────────────────────────────────────────────
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY = 5
MIN_CALL_INTERVAL = {'_load_zerodha_option_chain': 3.5}
DEFAULT_MIN_CALL_INTERVAL = 0.5
_last_call_at = {}


def _throttle(key):
    min_interval = MIN_CALL_INTERVAL.get(key, DEFAULT_MIN_CALL_INTERVAL)
    last = _last_call_at.get(key)
    now = time_module.monotonic()
    if last is not None and min_interval - (now - last) > 0:
        time_module.sleep(min_interval - (now - last))
    _last_call_at[key] = time_module.monotonic()


def _resilient_call(fn, *args, **kwargs):
    for attempt in range(RETRY_MAX_ATTEMPTS):
        _throttle(fn.__name__)
        try:
            return fn(*args, **kwargs)
        except requests.exceptions.RequestException as exc:
            if attempt == RETRY_MAX_ATTEMPTS - 1:
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            log.warning(f'{fn.__name__} failed ({exc}) - retrying in {delay:.0f}s (attempt {attempt + 1}/{RETRY_MAX_ATTEMPTS})')
            time_module.sleep(delay)


# ── Market snapshot ─────────────────────────────────────────────────────────────────────────────
def _fetch_market(symbol, cfg, day):
    """Spot/ATM, AliceBlue contracts, and live LTPs (Redis first, one batched REST fallback) for the
    ATM straddle, the open straddle's legs, and the strike a pending re-entry waits on."""
    zerodha_options = _resilient_call(_load_zerodha_option_chain, symbol, cfg)
    spot = _resilient_call(get_spot_ltp, symbol, cfg)
    atm = atm_strike(spot, cfg['strike_interval'])

    contracts = _resilient_call(_load_aliceblue_contracts, symbol, cfg)
    contracts_by_token = {int(c['token']): c for c in contracts}
    contracts_by_strike_type = {(int(float(c['strike_price'])), c['option_type']): c for c in contracts}

    needed = {(atm, 'CE'), (atm, 'PE')}
    for opt, leg in (day['straddle'] or {}).items():
        needed.add((leg['strike'], opt))
    if day['waiting'] is not None:
        needed.update({(day['waiting']['strike'], 'CE'), (day['waiting']['strike'], 'PE')})

    key_to_strike_type, token_to_key = {}, {}
    for strike, opt in needed:
        try:
            row = _zerodha_option_row(zerodha_options, strike, opt)
        except KeyError:
            log.warning(f'{opt} {strike}: no Zerodha instrument found - skipping this quote')
            continue
        key = f"{cfg['zerodha_options_exchange']}:{row['tradingsymbol']}"
        key_to_strike_type[key] = (strike, opt)
        token_to_key[int(row['instrument_token'])] = key

    zerodha_ltp_client.register_subscriptions(list(token_to_key))
    token_to_price = zerodha_ltp_client.get_ltps(
        token_to_key, rest_fetch_batch=lambda keys: _resilient_call(_zerodha_quote_ltp, keys), log=log,
    ) if token_to_key else {}
    price = {key_to_strike_type[token_to_key[t]]: p for t, p in token_to_price.items()}
    return dict(spot=spot, atm=atm, price=price,
                contracts_by_token=contracts_by_token, contracts_by_strike_type=contracts_by_strike_type)


def _fetch_market_until_success(symbol, cfg, day):
    attempt = 0
    while True:
        try:
            return _fetch_market(symbol, cfg, day)
        except Exception as exc:
            attempt += 1
            if attempt == 1 or attempt % 10 == 0:
                alert(f'Could not fetch market data ({exc}) - still retrying (attempt {attempt})', level=logging.ERROR)
            time_module.sleep(30)


def _straddle_premium(market, strike):
    ce, pe = market['price'].get((strike, 'CE')), market['price'].get((strike, 'PE'))
    return None if ce is None or pe is None else ce + pe


# ── Legs ────────────────────────────────────────────────────────────────────────────────────────
def _settle_timed_out_entry(order_no, instrument):
    """An entry SELL that didn't confirm filled within FILL_POLL_TIMEOUT: cancel it and read what
    became of it. Returns its fill price if it filled anyway; raises if it's dead (nothing filled)
    or can't be confirmed either way (CRITICAL alert - check manually)."""
    label = f'{instrument.name} entry'
    status, fill_price = _cancel_and_confirm(order_no, label)
    if status == 'complete' and fill_price:
        log.warning(f'{label} order {order_no} filled late @ {fill_price} - tracking it')
        return fill_price
    if status in ('cancelled', 'rejected'):
        o = _find_order(_read_order_book_or_none(f'{label} filled-qty check'), order_no)
        filled = int((o or {}).get('filledQuantity') or 0)
        if o is not None and not filled:
            raise RuntimeError(f'{label} order {order_no} not filled within {FILL_POLL_TIMEOUT}s - cancelled, nothing filled')
    alert(f'{label}: order {order_no} could not be confirmed filled or dead (status {status!r}) - CHECK MANUALLY', level=logging.CRITICAL)
    raise RuntimeError(f'{label} order {order_no} unresolved ({status!r})')


def _short_leg(instrument, quantity, ltp, opt, leg_id):
    """SELL to open at a marketable LIMIT. Returns the fill price (the LTP in DRY_RUN)."""
    price = _round_to_tick(ltp * (1 - LIMIT_OFFSET_PCT), instrument.tick_size)
    log.info(f'{"[DRY RUN] " if DRY_RUN else ""}SELL {quantity} x {instrument.name} LIMIT @ {price} (ltp {ltp})')
    if DRY_RUN:
        return ltp
    order = _place_order('SELL', instrument, quantity, 'LIMIT', price=str(price), order_tag=_order_tag(opt, TAG_ENTRY, leg_id))
    order_no = order.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} entry order rejected: {order}')
    try:
        return _wait_for_fill_price(order_no)
    except TimeoutError:
        return _settle_timed_out_entry(order_no, instrument)


def _buy_back_leg(leg, opt, fallback_ltp):
    """BUY to close a short leg, chasing until filled. Returns the fill price (LTP in DRY_RUN)."""
    instrument = leg['instrument']

    def _fresh_ltp():
        key = f'{instrument.exchange}:{instrument.name}'
        try:
            ltp = _zerodha_quote_ltp([key]).get(key)
        except Exception:
            ltp = None
        return ltp if ltp is not None else fallback_ltp

    log.info(f'{"[DRY RUN] " if DRY_RUN else ""}BUY (close) {leg["quantity"]} x {instrument.name}')
    if DRY_RUN:
        return _fresh_ltp()
    return _exit_chase_fill(instrument, 'BUY', leg['quantity'], _fresh_ltp, order_tag=_order_tag(opt, TAG_EXIT, leg['leg_id']))


def _in_parallel(tasks):
    """Run one no-arg callable per leg concurrently; returns {opt: result or Exception}."""
    results = {}
    with ThreadPoolExecutor(max_workers=len(tasks) or 1) as pool:
        futures = {opt: pool.submit(fn) for opt, fn in tasks.items()}
        for opt, fut in futures.items():
            try:
                results[opt] = fut.result()
            except Exception as exc:
                results[opt] = exc
    return results


# ── Strategy ────────────────────────────────────────────────────────────────────────────────────
def _enter_straddle(day, market, cfg, strike, why):
    """Short whichever legs of the straddle at `strike` aren't already open. A leg that fails stays
    missing - the next poll retries it at the same strike (see run_day)."""
    straddle = day['straddle'] or {}
    tasks = {}
    for opt in OPTION_TYPES:
        if opt in straddle:
            continue
        contract = market['contracts_by_strike_type'].get((strike, opt))
        ltp = market['price'].get((strike, opt))
        if contract is None or ltp is None:
            log.warning(f'{opt} {strike}: contract or live quote unavailable - leg not entered this poll')
            continue
        instrument = _to_instrument(contract)
        quantity = instrument.lot_size * cfg['lots']  # MCX quantity is in lots (GOLDM lot_size = 1)
        leg_id = _new_leg_id()
        tasks[opt] = (lambda i=instrument, q=quantity, l=ltp, o=opt, lid=leg_id: (i, q, lid, _short_leg(i, q, l, o, lid)))
    if not tasks:
        return
    results = _in_parallel(tasks)
    for opt, res in results.items():
        if isinstance(res, Exception):
            alert(f'{opt} {strike} entry failed: {res}', level=logging.ERROR)
            continue
        instrument, quantity, leg_id, fill = res
        straddle[opt] = dict(instrument=instrument, strike=strike, entry_price=fill, quantity=quantity, leg_id=leg_id)
    if straddle and day['straddle'] is None:
        day['straddle_count'] += 1
    day['straddle'] = straddle or None
    day['straddle_strike'] = strike
    if straddle:
        legs = ', '.join(f"{o} {l['instrument'].name} @ {l['entry_price']}" for o, l in straddle.items())
        alert(f'{why}: short straddle #{day["straddle_count"]} at {strike} - {legs}')


def _straddle_points(day, market):
    """Open straddle's P&L, points per unit (entry fill - live LTP, summed over its legs); None if a
    leg has no quote this poll."""
    total = 0.0
    for opt, leg in (day['straddle'] or {}).items():
        ltp = market['price'].get((leg['strike'], opt))
        if ltp is None:
            return None
        total += leg['entry_price'] - ltp
    return total


def _close_straddle(day, market, cfg, reason):
    """Buy back every open leg (in parallel). Legs that fail to close stay in the straddle (and get
    retried); returns True only if everything is closed."""
    straddle = day['straddle'] or {}
    if not straddle:
        return True
    results = _in_parallel({
        opt: (lambda l=leg, o=opt: _buy_back_leg(l, o, market['price'].get((l['strike'], o))))
        for opt, leg in straddle.items()
    })
    parts = []
    for opt, res in results.items():
        leg = straddle[opt]
        if isinstance(res, Exception) or res is None:
            alert(f'{reason}: closing {leg["instrument"].name} failed ({res}) - still OPEN, will retry', level=logging.CRITICAL)
            continue
        pts = leg['entry_price'] - res
        day['realized_pts'] += pts
        parts.append(f"{opt} {leg['entry_price']} -> {res} ({_rs(pts, cfg):+,.0f})")
        del straddle[opt]
    day['straddle'] = straddle or None
    if parts:
        alert(f'{reason}: closed straddle #{day["straddle_count"]} - {"; ".join(parts)} | day {_rs(day["realized_pts"], cfg):+,.0f} Rs')
    return day['straddle'] is None


def _finish_close(day, market, cfg, reason):
    """Close the straddle for `reason`; if a leg stays open, remember the reason in day['closing']
    so every following poll retries the close (never re-enters the leg already closed). Once flat:
    DAILY_LOSS_LIMIT halts the day; TARGET / STOPLOSS start the wait for a restart."""
    if not _close_straddle(day, market, cfg, reason):
        day['closing'] = reason
        return
    day['closing'] = None
    if reason == 'DAILY_LOSS_LIMIT':
        day['halted'] = True
    elif reason in ('TARGET', 'STOPLOSS'):
        day['waiting'] = dict(strike=market['atm'], premium=_straddle_premium(market, market['atm']),
                              direction=1 if reason == 'TARGET' else -1)
        log.info(f"waiting to restart: ATM {market['atm']} change, or ATM premium "
                 f"{'+' if reason == 'TARGET' else '-'}{_points(cfg['reentry_rs'], cfg):.0f} pts from {day['waiting']['premium']}")


def _strategy_tick(day, market, cfg):
    """One poll: target / stop / daily loss limit on an open straddle, re-entry when flat."""
    target_pts, stop_pts = _points(cfg['target_rs'], cfg), _points(cfg['stoploss_rs'], cfg)
    reentry_pts, limit_pts = _points(cfg['reentry_rs'], cfg), _points(cfg['daily_loss_limit_rs'], cfg)

    if day['closing'] is not None:  # a close that left a leg open - finish it before anything else
        _finish_close(day, market, cfg, day['closing'])
        return

    if day['straddle']:
        if len(day['straddle']) < len(OPTION_TYPES):  # a leg that failed to enter - retry it
            _enter_straddle(day, market, cfg, day['straddle_strike'], 'retry missing leg')
        pts = _straddle_points(day, market)
        if pts is None:
            return
        day['last_straddle_pts'] = pts
        if day['realized_pts'] + pts <= -limit_pts:
            alert(f'Daily loss limit: day {_rs(day["realized_pts"] + pts, cfg):+,.0f} Rs <= -{cfg["daily_loss_limit_rs"]:,} - closing and stopping for the day', level=logging.WARNING)
            _finish_close(day, market, cfg, 'DAILY_LOSS_LIMIT')
            return
        hit = 'TARGET' if pts >= target_pts else 'STOPLOSS' if pts <= -stop_pts else None
        if hit:
            log.info(f'{hit}: straddle {_rs(pts, cfg):+,.0f} Rs')
            _finish_close(day, market, cfg, hit)
        return

    if day['waiting'] is not None:
        w = day['waiting']
        if market['atm'] != w['strike']:
            day['waiting'] = None
            _enter_straddle(day, market, cfg, market['atm'], f"ATM moved {w['strike']} -> {market['atm']}")
            return
        cur = _straddle_premium(market, w['strike'])
        if w['premium'] is None:
            w['premium'] = cur
        elif cur is not None and (cur - w['premium']) * w['direction'] >= reentry_pts:
            day['waiting'] = None
            _enter_straddle(day, market, cfg, market['atm'], f"ATM premium {w['premium']} -> {cur}")
        return

    _enter_straddle(day, market, cfg, market['atm'], 'entry')  # initial entry, or a retry of a failed one


def _adopt_open_positions(day, market):
    """Mid-day restart: adopt open GOLDM positions on the current chain as the straddle."""
    open_legs = _resilient_call(get_open_legs, market['contracts_by_token'])
    if not open_legs:
        return False
    straddle = {}
    for token, pos in open_legs.items():
        contract = market['contracts_by_token'][token]
        opt, strike = contract['option_type'], int(float(contract['strike_price']))
        qty = int(pos['netQuantity'])
        if qty > 0 or opt in straddle:
            alert(f'Unexpected position {contract["trading_symbol"]} net {qty} - not adopted, handle manually', level=logging.CRITICAL)
            continue
        entry = _infer_entry_price(token, abs(qty))
        if entry is None:
            entry = market['price'].get((strike, opt)) or 0.0
            log.warning(f"{opt} {strike}: couldn't reconstruct entry price - using live LTP {entry} (approximate)")
        straddle[opt] = dict(instrument=_to_instrument(contract), strike=strike, entry_price=entry,
                             quantity=abs(qty), leg_id=_new_leg_id())
    if straddle:
        day['straddle'], day['straddle_count'] = straddle, 1
        day['straddle_strike'] = next(iter(straddle.values()))['strike']
        alert('Adopted open positions: ' + ', '.join(f"{o} {l['instrument'].name} @ {l['entry_price']:.2f}" for o, l in straddle.items()), level=logging.WARNING)
    return bool(straddle)


def _infer_entry_price(token, quantity):
    """Weighted average of the most recent complete SELL fills on `token` (ours or manual)."""
    try:
        orders = _get_order_book()
    except Exception as exc:
        log.warning(f'could not read order book to reconstruct entry for token {token}: {exc}')
        return None
    fills = [o for o in orders if str(o.get('instrumentId')) == str(token) and _counts_toward_position(o)
             and str(o.get('transactionType', '')).upper() == 'SELL' and _status(o) == 'complete']
    fills.sort(key=lambda o: o.get('orderGeneratedTime') or o.get('orderEntryTime') or o.get('brokerOrderId', ''), reverse=True)
    remaining, weighted, covered = quantity, 0.0, 0
    for o in fills:
        q, p = int(o.get('quantity') or o.get('filledQuantity') or 0), float(o.get('averageTradedPrice') or 0)
        if q <= 0 or p <= 0:
            continue
        take = min(q, remaining)
        weighted, covered, remaining = weighted + take * p, covered + take, remaining - take
        if remaining <= 0:
            break
    return weighted / covered if covered else None


def _reconcile(day, market):
    """Compare broker positions on the current chain with the straddle this process holds; alert on
    any mismatch (no automatic action - a manual trade or a missed fill needs a human)."""
    if DRY_RUN:
        return
    try:
        open_legs = get_open_legs(market['contracts_by_token'])
    except Exception as exc:
        log.warning(f'reconcile: positions read failed ({exc})', extra={'no_telegram': True})
        return
    broker = {int(t): abs(int(p['netQuantity'])) for t, p in open_legs.items()}
    ours = {leg['instrument'].token: leg['quantity'] for leg in (day['straddle'] or {}).values()}
    if broker != ours:
        alert(f'Position mismatch - broker {broker} vs strategy {ours}. Check manually (another strategy or a manual trade on GOLDM?)', level=logging.CRITICAL)


def _send_heartbeat(day, cfg, symbol, now):
    if day['straddle']:
        legs = ', '.join(f"{o} {l['instrument'].name} @ {l['entry_price']}" for o, l in day['straddle'].items())
        legs += f" | straddle {_rs(day.get('last_straddle_pts') or 0, cfg):+,.0f} Rs"
    elif day['waiting']:
        legs = f"flat, waiting to restart (ATM {day['waiting']['strike']})"
    else:
        legs = 'flat'
    last = f"{_last_event['text']} ({_last_event['at']:%H:%M:%S})" if _last_event['at'] else 'none yet'
    message = (f"🟢 still running - {symbol} target {now:%H:%M:%S}\n{legs}\n"
               f"day realized {_rs(day['realized_pts'], cfg):+,.0f} Rs, straddles {day['straddle_count']}\nlast event: {last}")
    log.info(f'heartbeat: {message}')
    _telegram_send(message)


# ── Day driver ──────────────────────────────────────────────────────────────────────────────────
def _sleep_until(target_time, label):
    wait = (datetime.combine(datetime.now().date(), target_time) - datetime.now()).total_seconds()
    if wait > 0:
        log.info(f'waiting until {label} ({target_time})...')
        time_module.sleep(wait)


def _mcx_ticker_running():
    return subprocess.run(['pgrep', '-f', r'mcx_ticker_service\.py'],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0


def _wait_for_mcx_ticker(symbol):
    """Blocks until mcx_ticker_service.py is running; False if EXIT_TIME arrives first."""
    if _mcx_ticker_running():
        return True
    log.info(f'mcx_ticker_service.py is not running - {symbol} waiting for it before starting')
    while not _mcx_ticker_running():
        if datetime.now().time() >= EXIT_TIME:
            log.info(f'mcx_ticker_service.py never started before {EXIT_TIME} - {symbol} NOT trading today')
            return False
        time_module.sleep(5)
    return True


def run_day(symbol, trade_weekdays):
    today_name = datetime.now().strftime('%A')
    if today_name not in trade_weekdays:
        log.info(f'{today_name} is not in TRADE_WEEKDAYS ({sorted(trade_weekdays)}) - not trading')
        return
    if not _wait_for_mcx_ticker(symbol):
        return

    cfg = CFG[symbol]
    alert(f"GOLDM straddle target/stop starting for {symbol} - {today_name} {datetime.now():%Y-%m-%d} | "
          f"{cfg['lots']} lot(s), target {cfg['target_rs']:,} / stop {cfg['stoploss_rs']:,} / restart {cfg['reentry_rs']:,} / "
          f"day limit {cfg['daily_loss_limit_rs']:,} Rs{' [DRY RUN]' if DRY_RUN else ''}")

    day = dict(straddle=None, straddle_strike=None, straddle_count=0, waiting=None,
               closing=None, realized_pts=0.0, halted=False, last_straddle_pts=None)

    _sleep_until(WARMUP_TIME, 'warm-up start')
    market = _fetch_market_until_success(symbol, cfg, day)
    while datetime.now().time() < ENTRY_TIME:
        time_module.sleep(WARMUP_POLL_SECONDS)
        try:
            market = _fetch_market(symbol, cfg, day)
        except Exception as exc:
            log.warning(f'warm-up market fetch failed ({exc}) - keeping previous snapshot')
    log.info(f'entry snapshot at {datetime.now():%H:%M:%S.%f} - spot={market["spot"]} atm={market["atm"]}')

    if DRY_RUN or not _adopt_open_positions(day, market):
        _enter_straddle(day, market, cfg, market['atm'], 'entry')

    next_heartbeat = datetime.now() + HEARTBEAT_INTERVAL
    next_reconcile = datetime.now() + RECONCILE_INTERVAL
    failures = 0
    first = True
    while not day['halted']:
        now = datetime.now()
        if now.time() >= EXIT_TIME:
            break
        try:
            if not first:
                market = _fetch_market(symbol, cfg, day)
            first = False
            _strategy_tick(day, market, cfg)
            if now >= next_reconcile:
                _reconcile(day, market)
                next_reconcile = now + RECONCILE_INTERVAL
            if now >= next_heartbeat:
                _send_heartbeat(day, cfg, symbol, now)
                next_heartbeat += HEARTBEAT_INTERVAL
            failures = 0
        except Exception as exc:
            failures += 1
            _alert_failure_throttled(f'Poll failed, retrying next cycle: {exc}', failures)
        remaining = (datetime.combine(now.date(), EXIT_TIME) - datetime.now()).total_seconds()
        time_module.sleep(max(0, min(POLL_INTERVAL_SECONDS, remaining)))

    if not day['halted']:
        log.info(f'{EXIT_TIME} reached - squaring off')
    for attempt in range(RETRY_MAX_ATTEMPTS):
        try:
            if _close_straddle(day, _fetch_market(symbol, cfg, day), cfg, 'EOD'):
                break
            raise RuntimeError('some legs still open')
        except Exception as exc:
            if attempt == RETRY_MAX_ATTEMPTS - 1:
                alert(f'{symbol}: final square-off failed after {RETRY_MAX_ATTEMPTS} attempts - positions may still be OPEN, check manually: {exc}', level=logging.CRITICAL)
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            log.warning(f'final square-off failed ({exc}) - retrying in {delay:.0f}s')
            time_module.sleep(delay)

    alert(f'GOLDM straddle target/stop done for {symbol} - {day["straddle_count"]} straddle(s), realized {_rs(day["realized_pts"], cfg):+,.0f} Rs (gross, before charges)')


if __name__ == '__main__':
    SYMBOL = _ARGV_SYMBOL
    if SYMBOL not in CFG:
        raise ValueError(f'unknown symbol {SYMBOL!r} - use one of {sorted(CFG)}')
    TRADE_WEEKDAYS = _parse_trade_weekdays(sys.argv[2] if len(sys.argv) > 2 else None)
    run_day(SYMBOL, TRADE_WEEKDAYS)
