"""
What it is: a standalone ATM-straddle buy strategy — no short leg anywhere in this file.

Window: 10:15 (ENTRY_TIME) → 15:13 (EXIT_TIME, forced day-end square-off).

Checkpoints: 10:15, 11:15, 12:15, 13:15, 14:15 (CHECK_TIMES). At each one, the combined ATM straddle premium (ATM CE close + ATM PE close) is recorded as the current baseline, overwriting whatever baseline the previous checkpoint set.

Signal (checked every minute, not just at checkpoints): if the live combined ATM premium has risen SPIKE_POINTS = 50 points or more above the latest checkpoint's baseline, buy the ATM straddle (CE+PE) right then.

Exit: hold for HOLD_MINUTES = 15 minutes, then exit at the first minute at/after that deadline (TIME_EXIT), or at day end if 15:13 arrives first (EOD).

Position sizing / concurrency rules:

Only one position open at a time — while a position is open, no new signal is evaluated at all.
Each checkpoint's baseline can fire at most one trade. Once a trade has been taken off a given checkpoint's value, that baseline is "used up" (checkpoint_used = True) and won't trigger again — even after the position closes and the premium is still elevated — until the next checkpoint sets a fresh baseline (which also re-arms the signal).
No other exits or filters — no per-leg stoploss, no profit target, no daily loss limit. Purely: spike-triggered entry, fixed-time (or EOD) exit, one trade per checkpoint window.

Day/weekday handling:

Trades every weekday by default (TRADE_WEEKDAYS); restrict via a second CLI arg using m/t/w/h/f codes, e.g. python backtest_atm_premium_spike_buy.py SENSEX th for Tuesday+Thursday only.
Days with no parquet file (weekends/holidays) are silently skipped.
Underlying handling: defaults to NIFTY; pass an underlying as the first CLI arg (SENSEX) to run just that one, or no args to loop over both NIFTY then SENSEX.

---

Live version of Data/backtests/backtest_atm_premium_spike_buy_v2.py, SENSEX only. Ported rule-for-rule
from that backtest's run_day(): checkpoints refresh the baseline regardless of whether a position is
open (so the baseline is current the moment a position closes, and re-armed since nothing has
traded off the fresh value yet); unlike v1, each checkpoint *pins* the exact strike (CE+PE) tagged
ATM at that moment, and the spike check and the straddle actually bought both track that same pinned
strike's premium for the rest of the hour - even after spot moves on and a different strike becomes
the new ATM - so a strike roll can never masquerade as a premium spike, or vice versa; a still-open
position is force-closed at day end even if its hold deadline hasn't arrived.

Progress (today's checkpoints already applied, the current baseline, whether it's used up, and any
open position's legs/entry prices/deadline) is persisted to STATE_FILE after every change, so a
restart mid-day resumes instead of losing track of an open position or re-arming a used baseline.
Late-start behaviour is automatic, based on where the process's actual start time falls relative to
CHECK_TIMES - no manual mode to pick: started before the first checkpoint (10:15) - wait for it to
fire for real, same as any on-time start, no special-casing needed. Started after the first
checkpoint has already passed - fire immediately: bootstrap a baseline/pinned-strike right now from
the live ATM straddle (the exact premium at that past checkpoint minute is gone, so this is an
approximation, logged clearly as such), then fall back to waiting for the *next* real checkpoint
exactly as normal.

Fully self-contained, unlike day_end_straddle_buy.py / execution_rolling_straddle_variation.py -
deliberately does NOT import execution_rolling_straddle.py, because that module authenticates with
Dhan at import time as a side effect (it's used there as the market-data source), and this script's
market data comes from Zerodha's Kite Connect API instead, never Dhan. So all the AliceBlue REST
plumbing needed to place orders (auth, contract master, order placement, tick rounding) is
reimplemented locally below, and Dhan is never touched, imported, or authenticated by this file at
all. Entries here carry no resting stoploss order at all - this strategy has no per-leg stoploss,
no profit target, and no daily loss limit, by design. Exiting a held leg (TIME_EXIT or EOD) places
a fresh SELL SL (stop-loss LIMIT) order and repeatedly re-prices it via modify (never cancel then a
brand-new order, and never MARKET/SLM - not permitted for this account/strategy) until the position
is actually confirmed closed - see _force_exit_leg. CE and PE, on both entry and exit, run in
parallel threads (_run_legs_in_parallel) reading from one shared order-book/positions poller
(_order_book_cache/_positions_cache) rather than each hammering AliceBlue's API on its own.

Market data - live LTP feed via zerodha_ltp_client.py's shared Redis cache, not REST polling: the
31 Aug 2026 review found this file's checkpoint/spike signal was being evaluated on a REST-poll
cadence (POLL_INTERVAL, then 60s) and, worse, that a signal firing still had to do a fresh REST LTP
fetch before it could even place an order - both add real, avoidable latency to exactly the fast
one-directional spikes this strategy exists to catch (see notes.md's 14:45->14:46:37 walkthrough).
zerodha_ticker_service.py - a separate, standalone process - holds the ONE shared Zerodha ticker
websocket connection every execution script reads from (rather than each script running its own)
and streams every tick straight into Redis; _get_ltp below just reads whatever's there (REST
fallback baked into zerodha_ltp_client.get_ltp if Redis or the ticker service is down), so a price
read anywhere in this file is a fast local lookup, not a network round trip on the hot path.

That said, the signal itself is still evaluated at most ONCE PER MINUTE, on the wall-clock minute
rollover - not on every tick. Reacting to every tick would fire on intra-minute noise the backtest
(which only ever sees each 1-minute bar's close) never simulated, producing trade counts and
patterns with no backtest to validate them against. So the shared feed's job is narrowly about
*latency*, not *frequency*: get exactly the same once-a-minute check the backtest performs, just
fed by an always-current price instead of a REST snapshot that might be a full poll cycle stale,
and place the order immediately off that same cached price with no extra fetch in between.

Requires zerodha_ticker_service.py to be running separately (and a reachable Redis) for the fast
path - if either is down, every price read here transparently falls back to the same REST call
this file made before either existed, just slower; nothing here fails outright.

Logging: goes to sensex_option_buying.log and stdout; if TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are
set in .env, every log record is also pushed to Telegram (see TelegramHandler below) - if either is
missing, Telegram alerts are just skipped (logged once as a warning) and trading proceeds normally.
"""

import csv
import io
import json
import logging
import os
import signal
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

LOG_FILE = os.path.join(os.path.dirname(__file__), 'sensex_option_buying.log')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_TIMEOUT = 10


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
    """Forwards every logged message (INFO and above - so every checkpoint/trade decision, plus
    warnings and uncaught exceptions) to Telegram, alongside the file/console handlers below -
    except records logged with extra={'no_telegram': True} (the once-a-minute status ticks below),
    which still go to the log file and stdout but are too frequent to be worth a Telegram alert."""

    def emit(self, record):
        if getattr(record, 'no_telegram', False):
            return
        try:
            _telegram_send(self.format(record))
        except Exception as exc:
            try:
                print(f'TelegramHandler.emit failed: {exc}', file=sys.stderr)
            except Exception:
                pass


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout), TelegramHandler()],
)
log = logging.getLogger(__name__)

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    log.warning('TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set in .env - Telegram alerts disabled')


def _log_uncaught_exception(exc_type, exc_value, exc_tb):
    log.critical('Uncaught exception', exc_info=(exc_type, exc_value, exc_tb))


sys.excepthook = _log_uncaught_exception


def _log_and_exit_on_signal(signum, frame):
    """sys.excepthook above only fires for uncaught Python exceptions - it never sees the process
    being killed by an OS signal (SSH session dropping -> SIGHUP, systemd/manual stop -> SIGTERM),
    which is exactly why a kill like that leaves zero trace in the log (see
    execution_rolling_straddle.py, which hit this same failure mode first). Trap the catchable
    ones here so at least *why* it stopped is on record - SIGKILL (OOM killer, kill -9) can never
    be caught by any process, no way around that one."""
    log.critical(f'Received signal {signal.Signals(signum).name} ({signum}) - exiting')
    sys.exit(1)


for _sig in (signal.SIGTERM, signal.SIGHUP):
    signal.signal(_sig, _log_and_exit_on_signal)

DRY_RUN = os.getenv('DRY_RUN', 'true').lower() != 'false'  # set DRY_RUN=false to place real orders

SYMBOL = 'SENSEX'
CFG = dict(aliceblue_exchange='BFO', strike_interval=100, lots=5)

STATE_FILE = os.path.join(os.path.dirname(__file__), 'sensex_option_buying_state.json')

ENTRY_TIME = dtime(10, 15)  # strategy start
EXIT_TIME = dtime(15, 13)  # day end / forced square-off
CHECK_TIMES = (dtime(10, 15), dtime(11, 15), dtime(12, 15), dtime(13, 15), dtime(14, 15))
SPIKE_POINTS = 50  # combined ATM premium rise above the latest checkpoint's baseline that triggers a buy
HOLD_MINUTES = 5  # how long a triggered buy is held before being time-exited

WALLCLOCK_TICK_SECONDS = 1  # was POLL_INTERVAL=60, then 20 - no longer a network poll interval at
# all now that prices stream continuously over the websocket (see that section above): this is
# just how often the main loop wakes up to check whether the wall-clock minute has rolled over.
# The signal itself still only evaluates once per new minute (see run_day below) - matching the
# backtest's once-per-bar granularity exactly (see module docstring) - this just makes sure that
# once-a-minute check fires within ~1s of the minute actually turning over, using a price that's
# already current, rather than however stale a REST poll cadence happened to leave it.
PRE_MARKET_POLL_SECONDS = 60  # how often to log the "waiting for market open" wait before ENTRY_TIME
REQUEST_TIMEOUT = 10

FAILURE_ALERT_EVERY = 10  # after the 1st consecutive tick/checkpoint failure alerts, only re-alert
# every Nth consecutive one after that - keeps a prolonged Zerodha/AliceBlue outage from spamming
# Telegram every poll while still landing every single failure in the log file/stdout.


def _log_failure_throttled(message, failure_count, every=FAILURE_ALERT_EVERY):
    """log.exception, but only forwarded to Telegram on the 1st consecutive failure and then every
    `every`th one after that - full detail always goes to the log file/stdout either way.

    Belt-and-braces: called from inside an `except` block that's already handling a real failure,
    so wrapped so that a problem in the logging/alerting path itself can never escape and crash
    the trading loop on top of the original failure."""
    try:
        extra = {} if (failure_count == 1 or failure_count % every == 0) else {'no_telegram': True}
        log.exception(message, extra=extra)
    except Exception as exc:
        print(f'_log_failure_throttled failed: {exc}', file=sys.stderr)


def _label(t):
    return f'{t:%H:%M}'


def _today_str():
    return datetime.now().strftime('%Y-%m-%d')


def atm_strike(spot, strike_interval):
    return round(spot / strike_interval) * strike_interval


# ── Zerodha (REST, Kite Connect) - market data only ─────────────────────────
ZERODHA_API_KEY = os.getenv('ZERODHA_API_KEY')
ZERODHA_TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'zerodha_token.json')
ZERODHA_BASE_URL = 'https://api.kite.trade'
ZERODHA_SPOT_INSTRUMENT = 'BSE:SENSEX'
ZERODHA_OPTIONS_EXCHANGE = 'BFO'
LTP_STALE_SECONDS = 5  # tighter than zerodha_ltp_client's own 10s default - this strategy fires
# off a fast premium spike and prices entries/exits directly off these LTPs, so a stale cached
# price matters more here than it does for every other script sharing that cache; passed
# explicitly as stale_seconds on every zerodha_ltp_client.get_ltp call below rather than changing
# the shared default (matches exec_rs_ps.py's same override).


def _zerodha_headers(access_token):
    return {'Authorization': f'token {ZERODHA_API_KEY}:{access_token}', 'X-Kite-Version': '3'}


def _load_zerodha_token():
    with open(ZERODHA_TOKEN_FILE) as f:
        return json.load(f)['access_token']


def _zerodha_token_is_valid(access_token):
    resp = requests.get(f'{ZERODHA_BASE_URL}/user/profile', headers=_zerodha_headers(access_token), timeout=REQUEST_TIMEOUT)
    return resp.ok


def _valid_zerodha_token():
    """Kite Connect login needs a manual browser step (see zerodha_generate_access_token.py) -
    unlike Dhan's token, this can't be refreshed automatically - fail loudly instead."""
    if not os.path.exists(ZERODHA_TOKEN_FILE):
        raise RuntimeError('No Zerodha access token found - run zerodha_generate_access_token.py to log in')
    access_token = _load_zerodha_token()
    if not _zerodha_token_is_valid(access_token):
        raise RuntimeError('Zerodha access token expired - run zerodha_generate_access_token.py to log in again')
    return access_token


ZERODHA_ACCESS_TOKEN = _valid_zerodha_token()

_zerodha_options_cache = {'date': None, 'options': None}  # avoids re-downloading the full BFO
# instrument dump (thousands of rows, all strikes/expiries/underlyings) on every tick


def _load_zerodha_current_week_options():
    """This week's SENSEX CE/PE instruments from Kite's BFO instrument dump - [{'tradingsymbol',
    'strike', 'instrument_type', 'expiry', ...}, ...], cached per calendar day."""
    today = _today_str()
    if _zerodha_options_cache['date'] == today:
        return _zerodha_options_cache['options']

    resp = requests.get(f'{ZERODHA_BASE_URL}/instruments/{ZERODHA_OPTIONS_EXCHANGE}',
                         headers=_zerodha_headers(ZERODHA_ACCESS_TOKEN), timeout=30)
    resp.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(resp.text)))
    today_date = datetime.now().date()
    opts = [
        row for row in rows
        if row['name'] == SYMBOL and row['instrument_type'] in ('CE', 'PE') and row['expiry']
        and datetime.strptime(row['expiry'], '%Y-%m-%d').date() >= today_date
    ]
    if not opts:
        raise RuntimeError(f'No {SYMBOL} option instruments found on Zerodha {ZERODHA_OPTIONS_EXCHANGE}')
    current_week_expiry = min(row['expiry'] for row in opts)
    opts = [row for row in opts if row['expiry'] == current_week_expiry]

    _zerodha_options_cache['date'] = today
    _zerodha_options_cache['options'] = opts
    return opts


def _zerodha_option_row(zerodha_options, strike, option_type):
    for row in zerodha_options:
        if int(float(row['strike'])) == strike and row['instrument_type'] == option_type:
            return row
    raise KeyError(f'No Zerodha {SYMBOL} instrument found for strike={strike} type={option_type}')


def _zerodha_quote_ltp(instrument_keys):
    """instrument_keys like ['BSE:SENSEX', 'BFO:SENSEX25813950CE']. Returns key -> last_price.
    REST fallback only now - see zerodha_ltp_client.py for the actual hot-path price feed; this is
    passed to it as the rest_fetch callable for _get_ltp below, and used to look up the SENSEX
    index's own instrument_token once per day."""
    resp = requests.get(f'{ZERODHA_BASE_URL}/quote/ltp', headers=_zerodha_headers(ZERODHA_ACCESS_TOKEN),
                         params=[('i', k) for k in instrument_keys], timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()['data']
    return {k: float(v['last_price']) for k, v in data.items()}


# ── Live LTP feed: shared Redis cache, kept fresh by zerodha_ticker_service.py ───────────────────
# That's the one process holding the actual Zerodha ticker websocket connection - every script
# (this one included) just reads whatever it last wrote to Redis, falling back to a direct REST
# fetch if Redis is unreachable or a token hasn't ticked recently enough (see zerodha_ltp_client.py
# for the full contract). This file no longer opens its own websocket connection at all.
def _get_ltp(token, rest_key):
    """Live LTP for `token` via the shared Redis cache (REST fallback baked in - see
    zerodha_ltp_client.get_ltp). Registers `token` with the ticker service first (harmless/no-op
    if already registered, or if Redis is down) so it starts streaming if it isn't already."""
    zerodha_ltp_client.register_subscription(token)
    return zerodha_ltp_client.get_ltp(
        token, rest_fetch=lambda: _zerodha_quote_ltp([rest_key])[rest_key], log=log, stale_seconds=LTP_STALE_SECONDS,
    )


_zerodha_spot_token_cache = {'date': None, 'token': None}


def _zerodha_spot_token():
    """SENSEX index's own instrument_token (needed to subscribe to it on the websocket) - looked
    up once per day from Zerodha's BSE instrument dump, same caching pattern as
    _load_zerodha_current_week_options above."""
    today = _today_str()
    if _zerodha_spot_token_cache['date'] == today:
        return _zerodha_spot_token_cache['token']

    resp = requests.get(f'{ZERODHA_BASE_URL}/instruments/BSE', headers=_zerodha_headers(ZERODHA_ACCESS_TOKEN), timeout=30)
    resp.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(resp.text)))
    for row in rows:
        if row.get('segment') == 'INDICES' and row.get('tradingsymbol') == 'SENSEX':
            token = int(row['instrument_token'])
            _zerodha_spot_token_cache['date'] = today
            _zerodha_spot_token_cache['token'] = token
            return token
    raise RuntimeError('SENSEX index instrument_token not found on Zerodha BSE instrument dump')


def get_spot_ltp():
    return _get_ltp(_zerodha_spot_token(), ZERODHA_SPOT_INSTRUMENT)


def get_option_ltp(zerodha_options, strike, option_type):
    row = _zerodha_option_row(zerodha_options, strike, option_type)
    token = int(row['instrument_token'])
    key = f'{ZERODHA_OPTIONS_EXCHANGE}:{row["tradingsymbol"]}'
    return _get_ltp(token, key)


def _current_atm(zerodha_options):
    spot = get_spot_ltp()
    strike = atm_strike(spot, CFG['strike_interval'])
    ce_ltp = get_option_ltp(zerodha_options, strike, 'CE')
    pe_ltp = get_option_ltp(zerodha_options, strike, 'PE')
    return strike, ce_ltp, pe_ltp


# ── AliceBlue (REST, v3 open-api) - order placement only ────────────────────
ALICEBLUE_TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'aliceblue_token.json')
ALICEBLUE_BASE_URL = 'https://a3.aliceblueonline.com/open-api/od/v1'
ALICEBLUE_CONTRACT_MASTER_URL = 'https://v2api.aliceblueonline.com/restpy/static/contract_master/V2/{exchange}'
LIMIT_OFFSET_PCT = 0.03  # was 0.01 - a 1% buffer on a strategy that specifically enters during a
# fast premium spike left almost no room for the LTP to move between being fetched and the order
# reaching the exchange, which is exactly when it moves fastest; 3% brings this closer to (if
# still tighter than) exec_rsv_cont.py's 5% for the same kind of order (see notes.md)
FILL_POLL_TIMEOUT = 10
FILL_POLL_INTERVAL = 1
TERMINAL_ORDER_STATUSES = {'complete', 'rejected', 'cancelled'}
# Status codes AliceBlue returns instead of 'Ok' to mean "the query is fine, there's just nothing
# to return" - e.g. /positions returns EC920 rather than an empty list when there are no open
# positions. Treat these as an empty result rather than an error.
_ALICEBLUE_EMPTY_RESULT_STATUSES = {'EC920'}

# Exit (sell_leg only - entries via buy_leg are unchanged): once we've decided to close, priority
# is getting OUT, not a clean fill price - see notes.md. Squares off via a forced SL (stop-loss
# LIMIT) SELL order, repeatedly re-priced/modified until filled - see _force_exit_leg and its
# FORCE_EXIT_* constants below. No MARKET/SLM fallback: not permitted for this account/strategy.


def _aliceblue_headers(session_id):
    return {'Authorization': f'Bearer {session_id}', 'Content-Type': 'application/json'}


def _load_aliceblue_session():
    with open(ALICEBLUE_TOKEN_FILE) as f:
        return json.load(f)['userSession']


def _aliceblue_session_is_valid(session_id):
    resp = requests.get(f'{ALICEBLUE_BASE_URL}/limits/', headers=_aliceblue_headers(session_id), timeout=REQUEST_TIMEOUT)
    return resp.ok


def _valid_aliceblue_session():
    """AliceBlue's v3 login needs a manual browser step (see aliceblue_token_generation.py), so
    unlike Dhan's token this can't be refreshed automatically - fail loudly instead."""
    if not os.path.exists(ALICEBLUE_TOKEN_FILE):
        raise RuntimeError('No AliceBlue session found - run aliceblue_token_generation.py to log in')
    session_id = _load_aliceblue_session()
    if not _aliceblue_session_is_valid(session_id):
        raise RuntimeError('AliceBlue session expired - run aliceblue_token_generation.py to log in again')
    return session_id


ALICEBLUE_SESSION_ID = _valid_aliceblue_session()


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
    resp.raise_for_status()
    return _aliceblue_result(path, resp.json())


class Instrument(NamedTuple):
    token: int
    symbol: str
    name: str
    lot_size: int
    tick_size: float
    exchange: str


def _load_aliceblue_contracts():
    """This week's SENSEX CE/PE contracts from AliceBlue's contract master - needed to place
    orders (instrument token/lot size/tick size). Separate from the Zerodha instruments above,
    which are used for pricing only.

    The contract master can keep listing an expiry after it's actually expired - expiry_date is
    UTC midnight and trading hours fall entirely within the same UTC calendar day as IST, so
    today's date is excluded only once it's genuinely gone."""
    resp = requests.get(ALICEBLUE_CONTRACT_MASTER_URL.format(exchange=CFG['aliceblue_exchange']), timeout=30)
    resp.raise_for_status()
    exchange = CFG['aliceblue_exchange']
    today = datetime.now().date()
    opts = [
        c for c in resp.json()[exchange]
        if c['symbol'] == SYMBOL and c['option_type'] in ('CE', 'PE')
        and datetime.fromtimestamp(c['expiry_date'] / 1000, tz=timezone.utc).date() >= today
    ]
    current_week_expiry = min(c['expiry_date'] for c in opts)
    return [c for c in opts if c['expiry_date'] == current_week_expiry]


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


class _PollingCache:
    """One shared poller/cache for a single broker read endpoint, for the whole process - refreshed
    every poll_interval by a single background thread, so CE and PE - entering or exiting in
    parallel threads (_run_legs_in_parallel) - read one shared snapshot instead of each hammering
    the same endpoint with their own poll loop. If a reader finds the cache missing or older than
    2x poll_interval (poller hasn't started yet, or fell behind), it fetches directly itself and
    publishes the result for everyone else. A failed fetch, or one that fails `validate`, is
    treated as invalid: whoever hit it (the poller thread or a reader's own direct fetch) waits
    error_backoff before the next attempt rather than retrying immediately."""

    def __init__(self, name, fetch_fn, poll_interval, error_backoff, validate=lambda v: isinstance(v, list)):
        self._name = name
        self._fetch_fn = fetch_fn
        self._poll_interval = poll_interval
        self._error_backoff = error_backoff
        self._validate = validate
        self._max_age = poll_interval * 2
        self._lock = threading.Lock()
        self._value = None
        self._fetched_at = None
        self._poller_started = False

    def _fetch_once(self):
        try:
            value = self._fetch_fn()
        except Exception as exc:
            log.warning(f'{self._name} fetch failed ({exc}) - retrying in {self._error_backoff}s', extra={'no_telegram': True})
            return None
        if not self._validate(value):
            log.warning(f'{self._name} fetch returned {type(value).__name__}, unexpected shape - treating as invalid, retrying in {self._error_backoff}s', extra={'no_telegram': True})
            return None
        return value

    def _publish(self, value):
        with self._lock:
            self._value = value
            self._fetched_at = time_module.monotonic()

    def _poll_loop(self):
        while True:
            value = self._fetch_once()
            if value is not None:
                self._publish(value)
                time_module.sleep(self._poll_interval)
            else:
                time_module.sleep(self._error_backoff)

    def start(self):
        """Idempotent - safe to call every time run_day starts."""
        with self._lock:
            if self._poller_started:
                return
            self._poller_started = True
        threading.Thread(target=self._poll_loop, name=f'{self._name}-poller', daemon=True).start()

    def get(self):
        """Return a reasonably fresh value. Uses the shared cache if it's fresh enough; otherwise
        fetches directly (backing off error_backoff once on a failed/invalid first attempt) and
        publishes the result. May still return None if both attempts failed - callers already poll
        in a loop with their own sleep, so they just try again next cycle."""
        with self._lock:
            value, fetched_at = self._value, self._fetched_at
        if value is not None and fetched_at is not None and time_module.monotonic() - fetched_at < self._max_age:
            return value
        value = self._fetch_once()
        if value is None:
            time_module.sleep(self._error_backoff)
            value = self._fetch_once()
        if value is not None:
            self._publish(value)
        return value


ORDER_BOOK_POLL_INTERVAL = 1  # seconds between shared order-book poller refreshes
ORDER_BOOK_ERROR_BACKOFF = 2  # seconds to wait before retrying after a failed/invalid fetch
_order_book_cache = _PollingCache('order book', _order_book, ORDER_BOOK_POLL_INTERVAL, ORDER_BOOK_ERROR_BACKOFF)

POSITIONS_POLL_INTERVAL = 1  # seconds between shared positions poller refreshes
POSITIONS_ERROR_BACKOFF = 2  # seconds to wait before retrying after a failed/invalid fetch
_positions_cache = _PollingCache('positions', lambda: _aliceblue_get('/positions'), POSITIONS_POLL_INTERVAL, POSITIONS_ERROR_BACKOFF)


def _leg_still_open(instrument):
    """True if the broker's positions still show a nonzero net quantity for this instrument -
    False once it's flat, whether that's because our own exit order filled or the leg was closed
    some other way entirely (manually, by a different process, etc). Used by _force_exit_leg to
    stop retrying once the position is already gone, rather than keep firing exit orders at
    nothing. Reads the shared _positions_cache - when CE and PE are both being force-closed at
    once, they read one shared positions fetch instead of each hitting /positions on their own.
    Fails safe: a missing/failed fetch is treated as 'still open' so a transient API hiccup can't
    make the forced exit give up early."""
    positions = _positions_cache.get()
    if positions is None:
        log.warning(f'position check for {instrument.name} failed - assuming still open', extra={'no_telegram': True})
        return True
    for p in positions:
        if str(p.get('instrumentId')) == str(instrument.token):
            return int(p.get('netQuantity', 0)) != 0
    return False


def _place_order(transaction_type, instrument, quantity, order_type, price='0', trigger_price=None, order_tag=None):
    payload = [{
        'exchange': instrument.exchange,
        'instrumentId': str(instrument.token),
        'transactionType': transaction_type,
        'quantity': quantity,
        'product': 'INTRADAY',
        'orderComplexity': 'REGULAR',
        'orderType': order_type,
        'validity': 'DAY',
        'price': price,
        'slTriggerPrice': trigger_price if trigger_price is not None else '',
        'orderTag': order_tag or '',
    }]
    result = _aliceblue_post('/orders/placeorder', payload)
    return result[0] if isinstance(result, list) and len(result) == 1 else result


def _cancel_order(broker_order_id):
    return _aliceblue_post('/orders/cancel', {'brokerOrderId': broker_order_id})


def _modify_order(broker_order_id, quantity, price, trigger_price=None, order_type='SL', validity='DAY'):
    """Modify a resting order in place. Payload matches AliceBlue's documented POST /orders/modify
    exactly - see https://v2api.aliceblueonline.com/orders%20Management/ ('brokerOrderId' required;
    quantity/orderType/price/slTriggerPrice/validity optional - no exchange/instrumentId/
    transactionType/product needed, unlike /orders/placeorder). Note this supersedes the wider
    payload this function used to send against /orders/modifyorder - the wrong endpoint entirely -
    which is what mcx_option_buying.py and mcx_short_straddle_premium_stoploss.py elsewhere in this
    folder document AliceBlue 400ing on; _force_exit_leg below still falls back to placing a fresh
    SL order if a modify call ever fails, so a broker-side rejection can never leave a live exit
    stuck."""
    payload = {
        'brokerOrderId': broker_order_id,
        'quantity': quantity,
        'orderType': order_type,
        'price': str(price),
        'slTriggerPrice': str(trigger_price) if trigger_price is not None else '',
        'validity': validity,
    }
    return _aliceblue_post('/orders/modify', payload)


def _wait_for_fill_price(broker_order_id):
    """Reads the shared _order_book_cache rather than polling /orders/book on its own - when CE and
    PE entries run in parallel threads (_run_legs_in_parallel), both threads' calls to this
    function share the one poller/cache instead of doubling the API traffic."""
    deadline = time_module.time() + FILL_POLL_TIMEOUT
    while time_module.time() < deadline:
        book = _order_book_cache.get()
        if book is not None:
            for o in book:
                if o.get('brokerOrderId') != broker_order_id:
                    continue
                status = str(o.get('orderStatus', '')).lower()
                if status == 'rejected':
                    raise RuntimeError(f'order {broker_order_id} rejected: {o.get("rejectionReason")}')
                if status == 'complete':
                    return float(o.get('averageTradedPrice') or 0)
        time_module.sleep(FILL_POLL_INTERVAL)
    raise TimeoutError(f'order {broker_order_id} not filled within {FILL_POLL_TIMEOUT}s')


FORCE_EXIT_POLL_INTERVAL = 2  # seconds between exit-order re-price attempts while forcing a close
FORCE_EXIT_STUCK_ALERT_EVERY = 30  # attempts between "still not squared off" escalation alerts (~1min at 2s cadence)


def _force_exit_leg(instrument, quantity, get_fresh_ltp, order_tag, log_prefix):
    """Force a bought leg closed using only SL (stop-loss LIMIT) SELL orders - MARKET/SLM orders are
    never used, they are not permitted for this account/strategy. Places a SELL SL order and
    repeatedly MODIFIES it (never cancel+replace) so trigger and limit both bracket the current
    LTP - trigger = ltp+5% (already breached from below, since a SELL SL fires once price falls TO
    OR BELOW the trigger - setting it above current price means that's already true), limit =
    ltp-5% (marketable), so it should fill immediately. If that order can't be found/modified
    (rejected, already filled/cancelled by something else) a brand new SL order is placed instead
    and the same retry loop continues working that one. There is NO attempt cap and NO MARKET
    fallback: this loops forever, re-pricing every FORCE_EXIT_POLL_INTERVAL, until the position is
    actually confirmed squared off - escalating to a CRITICAL alert every
    FORCE_EXIT_STUCK_ALERT_EVERY attempts so a stuck close doesn't go unnoticed, but it keeps
    retrying regardless. Each attempt also checks the broker's actual position (_leg_still_open) -
    if it's already flat, the leg was closed some other way (manually, or by anything other than
    the order this loop is tracking), and this stops immediately rather than continuing to fire
    exit orders at a position that no longer exists. `get_fresh_ltp` is a zero-arg callable,
    re-fetched every attempt, never reused stale. Mirrors exec_rs_ps.py's _force_exit_leg -
    duplicated rather than imported, matching this file's existing fully-self-contained design."""
    working_order_id = None
    attempt = 0
    while True:
        attempt += 1
        ltp = get_fresh_ltp()
        if ltp is None:
            log.warning(f'{log_prefix}: no LTP available on attempt {attempt} - retrying in {FORCE_EXIT_POLL_INTERVAL}s', extra={'no_telegram': True})
            time_module.sleep(FORCE_EXIT_POLL_INTERVAL)
            continue

        trigger_price = round(ltp * (1 + LIMIT_OFFSET_PCT), 1)
        limit_price = _round_to_tick(ltp * (1 - LIMIT_OFFSET_PCT), instrument.tick_size)

        if working_order_id is not None:
            log.info(
                f'{log_prefix}: attempt {attempt} - modifying SL {working_order_id} to '
                f'trigger={trigger_price} limit={limit_price} (ltp {ltp}) to force an immediate fill',
                extra={'no_telegram': True},
            )
            try:
                _modify_order(working_order_id, quantity, limit_price, trigger_price)
            except Exception as exc:
                log.warning(f'{log_prefix}: modify of {working_order_id} failed on attempt {attempt} ({exc}) - will place a fresh SL order instead', extra={'no_telegram': True})
                working_order_id = None

        if working_order_id is None:
            log.info(f'{log_prefix}: attempt {attempt} - placing a new SL order, trigger={trigger_price} limit={limit_price} (ltp {ltp})', extra={'no_telegram': True})
            try:
                new_order = _place_order('SELL', instrument, quantity, 'SL', price=str(limit_price), trigger_price=trigger_price, order_tag=order_tag)
                working_order_id = new_order.get('brokerOrderId')
                if not working_order_id:
                    log.warning(f'{log_prefix}: new SL order rejected on attempt {attempt}: {new_order}', extra={'no_telegram': True})
            except Exception as exc:
                log.warning(f'{log_prefix}: placing a new SL order failed on attempt {attempt} ({exc})', extra={'no_telegram': True})

        time_module.sleep(FORCE_EXIT_POLL_INTERVAL)

        book = _order_book_cache.get()
        if book is not None and working_order_id is not None:
            for o in book:
                if o.get('brokerOrderId') != working_order_id:
                    continue
                status = str(o.get('orderStatus', '')).lower()
                if status == 'complete':
                    fill_price = float(o.get('averageTradedPrice') or 0)
                    log.info(f'{log_prefix}: order {working_order_id} filled @ {fill_price} on attempt {attempt}')
                    return fill_price
                if status in ('rejected', 'cancelled'):
                    log.warning(f'{log_prefix}: order {working_order_id} is {status} - will place a fresh SL order next attempt', extra={'no_telegram': True})
                    working_order_id = None
                break
        elif book is None:
            log.warning(f'{log_prefix}: order book unavailable on attempt {attempt}', extra={'no_telegram': True})

        # Our own order isn't showing 'complete' yet - but the position might already be flat
        # anyway, closed outside this process entirely (manually, by a different order/process).
        # Nothing left to square off in that case - stop retrying rather than keep firing exit
        # orders at a position that no longer exists.
        if not _leg_still_open(instrument):
            log.info(f'{log_prefix}: broker position is already flat on attempt {attempt} - closed outside this process, stopping the forced exit')
            if working_order_id is not None:
                try:
                    _cancel_order(working_order_id)
                except Exception as exc:
                    log.warning(f'{log_prefix}: cancel of now-stale order {working_order_id} failed ({exc})')
            log.warning(
                f'{log_prefix}: position was already flat at the broker (closed outside this process, e.g. manually) - '
                f'stopping the forced exit; pnl is a best-effort estimate off the current LTP, not an actual fill price',
            )
            return ltp

        if attempt % FORCE_EXIT_STUCK_ALERT_EVERY == 0:
            log.critical(
                f'{log_prefix}: still NOT squared off after {attempt} attempts '
                f'(~{attempt * FORCE_EXIT_POLL_INTERVAL}s) - no MARKET fallback permitted, continuing to retry with SL orders',
            )


def _instrument_to_dict(instrument):
    return instrument._asdict()


def _instrument_from_dict(d):
    return Instrument(**d)


# ── Orders (no resting stoploss - this strategy has none, by design) ────────
def buy_leg(instrument, quantity, ltp):
    """Plain LIMIT buy entry, no resting stoploss order. Returns the fill price (or `ltp` in
    DRY_RUN, as a stand-in for pnl bookkeeping)."""
    entry_price = _round_to_tick(ltp * (1 + LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}BUY {quantity} x {instrument.name} LIMIT @ {entry_price} (ltp {ltp})'
    log.info(tag)
    if DRY_RUN:
        return entry_price

    entry = _place_order(
        'BUY', instrument, quantity, 'LIMIT', price=str(entry_price), order_tag='premium_spike_buy_entry',
    )
    order_no = entry.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} entry order rejected: {entry}')
    return _wait_for_fill_price(order_no)


def sell_leg(instrument, quantity, get_fresh_ltp):
    """Square off a bought leg: cancel any resting order on it (defensive - buy_leg above never
    leaves one) then force a SELL SL exit via _force_exit_leg - retried/re-priced (via modify, not
    cancel+replace) until actually filled, MARKET/SLM never used - once we've decided to close,
    speed matters more than price (see notes.md), but never at the cost of an order type not
    permitted for this account/strategy. `get_fresh_ltp` is a zero-arg callable, re-fetched every
    attempt so a retry never reprices off a stale quote."""
    initial_ltp = get_fresh_ltp()
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}SELL (square off) {quantity} x {instrument.name} (ltp {initial_ltp})'
    log.info(tag)
    if DRY_RUN:
        return _round_to_tick(initial_ltp * (1 - LIMIT_OFFSET_PCT), instrument.tick_size)

    for o in _order_book_cache.get() or []:
        if str(o.get('instrumentId')) == str(instrument.token) and str(o.get('orderStatus', '')).lower() not in TERMINAL_ORDER_STATUSES:
            _cancel_order(o['brokerOrderId'])

    log_prefix = f'{instrument.name} close'
    return _force_exit_leg(instrument, quantity, get_fresh_ltp, 'premium_spike_buy_exit', log_prefix)


# ── State persistence ─────────────────────────────────────────────────────
def _fresh_state():
    return {
        'date': _today_str(),
        'checkpoint': None,  # {'strike', 'premium'} - the exact strike pinned at the last checkpoint
        'checkpoint_used': False,
        'checkpoints_done': [],
        'position': None,  # {'entry_time', 'deadline', 'checkpoint_premium', 'entry_premium', 'legs': {'CE': {...}, 'PE': {...}}}
    }


def _load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)
        if state.get('date') == _today_str():
            if 'checkpoint' not in state:
                # pre-pinned-strike (v1) schema left over from an old checkpoint_premium-only state
                # file - the strike that set that baseline was never recorded, so it can't be
                # migrated; drop it and let the bootstrap logic below re-pin a fresh (approximate)
                # baseline from the current checkpoint window.
                log.warning(f'Persisted state uses the old schema (no pinned strike) - discarding '
                            f'stale baseline {state.pop("checkpoint_premium", None)}, will re-pin fresh')
                state['checkpoint'] = None
                state['checkpoint_used'] = False
            log.info(f'Resuming from persisted state: {state}')
            return state
        log.info(f'Persisted state is for {state.get("date")}, not today - starting fresh')
    return _fresh_state()


def _save_state(state):
    tmp = STATE_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)


# ── Strategy ──────────────────────────────────────────────────────────────
def _pinned_premium(zerodha_options, checkpoint):
    """Live combined premium of the checkpoint's pinned strike - the SAME strike pinned at the
    checkpoint, tracked directly by strike/type rather than via the ATM tag, which may have moved
    off this strike by now."""
    ce_ltp = get_option_ltp(zerodha_options, checkpoint['strike'], 'CE')
    pe_ltp = get_option_ltp(zerodha_options, checkpoint['strike'], 'PE')
    return ce_ltp + pe_ltp, ce_ltp, pe_ltp


def _run_legs_in_parallel(tasks):
    """Run one no-arg callable per leg ('CE'/'PE') concurrently rather than one after another -
    every call here is I/O (a REST order placement plus up to FILL_POLL_TIMEOUT of polling for a
    fill), so buying/selling CE then PE in sequence was pure added latency on every entry/exit,
    worst on exactly the fast spikes this strategy targets (see notes.md's 14:46:37 entry
    walkthrough). Every leg is always attempted even if a sibling fails; if any did, the first
    exception is re-raised afterwards so the caller's existing retry/alert path still fires."""
    if not tasks:
        return {}
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = {opt: pool.submit(fn) for opt, fn in tasks.items()}
        results, errors = {}, {}
        for opt, fut in futures.items():
            try:
                results[opt] = fut.result()
            except Exception as exc:
                errors[opt] = exc
    if errors:
        raise next(iter(errors.values()))
    return results


def _enter_position(state, checkpoint):
    """Buy the checkpoint's pinned strike's straddle - the same strike/leg symbols pinned at the
    checkpoint that set `checkpoint['premium']`, not whatever's ATM right now."""
    contracts = _load_aliceblue_contracts()
    contracts_by_strike_type = {(int(float(c['strike_price'])), c['option_type']): c for c in contracts}
    zerodha_options = _load_zerodha_current_week_options()
    strike = checkpoint['strike']
    entry_premium, ce_ltp, pe_ltp = _pinned_premium(zerodha_options, checkpoint)
    log.info(f'Spike buy: {SYMBOL} pinned_strike={strike} combined_premium={entry_premium:.1f} '
             f'(+{entry_premium - checkpoint["premium"]:.1f} vs checkpoint {checkpoint["premium"]:.1f})')

    instruments = {}
    for opt, ltp in (('CE', ce_ltp), ('PE', pe_ltp)):
        contract = contracts_by_strike_type[(strike, opt)]
        instruments[opt] = (_to_instrument(contract), ltp)

    entry_prices = _run_legs_in_parallel({
        opt: (lambda instrument=instrument, ltp=ltp: buy_leg(instrument, instrument.lot_size * CFG['lots'], ltp))
        for opt, (instrument, ltp) in instruments.items()
    })
    legs = {}
    for opt, (instrument, _ltp) in instruments.items():
        legs[opt] = {
            'instrument': _instrument_to_dict(instrument), 'quantity': instrument.lot_size * CFG['lots'],
            'strike': strike, 'entry': entry_prices[opt],
        }

    now = datetime.now()
    state['position'] = {
        'entry_time': now.isoformat(),
        'deadline': (now + timedelta(minutes=HOLD_MINUTES)).isoformat(),
        'checkpoint_premium': checkpoint['premium'],
        'entry_premium': entry_premium,
        'legs': legs,
    }
    state['checkpoint_used'] = True
    _save_state(state)


def _close_position(state, reason):
    """Square off the held position. Each leg's strike (recorded at entry) is looked up directly
    by strike/type rather than via the ATM tag, since the strike may have drifted off ATM by the
    time this exit fires."""
    position = state['position']
    if position is None:
        return
    zerodha_options = _load_zerodha_current_week_options()

    exit_prices = _run_legs_in_parallel({
        opt: (lambda leg=leg, opt=opt: sell_leg(
            _instrument_from_dict(leg['instrument']), leg['quantity'],
            lambda leg=leg, opt=opt: get_option_ltp(zerodha_options, leg['strike'], opt),
        ))
        for opt, leg in position['legs'].items()
    })
    pnl = sum(exit_prices[opt] - leg['entry'] for opt, leg in position['legs'].items())  # long: profit when exit > entry

    log.info(f'Closed {SYMBOL} spike-buy straddle ({reason}) - pnl ~{pnl:.1f} pts')
    state['position'] = None
    _save_state(state)


def run_day():
    log.info(f'Starting {SYMBOL} option buying strategy (DRY_RUN={DRY_RUN})')
    _order_book_cache.start()  # shared poller backing every _wait_for_fill_price/_force_exit_leg
    _positions_cache.start()   # order-book/positions read for the rest of the day - see _PollingCache
    state = _load_state()

    now_t = datetime.now().time()
    if now_t >= EXIT_TIME and state['position'] is None and not [c for c in CHECK_TIMES if _label(c) not in state['checkpoints_done']]:
        log.info(f'{EXIT_TIME} already passed for today with nothing pending - nothing to do')
        return


    # Late-start handling is automatic, no mode to pick: started before the first checkpoint (i.e.
    # now_t < ENTRY_TIME, so this whole block is skipped) - just wait for it to fire for real, same
    # as any on-time start. Started after the first checkpoint has already passed - fire
    # immediately: bootstrap a baseline/pinned-strike right now from the live ATM straddle (the
    # actual premium at that past checkpoint minute is gone, so this is an approximation), then
    # fall back into the normal loop below, which waits for the *next* real checkpoint as usual.
    if now_t >= ENTRY_TIME and state['checkpoint'] is None and state['position'] is None:
        recent = [c for c in CHECK_TIMES if c <= now_t]
        if recent:
            zerodha_options = _load_zerodha_current_week_options()
            strike, ce_ltp, pe_ltp = _current_atm(zerodha_options)
            state['checkpoint'] = {'strike': strike, 'premium': ce_ltp + pe_ltp}
            state['checkpoint_used'] = False
            state['checkpoints_done'].append(_label(recent[-1]))
            log.info(f'Started after {recent[-1]} with no baseline recorded - bootstrapping pinned '
                     f'strike={strike} baseline={state["checkpoint"]["premium"]:.1f} from the current '
                     f'premium (approximate)')
            _save_state(state)

    checkpoint_failure_count = 0
    tick_failure_count = 0
    last_evaluated_minute = None  # (hour, minute) of the last minute this loop actually acted on -
    # guards the per-minute body below so it runs exactly once per wall-clock minute, matching the
    # backtest's once-per-bar cadence (see module docstring), even though this loop itself now
    # wakes up every WALLCLOCK_TICK_SECONDS just to detect the rollover promptly.

    while True:
        now = datetime.now()
        t = now.time()

        if t < ENTRY_TIME:
            wait_s = (datetime.combine(now.date(), ENTRY_TIME) - now).total_seconds()
            log.info(f'Waiting for market open ({_label(ENTRY_TIME)}) - {wait_s / 60:.1f} min left',
                     extra={'no_telegram': True})
            time_module.sleep(min(PRE_MARKET_POLL_SECONDS, wait_s))
            continue

        if t >= EXIT_TIME:
            if state['position'] is not None:
                _close_position(state, 'EOD')
            break

        minute_key = (now.hour, now.minute)
        if minute_key == last_evaluated_minute:
            time_module.sleep(WALLCLOCK_TICK_SECONDS)
            continue
        last_evaluated_minute = minute_key

        label = _label(dtime(t.hour, t.minute))
        if dtime(t.hour, t.minute) in CHECK_TIMES and label not in state['checkpoints_done']:
            try:
                zerodha_options = _load_zerodha_current_week_options()
                strike, ce_ltp, pe_ltp = _current_atm(zerodha_options)
                state['checkpoint'] = {'strike': strike, 'premium': ce_ltp + pe_ltp}
                state['checkpoint_used'] = False
                log.info(f'{label} checkpoint: {SYMBOL} pinned_strike={strike} baseline={state["checkpoint"]["premium"]:.1f}')
            except Exception:
                checkpoint_failure_count += 1
                _log_failure_throttled(
                    f'{label} checkpoint: failed to record baseline - will retry next tick', checkpoint_failure_count,
                )
            else:
                checkpoint_failure_count = 0
                state['checkpoints_done'].append(label)
                _save_state(state)

        try:
            if state['position'] is not None:
                deadline = datetime.fromisoformat(state['position']['deadline'])
                remaining = (deadline - now).total_seconds() / 60
                log.info(f'Position open since {state["position"]["entry_time"]} - '
                         f'TIME_EXIT in {remaining:.1f} min (or EOD {_label(EXIT_TIME)})',
                         extra={'no_telegram': True})
                if now >= deadline:
                    _close_position(state, 'TIME_EXIT')
            elif state['checkpoint'] is not None and not state['checkpoint_used']:
                zerodha_options = _load_zerodha_current_week_options()
                current_premium, _, _ = _pinned_premium(zerodha_options, state['checkpoint'])
                gap = current_premium - state['checkpoint']['premium']
                log.info(f'{SYMBOL} pinned_strike={state["checkpoint"]["strike"]} premium={current_premium:.1f} '
                         f'baseline={state["checkpoint"]["premium"]:.1f} (+{gap:.1f}, '
                         f'need +{SPIKE_POINTS} to trigger)', extra={'no_telegram': True})
                if gap >= SPIKE_POINTS:
                    _enter_position(state, state['checkpoint'])
            elif state['checkpoint'] is not None and state['checkpoint_used']:
                log.info(f'{SYMBOL} pinned strike={state["checkpoint"]["strike"]} baseline='
                         f'{state["checkpoint"]["premium"]:.1f} already used - waiting for next '
                         f'checkpoint ({_label(EXIT_TIME)} EOD if none left today)',
                         extra={'no_telegram': True})
            else:
                log.info(f'{SYMBOL} no checkpoint baseline recorded yet', extra={'no_telegram': True})
            tick_failure_count = 0
        except Exception:
            tick_failure_count += 1
            _log_failure_throttled('tick failed - will retry next interval', tick_failure_count)

        time_module.sleep(WALLCLOCK_TICK_SECONDS)


if __name__ == '__main__':
    run_day()
