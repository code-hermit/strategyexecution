"""
Live execution of Data/backtests/backtest_rolling_straddle_variation_premium_stoploss.py: short a
first-OTM strangle (FIRST_OTM_STRIKES away from ATM; 0 = ATM itself) at ENTRY_TIME. The backtest's
own risk control is purely the software COMBINED stoploss below (no per-leg stop at all) - but live,
that check only runs if this process is actually alive and polling. So on top of the backtest's
rules, each leg also gets a resting broker-side BACKSTOP stoploss, LEG_STOPLOSS_POINTS (200) away
from its own entry price - a dead-man's-switch for the case the combined check can't fire (process
crashed, network down, a fast move blows through 50 points before the next poll even lands). 200
points is well past where the 50-point combined stoploss should already have closed things, so this
should essentially never fire while the process is healthy; it exists purely for the worst case.
Every CHECKPOINT_INTERVAL:

  - if the current ATM straddle premium (CE+PE at the true ATM strike, not the first-OTM legs
    actually traded) is HIGHER than it was at the previous checkpoint: take no new trade this
    hour; if either leg is still open, close it outright.
  - else if the combined stoploss fired earlier this hour (see below): stay flat for the rest of
    the hour - consumed here, not re-entered immediately.
  - else if either open leg has drifted off today's first-OTM strike (spot moved): roll the whole
    strangle - close whatever's open, re-enter fresh at the new first-OTM strikes.
  - else: re-enter any leg that isn't currently open (closed by the combined stoploss) at the same
    strike; leave already-open legs alone.

Every POLL_INTERVAL_SECONDS (not just once an hour):
  - COMBINED stoploss: sum the live premium move against entry across whichever legs are
    currently open. Once that combined move reaches COMBINED_STOPLOSS_POINTS (50), close both legs
    together as one straddle-level stop - not a per-leg stop. Stays flat until the next checkpoint.
  - halt trading for the day (square everything off, no more re-entries) once realized+unrealized
    pnl crosses -DAILY_LOSS_LIMIT (100 points, unscaled by lot size). Checked on EVERY poll tick
    unconditionally - whether or not a position is currently open - matching the backtest's "checked
    every minute, not just at checkpoints" rule. This is deliberately NOT how
    execution_straddle_premium_stoploss_ws.py does it: that script only evaluates the daily loss
    limit from inside its position-open monitor loop, so realized losses accumulated purely from
    checkpoint-driven closes (premium-rise flattens, strike rolls) are never checked against the
    limit, and a fresh position can still be opened at the next checkpoint even after the day's
    already blown through -100. Here the check runs unconditionally every poll, exactly like the
    backtest, so that gap doesn't exist.

There's no combined-stoploss equivalent to exec_rsv_cont.py's "continuous" per-leg fix: a per-leg
resting SL order is inherently continuous (fires the instant price touches it), but there's no
broker order type for "sum of two legs' premium move" - that can only ever be a software poll, live
or in backtest, so the backtest's once-a-minute close-sampled check already matches what live
trading actually does here. Nothing to fix - the backstop SL above is a separate, live-only safety
net, not an attempt to make the combined check itself continuous.

The backstop SL fires independently of this script's own logic (a resting broker order, not
something this process decides) - _sync_stopped_out_legs polls broker positions every
POLL_INTERVAL_SECONDS to notice when one has vanished, alerts loudly (it's the "combined stoploss
failed to catch this" case), and clears just that leg from state. The other leg is left exactly as
is - each leg's backstop is independent, not a straddle-level stop - and the checkpoint's normal
"reopen any leg that isn't currently open" branch picks the cleared leg back up next hour, same as
any other close.

Closing a leg (any reason - premium-rise, roll, combined stoploss, daily loss limit, EOD) never
cancels the resting backstop SL and places a brand new order. Instead it repeatedly MODIFIES that
same SL order (_force_exit_leg) so its trigger/limit bracket the current LTP (trigger = ltp-5%,
limit = ltp+5% - already past trigger, so it's marketable and should fill immediately), re-pricing
off a fresh LTP and retrying every FORCE_EXIT_POLL_INTERVAL (2s) up to FORCE_EXIT_MAX_ATTEMPTS (5)
before falling back to cancel + a plain MARKET order as a last resort. (A leg adopted at startup
has no tracked SL order id - see run_day - so it falls back to cancel + fresh order directly, same
as before.) This whole retry loop runs on a background thread, not the main poll loop, so a slow
close never stalls checkpoints/combined-stoploss/heartbeat elsewhere; the leg is marked 'closing'
the instant the close is requested so nothing else (a re-entry, another close, _sync_stopped_out_legs)
acts on it while the thread is working, and _state_lock guards state/day['realized_pnl'] writes from
the worker thread against the main thread. EOD square-off calls the same worker directly on the main
thread (blocking) instead, and run_day joins every background exit thread before the process exits,
since a daemon thread still mid-retry would otherwise be killed outright. Every modify/cancel/place
request and response goes to exec_rs_ps_orders.log.

EXIT_TIME is 15:13, not the backtest's literal 15:15 - the same 2-minute live-trading safety buffer
used throughout this folder (see execution_rolling_straddle_variation_mn_hs_fn.py, exec_rsv_cont.py).

Market data (spot LTP, option LTPs) comes from Zerodha's Kite Connect API - but every read goes
through zerodha_ltp_client.py's shared Redis cache first (kept fresh by zerodha_ticker_service.py's
one shared ticker websocket), falling straight through to a direct Kite REST /quote/ltp call
whenever Redis is unreachable or the cached price is missing/older than LTP_STALE_SECONDS - same
pattern as exec_rsv_cont.py, see that module's docstrings for the full rationale. Order placement
goes through AliceBlue's REST API. Fully self-contained, like nifty_option_buying_twhf.py /
exec_rsv_cont.py - deliberately does NOT import execution_rolling_straddle_tn.py (Dhan-backed);
Dhan is never touched, imported, or authenticated by this file at all.

Trades a single underlying (command-line arg, default SENSEX) on TRADE_WEEKDAYS (command-line
weekday codes, default Tuesday only).

No local state file: every startup reconciles against the broker instead (AliceBlue positions +
order book). On startup with legs already open (mid-day restart, or placed manually), each leg's
actual entry price is reconstructed from the AliceBlue order book's completed SELL fills where
possible, falling back to live LTP (logged clearly as an approximation) only if that reconstruction
fails - the backstop SL for an adopted leg is NOT re-placed at that reconstructed price (a fresh
one already rests at the broker from whenever the leg was actually entered; placing a second one
would just be a duplicate). A late start (process comes up after ENTRY_TIME with nothing open)
always fires the initial entry immediately.

Logging: three separate files under the logs/ folder (LOG_FOLDER), each also on stdout where noted:
  - exec_rs_ps.log: the main strategy log (also stdout). Lifecycle events are additionally pushed
    to Telegram via alert() below, and any WARNING+ log record is pushed automatically as a safety
    net. A HEARTBEAT_INTERVAL "still running" ping goes out with the current legs, day pnl so far,
    and the last real event/timestamp.
  - exec_rs_ps_premium.log: the combined ATM (CE+PE) premium, logged every poll tick.
  - exec_rs_ps_orders.log: every broker order request payload and response (AliceBlue placeorder/
    cancel), for after-the-fact reconciliation independent of the main log's narrative text.
Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env; if either is missing, Telegram alerts
(including heartbeats) are skipped (logged as a one-time warning) but trading proceeds normally.
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
from datetime import datetime, time as dtime, timedelta, timezone
from typing import NamedTuple

import requests
from dotenv import load_dotenv

import zerodha_ltp_client

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# ── Logging ──────────────────────────────────────────────────────────────────────────────────
LOG_FOLDER = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOG_FOLDER, exist_ok=True)
LOG_FILE = os.path.join(LOG_FOLDER, 'exec_rs_ps.log')
PREMIUM_LOG_FILE = os.path.join(LOG_FOLDER, 'exec_rs_ps_premium.log')
ORDER_LOG_FILE = os.path.join(LOG_FOLDER, 'exec_rs_ps_orders.log')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_TIMEOUT = 10

log = logging.getLogger('exec_rs_ps')
log.setLevel(logging.INFO)
log.propagate = False
_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
for _handler in (logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)):
    _handler.setFormatter(_formatter)
    log.addHandler(_handler)

# Dedicated ATM-premium log - one line per poll tick, separate from the narrative main log.
premium_log = logging.getLogger('exec_rs_ps.premium')
premium_log.setLevel(logging.INFO)
premium_log.propagate = False
_premium_handler = logging.FileHandler(PREMIUM_LOG_FILE)
_premium_handler.setFormatter(_formatter)
premium_log.addHandler(_premium_handler)

# Dedicated broker order log - every AliceBlue order request/response, verbatim.
order_log = logging.getLogger('exec_rs_ps.orders')
order_log.setLevel(logging.INFO)
order_log.propagate = False
_order_handler = logging.FileHandler(ORDER_LOG_FILE)
_order_handler.setFormatter(_formatter)
order_log.addHandler(_order_handler)


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

# ── Strategy config (mirrors backtest_rolling_straddle_variation_premium_stoploss.py) ──────────
ENTRY_TIME = dtime(9, 45)
EXIT_TIME = dtime(15, 13)  # 2-min live-trading safety buffer before the backtest's literal 15:15
CHECKPOINT_INTERVAL = timedelta(hours=1)  # no Friday override in this backtest, unlike the
# variation_continuous family - stays flat 1h every weekday
POLL_INTERVAL_SECONDS = 30
HEARTBEAT_INTERVAL = timedelta(minutes=30)

FIRST_OTM_STRIKES = 0  # 0 = ATM; n = n strikes OTM (CE up, PE down)
LEG_STOPLOSS_POINTS = 200  # per-leg resting broker-side backstop SL, flat points from that leg's
# own entry price - same distance for NIFTY and SENSEX. Not part of the backtest's rules; a
# live-only worst-case dead-man's-switch for when COMBINED_STOPLOSS_POINTS can't fire (process
# down, network outage) - see module docstring.
COMBINED_STOPLOSS_POINTS = 50  # exit both legs together once their combined premium has moved this many points against entry
DAILY_LOSS_LIMIT = 100  # points, unscaled by lot size - matches the backtest's convention

OPTION_TYPES = ('CE', 'PE')
DAY_CODE_TO_WEEKDAY = {'m': 'Monday', 't': 'Tuesday', 'w': 'Wednesday', 'h': 'Thursday', 'f': 'Friday'}

# Per-underlying config - strike_interval/lots/aliceblue_exchange match ers.UNDERLYINGS in
# execution_rolling_straddle_tn.py; zerodha_* fields are this file's own (market data only).
CFG = {
    'NIFTY': dict(
        strike_interval=50, lots=5, aliceblue_exchange='NFO',
        zerodha_options_exchange='NFO', zerodha_spot_instrument='NSE:NIFTY 50',
    ),
    'SENSEX': dict(
        strike_interval=100, lots=5, aliceblue_exchange='BFO',
        zerodha_options_exchange='BFO', zerodha_spot_instrument='BSE:SENSEX',
    ),
}


def _parse_trade_weekdays(codes):
    """Compact weekday-code string -> set of weekday names, e.g. 'th' -> {'Tuesday', 'Thursday'}.
    None/empty trades every weekday (matches the backtest's default). Codes: m/t/w/h/f."""
    if not codes:
        return set(DAY_CODE_TO_WEEKDAY.values())
    weekdays = set()
    for code in codes.lower():
        weekday = DAY_CODE_TO_WEEKDAY.get(code)
        if weekday is None:
            raise ValueError(f"unknown weekday code {code!r} in {codes!r} - use any combination of {''.join(DAY_CODE_TO_WEEKDAY)}")
        weekdays.add(weekday)
    return weekdays


def _label(t):
    return f'{t:%H:%M}'


def _today_str():
    return datetime.now().strftime('%Y-%m-%d')


def _first_otm_strike(atm, option_type, strike_interval):
    if FIRST_OTM_STRIKES == 0:
        return atm
    sign = 1 if option_type == 'CE' else -1
    return atm + sign * FIRST_OTM_STRIKES * strike_interval


def atm_strike(spot, strike_interval):
    return round(spot / strike_interval) * strike_interval


# ── Zerodha (REST, Kite Connect) - market data only ─────────────────────────────────────────────
ZERODHA_API_KEY = os.getenv('ZERODHA_API_KEY')
ZERODHA_TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'zerodha_token.json')
ZERODHA_BASE_URL = 'https://api.kite.trade'
LTP_STALE_SECONDS = 5  # tighter than zerodha_ltp_client's own 10s default - this strategy prices
# entries/exits/the combined stoploss off these LTPs, so a stale cached price matters more here
# than it does for every other script sharing that cache; passed explicitly as stale_seconds on
# every zerodha_ltp_client.get_ltp/get_ltps call below rather than changing the shared default.
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


def _load_zerodha_current_week_options(symbol, cfg):
    """This week's CE/PE instruments for `symbol` from Kite's instrument dump - cached per
    calendar day per symbol."""
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
    current_week_expiry = min(row['expiry'] for row in opts)
    opts = [row for row in opts if row['expiry'] == current_week_expiry]

    _zerodha_options_cache[symbol] = {'date': today, 'options': opts}
    return opts


def _zerodha_option_row(zerodha_options, strike, option_type):
    for row in zerodha_options:
        if int(float(row['strike'])) == strike and row['instrument_type'] == option_type:
            return row
    raise KeyError(f'no Zerodha instrument found for strike={strike} type={option_type}')


def _zerodha_quote_ltp(instrument_keys):
    """instrument_keys like ['NSE:NIFTY 50', 'NFO:NIFTY25813950CE']. Returns key -> last_price.
    REST only - the hot path is zerodha_ltp_client.py's shared Redis cache (see below); this is
    passed to it as the rest_fetch/rest_fetch_batch fallback callable."""
    resp = requests.get(
        f'{ZERODHA_BASE_URL}/quote/ltp', headers=_zerodha_headers(ZERODHA_ACCESS_TOKEN),
        params=[('i', k) for k in instrument_keys], timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()['data']
    return {k: float(v['last_price']) for k, v in data.items()}


_zerodha_spot_token_cache = {}  # cfg['zerodha_spot_instrument'] -> {'date', 'token'}


def _zerodha_spot_token(cfg):
    """This underlying's spot index instrument_token (needed to read/subscribe it via the shared
    Redis cache) - looked up once per day per underlying from the relevant exchange's instrument
    dump, same caching pattern as _load_zerodha_current_week_options above."""
    key = cfg['zerodha_spot_instrument']
    today = _today_str()
    cached = _zerodha_spot_token_cache.get(key)
    if cached and cached['date'] == today:
        return cached['token']

    exchange, tradingsymbol = key.split(':', 1)
    resp = requests.get(f'{ZERODHA_BASE_URL}/instruments/{exchange}', headers=_zerodha_headers(ZERODHA_ACCESS_TOKEN), timeout=30)
    resp.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(resp.text)))
    for row in rows:
        if row.get('segment') == 'INDICES' and row.get('tradingsymbol') == tradingsymbol:
            token = int(row['instrument_token'])
            _zerodha_spot_token_cache[key] = {'date': today, 'token': token}
            return token
    raise RuntimeError(f'{tradingsymbol} index instrument_token not found on Zerodha {exchange} dump')


def get_spot_ltp(cfg):
    """Live spot LTP via the shared Redis cache (REST fallback baked in - see
    zerodha_ltp_client.get_ltp). Registers the spot token with the ticker service first
    (harmless/no-op if already registered, or if Redis is down)."""
    key = cfg['zerodha_spot_instrument']
    token = _zerodha_spot_token(cfg)
    zerodha_ltp_client.register_subscription(token)
    return zerodha_ltp_client.get_ltp(
        token, rest_fetch=lambda: _zerodha_quote_ltp([key])[key], log=log, stale_seconds=LTP_STALE_SECONDS,
    )


# ── AliceBlue (REST, v3 open-api) - order placement only ────────────────────────────────────────
ALICEBLUE_TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'aliceblue_token.json')
ALICEBLUE_BASE_URL = 'https://a3.aliceblueonline.com/open-api/od/v1'
ALICEBLUE_CONTRACT_MASTER_URL = 'https://v2api.aliceblueonline.com/restpy/static/contract_master/V2/{exchange}'
LIMIT_OFFSET_PCT = 0.05  # limit price offset from LTP: below LTP for SELL, above LTP for BUY -
# matches execution_rolling_straddle_tn.py's rolling-straddle-family value
FILL_POLL_TIMEOUT = 10
FILL_POLL_INTERVAL = 1
TERMINAL_ORDER_STATUSES = {'complete', 'rejected', 'cancelled'}
_ALICEBLUE_EMPTY_RESULT_STATUSES = {'EC920'}


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
    resp.raise_for_status()
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
    """This week's CE/PE contracts for `symbol` from AliceBlue's contract master - needed to
    place orders (instrument token/lot size/tick size). Separate from the Zerodha instruments
    above, which are used for pricing only. Cached per calendar day per symbol."""
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
    current_week_expiry = min(c['expiry_date'] for c in opts)
    contracts = [c for c in opts if c['expiry_date'] == current_week_expiry]

    _aliceblue_contracts_cache[symbol] = {'date': today, 'contracts': contracts}
    return contracts


def _to_instrument(contract):
    return Instrument(
        token=int(contract['token']), symbol=contract['symbol'],
        name=contract['trading_symbol'], lot_size=int(contract['lot_size']),
        tick_size=float(contract['tick_size']), exchange=contract['exch'],
    )


def _instrument_to_dict(instrument):
    return instrument._asdict()


def _instrument_from_dict(d):
    return Instrument(**d)


def _round_to_tick(price, tick_size):
    return round(round(price / tick_size) * tick_size, 2)


def _order_book():
    return _aliceblue_get('/orders/book')


class _PollingCache:
    """One shared poller/cache for a single broker read endpoint, for the whole process - refreshed
    every poll_interval by a single background thread, so multiple concurrent threads (CE/PE
    entering or exiting in parallel) read one shared snapshot instead of each hammering the same
    endpoint with their own poll loop. If a reader finds the cache missing or older than 2x
    poll_interval (poller hasn't started yet, or fell behind), it fetches directly itself and
    publishes the result for everyone else - "a leg fetches it by itself" when the shared poller
    hasn't kept up. A failed fetch, or one that fails `validate`, is treated as invalid: whoever
    hit it (the poller thread or a reader's own direct fetch) waits error_backoff before the next
    attempt rather than retrying immediately."""

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
            order_log.warning(f'{self._name} fetch failed ({exc}) - retrying in {self._error_backoff}s')
            return None
        if not self._validate(value):
            order_log.warning(f'{self._name} fetch returned {type(value).__name__}, unexpected shape - treating as invalid, retrying in {self._error_backoff}s')
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
        'price': str(price),
        'slTriggerPrice': str(trigger_price) if trigger_price is not None else '',
        'orderTag': order_tag or '',
    }]
    order_log.info(f'REQUEST placeorder: {payload}')
    result = _aliceblue_post('/orders/placeorder', payload)
    order_log.info(f'RESPONSE placeorder: {result}')
    return result[0] if isinstance(result, list) and len(result) == 1 else result


def _cancel_order(broker_order_id):
    order_log.info(f'REQUEST cancel: {broker_order_id}')
    result = _aliceblue_post('/orders/cancel', {'brokerOrderId': broker_order_id})
    order_log.info(f'RESPONSE cancel: {result}')
    return result


def _modify_order(broker_order_id, quantity, price, trigger_price, order_type='SL', validity='DAY'):
    """Modify a resting order in place (used to force the backstop SL to fill immediately instead
    of cancel+replace - see _force_exit_leg). Payload matches AliceBlue's documented
    POST /orders/modify exactly - see https://v2api.aliceblueonline.com/orders%20Management/
    ('brokerOrderId' required; quantity/orderType/price/slTriggerPrice/validity optional - no
    exchange/instrumentId/transactionType/product needed, unlike /orders/placeorder)."""
    payload = {
        'brokerOrderId': broker_order_id,
        'quantity': quantity,
        'orderType': order_type,
        'price': str(price),
        'slTriggerPrice': str(trigger_price),
        'validity': validity,
    }
    order_log.info(f'REQUEST modify: {payload}')
    result = _aliceblue_post('/orders/modify', payload)
    order_log.info(f'RESPONSE modify: {result}')
    return result


def _wait_for_fill_price(broker_order_id):
    """Reads the shared _order_book_cache (see above) rather than polling /orders/book on its own -
    when CE and PE entries run in parallel threads (_enter_legs_parallel), both threads' calls to
    this function share the one poller/cache instead of doubling the API traffic."""
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


def get_open_legs(contracts_by_token):
    """token -> position dict, restricted to currently open (nonzero net qty) legs among this
    week's option contracts for the current underlying. Reads the shared _positions_cache rather
    than fetching /positions on its own."""
    positions = _positions_cache.get() or []
    return {
        int(p['instrumentId']): p for p in positions
        if int(p['instrumentId']) in contracts_by_token and int(p.get('netQuantity', 0)) != 0
    }


def _leg_still_open(instrument):
    """True if the broker's positions still show a nonzero net quantity for this instrument -
    False once it's flat, whether that's because our own exit order filled or the leg was closed
    some other way entirely (manually, by a different process, etc). Used by _force_exit_leg to
    stop retrying once the position is already gone, rather than keep firing exit orders at
    nothing. Reads the shared _positions_cache - when CE and PE are both being force-closed at
    once (two _force_exit_leg threads), they read one shared positions fetch instead of each
    hitting /positions on their own. Fails safe: a missing/failed fetch is treated as 'still open'
    so a transient API hiccup can't make the forced exit give up early."""
    positions = _positions_cache.get()
    if positions is None:
        order_log.warning(f'position check for {instrument.name} failed - assuming still open')
        return True
    for p in positions:
        if str(p.get('instrumentId')) == str(instrument.token):
            return int(p.get('netQuantity', 0)) != 0
    return False


# ── Retry/backoff for read-only calls (Zerodha quotes/instruments, AliceBlue positions/orders) ──
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY = 5

MIN_CALL_INTERVAL = {'_load_zerodha_current_week_options': 3.5}  # instrument-dump download - large, infrequent
DEFAULT_MIN_CALL_INTERVAL = 0.5
_last_call_at = {}


def _throttle(key):
    min_interval = MIN_CALL_INTERVAL.get(key, DEFAULT_MIN_CALL_INTERVAL)
    last = _last_call_at.get(key)
    now = time_module.monotonic()
    if last is not None:
        wait = min_interval - (now - last)
        if wait > 0:
            time_module.sleep(wait)
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
            response = getattr(exc, 'response', None)
            if response is not None and response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        delay = max(delay, float(retry_after))
                    except ValueError:
                        pass
            log.warning(f'{fn.__name__} failed ({exc}) - retrying in {delay:.0f}s (attempt {attempt + 1}/{RETRY_MAX_ATTEMPTS})')
            time_module.sleep(delay)


# ── Market snapshot (one fresh fetch shared by a checkpoint and its surrounding minute checks) ──
def _fetch_market(symbol, cfg, state):
    """spot/ATM + a batched Zerodha LTP fetch covering exactly the strikes currently in play: the
    true ATM straddle (for the checkpoint's premium-rise check and the premium log, regardless of
    FIRST_OTM_STRIKES), today's desired first-OTM strikes (for entries/rolls), and whatever strike
    each currently-open leg actually sits at (which may have drifted off first-OTM already, but
    still needs a live quote to monitor/close). Redis first (shared ticker service feed - see
    zerodha_ltp_client.py) for every quote, one batched REST call via _zerodha_quote_ltp for
    whatever's missing/stale - same batched-fallback efficiency as a plain REST call, just
    Redis-fast for the common case instead of a REST round trip every poll."""
    zerodha_options = _resilient_call(_load_zerodha_current_week_options, symbol, cfg)
    spot = _resilient_call(get_spot_ltp, cfg)
    atm = atm_strike(spot, cfg['strike_interval'])
    desired_strike = {opt: _first_otm_strike(atm, opt, cfg['strike_interval']) for opt in OPTION_TYPES}

    contracts = _resilient_call(_load_aliceblue_contracts, symbol, cfg)
    contracts_by_token = {int(c['token']): c for c in contracts}
    contracts_by_strike_type = {(int(float(c['strike_price'])), c['option_type']): c for c in contracts}

    strike_types_needed = {(atm, 'CE'), (atm, 'PE')}
    for opt in OPTION_TYPES:
        strike_types_needed.add((desired_strike[opt], opt))
        leg = state[opt]
        if leg is not None:
            strike_types_needed.add((leg['strike'], opt))

    key_to_strike_type = {}
    token_to_key = {}
    zerodha_token_by_strike_type = {}
    for strike, opt in strike_types_needed:
        try:
            row = _zerodha_option_row(zerodha_options, strike, opt)
        except KeyError:
            log.warning(f'{opt} {strike}: no Zerodha instrument found - skipping this quote')
            continue
        key = f"{cfg['zerodha_options_exchange']}:{row['tradingsymbol']}"
        token = int(row['instrument_token'])
        key_to_strike_type[key] = (strike, opt)
        token_to_key[token] = key
        zerodha_token_by_strike_type[(strike, opt)] = token

    zerodha_ltp_client.register_subscriptions(list(token_to_key))
    token_to_price = zerodha_ltp_client.get_ltps(
        token_to_key, rest_fetch_batch=lambda keys: _resilient_call(_zerodha_quote_ltp, keys),
        log=log, stale_seconds=LTP_STALE_SECONDS,
    ) if token_to_key else {}
    price = {key_to_strike_type[token_to_key[token]]: p for token, p in token_to_price.items()}

    return dict(
        spot=spot, atm=atm, desired_strike=desired_strike,
        contracts_by_token=contracts_by_token, contracts_by_strike_type=contracts_by_strike_type,
        price=price, zerodha_token_by_strike_type=zerodha_token_by_strike_type,
    )


FETCH_UNTIL_SUCCESS_DELAY = 30
FETCH_UNTIL_SUCCESS_ALERT_EVERY = 10


def _fetch_market_until_success(symbol, cfg, state):
    attempt = 0
    while True:
        try:
            return _fetch_market(symbol, cfg, state)
        except Exception as exc:
            attempt += 1
            if attempt == 1 or attempt % FETCH_UNTIL_SUCCESS_ALERT_EVERY == 0:
                alert(f'Could not fetch market data for entry ({exc}) - still retrying (attempt {attempt}, every {FETCH_UNTIL_SUCCESS_DELAY}s)', level=logging.ERROR)
            time_module.sleep(FETCH_UNTIL_SUCCESS_DELAY)


def _atm_premium(market):
    ce = market['price'].get((market['atm'], 'CE'))
    pe = market['price'].get((market['atm'], 'PE'))
    if ce is None or pe is None:
        return None
    return ce + pe


def _desired_legs(market, cfg):
    """{'CE': (instrument, ltp, strike), 'PE': (...)} for today's first-OTM strikes - skips (with
    a warning) any leg whose strike/quote isn't available."""
    desired = {}
    for opt in OPTION_TYPES:
        strike = market['desired_strike'][opt]
        contract = market['contracts_by_strike_type'].get((strike, opt))
        ltp = market['price'].get((strike, opt))
        if contract is None or ltp is None:
            log.warning(f'{opt} {strike}: contract or live quote unavailable - skipping this leg')
            continue
        desired[opt] = (_to_instrument(contract), ltp, strike)
    return desired


# ── Per-leg state ─────────────────────────────────────────────────────────────────────────────
def _new_state():
    return {opt: None for opt in OPTION_TYPES}
    # None (flat), or {'instrument', 'strike', 'entry_price', 'quantity', 'sl_order_id', 'closing'}.
    # sl_order_id is the broker order id of the resting backstop SL - None for a leg adopted at
    # startup whose SL was placed by an earlier process instance (id not known, see run_day).
    # closing is True from the moment a close is requested until the exit worker confirms the fill
    # and clears the leg back to None - see _close_leg/_close_leg_worker.


_state_lock = threading.Lock()  # guards state[opt] and day['realized_pnl'] mutations from exit
# worker threads racing against the main poll loop.


def _short_leg_with_backstop_sl(instrument, quantity, ltp, order_tag):
    """SELL to open, then a resting BUY SL (stop-loss LIMIT) backstop LEG_STOPLOSS_POINTS above
    the actual fill price - see module docstring. This strategy's real risk control is the
    COMBINED stoploss (checked in software every poll); this resting order is purely a worst-case
    backstop for when that check can't run at all. Returns (entry_price, sl_order_id) - sl_order_id
    is None in DRY_RUN, where entry_price is just the priced LIMIT as a stand-in for bookkeeping."""
    entry_price = _round_to_tick(ltp * (1 - LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}SELL {quantity} x {instrument.name} LIMIT @ {entry_price} (ltp {ltp})'
    log.info(tag)
    if DRY_RUN:
        return entry_price, None

    entry = _place_order('SELL', instrument, quantity, 'LIMIT', price=str(entry_price), order_tag=order_tag)
    order_no = entry.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} entry order rejected: {entry}')

    entry_price = _wait_for_fill_price(order_no)
    trigger_price = round(entry_price + LEG_STOPLOSS_POINTS, 1)
    sl_limit_price = _round_to_tick(trigger_price * (1 + LIMIT_OFFSET_PCT), instrument.tick_size)
    log.info(f'{instrument.name} entered @ {entry_price}, backstop SL trigger {trigger_price} limit {sl_limit_price}')

    sl = _place_order('BUY', instrument, quantity, 'SL', price=str(sl_limit_price), trigger_price=trigger_price, order_tag='rs_ps_backstop_sl')
    sl_order_id = sl.get('brokerOrderId')
    if not sl_order_id:
        raise RuntimeError(f'{instrument.name} filled at {entry_price} but backstop SL order rejected: {sl}')
    return entry_price, sl_order_id


def _enter_leg(state, opt, instrument, ltp, strike, cfg):
    quantity = instrument.lot_size * cfg['lots']
    entry_price, sl_order_id = _short_leg_with_backstop_sl(instrument, quantity, ltp, 'rs_ps_entry')
    state[opt] = dict(
        instrument=instrument, strike=strike, entry_price=entry_price, quantity=quantity,
        sl_order_id=sl_order_id, closing=False,
    )
    alert(f'ENTER {opt} {instrument.name} x{quantity} @ ~{entry_price} (backstop SL +{LEG_STOPLOSS_POINTS}pts)')


def _enter_legs_parallel(state, legs_to_enter, cfg):
    """Place entries for multiple option legs (CE/PE) in parallel threads instead of strictly
    sequential - one thread per leg, each calling _enter_leg for its own option type. Both threads'
    fill-polling (_wait_for_fill_price) reads the shared _order_book_cache rather than each hammering
    /orders/book on its own - see that class's docstring. Blocks until every thread finishes; if any
    leg's entry raised, re-raises a combined error afterward (once all threads are done) so callers
    see a failure exactly like the old sequential code did, just not necessarily from the first leg
    tried."""
    if not legs_to_enter:
        return
    errors = {}

    def _run(opt, instrument, ltp, strike):
        try:
            _enter_leg(state, opt, instrument, ltp, strike, cfg)
        except Exception as exc:
            errors[opt] = exc

    threads = [
        threading.Thread(target=_run, args=(opt, instrument, ltp, strike), name=f'entry-{opt}')
        for opt, (instrument, ltp, strike) in legs_to_enter.items()
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise RuntimeError('entry failed for ' + ', '.join(f'{opt} ({exc})' for opt, exc in errors.items()))


FORCE_EXIT_POLL_INTERVAL = 2  # seconds between exit-order re-price attempts while forcing a close
FORCE_EXIT_STUCK_ALERT_EVERY = 30  # attempts between "still not squared off" escalation alerts (~1min at 2s cadence)


def _force_exit_leg(instrument, quantity, working_order_id, order_tag, log_prefix, zerodha_token):
    """Force a position closed using only SL (stop-loss LIMIT) orders - MARKET/SLM orders are
    never used, they are not permitted for this account/strategy. Prefers MODIFYING the existing
    resting order (working_order_id) over cancel+replace: each attempt re-prices it so trigger and
    limit both bracket the current LTP - trigger = ltp-5%, limit = ltp+5%, i.e. already past
    trigger and marketable, so it should fill immediately. If that order can't be found/modified
    (rejected, already filled/cancelled by something else, or working_order_id is None - e.g. an
    adopted leg whose original SL id isn't known) a brand new SL order is placed instead and the
    same retry loop continues working that one. There is NO attempt cap and NO MARKET fallback:
    this loops forever, re-pricing every FORCE_EXIT_POLL_INTERVAL, until the position is actually
    confirmed squared off - escalating to a CRITICAL alert every FORCE_EXIT_STUCK_ALERT_EVERY
    attempts so a stuck close doesn't go unnoticed, but it keeps retrying regardless. Each attempt
    also checks the broker's actual position (_leg_still_open) - if it's already flat, the leg was
    closed some other way (manually, or by anything other than the order this loop is tracking),
    and this stops immediately rather than continuing to fire exit orders at a position that no
    longer exists. Every decision and broker call is logged to order_log. Blocks the calling
    thread - callers that must not stall the main poll loop run this inside a background thread
    (see _close_leg); EOD square-off calls it directly on the main thread so the process doesn't
    exit before the fill is confirmed. LTP comes from the shared Redis cache (zerodha_token, when
    known - looked up once in _fetch_market and passed down; None for a leg the current market
    snapshot has no strike/opt entry for) with a direct REST fallback, same as everywhere else."""
    key = f'{instrument.exchange}:{instrument.name}'
    if zerodha_token is not None:
        zerodha_ltp_client.register_subscription(zerodha_token)
    attempt = 0
    while True:
        attempt += 1
        try:
            if zerodha_token is not None:
                ltp = zerodha_ltp_client.get_ltp(
                    zerodha_token, rest_fetch=lambda: _zerodha_quote_ltp([key]).get(key),
                    log=order_log, stale_seconds=LTP_STALE_SECONDS,
                )
            else:
                ltp = _zerodha_quote_ltp([key]).get(key)
        except Exception as exc:
            ltp = None
            order_log.warning(f'{log_prefix}: LTP fetch failed on attempt {attempt} ({exc})')
        if ltp is None:
            order_log.warning(f'{log_prefix}: no LTP available on attempt {attempt} - retrying in {FORCE_EXIT_POLL_INTERVAL}s')
            time_module.sleep(FORCE_EXIT_POLL_INTERVAL)
            continue

        trigger_price = round(ltp * (1 - LIMIT_OFFSET_PCT), 1)
        limit_price = _round_to_tick(ltp * (1 + LIMIT_OFFSET_PCT), instrument.tick_size)

        if working_order_id is not None:
            order_log.info(
                f'{log_prefix}: attempt {attempt} - modifying SL {working_order_id} to '
                f'trigger={trigger_price} limit={limit_price} (ltp {ltp}) to force an immediate fill'
            )
            try:
                _modify_order(working_order_id, quantity, limit_price, trigger_price)
            except Exception as exc:
                order_log.warning(f'{log_prefix}: modify of {working_order_id} failed on attempt {attempt} ({exc}) - will place a fresh SL order instead')
                working_order_id = None

        if working_order_id is None:
            order_log.info(f'{log_prefix}: attempt {attempt} - placing a new SL order, trigger={trigger_price} limit={limit_price} (ltp {ltp})')
            try:
                new_order = _place_order('BUY', instrument, quantity, 'SL', price=str(limit_price), trigger_price=trigger_price, order_tag=order_tag)
                working_order_id = new_order.get('brokerOrderId')
                if not working_order_id:
                    order_log.warning(f'{log_prefix}: new SL order rejected on attempt {attempt}: {new_order}')
            except Exception as exc:
                order_log.warning(f'{log_prefix}: placing a new SL order failed on attempt {attempt} ({exc})')

        time_module.sleep(FORCE_EXIT_POLL_INTERVAL)

        book = _order_book_cache.get()
        if book is not None and working_order_id is not None:
            for o in book:
                if o.get('brokerOrderId') != working_order_id:
                    continue
                status = str(o.get('orderStatus', '')).lower()
                if status == 'complete':
                    fill_price = float(o.get('averageTradedPrice') or 0)
                    order_log.info(f'{log_prefix}: order {working_order_id} filled @ {fill_price} on attempt {attempt}')
                    return fill_price
                if status in ('rejected', 'cancelled'):
                    order_log.warning(f'{log_prefix}: order {working_order_id} is {status} - will place a fresh SL order next attempt')
                    working_order_id = None
                break
        elif book is None:
            order_log.warning(f'{log_prefix}: order book unavailable on attempt {attempt}')

        # Our own order isn't showing 'complete' yet - but the position might already be flat
        # anyway, closed outside this process entirely (manually, by a different order/process).
        # Nothing left to square off in that case - stop retrying rather than keep firing exit
        # orders at a position that no longer exists (which would just open a fresh naked position
        # in the other direction instead of closing anything).
        if not _leg_still_open(instrument):
            order_log.info(f'{log_prefix}: broker position is already flat on attempt {attempt} - closed outside this process, stopping the forced exit')
            if working_order_id is not None:
                try:
                    _cancel_order(working_order_id)
                except Exception as exc:
                    order_log.warning(f'{log_prefix}: cancel of now-stale order {working_order_id} failed ({exc})')
            alert(
                f'{log_prefix}: position was already flat at the broker (closed outside this process, e.g. manually) - '
                f'stopping the forced exit; pnl is a best-effort estimate off the current LTP, not an actual fill price',
                level=logging.WARNING,
            )
            return ltp

        if attempt % FORCE_EXIT_STUCK_ALERT_EVERY == 0:
            alert(
                f'{log_prefix}: still NOT squared off after {attempt} attempts '
                f'(~{attempt * FORCE_EXIT_POLL_INTERVAL}s) - no MARKET fallback permitted, continuing to retry with SL orders',
                level=logging.CRITICAL,
            )


def _close_leg_order(instrument, quantity, order_tag, sl_order_id, log_prefix, zerodha_token):
    """Square off one leg via _force_exit_leg - SL orders only, retried until actually filled (see
    that function's docstring). If a backstop SL order id is tracked, that order is force-filled by
    repeated modify; if not (a leg adopted at startup, whose SL was placed by an earlier process
    instance), any other resting order on this instrument is cancelled first and a fresh SL order
    is placed instead. Returns the fill price, or None in DRY_RUN."""
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}BUY (square off) {quantity} x {instrument.name}'
    log.info(tag)
    if DRY_RUN:
        return None

    if sl_order_id is None:
        order_log.info(f'{log_prefix}: no tracked backstop SL order id (adopted leg) - cancelling any resting order before forcing a fresh SL exit')
        for o in _order_book_cache.get() or []:
            if str(o.get('instrumentId')) == str(instrument.token) and str(o.get('orderStatus', '')).lower() not in TERMINAL_ORDER_STATUSES:
                _cancel_order(o['brokerOrderId'])

    return _force_exit_leg(instrument, quantity, sl_order_id, order_tag, log_prefix, zerodha_token)


def _close_leg_worker(state, opt, leg, market, cfg, reason, day):
    """Runs the actual broker-side close (blocking) then updates shared state/pnl under
    _state_lock. Called directly (blocking) for EOD; spawned in a thread otherwise - see
    _close_leg."""
    log_prefix = f'{opt} {leg["instrument"].name} close (reason={reason})'
    exit_ltp_snapshot = market['price'].get((leg['strike'], opt))
    zerodha_token = market.get('zerodha_token_by_strike_type', {}).get((leg['strike'], opt))
    try:
        fill_price = _close_leg_order(leg['instrument'], leg['quantity'], 'rs_ps_exit', leg.get('sl_order_id'), log_prefix, zerodha_token)
    except Exception as exc:
        alert(f'{log_prefix}: exit order failed - leg may still be OPEN, check manually: {exc}', level=logging.CRITICAL)
        with _state_lock:
            if state[opt] is leg:
                leg['closing'] = False  # not confirmed closed - leave it open so it gets retried
        return
    exit_price = fill_price if fill_price is not None else exit_ltp_snapshot  # DRY_RUN: no real fill, approximate off the snapshot LTP
    pnl = (leg['entry_price'] - exit_price) if exit_price is not None else 0.0
    with _state_lock:
        day['realized_pnl'] += pnl
        if state[opt] is leg:
            state[opt] = None
    alert(f'EXIT {opt} {leg["instrument"].name} @ ~{exit_price} (entry ~{leg["entry_price"]}) pnl~{pnl:+.2f} reason={reason}')


def _close_leg(state, opt, market, cfg, reason, day, blocking=False):
    """Request a leg be closed. Non-blocking by default: marks the leg 'closing' and hands the
    broker work off to a background thread so the main poll loop isn't stalled waiting on SL-modify
    retries; pass blocking=True (EOD only) to run it on the calling thread instead so the process
    doesn't exit before the fill is confirmed. A leg already closing is left alone - callers may
    request a close on the same leg from multiple checks in one poll tick without double-firing."""
    leg = state[opt]
    if leg is None or leg.get('closing'):
        return
    if market['price'].get((leg['strike'], opt)) is None and leg.get('sl_order_id') is None:
        log.warning(f'{opt} {leg["strike"]}: no live quote and no trackable SL order to force-exit against, leaving position open')
        return
    leg['closing'] = True
    if blocking:
        _close_leg_worker(state, opt, leg, market, cfg, reason, day)
    else:
        thread = threading.Thread(
            target=_close_leg_worker, args=(state, opt, leg, market, cfg, reason, day),
            name=f'exit-{opt}-{reason}', daemon=True,
        )
        day['exit_threads'].append(thread)
        thread.start()


def _close_open_legs(state, market, cfg, reason, day, blocking=False):
    for opt in OPTION_TYPES:
        _close_leg(state, opt, market, cfg, reason, day, blocking=blocking)


def _sync_stopped_out_legs(state, market):
    """Broker-side truth: a leg we believe is open but whose position has vanished means its
    resting backstop SL filled (see module docstring - LEG_STOPLOSS_POINTS, a worst-case-only
    dead-man's-switch; the combined stoploss should always have closed things well before this).
    Alerted as a WARNING since it firing means the software combined check failed to catch it in
    time - worth knowing about even if the pnl outcome itself is fine. Only the stopped-out leg is
    cleared; the other leg (if any) is left exactly as is - each leg's backstop is independent, not
    a straddle-level stop. Legs already 'closing' are skipped here - their SL is disappearing on
    purpose (forced via _force_exit_leg), not an unplanned stop-out; _close_leg_worker handles
    clearing those once the fill is confirmed."""
    open_tokens = set(_resilient_call(get_open_legs, market['contracts_by_token']))
    for opt in OPTION_TYPES:
        leg = state[opt]
        if leg is not None and not leg.get('closing') and leg['instrument'].token not in open_tokens:
            alert(
                f'BACKSTOP SL FILLED: {opt} {leg["instrument"].name} (entry ~{leg["entry_price"]}) - '
                f'combined stoploss did not catch this in time; flat until next checkpoint',
                level=logging.WARNING,
            )
            state[opt] = None


# ── Startup adoption: reconstruct entry price from the order book ──────────────────────────────
_ORDER_TIME_FIELDS = ('orderGeneratedTime', 'orderEntryTime', 'exchangeTime', 'orderTime')


def _order_sort_key(order):
    for field in _ORDER_TIME_FIELDS:
        if order.get(field):
            return order[field]
    return order.get('brokerOrderId', '')


def _infer_entry_price_from_orderbook(token, open_quantity):
    """Best-effort reconstruction of a short leg's actual average fill price from AliceBlue's
    order book - see execution_rolling_straddle_variation_mn_hs_fn.py's version of this for the
    full rationale. Returns None (caller falls back to live LTP) if not confident."""
    try:
        orders = _resilient_call(_order_book)
    except Exception as exc:
        log.warning(f'could not fetch order book to infer entry price for token {token}: {exc}')
        return None

    fills = [
        o for o in orders
        if str(o.get('instrumentId')) == str(token)
        and str(o.get('transactionType', '')).upper() == 'SELL'
        and str(o.get('orderStatus', '')).lower() == 'complete'
    ]
    fills.sort(key=_order_sort_key, reverse=True)

    remaining = open_quantity
    weighted_sum = 0.0
    covered = 0
    for o in fills:
        try:
            filled_qty = int(o.get('quantity') or o.get('filledQuantity') or 0)
            price = float(o.get('averageTradedPrice') or 0)
        except (TypeError, ValueError):
            continue
        if filled_qty <= 0 or price <= 0:
            continue
        take = min(filled_qty, remaining)
        weighted_sum += take * price
        covered += take
        remaining -= take
        if remaining <= 0:
            break

    if covered == 0:
        return None
    if covered < open_quantity:
        log.warning(f'order book only accounted for {covered}/{open_quantity} of open quantity for token {token} - using the weighted average of what it did find')
    return weighted_sum / covered


# ── Checkpoint (hourly) ──────────────────────────────────────────────────────────────────────
def run_checkpoint(state, market, cfg, day, symbol):
    prev_premium = day['prev_checkpoint_premium']
    current_premium = _atm_premium(market)
    premium_increased = (
        prev_premium is not None and current_premium is not None
        and current_premium > prev_premium
    )
    if current_premium is not None:
        day['prev_checkpoint_premium'] = current_premium

    log.info(f'checkpoint: spot={market["spot"]} atm={market["atm"]} atm_premium={current_premium} (prev={prev_premium}) day_pnl={day["realized_pnl"]:.2f}')

    if premium_increased:
        if state['CE'] is not None or state['PE'] is not None:
            alert('ATM premium rose vs previous checkpoint - closing any open legs, no new trade this hour')
            _close_open_legs(state, market, cfg, 'ATM_PREMIUM_RISE', day)
        else:
            log.info('ATM premium rose vs previous checkpoint - no legs open, no new trade this hour')
        return

    if day['suppress_reentry']:
        log.info('combined stoploss (or an optional premium check) fired earlier this hour - staying flat this checkpoint')
        day['suppress_reentry'] = False
        return

    desired = _desired_legs(market, cfg)
    drifted = any(
        state[opt] is not None and opt in desired and state[opt]['strike'] != desired[opt][2]
        for opt in OPTION_TYPES
    )
    if drifted:
        alert('first-OTM strike has moved - rolling both legs')
        _close_open_legs(state, market, cfg, 'ROLL_OTM_DRIFT', day)
        _enter_legs_parallel(state, desired, cfg)
        return

    to_enter = {}
    for opt, (instrument, ltp, strike) in desired.items():
        if state[opt] is None:
            to_enter[opt] = (instrument, ltp, strike)
        else:
            log.info(f'{opt} still open at {state[opt]["strike"]}, leaving as is')
    _enter_legs_parallel(state, to_enter, cfg)


# ── Minute-level checks (every poll, unconditionally - not gated on a position being open) ──────
def run_minute_checks(state, market, cfg, day, now, symbol):
    current_premium = _atm_premium(market)
    if current_premium is not None:
        premium_log.info(f'{symbol} atm={market["atm"]} atm_premium={current_premium:.2f}')
    any_open = state['CE'] is not None or state['PE'] is not None

    # combined CE+PE stoploss - sums the premium move against entry across whichever legs are
    # currently open; once that combined loss reaches COMBINED_STOPLOSS_POINTS, both legs are
    # exited together as one straddle-level stop (no per-leg stop exists in this strategy at all).
    open_legs = [(opt, state[opt]) for opt in OPTION_TYPES if state[opt] is not None]
    if open_legs:
        current_prices = {opt: market['price'].get((leg['strike'], opt)) for opt, leg in open_legs}
        if all(p is not None for p in current_prices.values()):
            combined_loss = sum(current_prices[opt] - leg['entry_price'] for opt, leg in open_legs)
            if combined_loss >= COMBINED_STOPLOSS_POINTS:
                alert(f'COMBINED STOPLOSS hit: premium moved {combined_loss:.2f} pts (>= {COMBINED_STOPLOSS_POINTS}) against entry - closing both legs, staying flat until next checkpoint')
                _close_open_legs(state, market, cfg, 'COMBINED_STOPLOSS', day)
                # deliberately no day['suppress_reentry'] = True here - matches the backtest, which
                # doesn't set it for the combined stoploss. The checkpoint's normal "reopen any leg
                # not open" branch already gives the "flat for the rest of this hour" behavior on
                # its own, since it only runs once per hour; setting suppress_reentry here would
                # make the *next* checkpoint skip its reopen too, leaving legs flat for two hours
                # instead of one.

    unrealized = 0.0
    for opt in OPTION_TYPES:
        leg = state[opt]
        if leg is None:
            continue
        current_ltp = market['price'].get((leg['strike'], opt))
        if current_ltp is None:
            continue
        unrealized += leg['entry_price'] - current_ltp

    # Checked unconditionally, every poll tick, whether or not a position is currently open - this
    # is the part execution_straddle_premium_stoploss_ws.py gets wrong (it only evaluates the daily
    # loss limit from inside its position-open monitor loop). Here, a run of checkpoint-driven
    # closes (premium-rise flattens, strike rolls) that push realized_pnl past -DAILY_LOSS_LIMIT
    # while flat is caught on the very next poll, not just the next time a position happens to be
    # open - matching the backtest's "checked every minute, not just at checkpoints" rule exactly.
    if day['realized_pnl'] + unrealized <= -DAILY_LOSS_LIMIT:
        alert(
            f'DAILY LOSS LIMIT hit: realized {day["realized_pnl"]:.2f} + unrealized {unrealized:.2f} '
            f'<= -{DAILY_LOSS_LIMIT} - halting for the day, squaring off',
            level=logging.CRITICAL,
        )
        # blocking=True here (unlike the other close paths): this halts the day and the main loop
        # exits right after, so the close must be confirmed before the EOD square-off below runs
        # into a leg still marked 'closing' from an in-flight background thread.
        _close_open_legs(state, market, cfg, 'DAILY_LOSS_LIMIT', day, blocking=True)
        day['halted'] = True


# ── Heartbeat ─────────────────────────────────────────────────────────────────────────────────
def _send_heartbeat(state, day, symbol, now):
    legs = ', '.join(
        f"{opt} {state[opt]['instrument'].name} @ ~{state[opt]['entry_price']}" if state[opt] else f'{opt} flat'
        for opt in OPTION_TYPES
    )
    if _last_event['at'] is not None:
        last = f"{_last_event['text']} ({_last_event['at']:%H:%M:%S})"
    else:
        last = 'none yet'
    message = (
        f"🟢 still running - {symbol} {now:%H:%M:%S}\n"
        f"legs: {legs}\n"
        f"day pnl so far: {day['realized_pnl']:+.2f}\n"
        f"last event: {last}"
    )
    log.info(f'heartbeat: {message}')
    try:
        _telegram_send(message)
    except Exception as exc:
        print(f'heartbeat Telegram send failed: {exc}', file=sys.stderr)


# ── Day driver ────────────────────────────────────────────────────────────────────────────────
def _sleep_until(target_time, label):
    now = datetime.now()
    target = datetime.combine(now.date(), target_time)
    wait = (target - now).total_seconds()
    if wait > 0:
        log.info(f'waiting until {label} ({target_time})...')
        time_module.sleep(wait)


def run_day(symbol, trade_weekdays):
    today_name = datetime.now().strftime('%A')
    if today_name not in trade_weekdays:
        log.info(f'{today_name} is not in TRADE_WEEKDAYS ({sorted(trade_weekdays)}) - not trading')
        return

    cfg = CFG[symbol]
    alert(f'Rolling straddle (premium stoploss) starting for {symbol} - {today_name} {datetime.now():%Y-%m-%d}')

    _order_book_cache.start()  # shared poller backing every _wait_for_fill_price/_force_exit_leg
    _positions_cache.start()   # order-book/positions read for the rest of the day - see _PollingCache

    state = _new_state()
    day = dict(
        realized_pnl=0.0, halted=False, suppress_reentry=False,
        prev_checkpoint_premium=None, exit_threads=[],
    )

    _sleep_until(ENTRY_TIME, 'entry time')
    entry_time = datetime.now()

    scheduled_entry = datetime.combine(entry_time.date(), ENTRY_TIME)
    next_checkpoint = scheduled_entry + CHECKPOINT_INTERVAL
    while next_checkpoint <= entry_time:
        next_checkpoint += CHECKPOINT_INTERVAL
    next_heartbeat = entry_time + HEARTBEAT_INTERVAL

    market = _fetch_market_until_success(symbol, cfg, state)
    open_tokens = set(_resilient_call(get_open_legs, market['contracts_by_token']))
    if open_tokens:
        alert('Found open positions at startup (mid-day restart?) - reconstructing entry prices from the order book where possible', level=logging.WARNING)
        open_legs_by_token = _resilient_call(get_open_legs, market['contracts_by_token'])
        for strike_opt, contract in market['contracts_by_strike_type'].items():
            token = int(contract['token'])
            if token in open_tokens:
                opt = contract['option_type']
                instrument = _to_instrument(contract)
                pos = open_legs_by_token[token]
                quantity = abs(int(pos['netQuantity']))
                entry_price = _infer_entry_price_from_orderbook(token, quantity)
                if entry_price is not None:
                    log.info(f'{opt} {strike_opt[0]}: reconstructed entry price {entry_price:.2f} from order book')
                else:
                    entry_price = market['price'].get((strike_opt[0], opt), 0.0)
                    log.warning(f"{opt} {strike_opt[0]}: couldn't reconstruct entry price from order book - falling back to live LTP {entry_price:.2f} (not the actual fill price)")
                # sl_order_id=None: this leg's backstop SL was placed by an earlier process
                # instance, so its broker order id isn't known here - _close_leg_order falls back
                # to cancel+fresh-order for it instead of the SL-modify force-exit path.
                state[opt] = dict(
                    instrument=instrument, strike=strike_opt[0], entry_price=entry_price, quantity=quantity,
                    sl_order_id=None, closing=False,
                )

        adopted_premium = sum(leg['entry_price'] for leg in state.values() if leg is not None) or None
        day['prev_checkpoint_premium'] = adopted_premium
        if adopted_premium is not None:
            alert(f'Adopted legs - reconstructed combined entry ~{adopted_premium:.2f}, used as the previous-checkpoint premium')
    else:
        log.info('No open positions - entering initial legs')
        day['prev_checkpoint_premium'] = _atm_premium(market)
        _enter_legs_parallel(state, _desired_legs(market, cfg), cfg)

    reuse_entry_market = True
    poll_failure_count = 0

    while not day['halted']:
        now = datetime.now()
        if now.time() >= EXIT_TIME:
            break

        try:
            if reuse_entry_market:
                reuse_entry_market = False
            else:
                market = _fetch_market(symbol, cfg, state)
            _sync_stopped_out_legs(state, market)
            run_minute_checks(state, market, cfg, day, now, symbol)

            if not day['halted'] and now >= next_checkpoint:
                run_checkpoint(state, market, cfg, day, symbol)
                next_checkpoint += CHECKPOINT_INTERVAL

            if now >= next_heartbeat:
                _send_heartbeat(state, day, symbol, now)
                next_heartbeat += HEARTBEAT_INTERVAL
            poll_failure_count = 0
        except Exception as exc:
            poll_failure_count += 1
            _alert_failure_throttled(f'Poll iteration failed, skipping to next cycle: {exc}', poll_failure_count)

        sleep_for = POLL_INTERVAL_SECONDS
        remaining_to_exit = (datetime.combine(now.date(), EXIT_TIME) - datetime.now()).total_seconds()
        time_module.sleep(max(0, min(sleep_for, remaining_to_exit)))

    if not day['halted']:
        log.info(f'{EXIT_TIME} reached - squaring off any open positions')

    for attempt in range(RETRY_MAX_ATTEMPTS):
        try:
            final_market = _fetch_market(symbol, cfg, state)
            # blocking=True: this is the last thing the process does before exiting, and any exit
            # thread still spawned as non-blocking (daemon=True) would be killed mid-flight the
            # instant the process exits - see the thread-join below for threads from earlier polls.
            _close_open_legs(state, final_market, cfg, 'EOD', day, blocking=True)
            break
        except Exception as exc:
            if attempt == RETRY_MAX_ATTEMPTS - 1:
                alert(f'{symbol}: final square-off failed after {RETRY_MAX_ATTEMPTS} attempts - positions may still be OPEN, check manually: {exc}', level=logging.CRITICAL)
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            log.warning(f'final square-off failed ({exc}) - retrying in {delay:.0f}s (attempt {attempt + 1}/{RETRY_MAX_ATTEMPTS})')
            time_module.sleep(delay)

    # Wait for any earlier-poll background exit threads to actually finish - they're daemon
    # threads, so if the process exits while one is still mid SL-modify-retry, it gets killed and
    # the leg it was closing could be left open with no one watching it.
    for thread in day['exit_threads']:
        thread.join(timeout=60)
        if thread.is_alive():
            alert(f'{symbol}: exit thread {thread.name} still running at shutdown - a leg may still be OPEN, check manually', level=logging.CRITICAL)

    alert(f"Rolling straddle (premium stoploss) done for {symbol} - realized pnl {day['realized_pnl']:+.2f} points")


if __name__ == '__main__':
    SYMBOL = sys.argv[1].upper() if len(sys.argv) > 1 and sys.argv[1] else 'SENSEX'
    if SYMBOL not in CFG:
        raise ValueError(f'unknown symbol {SYMBOL!r} - use one of {sorted(CFG)}')
    TRADE_WEEKDAYS = _parse_trade_weekdays(sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else 't')
    run_day(SYMBOL, TRADE_WEEKDAYS)
