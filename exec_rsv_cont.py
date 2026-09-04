"""
Live execution of Data/backtests/backtest_rolling_straddle_variation_continuous.py: short a
first-OTM strangle (FIRST_OTM_STRIKES away from ATM; 0 = ATM itself) at ENTRY_TIME, each leg with a
resting per-leg STOPLOSS_PCT stoploss. Every CHECKPOINT_INTERVAL thereafter:

  - if the current ATM straddle premium (CE+PE at the true ATM strike, not the first-OTM legs
    actually traded) is HIGHER than it was at the previous checkpoint: take no new trade this
    hour; if either leg is still open, close it outright.
  - else if a leg's stoploss fired earlier this hour (see below): stay flat for this leg for the
    rest of the hour - consumed here, not re-entered immediately.
  - else if either open leg has drifted off today's first-OTM strike (spot moved): roll the whole
    strangle - close whatever's open, re-enter fresh at the new first-OTM strikes.
  - else: re-enter any leg that isn't currently open (its resting stoploss got hit and closed the
    position) at the same strike; leave already-open legs alone.

NIFTY's Friday special-case is carried over from the backtest too, but it's a no-op: the backtest
sets a 1-hour CHECKPOINT_INTERVAL on Fridays, same as every other day, so this file does the same.

On "continuous": the backtest's _continuous variant exists to fix a *backtesting* limitation - a
close-only, once-a-minute stoploss check can miss (or overshoot) a fast intrabar move, so it
switches to sampling each 1-minute bar's intrabar high instead. That limitation doesn't apply to
live trading at all: every leg here is protected by a genuine resting broker-side SL order, placed
the moment the leg is entered, which fires the instant price actually touches it - inherently more
continuous than any minute-sampled backtest could simulate. So porting the *_continuous* backtest's
rules here means exactly what porting the plain (non-continuous) backtest's rules would have meant:
a resting SL order per leg, same as execution_rolling_straddle_variation_mn_hs_fn.py's per-leg
stoploss. Unlike that script, this one does NOT add the SENSEX flat-points stoploss override or the
COMBINED_STOPLOSS_POINTS check - neither is part of backtest_rolling_straddle_variation_continuous.py's
rules, so this stays a straight STOPLOSS_PCT-of-entry-premium stoploss on every underlying, matching
the backtest exactly.

Between checkpoints, polls every POLL_INTERVAL_SECONDS (not just once an hour) to:
  - notice a leg's resting stoploss has filled (broker-side truth, via AliceBlue positions) and
    alert immediately rather than waiting for the next checkpoint to notice it's gone.
  - (optional, off by default) close both legs if the ATM premium is now above its own highest
    reading over the trailing PREMIUM_HIGH_LOOKBACK window.
  - halt trading for the day (square everything off, no more re-entries) once realized+unrealized
    pnl crosses -daily_loss_limit (points, unscaled by lot size; per-underlying via CFG - SENSEX 100
    matches the backtest, NIFTY overridden tighter at 40).

EXIT_TIME is 15:13, not the backtest's literal 15:15 - a 2-minute live-trading safety buffer before
the hard close, matching every other live script in this folder's convention (see
execution_rolling_straddle_variation_mn_hs_fn.py).

Market data (spot LTP, option LTPs) comes from Zerodha's Kite Connect API. Order placement goes
through AliceBlue's REST API. Fully self-contained, like nifty_option_buying_twhf.py - deliberately
does NOT import execution_rolling_straddle_tn.py (which authenticates with Dhan at import time as
its market-data source); Dhan is never touched, imported, or authenticated by this file at all.

Trades a single underlying (command-line arg, default NIFTY) on TRADE_WEEKDAYS (command-line
weekday codes, default every weekday - matching the backtest's own default). Lots default to 5 for
both NIFTY and SENSEX (informational CFG below); pass a different symbol/weekday-codes pair on the
command line to override.

State (today's checkpoints, pinned baseline premium, open legs, realized pnl, halted flag) is
persisted to STATE_FILE after every change, so a restart mid-day resumes instead of losing track of
an open position. On startup with positions already open (mid-day restart, or legs placed
manually), each leg's actual entry price is reconstructed from the AliceBlue order book's completed
SELL fills where possible, falling back to live LTP (logged clearly as an approximation) only if
that reconstruction fails - same approach as execution_rolling_straddle_variation_mn_hs_fn.py. A
late start (process comes up after ENTRY_TIME with no positions open) always fires the initial entry
immediately - there's no honor-checkpoints mode here.

Logging: goes to exec_rsv_cont.log and stdout. Lifecycle events (day start/skip, entries, exits,
rolls, stoploss fills, halts, day summary) are additionally pushed to Telegram via alert() below,
and any WARNING+ log record is pushed automatically as a safety net. A HEARTBEAT_INTERVAL "still
running" ping goes out with the current legs, day pnl so far, and the last real event/timestamp.
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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time as dtime, timedelta, timezone
from typing import NamedTuple

import requests
from dotenv import load_dotenv

import zerodha_ltp_client

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# ── Logging ──────────────────────────────────────────────────────────────────────────────────
LOG_FILE = os.path.join(os.path.dirname(__file__), 'exec_rsv_cont.log')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_TIMEOUT = 10

log = logging.getLogger('exec_rsv_cont')
log.setLevel(logging.INFO)
log.propagate = False
_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
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

# ── Strategy config (mirrors backtest_rolling_straddle_variation_continuous.py) ────────────────
ENTRY_TIME = dtime(9, 45)
EXIT_TIME = dtime(15, 13)  # 2-min live-trading safety buffer before the backtest's literal 15:15
CHECKPOINT_INTERVAL = timedelta(hours=1)
POLL_INTERVAL_SECONDS = 15  # was 30 - halved after the 31 Aug 2026 review found up to ~30s of pure
# detection lag (a checkpoint/stoploss-fill notice waiting for the next poll) compounding with
# fill-wait/retry delays into multi-minute-late exits (see notes.md) - 15s roughly halves that
# worst case while staying well above the API throttle floor below (DEFAULT_MIN_CALL_INTERVAL).
HEARTBEAT_INTERVAL = timedelta(minutes=30)

FIRST_OTM_STRIKES = 0  # 0 = ATM; n = n strikes OTM (CE up, PE down)
STOPLOSS_PCT = 0.25  # per-leg resting stoploss, flat across every underlying - matches the backtest exactly
PREMIUM_HIGH_STOPLOSS_ENABLED = False
PREMIUM_HIGH_LOOKBACK = timedelta(hours=1)
# DAILY_LOSS_LIMIT is now per-underlying (see CFG's daily_loss_limit below) rather than one flat
# value - NIFTY's is tighter than SENSEX's, unlike the backtest this was ported from.

OPTION_TYPES = ('CE', 'PE')
DAY_CODE_TO_WEEKDAY = {'m': 'Monday', 't': 'Tuesday', 'w': 'Wednesday', 'h': 'Thursday', 'f': 'Friday'}

# Per-underlying config - strike_interval/lots/aliceblue_exchange match ers.UNDERLYINGS in
# execution_rolling_straddle_tn.py; zerodha_* fields are this file's own (market data only).
# daily_loss_limit: points, unscaled by lot size - was one flat DAILY_LOSS_LIMIT = 100 for every
# underlying (matching the backtest's convention); now set per-underlying instead.
CFG = {
    'NIFTY': dict(
        strike_interval=50, lots=3, aliceblue_exchange='NFO',
        zerodha_options_exchange='NFO', zerodha_spot_instrument='NSE:NIFTY 50',
        daily_loss_limit=40,
    ),
    'SENSEX': dict(
        strike_interval=100, lots=3, aliceblue_exchange='BFO',
        zerodha_options_exchange='BFO', zerodha_spot_instrument='BSE:SENSEX',
        daily_loss_limit=100,
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
    return zerodha_ltp_client.get_ltp(token, rest_fetch=lambda: _zerodha_quote_ltp([key])[key], log=log)


# ── AliceBlue (REST, v3 open-api) - order placement only ────────────────────────────────────────
ALICEBLUE_TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'aliceblue_token.json')
ALICEBLUE_BASE_URL = 'https://a3.aliceblueonline.com/open-api/od/v1'
ALICEBLUE_CONTRACT_MASTER_URL = 'https://v2api.aliceblueonline.com/restpy/static/contract_master/V2/{exchange}'
LIMIT_OFFSET_PCT = 0.05  # limit price offset from LTP: below LTP for SELL, above LTP for BUY -
# matches execution_rolling_straddle_tn.py's rolling-straddle-family value. Entries and the resting
# SL still use this fixed offset unchanged - only EXITS chase (see EXIT_CHASE_* below), since
# getting OUT fast is the priority once we've already decided to close, not getting a clean price.
FILL_POLL_TIMEOUT = 10
FILL_POLL_INTERVAL = 1
TERMINAL_ORDER_STATUSES = {'complete', 'rejected', 'cancelled'}
_ALICEBLUE_EMPTY_RESULT_STATUSES = {'EC920'}

# Exit chase: once we've decided to close a leg, priority is getting OUT, not a clean fill price -
# see notes.md's 14:45->14:55:51 NIFTY CE walkthrough, where a single passive LIMIT order got
# stuck chasing a fast move for ~10 minutes. Give a resting exit LIMIT order EXIT_CHASE_WAIT_SECONDS
# to fill; if it hasn't, cancel it and replace at a wider (more aggressive) offset, repeating with
# the offset escalating by EXIT_CHASE_OFFSET_STEP each round, capped at EXIT_CHASE_MAX_OFFSET_PCT
# (by then effectively marketable, though still technically a LIMIT - the broker rejects MARKET
# orders on these NFO options, EC965).
EXIT_CHASE_WAIT_SECONDS = 2
EXIT_CHASE_POLL_INTERVAL = 0.5
EXIT_CHASE_OFFSET_STEP = 0.05
EXIT_CHASE_MAX_OFFSET_PCT = 0.50


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
    result = _aliceblue_post('/orders/placeorder', payload)
    return result[0] if isinstance(result, list) and len(result) == 1 else result


def _cancel_order(broker_order_id):
    return _aliceblue_post('/orders/cancel', {'brokerOrderId': broker_order_id})


def _modify_order(broker_order_id, instrument, quantity, order_type, price, trigger_price=None):
    """Reprices a resting order in place via AliceBlue's /orders/modifyorder, mirroring
    _place_order's payload shape plus the brokerOrderId identifying which order to change.
    CAUTION: mcx_option_buying.py and mcx_short_straddle_premium_stoploss.py elsewhere in this
    folder both document this same endpoint returning a live 400 Bad Request against the payload
    shape they sent, and fell back to cancel+place-new. This may hit the same wall - see
    _exit_chase_fill below, which tries this first but falls back to cancel+place-new the first
    time this raises, so a modify rejection can never leave a live exit stuck."""
    payload = {
        'brokerOrderId': broker_order_id,
        'exchange': instrument.exchange,
        'instrumentId': str(instrument.token),
        'quantity': quantity,
        'product': 'INTRADAY',
        'orderComplexity': 'REGULAR',
        'orderType': order_type,
        'validity': 'DAY',
        'price': str(price),
        'slTriggerPrice': str(trigger_price) if trigger_price is not None else '',
    }
    return _aliceblue_post('/orders/modifyorder', payload)


def _wait_for_fill_price(broker_order_id):
    deadline = time_module.time() + FILL_POLL_TIMEOUT
    while time_module.time() < deadline:
        for o in _order_book():
            if o.get('brokerOrderId') != broker_order_id:
                continue
            status = str(o.get('orderStatus', '')).lower()
            if status == 'rejected':
                raise RuntimeError(f'order {broker_order_id} rejected: {o.get("rejectionReason")}')
            if status == 'complete':
                return float(o.get('averageTradedPrice') or 0)
        time_module.sleep(FILL_POLL_INTERVAL)
    raise TimeoutError(f'order {broker_order_id} not filled within {FILL_POLL_TIMEOUT}s')


def _poll_order_status(broker_order_id, deadline):
    """One EXIT_CHASE-style poll pass: checks the order book (at most once every
    EXIT_CHASE_POLL_INTERVAL) until `deadline` (a time_module.time() value) for `broker_order_id`
    to go terminal. Returns the fill price on completion, raises on rejection, or returns None if
    `deadline` passed with the order still resting (caller decides whether to chase further)."""
    while time_module.time() < deadline:
        for o in _order_book():
            if o.get('brokerOrderId') != broker_order_id:
                continue
            status = str(o.get('orderStatus', '')).lower()
            if status == 'rejected':
                raise RuntimeError(f'order {broker_order_id} rejected: {o.get("rejectionReason")}')
            if status == 'complete':
                return float(o.get('averageTradedPrice') or 0)
        time_module.sleep(min(EXIT_CHASE_POLL_INTERVAL, max(0, deadline - time_module.time())))
    return None


def _exit_chase_fill(instrument, transaction_type, quantity, get_fresh_ltp, order_tag):
    """Place an exit LIMIT order and, if it hasn't filled within EXIT_CHASE_WAIT_SECONDS, REPRICE
    it wider in place via AliceBlue's modify-order endpoint (_modify_order) rather than
    cancel+place-new - modifying a resting order doesn't waste the round trip of tearing one down
    and re-establishing another. Repeats, offset escalating each round up to
    EXIT_CHASE_MAX_OFFSET_PCT, until it fills - see EXIT_CHASE_* constants' comment: once we've
    decided to exit, speed matters more than price.

    Falls back to cancel+place-new if modify itself fails: see _modify_order's docstring - two
    other scripts in this folder document AliceBlue's /orders/modifyorder returning a live 400
    against the payload shape they sent, so this may hit the same wall. The FIRST modify failure
    latches this exit onto cancel+place-new for every remaining attempt (never bounces back), so a
    broker-side modify rejection can never leave a live exit stuck retrying the same broken path.

    `transaction_type` is 'BUY' (closing a short, price moves UP each round) or 'SELL' (closing a
    long, price moves DOWN each round). `get_fresh_ltp` is a zero-arg callable - re-fetched every
    attempt, never reused stale."""
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
    modify_broken = False  # latched True on the first _modify_order failure - see docstring

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
            log.warning(
                f'{instrument.name} exit not filled within {EXIT_CHASE_WAIT_SECONDS}s '
                f'(attempt {attempt}, offset {offset_pct:.0%}) - modifying resting order to {price}',
            )
            try:
                _modify_order(order_no, instrument, quantity, 'LIMIT', price)
                continue  # same order_no - poll it again next loop
            except Exception as exc:
                modify_broken = True
                log.warning(
                    f'{instrument.name}: modify of order {order_no} failed ({exc}) - falling back '
                    f'to cancel+place-new for the rest of this exit',
                )

        log.warning(
            f'{instrument.name} exit not filled within {EXIT_CHASE_WAIT_SECONDS}s '
            f'(attempt {attempt}, offset {offset_pct:.0%}) - cancelling and re-pricing more aggressively',
        )
        try:
            _cancel_order(order_no)
        except Exception as exc:
            log.warning(f'{instrument.name}: cancel of unfilled exit order {order_no} failed ({exc}) - placing a fresh order anyway')
        order = _place_order(transaction_type, instrument, quantity, 'LIMIT', price=str(price), order_tag=order_tag)
        order_no = order.get('brokerOrderId')
        if not order_no:
            raise RuntimeError(f'{instrument.name} exit order rejected: {order}')


def get_open_legs(contracts_by_token):
    """token -> position dict, restricted to currently open (nonzero net qty) legs among this
    week's option contracts for the current underlying."""
    positions = _aliceblue_get('/positions')
    return {
        int(p['instrumentId']): p for p in positions
        if int(p['instrumentId']) in contracts_by_token and int(p.get('netQuantity', 0)) != 0
    }


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
    true ATM straddle (for the premium-rise/premium-stoploss checks), today's desired first-OTM
    strikes (for entries/rolls), and whatever strike each currently-open leg actually sits at (which
    may have drifted off first-OTM already, but still needs a live quote to monitor/close)."""
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
    for strike, opt in strike_types_needed:
        try:
            row = _zerodha_option_row(zerodha_options, strike, opt)
        except KeyError:
            log.warning(f'{opt} {strike}: no Zerodha instrument found - skipping this quote')
            continue
        key = f"{cfg['zerodha_options_exchange']}:{row['tradingsymbol']}"
        key_to_strike_type[key] = (strike, opt)
        token_to_key[int(row['instrument_token'])] = key

    # Redis first (shared ticker service feed - see zerodha_ltp_client.py), one batched REST call
    # via _zerodha_quote_ltp for whatever's missing/stale - same batched-fallback efficiency this
    # had before, just Redis-fast for the common case instead of a REST round trip every poll.
    zerodha_ltp_client.register_subscriptions(list(token_to_key))
    token_to_price = zerodha_ltp_client.get_ltps(
        token_to_key, rest_fetch_batch=lambda keys: _resilient_call(_zerodha_quote_ltp, keys), log=log,
    ) if token_to_key else {}
    price = {key_to_strike_type[token_to_key[token]]: p for token, p in token_to_price.items()}

    return dict(
        spot=spot, atm=atm, desired_strike=desired_strike,
        contracts_by_token=contracts_by_token, contracts_by_strike_type=contracts_by_strike_type,
        price=price,
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
    return {opt: None for opt in OPTION_TYPES}  # None, or {'instrument','strike','entry_price','quantity'}


def _short_leg_with_stoploss(instrument, quantity, ltp):
    """SELL to open, then a resting BUY SL (stop-loss LIMIT) at entry_price * (1 + STOPLOSS_PCT) -
    flat percentage across every underlying, matching the backtest exactly (see module docstring
    on why there's no SENSEX points-based override here, unlike
    execution_rolling_straddle_variation_mn_hs_fn.py). Returns the fill price."""
    entry_price = _round_to_tick(ltp * (1 - LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}SELL {quantity} x {instrument.name} LIMIT @ {entry_price} (ltp {ltp})'
    log.info(tag)
    if DRY_RUN:
        return entry_price

    entry = _place_order('SELL', instrument, quantity, 'LIMIT', price=str(entry_price), order_tag='rsv_cont_entry')
    order_no = entry.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} entry order rejected: {entry}')

    entry_price = _wait_for_fill_price(order_no)
    # Rounded to the nearest integer, not just a tick - the exchange rejects SL trigger
    # prices for these contracts with "STOP PRICE IS NOT REASONABLE" unless they're whole
    # rupees.
    trigger_price = round(entry_price * (1 + STOPLOSS_PCT))
    # Nearest integer, not a tick - same "STOP PRICE IS NOT REASONABLE" rejection applies to the
    # SL order's limit price as well as its trigger.
    sl_limit_price = round(trigger_price * (1 + LIMIT_OFFSET_PCT))
    log.info(f'{instrument.name} entered @ {entry_price}, SL trigger {trigger_price} limit {sl_limit_price}')

    sl = _place_order('BUY', instrument, quantity, 'SL', price=str(sl_limit_price), trigger_price=trigger_price, order_tag='rsv_cont_sl')
    if not sl.get('brokerOrderId'):
        raise RuntimeError(f'{instrument.name} filled at {entry_price} but SL order rejected: {sl}')
    return entry_price


def _run_legs_in_parallel(tasks):
    """Run one no-arg callable per leg (keyed by 'CE'/'PE') concurrently instead of one after
    another. Every broker call in this file is I/O (REST + up to FILL_POLL_TIMEOUT of polling for
    a fill), so entering/closing two legs serially was pure wasted latency - up to a full
    FILL_POLL_TIMEOUT per extra leg on every multi-leg entry/exit, which is exactly what turned
    the 31 Aug 2026 10:45 checkpoint's close into a 10:47:08 fill (see notes.md). Every leg is
    always given the chance to run even if another one raises; if any did, the first exception is
    re-raised afterwards (matching the existing single-leg behaviour, so the caller's normal
    retry/alert path still fires) - but only after every other leg's order has already gone in,
    never skipped because a sibling leg failed first."""
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


def _enter_leg(state, opt, instrument, ltp, strike, cfg):
    quantity = instrument.lot_size * cfg['lots']
    entry_price = _short_leg_with_stoploss(instrument, quantity, ltp)
    with _state_lock:  # see _state_lock's comment - races the SL-watch thread's own clear
        state[opt] = dict(instrument=instrument, strike=strike, entry_price=entry_price, quantity=quantity)
    alert(f'ENTER {opt} {instrument.name} x{quantity} @ ~{entry_price} (SL {STOPLOSS_PCT:.0%})')


def _enter_legs_parallel(state, desired, cfg, only_missing=False):
    """Enter every (or, with only_missing, every currently-flat) leg in `desired` at once rather
    than one after another - see _run_legs_in_parallel."""
    tasks = {
        opt: (lambda opt=opt, instrument=instrument, ltp=ltp, strike=strike: _enter_leg(state, opt, instrument, ltp, strike, cfg))
        for opt, (instrument, ltp, strike) in desired.items()
        if not only_missing or state[opt] is None
    }
    _run_legs_in_parallel(tasks)


def _close_leg(state, opt, market, cfg, reason):
    leg = state[opt]
    if leg is None:
        return None
    exit_ltp = market['price'].get((leg['strike'], opt))
    if exit_ltp is None:
        log.warning(f'{opt} {leg["strike"]}: no live quote to close against, leaving position open')
        return None
    fill_price = _close_leg_order(leg['instrument'], leg['quantity'], fallback_ltp=exit_ltp)
    # prefer the ACTUAL fill price for pnl/alert once we have one - the exit chase (see
    # _close_leg_order) can land well away from `exit_ltp`, the price at the moment we merely
    # decided to close, so reporting off that decision-time snapshot would misstate the real pnl.
    realized_exit = fill_price if fill_price is not None else exit_ltp
    pnl = leg['entry_price'] - realized_exit
    alert(f'EXIT {opt} {leg["instrument"].name} @ ~{realized_exit} (entry ~{leg["entry_price"]}) pnl~{pnl:+.2f} reason={reason}')
    state[opt] = None
    return pnl


SL_TRIGGER_BELOW_LTP_PCT = 0.01  # how far below current LTP to drop a resting BUY SL's trigger -
# guarantees the "price >= trigger" condition is already true (not marginal/racy against the next
# tick), so the broker releases it immediately instead of continuing to wait for price to reach it
SL_RELEASE_PRICE_MIN_ABOVE_LTP_PCT = 0.01  # the released order's own limit price floor, as a % above LTP
SL_RELEASE_PRICE_MIN_POINTS_ABOVE_LTP = 10  # ...or this many points above LTP, whichever is higher


def _convert_resting_sl_to_market_exit(instrument, quantity, get_fresh_ltp):
    """Every short leg here always has a resting BUY-side SL order protecting it from the moment
    it's entered (_short_leg_with_stoploss) - so closing it doesn't need to cancel that order and
    place a fresh one at all. Instead, MODIFY the SAME resting SL in place: drop its trigger price
    to just below the current LTP (the "price >= trigger" condition is then already satisfied, so
    the broker releases it immediately rather than waiting for price to actually reach it) and set
    its release price aggressively - max(1% above LTP, LTP+10 points) - so the released BUY LIMIT
    is priced well through the touch and fills essentially instantly. One modify call, and the leg
    is never left unprotected in between (no cancel-then-place gap) - see EXIT_CHASE_* elsewhere
    for the cancel+place-new fallback this is preferred over.

    Returns the fill price if this worked, or None if there's no resting order to convert, the
    modify itself failed (see _modify_order's docstring - a documented risk with this broker), or
    it didn't fill within EXIT_CHASE_WAIT_SECONDS anyway - in every None case the caller falls back
    to the ordinary cancel+chase path in _close_leg_order below."""
    resting_order_id = None
    for o in _order_book():
        if str(o.get('instrumentId')) == str(instrument.token) and str(o.get('orderStatus', '')).lower() not in TERMINAL_ORDER_STATUSES:
            resting_order_id = o.get('brokerOrderId')
            break
    if resting_order_id is None:
        return None

    ltp = get_fresh_ltp()
    if ltp is None:
        return None
    # Rounded to the nearest integer, not just a tick - the exchange rejects SL trigger
    # prices for these contracts with "STOP PRICE IS NOT REASONABLE" unless they're whole
    # rupees.
    trigger_price = round(ltp * (1 - SL_TRIGGER_BELOW_LTP_PCT))
    # Nearest integer, not a tick - same rejection applies to the SL order's limit price.
    release_price = round(
        max(ltp * (1 + SL_RELEASE_PRICE_MIN_ABOVE_LTP_PCT), ltp + SL_RELEASE_PRICE_MIN_POINTS_ABOVE_LTP)
    )

    try:
        _modify_order(resting_order_id, instrument, quantity, 'SL', release_price, trigger_price=trigger_price)
    except Exception as exc:
        log.warning(f'{instrument.name}: modify-to-market of resting SL order {resting_order_id} failed ({exc}) - falling back to cancel+place-new')
        return None

    return _poll_order_status(resting_order_id, time_module.time() + EXIT_CHASE_WAIT_SECONDS)


def _close_leg_order(instrument, quantity, fallback_ltp=None):
    """Square off by converting the leg's existing resting SL order into an immediate market-ish
    exit (_convert_resting_sl_to_market_exit above) - falls back to cancelling whatever's resting
    and chasing a fresh LIMIT order (_exit_chase_fill) only if that didn't work (no resting order
    found, the modify failed, or it didn't fill in time). Returns the actual average fill price
    (None in DRY_RUN, where nothing is placed). Uses a fresh quote at close time rather than the
    one passed in by the caller - avoids placing against a stale price if a retry happened - but
    falls back to the caller's quote if the fresh fetch comes up empty. The broker rejects MARKET
    orders on these NFO options (EC965), so we never place one - EXIT_CHASE_MAX_OFFSET_PCT is as
    aggressive as the fallback path ever gets. If no LTP is available at all we leave the SL order
    in place rather than touch it and be left with an unprotected naked leg."""
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}BUY (square off) {quantity} x {instrument.name}'
    log.info(tag)
    if DRY_RUN:
        return None

    def _fresh_ltp():
        key = f'{instrument.exchange}:{instrument.name}'
        try:
            ltp = _zerodha_quote_ltp([key]).get(key)
        except Exception:
            ltp = None
        return ltp if ltp is not None else fallback_ltp

    if _fresh_ltp() is None:
        raise RuntimeError(f'{instrument.name}: no LTP available to square off, leaving SL in place')

    fill_price = _convert_resting_sl_to_market_exit(instrument, quantity, _fresh_ltp)
    if fill_price is not None:
        return fill_price

    for o in _order_book():
        if str(o.get('instrumentId')) == str(instrument.token) and str(o.get('orderStatus', '')).lower() not in TERMINAL_ORDER_STATUSES:
            _cancel_order(o['brokerOrderId'])

    return _exit_chase_fill(instrument, 'BUY', quantity, _fresh_ltp, order_tag='rsv_cont_exit')


def _close_open_legs(state, market, cfg, reason):
    tasks = {
        opt: (lambda opt=opt: _close_leg(state, opt, market, cfg, reason))
        for opt in OPTION_TYPES if state[opt] is not None
    }
    results = _run_legs_in_parallel(tasks)
    return sum(pnl for pnl in results.values() if pnl is not None)


_state_lock = threading.Lock()  # guards state[opt] wherever a concurrent thread could race a
# read-then-write against it - specifically _enter_leg's write vs. _sync_stopped_out_legs' below,
# now that the latter runs on its own dedicated thread (see SL_WATCH_INTERVAL_SECONDS) rather than
# only inline in the main loop. A plain dict read elsewhere doesn't need it (CPython's GIL already
# makes a single read/write atomic; the only unsafe pattern is read-then-write across threads).


def _sync_stopped_out_legs(state, market):
    """Broker-side truth: a leg we believe is open but whose position has vanished means its
    resting stoploss filled. This IS the live "continuous" stoploss check - a real resting order
    fires the instant price touches it; polling here just notices promptly. Runs on its own
    SL_WATCH_INTERVAL_SECONDS-cadence background thread (see _sl_watch_loop) as well as inline
    each main-loop cycle - noticing a fill promptly matters on its own, independent of the main
    loop's heavier market-data-fetching cadence (see notes.md)."""
    open_tokens = set(_resilient_call(get_open_legs, market['contracts_by_token']))
    for opt in OPTION_TYPES:
        with _state_lock:
            leg = state[opt]
            stopped = leg is not None and leg['instrument'].token not in open_tokens
            if stopped:
                state[opt] = None
        if stopped:
            alert(f'STOPLOSS FILLED: {opt} {leg["instrument"].name} (entry ~{leg["entry_price"]}) - flat until next checkpoint')


SL_WATCH_INTERVAL_SECONDS = 1  # dedicated cadence for noticing a resting stoploss fill -
# independent of POLL_INTERVAL_SECONDS (the main loop's cadence for market-data/checkpoint/
# heartbeat work, which is naturally heavier and slower) - see notes.md.


def _sl_watch_loop(state, contracts_box, stop_event):
    """Runs for the whole trading day on its own thread: checks every SL_WATCH_INTERVAL_SECONDS
    whether a resting stoploss has filled, independent of the main loop's own cadence.
    `contracts_box` is a 1-item list holding the most recently known contracts_by_token map, kept
    updated by the main loop each time it refreshes market data - this thread never fetches full
    market data itself (no quotes needed here), only AliceBlue's positions endpoint."""
    while not stop_event.is_set():
        try:
            contracts_by_token = contracts_box[0]
            if contracts_by_token is not None:
                _sync_stopped_out_legs(state, {'contracts_by_token': contracts_by_token})
        except Exception as exc:
            log.warning(f'SL watch thread: check failed ({exc})', extra={'no_telegram': True})
        stop_event.wait(SL_WATCH_INTERVAL_SECONDS)


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
            day['realized_pnl'] += _close_open_legs(state, market, cfg, 'ATM_PREMIUM_RISE')
        else:
            log.info('ATM premium rose vs previous checkpoint - no legs open, no new trade this hour')
        return

    if day['suppress_reentry']:
        log.info('minute-level stoploss fired earlier this hour - staying flat this checkpoint')
        day['suppress_reentry'] = False
        return

    desired = _desired_legs(market, cfg)
    drifted = any(
        state[opt] is not None and opt in desired and state[opt]['strike'] != desired[opt][2]
        for opt in OPTION_TYPES
    )
    if drifted:
        alert('first-OTM strike has moved - rolling both legs')
        day['realized_pnl'] += _close_open_legs(state, market, cfg, 'ROLL_OTM_DRIFT')
        _enter_legs_parallel(state, desired, cfg)
        return

    for opt in desired:
        if state[opt] is None:
            continue
        log.info(f'{opt} still open at {state[opt]["strike"]}, leaving as is')
    _enter_legs_parallel(state, desired, cfg, only_missing=True)


# ── Minute-level checks (every poll) ─────────────────────────────────────────────────────────
def run_minute_checks(state, market, cfg, day, now):
    current_premium = _atm_premium(market)
    any_open = state['CE'] is not None or state['PE'] is not None

    if PREMIUM_HIGH_STOPLOSS_ENABLED and current_premium is not None:
        window_start = now - PREMIUM_HIGH_LOOKBACK
        day['premium_history'] = [(t, p) for t, p in day['premium_history'] if t >= window_start]
        prior_high = max((p for _, p in day['premium_history']), default=None)
        if prior_high is not None and current_premium > prior_high and any_open:
            alert(f'ATM premium {current_premium} above its {PREMIUM_HIGH_LOOKBACK} high {prior_high} - closing legs')
            day['realized_pnl'] += _close_open_legs(state, market, cfg, 'ATM_PREMIUM_2H_HIGH')
            day['suppress_reentry'] = True
        day['premium_history'].append((now, current_premium))

    unrealized = 0.0
    for opt in OPTION_TYPES:
        leg = state[opt]
        if leg is None:
            continue
        current_ltp = market['price'].get((leg['strike'], opt))
        if current_ltp is None:
            continue
        unrealized += leg['entry_price'] - current_ltp

    daily_loss_limit = cfg['daily_loss_limit']
    if day['realized_pnl'] + unrealized <= -daily_loss_limit:
        alert(
            f'DAILY LOSS LIMIT hit: realized {day["realized_pnl"]:.2f} + unrealized {unrealized:.2f} '
            f'<= -{daily_loss_limit} - halting for the day, squaring off',
            level=logging.CRITICAL,
        )
        day['realized_pnl'] += _close_open_legs(state, market, cfg, 'DAILY_LOSS_LIMIT')
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
    checkpoint_interval = timedelta(hours=1) if symbol == 'NIFTY' and today_name == 'Friday' else CHECKPOINT_INTERVAL
    if checkpoint_interval != CHECKPOINT_INTERVAL:
        log.info(f'{symbol} {today_name}: overriding checkpoint interval {CHECKPOINT_INTERVAL} -> {checkpoint_interval}')

    alert(f'Rolling straddle (variation, continuous) starting for {symbol} - {today_name} {datetime.now():%Y-%m-%d}')

    state = _new_state()
    day = dict(
        realized_pnl=0.0, halted=False, suppress_reentry=False,
        prev_checkpoint_premium=None, premium_history=[],
    )

    _sleep_until(ENTRY_TIME, 'entry time')
    entry_time = datetime.now()

    scheduled_entry = datetime.combine(entry_time.date(), ENTRY_TIME)
    next_checkpoint = scheduled_entry + checkpoint_interval
    while next_checkpoint <= entry_time:
        next_checkpoint += checkpoint_interval
    next_heartbeat = entry_time + HEARTBEAT_INTERVAL

    market = _fetch_market_until_success(symbol, cfg, state)

    contracts_box = [market['contracts_by_token']]  # kept updated below every time market data
    # refreshes - see _sl_watch_loop's docstring on why this thread doesn't fetch its own.
    sl_watch_stop = threading.Event()
    sl_watch_thread = threading.Thread(
        target=_sl_watch_loop, args=(state, contracts_box, sl_watch_stop),
        daemon=True, name=f'sl-watch-{symbol}',
    )
    sl_watch_thread.start()

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
                state[opt] = dict(instrument=instrument, strike=strike_opt[0], entry_price=entry_price, quantity=quantity)

        adopted_premium = sum(leg['entry_price'] for leg in state.values() if leg is not None) or None
        day['prev_checkpoint_premium'] = adopted_premium
        if adopted_premium is not None:
            day['premium_history'] = [(datetime.now(), adopted_premium)]
            alert(f'Adopted legs - reconstructed combined entry ~{adopted_premium:.2f}, used as the previous-checkpoint premium')
    else:
        log.info('No open positions - entering initial legs')
        entry_premium = _atm_premium(market)
        day['prev_checkpoint_premium'] = entry_premium
        if entry_premium is not None:
            day['premium_history'] = [(datetime.now(), entry_premium)]
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
                contracts_box[0] = market['contracts_by_token']
            _sync_stopped_out_legs(state, market)
            run_minute_checks(state, market, cfg, day, now)

            if not day['halted'] and now >= next_checkpoint:
                run_checkpoint(state, market, cfg, day, symbol)
                next_checkpoint += checkpoint_interval

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
            day['realized_pnl'] += _close_open_legs(state, final_market, cfg, 'EOD')
            break
        except Exception as exc:
            if attempt == RETRY_MAX_ATTEMPTS - 1:
                sl_watch_stop.set()
                alert(f'{symbol}: final square-off failed after {RETRY_MAX_ATTEMPTS} attempts - positions may still be OPEN, check manually: {exc}', level=logging.CRITICAL)
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            log.warning(f'final square-off failed ({exc}) - retrying in {delay:.0f}s (attempt {attempt + 1}/{RETRY_MAX_ATTEMPTS})')
            time_module.sleep(delay)

    sl_watch_stop.set()
    alert(f'Rolling straddle (variation, continuous) done for {symbol} - realized pnl {day["realized_pnl"]:+.2f} points')


if __name__ == '__main__':
    SYMBOL = sys.argv[1].upper() if len(sys.argv) > 1 and sys.argv[1] else 'NIFTY'
    if SYMBOL not in CFG:
        raise ValueError(f'unknown symbol {SYMBOL!r} - use one of {sorted(CFG)}')
    TRADE_WEEKDAYS = _parse_trade_weekdays(sys.argv[2] if len(sys.argv) > 2 else None)
    run_day(SYMBOL, TRADE_WEEKDAYS)
