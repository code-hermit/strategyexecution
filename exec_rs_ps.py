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
    (optional, off by default) HEDGE_ON_COMBINED_STOPLOSS_ENABLED: every time the combined
    stoploss fires, buy the true ATM straddle (CE+PE) and hold it HEDGE_HOLD_MINUTES before
    selling - runs entirely independently of the short strategy (own pnl bucket, never touches
    day['realized_pnl'] or DAILY_LOSS_LIMIT, can stack if the combined stoploss fires again later
    in the day).
  - (optional, off by default) close both legs if the ATM premium is now above its own highest
    reading over the trailing PREMIUM_HIGH_LOOKBACK window (2 hours).
  - (optional, off by default) close both legs if the ATM premium has risen PREMIUM_STOPLOSS_PCT
    above its value at today's entry.
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

EXIT_TIME is 15:13, not the backtest's literal 15:15 - the same 2-minute live-trading safety buffer
used throughout this folder (see execution_rolling_straddle_variation_mn_hs_fn.py, exec_rsv_cont.py).

Market data (spot LTP, option LTPs) comes from Zerodha's Kite Connect API. Order placement goes
through AliceBlue's REST API. Fully self-contained, like nifty_option_buying_twhf.py /
exec_rsv_cont.py - deliberately does NOT import execution_rolling_straddle_tn.py (Dhan-backed);
Dhan is never touched, imported, or authenticated by this file at all.

Trades a single underlying (command-line arg, default NIFTY) on TRADE_WEEKDAYS (command-line
weekday codes, default every weekday - matching the backtest's own default).

No local state file: every startup reconciles against the broker instead (AliceBlue positions +
order book). On startup with legs already open (mid-day restart, or placed manually), each leg's
actual entry price is reconstructed from the AliceBlue order book's completed SELL fills where
possible, falling back to live LTP (logged clearly as an approximation) only if that reconstruction
fails - the backstop SL for an adopted leg is NOT re-placed at that reconstructed price (a fresh
one already rests at the broker from whenever the leg was actually entered; placing a second one
would just be a duplicate). A late start (process comes up after ENTRY_TIME with nothing open)
always fires the initial entry immediately.

Logging: goes to exec_rs_ps.log and stdout. Lifecycle events are additionally pushed to Telegram via
alert() below, and any WARNING+ log record is pushed automatically as a safety net. A
HEARTBEAT_INTERVAL "still running" ping goes out with the current legs, day pnl so far, and the last
real event/timestamp. Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env; if either is
missing, Telegram alerts (including heartbeats) are skipped (logged as a one-time warning) but
trading proceeds normally.
"""

import csv
import io
import json
import logging
import os
import signal
import sys
import time as time_module
from datetime import datetime, time as dtime, timedelta, timezone
from typing import NamedTuple

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# ── Logging ──────────────────────────────────────────────────────────────────────────────────
LOG_FILE = os.path.join(os.path.dirname(__file__), 'exec_rs_ps.log')
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
HEDGE_ON_COMBINED_STOPLOSS_ENABLED = False  # optional - set True to buy the ATM straddle whenever the combined stoploss fires
HEDGE_HOLD_MINUTES = 10  # how long to hold that ATM straddle buy before exiting it
PREMIUM_HIGH_STOPLOSS_ENABLED = False
PREMIUM_HIGH_LOOKBACK = timedelta(hours=2)
PREMIUM_STOPLOSS_ENABLED = False
PREMIUM_STOPLOSS_PCT = 0.25
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


def _zerodha_tradingsymbol(zerodha_options, strike, option_type):
    for row in zerodha_options:
        if int(float(row['strike'])) == strike and row['instrument_type'] == option_type:
            return row['tradingsymbol']
    raise KeyError(f'no Zerodha instrument found for strike={strike} type={option_type}')


def _zerodha_quote_ltp(instrument_keys):
    """instrument_keys like ['NSE:NIFTY 50', 'NFO:NIFTY25813950CE']. Returns key -> last_price."""
    resp = requests.get(
        f'{ZERODHA_BASE_URL}/quote/ltp', headers=_zerodha_headers(ZERODHA_ACCESS_TOKEN),
        params=[('i', k) for k in instrument_keys], timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()['data']
    return {k: float(v['last_price']) for k, v in data.items()}


def get_spot_ltp(cfg):
    key = cfg['zerodha_spot_instrument']
    return _zerodha_quote_ltp([key])[key]


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
    true ATM straddle (for the premium-rise/premium-stoploss checks AND the optional hedge buy,
    which always trades the literal ATM regardless of FIRST_OTM_STRIKES), today's desired
    first-OTM strikes (for entries/rolls), and whatever strike each currently-open leg actually
    sits at (which may have drifted off first-OTM already, but still needs a live quote to
    monitor/close)."""
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
    for strike, opt in strike_types_needed:
        try:
            symbol_name = _zerodha_tradingsymbol(zerodha_options, strike, opt)
        except KeyError:
            log.warning(f'{opt} {strike}: no Zerodha instrument found - skipping this quote')
            continue
        key_to_strike_type[f"{cfg['zerodha_options_exchange']}:{symbol_name}"] = (strike, opt)

    quotes = _resilient_call(_zerodha_quote_ltp, list(key_to_strike_type)) if key_to_strike_type else {}
    price = {key_to_strike_type[k]: v for k, v in quotes.items() if k in key_to_strike_type}

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


def _atm_legs(market):
    """{'CE': (instrument, ltp, strike), 'PE': (...)} for the literal ATM strike - used only by the
    optional hedge buy, which trades true ATM regardless of FIRST_OTM_STRIKES. Only includes
    whichever side(s) actually have a contract/quote."""
    legs = {}
    for opt in OPTION_TYPES:
        strike = market['atm']
        contract = market['contracts_by_strike_type'].get((strike, opt))
        ltp = market['price'].get((strike, opt))
        if contract is None or ltp is None:
            continue
        legs[opt] = (_to_instrument(contract), ltp, strike)
    return legs


# ── Per-leg state ─────────────────────────────────────────────────────────────────────────────
def _new_state():
    return {opt: None for opt in OPTION_TYPES}  # None, or {'instrument','strike','entry_price','quantity'}


def _short_leg_with_backstop_sl(instrument, quantity, ltp, order_tag):
    """SELL to open, then a resting BUY SL (stop-loss LIMIT) backstop LEG_STOPLOSS_POINTS above
    the actual fill price - see module docstring. This strategy's real risk control is the
    COMBINED stoploss (checked in software every poll); this resting order is purely a worst-case
    backstop for when that check can't run at all. Returns the fill price (or the priced LIMIT in
    DRY_RUN, as a stand-in for pnl bookkeeping)."""
    entry_price = _round_to_tick(ltp * (1 - LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}SELL {quantity} x {instrument.name} LIMIT @ {entry_price} (ltp {ltp})'
    log.info(tag)
    if DRY_RUN:
        return entry_price

    entry = _place_order('SELL', instrument, quantity, 'LIMIT', price=str(entry_price), order_tag=order_tag)
    order_no = entry.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} entry order rejected: {entry}')

    entry_price = _wait_for_fill_price(order_no)
    trigger_price = round(entry_price + LEG_STOPLOSS_POINTS, 1)
    sl_limit_price = _round_to_tick(trigger_price * (1 + LIMIT_OFFSET_PCT), instrument.tick_size)
    log.info(f'{instrument.name} entered @ {entry_price}, backstop SL trigger {trigger_price} limit {sl_limit_price}')

    sl = _place_order('BUY', instrument, quantity, 'SL', price=str(sl_limit_price), trigger_price=trigger_price, order_tag='rs_ps_backstop_sl')
    if not sl.get('brokerOrderId'):
        raise RuntimeError(f'{instrument.name} filled at {entry_price} but backstop SL order rejected: {sl}')
    return entry_price


def _enter_leg(state, opt, instrument, ltp, strike, cfg):
    quantity = instrument.lot_size * cfg['lots']
    entry_price = _short_leg_with_backstop_sl(instrument, quantity, ltp, 'rs_ps_entry')
    state[opt] = dict(instrument=instrument, strike=strike, entry_price=entry_price, quantity=quantity)
    alert(f'ENTER {opt} {instrument.name} x{quantity} @ ~{entry_price} (backstop SL +{LEG_STOPLOSS_POINTS}pts)')


def _close_leg_order(instrument, quantity, order_tag):
    """Cancel this leg's resting backstop SL (if still resting) then BUY at a LIMIT price through
    a fresh LTP to square off. Used for every close path: checkpoint roll/premium-rise, combined
    stoploss, daily loss limit, EOD, and hedge exits (which never have a resting order to cancel -
    the loop below is just a no-op for those)."""
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}BUY (square off) {quantity} x {instrument.name}'
    log.info(tag)
    if DRY_RUN:
        return

    for o in _order_book():
        if str(o.get('instrumentId')) == str(instrument.token) and str(o.get('orderStatus', '')).lower() not in TERMINAL_ORDER_STATUSES:
            _cancel_order(o['brokerOrderId'])

    key = f'{instrument.exchange}:{instrument.name}'
    try:
        ltp = _zerodha_quote_ltp([key]).get(key)
    except Exception:
        ltp = None
    if ltp is None:
        exit_order = _place_order('BUY', instrument, quantity, 'MARKET', order_tag=order_tag)
    else:
        exit_price = _round_to_tick(ltp * (1 + LIMIT_OFFSET_PCT), instrument.tick_size)
        exit_order = _place_order('BUY', instrument, quantity, 'LIMIT', price=str(exit_price), order_tag=order_tag)
    order_no = exit_order.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} exit order rejected: {exit_order}')
    _wait_for_fill_price(order_no)


def _close_leg(state, opt, market, cfg, reason):
    leg = state[opt]
    if leg is None:
        return None
    exit_ltp = market['price'].get((leg['strike'], opt))
    if exit_ltp is None:
        log.warning(f'{opt} {leg["strike"]}: no live quote to close against, leaving position open')
        return None
    _close_leg_order(leg['instrument'], leg['quantity'], 'rs_ps_exit')
    pnl = leg['entry_price'] - exit_ltp
    alert(f'EXIT {opt} {leg["instrument"].name} @ ~{exit_ltp} (entry ~{leg["entry_price"]}) pnl~{pnl:+.2f} reason={reason}')
    state[opt] = None
    return pnl


def _close_open_legs(state, market, cfg, reason):
    return sum(pnl for opt in OPTION_TYPES if (pnl := _close_leg(state, opt, market, cfg, reason)) is not None)


def _sync_stopped_out_legs(state, market):
    """Broker-side truth: a leg we believe is open but whose position has vanished means its
    resting backstop SL filled (see module docstring - LEG_STOPLOSS_POINTS, a worst-case-only
    dead-man's-switch; the combined stoploss should always have closed things well before this).
    Alerted as a WARNING since it firing means the software combined check failed to catch it in
    time - worth knowing about even if the pnl outcome itself is fine. Only the stopped-out leg is
    cleared; the other leg (if any) is left exactly as is - each leg's backstop is independent, not
    a straddle-level stop."""
    open_tokens = set(_resilient_call(get_open_legs, market['contracts_by_token']))
    for opt in OPTION_TYPES:
        leg = state[opt]
        if leg is not None and leg['instrument'].token not in open_tokens:
            alert(
                f'BACKSTOP SL FILLED: {opt} {leg["instrument"].name} (entry ~{leg["entry_price"]}) - '
                f'combined stoploss did not catch this in time; flat until next checkpoint',
                level=logging.WARNING,
            )
            state[opt] = None


# ── Hedge (optional): buy the ATM straddle whenever the combined stoploss fires ─────────────────
def _open_hedge(market, cfg, day, now):
    """Buy the literal ATM straddle (CE+PE) - runs entirely independent of the short strategy: its
    own trades, own pnl bucket (day['hedge_pnl'], purely for logging/heartbeat), never touches
    day['realized_pnl'] or DAILY_LOSS_LIMIT. Can stack - a new hedge is opened every time the
    combined stoploss fires, even while an earlier hedge is still open."""
    legs = {}
    for opt, (instrument, ltp, strike) in _atm_legs(market).items():
        quantity = instrument.lot_size * cfg['lots']
        entry_price = _round_to_tick(ltp * (1 + LIMIT_OFFSET_PCT), instrument.tick_size)
        tag = f'{"[DRY RUN] " if DRY_RUN else ""}BUY (hedge) {quantity} x {instrument.name} LIMIT @ {entry_price} (ltp {ltp})'
        log.info(tag)
        if not DRY_RUN:
            entry = _place_order('BUY', instrument, quantity, 'LIMIT', price=str(entry_price), order_tag='rs_ps_hedge_entry')
            order_no = entry.get('brokerOrderId')
            if not order_no:
                log.warning(f'{instrument.name} hedge entry order rejected: {entry} - skipping this leg of the hedge')
                continue
            entry_price = _wait_for_fill_price(order_no)
        legs[opt] = dict(instrument=instrument, strike=strike, entry_price=entry_price, quantity=quantity)

    if not legs:
        log.warning('hedge buy: no ATM quote/contract available for either leg - skipping this hedge')
        return
    day['active_hedges'].append({'exit_time': now + timedelta(minutes=HEDGE_HOLD_MINUTES), 'legs': legs})
    alert(f"HEDGE BUY ATM straddle {market['atm']} - held until {(now + timedelta(minutes=HEDGE_HOLD_MINUTES)):%H:%M:%S}")


def _run_hedge_checks(market, cfg, day, now, is_eod):
    """Close whichever active hedges are due (time-based exit reached, or day is ending)."""
    if not day['active_hedges']:
        return
    still_active = []
    for hedge in day['active_hedges']:
        if now >= hedge['exit_time'] or is_eod:
            pnl = 0.0
            remaining_legs = {}
            for opt, leg in hedge['legs'].items():
                exit_ltp = market['price'].get((leg['strike'], opt))
                if exit_ltp is None:
                    remaining_legs[opt] = leg  # try again next tick
                    continue
                _close_leg_order(leg['instrument'], leg['quantity'], 'rs_ps_hedge_exit')
                pnl += exit_ltp - leg['entry_price']  # long: profit when exit > entry
            hedge['legs'] = remaining_legs
            day['hedge_pnl'] += pnl
            if pnl:
                alert(f"HEDGE EXIT ATM straddle - pnl~{pnl:+.2f} reason={'HEDGE_EOD' if is_eod else 'HEDGE_TIME_EXIT'}")
            if hedge['legs']:
                still_active.append(hedge)  # a leg's quote was missing - keep trying
        else:
            still_active.append(hedge)
    day['active_hedges'] = still_active


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
        day['realized_pnl'] += _close_open_legs(state, market, cfg, 'ROLL_OTM_DRIFT')
        for opt, (instrument, ltp, strike) in desired.items():
            _enter_leg(state, opt, instrument, ltp, strike, cfg)
        return

    for opt, (instrument, ltp, strike) in desired.items():
        if state[opt] is None:
            _enter_leg(state, opt, instrument, ltp, strike, cfg)
        else:
            log.info(f'{opt} still open at {state[opt]["strike"]}, leaving as is')


# ── Minute-level checks (every poll, unconditionally - not gated on a position being open) ──────
def run_minute_checks(state, market, cfg, day, now):
    current_premium = _atm_premium(market)
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
                day['realized_pnl'] += _close_open_legs(state, market, cfg, 'COMBINED_STOPLOSS')
                # deliberately no day['suppress_reentry'] = True here - matches the backtest, which
                # doesn't set it for the combined stoploss either (only the optional premium checks
                # below do). The checkpoint's normal "reopen any leg not open" branch already gives
                # the "flat for the rest of this hour" behavior on its own, since it only runs once
                # per hour; setting suppress_reentry here would make the *next* checkpoint skip its
                # reopen too, leaving legs flat for two hours instead of one.
                if HEDGE_ON_COMBINED_STOPLOSS_ENABLED:
                    _open_hedge(market, cfg, day, now)

    if PREMIUM_HIGH_STOPLOSS_ENABLED and current_premium is not None:
        window_start = now - PREMIUM_HIGH_LOOKBACK
        day['premium_history'] = [(t, p) for t, p in day['premium_history'] if t >= window_start]
        prior_high = max((p for _, p in day['premium_history']), default=None)
        if prior_high is not None and current_premium > prior_high and any_open:
            alert(f'ATM premium {current_premium} above its {PREMIUM_HIGH_LOOKBACK} high {prior_high} - closing legs')
            day['realized_pnl'] += _close_open_legs(state, market, cfg, 'ATM_PREMIUM_2H_HIGH')
            day['suppress_reentry'] = True
        day['premium_history'].append((now, current_premium))

    if PREMIUM_STOPLOSS_ENABLED and day['entry_premium'] is not None and current_premium is not None:
        threshold = day['entry_premium'] * (1 + PREMIUM_STOPLOSS_PCT)
        if current_premium > threshold and any_open:
            alert(f'ATM premium {current_premium} above entry+{PREMIUM_STOPLOSS_PCT:.0%} ({threshold:.2f}) - closing legs')
            day['realized_pnl'] += _close_open_legs(state, market, cfg, 'ATM_PREMIUM_STOPLOSS')
            day['suppress_reentry'] = True

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
        f"day pnl so far: {day['realized_pnl']:+.2f} (hedge pnl: {day['hedge_pnl']:+.2f}, "
        f"active hedges: {len(day['active_hedges'])})\n"
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

    state = _new_state()
    day = dict(
        realized_pnl=0.0, halted=False, suppress_reentry=False,
        entry_premium=None, prev_checkpoint_premium=None, premium_history=[],
        active_hedges=[], hedge_pnl=0.0,
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
                state[opt] = dict(instrument=instrument, strike=strike_opt[0], entry_price=entry_price, quantity=quantity)

        adopted_premium = sum(leg['entry_price'] for leg in state.values() if leg is not None) or None
        day['entry_premium'] = adopted_premium
        day['prev_checkpoint_premium'] = adopted_premium
        if adopted_premium is not None:
            day['premium_history'] = [(datetime.now(), adopted_premium)]
            alert(f'Adopted legs - reconstructed combined entry ~{adopted_premium:.2f}, used as the previous-checkpoint premium')
    else:
        log.info('No open positions - entering initial legs')
        day['entry_premium'] = _atm_premium(market)
        day['prev_checkpoint_premium'] = day['entry_premium']
        if day['entry_premium'] is not None:
            day['premium_history'] = [(datetime.now(), day['entry_premium'])]
        for opt, (instrument, ltp, strike) in _desired_legs(market, cfg).items():
            _enter_leg(state, opt, instrument, ltp, strike, cfg)

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
            run_minute_checks(state, market, cfg, day, now)
            _run_hedge_checks(market, cfg, day, now, is_eod=False)

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
            day['realized_pnl'] += _close_open_legs(state, final_market, cfg, 'EOD')
            _run_hedge_checks(final_market, cfg, day, datetime.now(), is_eod=True)
            break
        except Exception as exc:
            if attempt == RETRY_MAX_ATTEMPTS - 1:
                alert(f'{symbol}: final square-off failed after {RETRY_MAX_ATTEMPTS} attempts - positions may still be OPEN, check manually: {exc}', level=logging.CRITICAL)
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            log.warning(f'final square-off failed ({exc}) - retrying in {delay:.0f}s (attempt {attempt + 1}/{RETRY_MAX_ATTEMPTS})')
            time_module.sleep(delay)

    alert(f"Rolling straddle (premium stoploss) done for {symbol} - realized pnl {day['realized_pnl']:+.2f} points (hedge pnl {day['hedge_pnl']:+.2f})")


if __name__ == '__main__':
    SYMBOL = sys.argv[1].upper() if len(sys.argv) > 1 and sys.argv[1] else 'NIFTY'
    if SYMBOL not in CFG:
        raise ValueError(f'unknown symbol {SYMBOL!r} - use one of {sorted(CFG)}')
    TRADE_WEEKDAYS = _parse_trade_weekdays(sys.argv[2] if len(sys.argv) > 2 else None)
    run_day(SYMBOL, TRADE_WEEKDAYS)
