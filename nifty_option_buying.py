"""
What it is: a standalone ATM-straddle buy strategy — no short leg anywhere in this file. NIFTY
version of sensex_option_buying.py, same rules, tuned trigger/hold:

Window: 10:15 (ENTRY_TIME) → 15:13 (EXIT_TIME, forced day-end square-off).

Checkpoints: 10:15, 11:15, 12:15, 13:15, 14:15 (CHECK_TIMES). At each one, the combined ATM straddle premium (ATM CE close + ATM PE close) is recorded as the current baseline, overwriting whatever baseline the previous checkpoint set.

Signal (checked every minute, not just at checkpoints): if the live combined ATM premium has risen SPIKE_POINTS = 15 points or more above the latest checkpoint's baseline, buy the ATM straddle (CE+PE) right then.

Exit: hold for HOLD_MINUTES = 3 minutes, then exit at the first minute at/after that deadline (TIME_EXIT), or at day end if 15:13 arrives first (EOD).

Position sizing / concurrency rules:

Only one position open at a time — while a position is open, no new signal is evaluated at all.
Each checkpoint's baseline can fire at most one trade. Once a trade has been taken off a given checkpoint's value, that baseline is "used up" (checkpoint_used = True) and won't trigger again — even after the position closes and the premium is still elevated — until the next checkpoint sets a fresh baseline (which also re-arms the signal).
No other exits or filters — no per-leg stoploss, no profit target, no daily loss limit. Purely: spike-triggered entry, fixed-time (or EOD) exit, one trade per checkpoint window.

Day/weekday handling:

Trades every weekday by default (TRADE_WEEKDAYS); restrict via a second CLI arg using m/t/w/h/f codes, e.g. python backtest_atm_premium_spike_buy.py NIFTY th for Tuesday+Thursday only.
Days with no parquet file (weekends/holidays) are silently skipped.
Underlying handling: NIFTY only, hardcoded (unlike backtest_atm_premium_spike_buy_v2.py's NIFTY/SENSEX loop) - this file is the NIFTY counterpart to sensex_option_buying.py, not a generalized version of it.

---

Live version of Data/backtests/backtest_atm_premium_spike_buy_v2.py, NIFTY only, with
SPIKE_POINTS/HOLD_MINUTES tuned down (50->15, 15->3) from sensex_option_buying.py's defaults.
Otherwise ported rule-for-rule from that backtest's run_day(): checkpoints refresh the baseline
regardless of whether a position is open (so the baseline is current the moment a position closes,
and re-armed since nothing has traded off the fresh value yet); each checkpoint *pins* the exact
strike (CE+PE) tagged ATM at that moment, and the spike check and the straddle actually bought both
track that same pinned strike's premium for the rest of the hour - even after spot moves on and a
different strike becomes the new ATM - so a strike roll can never masquerade as a premium spike, or
vice versa; a still-open position is force-closed at day end even if its hold deadline hasn't arrived.

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
no profit target, and no daily loss limit, by design.

Logging: goes to nifty_option_buying.log and stdout; if TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are
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
import time as time_module
from datetime import datetime, time as dtime, timedelta, timezone
from typing import NamedTuple

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

LOG_FILE = os.path.join(os.path.dirname(__file__), 'nifty_option_buying.log')
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

SYMBOL = 'NIFTY'
CFG = dict(aliceblue_exchange='NFO', strike_interval=50, lots=3)

STATE_FILE = os.path.join(os.path.dirname(__file__), 'nifty_option_buying_state.json')

ENTRY_TIME = dtime(10, 15)  # strategy start
EXIT_TIME = dtime(15, 13)  # day end / forced square-off
CHECK_TIMES = (dtime(10, 15), dtime(11, 15), dtime(12, 15), dtime(13, 15), dtime(14, 15))
SPIKE_POINTS = 15  # combined ATM premium rise above the latest checkpoint's baseline that triggers a buy
HOLD_MINUTES = 3  # how long a triggered buy is held before being time-exited

POLL_INTERVAL = 60  # seconds between minute ticks
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
ZERODHA_SPOT_INSTRUMENT = 'NSE:NIFTY 50'
ZERODHA_OPTIONS_EXCHANGE = 'NFO'


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

_zerodha_options_cache = {'date': None, 'options': None}  # avoids re-downloading the full NFO
# instrument dump (thousands of rows, all strikes/expiries/underlyings) on every tick


def _load_zerodha_current_week_options():
    """This week's NIFTY CE/PE instruments from Kite's NFO instrument dump - [{'tradingsymbol',
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


def _zerodha_tradingsymbol(zerodha_options, strike, option_type):
    for row in zerodha_options:
        if int(float(row['strike'])) == strike and row['instrument_type'] == option_type:
            return row['tradingsymbol']
    raise KeyError(f'No Zerodha {SYMBOL} instrument found for strike={strike} type={option_type}')


def _zerodha_quote_ltp(instrument_keys):
    """instrument_keys like ['NSE:NIFTY 50', 'NFO:NIFTY25813950CE']. Returns key -> last_price."""
    resp = requests.get(f'{ZERODHA_BASE_URL}/quote/ltp', headers=_zerodha_headers(ZERODHA_ACCESS_TOKEN),
                         params=[('i', k) for k in instrument_keys], timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()['data']
    return {k: float(v['last_price']) for k, v in data.items()}


def get_spot_ltp():
    return _zerodha_quote_ltp([ZERODHA_SPOT_INSTRUMENT])[ZERODHA_SPOT_INSTRUMENT]


def get_option_ltp(zerodha_options, strike, option_type):
    symbol = _zerodha_tradingsymbol(zerodha_options, strike, option_type)
    key = f'{ZERODHA_OPTIONS_EXCHANGE}:{symbol}'
    return _zerodha_quote_ltp([key])[key]


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
LIMIT_OFFSET_PCT = 0.01  # limit price offset from LTP: below LTP for SELL orders, above LTP for BUY orders
FILL_POLL_TIMEOUT = 10
FILL_POLL_INTERVAL = 1
TERMINAL_ORDER_STATUSES = {'complete', 'rejected', 'cancelled'}
# Status codes AliceBlue returns instead of 'Ok' to mean "the query is fine, there's just nothing
# to return" - e.g. /positions returns EC920 rather than an empty list when there are no open
# positions. Treat these as an empty result rather than an error.
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
    """This week's NIFTY CE/PE contracts from AliceBlue's contract master - needed to place
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


def sell_leg(instrument, quantity, ltp):
    """Square off a bought leg: cancel any resting order on it (defensive - buy_leg above never
    leaves one) then SELL at a LIMIT price through the LTP, mirroring day_end_straddle_buy.py."""
    exit_price = _round_to_tick(ltp * (1 - LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}SELL (square off) {quantity} x {instrument.name} LIMIT @ {exit_price} (ltp {ltp})'
    log.info(tag)
    if DRY_RUN:
        return exit_price

    for o in _order_book():
        if str(o.get('instrumentId')) == str(instrument.token) and str(o.get('orderStatus', '')).lower() not in TERMINAL_ORDER_STATUSES:
            _cancel_order(o['brokerOrderId'])

    exit_order = _place_order(
        'SELL', instrument, quantity, 'LIMIT', price=str(exit_price), order_tag='premium_spike_buy_exit',
    )
    order_no = exit_order.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} exit order rejected: {exit_order}')
    return _wait_for_fill_price(order_no)


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

    legs = {}
    for opt, ltp in (('CE', ce_ltp), ('PE', pe_ltp)):
        contract = contracts_by_strike_type[(strike, opt)]
        instrument = _to_instrument(contract)
        entry_price = buy_leg(instrument, instrument.lot_size * CFG['lots'], ltp)
        legs[opt] = {
            'instrument': _instrument_to_dict(instrument), 'quantity': instrument.lot_size * CFG['lots'],
            'strike': strike, 'entry': entry_price,
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

    pnl = 0.0
    for opt, leg in position['legs'].items():
        instrument = _instrument_from_dict(leg['instrument'])
        ltp = get_option_ltp(zerodha_options, leg['strike'], opt)
        exit_price = sell_leg(instrument, leg['quantity'], ltp)
        pnl += exit_price - leg['entry']  # long: profit when exit > entry

    log.info(f'Closed {SYMBOL} spike-buy straddle ({reason}) - pnl ~{pnl:.1f} pts')
    state['position'] = None
    _save_state(state)


def run_day():
    log.info(f'Starting {SYMBOL} option buying strategy (DRY_RUN={DRY_RUN})')
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

    while True:
        now = datetime.now()
        t = now.time()

        if t < ENTRY_TIME:
            wait_s = (datetime.combine(now.date(), ENTRY_TIME) - now).total_seconds()
            log.info(f'Waiting for market open ({_label(ENTRY_TIME)}) - {wait_s / 60:.1f} min left',
                     extra={'no_telegram': True})
            time_module.sleep(min(POLL_INTERVAL, wait_s))
            continue

        if t >= EXIT_TIME:
            if state['position'] is not None:
                _close_position(state, 'EOD')
            break

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

        time_module.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    run_day()
