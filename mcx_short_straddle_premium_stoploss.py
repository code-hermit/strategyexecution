"""
Live version of mcx_backtest_short_straddle_premium_stoploss.py, NATGASMINI. Same split as
mcx_option_buying.py: market data (futures/option LTPs) comes from Zerodha's Kite Connect API,
order placement goes through AliceBlue's REST API. Dhan is never touched, imported, or
authenticated by this file at all.

Rule (ported from mcx_backtest_short_straddle_premium_stoploss.py's simulate_day()):
  - Checkpoints hourly from CHECKPOINT_START_IST. The first checkpoint is baseline-only (records
    the ATM straddle premium, no trading); trading actions start at EXECUTION_START_IST.
  - At each checkpoint from EXECUTION_START_IST on: if the current ATM straddle premium has risen
    by AVOID_THRESHOLD_POINTS or more versus the previous checkpoint, avoid trading this checkpoint
    entirely - whatever is open just keeps running. Otherwise:
      - No position open -> SHORT the current ATM straddle (CE + PE), LIMIT orders.
      - Position open and ATM strike has changed -> close the old legs, SHORT a fresh straddle at
        the new ATM strike.
      - Position open and ATM strike unchanged -> leave it alone.
  - No per-leg stoploss. Instead a COMBINED STOPLOSS_POINTS premium stoploss: both legs' floating
    P&L (as a short) is checked every poll tick; if it drops to -STOPLOSS_POINTS or worse, both
    legs are squared off immediately (independent of checkpoints). Flat until the next checkpoint's
    normal revise logic reopens it.
  - Session end (EXIT_TIME_IST): whatever is open is squared off.

Progress (today's checkpoints done, current baseline premium/pinned strike, open position/legs) is
persisted to STATE_FILE after every change - a restart mid-day resumes instead of losing track of
an open short position. Late-start handling is automatic: started after EXECUTION_START_IST has
already passed - bootstrap a baseline right now from the live ATM straddle (approximate, logged as
such), then fall into the normal checkpoint loop.

Logging: goes to mcx_short_straddle.log and stdout; if TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are set
in .env, every log record is also pushed to Telegram - if either is missing, Telegram alerts are
just skipped (logged once as a warning) and trading proceeds normally.
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

LOG_FILE = os.path.join(os.path.dirname(__file__), 'mcx_short_straddle.log')
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
    log.critical(f'Received signal {signal.Signals(signum).name} ({signum}) - exiting')
    sys.exit(1)


for _sig in (signal.SIGTERM, signal.SIGHUP):
    signal.signal(_sig, _log_and_exit_on_signal)

DRY_RUN = os.getenv('DRY_RUN', 'true').lower() != 'false'  # set DRY_RUN=false to place real orders

SYMBOL = 'NATGASMINI'
EXCHANGE = 'MCX'
LOT_SIZE = 250  # informational default - actual order quantity uses the AliceBlue contract's lot_size
LOTS = 2
CFG = dict(aliceblue_exchange='MCX')

STATE_FILE = os.path.join(os.path.dirname(__file__), 'mcx_short_straddle_state.json')

cur_time=dtime(datetime.now().hour,datetime.now().minute)
# CHECKPOINT_START_IST = dtime(10, 45)   # first checkpoint: baseline only, no trading
CHECKPOINT_START_IST = cur_time   # first checkpoint: baseline only, no trading
# EXECUTION_START_IST = dtime(11, 45)    # trading begins at this checkpoint
EXECUTION_START_IST = cur_time    # trading begins at this checkpoint
EXIT_TIME_IST = dtime(22, 45)          # last checkpoint
SESSION_END_IST = dtime(23, 10)        # forced square-off
CHECKPOINT_INTERVAL_MIN = 60
STOPLOSS_POINTS = 0.75                 # combined (both legs) floating-loss stoploss (software-monitored)
PER_LEG_STOPLOSS_RS = 10.0             # resting broker-side SL per leg, entry_price + this (short: loss above entry)
SL_LIMIT_BUFFER_RS = 5.0               # SL order's limit price = trigger + this (MCX requires SL-L, not SL-MKT)
AVOID_THRESHOLD_POINTS = 0             # premium rise vs. previous checkpoint that triggers avoidance

POLL_INTERVAL = 60  # seconds between minute ticks
REQUEST_TIMEOUT = 10
LIMIT_OFFSET_PCT = 0.01  # limit price offset from LTP: below LTP for SELL (short entry), above LTP for BUY (cover)
FILL_POLL_TIMEOUT = 10
FILL_POLL_INTERVAL = 1
TERMINAL_ORDER_STATUSES = {'complete', 'rejected', 'cancelled'}
FAILURE_ALERT_EVERY = 10


def _log_failure_throttled(message, failure_count, every=FAILURE_ALERT_EVERY):
    try:
        extra = {} if (failure_count == 1 or failure_count % every == 0) else {'no_telegram': True}
        log.exception(message, extra=extra)
    except Exception as exc:
        print(f'_log_failure_throttled failed: {exc}', file=sys.stderr)


def _label(t):
    return f'{t:%H:%M}'


def _today_str():
    return datetime.now().strftime('%Y-%m-%d')


def checkpoints_today():
    cps = []
    t = datetime.combine(datetime.now().date(), CHECKPOINT_START_IST)
    end = datetime.combine(datetime.now().date(), EXIT_TIME_IST)
    while t.time() <= end.time():
        cps.append(t.time())
        t += timedelta(minutes=CHECKPOINT_INTERVAL_MIN)
    return cps


CHECK_TIMES = checkpoints_today()


# ── Zerodha (REST, Kite Connect) - market data only ─────────────────────────
ZERODHA_API_KEY = os.getenv('ZERODHA_API_KEY')
ZERODHA_TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'zerodha_token.json')
ZERODHA_BASE_URL = 'https://api.kite.trade'


def _zerodha_headers():
    return {'Authorization': f'token {ZERODHA_API_KEY}:{ZERODHA_ACCESS_TOKEN}', 'X-Kite-Version': '3'}


def _load_zerodha_token():
    with open(ZERODHA_TOKEN_FILE) as f:
        return json.load(f)['access_token']


def _zerodha_token_is_valid(access_token):
    resp = requests.get(f'{ZERODHA_BASE_URL}/user/profile',
                         headers={'Authorization': f'token {ZERODHA_API_KEY}:{access_token}', 'X-Kite-Version': '3'},
                         timeout=REQUEST_TIMEOUT)
    return resp.ok


def _valid_zerodha_token():
    if not os.path.exists(ZERODHA_TOKEN_FILE):
        raise RuntimeError('No Zerodha access token found - run zerodha_generate_access_token.py to log in')
    access_token = _load_zerodha_token()
    if not _zerodha_token_is_valid(access_token):
        raise RuntimeError('Zerodha access token expired - run zerodha_generate_access_token.py to log in again')
    return access_token


ZERODHA_ACCESS_TOKEN = _valid_zerodha_token()


def _kite_get(path, params=None):
    resp = requests.get(ZERODHA_BASE_URL + path, headers=_zerodha_headers(), params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    j = resp.json()
    if j.get('status') != 'success':
        raise RuntimeError(f'Zerodha request to {path} failed: {j}')
    return j['data']


_mcx_instruments_cache = {'date': None, 'options': None, 'futures': None}


def _load_mcx_instruments():
    """This month's NATGASMINI CE/PE option contracts and the nearest futures contract (used as
    the spot proxy for ATM), from Kite's MCX instrument dump - cached per calendar day."""
    today = _today_str()
    if _mcx_instruments_cache['date'] == today:
        return _mcx_instruments_cache['options'], _mcx_instruments_cache['futures']

    resp = requests.get(f'{ZERODHA_BASE_URL}/instruments/{EXCHANGE}', headers=_zerodha_headers(), timeout=30)
    resp.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(resp.text)))
    today_date = datetime.now().date()

    def _expiry(row):
        return datetime.strptime(row['expiry'], '%Y-%m-%d').date()

    opts = [
        r for r in rows
        if r['name'] == SYMBOL and r['instrument_type'] in ('CE', 'PE') and r['expiry']
        and _expiry(r) >= today_date
    ]
    if not opts:
        raise RuntimeError(f'No {SYMBOL} option instruments found on Zerodha {EXCHANGE}')
    nearest_expiry = min(_expiry(r) for r in opts)
    opts = [r for r in opts if _expiry(r) == nearest_expiry]

    futs = [
        r for r in rows
        if r['name'] == SYMBOL and r['instrument_type'] == 'FUT' and r['expiry']
        and _expiry(r) >= today_date
    ]
    if not futs:
        raise RuntimeError(f'No {SYMBOL} futures instrument found on Zerodha {EXCHANGE}')
    futs.sort(key=_expiry)
    future = futs[0]

    _mcx_instruments_cache.update(date=today, options=opts, futures=future)
    return opts, future


def _strike_step(options):
    strikes = sorted({int(float(r['strike'])) for r in options})
    diffs = [b - a for a, b in zip(strikes, strikes[1:])]
    return min(diffs) if diffs else 1


def _quote_ltp(instrument_keys):
    data = _kite_get('/quote/ltp', params=[('i', k) for k in instrument_keys])
    return {k: float(v['last_price']) for k, v in data.items()}


def get_future_ltp(future):
    key = f'{EXCHANGE}:{future["tradingsymbol"]}'
    return _quote_ltp([key])[key]


def _tradingsymbol(options, strike, option_type):
    for row in options:
        if int(float(row['strike'])) == strike and row['instrument_type'] == option_type:
            return row
    raise KeyError(f'No {SYMBOL} instrument found for strike={strike} type={option_type}')


def get_option_ltp(options, strike, option_type):
    row = _tradingsymbol(options, strike, option_type)
    key = f'{EXCHANGE}:{row["tradingsymbol"]}'
    return _quote_ltp([key])[key]


def atm_strike(spot, step):
    return int(round(spot / step) * step)


def _current_atm(options, future):
    spot = get_future_ltp(future)
    step = _strike_step(options)
    strike = atm_strike(spot, step)
    ce_ltp = get_option_ltp(options, strike, 'CE')
    pe_ltp = get_option_ltp(options, strike, 'PE')
    return strike, ce_ltp, pe_ltp


# ── AliceBlue (REST, v3 open-api) - order placement only ────────────────────
ALICEBLUE_TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'aliceblue_token.json')
ALICEBLUE_BASE_URL = 'https://a3.aliceblueonline.com/open-api/od/v1'
ALICEBLUE_CONTRACT_MASTER_URL = 'https://v2api.aliceblueonline.com/restpy/static/contract_master/V2/{exchange}'
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
    """This month's NATGASMINI CE/PE contracts from AliceBlue's contract master - needed to place
    orders (instrument token/lot size/tick size). Separate from the Zerodha instruments above,
    which are used for pricing only."""
    resp = requests.get(ALICEBLUE_CONTRACT_MASTER_URL.format(exchange=CFG['aliceblue_exchange']), timeout=30)
    resp.raise_for_status()
    exchange = CFG['aliceblue_exchange']
    today = datetime.now().date()
    opts = [
        c for c in resp.json()[exchange]
        if c['symbol'] == SYMBOL and c['option_type'] in ('CE', 'PE')
        and datetime.fromtimestamp(c['expiry_date'] / 1000, tz=timezone.utc).date() >= today
    ]
    if not opts:
        raise RuntimeError(f'No {SYMBOL} contracts found on AliceBlue {exchange}')
    nearest_expiry = min(c['expiry_date'] for c in opts)
    return [c for c in opts if c['expiry_date'] == nearest_expiry]


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


def _cancel_order(broker_order_id):
    return _aliceblue_post('/orders/cancel', {'brokerOrderId': broker_order_id})


def _place_order(transaction_type, instrument, quantity, order_tag, order_type='LIMIT', price='0', trigger_price=None):
    """order_type + trigger_price places a resting stop order (see short_leg below for the per-leg
    stoploss). AliceBlue's API accepts orderType in {'LIMIT', 'MARKET', 'SL', 'SLM'} (confirmed via
    a live EC930 validation error - 'SL-M' with a hyphen is rejected), but MCX itself rejects SLM
    (stop-loss-market) orders outright at the exchange level regardless of what the API accepts -
    use 'SL' (stop-loss LIMIT, needs both price and trigger_price) for anything trading on MCX."""
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
        'orderTag': order_tag,
    }]
    result = _aliceblue_post('/orders/placeorder', payload)
    return result[0] if isinstance(result, list) and len(result) == 1 else result


def _wait_for_fill_price(broker_order_id, instrument=None, quantity=None, transaction_type=None, order_tag=None, escalate_to_market=False):
    """Polls for a fill. If escalate_to_market is set and the order is still pending (not filled/
    rejected/cancelled) when FILL_POLL_TIMEOUT elapses, cancels it and places a fresh MARKET order
    for the same instrument/quantity/side, then polls that new order for one more FILL_POLL_TIMEOUT
    window - only raises TimeoutError if it's still stuck after that.

    Cancel + place-new rather than modify-in-place: AliceBlue's /orders/modifyorder returned a
    live 400 Bad Request against the payload shape this used to send - cancel and placeorder are
    both confirmed-working endpoints elsewhere in this file, so escalation uses those instead."""
    def _poll(order_id, timeout):
        deadline = time_module.time() + timeout
        while time_module.time() < deadline:
            for o in _order_book():
                if o.get('brokerOrderId') != order_id:
                    continue
                status = str(o.get('orderStatus', '')).lower()
                if status == 'rejected':
                    raise RuntimeError(f'order {order_id} rejected: {o.get("rejectionReason")}')
                if status == 'cancelled':
                    raise RuntimeError(f'order {order_id} cancelled')
                if status == 'complete':
                    return float(o.get('averageTradedPrice') or 0)
            time_module.sleep(FILL_POLL_INTERVAL)
        return None

    price = _poll(broker_order_id, FILL_POLL_TIMEOUT)
    if price is not None:
        return price

    if not escalate_to_market:
        raise TimeoutError(f'order {broker_order_id} not filled within {FILL_POLL_TIMEOUT}s')

    log.warning(f'order {broker_order_id} still pending after {FILL_POLL_TIMEOUT}s - cancelling and placing a MARKET order instead')
    try:
        _cancel_order(broker_order_id)
    except Exception as exc:
        log.warning(f'cancel of {broker_order_id} failed ({exc}) - it may already be filling; placing a MARKET order anyway')

    market_order = _place_order(transaction_type, instrument, quantity, order_tag or 'market_escalation', order_type='MARKET')
    new_order_id = market_order.get('brokerOrderId')
    if not new_order_id:
        raise RuntimeError(f'{instrument.name if instrument else "?"} MARKET escalation order rejected: {market_order}')

    price = _poll(new_order_id, FILL_POLL_TIMEOUT)
    if price is not None:
        return price
    raise TimeoutError(f'MARKET escalation order {new_order_id} still not filled {FILL_POLL_TIMEOUT}s after placing')


def _instrument_to_dict(instrument):
    return instrument._asdict()


def _instrument_from_dict(d):
    return Instrument(**d)


# ── Orders (short-entry with a resting per-leg SL-L stoploss + market-conversion exit) ──
def short_leg(instrument, quantity, ltp):
    """SELL to open a short leg, then place a resting BUY SL (stop-loss LIMIT) stoploss
    PER_LEG_STOPLOSS_RS above the actual fill price - a broker-side backstop independent of this
    script staying alive (e.g. if the process crashes or loses network, the position is still
    protected). This is in addition to, not instead of, the software-monitored combined stoploss
    in combined_floating_pnl().

    MCX rejects SL-MKT (stop-loss-market) orders outright ("SL-MKT ORDER TYPES ARE NOT ALLOWED TO
    TRADE" - a segment-level restriction, not just an API validation quirk) - only SL (stop-loss
    LIMIT) is permitted, which needs both a trigger price and a limit price. The limit price is set
    SL_LIMIT_BUFFER_RS above the trigger so the order has room to actually fill if price gaps past
    the trigger, rather than resting unfilled past its own limit.

    Returns (entry_price, sl_order_id) - sl_order_id is None in DRY_RUN."""
    entry_price = _round_to_tick(ltp * (1 - LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}SELL (short open) {quantity} x {instrument.name} LIMIT @ {entry_price} (ltp {ltp})'
    log.info(tag)
    if DRY_RUN:
        preview_trigger = _round_to_tick(entry_price + PER_LEG_STOPLOSS_RS, instrument.tick_size)
        preview_limit = _round_to_tick(preview_trigger + SL_LIMIT_BUFFER_RS, instrument.tick_size)
        log.info(f'[DRY RUN] would place resting BUY SL {quantity} x {instrument.name} @ trigger '
                  f'{preview_trigger} limit {preview_limit}')
        return entry_price, None

    entry = _place_order('SELL', instrument, quantity, 'short_straddle_entry', order_type='LIMIT', price=entry_price)
    order_no = entry.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} entry order rejected: {entry}')
    filled_price = _wait_for_fill_price(order_no)

    sl_trigger = _round_to_tick(filled_price + PER_LEG_STOPLOSS_RS, instrument.tick_size)
    sl_limit = _round_to_tick(sl_trigger + SL_LIMIT_BUFFER_RS, instrument.tick_size)
    sl = _place_order('BUY', instrument, quantity, 'short_straddle_leg_sl', order_type='SL', price=sl_limit, trigger_price=sl_trigger)
    sl_order_no = sl.get('brokerOrderId')
    if not sl_order_no:
        # Entry is already filled and unprotected - raise loudly rather than silently trading naked.
        raise RuntimeError(f'{instrument.name} filled at {filled_price} but per-leg SL order rejected: {sl}')
    log.info(f'{instrument.name}: resting SL placed, trigger {sl_trigger} limit {sl_limit} (entry {filled_price} + {PER_LEG_STOPLOSS_RS})')
    return filled_price, sl_order_no


def square_off_leg(instrument, quantity, sl_order_id, order_tag):
    """Squares off a short leg by cancelling its resting per-leg SL order and placing a fresh
    MARKET BUY order, rather than modifying the SL in place - AliceBlue's /orders/modifyorder
    returned a live 400 Bad Request against the payload shape this used to send, whereas cancel
    and placeorder are both confirmed-working endpoints elsewhere in this file. Used for every
    square-off path: software combined stoploss, strike-shift re-hedge, and EOD."""
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}Square off (cancel SL, MARKET) {quantity} x {instrument.name}'
    log.info(tag)
    if DRY_RUN:
        return 0.0  # no real fill price in DRY_RUN; pnl in this mode is approximate anyway

    if sl_order_id:
        for o in _order_book():
            if o.get('brokerOrderId') == sl_order_id and str(o.get('orderStatus', '')).lower() == 'complete':
                # Broker-side SL already triggered and filled on its own before we got here.
                return float(o.get('averageTradedPrice') or 0)
        try:
            _cancel_order(sl_order_id)
        except Exception as exc:
            log.warning(f'cancel of {instrument.name} SL order {sl_order_id} failed ({exc}) - it may '
                        f'already be filling/filled; placing a MARKET order anyway')
    else:
        # Defensive fallback only - short_leg raises before this can normally happen live.
        log.warning(f'{instrument.name}: no sl_order_id on record')

    exit_order = _place_order('BUY', instrument, quantity, order_tag, order_type='MARKET')
    order_no = exit_order.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} exit order rejected: {exit_order}')
    return _wait_for_fill_price(order_no)


# ── State persistence ─────────────────────────────────────────────────────
def _fresh_state():
    return {
        'date': _today_str(),
        'prev_premium': None,           # ATM straddle premium recorded at the last checkpoint
        'checkpoints_done': [],
        'position': None,  # {'strike', 'legs': {'CE': {...}, 'PE': {...}}}
        'first_entry_done': False,      # the avoid-threshold check is skipped for the day's first entry
    }


def _load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)
        if state.get('date') == _today_str():
            state.setdefault('first_entry_done', False)  # back-compat with pre-flag state files
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
def open_position(strike):
    options, _future = _load_mcx_instruments()
    contracts = _load_aliceblue_contracts()
    contracts_by_strike_type = {(int(float(c['strike_price'])), c['option_type']): c for c in contracts}

    legs = {}
    for opt in ('CE', 'PE'):
        ltp = get_option_ltp(options, strike, opt)
        contract = contracts_by_strike_type[(strike, opt)]
        instrument = _to_instrument(contract)
        if instrument.lot_size != LOT_SIZE:
            log.warning(f'{instrument.name}: AliceBlue lot_size={instrument.lot_size} differs from '
                        f'expected {LOT_SIZE} - informational only, not used in the order quantity')
        # AliceBlue's placeorder 'quantity' for MCX is in LOTS, not raw contract units - confirmed
        # by a live freeze-quantity rejection when this sent lot_size*LOTS (500) and the broker
        # read it as 500 lots (500*250 units), against a 240-lot/60000-unit freeze limit.
        quantity = LOTS
        entry_price, sl_order_id = short_leg(instrument, quantity, ltp)
        legs[opt] = {
            'instrument': _instrument_to_dict(instrument), 'quantity': quantity,
            'entry': entry_price, 'sl_order_id': sl_order_id,
        }

    return {'strike': strike, 'legs': legs}


def close_position(position, reason):
    if position is None:
        return 0.0
    pnl = 0.0
    for leg in position['legs'].values():
        instrument = _instrument_from_dict(leg['instrument'])
        exit_price = square_off_leg(instrument, leg['quantity'], leg.get('sl_order_id'), f'short_straddle_exit_{reason}')
        pnl += (leg['entry'] - exit_price) * leg['quantity']  # short: profit when exit < entry
    log.info(f'Closed {SYMBOL} short straddle ({reason}) - pnl ~{pnl:.0f} Rs')
    return pnl


def combined_floating_pnl(position):
    """Current combined (both legs) floating P&L of an open short position, in points (not Rs -
    matches the backtest's per-point STOPLOSS_POINTS, which is applied before multiplying by lot
    size)."""
    options, _future = _load_mcx_instruments()
    total = 0.0
    for opt, leg in position['legs'].items():
        ltp = get_option_ltp(options, position['strike'], opt)
        total += leg['entry'] - ltp  # short leg: profit when ltp < entry
    return total


# ── Startup reconciliation ───────────────────────────────────────────────
def _net_short_positions_from_orderbook(order_book, contracts_by_token):
    """Derives each NATGASMINI instrument's net open short quantity and actual average entry price
    from today's COMPLETE orders in the order book, rather than the /positions endpoint - this also
    recovers the real fill price (via averageTradedPrice), which /positions doesn't expose here.
    net_qty = total SELL fills - total BUY fills; for the common case (an entry short with no
    partial buyback yet), avg_entry reduces to the weighted-average SELL fill price, i.e. the
    actual entry price. token -> (net_qty, avg_entry)."""
    totals = {}  # token -> {'sell_qty', 'sell_value', 'buy_qty', 'buy_value'}
    for o in order_book:
        if str(o.get('orderStatus', '')).lower() != 'complete':
            continue
        token = int(o.get('instrumentId', 0) or 0)
        if token not in contracts_by_token:
            continue
        side = str(o.get('transactionType', '')).upper()
        qty = int(o.get('quantity', 0) or 0)
        price = float(o.get('averageTradedPrice', 0) or 0)
        if side not in ('BUY', 'SELL') or qty <= 0:
            continue
        t = totals.setdefault(token, {'sell_qty': 0, 'sell_value': 0.0, 'buy_qty': 0, 'buy_value': 0.0})
        if side == 'SELL':
            t['sell_qty'] += qty
            t['sell_value'] += qty * price
        else:
            t['buy_qty'] += qty
            t['buy_value'] += qty * price

    result = {}
    for token, t in totals.items():
        net_qty = t['sell_qty'] - t['buy_qty']
        if net_qty <= 0:
            continue  # flat or long - not something this strategy would ever hold
        avg_entry = (t['sell_value'] - t['buy_value']) / net_qty
        result[token] = (net_qty, avg_entry)
    return result


def reconcile_open_positions():
    """Startup safety check, independent of state.json: derives which NATGASMINI legs are short
    from the AliceBlue order book (broker truth), and for any one with no resting order on that
    instrument (e.g. left behind by a crash mid-open_position - exactly what happened when a leg's
    entry filled but its SL order was then rejected and open_position() raised before saving
    state), places a fresh SL at actual_entry_price + PER_LEG_STOPLOSS_RS.

    The order book's SELL/BUY netting is only trusted to answer "is this leg short and by roughly
    how much" (and for the average entry price, for the trigger calc) - NOT for the quantity to put
    on the SL order itself. That netted number has shown up wildly wrong in practice (500, against
    a real live position of 2 lots confirmed directly in the AliceBlue app) for reasons not fully
    diagnosed yet. This strategy only ever opens exactly LOTS quantity per leg, never partial, so
    the SL order always uses the fixed LOTS constant - not whatever the order book computed."""
    contracts = _load_aliceblue_contracts()
    contracts_by_token = {int(c['token']): c for c in contracts}

    order_book = _order_book()
    short_positions = _net_short_positions_from_orderbook(order_book, contracts_by_token)
    if not short_positions:
        log.info('Reconciliation: no open NATGASMINI short positions found in the order book')
        return

    protected_tokens = {
        str(o.get('instrumentId')) for o in order_book
        if str(o.get('orderStatus', '')).lower() not in TERMINAL_ORDER_STATUSES
    }

    for token, (net_qty_hint, avg_entry) in short_positions.items():
        contract = contracts_by_token[token]
        name = contract['trading_symbol']
        if str(token) in protected_tokens:
            log.info(f'Reconciliation: {name} already has a resting order - leaving it as-is')
            continue

        instrument = _to_instrument(contract)
        if net_qty_hint != LOTS:
            log.warning(f'Reconciliation: {name} order-book net qty ({net_qty_hint}) does not match '
                        f'this strategy\'s LOTS ({LOTS}) - placing the SL at LOTS anyway, since that\'s '
                        f'the only quantity this strategy ever actually opens per leg; check manually '
                        f'if the real position size might genuinely differ')

        log.warning(f'Reconciliation: {name} is SHORT (avg entry {avg_entry:.2f} from the order book) '
                    f'with no resting order protecting it - placing an SL now, qty {LOTS}')

        sl_trigger = _round_to_tick(avg_entry + PER_LEG_STOPLOSS_RS, instrument.tick_size)
        sl_limit = _round_to_tick(sl_trigger + SL_LIMIT_BUFFER_RS, instrument.tick_size)
        if DRY_RUN:
            log.info(f'[DRY RUN] would place resting BUY SL qty {LOTS} x {name} @ trigger {sl_trigger} limit {sl_limit}')
            continue

        sl = _place_order('BUY', instrument, LOTS, 'short_straddle_leg_sl_reconcile', order_type='SL', price=sl_limit, trigger_price=sl_trigger)
        if not sl.get('brokerOrderId'):
            log.error(f'Reconciliation: SL placement failed for {name}: {sl}')
        else:
            log.info(f'Reconciliation: {name} now protected, SL trigger {sl_trigger} limit {sl_limit}')


def run_day():
    log.info(f'Starting {SYMBOL} short straddle premium-stoploss strategy (DRY_RUN={DRY_RUN})')

    # NOTE: the "already protected" detection has previously failed to recognize an existing
    # resting SL order, causing a duplicate to be placed - the SL quantity is now always LOTS
    # (fixed, not derived from the order book), so a duplicate is just an extra correctly-sized
    # order rather than a sizing risk. Cancel any stray duplicates manually if they show up.
    if not DRY_RUN:
        try:
            reconcile_open_positions()
        except Exception:
            log.exception('Startup reconciliation failed - proceeding anyway, but check positions manually')

    state = _load_state()

    now_t = datetime.now().time()
    if now_t >= SESSION_END_IST and state['position'] is None and not [c for c in CHECK_TIMES if _label(c) not in state['checkpoints_done']]:
        log.info(f'{SESSION_END_IST} already passed for today with nothing pending - nothing to do')
        return

    if now_t >= EXECUTION_START_IST and state['prev_premium'] is None and state['position'] is None:
        recent = [c for c in CHECK_TIMES if c <= now_t]
        if recent:
            options, future = _load_mcx_instruments()
            strike, ce_ltp, pe_ltp = _current_atm(options, future)
            premium = ce_ltp + pe_ltp
            state['prev_premium'] = premium
            state['checkpoints_done'].append(_label(recent[-1]))
            if not state['first_entry_done']:
                # Same rule as the main loop's first-checkpoint handling: nothing open yet to
                # protect, so a late start enters directly instead of just recording a baseline
                # and waiting an hour for the next checkpoint.
                state['position'] = open_position(strike)
                state['first_entry_done'] = True
                log.info(f'Started after {recent[-1]} with no baseline recorded - entering directly '
                         f'(first entry of the day, approximate baseline={premium:.2f})')
            else:
                log.info(f'Started after {recent[-1]} with no baseline recorded - bootstrapping '
                         f'baseline={premium:.2f} from the current premium (approximate)')
            _save_state(state)

    checkpoint_failure_count = 0
    tick_failure_count = 0

    while True:
        now = datetime.now()
        t = now.time()

        if t < CHECKPOINT_START_IST:
            wait_s = (datetime.combine(now.date(), CHECKPOINT_START_IST) - now).total_seconds()
            log.info(f'Waiting for first checkpoint ({_label(CHECKPOINT_START_IST)}) - {wait_s / 60:.1f} min left',
                     extra={'no_telegram': True})
            time_module.sleep(min(POLL_INTERVAL, wait_s))
            continue

        if t >= SESSION_END_IST:
            if state['position'] is not None:
                close_position(state['position'], 'EOD')
                state['position'] = None
                _save_state(state)
            break

        label = _label(dtime(t.hour, t.minute))
        if label in [_label(c) for c in CHECK_TIMES] and label not in state['checkpoints_done']:
            try:
                options, future = _load_mcx_instruments()
                strike, ce_ltp, pe_ltp = _current_atm(options, future)
                premium = ce_ltp + pe_ltp

                if dtime(t.hour, t.minute) < EXECUTION_START_IST:
                    # baseline-only checkpoint - record, no trading. Compared against
                    # EXECUTION_START_IST (not CHECKPOINT_START_IST) so this still works correctly
                    # if the two are ever set equal - in that case no checkpoint is < execution
                    # start, so even the very first checkpoint goes straight to trading below.
                    state['prev_premium'] = premium
                elif not state['first_entry_done']:
                    # Day's first trading checkpoint - the avoid-threshold check exists to protect
                    # an already-open position from re-entering into a spike; there's nothing to
                    # protect yet, so enter directly regardless of the premium move.
                    state['position'] = open_position(strike)
                    state['first_entry_done'] = True
                    log.info(f'{label}: SHORT new {strike} straddle @ {premium:.2f} (first entry of the '
                             f'day - avoid-threshold check skipped)')
                else:
                    avoid = (
                        state['prev_premium'] is not None
                        and (premium - state['prev_premium']) >= AVOID_THRESHOLD_POINTS
                    )
                    if avoid:
                        log.info(f'{label}: ATM premium rose {state["prev_premium"]:.2f} -> {premium:.2f} '
                                  f'(+{premium - state["prev_premium"]:.2f} >= {AVOID_THRESHOLD_POINTS}) - '
                                  f'avoid trading this checkpoint')
                    elif state['position'] is None:
                        state['position'] = open_position(strike)
                        log.info(f'{label}: SHORT new {strike} straddle @ {premium:.2f}')
                    elif strike != state['position']['strike']:
                        log.info(f'{label}: ATM strike moved {state["position"]["strike"]} -> {strike} - re-hedging')
                        close_position(state['position'], 'strike_shift')
                        state['position'] = open_position(strike)
                    # else: same strike, already have a position - leave it running.

                if dtime(t.hour, t.minute) >= EXECUTION_START_IST:
                    state['prev_premium'] = premium

                state['checkpoints_done'].append(label)
                _save_state(state)
                checkpoint_failure_count = 0
            except Exception:
                checkpoint_failure_count += 1
                _log_failure_throttled(f'{label} checkpoint failed', checkpoint_failure_count)

        try:
            if state['position'] is not None:
                floating = combined_floating_pnl(state['position'])
                log.info(f'{label} tick: position open, strike={state["position"]["strike"]} '
                         f'floating={floating:.2f} (SL at -{STOPLOSS_POINTS})', extra={'no_telegram': True})
                if floating <= -STOPLOSS_POINTS:
                    log.info(f'Combined SL hit ({floating:.2f} <= -{STOPLOSS_POINTS}) - squaring off')
                    close_position(state['position'], 'stoploss')
                    state['position'] = None
                    _save_state(state)
            else:
                log.info(f'{label} tick: flat, prev_premium={state["prev_premium"]}', extra={'no_telegram': True})
            tick_failure_count = 0
        except Exception:
            tick_failure_count += 1
            _log_failure_throttled('Tick processing failed', tick_failure_count)

        time_module.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    run_day()
