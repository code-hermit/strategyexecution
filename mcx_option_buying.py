"""
Live version of mcx_backtest_option_buying.py, NATGASMINI. Same split as
nifty_option_buying_twhf.py/sensex_option_buying_twhf.py: market data (futures/option LTPs) comes
from Zerodha's Kite Connect API, order placement goes through AliceBlue's REST API. Dhan is never
touched, imported, or authenticated by this file at all.

Rule (ported from mcx_backtest_option_buying.py's run()):
  - Checkpoints hourly from START_TIME_IST to END_TIME_IST. Each checkpoint pins the current ATM
    strike (CE+PE) and its combined premium as that interval's baseline.
  - Checked every minute (not just at checkpoints): the instant the pinned strike's live combined
    premium is >= JUMP_THRESHOLD_POINTS above the interval's baseline, BUY that straddle (CE+PE),
    LIMIT orders, held for HOLD_MINUTES minutes then time-exited (or exited at SESSION_END_IST if
    that arrives first).
  - No stoploss/target - pure signal + fixed-time exit.
  - Unlike the backtest (which allows overlapping signals from different checkpoint intervals),
    this live version only evaluates a new signal while no position is open, same simplification
    nifty_option_buying_twhf.py makes over its own backtest - running two concurrent straddle
    positions live is a lot-sizing/capital-risk complication not worth it for this strategy's edge.
    Each checkpoint still fires at most one trade (checkpoint_used, re-armed by the next checkpoint).

Progress (today's checkpoints done, current baseline/pinned strike, whether it's used up, any open
position) is persisted to STATE_FILE after every change - a restart mid-day resumes instead of
losing track. Late-start handling is automatic: started after the first checkpoint has already
passed - bootstrap a baseline right now from the live ATM straddle (approximate, logged as such).

Logging: goes to mcx_option_buying.log and stdout; if TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are set
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

LOG_FILE = os.path.join(os.path.dirname(__file__), 'mcx_option_buying.log')
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

STATE_FILE = os.path.join(os.path.dirname(__file__), 'mcx_option_buying_state.json')

START_TIME_IST = dtime(10, 45)
END_TIME_IST = dtime(22, 45)          # last checkpoint
SESSION_END_IST = dtime(23, 10)       # forced square-off, even if a hold deadline hasn't arrived
CHECKPOINT_INTERVAL_MIN = 60
HOLD_MINUTES = 10
JUMP_THRESHOLD_POINTS = 0.75          # ATM premium rise vs. checkpoint baseline that triggers a buy

POLL_INTERVAL = 60  # seconds between minute ticks
REQUEST_TIMEOUT = 10
LIMIT_OFFSET_PCT = 0.01  # limit price offset from LTP: above LTP for BUY, below LTP for SELL
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
    t = datetime.combine(datetime.now().date(), START_TIME_IST)
    end = datetime.combine(datetime.now().date(), END_TIME_IST)
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


def _place_order(transaction_type, instrument, quantity, order_tag='mcx_option_buying', order_type='LIMIT', price='0'):
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
        'slTriggerPrice': '',
        'orderTag': order_tag,
    }]
    result = _aliceblue_post('/orders/placeorder', payload)
    return result[0] if isinstance(result, list) and len(result) == 1 else result


def _cancel_order(broker_order_id):
    return _aliceblue_post('/orders/cancel', {'brokerOrderId': broker_order_id})


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


# ── Orders (no resting stoploss - this strategy has none, by design) ────────
def buy_leg(instrument, quantity, ltp):
    entry_price = _round_to_tick(ltp * (1 + LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}BUY {quantity} x {instrument.name} LIMIT @ {entry_price} (ltp {ltp})'
    log.info(tag)
    if DRY_RUN:
        return entry_price
    entry = _place_order('BUY', instrument, quantity, 'premium_spike_buy_entry', price=entry_price)
    order_no = entry.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} entry order rejected: {entry}')
    return _wait_for_fill_price(order_no)


def sell_leg(instrument, quantity, ltp):
    exit_price = _round_to_tick(ltp * (1 - LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}SELL (square off) {quantity} x {instrument.name} LIMIT @ {exit_price} (ltp {ltp})'
    log.info(tag)
    if DRY_RUN:
        return exit_price
    for o in _order_book():
        if str(o.get('instrumentId')) == str(instrument.token) and str(o.get('orderStatus', '')).lower() not in TERMINAL_ORDER_STATUSES:
            _cancel_order(o['brokerOrderId'])
    exit_order = _place_order('SELL', instrument, quantity, 'premium_spike_buy_exit', price=exit_price)
    order_no = exit_order.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} exit order rejected: {exit_order}')
    return _wait_for_fill_price(order_no, instrument, quantity, 'SELL', 'premium_spike_buy_exit_market', escalate_to_market=True)


# ── State persistence ─────────────────────────────────────────────────────
def _fresh_state():
    return {
        'date': _today_str(),
        'checkpoint': None,  # {'strike', 'premium'} - pinned at the last checkpoint
        'checkpoint_used': False,
        'checkpoints_done': [],
        'position': None,  # {'entry_time', 'deadline', 'checkpoint_premium', 'entry_premium', 'legs': {...}}
    }


def _load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)
        if state.get('date') == _today_str():
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
def _pinned_premium(options, checkpoint):
    ce_ltp = get_option_ltp(options, checkpoint['strike'], 'CE')
    pe_ltp = get_option_ltp(options, checkpoint['strike'], 'PE')
    return ce_ltp + pe_ltp, ce_ltp, pe_ltp


def _enter_position(state, checkpoint):
    options, _future = _load_mcx_instruments()  # Zerodha - pricing only
    contracts = _load_aliceblue_contracts()      # AliceBlue - order placement only
    contracts_by_strike_type = {(int(float(c['strike_price'])), c['option_type']): c for c in contracts}
    strike = checkpoint['strike']
    entry_premium, ce_ltp, pe_ltp = _pinned_premium(options, checkpoint)
    log.info(f'Spike buy: {SYMBOL} pinned_strike={strike} combined_premium={entry_premium:.2f} '
             f'(+{entry_premium - checkpoint["premium"]:.2f} vs checkpoint {checkpoint["premium"]:.2f})')

    legs = {}
    for opt, ltp in (('CE', ce_ltp), ('PE', pe_ltp)):
        contract = contracts_by_strike_type[(strike, opt)]
        instrument = _to_instrument(contract)
        if instrument.lot_size != LOT_SIZE:
            log.warning(f'{instrument.name}: AliceBlue lot_size={instrument.lot_size} differs from '
                        f'expected {LOT_SIZE} - informational only, not used in the order quantity')
        # AliceBlue's placeorder 'quantity' for MCX is in LOTS, not raw contract units - confirmed
        # by a live freeze-quantity rejection when this sent lot_size*LOTS (500) and the broker
        # read it as 500 lots (500*250 units), against a 240-lot/60000-unit freeze limit.
        quantity = LOTS
        entry_price = buy_leg(instrument, quantity, ltp)
        legs[opt] = {'instrument': _instrument_to_dict(instrument), 'quantity': quantity, 'strike': strike, 'entry': entry_price}

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
    position = state['position']
    if position is None:
        return
    options, _future = _load_mcx_instruments()

    pnl = 0.0
    for opt, leg in position['legs'].items():
        instrument = _instrument_from_dict(leg['instrument'])
        ltp = get_option_ltp(options, leg['strike'], opt)
        exit_price = sell_leg(instrument, leg['quantity'], ltp)
        pnl += (exit_price - leg['entry']) * leg['quantity']  # long: profit when exit > entry

    log.info(f'Closed {SYMBOL} spike-buy straddle ({reason}) - pnl ~{pnl:.0f} Rs')
    state['position'] = None
    _save_state(state)


def run_day():
    log.info(f'Starting {SYMBOL} option buying strategy (DRY_RUN={DRY_RUN})')
    state = _load_state()

    now_t = datetime.now().time()
    if now_t >= SESSION_END_IST and state['position'] is None and not [c for c in CHECK_TIMES if _label(c) not in state['checkpoints_done']]:
        log.info(f'{SESSION_END_IST} already passed for today with nothing pending - nothing to do')
        return

    if now_t >= START_TIME_IST and state['checkpoint'] is None and state['position'] is None:
        recent = [c for c in CHECK_TIMES if c <= now_t]
        if recent:
            options, future = _load_mcx_instruments()
            strike, ce_ltp, pe_ltp = _current_atm(options, future)
            state['checkpoint'] = {'strike': strike, 'premium': ce_ltp + pe_ltp}
            state['checkpoint_used'] = False
            state['checkpoints_done'].append(_label(recent[-1]))
            log.info(f'Started after {recent[-1]} with no baseline recorded - bootstrapping pinned '
                     f'strike={strike} baseline={state["checkpoint"]["premium"]:.2f} from the current premium (approximate)')
            _save_state(state)

    checkpoint_failure_count = 0
    tick_failure_count = 0

    while True:
        now = datetime.now()
        t = now.time()

        if t < START_TIME_IST:
            wait_s = (datetime.combine(now.date(), START_TIME_IST) - now).total_seconds()
            log.info(f'Waiting for strategy start ({_label(START_TIME_IST)}) - {wait_s / 60:.1f} min left',
                     extra={'no_telegram': True})
            time_module.sleep(min(POLL_INTERVAL, wait_s))
            continue

        if t >= SESSION_END_IST:
            if state['position'] is not None:
                _close_position(state, 'EOD')
            break

        label = _label(dtime(t.hour, t.minute))
        if label in [_label(c) for c in CHECK_TIMES] and label not in state['checkpoints_done']:
            try:
                options, future = _load_mcx_instruments()
                strike, ce_ltp, pe_ltp = _current_atm(options, future)
                state['checkpoint'] = {'strike': strike, 'premium': ce_ltp + pe_ltp}
                state['checkpoint_used'] = False
                state['checkpoints_done'].append(label)
                log.info(f'{label} checkpoint: pinned strike={strike} baseline={state["checkpoint"]["premium"]:.2f}')
                _save_state(state)
                checkpoint_failure_count = 0
            except Exception:
                checkpoint_failure_count += 1
                _log_failure_throttled(f'{label} checkpoint failed', checkpoint_failure_count)

        try:
            if state['position'] is not None:
                deadline = datetime.fromisoformat(state['position']['deadline'])
                remaining = (deadline - now).total_seconds()
                log.info(f'{_label(dtime(t.hour, t.minute))} tick: position open, strike='
                         f'{state["position"]["legs"]["CE"]["strike"]}, time-exit in {max(remaining, 0) / 60:.1f} min',
                         extra={'no_telegram': True})
                if now >= deadline:
                    _close_position(state, 'time_exit')
            elif state['checkpoint'] is not None and not state['checkpoint_used']:
                options, _future = _load_mcx_instruments()
                premium, _ce, _pe = _pinned_premium(options, state['checkpoint'])
                baseline = state['checkpoint']['premium']
                log.info(f'{_label(dtime(t.hour, t.minute))} tick: strike={state["checkpoint"]["strike"]} '
                         f'premium={premium:.2f} baseline={baseline:.2f} (Δ{premium - baseline:+.2f}, '
                         f'need >= +{JUMP_THRESHOLD_POINTS})', extra={'no_telegram': True})
                if premium - baseline >= JUMP_THRESHOLD_POINTS:
                    _enter_position(state, state['checkpoint'])
            else:
                log.info(f'{_label(dtime(t.hour, t.minute))} tick: no checkpoint pinned yet', extra={'no_telegram': True})
            tick_failure_count = 0
        except Exception:
            tick_failure_count += 1
            _log_failure_throttled('Tick processing failed', tick_failure_count)

        time_module.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    run_day()
