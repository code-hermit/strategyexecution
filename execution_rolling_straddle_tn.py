"""
TUESDAY - NIFTY - 5 lots

Live execution: short an ATM straddle on current-week options at 9:45, 10:45, 11:45, 12:45,
13:45 and 14:45, with a 30% stoploss on each leg. Any open positions are squared off by 15:13.

The underlying traded depends on the day of week (see UNDERLYINGS / WEEKDAY_UNDERLYING) -
NIFTY on Monday/Tuesday, SENSEX on Wednesday/Thursday. No trading on other days.

At every checkpoint:
  - Fetch the underlying's spot LTP from Dhan, round to the nearest strike -> desired ATM CE/PE.
  - Look up current-week option contracts from AliceBlue's contract master.
  - No open legs -> short both (CE and PE), each with a resting 25% SL (limit) buy order.
  - Open legs are at a different strike than desired (spot has moved) -> square off
    whatever is open (cancelling its resting SL first) and short fresh legs at the new strike.
  - Open legs already match the desired strike -> leave them; only re-short whichever leg
    isn't open anymore (i.e. its SL got hit and closed the position).

No MARKET (or SL-M) orders are used anywhere: entries and exits are LIMIT orders priced 1% through
the option's LTP (below LTP for SELL, above LTP for BUY), fetched fresh from Dhan's option chain
each checkpoint; the resting stoploss is an SL (stop-loss LIMIT) order, capped 1% beyond its
trigger price rather than a stop-loss market order.

Daily loss limit: local realized pnl (points, unscaled by lot size - matches
backtest_rolling_straddle.py's convention) plus the mark-to-market of whatever's still open is
checked at every checkpoint - once it crosses -DAILY_LOSS_LIMIT, every open leg is squared off and
no further checkpoints re-enter for the rest of the day. Unlike
execution_rolling_straddle_variation.py, this script has no between-checkpoint polling loop, so
this can only be as fine-grained as the checkpoint cadence above, not truly every minute the way
the backtest checks it.

Local per-leg state (entry price, strike, quantity) is what pnl tracking is built on; if the
process starts mid-day with positions already open that local state doesn't know about (a
restart), they're adopted at the next checkpoint using their *current* LTP as an approximate entry
price (this codebase's AliceBlue positions wrapper doesn't expose the original average fill
price) - logged loudly, since realized/unrealized pnl for that leg effectively restarts from then.

Talks to Dhan and AliceBlue directly over their REST APIs (no dhanhq/pya3 SDKs).
AliceBlue calls use the newer v3 "open-api" (not the legacy v2 AliceBlueAPIService REST API).
"""

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

from dhan_generate_access_token import generate_and_store_token

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

LOG_FILE = os.path.join(os.path.dirname(__file__), 'execution_rolling_straddle.log')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_TIMEOUT = 10

# The real console streams, captured before sys.stdout/sys.stderr get redirected into the logger
# below - anything that needs to report a problem with the logging/Telegram machinery itself must
# write here instead of to sys.stdout/sys.stderr, or it'll loop back into that same machinery.
_REAL_STDOUT = sys.stdout
_REAL_STDERR = sys.stderr


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
        print(f'Telegram alert failed: {exc}', file=_REAL_STDERR)


class TelegramHandler(logging.Handler):
    """Forwards every logged message (this script only logs at INFO and above, so that's
    everything - checkpoint decisions, order tags, status pings, warnings, uncaught exceptions,
    and now also raw stdout/stderr output - see StreamToLogger below) to Telegram, alongside the
    file/console handlers below. Guards its own body so a Telegram hiccup (bad token, network
    blip, rate limit) can never escape and interrupt whatever log.info()/log.warning() call
    triggered it - logging.Logger.callHandlers() does not itself guard handler exceptions, so an
    unguarded emit() here could crash the whole strategy."""

    def emit(self, record):
        try:
            _telegram_send(self.format(record))
        except Exception as exc:
            # Not self.handleError(record) - its default implementation writes to sys.stderr,
            # which by the time this runs is StreamToLogger, not the real terminal - that would
            # route straight back through this same handler and recurse. And even _REAL_STDERR
            # itself can be gone (see _SilentHandlerError below) - never let this print raise.
            try:
                print(f'TelegramHandler.emit failed: {exc}', file=_REAL_STDERR)
            except Exception:
                pass


class _SilentHandlerError:
    """Mixin: swallow a handler's own write failures instead of the default handleError(), which
    writes to sys.stderr - by the time this runs sys.stderr is StreamToLogger (see below), so the
    default behavior routes the failure back through this same logger -> back into this same
    broken handler -> fails again -> logs again -> ... (exactly what happened when an SSH session
    dropped mid-run: every console write failed with an I/O error, every failure's diagnostic
    went back through the logger, which tried the same dead console handler again). There's
    nothing more useful to do with a handler-level failure anyway - the other handlers (file,
    Telegram) still get the record fine; only the one broken sink drops that message."""

    def handleError(self, record):
        pass


class _ConsoleHandler(_SilentHandlerError, logging.StreamHandler):
    pass


class _FileHandler(_SilentHandlerError, logging.FileHandler):
    pass


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[_FileHandler(LOG_FILE), _ConsoleHandler(_REAL_STDOUT), TelegramHandler()],
)
log = logging.getLogger(__name__)

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    log.warning('TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set in .env - Telegram alerts disabled')


class _StreamToLogger:
    """Redirect target for sys.stdout/sys.stderr: any raw print() or third-party library output
    (warnings.warn, a stray print in a dependency, a traceback from a non-main thread that never
    goes through sys.excepthook) lands in execution_rolling_straddle.log - and gets Telegram-
    alerted - instead of only ever appearing on a console nobody's watching (e.g. nohup with no
    redirect). Buffers partial writes and only logs complete lines, since print() issues the
    message and its trailing newline as separate write() calls."""

    def __init__(self, level):
        self.level = level
        self._buffer = ''

    def write(self, text):
        self._buffer += text
        while '\n' in self._buffer:
            line, self._buffer = self._buffer.split('\n', 1)
            if line.strip():
                log.log(self.level, line)

    def flush(self):
        pass


sys.stdout = _StreamToLogger(logging.INFO)
sys.stderr = _StreamToLogger(logging.ERROR)


def _log_uncaught_exception(exc_type, exc_value, exc_tb):
    """So a crash (e.g. an unhandled order rejection) lands in the log file, not just the console."""
    log.critical('Uncaught exception', exc_info=(exc_type, exc_value, exc_tb))


sys.excepthook = _log_uncaught_exception


def _log_and_exit_on_signal(signum, frame):
    """sys.excepthook above only fires for uncaught Python exceptions - it never sees the process
    being killed by an OS signal (SSH session dropping -> SIGHUP, systemd/manual stop -> SIGTERM),
    which is exactly why a kill like that leaves zero trace in the log. Trap the catchable ones
    here so at least *why* it stopped is on record - SIGKILL (OOM killer, kill -9) can never be
    caught by any process, no way around that one."""
    log.critical(f'Received signal {signal.Signals(signum).name} ({signum}) - exiting')
    sys.exit(1)


for _sig in (signal.SIGTERM, signal.SIGHUP):
    signal.signal(_sig, _log_and_exit_on_signal)

DHAN_TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'dhan_token.json')
DHAN_BASE_URL = 'https://api.dhan.co/v2'
ALICEBLUE_TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'aliceblue_token.json')
ALICEBLUE_BASE_URL = 'https://a3.aliceblueonline.com/open-api/od/v1'
ALICEBLUE_CONTRACT_MASTER_URL = 'https://v2api.aliceblueonline.com/restpy/static/contract_master/V2/{exchange}'
REQUEST_TIMEOUT = 10
# cur_dtime=dtime(datetime.now().hour, datetime.now().minute)
CHECKPOINT_TIMES = [dtime(9, 45), dtime(10, 45), dtime(11, 45), dtime(12, 45), dtime(13, 45), dtime(14, 45)]
EXIT_TIME = dtime(15, 13)  # any open positions are squared off at (or immediately after) this time
STOPLOSS_PCT = 0.30
DAILY_LOSS_LIMIT = 100  # points, unscaled by lot size (matches backtest_rolling_straddle.py) -
# once realized+unrealized pnl crosses -this, square off and stop re-entering for the rest of the day
STATUS_PING_INTERVAL = timedelta(minutes=30)  # periodic "still waiting" log+Telegram alert during
# the (up to ~1 hour) gaps between checkpoints, so a long wait doesn't read as the process having died
SCREEN_TICK_INTERVAL = 30  # seconds - just prints the time straight to the console during a long
# wait (not logged, not Telegrammed - would be way too noisy at this cadence). Keeps the terminal
# visibly alive for anyone watching it, and the steady trickle of output can also help stop an
# idle SSH session from being dropped by a NAT/firewall/proxy timeout in the first place.
DRY_RUN = os.getenv('DRY_RUN', 'true').lower() != 'false'  # set DRY_RUN=false to place real orders

# Per-underlying config, keyed by symbol as it appears in the AliceBlue contract master.
UNDERLYINGS = {
    'NIFTY': dict(
        dhan_security_id=13, dhan_segment='IDX_I', aliceblue_exchange='NFO',
        strike_interval=50, lots=5,
    ),
    'SENSEX': dict(
        dhan_security_id=51, dhan_segment='IDX_I', aliceblue_exchange='BFO',
        strike_interval=100, lots=5,
    ),
}
# datetime.weekday(): Monday=0 ... Sunday=6. Days not listed here don't trade.
WEEKDAY_UNDERLYING = {0: 'NIFTY', 1: 'NIFTY', 2: 'SENSEX', 3: 'SENSEX'}

FILL_POLL_TIMEOUT = 10
FILL_POLL_INTERVAL = 1
TERMINAL_ORDER_STATUSES = {'complete', 'rejected', 'cancelled'}
LIMIT_OFFSET_PCT = 0.05  # limit price offset from LTP: below LTP for SELL orders, above LTP for BUY orders -
# wide enough to fill through a normal intraday move immediately (the broker doesn't allow true
# MARKET orders, so this is standing in for one); too thin a gap risks the order sitting unfilled
# past FILL_POLL_TIMEOUT, which is what left a leg both unprotected and untracked in production


def todays_underlying():
    """Symbol to trade today (per WEEKDAY_UNDERLYING) and its config, or (None, None) on off days."""
    symbol = WEEKDAY_UNDERLYING.get(datetime.now().weekday())
    return (symbol, UNDERLYINGS[symbol]) if symbol else (None, None)


def _sleep(seconds, label):
    """Sleep in SCREEN_TICK_INTERVAL-sized chunks rather than one long time_module.sleep() call:
      - every tick, print the current time straight to the real console (bypassing logging/
        Telegram entirely - see _REAL_STDOUT) so the terminal keeps showing activity.
      - every STATUS_PING_INTERVAL, additionally log (and therefore Telegram-alert - see
        TelegramHandler above) a proper status line, so a long gap between checkpoints doesn't
        read as the process having gone quiet."""
    end = time_module.time() + seconds
    next_status_ping = time_module.time() + STATUS_PING_INTERVAL.total_seconds()
    while True:
        remaining = end - time_module.time()
        if remaining <= 0:
            return
        time_module.sleep(min(remaining, SCREEN_TICK_INTERVAL))
        now = time_module.time()
        remaining = end - now
        if remaining <= 0:
            return
        print(f'{datetime.now():%H:%M:%S} still {label} - about {remaining / 60:.0f} min left', file=_REAL_STDOUT, flush=True)
        if now >= next_status_ping:
            log.info(f'still waiting {label} - about {remaining / 60:.0f} min left')
            next_status_ping += STATUS_PING_INTERVAL.total_seconds()


class Instrument(NamedTuple):
    token: int
    symbol: str
    name: str
    lot_size: int
    tick_size: float
    exchange: str


# ── Dhan (REST) ──────────────────────────────────────────────────────────────
def _dhan_headers(access_token):
    return {
        'access-token': access_token,
        'client-id': os.getenv('DHAN_CLIENT_ID'),
        'Content-type': 'application/json',
        'Accept': 'application/json',
    }


def _load_dhan_token():
    with open(DHAN_TOKEN_FILE) as f:
        return json.load(f)['accessToken']


def _dhan_token_is_valid(access_token):
    resp = requests.get(f'{DHAN_BASE_URL}/fundlimit', headers=_dhan_headers(access_token), timeout=REQUEST_TIMEOUT)
    return resp.ok


def _valid_dhan_token():
    """Reuse the stored Dhan access token if it still works; otherwise refresh it."""
    if os.path.exists(DHAN_TOKEN_FILE):
        token = _load_dhan_token()
        if _dhan_token_is_valid(token):
            return token
        log.info('Stored Dhan token is invalid/expired, regenerating...')
    else:
        log.info('No stored Dhan token found, generating...')
    generate_and_store_token()
    return _load_dhan_token()


DHAN_ACCESS_TOKEN = _valid_dhan_token()


def get_spot_ltp(security_id, segment):
    resp = requests.post(
        f'{DHAN_BASE_URL}/marketfeed/ltp', headers=_dhan_headers(DHAN_ACCESS_TOKEN),
        json={segment: [security_id]}, timeout=REQUEST_TIMEOUT,
    )
    log.info(f'get_spot_ltp() response: {resp.status_code} {resp.text}')
    resp.raise_for_status()
    data = resp.json()['data']
    return float(data[segment][str(security_id)]['last_price'])


def atm_strike(spot, strike_interval):
    return round(spot / strike_interval) * strike_interval


def get_option_chain(expiry_date, security_id, segment):
    """expiry_date: 'YYYY-MM-DD'. Returns {'<strike>.000000': {'ce': {...}, 'pe': {...}}}."""
    resp = requests.post(
        f'{DHAN_BASE_URL}/optionchain', headers=_dhan_headers(DHAN_ACCESS_TOKEN),
        json={'UnderlyingScrip': security_id, 'UnderlyingSeg': segment, 'Expiry': expiry_date},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()['data']['oc']


def option_ltp(option_chain, strike, option_type):
    return float(option_chain[f'{strike:.6f}'][option_type.lower()]['last_price'])


# ── AliceBlue (REST, v3 open-api) ───────────────────────────────────────────
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


# Status codes AliceBlue returns instead of 'Ok' to mean "the query is fine, there's just nothing
# to return" - e.g. /positions returns EC920 rather than an empty list when there are no open
# positions. Treat these as an empty result rather than an error.
_ALICEBLUE_EMPTY_RESULT_STATUSES = {'EC920'}


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


# ── Instruments (AliceBlue contract master) ─────────────────────────────────
def load_current_week_options(symbol, exchange):
    """Current-week option contracts for `symbol` (e.g. 'NIFTY', 'SENSEX') on `exchange`
    (e.g. 'NFO', 'BFO'). Filtered by option_type rather than instrument_type since that field
    differs across exchanges (NFO uses 'OPTIDX', BFO index options use 'IO').

    The contract master can keep listing an expiry after it's actually expired (e.g. still showing
    yesterday's weekly as the nearest one the next morning) - expiry_date is UTC midnight and
    trading hours fall entirely within the same UTC calendar day as IST, so today's date is
    excluded only once it's genuinely gone."""
    resp = requests.get(ALICEBLUE_CONTRACT_MASTER_URL.format(exchange=exchange), timeout=30)
    resp.raise_for_status()
    today = datetime.now(timezone.utc).date()
    opts = [
        c for c in resp.json()[exchange]
        if c['symbol'] == symbol and c['option_type'] in ('CE', 'PE')
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


# ── Positions (AliceBlue) ────────────────────────────────────────────────────
def get_open_legs(contracts_by_token):
    """token -> position dict, restricted to currently open (nonzero net qty) legs among this
    week's option contracts for the current underlying - so a stray position from another
    strategy/expiry/underlying is never touched."""
    positions = _aliceblue_get('/positions')
    return {
        int(p['instrumentId']): p for p in positions
        if int(p['instrumentId']) in contracts_by_token and int(p.get('netQuantity', 0)) != 0
    }


# ── Orders ────────────────────────────────────────────────────────────────
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


class OrderNotFilledError(RuntimeError):
    """Raised by _wait_for_fill_price when an order never filled - the caller (short_leg_with_
    stoploss) must not treat the leg as entered: no state to record, no stoploss to place."""


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

    # No fill seen within FILL_POLL_TIMEOUT - don't just raise and walk away (that's exactly what
    # left an order both unprotected and untracked once already: the caller aborts, but the order
    # can still fill moments later at the broker with nothing here to place its stoploss or record
    # its state). Cancel it, then check once more - the cancel and a last-second fill can race, so
    # "we cancelled it" alone doesn't prove no position exists.
    log.warning(f'order {broker_order_id} not filled within {FILL_POLL_TIMEOUT}s - cancelling and reconciling')
    try:
        _cancel_order(broker_order_id)
    except Exception as exc:
        log.critical(
            f'order {broker_order_id}: fill-wait timed out AND cancel failed ({exc}) - fill status '
            f'unknown, MANUAL CHECK REQUIRED at the broker'
        )
        raise OrderNotFilledError(f'order {broker_order_id}: cancel failed after timeout, status unknown - check broker manually') from exc

    for o in _order_book():
        if o.get('brokerOrderId') != broker_order_id:
            continue
        status = str(o.get('orderStatus', '')).lower()
        if status == 'complete':
            log.warning(f'order {broker_order_id}: filled right as the cancel was sent - honoring the fill')
            return float(o.get('averageTradedPrice') or 0)
        if status in ('cancelled', 'rejected'):
            raise OrderNotFilledError(f'order {broker_order_id}: cancelled after {FILL_POLL_TIMEOUT}s with no fill - no position taken')

    # cancel call returned without error, but the order book doesn't show a terminal status yet -
    # genuinely ambiguous; don't guess either way.
    log.critical(f'order {broker_order_id}: cancel sent but status still unclear on reconciliation - MANUAL CHECK REQUIRED at the broker')
    raise OrderNotFilledError(f'order {broker_order_id}: status unclear after cancel - check broker manually')


def short_leg_with_stoploss(instrument, quantity, ltp):
    entry_price = _round_to_tick(ltp * (1 - LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}SELL {quantity} x {instrument.name} LIMIT @ {entry_price} (ltp {ltp})'
    log.info(tag)
    if DRY_RUN:
        return

    entry = _place_order(
        'SELL', instrument, quantity, 'LIMIT', price=str(entry_price), order_tag='rolling_straddle_entry',
    )
    order_no = entry.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} entry order rejected: {entry}')

    entry_price = _wait_for_fill_price(order_no)
    trigger_price = round(entry_price * (1 + STOPLOSS_PCT), 1)
    sl_limit_price = _round_to_tick(trigger_price * (1 + LIMIT_OFFSET_PCT), instrument.tick_size)
    log.info(f'{instrument.name} entered @ {entry_price}, SL trigger {trigger_price} limit {sl_limit_price}')

    _place_order(
        'BUY', instrument, quantity, 'SL', price=str(sl_limit_price),
        trigger_price=trigger_price, order_tag='rolling_straddle_sl',
    )


def close_leg(instrument, quantity, ltp):
    exit_price = _round_to_tick(ltp * (1 + LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if DRY_RUN else ""}BUY (square off) {quantity} x {instrument.name} LIMIT @ {exit_price} (ltp {ltp})'
    log.info(tag)
    if DRY_RUN:
        return

    for o in _order_book():
        if str(o.get('instrumentId')) == str(instrument.token) and str(o.get('orderStatus', '')).lower() not in TERMINAL_ORDER_STATUSES:
            _cancel_order(o['brokerOrderId'])

    _place_order(
        'BUY', instrument, quantity, 'LIMIT', price=str(exit_price), order_tag='rolling_straddle_exit',
    )


# ── Local per-leg state / pnl (for DAILY_LOSS_LIMIT - see module docstring) ────────────────────
def _new_state():
    return {opt: None for opt in ('CE', 'PE')}  # None, or {'instrument','strike','entry_price','quantity'}


def _enter_leg(state, opt, instrument, ltp, strike, cfg):
    quantity = instrument.lot_size * cfg['lots']
    short_leg_with_stoploss(instrument, quantity, ltp)
    state[opt] = dict(instrument=instrument, strike=strike, entry_price=ltp, quantity=quantity)


def _adopt_untracked_legs(symbol, state, open_legs, contracts_by_token, option_chain):
    """Broker shows a leg open that local state doesn't know about (process restarted mid-day with
    positions already open) - adopt it so rolling/re-entry and pnl tracking keep working, using its
    *current* LTP as an approximate entry price (see module docstring: no way to recover the actual
    fill price from this AliceBlue endpoint). Loud warning since pnl bookkeeping for that leg
    effectively restarts from now."""
    tracked_tokens = {leg['instrument'].token for leg in state.values() if leg is not None}
    for token, pos in open_legs.items():
        if token in tracked_tokens:
            continue
        contract = contracts_by_token[token]
        opt = contract['option_type']
        strike = int(float(contract['strike_price']))
        try:
            ltp = option_ltp(option_chain, strike, opt)
        except (KeyError, TypeError):
            ltp = 0.0
        log.warning(
            f'{symbol} {opt} {contract["trading_symbol"]} open at the broker but not tracked '
            f'locally (restart with a position already open?) - adopting at ~{ltp}, pnl bookkeeping '
            f'for this leg restarts from now'
        )
        state[opt] = dict(
            instrument=_to_instrument(contract), strike=strike, entry_price=ltp,
            quantity=abs(int(pos['netQuantity'])),
        )


def _close_state_leg(state, opt, exit_ltp):
    """Realize pnl for `opt` against local state and clear it - call only once the broker-side
    close has actually been placed."""
    leg = state[opt]
    pnl = leg['entry_price'] - exit_ltp
    state[opt] = None
    return pnl


def _check_daily_loss_limit(symbol, cfg, state, day, option_chain):
    unrealized = 0.0
    for opt, leg in state.items():
        if leg is None:
            continue
        try:
            current_ltp = option_ltp(option_chain, leg['strike'], opt)
        except (KeyError, TypeError):
            continue
        unrealized += leg['entry_price'] - current_ltp

    if day['realized_pnl'] + unrealized > -DAILY_LOSS_LIMIT:
        return

    log.critical(
        f'{symbol}: DAILY LOSS LIMIT hit (realized {day["realized_pnl"]:.2f} + unrealized '
        f'{unrealized:.2f} <= -{DAILY_LOSS_LIMIT}) - halting for the day, squaring off'
    )
    for opt, leg in list(state.items()):
        if leg is None:
            continue
        try:
            exit_ltp = option_ltp(option_chain, leg['strike'], opt)
        except (KeyError, TypeError):
            log.warning(f'{opt} {leg["strike"]}: no live quote to close against, leaving position open')
            continue
        close_leg(leg['instrument'], leg['quantity'], exit_ltp)
        day['realized_pnl'] += _close_state_leg(state, opt, exit_ltp)
    day['halted'] = True


# ── Checkpoint ────────────────────────────────────────────────────────────
def run_checkpoint(symbol, cfg, state, day):
    spot = get_spot_ltp(cfg['dhan_security_id'], cfg['dhan_segment'])
    strike = atm_strike(spot, cfg['strike_interval'])
    log.info(f'{datetime.now():%H:%M:%S} {symbol} spot={spot} atm_strike={strike} day_pnl={day["realized_pnl"]:.2f}')

    contracts = load_current_week_options(symbol, cfg['aliceblue_exchange'])
    contracts_by_token = {int(c['token']): c for c in contracts}
    contracts_by_strike_type = {(int(float(c['strike_price'])), c['option_type']): c for c in contracts}
    expiry_date = datetime.fromtimestamp(contracts[0]['expiry_date'] / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
    option_chain = get_option_chain(expiry_date, cfg['dhan_security_id'], cfg['dhan_segment'])

    desired = {
        opt: _to_instrument(contracts_by_strike_type[(strike, opt)])
        for opt in ('CE', 'PE')
    }
    desired_ltp = {opt: option_ltp(option_chain, strike, opt) for opt in ('CE', 'PE')}
    desired_tokens = {inst.token for inst in desired.values()}

    open_legs = get_open_legs(contracts_by_token)
    open_tokens = set(open_legs)
    _adopt_untracked_legs(symbol, state, open_legs, contracts_by_token, option_chain)

    if not open_tokens:
        for opt, inst in desired.items():
            _enter_leg(state, opt, inst, desired_ltp[opt], strike, cfg)
    elif open_tokens - desired_tokens:
        # ATM strike has moved - close whatever's open and re-enter fresh at the new strike
        for token, pos in open_legs.items():
            contract = contracts_by_token[token]
            opt = contract['option_type']
            close_ltp = option_ltp(option_chain, int(float(contract['strike_price'])), opt)
            close_leg(_to_instrument(contract), abs(int(pos['netQuantity'])), close_ltp)
            if state[opt] is not None:
                day['realized_pnl'] += _close_state_leg(state, opt, close_ltp)
        for opt, inst in desired.items():
            _enter_leg(state, opt, inst, desired_ltp[opt], strike, cfg)
    else:
        # same strike as before - only re-enter whichever leg's SL closed it
        for opt, inst in desired.items():
            if inst.token in open_tokens:
                log.info(f'{inst.name} still open, leaving as is')
            else:
                _enter_leg(state, opt, inst, desired_ltp[opt], strike, cfg)

    _check_daily_loss_limit(symbol, cfg, state, day, option_chain)


def exit_all_positions(symbol, cfg):
    """Square off every open leg for today's underlying - cancels each leg's resting SL first,
    same as the strike-roll path in run_checkpoint, but exits everything instead of re-entering."""
    contracts = load_current_week_options(symbol, cfg['aliceblue_exchange'])
    contracts_by_token = {int(c['token']): c for c in contracts}
    open_legs = get_open_legs(contracts_by_token)
    if not open_legs:
        log.info(f'No open {symbol} positions to exit')
        return

    expiry_date = datetime.fromtimestamp(contracts[0]['expiry_date'] / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
    option_chain = get_option_chain(expiry_date, cfg['dhan_security_id'], cfg['dhan_segment'])
    for token, pos in open_legs.items():
        contract = contracts_by_token[token]
        ltp = option_ltp(option_chain, int(float(contract['strike_price'])), contract['option_type'])
        close_leg(_to_instrument(contract), abs(int(pos['netQuantity'])), ltp)


def run_day():
    symbol, cfg = todays_underlying()
    if not symbol:
        log.info(f"{datetime.now():%A} isn't a trading day for this strategy (see WEEKDAY_UNDERLYING) - not trading")
        return
    log.info(f'Trading {symbol} today')

    state = _new_state()
    day = {'realized_pnl': 0.0, 'halted': False}

    # Fresh start (e.g. the process restarted mid-day) after the day's first checkpoint, with no
    # open positions yet - don't skip straight to waiting for the next checkpoint; place the
    # initial straddle right now even if one or more entry times have already elapsed. Before the
    # first checkpoint (a normal on-time start), just fall through to the loop below and wait.
    # (If positions ARE already open at this point, they're left for the next real checkpoint to
    # pick up - run_checkpoint's _adopt_untracked_legs call handles bringing them into local state.)
    if datetime.now().time() >= CHECKPOINT_TIMES[0]:
        contracts = load_current_week_options(symbol, cfg['aliceblue_exchange'])
        contracts_by_token = {int(c['token']): c for c in contracts}
        if not get_open_legs(contracts_by_token):
            log.info('No open positions found after the day\'s first checkpoint - placing initial orders now')
            run_checkpoint(symbol, cfg, state, day)

    for checkpoint in CHECKPOINT_TIMES:
        if day['halted']:
            log.info(f'{symbol}: daily loss limit already hit - skipping remaining checkpoints')
            break
        now = datetime.now()
        target = datetime.combine(now.date(), checkpoint)
        wait = (target - now).total_seconds()
        if wait > 0:
            log.info(f'waiting until {checkpoint} ...')
            _sleep(wait, f'for the {checkpoint} checkpoint')
        elif wait < -60:
            log.info(f'missed {checkpoint} (already {-wait:.0f}s past), skipping')
            continue
        run_checkpoint(symbol, cfg, state, day)

    # Exit is safety-critical, unlike the entry checkpoints above: always run it, even if
    # EXIT_TIME has already passed (e.g. a late start) or the day was already halted (harmless -
    # exit_all_positions is a no-op once everything's already flat) - rather than skipping.
    now = datetime.now()
    target = datetime.combine(now.date(), EXIT_TIME)
    wait = (target - now).total_seconds()
    if wait > 0 and not day['halted']:
        log.info(f'waiting until {EXIT_TIME} to exit any open positions...')
        _sleep(wait, f'to exit at {EXIT_TIME}')
    elif not day['halted']:
        log.info(f'{EXIT_TIME} already passed ({-wait:.0f}s ago) - exiting any open positions now')
    exit_all_positions(symbol, cfg)
    log.info(f'{symbol}: day pnl {day["realized_pnl"]:+.2f} points (realized)')


if __name__ == '__main__':
    run_day()
