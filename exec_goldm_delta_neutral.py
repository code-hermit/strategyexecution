"""
Live execution of "mcx trading/goldm_strangle_delta_neutral.py" on MCX GOLDMINI (GOLDM), 1 lot: a short
ATM straddle closed when its combined delta crosses zero, with a per-leg stop and target and hourly
checkpoints. Market data from Zerodha Kite (Redis feed from mcx_ticker_service.py, REST fallback),
orders through AliceBlue - same split and broker plumbing as exec_rsv_goldm_target.py (copied, not
imported - every live script in this folder is self-contained).

Rules (backtested as --otm-strikes 0 --leg-sl-pct 10 --leg-target-pct 20 --leg-trail-pct 0
--checkpoint-minutes 60 --cost-stop-after-sl):
  - Checkpoints every CHECKPOINT_INTERVAL (1 h) from ENTRY_TIME (15:15): 15:15, 16:15 ... 22:15. Each
    checkpoint window allows ONE straddle: at the first poll while flat in a window that hasn't had
    one yet, short the ATM CE + PE (ATM = strike nearest the option chain's underlying future) -
    normally right at the checkpoint. A straddle still open at a checkpoint is held, and that
    window's straddle then opens as soon as it closes (if still inside the window). Once a window
    has had its straddle, we stay flat after it closes until the next checkpoint.
  - Per leg (each leg on its own - the other leg keeps running):
      * 10% stop: a resting broker SL BUY, trigger entry * 1.10 (limit SL_LIMIT_OFFSET_PCT above),
        placed right after the entry fills. It is also the dead man's switch if this process dies.
        If the LTP sits above the trigger for LIMIT_WAIT_SECONDS without the SL filling (SL-limit
        jumped over), the SL is cancelled and the leg closed with a marketable LIMIT chase.
      * 20% target: LTP <= entry * 0.80 (checked every poll) -> buy that leg back.
      * Cost stop (COST_STOP_AFTER_SL): once one leg's 10% stop has filled, the other leg's stop
        moves to its own entry price - its resting SL is modified to trigger there (cancel + place
        new if the modify is refused); if its LTP is already at/above cost it is closed at once.
        Hitting it is a COST_STOP exit. Its 20% target is unchanged.
  - Delta zero (while both legs are open): every DELTA_CHECK_SECONDS (15 s), the straddle's combined delta (CE + PE,
    Black-76 on the future, each leg's IV backed out of its live LTP, r = 0) is computed; its sign
    at the first reading after entry is the side it starts on. When it reaches zero / flips sign,
    both legs are closed.
  - daily_loss_limit_rs (realized + unrealized) closes everything and stops for the day - a safety
    net, not in the backtest (its worst day was about -6,300 Rs at 1 lot).
  - EXIT_TIME (23:00): close whatever is open. NRML (LONGTERM) positions are never auto-squared-off
    by the broker, so this square-off is the only thing closing them.
  - The chain's expiry day is not traded (MCX options devolve into futures; the backtest has no
    expiry-day data either).

Orders: every entry and exit is a LIMIT at the live LTP; if it hasn't filled within
LIMIT_WAIT_SECONDS (15 s) it is "converted to market" - AliceBlue refuses MARKET orders on these
options, so instead it is repriced to a marketable LIMIT (LTP +/- 5%, i.e. through the book so it
fills at once), widened 5% every CHASE_WAIT_SECONDS up to 15% (inside AliceBlue's ~20% MCX price
band) until it fills.

Rs amounts are for CFG's lots: Rs = points x lots x UNITS_PER_LOT (GOLDM is quoted per 10 g and a
lot is 100 g, so 1 point = Rs 10 per lot; AliceBlue's MCX order quantity is in lots).

Orders are tagged 'dnG' + option (C/P) + role (E entry / S stop / X exit) + HHMM, e.g. 'dnGCE1515'.
Do NOT run this alongside exec_rsv_goldm.py / exec_rsv_goldm_target.py on the same account:
AliceBlue's positions endpoint can't tell the strategies' GOLDM legs apart.

On startup with GOLDM positions already open on the current chain (mid-day restart), they are
adopted as the current straddle (entry prices from the order book, LTP fallback; its resting SL
found by tag or placed fresh).

Run (DRY_RUN=false in the env or .env to place real orders; anything else simulates fills at LTP):
    python exec_goldm_delta_neutral.py [GOLDM] [weekday codes, e.g. mtwhf]
Logs:
  - exec_goldm_delta_neutral_<SYMBOL>.log + stdout: the readable story ([DECISION] / [EXEC] lines
    mirror the event logs below, minus the high-frequency ones); lifecycle events and WARNING+ to
    Telegram.
  - logs/exec_goldm_delta_neutral_<SYMBOL>_decisions_<YYYYMMDD>.jsonl - every strategy decision and
    the numbers it was made on: day start/skip, checkpoint entries (spot, ATM, leg LTPs), every
    delta check (future, time to expiry, each leg's LTP / IV / delta, decision), stop and target
    triggers, exits, straddle summaries, loss limit, adoption, heartbeats.
  - logs/exec_goldm_delta_neutral_<SYMBOL>_execution_<YYYYMMDD>.jsonl - every broker call: place /
    modify / cancel requests with response and latency, failures, order-status changes seen in the
    order book (with the raw order), limit timeouts, each marketable reprice, and every fill with its
    reference LTP, slippage, stage and time to fill; reconciliations.
  - logs/exec_goldm_delta_neutral_<SYMBOL>_trades.csv - one row per closed leg (backtest columns plus
    reference LTPs), cumulative across days, for live-vs-backtest comparison.
Every JSONL record has ts, event and dry_run; read them with pandas.read_json(path, lines=True).
"""

import csv
import io
import itertools
import json
import logging
import math
import os
import signal
import subprocess
import sys
import threading
import time as time_module
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import NamedTuple

import requests
from dotenv import load_dotenv

import zerodha_ltp_client

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# ── Logging / alerts ─────────────────────────────────────────────────────────────────────────────
_ARGV_SYMBOL = sys.argv[1].upper() if len(sys.argv) > 1 and sys.argv[1] else 'GOLDM'

LOG_FILE = os.path.join(os.path.dirname(__file__), f'exec_goldm_delta_neutral_{_ARGV_SYMBOL}.log')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_TIMEOUT = 10

log = logging.getLogger(f'exec_goldm_delta_neutral.{_ARGV_SYMBOL}')
log.setLevel(logging.INFO)
log.propagate = False
_formatter = logging.Formatter(f'%(asctime)s %(levelname)s [{_ARGV_SYMBOL} dn] %(message)s')
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

# ── Event logs: strategy decisions + broker execution (JSONL, one file per kind per day) ─────────
EVENT_LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
TRADES_CSV = os.path.join(EVENT_LOG_DIR, f'exec_goldm_delta_neutral_{_ARGV_SYMBOL}_trades.csv')
TRADES_CSV_FIELDS = ['date', 'straddle', 'option_type', 'strike', 'symbol', 'entry_time', 'entry_price', 'entry_ref_ltp',
                     'entry_slip_pts', 'exit_time', 'exit_price', 'exit_ref_ltp', 'exit_slip_pts', 'pts', 'lots', 'pnl_rs',
                     'reason', 'dry_run']  # slip: points against us vs the reference (decision LTP; a broker stop's trigger)


def _fmt(value):
    return round(value, 4) if isinstance(value, float) else value


class _EventLog:
    """Append-only JSONL event stream ('decisions' or 'execution'): {ts, event, dry_run, **fields}
    per line. Thread-safe (legs are worked in parallel). Each record is mirrored to the main log as
    '[LABEL] event k=v ...' unless echo=False (high-frequency records, e.g. the 15 s delta checks,
    or ones an alert() already reports). Never raises - logging must not break trading."""

    def __init__(self, kind, label):
        self.kind, self.label = kind, label
        self._lock = threading.Lock()

    def __call__(self, event, echo=True, level=logging.INFO, **fields):
        now = datetime.now()
        try:
            line = json.dumps({'ts': now.isoformat(timespec='milliseconds'), 'event': event, 'dry_run': DRY_RUN, **fields}, default=str)
            path = os.path.join(EVENT_LOG_DIR, f'exec_goldm_delta_neutral_{_ARGV_SYMBOL}_{self.kind}_{now:%Y%m%d}.jsonl')
            with self._lock:
                os.makedirs(EVENT_LOG_DIR, exist_ok=True)
                with open(path, 'a') as f:
                    f.write(line + '\n')
        except Exception as exc:
            print(f'{self.kind} event log write failed: {exc}', file=sys.stderr)
        if echo:
            try:
                log.log(level, f'[{self.label}] {event} ' + ' '.join(f'{k}={_fmt(v)}' for k, v in fields.items()),
                        extra={'no_telegram': True})
            except Exception as exc:
                print(f'{self.kind} event log mirror failed: {exc}', file=sys.stderr)


DECISION = _EventLog('decisions', 'DECISION')
EXECUTION = _EventLog('execution', 'EXEC')
_trades_csv_lock = threading.Lock()


def _append_trade_row(row):
    try:
        with _trades_csv_lock:
            os.makedirs(EVENT_LOG_DIR, exist_ok=True)
            new_file = not os.path.exists(TRADES_CSV)
            with open(TRADES_CSV, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=TRADES_CSV_FIELDS)
                if new_file:
                    writer.writeheader()
                writer.writerow(row)
    except Exception as exc:
        log.warning(f'trades CSV write failed: {exc}', extra={'no_telegram': True})


def _ms_since(t0):
    return round((time_module.time() - t0) * 1000)

# ── Strategy config ──────────────────────────────────────────────────────────────────────────────
WARMUP_TIME = dtime(15, 14)  # a minute ahead of ENTRY_TIME, so instrument/contract caches and the
# Redis feed are hot by the first checkpoint - no orders during warm-up.
ENTRY_TIME = dtime(15, 15)  # first checkpoint
CHECKPOINT_INTERVAL = timedelta(hours=1)  # one straddle per window: 15:15, 16:15, ... 22:15
EXIT_TIME = dtime(23, 0)  # the backtest's exit - 30 min inside MCX's 23:30 close
EXPIRY_CLOSE_TIME = dtime(23, 30)  # option expiry instant for the delta's time to expiry (as in the backtest)
LEG_SL_PCT = 0.10  # per-leg stop: resting SL BUY at entry * (1 + LEG_SL_PCT)
LEG_TARGET_PCT = 0.20  # per-leg target: buy back once LTP <= entry * (1 - LEG_TARGET_PCT)
COST_STOP_AFTER_SL = True  # once one leg's stop fills, the other leg's stop moves to its entry (cost)
POLL_INTERVAL_SECONDS = 5  # stop/target/loss-limit cadence
DELTA_CHECK_SECONDS = 15  # delta-zero cadence, on clock slots (:00/:15/:30/:45) - the backtest checks once a minute
HEARTBEAT_INTERVAL = timedelta(minutes=30)
WARMUP_POLL_SECONDS = 2
RECONCILE_INTERVAL = timedelta(minutes=5)  # how often broker positions are compared with what this process thinks it holds

OPTION_TYPES = ('CE', 'PE')
DAY_CODE_TO_WEEKDAY = {'m': 'Monday', 't': 'Tuesday', 'w': 'Wednesday', 'h': 'Thursday', 'f': 'Friday'}
UNITS_PER_LOT = 10  # GOLDM: price per 10 g, lot 100 g -> 1 point = Rs 10 per lot

CFG = {
    'GOLDM': dict(
        strike_interval=500, lots=1, aliceblue_exchange='MCX', zerodha_options_exchange='MCX',
        daily_loss_limit_rs=10000,
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
LIMIT_WAIT_SECONDS = 15  # a LIMIT at LTP gets this long to fill before it's repriced to be marketable
# AliceBlue refuses MARKET orders on these options - "market" is a LIMIT priced through the LTP:
LIMIT_OFFSET_PCT = 0.05  # first marketable reprice this far through LTP ...
CHASE_OFFSET_STEP = 0.05  # ... widened by this ...
CHASE_WAIT_SECONDS = 2  # ... every this many seconds ...
CHASE_MAX_OFFSET_PCT = 0.15  # ... up to this (under AliceBlue's ~20%-from-LTP MCX price band)
CHASE_MAX_STEPS = 30  # ~60 s of repricing; then the order is cancelled and the caller retries / reports
ALICEBLUE_PRODUCT = 'LONGTERM'  # = NRML - AliceBlue blocks MIS/INTRADAY on MCX GOLDM options (see exec_rsv_goldm.py)
TERMINAL_ORDER_STATUSES = {'complete', 'rejected', 'cancelled'}
_ALICEBLUE_EMPTY_RESULT_STATUSES = {'EC920'}
SL_LIMIT_OFFSET_PCT = 0.02  # SL limit above its trigger - stays inside the ~20% LTP band
# AliceBlue's MCX quantity units are inconsistent: /orders/placeorder takes LOTS (1 = one GOLDM lot), but
# /orders/modify and /positions (netQuantity) and the order book's quantity are in UNITS - 100 per GOLDM
# lot (100 g). Seen live 8 Oct 2026: modify with quantity 1 -> EC954 "'quantity' should be a multiple of
# the lot size"; a 1-lot short showed netQuantity -100. Leg quantities in this file are always LOTS.
ALICEBLUE_UNITS_PER_LOT = 100


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


_seen_order_status = {}  # order id -> (status, filled qty) last written to the execution log


def _note_order_status(o):
    """Execution-log an order's status the first time it's seen and on every change."""
    order_id, status = o.get('brokerOrderId'), _status(o)
    key = (status, str(o.get('filledQuantity')))
    if _seen_order_status.get(order_id) == key:
        return
    _seen_order_status[order_id] = key
    EXECUTION('order_status', echo=status in TERMINAL_ORDER_STATUSES,
              level=logging.WARNING if status == 'rejected' else logging.INFO,
              order_id=order_id, status=status, tag=_tag_of(o), filled_qty=o.get('filledQuantity'),
              avg_price=o.get('averageTradedPrice'), reason=o.get('rejectionReason') or None, raw=o)


def _find_order(order_book, order_id):
    for o in order_book or []:
        if o.get('brokerOrderId') == order_id:
            _note_order_status(o)
            return o
    return None


# ── Order tags ──────────────────────────────────────────────────────────────────────────────────
# 'dn' + underlying + option (C/P) + role (E entry / S stop / X exit) + leg id (HHMM opened) - distinct
# from exec_rsv_goldm.py's 'rv...' and exec_rsv_goldm_target.py's 'rt...' tags.
_TAG_UNDERLYING = {'GOLDM': 'G'}
TAG_PREFIX = 'dn' + _TAG_UNDERLYING.get(_ARGV_SYMBOL, _ARGV_SYMBOL[:1])
TAG_ENTRY, TAG_SL, TAG_EXIT = 'E', 'S', 'X'


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
_REQUEST_SEQ = itertools.count(1)


def _logged_post(kind, path, payload, summary, ref_ltp=None):
    """POST to AliceBlue with the exact payload execution-logged BEFORE it is sent ('<kind>_request',
    file only - on record even if the call hangs or the process dies), and any error after
    ('<kind>_failed', then re-raised). The caller logs the response as '<kind>' with the same
    req_id. ref_ltp: the live LTP the order was priced off, for slippage. Returns (req_id, result, t0)."""
    req_id = next(_REQUEST_SEQ)
    EXECUTION(f'{kind}_request', echo=False, req_id=req_id, path=path, ref_ltp=ref_ltp, **summary, payload=payload)
    t0 = time_module.time()
    try:
        return req_id, _aliceblue_post(path, payload), t0
    except Exception as exc:
        EXECUTION(f'{kind}_failed', level=logging.WARNING, req_id=req_id, ref_ltp=ref_ltp, **summary, error=str(exc), ms=_ms_since(t0))
        raise


def _place_order(transaction_type, instrument, quantity, order_type, price='0', trigger_price=None, order_tag=None, ref_ltp=None):
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
    request = dict(side=transaction_type, symbol=instrument.name, token=instrument.token, qty=quantity,
                   order_type=order_type, price=price, trigger=trigger_price, tag=order_tag)
    req_id, result, t0 = _logged_post('place', '/orders/placeorder', payload, request, ref_ltp)
    result = result[0] if isinstance(result, list) and len(result) == 1 else result
    order_id = result.get('brokerOrderId') if isinstance(result, dict) else None
    EXECUTION('place', req_id=req_id, ref_ltp=ref_ltp, **request, order_id=order_id, response=result, ms=_ms_since(t0))
    return result


def _cancel_order(broker_order_id):
    req_id, result, t0 = _logged_post('cancel', '/orders/cancel', {'brokerOrderId': broker_order_id}, dict(order_id=broker_order_id))
    EXECUTION('cancel', req_id=req_id, order_id=broker_order_id, response=result, ms=_ms_since(t0))
    return result


def _modify_order(broker_order_id, quantity, order_type, price, trigger_price=None, ref_ltp=None):
    payload = {
        'brokerOrderId': broker_order_id,
        'quantity': quantity * ALICEBLUE_UNITS_PER_LOT,  # modify takes units, not lots (see ALICEBLUE_UNITS_PER_LOT)
        'orderType': order_type,
        'price': str(price),
        'slTriggerPrice': str(trigger_price) if trigger_price is not None else '',
        'validity': 'DAY',
    }
    request = dict(order_id=broker_order_id, qty=quantity, order_type=order_type, price=price, trigger=trigger_price)
    req_id, result, t0 = _logged_post('modify', '/orders/modify', payload, request, ref_ltp)
    EXECUTION('modify', req_id=req_id, ref_ltp=ref_ltp, **request, response=result, ms=_ms_since(t0))
    return result


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
            fill_price = float(o.get('averageTradedPrice') or 0) or None
            EXECUTION('cancel_result', order_id=order_id, label=label, status=status, fill_price=fill_price,
                      note='filled before the cancel landed')
            return status, fill_price
        if status in TERMINAL_ORDER_STATUSES or time_module.time() >= deadline:
            EXECUTION('cancel_result', level=logging.INFO if status in TERMINAL_ORDER_STATUSES else logging.WARNING,
                      order_id=order_id, label=label, status=status, fill_price=None,
                      note=None if status in TERMINAL_ORDER_STATUSES else f'not terminal after {CANCEL_CONFIRM_SECONDS}s')
            return status, None


def _limit_then_market(instrument, transaction_type, quantity, get_fresh_ltp, order_tag):
    """LIMIT order at the live LTP; if it hasn't filled within LIMIT_WAIT_SECONDS it is "converted to
    market" - AliceBlue refuses MARKET orders on these options, so _chase_fill reprices it to a
    marketable LIMIT through the LTP instead. Returns the average fill price; the fill (reference
    LTP, slippage against it, stage, time taken) goes to the execution log."""
    t0 = time_module.time()
    ltp = get_fresh_ltp()
    if ltp is None:
        raise RuntimeError(f'{instrument.name}: no LTP available to place {transaction_type} order')
    price = _round_to_tick(ltp, instrument.tick_size)
    order = _place_order(transaction_type, instrument, quantity, 'LIMIT', price=str(price), order_tag=order_tag, ref_ltp=ltp)
    order_no = order.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} {transaction_type} order rejected: {order}')
    fill_price = _poll_order_status(order_no, time_module.time() + LIMIT_WAIT_SECONDS)
    stage, steps = 'limit', 0
    if fill_price is None:
        EXECUTION('limit_timeout', level=logging.WARNING, symbol=instrument.name, side=transaction_type, tag=order_tag,
                  order_id=order_no, price=price, waited_s=LIMIT_WAIT_SECONDS, next='reprice to a marketable LIMIT')
        fill_price, order_no, steps = _chase_fill(instrument, transaction_type, quantity, get_fresh_ltp, order_tag, order_no)
        stage = 'marketable'
    slippage = (fill_price - ltp) if transaction_type == 'BUY' else (ltp - fill_price)
    EXECUTION('fill', symbol=instrument.name, side=transaction_type, tag=order_tag, order_id=order_no, ref_ltp=ltp,
              first_limit=price, fill_price=fill_price, slippage_pts=round(slippage, 2), stage=stage, chase_steps=steps,
              secs=round(time_module.time() - t0, 1))
    return fill_price


def _chase_fill(instrument, transaction_type, quantity, get_fresh_ltp, order_tag, order_no):
    """Reprice `order_no` (a resting LIMIT) to LTP +/- LIMIT_OFFSET_PCT, widening by CHASE_OFFSET_STEP
    every CHASE_WAIT_SECONDS up to CHASE_MAX_OFFSET_PCT (inside AliceBlue's ~20% MCX price band),
    until it fills - modify in place; cancel + place-new once a modify fails (see exec_rsv_goldm.py).
    Returns (fill price, the order that filled, reprice steps taken). Gives up after CHASE_MAX_STEPS:
    the order is cancelled (a fill that beat the cancel is returned) and RuntimeError raised - an exit
    is then retried next poll, an entry reported failed - so a chase never blocks the main loop or
    the 23:00 square-off."""
    sign = 1 if transaction_type == 'BUY' else -1
    offset_pct = LIMIT_OFFSET_PCT
    modify_broken = False
    step = 0
    while True:
        step += 1
        if step > CHASE_MAX_STEPS:
            status, fill_price = _cancel_and_confirm(order_no, f'{instrument.name} {transaction_type} chase give-up')
            if status == 'complete':
                return fill_price, order_no, step - 1
            EXECUTION('chase_gave_up', level=logging.ERROR, symbol=instrument.name, side=transaction_type, tag=order_tag,
                      order_id=order_no, steps=CHASE_MAX_STEPS, cancel_status=status)
            if status not in TERMINAL_ORDER_STATUSES:
                alert(f'{instrument.name}: {transaction_type} order {order_no} gave up after {CHASE_MAX_STEPS} reprices and its cancel '
                      f'is unconfirmed ({status!r}) - CHECK MANUALLY', level=logging.CRITICAL)
            raise RuntimeError(f'{instrument.name}: {transaction_type} not filled after {CHASE_MAX_STEPS} reprices - order cancelled')
        ltp = get_fresh_ltp()
        if ltp is None:
            raise RuntimeError(f'{instrument.name}: no LTP available to reprice {transaction_type} order')
        price = _round_to_tick(ltp * (1 + sign * offset_pct), instrument.tick_size)
        method = 'modify'
        if not modify_broken:
            try:
                _modify_order(order_no, quantity, 'LIMIT', price, ref_ltp=ltp)
            except Exception as exc:
                modify_broken = True
                log.warning(f'{instrument.name}: modify of order {order_no} failed ({exc}) - cancel+place-new from now on')
        if modify_broken:
            method = 'replace'
            status, fill_price = _cancel_and_confirm(order_no, f'{instrument.name} {transaction_type}')
            if status == 'complete':
                return fill_price, order_no, step
            if status not in TERMINAL_ORDER_STATUSES:
                log.warning(f'{instrument.name}: order {order_no} still {status!r} after cancel - polling it again, not placing another')
                fill_price = _poll_order_status(order_no, time_module.time() + CHASE_WAIT_SECONDS)
                if fill_price is not None:
                    return fill_price, order_no, step
                continue
            order = _place_order(transaction_type, instrument, quantity, 'LIMIT', price=str(price), order_tag=order_tag, ref_ltp=ltp)
            order_no = order.get('brokerOrderId')
            if not order_no:
                raise RuntimeError(f'{instrument.name} {transaction_type} order rejected: {order}')
        EXECUTION('chase_reprice', symbol=instrument.name, side=transaction_type, tag=order_tag, order_id=order_no,
                  step=step, ltp=ltp, offset_pct=offset_pct, price=price, method=method)
        fill_price = _poll_order_status(order_no, time_module.time() + CHASE_WAIT_SECONDS)
        if fill_price is not None:
            return fill_price, order_no, step
        offset_pct = min(offset_pct + CHASE_OFFSET_STEP, CHASE_MAX_OFFSET_PCT)


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


# ── Delta (Black-76 on the chain's underlying future, r = 0 - as in the backtest) ───────────────
def _ncdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _b76_price(F, K, T, sigma, opt):
    sd = sigma * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sd * sd) / sd
    d2 = d1 - sd
    if opt == 'CE':
        return F * _ncdf(d1) - K * _ncdf(d2)
    return K * _ncdf(-d2) - F * _ncdf(-d1)


def _implied_vol(price, F, K, T, opt):
    """Bisection on Black-76; None if the price is at/below intrinsic or above the 500% vol price."""
    intrinsic = max(F - K, 0.0) if opt == 'CE' else max(K - F, 0.0)
    if T <= 0 or price <= intrinsic + 1e-6:
        return None
    lo, hi = 1e-3, 5.0
    if _b76_price(F, K, T, hi, opt) < price:
        return None
    while hi - lo > 1e-6:
        mid = 0.5 * (lo + hi)
        if _b76_price(F, K, T, mid, opt) > price:
            hi = mid
        else:
            lo = mid
    return 0.5 * (lo + hi)


def _b76_delta(F, K, T, sigma, opt):
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * math.sqrt(T))
    return _ncdf(d1) if opt == 'CE' else _ncdf(d1) - 1.0


def _chain_expiry(symbol):
    return date.fromisoformat(_zerodha_options_cache[symbol]['options'][0]['expiry'])


def _straddle_delta(day, market, symbol):
    """Combined delta (CE + PE, long-option sign) of the open legs, each leg's IV backed out of its
    live LTP against the future. A leg whose IV can't be solved this time keeps its last delta.
    Returns (delta or None if a leg has none yet, detail dict for the decision log)."""
    F = market['spot']
    expiry_at = datetime.combine(_chain_expiry(symbol), EXPIRY_CLOSE_TIME)
    T = (expiry_at - datetime.now()).total_seconds() / (365 * 24 * 3600)
    info = dict(fut=F, t_days=round(T * 365, 4), legs=[])
    if not F or T <= 0:
        return None, info
    total = 0.0
    for opt, leg in day['legs'].items():
        p = market['price'].get((leg['strike'], opt))
        iv = None if p is None else _implied_vol(p, F, leg['strike'], T, opt)
        if iv is not None:
            leg['delta'] = _b76_delta(F, leg['strike'], T, iv, opt)
        info['legs'].append(dict(opt=opt, strike=leg['strike'], ltp=p, iv=None if iv is None else round(iv, 5),
                                 delta=None if leg.get('delta') is None else round(leg['delta'], 5), reused_last_delta=iv is None))
        if leg.get('delta') is None:
            return None, info
        total += leg['delta']
    return total, info


# ── Market snapshot ─────────────────────────────────────────────────────────────────────────────
def _fetch_market(symbol, cfg, day):
    """Spot/ATM, AliceBlue contracts, and live LTPs (Redis first, one batched REST fallback) for the
    ATM straddle and the open legs."""
    zerodha_options = _resilient_call(_load_zerodha_option_chain, symbol, cfg)
    spot = _resilient_call(get_spot_ltp, symbol, cfg)
    atm = atm_strike(spot, cfg['strike_interval'])

    contracts = _resilient_call(_load_aliceblue_contracts, symbol, cfg)
    contracts_by_token = {int(c['token']): c for c in contracts}
    contracts_by_strike_type = {(int(float(c['strike_price'])), c['option_type']): c for c in contracts}

    needed = {(atm, 'CE'), (atm, 'PE')}
    for opt, leg in day['legs'].items():
        needed.add((leg['strike'], opt))
    if day['strike'] is not None:
        needed.update({(day['strike'], 'CE'), (day['strike'], 'PE')})

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


# ── Legs ────────────────────────────────────────────────────────────────────────────────────────
def _fresh_ltp_fn(instrument, strike, opt, fallback_ltp):
    """Zero-arg live-LTP getter for a leg, used while working its orders. The price is read through
    the leg's KITE instrument (looked up by strike/type in today's Kite chain, as _fetch_market does)
    - AliceBlue's trading symbol (e.g. GOLDM29OCT26C149000) is not a Kite key (GOLDM26OCT149000CE).
    Redis first (mcx_ticker_service.py), REST fallback; if both fail, `fallback_ltp` (the snapshot
    the order started from) is returned and the fallback is execution-logged."""
    def _fresh_ltp():
        try:
            row = _zerodha_option_row(_zerodha_options_cache[instrument.symbol]['options'], strike, opt)
            key = f"{row['exchange']}:{row['tradingsymbol']}"
            token = int(row['instrument_token'])
            zerodha_ltp_client.register_subscription(token)
            ltp = zerodha_ltp_client.get_ltp(token, rest_fetch=lambda: _zerodha_quote_ltp([key])[key], log=log)
            if ltp is not None:
                return ltp
            error = 'no price returned'
        except Exception as exc:
            error = str(exc)
        EXECUTION('ltp_fallback', level=logging.WARNING, symbol=instrument.name, strike=strike, opt=opt,
                  fallback_ltp=fallback_ltp, error=error)
        return fallback_ltp
    return _fresh_ltp


def _short_leg(instrument, quantity, ltp, strike, opt, leg_id):
    """SELL to open: LIMIT at LTP, made marketable after LIMIT_WAIT_SECONDS. Returns the fill
    price (the LTP in DRY_RUN)."""
    tag = _order_tag(opt, TAG_ENTRY, leg_id)
    if DRY_RUN:
        EXECUTION('dry_fill', side='SELL', symbol=instrument.name, tag=tag, qty=quantity, fill_price=ltp)
        return ltp
    return _limit_then_market(instrument, 'SELL', quantity, _fresh_ltp_fn(instrument, strike, opt, ltp), tag)


def _leg_stop_trigger(entry_price):
    # whole rupees - MCX rejects other SL prices as "STOP PRICE IS NOT REASONABLE" (see exec_rsv_goldm.py)
    return round(entry_price * (1 + LEG_SL_PCT))


def _cost_trigger(entry_price):
    return round(entry_price)  # whole rupees, as above


def _place_leg_stop(instrument, quantity, entry_price, opt, leg_id):
    """The leg's 10% stop, resting at the broker: SL BUY, trigger entry * (1 + LEG_SL_PCT), limit
    SL_LIMIT_OFFSET_PCT above. Doubles as the dead man's switch. Returns the broker order id, or
    None (DRY_RUN - the stop is then checked here on LTP - or placement failed: CRITICAL alert)."""
    trigger = _leg_stop_trigger(entry_price)
    limit = round(trigger * (1 + SL_LIMIT_OFFSET_PCT))
    if DRY_RUN:
        EXECUTION('dry_stop_order', side='BUY', symbol=instrument.name, tag=_order_tag(opt, TAG_SL, leg_id), qty=quantity,
                  trigger=trigger, limit=limit, entry=entry_price, note='not placed - stop checked on LTP')
        return None
    try:
        order = _place_order('BUY', instrument, quantity, 'SL', price=str(limit), trigger_price=trigger,
                             order_tag=_order_tag(opt, TAG_SL, leg_id))
        order_no = order.get('brokerOrderId')
        if not order_no:
            raise RuntimeError(f'rejected: {order}')
        return order_no
    except Exception as exc:
        alert(f'{instrument.name}: SL order FAILED ({exc}) - the 10% stop is now checked only by this process on LTP. Place one manually!',
              level=logging.CRITICAL)
        return None


def _buy_back_leg(leg, opt, fallback_ltp):
    """BUY to close a short leg: cancel its resting SL first (if the SL already filled, that fill is
    the exit; if the cancel can't be confirmed, raise and leave the leg open and protected), then
    LIMIT at LTP made marketable after LIMIT_WAIT_SECONDS. Returns the fill price (LTP in DRY_RUN)."""
    instrument = leg['instrument']
    fresh_ltp = _fresh_ltp_fn(instrument, leg['strike'], opt, fallback_ltp)
    if DRY_RUN:
        fill = fresh_ltp()
        EXECUTION('dry_fill', side='BUY', symbol=instrument.name, tag=_order_tag(opt, TAG_EXIT, leg['leg_id']),
                  qty=leg['quantity'], fill_price=fill)
        return fill
    if fresh_ltp() is None:
        raise RuntimeError(f'{instrument.name}: no LTP available to close - leaving the leg and its SL in place')
    if leg.get('sl_order_id'):
        status, fill_price = _cancel_and_confirm(leg['sl_order_id'], f'{instrument.name} SL')
        if status == 'complete':
            log.info(f"{instrument.name}: SL {leg['sl_order_id']} filled before its cancel landed @ {fill_price} - already closed")
            leg['sl_order_id'] = None
            return fill_price
        if status not in TERMINAL_ORDER_STATUSES:
            raise RuntimeError(f"{instrument.name}: SL {leg['sl_order_id']} still {status!r} after cancel - leaving leg open and protected")
        leg['sl_order_id'] = None
    return _limit_then_market(instrument, 'BUY', leg['quantity'], fresh_ltp, _order_tag(opt, TAG_EXIT, leg['leg_id']))


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
def _window(now):
    """Checkpoint window index: 0 from ENTRY_TIME, +1 every CHECKPOINT_INTERVAL."""
    return int((now - datetime.combine(now.date(), ENTRY_TIME)) / CHECKPOINT_INTERVAL)


def _next_checkpoint(now):
    return (datetime.combine(now.date(), ENTRY_TIME) + (_window(now) + 1) * CHECKPOINT_INTERVAL).strftime('%H:%M')


def _new_day_state():
    return dict(legs={}, closed=set(), closing={}, strike=None, entry_sign=None, straddle_count=0, straddle_pts=0.0,
                straddle_started=None, traded_windows=set(), wait_logged_window=None, realized_pts=0.0, halted=False,
                last_delta_slot=None, last_delta=None, last_open_pts=None, cost_pending=False,
                missing_logged=set())


def _record_exit(day, cfg, opt, fill, reason, ref_ltp=None):
    """Book a leg's exit: P&L, decision log, trades CSV. Returns a one-line summary for alerts."""
    leg = day['legs'].pop(opt)
    pts = leg['entry_price'] - fill
    day['realized_pts'] += pts
    day['straddle_pts'] += pts
    day['closed'].add(opt)
    day['closing'].pop(opt, None)
    if reason == 'LEG_STOPLOSS' and COST_STOP_AFTER_SL:
        day['cost_pending'] = True
    now = datetime.now()
    entry_ref = leg.get('entry_ref_ltp')
    row = dict(date=now.date().isoformat(), straddle=leg['straddle'], option_type=opt, strike=leg['strike'],
               symbol=leg['instrument'].name, entry_time=leg['entry_time'], entry_price=leg['entry_price'],
               entry_ref_ltp=entry_ref, entry_slip_pts=None if entry_ref is None else round(entry_ref - leg['entry_price'], 2),
               exit_time=now.isoformat(timespec='seconds'), exit_price=fill, exit_ref_ltp=ref_ltp,
               exit_slip_pts=None if ref_ltp is None else round(fill - ref_ltp, 2), pts=round(pts, 2), lots=cfg['lots'], pnl_rs=round(_rs(pts, cfg), 2), reason=reason,
               dry_run=DRY_RUN)
    DECISION('leg_exit', echo=False, **{k: v for k, v in row.items() if k not in ('date', 'dry_run')},
             held_min=round((now - leg['entry_dt']).total_seconds() / 60, 1), day_realized_rs=round(_rs(day['realized_pts'], cfg)))
    _append_trade_row(row)
    return f"{opt} {leg['entry_price']} -> {fill} ({_rs(pts, cfg):+,.0f})"


def _straddle_done(day, cfg):
    """Both legs of the current straddle are closed: log its summary, reset for the next one."""
    if day['legs'] or not day['closed']:
        return False
    now = datetime.now()
    DECISION('straddle_closed', straddle=day['straddle_count'], strike=day['strike'],
             pnl_rs=round(_rs(day['straddle_pts'], cfg)), day_realized_rs=round(_rs(day['realized_pts'], cfg)),
             held_min=round((now - day['straddle_started']).total_seconds() / 60, 1) if day['straddle_started'] else None,
             window=_window(now), next_checkpoint=_next_checkpoint(now))
    day.update(closed=set(), strike=None, entry_sign=None, straddle_pts=0.0, straddle_started=None, cost_pending=False)
    return True


def _enter_straddle(day, market, cfg, strike, why):
    """Short whichever legs of the straddle at `strike` aren't open or already closed. A leg that
    fails stays missing - the next poll retries it at the same strike."""
    now = datetime.now()
    to_enter = [opt for opt in OPTION_TYPES if opt not in day['legs'] and opt not in day['closed']]
    DECISION('entry_decision', why=why, window=_window(now), strike=strike, spot=market['spot'], atm=market['atm'],
             ce_ltp=market['price'].get((strike, 'CE')), pe_ltp=market['price'].get((strike, 'PE')), legs=to_enter)
    tasks = {}
    for opt in to_enter:
        contract = market['contracts_by_strike_type'].get((strike, opt))
        ltp = market['price'].get((strike, opt))
        if contract is None or ltp is None:
            key = (_window(now), strike, opt)  # echoed once per window/strike/leg - retried (and file-logged) every poll
            DECISION('leg_not_entered', echo=key not in day['missing_logged'], level=logging.WARNING, opt=opt, strike=strike,
                     have_contract=contract is not None, ltp=ltp, why='contract or live quote unavailable - retrying next poll')
            day['missing_logged'].add(key)
            continue
        instrument = _to_instrument(contract)
        quantity = instrument.lot_size * cfg['lots']  # MCX quantity is in lots (GOLDM lot_size = 1)
        leg_id = _new_leg_id()

        def _task(i=instrument, q=quantity, l=ltp, k=strike, o=opt, lid=leg_id):
            fill = _short_leg(i, q, l, k, o, lid)
            return i, q, lid, l, fill, _place_leg_stop(i, q, fill, o, lid)
        tasks[opt] = _task
    if not tasks:
        return
    new_straddle = not day['legs'] and not day['closed']
    straddle_id = day['straddle_count'] + (1 if new_straddle else 0)
    entered = []
    for opt, res in _in_parallel(tasks).items():
        if isinstance(res, Exception):
            DECISION('leg_entry_failed', echo=False, opt=opt, strike=strike, error=str(res))
            alert(f'{opt} {strike} entry failed: {res}', level=logging.ERROR)
            continue
        instrument, quantity, leg_id, ref_ltp, fill, sl_order_id = res
        filled_at = datetime.now()
        stop, target = _leg_stop_trigger(fill), round(fill * (1 - LEG_TARGET_PCT), 2)
        day['legs'][opt] = dict(instrument=instrument, strike=strike, entry_price=fill, quantity=quantity, leg_id=leg_id,
                                sl_order_id=sl_order_id, delta=None, sl_seen_at=None, straddle=straddle_id,
                                stop_trigger=stop, at_cost=False,
                                entry_dt=filled_at, entry_time=filled_at.isoformat(timespec='seconds'), entry_ref_ltp=ref_ltp)
        DECISION('leg_entered', echo=False, straddle=straddle_id, opt=opt, strike=strike, symbol=instrument.name,
                 ref_ltp=ref_ltp, fill=fill, stop_trigger=stop, target=target, sl_order_id=sl_order_id)
        entered.append(f"{opt} {instrument.name} @ {fill} (stop {stop}, target {target})")
    if not entered:
        return
    if new_straddle:
        day['straddle_count'] = straddle_id
        day['straddle_started'] = now
        day['traded_windows'].add(_window(now))
    day['strike'] = strike
    alert(f'{why}: short straddle #{day["straddle_count"]} at {strike} - {"; ".join(entered)}')


def _close_legs(day, market, cfg, opts, reason):
    """Buy back the legs in `opts` (in parallel). A leg that fails to close stays open with its
    reason in day['closing'] - retried every poll. Returns True if all of them closed."""
    opts = [o for o in opts if o in day['legs']]
    if not opts:
        return True
    refs = {o: market['price'].get((day['legs'][o]['strike'], o)) for o in opts}
    DECISION('close_decision', reason=reason, straddle=day['straddle_count'],
             legs={o: dict(strike=day['legs'][o]['strike'], entry=day['legs'][o]['entry_price'], ltp=refs[o]) for o in opts})
    results = _in_parallel({
        opt: (lambda l=day['legs'][opt], o=opt: _buy_back_leg(l, o, refs[o]))
        for opt in opts
    })
    parts = []
    for opt, res in results.items():
        if isinstance(res, Exception) or res is None:
            day['closing'][opt] = reason
            DECISION('leg_close_failed', echo=False, opt=opt, reason=reason, error=str(res))
            alert(f"{reason}: closing {day['legs'][opt]['instrument'].name} failed ({res}) - still OPEN, will retry", level=logging.CRITICAL)
            continue
        parts.append(_record_exit(day, cfg, opt, res, reason, refs[opt]))
    if parts:
        alert(f'{reason}: straddle #{day["straddle_count"]} - {"; ".join(parts)} | day {_rs(day["realized_pts"], cfg):+,.0f} Rs')
    return not any(o in day['legs'] for o in opts)


def _check_leg_stops(day, market, cfg):
    """The 10% stop. Live: a leg whose resting SL has filled is closed at that fill; one whose LTP
    has been at/above the trigger for LIMIT_WAIT_SECONDS without the SL filling (an SL-limit left
    behind by a fast move) is closed by the LIMIT -> marketable-LIMIT exit instead; a SL found
    rejected/cancelled by someone else is reported and the stop is then checked here on LTP.
    DRY_RUN: checked on LTP."""
    book = None
    if not DRY_RUN and any(leg.get('sl_order_id') for leg in day['legs'].values()):
        book = _read_order_book_or_none('leg SL check')
    now = time_module.time()
    for opt, leg in list(day['legs'].items()):
        ltp = market['price'].get((leg['strike'], opt))
        trigger = leg['stop_trigger']
        reason = 'COST_STOP' if leg['at_cost'] else 'LEG_STOPLOSS'
        if leg.get('sl_order_id'):
            o = _find_order(book, leg['sl_order_id']) if book is not None else None
            status = _status(o) if o else None
            if status == 'complete':
                fill = float(o.get('averageTradedPrice') or 0) or trigger
                DECISION('stop_filled_at_broker', echo=False, opt=opt, strike=leg['strike'], entry=leg['entry_price'],
                         trigger=trigger, fill=fill, ltp=ltp, sl_order_id=leg['sl_order_id'], reason=reason)
                leg['sl_order_id'] = None
                alert(f"{reason} (SL filled): {_record_exit(day, cfg, opt, fill, reason, trigger)} | day {_rs(day['realized_pts'], cfg):+,.0f} Rs",
                      level=logging.WARNING)
                continue
            if status in ('rejected', 'cancelled'):
                DECISION('stop_order_dead', echo=False, opt=opt, status=status, sl_order_id=leg['sl_order_id'],
                         reason=o.get('rejectionReason') or None, now='stop checked here on LTP')
                alert(f"{leg['instrument'].name}: SL {leg['sl_order_id']} is {status} ({o.get('rejectionReason') or ''}) - stop now checked here on LTP",
                      level=logging.CRITICAL)
                leg['sl_order_id'] = None
        if ltp is None or ltp < trigger:
            if leg['sl_seen_at'] is not None:
                DECISION('stop_trigger_cleared', opt=opt, ltp=ltp, trigger=trigger,
                         secs_above=round(now - leg['sl_seen_at'], 1))
            leg['sl_seen_at'] = None
            continue
        if leg.get('sl_order_id'):  # resting SL should be filling - give it LIMIT_WAIT_SECONDS
            if leg['sl_seen_at'] is None:
                leg['sl_seen_at'] = now
                DECISION('stop_triggered_waiting', opt=opt, strike=leg['strike'], ltp=ltp, trigger=trigger,
                         sl_order_id=leg['sl_order_id'], wait_s=LIMIT_WAIT_SECONDS)
            if now - leg['sl_seen_at'] < LIMIT_WAIT_SECONDS:
                continue
            DECISION('stop_forced_close', level=logging.WARNING, opt=opt, strike=leg['strike'], ltp=ltp, trigger=trigger,
                     sl_order_id=leg['sl_order_id'], secs_above=round(now - leg['sl_seen_at'], 1),
                     why='SL not filled - cancelling it and closing with a marketable LIMIT')
        else:
            DECISION('stop_hit_ltp', opt=opt, strike=leg['strike'], entry=leg['entry_price'], ltp=ltp, trigger=trigger,
                     reason=reason)
        _close_legs(day, market, cfg, [opt], reason)


def _move_leg_stop(leg, opt, trigger):
    """Re-price a leg's resting SL BUY to `trigger` (limit SL_LIMIT_OFFSET_PCT above): modify in
    place, or cancel + place new if the modify is refused. Returns (action, fill) - fill is set only
    when the old SL turned out to have filled before it could be replaced."""
    if DRY_RUN:
        return 'dry run - checked on LTP', None
    if not leg.get('sl_order_id'):
        return 'no resting SL - checked on LTP', None
    limit = round(trigger * (1 + SL_LIMIT_OFFSET_PCT))
    name = leg['instrument'].name
    try:
        _modify_order(leg['sl_order_id'], leg['quantity'], 'SL', limit, trigger_price=trigger)
        return f'SL {leg["sl_order_id"]} modified to trigger {trigger} limit {limit}', None
    except Exception as exc:
        log.warning(f"{name}: modify of SL {leg['sl_order_id']} to cost failed ({exc}) - cancel + place new")
    status, fill = _cancel_and_confirm(leg['sl_order_id'], f'{name} SL')
    if status == 'complete':
        leg['sl_order_id'] = None
        return 'old SL filled before it could be replaced', fill
    if status not in TERMINAL_ORDER_STATUSES:
        return f'old SL {leg["sl_order_id"]} still {status!r} after cancel - left in place, cost checked on LTP', None
    leg['sl_order_id'] = None
    try:
        order = _place_order('BUY', leg['instrument'], leg['quantity'], 'SL', price=str(limit), trigger_price=trigger,
                             order_tag=_order_tag(opt, TAG_SL, leg['leg_id']))
        if not order.get('brokerOrderId'):
            raise RuntimeError(f'rejected: {order}')
        leg['sl_order_id'] = order['brokerOrderId']
        return f'SL replaced: {leg["sl_order_id"]} trigger {trigger} limit {limit}', None
    except Exception as exc:
        alert(f'{name}: cost SL order FAILED ({exc}) - the cost stop is now checked only by this process on LTP. Place one manually!',
              level=logging.CRITICAL)
        return 'new SL failed - cost checked on LTP', None


def _apply_cost_stops(day, market, cfg):
    """Once a leg's 10% stop has filled (day['cost_pending']), move every other open leg's stop to
    its own entry price; one whose LTP is already at/above cost is closed now (COST_STOP)."""
    if not day['cost_pending']:
        return
    day['cost_pending'] = False
    for opt, leg in list(day['legs'].items()):
        if leg['at_cost']:
            continue
        old, trigger = leg['stop_trigger'], _cost_trigger(leg['entry_price'])
        ltp = market['price'].get((leg['strike'], opt))
        leg.update(stop_trigger=trigger, at_cost=True, sl_seen_at=None)
        if ltp is not None and ltp >= trigger:
            DECISION('cost_stop_move', level=logging.WARNING, opt=opt, strike=leg['strike'], entry=leg['entry_price'],
                     old_trigger=old, new_trigger=trigger, ltp=ltp, action='LTP already at/above cost - closing now')
            _close_legs(day, market, cfg, [opt], 'COST_STOP')
            continue
        action, fill = _move_leg_stop(leg, opt, trigger)
        DECISION('cost_stop_move', echo=False, opt=opt, strike=leg['strike'], entry=leg['entry_price'], old_trigger=old,
                 new_trigger=trigger, ltp=ltp, action=action)
        if fill is not None:  # the 10% SL beat the move - that fill is the exit
            alert(f"LEG_STOPLOSS (SL filled before the move to cost): {_record_exit(day, cfg, opt, fill, 'LEG_STOPLOSS', ltp)}",
                  level=logging.WARNING)
            continue
        alert(f"COST STOP: {leg['instrument'].name} stop {old} -> {trigger} (entry {leg['entry_price']}, ltp {ltp}) - {action}")


def _check_leg_targets(day, market, cfg):
    """The 20% target: a leg whose LTP is at/below entry * (1 - LEG_TARGET_PCT) is bought back."""
    hit = []
    for opt, leg in day['legs'].items():
        ltp = market['price'].get((leg['strike'], opt))
        target = leg['entry_price'] * (1 - LEG_TARGET_PCT)
        if ltp is not None and ltp <= target:
            DECISION('target_hit', opt=opt, strike=leg['strike'], entry=leg['entry_price'], target=round(target, 2), ltp=ltp)
            hit.append(opt)
    if hit:
        _close_legs(day, market, cfg, hit, 'LEG_TARGET')


def _check_delta_zero(day, market, cfg, symbol):
    """Every DELTA_CHECK_SECONDS (first poll in each clock slot), while both legs are open: close
    both when the combined delta has reached zero / flipped sign from where the straddle started.
    Every check goes to the decision log (only sign-set / exit / unknown are echoed)."""
    slot = int(datetime.now().timestamp() // DELTA_CHECK_SECONDS)
    if slot == day['last_delta_slot'] or len(day['legs']) < len(OPTION_TYPES):
        return
    day['last_delta_slot'] = slot
    d, info = _straddle_delta(day, market, symbol)
    base = dict(straddle=day['straddle_count'], delta=None if d is None else round(d, 5), entry_sign=day['entry_sign'], **info)
    if d is None:
        DECISION('delta_check', level=logging.WARNING, **base, decision='unknown - a leg has no IV/delta yet')
        return
    day['last_delta'] = d
    if day['entry_sign'] is None:
        day['entry_sign'] = 1 if d > 0 else -1
        DECISION('delta_check', **{**base, 'entry_sign': day['entry_sign']}, decision='entry sign set')
        return
    if d * day['entry_sign'] <= 0:
        DECISION('delta_check', **base, decision='EXIT - delta reached zero / flipped sign')
        _close_legs(day, market, cfg, list(day['legs']), 'DELTA_ZERO')
        return
    DECISION('delta_check', echo=False, **base, decision='hold')


def _open_points(day, market):
    """Open legs' P&L, points per unit (entry fill - live LTP); None if a leg has no quote."""
    total = 0.0
    for opt, leg in day['legs'].items():
        ltp = market['price'].get((leg['strike'], opt))
        if ltp is None:
            return None
        total += leg['entry_price'] - ltp
    return total


def _strategy_tick(day, market, cfg, symbol):
    """One poll: retry failed closes, stops, targets, delta-zero, daily loss limit; enter at a checkpoint when flat."""
    for opt, reason in list(day['closing'].items()):  # a close that failed - finish it first
        DECISION('retry_close', opt=opt, reason=reason)
        _close_legs(day, market, cfg, [opt], reason)
    if day['closing']:
        return

    if day['legs']:
        _check_leg_stops(day, market, cfg)
        _apply_cost_stops(day, market, cfg)
        _check_leg_targets(day, market, cfg)
        _check_delta_zero(day, market, cfg, symbol)
        pts = _open_points(day, market)
        if pts is not None:
            day['last_open_pts'] = pts
            if day['realized_pts'] + pts <= -_points(cfg['daily_loss_limit_rs'], cfg):
                DECISION('daily_loss_limit', level=logging.WARNING, day_rs=round(_rs(day['realized_pts'] + pts, cfg)),
                         realized_rs=round(_rs(day['realized_pts'], cfg)), open_rs=round(_rs(pts, cfg)),
                         limit_rs=cfg['daily_loss_limit_rs'])
                alert(f'Daily loss limit: day {_rs(day["realized_pts"] + pts, cfg):+,.0f} Rs <= -{cfg["daily_loss_limit_rs"]:,} - closing and stopping for the day',
                      level=logging.WARNING)
                if _close_legs(day, market, cfg, list(day['legs']), 'DAILY_LOSS_LIMIT'):
                    day['halted'] = True
                return

    if day['legs'] and len(day['legs']) + len(day['closed']) < len(OPTION_TYPES):  # a leg that failed to enter
        _enter_straddle(day, market, cfg, day['strike'], 'retry missing leg')
        return
    _straddle_done(day, cfg)
    if day['legs']:
        return
    now = datetime.now()
    window = _window(now)
    if window not in day['traded_windows']:
        _enter_straddle(day, market, cfg, market['atm'], f'checkpoint {now:%H:%M}')
    elif day['wait_logged_window'] != window:
        day['wait_logged_window'] = window
        DECISION('flat_wait', window=window, next_checkpoint=_next_checkpoint(now),
                 why='this checkpoint window already had its straddle')


def _adopt_open_positions(day, market):
    """Mid-day restart: adopt open GOLDM positions on the current chain as the current straddle
    (this checkpoint window counts as traded; a missing leg counts as already closed)."""
    open_legs = _resilient_call(get_open_legs, market['contracts_by_token'])
    if not open_legs:
        return False
    for token, pos in open_legs.items():
        contract = market['contracts_by_token'][token]
        opt, strike = contract['option_type'], int(float(contract['strike_price']))
        qty = int(pos['netQuantity'])  # units (see ALICEBLUE_UNITS_PER_LOT)
        lots, odd_units = divmod(abs(qty), ALICEBLUE_UNITS_PER_LOT)
        if qty > 0 or opt in day['legs'] or odd_units or not lots:
            alert(f'Unexpected position {contract["trading_symbol"]} net {qty} units - not adopted, handle manually', level=logging.CRITICAL)
            continue
        entry = _infer_entry_price(token, abs(qty))  # order book quantities are units too
        if entry is None:
            entry = market['price'].get((strike, opt)) or 0.0
            log.warning(f"{opt} {strike}: couldn't reconstruct entry price - using live LTP {entry} (approximate)")
        instrument, leg_id = _to_instrument(contract), _new_leg_id()
        sl_order_id, stop_trigger = _find_resting_leg_stop(token)
        if sl_order_id is None:
            sl_order_id = _place_leg_stop(instrument, lots, entry, opt, leg_id)
        if sl_order_id is None or stop_trigger is None:
            stop_trigger = _leg_stop_trigger(entry)
        adopted_at = datetime.now()
        day['legs'][opt] = dict(instrument=instrument, strike=strike, entry_price=entry, quantity=lots,
                                leg_id=leg_id, sl_order_id=sl_order_id, delta=None, sl_seen_at=None, straddle=1,
                                stop_trigger=stop_trigger, at_cost=stop_trigger <= _cost_trigger(entry),
                                entry_dt=adopted_at, entry_time=adopted_at.isoformat(timespec='seconds'), entry_ref_ltp=None)
        DECISION('leg_adopted', echo=False, opt=opt, strike=strike, symbol=instrument.name, lots=lots, net_units=qty, entry=entry,
                 entry_source='order book' if entry != market['price'].get((strike, opt)) else 'live LTP (approximate)',
                 sl_order_id=sl_order_id, stop_trigger=stop_trigger, at_cost=day['legs'][opt]['at_cost'])
    if not day['legs']:
        return False
    day['closed'] = {o for o in OPTION_TYPES if o not in day['legs']}
    day['strike'] = next(iter(day['legs'].values()))['strike']
    day['straddle_count'] = 1
    day['straddle_started'] = datetime.now()
    day['traded_windows'].add(_window(datetime.now()))
    alert('Adopted open positions: ' + ', '.join(f"{o} {l['instrument'].name} @ {l['entry_price']:.2f}" for o, l in day['legs'].items()),
          level=logging.WARNING)
    return True


def _find_resting_leg_stop(token):
    """(broker id, trigger price or None) of a still-resting SL BUY of ours on `token` (adoption
    after a restart), else (None, None)."""
    try:
        book = _get_order_book()
    except Exception as exc:
        log.warning(f'could not read order book to find a resting SL for token {token}: {exc}')
        return None, None
    for o in book:
        tag = (_tag_of(o) or '').lower()
        if (str(o.get('instrumentId')) == str(token) and tag.startswith(TAG_PREFIX.lower()) and tag[len(TAG_PREFIX) + 1:len(TAG_PREFIX) + 2] == TAG_SL.lower()
                and str(o.get('transactionType', '')).upper() == 'BUY' and _status(o) not in TERMINAL_ORDER_STATUSES):
            trigger = o.get('slTriggerPrice') or o.get('triggerPrice')
            try:
                trigger = float(trigger) if trigger not in (None, '', '--') else None
            except (TypeError, ValueError):
                trigger = None
            return o.get('brokerOrderId'), trigger
    return None, None


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
    """Compare broker positions on the current chain with the legs this process holds; alert on any
    mismatch (no automatic action - a manual trade or a missed fill needs a human)."""
    if DRY_RUN:
        return
    try:
        open_legs = get_open_legs(market['contracts_by_token'])
    except Exception as exc:
        log.warning(f'reconcile: positions read failed ({exc})', extra={'no_telegram': True})
        return
    broker = {int(t): abs(int(p['netQuantity'])) / ALICEBLUE_UNITS_PER_LOT for t, p in open_legs.items()}  # lots
    ours = {leg['instrument'].token: leg['quantity'] for leg in day['legs'].values()}
    EXECUTION('reconcile', echo=broker != ours, level=logging.INFO if broker == ours else logging.ERROR,
              broker=broker, ours=ours, match=broker == ours)
    if broker != ours:
        alert(f'Position mismatch - broker {broker} vs strategy {ours}. Check manually (another strategy or a manual trade on GOLDM?)', level=logging.CRITICAL)


def _send_heartbeat(day, cfg, symbol, now):
    if day['legs']:
        legs = ', '.join(f"{o} {l['instrument'].name} @ {l['entry_price']}" for o, l in day['legs'].items())
        legs += f" | open {_rs(day.get('last_open_pts') or 0, cfg):+,.0f} Rs"
        if day.get('last_delta') is not None:
            legs += f", delta {day['last_delta']:+.3f}"
    else:
        legs = 'flat, waiting for the next checkpoint'
    last = f"{_last_event['text']} ({_last_event['at']:%H:%M:%S})" if _last_event['at'] else 'none yet'
    message = (f"🟢 still running - {symbol} delta-neutral {now:%H:%M:%S}\n{legs}\n"
               f"day realized {_rs(day['realized_pts'], cfg):+,.0f} Rs, straddles {day['straddle_count']}\nlast event: {last}")
    log.info(f'heartbeat: {message}')
    DECISION('heartbeat', echo=False, legs={o: dict(strike=l['strike'], entry=l['entry_price']) for o, l in day['legs'].items()},
             open_rs=round(_rs(day.get('last_open_pts') or 0, cfg)), last_delta=day.get('last_delta'),
             day_realized_rs=round(_rs(day['realized_pts'], cfg)), straddles=day['straddle_count'])
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


def _square_off(day, symbol, cfg):
    for attempt in range(RETRY_MAX_ATTEMPTS):
        try:
            if _close_legs(day, _fetch_market(symbol, cfg, day), cfg, list(day['legs']), 'EOD'):
                return
            raise RuntimeError('some legs still open')
        except Exception as exc:
            if attempt == RETRY_MAX_ATTEMPTS - 1:
                alert(f'{symbol}: final square-off failed after {RETRY_MAX_ATTEMPTS} attempts - positions may still be OPEN, check manually: {exc}',
                      level=logging.CRITICAL)
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            log.warning(f'final square-off failed ({exc}) - retrying in {delay:.0f}s')
            time_module.sleep(delay)


def run_day(symbol, trade_weekdays):
    today_name = datetime.now().strftime('%A')
    if today_name not in trade_weekdays:
        DECISION('skip_day', reason=f'{today_name} is not in TRADE_WEEKDAYS ({sorted(trade_weekdays)})')
        return
    if not _wait_for_mcx_ticker(symbol):
        DECISION('skip_day', reason=f'mcx_ticker_service.py not running before {EXIT_TIME}')
        return

    cfg = CFG[symbol]
    day = _new_day_state()

    _sleep_until(WARMUP_TIME, 'warm-up start')
    market = _fetch_market_until_success(symbol, cfg, day)
    if _chain_expiry(symbol) == datetime.now().date():
        DECISION('skip_day', echo=False, reason=f'option chain expiry day ({_chain_expiry(symbol)})')
        alert(f'{symbol}: today is the option chain expiry ({_chain_expiry(symbol)}) - MCX options devolve into futures, NOT trading today',
              level=logging.WARNING)
        return
    alert(f"GOLDM delta-neutral straddle starting for {symbol} - {today_name} {datetime.now():%Y-%m-%d} | "
          f"{cfg['lots']} lot(s), ATM straddle, leg stop {LEG_SL_PCT:.0%} / "
          f"target {LEG_TARGET_PCT:.0%}{', other leg to cost after a stop' if COST_STOP_AFTER_SL else ''}, checkpoint every {CHECKPOINT_INTERVAL}, day limit {cfg['daily_loss_limit_rs']:,} Rs"
          f"{' [DRY RUN]' if DRY_RUN else ''}")
    DECISION('day_start', echo=False, symbol=symbol, lots=cfg['lots'], chain_expiry=_chain_expiry(symbol),
             future=_zerodha_future_cache[symbol]['future']['tradingsymbol'], leg_sl_pct=LEG_SL_PCT,
             leg_target_pct=LEG_TARGET_PCT, cost_stop_after_sl=COST_STOP_AFTER_SL, checkpoint_interval=CHECKPOINT_INTERVAL, entry_time=ENTRY_TIME,
             exit_time=EXIT_TIME, delta_check_s=DELTA_CHECK_SECONDS, poll_s=POLL_INTERVAL_SECONDS,
             limit_wait_s=LIMIT_WAIT_SECONDS, chase=(LIMIT_OFFSET_PCT, CHASE_OFFSET_STEP, CHASE_MAX_OFFSET_PCT),
             daily_loss_limit_rs=cfg['daily_loss_limit_rs'], spot=market['spot'], atm=market['atm'])
    while datetime.now().time() < ENTRY_TIME:
        time_module.sleep(WARMUP_POLL_SECONDS)
        try:
            market = _fetch_market(symbol, cfg, day)
        except Exception as exc:
            log.warning(f'warm-up market fetch failed ({exc}) - keeping previous snapshot')
    DECISION('entry_snapshot', spot=market['spot'], atm=market['atm'],
             ce_ltp=market['price'].get((market['atm'], 'CE')), pe_ltp=market['price'].get((market['atm'], 'PE')))

    if not DRY_RUN:
        _adopt_open_positions(day, market)

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
            _strategy_tick(day, market, cfg, symbol)
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

    DECISION('square_off', reason='daily loss limit' if day['halted'] else f'{EXIT_TIME} reached',
             open_legs=sorted(day['legs']))
    _square_off(day, symbol, cfg)
    _straddle_done(day, cfg)
    DECISION('day_end', straddles=day['straddle_count'], realized_rs=round(_rs(day['realized_pts'], cfg)),
             halted=day['halted'], note='gross, before charges')
    alert(f'GOLDM delta-neutral straddle done for {symbol} - {day["straddle_count"]} straddle(s), realized {_rs(day["realized_pts"], cfg):+,.0f} Rs (gross, before charges)')


if __name__ == '__main__':
    SYMBOL = _ARGV_SYMBOL
    if SYMBOL not in CFG:
        raise ValueError(f'unknown symbol {SYMBOL!r} - use one of {sorted(CFG)}')
    TRADE_WEEKDAYS = _parse_trade_weekdays(sys.argv[2] if len(sys.argv) > 2 else None)
    run_day(SYMBOL, TRADE_WEEKDAYS)
