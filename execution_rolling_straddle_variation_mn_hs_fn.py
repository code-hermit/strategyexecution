"""
MONDAY,FRIDAY - NIFTY
THURSDAY - SENSEX

MONDAY -NIFTY - 5 lots
THURSDAY - SENSEX - 5 lots
FRIDAY - NIFTY 3 lots

Live execution of the "variation" strategy from backtest_rolling_straddle_variation.py:
short a first-OTM strangle (FIRST_OTM_STRIKES away from ATM; 0 = ATM itself) at ENTRY_TIME,
each leg with a resting STOPLOSS_PCT stoploss. Every CHECKPOINT_INTERVAL thereafter:

  - if the current ATM straddle premium (CE+PE at the true ATM strike, not the first-OTM legs
    actually traded) is HIGHER than it was at the previous checkpoint: take no new trade this
    hour; if both legs are still open, close them outright.
  - else if a leg's stoploss fired earlier this hour (see minute-level checks below): stay flat
    for this leg for the rest of the hour - consumed here, not re-entered immediately.
  - else if either open leg has drifted off today's first-OTM strike (spot moved): roll the
    whole strangle - close whatever's open, re-enter fresh at the new first-OTM strikes.
  - else: re-enter any leg that isn't currently open (its resting stoploss got hit and closed
    the position) at the same strike; leave already-open legs alone.

Between checkpoints, polls every POLL_INTERVAL_SECONDS (not just once an hour) to:
  - notice a leg's resting stoploss has filled (broker-side truth, via AliceBlue positions) and
    alert immediately rather than waiting for the next checkpoint to notice it's gone.
  - (optional, off by default) close both legs if the ATM premium is now above its own highest
    reading over the trailing PREMIUM_HIGH_LOOKBACK window.
  - (optional, off by default) close both legs if the ATM premium has risen PREMIUM_STOPLOSS_PCT
    above its value at today's entry.
  - halt trading for the day (square everything off, no more re-entries) once realized+unrealized
    pnl crosses -DAILY_LOSS_LIMIT. Pnl here is in raw premium points (unscaled by lot size/qty),
    matching backtest_rolling_straddle_variation's convention - DAILY_LOSS_LIMIT carries over
    directly from a backtest run's config.

Any open positions are squared off by EXIT_TIME regardless of the above.

Built on top of execution_rolling_straddle.py (imported as `ers`) for all Dhan/AliceBlue REST
plumbing - auth, contract master, option chain, order placement, tick rounding - the same way
day_end_straddle_buy.py does. Importing it runs its Dhan/AliceBlue auth checks and honours the
same DRY_RUN env var and no-MARKET-orders policy (LIMIT entries/exits 1% through LTP, SL orders
capped 1% beyond trigger).

Trades a single underlying (command-line arg, default NIFTY) on TRADE_WEEKDAYS (command-line
weekday codes, default every weekday) - unlike execution_rolling_straddle.py's per-weekday
underlying switch, matching how backtest_rolling_straddle_variation.py is invoked.

Logging: every checkpoint/poll decision goes to execution_rolling_straddle_variation.log and
stdout. Lifecycle events (day start/skip, entries, exits, rolls, stoploss fills, halts, day
summary) are additionally pushed to Telegram via alert() below, and any WARNING+ log record
(order rejections, API failures, uncaught exceptions) is pushed automatically as a safety net -
see TelegramHandler. On top of that, every HEARTBEAT_INTERVAL a "still running" ping goes out
with the current legs, day pnl so far, and the last real event/timestamp - so a quiet stretch
with nothing to report doesn't read as the process having died. Requires TELEGRAM_BOT_TOKEN and
TELEGRAM_CHAT_ID in .env; if either is missing, Telegram alerts (including heartbeats) are
skipped (logged as a one-time warning) but trading proceeds normally.

Resuming after a restart: if the process starts after ENTRY_TIME with no open positions yet, entry
timing depends on ENTRY_MODE (command-line arg, default fire-immediately - see ENTRY_MODES above):
fire-immediately enters right away (matching execution_rolling_straddle.py's own late-start
handling); honor-checkpoints instead stays flat until the next ENTRY_TIME-anchored checkpoint
fires the entry. Either way, every checkpoint after that first entry is always grid-anchored -
ENTRY_MODE only changes when the first one lands. If it starts with positions already open
(mid-day restart, or legs placed manually), it adopts them into local state, reconstructing each
leg's actual entry price from the AliceBlue order book's completed SELL fills where possible (see
_infer_entry_price_from_orderbook) instead of assuming live LTP - falling back to LTP, logged
clearly as an approximation, only if that reconstruction fails. The reconstructed combined entry
also stands in for the previous checkpoint's premium, so the next checkpoint's ATM-rise comparison
isn't starting blind. Realized pnl from before the restart is still lost either way (this process
wasn't the one that tracked it) - alerted loudly.
"""

import logging
import os
import sys
import time as time_module
from datetime import datetime, time as dtime, timedelta, timezone

import requests
from dotenv import load_dotenv

# Load .env explicitly here, before reading TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID below - don't rely
# on execution_rolling_straddle.py (imported as `ers` further down) to do it: ers.load_dotenv()
# only runs once that import is reached, which is *after* this file's own TELEGRAM_* reads, so
# without this line those always came back None (and Telegram alerts were silently disabled) even
# with valid values sitting in .env.
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# ── Logging (own file/handlers - ers already claimed the root logger via basicConfig) ──────────
# Set up before importing execution_rolling_straddle below, so that if its module-level Dhan/
# AliceBlue auth fails, the failure is still caught, logged and Telegram-alerted rather than only
# going through ers's own logger.
LOG_FILE = os.path.join(os.path.dirname(__file__), 'execution_rolling_straddle_variation.log')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_TIMEOUT = 10

log = logging.getLogger('execution_rolling_straddle_variation')
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
    """Safety net: any WARNING+ log record (order rejections, API failures, uncaught exceptions)
    gets pushed to Telegram automatically, even where the code doesn't call alert() explicitly.
    Records from alert() itself are skipped here (marked via the `_alerted` extra) since alert()
    already sends them - otherwise a WARNING/CRITICAL alert() call would show up twice. Records
    marked extra={'no_telegram': True} are skipped too - see _alert_failure_throttled below,
    which uses this to keep a prolonged outage from re-alerting on every single poll."""

    def emit(self, record):
        # logging.Logger.callHandlers doesn't guard hdlr.handle() with a try/except, so an
        # exception escaping emit() would propagate straight out of whatever log.log()/alert()
        # call triggered it - i.e. a Telegram hiccup could crash the trading loop. Catch
        # everything here instead (self.handleError() prints to stderr, never raises).
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


_last_event = {'text': 'not started yet', 'at': None}  # updated by every alert() call - what the
# periodic heartbeat (see run_day) reports as "last action taken" between its own pings


def alert(message, level=logging.INFO):
    """Log + always push to Telegram exactly once (unlike a bare log.info(), which only reaches
    Telegram if it's WARNING+) - use for events the user actually wants pinged about.

    Belt-and-braces: log.log() and _telegram_send() are each wrapped so that nothing in the
    alerting path - Telegram being down, rate-limited, misconfigured, whatever - can ever raise
    into the caller and interrupt the strategy loop that called alert()."""
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


FAILURE_ALERT_EVERY = 10  # after the 1st consecutive poll-iteration failure alerts, only re-alert
# every Nth consecutive one after that - keeps a prolonged Dhan/AliceBlue outage from spamming
# Telegram every POLL_INTERVAL_SECONDS while still landing every single failure in the log file.


def _alert_failure_throttled(message, failure_count, level=logging.ERROR, every=FAILURE_ALERT_EVERY):
    """Same as alert(), except it only actually pings Telegram on the 1st consecutive failure and
    then every `every`th one after that - full detail always goes to the log file/stdout either
    way (via the plain log.log() branch, tagged no_telegram so the WARNING+ safety-net handler
    doesn't re-forward it either).

    Belt-and-braces: called from the outer except in run_day's poll loop, which is already the
    last-resort handler for the whole trading iteration - wrapped so that a problem in the
    logging/alerting path itself can never escape and kill the day's loop outright (alert() and
    plain log.log() are each already internally guarded the same way, but this is the one place
    that decision itself - which branch to take - runs, so it gets the same treatment)."""
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

try:
    import execution_rolling_straddle_tn as ers
except Exception:
    log.critical('Failed to import execution_rolling_straddle (Dhan/AliceBlue auth failed?)', exc_info=True)
    raise

# ── Strategy config (mirrors backtest_rolling_straddle_variation.py) ───────────────────────────
ENTRY_TIME = dtime(9, 45)
EXIT_TIME = dtime(15, 13)
CHECKPOINT_INTERVAL = timedelta(hours=1)
POLL_INTERVAL_SECONDS = 30  # how often minute-level checks (stoploss-fill detection, optional
# premium stoplosses, daily loss limit) run between hourly checkpoints

HEARTBEAT_INTERVAL = timedelta(minutes=30)  # "I'm still alive" ping between real events, so a
# quiet stretch (no rolls, no stoplosses, nothing) doesn't read as the process having died

# Entry mode - only matters on a late start (process comes up after ENTRY_TIME with no positions
# already open; an on-time start always enters right at ENTRY_TIME either way):
#   fire-immediately (default) - enter the moment the process comes up, off-grid strike included.
#   honor-checkpoints - stay flat and wait for the next ENTRY_TIME-anchored checkpoint (e.g. start
#     at 10:00 -> first entry at 10:45) instead of entering off-grid. Checkpoints after that first
#     entry are always grid-anchored either way - this only changes when the *first* entry lands.
ENTRY_MODE_HONOR_CHECKPOINTS = 'honor-checkpoints'
ENTRY_MODE_FIRE_IMMEDIATELY = 'fire-immediately'
ENTRY_MODES = (ENTRY_MODE_HONOR_CHECKPOINTS, ENTRY_MODE_FIRE_IMMEDIATELY)
DEFAULT_ENTRY_MODE = ENTRY_MODE_FIRE_IMMEDIATELY

FIRST_OTM_STRIKES = 0  # 0 = ATM; n = n strikes OTM (CE up, PE down)

# Per-leg stoploss: NIFTY keeps ers.STOPLOSS_PCT (30%, percentage-of-entry-premium) unchanged.
# SENSEX uses a fixed points-based distance instead - SENSEX premiums are large enough that a
# percentage stoploss ends up too tight or too loose depending on strike, so a flat points move
# is used instead (same idea as execution_straddle_premium_stoploss_sensex.py's LEG_STOPLOSS_POINTS).
SENSEX_LEG_STOPLOSS_POINTS = 300

PREMIUM_HIGH_STOPLOSS_ENABLED = False
PREMIUM_HIGH_LOOKBACK = timedelta(hours=2)
PREMIUM_STOPLOSS_ENABLED = False
PREMIUM_STOPLOSS_PCT = 0.25
DAILY_LOSS_LIMIT = 100  # points, unscaled by lot size - see module docstring

# Per-weekday lot overrides, keyed by (symbol, weekday name) - falls back to
# ers.UNDERLYINGS[symbol]['lots'] (NIFTY=5, SENSEX=5) when a symbol/weekday isn't listed here.
LOTS_OVERRIDE = {
    ('NIFTY', 'Friday'): 3,
}

# Per-weekday checkpoint interval overrides, keyed by (symbol, weekday name) - falls back to
# CHECKPOINT_INTERVAL when a symbol/weekday isn't listed here.
CHECKPOINT_INTERVAL_OVERRIDE = {
    ('NIFTY', 'Friday'): timedelta(hours=2),
}

OPTION_TYPES = ('CE', 'PE')
DAY_CODE_TO_WEEKDAY = {'m': 'Monday', 't': 'Tuesday', 'w': 'Wednesday', 'h': 'Thursday', 'f': 'Friday'}


def _parse_trade_weekdays(codes):
    """Compact weekday-code string -> set of weekday names, e.g. 'th' -> {'Tuesday', 'Thursday'}.
    None/empty trades every weekday. Codes: m=Mon, t=Tue, w=Wed, h=Thu, f=Fri."""
    if not codes:
        return set(DAY_CODE_TO_WEEKDAY.values())
    weekdays = set()
    for code in codes.lower():
        weekday = DAY_CODE_TO_WEEKDAY.get(code)
        if weekday is None:
            raise ValueError(f"unknown weekday code {code!r} in {codes!r} - use any combination of {''.join(DAY_CODE_TO_WEEKDAY)}")
        weekdays.add(weekday)
    return weekdays


# ── Strike/premium helpers ──────────────────────────────────────────────────────────────────
def _first_otm_strike(atm, option_type, strike_interval):
    """CE gets more OTM going UP in strike, PE gets more OTM going DOWN. FIRST_OTM_STRIKES=0 is
    just the ATM strike itself."""
    if FIRST_OTM_STRIKES == 0:
        return atm
    sign = 1 if option_type == 'CE' else -1
    return atm + sign * FIRST_OTM_STRIKES * strike_interval


def _atm_premium(option_chain, atm):
    """ATM CE LTP + ATM PE LTP (the literal ATM straddle premium, not the first-OTM legs this
    strategy actually trades) - None if either quote is unavailable."""
    try:
        return ers.option_ltp(option_chain, atm, 'CE') + ers.option_ltp(option_chain, atm, 'PE')
    except (KeyError, TypeError):
        return None


RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY = 5  # seconds; doubled on each successive attempt (5, 10, 20, 40)

# Proactive spacing between calls to the same endpoint, keyed by function name - so we space
# ourselves out ahead of time instead of only backing off reactively after Dhan/AliceBlue has
# already rejected a call with a 429. Dhan's /optionchain is documented at ~1 request/3s, which
# two _fetch_market() calls landing within the same second (e.g. the entry-time fetch immediately
# followed by the main loop's first iteration) blows straight through.
MIN_CALL_INTERVAL = {'get_option_chain': 3.5}
DEFAULT_MIN_CALL_INTERVAL = 1.0  # conservative floor for the other Dhan/AliceBlue GETs
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
    """Call a read-only Dhan/AliceBlue GET (spot LTP, option chain, contract master, positions)
    with retry + backoff through transient failures - a 429 rate limit, a 5xx, a network blip -
    instead of letting them turn into an uncaught exception that kills the whole session (this is
    exactly how a single /optionchain 429 took the process down before this was added). Respects
    a 429's Retry-After header when present, and proactively throttles (see _throttle above)
    before every attempt so we're not relying on 429s to tell us we're going too fast. Not used
    for order placement/cancellation - those aren't safely retryable blind (a timed-out response
    doesn't mean the order didn't go through)."""
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
            log.warning(
                f'{fn.__name__} failed ({exc}) - retrying in {delay:.0f}s '
                f'(attempt {attempt + 1}/{RETRY_MAX_ATTEMPTS})'
            )
            time_module.sleep(delay)


def _fetch_market(symbol, cfg):
    """One fresh snapshot (spot/ATM/contracts/option chain) shared by a checkpoint and its
    surrounding minute-level checks, rather than re-fetching for each."""
    spot = _resilient_call(ers.get_spot_ltp, cfg['dhan_security_id'], cfg['dhan_segment'])
    atm = ers.atm_strike(spot, cfg['strike_interval'])
    contracts = _resilient_call(ers.load_current_week_options, symbol, cfg['aliceblue_exchange'])
    contracts_by_token = {int(c['token']): c for c in contracts}
    contracts_by_strike_type = {(int(float(c['strike_price'])), c['option_type']): c for c in contracts}
    expiry_date = datetime.fromtimestamp(contracts[0]['expiry_date'] / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
    option_chain = _resilient_call(ers.get_option_chain, expiry_date, cfg['dhan_security_id'], cfg['dhan_segment'])
    return dict(
        spot=spot, atm=atm, contracts_by_token=contracts_by_token,
        contracts_by_strike_type=contracts_by_strike_type, option_chain=option_chain,
    )


FETCH_UNTIL_SUCCESS_DELAY = 30  # seconds between attempts once _resilient_call's own retries are exhausted
FETCH_UNTIL_SUCCESS_ALERT_EVERY = 10  # only re-alert every Nth attempt, so a long outage doesn't spam Telegram


def _fetch_market_until_success(symbol, cfg):
    """Entry-time market fetch: unlike a mid-day poll (where skipping one cycle and trying again
    in POLL_INTERVAL_SECONDS is harmless), failing to fetch market data here means the day's
    initial legs never get placed at all - so keep retrying (past _resilient_call's own bounded
    retries) rather than letting the day be abandoned over a prolonged Dhan/AliceBlue outage."""
    attempt = 0
    while True:
        try:
            return _fetch_market(symbol, cfg)
        except Exception as exc:
            attempt += 1
            if attempt == 1 or attempt % FETCH_UNTIL_SUCCESS_ALERT_EVERY == 0:
                alert(
                    f'Could not fetch market data for entry ({exc}) - still retrying '
                    f'(attempt {attempt}, every {FETCH_UNTIL_SUCCESS_DELAY}s)',
                    level=logging.ERROR,
                )
            time_module.sleep(FETCH_UNTIL_SUCCESS_DELAY)


def _desired_legs(market, cfg):
    """{'CE': (instrument, ltp, strike), 'PE': (...)} for today's first-OTM strikes - skips (with
    a warning) any leg whose strike isn't in the current contract master (e.g. FIRST_OTM_STRIKES
    pushed it off the available chain)."""
    desired = {}
    for opt in OPTION_TYPES:
        strike = _first_otm_strike(market['atm'], opt, cfg['strike_interval'])
        contract = market['contracts_by_strike_type'].get((strike, opt))
        if contract is None:
            log.warning(f'{opt} {strike} not found in current-week contracts - skipping this leg')
            continue
        try:
            ltp = ers.option_ltp(market['option_chain'], strike, opt)
        except (KeyError, TypeError):
            log.warning(f'{opt} {strike} has no live quote - skipping this leg')
            continue
        desired[opt] = (ers._to_instrument(contract), ltp, strike)
    return desired


# ── Per-leg state (local bookkeeping - see module docstring on why this isn't purely broker-derived) ──
def _new_state():
    return {opt: None for opt in OPTION_TYPES}  # None, or {'instrument','strike','entry_price','quantity'}


def _short_leg_with_stoploss(instrument, quantity, ltp, symbol):
    """Same as ers.short_leg_with_stoploss, except the stoploss distance is symbol-dependent: NIFTY
    keeps ers.STOPLOSS_PCT (percentage of entry premium), SENSEX uses a fixed points move instead
    (SENSEX_LEG_STOPLOSS_POINTS - see its definition above for why)."""
    entry_price = ers._round_to_tick(ltp * (1 - ers.LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if ers.DRY_RUN else ""}SELL {quantity} x {instrument.name} LIMIT @ {entry_price} (ltp {ltp})'
    log.info(tag)
    if ers.DRY_RUN:
        return entry_price

    entry = ers._place_order(
        'SELL', instrument, quantity, 'LIMIT', price=str(entry_price), order_tag='rolling_straddle_entry',
    )
    order_no = entry.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} entry order rejected: {entry}')

    entry_price = ers._wait_for_fill_price(order_no)
    if symbol == 'SENSEX':
        trigger_price = round(entry_price + SENSEX_LEG_STOPLOSS_POINTS, 1)
    else:
        trigger_price = round(entry_price * (1 + ers.STOPLOSS_PCT), 1)
    sl_limit_price = ers._round_to_tick(trigger_price * (1 + ers.LIMIT_OFFSET_PCT), instrument.tick_size)
    log.info(f'{instrument.name} entered @ {entry_price}, SL trigger {trigger_price} limit {sl_limit_price}')

    ers._place_order(
        'BUY', instrument, quantity, 'SL', price=str(sl_limit_price),
        trigger_price=trigger_price, order_tag='rolling_straddle_sl',
    )
    return entry_price


def _enter_leg(state, opt, instrument, ltp, strike, cfg, symbol):
    quantity = instrument.lot_size * cfg['lots']
    _short_leg_with_stoploss(instrument, quantity, ltp, symbol)
    state[opt] = dict(instrument=instrument, strike=strike, entry_price=ltp, quantity=quantity)
    sl_desc = f'{SENSEX_LEG_STOPLOSS_POINTS}pt' if symbol == 'SENSEX' else f'{ers.STOPLOSS_PCT:.0%}'
    alert(f'ENTER {opt} {instrument.name} x{quantity} @ ~{ltp} (SL {sl_desc})')


def _enter_initial_day_legs(state, market, cfg, day, symbol):
    """Today's first entry - shared by both ENTRY_MODEs: fire-immediately calls this the moment
    the process comes up, honor-checkpoints calls it once the first checkpoint after a late start
    arrives. Records today's baseline ATM premium (for the ATM-rise/premium-stoploss checks) and
    shorts the first-OTM legs."""
    day['entry_premium'] = _atm_premium(market['option_chain'], market['atm'])
    day['prev_checkpoint_premium'] = day['entry_premium']
    if day['entry_premium'] is not None:
        day['premium_history'] = [(datetime.now(), day['entry_premium'])]
    for opt, (instrument, ltp, strike) in _desired_legs(market, cfg).items():
        _enter_leg(state, opt, instrument, ltp, strike, cfg, symbol)


def _close_leg(state, opt, market, cfg, reason):
    """Close leg `opt` if we believe it's open, pricing the exit off a fresh LTP for its strike.
    Returns the realized pnl (points, short leg: entry - exit) or None if nothing was closed."""
    leg = state[opt]
    if leg is None:
        return None
    try:
        exit_ltp = ers.option_ltp(market['option_chain'], leg['strike'], opt)
    except (KeyError, TypeError):
        log.warning(f'{opt} {leg["strike"]}: no live quote to close against, leaving position open')
        return None
    ers.close_leg(leg['instrument'], leg['quantity'], exit_ltp)
    pnl = leg['entry_price'] - exit_ltp
    alert(f'EXIT {opt} {leg["instrument"].name} @ ~{exit_ltp} (entry ~{leg["entry_price"]}) pnl~{pnl:+.2f} reason={reason}')
    state[opt] = None
    return pnl


def _close_open_legs(state, market, cfg, reason):
    return sum(pnl for opt in OPTION_TYPES if (pnl := _close_leg(state, opt, market, cfg, reason)) is not None)


def _sync_stopped_out_legs(state, market):
    """Broker-side truth: a leg we believe is open but whose position has vanished means its
    resting stoploss filled. Detect that promptly (every poll) and clear local state, rather than
    waiting for the next hourly checkpoint to notice."""
    open_tokens = set(_resilient_call(ers.get_open_legs, market["contracts_by_token"]))
    for opt in OPTION_TYPES:
        leg = state[opt]
        if leg is not None and leg['instrument'].token not in open_tokens:
            alert(f'STOPLOSS FILLED: {opt} {leg["instrument"].name} (entry ~{leg["entry_price"]}) - flat until next checkpoint')
            state[opt] = None


# ── Startup adoption: reconstruct entry price from the order book ──────────────────────────────
# Ported from execution_straddle_premium_stoploss_sensex.py's _infer_entry_price_from_orderbook -
# used when this process starts up and finds positions already open at the broker (mid-day
# restart, or legs placed manually) so adoption can use the real fill price instead of assuming
# live LTP.
_ORDER_TIME_FIELDS = ('orderGeneratedTime', 'orderEntryTime', 'exchangeTime', 'orderTime')  # candidate
# order-timestamp fields, tried in order - AliceBlue's exact field name for this isn't confirmed
# anywhere else in this codebase (only brokerOrderId, orderStatus, rejectionReason,
# averageTradedPrice are established, via ers._wait_for_fill_price). Falls back to brokerOrderId
# (assigned sequentially, so still a decent recency proxy) if none of these are present.


def _order_sort_key(order):
    for field in _ORDER_TIME_FIELDS:
        if order.get(field):
            return order[field]
    return order.get('brokerOrderId', '')


def _infer_entry_price_from_orderbook(token, open_quantity):
    """Best-effort reconstruction of a short leg's actual average fill price from AliceBlue's
    order book: walks its completed SELL orders for this instrument, most-recent-first,
    accumulating filled quantity until it covers `open_quantity`, and returns the
    quantity-weighted average price across just those orders - so older fills belonging to a
    since-closed position (e.g. an earlier roll today) don't pollute the average. Returns None
    (caller falls back to live LTP) if the order book doesn't yield a confident answer - a wrong
    assumption about field names should fail closed, not silently produce a wrong entry price."""
    try:
        orders = _resilient_call(ers._order_book)
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
        log.warning(
            f'order book only accounted for {covered}/{open_quantity} of open quantity for token '
            f'{token} - using the weighted average of what it did find'
        )
    return weighted_sum / covered


# ── Checkpoint (hourly) ──────────────────────────────────────────────────────────────────────
def run_checkpoint(state, market, cfg, day, symbol):
    prev_premium = day['prev_checkpoint_premium']  # snapshot before it gets overwritten below,
    # so both the comparison and the log line below refer to the same "previous" value
    current_premium = _atm_premium(market['option_chain'], market['atm'])
    premium_increased = (
        prev_premium is not None and current_premium is not None
        and current_premium > prev_premium
    )
    if current_premium is not None:
        day['prev_checkpoint_premium'] = current_premium

    log.info(
        f'checkpoint: spot={market["spot"]} atm={market["atm"]} atm_premium={current_premium} '
        f'(prev={prev_premium}) day_pnl={day["realized_pnl"]:.2f}'
    )

    if premium_increased:
        # matches the backtest: close outright whichever legs are still open - not conditional on
        # both being open. A leg already flat from an earlier stoploss just stays flat.
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
        for opt, (instrument, ltp, strike) in desired.items():
            _enter_leg(state, opt, instrument, ltp, strike, cfg, symbol)
        return

    for opt, (instrument, ltp, strike) in desired.items():
        if state[opt] is None:
            _enter_leg(state, opt, instrument, ltp, strike, cfg, symbol)
        else:
            log.info(f'{opt} still open at {state[opt]["strike"]}, leaving as is')


# ── Minute-level checks (every poll) ─────────────────────────────────────────────────────────
def run_minute_checks(state, market, cfg, day, now):
    current_premium = _atm_premium(market['option_chain'], market['atm'])
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
        try:
            current_ltp = ers.option_ltp(market['option_chain'], leg['strike'], opt)
        except (KeyError, TypeError):
            continue
        unrealized += leg['entry_price'] - current_ltp

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
    """Periodic "I'm still working" ping, independent of alert() - deliberately doesn't go
    through alert() (which would make the heartbeat itself become "the last event", burying
    whatever real action actually happened last)."""
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
    except Exception as exc:  # heartbeats are non-critical - never let one break the loop
        print(f'heartbeat Telegram send failed: {exc}', file=sys.stderr)


# ── Day driver ────────────────────────────────────────────────────────────────────────────────
def _sleep_until(target_time, label):
    now = datetime.now()
    target = datetime.combine(now.date(), target_time)
    wait = (target - now).total_seconds()
    if wait > 0:
        log.info(f'waiting until {label} ({target_time})...')
        time_module.sleep(wait)


def run_day(symbol, trade_weekdays, entry_mode=DEFAULT_ENTRY_MODE):
    today_name = datetime.now().strftime('%A')
    if today_name not in trade_weekdays:
        log.info(f'{today_name} is not in TRADE_WEEKDAYS ({sorted(trade_weekdays)}) - not trading')
        return

    # Copy rather than mutate ers.UNDERLYINGS[symbol] - that dict is shared with
    # execution_rolling_straddle.py.
    cfg = dict(ers.UNDERLYINGS[symbol])
    override_lots = LOTS_OVERRIDE.get((symbol, today_name))
    if override_lots is not None:
        log.info(f'{symbol} {today_name}: overriding lots {cfg["lots"]} -> {override_lots}')
        cfg['lots'] = override_lots

    checkpoint_interval = CHECKPOINT_INTERVAL_OVERRIDE.get((symbol, today_name), CHECKPOINT_INTERVAL)
    if checkpoint_interval != CHECKPOINT_INTERVAL:
        log.info(f'{symbol} {today_name}: overriding checkpoint interval {CHECKPOINT_INTERVAL} -> {checkpoint_interval}')

    alert(f'Rolling straddle (variation) starting for {symbol} - {today_name} {datetime.now():%Y-%m-%d}')

    state = _new_state()
    day = dict(
        realized_pnl=0.0, halted=False, suppress_reentry=False,
        entry_premium=None, prev_checkpoint_premium=None, premium_history=[],
    )

    # Whether this is a late start has to be judged from *before* the sleep below, not after: a
    # time.sleep() wake-up is never exact to the microsecond, so entry_time.time() (captured after
    # waking) comes back a few milliseconds past ENTRY_TIME even on a perfectly on-time start -
    # comparing that to ENTRY_TIME would misclassify every on-time start as late.
    late_start = datetime.now().time() > ENTRY_TIME
    _sleep_until(ENTRY_TIME, 'entry time')
    entry_time = datetime.now()  # actual entry moment - may be later than ENTRY_TIME on a late start

    # Checkpoints stay anchored to the ENTRY_TIME grid (10:45, 11:45, ...) even on a late start -
    # a late start should just mean a late/skipped first checkpoint or two, not a schedule shifted
    # to match whenever the process happened to come up.
    scheduled_entry = datetime.combine(entry_time.date(), ENTRY_TIME)
    next_checkpoint = scheduled_entry + checkpoint_interval
    while next_checkpoint <= entry_time:
        next_checkpoint += checkpoint_interval
    next_heartbeat = entry_time + HEARTBEAT_INTERVAL

    market = _fetch_market_until_success(symbol, cfg)
    open_tokens = set(_resilient_call(ers.get_open_legs, market["contracts_by_token"]))
    pending_initial_entry = False
    if open_tokens:
        # Mid-day restart with positions already open - adopt them so rolling/re-entry keeps
        # working. Reconstruct each leg's real entry price from the AliceBlue order book's
        # completed SELL fills where possible (see _infer_entry_price_from_orderbook), falling
        # back to live LTP - logged clearly - only if that reconstruction fails. The reconstructed
        # combined entry also stands in for the previous checkpoint's premium, so the ATM-rise
        # check at the next checkpoint has something real to compare against instead of starting
        # blind.
        alert(
            'Found open positions at startup (mid-day restart?) - reconstructing entry prices '
            'from the order book where possible',
            level=logging.WARNING,
        )
        open_legs_by_token = _resilient_call(ers.get_open_legs, market["contracts_by_token"])
        for strike_opt, contract in market['contracts_by_strike_type'].items():
            token = int(contract['token'])
            if token in open_tokens:
                opt = contract['option_type']
                instrument = ers._to_instrument(contract)
                pos = open_legs_by_token[token]
                quantity = abs(int(pos['netQuantity']))
                entry_price = _infer_entry_price_from_orderbook(token, quantity)
                if entry_price is not None:
                    log.info(f'{opt} {strike_opt[0]}: reconstructed entry price {entry_price:.2f} from order book')
                else:
                    try:
                        entry_price = ers.option_ltp(market['option_chain'], strike_opt[0], opt)
                    except (KeyError, TypeError):
                        entry_price = 0.0
                    log.warning(
                        f"{opt} {strike_opt[0]}: couldn't reconstruct entry price from order book - "
                        f"falling back to live LTP {entry_price:.2f} (not the actual fill price)"
                    )
                state[opt] = dict(instrument=instrument, strike=strike_opt[0], entry_price=entry_price, quantity=quantity)

        adopted_premium = sum(leg['entry_price'] for leg in state.values() if leg is not None) or None
        day['entry_premium'] = adopted_premium
        day['prev_checkpoint_premium'] = adopted_premium
        if adopted_premium is not None:
            day['premium_history'] = [(datetime.now(), adopted_premium)]
            alert(f'Adopted legs - reconstructed combined entry ~{adopted_premium:.2f}, used as the previous-checkpoint premium')
    elif late_start and entry_mode == ENTRY_MODE_HONOR_CHECKPOINTS:
        # Late start, honoring checkpoints (the default) - stay flat, no bookkeeping yet, until
        # the main loop below hits next_checkpoint and fires the deferred entry from there.
        pending_initial_entry = True
        alert(
            f'Late start ({entry_time:%H:%M:%S}) - honoring checkpoints: staying flat until the '
            f'next checkpoint ({next_checkpoint:%H:%M}) fires the initial entry'
        )
    else:
        log.info('No open positions - entering initial legs')
        _enter_initial_day_legs(state, market, cfg, day, symbol)
    reuse_entry_market = True  # the entry-time `market` above is still fresh - don't immediately
    # re-fetch it (and hammer /optionchain a second time within the same second) on loop pass 1
    poll_failure_count = 0

    while not day['halted']:
        now = datetime.now()
        if now.time() >= EXIT_TIME:
            break

        try:
            if reuse_entry_market:
                reuse_entry_market = False
            else:
                market = _fetch_market(symbol, cfg)
            _sync_stopped_out_legs(state, market)
            run_minute_checks(state, market, cfg, day, now)

            if not day['halted'] and now >= next_checkpoint:
                if pending_initial_entry:
                    log.info('Checkpoint reached - firing the deferred initial entry')
                    _enter_initial_day_legs(state, market, cfg, day, symbol)
                    pending_initial_entry = False
                else:
                    run_checkpoint(state, market, cfg, day, symbol)
                next_checkpoint += checkpoint_interval

            if now >= next_heartbeat:
                _send_heartbeat(state, day, symbol, now)
                next_heartbeat += HEARTBEAT_INTERVAL
            poll_failure_count = 0
        except Exception as exc:
            # _resilient_call already smooths over transient 429s/5xx/network blips inside
            # _fetch_market - this is the outer net for anything else (a bug, AliceBlue session
            # expiring mid-day, etc.): skip this poll and try again next cycle rather than letting
            # the whole session die and leave open legs unmanaged for the rest of the day.
            poll_failure_count += 1
            _alert_failure_throttled(f'Poll iteration failed, skipping to next cycle: {exc}', poll_failure_count)

        sleep_for = POLL_INTERVAL_SECONDS
        remaining_to_exit = (datetime.combine(now.date(), EXIT_TIME) - datetime.now()).total_seconds()
        time_module.sleep(max(0, min(sleep_for, remaining_to_exit)))

    if not day['halted']:
        log.info(f'{EXIT_TIME} reached - squaring off any open positions')
    # Belt-and-braces square-off: goes through the broker's own position list rather than our
    # local `state`, so it also cleans up anything local bookkeeping lost track of. It's safe to
    # retry wholesale on failure - each call re-derives "what's still open" from the broker rather
    # than local memory, and close_leg() cancels any resting exit order for a leg before replacing
    # it, so a retry after a partial failure just re-squares-off whatever is still open rather than
    # double-submitting. Unlike _resilient_call this isn't read-only (it places exit orders), so it
    # gets its own loop here rather than reusing that helper. This is belt-and-braces itself: the
    # very call that used to die uncaught on a plain /optionchain 429 and leave positions open for
    # the rest of the day (see _resilient_call's docstring) - it must not go back to failing silently.
    for attempt in range(RETRY_MAX_ATTEMPTS):
        try:
            ers.exit_all_positions(symbol, cfg)
            break
        except Exception as exc:
            if attempt == RETRY_MAX_ATTEMPTS - 1:
                alert(f'{symbol}: final square-off failed after {RETRY_MAX_ATTEMPTS} attempts - '
                      f'positions may still be OPEN, check manually: {exc}', level=logging.CRITICAL)
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            log.warning(
                f'exit_all_positions failed ({exc}) - retrying in {delay:.0f}s '
                f'(attempt {attempt + 1}/{RETRY_MAX_ATTEMPTS})'
            )
            time_module.sleep(delay)

    alert(f'Rolling straddle (variation) done for {symbol} - realized pnl {day["realized_pnl"]:+.2f} points')


if __name__ == '__main__':
    SYMBOL = sys.argv[1].upper() if len(sys.argv) > 1 else 'NIFTY'
    TRADE_WEEKDAYS = _parse_trade_weekdays(sys.argv[2] if len(sys.argv) > 2 else None)
    ENTRY_MODE = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_ENTRY_MODE
    if ENTRY_MODE not in ENTRY_MODES:
        raise ValueError(f'unknown entry mode {ENTRY_MODE!r} - use one of {ENTRY_MODES}')
    run_day(SYMBOL, TRADE_WEEKDAYS, ENTRY_MODE)
