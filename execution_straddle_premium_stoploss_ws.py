"""
Live execution: short SENSEX ATM straddles between 10:15 and 15:13, managed by combined-premium
and daily-loss stoplosses rather than a fixed per-leg stoploss percentage.

Window: 10:15 (start/first entry) -> 15:13 (end/EOD square-off). Checkpoints on the hour-and-15:
10:15, 11:15, 12:15, 13:15, 14:15.

At 10:15 (entry): short the ATM straddle (ATM CE + ATM PE).

Every minute, while a position is open:
  - Combined stoploss: sum the live premium move against entry across the two held legs. If that
    combined loss reaches COMBINED_STOPLOSS_POINTS, close everything that's open. Stays flat - no
    immediate re-entry - until the next checkpoint.
  - Daily loss limit: track realized + unrealized pnl for the day. If it drops to DAILY_LOSS_LIMIT,
    close everything and stop trading entirely for the rest of the day - no more checkpoints get
    evaluated.

At each checkpoint from 11:15 to 14:15, in priority order:
  1. Compare the live ATM premium right now to the ATM premium recorded at the previous checkpoint
     (this happens regardless of whether a position is currently open). If it's higher, close
     whatever's open and take no new trade this checkpoint - stay flat until the checkpoint after
     next. Otherwise continue to step 2.
  2. Check whether the strike currently tagged ATM has moved off whatever strike the held position
     is on. If it drifted, close the existing straddle and immediately short a new ATM straddle at
     the current strike. If it's still the same strike, leave the existing position exactly as it
     is (same entry price/time) and keep monitoring - nothing happens this checkpoint. If nothing
     is currently open at this point (e.g. flattened earlier by the combined stoploss or an earlier
     premium-rise), this step just opens a fresh ATM straddle instead of rolling one.

At 15:13: force-close anything still open (EOD).

Each leg also gets its own extreme-move stoploss at entry, LEG_STOPLOSS_POINTS away from that leg's
entry price - a backstop independent of the combined/daily checks above, in case this process itself
is down when a leg blows through it. Whenever this script closes a leg itself (combined stoploss,
checkpoint roll, premium-rise flatten, daily loss limit, or EOD), that resting stoploss order is
cancelled and the leg is closed with a LIMIT order priced through the LTP (like
execution_rolling_straddle's exits) for a fast, near-certain fill - AliceBlue rejects MARKET orders
outright (EC965 "Market orders are not allowed."), so no order type in this codebase ever uses MARKET.

Progress (today's checkpoints already run, the current ATM strike, its per-leg entry prices, and
realized pnl so far) is persisted to STATE_FILE after every state change, so a restart mid-day
resumes from the last checkpoint instead of re-entering or losing track of an open position.

Every startup additionally reconciles against the broker (_adopt_or_reenter_at_startup) rather than
trusting persisted state alone, since state can go stale in either direction while the process is
down - a position it thinks is open may have been closed manually or by its own extreme-move
stoploss, and one it thinks is flat may have been opened manually or rolled to a different strike.
An adopted straddle's per-leg entry price is reconstructed from the order book's completed SELL
fills where possible (falling back to live LTP, logged as an approximation, if it can't be); the
combined entry also stands in for the previous checkpoint's premium. If no short leg is open at all
and no checkpoint has run yet today, the strategy is treated as not having started and enters
immediately, with the current moment standing in for the first checkpoint.

Every STATUS_PING_INTERVAL (default 30 min), logs a "still running fine" heartbeat with current
position/pnl status - since execution_rolling_straddle's TelegramHandler forwards every log.info()
call, this shows up as a periodic Telegram update even during long gaps between checkpoints, so a
silent process reads differently from one that's still alive and just waiting.

Reuses execution_rolling_straddle's Dhan/AliceBlue REST plumbing (auth, contract master, option
chain, order placement, tick rounding, logging/Telegram setup) - importing it also runs its
Dhan/AliceBlue auth checks.
"""

import json
import os
import time as time_module
from datetime import datetime, time as dtime, timedelta

import requests

import execution_rolling_straddle_tn as ers

log = ers.log

SYMBOL = 'SENSEX'
# Copy rather than reuse ers.UNDERLYINGS[SYMBOL] directly - this strategy trades 5 lots, but that
# dict is shared module state; mutating it in place would also change execution_rolling_straddle
# (and anything else importing it, e.g. day_end_straddle_buy.py) out from under them.
CFG = dict(ers.UNDERLYINGS[SYMBOL], lots=5)

STATE_FILE = os.path.join(os.path.dirname(__file__), 'execution_straddle_premium_stoploss_sensex_state.json')

CHECKPOINT_TIMES = [dtime(10, 15), dtime(11, 15), dtime(12, 15), dtime(13, 15), dtime(14, 15)]
EXIT_TIME = dtime(15, 13)  # EOD square-off

COMBINED_STOPLOSS_POINTS = 50  # combined CE+PE premium move against entry that flattens the straddle
DAILY_LOSS_LIMIT = -100  # realized + unrealized pnl (points) at which trading stops for the rest of the day
LEG_STOPLOSS_POINTS = 300  # per-leg extreme-move stoploss distance from that leg's entry price

MONITOR_INTERVAL = 60  # seconds between combined-stoploss / daily-loss-limit checks while a position is open
MISSED_CHECKPOINT_GRACE = 90  # seconds past a checkpoint time beyond which it's skipped rather than run late
STATUS_PING_INTERVAL = timedelta(minutes=30)  # cadence for the "still running fine" Telegram heartbeat
# during the (up to ~1 hour) gaps between checkpoints - ers.log's TelegramHandler forwards every
# log.info() call, so this is what actually shows up as a periodic Telegram update; without it,
# the only Telegram traffic would be checkpoint/stoploss events, and a long quiet stretch reads the
# same whether the process is fine or has silently died.


def _label(t):
    return f'{t:%H:%M}'


def _today_str():
    return datetime.now().strftime('%Y-%m-%d')


def _fresh_state():
    return {
        'date': _today_str(),
        'strike': None,
        'entry_ce': None,
        'entry_pe': None,
        'position_open': False,
        'last_checkpoint_atm_premium': None,
        'realized_pnl_points': 0.0,
        'stopped_for_day': False,
        'checkpoints_done': [],
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


# ── Resilient Dhan/AliceBlue reads ──────────────────────────────────────────
# Ported from execution_rolling_straddle_variation.py's _resilient_call/_throttle - this script
# hit the same uncaught 429 from ers.get_option_chain() that motivated that one. Dhan's
# /optionchain is documented at ~1 request/3s; this script calls it from several places
# (checkpoints, the once-a-minute monitor tick, status pings, closes) that can otherwise land
# close together, so proactively throttle ahead of time instead of only reacting to 429s after
# the fact.
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY = 5  # seconds; doubled on each successive attempt (5, 10, 20, 40)

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
    """Call a read-only Dhan/AliceBlue GET (spot LTP, option chain, contract master) with retry +
    backoff through transient failures - a 429 rate limit, a 5xx, a network blip - instead of
    letting them turn into an uncaught exception (this is exactly how a single /optionchain 429
    took the process down before this was added). Respects a 429's Retry-After header when
    present, and proactively throttles (see _throttle above) before every attempt."""
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


# ── Instruments / pricing ────────────────────────────────────────────────────
def _load_contracts_and_option_chain():
    contracts = _resilient_call(ers.load_current_week_options, SYMBOL, CFG['aliceblue_exchange'])
    expiry_date = datetime.fromtimestamp(contracts[0]['expiry_date'] / 1000, tz=ers.timezone.utc).strftime('%Y-%m-%d')
    option_chain = _resilient_call(ers.get_option_chain, expiry_date, CFG['dhan_security_id'], CFG['dhan_segment'])
    return contracts, option_chain


def _current_atm(option_chain):
    spot = _resilient_call(ers.get_spot_ltp, CFG['dhan_security_id'], CFG['dhan_segment'])
    strike = ers.atm_strike(spot, CFG['strike_interval'])
    ce_ltp = ers.option_ltp(option_chain, strike, 'CE')
    pe_ltp = ers.option_ltp(option_chain, strike, 'PE')
    return strike, ce_ltp, pe_ltp


# ── Orders ────────────────────────────────────────────────────────────────
def short_leg_with_extreme_sl(instrument, quantity, ltp):
    """Short one leg with a resting extreme-move stoploss LEG_STOPLOSS_POINTS above entry - a
    backstop independent of this script's own combined/daily-loss monitoring. Returns the entry
    fill price (or `ltp` in DRY_RUN, as a stand-in for pnl bookkeeping)."""
    entry_price = ers._round_to_tick(ltp * (1 - ers.LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if ers.DRY_RUN else ""}SELL {quantity} x {instrument.name} LIMIT @ {entry_price} (ltp {ltp})'
    log.info(tag)
    if ers.DRY_RUN:
        return entry_price

    entry = ers._place_order(
        'SELL', instrument, quantity, 'LIMIT', price=str(entry_price), order_tag='premium_sl_entry',
    )
    order_no = entry.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} entry order rejected: {entry}')

    entry_price = ers._wait_for_fill_price(order_no)
    trigger_price = round(entry_price + LEG_STOPLOSS_POINTS, 1)
    sl_limit_price = ers._round_to_tick(trigger_price * (1 + ers.LIMIT_OFFSET_PCT), instrument.tick_size)
    log.info(f'{instrument.name} entered @ {entry_price}, extreme-move SL trigger {trigger_price} limit {sl_limit_price}')

    ers._place_order(
        'BUY', instrument, quantity, 'SL', price=str(sl_limit_price),
        trigger_price=trigger_price, order_tag='premium_sl_leg_stoploss',
    )
    return entry_price


def close_leg_market(instrument, quantity, ltp):
    """Cancel this leg's resting extreme-move SL (if still resting) and close it with a LIMIT order
    priced through the LTP - AliceBlue rejects true MARKET orders (EC965), so this is the closest
    equivalent: an aggressively-priced LIMIT that fills like a market order would. This is always an
    urgent, stoploss-driven close (combined stoploss, checkpoint roll, premium-rise flatten, daily
    loss limit, or EOD). Returns the fill price used for pnl bookkeeping (falls back to `ltp` in
    DRY_RUN or if the fill price can't be confirmed)."""
    exit_price = ers._round_to_tick(ltp * (1 + ers.LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if ers.DRY_RUN else ""}BUY (market square off) {quantity} x {instrument.name} LIMIT @ {exit_price} (ltp {ltp})'
    log.info(tag)
    if ers.DRY_RUN:
        return exit_price

    for o in ers._order_book():
        if str(o.get('instrumentId')) == str(instrument.token) and str(o.get('orderStatus', '')).lower() not in ers.TERMINAL_ORDER_STATUSES:
            ers._cancel_order(o['brokerOrderId'])

    result = ers._place_order(
        'BUY', instrument, quantity, 'LIMIT', price=str(exit_price), order_tag='premium_sl_exit_market',
    )
    order_no = result.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} exit order rejected: {result}')
    try:
        return ers._wait_for_fill_price(order_no)
    except Exception as exc:
        log.warning(f'could not confirm exit fill price for {instrument.name}: {exc} - using ltp {ltp} for pnl bookkeeping')
        return ltp


def enter_straddle(strike, contracts_by_strike_type, option_chain):
    entry = {}
    for opt in ('CE', 'PE'):
        contract = contracts_by_strike_type[(strike, opt)]
        instrument = ers._to_instrument(contract)
        ltp = ers.option_ltp(option_chain, strike, opt)
        entry[opt] = short_leg_with_extreme_sl(instrument, instrument.lot_size * CFG['lots'], ltp)
    return entry


def close_open_position(state):
    """Close whatever's actually open for SYMBOL right now (source of truth: AliceBlue positions,
    not `state` - so a leg already closed by its own extreme-move SL isn't double-closed), and
    update state's realized pnl accordingly."""
    contracts, option_chain = _load_contracts_and_option_chain()
    contracts_by_token = {int(c['token']): c for c in contracts}
    open_legs = _resilient_call(ers.get_open_legs, contracts_by_token)
    if not open_legs:
        log.info(f'No open {SYMBOL} legs to close (already flat)')
        state['position_open'] = False
        state['entry_ce'] = None
        state['entry_pe'] = None
        _save_state(state)
        return

    pnl = 0.0
    for token, pos in open_legs.items():
        contract = contracts_by_token[token]
        opt = contract['option_type']
        strike = int(float(contract['strike_price']))
        ltp = ers.option_ltp(option_chain, strike, opt)
        instrument = ers._to_instrument(contract)
        exit_price = close_leg_market(instrument, abs(int(pos['netQuantity'])), ltp)
        entry_price = state['entry_ce'] if opt == 'CE' else state['entry_pe']
        if entry_price is not None:
            pnl += entry_price - exit_price  # short: profit when exit < entry

    state['realized_pnl_points'] += pnl
    state['position_open'] = False
    state['entry_ce'] = None
    state['entry_pe'] = None
    log.info(f'Closed {SYMBOL} straddle - realized pnl this close ~{pnl:.1f} pts, day total ~{state["realized_pnl_points"]:.1f} pts')
    _save_state(state)


# ── Startup reconciliation ───────────────────────────────────────────────────
# Candidate order-timestamp fields to sort the order book by, most-recent-first, when
# reconstructing a leg's actual fill price below - tried in order since AliceBlue's exact field
# name for this isn't confirmed anywhere else in this codebase (only brokerOrderId, orderStatus,
# rejectionReason, averageTradedPrice are established, via _wait_for_fill_price). Falls back to
# brokerOrderId (broker order ids are assigned sequentially, so it's at least a decent proxy for
# recency) if none of these are present.
_ORDER_TIME_FIELDS = ('orderGeneratedTime', 'orderEntryTime', 'exchangeTime', 'orderTime')


def _order_sort_key(order):
    for field in _ORDER_TIME_FIELDS:
        if order.get(field):
            return order[field]
    return order.get('brokerOrderId', '')


def _infer_entry_price_from_orderbook(token, open_quantity):
    """Best-effort reconstruction of the actual average execution price for a short leg that's
    already open at the broker but wasn't entered by this process (e.g. placed manually), so
    adoption can use the real fill price instead of live LTP. Walks the order book's completed
    SELL orders for this instrument, most-recent-first, accumulating filled quantity until it
    covers `open_quantity`, and returns the quantity-weighted average price across just those
    orders - so older fills belonging to a since-closed position (e.g. an earlier roll today)
    don't pollute the average. Returns None (caller falls back to live LTP) if the order book
    doesn't yield a confident answer - a wrong assumption about field names should fail closed,
    not silently produce a wrong entry price."""
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


def _adopt_straddle(state, strike, short_legs, option_chain):
    """Adopt a short ATM straddle found open at the broker but not (or no longer accurately)
    tracked in `state`, reconstructing each leg's real entry price from the order book's completed
    SELL fills for that instrument (see _infer_entry_price_from_orderbook) instead of assuming
    live LTP. The combined entry (CE + PE) also stands in for the 'previous checkpoint' premium
    that run_checkpoint's step 1 (premium-rise check) compares against. A leg whose fill price
    can't be reconstructed (order book doesn't have it, or doesn't parse as expected) falls back to
    its live LTP - logged clearly, since that's an approximation, not the real fill."""
    entry_price = {}
    for opt in ('CE', 'PE'):
        token, pos = short_legs[(strike, opt)]
        open_quantity = abs(int(pos.get('netQuantity', 0)))
        fill_price = _infer_entry_price_from_orderbook(token, open_quantity)
        if fill_price is not None:
            entry_price[opt] = fill_price
            log.info(f'{SYMBOL} {strike}{opt}: reconstructed entry price {fill_price:.2f} from order book')
        else:
            entry_price[opt] = ers.option_ltp(option_chain, strike, opt)
            log.warning(
                f"{SYMBOL} {strike}{opt}: couldn't reconstruct entry price from order book - "
                f"falling back to live LTP {entry_price[opt]:.2f} (not the actual fill price)"
            )
    combined = entry_price['CE'] + entry_price['PE']
    log.warning(
        f'Adopting open {SYMBOL} {strike} short straddle at startup - entry CE {entry_price["CE"]:.2f} '
        f'/ PE {entry_price["PE"]:.2f} (combined ~{combined:.1f}), also used as the previous-checkpoint premium'
    )
    state.update(strike=strike, entry_ce=entry_price['CE'], entry_pe=entry_price['PE'], position_open=True,
                  last_checkpoint_atm_premium=combined)
    now = datetime.now().time()
    for cp_time in CHECKPOINT_TIMES:
        label = _label(cp_time)
        if cp_time <= now and label not in state['checkpoints_done']:
            state['checkpoints_done'].append(label)
    _save_state(state)


def _adopt_or_reenter_at_startup(state):
    """Runs once at startup, always reconciling against what's actually open at the broker for
    SYMBOL rather than trusting persisted state alone - state can be stale in either direction: a
    position it thinks is still open may have been closed (manually, or its own extreme-move SL
    firing) while this process was down, and one it thinks is flat may have been opened manually.

      - State says a position is open, and the broker confirms the same strike is still short
        both CE and PE: trust the persisted entry prices (this process itself recorded them at
        fill time) and just resume - no change.

      - State says a position is open at strike X, but the broker instead shows a short straddle
        at a different strike (rolled manually while this process was down): adopt the broker's
        actual strike/legs via _adopt_straddle, replacing the stale one.

      - State says a position is open, but the broker shows no open short legs at all (closed
        manually, or its own extreme-move SL already closed it): mark flat. The realized pnl from
        that close isn't recoverable from here (this process wasn't the one that closed it), so
        it's left out of realized_pnl_points rather than guessed at - logged clearly so it's not
        mistaken for a tracked close.

      - State says flat, and the broker shows an open short ATM straddle (placed manually, or
        state was lost/reset): adopt it via _adopt_straddle.

      - State says flat, and the broker shows no short leg at all (a long position, if any, is
        left alone - not ours) and no checkpoint has run yet today: the strategy hasn't actually
        started, so enter right now, treating the current moment as the CHECKPOINT_TIMES[0]
        checkpoint - same as a normal on-time start, just later than usual.

    A mix of short legs that doesn't cleanly form a single-strike straddle is left untouched
    (logged, not auto-adopted or auto-cleared) rather than guessed at."""
    contracts, option_chain = _load_contracts_and_option_chain()
    contracts_by_token = {int(c['token']): c for c in contracts}
    open_legs = _resilient_call(ers.get_open_legs, contracts_by_token)

    short_legs = {}
    for token, pos in open_legs.items():
        if int(pos.get('netQuantity', 0)) >= 0:
            continue  # flat or long - not ours to adopt
        contract = contracts_by_token[token]
        short_legs[(int(float(contract['strike_price'])), contract['option_type'])] = (token, pos)

    strikes = {strike for strike, _ in short_legs}
    broker_strike = next(iter(strikes)) if len(strikes) == 1 and {'CE', 'PE'} <= {opt for _, opt in short_legs} else None

    if state['position_open']:
        if broker_strike == state['strike']:
            log.info(f'Broker confirms {SYMBOL} {state["strike"]} short straddle still open, matching state - resuming as tracked')
            return
        if broker_strike is not None:
            log.warning(
                f'State says {SYMBOL} {state["strike"]} is open, but broker shows a short straddle '
                f'at {broker_strike} instead (rolled manually while this process was down?) - adopting the broker\'s position'
            )
            _adopt_straddle(state, broker_strike, short_legs, option_chain)
            return
        if not short_legs:
            log.warning(
                f'State says {SYMBOL} {state["strike"]} is open, but the broker shows no open short legs '
                f'(closed manually, or its own extreme-move stoploss fired while this process was down?) - '
                f'marking flat; that close\'s realized pnl is unknown so it is not added to realized_pnl_points'
            )
            state.update(position_open=False, entry_ce=None, entry_pe=None)
            _save_state(state)
            return
        log.warning(
            f"State says {SYMBOL} {state['strike']} is open, but the broker's open short leg(s) don't "
            f"cleanly match ({sorted(short_legs)}) - leaving state as is, not auto-adjusting"
        )
        return

    if broker_strike is not None:
        log.warning(f'Found an open {SYMBOL} {broker_strike} short straddle at startup not tracked in state (placed manually, or a lost state file?)')
        _adopt_straddle(state, broker_strike, short_legs, option_chain)
        return

    if short_legs:
        log.warning(
            f"Open {SYMBOL} short leg(s) at startup don't form a clean single-strike straddle "
            f"({sorted(short_legs)}) - leaving state as is, not auto-adopting"
        )
        return

    entry_label = _label(CHECKPOINT_TIMES[0])
    if datetime.now().time() >= CHECKPOINT_TIMES[0] and entry_label not in state['checkpoints_done']:
        log.info(
            f'No open {SYMBOL} short position and no checkpoint run yet today - treating now as '
            f'the {entry_label} checkpoint and entering'
        )
        run_checkpoint(state, entry_label, is_entry_checkpoint=True)


# ── Monitoring (combined stoploss / daily loss limit) ───────────────────────
def _monitor_tick(state):
    """Runs once a minute while a position is open. Returns True if it closed the position."""
    _, option_chain = _load_contracts_and_option_chain()
    ce_ltp = ers.option_ltp(option_chain, state['strike'], 'CE')
    pe_ltp = ers.option_ltp(option_chain, state['strike'], 'PE')
    combined_move = (ce_ltp + pe_ltp) - (state['entry_ce'] + state['entry_pe'])  # positive = adverse for the short
    day_pnl = state['realized_pnl_points'] - combined_move

    # Console-only (not log.info/warning) - deliberately bypasses the file/Telegram handlers via
    # _REAL_STDOUT so this once-a-minute tick doesn't spam Telegram (STATUS_PING_INTERVAL above is
    # the intentional cadence for that); this is purely so someone watching the terminal can see
    # the live premium move without waiting on the 30-min heartbeat or a checkpoint.
    print(
        f'{datetime.now():%H:%M:%S} monitor tick: {SYMBOL} {state["strike"]} premium move {combined_move:+.1f} pts '
        f'vs entry (stoploss at +{COMBINED_STOPLOSS_POINTS}) - day pnl ~{day_pnl:.1f} pts',
        file=ers._REAL_STDOUT,
    )

    if day_pnl <= DAILY_LOSS_LIMIT:
        log.warning(f'Daily loss limit hit: day pnl ~{day_pnl:.1f} <= {DAILY_LOSS_LIMIT} - closing everything, stopping for the day')
        close_open_position(state)
        state['stopped_for_day'] = True
        _save_state(state)
        return True

    if combined_move >= COMBINED_STOPLOSS_POINTS:
        log.warning(f'Combined premium stoploss hit: premium moved {combined_move:.1f} pts (>= {COMBINED_STOPLOSS_POINTS}) against entry - closing straddle, staying flat until next checkpoint')
        close_open_position(state)
        return True

    return False


def _status_line(state):
    if state['stopped_for_day']:
        return f'stopped for the day - realized pnl ~{state["realized_pnl_points"]:.1f} pts'
    if not state['position_open']:
        return f'flat - realized pnl so far ~{state["realized_pnl_points"]:.1f} pts'
    try:
        _, option_chain = _load_contracts_and_option_chain()
        ce_ltp = ers.option_ltp(option_chain, state['strike'], 'CE')
        pe_ltp = ers.option_ltp(option_chain, state['strike'], 'PE')
        combined_move = (ce_ltp + pe_ltp) - (state['entry_ce'] + state['entry_pe'])
        return (f'holding {state["strike"]} straddle, premium move {combined_move:+.1f} pts vs entry '
                f'(stoploss at +{COMBINED_STOPLOSS_POINTS}) - day pnl ~{state["realized_pnl_points"] - combined_move:.1f} pts')
    except Exception as exc:
        return f'holding {state["strike"]} straddle - couldn\'t refresh live premium for status line: {exc}'


_next_status_ping = 0.0  # time_module.time() timestamp; module-level so it persists across the
# several _sleep_and_monitor() calls run_day() makes over the course of the day


def _status_ping(state):
    global _next_status_ping
    now = time_module.time()
    if now < _next_status_ping:
        return
    log.info(f'still running - {_status_line(state)}')
    _next_status_ping = now + STATUS_PING_INTERVAL.total_seconds()


def _sleep_and_monitor(state, until_time):
    """Sleep until `until_time` (a datetime.time today), checking the combined-premium/daily-loss
    stoplosses roughly once a minute while a position is open, and sending a "still running fine"
    heartbeat to Telegram (via log.info -> TelegramHandler) every STATUS_PING_INTERVAL regardless.
    Returns as soon as `until_time` is reached or trading is stopped for the day - the caller
    re-checks state['stopped_for_day']."""
    while True:
        now = datetime.now()
        target = datetime.combine(now.date(), until_time)
        remaining = (target - now).total_seconds()
        if remaining <= 0 or state['stopped_for_day']:
            return
        if state['position_open']:
            try:
                _monitor_tick(state)
            except Exception:
                log.exception('monitor tick failed - will retry next interval')
        _status_ping(state)
        time_module.sleep(min(remaining, MONITOR_INTERVAL))


# ── Checkpoint ────────────────────────────────────────────────────────────
def run_checkpoint(state, checkpoint_label, is_entry_checkpoint):
    contracts, option_chain = _load_contracts_and_option_chain()
    contracts_by_strike_type = {(int(float(c['strike_price'])), c['option_type']): c for c in contracts}
    strike, ce_ltp, pe_ltp = _current_atm(option_chain)
    current_atm_premium = ce_ltp + pe_ltp
    log.info(f'{datetime.now():%H:%M:%S} {checkpoint_label} checkpoint: {SYMBOL} atm_strike={strike} combined_atm_premium={current_atm_premium:.1f}')

    if is_entry_checkpoint:
        entry = enter_straddle(strike, contracts_by_strike_type, option_chain)
        state.update(strike=strike, entry_ce=entry['CE'], entry_pe=entry['PE'], position_open=True,
                      last_checkpoint_atm_premium=current_atm_premium)
        state['checkpoints_done'].append(checkpoint_label)
        _save_state(state)
        return

    # 1. premium-rise check, regardless of whether a position is currently open
    prev = state['last_checkpoint_atm_premium']
    if prev is not None and current_atm_premium > prev:
        log.info(f'ATM premium {current_atm_premium:.1f} > previous checkpoint {prev:.1f} - closing any open position, no new trade this checkpoint')
        close_open_position(state)
        state['last_checkpoint_atm_premium'] = current_atm_premium
        state['checkpoints_done'].append(checkpoint_label)
        _save_state(state)
        return

    # 2. strike-drift check (or fresh entry if nothing's open)
    if state['position_open'] and state['strike'] != strike:
        log.info(f'ATM strike moved {state["strike"]} -> {strike} - rolling straddle')
        close_open_position(state)
        entry = enter_straddle(strike, contracts_by_strike_type, option_chain)
        state.update(strike=strike, entry_ce=entry['CE'], entry_pe=entry['PE'], position_open=True)
    elif state['position_open']:
        log.info(f'ATM strike unchanged ({strike}) - leaving existing position as is')
    else:
        log.info('No open position - opening fresh ATM straddle')
        entry = enter_straddle(strike, contracts_by_strike_type, option_chain)
        state.update(strike=strike, entry_ce=entry['CE'], entry_pe=entry['PE'], position_open=True)

    state['last_checkpoint_atm_premium'] = current_atm_premium
    state['checkpoints_done'].append(checkpoint_label)
    _save_state(state)


def run_day():
    global _next_status_ping
    state = _load_state()
    log.info(f'Starting {SYMBOL} premium-stoploss execution for {state["date"]} - {_status_line(state)}')
    _next_status_ping = time_module.time() + STATUS_PING_INTERVAL.total_seconds()

    if not state['stopped_for_day']:
        _adopt_or_reenter_at_startup(state)

    for cp_time in CHECKPOINT_TIMES:
        label = _label(cp_time)
        if label in state['checkpoints_done']:
            continue
        if state['stopped_for_day']:
            log.info('stopped for the day (daily loss limit) - skipping remaining checkpoints')
            break

        now = datetime.now()
        target = datetime.combine(now.date(), cp_time)
        wait = (target - now).total_seconds()
        if wait > 0:
            log.info(f'waiting until {cp_time} ...')
            _sleep_and_monitor(state, cp_time)
            if state['stopped_for_day']:
                break
        elif -wait > MISSED_CHECKPOINT_GRACE:
            log.info(f'missed {cp_time} (already {-wait:.0f}s past) - skipping')
            state['checkpoints_done'].append(label)
            _save_state(state)
            continue

        run_checkpoint(state, label, is_entry_checkpoint=(cp_time == CHECKPOINT_TIMES[0]))

    if not state['stopped_for_day']:
        now = datetime.now()
        target = datetime.combine(now.date(), EXIT_TIME)
        wait = (target - now).total_seconds()
        if wait > 0:
            log.info(f'waiting until {EXIT_TIME} to square off...')
            _sleep_and_monitor(state, EXIT_TIME)
        else:
            log.info(f'{EXIT_TIME} already passed ({-wait:.0f}s ago) - squaring off now')

    log.info(f'{EXIT_TIME} EOD square-off')
    close_open_position(state)
    if 'EOD' not in state['checkpoints_done']:
        state['checkpoints_done'].append('EOD')
        _save_state(state)


if __name__ == '__main__':
    run_day()
