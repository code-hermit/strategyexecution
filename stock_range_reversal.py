"""
Live runner for the opening-range failed-breakout reversal on 99 NSE stocks.

Layers (each in its own file, this one only wires them together):
  data      stock_data_zerodha.py         - Zerodha historical API, 1-minute candles
  rules     stock_signal_range_reversal.py - range / arming / first-break logic, prices, sizing
  execution stock_broker_aliceblue.py      - AliceBlue orders (or PaperBroker when DRY_RUN)

Day timeline (IST):
  start ~10:10   auth both brokers, load instruments, load account equity
  10:15:05       download today's candles 09:15-10:14 for all stocks -> each stock's range
  every minute   download the new candle for every stock (about 35 s for 99 stocks), feed it to
                 the rules; when a tracked stock makes its first break, rest an SL-limit entry
                 order at the opposite end of the range
  every 5 s      read the order book: an entry that filled gets its 0.5% SL-limit stop at once;
                 a stop that filled closes the trade
  14:30          cancel every entry order that hasn't triggered
  15:15          cancel stops, square off open positions at market, write the day's summary,
                 update the equity file (compounding)

Two threads: the data thread (minute candles -> signals, onto a queue) and the main thread (order
book polling, entries, stops, cut-offs). The main thread never waits on the ~35 s data pass, so a
filled entry gets its stop within a few seconds.

Daily cap: at most 10 entries fill per day. Several entry orders can rest at once; when the 10th
fills, the rest are cancelled (two filling in the same second could overshoot - logged).

Entry orders are placed only when price is near them: AliceBlue blocks margin for resting orders as
well as positions, and on a busy day 15-20 stocks can be waiting for their second break at once -
far more than the margin can carry as resting orders. So a first break only ARMS the stock; every
5 s one batch LTP call checks all armed stocks, and the SL-limit entry is placed once price is
within PLACE_WITHIN_PCT of the trigger (nearest first, while (open positions + resting entries) /
leverage fits in the equity). A resting entry whose stock drifts more than RECALL_BEYOND_PCT away is
cancelled and the stock re-armed, freeing its margin. A level that is jumped straight through
between two checks is still entered with a limit at the SL-limit's own limit price, or skipped if
price has already run past that.

Sizing (stock_signal_range_reversal.trade_qty): equity from stock_range_reversal_equity.json; the
day's first 4 fills get the profit above the starting capital spread across them. A resting
entry is re-sized (modified) when it moves from a "first 4" slot to a normal one.

DRY_RUN (default true, like the other execution scripts): PaperBroker, no orders sent. Set
DRY_RUN=false in .env to trade.

State: logs/stock_range_reversal_<date>.json is rewritten on every change; on a restart the day's
trades are reloaded so no stock is entered twice.
"""

import json
import logging
import os
import queue
import signal
import sys
import threading
import time as time_module
from datetime import datetime, timedelta

from stock_common import HERE, alert, get_logger, now_ist
import stock_signal_range_reversal as rules
from stock_data_zerodha import ZerodhaMinuteData
from stock_broker_aliceblue import AliceBlueBroker, PaperBroker

DRY_RUN = os.getenv('DRY_RUN', 'true').lower() != 'false'

UNIVERSE = [
    'ABB', 'ADANIENT', 'ADANIGREEN', 'ADANIPORTS', 'ADANIPOWER', 'AMBUJACEM', 'APOLLOHOSP', 'ASHOKLEY',
    'ASIANPAINT', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJAJHLDNG', 'BAJFINANCE', 'BANKBARODA', 'BEL',
    'BHARTIARTL', 'BOSCHLTD', 'BPCL', 'BRITANNIA', 'CANBK', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COLPAL',
    'DABUR', 'DIVISLAB', 'DLF', 'DMART', 'DRREDDY', 'EICHERMOT', 'ETERNAL', 'GAIL', 'GODREJCP',
    'GODREJPROP', 'GRASIM', 'HAL', 'HAVELLS', 'HCLTECH', 'HDFCBANK', 'HDFCLIFE', 'HINDALCO', 'HINDUNILVR',
    'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'INDHOTEL', 'INDIGO', 'INDUSINDBK', 'INFY', 'IOC', 'IRFC', 'ITC',
    'JIOFIN', 'JSWSTEEL', 'KOTAKBANK', 'LICI', 'LODHA', 'LT', 'LUPIN', 'M&M', 'MARICO', 'MARUTI',
    'MOTHERSON', 'MUTHOOTFIN', 'NAUKRI', 'NESTLEIND', 'NTPC', 'ONGC', 'PAGEIND', 'PFC', 'PIDILITIND',
    'PIIND', 'PNB', 'POLYCAB', 'POWERGRID', 'RECLTD', 'RELIANCE', 'SBILIFE', 'SBIN', 'SHREECEM',
    'SHRIRAMFIN', 'SIEMENS', 'SRF', 'SUNPHARMA', 'TATACONSUM', 'TATAPOWER', 'TATASTEEL', 'TCS', 'TECHM',
    'TITAN', 'TMPV', 'TORNTPHARM', 'TRENT', 'TVSMOTOR', 'ULTRACEMCO', 'UNITDSPR', 'VEDL', 'WIPRO',
]

ENTRY_LIMIT_BUFFER_PCT = 0.2        # SL-limit entry: limit this far beyond the trigger
STOP_LIMIT_BUFFER_PCT = 0.5         # protective SL-limit: limit this far beyond its trigger
EXIT_LIMIT_BUFFER_PCT = 0.5         # fallback marketable LIMIT if a MARKET exit is rejected
PLACE_WITHIN_PCT = 0.3             # place the resting entry once LTP is this close to its trigger
RECALL_BEYOND_PCT = 0.6            # cancel a resting entry (free its margin) once LTP is this far away
POLL_SECONDS = 5
REFRESH_DELAY_SECONDS = 3           # fetch a minute's candle this long after the minute closes
SIGNAL_FRESH_MINUTES = 3            # ignore first breaks older than this (e.g. replayed after a restart)
HEARTBEAT_EVERY = timedelta(minutes=30)
EXIT_FILL_TIMEOUT_SECONDS = 60

# charges used for the P&L / equity bookkeeping (AliceBlue equity intraday)
BROKERAGE_PER_ORDER, BROKERAGE_RATE = 20.0, 0.0005
STT_SELL, EXCHANGE_TXN, SEBI, STAMP_BUY, GST = 0.00025, 0.0000297, 0.000001, 0.00003, 0.18

LOG_DIR = os.path.join(HERE, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
EQUITY_FILE = os.path.join(HERE, 'stock_range_reversal_equity.json')
LOG_FILE = os.path.join(HERE, 'stock_range_reversal.log')

log = get_logger('stock_range_reversal', LOG_FILE, 'SRR' + (' PAPER' if DRY_RUN else ''))


def _log_uncaught(exc_type, exc_value, exc_tb):
    log.critical('Uncaught exception', exc_info=(exc_type, exc_value, exc_tb))


sys.excepthook = _log_uncaught


def _exit_on_signal(signum, frame):
    log.critical(f'Received signal {signal.Signals(signum).name} - exiting')
    sys.exit(1)


for _sig in (signal.SIGTERM, signal.SIGHUP):
    signal.signal(_sig, _exit_on_signal)


def charges(side, entry, exit_, qty):
    buy_v, sell_v = (entry * qty, exit_ * qty) if side == 'BUY' else (exit_ * qty, entry * qty)
    brokerage = min(BROKERAGE_PER_ORDER, BROKERAGE_RATE * buy_v) + min(BROKERAGE_PER_ORDER, BROKERAGE_RATE * sell_v)
    exch, sebi = EXCHANGE_TXN * (buy_v + sell_v), SEBI * (buy_v + sell_v)
    return STT_SELL * sell_v + STAMP_BUY * buy_v + exch + sebi + brokerage + GST * (brokerage + exch + sebi)


def sleep_until(target):
    while True:
        remaining = (target - now_ist()).total_seconds()
        if remaining <= 0:
            return
        time_module.sleep(min(remaining, 30))


def today_at(t, seconds=0):
    return datetime.combine(now_ist().date(), t) + timedelta(seconds=seconds)


# ── equity (compounding) ────────────────────────────────────────────────────────────────────────
def load_equity():
    if not os.path.exists(EQUITY_FILE):
        return {'start_capital': rules.START_CAPITAL, 'equity': float(rules.START_CAPITAL), 'days': []}
    with open(EQUITY_FILE) as f:
        return json.load(f)


def save_equity(eq):
    with open(EQUITY_FILE, 'w') as f:
        json.dump(eq, f, indent=2, default=str)


class Runner:
    def __init__(self):
        self.day = now_ist().date()
        self.state_file = os.path.join(LOG_DIR, f'stock_range_reversal_{self.day:%Y%m%d}.json')
        self.lock = threading.RLock()
        self.signals = queue.Queue()
        self.broker = PaperBroker(log) if DRY_RUN else AliceBlueBroker(log)
        self.instruments = self.broker.instruments(UNIVERSE)
        missing = sorted(set(UNIVERSE) - set(self.instruments))
        if missing:
            log.warning(f'AliceBlue: no NSE EQ contract for {missing} - these stocks are skipped')
        self.data = ZerodhaMinuteData([s for s in UNIVERSE if s in self.instruments], log)
        self.machines = {s: rules.RangeReversal(s) for s in self.data.symbols}
        eq = load_equity()
        self.equity = float(eq['equity'])
        self.trades = self._load_state()          # symbol -> trade dict (one per stock per day)
        self.armed = {}                           # symbol -> EntrySignal with no order at the broker yet
        self.sigs = {}                            # symbol -> its EntrySignal (to re-arm a recalled entry)
        self.cutoff_done = False
        self.cap_done = False
        self.data_done = threading.Event()

    # ── persistence ───────────────────────────────────────────────────────────────────────────
    def _load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file) as f:
                trades = json.load(f)['trades']
            log.info(f'resumed {len(trades)} trades from {self.state_file}')
            return trades
        return {}

    def save_state(self):
        with open(self.state_file, 'w') as f:
            json.dump({'day': str(self.day), 'equity_start': self.equity, 'dry_run': DRY_RUN,
                       'trades': self.trades}, f, indent=2, default=str)

    # ── counts ────────────────────────────────────────────────────────────────────────────────
    def filled_count(self):
        return sum(1 for t in self.trades.values() if t['status'] in ('open', 'closed'))

    def pending(self):
        return [t for t in self.trades.values() if t['status'] == 'pending']

    def margin_in_use(self):
        """Margin blocked by open positions and resting entry orders (exposure / leverage)."""
        used = 0.0
        for t in self.trades.values():
            if t['status'] == 'open':
                used += t['filled_qty'] * t['entry_fill']
            elif t['status'] == 'pending':
                used += t['qty'] * t['entry_trigger']
        return used / rules.LEVERAGE

    # ── data thread ───────────────────────────────────────────────────────────────────────────
    def data_loop(self):
        try:
            sleep_until(today_at(rules.RANGE_END, REFRESH_DELAY_SECONDS))
            bars = self.data.bootstrap()
            for symbol, symbol_bars in bars.items():
                self._feed(symbol, symbol_bars)
            ranged = sum(1 for m in self.machines.values() if m.high is not None)
            alert(log, f'Ranges set for {ranged}/{len(self.machines)} stocks; equity ₹{self.equity:,.0f}'
                       f'{" (PAPER)" if DRY_RUN else ""}')
            next_run = now_ist().replace(second=0, microsecond=0) + timedelta(minutes=1, seconds=REFRESH_DELAY_SECONDS)
            end = today_at(rules.EXIT_TIME, 60)
            while now_ist() < end:
                sleep_until(next_run)
                started = time_module.time()
                new = self.data.refresh()
                for symbol, symbol_bars in new.items():
                    self._feed(symbol, symbol_bars)
                if DRY_RUN:
                    self.broker.simulate(new)
                took = time_module.time() - started
                if took > 50:
                    log.warning(f'candle refresh took {took:.0f}s - signals are running late')
                next_run = now_ist().replace(second=0, microsecond=0) + timedelta(minutes=1, seconds=REFRESH_DELAY_SECONDS)
        except Exception:
            log.critical('data thread crashed - no new signals will arrive', exc_info=True)
        finally:
            try:
                self.data.save(os.path.join(LOG_DIR, f'stock_candles_{self.day:%Y%m%d}.csv'))
            except Exception as exc:
                log.warning(f'could not save candles: {exc}')
            self.data_done.set()

    def _feed(self, symbol, bars):
        machine = self.machines[symbol]
        for bar in bars:
            sig = machine.on_bar(bar)
            if sig is None:
                continue
            age = now_ist() - bar.ts
            if age > timedelta(minutes=SIGNAL_FRESH_MINUTES):
                log.info(f'{symbol}: first break at {bar.ts:%H:%M} is {age} old - not traded')
                continue
            self.signals.put(sig)

    # ── order handling (main thread) ──────────────────────────────────────────────────────────
    @staticmethod
    def distance_pct(side, ltp, trigger):
        """How far (in %) price still has to go to reach the trigger; <= 0 means already through."""
        return 100 * ((trigger - ltp) if side == 'BUY' else (ltp - trigger)) / ltp

    def manage_armed(self):
        """Place entries for armed stocks whose price is near the trigger (nearest first) and recall
        resting entries whose price has drifted away."""
        if now_ist().time() >= rules.LAST_ENTRY:
            return
        pending_sl = [t for t in self.pending() if t['entry_type'] == 'SL' and not t.get('recall')]
        watch = list(self.armed) + [t['symbol'] for t in pending_sl]
        if not watch:
            return
        try:
            ltps = self.data.ltp(watch)
        except Exception as exc:
            log.info(f'LTP check failed ({exc}) - armed stocks re-checked next poll')
            return
        for t in pending_sl:
            ltp = ltps.get(t['symbol'])
            if ltp is not None and self.distance_pct(t['side'], ltp, t['entry_trigger']) > RECALL_BEYOND_PCT:
                try:
                    self.broker.cancel(t['entry_order'])
                    t['recall'] = True
                    log.info(f"{t['symbol']}: LTP {ltp} drifted away from {t['entry_trigger']} - entry recalled")
                except Exception as exc:
                    log.info(f"{t['symbol']}: recall cancel failed ({exc})")
        near = []
        for symbol, sig in self.armed.items():
            ltp = ltps.get(symbol)
            if ltp is None:
                continue
            trigger, _ = rules.entry_prices(sig.side, sig.level, self.instruments[symbol].tick, ENTRY_LIMIT_BUFFER_PCT)
            dist = self.distance_pct(sig.side, ltp, trigger)
            if dist <= PLACE_WITHIN_PCT:
                near.append((dist, sig, ltp))
        for _, sig, ltp in sorted(near, key=lambda x: x[0]):
            if self.place_entry(sig, ltp):
                self.armed.pop(sig.symbol, None)

    def place_entry(self, sig, ltp):
        """Rest the entry for `sig`. True when the signal is dealt with (placed / missed / rejected /
        skipped), False when it must stay armed (no free margin right now)."""
        if sig.symbol in self.trades:
            return True
        if now_ist().time() >= rules.LAST_ENTRY or self.filled_count() >= rules.MAX_TRADES_PER_DAY:
            return True
        inst = self.instruments[sig.symbol]
        trigger, limit = rules.entry_prices(sig.side, sig.level, inst.tick, ENTRY_LIMIT_BUFFER_PCT)
        qty = rules.trade_qty(self.equity, self.filled_count(), trigger)
        if qty < 1:
            log.info(f'{sig.symbol}: price {trigger} above the per-trade allocation - skipped')
            return True
        if self.margin_in_use() + qty * trigger / rules.LEVERAGE > self.equity + 1:
            log.info(f'{sig.symbol}: near its trigger but no free margin - stays armed')
            return False
        order_type, price, trig = 'SL', limit, trigger
        if ltp is not None and ((ltp >= trigger) if sig.side == 'BUY' else (ltp <= trigger)):
            # already through the level: an SL order would be rejected; a limit at the SL-limit's
            # own limit price gives the same fill, or nothing if price has run past it
            if (ltp > limit) if sig.side == 'BUY' else (ltp < limit):
                log.info(f'{sig.symbol}: LTP {ltp} already beyond limit {limit} - missed')
                self.trades[sig.symbol] = self._trade(sig, inst, qty, trigger, limit, None, 'missed')
                self.save_state()
                return True
            order_type, trig = 'LIMIT', None
        tag = f'srrE{len(self.trades):02d}'
        try:
            order_id = self.broker.place(inst, sig.side, qty, order_type, price=price, trigger=trig, tag=tag, ref_price=ltp)
        except Exception as exc:
            log.warning(f'{sig.symbol}: entry order failed: {exc}')
            self.trades[sig.symbol] = self._trade(sig, inst, qty, trigger, limit, None, 'rejected')
            self.save_state()
            return True
        self.trades[sig.symbol] = self._trade(sig, inst, qty, trigger, limit, order_id, 'pending', order_type)
        self.save_state()
        log.info(f'{sig.symbol}: first break {sig.first_break} at {sig.first_break_ts:%H:%M} '
                 f'(range {sig.range_low}-{sig.range_high}) -> {sig.side} {qty} {order_type} '
                 f'trigger {trig} limit {price} [{order_id}]')
        return True

    @staticmethod
    def _trade(sig, inst, qty, trigger, limit, order_id, status, order_type='SL'):
        return dict(symbol=sig.symbol, side=sig.side, level=sig.level, range_high=sig.range_high,
                    range_low=sig.range_low, first_break=sig.first_break, signal_ts=str(sig.first_break_ts),
                    qty=qty, entry_type=order_type, entry_trigger=trigger, entry_limit=limit,
                    entry_order=order_id, status=status, filled_qty=0, entry_fill=None, entry_ts=None,
                    stop_order=None, stop_trigger=None, exit_order=None, exit_fill=None, exit_reason=None,
                    exit_ts=None, pnl=None)

    def place_stop(self, t):
        inst = self.instruments[t['symbol']]
        trigger, limit = rules.stop_prices(t['side'], t['entry_fill'], inst.tick, STOP_LIMIT_BUFFER_PCT)
        exit_side = 'SELL' if t['side'] == 'BUY' else 'BUY'
        try:
            t['stop_order'] = self.broker.place(inst, exit_side, t['filled_qty'], 'SL', price=limit,
                                                trigger=trigger, tag=f'srrS{len(self.trades):02d}')
            t['stop_trigger'] = trigger
            log.info(f"{t['symbol']}: stop {exit_side} {t['filled_qty']} trigger {trigger} limit {limit} [{t['stop_order']}]")
        except Exception as exc:
            alert(log, f"{t['symbol']}: STOP ORDER FAILED ({exc}) - exiting at market", logging.ERROR)
            self.exit_market(t, 'stop_order_failed')

    def exit_market(self, t, reason):
        inst = self.instruments[t['symbol']]
        exit_side = 'SELL' if t['side'] == 'BUY' else 'BUY'
        try:
            ltp = self.data.ltp([t['symbol']]).get(t['symbol'])
        except Exception:
            ltp = None
        try:
            t['exit_order'] = self.broker.place(inst, exit_side, t['filled_qty'], 'MARKET', tag='srrX', ref_price=ltp)
        except Exception as exc:
            if ltp is None:
                alert(log, f"{t['symbol']}: EXIT FAILED ({exc}) and no LTP - CLOSE MANUALLY", logging.CRITICAL)
                return
            mult = 1 + EXIT_LIMIT_BUFFER_PCT / 100 if exit_side == 'BUY' else 1 - EXIT_LIMIT_BUFFER_PCT / 100
            price = rules.round_tick(ltp * mult, inst.tick, 'up' if exit_side == 'BUY' else 'down')
            log.warning(f"{t['symbol']}: MARKET exit rejected ({exc}) - LIMIT at {price}")
            t['exit_order'] = self.broker.place(inst, exit_side, t['filled_qty'], 'LIMIT', price=price, tag='srrX', ref_price=ltp)
        t['exit_reason'] = reason
        self.save_state()

    def sync(self):
        """Read the order book and move every trade forward."""
        book = self.broker.orders()
        changed = False
        recalled = []
        for t in self.trades.values():
            if t['status'] == 'pending':
                o = book.get(t['entry_order'])
                if o is None:
                    continue
                if o['status'] == 'complete' or (o['status'] == 'cancelled' and o['filled_qty'] > 0):
                    t.update(status='open', filled_qty=o['filled_qty'] or t['qty'], entry_fill=o['avg_price'],
                             entry_ts=str(now_ist()))
                    alert(log, f"ENTRY {t['symbol']} {t['side']} {t['filled_qty']} @ {t['entry_fill']} "
                               f"(trade {self.filled_count()}/{rules.MAX_TRADES_PER_DAY})")
                    self.place_stop(t)
                    changed = True
                elif o['status'] == 'cancelled' and t.get('recall'):
                    recalled.append(t['symbol'])            # our own drift-cancel: re-arm it
                elif o['status'] in ('rejected', 'cancelled'):
                    t['status'] = o['status']
                    if o['status'] == 'rejected':
                        log.warning(f"{t['symbol']}: entry rejected: {o['reason']}")
                    changed = True
            elif t['status'] == 'open':
                for key, reason in (('exit_order', None), ('stop_order', 'stop')):
                    o = book.get(t[key]) if t[key] else None
                    if o is None:
                        continue
                    if o['status'] == 'complete':
                        self._close(t, o['avg_price'], t['exit_reason'] or reason)
                        changed = True
                        break
                    if key == 'stop_order' and o['status'] == 'rejected' and not t['exit_order']:
                        alert(log, f"{t['symbol']}: stop rejected ({o['reason']}) - exiting at market", logging.ERROR)
                        self.exit_market(t, 'stop_rejected')
                        changed = True
        for symbol in recalled:
            del self.trades[symbol]
            if now_ist().time() < rules.LAST_ENTRY and self.filled_count() < rules.MAX_TRADES_PER_DAY:
                self.armed[symbol] = self.sigs[symbol]
            changed = True
        if changed:
            self.save_state()

    def _close(self, t, price, reason):
        sign = 1 if t['side'] == 'BUY' else -1
        gross = sign * (price - t['entry_fill']) * t['filled_qty']
        t.update(status='closed', exit_fill=price, exit_reason=reason, exit_ts=str(now_ist()),
                 pnl=round(gross - charges(t['side'], t['entry_fill'], price, t['filled_qty']), 2))
        alert(log, f"EXIT {t['symbol']} ({reason}) @ {price}  P&L ₹{t['pnl']:,.0f}")

    def cancel_pending(self, why):
        for t in self.pending():
            try:
                self.broker.cancel(t['entry_order'])
                log.info(f"{t['symbol']}: entry cancelled ({why})")
            except Exception as exc:
                log.info(f"{t['symbol']}: cancel failed ({exc}) - it may have filled; next sync decides")

    def resize_pending(self):
        """Pending entries take the size of the next trade slot (first 4 boosted)."""
        rank = self.filled_count()
        for t in self.pending():
            if t['entry_type'] != 'SL':
                continue
            want = rules.trade_qty(self.equity, rank, t['entry_trigger'])
            if want >= 1 and want != t['qty']:
                try:
                    self.broker.modify(t['entry_order'], self.instruments[t['symbol']], want, 'SL',
                                       t['entry_limit'], t['entry_trigger'])
                    log.info(f"{t['symbol']}: entry re-sized {t['qty']} -> {want}")
                    t['qty'] = want
                except Exception as exc:
                    log.info(f"{t['symbol']}: re-size failed ({exc})")

    def square_off(self):
        self.cancel_pending('exit time')
        for t in self.trades.values():
            if t['status'] == 'open' and not t['exit_order']:
                if t['stop_order']:
                    try:
                        self.broker.cancel(t['stop_order'])
                    except Exception as exc:
                        log.info(f"{t['symbol']}: stop cancel failed ({exc})")
                self.sync()                       # the stop may have filled just now
                if t['status'] == 'open':
                    self.exit_market(t, 'day_end')
        deadline = time_module.time() + EXIT_FILL_TIMEOUT_SECONDS
        while time_module.time() < deadline and any(t['status'] == 'open' for t in self.trades.values()):
            time_module.sleep(2)
            self.sync()
        still_open = [t['symbol'] for t in self.trades.values() if t['status'] == 'open']
        if still_open:
            alert(log, f'STILL OPEN after square-off: {still_open} - CHECK THE BROKER', logging.CRITICAL)

    # ── main loop ─────────────────────────────────────────────────────────────────────────────
    def run(self):
        alert(log, f"Stock range reversal starting {'(PAPER)' if DRY_RUN else '(LIVE)'}: "
                   f"{len(self.machines)} stocks, equity ₹{self.equity:,.0f}")
        threading.Thread(target=self.data_loop, name='data', daemon=True).start()
        last_heartbeat = now_ist()
        while now_ist() < today_at(rules.EXIT_TIME):
            with self.lock:
                try:
                    self.sync()
                    while not self.signals.empty():
                        sig = self.signals.get_nowait()
                        if sig.symbol not in self.trades:
                            self.sigs[sig.symbol] = sig
                            self.armed[sig.symbol] = sig
                            log.info(f'{sig.symbol}: first break {sig.first_break} at {sig.first_break_ts:%H:%M} '
                                     f'-> armed {sig.side} at {sig.level}')
                    self.manage_armed()
                    self.resize_pending()
                    if not self.cap_done and self.filled_count() >= rules.MAX_TRADES_PER_DAY:
                        self.cap_done = True
                        self.cancel_pending('daily cap reached')
                        self.armed.clear()
                    if not self.cutoff_done and now_ist().time() >= rules.LAST_ENTRY:
                        self.cutoff_done = True
                        self.cancel_pending('14:30 entry cut-off')
                        if self.armed:
                            log.info(f'14:30: {len(self.armed)} armed stocks never reached their trigger: {sorted(self.armed)}')
                            self.armed.clear()
                    if self.filled_count() > rules.MAX_TRADES_PER_DAY:
                        log.warning(f'{self.filled_count()} entries filled - over the daily cap')
                except Exception:
                    log.error('main loop error', exc_info=True)
            if now_ist() - last_heartbeat >= HEARTBEAT_EVERY:
                last_heartbeat = now_ist()
                opn = [t['symbol'] for t in self.trades.values() if t['status'] == 'open']
                alert(log, f'heartbeat: {self.filled_count()} filled, open {opn}, {len(self.pending())} resting, '
                           f'{len(self.armed)} armed, margin ₹{self.margin_in_use():,.0f}/₹{self.equity:,.0f}')
            time_module.sleep(POLL_SECONDS)

        with self.lock:
            self.square_off()
        self.data_done.wait(timeout=120)
        self.finish()

    def finish(self):
        closed = [t for t in self.trades.values() if t['status'] == 'closed']
        day_pnl = round(sum(t['pnl'] for t in closed), 2)
        eq = load_equity()
        if DRY_RUN:
            log.info('DRY_RUN - equity file not updated')
        elif not any(d.get('date') == str(self.day) for d in eq['days']):
            eq['equity'] = round(eq['equity'] + day_pnl, 2)
            eq['days'].append({'date': str(self.day), 'trades': len(closed), 'pnl': day_pnl, 'equity': eq['equity']})
            save_equity(eq)
        wins = sum(1 for t in closed if t['pnl'] > 0)
        lines = [f"{t['symbol']} {t['side']} {t['filled_qty']} {t['entry_fill']}->{t['exit_fill']} "
                 f"({t['exit_reason']}) ₹{t['pnl']:,.0f}" for t in closed]
        alert(log, f"Day done{' (PAPER)' if DRY_RUN else ''}: {len(closed)} trades, {wins} winners, "
                   f"P&L ₹{day_pnl:,.0f}, equity ₹{self.equity + day_pnl:,.0f}\n" + '\n'.join(lines))
        self.save_state()


if __name__ == '__main__':
    now = now_ist()
    if now.weekday() >= 5:
        log.info('weekend - nothing to do')
        sys.exit(0)
    if now.time() >= rules.EXIT_TIME:
        log.info('after exit time - nothing to do')
        sys.exit(0)
    Runner().run()
