"""
STRATEGY RULES for the opening-range failed-breakout reversal - pure logic, no I/O, no broker,
no data vendor. Mirrors stock trading/backtest_range_reversal.py (1-hour range, entries until
14:30, fixed 0.5% stop) so the live program and the backtest agree bar for bar.

Per stock, per day:
  1. Range = high / low of the 09:15-10:14 candles (H, L).
  2. From 10:15, tracking starts at the first candle that CLOSES inside the middle band
     L + 0.4*(H-L) .. L + 0.6*(H-L). Breaks before that are ignored.
  3. The first candle after that to trade above H (high > H) or below L (low < L) is the first
     break. If one candle breaks both before the first break is known, the stock is skipped today.
  4. First break at the high -> SHORT at L; first break at the low -> BUY at H. The signal is
     emitted the moment the first break is seen, so the caller can rest an SL-limit entry order
     at the opposite level straight away.
  5. Entries only before LAST_ENTRY (14:30); one trade per stock per day.

Also here: the stop/limit price helpers and the position-sizing rule, so every number the strategy
depends on lives in one file.
"""

import math
from datetime import time as dtime
from typing import NamedTuple

RANGE_START = dtime(9, 15)
RANGE_END = dtime(10, 15)           # range = candles before this
MID_LO, MID_HI = 0.4, 0.6
LAST_ENTRY = dtime(14, 30)          # no entries at/after this; pending entry orders are cancelled
EXIT_TIME = dtime(15, 15)           # square off whatever is open
STOP_PCT = 0.5                      # fixed stop, % from the entry fill
MAX_TRADES_PER_DAY = 10
MIN_RANGE_BARS = 48                 # 80% of the 60 range minutes, else the range is unreliable

# position sizing (compounding): base margin per trade x leverage; profit above the starting
# capital is spread over the day's first BOOSTED_TRADES trades.
START_CAPITAL = 100_000
BASE_MARGIN = 10_000
LEVERAGE = 5
BOOSTED_TRADES = 4


class EntrySignal(NamedTuple):
    symbol: str
    side: str                       # 'BUY' (enter long at H) or 'SELL' (enter short at L)
    level: float                    # H for BUY, L for SELL
    range_high: float
    range_low: float
    first_break: str                # 'high' / 'low'
    first_break_ts: object          # datetime of the first-break candle


class RangeReversal:
    """One stock's state machine. Feed it completed 1-minute bars in time order via on_bar()."""

    def __init__(self, symbol):
        self.symbol = symbol
        self.high = self.low = None
        self.status = 'waiting_range'     # waiting_range -> tracking -> armed -> signalled | skipped
        self.reason = ''
        self._range_bars = 0

    def _set_range(self, bar):
        self.high = bar.high if self.high is None else max(self.high, bar.high)
        self.low = bar.low if self.low is None else min(self.low, bar.low)
        self._range_bars += 1

    def on_bar(self, bar):
        """Process one completed candle. Returns an EntrySignal when the first break happens."""
        t = bar.ts.time()
        if self.status in ('signalled', 'skipped') or t < RANGE_START:
            return None
        if t < RANGE_END:
            self._set_range(bar)
            return None
        if self.status == 'waiting_range':
            if self._range_bars < MIN_RANGE_BARS or self.high is None or self.high <= self.low:
                self.status, self.reason = 'skipped', f'incomplete range ({self._range_bars} bars)'
                return None
            self.status = 'tracking'
        if t >= LAST_ENTRY:
            self.status, self.reason = 'skipped', 'no first break before 14:30'
            return None

        H, L = self.high, self.low
        if self.status == 'tracking':
            if L + MID_LO * (H - L) <= bar.close <= L + MID_HI * (H - L):
                self.status = 'armed'
            return None

        up, dn = bar.high > H, bar.low < L                     # status == 'armed'
        if up and dn:
            self.status, self.reason = 'skipped', 'one candle broke both sides'
            return None
        if up or dn:
            self.status = 'signalled'
            side, level = ('SELL', L) if up else ('BUY', H)
            return EntrySignal(self.symbol, side, level, H, L, 'high' if up else 'low', bar.ts)
        return None


# ── prices ──────────────────────────────────────────────────────────────────────────────────────
def round_tick(price, tick, mode='nearest'):
    steps = price / tick
    steps = math.ceil(steps - 1e-9) if mode == 'up' else math.floor(steps + 1e-9) if mode == 'down' else round(steps)
    return round(steps * tick, 2)


def entry_prices(side, level, tick, limit_buffer_pct):
    """(trigger, limit) for the SL-limit entry: trigger one tick beyond the level (the backtest
    enters when price trades strictly beyond it), limit a small buffer further."""
    if side == 'BUY':
        trigger = round_tick(level + tick, tick, 'up')
        return trigger, round_tick(trigger * (1 + limit_buffer_pct / 100), tick, 'up')
    trigger = round_tick(level - tick, tick, 'down')
    return trigger, round_tick(trigger * (1 - limit_buffer_pct / 100), tick, 'down')


def stop_prices(entry_side, entry_fill, tick, limit_buffer_pct):
    """(trigger, limit) for the protective SL order, STOP_PCT from the fill."""
    if entry_side == 'BUY':                                    # long -> sell stop below
        trigger = round_tick(entry_fill * (1 - STOP_PCT / 100), tick, 'down')
        return trigger, round_tick(trigger * (1 - limit_buffer_pct / 100), tick, 'down')
    trigger = round_tick(entry_fill * (1 + STOP_PCT / 100), tick, 'up')
    return trigger, round_tick(trigger * (1 + limit_buffer_pct / 100), tick, 'up')


# ── sizing ──────────────────────────────────────────────────────────────────────────────────────
def trade_exposure(equity, trade_rank):
    """Rupee value of stock to buy/short for the day's trade number `trade_rank` (0-based), given
    the account equity at the start of the day."""
    if equity < START_CAPITAL:
        margin = BASE_MARGIN * equity / START_CAPITAL
    elif trade_rank < BOOSTED_TRADES:
        margin = BASE_MARGIN + (equity - START_CAPITAL) / BOOSTED_TRADES
    else:
        margin = BASE_MARGIN
    return margin * LEVERAGE


def trade_qty(equity, trade_rank, price):
    return int(trade_exposure(equity, trade_rank) // price)
