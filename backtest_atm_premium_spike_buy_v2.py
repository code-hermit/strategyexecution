"""
v2 of backtest_atm_premium_spike_buy.py - same rules, but fixes what strike the signal is measured
against.

In v1, the "combined ATM premium" compared every minute against the checkpoint baseline was
whichever strike happened to be tagged ATM *at that minute* - if spot drifted enough between
checkpoints to roll the ATM strike, the comparison silently switched to a different straddle's
premium mid-hour, conflating strike-selection effects with an actual premium spike.

v2 instead pins the exact strike (CE+PE symbols) that was ATM at each checkpoint, and tracks that
same strike's premium for the rest of the hour, even after spot moves on and a different strike
becomes the new ATM. The signal - and the straddle actually bought - are always about that one
pinned checkpoint strike, not "whatever's ATM right now".

Checkpoints: 10:15, 11:15, 12:15, 13:15, 14:15 - at each of these, the strike currently tagged ATM
is pinned, and its combined premium (CE close + PE close) recorded as the baseline, replacing
whatever strike/baseline was pinned at the previous checkpoint.

Strategy start time: 10:15. Day end time: 15:15.

Checked every minute (not just at the checkpoints): if the pinned strike's combined premium has
risen SPIKE_POINTS points above the checkpoint baseline, buy that same strike's straddle (CE+PE)
right then and exit HOLD_MINUTES minutes later (or at day end, whichever comes first). Only one
position is held at a time - a new signal is ignored while a position from an earlier spike is
still open. Each checkpoint's baseline can trigger at most one trade: once a trade has fired off
a given checkpoint's pinned strike, no further trade is taken against that same strike/value, even
after the position closes - the signal only re-arms when the next checkpoint pins a fresh strike.

Trades every weekday by default; restrict to specific weekdays with a second CLI arg, e.g.
`python backtest_atm_premium_spike_buy_v2.py SENSEX th` for Tuesday+Thursday only (codes: m/t/w/h/f).
"""

import os
from datetime import date, time, timedelta
from datetime import datetime as dttime

import duckdb
import matplotlib.pyplot as plt
import pandas as pd

from Data import config

START_DATE = date(2026, 1, 1)
END_DATE = dttime.now().date()
ENTRY_TIME = time(10, 15)  # strategy start
EXIT_TIME = time(15, 15)  # day end / forced square-off
CHECK_TIMES = (time(10, 15), time(11, 15), time(12, 15), time(13, 15), time(14, 15))
SPIKE_POINTS = 50  # pinned strike's combined premium rise above its checkpoint baseline that triggers a buy
HOLD_MINUTES = 15  # how long the buy is held before being exited
UNDERLYINGS = ('NIFTY', 'SENSEX')
UNDERLYING = 'NIFTY'  # default/single-run underlying, matching the other scripts here
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


TRADE_WEEKDAYS = _parse_trade_weekdays(None)  # overridden from the command line - see __main__ below

con = duckdb.connect()

QUERY = """
    select timestamp, symbol, option_type, atm_level, close
    from read_parquet(?)
    where option_type in ('CE', 'PE', 'CALL', 'PUT')
    and cast(timestamp as time) between cast(? as time) and cast(? as time)
"""
# some datewise files tag option_type as CE/PE, others as CALL/PUT - normalize to CE/PE
OPTION_TYPE_ALIASES = {'CALL': 'CE', 'PUT': 'PE'}


def _options_path(underlying):
    return config.options_path if underlying == 'NIFTY' else f'Data/expired_options_data/{underlying}/date_wise/datewise'


def _file_path(d, underlying=UNDERLYING):
    return os.path.join(
        _options_path(underlying), f'year={d.year}', f'month={d.month:02d}', f'day={d.day:02d}',
        f'{d.strftime("%Y%m%d")}.parquet',
    )


def _load_day(d, underlying=UNDERLYING):
    path = _file_path(d, underlying)
    if not os.path.exists(path):
        return None
    df = con.execute(QUERY, [path, ENTRY_TIME.isoformat(), EXIT_TIME.isoformat()]).df()
    if df.empty:
        return None
    df['option_type'] = df['option_type'].replace(OPTION_TYPE_ALIASES)
    return df


def _atm_lookup(df):
    """(timestamp, option_type) -> (symbol, close), ATM-tagged leg only - used to pin the
    checkpoint strike (which strike is ATM *right now*)."""
    lookup = {}
    atm = df[df['atm_level'] == 'ATM']
    for ts, sym, opt, close in atm[['timestamp', 'symbol', 'option_type', 'close']].itertuples(index=False):
        lookup[(ts, opt)] = (sym, close)
    return lookup


def _price_lookup(df):
    """(timestamp, symbol) -> close, across every strike - used to keep tracking/trading a pinned
    strike's premium even after it stops being tagged ATM (spot has moved on)."""
    return {(ts, sym): close for ts, sym, close in df[['timestamp', 'symbol', 'close']].itertuples(index=False)}


def run_day(d, underlying=UNDERLYING):
    if d.strftime('%A') not in TRADE_WEEKDAYS:
        return []

    df = _load_day(d, underlying)
    if df is None:
        print(f'{d}: missing data, skipped')
        return []

    timestamps = sorted(set(df['timestamp']))
    if not timestamps:
        print(f'{d}: no timestamps in trading window, skipped')
        return []

    atm = _atm_lookup(df)
    price = _price_lookup(df)

    trades = []
    checkpoint = None  # {'ce_symbol', 'pe_symbol', 'premium'} - the exact strike pinned at the last checkpoint
    checkpoint_used = False  # a trade has already been taken off the current checkpoint's pinned strike -
    # don't take another until the next checkpoint pins a fresh one
    position = None  # {'entry_time','checkpoint_premium','entry_premium','deadline','legs':{'CE':{'symbol','entry'},'PE':{...}}}

    def _pinned_premium(ts):
        """Live combined premium of the pinned checkpoint strike at ts (the SAME symbols pinned
        at the checkpoint, tracked via price/all-strikes lookup, not the atm_level tag which may
        have moved off this strike by now) - None if either leg's quote is missing at ts."""
        if checkpoint is None:
            return None
        ce = price.get((ts, checkpoint['ce_symbol']))
        pe = price.get((ts, checkpoint['pe_symbol']))
        if ce is None or pe is None:
            return None
        return ce + pe

    def _close_position(ts, reason):
        legs = position['legs']
        ce_exit = price.get((ts, legs['CE']['symbol']))
        pe_exit = price.get((ts, legs['PE']['symbol']))
        if ce_exit is None or pe_exit is None:
            return False  # try again next minute
        pnl = (ce_exit - legs['CE']['entry']) + (pe_exit - legs['PE']['entry'])
        trades.append({
            'date': d, 'entry_time': position['entry_time'], 'exit_time': ts, 'reason': reason,
            'checkpoint_premium': position['checkpoint_premium'], 'entry_premium': position['entry_premium'],
            'call_symbol': legs['CE']['symbol'], 'call_entry': legs['CE']['entry'], 'call_exit': ce_exit,
            'put_symbol': legs['PE']['symbol'], 'put_entry': legs['PE']['entry'], 'put_exit': pe_exit,
            'pnl': pnl,
        })
        return True

    for ts in timestamps:
        t = ts.time()

        # checkpoints (re)pin whichever strike is ATM right now, regardless of whether a position
        # is currently open, and re-arm the signal - this is a fresh strike/baseline nothing has
        # traded off yet, even if it happens to be the same strike as before.
        if t in CHECK_TIMES:
            ce = atm.get((ts, 'CE'))
            pe = atm.get((ts, 'PE'))
            if ce is not None and pe is not None:
                checkpoint = {'ce_symbol': ce[0], 'pe_symbol': pe[0], 'premium': ce[1] + pe[1]}
                checkpoint_used = False

        if position is not None:
            day_end = t >= EXIT_TIME
            if ts >= position['deadline'] or day_end:
                if _close_position(ts, 'EOD' if day_end else 'TIME_EXIT'):
                    position = None
            continue  # only one position at a time - no new signal while this one is open

        if t >= EXIT_TIME or checkpoint is None or checkpoint_used:
            continue

        current_premium = _pinned_premium(ts)
        if current_premium is None:
            continue

        if current_premium - checkpoint['premium'] >= SPIKE_POINTS:
            ce_entry = price.get((ts, checkpoint['ce_symbol']))
            pe_entry = price.get((ts, checkpoint['pe_symbol']))
            if ce_entry is not None and pe_entry is not None:
                position = {
                    'entry_time': ts, 'checkpoint_premium': checkpoint['premium'], 'entry_premium': current_premium,
                    'deadline': ts + timedelta(minutes=HOLD_MINUTES),
                    'legs': {
                        'CE': {'symbol': checkpoint['ce_symbol'], 'entry': ce_entry},
                        'PE': {'symbol': checkpoint['pe_symbol'], 'entry': pe_entry},
                    },
                }
                checkpoint_used = True  # this checkpoint's pinned strike has now produced a trade - no more off it

    # position still open when the trading window ran out (e.g. data stops before its deadline) -
    # force-close at the last quote seen for it
    if position is not None:
        for ts in reversed(timestamps):
            if _close_position(ts, 'EOD'):
                break
        else:
            print(f'{d}: open position at EOD but no exit quote ever available, no trade recorded')

    if not trades:
        print(f'{d}: no signal, no trade')
    return trades


def _out_path(name, underlying):
    if underlying != UNDERLYING:
        base, ext = os.path.splitext(name)
        name = f'{base}_{underlying}{ext}'
    return os.path.join(os.path.dirname(__file__), name)


COLOR_GOOD = '#0ca30c'
COLOR_CRITICAL = '#d03b3b'
COLOR_NEUTRAL = '#2a78d6'
COLOR_MUTED_INK = '#898781'
COLOR_SURFACE = '#fcfcfb'


def plot_daily_summary(trades_df, save_path, underlying=UNDERLYING):
    """Daily pnl (signed, good/critical) with trade count on a twin axis - this strategy can fire
    (and re-arm) several times a day."""
    daily = trades_df.groupby('date')['pnl'].agg(['sum', 'count'])
    daily.index = pd.to_datetime(daily.index)

    fig, ax = plt.subplots(figsize=(10, 4.5), facecolor=COLOR_SURFACE)
    colors = [COLOR_GOOD if v >= 0 else COLOR_CRITICAL for v in daily['sum']]
    ax.bar(daily.index, daily['sum'], color=colors, width=0.7)
    ax.axhline(0, color=COLOR_MUTED_INK, linewidth=0.8)
    ax.set_ylabel('PnL (points)')
    ax.set_title(f'{underlying}: Daily PnL - ATM premium spike straddle buy (v2, pinned strike)')
    ax.set_facecolor(COLOR_SURFACE)
    for spine in ('top', 'right'):
        ax.spines[spine].set_visible(False)
    ax.tick_params(colors=COLOR_MUTED_INK)

    ax2 = ax.twinx()
    ax2.plot(daily.index, daily['count'], color=COLOR_NEUTRAL, marker='o', markersize=3, linewidth=1, alpha=0.7)
    ax2.set_ylabel('Trades', color=COLOR_NEUTRAL)
    ax2.tick_params(axis='y', colors=COLOR_NEUTRAL)
    for spine in ('top',):
        ax2.spines[spine].set_visible(False)
    fig.autofmt_xdate()

    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.show()
    print(f'daily summary plot written to {save_path}')


def plot_equity_and_drawdown(trades_df, save_path, underlying=UNDERLYING):
    """Two small multiples sharing a date axis: cumulative pnl (equity curve) on top, drawdown
    from the running peak below."""
    daily_pnl = trades_df.groupby('date')['pnl'].sum().sort_index()
    daily_pnl.index = pd.to_datetime(daily_pnl.index)

    equity = daily_pnl.cumsum()
    running_peak = equity.cummax()
    drawdown = equity - running_peak

    fig, (ax_equity, ax_dd) = plt.subplots(
        2, 1, figsize=(10, 6), sharex=True, facecolor=COLOR_SURFACE,
        gridspec_kw={'height_ratios': [2, 1]},
    )

    ax_equity.plot(equity.index, equity.values, color=COLOR_NEUTRAL, linewidth=2)
    ax_equity.axhline(0, color=COLOR_MUTED_INK, linewidth=0.8)
    ax_equity.set_ylabel('Cumulative PnL (points)')
    ax_equity.set_title(f'{underlying}: Equity curve and drawdown - ATM premium spike straddle buy (v2, pinned strike)')
    ax_equity.set_facecolor(COLOR_SURFACE)
    for spine in ('top', 'right'):
        ax_equity.spines[spine].set_visible(False)
    ax_equity.tick_params(colors=COLOR_MUTED_INK)

    ax_dd.fill_between(drawdown.index, drawdown.values, 0, color=COLOR_CRITICAL, alpha=0.85, linewidth=0)
    ax_dd.set_ylabel('Drawdown (points)')
    ax_dd.set_facecolor(COLOR_SURFACE)
    for spine in ('top', 'right'):
        ax_dd.spines[spine].set_visible(False)
    ax_dd.tick_params(colors=COLOR_MUTED_INK)
    fig.autofmt_xdate()

    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.show()
    print(f'equity/drawdown plot written to {save_path}')


def plot_monthly_pnl(trades_df, save_path, underlying=UNDERLYING):
    """Histogram of total pnl points booked per calendar month."""
    monthly = trades_df.copy()
    monthly['date'] = pd.to_datetime(monthly['date'])
    monthly_pnl = monthly.groupby(monthly['date'].dt.to_period('M'))['pnl'].sum()
    labels = monthly_pnl.index.strftime('%Y-%m')

    fig, ax = plt.subplots(figsize=(10, 5), facecolor=COLOR_SURFACE)
    colors = [COLOR_GOOD if v >= 0 else COLOR_CRITICAL for v in monthly_pnl]
    ax.bar(labels, monthly_pnl.values, color=colors, width=0.6)
    ax.axhline(0, color=COLOR_MUTED_INK, linewidth=0.8)
    ax.set_ylabel('PnL (points)')
    ax.set_title(f'{underlying}: Monthly PnL - ATM premium spike straddle buy (v2, pinned strike)')
    ax.set_facecolor(COLOR_SURFACE)
    for spine in ('top', 'right'):
        ax.spines[spine].set_visible(False)
    ax.tick_params(colors=COLOR_MUTED_INK)
    fig.autofmt_xdate()

    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.show()
    print(f'monthly pnl plot written to {save_path}')


def run_backtest(start_date=START_DATE, end_date=END_DATE, trades_csv_path=None, underlying=UNDERLYING):
    if trades_csv_path is None:
        trades_csv_path = _out_path('atm_premium_spike_buy_v2_trades.csv', underlying)

    all_trades = []
    d = start_date
    while d <= end_date:
        all_trades.extend(run_day(d, underlying))
        d += timedelta(days=1)

    trades_df = pd.DataFrame(all_trades)
    if trades_df.empty:
        print('No trades generated.')
        return trades_df

    trades_df['entry_time'] = pd.to_datetime(trades_df['entry_time'])

    daily_pnl = trades_df.groupby('date')['pnl'].sum()
    print(daily_pnl)
    print(f"total trades: {len(trades_df)}")
    print(f"total pnl (points): {trades_df['pnl'].sum():.2f}")
    win_rate = (trades_df['pnl'] > 0).mean() * 100
    print(f"win rate: {win_rate:.1f}%")

    reason_pnl = trades_df.groupby('reason')['pnl'].agg(['sum', 'count'])
    print(reason_pnl)

    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    weekday_pnl = trades_df.groupby(trades_df['entry_time'].dt.day_name())['pnl'].agg(['sum', 'count'])
    weekday_pnl = weekday_pnl.reindex([w for w in weekday_order if w in weekday_pnl.index])
    weekday_pnl.index.name = 'entry_weekday'
    print(weekday_pnl)

    if trades_csv_path:
        trades_df.to_csv(trades_csv_path, index=False)
        print(f"trades written to {trades_csv_path}")

    plot_daily_summary(trades_df, save_path=_out_path('atm_premium_spike_buy_v2_daily_summary.png', underlying), underlying=underlying)
    plot_equity_and_drawdown(trades_df, save_path=_out_path('atm_premium_spike_buy_v2_equity_drawdown.png', underlying), underlying=underlying)
    plot_monthly_pnl(trades_df, save_path=_out_path('atm_premium_spike_buy_v2_monthly_pnl.png', underlying), underlying=underlying)

    return trades_df


if __name__ == '__main__':
    import sys
    TRADE_WEEKDAYS = _parse_trade_weekdays(sys.argv[2] if len(sys.argv) > 2 else None)
    if len(sys.argv) > 1:
        run_backtest(underlying=sys.argv[1].upper())
    else:
        for u in UNDERLYINGS:
            print(f'=== {u} ===')
            run_backtest(underlying=u)
