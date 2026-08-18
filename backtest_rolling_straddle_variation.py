"""
short first OTM strangle at 10:15 AM; with 25% stoploss on both sides.
 after 1 hour : if these legs are not the first OTM anymore squareoff these legs and short new positions,
 if these are stil the first OTM, but any of the legs hit Stoploss, then re-enter that leg.

 start date :20260101 , end date 20260731

adding one more condition to this strategy, if the ATM premium of the new hour is more than that of the previous hour, dont take trade for that hour
if ATM premium is more than that of the previous hour, and both legs open, then close the legs.

add a new stoploss to this strategy, if ATM premium is greater than the highest premium in the last 2 hours exit the trade for that hour, check this every minute

add an optional premium stoploss of 50%: if ATM premium has increased by 50% from its entry-time
value, exit.

each leg's own stoploss is checked every minute, not just at the hourly checkpoint - it can fire
at any point within the hour, the moment that leg's premium has moved STOPLOSS_PCT against entry.
once it fires, that leg stays flat (no position) for the rest of the hour - it does NOT
re-enter immediately. it only re-enters at the next hourly checkpoint (same as a leg that was
closed for any other reason), per the "re-enter that leg" rule above.
"""

import os
from datetime import date, time, timedelta
from datetime import datetime as dttime
import duckdb
import matplotlib.pyplot as plt
import pandas as pd

START_DATE = date(2026, 1, 1)
END_DATE = dttime.now().date() 
ENTRY_TIME = time(9, 45)
EXIT_TIME = time(15, 15)
CHECKPOINT_INTERVAL = timedelta(hours=1)
STOPLOSS_PCT = 0.3
PREMIUM_HIGH_STOPLOSS_ENABLED = False  # optional - set True to enable the ATM-premium-high stoploss below

PREMIUM_HIGH_LOOKBACK = timedelta(hours=2)  # rolling window for the ATM-premium-high stoploss below
PREMIUM_STOPLOSS_ENABLED = False  # optional - set False to disable the entry-premium stoploss below
PREMIUM_STOPLOSS_PCT = 0.25  # exit once ATM premium has risen this fraction above its entry-time value
FIRST_OTM_STRIKES = 0 # "first OTM" = three strikes away from ATM
DAILY_LOSS_LIMIT = 100  # stop trading for the day once realized+unrealized loss crosses this many points
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
    select timestamp, symbol, option_type, atm_level, strike, close
    from read_parquet(?)
    where option_type in ('CE', 'PE', 'CALL', 'PUT')
    and cast(timestamp as time) between cast(? as time) and cast(? as time)
"""
# some datewise files tag option_type as CE/PE, others as CALL/PUT - normalize to CE/PE
OPTION_TYPE_ALIASES = {'CALL': 'CE', 'PUT': 'PE'}


def _first_otm_level(option_type):
    """CE gets more OTM going UP in strike (ATM+n), PE gets more OTM going DOWN in strike (ATM-n).
    n=0 is just ATM itself, tagged 'ATM' in the data (not 'ATM+0'/'ATM-0')."""
    if FIRST_OTM_STRIKES == 0:
        return 'ATM'
    sign = '+' if option_type == 'CE' else '-'
    return f'ATM{sign}{FIRST_OTM_STRIKES}'


FIRST_OTM_LEVEL = {opt: _first_otm_level(opt) for opt in OPTION_TYPES}


def _options_path(underlying):
    return f'Data/expired_options_data/{underlying}/date_wise/datewise'


def _file_path(d, underlying='NIFTY'):
    return os.path.join(
        _options_path(underlying), f'year={d.year}', f'month={d.month:02d}', f'day={d.day:02d}',
        f'{d.strftime("%Y%m%d")}.parquet',
    )


def _load_day(d, underlying='NIFTY'):
    """Load current_week minute bars for date d, restricted to the trading window."""
    path = _file_path(d, underlying)
    if not os.path.exists(path):
        return None
    df = con.execute(QUERY, [path, ENTRY_TIME.isoformat(), EXIT_TIME.isoformat()]).df()
    if df.empty:
        return None
    df['option_type'] = df['option_type'].replace(OPTION_TYPE_ALIASES)
    return df


def _price_lookup(df):
    return {(ts, sym): close for ts, sym, close in df[['timestamp', 'symbol', 'close']].itertuples(index=False)}


def _atm_lookup(df):
    """(timestamp, atm_level, option_type) -> (symbol, strike, close) - for picking whichever
    strike is currently tagged first-OTM."""
    lookup = {}
    for ts, sym, opt, atm_level, strike, close in df[['timestamp', 'symbol', 'option_type', 'atm_level', 'strike', 'close']].itertuples(index=False):
        lookup[(ts, atm_level, opt)] = (sym, strike, close)
    return lookup


def _symbol_atm_level_lookup(df):
    """(timestamp, symbol) -> atm_level - to check whether a leg we're already holding is still
    tagged first-OTM at a later checkpoint (spot may have moved it to a different level)."""
    return {(ts, sym): atm_level for ts, sym, atm_level in df[['timestamp', 'symbol', 'atm_level']].itertuples(index=False)}


def _atm_premium(atm_lookup, ts):
    """ATM CE close + ATM PE close at ts (the literal ATM straddle premium, not the first-OTM
    legs this strategy actually trades) - None if either leg's ATM quote is missing."""
    atm_ce = atm_lookup.get((ts, 'ATM', 'CE'))
    atm_pe = atm_lookup.get((ts, 'ATM', 'PE'))
    if atm_ce is None or atm_pe is None:
        return None
    return atm_ce[2] + atm_pe[2]


def _open_leg(atm_lookup, ts, option_type):
    picked = atm_lookup.get((ts, FIRST_OTM_LEVEL[option_type], option_type))
    if picked is None:
        return None
    symbol, strike, entry_price = picked
    return {'symbol': symbol, 'strike': strike, 'entry_price': entry_price, 'entry_time': ts}


def _record_leg_trade(trades, d, option_type, leg, exit_ts, exit_price, reason):
    pnl = leg['entry_price'] - exit_price  # short leg
    trades.append({
        'date': d, 'option_type': option_type, 'strike': leg['strike'],
        'entry_time': leg['entry_time'], 'entry_price': leg['entry_price'],
        'exit_time': exit_ts, 'exit_price': exit_price,
        'pnl': pnl, 'reason': reason,
    })
    return pnl


def _unrealized_pnl(state, ts, price):
    """Mark-to-market pnl of whatever legs are still open at ts - checked every minute (not just
    at hourly checkpoints) so a fast intraday move can't blow past DAILY_LOSS_LIMIT unnoticed
    between checkpoints."""
    total = 0.0
    for opt in OPTION_TYPES:
        leg = state[opt]
        if leg is None:
            continue
        current_price = price.get((ts, leg['symbol']))
        if current_price is not None:
            total += leg['entry_price'] - current_price
    return total


def run_day(d, underlying='NIFTY'):
    if d.strftime('%A') not in TRADE_WEEKDAYS:
        return []

    df = _load_day(d, underlying)
    if df is None:
        print(f'{d}: missing current week data, skipped')
        return []

    timestamps = sorted(set(df['timestamp']))
    if not timestamps:
        print(f'{d}: no timestamps in trading window, skipped')
        return []

    price = _price_lookup(df)
    atm = _atm_lookup(df)
    symbol_atm_level = _symbol_atm_level_lookup(df)

    entry_ts = timestamps[0]
    trades = []
    state = {}
    for opt in OPTION_TYPES:
        leg = _open_leg(atm, entry_ts, opt)
        if leg is None:
            print(f'{d} {opt}: no first-OTM quote at entry, skipped')
        state[opt] = leg

    next_checkpoint = entry_ts + CHECKPOINT_INTERVAL
    day_pnl = 0.0
    halted = False
    prev_checkpoint_atm_premium = _atm_premium(atm, entry_ts)  # baseline for the first checkpoint's comparison
    entry_atm_premium = prev_checkpoint_atm_premium  # fixed for the whole day - baseline for PREMIUM_STOPLOSS_PCT below
    premium_history = [(entry_ts, prev_checkpoint_atm_premium)] if prev_checkpoint_atm_premium is not None else []
    suppress_reentry = False  # set by either minute-level premium stoploss below; consumed by
    # the next checkpoint (skips its reopen), so the position stays flat "for that hour"

    for ts in timestamps[1:]:
        if halted:
            break
        is_eod = ts.time() >= EXIT_TIME

        if is_eod:
            for opt in OPTION_TYPES:
                leg = state[opt]
                if leg is None:
                    continue
                exit_price = price.get((ts, leg['symbol']))
                if exit_price is not None:
                    day_pnl += _record_leg_trade(trades, d, opt, leg, ts, exit_price, 'EOD')
                state[opt] = None
            break

        if ts >= next_checkpoint:
            next_checkpoint += CHECKPOINT_INTERVAL

            current_atm_premium = _atm_premium(atm, ts)
            premium_increased = (
                prev_checkpoint_atm_premium is not None and current_atm_premium is not None
                and current_atm_premium > prev_checkpoint_atm_premium
            )
            if current_atm_premium is not None:
                prev_checkpoint_atm_premium = current_atm_premium

            if premium_increased:
                # ATM premium rose vs the previous checkpoint - don't take any new trade this
                # hour (no roll, no stoploss re-entry); if both legs are currently open, close
                # them outright instead.
                if state['CE'] is not None and state['PE'] is not None:
                    for opt in OPTION_TYPES:
                        leg = state[opt]
                        exit_price = price.get((ts, leg['symbol']))
                        if exit_price is not None:
                            day_pnl += _record_leg_trade(trades, d, opt, leg, ts, exit_price, 'ATM_PREMIUM_RISE')
                        state[opt] = None
            elif suppress_reentry:
                # the minute-level premium-high stoploss fired earlier this hour - stay flat for
                # this checkpoint (no reopen) rather than immediately re-entering; consumed here.
                suppress_reentry = False
            else:
                # 1. has either leg drifted off the first-OTM strike? if so, roll the whole strangle.
                drifted = False
                for opt in OPTION_TYPES:
                    leg = state[opt]
                    if leg is None:
                        continue
                    current_level = symbol_atm_level.get((ts, leg['symbol']))
                    if current_level != FIRST_OTM_LEVEL[opt]:
                        drifted = True
                        break

                if drifted:
                    for opt in OPTION_TYPES:
                        leg = state[opt]
                        if leg is not None:
                            exit_price = price.get((ts, leg['symbol']))
                            if exit_price is not None:
                                day_pnl += _record_leg_trade(trades, d, opt, leg, ts, exit_price, 'ROLL_OTM_DRIFT')
                            state[opt] = _open_leg(atm, ts, opt)
                        else:
                            state[opt] = _open_leg(atm, ts, opt)
                else:
                    # 2. still first-OTM - reopen any leg that isn't currently open (e.g. a
                    # stoploss closed it earlier this hour - see the minute-level check below -
                    # or a prior hour's suppression just ended).
                    for opt in OPTION_TYPES:
                        if state[opt] is None:
                            state[opt] = _open_leg(atm, ts, opt)

        # per-leg 30% stoploss - checked every minute, not just at the hourly checkpoint, so a
        # leg is stopped out the instant it moves STOPLOSS_PCT against entry, rather than however
        # far it drifts before the next checkpoint notices. It does NOT re-enter immediately -
        # that leg stays flat for the rest of the hour; the checkpoint block above re-opens it
        # (like any other closed leg) once the next hour starts.
        for opt in OPTION_TYPES:
            leg = state[opt]
            if leg is None:
                continue
            current_price = price.get((ts, leg['symbol']))
            if current_price is None:
                continue
            loss = current_price - leg['entry_price']
            if loss >= STOPLOSS_PCT * leg['entry_price']:
                day_pnl += _record_leg_trade(trades, d, opt, leg, ts, current_price, 'STOPLOSS')
                state[opt] = None

        # ATM-premium-high stoploss (optional) - checked every minute, not just at checkpoints:
        # if the ATM premium is now higher than its own highest reading over the trailing 2
        # hours, close whatever legs are still open and stay flat for the rest of this hour
        # (consumed by the next checkpoint above, which skips its reopen once).
        if PREMIUM_HIGH_STOPLOSS_ENABLED:
            current_atm_premium = _atm_premium(atm, ts)
            if current_atm_premium is not None:
                window_start = ts - PREMIUM_HIGH_LOOKBACK
                prior_high = max((p for t, p in premium_history if t >= window_start), default=None)
              
             
                if prior_high is not None and current_atm_premium > prior_high and (state['CE'] is not None or state['PE'] is not None):
                    for opt in OPTION_TYPES:
                        leg = state[opt]
                        if leg is None:
                            continue
                        exit_price = price.get((ts, leg['symbol']))
                        if exit_price is not None:
                            day_pnl += _record_leg_trade(trades, d, opt, leg, ts, exit_price, 'ATM_PREMIUM_2H_HIGH')
                        state[opt] = None
                    suppress_reentry = True
                premium_history.append((ts, current_atm_premium))
                premium_history = [(t, p) for t, p in premium_history if t >= window_start]

        # ATM entry-premium stoploss (optional) - checked every minute: if ATM premium has risen
        # PREMIUM_STOPLOSS_PCT above its entry-time value, close whatever legs are still open and
        # stay flat for the rest of this hour (same suppress_reentry mechanism as the 2h-high
        # stoploss above).
        if PREMIUM_STOPLOSS_ENABLED and entry_atm_premium is not None:
            current_atm_premium = _atm_premium(atm, ts)
            if (
                current_atm_premium is not None
                and current_atm_premium > entry_atm_premium * (1 + PREMIUM_STOPLOSS_PCT)
                and (state['CE'] is not None or state['PE'] is not None)
            ):
                for opt in OPTION_TYPES:
                    leg = state[opt]
                    if leg is None:
                        continue
                    exit_price = price.get((ts, leg['symbol']))
                    if exit_price is not None:
                        day_pnl += _record_leg_trade(trades, d, opt, leg, ts, exit_price, 'ATM_PREMIUM_STOPLOSS')
                    state[opt] = None
                suppress_reentry = True

        # checked every minute (not just at checkpoints) so a fast move between checkpoints
        # can't blow past the limit unnoticed.
        if day_pnl + _unrealized_pnl(state, ts, price) <= -DAILY_LOSS_LIMIT:
            halted = True
            for opt in OPTION_TYPES:
                leg = state[opt]
                if leg is None:
                    continue
                exit_price = price.get((ts, leg['symbol']))
                if exit_price is not None:
                    day_pnl += _record_leg_trade(trades, d, opt, leg, ts, exit_price, 'DAILY_LOSS_LIMIT')
                state[opt] = None
            print(f'{d}: daily loss limit hit ({day_pnl:.2f}), trading halted at {ts.time()}')

    # in case the loop never reached an is_eod timestamp (e.g. data stops before EXIT_TIME)
    if any(state[opt] is not None for opt in OPTION_TYPES):
        last_ts = timestamps[-1]
        for opt in OPTION_TYPES:
            leg = state[opt]
            if leg is None:
                continue
            exit_price = price.get((last_ts, leg['symbol']))
            if exit_price is not None:
                _record_leg_trade(trades, d, opt, leg, last_ts, exit_price, 'EOD')

    return trades


def _out_path(name, underlying):
    """NIFTY keeps the original bare filenames (backward compatible with existing artifacts);
    any other underlying gets tagged so its outputs don't clobber NIFTY's."""
    if underlying != 'NIFTY':
        base, ext = os.path.splitext(name)
        name = f'{base}_{underlying}{ext}'
    return os.path.join(os.path.dirname(__file__), name)


TRADES_CSV_PATH = _out_path('rolling_straddle_trades_variation.csv', 'NIFTY')
PLOT_PATH = _out_path('rolling_straddle_daily_summary_variation.png', 'NIFTY')
EQUITY_PLOT_PATH = _out_path('rolling_straddle_equity_drawdown_variation.png', 'NIFTY')
MONTHLY_PLOT_PATH = _out_path('rolling_straddle_monthly_pnl_variation.png', 'NIFTY')

COLOR_GOOD = '#0ca30c'
COLOR_CRITICAL = '#d03b3b'
COLOR_NEUTRAL = '#2a78d6'
COLOR_MUTED_INK = '#898781'
COLOR_SURFACE = '#fcfcfb'


def plot_daily_summary(trades_df, save_path=PLOT_PATH, underlying='NIFTY'):
    """Two small multiples sharing a date axis: daily pnl (signed, good/critical) on top,
    daily trade count (neutral) below - kept as separate charts since the two measures
    are on different scales (points vs. count)."""
    daily = trades_df.groupby('date').agg(pnl=('pnl', 'sum'), trades=('pnl', 'count'))
    daily.index = pd.to_datetime(daily.index)

    fig, (ax_pnl, ax_count) = plt.subplots(
        2, 1, figsize=(10, 6), sharex=True, facecolor=COLOR_SURFACE,
        gridspec_kw={'height_ratios': [2, 1]},
    )

    pnl_colors = [COLOR_GOOD if v >= 0 else COLOR_CRITICAL for v in daily['pnl']]
    ax_pnl.bar(daily.index, daily['pnl'], color=pnl_colors, width=0.7)
    ax_pnl.axhline(0, color=COLOR_MUTED_INK, linewidth=0.8)
    ax_pnl.set_ylabel('PnL (points)')
    ax_pnl.set_title(f'{underlying}: Daily PnL and trade count - rolling first-OTM strangle Variation')
    ax_pnl.set_facecolor(COLOR_SURFACE)
    for spine in ('top', 'right'):
        ax_pnl.spines[spine].set_visible(False)
    ax_pnl.tick_params(colors=COLOR_MUTED_INK)

    ax_count.bar(daily.index, daily['trades'], color=COLOR_NEUTRAL, width=0.7)
    ax_count.set_ylabel('Trades')
    ax_count.set_facecolor(COLOR_SURFACE)
    for spine in ('top', 'right'):
        ax_count.spines[spine].set_visible(False)
    ax_count.tick_params(colors=COLOR_MUTED_INK)
    fig.autofmt_xdate()

    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    # plt.close(fig)
    plt.show()
    print(f'daily summary plot written to {save_path}')


def plot_equity_and_drawdown(trades_df, save_path=EQUITY_PLOT_PATH, underlying='NIFTY'):
    """Two small multiples sharing a date axis: cumulative pnl (equity curve) on top,
    drawdown from the running peak below - different measures (running total vs.
    distance-from-peak), so kept as separate charts rather than one dual-axis plot."""
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
    ax_equity.set_title(f'{underlying}: Equity curve and drawdown - rolling first-OTM strangle')
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
    # plt.close(fig)
    plt.show()
    print(f'equity/drawdown plot written to {save_path}')


def plot_monthly_pnl(trades_df, save_path=MONTHLY_PLOT_PATH, underlying='NIFTY'):
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
    ax.set_title(f'{underlying}: Monthly PnL - rolling first-OTM strangle')
    ax.set_facecolor(COLOR_SURFACE)
    for spine in ('top', 'right'):
        ax.spines[spine].set_visible(False)
    ax.tick_params(colors=COLOR_MUTED_INK)
    fig.autofmt_xdate()

    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.show()
    print(f'monthly pnl plot written to {save_path}')


def run_backtest(start_date=START_DATE, end_date=END_DATE, trades_csv_path=None, underlying='NIFTY'):
    if trades_csv_path is None:
        trades_csv_path = TRADES_CSV_PATH

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

    hourly_pnl = trades_df.groupby(trades_df['entry_time'].dt.hour)['pnl'].agg(['sum', 'count'])
    hourly_pnl.index.name = 'entry_hour'
    print(hourly_pnl)

    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    weekday_pnl = trades_df.groupby(trades_df['entry_time'].dt.day_name())['pnl'].agg(['sum', 'count'])
    weekday_pnl = weekday_pnl.reindex([w for w in weekday_order if w in weekday_pnl.index])
    weekday_pnl.index.name = 'entry_weekday'
    print(weekday_pnl)

    if trades_csv_path:
        trades_df.to_csv(trades_csv_path, index=False)
        print(f"trades written to {trades_csv_path}")

    plot_daily_summary(trades_df, save_path=_out_path('rolling_straddle_daily_summary.png', underlying), underlying=underlying)
    plot_equity_and_drawdown(trades_df, save_path=_out_path('rolling_straddle_equity_drawdown.png', underlying), underlying=underlying)
    plot_monthly_pnl(trades_df, save_path=_out_path('rolling_straddle_monthly_pnl.png', underlying), underlying=underlying)

    return trades_df


if __name__ == '__main__':
    import sys
    underlying = sys.argv[1].upper() if len(sys.argv) > 1 else 'NIFTY'
    TRADE_WEEKDAYS = _parse_trade_weekdays(sys.argv[2] if len(sys.argv) > 2 else None)
    run_backtest(underlying=underlying)
