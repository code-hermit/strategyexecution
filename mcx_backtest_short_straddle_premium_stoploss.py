"""
Backtest: MCX GOLDMINI short ATM straddle with a combined premium stoploss.

Rule:
  - Checkpoints hourly from 14:45 IST (14:45, 15:45, 16:45, ... while <= 23:15).
    14:45 is a baseline-only checkpoint (records the ATM premium, no trading).
    Trading actions start at the 15:45 checkpoint.
  - At each checkpoint from 15:45 on: compute the current ATM straddle
    premium (CE + PE at whichever strike is ATM right now). If it has risen
    by AVOID_THRESHOLD_POINTS (default 100) or more versus the previous
    checkpoint (i.e. just the immediately preceding hour), avoid trading
    this checkpoint entirely - whatever is currently open just keeps
    running, unchanged. A smaller rise (below the threshold) does not
    trigger avoidance.
  - Otherwise (premium flat or down since the previous checkpoint), revise
    positions:
      - No position open yet -> SHORT the current ATM straddle (CE + PE).
      - A position is open and the ATM strike has changed -> close the old
        legs (at this checkpoint's prices) and SHORT a fresh straddle at the
        new ATM strike.
      - A position is open and the ATM strike is unchanged -> leave it alone.
  - No stoploss on each leg individually. Instead, a COMBINED 200-point
    premium stoploss: the sum of both legs' floating P&L is tracked minute
    by minute; if it drops to -200 (Rs) or worse at any time, both legs are
    squared off immediately (independent of checkpoints). The position stays
    flat until the next checkpoint's normal revise logic reopens it.
  - Session end 23:15 IST: whatever is still open is squared off.

Data:
  - Underlying (spot proxy): data/futures/GOLDM-04Sep2026-FUT.parquet
  - Options: data/options/GOLDM-{expiry}-{strike}-{CE,PE}.parquet
  - All timestamps in the downloaded parquet files are UTC; IST = UTC + 5:30.

Output:
  - backtest_trades_short_straddle_premium_sl.csv - one row per leg per
    open/close cycle (initial entry, strike-shift re-hedge, or stoploss/time exit)
  - Console summary: total P&L, win rate, stoploss/shift counts
"""

import argparse
import logging
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd

import symbols_config as sc

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuration (defaults; overridden in __main__ via --symbol) ────────────
SYMBOL = "GOLDM"
FUTURES_FILE = Path("data/futures/GOLDM-04Sep2026-FUT.parquet")
OPTIONS_DIR = Path("data/options")
EXPIRY_LABEL = "28Aug2026"   # near-month chain

BACKTEST_START = None   # None = use the full range of downloaded futures history

STRIKE_STEP = 5
CHECKPOINT_START_IST = time(10, 45)   # first checkpoint: baseline only, no trading
EXECUTION_START_IST = time(11, 45)    # trading begins at this checkpoint
EXIT_TIME_IST = time(22, 45)
CHECKPOINT_INTERVAL_MIN = 60
STOPLOSS_POINTS = 0.75                 # combined (both legs) floating-loss stoploss
AVOID_THRESHOLD_POINTS =0          # premium rise vs. previous checkpoint that triggers avoidance
LOTS = 1
IST_OFFSET = timedelta(hours=5, minutes=30)


# ── Helpers ───────────────────────────────────────────────────────────────────
def to_ist(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = df["timestamp"] + IST_OFFSET
    return df


def load_futures() -> pd.DataFrame:
    df = pd.read_parquet(FUTURES_FILE, columns=["timestamp", "close"])
    return to_ist(df).sort_values("timestamp").reset_index(drop=True)


def round_to_strike(price: float) -> int:
    return int(round(price / STRIKE_STEP) * STRIKE_STEP)


def option_file(strike: int, option_type: str) -> Path:
    return OPTIONS_DIR / f"{SYMBOL}-{EXPIRY_LABEL}-{strike}-{option_type}.parquet"


_option_cache: dict[Path, pd.DataFrame] = {}


def load_option(strike: int, option_type: str) -> pd.DataFrame | None:
    f = option_file(strike, option_type)
    if f in _option_cache:
        return _option_cache[f]
    if not f.exists():
        _option_cache[f] = None
        return None
    df = pd.read_parquet(f, columns=["timestamp", "close"])
    df = to_ist(df).sort_values("timestamp").reset_index(drop=True)
    _option_cache[f] = df
    return df


def price_at_or_before(df: pd.DataFrame, trade_date, ts: pd.Timestamp):
    day = df[(df["timestamp"].dt.date == trade_date) & (df["timestamp"] <= ts)]
    if day.empty:
        return None, None
    row = day.iloc[-1]
    return row["close"], row["timestamp"]


def checkpoints_for(trade_date) -> list[pd.Timestamp]:
    cps = []
    t = datetime.combine(trade_date, CHECKPOINT_START_IST)
    end = datetime.combine(trade_date, EXIT_TIME_IST)
    while t <= end:
        cps.append(pd.Timestamp(t))
        t += timedelta(minutes=CHECKPOINT_INTERVAL_MIN)
    return cps


def atm_premium_at(fut_day: pd.DataFrame, trade_date, cp: pd.Timestamp):
    """Returns (atm_strike, ce_price, pe_price, premium) or (None, None, None, None)."""
    spot, _ = price_at_or_before(fut_day, trade_date, cp)
    if spot is None:
        return None, None, None, None
    strike = round_to_strike(spot)
    ce_df, pe_df = load_option(strike, "CE"), load_option(strike, "PE")
    if ce_df is None or pe_df is None:
        return strike, None, None, None
    ce_price, _ = price_at_or_before(ce_df, trade_date, cp)
    pe_price, _ = price_at_or_before(pe_df, trade_date, cp)
    if ce_price is None or pe_price is None:
        return strike, None, None, None
    return strike, ce_price, pe_price, ce_price + pe_price


# ── Position open/close ─────────────────────────────────────────────────────
def open_position(trade_date, strike, cp):
    ce_df, pe_df = load_option(strike, "CE"), load_option(strike, "PE")
    if ce_df is None or pe_df is None:
        return None
    ce_price, ce_ts = price_at_or_before(ce_df, trade_date, cp)
    pe_price, pe_ts = price_at_or_before(pe_df, trade_date, cp)
    if ce_price is None or pe_price is None:
        return None
    return {
        "strike": strike,
        "ce_entry_price": ce_price, "ce_entry_time": ce_ts,
        "pe_entry_price": pe_price, "pe_entry_time": pe_ts,
    }


def close_position(trades, trade_date, pos, exit_ts_hint: pd.Timestamp, reason: str, exit_ts_override=None):
    """Closes both legs at (or before) exit_ts_hint - unless exit_ts_override is given
    (used for a stoploss breach, where both legs close at the exact breach timestamp)."""
    ce_df, pe_df = load_option(pos["strike"], "CE"), load_option(pos["strike"], "PE")
    for option_type, entry_price, entry_ts, opt_df in (
        ("CE", pos["ce_entry_price"], pos["ce_entry_time"], ce_df),
        ("PE", pos["pe_entry_price"], pos["pe_entry_time"], pe_df),
    ):
        if opt_df is None:
            continue
        ts = exit_ts_override if exit_ts_override is not None else exit_ts_hint
        exit_price, exit_ts = price_at_or_before(opt_df, trade_date, ts)
        if exit_price is None:
            continue
        trades.append({
            "date": trade_date,
            "option_type": option_type,
            "strike": pos["strike"],
            "entry_time": entry_ts,
            "entry_price": entry_price,
            "exit_time": exit_ts,
            "exit_price": exit_price,
            "exit_reason": reason,
            "pnl": (entry_price - exit_price) * LOTS,  # short: profit on price down
        })


def check_combined_sl(trade_date, pos, window_end: pd.Timestamp):
    """Looks for the combined (both legs) floating loss breaching -STOPLOSS_POINTS between
    the position's entry and window_end. Returns the breach timestamp, or None."""
    ce_df, pe_df = load_option(pos["strike"], "CE"), load_option(pos["strike"], "PE")
    if ce_df is None or pe_df is None:
        return None

    series = {}
    for label, entry_price, entry_ts, opt_df in (
        ("CE", pos["ce_entry_price"], pos["ce_entry_time"], ce_df),
        ("PE", pos["pe_entry_price"], pos["pe_entry_time"], pe_df),
    ):
        day = opt_df[(opt_df["timestamp"].dt.date == trade_date)
                     & (opt_df["timestamp"] > entry_ts)
                     & (opt_df["timestamp"] <= window_end)]
        if day.empty:
            continue
        pnl = (entry_price - day["close"]) * LOTS  # short leg
        series[label] = pd.Series(pnl.values, index=day["timestamp"].values)

    if not series:
        return None

    combined = pd.concat(series.values(), axis=1, keys=series.keys()).sort_index().ffill()
    total = combined.sum(axis=1, skipna=True)
    breach = total[total <= -STOPLOSS_POINTS]
    return breach.index[0] if not breach.empty else None


# ── Per-day simulation ────────────────────────────────────────────────────────
def simulate_day(trade_date, fut: pd.DataFrame) -> list[dict]:
    trades = []
    fut_day = fut[fut["timestamp"].dt.date == trade_date]
    cps = checkpoints_for(trade_date)
    boundaries = cps + [pd.Timestamp.combine(trade_date, EXIT_TIME_IST)]

    position = None
    prev_premium = None

    for idx, cp in enumerate(cps):
        strike, ce_price, pe_price, premium = atm_premium_at(fut_day, trade_date, cp)
        if premium is None:
            log.warning("%s %s: no premium data — leaving position as-is", trade_date, cp.time())
        elif cp.time() < EXECUTION_START_IST:
            # 14:45 baseline checkpoint: record premium, no trading.
            prev_premium = premium
        else:
            avoid = (
                prev_premium is not None
                and (premium - prev_premium) >= AVOID_THRESHOLD_POINTS
            )
            if avoid:
                log.info("%s %s: ATM premium rose %.1f -> %.1f (+%.1f >= %.0f) — avoid trading this checkpoint",
                          trade_date, cp.time(), prev_premium, premium, premium - prev_premium, AVOID_THRESHOLD_POINTS)
            elif position is None:
                position = open_position(trade_date, strike, cp)
                if position:
                    log.info("%s %s: SHORT new %s straddle @ %.1f",
                              trade_date, cp.time(), strike, premium)
            elif strike != position["strike"]:
                log.info("%s %s: ATM strike moved %s -> %s — re-hedging",
                          trade_date, cp.time(), position["strike"], strike)
                close_position(trades, trade_date, position, cp, "strike_shift")
                position = open_position(trade_date, strike, cp)
            # else: same strike, already have a position - leave it running.

            prev_premium = premium

        # Monitor for a combined-SL breach until the next checkpoint (or session end).
        if position is not None:
            interval_end = boundaries[idx + 1]
            breach_ts = check_combined_sl(trade_date, position, interval_end)
            if breach_ts is not None:
                log.info("%s: combined SL hit at %s — squaring off", trade_date, breach_ts)
                close_position(trades, trade_date, position, interval_end, "stoploss", exit_ts_override=breach_ts)
                position = None

    if position is not None:
        close_position(trades, trade_date, position, pd.Timestamp.combine(trade_date, EXIT_TIME_IST), "time_exit")

    return trades


# ── Main backtest loop ────────────────────────────────────────────────────────
def run() -> pd.DataFrame:
    fut = load_futures()
    start = BACKTEST_START or fut["timestamp"].dt.date.min()
    trading_dates = sorted(d for d in fut["timestamp"].dt.date.unique() if d >= start)

    all_trades = []
    for trade_date in trading_dates:
        exit_dt = pd.Timestamp.combine(trade_date, EXIT_TIME_IST)
        day_fut = fut[fut["timestamp"].dt.date == trade_date]
        if day_fut.empty or day_fut["timestamp"].max() < exit_dt:
            log.warning("%s: futures data doesn't reach session end — skipping incomplete day", trade_date)
            continue
        all_trades.extend(simulate_day(trade_date, fut))

    if not all_trades:
        log.error("No trades generated.")
        return pd.DataFrame()

    return pd.DataFrame(all_trades).sort_values(["date", "entry_time", "option_type"]).reset_index(drop=True)


def summarize(trades_df: pd.DataFrame):
    if trades_df.empty:
        return
    daily = trades_df.groupby("date")["pnl"].sum()

    total_pnl = trades_df["pnl"].sum()
    wins = (trades_df["pnl"] > 0).sum()
    losses = (trades_df["pnl"] <= 0).sum()
    stopped = (trades_df["exit_reason"] == "stoploss").sum()
    shifted = (trades_df["exit_reason"] == "strike_shift").sum()

    print("\n" + "=" * 70)
    print(f"{SYMBOL} Short ATM Straddle — checkpoints hourly from "
          f"{CHECKPOINT_START_IST.strftime('%H:%M')} (trading from {EXECUTION_START_IST.strftime('%H:%M')}), "
          f"{STOPLOSS_POINTS}-pt combined SL, avoid-if-premium-rose >= {AVOID_THRESHOLD_POINTS}pt")
    print("=" * 70)
    print(f"Trading days:          {trades_df['date'].nunique()}")
    print(f"Leg-instances traded:  {len(trades_df)}  (wins {wins} / losses {losses})")
    print(f"  ...stopped out:      {stopped}")
    print(f"  ...closed on shift:  {shifted}")
    print(f"Total P&L (1 lot ea):  {total_pnl:,.1f} Rs")
    print(f"Avg P&L per leg:       {trades_df['pnl'].mean():,.1f} Rs")
    print(f"Avg daily P&L:         {daily.mean():,.1f} Rs")
    print(f"Best day:              {daily.idxmax()}  ({daily.max():,.1f} Rs)")
    print(f"Worst day:             {daily.idxmin()}  ({daily.min():,.1f} Rs)")
    print(f"Max drawdown (cum):    {(daily.cumsum() - daily.cumsum().cummax()).min():,.1f} Rs")
    print("\nDaily P&L:")
    print(daily.to_string())


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=SYMBOL, help=f"MCX symbol to backtest (default: {SYMBOL})")
    parser.add_argument("--sl-points", type=float, default=STOPLOSS_POINTS,
                         help=f"Combined (both legs) floating-loss stoploss in points/Rs (default: {STOPLOSS_POINTS})")
    parser.add_argument("--avoid-threshold", type=float, default=AVOID_THRESHOLD_POINTS,
                         help=f"Premium rise vs. previous checkpoint (points/Rs) that triggers avoiding "
                              f"that checkpoint's trade (default: {AVOID_THRESHOLD_POINTS})")
    parser.add_argument("--start", type=date.fromisoformat, default=None,
                         help="Backtest start date YYYY-MM-DD (default: full downloaded history)")
    parser.add_argument("--out", default=None,
                         help="Output trade-log CSV path (default: backtest_trades_short_straddle_premium_sl_<symbol>.csv)")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    cfg = sc.resolve(args.symbol)
    SYMBOL = cfg.symbol
    FUTURES_FILE = cfg.near_futures_file
    EXPIRY_LABEL = cfg.near_expiry
    STRIKE_STEP = cfg.strike_step
    STOPLOSS_POINTS = args.sl_points
    AVOID_THRESHOLD_POINTS = args.avoid_threshold
    BACKTEST_START = args.start
    out_path = args.out or f"backtest_trades_short_straddle_premium_sl_{SYMBOL}.csv"

    trades_df = run()
    if not trades_df.empty:
        trades_df.to_csv(out_path, index=False)
        log.info("Wrote %d leg rows -> %s", len(trades_df), out_path)
    summarize(trades_df)
