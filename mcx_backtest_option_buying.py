"""
Backtest: MCX GOLDMINI ATM straddle BUYING on a premium-jump signal.

Rule:
  - Checkpoints every day, hourly, from 14:45 to 22:45 IST (14:45, 15:45,
    16:45, ... 22:45) - session window is 14:45 to 23:15 IST. Each checkpoint
    establishes a baseline ATM premium (CE + PE at whichever strike is ATM
    right now, same "current ATM, re-evaluated live" concept as
    plot_atm_premium.py).
  - The signal is NOT checked only at the next checkpoint - every minute
    between one checkpoint and the next (or session end, after the last
    checkpoint) is scanned against that interval's baseline. The instant the
    ATM premium is >= JUMP_THRESHOLD_POINTS (Rs) above the baseline, at
    whatever minute that happens to be, a BUY fires right then: the current
    ATM straddle (1 lot CE + 1 lot PE), held for exactly HOLD_MINUTES minutes
    (capped at the 23:15 session end). Each interval fires at most one signal
    (the first minute the condition is met); the next checkpoint then starts
    a fresh baseline regardless of what happened in the previous interval.
  - No stoploss/target - pure signal + fixed-time exit, buying (long) both
    legs, so P&L = (exit_price - entry_price) per leg.
  - Signals from different intervals are independent trades and are not
    prevented from overlapping in time.

Data:
  - Underlying (spot proxy): data/futures/GOLDM-04Sep2026-FUT.parquet
  - Options: data/options/GOLDM-{expiry}-{strike}-{CE,PE}.parquet
  - All timestamps in the downloaded parquet files are UTC; IST = UTC + 5:30.

Output:
  - backtest_trades_option_buying.csv - one row per leg per signal
  - Console summary: total P&L, win rate, signal count
"""

import argparse
import logging
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd

import plot_atm_premium as atm  # reuses its per-minute ATM-premium series builder
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
START_TIME_IST = time(11, 45)
END_TIME_IST = time(22, 45)
CHECKPOINT_INTERVAL_MIN = 60
HOLD_MINUTES = 10
JUMP_THRESHOLD_POINTS = 0.75   # ATM premium rise vs previous checkpoint that triggers a buy
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
    t = datetime.combine(trade_date, START_TIME_IST)
    end = datetime.combine(trade_date, END_TIME_IST)
    while t <= end:
        cps.append(pd.Timestamp(t))
        t += timedelta(minutes=CHECKPOINT_INTERVAL_MIN)
    return cps


# ── Main backtest loop ────────────────────────────────────────────────────────
def _premium_at_or_before(day_premium: pd.DataFrame, ts: pd.Timestamp):
    """Row of the per-minute ATM-premium series at/before ts, or None."""
    rows = day_premium[day_premium["timestamp"] <= ts]
    return rows.iloc[-1] if not rows.empty else None


def run() -> pd.DataFrame:
    fut = load_futures()
    start = BACKTEST_START or fut["timestamp"].dt.date.min()
    trading_dates = sorted(d for d in fut["timestamp"].dt.date.unique() if d >= start)

    trades = []
    for trade_date in trading_dates:
        end_dt = pd.Timestamp.combine(trade_date, END_TIME_IST)
        day_fut = fut[fut["timestamp"].dt.date == trade_date]
        if day_fut.empty or day_fut["timestamp"].max() < end_dt:
            log.warning("%s: futures data doesn't reach session end — skipping incomplete day", trade_date)
            continue

        day_premium = atm.build_atm_premium(trade_date, EXPIRY_LABEL)
        cps = checkpoints_for(trade_date)
        boundaries = cps + [end_dt]

        for i, cp in enumerate(cps):
            baseline_row = _premium_at_or_before(day_premium, cp)
            if baseline_row is None:
                log.warning("%s %s: no premium data yet — skipping interval", trade_date, cp.time())
                continue
            baseline_premium = baseline_row["atm_premium"]

            interval_end = boundaries[i + 1]
            window = day_premium[(day_premium["timestamp"] > cp) & (day_premium["timestamp"] <= interval_end)]
            breaches = window[(window["atm_premium"] - baseline_premium) >= JUMP_THRESHOLD_POINTS]
            if breaches.empty:
                continue

            trigger = breaches.iloc[0]
            trigger_ts = trigger["timestamp"]
            atm_strike = int(trigger["atm_strike"])
            ce_df, pe_df = load_option(atm_strike, "CE"), load_option(atm_strike, "PE")
            if ce_df is None or pe_df is None:
                log.warning("%s %s: no option file for triggered strike %s — skipping signal",
                            trade_date, trigger_ts, atm_strike)
                continue

            log.info("%s %s: premium jump %.1f -> %.1f (+%.1f, baseline @ %s) — BUY ATM %s straddle",
                      trade_date, trigger_ts.time(), baseline_premium, trigger["atm_premium"],
                      trigger["atm_premium"] - baseline_premium, cp.time(), atm_strike)
            exit_dt = min(trigger_ts + timedelta(minutes=HOLD_MINUTES), end_dt)

            for option_type, opt_df in (("CE", ce_df), ("PE", pe_df)):
                entry_price, entry_ts = price_at_or_before(opt_df, trade_date, trigger_ts)
                if entry_price is None:
                    log.warning("%s %s: no entry price for %s %s — skipping leg",
                                trade_date, trigger_ts, atm_strike, option_type)
                    continue
                exit_price, exit_ts = price_at_or_before(opt_df, trade_date, exit_dt)
                if exit_price is None:
                    log.warning("%s %s: no exit price for %s %s — skipping leg",
                                trade_date, trigger_ts, atm_strike, option_type)
                    continue
                trades.append({
                    "date": trade_date,
                    "checkpoint": cp,
                    "trigger_time": trigger_ts,
                    "option_type": option_type,
                    "strike": atm_strike,
                    "premium_before": baseline_premium,
                    "premium_at_signal": trigger["atm_premium"],
                    "entry_time": entry_ts,
                    "entry_price": entry_price,
                    "exit_time": exit_ts,
                    "exit_price": exit_price,
                    "pnl": (exit_price - entry_price) * LOTS,  # long: profit on price up
                })

    if not trades:
        log.error("No trades generated.")
        return pd.DataFrame()

    return pd.DataFrame(trades).sort_values(["date", "trigger_time", "option_type"]).reset_index(drop=True)


def summarize(trades_df: pd.DataFrame):
    if trades_df.empty:
        return
    daily = trades_df.groupby("date")["pnl"].sum()
    signals = trades_df.groupby(["date", "checkpoint"]).ngroups

    total_pnl = trades_df["pnl"].sum()
    wins = (trades_df["pnl"] > 0).sum()
    losses = (trades_df["pnl"] <= 0).sum()

    print("\n" + "=" * 70)
    print(f"{SYMBOL} ATM Straddle BUY on premium jump — checkpoints hourly "
          f"{START_TIME_IST.strftime('%H:%M')}-{END_TIME_IST.strftime('%H:%M')}, "
          f"+{JUMP_THRESHOLD_POINTS}pt trigger, {HOLD_MINUTES}min hold")
    print("=" * 70)
    print(f"Trading days:          {trades_df['date'].nunique()}")
    print(f"Signals fired:         {signals}")
    print(f"Legs traded:           {len(trades_df)}  (wins {wins} / losses {losses})")
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
    parser.add_argument("--threshold", type=float, default=JUMP_THRESHOLD_POINTS,
                         help=f"Premium-rise trigger in points/Rs (default: {JUMP_THRESHOLD_POINTS})")
    parser.add_argument("--hold", type=int, default=HOLD_MINUTES,
                         help=f"Hold duration in minutes (default: {HOLD_MINUTES})")
    parser.add_argument("--start", type=date.fromisoformat, default=None,
                         help="Backtest start date YYYY-MM-DD (default: full downloaded history)")
    parser.add_argument("--out", default=None, help="Output trade-log CSV path (default: backtest_trades_option_buying_<symbol>.csv)")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    cfg = sc.resolve(args.symbol)
    SYMBOL = cfg.symbol
    FUTURES_FILE = cfg.near_futures_file
    EXPIRY_LABEL = cfg.near_expiry
    STRIKE_STEP = cfg.strike_step
    # plot_atm_premium's build_atm_premium() reads its own module-level FUTURES_FILE/
    # STRIKE_STEP/SYMBOL globals - propagate this run's resolved symbol into it too.
    atm.SYMBOL = SYMBOL
    atm.FUTURES_FILE = FUTURES_FILE
    atm.STRIKE_STEP = STRIKE_STEP

    JUMP_THRESHOLD_POINTS = args.threshold
    HOLD_MINUTES = args.hold
    BACKTEST_START = args.start
    out_path = args.out or f"backtest_trades_option_buying_{SYMBOL}.csv"

    trades_df = run()
    if not trades_df.empty:
        trades_df.to_csv(out_path, index=False)
        log.info("Wrote %d leg rows -> %s", len(trades_df), out_path)
    summarize(trades_df)
