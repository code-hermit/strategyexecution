"""
Margin table: short ATM straddle (current weekly expiry) hedged with cheap OTM
strangles (monthly expiry), using Zerodha Kite's basket margin API.

Two ways of choosing the monthly hedge strangle (--mode):
    strikes: pair the k-th OTM call with the k-th OTM put (k = 1 is the first
             strike on each side of the ATM), out to the farthest priced strikes
    price:   for each target premium (Rs 1, 2, ... 50), the OTM call and the OTM
             put each priced nearest to that target
For every pair it asks Kite for the margin of:
    1. naked short straddle (weekly, MIS by default)
    2. short straddle (weekly, MIS) + long strangle (monthly, NRML)
    3. same as 2 but with every leg NRML, to show whether mixing products
       loses any hedge benefit
    4. same as 2 but with every leg MIS

Usage:
    export KITE_API_KEY=...  KITE_ACCESS_TOKEN=...
    python straddle_hedge_margin.py --index NIFTY --lots 1
    python straddle_hedge_margin.py --index SENSEX --max-otm 30 --csv out.csv
    python straddle_hedge_margin.py --mode price --price-targets 1 2 5 10 20 50
"""

import argparse
import os
import time
from datetime import date
import json
import pandas as pd
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
from pathlib import Path

zerodha_access_token_file=Path(__file__).parent/'zerodha_token.json'
INDEX_CONFIG = {
    "NIFTY": {"exchange": "NFO", "spot": "NSE:NIFTY 50"},
    "BANKNIFTY": {"exchange": "NFO", "spot": "NSE:NIFTY BANK"},
    "FINNIFTY": {"exchange": "NFO", "spot": "NSE:NIFTY FIN SERVICE"},
    "MIDCPNIFTY": {"exchange": "NFO", "spot": "NSE:NIFTY MID SELECT"},
    "SENSEX": {"exchange": "BFO", "spot": "BSE:SENSEX"},
    "BANKEX": {"exchange": "BFO", "spot": "BSE:BANKEX"},
}

QUOTE_CHUNK = 500  # kite.quote() limit per call

# Minimum seconds between calls, kept well under Kite's limits (quote/ltp: 1/s,
# other endpoints: 10/s) so a live program sharing the same API key keeps headroom.
MIN_INTERVAL = {"quote": 2.0, "ltp": 2.0, "instruments": 2.0, "basket_order_margins": 1.0}


class ReadOnlyKite:
    """Only exposes data/margin calls, throttled. Order, GTT, session and token
    methods are unreachable, so this script cannot place or modify trades."""

    def __init__(self, kite):
        self._kite = kite
        self._last = {}

    def __getattr__(self, name):
        if name not in MIN_INTERVAL:
            raise AttributeError(f"ReadOnlyKite: '{name}' is not allowed")
        fn = getattr(self._kite, name)

        def throttled(*args, **kwargs):
            wait = self._last.get(name, 0) + MIN_INTERVAL[name] - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            try:
                return fn(*args, **kwargs)
            finally:
                self._last[name] = time.monotonic()

        return throttled


def get_kite():
    # Reuses an existing access token. Never calls generate_session(): a fresh
    # login on the same app would invalidate the token a live program is using.
    kite = KiteConnect(api_key=os.getenv("ZERODHA_API_KEY"))
    kite_access_token=None
    with open(zerodha_access_token_file,'r') as fr:
        kite_access_token=json.load(fr)['access_token']
    kite.set_access_token(kite_access_token)
    return ReadOnlyKite(kite)


def load_chain(kite, index, exchange):
    df = pd.DataFrame(kite.instruments(exchange))
    df = df[(df["name"] == index) & (df["instrument_type"].isin(["CE", "PE"]))].copy()
    df["expiry"] = pd.to_datetime(df["expiry"]).dt.date
    return df[df["expiry"] >= date.today()]


def pick_expiries(chain):
    """Weekly = nearest expiry. Monthly = last expiry of a calendar month, strictly after the weekly."""
    expiries = sorted(chain["expiry"].unique())
    weekly = expiries[0]
    monthlies = sorted({max(e for e in expiries if (e.year, e.month) == (y, m))
                        for y, m in {(e.year, e.month) for e in expiries}})
    monthly = next(e for e in monthlies if e > weekly)
    return weekly, monthly


def fetch_prices(kite, exchange, symbols, field):
    """Return {tradingsymbol: price}. field='ltp' or 'ask' (best offer, falls back to LTP)."""
    prices = {}
    keys = [f"{exchange}:{s}" for s in symbols]
    for i in range(0, len(keys), QUOTE_CHUNK):
        for key, q in kite.quote(keys[i:i + QUOTE_CHUNK]).items():
            px = q.get("last_price") or 0.0
            if field == "ask":
                sells = q.get("depth", {}).get("sell", [])
                if sells and sells[0]["price"] > 0:
                    px = sells[0]["price"]
            prices[key.split(":", 1)[1]] = px
    return prices


def order(exchange, symbol, side, qty, product):
    return {
        "exchange": exchange,
        "tradingsymbol": symbol,
        "transaction_type": side,
        "variety": "regular",
        "product": product,
        "order_type": "MARKET",
        "quantity": qty,
        "price": 0,
        "trigger_price": 0,
    }


def basket_margin(kite, orders):
    resp = kite.basket_order_margins(orders, consider_positions=False, mode="compact")
    return resp["final"]["total"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--index", default="NIFTY", choices=INDEX_CONFIG)
    ap.add_argument("--lots", type=int, default=1)
    ap.add_argument("--max-otm", type=int, default=None,
                    help="only test hedge pairs up to this many strikes OTM (default: all priced strikes)")
    ap.add_argument("--mode", default="both", choices=["strikes", "price", "both"],
                    help="how to pick hedge pairs (see above)")
    ap.add_argument("--price-targets", type=float, nargs="+", default=list(range(1, 51)),
                    help="per-leg premium targets for --mode price (default: 1..50)")
    ap.add_argument("--price-field", default="ltp", choices=["ltp", "ask"],
                    help="price used to choose hedge strikes")
    ap.add_argument("--straddle-product", default="MIS", choices=["MIS", "NRML"])
    ap.add_argument("--hedge-product", default="NRML", choices=["MIS", "NRML"])
    ap.add_argument("--csv", help="path to save the table (default: <INDEX>_margin.csv)")
    args = ap.parse_args()
    if args.csv is None:
        args.csv = f"{args.index}_margin.csv"

    cfg = INDEX_CONFIG[args.index]
    exch = cfg["exchange"]
    kite = get_kite()

    chain = load_chain(kite, args.index, exch)
    weekly, monthly = pick_expiries(chain)
    wk = chain[chain["expiry"] == weekly]
    mo = chain[chain["expiry"] == monthly].copy()

    lot_size = int(wk["lot_size"].iloc[0])
    qty = args.lots * lot_size

    spot = kite.ltp([cfg["spot"]])[cfg["spot"]]["last_price"]
    strikes = sorted(wk["strike"].unique())
    atm = min(strikes, key=lambda k: abs(k - spot))
    ce_w = wk[(wk["strike"] == atm) & (wk["instrument_type"] == "CE")].iloc[0]["tradingsymbol"]
    pe_w = wk[(wk["strike"] == atm) & (wk["instrument_type"] == "PE")].iloc[0]["tradingsymbol"]
    w_px = fetch_prices(kite, exch, [ce_w, pe_w], "ltp")
    straddle_prem = w_px[ce_w] + w_px[pe_w]

    # Monthly OTM chain with prices
    mo = mo[((mo["instrument_type"] == "CE") & (mo["strike"] > atm)) |
            ((mo["instrument_type"] == "PE") & (mo["strike"] < atm))]
    prices = fetch_prices(kite, exch, mo["tradingsymbol"].tolist(), args.price_field)
    mo["price"] = mo["tradingsymbol"].map(prices).fillna(0.0)
    mo["dist"] = (mo["strike"] - atm).abs()
    # k-th OTM call paired with k-th OTM put, nearest first; drop unpriced strikes
    otm_ce = mo[(mo["instrument_type"] == "CE") & (mo["price"] > 0)].sort_values("dist").reset_index(drop=True)
    otm_pe = mo[(mo["instrument_type"] == "PE") & (mo["price"] > 0)].sort_values("dist").reset_index(drop=True)
    n_pairs = min(len(otm_ce), len(otm_pe))
    if args.max_otm:
        n_pairs = min(n_pairs, args.max_otm)

    def straddle_orders(product):
        return [order(exch, ce_w, "SELL", qty, product), order(exch, pe_w, "SELL", qty, product)]

    naked = basket_margin(kite, straddle_orders(args.straddle_product))

    print(f"\n{args.index}  spot={spot:.2f}  ATM={atm:g}  lots={args.lots} (qty {qty})")
    print(f"Straddle (weekly {weekly}): {ce_w} @ {w_px[ce_w]:.2f} + {pe_w} @ {w_px[pe_w]:.2f}"
          f" = {straddle_prem:.2f} pts (Rs {straddle_prem * qty:,.0f})")
    print(f"Hedge expiry (monthly): {monthly}   price field: {args.price_field}")
    print(f"Products: straddle {args.straddle_product}, hedge {args.hedge_product}")
    print(f"Naked straddle margin: Rs {naked:,.0f}")
    print()

    def run_pair(variation, label, ce, pe):
        def hedge_orders(product):
            return [order(exch, ce["tradingsymbol"], "BUY", qty, product),
                    order(exch, pe["tradingsymbol"], "BUY", qty, product)]

        hedged = basket_margin(kite, straddle_orders(args.straddle_product) + hedge_orders(args.hedge_product))
        hedged_all_nrml = basket_margin(kite, straddle_orders("NRML") + hedge_orders("NRML"))
        hedged_all_mis = basket_margin(kite, straddle_orders("MIS") + hedge_orders("MIS"))
        hedge_pts = ce["price"] + pe["price"]
        rows.append({
            "variation": variation,
            "pick": label,
            "straddle_expiry": weekly,
            "ATM_strike": atm,
            "hedge_expiry": monthly,
            "CE_strike": ce["strike"], "CE_px": ce["price"], "CE_dist": ce["dist"],
            "PE_strike": pe["strike"], "PE_px": pe["price"], "PE_dist": pe["dist"],
            "strangle_pts": round(hedge_pts, 2),
            "hedge_cost_Rs": round(hedge_pts * qty),
            "naked_margin": round(naked),
            "hedged_margin": round(hedged),
            "margin_saved": round(naked - hedged),
            "saved_%": round(100 * (naked - hedged) / naked, 1),
            "hedged_all_NRML": round(hedged_all_nrml),
            "hedged_all_MIS": round(hedged_all_mis),
            "net_credit_Rs": round((straddle_prem - hedge_pts) * qty),
        })
        print(f"  {label:<10} {ce['strike']:g}CE @ {ce['price']:.2f} + {pe['strike']:g}PE @ {pe['price']:.2f}"
              f" = {hedge_pts:.2f} pts (Rs {hedge_pts * qty:,.0f}) | hedged Rs {hedged:,.0f}"
              f" (saved Rs {naked - hedged:,.0f}, {100 * (naked - hedged) / naked:.1f}%)"
              f" | all NRML Rs {hedged_all_nrml:,.0f} | all MIS Rs {hedged_all_mis:,.0f}"
              f" | net credit Rs {(straddle_prem - hedge_pts) * qty:,.0f}", flush=True)

    rows = []
    if args.mode in ("strikes", "both"):
        print(f"Strike walk: {n_pairs} hedge pairs, 1st OTM outward")
        for k in range(n_pairs):
            run_pair("strikes", f"OTM #{k + 1}", otm_ce.iloc[k], otm_pe.iloc[k])
        print()

    if args.mode in ("price", "both"):
        print(f"Price match: legs priced nearest to Rs {args.price_targets[0]:g}..{args.price_targets[-1]:g}")
        seen = set()
        for t in args.price_targets:
            # nearest price to target; on a tie prefer the farther (cheaper-risk) strike
            ce = otm_ce.iloc[((otm_ce["price"] - t).abs() - otm_ce["dist"] * 1e-9).idxmin()]
            pe = otm_pe.iloc[((otm_pe["price"] - t).abs() - otm_pe["dist"] * 1e-9).idxmin()]
            pair = (ce["tradingsymbol"], pe["tradingsymbol"])
            if pair in seen:  # same strikes as an earlier target, skip re-query
                continue
            seen.add(pair)
            run_pair("price", f"~Rs {t:g}", ce, pe)
        print()

    table = pd.DataFrame(rows)
    with pd.option_context("display.width", 250, "display.max_columns", None):
        print(table.to_string(index=False))
    if args.csv:
        table.to_csv(args.csv, index=False)
        print(f"\nSaved to {args.csv}")


if __name__ == "__main__":
    main()
