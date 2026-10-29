"""
hedge_buyer.py - picks and buys each morning's NIFTY + SENSEX straddle hedges on AliceBlue at 09:38,
ahead of exec_rsv_adjust_sl.py's 09:45 entry. Automates the process worked out by hand in Oct 2026.

How the legs are chosen:
  1. Budget = AliceBlue /limits tradingLimit (cash + pledged collateral after haircut).
  2. Reserved out of it: AliceBlue's extra over SPAN + exposure (~Rs 8.3k per NIFTY+SENSEX lot,
     measured 22-23 Oct 2026), sensex_option_buying.py's premium (its lots x lot size x SENSEX ATM
     leg premium), and a spare buffer that grows with VIX (SPARE_BASE at VIX <= SPARE_VIX_FLOOR,
     + SPARE_PER_VIX_POINT for every point above).
  3. For each index, every symmetric strangle (ATM +/- d) whose two legs actually trade (live ask and
     MIN_VOLUME) is priced, and the full straddle + hedge (+ any hedges already held on this expiry)
     is run through Zerodha's basket-margin API. AliceBlue charges SPAN + exposure with NO credit for
     the premium received, so Zerodha's `total` (which nets that premium off) is NOT used.
     Every such margin figure is padded by MARGIN_SAFETY_PCT (5%) before it is compared to the budget.
  4. The cheapest NIFTY + SENSEX combination that fits the budget wins - i.e. the farthest hedges
     that still leave the spare buffer. If none does (typically an expiry day with the account near
     its limit), the cheapest one leaving at least SPARE_MIN; failing that, the lowest-margin one.
     Spend is always capped at MAX_TOTAL_HEDGE_COST, and every fallback alerts.
Expiry days need no special case: Zerodha's margin already includes the expiring index's extra
exposure, which pushes the choice closer on its own, and close hedges are cheap that day anyway.

Hedges already held on this week's expiry (bought on an earlier day, NRML) are counted in step 3,
and "buy nothing for this index" is always a candidate - so a held hedge that still does the job is
kept and nothing new is bought. exec_rsv_adjust_sl.py ignores long positions, so it never touches
these legs.

Every order placed (buy and sell), its fill and the positions it opened are written to
hedge_buyer_state.json as they happen. A second run with --close (cron 15:14) reads that file at
15:15 and sells back exactly what was bought - never more than is still held - once that index's
straddle is flat (exec_rsv_adjust_sl.py exits at 15:13), so a hedge is never sold out from under a
still-open short. Hedges bought by hand are not in the file and are left alone.

Usage:
    python3 hedge_buyer.py            # cron at 09:37: plan, wait for 09:38, buy (DRY_RUN from .env)
    python3 hedge_buyer.py --close    # cron at 15:14: at 15:15 sell back this morning's hedges
    python3 hedge_buyer.py --plan     # just print today's picks, place nothing (any time in market hours)
    python3 hedge_buyer.py --plan --use-ltp   # same, priced off LTP with no liquidity filter (pre-open/after hours)
"""

import argparse
import csv
import io
import json
import logging
import math
import os
import signal
import sys
import time as time_module
from datetime import datetime, time as dtime, timezone
from typing import NamedTuple, Optional

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOGS_DIR, 'hedge_buyer.log')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_TIMEOUT = 10


def _telegram_send(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage',
            json={'chat_id': TELEGRAM_CHAT_ID, 'text': message},
            timeout=TELEGRAM_TIMEOUT,
        )
    except Exception as exc:  # never let a Telegram hiccup break the run
        print(f'Telegram alert failed: {exc}', file=sys.stderr)


class TelegramHandler(logging.Handler):
    """Forwards log records to Telegram, except those logged with extra={'no_telegram': True} (the
    per-candidate margin lines - too many to be worth an alert each)."""

    def emit(self, record):
        if getattr(record, 'no_telegram', False):
            return
        try:
            _telegram_send(self.format(record))
        except Exception as exc:
            print(f'TelegramHandler.emit failed: {exc}', file=sys.stderr)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout), TelegramHandler()],
)
log = logging.getLogger(__name__)
QUIET = {'no_telegram': True}


def _log_uncaught_exception(exc_type, exc_value, exc_tb):
    log.critical('Uncaught exception', exc_info=(exc_type, exc_value, exc_tb))


sys.excepthook = _log_uncaught_exception


def _log_and_exit_on_signal(signum, frame):
    log.critical(f'Received signal {signal.Signals(signum).name} ({signum}) - exiting')
    sys.exit(1)


for _sig in (signal.SIGTERM, signal.SIGHUP):
    signal.signal(_sig, _log_and_exit_on_signal)


# ── Config ──────────────────────────────────────────────────────────────────────────────────────
DRY_RUN = os.getenv('DRY_RUN', 'true').lower() != 'false'  # set DRY_RUN=false to place real orders

PLACE_TIME = dtime(9, 38)  # orders go in here (planning starts as soon as the process does - cron 09:37)
LATEST_PLACE_TIME = dtime(9, 44, 30)  # past this the straddles are about to enter - alert, buy nothing
CLOSE_TIME = dtime(15, 15)  # --close sells the hedges here - after the straddles' 15:13 EXIT_TIME
CLOSE_GIVE_UP = dtime(15, 35)  # an index's straddle still short by now -> leave its hedges, alert (F&O closes 15:40)
STATE_FILE = os.path.join(os.path.dirname(__file__), 'hedge_buyer_state.json')  # today's orders/fills/positions

# lots MUST match exec_rsv_adjust_sl.py's CFG[...]['lots']. min/max_distance bound the strangles
# tried (points from ATM); min_volume is the day's traded quantity a leg needs to count as liquid.
# freeze_qty = exchange max quantity per order (larger orders are sliced).
CFG = {
    'NIFTY': dict(
        lots=7, strike_interval=50, aliceblue_exchange='NFO', zerodha_exchange='NFO',
        spot='NSE:NIFTY 50', min_distance=250, max_distance=2000, min_volume=200_000, freeze_qty=1800,
    ),
    'SENSEX': dict(
        lots=7, strike_interval=100, aliceblue_exchange='BFO', zerodha_exchange='BFO',
        spot='BSE:SENSEX', min_distance=600, max_distance=6000, min_volume=20_000, freeze_qty=1000,
    ),
}
OPTION_BUYING_LOTS = 7  # sensex_option_buying.py CFG['lots'] - its premium is reserved out of the budget
ALICEBLUE_EXTRA_PER_LOT = 4_150  # per index-lot: AliceBlue blocked ~Rs 8.3k per NIFTY+SENSEX lot over SPAN+exposure
# Extra exposure margin the broker announces for a day (e.g. 2026-10-28: 'additional 2% exposure'), as a
# percent of the straddle's short notional (both legs). Hedges don't reduce it. Set in .env for that day,
# e.g. EXTRA_EXPOSURE_PCT=2, and back to 0 after.
EXTRA_EXPOSURE_PCT = float(os.getenv('EXTRA_EXPOSURE_PCT', '0'))
MARGIN_SAFETY_PCT = 0.05  # every straddle + hedge margin estimate is padded this much (added 28 Oct 2026) -
# AliceBlue's live margin can run above Zerodha's basket figure, so hedges are picked for 5% more than it says
SPARE_BASE = 300_000  # spare buffer left unused at VIX <= SPARE_VIX_FLOOR ...
SPARE_VIX_FLOOR = 16.0
SPARE_PER_VIX_POINT = 75_000  # ... plus this per VIX point above it (~Rs 4.5L at VIX 18)
SPARE_MIN = 150_000  # when the VIX target can't be met (e.g. expiry days), cheapest set leaving at least this
MAX_TOTAL_HEDGE_COST = 25_000  # hard cap on a morning's hedge spend (both indices, all legs)
LOGIN_WAIT_UNTIL = dtime(9, 43)  # tokens come from a manual morning login - wait for them until here
VIX_INSTRUMENT = 'NSE:INDIA VIX'

ORDER_TAG_PREFIX = 'hg'  # buys: 'hg' + N/S + C/P + HHMM, e.g. 'hgNC0938' - see exec_rsv_adjust_sl.py's tag notes
CLOSE_TAG_PREFIX = 'hx'  # 15:15 sells, e.g. 'hxNC1515'
ENTRY_OFFSET_PCT = 0.02  # first limit price: ask + 2%
CHASE_WAIT_SECONDS = 4  # reprice an unfilled leg after this long ...
CHASE_STEP_PCT = 0.05  # ... by another 5% of its fresh ask ...
CHASE_MAX_PCT = 0.40  # ... up to ask + 40%, then leave it resting and alert
CHASE_TIMEOUT_SECONDS = 90
BROKER_FETCH_GAP_SECONDS = 2.0  # order-book reads at most every 2s - shared account with the strategies
TERMINAL_ORDER_STATUSES = {'complete', 'rejected', 'cancelled'}
REQUEST_TIMEOUT = 10
MARGIN_CALL_GAP_SECONDS = 0.25  # Zerodha basket-margin pacing
QUOTE_CHUNK = 500  # Zerodha /quote instruments per call


def _today():
    return datetime.now().date()


def _wait_until(t):
    while datetime.now().time() < t:
        time_module.sleep(0.5)


# ── Zerodha (REST, Kite Connect) - prices, VIX and margin only ─────────────────────────────────
ZERODHA_API_KEY = os.getenv('ZERODHA_API_KEY')
ZERODHA_TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'zerodha_token.json')
ZERODHA_BASE_URL = 'https://api.kite.trade'


ZERODHA_HEADERS = None  # set by _wait_for_logins()


def _zerodha_headers():
    with open(ZERODHA_TOKEN_FILE) as f:
        token = json.load(f)['access_token']
    return {'Authorization': f'token {ZERODHA_API_KEY}:{token}', 'X-Kite-Version': '3'}


def _zerodha_weekly_options(index, cfg):
    """{(strike, 'CE'/'PE'): row} for `index`'s nearest expiry, plus that expiry date."""
    resp = requests.get(f"{ZERODHA_BASE_URL}/instruments/{cfg['zerodha_exchange']}", headers=ZERODHA_HEADERS, timeout=30)
    resp.raise_for_status()
    rows = [
        r for r in csv.DictReader(io.StringIO(resp.text))
        if r['name'] == index and r['instrument_type'] in ('CE', 'PE') and r['expiry']
        and datetime.strptime(r['expiry'], '%Y-%m-%d').date() >= _today()
    ]
    if not rows:
        raise RuntimeError(f"No {index} options on Zerodha {cfg['zerodha_exchange']}")
    expiry = min(r['expiry'] for r in rows)
    return {(int(float(r['strike'])), r['instrument_type']): r for r in rows if r['expiry'] == expiry}, expiry


def _zerodha_quotes(keys):
    """{'NFO:SYMBOL': full quote} - depth included."""
    out = {}
    for i in range(0, len(keys), QUOTE_CHUNK):
        resp = requests.get(
            f'{ZERODHA_BASE_URL}/quote', headers=ZERODHA_HEADERS,
            params=[('i', k) for k in keys[i:i + QUOTE_CHUNK]], timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        out.update(resp.json()['data'])
        time_module.sleep(1.0)  # /quote is limited to 1 request/s
    return out


def _ask(quote):
    sells = (quote or {}).get('depth', {}).get('sell', [])
    return float(sells[0]['price']) if sells and sells[0]['price'] > 0 and sells[0]['quantity'] > 0 else 0.0


def _margin_order(exchange, tradingsymbol, side, qty):
    return {
        'exchange': exchange, 'tradingsymbol': tradingsymbol, 'transaction_type': side, 'variety': 'regular',
        'product': 'NRML', 'order_type': 'MARKET', 'quantity': qty, 'price': 0, 'trigger_price': 0,
    }


def _span_plus_exposure(orders):
    """AliceBlue-style margin for a basket: SPAN + exposure, no credit for premium received, padded by
    MARGIN_SAFETY_PCT."""
    time_module.sleep(MARGIN_CALL_GAP_SECONDS)
    resp = requests.post(
        f'{ZERODHA_BASE_URL}/margins/basket', params={'consider_positions': 'false'},
        json=orders, headers=ZERODHA_HEADERS, timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    final = resp.json()['data']['final']
    return (float(final['span']) + float(final['exposure'])) * (1 + MARGIN_SAFETY_PCT)


# ── AliceBlue (REST, v3 open-api) - funds, positions, orders ───────────────────────────────────
ALICEBLUE_TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'aliceblue_token.json')
ALICEBLUE_BASE_URL = 'https://a3.aliceblueonline.com/open-api/od/v1'
ALICEBLUE_CONTRACT_MASTER_URL = 'https://v2api.aliceblueonline.com/restpy/static/contract_master/V2/{exchange}'
ALICEBLUE_PRODUCT = 'LONGTERM'  # = NRML - hedges are held to expiry, never auto-squared-off

ALICEBLUE_HEADERS = None  # set by _wait_for_logins()


def _aliceblue_headers():
    with open(ALICEBLUE_TOKEN_FILE) as f:
        return {'Authorization': f"Bearer {json.load(f)['userSession']}", 'Content-Type': 'application/json'}


def _aliceblue_result(path, data):
    if data.get('status') == 'Ok':
        return data['result']
    if data.get('status') == 'EC920':  # empty result (e.g. no orders yet today)
        return []
    # EC916 "No orders found" = empty order book (crashed the 2026-10-27 buy run) - only trusted on
    # the /orders/book read; on placeorder/modify it must still raise
    if path.startswith('/orders/book') and data.get('status') == 'EC916':
        return []
    raise RuntimeError(f'AliceBlue request to {path} failed: {data}')


def _aliceblue_get(path):
    resp = requests.get(ALICEBLUE_BASE_URL + path, headers=ALICEBLUE_HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return _aliceblue_result(path, resp.json())


def _aliceblue_post(path, payload):
    resp = requests.post(ALICEBLUE_BASE_URL + path, json=payload, headers=ALICEBLUE_HEADERS, timeout=REQUEST_TIMEOUT)
    if not resp.ok:
        raise RuntimeError(f'AliceBlue POST {path} failed ({resp.status_code}): {resp.text}')
    return _aliceblue_result(path, resp.json())


def _logins_valid():
    """(zerodha_headers, aliceblue_headers) if both of today's tokens work, else None."""
    try:
        z, a = _zerodha_headers(), _aliceblue_headers()
        ok = (requests.get(f'{ZERODHA_BASE_URL}/user/profile', headers=z, timeout=REQUEST_TIMEOUT).ok
              and requests.get(f'{ALICEBLUE_BASE_URL}/limits/', headers=a, timeout=REQUEST_TIMEOUT).ok)
        return (z, a) if ok else None
    except Exception:  # token file missing/half-written mid-login, network blip - just try again
        return None


def _wait_for_logins(plan_only):
    """Both tokens come from a manual browser login each morning - if that hasn't happened yet at
    09:37, wait for it (re-checking every 15s) until LOGIN_WAIT_UNTIL rather than dying."""
    global ZERODHA_HEADERS, ALICEBLUE_HEADERS
    warned = False
    while True:
        headers = _logins_valid()
        if headers:
            ZERODHA_HEADERS, ALICEBLUE_HEADERS = headers
            if warned:
                log.info('logins found - continuing')
            return True
        if plan_only or datetime.now().time() >= LOGIN_WAIT_UNTIL:
            log.error('Zerodha/AliceBlue login not valid - run zerodha_generate_access_token.py and '
                      'aliceblue_token_generation.py. NOT buying hedges - buy them manually.')
            return False
        if not warned:
            log.warning(f'Zerodha/AliceBlue login not valid yet - waiting for it until {LOGIN_WAIT_UNTIL:%H:%M}')
            warned = True
        time_module.sleep(15)


class Instrument(NamedTuple):
    token: int
    name: str
    lot_size: int
    tick_size: float
    exchange: str


def _aliceblue_weekly_contracts(index, cfg):
    """{(strike, 'CE'/'PE'): Instrument} for `index`'s nearest expiry, plus that expiry date."""
    exchange = cfg['aliceblue_exchange']
    resp = requests.get(ALICEBLUE_CONTRACT_MASTER_URL.format(exchange=exchange), timeout=30)
    resp.raise_for_status()
    opts = [
        c for c in resp.json()[exchange]
        if c['symbol'] == index and c['option_type'] in ('CE', 'PE')
        and datetime.fromtimestamp(c['expiry_date'] / 1000, tz=timezone.utc).date() >= _today()
    ]
    if not opts:
        raise RuntimeError(f'No {index} contracts on AliceBlue {exchange}')
    expiry_ms = min(c['expiry_date'] for c in opts)
    contracts = {
        (int(float(c['strike_price'])), c['option_type']): Instrument(
            int(c['token']), c['trading_symbol'], int(c['lot_size']), float(c['tick_size']), c['exch'])
        for c in opts if c['expiry_date'] == expiry_ms
    }
    return contracts, datetime.fromtimestamp(expiry_ms / 1000, tz=timezone.utc).date()


def _available_funds():
    """Total trading limit (cash + pledged collateral after haircut). Deliberately NOT net of
    utilizedMargin: the plan adds the straddles' own margin itself, and during market hours
    utilizedMargin already includes them (--plan mid-day would count them twice)."""
    return float(_aliceblue_get('/limits/')[0]['tradingLimit'])


def _status(o):
    status = str(o.get('orderStatus', '')).strip().lower()
    return 'cancelled' if status.startswith('cancel') else status  # AliceBlue spells it 'CANCELED'


def _tag_of(o):
    return str(o.get('remarks') or o.get('orderTag') or '').strip()


# ── Planning ────────────────────────────────────────────────────────────────────────────────────
class Leg(NamedTuple):
    strike: int
    opt: str
    zsym: str
    ask: float


class Candidate(NamedTuple):
    index: str
    distance: Optional[int]  # None = buy nothing new for this index
    legs: tuple
    qty: int
    cost: float
    margin: float


def _plan_index(index, cfg, use_ltp):
    """Every viable hedge choice for `index`, nearest first, with its cost and SPAN + exposure."""
    zopts, zexpiry = _zerodha_weekly_options(index, cfg)
    contracts, aexpiry = _aliceblue_weekly_contracts(index, cfg)
    if str(aexpiry) != zexpiry:
        raise RuntimeError(f'{index}: AliceBlue expiry {aexpiry} != Zerodha expiry {zexpiry}')
    lot_size = next(iter(contracts.values())).lot_size
    qty = cfg['lots'] * lot_size
    exch = cfg['zerodha_exchange']

    spot = _zerodha_quotes([cfg['spot']])[cfg['spot']]['last_price']
    step = cfg['strike_interval']
    atm = round(spot / step) * step
    distances = range(cfg['min_distance'], cfg['max_distance'] + 1, step)
    wanted = [(atm, 'CE'), (atm, 'PE')] + [(atm + d, 'CE') for d in distances] + [(atm - d, 'PE') for d in distances]
    keys = [f"{exch}:{zopts[k]['tradingsymbol']}" for k in wanted if k in zopts]
    quotes = _zerodha_quotes(keys)

    def price(strike, opt):
        row = zopts.get((strike, opt))
        q = quotes.get(f"{exch}:{row['tradingsymbol']}") if row else None
        if not q:
            return None
        if use_ltp:
            return Leg(strike, opt, row['tradingsymbol'], float(q['last_price']))
        if q.get('volume', 0) < cfg['min_volume'] or _ask(q) <= 0:
            return None
        return Leg(strike, opt, row['tradingsymbol'], _ask(q))

    atm_ce, atm_pe = (quotes.get(f"{exch}:{zopts[(atm, o)]['tradingsymbol']}", {}) for o in ('CE', 'PE'))
    if not use_ltp and not (atm_ce.get('volume') and atm_pe.get('volume')):
        raise RuntimeError(f'{index}: ATM {atm} has no volume today - market closed/holiday?')

    # Hedges already held on this expiry (bought on an earlier day) stay in every basket.
    by_token = {inst.token: key for key, inst in contracts.items()}
    held = []
    for p in _aliceblue_get('/positions'):
        key = by_token.get(int(p['instrumentId']))
        net = int(p.get('netQuantity', 0))
        if key and net > 0 and key in zopts:
            held.append(_margin_order(exch, zopts[key]['tradingsymbol'], 'BUY', net))
    straddle = [_margin_order(exch, zopts[(atm, o)]['tradingsymbol'], 'SELL', qty) for o in ('CE', 'PE')]
    base = straddle + held

    held_txt = ', '.join(f"{h['tradingsymbol']} +{h['quantity']}" for h in held) or 'none'
    extra_exposure = EXTRA_EXPOSURE_PCT / 100 * spot * qty * 2  # both short legs; not offset by hedges
    none_margin = _span_plus_exposure(base) + extra_exposure
    log.info(f'{index}: spot {spot:.2f} ATM {atm} expiry {zexpiry} qty {qty} | held hedges: {held_txt} | '
             f'margin with no new hedge Rs {none_margin:,.0f}'
             + (f' (incl. broker extra {EXTRA_EXPOSURE_PCT:g}% exposure Rs {extra_exposure:,.0f})' if extra_exposure else ''), extra=QUIET)
    cands = [Candidate(index, None, (), qty, 0.0, none_margin)]
    for d in distances:
        ce, pe = price(atm + d, 'CE'), price(atm - d, 'PE')
        if not ce or not pe:
            continue
        margin = _span_plus_exposure(base + [_margin_order(exch, leg.zsym, 'BUY', qty) for leg in (ce, pe)]) + extra_exposure
        cost = (ce.ask + pe.ask) * qty
        cands.append(Candidate(index, d, (ce, pe), qty, cost, margin))
        log.info(f'  {index} +-{d}: {ce.zsym} {ce.ask} + {pe.zsym} {pe.ask} | cost Rs {cost:,.0f} | margin Rs {margin:,.0f}', extra=QUIET)
        if margin >= 0.98 * none_margin:  # farther out buys no meaningful relief
            break
    if len(cands) == 1 and not held:
        # No tradable strangle at all - a data/market problem, not a real "no hedge needed". Buying the
        # other index's hedge alone would be planned against a wrong budget, so stop everything.
        raise RuntimeError(f'{index}: no hedge strikes with a live ask and volume >= {cfg["min_volume"]:,} - '
                           f'NOT buying any hedges, buy them manually')
    atm_prem = (float(atm_ce.get('last_price') or 0) + float(atm_pe.get('last_price') or 0)) / 2
    return cands, contracts, lot_size, atm_prem


def _spare_target(vix):
    return SPARE_BASE + SPARE_PER_VIX_POINT * max(0.0, vix - SPARE_VIX_FLOOR)


def build_plan(use_ltp=False):
    vix = float(_zerodha_quotes([VIX_INSTRUMENT])[VIX_INSTRUMENT]['last_price'])
    funds = _available_funds()
    per_index = {index: _plan_index(index, cfg, use_ltp) for index, cfg in CFG.items()}

    sensex_lot = per_index['SENSEX'][2]
    option_buying_reserve = OPTION_BUYING_LOTS * sensex_lot * per_index['SENSEX'][3]
    extra = ALICEBLUE_EXTRA_PER_LOT * sum(cfg['lots'] for cfg in CFG.values())
    spare = _spare_target(vix)
    straddle_budget = funds - extra - option_buying_reserve - spare

    combos = [(n, s) for n in per_index['NIFTY'][0] for s in per_index['SENSEX'][0]
              if n.cost + s.cost <= MAX_TOTAL_HEDGE_COST]
    cheapest = lambda cs: min(cs, key=lambda c: (round(c[0].cost + c[1].cost), c[0].margin + c[1].margin))
    fits = [c for c in combos if c[0].margin + c[1].margin <= straddle_budget]
    fits_min = [c for c in combos if c[0].margin + c[1].margin <= straddle_budget + spare - SPARE_MIN]
    if fits:
        chosen, shortfall = cheapest(fits), None
    elif fits_min:  # full spare out of reach - cheapest set that still leaves SPARE_MIN
        chosen, shortfall = cheapest(fits_min), 'target'
    else:  # not even SPARE_MIN - the most margin relief the cost cap allows
        chosen, shortfall = min(combos, key=lambda c: c[0].margin + c[1].margin), 'min'

    used = sum(c.margin for c in chosen) + extra + option_buying_reserve
    return dict(
        vix=vix, funds=funds, spare_target=spare, extra=extra, option_buying_reserve=option_buying_reserve,
        chosen=chosen, used=used, spare=funds - used, shortfall=shortfall,
        contracts={index: per_index[index][1] for index in CFG},
    )


def plan_text(plan):
    lines = [f"{'[DRY RUN] ' if DRY_RUN else ''}Hedge plan - VIX {plan['vix']:.2f}, funds Rs {plan['funds']:,.0f}"]
    for c in plan['chosen']:
        if c.distance is None:
            lines.append(f'{c.index}: buy nothing new (held hedges do the job) - margin Rs {c.margin:,.0f}')
        else:
            ce, pe = c.legs
            lines.append(f'{c.index}: BUY {ce.zsym} @ {ce.ask} + {pe.zsym} @ {pe.ask} x {c.qty} '
                         f'(+-{c.distance}) - cost Rs {c.cost:,.0f}, margin Rs {c.margin:,.0f}')
    lines.append(f"Total hedge cost Rs {sum(c.cost for c in plan['chosen']):,.0f} | est. margin used Rs {plan['used']:,.0f} "
                 f"(incl. AliceBlue extra Rs {plan['extra']:,.0f} + option buying Rs {plan['option_buying_reserve']:,.0f})")
    lines.append(f"Spare Rs {plan['spare']:,.0f} (target Rs {plan['spare_target']:,.0f} at this VIX)")
    if plan['shortfall'] == 'target':
        lines.append(f"WARNING: target spare not reachable within the Rs {MAX_TOTAL_HEDGE_COST:,} cap - took the cheapest "
                     f"set leaving at least Rs {SPARE_MIN:,}. Watch margin today.")
    elif plan['shortfall'] == 'min':
        lines.append(f'WARNING: not even Rs {SPARE_MIN:,} spare is reachable within the Rs {MAX_TOTAL_HEDGE_COST:,} cap - '
                     f'took the most margin relief the cap allows. Add funds or trade fewer lots.')
    return '\n'.join(lines)


# ── State file (what was bought / sold today) ───────────────────────────────────────────────────
def _load_state():
    try:
        with open(STATE_FILE) as f:
            state = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        state = None
    if not state or state.get('date') != str(_today()):
        state = {'date': str(_today()), 'buys': [], 'positions_opened': {}, 'sells': []}
    return state


def _save_state(state):
    tmp = STATE_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f, indent=1)
    os.replace(tmp, STATE_FILE)  # atomic - a crash mid-write never leaves a half-written file


def _instrument_from(rec):
    return Instrument(rec['token'], rec['name'], rec['lot_size'], rec['tick_size'], rec['exchange'])


def _record(index, opt, inst, zkey, side, qty, price, tag, order_no):
    return {
        'index': index, 'opt': opt, 'name': inst.name, 'token': inst.token, 'exchange': inst.exchange, 'lot_size': inst.lot_size,
        'tick_size': inst.tick_size, 'zkey': zkey, 'side': side, 'qty': qty, 'limit_price': price, 'tag': tag,
        'order_no': order_no, 'status': 'open', 'fill_price': None, 'placed_at': datetime.now().strftime('%H:%M:%S'),
        'filled_at': None,
    }


# ── Ordering ────────────────────────────────────────────────────────────────────────────────────
def _round_up_to_tick(price, tick):
    return round(math.ceil(round(price / tick, 6)) * tick, 2)


def _round_down_to_tick(price, tick):
    return round(max(math.floor(round(price / tick, 6)) * tick, tick), 2)


def _bid(quote):
    buys = (quote or {}).get('depth', {}).get('buy', [])
    return float(buys[0]['price']) if buys and buys[0]['price'] > 0 and buys[0]['quantity'] > 0 else 0.0


def _limit_price(side, ref, offset, tick):
    """BUY: above the ask, SELL: below the bid - by `offset`, on the tick grid."""
    return _round_up_to_tick(ref * (1 + offset), tick) if side == 'BUY' else _round_down_to_tick(ref * (1 - offset), tick)


def _place(side, inst, qty, price, tag):
    payload = [{
        'exchange': inst.exchange, 'instrumentId': str(inst.token), 'transactionType': side, 'quantity': qty,
        'product': ALICEBLUE_PRODUCT, 'orderComplexity': 'REGULAR', 'orderType': 'LIMIT', 'validity': 'DAY',
        'price': str(price), 'slTriggerPrice': '', 'orderTag': tag,
    }]
    result = _aliceblue_post('/orders/placeorder', payload)
    order = result[0] if isinstance(result, list) and len(result) == 1 else result
    order_no = order.get('brokerOrderId') if isinstance(order, dict) else None
    if not order_no:
        raise RuntimeError(f'{inst.name}: order not accepted: {order}')
    return order_no


def _modify_price(order_no, qty, price):
    _aliceblue_post('/orders/modify', {
        'brokerOrderId': order_no, 'quantity': qty, 'orderType': 'LIMIT', 'price': str(price),
        'slTriggerPrice': '', 'validity': 'DAY',
    })


def _already_ran_today():
    """Any non-cancelled/rejected BUY order already carrying our tag today -> a previous run got this far."""
    for o in _aliceblue_get('/orders/book'):
        if _tag_of(o).lower().startswith(ORDER_TAG_PREFIX) and _status(o) not in {'cancelled', 'rejected'}:
            return True
    return False


def _slices(qty, freeze_qty, lot_size):
    max_slice = (freeze_qty // lot_size) * lot_size
    out = []
    while qty > 0:
        out.append(min(qty, max_slice))
        qty -= out[-1]
    return out


def _chase(state, live):
    """Watch `live` (order_no -> state record) until each fills, repricing unfilled ones more
    aggressively every CHASE_WAIT_SECONDS (BUY up from the ask, SELL down from the bid, capped at
    CHASE_MAX_PCT). Every status change is written to the state file as it happens."""
    meta = {n: {'offset': ENTRY_OFFSET_PCT, 'last_change': time_module.time()} for n in live}
    deadline = time_module.time() + CHASE_TIMEOUT_SECONDS
    while live and time_module.time() < deadline:
        time_module.sleep(BROKER_FETCH_GAP_SECONDS)
        try:
            book = {o.get('brokerOrderId'): o for o in _aliceblue_get('/orders/book')}
        except Exception as exc:
            log.warning(f'order book read failed ({exc}) - retrying', extra=QUIET)
            continue
        stale = []
        for order_no, rec in list(live.items()):
            entry = book.get(order_no)
            status = _status(entry) if entry else ''
            if status in TERMINAL_ORDER_STATUSES:
                rec['status'] = status
                if status == 'complete':
                    rec['fill_price'] = float(entry.get('averageTradedPrice') or rec['limit_price'])
                    rec['filled_at'] = datetime.now().strftime('%H:%M:%S')
                else:
                    log.error(f"{rec['name']}: {rec['side']} order {order_no} {status}: {entry.get('rejectionReason', '')} - "
                              f'do this leg manually')
                del live[order_no]
                _save_state(state)
            elif time_module.time() - meta[order_no]['last_change'] >= CHASE_WAIT_SECONDS and meta[order_no]['offset'] < CHASE_MAX_PCT:
                stale.append(order_no)
        if not stale:
            continue
        try:
            fresh = _zerodha_quotes(sorted({live[n]['zkey'] for n in stale}))
        except Exception as exc:
            log.warning(f'quote refresh failed ({exc}) - retrying', extra=QUIET)
            continue
        for order_no in stale:
            rec, m = live[order_no], meta[order_no]
            buy = rec['side'] == 'BUY'
            ref = (_ask if buy else _bid)(fresh.get(rec['zkey'])) or rec['limit_price']
            m['offset'] = min(m['offset'] + CHASE_STEP_PCT, CHASE_MAX_PCT)
            price = _limit_price(rec['side'], ref, m['offset'], rec['tick_size'])
            price = max(price, rec['limit_price']) if buy else min(price, rec['limit_price'])  # only ever more aggressive
            m['last_change'] = time_module.time()
            if price == rec['limit_price']:
                continue
            try:
                _modify_price(order_no, rec['qty'], price)
                log.info(f"{rec['name']}: not filled - repriced {rec['limit_price']} -> {price} ({'ask' if buy else 'bid'} {ref})", extra=QUIET)
                rec['limit_price'] = price
                _save_state(state)
            except Exception as exc:
                log.warning(f"{rec['name']}: reprice of {order_no} failed ({exc})", extra=QUIET)

    for order_no, rec in live.items():
        log.error(f"{rec['name']}: {rec['side']} order {order_no} still unfilled at {rec['limit_price']} after "
                  f'{CHASE_TIMEOUT_SECONDS}s - left resting, check it manually')


def _net_qty_by_token():
    return {int(p['instrumentId']): int(p.get('netQuantity', 0)) for p in _aliceblue_get('/positions')}


def place_plan(plan):
    state = _load_state()
    tag_time = datetime.now().strftime('%H%M')
    live = {}
    for c in plan['chosen']:
        if c.distance is None:
            continue
        cfg = CFG[c.index]
        for leg in c.legs:
            inst = plan['contracts'][c.index][(leg.strike, leg.opt)]
            tag = f'{ORDER_TAG_PREFIX}{c.index[0]}{leg.opt[0]}{tag_time}'
            zkey = f"{cfg['zerodha_exchange']}:{leg.zsym}"
            for qty in _slices(c.qty, cfg['freeze_qty'], inst.lot_size):
                price = _limit_price('BUY', leg.ask, ENTRY_OFFSET_PCT, inst.tick_size)
                if DRY_RUN:
                    log.info(f'[DRY RUN] BUY {qty} x {inst.name} LIMIT @ {price} tag {tag}')
                    continue
                try:
                    order_no = _place('BUY', inst, qty, price, tag)
                except Exception as exc:
                    log.error(f'{inst.name}: BUY {qty} failed to place ({exc}) - buy this hedge manually')
                    continue
                rec = _record(c.index, leg.opt, inst, zkey, 'BUY', qty, price, tag, order_no)
                state['buys'].append(rec)
                _save_state(state)  # recorded the moment it exists, before waiting on the fill
                live[order_no] = rec
                log.info(f'BUY {qty} x {inst.name} LIMIT @ {price} (ask {leg.ask}) - order {order_no}', extra=QUIET)
    if not live:
        return
    _chase(state, live)

    filled = [r for r in state['buys'] if r['status'] == 'complete']
    try:
        net = _net_qty_by_token()
        state['positions_opened'] = {r['name']: net.get(r['token'], 0) for r in filled}
        _save_state(state)
    except Exception as exc:
        log.warning(f'positions read after buying failed ({exc})', extra=QUIET)
    if filled:
        cost = sum(r['qty'] * r['fill_price'] for r in filled)
        log.info('Hedges filled:\n' + '\n'.join(f"{r['name']} x{r['qty']} @ {r['fill_price']}" for r in filled)
                 + f'\nTotal Rs {cost:,.0f} | positions now: '
                 + ', '.join(f'{k} {v:+d}' for k, v in state['positions_opened'].items()))


# ── Closing at 15:15 ────────────────────────────────────────────────────────────────────────────
def _shorts_open(index):
    """This index's weekly option positions that are still SHORT (a straddle leg not yet closed)."""
    tokens = {inst.token: inst.name for inst in _aliceblue_weekly_contracts(index, CFG[index])[0].values()}
    return [f'{tokens[t]} {q}' for t, q in _net_qty_by_token().items() if t in tokens and q < 0]


def close_positions():
    """Sell back exactly what this morning's run bought (never more than is still held), once that
    index's straddle is flat - selling the hedge under a still-open short would leave it naked."""
    state = _load_state()
    # Morning buy orders that never filled: cancel, so they can't fill now that the day is ending.
    try:
        book = {o.get('brokerOrderId'): o for o in _aliceblue_get('/orders/book')}
    except Exception as exc:
        book = {}
        log.warning(f'order book read failed ({exc}) - skipping the unfilled-buy check', extra=QUIET)
    for rec in state['buys']:
        entry = book.get(rec['order_no'])
        if rec['status'] != 'complete' and entry and _status(entry) not in TERMINAL_ORDER_STATUSES:
            try:
                _aliceblue_post('/orders/cancel', {'brokerOrderId': rec['order_no']})
                log.info(f"{rec['name']}: cancelled unfilled morning BUY {rec['order_no']}")
            except Exception as exc:
                log.error(f"{rec['name']}: could not cancel unfilled BUY {rec['order_no']} ({exc}) - cancel it manually")
        elif entry and _status(entry) == 'complete' and rec['status'] != 'complete':  # filled after the chase gave up
            rec['status'], rec['fill_price'] = 'complete', float(entry.get('averageTradedPrice') or rec['limit_price'])
    _save_state(state)

    bought = {}  # token -> (record, total qty)
    for rec in state['buys']:
        if rec['status'] == 'complete':
            prev = bought.get(rec['token'], (rec, 0))
            bought[rec['token']] = (rec, prev[1] + rec['qty'])
    if not bought:
        log.info('hedge close: nothing bought today - nothing to close')
        return

    _wait_until(CLOSE_TIME)
    tag_time = datetime.now().strftime('%H%M')
    live = {}
    for index in CFG:
        legs = [(rec, qty) for rec, qty in bought.values() if rec['index'] == index]
        if not legs:
            continue
        warned = False
        while True:
            shorts = _shorts_open(index)
            if not shorts:
                break
            if datetime.now().time() >= CLOSE_GIVE_UP:
                log.error(f"{index}: straddle still short at {CLOSE_GIVE_UP:%H:%M} ({', '.join(shorts)}) - NOT selling its "
                          f"hedges ({', '.join(r['name'] for r, _ in legs)}). Close the straddle, then the hedges, manually.")
                break
            if not warned:
                log.warning(f"{index}: straddle still short ({', '.join(shorts)}) - holding its hedges until it's flat")
                warned = True
            time_module.sleep(10)
        if shorts:
            continue

        net = _net_qty_by_token()
        quotes = _zerodha_quotes([rec['zkey'] for rec, _ in legs])
        for rec, qty in legs:
            inst = _instrument_from(rec)
            to_sell = min(qty, max(net.get(inst.token, 0), 0))
            if to_sell <= 0:
                log.info(f'{inst.name}: already closed - nothing to sell')
                continue
            bid = _bid(quotes.get(rec['zkey']))
            price = _limit_price('SELL', bid, 0.0, inst.tick_size) if bid else inst.tick_size
            tag = f"{CLOSE_TAG_PREFIX}{index[0]}{rec['opt'][0]}{tag_time}"
            for q in _slices(to_sell, CFG[index]['freeze_qty'], inst.lot_size):
                if DRY_RUN:
                    log.info(f'[DRY RUN] SELL {q} x {inst.name} LIMIT @ {price} (bid {bid}) tag {tag}')
                    continue
                try:
                    order_no = _place('SELL', inst, q, price, tag)
                except Exception as exc:
                    log.error(f'{inst.name}: SELL {q} failed to place ({exc}) - close this hedge manually')
                    continue
                sell = _record(index, rec['opt'], inst, rec['zkey'], 'SELL', q, price, tag, order_no)
                state['sells'].append(sell)
                _save_state(state)
                live[order_no] = sell
                log.info(f'SELL {q} x {inst.name} LIMIT @ {price} (bid {bid}) - order {order_no}', extra=QUIET)
    if live:
        _chase(state, live)

    sold = [r for r in state['sells'] if r['status'] == 'complete']
    if sold:
        paid = sum(r['qty'] * r['fill_price'] for r in state['buys'] if r['status'] == 'complete')
        got = sum(r['qty'] * r['fill_price'] for r in sold)
        log.info('Hedges closed:\n' + '\n'.join(f"{r['name']} x{r['qty']} @ {r['fill_price']}" for r in sold)
                 + f'\nBought Rs {paid:,.0f}, sold Rs {got:,.0f} -> hedge cost today Rs {paid - got:,.0f}')


def main():
    ap = argparse.ArgumentParser(description='Pick and buy the morning straddle hedges; --close sells them at 15:15.')
    ap.add_argument('--plan', action='store_true', help='print the picks only, place nothing')
    ap.add_argument('--use-ltp', action='store_true', help='price off LTP with no liquidity filter (with --plan, outside market hours)')
    ap.add_argument('--close', action='store_true', help='at 15:15, sell back the hedges this morning\'s run bought')
    args = ap.parse_args()
    if args.use_ltp and not args.plan:
        ap.error('--use-ltp is for --plan only - never trade off LTP')

    mode = 'close' if args.close else 'plan' if args.plan else 'buy'
    log.info(f"hedge_buyer {mode} starting (DRY_RUN={DRY_RUN}, lots NIFTY {CFG['NIFTY']['lots']} / SENSEX {CFG['SENSEX']['lots']})")
    if not _wait_for_logins(args.plan):
        return
    if args.close:
        close_positions()
        return
    if not args.plan and _already_ran_today():
        log.warning('hedge orders already placed today (tagged hg...) - not buying again. Check positions if unsure.')
        return

    plan = build_plan(use_ltp=args.use_ltp)
    log.info(plan_text(plan))
    if args.plan:
        return

    _wait_until(PLACE_TIME)
    if datetime.now().time() > LATEST_PLACE_TIME:
        log.error(f'past {LATEST_PLACE_TIME:%H:%M:%S} - straddles are about to enter, NOT buying hedges. Buy them manually.')
        return
    place_plan(plan)


if __name__ == '__main__':
    main()
