"""
EXECUTION LAYER for the stock_* programs - AliceBlue (REST, v3 open-api) order placement only. No
market data from here (that's stock_data_zerodha.py) and no strategy logic.

Two brokers with the same interface, so the runner never knows which one it has:
  AliceBlueBroker - real orders, NSE cash market, INTRADAY (MIS) product.
  PaperBroker     - DRY_RUN stand-in: keeps orders in memory and fills them against the 1-minute
                    candles the data layer downloads (simulate()), using the same fill rules as
                    the backtest. Nothing is sent to AliceBlue.

Interface:
  instruments(symbols)  -> {symbol: Instrument(token, trading_symbol, tick)}
  place(instrument, side, qty, order_type, price=None, trigger=None, tag='', ref_price=None) -> order id
                           order_type: 'SL' (stop-limit), 'LIMIT' or 'MARKET'
  modify(order_id, instrument, qty, order_type, price, trigger)
  cancel(order_id)
  orders()              -> {order_id: {'status', 'qty', 'filled_qty', 'avg_price', 'reason'}}
                           status is normalised to pending / complete / rejected / cancelled

AliceBlue auth: execution/aliceblue_token.json (written by aliceblue_token_generation.py).
"""

import json
import os
import threading
import time as time_module
from typing import NamedTuple

import requests

from stock_common import HERE

ALICEBLUE_TOKEN_FILE = os.path.join(HERE, 'aliceblue_token.json')
ALICEBLUE_BASE_URL = 'https://a3.aliceblueonline.com/open-api/od/v1'
ALICEBLUE_CONTRACT_MASTER_URL = 'https://v2api.aliceblueonline.com/restpy/static/contract_master/V2/NSE'
ALICEBLUE_PRODUCT = 'INTRADAY'      # MIS - AliceBlue squares these off itself ~15:20 as a backstop
REQUEST_TIMEOUT = 10
ORDER_BOOK_MIN_GAP_SECONDS = 2.0    # other strategies share this AliceBlue account - keep load low
_EMPTY_RESULT_STATUSES = {'EC920'}  # AliceBlue's "no data" answer for an empty order book


class Instrument(NamedTuple):
    symbol: str
    token: int
    trading_symbol: str
    tick: float


def _normalise_status(raw):
    s = str(raw or '').strip().lower()
    if s.startswith('cancel'):                  # AliceBlue also spells it 'CANCELED'
        return 'cancelled'
    if s in ('complete', 'rejected'):
        return s
    return 'pending'                            # open, trigger pending, after market order req ...


def load_instruments(symbols):
    """NSE cash-market (EQ series) instruments from AliceBlue's public contract master."""
    resp = requests.get(ALICEBLUE_CONTRACT_MASTER_URL, timeout=60)
    resp.raise_for_status()
    wanted = set(symbols)
    out = {}
    for c in resp.json()['NSE']:
        if c.get('symbol') in wanted and c.get('group_name') == 'EQ':
            out[c['symbol']] = Instrument(c['symbol'], int(c['token']), c['trading_symbol'], float(c['tick_size']))
    return out


class AliceBlueBroker:
    def __init__(self, log):
        self.log = log
        self.session = self._valid_session()
        self._book_lock = threading.Lock()
        self._book, self._book_at = None, 0.0

    # ── auth / http ───────────────────────────────────────────────────────────────────────────
    def _headers(self, session=None):
        return {'Authorization': f'Bearer {session or self.session}', 'Content-Type': 'application/json'}

    def _valid_session(self):
        if not os.path.exists(ALICEBLUE_TOKEN_FILE):
            raise RuntimeError('No AliceBlue session - run aliceblue_token_generation.py')
        with open(ALICEBLUE_TOKEN_FILE) as f:
            session = json.load(f)['userSession']
        resp = requests.get(f'{ALICEBLUE_BASE_URL}/limits/', headers=self._headers(session), timeout=REQUEST_TIMEOUT)
        if not resp.ok:
            raise RuntimeError('AliceBlue session expired - run aliceblue_token_generation.py again')
        return session

    @staticmethod
    def _result(path, data):
        if data.get('status') == 'Ok':
            return data['result']
        if data.get('status') in _EMPTY_RESULT_STATUSES:
            return []
        raise RuntimeError(f'AliceBlue {path} failed: {data}')

    def _get(self, path):
        resp = requests.get(ALICEBLUE_BASE_URL + path, headers=self._headers(), timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return self._result(path, resp.json())

    def _post(self, path, payload):
        resp = requests.post(ALICEBLUE_BASE_URL + path, json=payload, headers=self._headers(), timeout=REQUEST_TIMEOUT)
        if not resp.ok:                         # the rejection reason is in the body, keep it
            raise RuntimeError(f'AliceBlue POST {path} failed ({resp.status_code}): {resp.text}')
        return self._result(path, resp.json())

    # ── interface ─────────────────────────────────────────────────────────────────────────────
    def instruments(self, symbols):
        return load_instruments(symbols)

    def place(self, instrument, side, qty, order_type, price=None, trigger=None, tag='', ref_price=None):
        payload = [{
            'exchange': 'NSE',
            'instrumentId': str(instrument.token),
            'transactionType': side,
            'quantity': int(qty),
            'product': ALICEBLUE_PRODUCT,
            'orderComplexity': 'REGULAR',
            'orderType': order_type,
            'validity': 'DAY',
            'price': str(price) if price is not None else '0',
            'slTriggerPrice': str(trigger) if trigger is not None else '',
            'orderTag': tag,
        }]
        result = self._post('/orders/placeorder', payload)
        order = result[0] if isinstance(result, list) and result else result
        order_id = (order or {}).get('brokerOrderId')
        if not order_id:
            raise RuntimeError(f'AliceBlue placeorder returned no brokerOrderId: {result}')
        return str(order_id)

    def modify(self, order_id, instrument, qty, order_type, price, trigger=None):
        self._post('/orders/modify', {
            'brokerOrderId': order_id, 'quantity': int(qty), 'orderType': order_type,
            'price': str(price), 'slTriggerPrice': str(trigger) if trigger is not None else '', 'validity': 'DAY',
        })

    def cancel(self, order_id):
        self._post('/orders/cancel', {'brokerOrderId': order_id})

    def orders(self, fresh=False):
        """Normalised order book, fetched at most once every ORDER_BOOK_MIN_GAP_SECONDS."""
        with self._book_lock:
            age = time_module.time() - self._book_at
            if self._book is None or fresh or age >= ORDER_BOOK_MIN_GAP_SECONDS:
                if age < ORDER_BOOK_MIN_GAP_SECONDS:
                    time_module.sleep(ORDER_BOOK_MIN_GAP_SECONDS - age)
                self._book_at = time_module.time()
                book = {}
                for o in self._get('/orders/book'):
                    filled = o.get('filledQuantity') or o.get('Fillshares') or 0
                    book[str(o.get('brokerOrderId'))] = {
                        'status': _normalise_status(o.get('orderStatus')),
                        'qty': int(o.get('quantity') or 0),
                        'filled_qty': int(filled or 0),
                        'avg_price': float(o.get('averageTradedPrice') or 0),
                        'reason': o.get('rejectionReason') or '',
                    }
                self._book = book
            return dict(self._book)


class PaperBroker:
    """DRY_RUN broker: orders live in memory and are filled by simulate() against completed
    1-minute candles - the backtest's fill rules: an SL order triggers when a candle trades through
    its trigger and fills at the trigger (or the candle's open if it gapped past it), capped at its
    limit; a MARKET / marketable LIMIT fills at the reference price passed in at placement."""

    def __init__(self, log):
        self.log = log
        self._lock = threading.Lock()
        self._orders = {}
        self._next_id = 1

    def instruments(self, symbols):
        return load_instruments(symbols)

    def place(self, instrument, side, qty, order_type, price=None, trigger=None, tag='', ref_price=None):
        from stock_common import now_ist
        with self._lock:
            order_id = f'PAPER{self._next_id:05d}'
            self._next_id += 1
            o = dict(symbol=instrument.symbol, side=side, qty=int(qty), type=order_type, price=price,
                     trigger=trigger, tag=tag, status='pending', filled_qty=0, avg_price=0.0, reason='',
                     placed=now_ist().replace(second=0, microsecond=0))
            marketable = order_type == 'MARKET' or (
                order_type == 'LIMIT' and ref_price is not None
                and (ref_price <= price if side == 'BUY' else ref_price >= price))
            if marketable:
                if ref_price is None:
                    o.update(status='rejected', reason='paper: MARKET order needs ref_price')
                else:
                    o.update(status='complete', filled_qty=o['qty'], avg_price=float(ref_price))
            self._orders[order_id] = o
            return order_id

    def modify(self, order_id, instrument, qty, order_type, price, trigger=None):
        with self._lock:
            o = self._orders[order_id]
            if o['status'] != 'pending':
                raise RuntimeError(f'paper: cannot modify {order_id} ({o["status"]})')
            o.update(qty=int(qty), type=order_type, price=price, trigger=trigger)

    def cancel(self, order_id):
        with self._lock:
            o = self._orders[order_id]
            if o['status'] == 'pending':
                o['status'] = 'cancelled'

    def orders(self, fresh=False):
        with self._lock:
            return {k: {kk: o[kk] for kk in ('status', 'qty', 'filled_qty', 'avg_price', 'reason')}
                    for k, o in self._orders.items()}

    def simulate(self, new_bars):
        """Fill resting orders against newly completed candles {symbol: [Bar, ...]}."""
        with self._lock:
            for o in self._orders.values():
                if o['status'] != 'pending' or o['type'] not in ('SL', 'LIMIT'):
                    continue
                for bar in new_bars.get(o['symbol'], []):
                    if bar.ts < o['placed']:
                        continue
                    fill = self._fill_price(o, bar)
                    if fill is not None:
                        o.update(status='complete', filled_qty=o['qty'], avg_price=fill)
                        break

    @staticmethod
    def _fill_price(o, bar):
        buy = o['side'] == 'BUY'
        if o['type'] == 'SL' and o['trigger'] is not None:
            if (bar.high >= o['trigger']) if buy else (bar.low <= o['trigger']):
                px = max(bar.open, o['trigger']) if buy else min(bar.open, o['trigger'])
                o['type'] = 'LIMIT'             # triggered: now a resting limit order
                if (px <= o['price']) if buy else (px >= o['price']):
                    return px
                if (bar.low <= o['price']) if buy else (bar.high >= o['price']):
                    return o['price']
            return None
        if (bar.low <= o['price']) if buy else (bar.high >= o['price']):   # resting LIMIT
            return min(bar.open, o['price']) if buy else max(bar.open, o['price'])
        return None
