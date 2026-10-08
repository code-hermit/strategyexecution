"""
EXECUTION LAYER for the stock_* programs - Finvasia / Shoonya (NorenOMS REST, OAuth) order placement
only. No market data from here (that's stock_data_zerodha.py) and no strategy logic.

Same interface as stock_broker_aliceblue.py, so the runner never knows which one it has:
  FinvasiaBroker - real orders, NSE cash market, INTRADAY (MIS, prd 'I') product.
  PaperBroker    - DRY_RUN stand-in (the AliceBlue module's paper broker, with Shoonya's contracts).

Interface:
  instruments(symbols)  -> {symbol: Instrument(symbol, token, trading_symbol, tick)}
  place(instrument, side, qty, order_type, price=None, trigger=None, tag='', ref_price=None) -> order id
                           order_type: 'SL' (stop-limit), 'LIMIT' or 'MARKET'
  modify(order_id, instrument, qty, order_type, price, trigger)
  cancel(order_id)
  orders()              -> {order_id: {'status', 'qty', 'filled_qty', 'avg_price', 'reason'}}
                           status is normalised to pending / complete / rejected / cancelled

Finvasia auth: execution/finvasia_token.json (written by finvasia_token_generation.py, once a day).
Every call is a form POST 'jData=<json>' with the OAuth access token as a Bearer header.
"""

import json
import os
import threading
import time as time_module
import zipfile
from io import BytesIO
from urllib.parse import quote_plus

import requests

from stock_common import HERE
from stock_broker_aliceblue import Instrument, PaperBroker as _PaperBroker

FINVASIA_TOKEN_FILE = os.path.join(HERE, 'finvasia_token.json')
FINVASIA_BASE_URL = 'https://api.shoonya.com/NorenWClientAPI'
FINVASIA_CONTRACT_MASTER_URL = 'https://api.shoonya.com/NSE_symbols.txt.zip'
FINVASIA_PRODUCT = 'I'              # INTRADAY (MIS) - Finvasia squares these off itself as a backstop
REQUEST_TIMEOUT = 10
ORDER_BOOK_MIN_GAP_SECONDS = 2.0
_ORDER_TYPES = {'SL': 'SL-LMT', 'LIMIT': 'LMT', 'MARKET': 'MKT'}


def _normalise_status(raw):
    s = str(raw or '').strip().lower()
    if s.startswith('cancel'):                  # Shoonya spells it 'CANCELED'
        return 'cancelled'
    if s in ('complete', 'rejected'):
        return s
    return 'pending'                            # OPEN, TRIGGER_PENDING, PENDING ...


def load_instruments(symbols):
    """NSE cash-market (EQ series) instruments from Shoonya's public contract master."""
    resp = requests.get(FINVASIA_CONTRACT_MASTER_URL, timeout=60)
    resp.raise_for_status()
    with zipfile.ZipFile(BytesIO(resp.content)) as z:
        lines = z.read(z.namelist()[0]).decode().splitlines()
    wanted = set(symbols)
    out = {}
    for line in lines[1:]:
        # Exchange,Token,LotSize,Symbol,TradingSymbol,Instrument,TickSize,
        f = line.split(',')
        if len(f) >= 7 and f[3] in wanted and f[5] == 'EQ':
            out[f[3]] = Instrument(f[3], int(f[1]), f[4], float(f[6]))
    return out


class FinvasiaBroker:
    def __init__(self, log):
        self.log = log
        self.access_token, self.uid, self.actid = self._valid_session()
        self._book_lock = threading.Lock()
        self._book, self._book_at = None, 0.0

    # ── auth / http ───────────────────────────────────────────────────────────────────────────
    def _raw_post(self, path, values, access_token):
        resp = requests.post(f'{FINVASIA_BASE_URL}/{path}', data='jData=' + json.dumps(values),
                             headers={'Authorization': f'Bearer {access_token}'}, timeout=REQUEST_TIMEOUT)
        if not resp.ok:                         # the rejection reason is in the body, keep it
            raise RuntimeError(f'Finvasia {path} failed ({resp.status_code}): {resp.text}')
        return resp.json()

    def _valid_session(self):
        if not os.path.exists(FINVASIA_TOKEN_FILE):
            raise RuntimeError('No Finvasia session - run finvasia_token_generation.py')
        with open(FINVASIA_TOKEN_FILE) as f:
            tok = json.load(f)
        access_token, uid, actid = tok['access_token'], tok['uid'], tok['actid']
        data = self._raw_post('Limits', {'uid': uid, 'actid': actid}, access_token)
        if not isinstance(data, dict) or data.get('stat') != 'Ok':
            raise RuntimeError(f'Finvasia session expired ({data}) - run finvasia_token_generation.py again')
        return access_token, uid, actid

    def _post(self, path, values):
        data = self._raw_post(path, {'ordersource': 'API', 'uid': self.uid, **values}, self.access_token)
        if isinstance(data, dict) and data.get('stat') != 'Ok':
            raise RuntimeError(f'Finvasia {path} failed: {data.get("emsg") or data}')
        return data

    # ── interface ─────────────────────────────────────────────────────────────────────────────
    def instruments(self, symbols):
        return load_instruments(symbols)

    def _order_values(self, instrument, qty, order_type, price, trigger):
        values = {
            'actid': self.actid,
            'exch': 'NSE',
            'tsym': quote_plus(instrument.trading_symbol),     # 'M&M-EQ' - the body is a raw form
            'qty': str(int(qty)),
            'prctyp': _ORDER_TYPES[order_type],
            'prc': str(price) if price is not None and order_type != 'MARKET' else '0',
        }
        if order_type == 'SL':
            values['trgprc'] = str(trigger)
        return values

    def place(self, instrument, side, qty, order_type, price=None, trigger=None, tag='', ref_price=None):
        values = self._order_values(instrument, qty, order_type, price, trigger)
        values.update(trantype='B' if side == 'BUY' else 'S', prd=FINVASIA_PRODUCT, dscqty='0',
                      ret='DAY', remarks=tag)
        order_id = self._post('PlaceOrder', values).get('norenordno')
        if not order_id:
            raise RuntimeError(f'Finvasia PlaceOrder returned no norenordno for {instrument.symbol}')
        return str(order_id)

    def modify(self, order_id, instrument, qty, order_type, price, trigger=None):
        values = self._order_values(instrument, qty, order_type, price, trigger)
        values.update(norenordno=str(order_id), ret='DAY')
        self._post('ModifyOrder', values)

    def cancel(self, order_id):
        self._post('CancelOrder', {'norenordno': str(order_id)})

    def orders(self, fresh=False):
        """Normalised order book, fetched at most once every ORDER_BOOK_MIN_GAP_SECONDS."""
        with self._book_lock:
            age = time_module.time() - self._book_at
            if self._book is None or fresh or age >= ORDER_BOOK_MIN_GAP_SECONDS:
                if age < ORDER_BOOK_MIN_GAP_SECONDS:
                    time_module.sleep(ORDER_BOOK_MIN_GAP_SECONDS - age)
                self._book_at = time_module.time()
                data = self._raw_post('OrderBook', {'ordersource': 'API', 'uid': self.uid}, self.access_token)
                if isinstance(data, dict):      # an empty book comes back as stat Not_Ok "no data"
                    if 'no data' not in str(data.get('emsg', '')).lower():
                        raise RuntimeError(f'Finvasia OrderBook failed: {data}')
                    data = []
                book = {}
                for o in data:
                    book[str(o.get('norenordno'))] = {
                        'status': _normalise_status(o.get('status')),
                        'qty': int(o.get('qty') or 0),
                        'filled_qty': int(o.get('fillshares') or 0),
                        'avg_price': float(o.get('avgprc') or 0),
                        'reason': o.get('rejreason') or '',
                    }
                self._book = book
            return dict(self._book)


class PaperBroker(_PaperBroker):
    """DRY_RUN broker - fills against the 1-minute candles; contracts from Shoonya's master."""

    def instruments(self, symbols):
        return load_instruments(symbols)
