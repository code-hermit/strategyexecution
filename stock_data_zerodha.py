"""
DATA LAYER for the stock_* programs - Zerodha Kite Connect historical-data API only. No order
placement, no strategy logic.

ZerodhaMinuteData keeps today's completed 1-minute candles for a list of NSE stocks:
  bootstrap()  - downloads today's candles (09:15 -> now) for every stock, one by one.
  refresh()    - called once a minute: re-downloads each stock from its last stored candle to
                 now (the last stored candle is re-fetched too, in case Kite revised it) and
                 returns only the newly COMPLETED candles per stock.
  ltp()        - batch last-traded prices (/quote/ltp), used to sanity-check a level before an
                 order is placed.

Only completed minutes are ever stored: the candle for the minute still in progress (which Kite's
historical API does return) is dropped until that minute has ended.

Kite's historical endpoint is limited to 3 requests/second, so one pass over 99 stocks takes about
35 seconds; requests are paced at HIST_MIN_INTERVAL. A stock whose fetch fails just keeps its old
candles for that pass (logged) and catches up on the next one.

Auth: reads execution/zerodha_token.json (written by zerodha_generate_access_token.py) and
ZERODHA_API_KEY from .env. Needs the Kite Connect historical-data add-on.
"""

import csv
import io
import json
import os
import time as time_module
from datetime import datetime, timedelta
from typing import NamedTuple

import requests

from stock_common import HERE, now_ist

ZERODHA_API_KEY = os.getenv('ZERODHA_API_KEY')
ZERODHA_TOKEN_FILE = os.path.join(HERE, 'zerodha_token.json')
ZERODHA_BASE_URL = 'https://api.kite.trade'
REQUEST_TIMEOUT = 10
HIST_MIN_INTERVAL = 0.36            # seconds between historical calls (Kite limit: 3/s)
MAX_RETRIES = 3
LTP_BATCH = 200                     # instruments per /quote/ltp call


class Bar(NamedTuple):
    ts: datetime                    # candle start, IST, naive
    open: float
    high: float
    low: float
    close: float
    volume: int


class ZerodhaMinuteData:
    def __init__(self, symbols, log):
        self.log = log
        self.symbols = list(symbols)
        self.access_token = self._valid_token()
        self.tokens = self._load_instrument_tokens(self.symbols)
        self.symbols = [s for s in self.symbols if s in self.tokens]
        self.bars = {s: [] for s in self.symbols}
        self._last_call = 0.0

    # ── auth / instruments ────────────────────────────────────────────────────────────────────
    def _headers(self, token=None):
        return {'Authorization': f'token {ZERODHA_API_KEY}:{token or self.access_token}', 'X-Kite-Version': '3'}

    def _valid_token(self):
        if not os.path.exists(ZERODHA_TOKEN_FILE):
            raise RuntimeError('No Zerodha access token - run zerodha_generate_access_token.py')
        with open(ZERODHA_TOKEN_FILE) as f:
            token = json.load(f)['access_token']
        resp = requests.get(f'{ZERODHA_BASE_URL}/user/profile', headers=self._headers(token), timeout=REQUEST_TIMEOUT)
        if not resp.ok:
            raise RuntimeError('Zerodha access token expired - run zerodha_generate_access_token.py again')
        return token

    def _load_instrument_tokens(self, symbols):
        resp = requests.get(f'{ZERODHA_BASE_URL}/instruments/NSE', headers=self._headers(), timeout=30)
        resp.raise_for_status()
        wanted = set(symbols)
        tokens = {
            row['tradingsymbol']: int(row['instrument_token'])
            for row in csv.DictReader(io.StringIO(resp.text))
            if row['tradingsymbol'] in wanted and row['segment'] == 'NSE' and row['instrument_type'] == 'EQ'
        }
        missing = sorted(wanted - set(tokens))
        if missing:
            self.log.warning(f'Zerodha: no NSE EQ instrument for {missing} - these stocks are skipped')
        return tokens

    # ── HTTP with pacing / retries ────────────────────────────────────────────────────────────
    def _get(self, path, params):
        for attempt in range(1, MAX_RETRIES + 1):
            wait = self._last_call + HIST_MIN_INTERVAL - time_module.time()
            if wait > 0:
                time_module.sleep(wait)
            self._last_call = time_module.time()
            try:
                resp = requests.get(ZERODHA_BASE_URL + path, headers=self._headers(), params=params,
                                    timeout=REQUEST_TIMEOUT)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise RuntimeError(f'HTTP {resp.status_code}: {resp.text[:200]}')
                resp.raise_for_status()
                return resp.json()['data']
            except Exception as exc:
                if attempt == MAX_RETRIES:
                    raise
                time_module.sleep(0.5 * attempt)
                self.log.info(f'Zerodha {path} retry {attempt}: {exc}')

    def _fetch_candles(self, symbol, frm, to):
        data = self._get(f'/instruments/historical/{self.tokens[symbol]}/minute',
                         {'from': frm.strftime('%Y-%m-%d %H:%M:%S'), 'to': to.strftime('%Y-%m-%d %H:%M:%S')})
        bars = []
        for c in data.get('candles', []):
            ts = datetime.strptime(c[0][:19], '%Y-%m-%dT%H:%M:%S')    # '2026-10-06T09:15:00+0530'
            bars.append(Bar(ts, float(c[1]), float(c[2]), float(c[3]), float(c[4]), int(c[5] or 0)))
        return bars

    # ── public API ────────────────────────────────────────────────────────────────────────────
    @staticmethod
    def _completed(bars, now):
        cutoff = now.replace(second=0, microsecond=0)             # start of the minute in progress
        return [b for b in bars if b.ts < cutoff]

    def bootstrap(self):
        """Today's completed candles from 09:15 for every stock. Returns {symbol: bars}."""
        now = now_ist()
        start = now.replace(hour=9, minute=15, second=0, microsecond=0)
        failed = []
        for symbol in self.symbols:
            try:
                self.bars[symbol] = self._completed(self._fetch_candles(symbol, start, now), now)
            except Exception as exc:
                failed.append(symbol)
                self.log.warning(f'bootstrap {symbol} failed: {exc}', extra={'no_telegram': True})
        if failed:
            self.log.warning(f'bootstrap failed for {len(failed)} stocks (retried on refresh): {failed}')
        return {s: list(b) for s, b in self.bars.items()}

    def refresh(self):
        """Pull each stock forward to now. Returns {symbol: [newly completed bars]}."""
        now = now_ist()
        new = {}
        failures = 0
        for symbol in self.symbols:
            have = self.bars[symbol]
            frm = have[-1].ts if have else now.replace(hour=9, minute=15, second=0, microsecond=0)
            try:
                fetched = self._completed(self._fetch_candles(symbol, frm, now), now)
            except Exception as exc:
                failures += 1
                self.log.info(f'refresh {symbol} failed: {exc}')
                continue
            known = {b.ts for b in have}
            if have and fetched and fetched[0].ts == have[-1].ts:
                have[-1] = fetched[0]                              # Kite may revise the last candle
            added = [b for b in fetched if b.ts not in known]
            have.extend(added)
            if added:
                new[symbol] = added
        if failures:
            self.log.warning(f'refresh: {failures}/{len(self.symbols)} stocks failed this pass',
                             extra={'no_telegram': failures < 10})
        return new

    def ltp(self, symbols):
        """{symbol: last traded price} via /quote/ltp (batched)."""
        out = {}
        symbols = [s for s in symbols if s in self.tokens]
        for i in range(0, len(symbols), LTP_BATCH):
            keys = [f'NSE:{s}' for s in symbols[i:i + LTP_BATCH]]
            resp = requests.get(f'{ZERODHA_BASE_URL}/quote/ltp', headers=self._headers(),
                                params=[('i', k) for k in keys], timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            for key, v in resp.json()['data'].items():
                out[key.split(':', 1)[1]] = float(v['last_price'])
        return out

    def save(self, path):
        """Dump today's candles to CSV (symbol,ts,open,high,low,close,volume) for the record."""
        with open(path, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['symbol', 'ts', 'open', 'high', 'low', 'close', 'volume'])
            for symbol, bars in self.bars.items():
                for b in bars:
                    w.writerow([symbol, b.ts.strftime('%Y-%m-%d %H:%M'), b.open, b.high, b.low, b.close, b.volume])
