"""
Shared plumbing for the stock_* programs: IST clock, logging (file + stdout + Telegram for
WARNING+), and alert() for events worth a Telegram ping. No broker or data-vendor code here -
see stock_data_zerodha.py (data layer) and stock_broker_aliceblue.py (execution layer).

Times are always IST wall-clock via now_ist(), never the host's local time - the EC2 box these
run on may not be set to IST.
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(HERE, '.env'))

IST = timezone(timedelta(hours=5, minutes=30))
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_TIMEOUT = 10


def now_ist():
    """Current IST wall-clock time as a naive datetime (comparable with the candle timestamps)."""
    return datetime.now(IST).replace(tzinfo=None)


def _telegram_send(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage',
            json={'chat_id': TELEGRAM_CHAT_ID, 'text': message},
            timeout=TELEGRAM_TIMEOUT,
        )
    except Exception as exc:  # never let a Telegram hiccup break the strategy
        print(f'Telegram alert failed: {exc}', file=sys.stderr)


class _TelegramHandler(logging.Handler):
    """Pushes WARNING+ records to Telegram, except ones alert() already sent (`_alerted`) and ones
    marked extra={'no_telegram': True} (throttled repeats)."""

    def emit(self, record):
        try:
            if getattr(record, '_alerted', False) or getattr(record, 'no_telegram', False):
                return
            _telegram_send(f'[{record.levelname}] {self.format(record)}')
        except Exception:
            self.handleError(record)


def get_logger(name, log_file, prefix):
    log = logging.getLogger(name)
    if log.handlers:
        return log
    log.setLevel(logging.INFO)
    log.propagate = False
    fmt = logging.Formatter(f'%(asctime)s %(levelname)s [{prefix}] %(message)s')
    for handler in (logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)):
        handler.setFormatter(fmt)
        log.addHandler(handler)
    tg = _TelegramHandler(level=logging.WARNING)
    tg.setFormatter(logging.Formatter('%(message)s'))
    log.addHandler(tg)
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning('TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set in .env - Telegram alerts disabled')
    return log


def alert(log, message, level=logging.INFO):
    """Log + push to Telegram exactly once - for events the user wants pinged about."""
    try:
        log.log(level, message, extra={'_alerted': True})
    except Exception as exc:
        print(f'alert() logging failed: {exc}', file=sys.stderr)
    _telegram_send(message)
