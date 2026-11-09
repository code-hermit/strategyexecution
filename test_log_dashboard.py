"""Checks log_dashboard's verdicts against real downloaded logs in logs/. Run:
/usr/local/bin/python3.10 -m unittest test_log_dashboard -v"""

import unittest
from datetime import date, datetime
from pathlib import Path

from log_dashboard import IST, STRATEGIES, check, todays_events

LOGS = Path(__file__).parent / 'logs'
NIFTY, SENSEX, BUYING = STRATEGIES


def events(file, day, until=None):
    evs = todays_events((LOGS / file).read_text(), date.fromisoformat(day))
    return [e for e in evs if until is None or e[0] <= at(day, until)]


def at(day, hhmm):
    return datetime.fromisoformat(f'{day} {hhmm}').replace(tzinfo=IST)


class StraddleTests(unittest.TestCase):
    def test_clean_full_day_is_green(self):
        red, amber, status = check(SENSEX, events('exec_rsv_adjust_sl_SENSEX_2026-11-02.log', '2026-11-02'), at('2026-11-02', '15:25'))
        self.assertEqual(red, [])
        self.assertTrue(amber)  # the 12:12 EC993 failed-to-move-SL warning
        self.assertIn('done', status)

    def test_mid_session_shows_legs_with_sl(self):
        red, _, status = check(NIFTY, events('exec_rsv_adjust_sl_NIFTY_2026-11-05.log', '2026-11-05', '11:00:00'), at('2026-11-05', '11:00'))
        self.assertEqual(red, [])
        self.assertIn('CE NIFTY09NOV26C24350 SL ok', status)
        self.assertIn('PE NIFTY09NOV26P24350 SL ok', status)

    def test_sl_not_placed_is_red_until_retry(self):
        evs = events('exec_rsv_adjust_sl_NIFTY_2026-11-02.log', '2026-11-02', '09:45:18')
        red, _, _ = check(NIFTY, evs, at('2026-11-02', '09:46'))
        self.assertTrue(any('PE NIFTY03NOV26P23300: NO SL' in r for r in red))
        evs = events('exec_rsv_adjust_sl_NIFTY_2026-11-02.log', '2026-11-02', '09:46:00')
        red, _, _ = check(NIFTY, evs, at('2026-11-02', '09:46'))
        self.assertFalse(any('NO SL' in r for r in red))
        self.assertTrue(any('CRITICAL' in r for r in red))  # the alert itself stays until acknowledged

    def test_silent_log_is_red(self):
        evs = events('exec_rsv_adjust_sl_NIFTY_2026-11-05.log', '2026-11-05', '10:16:00')
        red, _, _ = check(NIFTY, evs, at('2026-11-05', '11:30'))
        self.assertTrue(any('silent' in r for r in red))

    def test_daily_loss_limit_halt_is_not_red(self):
        red, _, status = check(SENSEX, events('exec_rsv_adjust_sl_SENSEX_2026-11-06.log', '2026-11-06'), at('2026-11-06', '12:00'))
        self.assertEqual(red, [])
        self.assertIn('halted', status)

    def test_no_done_line_after_close_is_red(self):
        evs = events('exec_rsv_adjust_sl_NIFTY_2026-11-02.log', '2026-11-02', '15:13:03')
        red, _, _ = check(NIFTY, evs, at('2026-11-02', '15:25'))
        self.assertTrue(any('square-off' in r for r in red))

    def test_not_started_is_red(self):
        red, _, _ = check(NIFTY, [], at('2026-11-05', '09:55'))
        self.assertTrue(any('did not start' in r for r in red))
        red, _, _ = check(NIFTY, [], at('2026-11-05', '09:30'))
        self.assertEqual(red, [])


class BuyingTests(unittest.TestCase):
    def test_full_day_is_green(self):
        red, _, _ = check(BUYING, events('sensex_option_buying_2026-11-05.log', '2026-11-05'), at('2026-11-05', '14:16'))
        self.assertEqual(red, [])

    def test_open_position_within_hold_is_green(self):
        red, _, status = check(BUYING, events('sensex_option_buying_2026-11-05.log', '2026-11-05', '13:41:30'), at('2026-11-05', '13:41:30'))
        self.assertEqual(red, [])
        self.assertIn('SENSEX26N0579200CE', status)

    def test_position_stuck_open_is_red(self):
        red, _, _ = check(BUYING, events('sensex_option_buying_2026-11-05.log', '2026-11-05', '13:41:30'), at('2026-11-05', '13:50'))
        self.assertTrue(any('still open' in r for r in red))


if __name__ == '__main__':
    unittest.main()
