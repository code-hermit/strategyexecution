"""Health dashboard for the live strategies, run on the Mac - no broker or server login needed.

Every 10 minutes (on the :x0:30 mark, 09:40-15:31 IST, weekdays) it scp's the hedge buyer, NIFTY/SENSEX
straddle and SENSEX option-buying logs into logs/<name>_<today>.log, replays today's lines, and shows a
green tick or a red cross per strategy. Red = act now: hedges not filled by 09:40, a short leg without
an SL, a process that went silent, an ERROR/CRITICAL line, a missing end-of-day square-off or hedge
close, or the server unreachable.
Red beeps every 20s until Acknowledged; a new issue beeps again. P&L is deliberately never shown.

Caveat: this only knows what the logs say - it cannot see the broker's order book. A log claiming
an SL exists is trusted.

Run:  caffeinate -i /usr/local/bin/python3.10 log_dashboard.py   (python3.12 here has no tkinter)
"""

import queue
import re
import subprocess
import threading
import tkinter as tk
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo('Asia/Kolkata')
KEY = '/Users/subhash/AWS/AWS/aws keys/zct.pem'  # same box/key as download_logs.sh
HOST = 'ec2-user@3.111.138.194'
REMOTE = '/home/ec2-user/trading'
LOGS_DIR = Path(__file__).parent / 'logs'

REFRESH_MINUTES = 10
CHECK_FROM, CHECK_UNTIL = dtime(9, 40), dtime(15, 31)
END_TIME = dtime(15, 13)  # both strategies' EXIT_TIME
END_GRACE = timedelta(minutes=7)  # 15:13 -> straddle's "done for" line
# Hedges normally fill ~09:38. 09:40 (not later) so the 09:40:30 check catches a miss before the 09:45 entry.
HEDGES_FILLED_BY = dtime(9, 40)
HEDGE_CLOSE_STARTED_BY = dtime(15, 19)  # hedge_closer cron runs 15:14
BUY_MAX_HOLD = timedelta(minutes=8)  # option buying holds 5 min (HOLD_MINUTES), then exits
BEEP_MS = 20_000
BEEP_SOUND = '/System/Library/Sounds/Sosumi.aiff'
BEEP_VOLUME = '6'  # afplay multiplier; >1 amplifies past the system's normal level
BEEP_REPEAT = 3  # plays per alarm, 0.7s apart
GLOW_MS = 400  # flash period of the red header/border while an issue is unacknowledged


@dataclass(frozen=True)
class Strategy:
    name: str
    remote_path: str  # relative to REMOTE
    kind: str  # 'hedge' | 'straddle' | 'buying'
    first_log_by: dtime  # cron start on the box + a few minutes
    max_silence: timedelta | None  # straddle heartbeats every 30 min; buying logs every minute; hedge exits


STRATEGIES = [  # display order
    Strategy('Hedges', 'logs/hedge_buyer.log', 'hedge', dtime(9, 40), None),
    Strategy('NIFTY straddle', 'exec_rsv_adjust_sl_NIFTY.log', 'straddle', dtime(9, 49), timedelta(minutes=40)),
    Strategy('SENSEX straddle', 'exec_rsv_adjust_sl_SENSEX.log', 'straddle', dtime(9, 49), timedelta(minutes=40)),
    Strategy('SENSEX option buying', 'logs/sensex_option_buying.log', 'buying', dtime(10, 19), timedelta(minutes=5)),
]

LINE = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ (\w+) (?:\[\w+\] )?(.*)$')

# Straddle leg events, replayed in order; the last one seen per leg (CE/PE) is its state.
LEG_RULES = [
    (re.compile(r'^ENTER (CE|PE) (\S+) '), 'SL ok'),  # always carries the SL order id
    (re.compile(r'^(CE|PE) (\S+): FILLED @ .* SL NOT placed'), 'NO SL'),
    (re.compile(r'^(CE|PE) (\S+): still short but its SL order'), 'NO SL'),
    (re.compile(r'^(CE|PE) (\S+): no resting SL order found to adopt'), 'NO SL'),
    (re.compile(r'^(CE|PE)(): chop reentry filled .* protective SL failed'), 'NO SL'),
    (re.compile(r'^CHOP REENTRY (CE|PE) (\S+) '), 'chop'),  # SL ok unless the line just before said it failed
    (re.compile(r'^EXIT (CE|PE) (\S+) '), 'flat'),
    (re.compile(r'^STOPLOSS FILLED: (CE|PE) (\S+) '), 'flat'),
]
HEARTBEAT_LEG = re.compile(r'(CE|PE) (\S+) @ ~[\d.]+ \[(\w+)\]')  # [stoploss] = SL resting, [entry] = none yet
HEARTBEAT_FLAT = re.compile(r'(CE|PE) flat')
BUY_OPEN = re.compile(r'^BUY \d+ x (\S+) LIMIT')
BUY_CLOSED = re.compile(r'^Closed .* leg \(')


def _at(now, t):
    return datetime.combine(now.date(), t, tzinfo=IST)


def todays_events(text, day):
    """[(time, level, message)] for `day`'s lines. Untimestamped lines (the heartbeat's legs: ...)
    are appended to the message they belong to."""
    events, keep = [], False
    for line in text.splitlines():
        m = LINE.match(line)
        if m:
            keep = m[1].startswith(day.isoformat())
            if keep:
                events.append((datetime.strptime(m[1], '%Y-%m-%d %H:%M:%S').replace(tzinfo=IST), m[2], m[3]))
        elif keep and events:
            t, level, msg = events[-1]
            events[-1] = (t, level, f'{msg}\n{line}')
    return events


def straddle_status(events, now):
    legs = {'CE': ('flat', ''), 'PE': ('flat', '')}
    finished = halted = False
    for _, _, msg in events:
        if msg.startswith('heartbeat:'):
            for opt, inst, state in HEARTBEAT_LEG.findall(msg):
                legs[opt] = ('SL ok' if state == 'stoploss' else 'NO SL', inst)
            for opt in HEARTBEAT_FLAT.findall(msg):
                legs[opt] = ('flat', '')
        elif 'DAILY LOSS LIMIT' in msg:
            halted = True
        elif msg.startswith('Rolling straddle') and ' done for ' in msg:
            finished = True
        for rule, state in LEG_RULES:
            m = rule.match(msg)
            if m:
                inst = m[2] or legs[m[1]][1]
                if state == 'chop':
                    state = 'NO SL' if legs[m[1]][0] == 'NO SL' else 'SL ok'
                legs[m[1]] = (state, inst)
                break
    red = [f'{opt} {inst}: NO SL - short position unprotected' for opt, (state, inst) in legs.items() if state == 'NO SL']
    if not finished and now >= _at(now, END_TIME) + END_GRACE:
        red.append('no "done for" line after 15:13 - square-off not confirmed, positions may be open')
    text = 'done for the day' if finished else ' · '.join(
        f'{opt} flat' if state == 'flat' else f'{opt} {inst} {state}' for opt, (state, inst) in legs.items())
    return red, ('halted (daily loss limit) - ' if halted else '') + text, finished


def buying_status(events, now):
    position = None
    for t, _, msg in events:
        if m := BUY_OPEN.match(msg):
            position = (t, m[1])
        elif BUY_CLOSED.match(msg):
            position = None
    if position is None:
        return [], 'flat', now >= _at(now, END_TIME)
    since, inst = position
    red = [f'{inst} bought {since:%H:%M}, still open (no "Closed" line) after {BUY_MAX_HOLD.seconds // 60} min '
           f'- exit may have failed'] if now - since > BUY_MAX_HOLD else []
    return red, f'holding {inst} since {since:%H:%M}', False


def hedge_status(events, now):
    bought = closed = None
    for t, _, msg in events:
        if msg.startswith('Hedges filled:'):
            bought = (t, len(msg.splitlines()) - 2)  # header, one line per leg, Total line
        elif msg.startswith('hedge_buyer close starting'):
            closed = closed or (t, 'closing since')
        elif msg.startswith('Hedges closed:') or 'already closed - nothing to sell' in msg:
            closed = (t, 'closed')
    red = []
    if bought is None and now >= _at(now, HEDGES_FILLED_BY):
        red.append(f'no "Hedges filled" line by {HEDGES_FILLED_BY:%H:%M} - hedges may not be in place for the 09:45 '
                   f'straddle entry (false alarm only if hedges were already held)')
    if closed is None and now >= _at(now, HEDGE_CLOSE_STARTED_BY):
        red.append(f'hedge close run did not start by {HEDGE_CLOSE_STARTED_BY:%H:%M} - hedges still held?')
    text = f'hedges bought {bought[0]:%H:%M} ({bought[1]} legs)' if bought else 'no hedges bought yet'
    if closed:
        text += f' · {closed[1]} {closed[0]:%H:%M}'
    return red, text, True  # the script exits after buying - a quiet log is normal


STATUS_FNS = {'hedge': hedge_status, 'straddle': straddle_status, 'buying': buying_status}


def check(strategy, events, now):
    """-> (red, amber, status). red = act now (beeps), amber = warnings worth a glance."""
    if now.weekday() >= 5:
        return [], [], 'weekend - not trading'
    if not events:
        late = now >= _at(now, strategy.first_log_by)
        return (['no log lines today - strategy did not start'] if late else []), [], 'waiting to start'
    red = [f'{t:%H:%M:%S} {level}: {msg.splitlines()[0][:220]}' for t, level, msg in events
           if level in ('ERROR', 'CRITICAL') and 'DAILY LOSS LIMIT' not in msg]
    amber = [f'{t:%H:%M:%S} {msg.splitlines()[0][:220]}' for t, level, msg in events if level == 'WARNING']
    state_red, status, finished = STATUS_FNS[strategy.kind](events, now)
    last = events[-1][0]
    if not finished and now < _at(now, END_TIME) and now - last > strategy.max_silence:
        state_red.append(f'log silent since {last:%H:%M} - process may have died')
    return red + state_red, amber, f'{status}   (last log {last:%H:%M})'


def download(strategy, day):
    """scp the live log to logs/<name>_<day>.log (download_logs.sh's naming) and return its text."""
    dest = LOGS_DIR / f'{Path(strategy.remote_path).stem}_{day}.log'
    part = dest.with_name(dest.name + '.part')
    subprocess.run(['scp', '-q', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15', '-i', KEY,
                    f'{HOST}:{REMOTE}/{strategy.remote_path}', str(part)],
                   check=True, capture_output=True, text=True, timeout=120)
    part.replace(dest)
    return dest.read_text(errors='replace')


def run_checks():
    now = datetime.now(IST)
    results = []
    for s in STRATEGIES:
        if now.weekday() >= 5:
            results.append((s, *check(s, [], now)))
            continue
        try:
            text = download(s, now.date())
        except subprocess.CalledProcessError as exc:
            if 'No such file' in exc.stderr:  # box is up, strategy hasn't created its log yet
                results.append((s, *check(s, [], now)))
                continue
            results.append((s, [f'download failed - server unreachable? ({exc.stderr.strip()[-150:]})'], [], 'unknown'))
            continue
        except subprocess.TimeoutExpired:
            results.append((s, ['download timed out - server unreachable?'], [], 'unknown'))
            continue
        results.append((s, *check(s, todays_events(text, now.date()), now)))
    return now, results


def main():
    root = tk.Tk()
    root.title('Trading health')
    root.configure(padx=12, pady=12)
    header = tk.Label(root, font=('Helvetica', 26, 'bold'), padx=10, pady=6)
    header.pack(fill='x')
    info = tk.Label(root, anchor='w')
    info.pack(fill='x', pady=(4, 8))
    rows, frames = {}, {}
    for s in STRATEGIES:
        frame = tk.Frame(root, bd=1, relief='solid', padx=8, pady=6,
                         highlightthickness=5, highlightbackground=root.cget('bg'))
        frames[s.name] = frame
        frame.pack(fill='x', pady=4)
        widgets = [tk.Label(frame, anchor='w', justify='left', wraplength=820, font=font, fg=fg)
                   for font, fg in ((('Helvetica', 16, 'bold'), None), (('Helvetica', 13), None),
                                    (('Helvetica', 13, 'bold'), '#c00000'), (('Helvetica', 11), '#a05a00'),
                                    (('Menlo', 11), '#606060'))]
        for w in widgets:
            w.pack(fill='x')
        rows[s.name] = widgets
    ui = {'red': [], 'red_rows': set(), 'acked': set(), 'busy': False, 'checked': None,
          'history': {s.name: [] for s in STRATEGIES}}  # today's check results per strategy
    results_q = queue.Queue()

    def start_check():
        if ui['busy']:
            return
        ui['busy'] = True
        info.config(text='checking...')
        threading.Thread(target=lambda: results_q.put(run_checks()), daemon=True).start()

    def show(now, results):
        if ui['checked'] and ui['checked'].date() != now.date():
            ui['history'] = {s.name: [] for s in STRATEGIES}
        ui['checked'], ui['red'] = now, []
        ui['red_rows'] = {s.name for s, red, _, _ in results if red}
        for s, red, amber, status in results:
            title, status_w, red_w, amber_w, history_w = rows[s.name]
            ui['history'][s.name].append(f'{"✖" if red else "✔"} {now:%H:%M}')
            history_w.config(text='checks: ' + '  '.join(ui['history'][s.name]))
            title.config(text=f'{"✖" if red else "✔"}  {s.name}', fg='#c00000' if red else '#008000')
            status_w.config(text=status)
            red_w.config(text='\n'.join(f'• {r}' for r in red))
            amber_w.config(text=f'{len(amber)} warning(s), latest:\n' + '\n'.join(amber[-3:]) if amber else '')
            ui['red'] += [f'{s.name}: {r}' for r in red]
        refresh_header()
        if any(r not in ui['acked'] for r in ui['red']):
            root.deiconify()
            root.lift()
            root.attributes('-topmost', True)
            root.after(1000, lambda: root.attributes('-topmost', False))

    def refresh_header():
        unacked = [r for r in ui['red'] if r not in ui['acked']]
        text, bg = (('✖  ACTION NEEDED', '#c00000') if unacked else
                    ('⚠  issues acknowledged - still red below', '#d08000') if ui['red'] else ('✔  ALL OK', '#008000'))
        header.config(text=text, bg=bg, fg='white')
        checked = f'last checked {ui["checked"]:%H:%M:%S}' if ui['checked'] else 'not checked yet'
        info.config(text=f'{checked} IST · auto every {REFRESH_MINUTES} min, {CHECK_FROM:%H:%M}-{CHECK_UNTIL:%H:%M} weekdays')

    def poll():
        try:
            show(*results_q.get_nowait())
            ui['busy'] = False
        except queue.Empty:
            pass
        root.after(500, poll)

    def schedule():
        now = datetime.now(IST)
        if CHECK_FROM <= now.time() <= CHECK_UNTIL:
            start_check()
        secs_into_slot = (now.minute % REFRESH_MINUTES) * 60 + now.second
        root.after((REFRESH_MINUTES * 60 - secs_into_slot + 30) * 1000, schedule)

    def beep():
        if any(r not in ui['acked'] for r in ui['red']):
            for i in range(BEEP_REPEAT):
                root.after(i * 700, lambda: subprocess.Popen(['afplay', '-v', BEEP_VOLUME, BEEP_SOUND]))
            root.bell()
        root.after(BEEP_MS, beep)

    def glow(on=True):
        unacked = any(r not in ui['acked'] for r in ui['red'])
        for name, frame in frames.items():
            lit = name in ui['red_rows'] and (on or not unacked)  # flashes until acked, then steady red
            frame.config(highlightbackground='#ff2020' if lit else root.cget('bg'))
        if unacked:
            header.config(bg='#ff2020' if on else '#6a0000')
        root.after(GLOW_MS, glow, not on)

    def acknowledge():
        ui['acked'] |= set(ui['red'])
        refresh_header()

    buttons = tk.Frame(root)
    buttons.pack(fill='x', pady=(8, 0))
    tk.Button(buttons, text='Check now', command=start_check).pack(side='left')
    tk.Button(buttons, text='Acknowledge (stop beeping)', command=acknowledge).pack(side='left', padx=8)

    refresh_header()
    schedule()  # checks now if within hours (outside, the box may be off - use "Check now")
    root.after(500, poll)
    root.after(BEEP_MS, beep)
    glow()
    root.mainloop()


if __name__ == '__main__':
    main()
