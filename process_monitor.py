"""
Watches the python strategy scripts that should be running on the trading box at any given
time (mirroring aws_lambda_ssm_runner.py's cron schedule) and fires a MacroDroid/Telegram
alarm (macdroid_alarm.raise_alarm) if one of them should be running right now but its
process isn't found - i.e. it crashed or was killed.

Checks are done via `pgrep -f <pattern>` against the raw process list, not tmux sessions -
so it catches a dead script whether it was launched via cron/tmux or run directly in a shell.
"""

import argparse
import subprocess
import time
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo

from macdroid_alarm import raise_alarm

IST = ZoneInfo("Asia/Kolkata")

# Mirrors the cron lines aws_lambda_ssm_runner.py provisions. weekdays: None = every day,
# else a set of Python weekday() values (Mon=0 ... Sun=6).
# "match" is an extended-regex pattern matched against the full command line (pgrep -f).
JOBS = [
    {
        "name": "zerodha_ticker",
        "match": r"zerodha_ticker_service\.py",
        "start": dtime(9, 42),
        "end": dtime(15, 30),
        "weekdays": {0, 1, 2, 3, 4},
    },
    {
        "name": "option_selling (NIFTY)",
        # No trailing SENSEX arg - anchored at end of line so this doesn't also match the
        # SENSEX invocation below.
        "match": r"exec_rsv_adjust_sl\.py$",
        "start": dtime(9, 44),
        "end": dtime(15, 30),
        "weekdays": {0, 1, 2, 3, 4},
    },
    {
        "name": "option_selling_sensex",
        "match": r"exec_rsv_adjust_sl\.py SENSEX",
        "start": dtime(9, 44),
        "end": dtime(15, 30),
        "weekdays": {0, 1, 2, 3, 4},
    },
    {
        "name": "sensex_buying",
        "match": r"sensex_option_buying\.py",
        "start": dtime(10, 14),
        "end": dtime(15, 30),
        "weekdays": None,
    },
]

# Give a freshly-cron-started process this long to actually appear in the process list
# before we treat it missing as a crash.
STARTUP_GRACE = timedelta(minutes=3)

# Once alarmed, keep nagging at this interval until the process comes back.
RENOTIFY_INTERVAL = timedelta(minutes=10)

DEFAULT_POLL_INTERVAL_SECONDS = 1


def process_alive(match_pattern):
    result = subprocess.run(
        ["pgrep", "-f", match_pattern],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def job_should_be_running(job, now):
    if job["weekdays"] is not None and now.weekday() not in job["weekdays"]:
        return False
    grace = job.get("grace", STARTUP_GRACE)
    window_start = datetime.combine(now.date(), job["start"], tzinfo=IST) + grace
    window_end = datetime.combine(now.date(), job["end"], tzinfo=IST)
    return window_start <= now <= window_end


def make_test_job(match_pattern, duration_minutes, grace_seconds):
    now = datetime.now(IST)
    end = now + timedelta(minutes=duration_minutes)
    return {
        "name": f"test: {match_pattern}",
        "match": match_pattern,
        "start": now.time(),
        "end": end.time(),
        "weekdays": {now.weekday()},
        "grace": timedelta(seconds=grace_seconds),
    }


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--test",
        metavar="PATTERN",
        help=(
            "Watch a single ad-hoc process instead of the real schedule, so the alarm path "
            "can be exercised by hand. PATTERN is matched with `pgrep -f`, so a script "
            "filename works, e.g. `--test test_strategy.py`. Start the script normally "
            "(`python3 test_strategy.py &` or in another terminal), leave this running, "
            "then kill it (`pkill -f test_strategy.py` or Ctrl+C the other terminal) and "
            "confirm the alarm fires."
        ),
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=15,
        help="Minutes the --test process is considered 'should be running' for (default: 15).",
    )
    parser.add_argument(
        "--grace-seconds",
        type=float,
        default=5,
        help="Startup grace for --test, in seconds, instead of the real 3-minute grace (default: 5).",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        help=f"Seconds between scans (default: {DEFAULT_POLL_INTERVAL_SECONDS}).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.test:
        jobs = [make_test_job(args.test, args.duration, args.grace_seconds)]
        print(
            f"TEST MODE: watching processes matching '{args.test}' for {args.duration} min "
            f"(grace {args.grace_seconds}s). Start it, then kill it, and watch for the alarm."
        )
    else:
        jobs = JOBS
        print("process_monitor started, watching:", ", ".join(j["name"] for j in jobs))

    last_alarmed_at = {job["name"]: None for job in jobs}

    while True:
        now = datetime.now(IST)

        for job in jobs:
            name = job["name"]

            if not job_should_be_running(job, now):
                last_alarmed_at[name] = None
                continue

            if process_alive(job["match"]):
                last_alarmed_at[name] = None
                continue

            last = last_alarmed_at[name]
            if last is not None and now - last < RENOTIFY_INTERVAL:
                continue

            message = (
                f"ALARM: '{name}' is not running but should be "
                f"(scheduled {job['start']}-{job['end']} IST)."
            )
            print(message)
            try:
                raise_alarm(message)
            except Exception as exc:
                print(f"failed to send alarm for {name}: {exc}")
            last_alarmed_at[name] = now

        time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()
