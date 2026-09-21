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


DEFAULT_TEST_PATTERN = "test_strategy.py"


def make_test_job(match_pattern):
    # No schedule: it arms itself the first time the process is seen running, then alarms
    # if that process later disappears. Nothing to configure time-wise.
    return {
        "name": f"test: {match_pattern}",
        "match": match_pattern,
        "track_mode": "seen",
    }


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--test",
        nargs="?",
        const=DEFAULT_TEST_PATTERN,
        default=None,
        metavar="PATTERN",
        help=(
            "Watch a single ad-hoc process instead of the real schedule, with no time "
            f"window: defaults to '{DEFAULT_TEST_PATTERN}' if given with no value. PATTERN "
            "is matched with `pgrep -f`, so a script filename works. It starts tracking as "
            "soon as it first sees the process running, then alarms if that process "
            "disappears - start the script (`python3 test_strategy.py`), confirm the "
            "monitor logs that it's now tracking it, then kill it and watch for the alarm."
        ),
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
        jobs = [make_test_job(args.test)]
        print(
            f"TEST MODE: watching for processes matching '{args.test}' (no time window). "
            "Start it, wait for tracking to arm, then kill it and watch for the alarm."
        )
    else:
        jobs = JOBS
        print("process_monitor started, watching:", ", ".join(j["name"] for j in jobs))

    alarmed = {job["name"]: False for job in jobs}
    armed = {job["name"]: False for job in jobs}

    while True:
        now = datetime.now(IST)

        for job in jobs:
            name = job["name"]
            alive = process_alive(job["match"])

            if job.get("track_mode") == "seen":
                if alive:
                    if not armed[name]:
                        armed[name] = True
                        print(f"'{name}' seen running - now tracking it.")
                    alarmed[name] = False
                    continue
                if not armed[name]:
                    continue  # never started yet, nothing to track
            else:
                if not job_should_be_running(job, now):
                    alarmed[name] = False
                    continue
                if alive:
                    alarmed[name] = False
                    continue

            if alarmed[name]:
                continue  # already sent the one alarm for this outage

            if job.get("track_mode") == "seen":
                message = f"ALARM: SERVER ERROR"
            else:
                message = (
                    f"ALARM: '{name}' is not running but should be "
                    f"(scheduled {job['start']}-{job['end']} IST)."
                )
            print(message)
            try:
                raise_alarm(message)
            except Exception as exc:
                print(f"failed to send alarm for {name}: {exc}")
            alarmed[name] = True

        time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()
