"""
Import this at the very top of any execution_*.py entrypoint - before anything else that has side
effects (broker auth, order placement, etc.) - to make sure only one instance of that script is
ever running at a time. Duplicate instances are exactly how a strategy could end up double-
entering/double-exiting positions: a stray tmux session left over from a previous day, a cron job
firing while yesterday's process is still limping through EOD square-off, a manual re-run while
the scheduled one is still up, and so on.

Uses psutil to inspect actually-running processes rather than a lockfile/pidfile, so a stale file
left behind by a hard crash (kill -9, OOM) can never cause a false "already running" positive -
the check always reflects what's genuinely running right now.

Matches by script basename (e.g. 'execution_rolling_straddle_tn.py'), not the full command line,
so it doesn't matter whether the other process was launched as `python3 foo.py`, `./foo.py`, from
a different working directory, or with a different interpreter path - only the script name has to
match. Importing this module runs the check immediately (see guard() call at the bottom) and exits
the process if a match is found - callers don't need to call anything themselves, just:

    import guard_module
"""

import logging
import os
import sys

import psutil

log = logging.getLogger(__name__)


def _this_script_name():
    """Basename of the script that's actually running (sys.argv[0]), e.g.
    'execution_rolling_straddle_tn.py'. None if argv[0] is empty (e.g. an interactive shell or
    `python -c ...`) - there's no meaningful script name to guard on in that case."""
    argv0 = sys.argv[0] if sys.argv else ''
    return os.path.basename(argv0) if argv0 else None


def _cmdline_script_names(cmdline):
    """Basenames of every '.py' argument on a process's command line - a process could be
    `python3 foo.py`, `python3 /home/ec2-user/trading/foo.py`, or (via a shebang) just `./foo.py`,
    so every argument is checked rather than assuming a fixed position."""
    return {os.path.basename(arg) for arg in cmdline if arg.endswith('.py')}


def already_running(script_name=None):
    """True if some *other* process on this machine is running a Python script with the same
    basename as this one. Processes that vanish mid-scan or can't be read (permission denied,
    zombie) are skipped rather than treated as a match - a false negative here just means the
    guard occasionally misses a duplicate, whereas a false positive would refuse to start a
    strategy that should legitimately be running."""
    script_name = script_name or _this_script_name()
    if not script_name:
        return False

    own_pid = os.getpid()
    for proc in psutil.process_iter(['pid', 'cmdline']):
        if proc.info['pid'] == own_pid:
            continue
        try:
            cmdline = proc.info['cmdline'] or []
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
        if script_name in _cmdline_script_names(cmdline):
            return True
    return False


def guard(script_name=None):
    """Exit the process immediately if another instance of `script_name` (default: this script)
    is already running. Safe to call more than once - it's just re-checking the same condition."""
    script_name = script_name or _this_script_name()
    if already_running(script_name):
        message = f'Another instance of {script_name} is already running - exiting to avoid duplicate trading'
        log.critical(message)
        print(message, file=sys.stderr)
        sys.exit(1)


guard()  # runs at import time - see module docstring
