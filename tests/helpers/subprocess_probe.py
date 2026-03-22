#!/usr/bin/env python
"""Small probe process for stagegate subprocess-helper tests."""

from __future__ import annotations

import signal
import sys
import time


sigterm_seen = False


def _handle_sigterm(signum: int, frame) -> None:
    """Record that SIGTERM was observed and exit cooperatively later."""

    del signum
    del frame
    global sigterm_seen
    sigterm_seen = True


def _parse_argv(argv: list[str]) -> tuple[bool, list[int]]:
    ignore_term = False
    if argv and argv[0] == "--ignore-term":
        ignore_term = True
        argv = argv[1:]

    if len(argv) < 2:
        raise SystemExit(
            "usage: subprocess_probe.py [--ignore-term] "
            "<sleep_ms> <term_sleep_ms> [payload_int ...]"
        )

    try:
        values = [int(arg) for arg in argv]
    except ValueError as exc:
        raise SystemExit(f"all probe arguments must be integers: {exc}") from exc

    return ignore_term, values


def _sleep_until(deadline: float) -> None:
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 0.05))


def main(argv: list[str]) -> int:
    ignore_term, values = _parse_argv(argv)
    sleep_ms = values[0]
    term_sleep_ms = values[1]
    exit_code = sum(values) & 0x7F

    if ignore_term:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    else:
        signal.signal(signal.SIGTERM, _handle_sigterm)

    normal_deadline = time.monotonic() + (sleep_ms / 1000.0)
    while True:
        if sigterm_seen:
            term_deadline = time.monotonic() + (term_sleep_ms / 1000.0)
            _sleep_until(term_deadline)
            return exit_code | 0x80

        remaining = normal_deadline - time.monotonic()
        if remaining <= 0:
            return exit_code
        time.sleep(min(remaining, 0.05))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
