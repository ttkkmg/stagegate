"""Subprocess helpers that integrate with cooperative task termination."""

from __future__ import annotations

from dataclasses import dataclass
import os
from os import PathLike
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Sequence

from .exceptions import TerminatedError
from ._task_context import current_task_context


@dataclass(slots=True)
class _ProcessWaitState:
    exited: bool = False
    returncode: int | None = None


def _normalize_argv(argv: Sequence[str | PathLike[str]]) -> tuple[str, ...]:
    normalized = tuple(os.fspath(arg) for arg in argv)
    if not normalized:
        raise ValueError("argv must not be empty")
    return normalized


def _send_process_group_signal(pid: int, signum: int) -> None:
    try:
        os.killpg(pid, signum)
    except ProcessLookupError:
        return


def run_subprocess(
    argv: Sequence[str | PathLike[str]],
    *,
    terminate_grace_seconds: float | None = 5.0,
) -> int:
    """Run a subprocess that cooperates with task termination requests.

    The child is started in its own process group. If a task terminate request
    is observed first, the helper sends ``SIGTERM`` to that process group and,
    if necessary, escalates to ``SIGKILL`` after the grace timeout.

    This helper is currently intended for POSIX platforms such as Linux,
    macOS, and BSD. Its terminate path depends on process-group signaling
    semantics that are not currently documented or supported on Windows.
    `stagegate` itself may still be usable on Windows for workloads that do not
    rely on this helper.

    Args:
        argv: Executable plus arguments. ``shell=True`` is never used.
        terminate_grace_seconds: Seconds to wait after ``SIGTERM`` before
            sending ``SIGKILL``. ``None`` means wait indefinitely after
            ``SIGTERM``. ``0`` is allowed and means immediate escalation if the
            process has not already exited.

    Returns:
        int: Process return code on normal completion.

    Raises:
        ValueError: If ``argv`` is empty or the grace timeout is negative.
        NotImplementedError: If called on Windows, where the current
            process-group terminate path is not supported.
        TerminatedError: If the terminate path is taken and the child exits
            after the terminate request.
    """

    normalized_argv = _normalize_argv(argv)
    if terminate_grace_seconds is not None and terminate_grace_seconds < 0:
        raise ValueError(
            "terminate_grace_seconds must be None or a non-negative number"
        )
    if sys.platform == "win32":
        raise NotImplementedError(
            "run_subprocess() terminate support is currently available only "
            "on POSIX platforms"
        )

    proc = subprocess.Popen(
        normalized_argv,
        stdin=subprocess.DEVNULL,
        stdout=None,
        stderr=None,
        start_new_session=True,
    )
    pid = proc.pid
    context = current_task_context()
    if context is None:
        return proc.wait()

    wait_state = _ProcessWaitState()

    def wait_for_process_exit() -> None:
        returncode = proc.wait()
        with context._condition:
            wait_state.exited = True
            wait_state.returncode = returncode
            context._condition.notify_all()

    waiter = threading.Thread(
        target=wait_for_process_exit,
        name=f"stagegate-subprocess-waiter-{pid}",
        daemon=True,
    )
    waiter.start()

    with context._condition:
        while not wait_state.exited and not context.terminate_requested():
            context._condition.wait()
        if wait_state.exited:
            waiter.join()
            assert wait_state.returncode is not None
            return wait_state.returncode

    _send_process_group_signal(pid, signal.SIGTERM)
    forced_kill = False

    if terminate_grace_seconds is None:
        with context._condition:
            while not wait_state.exited:
                context._condition.wait()
    else:
        deadline = time.monotonic() + terminate_grace_seconds
        with context._condition:
            while not wait_state.exited:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                context._condition.wait(remaining)

        if not wait_state.exited:
            forced_kill = True
            _send_process_group_signal(pid, signal.SIGKILL)
            with context._condition:
                while not wait_state.exited:
                    context._condition.wait()

    waiter.join()
    raise TerminatedError(
        argv=normalized_argv,
        pid=pid,
        returncode=wait_state.returncode,
        forced_kill=forced_kill,
    )
