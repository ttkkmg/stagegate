"""Internal helpers for owner-scoped wait APIs."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from typing import TypeVar

from ._states import PipelineState, TaskState
from .handles import PipelineHandle, TaskHandle
from .wait import ALL_COMPLETED, FIRST_COMPLETED, WAIT_CONDITIONS

HandleT = TypeVar("HandleT", TaskHandle, PipelineHandle)


def validate_wait_request(
    handles: Iterable[HandleT],
    *,
    expected_type: type[HandleT],
    owner_check: Callable[[HandleT], bool],
    timeout: float | None,
    return_when: str,
) -> set[HandleT]:
    """Normalize and validate public wait() inputs."""

    if timeout is not None and timeout < 0:
        raise ValueError("timeout must be None or a non-negative number")
    if return_when not in WAIT_CONDITIONS:
        raise ValueError("invalid return_when")

    candidates = tuple(handles)
    if not candidates:
        raise ValueError("handles must not be empty")

    for handle in candidates:
        if not isinstance(handle, expected_type):
            raise TypeError("wrong handle type")
        if not owner_check(handle):
            raise ValueError("handle does not belong to this owner")

    normalized = set(candidates)
    return normalized


def split_done_pending(handles: set[HandleT]) -> tuple[set[HandleT], set[HandleT]]:
    """Partition handles into terminal and non-terminal subsets."""

    done: set[HandleT] = set()
    pending: set[HandleT] = set()
    for handle in handles:
        if handle.done():
            done.add(handle)
        else:
            pending.add(handle)
    return done, pending


def should_return(
    *,
    done: set[HandleT],
    pending: set[HandleT],
    return_when: str,
) -> bool:
    """Evaluate the public wait-return condition."""

    if return_when == ALL_COMPLETED:
        return not pending
    if return_when == FIRST_COMPLETED:
        return bool(done)
    if any(_is_failed(handle) for handle in done):
        return True
    return not pending


def monotonic_deadline(timeout: float | None) -> float | None:
    """Convert a public timeout value into an absolute deadline."""

    if timeout is None:
        return None
    return time.monotonic() + timeout


def remaining_timeout(deadline: float | None) -> float | None:
    """Compute remaining timeout for a condition wait."""

    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return 0.0
    return remaining


def _is_failed(handle: HandleT) -> bool:
    if isinstance(handle, TaskHandle):
        return handle._record.state in (TaskState.FAILED, TaskState.TERMINATED)
    return handle._record.state is PipelineState.FAILED
