"""Task-local context helpers for cooperative termination."""

from __future__ import annotations

import threading

from .exceptions import TerminatedError


_TASK_LOCAL = threading.local()


class TaskContext:
    """Ambient task context installed on worker threads while user code runs."""

    __slots__ = ("task_id", "pipeline_id", "_condition", "_terminate_requested")

    def __init__(
        self,
        *,
        task_id: int,
        pipeline_id: int,
        terminate_requested: bool = False,
    ) -> None:
        self.task_id = task_id
        self.pipeline_id = pipeline_id
        self._condition = threading.Condition()
        self._terminate_requested = terminate_requested

    def terminate_requested(self) -> bool:
        """Return whether cooperative termination has been requested."""

        return self._terminate_requested

    def request_terminate(self) -> None:
        """Record a terminate request and wake condition waiters."""

        with self._condition:
            self._terminate_requested = True
            self._condition.notify_all()


def current_task_context() -> TaskContext | None:
    """Return the current ambient task context, if any."""

    context = getattr(_TASK_LOCAL, "current_task_context", None)
    if context is None:
        return None
    return context


def install_task_context(context: TaskContext) -> None:
    """Install an ambient task context on the current worker thread."""

    _TASK_LOCAL.current_task_context = context


def clear_task_context() -> None:
    """Remove any ambient task context from the current worker thread."""

    if hasattr(_TASK_LOCAL, "current_task_context"):
        delattr(_TASK_LOCAL, "current_task_context")


def terminate_requested() -> bool:
    """Return whether the current task has received a terminate request.

    Returns:
        bool: ``True`` only while user code is running on a worker thread with
        an ambient task context whose terminate-request flag is set.
    """

    context = current_task_context()
    if context is None:
        return False
    return context.terminate_requested()


def raise_if_termination_requested() -> None:
    """Raise ``TerminatedError`` if the current task was asked to terminate."""

    if terminate_requested():
        raise TerminatedError(
            argv=(),
            pid=None,
            returncode=None,
            forced_kill=False,
        )
