"""Public handle skeletons."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from ._records import TerminalPipelineState, TerminalTaskState
from ._states import PipelineState, TaskState
from .exceptions import CancelledError, DiscardedHandleError
from .snapshots import PipelineSnapshot, TaskCountsSnapshot, pipeline_state_to_public_name

if TYPE_CHECKING:
    from ._records import PipelineRecord, TaskRecord


def _validate_timeout(timeout: float | None) -> float | None:
    if timeout is None:
        return None
    if timeout < 0:
        raise ValueError("timeout must be None or a non-negative number")
    return timeout


def _remaining_timeout(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError
    return remaining


class TaskHandle:
    """Handle for a task submitted from a pipeline."""

    __slots__ = ("_record",)

    def __init__(self, record: TaskRecord) -> None:
        self._record = record

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaskHandle):
            return NotImplemented
        return (
            self._record.task_id == other._record.task_id
            and self._record.scheduler is other._record.scheduler
        )

    def __hash__(self) -> int:
        return hash((TaskHandle, id(self._record.scheduler), self._record.task_id))

    def __repr__(self) -> str:
        return (
            f"TaskHandle(scheduler=0x{id(self._record.scheduler):x}, "
            f"task_id={self._record.task_id})"
        )

    def cancel(self) -> bool:
        """Cancel the task if it has not started yet."""
        scheduler = self._record.scheduler
        with scheduler._condition:
            return scheduler._cancel_task_if_possible_locked(self._record)

    def done(self) -> bool:
        """Return whether the task is terminal."""
        scheduler = self._record.scheduler
        with scheduler._condition:
            return self._record.state in TerminalTaskState

    def running(self) -> bool:
        """Return whether the task callable is actively running."""
        scheduler = self._record.scheduler
        with scheduler._condition:
            return self._record.state is TaskState.RUNNING

    def cancelled(self) -> bool:
        """Return whether the task ended in the cancelled state."""
        scheduler = self._record.scheduler
        with scheduler._condition:
            return self._record.state is TaskState.CANCELLED

    def result(self, timeout: float | None = None) -> Any:
        """Return the task result or raise its stored terminal outcome."""
        timeout = _validate_timeout(timeout)
        scheduler = self._record.scheduler
        deadline = None if timeout is None else time.monotonic() + timeout
        with scheduler._condition:
            while self._record.state not in TerminalTaskState:
                wait_timeout = (
                    None if deadline is None else _remaining_timeout(deadline)
                )
                scheduler._condition.wait(wait_timeout)

            if self._record.state is TaskState.SUCCEEDED:
                return self._record.result_value
            if self._record.state is TaskState.FAILED:
                assert self._record.exception is not None
                raise self._record.exception
            raise CancelledError("result() was requested from a cancelled task handle")

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """Return the task exception object, if any."""
        timeout = _validate_timeout(timeout)
        scheduler = self._record.scheduler
        deadline = None if timeout is None else time.monotonic() + timeout
        with scheduler._condition:
            while self._record.state not in TerminalTaskState:
                wait_timeout = (
                    None if deadline is None else _remaining_timeout(deadline)
                )
                scheduler._condition.wait(wait_timeout)

            if self._record.state is TaskState.SUCCEEDED:
                return None
            if self._record.state is TaskState.FAILED:
                assert self._record.exception is not None
                return self._record.exception
            raise CancelledError(
                "exception() was requested from a cancelled task handle"
            )


class PipelineHandle:
    """Handle for a pipeline submitted to a scheduler."""

    __slots__ = ("_record",)

    def __init__(self, record: PipelineRecord) -> None:
        self._record = record

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PipelineHandle):
            return NotImplemented
        return (
            self._record.pipeline_id == other._record.pipeline_id
            and self._record.scheduler is other._record.scheduler
        )

    def __hash__(self) -> int:
        return hash(
            (PipelineHandle, id(self._record.scheduler), self._record.pipeline_id)
        )

    def __repr__(self) -> str:
        return (
            f"PipelineHandle(scheduler=0x{id(self._record.scheduler):x}, "
            f"pipeline_id={self._record.pipeline_id})"
        )

    def cancel(self) -> bool:
        """Cancel the pipeline if it has not started yet."""
        scheduler = self._record.scheduler
        with scheduler._condition:
            if self._record.state is PipelineState.QUEUED:
                scheduler._finalize_pipeline_terminal_locked(
                    self._record,
                    state=PipelineState.CANCELLED,
                )
                scheduler._notify_state_change_locked()
                return True
            return False

    def discard(self) -> None:
        """Discard this handle and release retained terminal outcome data."""
        scheduler = self._record.scheduler
        with scheduler._condition:
            if self._record.discarded:
                return
            if self._record.state not in TerminalPipelineState:
                raise RuntimeError("discard() requires a terminal pipeline handle")

            self._record.discarded = True
            self._record.pipeline = None
            self._record.result_value = None
            self._record.exception = None
            self._record.coordinator_thread_ident = None

    def _ensure_not_discarded_locked(self) -> None:
        if self._record.discarded:
            raise DiscardedHandleError(
                "operation was requested from a discarded pipeline handle"
            )

    def done(self) -> bool:
        """Return whether the pipeline is terminal."""
        scheduler = self._record.scheduler
        with scheduler._condition:
            self._ensure_not_discarded_locked()
            return self._record.state in TerminalPipelineState

    def running(self) -> bool:
        """Return whether the pipeline ``run()`` method is active."""
        scheduler = self._record.scheduler
        with scheduler._condition:
            self._ensure_not_discarded_locked()
            return self._record.state is PipelineState.RUNNING

    def cancelled(self) -> bool:
        """Return whether the pipeline ended in the cancelled state."""
        scheduler = self._record.scheduler
        with scheduler._condition:
            self._ensure_not_discarded_locked()
            return self._record.state is PipelineState.CANCELLED

    def result(self, timeout: float | None = None) -> Any:
        """Return the pipeline result or raise its stored terminal outcome."""
        timeout = _validate_timeout(timeout)
        scheduler = self._record.scheduler
        deadline = None if timeout is None else time.monotonic() + timeout
        with scheduler._condition:
            self._ensure_not_discarded_locked()
            while self._record.state not in TerminalPipelineState:
                wait_timeout = (
                    None if deadline is None else _remaining_timeout(deadline)
                )
                scheduler._condition.wait(wait_timeout)

            if self._record.state is PipelineState.SUCCEEDED:
                return self._record.result_value
            if self._record.state is PipelineState.FAILED:
                assert self._record.exception is not None
                raise self._record.exception
            raise CancelledError(
                "result() was requested from a cancelled pipeline handle"
            )

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """Return the pipeline exception object, if any."""
        timeout = _validate_timeout(timeout)
        scheduler = self._record.scheduler
        deadline = None if timeout is None else time.monotonic() + timeout
        with scheduler._condition:
            self._ensure_not_discarded_locked()
            while self._record.state not in TerminalPipelineState:
                wait_timeout = (
                    None if deadline is None else _remaining_timeout(deadline)
                )
                scheduler._condition.wait(wait_timeout)

            if self._record.state is PipelineState.SUCCEEDED:
                return None
            if self._record.state is PipelineState.FAILED:
                assert self._record.exception is not None
                return self._record.exception
            raise CancelledError(
                "exception() was requested from a cancelled pipeline handle"
            )

    def snapshot(self) -> PipelineSnapshot:
        """Return an immutable point-in-time snapshot for this pipeline."""
        scheduler = self._record.scheduler
        with scheduler._condition:
            self._ensure_not_discarded_locked()
            tasks = TaskCountsSnapshot(
                queued=self._record.queued_task_count,
                admitted=self._record.admitted_task_count,
                running=self._record.running_task_count,
                succeeded=self._record.succeeded_task_count,
                failed=self._record.failed_task_count,
                cancelled=self._record.cancelled_task_count,
                total=self._record.total_task_count,
            )
            return PipelineSnapshot(
                pipeline_id=self._record.pipeline_id,
                state=pipeline_state_to_public_name(self._record.state),
                stage_index=self._record.stage_index,
                tasks=tasks,
            )
