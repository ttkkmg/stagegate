"""Pipeline base class and task-builder skeleton."""

from __future__ import annotations

from dataclasses import dataclass, field
import threading
from typing import TYPE_CHECKING, Any

from .handles import TaskHandle
from ._states import PipelineState
from ._wait_utils import (
    monotonic_deadline,
    remaining_timeout,
    should_return,
    split_done_pending,
    validate_wait_request,
)
from .wait import ALL_COMPLETED

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from ._records import PipelineRecord
    from .scheduler import Scheduler


@dataclass(frozen=True, slots=True)
class TaskBuilder:
    """Factory object returned by ``Pipeline.task(...)``.

    Attributes:
        pipeline: Owning pipeline instance.
        fn: Callable to execute later on a worker thread.
        resources: Abstract resource requirements for admission control.
        args: Positional arguments passed to ``fn``.
        kwargs: Keyword arguments passed to ``fn``.
        name: Optional user-facing task label.
    """

    pipeline: Pipeline
    fn: Callable[..., Any]
    resources: dict[str, int | float]
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    name: str | None = None

    def run(self) -> TaskHandle:
        """Submit the task to the owning scheduler and return its handle.

        Returns:
            TaskHandle: Handle for observing the submitted task.

        Raises:
            RuntimeError: If called outside the active coordinator-thread
                control context for the owning pipeline.
            UnknownResourceError: If a requested resource label is unknown to
                the scheduler.
            UnschedulableTaskError: If the task can never fit within scheduler
                capacity.
            ValueError: If resource amounts are invalid.
        """
        pipeline = self.pipeline
        scheduler, _ = pipeline._require_control_context()
        with scheduler._condition:
            return scheduler._submit_task_builder_locked(self)


class Pipeline:
    """Base class for user-defined pipelines.

    Subclass this type and override :meth:`run` with the pipeline body.
    Pipeline-control APIs such as :meth:`task`, :meth:`wait`, and
    :meth:`stage_forward` are valid only while ``run()`` is executing on the
    scheduler-owned coordinator thread.
    """

    _stagegate_record: PipelineRecord | None = None
    _stagegate_scheduler: Scheduler | None = None
    _stagegate_submitted: bool = False

    def run(self) -> Any:
        """Execute pipeline logic on a scheduler-owned coordinator thread.

        Returns:
            Any: Arbitrary pipeline result object. Returning normally marks the
                pipeline as succeeded.

        Raises:
            Exception: Any uncaught exception raised by user code marks the
                pipeline as failed and is later re-raised by the pipeline
                handle.
        """
        raise NotImplementedError

    def _require_control_context(self):
        record = getattr(self, "_stagegate_record", None)
        scheduler = getattr(self, "_stagegate_scheduler", None)
        if record is None or scheduler is None:
            raise RuntimeError("pipeline control requires a running pipeline")
        if record.state is not PipelineState.RUNNING:
            raise RuntimeError("pipeline control requires a running pipeline")
        if record.coordinator_thread_ident != threading.get_ident():
            raise RuntimeError(
                "pipeline control is allowed only on the coordinator thread"
            )
        return scheduler, record

    def task(
        self,
        fn: Callable[..., Any],
        *,
        resources: dict[str, int | float],
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        name: str | None = None,
    ) -> TaskBuilder:
        """Create a task builder for later submission via ``.run()``.

        Args:
            fn: Callable to execute later on a worker thread.
            resources: Abstract resource requirements for admission control.
            args: Positional arguments passed to ``fn``.
            kwargs: Keyword arguments passed to ``fn``.
            name: Optional user-facing task label.

        Returns:
            TaskBuilder: Builder object that can later be submitted with
                ``.run()``.
        """
        return TaskBuilder(
            pipeline=self,
            fn=fn,
            resources=dict(resources),
            args=args,
            kwargs={} if kwargs is None else dict(kwargs),
            name=name,
        )

    def stage_forward(self) -> None:
        """Advance the pipeline stage used by future task submissions.

        Raises:
            RuntimeError: If called outside the active coordinator-thread
                control context for the pipeline.
        """
        scheduler, record = self._require_control_context()
        with scheduler._condition:
            record.stage_index += 1

    def wait(
        self,
        handles: Iterable[TaskHandle],
        timeout: float | None = None,
        return_when: str = ALL_COMPLETED,
    ) -> tuple[set[TaskHandle], set[TaskHandle]]:
        """Wait for task handles created by this pipeline.

        Args:
            handles: Task handles created by this pipeline.
            timeout: Maximum wait time in seconds. ``None`` means unbounded
                wait and ``0`` means immediate poll.
            return_when: One of the public wait-condition constants.

        Returns:
            tuple[set[TaskHandle], set[TaskHandle]]: A ``(done, pending)`` pair.

        Raises:
            RuntimeError: If called outside the active coordinator-thread
                control context for the pipeline.
            TypeError: If a non-task handle is provided.
            ValueError: If ``handles`` is empty, ownership is wrong, timeout is
                invalid, or ``return_when`` is invalid.
        """
        # Concrete implementation must validate return_when against WAIT_CONDITIONS.
        scheduler, _ = self._require_control_context()
        normalized = validate_wait_request(
            handles,
            expected_type=TaskHandle,
            owner_check=lambda handle: handle._record.pipeline_record.pipeline is self,
            timeout=timeout,
            return_when=return_when,
        )
        deadline = monotonic_deadline(timeout)

        with scheduler._condition:
            while True:
                done, pending = split_done_pending(normalized)
                if should_return(done=done, pending=pending, return_when=return_when):
                    return done, pending

                wait_timeout = remaining_timeout(deadline)
                if wait_timeout == 0.0:
                    return done, pending
                scheduler._condition.wait(wait_timeout)
