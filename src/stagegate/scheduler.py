"""Scheduler skeleton."""

from __future__ import annotations

import heapq
import math
import threading
from typing import TYPE_CHECKING

from ._task_context import TaskContext, clear_task_context, install_task_context
from ._records import (
    PipelineQueueEntry,
    PipelineRecord,
    ReadyQueueEntry,
    SchedulerRuntime,
    TaskQueueEntry,
    TaskRecord,
)
from ._states import PipelineState, SchedulerState, TaskState
from .exceptions import TerminatedError, UnknownResourceError, UnschedulableTaskError
from .handles import PipelineHandle
from .pipeline import Pipeline
from .snapshots import (
    PipelineCountsSnapshot,
    ResourceSnapshot,
    SchedulerSnapshot,
    TaskCountsSnapshot,
)
from ._wait_utils import (
    monotonic_deadline,
    remaining_timeout,
    should_return,
    split_done_pending,
    validate_wait_request,
)
from .wait import ALL_COMPLETED

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .handles import TaskHandle
    from .pipeline import TaskBuilder


class Scheduler:
    """Single-process scheduler for stage-aware local pipelines and tasks.

    Args:
        resources: Optional abstract resource capacities such as
            ``{"cpu": 8}``. ``None`` means no resource labels are configured.
            These are scheduler-defined admission labels, not OS-enforced
            quotas.
        pipeline_parallelism: Maximum number of pipeline coordinator threads.
        task_parallelism: Maximum number of concurrently admitted tasks. If
            ``None``, the effective worker count is ``1``.
    """

    def __init__(
        self,
        *,
        resources: dict[str, int | float] | None = None,
        pipeline_parallelism: int = 1,
        task_parallelism: int | None = None,
    ) -> None:
        self._resources = {} if resources is None else dict(resources)
        self._pipeline_parallelism = pipeline_parallelism
        self._task_parallelism = task_parallelism
        self._runtime = SchedulerRuntime()
        self._runtime.resources_in_use = {label: 0 for label in self._resources}
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._effective_task_parallelism = (
            1 if task_parallelism is None else task_parallelism
        )
        self._pipeline_runtime_started = False
        self._coordinator_threads: list[threading.Thread] = []
        self._task_runtime_started = False
        self._dispatcher_thread: threading.Thread | None = None
        self._worker_threads: list[threading.Thread] = []
        self._runtime_stop_requested = False

    def __enter__(self) -> Scheduler:
        """Return the scheduler for use as a context manager.

        Returns:
            Scheduler: The scheduler itself.
        """
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Fully close the scheduler when leaving a context-manager block.

        Args:
            exc_type: Exception type from the context manager protocol.
            exc: Exception instance from the context manager protocol.
            tb: Traceback object from the context manager protocol.
        """
        self.close()

    def shutdown_started(self) -> bool:
        """Return whether shutdown has begun.

        Returns:
            bool: ``True`` once the scheduler has left the open state.
        """
        with self._condition:
            return self._runtime.state is not SchedulerState.OPEN

    def closed(self) -> bool:
        """Return whether the scheduler has fully closed.

        Returns:
            bool: ``True`` only after live work has drained and runtime threads
                have been joined.
        """
        with self._condition:
            return self._runtime.state is SchedulerState.CLOSED

    def snapshot(self) -> SchedulerSnapshot:
        """Return an immutable point-in-time snapshot of scheduler state.

        Returns:
            SchedulerSnapshot: Detached aggregate view of scheduler state,
                counts, and resources.
        """
        with self._condition:
            queued_pipelines = sum(
                1
                for record in self._runtime.pipeline_records.values()
                if record.state is PipelineState.QUEUED
            )
            running_pipelines = sum(
                1
                for record in self._runtime.pipeline_records.values()
                if record.state is PipelineState.RUNNING
            )
            pipeline_counts = PipelineCountsSnapshot(
                queued=queued_pipelines,
                running=running_pipelines,
                succeeded=self._runtime.succeeded_pipeline_count,
                failed=self._runtime.failed_pipeline_count,
                cancelled=self._runtime.cancelled_pipeline_count,
                total=(
                    queued_pipelines
                    + running_pipelines
                    + self._runtime.succeeded_pipeline_count
                    + self._runtime.failed_pipeline_count
                    + self._runtime.cancelled_pipeline_count
                ),
            )
            queued_tasks = sum(
                1
                for record in self._runtime.task_records.values()
                if record.state is TaskState.QUEUED
            )
            running_tasks = sum(
                1
                for record in self._runtime.task_records.values()
                if record.state is TaskState.RUNNING
            )
            task_counts = TaskCountsSnapshot(
                queued=queued_tasks,
                admitted=self._runtime.admitted_task_count,
                running=running_tasks,
                succeeded=self._runtime.succeeded_task_count,
                failed=self._runtime.failed_task_count,
                terminated=self._runtime.terminated_task_count,
                cancelled=self._runtime.cancelled_task_count,
                total=(
                    queued_tasks
                    + self._runtime.admitted_task_count
                    + self._runtime.succeeded_task_count
                    + self._runtime.failed_task_count
                    + self._runtime.terminated_task_count
                    + self._runtime.cancelled_task_count
                ),
            )
            resources = tuple(
                ResourceSnapshot(
                    label=label,
                    capacity=self._resources[label],
                    in_use=self._runtime.resources_in_use[label],
                    available=self._resources[label]
                    - self._runtime.resources_in_use[label],
                )
                for label in sorted(self._resources)
            )
            return SchedulerSnapshot(
                shutdown_started=self._runtime.state is not SchedulerState.OPEN,
                closed=self._runtime.state is SchedulerState.CLOSED,
                pipeline_parallelism=self._pipeline_parallelism,
                task_parallelism=self._effective_task_parallelism,
                pipelines=pipeline_counts,
                tasks=task_counts,
                resources=resources,
            )

    def run_pipeline(self, pipeline: Pipeline) -> PipelineHandle:
        """Submit a pipeline instance for FIFO execution and return its handle.

        Args:
            pipeline: Pipeline instance to enqueue.

        Returns:
            PipelineHandle: Handle for observing the submitted pipeline.

        Raises:
            TypeError: If ``pipeline`` is not a ``Pipeline`` instance.
            RuntimeError: If shutdown has already started or the same pipeline
                instance was submitted previously.
        """
        if not isinstance(pipeline, Pipeline):
            raise TypeError("pipeline must be a Pipeline instance")

        with self._condition:
            if self._runtime.state is not SchedulerState.OPEN:
                raise RuntimeError("scheduler is shutting down")
            if getattr(pipeline, "_stagegate_submitted", False):
                raise RuntimeError(
                    "the same Pipeline instance cannot be submitted twice"
                )

            self._ensure_pipeline_runtime_started_locked()

            self._runtime.next_pipeline_id += 1
            self._runtime.next_pipeline_enqueue_seq += 1
            record = PipelineRecord(
                scheduler=self,
                pipeline=pipeline,
                pipeline_id=self._runtime.next_pipeline_id,
                enqueue_seq=self._runtime.next_pipeline_enqueue_seq,
            )
            self._runtime.pipeline_records[record.pipeline_id] = record
            self._runtime.pipeline_queue.append(
                PipelineQueueEntry(enqueue_seq=record.enqueue_seq, record=record)
            )
            pipeline._stagegate_record = record
            pipeline._stagegate_scheduler = self
            pipeline._stagegate_submitted = True
            self._condition.notify_all()
            return PipelineHandle(record)

    def _ensure_pipeline_runtime_started_locked(self) -> None:
        if self._pipeline_runtime_started:
            return
        self._pipeline_runtime_started = True
        for index in range(self._pipeline_parallelism):
            coordinator = threading.Thread(
                target=self._coordinator_loop,
                name=f"stagegate-coordinator-{index}",
                daemon=True,
            )
            coordinator.start()
            self._coordinator_threads.append(coordinator)

    def wait_pipelines(
        self,
        handles: Iterable[PipelineHandle],
        timeout: float | None = None,
        return_when: str = ALL_COMPLETED,
    ) -> tuple[set[PipelineHandle], set[PipelineHandle]]:
        """Wait for pipeline handles owned by this scheduler.

        Args:
            handles: Pipeline handles created by this scheduler.
            timeout: Maximum wait time in seconds. ``None`` means unbounded
                wait and ``0`` means immediate poll.
            return_when: One of the public wait-condition constants.

        Returns:
            tuple[set[PipelineHandle], set[PipelineHandle]]: A ``(done, pending)``
                pair.

        Raises:
            TypeError: If a non-pipeline handle is provided.
            ValueError: If ``handles`` is empty, ownership is wrong, timeout is
                invalid, or ``return_when`` is invalid.
        """
        # Concrete implementation must validate return_when against WAIT_CONDITIONS.
        normalized = validate_wait_request(
            handles,
            expected_type=PipelineHandle,
            owner_check=lambda handle: handle._record.scheduler is self,
            timeout=timeout,
            return_when=return_when,
        )
        deadline = monotonic_deadline(timeout)

        with self._condition:
            while True:
                done, pending = split_done_pending(normalized)
                if should_return(done=done, pending=pending, return_when=return_when):
                    return done, pending

                wait_timeout = remaining_timeout(deadline)
                if wait_timeout == 0.0:
                    return done, pending
                self._condition.wait(wait_timeout)

    def shutdown(self, cancel_pending_pipelines: bool = False) -> None:
        """Start shutdown without waiting for full close.

        Args:
            cancel_pending_pipelines: Whether queued pipelines should be
                cancelled during shutdown processing.
        """
        with self._condition:
            if self._runtime.state is SchedulerState.CLOSED:
                return
            if self._runtime.state is SchedulerState.OPEN:
                self._runtime.state = SchedulerState.SHUTTING_DOWN
                self._condition.notify_all()

            if cancel_pending_pipelines:
                self._cancel_pending_pipelines_locked()

            self._condition.notify_all()

    def close(self, cancel_pending_pipelines: bool = False) -> None:
        """Block until shutdown completes and runtime threads have exited.

        Args:
            cancel_pending_pipelines: Whether queued pipelines should be
                cancelled during close processing.

        Raises:
            RuntimeError: If called from a scheduler-owned runtime thread.
        """
        current_ident = threading.get_ident()
        with self._condition:
            if self._is_runtime_thread_locked(current_ident):
                raise RuntimeError(
                    "close() cannot be called from a scheduler-owned runtime thread"
                )
            if self._runtime.state is SchedulerState.CLOSED:
                return
            if self._runtime.state is SchedulerState.OPEN:
                self._runtime.state = SchedulerState.SHUTTING_DOWN
            if cancel_pending_pipelines:
                self._cancel_pending_pipelines_locked()

            while self._has_live_work_locked():
                self._condition.wait()
                if cancel_pending_pipelines:
                    self._cancel_pending_pipelines_locked()

            self._runtime_stop_requested = True
            self._condition.notify_all()
            threads_to_join = self._runtime_threads_locked()

        current_thread = threading.current_thread()
        for thread in threads_to_join:
            if thread is current_thread:
                continue
            thread.join()

        with self._condition:
            self._runtime.state = SchedulerState.CLOSED
            self._condition.notify_all()

    def _has_live_work_locked(self) -> bool:
        return any(
            record.state in (PipelineState.QUEUED, PipelineState.RUNNING)
            for record in self._runtime.pipeline_records.values()
        ) or any(
            record.state in (TaskState.QUEUED, TaskState.READY, TaskState.RUNNING)
            for record in self._runtime.task_records.values()
        )

    def _cancel_pending_pipelines_locked(self) -> None:
        changed = False
        for record in list(self._runtime.pipeline_records.values()):
            if record.state is PipelineState.QUEUED:
                self._finalize_pipeline_terminal_locked(
                    record,
                    state=PipelineState.CANCELLED,
                )
                changed = True
        if changed:
            self._condition.notify_all()

    def _notify_state_change_locked(self) -> None:
        self._condition.notify_all()

    def _runtime_threads_locked(self) -> list[threading.Thread]:
        threads: list[threading.Thread] = []
        if self._dispatcher_thread is not None:
            threads.append(self._dispatcher_thread)
        threads.extend(self._worker_threads)
        threads.extend(self._coordinator_threads)
        return threads

    def _is_runtime_thread_locked(self, thread_ident: int) -> bool:
        if (
            self._dispatcher_thread is not None
            and self._dispatcher_thread.ident == thread_ident
        ):
            return True
        if any(thread.ident == thread_ident for thread in self._worker_threads):
            return True
        if any(thread.ident == thread_ident for thread in self._coordinator_threads):
            return True
        return False

    def _pop_next_live_pipeline_locked(self) -> PipelineRecord | None:
        while self._runtime.pipeline_queue:
            entry = self._runtime.pipeline_queue.popleft()
            record = entry.record
            if record.state is PipelineState.QUEUED:
                return record
        return None

    def _cancel_task_if_possible_locked(self, record: TaskRecord) -> bool:
        if record.state is TaskState.QUEUED:
            self._finalize_task_terminal_locked(record, state=TaskState.CANCELLED)
            self._notify_state_change_locked()
            return True
        if record.state is TaskState.READY:
            self._finalize_task_terminal_locked(
                record,
                state=TaskState.CANCELLED,
                release_resources=True,
            )
            self._notify_state_change_locked()
            return True
        return False

    def _request_task_terminate_if_possible_locked(self, record: TaskRecord) -> bool:
        if record.state is TaskState.QUEUED:
            self._finalize_task_terminal_locked(record, state=TaskState.CANCELLED)
            self._notify_state_change_locked()
            return True
        if record.state is TaskState.READY:
            self._finalize_task_terminal_locked(
                record,
                state=TaskState.CANCELLED,
                release_resources=True,
            )
            self._notify_state_change_locked()
            return True
        if record.state is TaskState.RUNNING:
            record.termination_requested = True
            if record.active_context is not None:
                record.active_context.request_terminate()
            self._notify_state_change_locked()
            return True
        return False

    def _submit_task_builder_locked(self, builder: TaskBuilder) -> TaskHandle:
        self._ensure_task_runtime_started_locked()
        self._validate_task_resources_locked(builder.resources)
        pipeline_record = builder.pipeline._stagegate_record
        assert pipeline_record is not None

        self._runtime.next_task_id += 1
        self._runtime.next_global_task_submit_seq += 1
        pipeline_record.next_task_seq += 1

        record = TaskRecord(
            scheduler=self,
            pipeline_record=pipeline_record,
            task_id=self._runtime.next_task_id,
            fn=builder.fn,
            resources_required=dict(builder.resources),
            args=builder.args,
            kwargs=dict(builder.kwargs),
            name=builder.name,
            stage_snapshot=pipeline_record.stage_index,
            pipeline_enqueue_seq=pipeline_record.enqueue_seq,
            pipeline_local_task_seq=pipeline_record.next_task_seq,
            global_task_submit_seq=self._runtime.next_global_task_submit_seq,
            state=TaskState.QUEUED,
        )
        self._runtime.task_records[record.task_id] = record
        pipeline_record.task_records.add(record)
        pipeline_record.queued_task_count += 1
        pipeline_record.total_task_count += 1
        heapq.heappush(self._runtime.task_queue, TaskQueueEntry.from_record(record))
        self._condition.notify_all()
        from .handles import TaskHandle

        return TaskHandle(record)

    def _validate_task_resources_locked(
        self, resources_required: dict[str, int | float]
    ) -> None:
        for label, amount in resources_required.items():
            if (
                isinstance(amount, bool)
                or not isinstance(amount, (int, float))
                or not math.isfinite(amount)
                or amount < 0
            ):
                raise ValueError(
                    f"resource amounts must be finite non-negative numbers: {label}"
                )
            if label not in self._resources:
                raise UnknownResourceError(f"unknown resource label: {label}")
            if amount > self._resources[label]:
                raise UnschedulableTaskError(
                    f"task requirement exceeds capacity for resource: {label}"
                )

    def _ensure_task_runtime_started_locked(self) -> None:
        if self._task_runtime_started:
            return
        self._task_runtime_started = True
        self._dispatcher_thread = threading.Thread(
            target=self._dispatcher_loop,
            name="stagegate-dispatcher",
            daemon=True,
        )
        self._dispatcher_thread.start()
        for index in range(self._effective_task_parallelism):
            worker = threading.Thread(
                target=self._worker_loop,
                name=f"stagegate-worker-{index}",
                daemon=True,
            )
            worker.start()
            self._worker_threads.append(worker)

    def _peek_top_live_task_locked(self) -> TaskRecord | None:
        while self._runtime.task_queue:
            entry = self._runtime.task_queue[0]
            record = entry.record
            if record.state is TaskState.QUEUED:
                return record
            heapq.heappop(self._runtime.task_queue)
        return None

    def _pop_top_live_task_locked(self) -> TaskRecord | None:
        while self._runtime.task_queue:
            entry = heapq.heappop(self._runtime.task_queue)
            record = entry.record
            if record.state is TaskState.QUEUED:
                return record
        return None

    def _pop_next_ready_task_locked(self) -> TaskRecord | None:
        while self._runtime.ready_queue:
            entry = self._runtime.ready_queue.popleft()
            record = entry.record
            if record.state is TaskState.READY:
                return record
        return None

    def _resources_fit_locked(self, resources_required: dict[str, int | float]) -> bool:
        return all(
            self._runtime.resources_in_use[label] + amount <= self._resources[label]
            for label, amount in resources_required.items()
        )

    def _reserve_resources_locked(
        self, resources_required: dict[str, int | float]
    ) -> None:
        for label, amount in resources_required.items():
            self._runtime.resources_in_use[label] += amount

    def _release_resources_locked(
        self, resources_required: dict[str, int | float]
    ) -> None:
        for label, amount in resources_required.items():
            self._runtime.resources_in_use[label] -= amount

    def _dispatch_one_task_locked(self) -> bool:
        record = self._peek_top_live_task_locked()
        if record is None:
            return False
        if self._runtime.admitted_task_count >= self._effective_task_parallelism:
            return False
        if not self._resources_fit_locked(record.resources_required):
            return False

        record = self._pop_top_live_task_locked()
        assert record is not None
        self._reserve_resources_locked(record.resources_required)
        self._runtime.admitted_task_count += 1
        self._runtime.next_ready_seq += 1
        record.pipeline_record.queued_task_count -= 1
        record.pipeline_record.admitted_task_count += 1
        record.state = TaskState.READY
        record.ready_seq = self._runtime.next_ready_seq
        self._runtime.ready_queue.append(ReadyQueueEntry(record.ready_seq, record))
        self._condition.notify_all()
        return True

    def _finalize_task_terminal_locked(
        self,
        record: TaskRecord,
        *,
        state: TaskState,
        result_value=None,
        exception: BaseException | None = None,
        release_resources: bool = False,
    ) -> None:
        previous_state = record.state
        pipeline_record = record.pipeline_record

        if previous_state is TaskState.QUEUED:
            pipeline_record.queued_task_count -= 1
        elif previous_state is TaskState.READY:
            pipeline_record.admitted_task_count -= 1
        elif previous_state is TaskState.RUNNING:
            pipeline_record.admitted_task_count -= 1
            pipeline_record.running_task_count -= 1

        record.state = state
        record.ready_seq = None
        record.worker_thread_ident = None
        if state is TaskState.SUCCEEDED:
            record.result_value = result_value
            record.exception = None
            pipeline_record.succeeded_task_count += 1
            self._runtime.succeeded_task_count += 1
        elif state is TaskState.FAILED:
            record.result_value = None
            record.exception = exception
            pipeline_record.failed_task_count += 1
            self._runtime.failed_task_count += 1
        elif state is TaskState.TERMINATED:
            record.result_value = None
            record.exception = exception
            pipeline_record.terminated_task_count += 1
            self._runtime.terminated_task_count += 1
        else:
            record.result_value = None
            record.exception = None
            pipeline_record.cancelled_task_count += 1
            self._runtime.cancelled_task_count += 1

        if release_resources:
            self._release_resources_locked(record.resources_required)
            self._runtime.admitted_task_count -= 1
        elif previous_state is TaskState.RUNNING:
            self._release_resources_locked(record.resources_required)
            self._runtime.admitted_task_count -= 1

        self._runtime.task_records.pop(record.task_id, None)
        pipeline_record.task_records.discard(record)
        record.active_context = None

        record.fn = lambda: None
        record.resources_required = {}
        record.args = ()
        record.kwargs = {}
        record.name = None
        record.stage_snapshot = 0
        record.pipeline_enqueue_seq = 0
        record.pipeline_local_task_seq = 0
        record.global_task_submit_seq = 0

    def _finalize_pipeline_terminal_locked(
        self,
        record: PipelineRecord,
        *,
        state: PipelineState,
        result_value=None,
        exception: BaseException | None = None,
    ) -> None:
        record.state = state
        record.coordinator_thread_ident = None
        if state is PipelineState.SUCCEEDED:
            record.result_value = result_value
            record.exception = None
            self._runtime.succeeded_pipeline_count += 1
        elif state is PipelineState.FAILED:
            record.result_value = None
            record.exception = exception
            self._runtime.failed_pipeline_count += 1
        else:
            self._runtime.cancelled_pipeline_count += 1

        self._runtime.pipeline_records.pop(record.pipeline_id, None)

    def _dispatcher_loop(self) -> None:
        with self._condition:
            while True:
                if self._runtime.state is SchedulerState.CLOSED:
                    return
                if self._runtime_stop_requested:
                    return
                if self._dispatch_one_task_locked():
                    continue
                self._condition.wait()

    def _worker_loop(self) -> None:
        worker_ident = threading.get_ident()
        while True:
            with self._condition:
                if self._runtime.state is SchedulerState.CLOSED:
                    return
                if self._runtime_stop_requested:
                    return
                record = self._pop_next_ready_task_locked()
                if record is None:
                    self._condition.wait()
                    continue
                record.pipeline_record.running_task_count += 1
                record.state = TaskState.RUNNING
                record.ready_seq = None
                record.worker_thread_ident = worker_ident
                task_context = TaskContext(
                    task_id=record.task_id,
                    pipeline_id=record.pipeline_record.pipeline_id,
                    terminate_requested=record.termination_requested,
                )
                record.active_context = task_context
                self._condition.notify_all()

            result_value = None
            error: BaseException | None = None
            install_task_context(task_context)
            try:
                result_value = record.fn(*record.args, **record.kwargs)
            except BaseException as exc:
                # Background runtime must preserve scheduler invariants even for
                # non-Exception failures raised by user code.
                error = exc
            finally:
                clear_task_context()

            with self._condition:
                record.active_context = None
                if error is None:
                    self._finalize_task_terminal_locked(
                        record,
                        state=TaskState.SUCCEEDED,
                        result_value=result_value,
                    )
                else:
                    terminal_state = TaskState.FAILED
                    if (
                        isinstance(error, TerminatedError)
                        and record.termination_requested
                    ):
                        terminal_state = TaskState.TERMINATED
                    self._finalize_task_terminal_locked(
                        record,
                        state=terminal_state,
                        exception=error,
                    )
                self._notify_state_change_locked()

    def _coordinator_loop(self) -> None:
        coordinator_ident = threading.get_ident()
        while True:
            with self._condition:
                if self._runtime.state is SchedulerState.CLOSED:
                    return
                if self._runtime_stop_requested:
                    return
                record = self._pop_next_live_pipeline_locked()
                if record is None:
                    self._condition.wait()
                    continue
                record.state = PipelineState.RUNNING
                record.coordinator_thread_ident = coordinator_ident
                self._condition.notify_all()

            result_value = None
            error: BaseException | None = None
            try:
                pipeline = record.pipeline
                assert pipeline is not None
                result_value = pipeline.run()
            except BaseException as exc:
                # Background runtime must preserve scheduler invariants even for
                # non-Exception failures raised by user code.
                error = exc

            with self._condition:
                if error is None:
                    self._finalize_pipeline_terminal_locked(
                        record,
                        state=PipelineState.SUCCEEDED,
                        result_value=result_value,
                    )
                else:
                    self._finalize_pipeline_terminal_locked(
                        record,
                        state=PipelineState.FAILED,
                        exception=error,
                    )
                self._notify_state_change_locked()
