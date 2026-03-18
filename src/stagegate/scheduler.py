"""Scheduler skeleton."""

from __future__ import annotations

import heapq
import math
import threading
from typing import TYPE_CHECKING

from ._records import (
    PipelineQueueEntry,
    PipelineRecord,
    ReadyQueueEntry,
    SchedulerRuntime,
    TaskQueueEntry,
    TaskRecord,
)
from ._states import PipelineState, SchedulerState, TaskState
from .exceptions import UnknownResourceError, UnschedulableTaskError
from .handles import PipelineHandle
from .pipeline import Pipeline
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
    """Single-process scheduler for stage-aware local pipelines and tasks."""

    def __init__(
        self,
        *,
        resources: dict[str, int | float],
        pipeline_parallelism: int = 1,
        task_parallelism: int | None = None,
    ) -> None:
        self._resources = dict(resources)
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
        """Return the scheduler for use as a context manager."""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Fully close the scheduler when leaving a context-manager block."""
        self.close()

    def shutdown_started(self) -> bool:
        """Return whether shutdown has begun."""
        with self._condition:
            return self._runtime.state is not SchedulerState.OPEN

    def closed(self) -> bool:
        """Return whether the scheduler has fully closed."""
        with self._condition:
            return self._runtime.state is SchedulerState.CLOSED

    def run_pipeline(self, pipeline: Pipeline) -> PipelineHandle:
        """Submit a pipeline instance for FIFO execution and return its handle."""
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
        """Wait for pipeline handles owned by this scheduler."""
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
        """Start shutdown without waiting for full close."""
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
        """Block until shutdown completes and runtime threads have exited."""
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
        for record in self._runtime.pipeline_records.values():
            if record.state is PipelineState.QUEUED:
                record.state = PipelineState.CANCELLED
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
        if self._dispatcher_thread is not None and self._dispatcher_thread.ident == thread_ident:
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
            record.state = TaskState.CANCELLED
            self._notify_state_change_locked()
            return True
        if record.state is TaskState.READY:
            record.state = TaskState.CANCELLED
            record.ready_seq = None
            self._release_resources_locked(record.resources_required)
            self._runtime.admitted_task_count -= 1
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
        pipeline_record.task_records.append(record)
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
        record.state = TaskState.READY
        record.ready_seq = self._runtime.next_ready_seq
        self._runtime.ready_queue.append(ReadyQueueEntry(record.ready_seq, record))
        self._condition.notify_all()
        return True

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
                record.state = TaskState.RUNNING
                record.ready_seq = None
                record.worker_thread_ident = worker_ident
                self._condition.notify_all()

            result_value = None
            error: BaseException | None = None
            try:
                result_value = record.fn(*record.args, **record.kwargs)
            except BaseException as exc:
                # Background runtime must preserve scheduler invariants even for
                # non-Exception failures raised by user code.
                error = exc

            with self._condition:
                record.worker_thread_ident = None
                if error is None:
                    record.state = TaskState.SUCCEEDED
                    record.result_value = result_value
                else:
                    record.state = TaskState.FAILED
                    record.exception = error
                self._release_resources_locked(record.resources_required)
                self._runtime.admitted_task_count -= 1
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
                result_value = record.pipeline.run()
            except BaseException as exc:
                # Background runtime must preserve scheduler invariants even for
                # non-Exception failures raised by user code.
                error = exc

            with self._condition:
                record.coordinator_thread_ident = None
                if error is None:
                    record.state = PipelineState.SUCCEEDED
                    record.result_value = result_value
                else:
                    record.state = PipelineState.FAILED
                    record.exception = error
                self._notify_state_change_locked()
