"""Internal scheduler record and queue-entry definitions."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ._task_context import TaskContext
from ._states import PipelineState, SchedulerState, TaskState

if TYPE_CHECKING:
    from .pipeline import Pipeline
    from .scheduler import Scheduler


TerminalTaskState = frozenset(
    {
        TaskState.SUCCEEDED,
        TaskState.FAILED,
        TaskState.TERMINATED,
        TaskState.CANCELLED,
    }
)

TerminalPipelineState = frozenset(
    {
        PipelineState.SUCCEEDED,
        PipelineState.FAILED,
        PipelineState.CANCELLED,
    }
)


@dataclass(order=True, frozen=True, slots=True)
class PipelineQueueEntry:
    """FIFO queue entry for pipeline admission."""

    enqueue_seq: int
    record: PipelineRecord = field(compare=False)


@dataclass(order=True, frozen=True, slots=True)
class TaskPriorityKey:
    """Deterministic ordering key for queued tasks."""

    neg_stage: int
    pipeline_enqueue_seq: int
    pipeline_local_task_seq: int
    global_task_submit_seq: int


@dataclass(order=True, frozen=True, slots=True)
class TaskQueueEntry:
    """Priority-queue entry for task admission control."""

    priority: TaskPriorityKey
    record: TaskRecord = field(compare=False)

    @classmethod
    def from_record(cls, record: TaskRecord) -> TaskQueueEntry:
        """Build a queue entry from the record's canonical priority key."""

        return cls(priority=record.priority_key(), record=record)


@dataclass(order=True, frozen=True, slots=True)
class ReadyQueueEntry:
    """FIFO queue entry for tasks admitted but not yet started by a worker."""

    ready_seq: int
    record: TaskRecord = field(compare=False)


@dataclass(eq=False, slots=True)
class PipelineRecord:
    """Mutable source-of-truth record for one pipeline instance."""

    scheduler: Scheduler
    pipeline: Pipeline | None
    pipeline_id: int
    enqueue_seq: int
    name: str | None = None
    state: PipelineState = PipelineState.QUEUED
    stage_index: int = 0
    next_task_seq: int = 0
    result_value: Any = None
    exception: BaseException | None = None
    coordinator_thread_ident: int | None = None
    discarded: bool = False
    queued_task_count: int = 0
    admitted_task_count: int = 0
    running_task_count: int = 0
    succeeded_task_count: int = 0
    failed_task_count: int = 0
    terminated_task_count: int = 0
    cancelled_task_count: int = 0
    total_task_count: int = 0
    task_records: set[TaskRecord] = field(default_factory=set)

    def is_terminal(self) -> bool:
        """Return whether the pipeline is in a terminal state."""

        return self.state in TerminalPipelineState


@dataclass(eq=False, slots=True)
class TaskRecord:
    """Mutable source-of-truth record for one scheduled task."""

    scheduler: Scheduler
    pipeline_record: PipelineRecord
    task_id: int
    fn: Callable[..., Any]
    resources_required: dict[str, int | float]
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    name: str | None = None
    stage_snapshot: int = 0
    pipeline_enqueue_seq: int = 0
    pipeline_local_task_seq: int = 0
    global_task_submit_seq: int = 0
    state: TaskState = TaskState.QUEUED
    result_value: Any = None
    exception: BaseException | None = None
    ready_seq: int | None = None
    worker_thread_ident: int | None = None
    termination_requested: bool = False
    active_context: TaskContext | None = None

    def priority_key(self) -> TaskPriorityKey:
        """Build the ordering key used by the task priority queue."""

        return TaskPriorityKey(
            neg_stage=-self.stage_snapshot,
            pipeline_enqueue_seq=self.pipeline_enqueue_seq,
            pipeline_local_task_seq=self.pipeline_local_task_seq,
            global_task_submit_seq=self.global_task_submit_seq,
        )

    def is_terminal(self) -> bool:
        """Return whether the task is in a terminal state."""

        return self.state in TerminalTaskState


@dataclass(slots=True)
class SchedulerRuntime:
    """Mutable internal scheduler-wide runtime bookkeeping."""

    state: SchedulerState = SchedulerState.OPEN
    next_pipeline_id: int = 0
    next_pipeline_enqueue_seq: int = 0
    next_task_id: int = 0
    next_global_task_submit_seq: int = 0
    next_ready_seq: int = 0
    admitted_task_count: int = 0
    succeeded_pipeline_count: int = 0
    failed_pipeline_count: int = 0
    cancelled_pipeline_count: int = 0
    succeeded_task_count: int = 0
    failed_task_count: int = 0
    terminated_task_count: int = 0
    cancelled_task_count: int = 0
    pipeline_queue: deque[PipelineQueueEntry] = field(default_factory=deque)
    task_queue: list[TaskQueueEntry] = field(default_factory=list)
    ready_queue: deque[ReadyQueueEntry] = field(default_factory=deque)
    resources_in_use: dict[str, int | float] = field(default_factory=dict)
    pipeline_records: dict[int, PipelineRecord] = field(default_factory=dict)
    task_records: dict[int, TaskRecord] = field(default_factory=dict)
