"""Public immutable snapshot dataclasses and helpers."""

from __future__ import annotations

from dataclasses import dataclass

from ._states import PipelineState


@dataclass(frozen=True, slots=True)
class ResourceSnapshot:
    """Immutable point-in-time view of one configured resource label.

    Attributes:
        label: Scheduler-defined resource label.
        capacity: Total configured capacity for the resource.
        in_use: Current reserved amount.
        available: Remaining unreserved amount.
    """

    label: str
    capacity: int | float
    in_use: int | float
    available: int | float


@dataclass(frozen=True, slots=True)
class TaskCountsSnapshot:
    """Immutable aggregate task counts.

    Attributes:
        queued: Number of queued tasks.
        admitted: Number of admitted tasks, including running tasks.
        running: Number of actively running tasks.
        succeeded: Number of succeeded tasks.
        failed: Number of failed tasks.
        terminated: Number of cooperatively terminated tasks.
        cancelled: Number of cancelled tasks.
        total: Total number of tasks represented by the snapshot.
    """

    queued: int
    admitted: int
    running: int
    succeeded: int
    failed: int
    terminated: int
    cancelled: int
    total: int


@dataclass(frozen=True, slots=True)
class PipelineCountsSnapshot:
    """Immutable aggregate pipeline counts.

    Attributes:
        queued: Number of queued pipelines.
        running: Number of running pipelines.
        succeeded: Number of succeeded pipelines.
        failed: Number of failed pipelines.
        cancelled: Number of cancelled pipelines.
        total: Total number of pipelines represented by the snapshot.
    """

    queued: int
    running: int
    succeeded: int
    failed: int
    cancelled: int
    total: int


@dataclass(frozen=True, slots=True)
class SchedulerSnapshot:
    """Immutable scheduler-wide snapshot.

    Attributes:
        shutdown_started: Whether shutdown has begun.
        closed: Whether the scheduler is fully closed.
        pipeline_parallelism: Configured number of pipeline coordinator slots.
        task_parallelism: Effective task parallelism.
        pipelines: Aggregate pipeline counts.
        tasks: Aggregate task counts.
        resources: Deterministically ordered resource snapshots.
    """

    shutdown_started: bool
    closed: bool
    pipeline_parallelism: int
    task_parallelism: int
    pipelines: PipelineCountsSnapshot
    tasks: TaskCountsSnapshot
    resources: tuple[ResourceSnapshot, ...]


@dataclass(frozen=True, slots=True)
class PipelineSnapshot:
    """Immutable per-pipeline snapshot.

    Attributes:
        pipeline_id: Stable local identifier for the pipeline.
        state: Public string representation of the pipeline state.
        stage_index: Current pipeline stage index.
        tasks: Aggregate task counts for that pipeline only.
    """

    pipeline_id: int
    state: str
    stage_index: int
    tasks: TaskCountsSnapshot


def pipeline_state_to_public_name(state: PipelineState) -> str:
    """Map an internal pipeline state to the public snapshot vocabulary."""

    return {
        PipelineState.QUEUED: "queued",
        PipelineState.RUNNING: "running",
        PipelineState.SUCCEEDED: "succeeded",
        PipelineState.FAILED: "failed",
        PipelineState.CANCELLED: "cancelled",
    }[state]
