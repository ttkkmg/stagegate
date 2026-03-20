"""Public immutable snapshot dataclasses and helpers."""

from __future__ import annotations

from dataclasses import dataclass

from ._states import PipelineState


@dataclass(frozen=True, slots=True)
class ResourceSnapshot:
    """Immutable point-in-time view of one configured resource label."""

    label: str
    capacity: int | float
    in_use: int | float
    available: int | float


@dataclass(frozen=True, slots=True)
class TaskCountsSnapshot:
    """Immutable aggregate task counts."""

    queued: int
    admitted: int
    running: int
    succeeded: int
    failed: int
    cancelled: int
    total: int


@dataclass(frozen=True, slots=True)
class PipelineCountsSnapshot:
    """Immutable aggregate pipeline counts."""

    queued: int
    running: int
    succeeded: int
    failed: int
    cancelled: int
    total: int


@dataclass(frozen=True, slots=True)
class SchedulerSnapshot:
    """Immutable scheduler-wide snapshot."""

    shutdown_started: bool
    closed: bool
    pipeline_parallelism: int
    task_parallelism: int
    pipelines: PipelineCountsSnapshot
    tasks: TaskCountsSnapshot
    resources: tuple[ResourceSnapshot, ...]


@dataclass(frozen=True, slots=True)
class PipelineSnapshot:
    """Immutable per-pipeline snapshot."""

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
