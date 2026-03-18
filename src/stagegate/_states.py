"""Internal state definitions for scheduler records."""

from __future__ import annotations

from enum import Enum


class SchedulerState(Enum):
    """Internal lifecycle for scheduler shutdown progression."""

    OPEN = "open"
    SHUTTING_DOWN = "shutting_down"
    CLOSED = "closed"


class PipelineState(Enum):
    """Internal pipeline execution states."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskState(Enum):
    """Internal task execution states."""

    QUEUED = "queued"
    READY = "ready"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
