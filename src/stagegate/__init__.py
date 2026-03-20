"""Public package namespace for stage-aware local pipeline execution."""

from .exceptions import CancelledError, UnknownResourceError, UnschedulableTaskError
from .handles import PipelineHandle, TaskHandle
from .pipeline import Pipeline
from .scheduler import Scheduler
from .snapshots import (
    PipelineCountsSnapshot,
    PipelineSnapshot,
    ResourceSnapshot,
    SchedulerSnapshot,
    TaskCountsSnapshot,
)
from .wait import ALL_COMPLETED, FIRST_COMPLETED, FIRST_EXCEPTION

__all__ = [
    "Scheduler",
    "Pipeline",
    "TaskHandle",
    "PipelineHandle",
    "FIRST_COMPLETED",
    "FIRST_EXCEPTION",
    "ALL_COMPLETED",
    "CancelledError",
    "UnknownResourceError",
    "UnschedulableTaskError",
    "ResourceSnapshot",
    "TaskCountsSnapshot",
    "PipelineCountsSnapshot",
    "SchedulerSnapshot",
    "PipelineSnapshot",
]
