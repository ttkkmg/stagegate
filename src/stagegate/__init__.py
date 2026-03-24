"""Public package namespace for stage-aware local pipeline execution."""

from .exceptions import (
    CancelledError,
    DiscardedHandleError,
    TerminatedError,
    UnknownResourceError,
    UnschedulableTaskError,
)
from .handles import PipelineHandle, TaskHandle
from .pipeline import Pipeline
from .scheduler import Scheduler
from .subprocesses import run_shell, run_subprocess
from .snapshots import (
    PipelineCountsSnapshot,
    PipelineSnapshot,
    ResourceSnapshot,
    RunningPipelineSummary,
    SchedulerSnapshot,
    TaskCountsSnapshot,
)
from ._task_context import terminate_requested
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
    "DiscardedHandleError",
    "TerminatedError",
    "UnknownResourceError",
    "UnschedulableTaskError",
    "terminate_requested",
    "run_subprocess",
    "run_shell",
    "ResourceSnapshot",
    "RunningPipelineSummary",
    "TaskCountsSnapshot",
    "PipelineCountsSnapshot",
    "SchedulerSnapshot",
    "PipelineSnapshot",
]
