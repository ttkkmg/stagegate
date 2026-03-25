from __future__ import annotations

import stagegate


def test_top_level_namespace_exports_expected_symbols() -> None:
    assert stagegate.Scheduler is not None
    assert stagegate.Pipeline is not None
    assert stagegate.TaskHandle is not None
    assert stagegate.PipelineHandle is not None
    assert stagegate.FIRST_COMPLETED == "FIRST_COMPLETED"
    assert stagegate.FIRST_EXCEPTION == "FIRST_EXCEPTION"
    assert stagegate.ALL_COMPLETED == "ALL_COMPLETED"
    assert issubclass(stagegate.CancelledError, Exception)
    assert issubclass(stagegate.DiscardedHandleError, RuntimeError)
    assert issubclass(stagegate.TerminatedError, Exception)
    assert issubclass(stagegate.UnknownResourceError, ValueError)
    assert issubclass(stagegate.UnschedulableTaskError, ValueError)
    assert stagegate.terminate_requested is not None
    assert stagegate.terminate_tracked_subprocesses is not None
    assert stagegate.run_subprocess is not None
    assert stagegate.run_shell is not None
    assert stagegate.ResourceSnapshot is not None
    assert stagegate.RunningPipelineSummary is not None
    assert stagegate.TaskCountsSnapshot is not None
    assert stagegate.PipelineCountsSnapshot is not None
    assert stagegate.SchedulerSnapshot is not None
    assert stagegate.PipelineSnapshot is not None
