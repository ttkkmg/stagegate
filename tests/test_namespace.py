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
    assert issubclass(stagegate.UnknownResourceError, ValueError)
    assert issubclass(stagegate.UnschedulableTaskError, ValueError)
