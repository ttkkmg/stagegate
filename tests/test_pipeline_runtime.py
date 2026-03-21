from __future__ import annotations

import threading
import time

import pytest

import stagegate
from stagegate._states import PipelineState


class TaskResultPipeline(stagegate.Pipeline):
    def __init__(self) -> None:
        self.coordinator_ident: int | None = None

    def run(self) -> str:
        self.coordinator_ident = threading.get_ident()
        self.stage_forward()
        handle = self.task(lambda: "task-done", resources={"cpu": 1}).run()
        return handle.result(timeout=1.0)


class StageCapturePipeline(stagegate.Pipeline):
    def __init__(self) -> None:
        self.first_stage_snapshot: int | None = None
        self.second_stage_snapshot: int | None = None

    def run(self) -> None:
        first = self.task(lambda: None, resources={"cpu": 1}).run()
        self.stage_forward()
        second = self.task(lambda: None, resources={"cpu": 1}).run()
        self.first_stage_snapshot = first._record.stage_snapshot
        self.second_stage_snapshot = second._record.stage_snapshot
        first.result(timeout=1.0)
        second.result(timeout=1.0)


class BlockingPipeline(stagegate.Pipeline):
    def __init__(self, name: str, order: list[str]) -> None:
        self.name = name
        self.order = order
        self.started = threading.Event()
        self.release = threading.Event()

    def run(self) -> str:
        self.order.append(self.name)
        self.started.set()
        self.release.wait(timeout=1.0)
        return self.name


class FailingPipeline(stagegate.Pipeline):
    def run(self) -> None:
        raise ValueError("boom")


class DetachedTaskPipeline(stagegate.Pipeline):
    def __init__(self) -> None:
        self.task_started = threading.Event()
        self.task_release = threading.Event()
        self.task_handle: stagegate.TaskHandle | None = None

    def run(self) -> str:
        def child() -> str:
            self.task_started.set()
            self.task_release.wait(timeout=1.0)
            return "child-done"

        self.task_handle = self.task(child, resources={"cpu": 1}).run()
        return "pipeline-done"


def test_pipeline_run_executes_on_coordinator_thread_and_returns_result() -> None:
    scheduler = stagegate.Scheduler(
        resources={"cpu": 2},
        pipeline_parallelism=1,
        task_parallelism=1,
    )
    pipeline = TaskResultPipeline()

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) == "task-done"
    assert pipeline.coordinator_ident is not None
    assert pipeline.coordinator_ident != threading.get_ident()
    assert handle._record.state is PipelineState.SUCCEEDED
    assert handle._record.coordinator_thread_ident is None
    assert handle._record.stage_index == 1
    with scheduler._condition:
        assert handle._record.pipeline_id not in scheduler._runtime.pipeline_records


def test_stage_forward_changes_stage_snapshot_for_later_tasks() -> None:
    scheduler = stagegate.Scheduler(
        resources={"cpu": 2},
        pipeline_parallelism=1,
        task_parallelism=1,
    )
    pipeline = StageCapturePipeline()

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) is None
    assert pipeline.first_stage_snapshot == 0
    assert pipeline.second_stage_snapshot == 1


def test_pipeline_parallelism_one_starts_pipelines_in_fifo_order() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, pipeline_parallelism=1)
    order: list[str] = []
    first = BlockingPipeline("first", order)
    second = BlockingPipeline("second", order)

    first_handle = scheduler.run_pipeline(first)
    second_handle = scheduler.run_pipeline(second)

    assert first.started.wait(timeout=1.0) is True
    time.sleep(0.05)
    assert second.started.is_set() is False

    first.release.set()
    assert first_handle.result(timeout=1.0) == "first"
    assert second.started.wait(timeout=1.0) is True
    second.release.set()
    assert second_handle.result(timeout=1.0) == "second"
    assert order == ["first", "second"]


def test_cancelled_queued_pipeline_is_skipped_by_coordinator() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, pipeline_parallelism=1)
    order: list[str] = []
    first = BlockingPipeline("first", order)
    second = BlockingPipeline("second", order)

    first_handle = scheduler.run_pipeline(first)
    second_handle = scheduler.run_pipeline(second)

    assert first.started.wait(timeout=1.0) is True
    assert second_handle.cancel() is True

    first.release.set()
    assert first_handle.result(timeout=1.0) == "first"
    with pytest.raises(stagegate.CancelledError):
        second_handle.result(timeout=0)
    assert second.started.is_set() is False
    assert order == ["first"]


def test_pipeline_failure_sets_failed_state_and_preserves_exception() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, pipeline_parallelism=1)
    handle = scheduler.run_pipeline(FailingPipeline())

    with pytest.raises(ValueError, match="boom"):
        handle.result(timeout=1.0)

    error = handle.exception(timeout=0)
    assert isinstance(error, ValueError)
    assert str(error) == "boom"
    assert handle._record.state is PipelineState.FAILED
    with scheduler._condition:
        assert handle._record.pipeline_id not in scheduler._runtime.pipeline_records


def test_terminal_pipeline_may_still_have_live_child_tasks() -> None:
    scheduler = stagegate.Scheduler(
        resources={"cpu": 2},
        pipeline_parallelism=1,
        task_parallelism=1,
    )
    pipeline = DetachedTaskPipeline()

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) == "pipeline-done"
    assert pipeline.task_handle is not None
    with scheduler._condition:
        assert handle._record.state is PipelineState.SUCCEEDED
        assert pipeline.task_handle.done() is False
        assert handle._record.task_records
        assert handle._record.pipeline_id not in scheduler._runtime.pipeline_records

    pipeline.task_release.set()
    assert pipeline.task_handle.result(timeout=1.0) == "child-done"


def test_discarded_terminal_pipeline_may_still_have_live_child_tasks() -> None:
    scheduler = stagegate.Scheduler(
        resources={"cpu": 2},
        pipeline_parallelism=1,
        task_parallelism=1,
    )
    pipeline = DetachedTaskPipeline()

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) == "pipeline-done"
    assert pipeline.task_handle is not None

    with scheduler._condition:
        assert handle._record.task_records

    handle.discard()

    with scheduler._condition:
        assert handle._record.pipeline is None
        assert handle._record.result_value is None
        assert handle._record.exception is None
        assert handle._record.task_records

    with pytest.raises(stagegate.DiscardedHandleError):
        handle.snapshot()

    pipeline.task_release.set()
    assert pipeline.task_handle.result(timeout=1.0) == "child-done"
    with scheduler._condition:
        assert not handle._record.task_records


def test_stage_forward_rejects_outside_active_control_context() -> None:
    pipeline = stagegate.Pipeline()

    with pytest.raises(RuntimeError):
        pipeline.stage_forward()
