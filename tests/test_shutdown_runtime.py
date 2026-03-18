from __future__ import annotations

import threading
import time

import stagegate
from stagegate._states import PipelineState, TaskState


class SubmitAfterShutdownPipeline(stagegate.Pipeline):
    def __init__(self) -> None:
        self.started = threading.Event()
        self.shutdown_may_continue = threading.Event()
        self.finished = threading.Event()
        self.task_handle: stagegate.TaskHandle | None = None

    def run(self) -> str:
        self.started.set()
        self.shutdown_may_continue.wait(timeout=1.0)
        self.task_handle = self.task(
            lambda: "task-after-shutdown", resources={"cpu": 1}
        ).run()
        result = self.task_handle.result(timeout=1.0)
        self.finished.set()
        return result


class BlockingPipeline(stagegate.Pipeline):
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    def run(self) -> str:
        self.started.set()
        self.release.wait(timeout=1.0)
        return "done"


class CloseFromCoordinatorPipeline(stagegate.Pipeline):
    def __init__(self, scheduler: stagegate.Scheduler) -> None:
        self.scheduler = scheduler

    def run(self) -> None:
        self.scheduler.close()


class CloseFromWorkerPipeline(stagegate.Pipeline):
    def __init__(self, scheduler: stagegate.Scheduler) -> None:
        self.scheduler = scheduler

    def run(self) -> None:
        task_handle = self.task(self.scheduler.close, resources={"cpu": 1}).run()
        task_handle.result(timeout=1.0)


def test_shutdown_allows_task_submission_from_running_pipeline() -> None:
    scheduler = stagegate.Scheduler(
        resources={"cpu": 2},
        pipeline_parallelism=1,
        task_parallelism=1,
    )
    pipeline = SubmitAfterShutdownPipeline()
    handle = scheduler.run_pipeline(pipeline)

    assert pipeline.started.wait(timeout=1.0) is True
    scheduler.shutdown()
    pipeline.shutdown_may_continue.set()

    assert handle.result(timeout=1.0) == "task-after-shutdown"
    assert pipeline.finished.is_set() is True
    assert pipeline.task_handle is not None
    assert pipeline.task_handle._record.state is TaskState.SUCCEEDED


def test_shutdown_returns_while_running_pipeline_continues() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, pipeline_parallelism=1)
    pipeline = BlockingPipeline()
    handle = scheduler.run_pipeline(pipeline)

    assert pipeline.started.wait(timeout=1.0) is True

    started_at = time.monotonic()
    scheduler.shutdown()
    elapsed = time.monotonic() - started_at

    assert elapsed < 0.5
    assert scheduler.shutdown_started() is True
    assert scheduler.closed() is False
    assert handle.running() is True

    pipeline.release.set()
    assert handle.result(timeout=1.0) == "done"
    assert scheduler.closed() is False
    scheduler.close()
    assert scheduler.closed() is True


def test_shutdown_cancel_pending_pipelines_leaves_running_pipeline_alive() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, pipeline_parallelism=1)
    running = BlockingPipeline()
    queued = BlockingPipeline()

    running_handle = scheduler.run_pipeline(running)
    queued_handle = scheduler.run_pipeline(queued)

    assert running.started.wait(timeout=1.0) is True

    scheduler.shutdown(cancel_pending_pipelines=True)

    assert running_handle.running() is True
    assert queued_handle.cancelled() is True
    assert queued.started.is_set() is False

    running.release.set()
    assert running_handle.result(timeout=1.0) == "done"


def test_shutdown_can_be_strengthened_with_cancel_pending_and_close() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, pipeline_parallelism=1)
    running = BlockingPipeline()
    queued = BlockingPipeline()

    running_handle = scheduler.run_pipeline(running)
    queued_handle = scheduler.run_pipeline(queued)

    assert running.started.wait(timeout=1.0) is True

    scheduler.shutdown()

    def release_later() -> None:
        time.sleep(0.05)
        running.release.set()

    thread = threading.Thread(target=release_later)
    thread.start()
    try:
        scheduler.close(cancel_pending_pipelines=True)
    finally:
        thread.join()

    assert scheduler.closed() is True
    assert running_handle.done() is True
    assert queued_handle.cancelled() is True


def test_close_returns_with_all_extant_handles_terminal() -> None:
    scheduler = stagegate.Scheduler(
        resources={"cpu": 2},
        pipeline_parallelism=1,
        task_parallelism=1,
    )
    pipeline = SubmitAfterShutdownPipeline()
    handle = scheduler.run_pipeline(pipeline)

    assert pipeline.started.wait(timeout=1.0) is True

    def continue_pipeline() -> None:
        time.sleep(0.05)
        pipeline.shutdown_may_continue.set()

    thread = threading.Thread(target=continue_pipeline)
    thread.start()
    try:
        scheduler.close()
    finally:
        thread.join()

    assert scheduler.closed() is True
    assert handle.done() is True
    assert pipeline.task_handle is not None
    assert pipeline.task_handle.done() is True
    assert handle._record.state is PipelineState.SUCCEEDED


def test_close_is_idempotent_after_scheduler_is_closed() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})

    scheduler.close()
    scheduler.close()

    assert scheduler.shutdown_started() is True
    assert scheduler.closed() is True


def test_close_with_cancel_pending_is_idempotent_after_closed() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, pipeline_parallelism=1)
    running = BlockingPipeline()
    queued = BlockingPipeline()

    running_handle = scheduler.run_pipeline(running)
    queued_handle = scheduler.run_pipeline(queued)
    assert running.started.wait(timeout=1.0) is True
    running.release.set()

    scheduler.close(cancel_pending_pipelines=True)
    scheduler.close(cancel_pending_pipelines=True)

    assert scheduler.closed() is True
    assert running_handle.done() is True
    assert queued_handle.cancelled() is True


def test_close_rejects_call_from_coordinator_thread() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, pipeline_parallelism=1)
    handle = scheduler.run_pipeline(CloseFromCoordinatorPipeline(scheduler))

    try:
        error = handle.exception(timeout=1.0)
    finally:
        scheduler.close()

    assert isinstance(error, RuntimeError)
    assert "runtime thread" in str(error)


def test_close_rejects_call_from_worker_thread() -> None:
    scheduler = stagegate.Scheduler(
        resources={"cpu": 2},
        pipeline_parallelism=1,
        task_parallelism=1,
    )
    handle = scheduler.run_pipeline(CloseFromWorkerPipeline(scheduler))

    try:
        error = handle.exception(timeout=1.0)
    finally:
        scheduler.close()

    assert isinstance(error, RuntimeError)
    assert "runtime thread" in str(error)
