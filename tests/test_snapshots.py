from __future__ import annotations

import threading

import stagegate


class BlockingTaskPipeline(stagegate.Pipeline):
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


class SimpleTaskPipeline(stagegate.Pipeline):
    def run(self) -> str:
        task_handle = self.task(lambda: "task-done", resources={"cpu": 1}).run()
        task_handle.result(timeout=1.0)
        return "pipeline-done"


class BlockingPipeline(stagegate.Pipeline):
    def __init__(self, user_name: str) -> None:
        self.name = user_name
        self.started = threading.Event()
        self.release = threading.Event()

    def run(self) -> str:
        self.started.set()
        self.release.wait(timeout=1.0)
        return "pipeline-done"


def test_scheduler_snapshot_on_new_scheduler_is_empty() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2, "mem": 4}, task_parallelism=3)

    snapshot = scheduler.snapshot()

    assert snapshot.shutdown_started is False
    assert snapshot.closed is False
    assert snapshot.pipeline_parallelism == 1
    assert snapshot.task_parallelism == 3
    assert snapshot.pipelines.queued == 0
    assert snapshot.pipelines.running == 0
    assert snapshot.pipelines.succeeded == 0
    assert snapshot.pipelines.failed == 0
    assert snapshot.pipelines.cancelled == 0
    assert snapshot.pipelines.total == 0
    assert snapshot.tasks.total == 0
    assert snapshot.resources == (
        stagegate.ResourceSnapshot(label="cpu", capacity=2, in_use=0, available=2),
        stagegate.ResourceSnapshot(label="mem", capacity=4, in_use=0, available=4),
    )


def test_scheduler_snapshot_on_new_scheduler_without_resources_has_empty_resources() -> (
    None
):
    scheduler = stagegate.Scheduler(task_parallelism=3)

    snapshot = scheduler.snapshot()

    assert snapshot.shutdown_started is False
    assert snapshot.closed is False
    assert snapshot.pipeline_parallelism == 1
    assert snapshot.task_parallelism == 3
    assert snapshot.resources == ()


def test_scheduler_snapshot_without_opt_in_exposes_no_running_pipeline_summaries() -> (
    None
):
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)

    snapshot = scheduler.snapshot()

    assert snapshot.running_pipelines == ()


def test_scheduler_snapshot_tracks_terminal_pipeline_and_task_totals() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)

    handle = scheduler.run_pipeline(SimpleTaskPipeline())

    assert handle.result(timeout=1.0) == "pipeline-done"
    snapshot = scheduler.snapshot()

    assert snapshot.pipelines.succeeded == 1
    assert snapshot.pipelines.total == 1
    assert snapshot.tasks.succeeded == 1
    assert snapshot.tasks.total == 1
    assert snapshot.tasks.queued == 0
    assert snapshot.tasks.admitted == 0
    assert snapshot.tasks.running == 0


def test_pipeline_handle_snapshot_reports_terminal_pipeline_counts() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)

    handle = scheduler.run_pipeline(SimpleTaskPipeline(), name="sample-1")

    assert handle.result(timeout=1.0) == "pipeline-done"
    snapshot = handle.snapshot()

    assert snapshot.pipeline_id == handle._record.pipeline_id
    assert snapshot.name == "sample-1"
    assert snapshot.state == "succeeded"
    assert snapshot.stage_index == 0
    assert snapshot.tasks.queued == 0
    assert snapshot.tasks.admitted == 0
    assert snapshot.tasks.running == 0
    assert snapshot.tasks.succeeded == 1
    assert snapshot.tasks.failed == 0
    assert snapshot.tasks.cancelled == 0
    assert snapshot.tasks.total == 1


def test_pipeline_snapshot_may_show_terminal_pipeline_with_live_child_tasks() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    pipeline = BlockingTaskPipeline()

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) == "pipeline-done"
    assert pipeline.task_handle is not None

    snapshot = handle.snapshot()
    assert snapshot.state == "succeeded"
    assert snapshot.tasks.total == 1
    assert snapshot.tasks.queued + snapshot.tasks.admitted >= 1
    assert snapshot.tasks.succeeded == 0

    pipeline.task_release.set()
    assert pipeline.task_handle.result(timeout=1.0) == "child-done"

    terminal_snapshot = handle.snapshot()
    assert terminal_snapshot.tasks.succeeded == 1
    assert terminal_snapshot.tasks.total == 1
    assert terminal_snapshot.tasks.queued == 0
    assert terminal_snapshot.tasks.admitted == 0
    assert terminal_snapshot.tasks.running == 0


def test_snapshots_remain_usable_after_close() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    handle = scheduler.run_pipeline(SimpleTaskPipeline())

    assert handle.result(timeout=1.0) == "pipeline-done"
    scheduler.close()

    scheduler_snapshot = scheduler.snapshot()
    pipeline_snapshot = handle.snapshot()

    assert scheduler_snapshot.closed is True
    assert scheduler_snapshot.shutdown_started is True
    assert scheduler_snapshot.pipelines.succeeded == 1
    assert scheduler_snapshot.tasks.succeeded == 1
    assert pipeline_snapshot.state == "succeeded"
    assert pipeline_snapshot.tasks.succeeded == 1


def test_scheduler_snapshot_with_running_pipelines_reports_submission_names() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, pipeline_parallelism=1)
    running = BlockingPipeline(user_name="user-owned")
    queued = BlockingPipeline(user_name="other-user-owned")

    running_handle = scheduler.run_pipeline(running, name="submitted-running")
    queued_handle = scheduler.run_pipeline(queued, name="submitted-queued")

    assert running.started.wait(timeout=1.0) is True

    snapshot = scheduler.snapshot(include_running_pipelines=True)

    assert snapshot.running_pipelines == (
        stagegate.RunningPipelineSummary(
            pipeline_id=running_handle._record.pipeline_id,
            name="submitted-running",
        ),
    )

    running.release.set()
    assert running_handle.result(timeout=1.0) == "pipeline-done"
    queued.release.set()
    assert queued_handle.result(timeout=1.0) == "pipeline-done"
