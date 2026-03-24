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


def test_scheduler_snapshot_reports_running_task_counts_for_live_child_task() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    pipeline = BlockingTaskPipeline()
    pipeline_handle = scheduler.run_pipeline(pipeline)

    assert pipeline.task_started.wait(timeout=1.0) is True
    assert pipeline.task_handle is not None

    snapshot = scheduler.snapshot()
    assert snapshot.tasks.queued == 0
    assert snapshot.tasks.admitted == 1
    assert snapshot.tasks.running == 1

    pipeline.task_release.set()
    assert pipeline.task_handle.result(timeout=1.0) == "child-done"
    assert pipeline_handle.result(timeout=1.0) == "pipeline-done"

    terminal_snapshot = scheduler.snapshot()
    assert terminal_snapshot.tasks.queued == 0
    assert terminal_snapshot.tasks.admitted == 0
    assert terminal_snapshot.tasks.running == 0
    assert terminal_snapshot.tasks.succeeded == 1


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


def test_scheduler_snapshot_with_running_pipelines_orders_summaries_by_pipeline_id() -> (
    None
):
    scheduler = stagegate.Scheduler(resources={"cpu": 4}, pipeline_parallelism=3)
    first = BlockingPipeline(user_name="first-user-owned")
    second = BlockingPipeline(user_name="second-user-owned")
    third = BlockingPipeline(user_name="third-user-owned")

    first_handle = scheduler.run_pipeline(first, name="sample-b")
    second_handle = scheduler.run_pipeline(second, name="sample-a")
    third_handle = scheduler.run_pipeline(third, name="sample-c")

    assert first.started.wait(timeout=1.0) is True
    assert second.started.wait(timeout=1.0) is True
    assert third.started.wait(timeout=1.0) is True

    snapshot = scheduler.snapshot(include_running_pipelines=True)

    assert snapshot.pipelines.running == 3
    assert snapshot.running_pipelines == (
        stagegate.RunningPipelineSummary(
            pipeline_id=first_handle._record.pipeline_id,
            name="sample-b",
        ),
        stagegate.RunningPipelineSummary(
            pipeline_id=second_handle._record.pipeline_id,
            name="sample-a",
        ),
        stagegate.RunningPipelineSummary(
            pipeline_id=third_handle._record.pipeline_id,
            name="sample-c",
        ),
    )

    first.release.set()
    second.release.set()
    third.release.set()
    assert first_handle.result(timeout=1.0) == "pipeline-done"
    assert second_handle.result(timeout=1.0) == "pipeline-done"
    assert third_handle.result(timeout=1.0) == "pipeline-done"


def test_scheduler_snapshot_with_running_pipelines_excludes_queued_and_terminal_entries() -> (
    None
):
    scheduler = stagegate.Scheduler(resources={"cpu": 3}, pipeline_parallelism=2)
    first = BlockingPipeline(user_name="first-user-owned")
    second = BlockingPipeline(user_name="second-user-owned")
    queued = BlockingPipeline(user_name="queued-user-owned")

    first_handle = scheduler.run_pipeline(first, name="sample-1")
    second_handle = scheduler.run_pipeline(second, name="sample-2")
    queued_handle = scheduler.run_pipeline(queued, name="sample-queued")

    assert first.started.wait(timeout=1.0) is True
    assert second.started.wait(timeout=1.0) is True

    initial_snapshot = scheduler.snapshot(include_running_pipelines=True)
    assert initial_snapshot.pipelines.queued == 1
    assert initial_snapshot.pipelines.running == 2
    assert initial_snapshot.running_pipelines == (
        stagegate.RunningPipelineSummary(
            pipeline_id=first_handle._record.pipeline_id,
            name="sample-1",
        ),
        stagegate.RunningPipelineSummary(
            pipeline_id=second_handle._record.pipeline_id,
            name="sample-2",
        ),
    )

    first.release.set()
    assert first_handle.result(timeout=1.0) == "pipeline-done"
    assert queued.started.wait(timeout=1.0) is True

    second_snapshot = scheduler.snapshot(include_running_pipelines=True)
    assert second_snapshot.pipelines.queued == 0
    assert second_snapshot.pipelines.running == 2
    assert second_snapshot.pipelines.succeeded == 1
    assert second_snapshot.running_pipelines == (
        stagegate.RunningPipelineSummary(
            pipeline_id=second_handle._record.pipeline_id,
            name="sample-2",
        ),
        stagegate.RunningPipelineSummary(
            pipeline_id=queued_handle._record.pipeline_id,
            name="sample-queued",
        ),
    )

    second.release.set()
    queued.release.set()
    assert second_handle.result(timeout=1.0) == "pipeline-done"
    assert queued_handle.result(timeout=1.0) == "pipeline-done"
