from __future__ import annotations

import threading
import time

import stagegate


class BlockingPipeline(stagegate.Pipeline):
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    def run(self) -> str:
        self.started.set()
        self.release.wait(timeout=5.0)
        return "pipeline-done"


class TaskBurstPipeline(stagegate.Pipeline):
    def __init__(self, *, task_count: int, sleep_seconds: float) -> None:
        self.task_count = task_count
        self.sleep_seconds = sleep_seconds
        self.started = threading.Event()

    def _child(self) -> str:
        time.sleep(self.sleep_seconds)
        return "task-done"

    def run(self) -> str:
        self.started.set()
        pending = {
            self.task(self._child, resources={"cpu": 1}, name=f"task-{index}").run()
            for index in range(self.task_count)
        }
        while pending:
            done, pending = self.wait(
                pending,
                timeout=0.01,
                return_when=stagegate.FIRST_COMPLETED,
            )
            for handle in done:
                assert handle.result() == "task-done"
        return "pipeline-done"


def test_snapshot_with_running_pipelines_returns_without_blocking() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, pipeline_parallelism=2)
    first = BlockingPipeline()
    second = BlockingPipeline()

    first_handle = scheduler.run_pipeline(first, name="sample-1")
    second_handle = scheduler.run_pipeline(second, name="sample-2")

    assert first.started.wait(timeout=1.0) is True
    assert second.started.wait(timeout=1.0) is True

    result: dict[str, object] = {}
    finished = threading.Event()

    def capture_snapshot() -> None:
        try:
            result["snapshot"] = scheduler.snapshot()
        except BaseException as exc:  # pragma: no cover - diagnostic path
            result["error"] = exc
        finally:
            finished.set()

    thread = threading.Thread(target=capture_snapshot, name="snapshot-hunt")
    thread.start()

    try:
        assert finished.wait(timeout=1.0) is True
        assert "error" not in result
        snapshot = result["snapshot"]
        assert isinstance(snapshot, stagegate.SchedulerSnapshot)
        assert snapshot.pipelines.running == 2
        assert snapshot.running_pipelines == (
            stagegate.RunningPipelineSummary(
                pipeline_id=first_handle._record.pipeline_id,
                name="sample-1",
            ),
            stagegate.RunningPipelineSummary(
                pipeline_id=second_handle._record.pipeline_id,
                name="sample-2",
            ),
        )
    finally:
        first.release.set()
        second.release.set()
        thread.join(timeout=1.0)
        assert first_handle.result(timeout=1.0) == "pipeline-done"
        assert second_handle.result(timeout=1.0) == "pipeline-done"


def test_snapshot_with_many_running_pipelines_makes_bounded_progress() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 32}, pipeline_parallelism=16)
    pipelines = [BlockingPipeline() for _ in range(16)]
    handles = {
        scheduler.run_pipeline(pipeline, name=f"sample-{index}")
        for index, pipeline in enumerate(pipelines)
    }

    for pipeline in pipelines:
        assert pipeline.started.wait(timeout=1.0) is True

    completed = 0
    errors: list[BaseException] = []
    finished = threading.Event()

    def capture_many_snapshots() -> None:
        nonlocal completed
        try:
            for _ in range(100):
                snapshot = scheduler.snapshot()
                assert snapshot.pipelines.running == 16
                assert len(snapshot.running_pipelines) == 16
                completed += 1
        except BaseException as exc:  # pragma: no cover - diagnostic path
            errors.append(exc)
        finally:
            finished.set()

    thread = threading.Thread(target=capture_many_snapshots, name="snapshot-many")
    thread.start()

    try:
        assert finished.wait(timeout=2.0) is True
        assert errors == []
        assert completed == 100
    finally:
        for pipeline in pipelines:
            pipeline.release.set()
        thread.join(timeout=1.0)
        for handle in handles:
            assert handle.result(timeout=1.0) == "pipeline-done"


def test_snapshot_under_wait_loop_contention_makes_progress() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 16}, pipeline_parallelism=8)
    pipelines = [BlockingPipeline() for _ in range(8)]
    pending = {
        scheduler.run_pipeline(pipeline, name=f"sample-{index}")
        for index, pipeline in enumerate(pipelines)
    }

    for pipeline in pipelines:
        assert pipeline.started.wait(timeout=1.0) is True

    snapshot_count = 0
    errors: list[BaseException] = []
    stop = threading.Event()

    def snapshot_loop() -> None:
        nonlocal snapshot_count
        try:
            while not stop.is_set():
                scheduler.snapshot()
                snapshot_count += 1
        except BaseException as exc:  # pragma: no cover - diagnostic path
            errors.append(exc)
            stop.set()

    thread = threading.Thread(target=snapshot_loop, name="snapshot-contention")
    thread.start()

    try:
        for pipeline in pipelines:
            time.sleep(0.02)
            pipeline.release.set()

        while pending:
            done, pending = scheduler.wait_pipelines(
                pending,
                timeout=0.01,
                return_when=stagegate.FIRST_COMPLETED,
            )
            for handle in done:
                assert handle.result(timeout=1.0) == "pipeline-done"
    finally:
        stop.set()
        thread.join(timeout=1.0)

    assert errors == []
    assert snapshot_count > 0


def test_snapshot_under_task_scheduler_churn_makes_progress() -> None:
    scheduler = stagegate.Scheduler(
        resources={"cpu": 4},
        pipeline_parallelism=4,
        task_parallelism=4,
    )
    pipelines = [TaskBurstPipeline(task_count=12, sleep_seconds=0.01) for _ in range(4)]
    pending = {
        scheduler.run_pipeline(pipeline, name=f"burst-{index}")
        for index, pipeline in enumerate(pipelines)
    }

    for pipeline in pipelines:
        assert pipeline.started.wait(timeout=1.0) is True

    snapshot_count = 0
    errors: list[BaseException] = []
    stop = threading.Event()

    def snapshot_loop() -> None:
        nonlocal snapshot_count
        try:
            while not stop.is_set():
                scheduler.snapshot()
                snapshot_count += 1
        except BaseException as exc:  # pragma: no cover - diagnostic path
            errors.append(exc)
            stop.set()

    thread = threading.Thread(target=snapshot_loop, name="snapshot-task-churn")
    thread.start()

    try:
        while pending:
            done, pending = scheduler.wait_pipelines(
                pending,
                timeout=0.01,
                return_when=stagegate.FIRST_COMPLETED,
            )
            for handle in done:
                assert handle.result(timeout=1.0) == "pipeline-done"
    finally:
        stop.set()
        thread.join(timeout=1.0)

    assert errors == []
    assert snapshot_count > 0
