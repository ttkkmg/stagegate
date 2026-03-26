from __future__ import annotations

import threading
import time

import pytest

import stagegate
from stagegate._records import PipelineQueueEntry, PipelineRecord
from stagegate._states import PipelineState


def make_pipeline_record(
    scheduler: stagegate.Scheduler,
    *,
    pipeline_id: int,
    enqueue_seq: int,
    state: PipelineState,
) -> PipelineRecord:
    return PipelineRecord(
        scheduler=scheduler,
        pipeline=stagegate.Pipeline(),
        pipeline_id=pipeline_id,
        enqueue_seq=enqueue_seq,
        state=state,
    )


def test_scheduler_observation_predicates_on_new_scheduler() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})

    assert scheduler.shutdown_started() is False
    assert scheduler.closed() is False


def test_scheduler_observation_predicates_on_new_scheduler_without_resources() -> None:
    scheduler = stagegate.Scheduler()

    assert scheduler.shutdown_started() is False
    assert scheduler.closed() is False


def test_scheduler_rejects_invalid_exception_exit_policy() -> None:
    with pytest.raises(ValueError, match="invalid exception_exit_policy"):
        stagegate.Scheduler(exception_exit_policy="invalid")


def test_context_manager_calls_close_on_exit() -> None:
    with stagegate.Scheduler(resources={"cpu": 2}) as scheduler:
        assert scheduler.closed() is False

    assert scheduler.shutdown_started() is True
    assert scheduler.closed() is True


def test_shutdown_only_starts_shutdown() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})

    scheduler.shutdown()

    assert scheduler.shutdown_started() is True
    assert scheduler.closed() is False


def test_pipeline_submission_and_cancel_update_scheduler_queued_pipeline_counter() -> (
    None
):
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    scheduler._ensure_pipeline_runtime_started_locked = lambda: None  # type: ignore[method-assign]

    first = scheduler.run_pipeline(stagegate.Pipeline())
    second = scheduler.run_pipeline(stagegate.Pipeline())

    with scheduler._condition:
        assert scheduler._runtime.queued_pipeline_count == 2
        assert scheduler._runtime.running_pipeline_count == 0

    assert first.cancel() is True
    with scheduler._condition:
        assert scheduler._runtime.queued_pipeline_count == 1
        assert scheduler._runtime.running_pipeline_count == 0

    assert second.cancel() is True
    with scheduler._condition:
        assert scheduler._runtime.queued_pipeline_count == 0
        assert scheduler._runtime.running_pipeline_count == 0


def test_close_closes_empty_scheduler() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})

    scheduler.close()

    assert scheduler.shutdown_started() is True
    assert scheduler.closed() is True


def test_close_closes_empty_scheduler_without_resources() -> None:
    scheduler = stagegate.Scheduler()

    scheduler.close()

    assert scheduler.shutdown_started() is True
    assert scheduler.closed() is True


def test_shutdown_can_be_followed_by_close_later() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    queued = make_pipeline_record(
        scheduler,
        pipeline_id=1,
        enqueue_seq=1,
        state=PipelineState.QUEUED,
    )
    with scheduler._condition:
        scheduler._runtime.pipeline_records[queued.pipeline_id] = queued
        scheduler._runtime.pipeline_queue.append(
            PipelineQueueEntry(enqueue_seq=queued.enqueue_seq, record=queued)
        )

    scheduler.shutdown()
    assert scheduler.closed() is False

    with scheduler._condition:
        queued.state = PipelineState.CANCELLED
        scheduler._notify_state_change_locked()

    scheduler.close()
    assert scheduler.closed() is True


def test_close_blocks_until_live_work_is_gone() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    queued = make_pipeline_record(
        scheduler,
        pipeline_id=1,
        enqueue_seq=1,
        state=PipelineState.QUEUED,
    )
    with scheduler._condition:
        scheduler._runtime.pipeline_records[queued.pipeline_id] = queued
        scheduler._runtime.pipeline_queue.append(
            PipelineQueueEntry(enqueue_seq=queued.enqueue_seq, record=queued)
        )

    finished = threading.Event()

    def close_worker() -> None:
        scheduler.close()
        finished.set()

    thread = threading.Thread(target=close_worker)
    thread.start()
    try:
        time.sleep(0.05)
        assert finished.is_set() is False
        assert scheduler.closed() is False

        with scheduler._condition:
            queued.state = PipelineState.CANCELLED
            scheduler._notify_state_change_locked()

        thread.join(timeout=1.0)
        assert finished.is_set() is True
        assert scheduler.closed() is True
    finally:
        thread.join()


def test_shutdown_does_not_mark_closed_after_live_work_naturally_drains() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    queued = make_pipeline_record(
        scheduler,
        pipeline_id=1,
        enqueue_seq=1,
        state=PipelineState.QUEUED,
    )
    with scheduler._condition:
        scheduler._runtime.pipeline_records[queued.pipeline_id] = queued
        scheduler._runtime.pipeline_queue.append(
            PipelineQueueEntry(enqueue_seq=queued.enqueue_seq, record=queued)
        )

    scheduler.shutdown()
    assert scheduler.closed() is False

    with scheduler._condition:
        queued.state = PipelineState.CANCELLED
        scheduler._notify_state_change_locked()

    assert scheduler.closed() is False


def test_shutdown_cancel_pending_pipelines_cancels_only_queued_pipelines() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    queued = make_pipeline_record(
        scheduler,
        pipeline_id=1,
        enqueue_seq=1,
        state=PipelineState.QUEUED,
    )
    running = make_pipeline_record(
        scheduler,
        pipeline_id=2,
        enqueue_seq=2,
        state=PipelineState.RUNNING,
    )
    with scheduler._condition:
        scheduler._runtime.pipeline_records[queued.pipeline_id] = queued
        scheduler._runtime.pipeline_records[running.pipeline_id] = running
        scheduler._runtime.pipeline_queue.append(
            PipelineQueueEntry(enqueue_seq=queued.enqueue_seq, record=queued)
        )

    scheduler.shutdown(cancel_pending_pipelines=True)

    assert queued.state is PipelineState.CANCELLED
    assert running.state is PipelineState.RUNNING
    assert scheduler.shutdown_started() is True
    assert scheduler.closed() is False


def test_close_joins_started_runtime_threads() -> None:
    scheduler = stagegate.Scheduler(
        resources={"cpu": 2},
        pipeline_parallelism=1,
        task_parallelism=1,
    )

    class SimplePipeline(stagegate.Pipeline):
        def run(self) -> str:
            task_handle = self.task(lambda: "ok", resources={"cpu": 1}).run()
            return task_handle.result()

    handle = scheduler.run_pipeline(SimplePipeline())
    assert handle.result(timeout=1.0) == "ok"

    scheduler.close()

    assert scheduler.closed() is True
    assert scheduler._dispatcher_thread is not None
    assert scheduler._dispatcher_thread.is_alive() is False
    assert all(thread.is_alive() is False for thread in scheduler._worker_threads)
    assert all(thread.is_alive() is False for thread in scheduler._coordinator_threads)
