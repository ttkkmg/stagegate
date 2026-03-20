from __future__ import annotations

import threading
import time

import pytest

import stagegate
from stagegate._records import PipelineRecord, TaskRecord
from stagegate._states import PipelineState, TaskState


def add_task_to_live_registry(pipeline_record: object, record: TaskRecord) -> None:
    registry = pipeline_record.task_records
    if hasattr(registry, "add"):
        registry.add(record)
    else:
        registry.append(record)


class BlockingPipeline(stagegate.Pipeline):
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    def run(self) -> str:
        self.started.set()
        self.release.wait(timeout=1.0)
        return "done"


def bind_pipeline(pipeline: stagegate.Pipeline, record: PipelineRecord) -> None:
    pipeline._stagegate_record = record
    pipeline._stagegate_scheduler = record.scheduler
    pipeline._stagegate_submitted = True


def make_bound_pipeline(
    scheduler: stagegate.Scheduler,
    *,
    pipeline_id: int = 1,
    state: PipelineState = PipelineState.RUNNING,
) -> tuple[stagegate.Pipeline, PipelineRecord]:
    pipeline = stagegate.Pipeline()
    record = PipelineRecord(
        scheduler=scheduler,
        pipeline=pipeline,
        pipeline_id=pipeline_id,
        enqueue_seq=pipeline_id,
        state=state,
        coordinator_thread_ident=(
            threading.get_ident() if state is PipelineState.RUNNING else None
        ),
    )
    bind_pipeline(pipeline, record)
    scheduler._runtime.pipeline_records[record.pipeline_id] = record
    return pipeline, record


def make_pipeline_handle(
    scheduler: stagegate.Scheduler,
    *,
    pipeline_id: int,
    state: PipelineState,
) -> stagegate.PipelineHandle:
    pipeline, record = make_bound_pipeline(
        scheduler,
        pipeline_id=pipeline_id,
        state=state,
    )
    del pipeline
    return stagegate.PipelineHandle(record)


def make_task_handle(
    scheduler: stagegate.Scheduler,
    *,
    pipeline: stagegate.Pipeline,
    pipeline_record: PipelineRecord,
    task_id: int,
    state: TaskState,
) -> stagegate.TaskHandle:
    record = TaskRecord(
        scheduler=scheduler,
        pipeline_record=pipeline_record,
        task_id=task_id,
        fn=lambda: None,
        resources_required={"cpu": 1},
        state=state,
    )
    add_task_to_live_registry(pipeline_record, record)
    scheduler._runtime.task_records[record.task_id] = record
    return stagegate.TaskHandle(record)


def test_run_pipeline_rejects_wrong_type() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})

    with pytest.raises(TypeError):
        scheduler.run_pipeline(object())  # type: ignore[arg-type]


def test_run_pipeline_returns_pipeline_handle_and_registers_queued_record() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline = BlockingPipeline()

    handle = scheduler.run_pipeline(pipeline)

    assert isinstance(handle, stagegate.PipelineHandle)
    assert handle._record.pipeline is pipeline
    assert (
        scheduler._runtime.pipeline_records[handle._record.pipeline_id]
        is handle._record
    )
    assert pipeline._stagegate_record is handle._record
    assert pipeline._stagegate_scheduler is scheduler
    assert pipeline._stagegate_submitted is True
    assert handle._record.state in (PipelineState.QUEUED, PipelineState.RUNNING)
    pipeline.release.set()
    assert handle.result(timeout=1.0) == "done"


def test_run_pipeline_rejects_reusing_same_pipeline_instance() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline = stagegate.Pipeline()

    scheduler.run_pipeline(pipeline)

    with pytest.raises(RuntimeError):
        scheduler.run_pipeline(pipeline)


def test_run_pipeline_rejects_reusing_cancelled_pipeline_instance() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, pipeline_parallelism=1)
    blocker = BlockingPipeline()
    scheduler.run_pipeline(blocker)
    assert blocker.started.wait(timeout=1.0) is True

    pipeline = BlockingPipeline()
    handle = scheduler.run_pipeline(pipeline)

    assert handle.cancel() is True
    with pytest.raises(RuntimeError):
        scheduler.run_pipeline(pipeline)
    blocker.release.set()


def test_run_pipeline_rejects_after_shutdown_starts() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    scheduler.shutdown()

    with pytest.raises(RuntimeError):
        scheduler.run_pipeline(stagegate.Pipeline())


def test_wait_pipelines_rejects_empty_iterable() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})

    with pytest.raises(ValueError):
        scheduler.wait_pipelines([])


def test_wait_pipelines_rejects_wrong_handle_type() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, pipeline_record = make_bound_pipeline(scheduler)
    task_handle = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=pipeline_record,
        task_id=1,
        state=TaskState.QUEUED,
    )

    with pytest.raises(TypeError):
        scheduler.wait_pipelines([task_handle])  # type: ignore[list-item]


def test_wait_pipelines_rejects_unhashable_wrong_handle_type_stably() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})

    with pytest.raises(TypeError):
        scheduler.wait_pipelines([{}])  # type: ignore[list-item]


def test_wait_pipelines_rejects_foreign_pipeline_handle() -> None:
    scheduler1 = stagegate.Scheduler(resources={"cpu": 2})
    scheduler2 = stagegate.Scheduler(resources={"cpu": 2})
    handle1 = make_pipeline_handle(
        scheduler1,
        pipeline_id=1,
        state=PipelineState.QUEUED,
    )
    handle2 = make_pipeline_handle(
        scheduler2,
        pipeline_id=1,
        state=PipelineState.QUEUED,
    )

    with pytest.raises(ValueError):
        scheduler1.wait_pipelines([handle1, handle2])


def test_wait_pipelines_rejects_invalid_return_when() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    handle = make_pipeline_handle(
        scheduler,
        pipeline_id=1,
        state=PipelineState.QUEUED,
    )

    with pytest.raises(ValueError):
        scheduler.wait_pipelines([handle], return_when="INVALID")


def test_wait_pipelines_deduplicates_handles() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    handle = make_pipeline_handle(
        scheduler,
        pipeline_id=1,
        state=PipelineState.SUCCEEDED,
    )

    done, pending = scheduler.wait_pipelines([handle, handle])

    assert done == {handle}
    assert pending == set()


def test_wait_pipelines_timeout_zero_returns_immediate_poll() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    handle = make_pipeline_handle(
        scheduler,
        pipeline_id=1,
        state=PipelineState.QUEUED,
    )

    done, pending = scheduler.wait_pipelines([handle], timeout=0)

    assert done == set()
    assert pending == {handle}


def test_wait_pipelines_first_completed_returns_immediately_for_cancelled() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    cancelled = make_pipeline_handle(
        scheduler,
        pipeline_id=1,
        state=PipelineState.CANCELLED,
    )
    queued = make_pipeline_handle(
        scheduler,
        pipeline_id=2,
        state=PipelineState.QUEUED,
    )

    done, pending = scheduler.wait_pipelines(
        [cancelled, queued], return_when=stagegate.FIRST_COMPLETED
    )

    assert done == {cancelled}
    assert pending == {queued}


def test_wait_pipelines_first_exception_does_not_trigger_for_cancelled_alone() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    cancelled = make_pipeline_handle(
        scheduler,
        pipeline_id=1,
        state=PipelineState.CANCELLED,
    )
    queued = make_pipeline_handle(
        scheduler,
        pipeline_id=2,
        state=PipelineState.QUEUED,
    )

    done, pending = scheduler.wait_pipelines(
        [cancelled, queued], return_when=stagegate.FIRST_EXCEPTION, timeout=0
    )

    assert done == {cancelled}
    assert pending == {queued}


def test_wait_pipelines_waits_until_failure_for_first_exception() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    failed = make_pipeline_handle(
        scheduler,
        pipeline_id=1,
        state=PipelineState.QUEUED,
    )
    pending_handle = make_pipeline_handle(
        scheduler,
        pipeline_id=2,
        state=PipelineState.QUEUED,
    )

    def worker() -> None:
        time.sleep(0.02)
        with scheduler._condition:
            failed._record.state = PipelineState.FAILED
            failed._record.exception = RuntimeError("boom")
            scheduler._notify_state_change_locked()

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        done, pending = scheduler.wait_pipelines(
            [failed, pending_handle], return_when=stagegate.FIRST_EXCEPTION, timeout=1.0
        )
    finally:
        thread.join()

    assert done == {failed}
    assert pending == {pending_handle}


def test_pipeline_wait_rejects_empty_iterable() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, _ = make_bound_pipeline(scheduler)

    with pytest.raises(ValueError):
        pipeline.wait([])


def test_pipeline_wait_rejects_wrong_handle_type() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, _ = make_bound_pipeline(scheduler)
    pipeline_handle = make_pipeline_handle(
        scheduler,
        pipeline_id=2,
        state=PipelineState.QUEUED,
    )

    with pytest.raises(TypeError):
        pipeline.wait([pipeline_handle])  # type: ignore[list-item]


def test_pipeline_wait_rejects_unhashable_wrong_handle_type_stably() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, _ = make_bound_pipeline(scheduler)

    with pytest.raises(TypeError):
        pipeline.wait([{}])  # type: ignore[list-item]


def test_pipeline_wait_rejects_foreign_task_handle() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline1, record1 = make_bound_pipeline(scheduler, pipeline_id=1)
    pipeline2, record2 = make_bound_pipeline(scheduler, pipeline_id=2)
    handle1 = make_task_handle(
        scheduler,
        pipeline=pipeline1,
        pipeline_record=record1,
        task_id=1,
        state=TaskState.QUEUED,
    )
    handle2 = make_task_handle(
        scheduler,
        pipeline=pipeline2,
        pipeline_record=record2,
        task_id=2,
        state=TaskState.QUEUED,
    )

    with pytest.raises(ValueError):
        pipeline1.wait([handle1, handle2])


def test_pipeline_wait_rejects_invalid_return_when() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, record = make_bound_pipeline(scheduler)
    handle = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=record,
        task_id=1,
        state=TaskState.QUEUED,
    )

    with pytest.raises(ValueError):
        pipeline.wait([handle], return_when="INVALID")


def test_pipeline_wait_rejects_when_pipeline_is_not_running() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, record = make_bound_pipeline(scheduler, state=PipelineState.QUEUED)
    handle = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=record,
        task_id=1,
        state=TaskState.QUEUED,
    )

    with pytest.raises(RuntimeError):
        pipeline.wait([handle])


def test_pipeline_wait_rejects_from_wrong_thread() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, record = make_bound_pipeline(scheduler, state=PipelineState.RUNNING)
    record.coordinator_thread_ident = 999999
    handle = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=record,
        task_id=1,
        state=TaskState.QUEUED,
    )

    with pytest.raises(RuntimeError):
        pipeline.wait([handle])


def test_pipeline_wait_deduplicates_handles() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, record = make_bound_pipeline(scheduler)
    handle = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=record,
        task_id=1,
        state=TaskState.SUCCEEDED,
    )

    done, pending = pipeline.wait([handle, handle])

    assert done == {handle}
    assert pending == set()


def test_pipeline_wait_timeout_zero_returns_immediate_poll() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, record = make_bound_pipeline(scheduler)
    handle = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=record,
        task_id=1,
        state=TaskState.QUEUED,
    )

    done, pending = pipeline.wait([handle], timeout=0)

    assert done == set()
    assert pending == {handle}


def test_pipeline_wait_first_completed_returns_immediately_for_cancelled() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, record = make_bound_pipeline(scheduler)
    cancelled = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=record,
        task_id=1,
        state=TaskState.CANCELLED,
    )
    queued = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=record,
        task_id=2,
        state=TaskState.QUEUED,
    )

    done, pending = pipeline.wait(
        [cancelled, queued], return_when=stagegate.FIRST_COMPLETED
    )

    assert done == {cancelled}
    assert pending == {queued}


def test_pipeline_wait_first_exception_does_not_trigger_for_cancelled_alone() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, record = make_bound_pipeline(scheduler)
    cancelled = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=record,
        task_id=1,
        state=TaskState.CANCELLED,
    )
    queued = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=record,
        task_id=2,
        state=TaskState.QUEUED,
    )

    done, pending = pipeline.wait(
        [cancelled, queued], return_when=stagegate.FIRST_EXCEPTION, timeout=0
    )

    assert done == {cancelled}
    assert pending == {queued}


def test_pipeline_wait_waits_until_failure_for_first_exception() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, record = make_bound_pipeline(scheduler)
    failed = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=record,
        task_id=1,
        state=TaskState.QUEUED,
    )
    queued = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=record,
        task_id=2,
        state=TaskState.QUEUED,
    )

    def worker() -> None:
        time.sleep(0.02)
        with scheduler._condition:
            failed._record.state = TaskState.FAILED
            failed._record.exception = RuntimeError("boom")
            scheduler._notify_state_change_locked()

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        done, pending = pipeline.wait(
            [failed, queued], return_when=stagegate.FIRST_EXCEPTION, timeout=1.0
        )
    finally:
        thread.join()

    assert done == {failed}
    assert pending == {queued}


def test_pipeline_wait_first_exception_ignores_cancel_until_failure_arrives() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, record = make_bound_pipeline(scheduler)
    cancelled = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=record,
        task_id=1,
        state=TaskState.READY,
    )
    cancelled._record.ready_seq = 1
    failed = make_task_handle(
        scheduler,
        pipeline=pipeline,
        pipeline_record=record,
        task_id=2,
        state=TaskState.RUNNING,
    )

    def worker() -> None:
        time.sleep(0.02)
        with scheduler._condition:
            scheduler._cancel_task_if_possible_locked(cancelled._record)
        time.sleep(0.02)
        with scheduler._condition:
            failed._record.state = TaskState.FAILED
            failed._record.exception = RuntimeError("boom")
            scheduler._notify_state_change_locked()

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        done, pending = pipeline.wait(
            [cancelled, failed], return_when=stagegate.FIRST_EXCEPTION, timeout=1.0
        )
    finally:
        thread.join()

    assert done == {cancelled, failed}
    assert pending == set()
