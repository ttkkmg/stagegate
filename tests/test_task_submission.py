from __future__ import annotations

import heapq
import threading

import pytest

import stagegate
from stagegate._records import PipelineRecord
from stagegate._states import PipelineState, TaskState


def add_task_to_live_registry(pipeline_record: object, record: object) -> None:
    registry = pipeline_record.task_records
    if hasattr(registry, "add"):
        registry.add(record)
    else:
        registry.append(record)


def bind_running_pipeline(
    scheduler: stagegate.Scheduler,
    *,
    stage_index: int = 0,
) -> tuple[stagegate.Pipeline, object]:
    pipeline = stagegate.Pipeline()
    scheduler._runtime.next_pipeline_id += 1
    scheduler._runtime.next_pipeline_enqueue_seq += 1
    record = PipelineRecord(
        scheduler=scheduler,
        pipeline=pipeline,
        pipeline_id=scheduler._runtime.next_pipeline_id,
        enqueue_seq=scheduler._runtime.next_pipeline_enqueue_seq,
        state=PipelineState.RUNNING,
        stage_index=stage_index,
        coordinator_thread_ident=threading.get_ident(),
    )
    with scheduler._condition:
        scheduler._runtime.pipeline_records[record.pipeline_id] = record
        pipeline._stagegate_record = record
        pipeline._stagegate_scheduler = scheduler
        pipeline._stagegate_submitted = True
        scheduler._notify_state_change_locked()
    return pipeline, record


def test_task_builder_run_rejects_pipeline_not_bound_to_scheduler() -> None:
    pipeline = stagegate.Pipeline()
    builder = pipeline.task(lambda: None, resources={"cpu": 1})

    with pytest.raises(RuntimeError):
        builder.run()


def test_task_builder_run_rejects_pipeline_not_running() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline = stagegate.Pipeline()
    scheduler.run_pipeline(pipeline)
    builder = pipeline.task(lambda: None, resources={"cpu": 1})

    with pytest.raises(RuntimeError):
        builder.run()


def test_task_builder_run_rejects_wrong_thread_even_if_pipeline_running() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline = stagegate.Pipeline()
    handle = scheduler.run_pipeline(pipeline)
    with scheduler._condition:
        handle._record.state = PipelineState.RUNNING
        handle._record.coordinator_thread_ident = 999999
        scheduler._notify_state_change_locked()

    builder = pipeline.task(lambda: None, resources={"cpu": 1})

    with pytest.raises(RuntimeError):
        builder.run()


def test_task_builder_run_rejects_unknown_resource_label() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, _ = bind_running_pipeline(scheduler)
    builder = pipeline.task(lambda: None, resources={"gpu": 1})

    with pytest.raises(stagegate.UnknownResourceError):
        builder.run()


def test_task_builder_run_rejects_unschedulable_single_task() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, _ = bind_running_pipeline(scheduler)
    builder = pipeline.task(lambda: None, resources={"cpu": 3})

    with pytest.raises(stagegate.UnschedulableTaskError):
        builder.run()


@pytest.mark.parametrize(
    "resources", [{"cpu": -1}, {"cpu": "2"}, {"cpu": float("nan")}]
)
def test_task_builder_run_rejects_invalid_resource_amounts(
    resources: dict[str, object],
) -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    pipeline, _ = bind_running_pipeline(scheduler)
    builder = pipeline.task(lambda: None, resources=resources)  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        builder.run()


def test_task_builder_run_returns_task_handle_and_registers_task() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 4, "mem": 16})
    pipeline, pipeline_record = bind_running_pipeline(scheduler, stage_index=3)
    builder = pipeline.task(
        lambda sample_id: sample_id,
        resources={"cpu": 2, "mem": 4},
        args=("S1",),
        kwargs={"flag": True},
        name="task-1",
    )

    handle = builder.run()

    assert isinstance(handle, stagegate.TaskHandle)
    record = handle._record
    assert record.scheduler is scheduler
    assert record.pipeline_record is pipeline_record
    assert record.state is TaskState.QUEUED
    assert record.resources_required == {"cpu": 2, "mem": 4}
    assert record.args == ("S1",)
    assert record.kwargs == {"flag": True}
    assert record.name == "task-1"
    assert record.stage_snapshot == 3
    assert record.pipeline_enqueue_seq == pipeline_record.enqueue_seq
    assert record.pipeline_local_task_seq == 1
    assert record.global_task_submit_seq == 1
    assert scheduler._runtime.task_records[record.task_id] is record
    assert record in pipeline_record.task_records
    assert len(pipeline_record.task_records) == 1
    assert len(scheduler._runtime.task_queue) == 1
    assert scheduler._runtime.task_queue[0].record is record


def test_task_builder_run_assigns_monotonic_local_and_global_sequences() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 4})
    pipeline, pipeline_record = bind_running_pipeline(scheduler, stage_index=2)

    first = pipeline.task(lambda: None, resources={"cpu": 1}).run()
    second = pipeline.task(lambda: None, resources={"cpu": 1}).run()

    assert first._record.pipeline_local_task_seq == 1
    assert second._record.pipeline_local_task_seq == 2
    assert first._record.global_task_submit_seq == 1
    assert second._record.global_task_submit_seq == 2
    assert pipeline_record.next_task_seq == 2


def test_task_builder_run_uses_stage_snapshot_at_submit_time() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 4})
    pipeline, pipeline_record = bind_running_pipeline(scheduler, stage_index=1)
    with scheduler._condition:
        scheduler._reserve_resources_locked({"cpu": 4})

    first = pipeline.task(lambda: None, resources={"cpu": 1}).run()
    with scheduler._condition:
        pipeline_record.stage_index = 4
    second = pipeline.task(lambda: None, resources={"cpu": 1}).run()

    assert first._record.stage_snapshot == 1
    assert second._record.stage_snapshot == 4
    with scheduler._condition:
        scheduler._release_resources_locked({"cpu": 4})
        scheduler._notify_state_change_locked()
        pipeline_record.state = PipelineState.SUCCEEDED
        pipeline_record.coordinator_thread_ident = None
        scheduler._notify_state_change_locked()
    scheduler.close()


def test_task_builder_run_enqueues_priority_key_from_record() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 4})
    pipeline, _ = bind_running_pipeline(scheduler, stage_index=5)

    handle = pipeline.task(lambda: None, resources={"cpu": 1}).run()

    entry = heapq.heappop(scheduler._runtime.task_queue)
    assert entry.record is handle._record
    assert entry.priority == handle._record.priority_key()
