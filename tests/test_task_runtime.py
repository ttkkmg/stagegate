from __future__ import annotations

import heapq
import threading
import time

import pytest

import stagegate
from stagegate._records import PipelineRecord
from stagegate._records import ReadyQueueEntry, TaskQueueEntry, TaskRecord
from stagegate._states import PipelineState, TaskState


def add_task_to_live_registry(pipeline_record: object, record: TaskRecord) -> None:
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


def make_queued_task_record(
    scheduler: stagegate.Scheduler,
    *,
    pipeline_record,
    task_id: int,
    cpu: int,
    stage_snapshot: int,
    pipeline_local_task_seq: int,
    global_task_submit_seq: int,
) -> TaskRecord:
    record = TaskRecord(
        scheduler=scheduler,
        pipeline_record=pipeline_record,
        task_id=task_id,
        fn=lambda: None,
        resources_required={"cpu": cpu},
        stage_snapshot=stage_snapshot,
        pipeline_enqueue_seq=pipeline_record.enqueue_seq,
        pipeline_local_task_seq=pipeline_local_task_seq,
        global_task_submit_seq=global_task_submit_seq,
        state=TaskState.QUEUED,
    )
    add_task_to_live_registry(pipeline_record, record)
    scheduler._runtime.task_records[task_id] = record
    scheduler._runtime.queued_task_count += 1
    heapq.heappush(scheduler._runtime.task_queue, TaskQueueEntry.from_record(record))
    return record


class BlockingChildTaskPipeline(stagegate.Pipeline):
    def __init__(self) -> None:
        self.task_started = threading.Event()
        self.task_release = threading.Event()
        self.task_handle: stagegate.TaskHandle | None = None

    def run(self) -> str:
        def child() -> str:
            self.task_started.set()
            self.task_release.wait(timeout=1.0)
            return "task-done"

        self.task_handle = self.task(child, resources={"cpu": 1}).run()
        return "pipeline-done"


def test_task_parallelism_none_defaults_to_one_worker() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    assert scheduler._effective_task_parallelism == 1


def test_task_record_live_registry_uses_identity_membership() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    _, pipeline_record = bind_running_pipeline(scheduler)

    first = TaskRecord(
        scheduler=scheduler,
        pipeline_record=pipeline_record,
        task_id=1,
        fn=lambda: None,
        resources_required={"cpu": 1},
    )
    second = TaskRecord(
        scheduler=scheduler,
        pipeline_record=pipeline_record,
        task_id=1,
        fn=lambda: None,
        resources_required={"cpu": 1},
    )

    registry = {first}
    first.state = TaskState.RUNNING

    assert first in registry
    assert second not in registry


def test_resource_ledger_reserve_and_release_helpers() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 4, "mem": 8})

    with scheduler._condition:
        assert scheduler._resources_fit_locked({"cpu": 3, "mem": 8}) is True
        scheduler._reserve_resources_locked({"cpu": 2, "mem": 3})
        assert scheduler._runtime.resources_in_use == {"cpu": 2, "mem": 3}
        assert scheduler._resources_fit_locked({"cpu": 3}) is False
        scheduler._release_resources_locked({"cpu": 2, "mem": 3})
        assert scheduler._runtime.resources_in_use == {"cpu": 0, "mem": 0}


def test_peek_top_live_task_skips_stale_entries() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 4})
    _, pipeline_record = bind_running_pipeline(scheduler)

    with scheduler._condition:
        stale = make_queued_task_record(
            scheduler,
            pipeline_record=pipeline_record,
            task_id=1,
            cpu=1,
            stage_snapshot=5,
            pipeline_local_task_seq=1,
            global_task_submit_seq=1,
        )
        live = make_queued_task_record(
            scheduler,
            pipeline_record=pipeline_record,
            task_id=2,
            cpu=1,
            stage_snapshot=4,
            pipeline_local_task_seq=2,
            global_task_submit_seq=2,
        )
        stale.state = TaskState.CANCELLED

        top = scheduler._peek_top_live_task_locked()

        assert top is live
        assert scheduler._runtime.task_queue[0].record is live


def test_pop_next_ready_task_skips_stale_entries() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 4})
    _, pipeline_record = bind_running_pipeline(scheduler)

    with scheduler._condition:
        stale = TaskRecord(
            scheduler=scheduler,
            pipeline_record=pipeline_record,
            task_id=1,
            fn=lambda: None,
            resources_required={"cpu": 1},
            state=TaskState.CANCELLED,
        )
        live = TaskRecord(
            scheduler=scheduler,
            pipeline_record=pipeline_record,
            task_id=2,
            fn=lambda: None,
            resources_required={"cpu": 1},
            state=TaskState.READY,
        )
        scheduler._runtime.ready_queue.append(ReadyQueueEntry(1, stale))
        scheduler._runtime.ready_queue.append(ReadyQueueEntry(2, live))

        popped = scheduler._pop_next_ready_task_locked()

        assert popped is live
        assert len(scheduler._runtime.ready_queue) == 0


def test_dispatch_once_moves_task_to_ready_and_reserves_resources() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 4})
    _, pipeline_record = bind_running_pipeline(scheduler)

    with scheduler._condition:
        record = make_queued_task_record(
            scheduler,
            pipeline_record=pipeline_record,
            task_id=1,
            cpu=2,
            stage_snapshot=1,
            pipeline_local_task_seq=1,
            global_task_submit_seq=1,
        )
        scheduler._runtime.running_task_count = 0

        changed = scheduler._dispatch_one_task_locked()

        assert changed is True
        assert record.state is TaskState.READY
        assert record.ready_seq == 1
        assert scheduler._runtime.queued_task_count == 0
        assert scheduler._runtime.admitted_task_count == 1
        assert scheduler._runtime.running_task_count == 0
        assert scheduler._runtime.resources_in_use["cpu"] == 2
        assert scheduler._runtime.ready_queue[0].record is record


def test_dispatch_once_enforces_strict_head_of_line_blocking() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2})
    _, pipeline_record = bind_running_pipeline(scheduler)

    with scheduler._condition:
        top = make_queued_task_record(
            scheduler,
            pipeline_record=pipeline_record,
            task_id=1,
            cpu=2,
            stage_snapshot=10,
            pipeline_local_task_seq=1,
            global_task_submit_seq=1,
        )
        lower = make_queued_task_record(
            scheduler,
            pipeline_record=pipeline_record,
            task_id=2,
            cpu=1,
            stage_snapshot=1,
            pipeline_local_task_seq=2,
            global_task_submit_seq=2,
        )
        scheduler._reserve_resources_locked({"cpu": 1})

        changed = scheduler._dispatch_one_task_locked()

        assert changed is False
        assert top.state is TaskState.QUEUED
        assert lower.state is TaskState.QUEUED
        assert len(scheduler._runtime.ready_queue) == 0


def test_task_priority_prefers_stage_then_pipeline_fifo_then_local_fifo() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 4})
    _, older_pipeline = bind_running_pipeline(scheduler)
    _, newer_pipeline = bind_running_pipeline(scheduler)

    with scheduler._condition:
        older_first = make_queued_task_record(
            scheduler,
            pipeline_record=older_pipeline,
            task_id=1,
            cpu=1,
            stage_snapshot=5,
            pipeline_local_task_seq=1,
            global_task_submit_seq=2,
        )
        older_second = make_queued_task_record(
            scheduler,
            pipeline_record=older_pipeline,
            task_id=2,
            cpu=1,
            stage_snapshot=5,
            pipeline_local_task_seq=2,
            global_task_submit_seq=3,
        )
        newer_first = make_queued_task_record(
            scheduler,
            pipeline_record=newer_pipeline,
            task_id=3,
            cpu=1,
            stage_snapshot=5,
            pipeline_local_task_seq=1,
            global_task_submit_seq=1,
        )
        lower_stage = make_queued_task_record(
            scheduler,
            pipeline_record=newer_pipeline,
            task_id=4,
            cpu=1,
            stage_snapshot=1,
            pipeline_local_task_seq=2,
            global_task_submit_seq=4,
        )

        popped = [
            scheduler._pop_top_live_task_locked(),
            scheduler._pop_top_live_task_locked(),
            scheduler._pop_top_live_task_locked(),
            scheduler._pop_top_live_task_locked(),
        ]

    assert popped == [older_first, older_second, newer_first, lower_stage]


def test_dispatcher_thread_respects_head_of_line_blocking() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    pipeline, pipeline_record = bind_running_pipeline(scheduler)
    with scheduler._condition:
        scheduler._reserve_resources_locked({"cpu": 1})

    top = pipeline.task(lambda: None, resources={"cpu": 2}).run()
    lower = pipeline.task(lambda: None, resources={"cpu": 1}).run()

    time.sleep(0.05)

    assert top._record.state is TaskState.QUEUED
    assert lower._record.state is TaskState.QUEUED

    with scheduler._condition:
        scheduler._release_resources_locked({"cpu": 1})
        scheduler._condition.notify_all()
    assert top.result(timeout=1.0) is None
    assert lower.result(timeout=1.0) is None
    with scheduler._condition:
        pipeline_record.state = PipelineState.SUCCEEDED
        pipeline_record.coordinator_thread_ident = None
        scheduler._notify_state_change_locked()
    scheduler.close()


def test_worker_executes_task_and_releases_resources() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    pipeline, _ = bind_running_pipeline(scheduler)

    handle = pipeline.task(lambda: "done", resources={"cpu": 1}).run()

    assert handle.result(timeout=1.0) == "done"
    with scheduler._condition:
        assert handle._record.state is TaskState.SUCCEEDED
        assert scheduler._runtime.resources_in_use["cpu"] == 0
        assert scheduler._runtime.admitted_task_count == 0


def test_worker_updates_scheduler_running_task_counter_during_execution() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    pipeline = BlockingChildTaskPipeline()
    pipeline_handle = scheduler.run_pipeline(pipeline)

    assert pipeline.task_started.wait(timeout=1.0) is True
    assert pipeline.task_handle is not None

    with scheduler._condition:
        assert scheduler._runtime.admitted_task_count == 1
        assert scheduler._runtime.running_task_count == 1

    pipeline.task_release.set()
    assert pipeline.task_handle.result(timeout=1.0) == "task-done"
    assert pipeline_handle.result(timeout=1.0) == "pipeline-done"

    with scheduler._condition:
        assert scheduler._runtime.admitted_task_count == 0
        assert scheduler._runtime.running_task_count == 0


def test_ready_cancel_releases_resources_and_worker_skips_stale_entry() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    _, pipeline_record = bind_running_pipeline(scheduler)

    with scheduler._condition:
        record = TaskRecord(
            scheduler=scheduler,
            pipeline_record=pipeline_record,
            task_id=1,
            fn=lambda: None,
            resources_required={"cpu": 1},
            state=TaskState.READY,
            ready_seq=1,
        )
        scheduler._runtime.task_records[record.task_id] = record
        add_task_to_live_registry(pipeline_record, record)
        scheduler._runtime.ready_queue.append(ReadyQueueEntry(1, record))
        scheduler._runtime.queued_task_count = 0
        scheduler._runtime.admitted_task_count = 1
        scheduler._runtime.running_task_count = 0
        scheduler._reserve_resources_locked({"cpu": 1})

    handle = stagegate.TaskHandle(record)
    assert handle.cancel() is True
    with scheduler._condition:
        assert scheduler._runtime.queued_task_count == 0
        assert scheduler._runtime.resources_in_use["cpu"] == 0
        assert scheduler._runtime.admitted_task_count == 0
        assert scheduler._runtime.running_task_count == 0
        assert record.ready_seq is None
        assert record.task_id not in scheduler._runtime.task_records
        assert record not in pipeline_record.task_records
        assert scheduler._pop_next_ready_task_locked() is None


def test_finalize_task_from_queued_updates_scheduler_live_task_counters() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    _, pipeline_record = bind_running_pipeline(scheduler)

    with scheduler._condition:
        record = make_queued_task_record(
            scheduler,
            pipeline_record=pipeline_record,
            task_id=1,
            cpu=1,
            stage_snapshot=0,
            pipeline_local_task_seq=1,
            global_task_submit_seq=1,
        )
        scheduler._runtime.running_task_count = 0

        scheduler._finalize_task_terminal_locked(record, state=TaskState.CANCELLED)

        assert scheduler._runtime.queued_task_count == 0
        assert scheduler._runtime.admitted_task_count == 0
        assert scheduler._runtime.running_task_count == 0


def test_cancel_vs_start_is_linearized_for_ready_task() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    _, pipeline_record = bind_running_pipeline(scheduler)

    with scheduler._condition:
        record = TaskRecord(
            scheduler=scheduler,
            pipeline_record=pipeline_record,
            task_id=1,
            fn=lambda: None,
            resources_required={"cpu": 1},
            state=TaskState.READY,
            ready_seq=1,
        )
        scheduler._runtime.task_records[record.task_id] = record
        add_task_to_live_registry(pipeline_record, record)
        scheduler._runtime.ready_queue.append(ReadyQueueEntry(1, record))
        scheduler._runtime.admitted_task_count = 1
        scheduler._reserve_resources_locked({"cpu": 1})

    handle = stagegate.TaskHandle(record)
    barrier = threading.Barrier(2)
    worker_started: list[bool] = []

    def try_start() -> None:
        barrier.wait()
        with scheduler._condition:
            ready = scheduler._pop_next_ready_task_locked()
            if ready is None:
                worker_started.append(False)
                return
            ready.state = TaskState.RUNNING
            ready.worker_thread_ident = threading.get_ident()
            scheduler._condition.notify_all()
            worker_started.append(True)

    thread = threading.Thread(target=try_start)
    thread.start()
    barrier.wait()
    cancel_result = handle.cancel()
    thread.join()

    with scheduler._condition:
        assert (cancel_result, record.state) in {
            (True, TaskState.CANCELLED),
            (False, TaskState.RUNNING),
        }
        if record.state is TaskState.CANCELLED:
            assert scheduler._runtime.resources_in_use["cpu"] == 0
            assert scheduler._runtime.admitted_task_count == 0
        else:
            assert scheduler._runtime.resources_in_use["cpu"] == 1
            assert scheduler._runtime.admitted_task_count == 1
            assert worker_started == [True]


def test_task_success_cleanup_removes_task_from_live_registries() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    pipeline, pipeline_record = bind_running_pipeline(scheduler)

    handle = pipeline.task(lambda: "done", resources={"cpu": 1}).run()

    assert handle.result(timeout=1.0) == "done"
    with scheduler._condition:
        assert handle._record.state is TaskState.SUCCEEDED
        assert handle._record.task_id not in scheduler._runtime.task_records
        assert handle._record not in pipeline_record.task_records


def test_task_success_cleanup_scrubs_heavy_execution_inputs() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    pipeline, _ = bind_running_pipeline(scheduler, stage_index=3)

    def run_task(sample_id: str, *, flag: bool) -> str:
        assert flag is True
        return sample_id

    handle = pipeline.task(
        run_task,
        resources={"cpu": 1},
        args=("S1",),
        kwargs={"flag": True},
        name="sample-task",
    ).run()

    assert handle.result(timeout=1.0) == "S1"
    record = handle._record
    with scheduler._condition:
        assert record.args == ()
        assert record.kwargs == {}
        assert record.resources_required == {}
        assert record.name == "sample-task"
        assert record.stage_snapshot == 0
        assert record.pipeline_enqueue_seq == 0
        assert record.pipeline_local_task_seq == 0
        assert record.global_task_submit_seq == 0
    assert handle.name() == "sample-task"


def test_task_failure_cleanup_removes_task_from_live_registries() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    pipeline, pipeline_record = bind_running_pipeline(scheduler)

    handle = pipeline.task(
        lambda: (_ for _ in ()).throw(RuntimeError("boom")),
        resources={"cpu": 1},
    ).run()

    with pytest.raises(RuntimeError, match="boom"):
        handle.result(timeout=1.0)
    with scheduler._condition:
        assert handle._record.state is TaskState.FAILED
        assert handle._record.task_id not in scheduler._runtime.task_records
        assert handle._record not in pipeline_record.task_records
