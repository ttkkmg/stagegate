from __future__ import annotations

import threading
import time

import pytest

import stagegate
from stagegate._records import PipelineRecord, TaskRecord
from stagegate._states import PipelineState, TaskState


def make_scheduler() -> stagegate.Scheduler:
    return stagegate.Scheduler(resources={"cpu": 4})


def make_pipeline_record(
    scheduler: stagegate.Scheduler | None = None,
    *,
    state: PipelineState = PipelineState.QUEUED,
) -> PipelineRecord:
    scheduler = make_scheduler() if scheduler is None else scheduler
    pipeline = stagegate.Pipeline()
    return PipelineRecord(
        scheduler=scheduler,
        pipeline=pipeline,
        pipeline_id=1,
        enqueue_seq=1,
        state=state,
    )


def make_task_record(
    scheduler: stagegate.Scheduler | None = None,
    *,
    state: TaskState = TaskState.QUEUED,
) -> TaskRecord:
    scheduler = make_scheduler() if scheduler is None else scheduler
    pipeline_record = make_pipeline_record(scheduler)
    return TaskRecord(
        scheduler=scheduler,
        pipeline_record=pipeline_record,
        task_id=1,
        fn=lambda: None,
        resources_required={"cpu": 1},
        state=state,
    )


@pytest.mark.parametrize(
    ("state", "done", "running", "cancelled"),
    [
        (TaskState.QUEUED, False, False, False),
        (TaskState.READY, False, False, False),
        (TaskState.RUNNING, False, True, False),
        (TaskState.SUCCEEDED, True, False, False),
        (TaskState.FAILED, True, False, False),
        (TaskState.CANCELLED, True, False, True),
    ],
)
def test_task_handle_predicates_reflect_record_state(
    state: TaskState,
    done: bool,
    running: bool,
    cancelled: bool,
) -> None:
    handle = stagegate.TaskHandle(make_task_record(state=state))
    assert handle.done() is done
    assert handle.running() is running
    assert handle.cancelled() is cancelled


@pytest.mark.parametrize(
    ("state", "done", "running", "cancelled"),
    [
        (PipelineState.QUEUED, False, False, False),
        (PipelineState.RUNNING, False, True, False),
        (PipelineState.SUCCEEDED, True, False, False),
        (PipelineState.FAILED, True, False, False),
        (PipelineState.CANCELLED, True, False, True),
    ],
)
def test_pipeline_handle_predicates_reflect_record_state(
    state: PipelineState,
    done: bool,
    running: bool,
    cancelled: bool,
) -> None:
    handle = stagegate.PipelineHandle(make_pipeline_record(state=state))
    assert handle.done() is done
    assert handle.running() is running
    assert handle.cancelled() is cancelled


def test_task_handle_cancel_queued_task_transitions_to_cancelled() -> None:
    record = make_task_record(state=TaskState.QUEUED)
    handle = stagegate.TaskHandle(record)

    assert handle.cancel() is True
    assert record.state is TaskState.CANCELLED
    assert handle.done() is True
    assert handle.cancelled() is True


def test_task_handle_cancel_ready_task_transitions_to_cancelled() -> None:
    record = make_task_record(state=TaskState.READY)
    handle = stagegate.TaskHandle(record)

    assert handle.cancel() is True
    assert record.state is TaskState.CANCELLED


@pytest.mark.parametrize(
    "state",
    [TaskState.RUNNING, TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED],
)
def test_task_handle_cancel_running_or_terminal_task_returns_false(
    state: TaskState,
) -> None:
    record = make_task_record(state=state)
    handle = stagegate.TaskHandle(record)

    assert handle.cancel() is False
    assert record.state is state


def test_pipeline_handle_cancel_queued_pipeline_transitions_to_cancelled() -> None:
    record = make_pipeline_record(state=PipelineState.QUEUED)
    handle = stagegate.PipelineHandle(record)

    assert handle.cancel() is True
    assert record.state is PipelineState.CANCELLED
    assert handle.done() is True
    assert handle.cancelled() is True


@pytest.mark.parametrize(
    "state",
    [
        PipelineState.RUNNING,
        PipelineState.SUCCEEDED,
        PipelineState.FAILED,
        PipelineState.CANCELLED,
    ],
)
def test_pipeline_handle_cancel_running_or_terminal_pipeline_returns_false(
    state: PipelineState,
) -> None:
    record = make_pipeline_record(state=state)
    handle = stagegate.PipelineHandle(record)

    assert handle.cancel() is False
    assert record.state is state


def test_task_handle_result_and_exception_for_success() -> None:
    record = make_task_record(state=TaskState.SUCCEEDED)
    record.result_value = 123
    handle = stagegate.TaskHandle(record)

    assert handle.result() == 123
    assert handle.exception() is None


def test_task_handle_result_and_exception_for_failure() -> None:
    record = make_task_record(state=TaskState.FAILED)
    error = RuntimeError("boom")
    record.exception = error
    handle = stagegate.TaskHandle(record)

    with pytest.raises(RuntimeError) as exc_info:
        handle.result()
    assert exc_info.value is error
    assert handle.exception() is error


def test_task_handle_result_and_exception_for_cancelled() -> None:
    record = make_task_record(state=TaskState.CANCELLED)
    handle = stagegate.TaskHandle(record)

    with pytest.raises(stagegate.CancelledError):
        handle.result()
    with pytest.raises(stagegate.CancelledError):
        handle.exception()


def test_pipeline_handle_result_and_exception_for_success() -> None:
    record = make_pipeline_record(state=PipelineState.SUCCEEDED)
    record.result_value = "ok"
    handle = stagegate.PipelineHandle(record)

    assert handle.result() == "ok"
    assert handle.exception() is None


def test_pipeline_handle_result_and_exception_for_failure() -> None:
    record = make_pipeline_record(state=PipelineState.FAILED)
    error = ValueError("failed")
    record.exception = error
    handle = stagegate.PipelineHandle(record)

    with pytest.raises(ValueError) as exc_info:
        handle.result()
    assert exc_info.value is error
    assert handle.exception() is error


def test_pipeline_handle_result_and_exception_for_cancelled() -> None:
    record = make_pipeline_record(state=PipelineState.CANCELLED)
    handle = stagegate.PipelineHandle(record)

    with pytest.raises(stagegate.CancelledError):
        handle.result()
    with pytest.raises(stagegate.CancelledError):
        handle.exception()


@pytest.mark.parametrize("state", [PipelineState.QUEUED, PipelineState.RUNNING])
def test_pipeline_handle_discard_requires_terminal_pipeline(
    state: PipelineState,
) -> None:
    handle = stagegate.PipelineHandle(make_pipeline_record(state=state))

    with pytest.raises(RuntimeError):
        handle.discard()


def test_pipeline_handle_discard_invalidates_observation_methods_and_clears_result() -> None:
    record = make_pipeline_record(state=PipelineState.SUCCEEDED)
    record.result_value = {"payload": "ok"}
    handle = stagegate.PipelineHandle(record)

    handle.discard()

    assert record.pipeline is None
    assert record.result_value is None
    assert record.exception is None
    for method_name in (
        "done",
        "running",
        "cancelled",
        "result",
        "exception",
        "snapshot",
    ):
        with pytest.raises(stagegate.DiscardedHandleError):
            getattr(handle, method_name)()


def test_pipeline_handle_discard_invalidates_failure_handle_and_clears_exception() -> None:
    record = make_pipeline_record(state=PipelineState.FAILED)
    record.exception = RuntimeError("boom")
    handle = stagegate.PipelineHandle(record)

    handle.discard()

    assert record.pipeline is None
    assert record.result_value is None
    assert record.exception is None
    with pytest.raises(stagegate.DiscardedHandleError):
        handle.result()
    with pytest.raises(stagegate.DiscardedHandleError):
        handle.exception()


def test_pipeline_handle_discard_is_idempotent() -> None:
    handle = stagegate.PipelineHandle(
        make_pipeline_record(state=PipelineState.CANCELLED)
    )

    handle.discard()
    handle.discard()

    with pytest.raises(stagegate.DiscardedHandleError):
        handle.done()


@pytest.mark.parametrize("method_name", ["result", "exception"])
def test_task_handle_negative_timeout_raises_value_error(method_name: str) -> None:
    handle = stagegate.TaskHandle(make_task_record())

    with pytest.raises(ValueError):
        getattr(handle, method_name)(timeout=-1)


@pytest.mark.parametrize("method_name", ["result", "exception"])
def test_pipeline_handle_negative_timeout_raises_value_error(method_name: str) -> None:
    handle = stagegate.PipelineHandle(make_pipeline_record())

    with pytest.raises(ValueError):
        getattr(handle, method_name)(timeout=-1)


@pytest.mark.parametrize("method_name", ["result", "exception"])
def test_task_handle_timeout_zero_raises_timeout_error(method_name: str) -> None:
    handle = stagegate.TaskHandle(make_task_record(state=TaskState.QUEUED))

    with pytest.raises(TimeoutError):
        getattr(handle, method_name)(timeout=0)


@pytest.mark.parametrize("method_name", ["result", "exception"])
def test_pipeline_handle_timeout_zero_raises_timeout_error(method_name: str) -> None:
    handle = stagegate.PipelineHandle(make_pipeline_record(state=PipelineState.QUEUED))

    with pytest.raises(TimeoutError):
        getattr(handle, method_name)(timeout=0)


@pytest.mark.parametrize(
    "handle_type", [stagegate.TaskHandle, stagegate.PipelineHandle]
)
def test_handles_deduplicate_by_scheduler_and_local_id(
    handle_type: type[object],
) -> None:
    scheduler = make_scheduler()
    if handle_type is stagegate.TaskHandle:
        record = make_task_record(scheduler=scheduler)
        handle1 = stagegate.TaskHandle(record)
        handle2 = stagegate.TaskHandle(record)
    else:
        record = make_pipeline_record(scheduler=scheduler)
        handle1 = stagegate.PipelineHandle(record)
        handle2 = stagegate.PipelineHandle(record)

    assert handle1 == handle2
    assert len({handle1, handle2}) == 1


def test_task_handle_waits_until_terminal_completion() -> None:
    scheduler = make_scheduler()
    record = make_task_record(scheduler=scheduler, state=TaskState.QUEUED)
    handle = stagegate.TaskHandle(record)

    def worker() -> None:
        time.sleep(0.02)
        with scheduler._condition:
            record.state = TaskState.SUCCEEDED
            record.result_value = "done"
            scheduler._notify_state_change_locked()

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert handle.result(timeout=1.0) == "done"
    finally:
        thread.join()


def test_pipeline_handle_waits_until_terminal_completion() -> None:
    scheduler = make_scheduler()
    record = make_pipeline_record(scheduler=scheduler, state=PipelineState.QUEUED)
    handle = stagegate.PipelineHandle(record)

    def worker() -> None:
        time.sleep(0.02)
        with scheduler._condition:
            record.state = PipelineState.SUCCEEDED
            record.result_value = "done"
            scheduler._notify_state_change_locked()

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert handle.result(timeout=1.0) == "done"
    finally:
        thread.join()
