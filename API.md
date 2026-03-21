# stagegate API Reference

## Top-Level Namespace

```python
import stagegate
```

Public names:

- `Scheduler`
- `Pipeline`
- `TaskHandle`
- `PipelineHandle`
- `FIRST_COMPLETED`
- `FIRST_EXCEPTION`
- `ALL_COMPLETED`
- `CancelledError`
- `DiscardedHandleError`
- `UnknownResourceError`
- `UnschedulableTaskError`
- `ResourceSnapshot`
- `TaskCountsSnapshot`
- `PipelineCountsSnapshot`
- `SchedulerSnapshot`
- `PipelineSnapshot`

## `Scheduler`

```python
stagegate.Scheduler(
    *,
    resources: dict[str, int | float],
    pipeline_parallelism: int = 1,
    task_parallelism: int | None = None,
)
```

Create a single-process scheduler.

### Constructor arguments

- `resources: dict[str, int | float]`
  Abstract resource capacities such as `{"cpu": 16, "mem": 64}`.
  These are scheduler admission labels, not OS-enforced quotas.
- `pipeline_parallelism: int = 1`
  Maximum number of pipeline coordinator threads that may run `Pipeline.run()` concurrently.
- `task_parallelism: int | None = None`
  Maximum number of tasks that may be admitted concurrently.
  If `None`, the effective worker count is `1`.

### Context manager behavior

`Scheduler` can be used in a `with` block:

```python
with stagegate.Scheduler(resources={"cpu": 8}) as scheduler:
    ...
```

Leaving the block calls `close()`.

### `Scheduler.run_pipeline(pipeline)`

```python
run_pipeline(pipeline: Pipeline) -> PipelineHandle
```

Submit a pipeline instance for FIFO execution.

Arguments:

- `pipeline`
  A `Pipeline` instance.

Returns:

- `PipelineHandle`
  A handle for observing, waiting for, or cancelling that pipeline submission.

Raises:

- `TypeError`
  If `pipeline` is not a `Pipeline` instance.
- `RuntimeError`
  If shutdown has already started.
- `RuntimeError`
  If the same pipeline instance has already been submitted once before.

Notes:

- the same pipeline instance is single-use
- queue order is FIFO
- pipeline execution begins later on a scheduler-owned coordinator thread

### `Scheduler.wait_pipelines(handles, timeout=None, return_when=...)`

```python
wait_pipelines(
    handles,
    timeout: float | None = None,
    return_when: str = stagegate.ALL_COMPLETED,
) -> tuple[set[PipelineHandle], set[PipelineHandle]]
```

Wait on pipeline handles owned by the same scheduler.

Arguments:

- `handles`
  An iterable of `PipelineHandle` objects created by this scheduler.
- `timeout`
  Maximum wait time in seconds.
  `None` means wait indefinitely.
  `0` means immediate poll.
- `return_when`
  One of `stagegate.FIRST_COMPLETED`, `stagegate.FIRST_EXCEPTION`, or `stagegate.ALL_COMPLETED`.

Returns:

- `tuple[set[PipelineHandle], set[PipelineHandle]]`
  A pair `(done, pending)`.
- `done`
  The subset of input handles that are terminal at the moment the call returns.
  A pipeline handle is terminal when it is `SUCCEEDED`, `FAILED`, or `CANCELLED`.
- `pending`
  The subset of input handles that are still non-terminal at the moment the call returns.

Important details:

- timeout does not raise; it returns the current `(done, pending)`
- duplicate handles are deduplicated before waiting
- already terminal handles are placed in `done` immediately
- this method does not raise the pipeline's stored execution exception

Raises:

- `ValueError`
  If `handles` is empty.
- `TypeError`
  If any element is not a `PipelineHandle`.
- `ValueError`
  If any handle belongs to a different scheduler.
- `ValueError`
  If `timeout < 0`.
- `ValueError`
  If `return_when` is invalid.

### `Scheduler.shutdown(cancel_pending_pipelines=False)`

```python
shutdown(
    cancel_pending_pipelines: bool = False,
) -> None
```

Start shutdown.

Arguments:

- `cancel_pending_pipelines`
  If `True`, cancel pipelines that are still queued and not yet started.
  Running pipelines are not cancelled.

Behavior:

- returns after shutdown initiation
- does not wait for in-flight work to finish
- does not stop or join internal coordinator, dispatcher, or worker threads
- after shutdown starts, new pipeline submission is rejected
- already running pipelines continue
- already running pipelines may still submit tasks on their coordinator thread
- existing handles remain usable
- repeated shutdown calls are allowed

Related predicates:

```python
shutdown_started() -> bool
closed() -> bool
```

Return values:

- `shutdown_started()`
  `True` once shutdown has begun.
- `closed()`
  `True` only after `close()` completes full shutdown.

### `Scheduler.close(cancel_pending_pipelines=False)`

```python
close(
    cancel_pending_pipelines: bool = False,
) -> None
```

Fully close the scheduler.

Arguments:

- `cancel_pending_pipelines`
  If `True`, cancel pipelines that are still queued and not yet started.
  Running pipelines are not cancelled.

Behavior:

- starts shutdown if it has not started already
- waits until no live work remains
- joins the scheduler-owned runtime threads that were started
- returns only after the scheduler is fully closed
- already running pipelines continue
- already running pipelines may still submit tasks on their coordinator thread
- existing handles remain usable
- repeated close calls are allowed
- later calls made before the scheduler reaches `CLOSED` may strengthen the request by adding `cancel_pending_pipelines=True`

Raises:

- `RuntimeError`
  If called from a scheduler-owned runtime thread such as a pipeline coordinator thread, worker thread, or dispatcher thread.

Related predicates:

```python
shutdown_started() -> bool
closed() -> bool
```

Return values:

- `shutdown_started()`
  `True` once shutdown has begun.
- `closed()`
  `True` once no live work remains and the scheduler-owned runtime threads have terminated.

### `Scheduler.snapshot()`

```python
snapshot() -> SchedulerSnapshot
```

Return an immutable point-in-time aggregate snapshot.

The snapshot includes:

- `shutdown_started`
- `closed`
- `pipeline_parallelism`
- effective `task_parallelism`
- aggregate pipeline counts
- aggregate task counts
- deterministic per-resource usage data

Important details:

- may be called before submission, during execution, after `shutdown()`, and after `close()`
- does not wait for work to finish
- does not expose internal records or queue entries
- terminal totals remain visible even though scheduler-global terminal bookkeeping is cleaned up eagerly

## `Pipeline`

Subclass `stagegate.Pipeline` and implement `run()`.

```python
class MyPipeline(stagegate.Pipeline):
    def run(self):
        ...
```

Pipeline control APIs are valid only while `run()` is actively executing on the scheduler-owned coordinator thread.

### `Pipeline.run()`

```python
run() -> Any
```

User-defined pipeline body.

Return value:

- any Python object
  If `run()` returns normally, that value becomes the success result of the `PipelineHandle`.

Failure behavior:

- if `run()` raises, the pipeline becomes `FAILED`
- the original exception is stored on the handle and re-raised by `PipelineHandle.result()`

### `Pipeline.task(...)`

```python
task(
    fn,
    *,
    resources: dict[str, int | float],
    args: tuple = (),
    kwargs: dict | None = None,
    name: str | None = None,
) -> TaskBuilder
```

Create a task builder.
The task is not submitted until `.run()` is called on the returned builder.

Arguments:

- `fn`
  The callable to execute later on a worker thread.
- `resources`
  Abstract resource requirements for admission control.
- `args`
  Positional arguments passed to `fn`.
- `kwargs`
  Keyword arguments passed to `fn`.
- `name`
  Optional user-facing label for debugging or future diagnostics.

Returns:

- `TaskBuilder`
  A builder/factory object.
  Calling `.run()` on it submits the task and returns a `TaskHandle`.

Raises on builder `.run()`:

- `RuntimeError`
  If the pipeline is not currently running.
- `RuntimeError`
  If the call is not made from the pipeline's coordinator thread.
- `UnknownResourceError`
  If any resource label is unknown.
- `UnschedulableTaskError`
  If a single-task requirement exceeds configured capacity.
- `ValueError`
  If a resource amount is non-numeric, non-finite, or negative.

### `Pipeline.stage_forward()`

```python
stage_forward() -> None
```

Advance the internal stage by one.

Effects:

- later task submissions get the new stage snapshot
- already queued tasks keep the priority captured when they were submitted

Raises:

- `RuntimeError`
  If called before pipeline execution starts.
- `RuntimeError`
  If called after pipeline execution ends.
- `RuntimeError`
  If called from any thread other than the pipeline's coordinator thread.

### `Pipeline.wait(handles, timeout=None, return_when=...)`

```python
wait(
    handles,
    timeout: float | None = None,
    return_when: str = stagegate.ALL_COMPLETED,
) -> tuple[set[TaskHandle], set[TaskHandle]]
```

Wait on task handles created by that pipeline.

Arguments:

- `handles`
  An iterable of `TaskHandle` objects created by this pipeline.
- `timeout`
  Maximum wait time in seconds.
  `None` means wait indefinitely.
  `0` means immediate poll.
- `return_when`
  One of `stagegate.FIRST_COMPLETED`, `stagegate.FIRST_EXCEPTION`, or `stagegate.ALL_COMPLETED`.

Returns:

- `tuple[set[TaskHandle], set[TaskHandle]]`
  A pair `(done, pending)`.
- `done`
  The subset of input handles that are terminal when the call returns.
- `pending`
  The subset of input handles that are still non-terminal when the call returns.

Important details:

- timeout does not raise; it returns the current `(done, pending)`
- duplicate handles are deduplicated before waiting
- already terminal handles are placed in `done` immediately
- this method does not raise the task's stored execution exception

Raises:

- `RuntimeError`
  If called outside the active pipeline coordinator-thread context.
- `ValueError`
  If `handles` is empty.
- `TypeError`
  If any element is not a `TaskHandle`.
- `ValueError`
  If any handle belongs to a different pipeline.
- `ValueError`
  If `timeout < 0`.
- `ValueError`
  If `return_when` is invalid.

## `TaskHandle`

Handle for one submitted task.

### `TaskHandle.cancel()`

```python
cancel() -> bool
```

Return value:

- `True`
  The task had not started yet and was transitioned to `CANCELLED`.
- `False`
  The task was already running or already terminal.

Important detail:

- cancellation is non-preemptive
- running tasks are never force-killed

### `TaskHandle.done()`

```python
done() -> bool
```

Return value:

- `True`
  The task is terminal: `SUCCEEDED`, `FAILED`, or `CANCELLED`.
- `False`
  The task is still queued, ready, or running.

### `TaskHandle.running()`

```python
running() -> bool
```

Return value:

- `True`
  The task callable is actively executing on a worker thread.
- `False`
  Otherwise.

### `TaskHandle.cancelled()`

```python
cancelled() -> bool
```

Return value:

- `True`
  The task ended in the cancelled state.
- `False`
  Otherwise.

### `TaskHandle.result(timeout=None)`

```python
result(timeout: float | None = None) -> Any
```

Arguments:

- `timeout`
  Maximum wait time in seconds.
  `None` means wait indefinitely.
  `0` means immediate check.

Return value:

- the task callable's normal return value

Raises:

- `TimeoutError`
  If the task is not terminal before the timeout expires.
- `CancelledError`
  If the task was cancelled.
- original task exception
  If the task failed by raising.
- `ValueError`
  If `timeout < 0`.

### `TaskHandle.exception(timeout=None)`

```python
exception(timeout: float | None = None) -> BaseException | None
```

Arguments:

- `timeout`
  Maximum wait time in seconds.
  `None` means wait indefinitely.
  `0` means immediate check.

Return value:

- `None`
  If the task succeeded.
- the stored exception object
  If the task failed.

Raises:

- `TimeoutError`
  If the task is not terminal before the timeout expires.
- `CancelledError`
  If the task was cancelled.
- `ValueError`
  If `timeout < 0`.

## `PipelineHandle`

Handle for one submitted pipeline.

### `PipelineHandle.cancel()`

```python
cancel() -> bool
```

Return value:

- `True`
  The pipeline had not started yet and was transitioned to `CANCELLED`.
- `False`
  The pipeline was already running or already terminal.

Important detail:

- started pipelines are not force-stopped

### `PipelineHandle.done()`

```python
done() -> bool
```

Return value:

- `True`
  The pipeline is terminal: `SUCCEEDED`, `FAILED`, or `CANCELLED`.
- `False`
  The pipeline is still queued or running.

### `PipelineHandle.running()`

```python
running() -> bool
```

Return value:

- `True`
  The pipeline `run()` method is actively executing.
- `False`
  Otherwise.

### `PipelineHandle.cancelled()`

```python
cancelled() -> bool
```

Return value:

- `True`
  The pipeline ended in the cancelled state.
- `False`
  Otherwise.

### `PipelineHandle.result(timeout=None)`

```python
result(timeout: float | None = None) -> Any
```

Arguments:

- `timeout`
  Maximum wait time in seconds.
  `None` means wait indefinitely.
  `0` means immediate check.

Return value:

- the normal return value of `Pipeline.run()`

Raises:

- `TimeoutError`
  If the pipeline is not terminal before the timeout expires.
- `CancelledError`
  If the pipeline was cancelled.
- original pipeline exception
  If the pipeline failed by raising.
- `ValueError`
  If `timeout < 0`.

### `PipelineHandle.exception(timeout=None)`

```python
exception(timeout: float | None = None) -> BaseException | None
```

Arguments:

- `timeout`
  Maximum wait time in seconds.
  `None` means wait indefinitely.
  `0` means immediate check.

Return value:

- `None`
  If the pipeline succeeded.
- the stored exception object
  If the pipeline failed.

Raises:

- `TimeoutError`
  If the pipeline is not terminal before the timeout expires.
- `CancelledError`
  If the pipeline was cancelled.
- `ValueError`
  If `timeout < 0`.

### `PipelineHandle.snapshot()`

```python
snapshot() -> PipelineSnapshot
```

Return an immutable point-in-time snapshot for that pipeline.

The snapshot includes:

- `pipeline_id`
- public string `state`
- current `stage_index`
- aggregate task counts for that pipeline

Important details:

- may be called while the pipeline is queued, running, or terminal
- may show a terminal pipeline state while child tasks still remain queued, admitted, or running
- raises `DiscardedHandleError` after `discard()`

### `PipelineHandle.discard()`

```python
discard() -> None
```

Explicitly abandon a terminal pipeline handle so retained terminal outcome data
may be released.

Behavior:

- allowed only after the pipeline is terminal
- prior `result()` or `exception()` observation is not required
- idempotent
- may be called even if detached child tasks from that pipeline are still live
- does not invalidate separately held `TaskHandle` objects

After `discard()`, the following raise `DiscardedHandleError`:

- `done()`
- `running()`
- `cancelled()`
- `result()`
- `exception()`
- `snapshot()`

## Wait Constants

```python
stagegate.FIRST_COMPLETED
stagegate.FIRST_EXCEPTION
stagegate.ALL_COMPLETED
```

These constants are used by both:

- `Pipeline.wait(...)`
- `Scheduler.wait_pipelines(...)`

### `FIRST_COMPLETED`

Return once at least one handle is terminal.

Terminal means:

- `SUCCEEDED`
- `FAILED`
- `CANCELLED`

### `FIRST_EXCEPTION`

Return once at least one handle is `FAILED`.

Important detail:

- `CANCELLED` does not count as an exception trigger
- if no handle ever fails, this behaves like `ALL_COMPLETED`

### `ALL_COMPLETED`

Return once every handle is terminal.

## Exceptions

### `CancelledError`

Raised by `result()` and `exception()` on cancelled task or pipeline handles.

### `DiscardedHandleError`

Raised by pipeline-handle observation methods after `PipelineHandle.discard()`.

### `UnknownResourceError`

Raised when task submission requests a resource label unknown to the scheduler.

### `UnschedulableTaskError`

Raised when a single task requires more of some resource than the scheduler can ever provide.

## Minimal Example

```python
import stagegate


class Demo(stagegate.Pipeline):
    def run(self) -> str:
        handle = self.task(lambda: "ok", resources={"cpu": 1}).run()
        return handle.result()


with stagegate.Scheduler(resources={"cpu": 2}) as scheduler:
    pipeline_handle = scheduler.run_pipeline(Demo())
    print(pipeline_handle.result())
    pipeline_handle.discard()
```
