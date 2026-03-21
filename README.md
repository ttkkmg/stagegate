# stagegate

`stagegate` is a small, explicit, single-process Python library for running many forward-only pipelines with:

- FIFO pipeline scheduling
- stage-aware task priority
- strict head-of-line blocking
- abstract resource quotas

It is designed for batch workloads where users want predictable scheduling behavior without adopting a full workflow engine.

Because the runtime is based on Python threads, task suitability depends on the GIL. In practice, `stagegate` is a better fit for tasks that spend meaningful time in NumPy, SciPy, Pandas, or other C-extension code, or for tasks that call external programs via `subprocess.run(...)`, than for long pure-Python CPU-bound loops.

## What It Is For

`stagegate` fits workloads such as:

- data-processing pipelines
- machine-learning experiment batches
- numerical optimization with many starts
- system administration and log collection jobs

The core idea is simple: once a pipeline starts, let it keep moving, and strongly prefer later-stage task work so partially completed pipelines drain instead of building up large unfinished fronts.

## Install

```bash
pip install stagegate
```

## What It Is Not

`stagegate` is intentionally not:

- a DAG engine
- a persistent job database
- a distributed scheduler
- a remote task broker
- a forced-termination runtime

Running tasks and running pipelines are never force-killed by the scheduler.

## Quick Example

```python
import random
import time

import stagegate


class DemoPipeline(stagegate.Pipeline):
    def __init__(self, pipeline_index: int) -> None:
        self.pipeline_index = pipeline_index

    @staticmethod
    def stage1_work(pipeline_index: int, value: int) -> int:
        time.sleep(random.uniform(0.05, 0.3))
        value = pipeline_index * 10 + value
        print(f"pipeline {pipeline_index}, stage1: {value}")
        return value

    @staticmethod
    def stage2_sum(pipeline_index: int, values: list[int]) -> int:
        time.sleep(random.uniform(0.1, 0.4))
        total = sum(values)
        print(f"pipeline {pipeline_index}, stage2: {total}")
        return total

    def run(self) -> int:
        stage1_handles = [
            self.task(
                self.stage1_work,
                resources={"cpu": 1},
                args=(
                    self.pipeline_index,
                    n,
                ),
            ).run()
            for n in range(10)
        ]

        done, pending = self.wait(
            stage1_handles,
            return_when=stagegate.ALL_COMPLETED,
        )
        assert not pending
        stage1_values = [handle.result() for handle in done]

        self.stage_forward()

        stage2_handle = self.task(
            self.stage2_sum,
            resources={"cpu": 4},
            args=(
                self.pipeline_index,
                stage1_values,
            ),
        ).run()
        return stage2_handle.result()


with stagegate.Scheduler(
    resources={"cpu": 8},
    pipeline_parallelism=2,
    task_parallelism=4,
) as scheduler:
    pending = set()
    pipeline_indices = {}

    for pipeline_index in range(10):
        handle = scheduler.run_pipeline(DemoPipeline(pipeline_index))
        pending.add(handle)
        pipeline_indices[handle] = pipeline_index

    while pending:
        done, pending = scheduler.wait_pipelines(
            pending,
            return_when=stagegate.FIRST_COMPLETED,
        )
        for handle in done:
            pipeline_index = pipeline_indices[handle]
            pipeline_sum = handle.result()
            stage1_base = pipeline_index * 10
            stage1_total = sum(range(stage1_base, stage1_base + 10))
            print(
                f"pipeline {pipeline_index}: "
                f"sum({stage1_base} .. {stage1_base + 9}) = {pipeline_sum}"
            )
            assert pipeline_sum == stage1_total
            handle.discard()
```

## Public API

Top-level exports:

- `stagegate.Scheduler`
- `stagegate.Pipeline`
- `stagegate.TaskHandle`
- `stagegate.PipelineHandle`
- `stagegate.FIRST_COMPLETED`
- `stagegate.FIRST_EXCEPTION`
- `stagegate.ALL_COMPLETED`
- `stagegate.CancelledError`
- `stagegate.DiscardedHandleError`
- `stagegate.UnknownResourceError`
- `stagegate.UnschedulableTaskError`
- `stagegate.ResourceSnapshot`
- `stagegate.TaskCountsSnapshot`
- `stagegate.PipelineCountsSnapshot`
- `stagegate.SchedulerSnapshot`
- `stagegate.PipelineSnapshot`

See: [API.md](https://github.com/ttkkmg/stagegate/blob/main/API.md)

## Use Cases

Pseudo-code walkthroughs based on the project use-case spec:

- many lightweight preparations -> heavy aggregate -> cleanup
- numerical multi-start optimization
- machine-learning batch experiments
- system administration and log collection
- top-level coordination with pipeline handles
- long-lived batch loops with `PipelineHandle.discard()`

Use-case guide: [USE_CASES.md](https://github.com/ttkkmg/stagegate/blob/main/USE_CASES.md)

## Scheduling Model

The scheduler has two layers:

1. Pipeline layer
   Queued pipelines are started in FIFO order, limited by `pipeline_parallelism`.
2. Task layer
   Queued tasks are prioritized by submit-time stage snapshot and admitted under resource quotas, limited by `task_parallelism`.

Task priority is ordered by:

1. higher stage first
2. older pipeline first
3. earlier task submission inside that pipeline first
4. final global task submit sequence as a stable tie-break

If the highest-priority queued task cannot run because resources are unavailable, lower-priority tasks must not bypass it.

## Waiting

`stagegate` provides wait operations at both levels:

- `Pipeline.wait(...)` for task handles inside one pipeline
- `Scheduler.wait_pipelines(...)` for pipeline handles at the application boundary

Both support `ALL_COMPLETED`, `FIRST_COMPLETED`, and `FIRST_EXCEPTION`.

## Snapshots and Discard

For observation without exposing mutable internals:

- `scheduler.snapshot()`
- `pipeline_handle.snapshot()`

These return immutable aggregate dataclasses.

For long-lived scripts that keep many pipeline handles in scope:

- call `pipeline_handle.discard()` after you no longer need that handle's
  `result()`, `exception()`, or `snapshot()`
- discard invalidates that pipeline handle but does not invalidate any
  separately held `TaskHandle`
- discard is useful when a pipeline may be terminal even though detached child
  tasks still continue draining in the background

## Shutdown and Close

Shutdown is monotonic:

- `OPEN -> SHUTTING_DOWN -> CLOSED`

Use:

- `scheduler.shutdown()` to stop accepting new pipelines and let in-flight work drain
- `scheduler.close()` to wait for drain and fully stop the scheduler

After shutdown starts:

- new pipeline submission is rejected
- already running pipelines may continue
- task submission from those running pipelines remains allowed on their coordinator thread
- existing handles remain usable

`close()` waits until live work has drained, joins the scheduler's runtime threads, and marks the scheduler fully closed.
Using `Scheduler(...)` as a context manager calls `close()` on block exit.

`closed()` means the scheduler has fully closed and its runtime threads have been joined.
