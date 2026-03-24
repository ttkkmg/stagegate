# Use Cases

This page introduces `stagegate` by the way its APIs are intended to be
combined.

## 1. Barriered Multi-Stage Pipelines

This is the baseline `stagegate` pattern:

1. launch a wave of sibling tasks
2. wait for the whole phase
3. read results
4. call `stage_forward()`
5. launch the next phase

```python
class RNASeqPipeline(stagegate.Pipeline):
    def __init__(self, sample_id: str) -> None:
        self.sample_id = sample_id

    def run(self) -> str:
        fetch_r1 = self.task(
            fetch_read1,
            resources={"cpu": 1},
            args=(self.sample_id,),
        ).run()
        fetch_r2 = self.task(
            fetch_read2,
            resources={"cpu": 1},
            args=(self.sample_id,),
        ).run()

        self.wait([fetch_r1, fetch_r2], return_when=stagegate.ALL_COMPLETED)
        read1 = fetch_r1.result()
        read2 = fetch_r2.result()

        self.stage_forward()

        trim_r1 = self.task(
            trim_read1,
            resources={"cpu": 2},
            args=(read1,),
        ).run()
        trim_r2 = self.task(
            trim_read2,
            resources={"cpu": 2},
            args=(read2,),
        ).run()

        self.wait([trim_r1, trim_r2], return_when=stagegate.ALL_COMPLETED)
        trimmed_r1 = trim_r1.result()
        trimmed_r2 = trim_r2.result()

        self.stage_forward()

        align = self.task(
            align_reads,
            resources={"cpu": 8, "mem": 32},
            args=(trimmed_r1, trimmed_r2),
        ).run()
        return align.result()


def main(sample_ids: list[str]) -> list[str]:
    scheduler = stagegate.Scheduler(
        resources={"cpu": 32, "mem": 128},
        pipeline_parallelism=4,
        task_parallelism=8,
    )
    try:
        handles = {
            scheduler.run_pipeline(RNASeqPipeline(sample_id))
            for sample_id in sample_ids
        }
        done, pending = scheduler.wait_pipelines(
            handles,
            return_when=stagegate.ALL_COMPLETED,
        )
        assert not pending
        return [handle.result() for handle in done]
    finally:
        scheduler.close()
```

Use this pattern when:

- each sample is naturally one pipeline
- each phase has a real barrier
- later-stage work should be favored so partially processed samples finish

## 2. Incremental Collection with `FIRST_COMPLETED`

Use this when sibling tasks behave more like a stream of arrivals than like a
strict batch barrier.

```python
class MultiStartPipeline(stagegate.Pipeline):
    def __init__(self, starts) -> None:
        self.starts = list(starts)

    def run(self):
        pending = {
            self.task(run_solver, resources={"cpu": 2}, args=(x0,)).run()
            for x0 in self.starts
        }
        accepted = []

        while pending and len(accepted) < 5:
            done, pending = self.wait(
                pending,
                return_when=stagegate.FIRST_COMPLETED,
            )
            for handle in done:
                try:
                    outcome = handle.result()
                except Exception:
                    continue
                if outcome.ok:
                    accepted.append(outcome)

        if len(accepted) < 5:
            raise RuntimeError("not enough acceptable results")

        self.stage_forward()
        summary = self.task(
            summarize_results,
            resources={"cpu": 1},
            args=(accepted,),
        ).run()
        return summary.result()


def main(starts_per_pipeline) -> None:
    with stagegate.Scheduler(
        resources={"cpu": 32},
        pipeline_parallelism=4,
        task_parallelism=8,
    ) as scheduler:
        handles = {
            scheduler.run_pipeline(MultiStartPipeline(starts))
            for starts in starts_per_pipeline
        }
        ...
```

Use this pattern when:

- many sibling tasks are launched together
- progress is observed incrementally through handles
- domain-level soft failure can remain a normal return value

## 3. Pruning Queued Work with `cancel()`

This pattern is for cases where the main savings come from preventing more work
from starting, without trying to stop already running tasks.

```python
class ExperimentPipeline(stagegate.Pipeline):
    def __init__(self, configs) -> None:
        self.configs = list(configs)

    def run(self):
        pending = {
            self.task(
                train_and_eval,
                resources={"cpu": 4, "mem": 16},
                args=(cfg,),
            ).run()
            for cfg in self.configs
        }
        selected = []

        while pending and len(selected) < 3:
            done, pending = self.wait(
                pending,
                return_when=stagegate.FIRST_COMPLETED,
            )
            for handle in done:
                report = handle.result()
                if report.score >= 0.9:
                    selected.append(report.model_path)
                    if len(selected) >= 3:
                        for other in pending:
                            other.cancel()
                        break

        self.stage_forward()
        package = self.task(
            package_models,
            resources={"cpu": 1},
            args=(selected,),
        ).run()
        return package.result()


def main(config_groups) -> None:
    with stagegate.Scheduler(
        resources={"cpu": 64, "mem": 256},
        pipeline_parallelism=4,
        task_parallelism=8,
    ) as scheduler:
        handles = {
            scheduler.run_pipeline(ExperimentPipeline(configs))
            for configs in config_groups
        }
        ...
```

Use this pattern when:

- the pipeline can stop queued tail work once enough good results exist
- cancellation should remain explicitly non-preemptive
- later packaging work can proceed without adding more low-value starts

## 4. Terminating Running Siblings from a Python Loop

When queued-task cancellation is not enough, the next pattern is cooperative
terminate from a Python-controlled loop.

```python
class SearchPipeline(stagegate.Pipeline):
    def __init__(self, starts) -> None:
        self.starts = list(starts)

    def run(self):
        pending = {
            self.task(self.run_candidate, resources={"cpu": 1}, args=(x0,)).run()
            for x0 in self.starts
        }
        accepted = []

        while pending and len(accepted) < 3:
            done, pending = self.wait(
                pending,
                return_when=stagegate.FIRST_COMPLETED,
            )
            for handle in done:
                try:
                    accepted.append(handle.result())
                except Exception:
                    continue

        for handle in pending:
            handle.request_terminate()

        done, pending = self.wait(
            pending,
            return_when=stagegate.FIRST_EXCEPTION,
        )
        for handle in done:
            try:
                handle.result()
            except stagegate.TerminatedError:
                cleanup_partial_outputs()

        self.stage_forward()
        return summarize(accepted)

    def run_candidate(self, x0):
        state = initialize_candidate(x0)
        for _ in range(10_000):
            state = numpy_step(state)
            if stagegate.terminate_requested():
                raise stagegate.TerminatedError(
                    argv=(),
                    pid=None,
                    returncode=None,
                    forced_kill=False,
                )
        return state


def main(search_batches) -> None:
    with stagegate.Scheduler(
        resources={"cpu": 32},
        pipeline_parallelism=4,
        task_parallelism=8,
    ) as scheduler:
        handles = {
            scheduler.run_pipeline(SearchPipeline(starts))
            for starts in search_batches
        }
        ...
```

Use this pattern when:

- the task owns its loop structure and can define explicit polling checkpoints
- terminate remains cooperative and observable through handles
- `FIRST_EXCEPTION` can observe terminated siblings before the next stage

## 5. Terminating External Processes with `run_subprocess(...)`

If the task mostly launches a CLI tool, prefer `stagegate.run_subprocess(...)`
instead of `subprocess.run(...)`.

```python
class CliSearchPipeline(stagegate.Pipeline):
    def __init__(self, argv_groups) -> None:
        self.argv_groups = list(argv_groups)

    def run(self):
        pending = {
            self.task(
                stagegate.run_subprocess,
                resources={"cpu": 1},
                args=(argv,),
            ).run()
            for argv in self.argv_groups
        }
        winner = None

        while pending and winner is None:
            done, pending = self.wait(
                pending,
                return_when=stagegate.FIRST_COMPLETED,
            )
            for handle in done:
                try:
                    returncode = handle.result()
                except Exception:
                    continue
                if returncode == 0:
                    winner = handle
                    break

        for handle in pending:
            handle.request_terminate()

        done, pending = self.wait(
            pending,
            return_when=stagegate.FIRST_EXCEPTION,
        )
        for handle in done:
            try:
                handle.result()
            except stagegate.TerminatedError:
                cleanup_partial_outputs()

        self.stage_forward()
        return finalize_winner(winner)


def main(argv_batches) -> None:
    with stagegate.Scheduler(
        resources={"cpu": 32},
        pipeline_parallelism=4,
        task_parallelism=8,
    ) as scheduler:
        handles = {
            scheduler.run_pipeline(CliSearchPipeline(argv_groups))
            for argv_groups in argv_batches
        }
        ...
```

Use this pattern when:

- child processes are launched with explicit `argv`
- terminate requests have a defined subprocess-aware path
- if you need shell redirection or pipes, prefer `stagegate.run_shell(...)`
  instead of forcing shell syntax into `run_subprocess(...)`
- the pipeline can wait for terminated siblings, clean up partial files, and continue

## 6. Coordinating Many Pipelines with `wait_pipelines(...)`

This is the standard outer control loop at the application boundary. Inside
`Pipeline.run()`, use `Pipeline.wait(...)`. Outside pipelines, use
`Scheduler.wait_pipelines(...)`.

```python
scheduler = stagegate.Scheduler(
    resources={"cpu": 32, "mem": 256},
    pipeline_parallelism=4,
    task_parallelism=8,
)
try:
    pending = {
        scheduler.run_pipeline(SamplePipeline(sample_id), name=sample_id)
        for sample_id in sample_ids
    }

    while pending:
        done, pending = scheduler.wait_pipelines(
            pending,
            return_when=stagegate.FIRST_COMPLETED,
        )
        for handle in done:
            publish_result(handle.name(), handle.result())
finally:
    scheduler.close()
```

Use this pattern when:

- one scheduler can coordinate many pipeline instances
- the outer loop can consume results incrementally
- ownership stays explicit because pipeline handles never mix with task handles
- submission `name` lets the outer loop recover a sample/item label directly
  from each completed handle without a parallel `dict[handle] -> name`

## 7. Releasing Pipeline Handles with `discard()`

In long-lived scripts, completed pipeline handles may accumulate even after
their results have already been persisted elsewhere. `discard()` exists to make
that release explicit.

```python
scheduler = stagegate.Scheduler(
    resources={"cpu": 32, "mem": 256},
    pipeline_parallelism=4,
    task_parallelism=8,
)
try:
    pending = {
        scheduler.run_pipeline(SamplePipeline(config))
        for config in parameter_sweep()
    }

    while pending:
        done, pending = scheduler.wait_pipelines(
            pending,
            return_when=stagegate.FIRST_COMPLETED,
        )
        for handle in done:
            try:
                record_result(handle.result())
            except Exception as exc:
                record_failure(exc)
            finally:
                handle.discard()
finally:
    scheduler.close()
```

Use this pattern when:

- the script may submit thousands of pipelines over time
- completed handle outcome data can be released explicitly after recording
- separately held child task handles remain unaffected

## 8. Scheduler Inspection

Snapshots are useful for lightweight operational inspection without exposing
mutable internal records.

```python
def main(sample_ids) -> None:
    with stagegate.Scheduler(
        resources={"cpu": 32, "mem": 128},
        pipeline_parallelism=4,
        task_parallelism=8,
    ) as scheduler:
        pending = {
            scheduler.run_pipeline(RNASeqPipeline(sample_id), name=sample_id)
            for sample_id in sample_ids
        }

        while pending:
            view = scheduler.snapshot(include_running_pipelines=True)
            print(
                "running pipelines:",
                view.pipelines.running,
                [item.name for item in view.running_pipelines],
                "admitted tasks:",
                view.tasks.admitted,
                "resources:",
                [(r.label, r.in_use, r.capacity) for r in view.resources],
            )

            done, pending = scheduler.wait_pipelines(
                pending,
                timeout=10.0,
                return_when=stagegate.FIRST_COMPLETED,
            )
            for handle in done:
                consume_result(handle.result())
```

Use this pattern when:

- monitoring should happen while pipelines are still running
- aggregate scheduler pressure should be visible without exposing mutable records
- with `include_running_pipelines=True`, running pipeline names can be shown
  directly from snapshot summaries
- snapshots are useful for debugging, progress reporting, and tests

## 9. Anti-Patterns and Common Mistakes

These examples show mistakes that are easy to make when you first start writing
pipelines.

### 9.1 Prohibited: calling pipeline control APIs from task worker code

Task functions are not allowed to behave like mini coordinators.

```python
class BadPipeline(stagegate.Pipeline):
    def run(self):
        first = self.task(self.worker, resources={"cpu": 1}).run()
        return first.result()

    def worker(self):
        child = self.task(do_more_work, resources={"cpu": 1}).run()
        self.wait([child], return_when=stagegate.ALL_COMPLETED)
        return child.result()
```

Why this is wrong:

- task worker threads are not part of the pipeline control plane
- `Pipeline.task(...)`, `Pipeline.wait(...)`, and `stage_forward()` are valid
  only on the scheduler-owned coordinator thread that is running `Pipeline.run()`

Do this instead:

- keep task submission and waiting in `Pipeline.run()` or helper methods called
  from that coordinator-thread context
- keep task functions focused on performing work only

### 9.2 Anti-pattern: passing a scheduler into a task and launching pipelines from it

Tasks should not become hidden pipeline launchers.

```python
def worker(scheduler, sample_id):
    handle = scheduler.run_pipeline(SamplePipeline(sample_id))
    return handle.result()


class BadPipeline(stagegate.Pipeline):
    def run(self):
        task_handle = self.task(
            worker,
            resources={"cpu": 1},
            args=(self._stagegate_scheduler, "S1"),
        ).run()
        return task_handle.result()
```

Why this is a problem:

- it collapses the distinction between outer application control and task work
- ownership and shutdown responsibilities become harder to reason about
- even if it appears to work, it pushes orchestration into worker code and
  makes debugging much harder

Do this instead:

- submit pipelines from the outer application boundary
- use `Scheduler.wait_pipelines(...)` there
- keep tasks as work units, not as pipeline launchers

### 9.3 Prohibited: reusing the same pipeline instance for multiple submissions

```python
pipeline = SamplePipeline("S1")

handle1 = scheduler.run_pipeline(pipeline)
handle1.result()

handle2 = scheduler.run_pipeline(pipeline)
```

Why this is wrong:

- a `Pipeline` instance is single-use for submission
- the same instance must not be submitted again, even if the earlier
  submission was cancelled

Do this instead:

```python
handle1 = scheduler.run_pipeline(SamplePipeline("S1"))
handle2 = scheduler.run_pipeline(SamplePipeline("S1"))
```

### 9.4 Common mistake: expecting `cancel()` to stop already running tasks

```python
for handle in pending:
    handle.cancel()
```

Why this is a mistake:

- `TaskHandle.cancel()` only cancels tasks that have not started yet
- queued or ready tasks may be cancelled
- running tasks are never force-stopped by `cancel()`

Do this instead:

- use `cancel()` when the goal is to prune queued tail work
- if running siblings must also stop, switch to `request_terminate()` and a
  terminate-aware task body

### 9.5 Common mistake: expecting `request_terminate()` to force-stop arbitrary Python code

```python
def long_running_task():
    for item in huge_input:
        process(item)


class SearchPipeline(stagegate.Pipeline):
    def run(self):
        handle = self.task(long_running_task, resources={"cpu": 1}).run()
        handle.request_terminate()
        return handle.result()
```

Why this is a mistake:

- `request_terminate()` is cooperative, not a strong preemption mechanism
- Python task code must poll `stagegate.terminate_requested()` at explicit safe
  points

Do this instead:

- insert explicit termination checkpoints in Python loops
- use `stagegate.run_subprocess(...)` for long-running external commands

### 9.6 Prohibited: mixing up `Pipeline.wait(...)` and `Scheduler.wait_pipelines(...)`

```python
class BadPipeline(stagegate.Pipeline):
    def run(self):
        pipeline_handle = self._stagegate_scheduler.run_pipeline(OtherPipeline())
        self.wait([pipeline_handle], return_when=stagegate.ALL_COMPLETED)
```

```python
task_handle = pipeline.task(run_step, resources={"cpu": 1}).run()
scheduler.wait_pipelines([task_handle], return_when=stagegate.ALL_COMPLETED)
```

Why this is wrong:

- `Pipeline.wait(...)` is for task handles created by that pipeline
- `Scheduler.wait_pipelines(...)` is for pipeline handles created by that scheduler
- task handles and pipeline handles must never be mixed

Do this instead:

- inside a pipeline, wait on task handles with `Pipeline.wait(...)`
- outside pipelines, wait on pipeline handles with `Scheduler.wait_pipelines(...)`
