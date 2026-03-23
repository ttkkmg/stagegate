# Use Cases

This page introduces `stagegate` by the way its APIs are intended to be
combined.

## 1. Barriered Multi-Stage Pipelines

The most basic `stagegate` pipeline has clear phase boundaries:

- launch a set of sibling tasks
- wait until that phase is complete
- read the outputs
- advance the stage
- launch the next phase

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
```

Use this pattern when a pipeline is naturally forward-only and each phase has a
real barrier. This is the basic model for `stagegate`.

## 2. Incremental Collection with `FIRST_COMPLETED`

Some pipelines do not need every sibling task to finish before they can make a
decision. In that case, keep a `pending` set and repeatedly wait for the next
arrival.

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
```

This is the intended pattern when sibling tasks behave more like a stream of
arrivals than like a strict batch barrier.

## 3. Pruning Queued Work with `cancel()`

If a pipeline has already collected enough useful results, it may want to stop
the queued tail without touching already running work. That is exactly what
`cancel()` is for.

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
```

This is best when the main savings come from preventing more work from starting.
It preserves the simple scheduler rule that already running tasks keep draining.

## 4. Terminating Running Siblings from a Python Loop

When queued-task cancellation is not enough, the next pattern is cooperative
terminate.

The intended control loop is:

1. collect enough successful sibling results
2. call `request_terminate()` on the remaining handles
3. wait for termination-related exceptions
4. run domain cleanup
5. call `stage_forward()`

```python
class SearchPipeline(stagegate.Pipeline):
    def __init__(self, starts) -> None:
        self.starts = list(starts)

    def run(self):
        pending = {
            self.task(run_candidate, resources={"cpu": 1}, args=(x0,)).run()
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


def run_candidate(x0):
    state = initialize_candidate(x0)
    for _ in range(10_000):
        state = numpy_step(state)
        stagegate.raise_if_termination_requested()
    return state
```

This is the pattern when the task owns its loop structure and can define
safe polling checkpoints.

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
```

Use this pattern when the main workload is a long-running external executable.
`run_subprocess(...)` gives `stagegate` a well-defined way to react to
terminate requests by sending `SIGTERM` and, if needed, `SIGKILL`.

## 6. Coordinating Many Pipelines with `wait_pipelines(...)`

Inside a pipeline, use `Pipeline.wait(...)`. Outside pipelines, use
`Scheduler.wait_pipelines(...)`.

```python
scheduler = stagegate.Scheduler(
    resources={"cpu": 32, "mem": 256},
    pipeline_parallelism=4,
    task_parallelism=8,
)
try:
    pending = {
        scheduler.run_pipeline(SamplePipeline(sample_id))
        for sample_id in sample_ids
    }

    while pending:
        done, pending = scheduler.wait_pipelines(
            pending,
            return_when=stagegate.FIRST_COMPLETED,
        )
        for handle in done:
            publish_result(handle.result())
finally:
    scheduler.close()
```

This is the standard outer control loop for applications that keep one
scheduler alive and feed it many pipeline instances.

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

This pattern is especially useful when the outer loop may submit thousands of
pipelines over time and the handle is no longer needed after the result has been
recorded.

## Choosing a Pattern

- use `ALL_COMPLETED` when phases have hard barriers
- use `FIRST_COMPLETED` when sibling results can be consumed incrementally
- use `cancel()` when it is enough to stop work that has not started yet
- use `request_terminate()` when running sibling tasks must also be told to stop
- use `run_subprocess(...)` when task work is mostly external CLI execution
- use `wait_pipelines(...)` at the application boundary
- use `discard()` when a long-lived outer loop should not keep completed
  pipeline handles around
