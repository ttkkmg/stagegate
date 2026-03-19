# stagegate Use-Case Guide

This document turns the project use-case specification into pseudo-code-oriented explanations for readers who want to understand how `stagegate` is intended to be used.

## Common Pattern

Most pipelines follow the same control structure:

1. submit one or more tasks
2. wait for the relevant handles
3. inspect results
4. call `stage_forward()` when moving to the next phase
5. submit the next wave

## 1. Many Lightweight Preparations -> Heavy Aggregate -> Cleanup

Pseudo-code:

```python
class RNASeqPipeline(stagegate.Pipeline):
    def __init__(self, sample_id: str) -> None:
        self.sample_id = sample_id

    def run(self) -> str:
        fetch_r1_handle = self.task(
            fetch_r1,
            resources={"cpu": 1},
            args=(self.sample_id,),
        ).run()
        fetch_r2_handle = self.task(
            fetch_r2,
            resources={"cpu": 1},
            args=(self.sample_id,),
        ).run()

        done, pending = self.wait(
            [fetch_r1_handle, fetch_r2_handle],
            return_when=stagegate.ALL_COMPLETED,
        )
        assert not pending
        read1 = fetch_r1_handle.result()
        read2 = fetch_r2_handle.result()

        self.stage_forward()

        trim_r1_handle = self.task(
            trim_r1,
            resources={"cpu": 2},
            args=(read1,),
        ).run()
        trim_r2_handle = self.task(
            trim_r2,
            resources={"cpu": 2},
            args=(read2,),
        ).run()

        self.wait(
            [trim_r1_handle, trim_r2_handle],
            return_when=stagegate.ALL_COMPLETED,
        )
        trimmed_r1 = trim_r1_handle.result()
        trimmed_r2 = trim_r2_handle.result()

        self.stage_forward()

        mapping_handle = self.task(
            map_reads,
            resources={"cpu": 16, "mem": 64},
            args=(trimmed_r1, trimmed_r2),
        ).run()
        bam_path = mapping_handle.result()

        self.stage_forward()

        cleanup_handle = self.task(
            cleanup_files,
            resources={"cpu": 1},
            args=(self.sample_id,),
        ).run()
        cleanup_handle.result()
        return bam_path
```

Shown here as an RNA-seq-flavored example, typical bioinformatics pipelines often have this shape:

- many lightweight preparation tasks
- one heavier aggregate or mapping step
- final cleanup of temporary outputs

Why it fits:

- each sample is naturally a pipeline
- each phase has a clear barrier
- later-stage work should be favored so partially processed samples finish

## 2. Numerical Multi-Start Optimization

Pseudo-code:

```python
class MultiStartPipeline(stagegate.Pipeline):
    def __init__(self, starts) -> None:
        self.starts = list(starts)

    def run(self):
        handles = [
            self.task(run_solver, resources={"cpu": 2}, args=(x0,)).run()
            for x0 in self.starts
        ]

        pending = set(handles)
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
                    if len(accepted) >= 5:
                        for other in pending:
                            other.cancel()
                        break

        if len(accepted) < 5:
            raise RuntimeError("not enough acceptable results")

        self.stage_forward()
        summary_handle = self.task(
            summarize_results,
            resources={"cpu": 1},
            args=(accepted,),
        ).run()
        return summary_handle.result()
```

Cancellation is best-effort: queued tasks may be cancelled, but already running tasks continue.

Why it fits:

- many sibling tasks are launched together
- progress is observed incrementally through handles
- domain-level soft failure can remain a normal return value

## 3. Machine-Learning Batch Experiments

Pseudo-code:

```python
class BatchExperimentPipeline(stagegate.Pipeline):
    def __init__(self, configs) -> None:
        self.configs = list(configs)

    def run(self):
        handles = [
            self.task(train_and_eval, resources={"cpu": 4, "mem": 16}, args=(cfg,)).run()
            for cfg in self.configs
        ]

        pending = set(handles)
        accepted_models = []

        while pending and len(accepted_models) < 3:
            done, pending = self.wait(
                pending,
                return_when=stagegate.FIRST_COMPLETED,
            )
            for handle in done:
                report = handle.result()
                if report.score >= 0.9:
                    accepted_models.append(report.model_path)
                    if len(accepted_models) >= 3:
                        for other in pending:
                            other.cancel()
                        break

        if len(accepted_models) < 3:
            raise RuntimeError("not enough good models")

        self.stage_forward()
        package_handle = self.task(
            package_models,
            resources={"cpu": 1},
            args=(accepted_models,),
        ).run()
        return package_handle.result()
```

Cancellation is best-effort: queued tasks may be cancelled, but already running tasks continue.

Why it fits:

- tasks are independent but belong to one pipeline stage
- the pipeline can stop early once enough acceptable models exist
- later packaging work should not be starved by older low-value experiments

## 4. System Administration and Log Collection

Pseudo-code:

```python
class HostLogPipeline(stagegate.Pipeline):
    def __init__(self, host: str) -> None:
        self.host = host

    def run(self) -> None:
        fetch_handle = self.task(
            fetch_logs,
            resources={"cpu": 1, "net": 1},
            args=(self.host,),
        ).run()
        log_bundle = fetch_handle.result()

        self.stage_forward()

        parse_handle = self.task(
            parse_logs,
            resources={"cpu": 2, "mem": 4},
            args=(log_bundle,),
        ).run()
        parsed = parse_handle.result()

        self.stage_forward()

        archive_handle = self.task(
            archive_logs,
            resources={"cpu": 1},
            args=(self.host, parsed),
        ).run()
        archive_handle.result()
```

Why it fits:

- each host or service can map to one pipeline
- failures are observed directly through handle results
- already-started hosts continue draining through later stages

## 5. Top-Level Coordination

At the application boundary, users submit multiple pipelines and wait on pipeline handles as a group.

Pseudo-code:

```python
scheduler = stagegate.Scheduler(
    resources={"cpu": 32, "mem": 256},
    pipeline_parallelism=4,
    task_parallelism=8,
)
try:
    pipeline_handles = [
        scheduler.run_pipeline(RNASeqPipeline(sample_id))
        for sample_id in ["S1", "S2", "S3"]
    ]

    done, pending = scheduler.wait_pipelines(
        pipeline_handles,
        return_when=stagegate.ALL_COMPLETED,
    )
    assert not pending

    outputs = [handle.result() for handle in done]
finally:
    scheduler.close()
```

This is where `Scheduler.wait_pipelines(...)` belongs. Inside `Pipeline.run()`, use `Pipeline.wait(...)` for task handles.

## 6. What To Avoid Expecting

Users should not expect `stagegate` to:

- infer task graphs automatically
- reprioritize already queued tasks after `stage_forward()`
- bypass a blocked high-priority task with a lower-priority task
- resume from checkpoints
- kill already running tasks or pipelines

Those are deliberate design constraints.
