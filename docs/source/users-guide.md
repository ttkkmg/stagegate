# User's Guide

`stagegate` is for people whose local batch scripts have grown beyond a plain
thread pool, but who still do not want the cost and complexity of a full
workflow engine.

If your workloads look like:

- many similar pipelines running on one machine
- tasks with heterogeneous resource needs
- later stages that should be preferred over endlessly producing more
  intermediate data
- tasks that spend meaningful time in NumPy, SciPy, Pandas, other C-extension code, or external CLI tools

then `stagegate` is the right shape of tool.

## The Problem It Solves

Many local workflows start simple:

- submit a batch of tasks
- wait for them
- inspect the results
- launch the next wave

That model works until the tasks stop being uniform.

One task needs a lot of RAM. Another needs more CPU. A third launches a heavy
external binary. Some jobs are quick, others are long. Meanwhile, the early
stage of the pipeline can generate temporary data much faster than later stages
can consume it.

At that point, plain thread pools become too blunt:

- they limit concurrency by worker count, not by resource shape
- they have no native concept of pipeline stage
- they happily flood the machine with upstream work
- they give you little structure for coordinating multiple forward-only
  pipelines

The usual next suggestion is to adopt a workflow engine. Sometimes that is the
right answer. But for a single powerful workstation, a thick cloud instance, or
other single-node batch environment, that can be a large jump in complexity:

- DAG modeling
- orchestration frameworks
- brokers or external services
- persistence and resume machinery
- operational overhead that may not match the actual problem

`stagegate` exists for the space between those two extremes.

## What Stagegate Is

`stagegate` is a small, explicit, single-process Python scheduler for
forward-only pipelines.

It provides:

- FIFO pipeline scheduling
- stage-aware task priority
- strict head-of-line blocking
- abstract resource quotas
- owner-scoped wait APIs

It stays intentionally small:

- one Python process
- multiple threads
- no distributed runtime
- no DAG engine
- no persistence layer

This keeps behavior easier to reason about and debug.

## The Core Mental Model

1. define a pipeline as a Python class
2. submit task waves from `Pipeline.run()`
3. wait on the relevant task handles
4. call `stage_forward()` when moving downstream
5. let the scheduler prioritize later-stage work

The scheduler has two layers.

### Pipeline Layer

The pipeline layer is FIFO.

- queued pipelines wait in submission order
- coordinator threads start `Pipeline.run()`
- a pipeline instance is single-use at submission time

This gives you explicit, stateful pipeline objects without introducing a
workflow graph DSL.

### Task Layer

The task layer is stage-aware and resource-aware.

- tasks are queued with a submit-time stage snapshot
- later-stage tasks outrank earlier-stage tasks
- admission is constrained by abstract resource quotas
- admitted tasks reserve resources before they begin running

This is where `stagegate` differs most from a generic thread pool.

## Why Stage Awareness Matters

The central scheduling idea in `stagegate` is that later-stage tasks should be
preferred over earlier-stage tasks.

Why? Because many pipelines are not bottlenecked by CPU alone. They are
bottlenecked by the accumulation of unfinished intermediate work.

Suppose stage 1 generates large temporary files and stage 2 compresses,
aggregates, or deletes them. A naive scheduler may maximize short-term worker
occupancy by launching more and more stage-1 tasks. The result is often:

- temporary-file explosion
- memory pressure
- disk pressure
- long time-to-first-finished-result

`stagegate` pushes in the opposite direction. Once a pipeline has progressed,
its downstream tasks are favored. This tends to:

- bound the amount of intermediate state in flight
- produce completed results continuously
- keep partially processed pipelines draining instead of piling up

This is especially valuable in scientific and media-processing workloads where
intermediate artifacts are expensive.

## Resource Management Is Abstract on Purpose

`stagegate` resources are scheduler-defined admission labels, not OS-enforced
measurements.

You might configure:

```python
{"cpu": 16, "mem_gb": 64, "gpu_vram_gb": 24}
```

and then assign task requirements such as:

```python
{"cpu": 4, "mem_gb": 32}
```

The scheduler uses those labels only to decide whether a task may be admitted.
This is deliberate:

- it keeps the implementation small
- it avoids pretending to be an operating-system-level resource manager
- it lets you model the constraints that actually matter in your environment

If you create `Scheduler()` without `resources=...`, no resource labels exist.
In that mode, tasks should omit `resources`; supplying non-empty task resource
requirements will be rejected.

## What Writing a Pipeline Feels Like

A pipeline usually reads like structured Python, not orchestration metadata.

Typical shape:

```python
class ExamplePipeline(stagegate.Pipeline):
    def run(self):
        a = self.task(step_a, resources={"cpu": 1}).run()
        b = self.task(step_b, resources={"cpu": 1}).run()

        self.wait([a, b], return_when=stagegate.ALL_COMPLETED)
        result_a = a.result()
        result_b = b.result()

        self.stage_forward()

        c = self.task(
            combine,
            resources={"cpu": 4, "mem": 16},
            args=(result_a, result_b),
        ).run()
        return c.result()
```

The library does not try to infer structure that your code can already express
directly.

## Failure Semantics Stay Simple

At the scheduler level:

- return normally -> success
- raise an exception -> failure

If your domain has soft-failure states such as:

- score too low
- no biological signal found
- optimization did not converge
- candidate was not useful

those should generally stay in the return value rather than becoming
scheduler-level failure states.

## Cooperative Terminate for Long-Running Work

Some workloads need more than queued-task cancellation.

A common pattern is:

1. launch many sibling tasks
2. keep the first few useful results
3. stop the rest if they are no longer worth running

For that, `stagegate` supports cooperative terminate.

From outside the task:

- `TaskHandle.request_terminate()`

Inside the task:

- `stagegate.terminate_requested()`

For external processes:

- `stagegate.run_subprocess(...)`
- `stagegate.run_shell(...)`

This keeps the scheduler itself non-preemptive while still giving users a
practical stop path for long-running work.

The intended model is cooperative:

- running Python code polls at safe checkpoints
- subprocess-heavy tasks use the provided helper
- the scheduler does not pretend to kill arbitrary Python code safely

`stagegate` remains explicit about what it can and cannot stop.

The subprocess helper is currently intended for POSIX platforms such as Linux,
macOS, and BSD. It relies on process-group signaling with `SIGTERM` and
optional `SIGKILL`. If you do not use `stagegate.run_subprocess(...)`,
`stagegate.run_shell(...)`, `stagegate` may still be a reasonable fit for
Windows-based workloads, but these terminate paths are not yet documented as
Windows-compatible.

## When Stagegate Is a Good Fit

`stagegate` is especially well matched to:

- bioinformatics and genomics pipelines
- numerical multi-start optimization
- local machine-learning inference or evaluation batches
- media-processing and transcoding chains
- system administration batches with staged processing

These workloads often share the same properties:

- repeated forward-only phases
- many sibling tasks at each phase
- heterogeneous resource pressure
- benefit from draining already-started pipelines

## When It Is Not the Right Tool

`stagegate` is intentionally not:

- a DAG workflow engine
- a persistent scheduler with resume/recovery
- a distributed execution system
- a broker-backed job queue
- an OS-level resource enforcement system

## Design Values

The project is intentionally opinionated about a few things.

### Explicit Over Magical

Pipelines are Python classes. Tasks are explicit submissions. Waiting is
explicit. Stage changes are explicit.

### Predictability Over Clever Throughput

The dispatcher uses strict head-of-line blocking. If the highest-priority task
cannot run because resources are unavailable, lower-priority tasks do not jump
ahead.

This may leave some capacity temporarily idle, but it preserves scheduling
behavior that is much easier to reason about.

### Small Over General

The project deliberately avoids becoming a general workflow platform. The goal
is a narrow tool that remains understandable.

## Practical Next Step

If this model sounds like your workload, the next pages to read are:

- **Use Cases** for representative patterns
- **API Reference** for the public surface
