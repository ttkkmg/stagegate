# stagegate

`stagegate` is a small, explicit, single-process Python library for running
many forward-only pipelines with:

- FIFO pipeline scheduling
- stage-aware task priority
- strict head-of-line blocking
- abstract resource quotas

It is aimed at local batch workloads that have grown beyond a plain thread pool
but do not justify a full workflow engine.

## Why Stagegate

`stagegate` is built for workloads where:

- many similar pipelines run on one machine
- tasks have heterogeneous CPU / memory / other resource needs
- later-stage work should be preferred over endlessly producing more upstream
  intermediates
- tasks that spend meaningful time in NumPy, SciPy, Pandas, other C-extension code, or external CLI tools

The scheduler stays intentionally small:

- one Python process
- multiple threads
- no DAG engine
- no persistence layer
- no distributed runtime

## Install

```bash
pip install stagegate
```

## Quick Example

```python
import stagegate


class ExamplePipeline(stagegate.Pipeline):
    def run(self) -> int:
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


with stagegate.Scheduler(
    resources={"cpu": 8, "mem": 32},
    pipeline_parallelism=2,
    task_parallelism=4,
) as scheduler:
    handle = scheduler.run_pipeline(ExamplePipeline())
    print(handle.result())
```

## Cooperative Terminate

For long-running tasks, `stagegate` supports cooperative terminate:

- `TaskHandle.request_terminate()`
- `stagegate.terminate_requested()`
- `stagegate.run_subprocess(...)`

The scheduler itself does not forcibly stop arbitrary running Python code or
running pipelines. Cooperative terminate is opt-in.

`stagegate.run_subprocess(...)` is currently intended for POSIX platforms such
as Linux, macOS, and BSD. `stagegate` itself may still be usable on Windows if
your tasks do not depend on that helper.

## Documentation

User's guide, use cases and API reference can be found at [http://stagegate.readthedocs.io](http://stagegate.readthedocs.io).
