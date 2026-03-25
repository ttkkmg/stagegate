from __future__ import annotations

from pathlib import Path
import shlex
import signal
import threading
import time
from typing import Any

import pytest

import stagegate
import stagegate.subprocesses as subprocess_helpers
from stagegate._states import TaskState


PROBE_PATH = Path(__file__).parent / "helpers" / "subprocess_probe.py"


def wait_for_tracked_subprocess_count(expected: int, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        with subprocess_helpers._TRACKED_SUBPROCESSES_LOCK:
            count = len(subprocess_helpers._TRACKED_SUBPROCESSES)
        if count == expected:
            return
        assert time.monotonic() < deadline
        time.sleep(0.01)


def run_in_thread_and_capture(fn, *args: Any, **kwargs: Any) -> tuple[threading.Thread, dict[str, Any]]:
    result: dict[str, Any] = {}

    def target() -> None:
        try:
            result["value"] = fn(*args, **kwargs)
        except BaseException as exc:  # pragma: no cover - diagnostic path
            result["error"] = exc

    thread = threading.Thread(target=target, name="subprocess-capture")
    thread.start()
    return thread, result


class CooperativeTerminatePipeline(stagegate.Pipeline):
    def __init__(self) -> None:
        self.task_started = threading.Event()
        self.task_handle: stagegate.TaskHandle | None = None

    def _child(self) -> None:
        self.task_started.set()
        while True:
            if stagegate.terminate_requested():
                raise stagegate.TerminatedError(
                    argv=(),
                    pid=None,
                    returncode=None,
                    forced_kill=False,
                )
            time.sleep(0.01)

    def run(self) -> str:
        self.task_handle = self.task(self._child, resources={"cpu": 1}).run()
        return "pipeline-done"


class UnexpectedTerminatePipeline(stagegate.Pipeline):
    def __init__(self) -> None:
        self.task_handle: stagegate.TaskHandle | None = None

    def _child(self) -> None:
        raise stagegate.TerminatedError(
            argv=(),
            pid=None,
            returncode=None,
            forced_kill=False,
        )

    def run(self) -> str:
        self.task_handle = self.task(self._child, resources={"cpu": 1}).run()
        return "pipeline-done"


class WaitFirstExceptionOnTerminatePipeline(stagegate.Pipeline):
    def __init__(self) -> None:
        self.handles_ready = threading.Event()
        self.block_release = threading.Event()
        self.terminate_handle: stagegate.TaskHandle | None = None
        self.blocking_handle: stagegate.TaskHandle | None = None
        self.wait_done: set[stagegate.TaskHandle] | None = None
        self.wait_pending: set[stagegate.TaskHandle] | None = None

    def _terminating_child(self) -> None:
        while True:
            if stagegate.terminate_requested():
                raise stagegate.TerminatedError(
                    argv=(),
                    pid=None,
                    returncode=None,
                    forced_kill=False,
                )
            time.sleep(0.01)

    def _blocking_child(self) -> str:
        self.block_release.wait(timeout=1.0)
        return "done"

    def run(self) -> str:
        self.terminate_handle = self.task(
            self._terminating_child,
            resources={"cpu": 1},
        ).run()
        self.blocking_handle = self.task(
            self._blocking_child,
            resources={"cpu": 1},
        ).run()
        self.handles_ready.set()
        self.wait_done, self.wait_pending = self.wait(
            [self.terminate_handle, self.blocking_handle],
            return_when=stagegate.FIRST_EXCEPTION,
        )
        return "pipeline-done"


class SubprocessTaskPipeline(stagegate.Pipeline):
    def __init__(
        self,
        argv: list[str],
        *,
        terminate_grace_seconds: float | None = 5.0,
    ) -> None:
        self.argv = argv
        self.terminate_grace_seconds = terminate_grace_seconds
        self.task_started = threading.Event()
        self.task_handle: stagegate.TaskHandle | None = None

    def _child(self) -> int:
        self.task_started.set()
        return stagegate.run_subprocess(
            self.argv,
            terminate_grace_seconds=self.terminate_grace_seconds,
        )

    def run(self) -> str:
        self.task_handle = self.task(self._child, resources={"cpu": 1}).run()
        return "pipeline-done"


class ShellTaskPipeline(stagegate.Pipeline):
    def __init__(
        self,
        command: str,
        *,
        terminate_grace_seconds: float | None = 5.0,
    ) -> None:
        self.command = command
        self.terminate_grace_seconds = terminate_grace_seconds
        self.task_started = threading.Event()
        self.task_handle: stagegate.TaskHandle | None = None

    def _child(self) -> int:
        self.task_started.set()
        return stagegate.run_shell(
            self.command,
            terminate_grace_seconds=self.terminate_grace_seconds,
        )

    def run(self) -> str:
        self.task_handle = self.task(self._child, resources={"cpu": 1}).run()
        return "pipeline-done"


class TerminateTlsCleanupPipeline(stagegate.Pipeline):
    def __init__(self) -> None:
        self.first_started = threading.Event()
        self.first_handle: stagegate.TaskHandle | None = None
        self.second_handle: stagegate.TaskHandle | None = None

    def _terminating_child(self) -> None:
        self.first_started.set()
        while True:
            if stagegate.terminate_requested():
                raise stagegate.TerminatedError(
                    argv=(),
                    pid=None,
                    returncode=None,
                    forced_kill=False,
                )
            time.sleep(0.01)

    def _tls_probe_child(self) -> bool:
        return stagegate.terminate_requested()

    def run(self) -> bool:
        self.first_handle = self.task(
            self._terminating_child,
            resources={"cpu": 1},
        ).run()
        self.wait([self.first_handle], return_when=stagegate.ALL_COMPLETED)
        self.second_handle = self.task(
            self._tls_probe_child,
            resources={"cpu": 1},
        ).run()
        return self.second_handle.result(timeout=1.0)


def test_terminate_requested_returns_false_without_task_context() -> None:
    assert stagegate.terminate_requested() is False


def test_request_terminate_running_task_produces_terminated_outcome() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    pipeline = CooperativeTerminatePipeline()

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) == "pipeline-done"
    assert pipeline.task_handle is not None
    assert pipeline.task_started.wait(timeout=1.0) is True
    time.sleep(0.05)
    assert pipeline.task_handle.request_terminate() is True

    with pytest.raises(stagegate.TerminatedError):
        pipeline.task_handle.result(timeout=1.0)

    error = pipeline.task_handle.exception(timeout=0)
    assert isinstance(error, stagegate.TerminatedError)
    assert pipeline.task_handle._record.state is TaskState.TERMINATED
    snapshot = scheduler.snapshot()
    assert snapshot.tasks.terminated == 1


def test_terminated_error_without_request_is_classified_as_failed() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    pipeline = UnexpectedTerminatePipeline()

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) == "pipeline-done"
    assert pipeline.task_handle is not None
    with pytest.raises(stagegate.TerminatedError):
        pipeline.task_handle.result(timeout=1.0)

    assert pipeline.task_handle._record.state is TaskState.FAILED


def test_wait_first_exception_triggers_for_terminated_task() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=2)
    pipeline = WaitFirstExceptionOnTerminatePipeline()

    handle = scheduler.run_pipeline(pipeline)

    assert pipeline.handles_ready.wait(timeout=1.0) is True
    assert pipeline.terminate_handle is not None
    assert pipeline.blocking_handle is not None
    deadline = time.monotonic() + 1.0
    while not pipeline.terminate_handle.running():
        assert time.monotonic() < deadline
        time.sleep(0.01)
    assert pipeline.terminate_handle.request_terminate() is True

    assert handle.result(timeout=1.0) == "pipeline-done"
    assert pipeline.wait_done == {pipeline.terminate_handle}
    assert pipeline.wait_pending == {pipeline.blocking_handle}
    pipeline.block_release.set()
    assert pipeline.blocking_handle.result(timeout=1.0) == "done"


def test_terminated_task_does_not_leak_tls_into_later_task() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    pipeline = TerminateTlsCleanupPipeline()

    handle = scheduler.run_pipeline(pipeline)

    assert pipeline.first_started.wait(timeout=1.0) is True
    assert pipeline.first_handle is not None
    assert pipeline.first_handle.request_terminate() is True

    assert handle.result(timeout=1.0) is False
    assert pipeline.second_handle is not None
    assert pipeline.second_handle.result(timeout=0) is False


def test_run_subprocess_returns_probe_exit_code() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    argv = [str(PROBE_PATH), "20", "30", "1", "2", "3"]
    pipeline = SubprocessTaskPipeline(argv)

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) == "pipeline-done"
    assert pipeline.task_handle is not None
    assert pipeline.task_handle.result(timeout=1.0) == ((20 + 30 + 1 + 2 + 3) & 0x7F)


def test_run_subprocess_graceful_terminate_raises_terminated_error() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    argv = [str(PROBE_PATH), "1000", "50", "1", "2"]
    pipeline = SubprocessTaskPipeline(argv, terminate_grace_seconds=0.5)

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) == "pipeline-done"
    assert pipeline.task_handle is not None
    assert pipeline.task_started.wait(timeout=1.0) is True
    time.sleep(0.05)
    assert pipeline.task_handle.request_terminate() is True

    with pytest.raises(stagegate.TerminatedError) as exc_info:
        pipeline.task_handle.result(timeout=2.0)

    error = exc_info.value
    assert error.argv == tuple(argv)
    assert isinstance(error.pid, int)
    assert error.pid > 0
    assert error.returncode == (((1000 + 50 + 1 + 2) & 0x7F) | 0x80)
    assert error.forced_kill is False
    assert pipeline.task_handle._record.state is TaskState.TERMINATED


def test_run_subprocess_forced_kill_sets_forced_kill_flag() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    argv = [str(PROBE_PATH), "--ignore-term", "1000", "50", "1", "2"]
    pipeline = SubprocessTaskPipeline(argv, terminate_grace_seconds=0)

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) == "pipeline-done"
    assert pipeline.task_handle is not None
    assert pipeline.task_started.wait(timeout=1.0) is True
    time.sleep(0.05)
    assert pipeline.task_handle.request_terminate() is True

    with pytest.raises(stagegate.TerminatedError) as exc_info:
        pipeline.task_handle.result(timeout=2.0)

    error = exc_info.value
    assert error.argv == tuple(argv)
    assert isinstance(error.pid, int)
    assert error.pid > 0
    assert error.returncode == -signal.SIGKILL
    assert error.forced_kill is True
    assert pipeline.task_handle._record.state is TaskState.TERMINATED


def test_run_subprocess_rejects_negative_grace_timeout() -> None:
    with pytest.raises(ValueError):
        stagegate.run_subprocess(
            [str(PROBE_PATH), "0", "0"],
            terminate_grace_seconds=-0.1,
        )


def test_run_subprocess_rejects_win32(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subprocess_helpers.sys, "platform", "win32")

    with pytest.raises(NotImplementedError, match="POSIX platforms"):
        stagegate.run_subprocess(["dummy-command"])


def test_run_subprocess_none_grace_timeout_allows_sigterm_only_termination() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    argv = [str(PROBE_PATH), "1000", "50", "1", "2"]
    pipeline = SubprocessTaskPipeline(argv, terminate_grace_seconds=None)

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) == "pipeline-done"
    assert pipeline.task_handle is not None
    assert pipeline.task_started.wait(timeout=1.0) is True
    time.sleep(0.05)
    assert pipeline.task_handle.request_terminate() is True

    with pytest.raises(stagegate.TerminatedError) as exc_info:
        pipeline.task_handle.result(timeout=2.0)

    error = exc_info.value
    assert error.argv == tuple(argv)
    assert isinstance(error.pid, int)
    assert error.pid > 0
    assert error.returncode == (((1000 + 50 + 1 + 2) & 0x7F) | 0x80)
    assert error.forced_kill is False


def test_run_shell_returns_zero_and_supports_pipe_and_redirection(
    tmp_path: Path,
) -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    output_path = tmp_path / "upper.txt"
    command = f"printf 'abc' | tr a-z A-Z > {shlex.quote(str(output_path))}"
    pipeline = ShellTaskPipeline(command)

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) == "pipeline-done"
    assert pipeline.task_handle is not None
    assert pipeline.task_handle.result(timeout=1.0) == 0
    assert output_path.read_text() == "ABC"


def test_run_shell_graceful_terminate_raises_terminated_error() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    command = f"{shlex.quote(str(PROBE_PATH))} 1000 50 1 2"
    pipeline = ShellTaskPipeline(command, terminate_grace_seconds=0.5)

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) == "pipeline-done"
    assert pipeline.task_handle is not None
    assert pipeline.task_started.wait(timeout=1.0) is True
    time.sleep(0.05)
    assert pipeline.task_handle.request_terminate() is True

    with pytest.raises(stagegate.TerminatedError) as exc_info:
        pipeline.task_handle.result(timeout=2.0)

    error = exc_info.value
    assert error.argv == ("/bin/sh", "-c", command)
    assert isinstance(error.pid, int)
    assert error.pid > 0
    assert error.returncode == -signal.SIGTERM
    assert error.forced_kill is False
    assert pipeline.task_handle._record.state is TaskState.TERMINATED


def test_run_shell_forced_kill_sets_forced_kill_flag() -> None:
    scheduler = stagegate.Scheduler(resources={"cpu": 2}, task_parallelism=1)
    command = f"{shlex.quote(str(PROBE_PATH))} --ignore-term 1000 50 1 2"
    pipeline = ShellTaskPipeline(command, terminate_grace_seconds=0)

    handle = scheduler.run_pipeline(pipeline)

    assert handle.result(timeout=1.0) == "pipeline-done"
    assert pipeline.task_handle is not None
    assert pipeline.task_started.wait(timeout=1.0) is True
    time.sleep(0.05)
    assert pipeline.task_handle.request_terminate() is True

    with pytest.raises(stagegate.TerminatedError) as exc_info:
        pipeline.task_handle.result(timeout=2.0)

    error = exc_info.value
    assert error.argv == ("/bin/sh", "-c", command)
    assert isinstance(error.pid, int)
    assert error.pid > 0
    assert error.returncode in {-signal.SIGTERM, -signal.SIGKILL}
    assert error.forced_kill is True
    assert pipeline.task_handle._record.state is TaskState.TERMINATED


def test_run_shell_rejects_negative_grace_timeout() -> None:
    with pytest.raises(ValueError):
        stagegate.run_shell("true", terminate_grace_seconds=-0.1)


def test_run_shell_rejects_win32(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subprocess_helpers.sys, "platform", "win32")

    with pytest.raises(NotImplementedError, match="POSIX platforms"):
        stagegate.run_shell("echo hi")


def test_terminate_tracked_subprocesses_returns_zero_when_registry_is_empty() -> None:
    wait_for_tracked_subprocess_count(0)

    assert stagegate.terminate_tracked_subprocesses() == 0


def test_terminate_tracked_subprocesses_signals_run_subprocess_child_group() -> None:
    argv = [str(PROBE_PATH), "1000", "20", "1", "2"]
    thread, result = run_in_thread_and_capture(stagegate.run_subprocess, argv)

    try:
        wait_for_tracked_subprocess_count(1)
        assert stagegate.terminate_tracked_subprocesses() == 1
        thread.join(timeout=2.0)
        assert thread.is_alive() is False
        assert "error" not in result
        assert isinstance(result["value"], int)
        assert result["value"] != ((1000 + 20 + 1 + 2) & 0x7F)
        wait_for_tracked_subprocess_count(0)
    finally:
        if thread.is_alive():
            stagegate.terminate_tracked_subprocesses()
        thread.join(timeout=0.1)


def test_terminate_tracked_subprocesses_signals_run_shell_child_group() -> None:
    command = f"{shlex.quote(str(PROBE_PATH))} 1000 20 1 2"
    thread, result = run_in_thread_and_capture(stagegate.run_shell, command)

    try:
        wait_for_tracked_subprocess_count(1)
        assert stagegate.terminate_tracked_subprocesses() == 1
        thread.join(timeout=2.0)
        assert thread.is_alive() is False
        assert "error" not in result
        assert isinstance(result["value"], int)
        assert result["value"] != 0
        wait_for_tracked_subprocess_count(0)
    finally:
        if thread.is_alive():
            stagegate.terminate_tracked_subprocesses()
        thread.join(timeout=0.1)


def test_terminate_tracked_subprocesses_does_not_keep_naturally_exited_processes() -> (
    None
):
    argv = [str(PROBE_PATH), "20", "0", "1", "2"]
    thread, result = run_in_thread_and_capture(stagegate.run_subprocess, argv)

    thread.join(timeout=2.0)
    assert thread.is_alive() is False
    assert "error" not in result
    assert result["value"] == ((20 + 0 + 1 + 2) & 0x7F)
    wait_for_tracked_subprocess_count(0)
    assert stagegate.terminate_tracked_subprocesses() == 0


def test_terminate_tracked_subprocesses_rejects_win32(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subprocess_helpers.sys, "platform", "win32")

    with pytest.raises(NotImplementedError, match="POSIX platforms"):
        stagegate.terminate_tracked_subprocesses()
