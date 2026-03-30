"""Runtime bridge from validated minigate programs to stagegate execution."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import stagegate

from .ast import (
    AcceptRetvalsRange,
    AcceptRetvalsValue,
    ValidatedProgram,
    ValidatedRunListStatement,
    ValidatedRunStatement,
)
from .eval import (
    evaluate_list_template,
    evaluate_scalar_template,
    make_evaluation_context,
)
from .errors import StaticValidationError
from .input_loader import InputLoaderConfig, InputRow, load_rows
from .render import RuntimeReporter

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from typing import TextIO


@dataclass(frozen=True, slots=True)
class RuntimeOverrides:
    """CLI-level runtime overrides applied after static validation."""

    pipelines: int | None = None
    tasks: int | None = None
    limits: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True, slots=True)
class RuntimeOptions:
    """Execution-time options that do not change validated semantics."""

    verbose: int = 0
    dry_run: bool = False
    stdin_text: str | None = None


class AcceptRetvalsError(RuntimeError):
    """Raised when a command returns a disallowed exit code."""


def _execute_command(command: str, *, dry_run: bool) -> int:
    if dry_run:
        return 0
    return stagegate.run_shell(command)


def _format_accept_retvals(items) -> str:
    parts: list[str] = []
    for item in items:
        if isinstance(item, AcceptRetvalsValue):
            parts.append(str(item.value))
            continue
        assert isinstance(item, AcceptRetvalsRange)
        parts.append(f"{item.left}-{item.right}")
    return ", ".join(parts)


def _retcode_accepted(items, return_code: int) -> bool:
    for item in items:
        if isinstance(item, AcceptRetvalsValue):
            if return_code == item.value:
                return True
            continue
        assert isinstance(item, AcceptRetvalsRange)
        if item.left <= return_code <= item.right:
            return True
    return False


def _merge_limit_overrides(
    program: ValidatedProgram,
    overrides: RuntimeOverrides,
) -> dict[str, int]:
    merged = {label: int(value) for label, value in program.config.limits}
    for label, value in overrides.limits:
        merged[label] = int(value)
    return merged


def apply_runtime_overrides(
    program: ValidatedProgram,
    overrides: RuntimeOverrides,
) -> ValidatedProgram:
    """Apply CLI-level runtime overrides and validate the effective config."""

    effective_pipelines = (
        program.config.pipelines if overrides.pipelines is None else overrides.pipelines
    )
    effective_tasks = (
        program.config.tasks if overrides.tasks is None else overrides.tasks
    )
    if effective_pipelines < 1:
        raise StaticValidationError("effective pipelines must be >= 1")
    if effective_tasks < 1:
        raise StaticValidationError("effective tasks must be >= 1")

    merged_limits = _merge_limit_overrides(program, overrides)
    for label, value in merged_limits.items():
        if value < 0:
            raise StaticValidationError(f"resource limit must be >= 0: {label}")

    for stage in program.stages:
        for label, value in stage.resources:
            if label not in merged_limits:
                raise StaticValidationError(f"undefined resource label in use: {label}")
            if value < 0:
                raise StaticValidationError(f"use amount must be >= 0: {label}")
            if value > merged_limits[label]:
                raise StaticValidationError(
                    f"use amount exceeds effective limit for resource: {label}"
                )

    return replace(
        program,
        config=replace(
            program.config,
            pipelines=effective_pipelines,
            tasks=effective_tasks,
            limits=tuple(merged_limits.items()),
        ),
    )


class _MinigatePipeline(stagegate.Pipeline):
    """One input row compiled into a stagegate pipeline."""

    def __init__(
        self,
        *,
        program: ValidatedProgram,
        row: InputRow,
        reporter: RuntimeReporter | None,
        dry_run: bool,
    ) -> None:
        super().__init__()
        self._program = program
        self._row = row
        self._reporter = reporter
        self._dry_run = dry_run

    def _render_and_submit(
        self,
        *,
        stage_number: int,
        statement_name: str,
        commands: tuple[str, ...],
        resources: dict[str, int],
        accept_retvals,
    ) -> None:
        if self._reporter is not None:
            self._reporter.emit_command_group(
                stage_number=stage_number,
                statement_name=statement_name,
                commands=commands,
            )

        handles = tuple(
            self.task(
                _execute_command,
                resources=resources,
                kwargs={"command": command, "dry_run": self._dry_run},
                name=command,
            ).run()
            for command in commands
        )
        done, pending = self.wait(handles, return_when=stagegate.ALL_COMPLETED)
        assert not pending
        for handle, command in zip(handles, commands, strict=True):
            return_code = handle.result()
            if not _retcode_accepted(accept_retvals, return_code):
                raise AcceptRetvalsError(
                    "command returned disallowed exit code "
                    f"{return_code}: {command!r}; "
                    f"accepted={_format_accept_retvals(accept_retvals)}"
                )

    def run(self) -> None:
        context = make_evaluation_context(self._program, self._row)
        last_stage_index = len(self._program.stages) - 1

        for stage_index, stage in enumerate(self._program.stages):
            resources = {label: value for label, value in stage.resources}
            if isinstance(stage.execution, ValidatedRunStatement):
                commands = tuple(
                    evaluate_scalar_template(command, context=context)
                    for command in stage.execution.commands
                )
                self._render_and_submit(
                    stage_number=stage.stage_number,
                    statement_name="run",
                    commands=commands,
                    resources=resources,
                    accept_retvals=stage.accept_retvals,
                )
            else:
                assert isinstance(stage.execution, ValidatedRunListStatement)
                commands = evaluate_list_template(
                    stage.execution.command, context=context
                )
                self._render_and_submit(
                    stage_number=stage.stage_number,
                    statement_name="run_list",
                    commands=commands,
                    resources=resources,
                    accept_retvals=stage.accept_retvals,
                )

            if stage_index != last_stage_index:
                self.stage_forward()


def run_program(
    program: ValidatedProgram,
    *,
    sources: Sequence[str],
    overrides: RuntimeOverrides | None = None,
    options: RuntimeOptions | None = None,
    stdout: TextIO | None = None,
    warning_sink: Callable[[str], None] | None = None,
) -> None:
    """Execute a validated program against input sources."""

    overrides = RuntimeOverrides() if overrides is None else overrides
    options = RuntimeOptions() if options is None else options
    effective_program = apply_runtime_overrides(program, overrides)
    reporter = None
    if (options.verbose > 0 or options.dry_run) and stdout is not None:
        reporter = RuntimeReporter(stdout)

    if warning_sink is None:

        def warning_sink(_message: str) -> None:
            return None

    loader_config = InputLoaderConfig(
        format_name=effective_program.config.format_name,
        header=effective_program.config.header,
        codepage=effective_program.config.codepage,
        delimiter=effective_program.config.delimiter,
    )
    row_iter = iter(
        load_rows(
            config=loader_config,
            sources=sources,
            warning_sink=warning_sink,
            stdin_text=options.stdin_text,
        )
    )

    scheduler = stagegate.Scheduler(
        resources=(
            None
            if not effective_program.config.limits
            else {label: value for label, value in effective_program.config.limits}
        ),
        pipeline_parallelism=effective_program.config.pipelines,
        task_parallelism=effective_program.config.tasks,
        exception_exit_policy="fail_fast",
    )

    active_handles: set[stagegate.PipelineHandle] = set()
    exhausted = False
    try:
        with scheduler:
            while True:
                while (
                    not exhausted
                    and len(active_handles) < effective_program.config.pipelines
                ):
                    try:
                        row = next(row_iter)
                    except StopIteration:
                        exhausted = True
                        break
                    pipeline = _MinigatePipeline(
                        program=effective_program,
                        row=row,
                        reporter=reporter,
                        dry_run=options.dry_run,
                    )
                    active_handles.add(
                        scheduler.run_pipeline(
                            pipeline,
                            name=f"{row.source_name}:row={row.row_number}",
                        )
                    )

                if not active_handles:
                    break

                done, pending = scheduler.wait_pipelines(
                    active_handles,
                    return_when=stagegate.FIRST_COMPLETED,
                )
                for handle in done:
                    try:
                        handle.result()
                    finally:
                        if handle.done() and not handle.cancelled():
                            handle.discard()
                active_handles = pending
    except BaseException:
        scheduler.close(cancel_pending_pipelines=True)
        raise
