"""Command-line entry point for minigate."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import TYPE_CHECKING

from .errors import CliUsageError, MinigateError
from .parser import parse_program
from .preprocess import preprocess_text
from .runtime import RuntimeOptions, RuntimeOverrides, run_program
from .validate import validate_program

if TYPE_CHECKING:
    from collections.abc import Sequence


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="minigate",
        usage="minigate -f PIPELINE_FILE [options] {- | input1 [input2 ...]}",
    )
    parser.add_argument("-f", "--file", required=True, dest="pipeline_file")
    parser.add_argument("-pp", "--pipelines", type=int, dest="pipelines")
    parser.add_argument("-tp", "--tasks", type=int, dest="tasks")
    parser.add_argument(
        "-l",
        "--limit",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        dest="limits",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument("-n", "--dry-run", action="store_true", dest="dry_run")
    parser.add_argument("inputs", nargs="*")
    return parser


def _parse_limit_override(text: str) -> tuple[str, int]:
    label, sep, value_text = text.partition("=")
    if not sep or not label:
        raise CliUsageError(f"invalid limit override: {text!r}")
    try:
        value = int(value_text)
    except ValueError as exc:
        raise CliUsageError(f"invalid limit override: {text!r}") from exc
    return label, value


def _read_pipeline_file(path_text: str) -> str:
    path = Path(path_text)
    return path.read_text(encoding="utf-8-sig")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the minigate CLI."""

    parser = _build_argument_parser()
    try:
        namespace = parser.parse_args(argv)
        if not namespace.inputs:
            parser.print_usage(sys.stderr)
            raise CliUsageError("at least one input source or '-' is required")

        if namespace.pipeline_file == "-":
            raise CliUsageError("PIPELINE_FILE must be a real file path")

        if namespace.pipelines is not None and namespace.pipelines < 1:
            raise CliUsageError("--pipelines must be >= 1")
        if namespace.tasks is not None and namespace.tasks < 1:
            raise CliUsageError("--tasks must be >= 1")

        source = _read_pipeline_file(namespace.pipeline_file)
        logical_lines = preprocess_text(source, source_name=namespace.pipeline_file)
        raw_program = parse_program(logical_lines)
        validated_program = validate_program(raw_program)

        stdin_text = None
        if validated_program.config.format_name != "args" and "-" in namespace.inputs:
            stdin_text = sys.stdin.read()

        run_program(
            validated_program,
            sources=tuple(namespace.inputs),
            overrides=RuntimeOverrides(
                pipelines=namespace.pipelines,
                tasks=namespace.tasks,
                limits=tuple(_parse_limit_override(item) for item in namespace.limits),
            ),
            options=RuntimeOptions(
                verbose=namespace.verbose,
                dry_run=namespace.dry_run,
                stdin_text=stdin_text,
            ),
            stdout=sys.stdout,
            warning_sink=lambda message: print(message, file=sys.stderr),
        )
        return 0
    except (CliUsageError, MinigateError, OSError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
