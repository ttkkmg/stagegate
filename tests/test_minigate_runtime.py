from __future__ import annotations

from io import StringIO
from textwrap import dedent

import pytest


parser = pytest.importorskip(
    "stagegate.tools.minigate.parser",
    reason="minigate parser module is not implemented yet",
)
preprocess = pytest.importorskip(
    "stagegate.tools.minigate.preprocess",
    reason="minigate preprocess module is not implemented yet",
)
runtime = pytest.importorskip(
    "stagegate.tools.minigate.runtime",
    reason="minigate runtime module is not implemented yet",
)
validate = pytest.importorskip(
    "stagegate.tools.minigate.validate",
    reason="minigate validate module is not implemented yet",
)


def build_program(source: str):
    logical_lines = preprocess.preprocess_text(source, source_name="spec.pipeline")
    raw_program = parser.parse_program(logical_lines)
    return validate.validate_program(raw_program)


def test_run_program_dry_run_renders_commands_for_args_input() -> None:
    program = build_program(
        dedent(
            """
            format args

            [stage.1]
            run "echo {arg}"
            """
        )
    )
    stdout = StringIO()

    runtime.run_program(
        program,
        sources=("alpha", "beta"),
        options=runtime.RuntimeOptions(dry_run=True),
        stdout=stdout,
    )

    rendered = stdout.getvalue()
    assert "[1]run(1) : echo alpha" in rendered
    assert "[1]run(1) : echo beta" in rendered


def test_run_program_raises_on_accept_retvals_violation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    program = build_program(
        dedent(
            """
            format args

            [stage.1]
            accept_retvals 2
            run "echo {arg}"
            """
        )
    )

    monkeypatch.setattr(runtime.stagegate, "run_shell", lambda _command: 1)

    with pytest.raises(runtime.AcceptRetvalsError, match="disallowed exit code"):
        runtime.run_program(
            program,
            sources=("alpha",),
            options=runtime.RuntimeOptions(dry_run=False),
            stdout=StringIO(),
        )
