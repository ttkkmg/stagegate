from __future__ import annotations

from textwrap import dedent

from stagegate.tools.minigate.ast import (
    AliasSection,
    ListSection,
    RawProgram,
    StageSection,
    UnknownSection,
)
from stagegate.tools.minigate.preprocess import preprocess_text
from stagegate.tools.minigate.parser import parse_program


def test_preprocess_strips_comments_and_preserves_string_hash() -> None:
    source = dedent(
        """
        format csv  # outer comment
        [stage.1]
        run "echo # kept"  # trimmed
        """
    )

    logical_lines = preprocess_text(source, source_name="spec.pipeline")

    assert [line.text for line in logical_lines] == [
        "format csv",
        "[stage.1]",
        'run "echo # kept"',
    ]


def test_preprocess_joins_continuation_lines_and_keeps_start_line() -> None:
    source = dedent(
        """
        [stage.1]
        run "cmd1",
            "cmd2"
        """
    )

    logical_lines = preprocess_text(source, source_name="spec.pipeline")

    assert [line.text for line in logical_lines] == [
        "[stage.1]",
        'run "cmd1",    "cmd2"',
    ]
    assert logical_lines[1].location.line_number == 3


def test_preprocess_comment_line_does_not_break_continuation_chain() -> None:
    source = dedent(
        """
        [stage.1]
        run "cmd1",
        # comment only
            "cmd2"
        """
    )

    logical_lines = preprocess_text(source, source_name="spec.pipeline")

    assert [line.text for line in logical_lines] == [
        "[stage.1]",
        'run "cmd1",    "cmd2"',
    ]


def test_parse_program_returns_raw_program_preserving_section_boundaries() -> None:
    source = dedent(
        """
        format csv
        pipelines 2

        [list]
        xs {#1}, {#2}

        [alias]
        out "{#1}.out"

        [stage.2]
        use cpu, 1
        run "echo ok"
        """
    )

    program = parse_program(preprocess_text(source, source_name="spec.pipeline"))

    assert isinstance(program, RawProgram)
    assert len(program.top_level_statements) == 2
    assert len(program.sections) == 3
    assert isinstance(program.sections[0], ListSection)
    assert isinstance(program.sections[1], AliasSection)
    assert isinstance(program.sections[2], StageSection)
    assert program.sections[2].stage_number == 2
    assert len(program.sections[2].statements) == 2


def test_parse_program_preserves_unknown_sections_for_later_validation() -> None:
    source = dedent(
        """
        format csv

        [mystery]
        raw data here
        """
    )

    program = parse_program(preprocess_text(source, source_name="spec.pipeline"))

    assert len(program.sections) == 1
    assert isinstance(program.sections[0], UnknownSection)
    assert program.sections[0].name == "mystery"
    assert [line.text for line in program.sections[0].lines] == ["raw data here"]


def test_parse_run_and_run_list_keep_template_literals_raw() -> None:
    source = dedent(
        r"""
        format csv

        [stage.1]
        run "echo \{literal\}", "echo {sample}"

        [stage.2]
        run_list "prep [xs] > [outs]"
        """
    )

    program = parse_program(preprocess_text(source, source_name="spec.pipeline"))

    stage1 = program.sections[0]
    assert isinstance(stage1, StageSection)
    run_stmt = stage1.statements[0]
    assert run_stmt.commands[0].value == r"echo \{literal\}"
    assert run_stmt.commands[1].value == "echo {sample}"

    stage2 = program.sections[1]
    assert isinstance(stage2, StageSection)
    run_list_stmt = stage2.statements[0]
    assert run_list_stmt.command.value == "prep [xs] > [outs]"
