from __future__ import annotations

from textwrap import dedent

import pytest


preprocess = pytest.importorskip(
    "stagegate.tools.minigate.preprocess",
    reason="minigate preprocess module is not implemented yet",
)
parser = pytest.importorskip(
    "stagegate.tools.minigate.parser",
    reason="minigate parser module is not implemented yet",
)
validate = pytest.importorskip(
    "stagegate.tools.minigate.validate",
    reason="minigate validate module is not implemented yet",
)
errors = pytest.importorskip(
    "stagegate.tools.minigate.errors",
    reason="minigate errors module is not implemented yet",
)


SOURCE_NAME = "spec.pipeline"


def validate_text(source: str) -> object:
    logical_lines = preprocess.preprocess_text(source, source_name=SOURCE_NAME)
    program = parser.parse_program(logical_lines)
    return validate.validate_program(program)


def test_validate_rejects_duplicate_limit_labels() -> None:
    source = dedent(
        """
        format csv
        limit cpu, 4
        limit cpu, 8

        [stage.1]
        run "echo ok"
        """
    )

    with pytest.raises(errors.StaticValidationError, match="limit"):
        validate_text(source)


def test_validate_rejects_duplicate_use_labels_within_one_stage() -> None:
    source = dedent(
        """
        format csv

        [stage.1]
        use cpu, 1
        use cpu, 2
        run "echo ok"
        """
    )

    with pytest.raises(errors.StaticValidationError, match="use"):
        validate_text(source)


def test_validate_rejects_duplicate_top_level_singletons() -> None:
    source = dedent(
        """
        format csv
        pipelines 1
        pipelines 2

        [stage.1]
        run "echo ok"
        """
    )

    with pytest.raises(errors.StaticValidationError, match="pipelines"):
        validate_text(source)


def test_validate_rejects_delimiter_outside_varlists() -> None:
    source = dedent(
        """
        format csv
        delimiter comma

        [stage.1]
        run "echo ok"
        """
    )

    with pytest.raises(errors.StaticValidationError, match="delimiter"):
        validate_text(source)


def test_validate_rejects_header_under_format_args() -> None:
    source = dedent(
        """
        format args
        header false

        [stage.1]
        run "echo {arg}"
        """
    )

    with pytest.raises(errors.StaticValidationError, match="header"):
        validate_text(source)


def test_validate_requires_delimiter_for_varlists() -> None:
    source = dedent(
        """
        format varlists

        [stage.1]
        run "echo {#1}"
        """
    )

    with pytest.raises(errors.StaticValidationError, match="delimiter"):
        validate_text(source)


def test_validate_rejects_header_true_for_varlists() -> None:
    source = dedent(
        """
        format varlists
        header true
        delimiter whitespace

        [stage.1]
        run "echo {#1}"
        """
    )

    with pytest.raises(errors.StaticValidationError, match="header"):
        validate_text(source)


def test_validate_rejects_stage_without_run_or_run_list() -> None:
    source = dedent(
        """
        format csv

        [stage.1]
        use cpu, 1
        """
    )

    with pytest.raises(errors.StaticValidationError, match="stage"):
        validate_text(source)


def test_validate_rejects_forward_alias_reference() -> None:
    source = dedent(
        """
        format csv

        [alias]
        first "{second}"
        second "ok"

        [stage.1]
        run "echo {first}"
        """
    )

    with pytest.raises(errors.StaticValidationError, match="alias"):
        validate_text(source)


def test_validate_rejects_list_parallel_expansion_inside_run() -> None:
    source = dedent(
        """
        format csv

        [list]
        xs {#1}, {#2}

        [stage.1]
        run "echo [xs]"
        """
    )

    with pytest.raises(errors.StaticValidationError, match="run"):
        validate_text(source)


def test_validate_rejects_named_column_reference_when_header_is_false() -> None:
    source = dedent(
        """
        format csv
        header false

        [stage.1]
        run "echo {sample}"
        """
    )

    with pytest.raises(errors.StaticValidationError, match="named scalar reference"):
        validate_text(source)


def test_validate_rejects_undefined_list_reference() -> None:
    source = dedent(
        """
        format csv

        [stage.1]
        run_list "echo [missing]"
        """
    )

    with pytest.raises(errors.StaticValidationError, match="undefined list"):
        validate_text(source)


def test_validate_rejects_scalar_reference_to_list_alias() -> None:
    source = dedent(
        """
        format csv
        header true

        [list]
        xs {a}, {b}

        [alias]
        outs "[xs].out"

        [stage.1]
        run "echo {outs}"
        """
    )

    with pytest.raises(errors.StaticValidationError, match="type mismatch"):
        validate_text(source)


@pytest.mark.parametrize(
    "item",
    [
        "0-3",
        "3-3",
        "5-2",
    ],
)
def test_validate_rejects_invalid_accept_retvals_ranges(item: str) -> None:
    source = dedent(
        f"""
        format csv

        [stage.1]
        accept_retvals {item}
        run "echo ok"
        """
    )

    with pytest.raises(errors.StaticValidationError, match="accept_retvals"):
        validate_text(source)


def test_validate_accepts_minimal_varlists_program_with_explicit_header_false() -> None:
    source = dedent(
        """
        format varlists
        header false
        delimiter whitespace

        [list]
        xs {#2}, ...

        [alias]
        outs "[xs:stem].out"

        [stage.1]
        run_list "prep [xs] > [outs]"

        [stage.2]
        run "join {[outs]} > {#1}.txt"
        """
    )

    validated = validate_text(source)

    assert validated is not None
