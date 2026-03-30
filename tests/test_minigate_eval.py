from __future__ import annotations

from textwrap import dedent

import pytest


ast = pytest.importorskip(
    "stagegate.tools.minigate.ast",
    reason="minigate ast module is not implemented yet",
)
eval_mod = pytest.importorskip(
    "stagegate.tools.minigate.eval",
    reason="minigate eval module is not implemented yet",
)
input_loader = pytest.importorskip(
    "stagegate.tools.minigate.input_loader",
    reason="minigate input loader module is not implemented yet",
)
parser = pytest.importorskip(
    "stagegate.tools.minigate.parser",
    reason="minigate parser module is not implemented yet",
)
preprocess = pytest.importorskip(
    "stagegate.tools.minigate.preprocess",
    reason="minigate preprocess module is not implemented yet",
)
validate = pytest.importorskip(
    "stagegate.tools.minigate.validate",
    reason="minigate validate module is not implemented yet",
)
errors = pytest.importorskip(
    "stagegate.tools.minigate.errors",
    reason="minigate errors module is not implemented yet",
)


def build_program(source: str):
    logical_lines = preprocess.preprocess_text(source, source_name="spec.pipeline")
    raw_program = parser.parse_program(logical_lines)
    return validate.validate_program(raw_program)


def test_eval_resolves_list_aliases_and_joined_scalar_aliases() -> None:
    program = build_program(
        dedent(
            """
            format csv
            header true

            [list]
            imgs {img1}, ..., {img3}

            [alias]
            dat_files "[imgs:stem].dat"
            summary "makestat {sample} {[dat_files]}"
            special COLUMN("sample id")

            [stage.1]
            run_list "prep [imgs] --out [dat_files]"

            [stage.2]
            run "{summary} {special}"
            """
        )
    )
    row = input_loader.InputRow(
        values=("s1", "S 1", "cat1.jpg", "cat2.jpg", "cat3.jpg"),
        source_name="input.csv",
        row_number=2,
        headers=("sample", "sample id", "img1", "img2", "img3"),
    )
    context = eval_mod.make_evaluation_context(program, row)

    dat_alias = next(alias for alias in program.aliases if alias.name == "dat_files")
    summary_alias = next(alias for alias in program.aliases if alias.name == "summary")

    assert eval_mod.evaluate_list_template(dat_alias.value, context=context) == (
        "cat1.dat",
        "cat2.dat",
        "cat3.dat",
    )
    assert eval_mod.evaluate_scalar_template(summary_alias.value, context=context) == (
        "makestat s1 cat1.dat cat2.dat cat3.dat"
    )

    stage2 = program.stages[1]
    assert isinstance(stage2.execution, ast.ValidatedRunStatement)
    assert (
        eval_mod.evaluate_scalar_template(
            stage2.execution.commands[0],
            context=context,
        )
        == "makestat s1 cat1.dat cat2.dat cat3.dat S 1"
    )


def test_eval_rejects_list_length_mismatch_in_run_list_template() -> None:
    program = build_program(
        dedent(
            """
            format csv
            header true

            [list]
            xs {x1}, {x2}
            ys {y1}

            [stage.1]
            run_list "cmd [xs] [ys]"
            """
        )
    )
    row = input_loader.InputRow(
        values=("a.txt", "b.txt", "out.txt"),
        source_name="input.csv",
        row_number=2,
        headers=("x1", "x2", "y1"),
    )
    context = eval_mod.make_evaluation_context(program, row)

    stage1 = program.stages[0]
    assert isinstance(stage1.execution, ast.ValidatedRunListStatement)
    with pytest.raises(errors.RuntimeRowError, match="length mismatch"):
        eval_mod.evaluate_list_template(stage1.execution.command, context=context)


def test_eval_supports_varlists_tail_range_definition() -> None:
    program = build_program(
        dedent(
            """
            format varlists
            delimiter whitespace

            [list]
            target_images {#2}, ...

            [alias]
            dat_files "[target_images:stem].dat"

            [stage.1]
            run "join {[dat_files]}"
            """
        )
    )
    row = input_loader.InputRow(
        values=("cat", "cat1.jpg", "cat2.jpg", "cat3.jpg"),
        source_name="input.txt",
        row_number=1,
        headers=None,
    )
    context = eval_mod.make_evaluation_context(program, row)

    stage1 = program.stages[0]
    assert isinstance(stage1.execution, ast.ValidatedRunStatement)
    assert (
        eval_mod.evaluate_scalar_template(
            stage1.execution.commands[0],
            context=context,
        )
        == "join cat1.dat cat2.dat cat3.dat"
    )
