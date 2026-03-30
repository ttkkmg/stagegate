from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest


cli = pytest.importorskip(
    "stagegate.tools.minigate.cli",
    reason="minigate cli module is not implemented yet",
)


def write_pipeline(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "test.pipeline"
    path.write_text(dedent(text), encoding="utf-8")
    return path


def test_cli_dry_run_for_args_input(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = write_pipeline(
        tmp_path,
        """
        format args

        [stage.1]
        run "echo {arg}"
        """,
    )

    exit_code = cli.main(["-f", str(path), "-n", "foo"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "[1]run(1) : echo foo" in captured.out
    assert captured.err == ""


def test_cli_reports_missing_inputs_as_usage_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = write_pipeline(
        tmp_path,
        """
        format args

        [stage.1]
        run "echo {arg}"
        """,
    )

    exit_code = cli.main(["-f", str(path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "usage:" in captured.err
    assert "input source" in captured.err
