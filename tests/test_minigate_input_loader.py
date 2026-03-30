from __future__ import annotations

from pathlib import Path

import pytest


input_loader = pytest.importorskip(
    "stagegate.tools.minigate.input_loader",
    reason="minigate input loader is not implemented yet",
)
errors = pytest.importorskip(
    "stagegate.tools.minigate.errors",
    reason="minigate errors module is not implemented yet",
)


FIXTURE_ROOT = Path(__file__).parent / "data" / "minigate" / "inputs"


def make_config(
    *,
    format_name: str,
    header: bool = False,
    codepage: str = "utf-8-sig",
    delimiter: str | None = None,
):
    # Provisional contract for the future loader API.
    return input_loader.InputLoaderConfig(
        format_name=format_name,
        header=header,
        codepage=codepage,
        delimiter=delimiter,
    )


def collect_rows(
    relpath: str,
    *,
    format_name: str,
    header: bool = False,
    codepage: str = "utf-8-sig",
    delimiter: str | None = None,
):
    warnings: list[str] = []
    config = make_config(
        format_name=format_name,
        header=header,
        codepage=codepage,
        delimiter=delimiter,
    )
    rows = list(
        input_loader.load_rows(
            config=config,
            sources=[FIXTURE_ROOT / relpath],
            warning_sink=warnings.append,
        )
    )
    return rows, warnings


def test_csv_header_loader_yields_rows_with_original_values_and_source_name() -> None:
    rows, warnings = collect_rows(
        "csv/basic_header.csv",
        format_name="csv",
        header=True,
    )

    assert warnings == []
    assert len(rows) == 3
    assert tuple(rows[0].values) == (
        "s1",
        "reads/s1_R1.fastq.gz",
        "reads/s1_R2.fastq.gz",
        "control",
    )
    assert rows[0].source_name.endswith("basic_header.csv")


def test_csv_duplicate_header_emits_warning() -> None:
    rows, warnings = collect_rows(
        "csv/duplicate_header.csv",
        format_name="csv",
        header=True,
    )

    assert len(rows) == 2
    assert len(warnings) == 1
    assert "duplicate header" in warnings[0]
    assert "rightmost column wins" in warnings[0]


def test_csv_header_only_file_yields_zero_rows() -> None:
    rows, warnings = collect_rows(
        "csv/header_only.csv",
        format_name="csv",
        header=True,
    )

    assert rows == []
    assert warnings == []


def test_csv_empty_file_yields_zero_rows() -> None:
    rows, warnings = collect_rows(
        "csv/empty.csv",
        format_name="csv",
        header=True,
    )

    assert rows == []
    assert warnings == []


def test_tsv_basic_no_header_loader_preserves_row_width() -> None:
    rows, warnings = collect_rows(
        "tsv/basic_no_header.tsv",
        format_name="tsv",
        header=False,
    )

    assert warnings == []
    assert [len(row.values) for row in rows] == [3, 3, 3]
    assert tuple(rows[1].values) == (
        "sampleB",
        "reads/b_R1.fastq.gz",
        "reads/b_R2.fastq.gz",
    )


def test_varlists_whitespace_loader_preserves_ragged_row_lengths() -> None:
    rows, warnings = collect_rows(
        "varlists/ragged_lengths.txt",
        format_name="varlists",
        delimiter="whitespace",
    )

    assert warnings == []
    assert [len(row.values) for row in rows] == [6, 3, 5]
    assert tuple(rows[0].values[:2]) == ("cat", "cat1.jpg")


def test_varlists_blank_lines_are_ignored() -> None:
    rows, warnings = collect_rows(
        "varlists/blank_lines.txt",
        format_name="varlists",
        delimiter="whitespace",
    )

    assert warnings == []
    assert len(rows) == 3
    assert tuple(row.values[0] for row in rows) == ("cat", "dog", "bird")


def test_varlists_comma_loader_trims_spaces_around_fields() -> None:
    rows, warnings = collect_rows(
        "varlists/comma_with_spaces.txt",
        format_name="varlists",
        delimiter="comma",
    )

    assert warnings == []
    assert tuple(rows[0].values) == ("cat", "cat1.jpg", "cat2.jpg", "cat3.jpg")
    assert tuple(rows[1].values) == ("dog", "dog1.jpg", "dog2.jpg", "dog3.jpg")


@pytest.mark.parametrize(
    "relpath",
    [
        "varlists/leading_comma.txt",
        "varlists/trailing_comma.txt",
        "varlists/double_comma.txt",
    ],
)
def test_varlists_comma_loader_rejects_empty_fields(relpath: str) -> None:
    config = make_config(format_name="varlists", delimiter="comma")

    with pytest.raises(errors.RuntimeRowError):
        list(
            input_loader.load_rows(
                config=config,
                sources=[FIXTURE_ROOT / relpath],
                warning_sink=lambda _message: None,
            )
        )


def test_varlists_fullwidth_space_is_not_a_whitespace_delimiter() -> None:
    rows, warnings = collect_rows(
        "varlists/fullwidth_space.txt",
        format_name="varlists",
        delimiter="whitespace",
    )

    assert warnings == []
    assert tuple(rows[0].values) == ("cat　cat1.jpg　cat2.jpg",)
    assert tuple(rows[1].values) == ("dog", "dog1.jpg", "dog2.jpg")


def test_varlists_hash_prefix_is_treated_as_plain_data() -> None:
    rows, warnings = collect_rows(
        "varlists/hash_literal.txt",
        format_name="varlists",
        delimiter="whitespace",
    )

    assert warnings == []
    assert tuple(rows[0].values) == ("#notacomment", "image1.jpg", "image2.jpg")
    assert tuple(rows[1].values) == ("cat", "cat1.jpg", "cat2.jpg")
