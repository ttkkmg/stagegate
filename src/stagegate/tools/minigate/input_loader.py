"""Input-row loaders for minigate formats."""

from __future__ import annotations

import csv
from dataclasses import dataclass
import io
from pathlib import Path
import re
from typing import TYPE_CHECKING

from .ast import SourceLocation
from .errors import RuntimeRowError, StaticValidationError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from typing import TextIO


_WHITESPACE_SPLIT_RE = re.compile(r"[ \t]+")


@dataclass(frozen=True, slots=True)
class InputLoaderConfig:
    """Configuration required to load parameter-matrix rows."""

    format_name: str
    header: bool = False
    codepage: str = "utf-8-sig"
    delimiter: str | None = None


@dataclass(frozen=True, slots=True)
class InputRow:
    """Detached input row plus source metadata."""

    values: tuple[str, ...]
    source_name: str
    row_number: int
    headers: tuple[str, ...] | None = None

    @property
    def location(self) -> SourceLocation:
        """Return row-level source location."""
        return SourceLocation(source_name=self.source_name, row_number=self.row_number)


def _warn_duplicate_headers(
    *,
    header_row: tuple[str, ...],
    source_name: str,
    warning_sink: Callable[[str], None],
) -> None:
    seen: set[str] = set()
    warned: set[str] = set()
    for name in header_row:
        if name in seen and name not in warned:
            warning_sink(
                f'warning: {source_name}:1: duplicate header "{name}"; '
                "rightmost column wins"
            )
            warned.add(name)
        seen.add(name)


def _load_delimited_rows(
    *,
    source: str | Path,
    dialect: str,
    config: InputLoaderConfig,
    warning_sink: Callable[[str], None],
    stdin_text: str | None,
) -> Iterator[InputRow]:
    source_name, handle = _open_text_source(
        source=source,
        encoding=config.codepage,
        stdin_text=stdin_text,
    )
    with handle:
        reader = csv.reader(handle, dialect=dialect)
        try:
            first_row = next(reader)
        except StopIteration:
            return

        if config.header:
            header_row = tuple(first_row)
            _warn_duplicate_headers(
                header_row=header_row,
                source_name=source_name,
                warning_sink=warning_sink,
            )
            for row_number, row in enumerate(reader, start=2):
                if row == []:
                    continue
                yield InputRow(
                    values=tuple(row),
                    source_name=source_name,
                    row_number=row_number,
                    headers=header_row,
                )
            return

        if first_row != []:
            yield InputRow(
                values=tuple(first_row),
                source_name=source_name,
                row_number=1,
                headers=None,
            )
        for row_number, row in enumerate(reader, start=2):
            if row == []:
                continue
            yield InputRow(
                values=tuple(row),
                source_name=source_name,
                row_number=row_number,
                headers=None,
            )


def _split_varlists_whitespace(line: str) -> tuple[str, ...]:
    stripped = line.strip(" \t")
    if not stripped:
        return ()
    return tuple(part for part in _WHITESPACE_SPLIT_RE.split(stripped) if part)


def _split_varlists_comma(line: str, *, location: SourceLocation) -> tuple[str, ...]:
    stripped = line.rstrip("\n\r")
    if not stripped.strip(" \t"):
        return ()

    raw_parts = stripped.split(",")
    parts = tuple(part.strip(" \t") for part in raw_parts)
    if any(part == "" for part in parts):
        raise RuntimeRowError(
            "empty field is not allowed for comma varlists", location=location
        )
    return parts


def _load_varlists_rows(
    *,
    source: str | Path,
    config: InputLoaderConfig,
    stdin_text: str | None,
) -> Iterator[InputRow]:
    source_name, handle = _open_text_source(
        source=source,
        encoding=config.codepage,
        stdin_text=stdin_text,
    )
    with handle:
        for row_number, raw_line in enumerate(handle, start=1):
            location = SourceLocation(source_name=source_name, row_number=row_number)
            line = raw_line.rstrip("\n\r")
            if config.delimiter == "whitespace":
                parts = _split_varlists_whitespace(line)
            elif config.delimiter == "comma":
                parts = _split_varlists_comma(line, location=location)
            else:
                raise StaticValidationError(
                    "invalid delimiter for varlists loader",
                    location=SourceLocation(source_name=source_name),
                )
            if not parts:
                continue
            yield InputRow(
                values=parts,
                source_name=source_name,
                row_number=row_number,
                headers=None,
            )


def _load_args_rows(
    *,
    sources: Sequence[str | Path],
) -> Iterator[InputRow]:
    for row_number, item in enumerate(sources, start=1):
        yield InputRow(
            values=(str(item),),
            source_name="<args>",
            row_number=row_number,
            headers=("arg",),
        )


def _open_text_source(
    *,
    source: str | Path,
    encoding: str,
    stdin_text: str | None,
) -> tuple[str, TextIO]:
    source_name = str(source)
    if source_name == "-":
        if stdin_text is None:
            raise StaticValidationError(
                "stdin source '-' requires explicit stdin_text",
                location=SourceLocation(source_name="<input-loader>"),
            )
        return "<stdin>", io.StringIO(stdin_text)

    path = Path(source)
    return source_name, path.open("r", encoding=encoding, newline="")


def load_rows(
    *,
    config: InputLoaderConfig,
    sources: Sequence[str | Path],
    warning_sink: Callable[[str], None],
    stdin_text: str | None = None,
) -> Iterator[InputRow]:
    """Yield detached input rows for the configured format."""

    if config.format_name == "args":
        yield from _load_args_rows(sources=sources)
        return

    path_sources = list(sources)

    if config.format_name == "csv":
        for source in path_sources:
            yield from _load_delimited_rows(
                source=source,
                dialect="excel",
                config=config,
                warning_sink=warning_sink,
                stdin_text=stdin_text,
            )
        return

    if config.format_name == "tsv":
        for source in path_sources:
            yield from _load_delimited_rows(
                source=source,
                dialect="excel-tab",
                config=config,
                warning_sink=warning_sink,
                stdin_text=stdin_text,
            )
        return

    if config.format_name == "varlists":
        for source in path_sources:
            yield from _load_varlists_rows(
                source=source,
                config=config,
                stdin_text=stdin_text,
            )
        return

    raise StaticValidationError(
        f"unsupported input format: {config.format_name}",
        location=SourceLocation(source_name="<input-loader>"),
    )
