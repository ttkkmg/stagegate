"""Logical-line preprocessing for minigate DSL files."""

from __future__ import annotations

from .ast import LogicalLine, SourceLocation
from .errors import ParseError


_VALID_STRING_ESCAPES = frozenset({"\\", '"', "{", "}"})


def _strip_comment_preserving_strings(line: str, *, location: SourceLocation) -> str:
    in_string = False
    escape = False
    brace_depth = 0

    for index, char in enumerate(line):
        if in_string:
            if escape:
                if char not in _VALID_STRING_ESCAPES:
                    raise ParseError(
                        f"invalid string escape \\{char}",
                        location=location,
                    )
                escape = False
                continue
            if char == "\\":
                escape = True
                continue
            if char == '"':
                in_string = False
                continue
            continue

        if char == '"':
            in_string = True
            continue
        if char == "{":
            brace_depth += 1
            continue
        if char == "}":
            if brace_depth > 0:
                brace_depth -= 1
            continue
        if char == "#":
            if brace_depth > 0:
                continue
            return line[:index]

    if in_string or escape:
        raise ParseError("unterminated string literal", location=location)
    return line


def preprocess_text(
    source: str,
    *,
    source_name: str,
) -> tuple[LogicalLine, ...]:
    """Convert physical lines into logical lines with source mapping."""

    logical_lines: list[LogicalLine] = []
    current_parts: list[str] = []
    current_start_line: int | None = None

    for line_number, physical_line in enumerate(source.splitlines(), start=1):
        location = SourceLocation(source_name=source_name, line_number=line_number)
        stripped = _strip_comment_preserving_strings(physical_line, location=location)
        comment_only = physical_line.lstrip().startswith("#")

        if stripped[:1] in {" ", "\t"}:
            if current_start_line is None:
                raise ParseError("unexpected continuation line", location=location)
            current_parts.append(stripped)
            continue

        if comment_only and not stripped.strip():
            continue

        if current_start_line is not None:
            joined = "".join(current_parts).strip()
            if joined:
                logical_lines.append(
                    LogicalLine(
                        text=joined,
                        location=SourceLocation(
                            source_name=source_name,
                            line_number=current_start_line,
                        ),
                    )
                )
            current_parts = []
            current_start_line = None

        if not stripped.strip():
            continue

        current_parts = [stripped]
        current_start_line = line_number

    if current_start_line is not None:
        joined = "".join(current_parts).strip()
        if joined:
            logical_lines.append(
                LogicalLine(
                    text=joined,
                    location=SourceLocation(
                        source_name=source_name,
                        line_number=current_start_line,
                    ),
                )
            )

    return tuple(logical_lines)
