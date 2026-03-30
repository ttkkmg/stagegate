"""File-level parser for minigate DSL logical lines."""

from __future__ import annotations

import re
from typing import Literal

from .ast import (
    AcceptRetvalsRange,
    AcceptRetvalsStatement,
    AcceptRetvalsValue,
    AliasDefinition,
    AliasSection,
    CodepageStatement,
    ColumnCall,
    ColumnReference,
    DelimiterStatement,
    FormatStatement,
    HeaderStatement,
    LimitStatement,
    ListDefinition,
    ListSection,
    LogicalLine,
    PipelinesStatement,
    RawProgram,
    RawSection,
    RunListStatement,
    RunStatement,
    StageSection,
    TasksStatement,
    TemplateLiteral,
    TopLevelStatement,
    UnknownSection,
    UseStatement,
)
from .errors import ParseError


_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_STAGE_HEADER_RE = re.compile(r"^\[stage\.([1-9][0-9]*)\]$")
_ANY_SECTION_RE = re.compile(r"^\[(.+)\]$")
_INT_RE = re.compile(r"^[+-]?[0-9]+$")
_NONNEG_INT_RE = re.compile(r"^[0-9]+$")
_POS_INT_RE = re.compile(r"^[1-9][0-9]*$")
_COLUMN_REF_RE = re.compile(r"^\{([A-Za-z][A-Za-z0-9_]*|#[1-9][0-9]*)\}$")


def _parse_int(text: str, *, location) -> int:
    if not _INT_RE.match(text):
        raise ParseError(f"invalid integer: {text!r}", location=location)
    return int(text)


def _parse_nonneg_int(text: str, *, location) -> int:
    if not _NONNEG_INT_RE.match(text):
        raise ParseError(f"invalid non-negative integer: {text!r}", location=location)
    return int(text)


def _expect_identifier(text: str, *, location) -> str:
    if not _IDENTIFIER_RE.match(text):
        raise ParseError(f"invalid identifier: {text!r}", location=location)
    return text


def _split_top_level_commas(text: str, *, location) -> tuple[str, ...]:
    parts: list[str] = []
    current: list[str] = []
    in_string = False
    escape = False
    depth_paren = 0
    depth_brace = 0
    depth_bracket = 0

    for char in text:
        if in_string:
            current.append(char)
            if escape:
                escape = False
                continue
            if char == "\\":
                escape = True
                continue
            if char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            current.append(char)
            continue
        if char == "(":
            depth_paren += 1
            current.append(char)
            continue
        if char == ")":
            depth_paren -= 1
            current.append(char)
            continue
        if char == "{":
            depth_brace += 1
            current.append(char)
            continue
        if char == "}":
            depth_brace -= 1
            current.append(char)
            continue
        if char == "[":
            depth_bracket += 1
            current.append(char)
            continue
        if char == "]":
            depth_bracket -= 1
            current.append(char)
            continue
        if (
            char == ","
            and not in_string
            and depth_paren == 0
            and depth_brace == 0
            and depth_bracket == 0
        ):
            part = "".join(current).strip()
            if not part:
                raise ParseError("empty comma-separated item", location=location)
            parts.append(part)
            current = []
            continue
        current.append(char)

    part = "".join(current).strip()
    if not part:
        raise ParseError("trailing comma is not allowed", location=location)
    parts.append(part)
    return tuple(parts)


def _parse_string_literal(
    text: str,
    *,
    location,
    decode: bool,
) -> str:
    stripped = text.strip()
    if len(stripped) < 2 or stripped[0] != '"' or stripped[-1] != '"':
        raise ParseError("expected double-quoted string literal", location=location)

    body = stripped[1:-1]
    chars: list[str] = []
    index = 0
    while index < len(body):
        char = body[index]
        if char != "\\":
            chars.append(char)
            index += 1
            continue
        if index + 1 >= len(body):
            raise ParseError("unterminated string escape", location=location)
        escaped = body[index + 1]
        if escaped not in {"\\", '"', "{", "}"}:
            raise ParseError(f"invalid string escape \\{escaped}", location=location)
        if decode:
            chars.append(escaped)
        else:
            chars.append("\\")
            chars.append(escaped)
        index += 2
    return "".join(chars)


def _consume_keyword(line: LogicalLine, keyword: str) -> str:
    text = line.text
    if text == keyword:
        return ""
    prefix = f"{keyword} "
    if text.startswith(prefix):
        return text[len(prefix) :]
    raise ParseError(f"expected {keyword} statement", location=line.location)


def _parse_column_reference(text: str, *, location) -> ColumnReference:
    match = _COLUMN_REF_RE.match(text.strip())
    if match is None:
        raise ParseError(f"invalid column reference: {text!r}", location=location)
    token = match.group(1)
    if token.startswith("#"):
        return ColumnReference(column_index=int(token[1:]))
    return ColumnReference(identifier=token)


def _parse_top_level_statement(line: LogicalLine) -> TopLevelStatement:
    keyword = line.text.split(None, 1)[0]
    remainder = _consume_keyword(line, keyword)

    if keyword == "pipelines":
        return PipelinesStatement(
            _parse_int(remainder.strip(), location=line.location), line.location
        )
    if keyword == "tasks":
        return TasksStatement(
            _parse_int(remainder.strip(), location=line.location), line.location
        )
    if keyword == "format":
        value = remainder.strip()
        if value not in {"csv", "tsv", "varlists", "args"}:
            raise ParseError(f"invalid format value: {value!r}", location=line.location)
        return FormatStatement(value, line.location)
    if keyword == "header":
        value = remainder.strip()
        if value == "true":
            return HeaderStatement(True, line.location)
        if value == "false":
            return HeaderStatement(False, line.location)
        raise ParseError(f"invalid header value: {value!r}", location=line.location)
    if keyword == "codepage":
        return CodepageStatement(
            _parse_string_literal(remainder, location=line.location, decode=True),
            line.location,
        )
    if keyword == "delimiter":
        value = remainder.strip()
        if value not in {"comma", "whitespace"}:
            raise ParseError(
                f"invalid delimiter value: {value!r}", location=line.location
            )
        return DelimiterStatement(value, line.location)
    if keyword == "limit":
        parts = _split_top_level_commas(remainder, location=line.location)
        if len(parts) != 2:
            raise ParseError("limit requires LABEL, INT", location=line.location)
        return LimitStatement(
            label=_expect_identifier(parts[0].strip(), location=line.location),
            value=_parse_int(parts[1].strip(), location=line.location),
            location=line.location,
        )

    raise ParseError(
        f"unknown top-level statement: {keyword!r}", location=line.location
    )


def _parse_list_definition(line: LogicalLine) -> ListDefinition:
    parts = line.text.split(None, 1)
    if len(parts) != 2:
        raise ParseError(
            "list definition requires name and elements", location=line.location
        )
    name = _expect_identifier(parts[0], location=line.location)
    element_texts = _split_top_level_commas(parts[1], location=line.location)
    elements: list[ColumnReference | Literal["..."]] = []
    for item in element_texts:
        if item == "...":
            elements.append("...")
        else:
            elements.append(_parse_column_reference(item, location=line.location))
    return ListDefinition(name=name, elements=tuple(elements), location=line.location)


def _parse_column_call(text: str, *, location) -> ColumnCall:
    stripped = text.strip()
    prefix = "COLUMN("
    if not stripped.startswith(prefix) or not stripped.endswith(")"):
        raise ParseError("invalid COLUMN(...) expression", location=location)
    inner = stripped[len(prefix) : -1].strip()
    return ColumnCall(
        column_name=_parse_string_literal(inner, location=location, decode=True),
        location=location,
    )


def _parse_alias_definition(line: LogicalLine) -> AliasDefinition:
    parts = line.text.split(None, 1)
    if len(parts) != 2:
        raise ParseError(
            "alias definition requires name and value", location=line.location
        )
    name = _expect_identifier(parts[0], location=line.location)
    rhs = parts[1].strip()
    if rhs.startswith('"'):
        value = TemplateLiteral(
            value=_parse_string_literal(rhs, location=line.location, decode=False),
            location=line.location,
        )
    else:
        value = _parse_column_call(rhs, location=line.location)
    return AliasDefinition(name=name, value=value, location=line.location)


def _parse_run_statement(line: LogicalLine) -> RunStatement:
    parts = _split_top_level_commas(
        _consume_keyword(line, "run"), location=line.location
    )
    commands = tuple(
        TemplateLiteral(
            value=_parse_string_literal(part, location=line.location, decode=False),
            location=line.location,
        )
        for part in parts
    )
    return RunStatement(commands=commands, location=line.location)


def _parse_run_list_statement(line: LogicalLine) -> RunListStatement:
    command = TemplateLiteral(
        value=_parse_string_literal(
            _consume_keyword(line, "run_list"),
            location=line.location,
            decode=False,
        ),
        location=line.location,
    )
    return RunListStatement(command=command, location=line.location)


def _parse_use_statement(line: LogicalLine) -> UseStatement:
    parts = _split_top_level_commas(
        _consume_keyword(line, "use"), location=line.location
    )
    if len(parts) != 2:
        raise ParseError("use requires LABEL, INT", location=line.location)
    return UseStatement(
        label=_expect_identifier(parts[0].strip(), location=line.location),
        value=_parse_int(parts[1].strip(), location=line.location),
        location=line.location,
    )


def _parse_accept_retvals_statement(line: LogicalLine) -> AcceptRetvalsStatement:
    parts = _split_top_level_commas(
        _consume_keyword(line, "accept_retvals"),
        location=line.location,
    )
    items = []
    for item in parts:
        if "-" in item:
            left_text, sep, right_text = item.partition("-")
            if not sep:
                raise ParseError("invalid accept_retvals item", location=line.location)
            items.append(
                AcceptRetvalsRange(
                    left=_parse_nonneg_int(left_text.strip(), location=line.location),
                    right=_parse_nonneg_int(right_text.strip(), location=line.location),
                    location=line.location,
                )
            )
        else:
            items.append(
                AcceptRetvalsValue(
                    value=_parse_nonneg_int(item.strip(), location=line.location),
                    location=line.location,
                )
            )
    return AcceptRetvalsStatement(items=tuple(items), location=line.location)


def _parse_stage_statement(line: LogicalLine):
    keyword = line.text.split(None, 1)[0]
    if keyword == "run":
        return _parse_run_statement(line)
    if keyword == "run_list":
        return _parse_run_list_statement(line)
    if keyword == "use":
        return _parse_use_statement(line)
    if keyword == "accept_retvals":
        return _parse_accept_retvals_statement(line)
    raise ParseError(f"invalid stage statement: {keyword!r}", location=line.location)


def _build_section(
    *,
    section_name: str,
    stage_number: int | None,
    location,
    lines: list[LogicalLine],
) -> RawSection:
    if section_name == "list":
        definitions = tuple(_parse_list_definition(line) for line in lines)
        return ListSection(definitions=definitions, location=location)
    if section_name == "alias":
        definitions = tuple(_parse_alias_definition(line) for line in lines)
        return AliasSection(definitions=definitions, location=location)
    if section_name == "stage":
        assert stage_number is not None
        statements = tuple(_parse_stage_statement(line) for line in lines)
        return StageSection(
            stage_number=stage_number,
            statements=statements,
            location=location,
        )
    return UnknownSection(name=section_name, lines=tuple(lines), location=location)


def parse_program(
    logical_lines: tuple[LogicalLine, ...] | list[LogicalLine],
) -> RawProgram:
    """Parse logical DSL lines into a raw AST preserving section boundaries."""

    top_level_statements: list[TopLevelStatement] = []
    sections: list[RawSection] = []

    current_section_name: str | None = None
    current_stage_number: int | None = None
    current_section_location = None
    current_section_lines: list[LogicalLine] = []

    def flush_section() -> None:
        nonlocal \
            current_section_name, \
            current_stage_number, \
            current_section_location, \
            current_section_lines
        if current_section_name is None or current_section_location is None:
            return
        sections.append(
            _build_section(
                section_name=current_section_name,
                stage_number=current_stage_number,
                location=current_section_location,
                lines=current_section_lines,
            )
        )
        current_section_name = None
        current_stage_number = None
        current_section_location = None
        current_section_lines = []

    seen_first_section = False

    for line in logical_lines:
        text = line.text
        if text == "[list]":
            flush_section()
            current_section_name = "list"
            current_stage_number = None
            current_section_location = line.location
            seen_first_section = True
            continue
        if text == "[alias]":
            flush_section()
            current_section_name = "alias"
            current_stage_number = None
            current_section_location = line.location
            seen_first_section = True
            continue
        stage_match = _STAGE_HEADER_RE.match(text)
        if stage_match is not None:
            flush_section()
            current_section_name = "stage"
            current_stage_number = int(stage_match.group(1))
            current_section_location = line.location
            seen_first_section = True
            continue
        section_match = _ANY_SECTION_RE.match(text)
        if section_match is not None:
            flush_section()
            current_section_name = section_match.group(1)
            current_stage_number = None
            current_section_location = line.location
            seen_first_section = True
            continue

        if not seen_first_section:
            top_level_statements.append(_parse_top_level_statement(line))
            continue

        if current_section_name is None:
            raise ParseError("statement outside section", location=line.location)
        current_section_lines.append(line)

    flush_section()
    return RawProgram(
        top_level_statements=tuple(top_level_statements),
        sections=tuple(sections),
    )
