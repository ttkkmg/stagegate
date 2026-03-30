"""Compiled-template parser for minigate string literals."""

from __future__ import annotations

import re

from .ast import (
    ListConcatPart,
    ListExpandPart,
    LiteralPart,
    PathAttributeModifier,
    RemoveSuffixModifier,
    ScalarLookup,
    ScalarReferencePart,
    SliceModifier,
    Template,
    TemplateLiteral,
)
from .errors import ParseError


_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_POS_INT_RE = re.compile(r"^[1-9][0-9]*$")
_SIGNED_INT_RE = re.compile(r"^[+-]?[0-9]+$")
_VALID_LITERAL_ESCAPES = frozenset({"\\", '"', "{", "}"})


def _raise(message: str, *, location) -> None:
    raise ParseError(message, location=location)


def _flush_literal(parts: list[object], literal_chars: list[str]) -> None:
    if not literal_chars:
        return
    parts.append(LiteralPart("".join(literal_chars)))
    literal_chars.clear()


def _expect_identifier(text: str, *, location) -> str:
    candidate = text.strip()
    if not _IDENTIFIER_RE.match(candidate):
        _raise(f"invalid identifier: {candidate!r}", location=location)
    return candidate


def _parse_scalar_lookup(text: str, *, location) -> ScalarLookup:
    candidate = text.strip()
    if candidate.startswith("#"):
        index_text = candidate[1:]
        if not _POS_INT_RE.match(index_text):
            _raise(f"invalid column index reference: {candidate!r}", location=location)
        return ScalarLookup(column_index=int(index_text))
    return ScalarLookup(identifier=_expect_identifier(candidate, location=location))


def _find_matching_closer(text: str, start: int, *, opener: str, closer: str) -> int:
    escape = False
    for index in range(start, len(text)):
        char = text[index]
        if escape:
            escape = False
            continue
        if char == "\\":
            escape = True
            continue
        if char == closer:
            return index
    return -1


def _split_modifier_segments(text: str, *, location) -> tuple[str, str]:
    stripped = text.strip()
    if ":" not in stripped:
        return stripped, ""

    depth = 0
    escape = False
    for index, char in enumerate(stripped):
        if escape:
            escape = False
            continue
        if char == "\\":
            escape = True
            continue
        if char == "(":
            depth += 1
            continue
        if char == ")":
            if depth == 0:
                _raise(
                    "unexpected closing parenthesis in modifier chain",
                    location=location,
                )
            depth -= 1
            continue
        if char == ":" and depth == 0:
            return stripped[:index].strip(), stripped[index + 1 :].strip()

    if depth != 0:
        _raise("unterminated modifier argument list", location=location)
    return stripped, ""


def _split_modifier_list(text: str, *, location) -> tuple[str, ...]:
    if not text:
        return ()

    parts: list[str] = []
    current: list[str] = []
    depth = 0
    escape = False
    for char in text:
        if escape:
            current.append(char)
            escape = False
            continue
        if char == "\\":
            current.append(char)
            escape = True
            continue
        if char == "(":
            depth += 1
            current.append(char)
            continue
        if char == ")":
            if depth == 0:
                _raise(
                    "unexpected closing parenthesis in modifier chain",
                    location=location,
                )
            depth -= 1
            current.append(char)
            continue
        if char == ":" and depth == 0:
            part = "".join(current).strip()
            if not part:
                _raise("empty modifier is not allowed", location=location)
            parts.append(part)
            current = []
            continue
        current.append(char)

    if depth != 0:
        _raise("unterminated modifier argument list", location=location)

    tail = "".join(current).strip()
    if not tail:
        _raise("empty modifier is not allowed", location=location)
    parts.append(tail)
    return tuple(parts)


def _parse_signed_int(text: str, *, location) -> int:
    stripped = text.strip()
    if not _SIGNED_INT_RE.match(stripped):
        _raise(f"invalid signed integer: {stripped!r}", location=location)
    return int(stripped)


def _decode_removesuffix_argument(text: str, *, location) -> str:
    chars: list[str] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char != "\\":
            chars.append(char)
            index += 1
            continue
        if index + 1 >= len(text):
            _raise("unterminated removesuffix escape", location=location)
        escaped = text[index + 1]
        if escaped not in {")", "\\"}:
            _raise(
                f"invalid removesuffix escape \\{escaped}",
                location=location,
            )
        chars.append(escaped)
        index += 2
    return "".join(chars)


def _parse_modifier(segment: str, *, location):
    if segment in {"name", "stem", "parent"}:
        return PathAttributeModifier(attribute=segment)

    if segment.startswith("slice(") and segment.endswith(")"):
        inner = segment[len("slice(") : -1]
        left, sep, right = inner.partition(",")
        if not sep:
            _raise("slice requires two arguments", location=location)
        return SliceModifier(
            start=_parse_signed_int(left, location=location),
            stop=_parse_signed_int(right, location=location),
        )

    if segment.startswith("removesuffix(") and segment.endswith(")"):
        inner = segment[len("removesuffix(") : -1]
        return RemoveSuffixModifier(
            suffix=_decode_removesuffix_argument(inner, location=location)
        )

    _raise(f"invalid modifier: {segment!r}", location=location)


def _parse_scalar_part(inner: str, *, location) -> ScalarReferencePart:
    target_text, modifiers_text = _split_modifier_segments(inner, location=location)
    if not target_text:
        _raise("empty scalar interpolation", location=location)
    modifiers = tuple(
        _parse_modifier(part, location=location)
        for part in _split_modifier_list(modifiers_text, location=location)
    )
    return ScalarReferencePart(
        target=_parse_scalar_lookup(target_text, location=location),
        modifiers=modifiers,
    )


def _parse_list_expand_part(inner: str, *, location) -> ListExpandPart:
    target_text, modifiers_text = _split_modifier_segments(inner, location=location)
    if not target_text:
        _raise("empty list expansion", location=location)
    modifiers = tuple(
        _parse_modifier(part, location=location)
        for part in _split_modifier_list(modifiers_text, location=location)
    )
    return ListExpandPart(
        name=_expect_identifier(target_text, location=location),
        modifiers=modifiers,
    )


def parse_template(template_literal: TemplateLiteral) -> Template:
    """Parse one raw string-template literal into a compiled AST."""

    text = template_literal.value
    location = template_literal.location
    parts: list[object] = []
    literal_chars: list[str] = []
    index = 0

    while index < len(text):
        char = text[index]
        if char == "\\":
            if index + 1 >= len(text):
                _raise("unterminated string escape", location=location)
            escaped = text[index + 1]
            if escaped not in _VALID_LITERAL_ESCAPES:
                _raise(f"invalid string escape \\{escaped}", location=location)
            literal_chars.append(escaped)
            index += 2
            continue

        if char == "{" and index + 1 < len(text) and text[index + 1] == "[":
            _flush_literal(parts, literal_chars)
            end = text.find("]}", index + 2)
            if end == -1:
                _raise("unterminated list concatenation", location=location)
            inner = text[index + 2 : end].strip()
            if not inner:
                _raise("empty list concatenation target", location=location)
            parts.append(
                ListConcatPart(name=_expect_identifier(inner, location=location))
            )
            index = end + 2
            continue

        if char == "{":
            _flush_literal(parts, literal_chars)
            end = _find_matching_closer(text, index + 1, opener="{", closer="}")
            if end == -1:
                _raise("unterminated scalar interpolation", location=location)
            parts.append(_parse_scalar_part(text[index + 1 : end], location=location))
            index = end + 1
            continue

        if char == "[":
            _flush_literal(parts, literal_chars)
            end = _find_matching_closer(text, index + 1, opener="[", closer="]")
            if end == -1:
                _raise("unterminated list expansion", location=location)
            parts.append(
                _parse_list_expand_part(text[index + 1 : end], location=location)
            )
            index = end + 1
            continue

        literal_chars.append(char)
        index += 1

    _flush_literal(parts, literal_chars)
    return Template(parts=tuple(parts), location=location)
