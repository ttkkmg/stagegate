"""Row-level template and alias evaluation for minigate."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePath
from typing import TYPE_CHECKING

from .ast import (
    ColumnCall,
    ColumnReference,
    ListConcatPart,
    ListExpandPart,
    LiteralPart,
    PathAttributeModifier,
    RemoveSuffixModifier,
    ScalarLookup,
    ScalarReferencePart,
    SliceModifier,
    Template,
    ValidatedAliasDefinition,
    ValidatedListDefinition,
    ValidatedProgram,
)
from .errors import RuntimeRowError

if TYPE_CHECKING:
    from .input_loader import InputRow


@dataclass(slots=True)
class EvaluationContext:
    """Evaluation context for one input row."""

    program: ValidatedProgram
    row: InputRow
    _header_lookup: dict[str, int] = field(init=False, default_factory=dict)
    _list_definitions: dict[str, ValidatedListDefinition] = field(
        init=False, default_factory=dict
    )
    _alias_definitions: dict[str, ValidatedAliasDefinition] = field(
        init=False, default_factory=dict
    )
    _scalar_alias_cache: dict[str, str] = field(init=False, default_factory=dict)
    _list_alias_cache: dict[str, tuple[str, ...]] = field(
        init=False, default_factory=dict
    )
    _resolving_scalar_aliases: set[str] = field(init=False, default_factory=set)
    _resolving_list_aliases: set[str] = field(init=False, default_factory=set)

    def __post_init__(self) -> None:
        if self.row.headers is not None:
            self._header_lookup = {
                header: index for index, header in enumerate(self.row.headers, start=1)
            }
        self._list_definitions = {
            definition.name: definition for definition in self.program.lists
        }
        self._alias_definitions = {
            definition.name: definition for definition in self.program.aliases
        }

    @property
    def location(self):
        return self.row.location


def make_evaluation_context(
    program: ValidatedProgram,
    row: InputRow,
) -> EvaluationContext:
    """Build a reusable evaluation context for one row."""

    return EvaluationContext(program=program, row=row)


def _raise_row_error(message: str, *, context: EvaluationContext) -> None:
    raise RuntimeRowError(message, location=context.location)


def _apply_modifier(value: str, modifier) -> str:
    if isinstance(modifier, SliceModifier):
        return value[modifier.start : modifier.stop]
    if isinstance(modifier, RemoveSuffixModifier):
        return value.removesuffix(modifier.suffix)
    if isinstance(modifier, PathAttributeModifier):
        path = PurePath(value)
        if modifier.attribute == "name":
            return path.name
        if modifier.attribute == "stem":
            return path.stem
        return str(path.parent)
    raise TypeError(f"unsupported modifier type: {type(modifier)!r}")


def _apply_modifiers(value: str, modifiers: tuple[object, ...]) -> str:
    current = value
    for modifier in modifiers:
        current = _apply_modifier(current, modifier)
    return current


def _lookup_column_index(column_index: int, *, context: EvaluationContext) -> str:
    zero_based = column_index - 1
    if zero_based < 0 or zero_based >= len(context.row.values):
        _raise_row_error(f"column #{column_index} out of range", context=context)
    return context.row.values[zero_based]


def _lookup_named_column(column_name: str, *, context: EvaluationContext) -> str:
    column_index = context._header_lookup.get(column_name)
    if column_index is None:
        _raise_row_error(f"unknown column name: {column_name}", context=context)
    return _lookup_column_index(column_index, context=context)


def _resolve_scalar_lookup(target: ScalarLookup, *, context: EvaluationContext) -> str:
    if target.column_index is not None:
        return _lookup_column_index(target.column_index, context=context)

    assert target.identifier is not None
    alias = context._alias_definitions.get(target.identifier)
    if alias is not None:
        if alias.value_kind != "scalar":
            _raise_row_error(
                f"alias {target.identifier!r} is list-typed and cannot be used as scalar",
                context=context,
            )
        return _resolve_scalar_alias(target.identifier, context=context)

    return _lookup_named_column(target.identifier, context=context)


def _resolve_column_reference(
    reference: ColumnReference, *, context: EvaluationContext
) -> str:
    if reference.column_index is not None:
        return _lookup_column_index(reference.column_index, context=context)
    assert reference.identifier is not None
    return _lookup_named_column(reference.identifier, context=context)


def _resolve_range_endpoint(
    reference: ColumnReference, *, context: EvaluationContext
) -> int:
    if reference.column_index is not None:
        column_index = reference.column_index
        if column_index < 1 or column_index > len(context.row.values):
            _raise_row_error(f"column #{column_index} out of range", context=context)
        return column_index
    assert reference.identifier is not None
    column_index = context._header_lookup.get(reference.identifier)
    if column_index is None:
        _raise_row_error(
            f"unknown column name: {reference.identifier}", context=context
        )
    return column_index


def _resolve_list_definition(
    definition: ValidatedListDefinition,
    *,
    context: EvaluationContext,
) -> tuple[str, ...]:
    if "..." not in definition.elements:
        return tuple(
            _resolve_column_reference(element, context=context)
            for element in definition.elements
            if isinstance(element, ColumnReference)
        )

    ellipsis_index = definition.elements.index("...")
    left_refs = tuple(
        element
        for element in definition.elements[:ellipsis_index]
        if isinstance(element, ColumnReference)
    )
    right_refs = tuple(
        element
        for element in definition.elements[ellipsis_index + 1 :]
        if isinstance(element, ColumnReference)
    )

    if len(left_refs) > 1 or len(right_refs) > 1:
        _raise_row_error(
            f"list {definition.name!r} uses unsupported ellipsis shape",
            context=context,
        )

    if left_refs:
        start_index = _resolve_range_endpoint(left_refs[-1], context=context)
    else:
        start_index = 1

    if right_refs:
        stop_index = _resolve_range_endpoint(right_refs[0], context=context)
    else:
        stop_index = len(context.row.values)

    if start_index > stop_index:
        _raise_row_error(
            f"list {definition.name!r} resolves to an empty descending range",
            context=context,
        )

    return tuple(
        context.row.values[index - 1] for index in range(start_index, stop_index + 1)
    )


def _resolve_list_name(name: str, *, context: EvaluationContext) -> tuple[str, ...]:
    alias = context._alias_definitions.get(name)
    if alias is not None:
        if alias.value_kind != "list":
            _raise_row_error(
                f"alias {name!r} is scalar and cannot be used as list",
                context=context,
            )
        return _resolve_list_alias(name, context=context)

    definition = context._list_definitions.get(name)
    if definition is None:
        _raise_row_error(f"unknown list or list alias: {name}", context=context)
    return _resolve_list_definition(definition, context=context)


def _resolve_scalar_alias(name: str, *, context: EvaluationContext) -> str:
    cached = context._scalar_alias_cache.get(name)
    if cached is not None:
        return cached
    if name in context._resolving_scalar_aliases:
        _raise_row_error(f"cyclic scalar alias detected: {name}", context=context)

    alias = context._alias_definitions[name]
    context._resolving_scalar_aliases.add(name)
    try:
        if isinstance(alias.value, ColumnCall):
            if context.row.headers is None:
                _raise_row_error(
                    f"COLUMN(...) alias {name!r} requires header-enabled input",
                    context=context,
                )
            value = _lookup_named_column(alias.value.column_name, context=context)
        else:
            value = evaluate_scalar_template(alias.value, context=context)
    finally:
        context._resolving_scalar_aliases.remove(name)

    context._scalar_alias_cache[name] = value
    return value


def _resolve_list_alias(name: str, *, context: EvaluationContext) -> tuple[str, ...]:
    cached = context._list_alias_cache.get(name)
    if cached is not None:
        return cached
    if name in context._resolving_list_aliases:
        _raise_row_error(f"cyclic list alias detected: {name}", context=context)

    alias = context._alias_definitions[name]
    context._resolving_list_aliases.add(name)
    try:
        if isinstance(alias.value, ColumnCall):
            _raise_row_error(
                f"COLUMN(...) alias {name!r} cannot be list-typed",
                context=context,
            )
        value = evaluate_list_template(alias.value, context=context)
    finally:
        context._resolving_list_aliases.remove(name)

    context._list_alias_cache[name] = value
    return value


def evaluate_scalar_template(template: Template, *, context: EvaluationContext) -> str:
    """Evaluate a scalar template against one row."""

    if template.is_list_typed():
        _raise_row_error(
            "list-typed template cannot be evaluated as scalar", context=context
        )

    parts: list[str] = []
    for part in template.parts:
        if isinstance(part, LiteralPart):
            parts.append(part.text)
            continue
        if isinstance(part, ScalarReferencePart):
            value = _resolve_scalar_lookup(part.target, context=context)
            parts.append(_apply_modifiers(value, part.modifiers))
            continue
        if isinstance(part, ListConcatPart):
            parts.append(" ".join(_resolve_list_name(part.name, context=context)))
            continue
        if isinstance(part, ListExpandPart):
            _raise_row_error(
                "list expansion cannot appear in scalar template evaluation",
                context=context,
            )
    return "".join(parts)


def evaluate_list_template(
    template: Template,
    *,
    context: EvaluationContext,
) -> tuple[str, ...]:
    """Evaluate a list-typed template against one row."""

    if not template.is_list_typed():
        return (evaluate_scalar_template(template, context=context),)

    resolved_parts: list[str | tuple[str, ...]] = []
    list_lengths: set[int] = set()
    for part in template.parts:
        if isinstance(part, LiteralPart):
            resolved_parts.append(part.text)
            continue
        if isinstance(part, ScalarReferencePart):
            value = _resolve_scalar_lookup(part.target, context=context)
            resolved_parts.append(_apply_modifiers(value, part.modifiers))
            continue
        if isinstance(part, ListConcatPart):
            resolved_parts.append(
                " ".join(_resolve_list_name(part.name, context=context))
            )
            continue
        if isinstance(part, ListExpandPart):
            values = tuple(
                _apply_modifiers(value, part.modifiers)
                for value in _resolve_list_name(part.name, context=context)
            )
            resolved_parts.append(values)
            list_lengths.add(len(values))
            continue

    if len(list_lengths) != 1:
        _raise_row_error("list length mismatch in template expansion", context=context)

    result_length = next(iter(list_lengths))
    built: list[str] = []
    for index in range(result_length):
        fragments: list[str] = []
        for part in resolved_parts:
            if isinstance(part, tuple):
                fragments.append(part[index])
            else:
                fragments.append(part)
        built.append("".join(fragments))
    return tuple(built)


def evaluate_template(
    template: Template,
    *,
    context: EvaluationContext,
) -> str | tuple[str, ...]:
    """Evaluate one template using its compiled type shape."""

    if template.is_list_typed():
        return evaluate_list_template(template, context=context)
    return evaluate_scalar_template(template, context=context)
