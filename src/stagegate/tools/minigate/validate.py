"""Static validation and normalized-model construction for minigate."""

from __future__ import annotations

from .ast import (
    AcceptRetvalsItem,
    AcceptRetvalsStatement,
    AcceptRetvalsValue,
    AliasSection,
    CodepageStatement,
    ColumnCall,
    ColumnReference,
    DelimiterStatement,
    FormatStatement,
    HeaderStatement,
    LimitStatement,
    ListConcatPart,
    ListExpandPart,
    ListSection,
    PipelinesStatement,
    RawProgram,
    RunListStatement,
    RunStatement,
    ScalarReferencePart,
    StageSection,
    TasksStatement,
    Template,
    TopLevelStatement,
    UnknownSection,
    UseStatement,
    ValidatedAliasDefinition,
    ValidatedConfig,
    ValidatedListDefinition,
    ValidatedProgram,
    ValidatedRunListStatement,
    ValidatedRunStatement,
    ValidatedStageDefinition,
)
from .errors import ParseError, StaticValidationError
from .template_parser import parse_template


def _raise(message: str, *, location) -> None:
    raise StaticValidationError(message, location=location)


def _normalize_singletons(
    statements: tuple[TopLevelStatement, ...],
) -> dict[type[TopLevelStatement], TopLevelStatement]:
    singletons: dict[type[TopLevelStatement], TopLevelStatement] = {}
    names = {
        PipelinesStatement: "pipelines",
        TasksStatement: "tasks",
        FormatStatement: "format",
        HeaderStatement: "header",
        CodepageStatement: "codepage",
        DelimiterStatement: "delimiter",
    }
    for statement in statements:
        if isinstance(statement, LimitStatement):
            continue
        statement_type = type(statement)
        if statement_type in singletons:
            _raise(
                "duplicate top-level statement: "
                f"{names.get(statement_type, statement_type.__name__)}",
                location=statement.location,
            )
        singletons[statement_type] = statement
    return singletons


def _build_validated_config(raw_program: RawProgram) -> ValidatedConfig:
    singletons = _normalize_singletons(raw_program.top_level_statements)
    limits = [
        statement
        for statement in raw_program.top_level_statements
        if isinstance(statement, LimitStatement)
    ]

    seen_limit_labels: set[str] = set()
    for limit in limits:
        if limit.label in seen_limit_labels:
            _raise(f"duplicate limit label: {limit.label}", location=limit.location)
        seen_limit_labels.add(limit.label)

    format_stmt = singletons.get(FormatStatement)
    if format_stmt is None:
        location = (
            raw_program.top_level_statements[0].location
            if raw_program.top_level_statements
            else None
        )
        _raise("format is required", location=location)
    assert isinstance(format_stmt, FormatStatement)

    header_stmt = singletons.get(HeaderStatement)
    codepage_stmt = singletons.get(CodepageStatement)
    delimiter_stmt = singletons.get(DelimiterStatement)
    pipelines_stmt = singletons.get(PipelinesStatement)
    tasks_stmt = singletons.get(TasksStatement)

    format_name = format_stmt.value
    if format_name == "args":
        if header_stmt is not None:
            _raise("header is invalid under format args", location=header_stmt.location)
        if codepage_stmt is not None:
            _raise(
                "codepage is invalid under format args",
                location=codepage_stmt.location,
            )
        if delimiter_stmt is not None:
            _raise(
                "delimiter is available only for format varlists",
                location=delimiter_stmt.location,
            )
    elif format_name in {"csv", "tsv"}:
        if delimiter_stmt is not None:
            _raise(
                "delimiter is available only for format varlists",
                location=delimiter_stmt.location,
            )
    elif format_name == "varlists":
        if delimiter_stmt is None:
            _raise(
                "delimiter is required for format varlists",
                location=format_stmt.location,
            )
        if header_stmt is not None and header_stmt.value:
            _raise(
                "header true is invalid for format varlists",
                location=header_stmt.location,
            )

    return ValidatedConfig(
        format_name=format_name,
        pipelines=1 if pipelines_stmt is None else pipelines_stmt.value,
        tasks=1 if tasks_stmt is None else tasks_stmt.value,
        header=False if header_stmt is None else header_stmt.value,
        codepage="utf-8-sig" if codepage_stmt is None else codepage_stmt.value,
        delimiter=None if delimiter_stmt is None else delimiter_stmt.value,
        limits=tuple((limit.label, limit.value) for limit in limits),
    )


def _validate_list_definition_shape(definition, *, location) -> None:
    ellipsis_count = sum(1 for element in definition.elements if element == "...")
    if ellipsis_count > 1:
        _raise("list definition may contain at most one ellipsis", location=location)

    if ellipsis_count == 0:
        return

    ellipsis_index = definition.elements.index("...")
    left_refs = [
        element
        for element in definition.elements[:ellipsis_index]
        if isinstance(element, ColumnReference)
    ]
    right_refs = [
        element
        for element in definition.elements[ellipsis_index + 1 :]
        if isinstance(element, ColumnReference)
    ]

    if len(left_refs) > 1 or len(right_refs) > 1:
        _raise(
            "ellipsis form must be one of A,...,B / ...,B / A,... / ...",
            location=location,
        )

    endpoint_refs = left_refs + right_refs
    has_named = any(ref.identifier is not None for ref in endpoint_refs)
    has_indexed = any(ref.column_index is not None for ref in endpoint_refs)
    if has_named and has_indexed:
        _raise(
            "named and indexed references cannot be mixed around ellipsis",
            location=location,
        )


def _collect_lists(
    raw_program: RawProgram,
    *,
    config: ValidatedConfig,
) -> tuple[ValidatedListDefinition, ...]:
    validated: list[ValidatedListDefinition] = []
    seen_names: set[str] = set()

    for section in raw_program.sections:
        if isinstance(section, UnknownSection):
            _raise(f"unknown section: [{section.name}]", location=section.location)
        if not isinstance(section, ListSection):
            continue
        for definition in section.definitions:
            if definition.name in seen_names:
                _raise(
                    f"duplicate list definition: {definition.name}",
                    location=definition.location,
                )
            _validate_list_definition_shape(definition, location=definition.location)
            for element in definition.elements:
                if not isinstance(element, ColumnReference):
                    continue
                if element.identifier is None:
                    continue
                if config.format_name == "args":
                    if element.identifier != "arg":
                        _raise(
                            f"named column reference is invalid here: {element.identifier}",
                            location=definition.location,
                        )
                    continue
                if not config.header:
                    _raise(
                        f"named column reference is invalid here: {element.identifier}",
                        location=definition.location,
                    )
            seen_names.add(definition.name)
            validated.append(
                ValidatedListDefinition(
                    name=definition.name,
                    elements=definition.elements,
                    location=definition.location,
                )
            )
    return tuple(validated)


def _validate_scalar_target(
    *,
    name: str,
    location,
    alias_kinds: dict[str, str],
    all_alias_names: set[str],
    allow_columns_by_name: bool,
    allow_arg_name: bool,
) -> None:
    if name in all_alias_names and name not in alias_kinds:
        _raise(
            f"alias forward reference is not allowed: {name}",
            location=location,
        )
    alias_kind = alias_kinds.get(name)
    if alias_kind is not None:
        if alias_kind != "scalar":
            _raise(
                f"alias type mismatch for scalar reference: {name}",
                location=location,
            )
        return
    if allow_columns_by_name:
        return
    if allow_arg_name and name == "arg":
        return
    _raise(f"named scalar reference is invalid here: {name}", location=location)


def _validate_list_target(
    *,
    name: str,
    location,
    alias_kinds: dict[str, str],
    all_alias_names: set[str],
    list_names: set[str],
) -> None:
    if name in all_alias_names and name not in alias_kinds:
        _raise(
            f"alias forward reference is not allowed: {name}",
            location=location,
        )
    alias_kind = alias_kinds.get(name)
    if alias_kind is not None:
        if alias_kind != "list":
            _raise(f"alias type mismatch for list reference: {name}", location=location)
        return
    if name in list_names:
        return
    _raise(f"undefined list or list alias: {name}", location=location)


def _validate_compiled_template(
    template: Template,
    *,
    alias_kinds: dict[str, str],
    all_alias_names: set[str],
    list_names: set[str],
    allow_columns_by_name: bool,
    allow_arg_name: bool,
    allow_list_expand: bool,
) -> None:
    for part in template.parts:
        if isinstance(part, ScalarReferencePart):
            if part.target.identifier is None:
                continue
            _validate_scalar_target(
                name=part.target.identifier,
                location=template.location,
                alias_kinds=alias_kinds,
                all_alias_names=all_alias_names,
                allow_columns_by_name=allow_columns_by_name,
                allow_arg_name=allow_arg_name,
            )
            continue

        if isinstance(part, ListExpandPart):
            if not allow_list_expand:
                _raise(
                    "run does not support list-parallel expansion",
                    location=template.location,
                )
            _validate_list_target(
                name=part.name,
                location=template.location,
                alias_kinds=alias_kinds,
                all_alias_names=all_alias_names,
                list_names=list_names,
            )
            continue

        if isinstance(part, ListConcatPart):
            _validate_list_target(
                name=part.name,
                location=template.location,
                alias_kinds=alias_kinds,
                all_alias_names=all_alias_names,
                list_names=list_names,
            )


def _compile_template(template_literal):
    try:
        return parse_template(template_literal)
    except ParseError as exc:
        raise StaticValidationError(exc.message, location=exc.location) from exc


def _collect_aliases(
    raw_program: RawProgram,
    *,
    config: ValidatedConfig,
    lists: tuple[ValidatedListDefinition, ...],
) -> tuple[ValidatedAliasDefinition, ...]:
    validated: list[ValidatedAliasDefinition] = []
    list_names = {definition.name for definition in lists}
    defined_alias_kinds: dict[str, str] = {}
    all_alias_names = {
        definition.name
        for section in raw_program.sections
        if isinstance(section, AliasSection)
        for definition in section.definitions
    }

    for section in raw_program.sections:
        if not isinstance(section, AliasSection):
            continue
        for definition in section.definitions:
            if definition.name in defined_alias_kinds:
                _raise(
                    f"duplicate alias definition: {definition.name}",
                    location=definition.location,
                )

            value = definition.value
            if isinstance(value, ColumnCall):
                if config.format_name == "varlists":
                    _raise(
                        "COLUMN(...) is invalid for format varlists",
                        location=definition.location,
                    )
                if config.format_name != "args" and not config.header:
                    _raise(
                        "COLUMN(...) requires header-enabled input",
                        location=definition.location,
                    )
                validated.append(
                    ValidatedAliasDefinition(
                        name=definition.name,
                        value=value,
                        value_kind="scalar",
                        location=definition.location,
                    )
                )
                defined_alias_kinds[definition.name] = "scalar"
                continue

            compiled = _compile_template(value)
            _validate_compiled_template(
                compiled,
                alias_kinds=defined_alias_kinds,
                all_alias_names=all_alias_names,
                list_names=list_names,
                allow_columns_by_name=config.header,
                allow_arg_name=config.format_name == "args",
                allow_list_expand=True,
            )
            kind = "list" if compiled.is_list_typed() else "scalar"
            validated.append(
                ValidatedAliasDefinition(
                    name=definition.name,
                    value=compiled,
                    value_kind=kind,
                    location=definition.location,
                )
            )
            defined_alias_kinds[definition.name] = kind

    return tuple(validated)


def _default_accept_retvals(location) -> tuple[AcceptRetvalsItem, ...]:
    return (AcceptRetvalsValue(value=0, location=location),)


def _validate_accept_retvals_items(items: tuple[AcceptRetvalsItem, ...]) -> None:
    for item in items:
        if isinstance(item, AcceptRetvalsValue):
            continue
        if item.left < 1 or item.right < 1 or item.left >= item.right:
            _raise("invalid accept_retvals range", location=item.location)


def _collect_stages(
    raw_program: RawProgram,
    *,
    config: ValidatedConfig,
    lists: tuple[ValidatedListDefinition, ...],
    aliases: tuple[ValidatedAliasDefinition, ...],
) -> tuple[ValidatedStageDefinition, ...]:
    stage_sections = [
        section for section in raw_program.sections if isinstance(section, StageSection)
    ]
    list_names = {definition.name for definition in lists}
    alias_kinds = {definition.name: definition.value_kind for definition in aliases}
    all_alias_names = set(alias_kinds)
    seen_stage_numbers: set[int] = set()
    validated: list[ValidatedStageDefinition] = []

    for section in stage_sections:
        if section.stage_number in seen_stage_numbers:
            _raise(
                f"duplicate stage: {section.stage_number}",
                location=section.location,
            )
        seen_stage_numbers.add(section.stage_number)

        run_statements = [
            statement
            for statement in section.statements
            if isinstance(statement, RunStatement)
        ]
        run_list_statements = [
            statement
            for statement in section.statements
            if isinstance(statement, RunListStatement)
        ]
        use_statements = [
            statement
            for statement in section.statements
            if isinstance(statement, UseStatement)
        ]
        accept_statements = [
            statement
            for statement in section.statements
            if isinstance(statement, AcceptRetvalsStatement)
        ]

        if len(run_statements) + len(run_list_statements) != 1:
            _raise(
                "stage must contain exactly one of run or run_list",
                location=section.location,
            )
        if len(accept_statements) > 1:
            _raise("stage accepts only one accept_retvals", location=section.location)

        seen_use_labels: set[str] = set()
        resources: list[tuple[str, int]] = []
        for use in use_statements:
            if use.label in seen_use_labels:
                _raise(f"duplicate use label: {use.label}", location=use.location)
            seen_use_labels.add(use.label)
            resources.append((use.label, use.value))

        allow_columns_by_name = config.header
        allow_arg_name = config.format_name == "args"

        if run_statements:
            run_stmt = run_statements[0]
            compiled_commands = []
            for command in run_stmt.commands:
                compiled = _compile_template(command)
                _validate_compiled_template(
                    compiled,
                    alias_kinds=alias_kinds,
                    all_alias_names=all_alias_names,
                    list_names=list_names,
                    allow_columns_by_name=allow_columns_by_name,
                    allow_arg_name=allow_arg_name,
                    allow_list_expand=False,
                )
                compiled_commands.append(compiled)
            execution = ValidatedRunStatement(
                commands=tuple(compiled_commands),
                location=run_stmt.location,
            )
        else:
            run_list_stmt = run_list_statements[0]
            compiled = _compile_template(run_list_stmt.command)
            _validate_compiled_template(
                compiled,
                alias_kinds=alias_kinds,
                all_alias_names=all_alias_names,
                list_names=list_names,
                allow_columns_by_name=allow_columns_by_name,
                allow_arg_name=allow_arg_name,
                allow_list_expand=True,
            )
            execution = ValidatedRunListStatement(
                command=compiled,
                location=run_list_stmt.location,
            )

        accept_items = (
            _default_accept_retvals(section.location)
            if not accept_statements
            else accept_statements[0].items
        )
        _validate_accept_retvals_items(accept_items)

        validated.append(
            ValidatedStageDefinition(
                stage_number=section.stage_number,
                execution=execution,
                resources=tuple(resources),
                accept_retvals=tuple(accept_items),
                location=section.location,
            )
        )

    validated.sort(key=lambda stage: stage.stage_number)
    return tuple(validated)


def validate_program(raw_program: RawProgram) -> ValidatedProgram:
    """Validate a raw AST and build a normalized runtime-oriented model."""

    config = _build_validated_config(raw_program)
    lists = _collect_lists(raw_program, config=config)
    aliases = _collect_aliases(raw_program, config=config, lists=lists)
    stages = _collect_stages(raw_program, config=config, lists=lists, aliases=aliases)
    return ValidatedProgram(
        config=config,
        lists=lists,
        aliases=aliases,
        stages=stages,
    )
