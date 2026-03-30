"""AST and shared datatypes for minigate parsing and evaluation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TypeAlias


@dataclass(frozen=True, slots=True)
class SourceLocation:
    """Location of a DSL or input-data item."""

    source_name: str
    line_number: int | None = None
    row_number: int | None = None

    def format_prefix(self) -> str:
        """Return a stable human-readable location prefix."""
        if self.line_number is not None:
            return f"{self.source_name}:{self.line_number}"
        if self.row_number is not None:
            return f"{self.source_name}: row={self.row_number}"
        return self.source_name


@dataclass(frozen=True, slots=True)
class LogicalLine:
    """Preprocessed logical DSL line plus origin metadata."""

    text: str
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class TemplateLiteral:
    """Raw template literal before template-parser compilation."""

    value: str
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class ColumnCall:
    """Special alias-only COLUMN(\"...\") source node."""

    column_name: str
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class PipelinesStatement:
    """Top-level pipelines statement."""

    value: int
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class TasksStatement:
    """Top-level tasks statement."""

    value: int
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class FormatStatement:
    """Top-level format statement."""

    value: Literal["csv", "tsv", "varlists", "args"]
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class HeaderStatement:
    """Top-level header statement."""

    value: bool
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class CodepageStatement:
    """Top-level codepage statement."""

    value: str
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class DelimiterStatement:
    """Top-level varlists delimiter statement."""

    value: Literal["comma", "whitespace"]
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class LimitStatement:
    """Top-level abstract resource capacity statement."""

    label: str
    value: int
    location: SourceLocation


TopLevelStatement: TypeAlias = (
    PipelinesStatement
    | TasksStatement
    | FormatStatement
    | HeaderStatement
    | CodepageStatement
    | DelimiterStatement
    | LimitStatement
)


@dataclass(frozen=True, slots=True)
class ColumnReference:
    """Direct column reference used in list definitions."""

    identifier: str | None = None
    column_index: int | None = None

    def __post_init__(self) -> None:
        has_identifier = self.identifier is not None
        has_index = self.column_index is not None
        if has_identifier == has_index:
            raise ValueError(
                "ColumnReference requires exactly one of identifier or column_index"
            )


@dataclass(frozen=True, slots=True)
class ListDefinition:
    """[list] section definition."""

    name: str
    elements: tuple[ColumnReference | Literal["..."], ...]
    location: SourceLocation


AliasSource: TypeAlias = TemplateLiteral | ColumnCall


@dataclass(frozen=True, slots=True)
class AliasDefinition:
    """[alias] section definition."""

    name: str
    value: AliasSource
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class RunStatement:
    """run statement with one or more templates."""

    commands: tuple[TemplateLiteral, ...]
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class RunListStatement:
    """run_list statement with exactly one template."""

    command: TemplateLiteral
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class UseStatement:
    """Stage-local abstract resource request."""

    label: str
    value: int
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class AcceptRetvalsValue:
    """Single accepted return code."""

    value: int
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class AcceptRetvalsRange:
    """Inclusive accepted return-code range."""

    left: int
    right: int
    location: SourceLocation


AcceptRetvalsItem: TypeAlias = AcceptRetvalsValue | AcceptRetvalsRange


@dataclass(frozen=True, slots=True)
class AcceptRetvalsStatement:
    """Stage-local accepted return-code policy."""

    items: tuple[AcceptRetvalsItem, ...]
    location: SourceLocation


StageStatement: TypeAlias = (
    RunStatement | RunListStatement | UseStatement | AcceptRetvalsStatement
)


@dataclass(frozen=True, slots=True)
class ListSection:
    """Raw [list] section preserving original section boundary."""

    definitions: tuple[ListDefinition, ...]
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class AliasSection:
    """Raw [alias] section preserving original section boundary."""

    definitions: tuple[AliasDefinition, ...]
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class StageSection:
    """Raw [stage.N] section preserving original section boundary."""

    stage_number: int
    statements: tuple[StageStatement, ...]
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class UnknownSection:
    """Raw unknown section kept for later validation and diagnostics."""

    name: str
    lines: tuple[LogicalLine, ...]
    location: SourceLocation


RawSection: TypeAlias = ListSection | AliasSection | StageSection | UnknownSection


@dataclass(frozen=True, slots=True)
class RawProgram:
    """Whole parsed DSL program before validation."""

    top_level_statements: tuple[TopLevelStatement, ...] = ()
    sections: tuple[RawSection, ...] = ()


Program: TypeAlias = RawProgram


@dataclass(frozen=True, slots=True)
class ScalarLookup:
    """Scalar lookup target inside a compiled template."""

    identifier: str | None = None
    column_index: int | None = None

    def __post_init__(self) -> None:
        has_identifier = self.identifier is not None
        has_index = self.column_index is not None
        if has_identifier == has_index:
            raise ValueError(
                "ScalarLookup requires exactly one of identifier or column_index"
            )


@dataclass(frozen=True, slots=True)
class Modifier:
    """Base modifier marker type."""


@dataclass(frozen=True, slots=True)
class SliceModifier(Modifier):
    """slice(i,j) modifier."""

    start: int
    stop: int


@dataclass(frozen=True, slots=True)
class RemoveSuffixModifier(Modifier):
    """removesuffix(...) modifier."""

    suffix: str


@dataclass(frozen=True, slots=True)
class PathAttributeModifier(Modifier):
    """name/stem/parent path attribute modifier."""

    attribute: Literal["name", "stem", "parent"]


@dataclass(frozen=True, slots=True)
class LiteralPart:
    """Literal text segment in a compiled template."""

    text: str


@dataclass(frozen=True, slots=True)
class ScalarReferencePart:
    """Scalar lookup plus modifiers."""

    target: ScalarLookup
    modifiers: tuple[Modifier, ...] = ()


@dataclass(frozen=True, slots=True)
class ListExpandPart:
    """List expansion part used by run_list and list-typed aliases."""

    name: str
    modifiers: tuple[Modifier, ...] = ()


@dataclass(frozen=True, slots=True)
class ListConcatPart:
    """List concatenation part that joins list values with spaces."""

    name: str


TemplatePart: TypeAlias = (
    LiteralPart | ScalarReferencePart | ListExpandPart | ListConcatPart
)


@dataclass(frozen=True, slots=True)
class Template:
    """Compiled template AST."""

    parts: tuple[TemplatePart, ...] = field(default_factory=tuple)
    location: SourceLocation | None = None

    def is_list_typed(self) -> bool:
        """Return whether the template expands to a list value."""
        return any(isinstance(part, ListExpandPart) for part in self.parts)


@dataclass(frozen=True, slots=True)
class ValidatedConfig:
    """Normalized top-level configuration after validation."""

    format_name: Literal["csv", "tsv", "varlists", "args"]
    pipelines: int
    tasks: int
    header: bool
    codepage: str
    delimiter: Literal["comma", "whitespace"] | None
    limits: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class ValidatedListDefinition:
    """Normalized list definition."""

    name: str
    elements: tuple[ColumnReference | Literal["..."], ...]
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class ValidatedAliasDefinition:
    """Normalized alias definition with compiled or special value."""

    name: str
    value: Template | ColumnCall
    value_kind: Literal["scalar", "list"]
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class ValidatedRunStatement:
    """Validated run statement with compiled templates."""

    commands: tuple[Template, ...]
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class ValidatedRunListStatement:
    """Validated run_list statement with compiled template."""

    command: Template
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class ValidatedStageDefinition:
    """Normalized stage definition ready for runtime use."""

    stage_number: int
    execution: ValidatedRunStatement | ValidatedRunListStatement
    resources: tuple[tuple[str, int], ...]
    accept_retvals: tuple[AcceptRetvalsItem, ...]
    location: SourceLocation


@dataclass(frozen=True, slots=True)
class ValidatedProgram:
    """Fully normalized representation after static validation."""

    config: ValidatedConfig
    lists: tuple[ValidatedListDefinition, ...]
    aliases: tuple[ValidatedAliasDefinition, ...]
    stages: tuple[ValidatedStageDefinition, ...]
