from __future__ import annotations

import pytest


ast = pytest.importorskip(
    "stagegate.tools.minigate.ast",
    reason="minigate ast module is not implemented yet",
)
errors = pytest.importorskip(
    "stagegate.tools.minigate.errors",
    reason="minigate errors module is not implemented yet",
)
template_parser = pytest.importorskip(
    "stagegate.tools.minigate.template_parser",
    reason="minigate template parser module is not implemented yet",
)


LOCATION = ast.SourceLocation(source_name="spec.pipeline", line_number=10)


def test_parse_template_builds_scalar_list_and_concat_parts() -> None:
    template = ast.TemplateLiteral(
        value="prep {#2:stem:removesuffix(.nii.gz)} [imgs:name] {[outs]}",
        location=LOCATION,
    )

    compiled = template_parser.parse_template(template)

    assert len(compiled.parts) == 6
    assert isinstance(compiled.parts[0], ast.LiteralPart)
    assert isinstance(compiled.parts[1], ast.ScalarReferencePart)
    assert compiled.parts[1].target.column_index == 2
    assert [type(modifier) for modifier in compiled.parts[1].modifiers] == [
        ast.PathAttributeModifier,
        ast.RemoveSuffixModifier,
    ]
    assert isinstance(compiled.parts[3], ast.ListExpandPart)
    assert compiled.parts[3].name == "imgs"
    assert isinstance(compiled.parts[5], ast.ListConcatPart)
    assert compiled.parts[5].name == "outs"


def test_parse_template_decodes_literal_escapes() -> None:
    template = ast.TemplateLiteral(
        value=r"literal \{ brace \} quote \" slash \\",
        location=LOCATION,
    )

    compiled = template_parser.parse_template(template)

    assert compiled.parts == (ast.LiteralPart('literal { brace } quote " slash \\'),)


def test_parse_template_rejects_invalid_modifier() -> None:
    template = ast.TemplateLiteral(
        value="{name:unknown()}",
        location=LOCATION,
    )

    with pytest.raises(errors.ParseError, match="modifier"):
        template_parser.parse_template(template)


def test_parse_template_rejects_modifier_on_list_concat() -> None:
    template = ast.TemplateLiteral(
        value="{[outs:name]}",
        location=LOCATION,
    )

    with pytest.raises(errors.ParseError, match="identifier"):
        template_parser.parse_template(template)
