"""Internal exceptions for minigate parsing, validation, and runtime."""

from __future__ import annotations

from .ast import SourceLocation


class MinigateError(Exception):
    """Base exception for minigate-specific failures."""

    def __init__(
        self,
        message: str,
        *,
        location: SourceLocation | None = None,
    ) -> None:
        self.message = message
        self.location = location
        if location is None:
            super().__init__(message)
        else:
            super().__init__(f"{location.format_prefix()}: {message}")


class CliUsageError(MinigateError):
    """Raised for invalid CLI usage before scheduler execution starts."""


class ParseError(MinigateError):
    """Raised for lexical or grammar failures in the DSL file."""


class StaticValidationError(MinigateError):
    """Raised for row-independent configuration or type errors."""


class RuntimeRowError(MinigateError):
    """Raised for errors that depend on a concrete input row."""
