"""Public wait-condition constants used by wait APIs.

Attributes:
    FIRST_COMPLETED: Return from a wait call when at least one handle is
        terminal.
    FIRST_EXCEPTION: Return from a wait call when at least one handle is in a
        failure-like terminal state, otherwise behave like
        ``ALL_COMPLETED``.
    ALL_COMPLETED: Return from a wait call only when all handles are
        terminal.
"""

from __future__ import annotations

from typing import Final

FIRST_COMPLETED: Final[str] = "FIRST_COMPLETED"
FIRST_EXCEPTION: Final[str] = "FIRST_EXCEPTION"
ALL_COMPLETED: Final[str] = "ALL_COMPLETED"

WAIT_CONDITIONS: Final[frozenset[str]] = frozenset(
    {FIRST_COMPLETED, FIRST_EXCEPTION, ALL_COMPLETED}
)
