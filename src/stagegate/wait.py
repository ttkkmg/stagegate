"""Public wait-condition constants used by pipeline and scheduler wait APIs."""

from __future__ import annotations

from typing import Final

FIRST_COMPLETED: Final[str] = "FIRST_COMPLETED"
FIRST_EXCEPTION: Final[str] = "FIRST_EXCEPTION"
ALL_COMPLETED: Final[str] = "ALL_COMPLETED"

WAIT_CONDITIONS: Final[frozenset[str]] = frozenset(
    {FIRST_COMPLETED, FIRST_EXCEPTION, ALL_COMPLETED}
)
