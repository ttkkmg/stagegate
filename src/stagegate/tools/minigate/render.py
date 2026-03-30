"""Output rendering helpers for minigate."""

from __future__ import annotations

from dataclasses import dataclass, field
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import TextIO


def format_command_group(
    *,
    stage_number: int,
    statement_name: str,
    commands: Sequence[str],
) -> str:
    """Format one rendered command group for verbose or dry-run output."""

    if not commands:
        return f"[{stage_number}]{statement_name}(0) :"

    prefix = f"[{stage_number}]{statement_name}({len(commands)}) : "
    indent = " " * len(prefix)
    lines = [f"{prefix}{commands[0]}"]
    lines.extend(f"{indent}{command}" for command in commands[1:])
    return "\n".join(lines)


@dataclass(slots=True)
class RuntimeReporter:
    """Thread-safe text reporter for verbose and dry-run output."""

    stream: TextIO
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def emit_command_group(
        self,
        *,
        stage_number: int,
        statement_name: str,
        commands: Sequence[str],
    ) -> None:
        text = format_command_group(
            stage_number=stage_number,
            statement_name=statement_name,
            commands=commands,
        )
        with self._lock:
            self.stream.write(text)
            self.stream.write("\n")
            self.stream.flush()
