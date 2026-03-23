"""Public exceptions for stagegate."""


class CancelledError(Exception):
    """Raised when a cancelled handle is observed as if it had a result.

    This exception is used by both task and pipeline handles when
    ``result()`` or ``exception()`` is requested after a queued item was
    cancelled before start.
    """


class TerminatedError(Exception):
    """Raised when a task terminates cooperatively after a terminate request.

    Attributes:
        argv: Command-line arguments associated with the terminated subprocess,
            or an empty tuple when no subprocess metadata exists.
        pid: Process identifier for the terminated subprocess, if any.
        returncode: Observed subprocess return code, if any.
        forced_kill: Whether the subprocess helper had to escalate from
            ``SIGTERM`` to ``SIGKILL``.
    """

    def __init__(
        self,
        *,
        argv: tuple[str, ...],
        pid: int | None,
        returncode: int | None,
        forced_kill: bool,
    ) -> None:
        """Initialize a termination exception with optional subprocess details.

        Args:
            argv: Command-line arguments associated with the termination path.
            pid: Process identifier for the subprocess, if any.
            returncode: Observed subprocess return code, if any.
            forced_kill: Whether a forced kill was required.
        """
        self.argv = argv
        self.pid = pid
        self.returncode = returncode
        self.forced_kill = forced_kill

        message = "task terminated cooperatively"
        if pid is not None:
            message = f"{message}: pid={pid}"
        if forced_kill:
            message = f"{message}, forced_kill=True"
        super().__init__(message)


class DiscardedHandleError(RuntimeError):
    """Raised when an operation is requested from a discarded handle."""


class UnknownResourceError(ValueError):
    """Raised when a task requests a resource label unknown to the scheduler."""


class UnschedulableTaskError(ValueError):
    """Raised when a single task can never fit within scheduler capacity."""
