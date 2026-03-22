"""Public exceptions for stagegate."""


class CancelledError(Exception):
    """Raised when result() or exception() is requested from a cancelled handle."""


class TerminatedError(Exception):
    """Raised when a task or helper terminates cooperatively after a request."""

    def __init__(
        self,
        *,
        argv: tuple[str, ...],
        pid: int | None,
        returncode: int | None,
        forced_kill: bool,
    ) -> None:
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
