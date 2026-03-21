"""Public exceptions for stagegate."""


class CancelledError(Exception):
    """Raised when result() or exception() is requested from a cancelled handle."""


class DiscardedHandleError(RuntimeError):
    """Raised when an operation is requested from a discarded handle."""


class UnknownResourceError(ValueError):
    """Raised when a task requests a resource label unknown to the scheduler."""


class UnschedulableTaskError(ValueError):
    """Raised when a single task can never fit within scheduler capacity."""
