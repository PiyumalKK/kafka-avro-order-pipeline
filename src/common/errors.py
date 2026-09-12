"""Failure taxonomy.

The whole retry/DLQ decision collapses into one question: is this failure
worth trying again? Two exception families answer it.
"""

from __future__ import annotations


class ProcessingError(Exception):
    """Base class for anything that goes wrong while handling an order."""


class TransientError(ProcessingError):
    """A temporary fault -- network blip, downstream service busy, lock timeout.

    Retrying the same message may well succeed, so the consumer backs off and
    tries again before giving up.
    """


class PermanentError(ProcessingError):
    """The message itself is unacceptable -- bad schema, invalid price.

    Retrying can never help, so it goes straight to the Dead Letter Queue.
    """


class RetriesExhausted(ProcessingError):
    """A transient fault that never cleared within the retry budget."""

    def __init__(self, attempts: int, last_error: Exception) -> None:
        super().__init__(f"gave up after {attempts} attempt(s): {last_error}")
        self.attempts = attempts
        self.last_error = last_error
