"""Exception types raised by streamcontext components."""

from __future__ import annotations


class StreamcontextError(Exception):
    """Base class for all streamcontext-raised errors."""


class ConfigurationError(StreamcontextError):
    """Raised at startup when settings are inconsistent or unusable."""


class PipelineFatalError(StreamcontextError):
    """Raised when the pipeline cannot make forward progress and must stop.

    Examples: a batch failing every retry against the embedder or sink.
    Catching this at the top level should trigger a clean shutdown so an
    operator can investigate without silent data loss.
    """
