"""Recursive field redaction for vector store payloads.

Applied between the consumer and the sink so that even if a producer ships PII
or secret fields, they never reach the queryable store.
"""

from __future__ import annotations

from typing import Any


def redact(value: Any, fields: frozenset[str]) -> Any:
    """Return a copy of `value` with any matching keys dropped.

    Walks dicts, lists, and tuples. Scalars are returned as-is. Field matching
    is case-sensitive and applies at every nesting level.
    """
    if not fields:
        return value
    if isinstance(value, dict):
        return {k: redact(v, fields) for k, v in value.items() if k not in fields}
    if isinstance(value, list):
        return [redact(v, fields) for v in value]
    if isinstance(value, tuple):
        return tuple(redact(v, fields) for v in value)
    return value
