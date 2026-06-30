"""Privacy controls for the catalog.

This module is the single source of truth for what counts as PII and how it
gets stripped from sampled records. The catalog builder applies redaction
before *anything else* touches a sample — meaning samples land in SQLite
already-redacted, and any LLM prompt built later does not need to redact a
second time. (The inference layer still applies the same patterns
defensively, so a misconfigured builder cannot leak through.)

Two redaction strategies, combined:

  - Field-name redaction. Any key whose name appears in the configured
    `pii_redact_fields` list is dropped at every nesting level. Use this for
    fields you know contain sensitive data: `email`, `phone`, `ssn`,
    `card_number`, etc.

  - Regex redaction. String values that match any compiled pattern are
    replaced with the literal `[redacted]`. Built-in patterns cover the
    common shapes (emails, phone numbers, 13-19 digit card numbers, SSN).
    Operators add more via `pii_redact_patterns`.

Regex patterns operate on *string values only*. They do not recurse into
keys. Numeric or boolean values are passed through unchanged.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

# Built-in patterns covering the obvious shapes. Conservative — false positives
# are preferable to leaking real PII through the catalog.
_DEFAULT_PATTERNS: tuple[str, ...] = (
    r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b",
    r"\b(?:\+?\d{1,3}[\s-]?)?(?:\(?\d{3}\)?[\s-]?)?\d{3}[\s-]?\d{4}\b",
    r"\b\d{13,19}\b",
    r"\b\d{3}-\d{2}-\d{4}\b",
)


REDACTED_TOKEN: str = "[redacted]"


def compile_patterns(extra: Iterable[str] | None = None) -> tuple[re.Pattern[str], ...]:
    """Combine the built-in patterns with operator-supplied ones."""
    compiled: list[re.Pattern[str]] = [re.compile(p, re.IGNORECASE) for p in _DEFAULT_PATTERNS]
    for raw in extra or []:
        if not raw or not raw.strip():
            continue
        compiled.append(re.compile(raw, re.IGNORECASE))
    return tuple(compiled)


def redact_value(
    value: Any,
    *,
    drop_fields: frozenset[str],
    patterns: tuple[re.Pattern[str], ...],
) -> Any:
    """Return a redacted copy of `value`.

    - Drops any dict key whose name is in `drop_fields`, recursively.
    - Replaces any pattern match in string values with `REDACTED_TOKEN`.
    - Walks lists and tuples. Other scalar types are returned unchanged.
    """
    if isinstance(value, dict):
        return {
            k: redact_value(v, drop_fields=drop_fields, patterns=patterns)
            for k, v in value.items()
            if k not in drop_fields
        }
    if isinstance(value, list):
        return [redact_value(v, drop_fields=drop_fields, patterns=patterns) for v in value]
    if isinstance(value, tuple):
        return tuple(
            redact_value(v, drop_fields=drop_fields, patterns=patterns) for v in value
        )
    if isinstance(value, str):
        out = value
        for pat in patterns:
            out = pat.sub(REDACTED_TOKEN, out)
        return out
    return value


__all__ = ["REDACTED_TOKEN", "compile_patterns", "redact_value"]
