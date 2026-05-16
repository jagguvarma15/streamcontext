"""Catalog read helpers for the MCP server.

The MCP server is read-only against the catalog: it never refreshes entries
itself (that is the refresher process's job). This module wraps `CatalogStore`
with the shaping logic the MCP tools need, plus a couple of small helpers
(topic-description embedding, allowlist gating) that don't belong on the
store.

Keep this layer thin. Anything that touches Kafka or Schema Registry belongs
in the refresher; anything that touches Qdrant belongs in `SearchEngine`.
"""

from __future__ import annotations

import math
from typing import Any, Protocol

from streamcontext.catalog.models import (
    FieldEntry,
    RelationshipEntry,
    SampleMessage,
    TopicEntry,
)
from streamcontext.catalog.store import CatalogStore
from streamcontext.logging import get_logger
from streamcontext.mcp_models import (
    FieldExplanation,
    RelationshipInfo,
    TopicMatch,
)

log = get_logger("streamcontext.mcp.catalog")


class _EmbedderLike(Protocol):
    dim: int

    async def embed(self, texts: list[str]) -> list[list[float]]: ...


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))


def synthesize_description(entry: TopicEntry) -> str:
    """Build a fallback description from topic name + field names.

    Used as the embedding target when the catalog has not produced an
    inferred description yet. Kept short so the embedding stays focused.
    """
    field_summary = ", ".join(f.name for f in entry.fields[:8])
    if field_summary:
        return f"Kafka topic '{entry.name}' with fields: {field_summary}."
    return f"Kafka topic '{entry.name}'."


class CatalogReader:
    """Read-side wrapper around `CatalogStore` for the MCP layer."""

    def __init__(
        self,
        *,
        store: CatalogStore,
        allowlist: frozenset[str] = frozenset(),
    ) -> None:
        self._store = store
        self._allowlist = allowlist

    def topic_is_allowed(self, name: str) -> bool:
        return not self._allowlist or name in self._allowlist

    def list_allowed_topics(self) -> list[TopicEntry]:
        names = self._store.list_topic_names()
        if self._allowlist:
            names = [n for n in names if n in self._allowlist]
        out: list[TopicEntry] = []
        for n in names:
            entry = self._store.get_topic(n)
            if entry is not None:
                out.append(entry)
        return out

    def get_topic(self, name: str) -> TopicEntry | None:
        if not self.topic_is_allowed(name):
            return None
        return self._store.get_topic(name)

    def get_relationships(self, name: str) -> list[RelationshipInfo]:
        if not self.topic_is_allowed(name):
            return []
        rels = self._store.get_relationships(name)
        return [
            RelationshipInfo(
                source_topic=r.source_topic,
                target_topic=r.target_topic,
                relationship_type=r.relationship_type,
                source_field=r.source_field,
                target_field=r.target_field,
                confidence=r.confidence,
                rationale=r.rationale,
            )
            for r in rels
            if self.topic_is_allowed(r.source_topic) and self.topic_is_allowed(r.target_topic)
        ]

    def explain_field(
        self, *, topic: str, field: str, max_examples: int = 5
    ) -> FieldExplanation | None:
        entry = self.get_topic(topic)
        if entry is None:
            return None
        matched: FieldEntry | None = next(
            (f for f in entry.fields if f.name == field), None
        )
        if matched is None:
            return None
        examples = _example_values_for(entry.samples, field, limit=max_examples)
        return FieldExplanation(
            topic=topic,
            field=matched.name,
            type=matched.type,
            nullable=matched.nullable,
            doc=matched.doc,
            inferred_meaning=matched.inferred_meaning,
            inferred_confidence=matched.inferred_confidence,
            example_values=examples,
        )

    async def find_topics_by_purpose(
        self,
        *,
        embedder: _EmbedderLike,
        description: str,
        limit: int = 5,
    ) -> list[TopicMatch]:
        topics = self.list_allowed_topics()
        if not topics or not description.strip():
            return []

        targets: list[tuple[TopicEntry, str, str]] = []
        for entry in topics:
            if entry.description:
                targets.append((entry, entry.description, "inferred"))
            else:
                targets.append((entry, synthesize_description(entry), "synthesized"))

        texts = [description] + [t[1] for t in targets]
        vectors = await embedder.embed(texts)
        if not vectors:
            return []
        query_vec, topic_vecs = vectors[0], vectors[1:]
        scored: list[tuple[float, TopicEntry, str]] = []
        for (entry, _, source), vec in zip(targets, topic_vecs, strict=True):
            scored.append((_cosine(query_vec, vec), entry, source))
        scored.sort(key=lambda triple: triple[0], reverse=True)
        out: list[TopicMatch] = []
        for score, entry, source in scored[: max(1, limit)]:
            out.append(
                TopicMatch(
                    name=entry.name,
                    score=float(score),
                    description=entry.description,
                    description_confidence=entry.description_confidence,
                    description_source=source,
                )
            )
        return out


def _example_values_for(
    samples: list[SampleMessage], field: str, *, limit: int
) -> list[Any]:
    """Pull up to `limit` representative scalar values for `field` from samples."""
    parts = field.split(".")
    out: list[Any] = []
    seen: set[Any] = set()
    for s in samples:
        cursor: Any = s.value
        ok = True
        for part in parts:
            if isinstance(cursor, dict) and part in cursor:
                cursor = cursor[part]
            else:
                ok = False
                break
        if not ok or cursor is None:
            continue
        if isinstance(cursor, (list, dict)):
            continue
        try:
            if cursor in seen:
                continue
            seen.add(cursor)
        except TypeError:
            pass
        out.append(cursor)
        if len(out) >= limit:
            break
    return out


__all__ = ["CatalogReader", "synthesize_description"]
