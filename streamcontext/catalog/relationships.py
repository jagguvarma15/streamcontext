"""Detect relationships between catalog topics.

Two layers, applied in order:

  1. Heuristic. For each ordered pair of topics, find fields whose name and
     type agree. When the field values overlap across samples, the pair gets a
     `shared_key` relationship; when one side's matching field is also that
     topic's primary identifier, the other side is treated as a
     `foreign_reference`.

  2. LLM polish (optional). For pairs the heuristic did not score, ask the
     configured LLM whether the topics are related given their descriptions
     and schemas. Results are cached on `(fingerprint_a, fingerprint_b)` in
     the same SQLite cache used for description inference, so the same pair
     never costs twice.

The heuristic does most of the work. The LLM exists to catch the cases the
heuristic cannot see — for example `payment_attempts` vs `order_completions`,
which do not share a key but are clearly the same flow.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Iterable

from streamcontext.catalog.inference import InferenceEngine, _safe_json
from streamcontext.catalog.models import (
    FieldEntry,
    RelationshipEntry,
    SampleMessage,
    TopicEntry,
)
from streamcontext.catalog.store import CatalogStore
from streamcontext.logging import get_logger

log = get_logger("streamcontext.catalog.relationships")


# Field-name suffixes that strongly suggest an identifier. Tuned conservatively.
_ID_SUFFIXES: tuple[str, ...] = ("_id", "_uuid", "_key")


def _field_is_identifier(field: FieldEntry) -> bool:
    name = field.name.split(".")[-1].lower()
    if name in {"id", "uuid", "pk"}:
        return True
    return name.endswith(_ID_SUFFIXES)


def _types_compatible(a: str, b: str) -> bool:
    """Loose type compatibility. Same base type ignoring nullability/wrappers."""
    if a == b:
        return True
    # Strip everything inside angle brackets so 'record<X>' compares as 'record'.
    base_a = a.split("<", 1)[0]
    base_b = b.split("<", 1)[0]
    return base_a == base_b


def _extract_values(samples: Iterable[SampleMessage], field_path: str) -> set[Any]:
    """Pull a flat set of values at `field_path` (supporting `parent.child`)."""
    parts = field_path.split(".")
    out: set[Any] = set()
    for s in samples:
        cursor: Any = s.value
        ok = True
        for part in parts:
            if isinstance(cursor, dict) and part in cursor:
                cursor = cursor[part]
            else:
                ok = False
                break
        if not ok:
            continue
        if isinstance(cursor, (list, dict)):
            continue  # only count scalar identifier values
        if cursor is None:
            continue
        try:
            out.add(cursor)
        except TypeError:
            # Unhashable values land here; skip.
            continue
    return out


def _is_primary_identifier(topic: TopicEntry, field_name: str) -> bool:
    """Heuristic: a topic's primary id is a field named like the topic, an
    `id`/`uuid` field, or a field that matches the singularised topic name."""
    if field_name in {"id", "uuid", "pk"}:
        return True
    bare = field_name.split(".")[-1].lower()
    topic_lower = topic.name.lower()
    if bare == f"{topic_lower}_id":
        return True
    if topic_lower.endswith("s") and bare == f"{topic_lower[:-1]}_id":
        return True
    return False


def detect_pair_heuristic(
    source: TopicEntry,
    target: TopicEntry,
    *,
    min_overlap_ratio: float = 0.2,
) -> list[RelationshipEntry]:
    """Run heuristic detection from `source` to `target`.

    Returns at most one entry per matched field. Symmetric `shared_key`
    relationships are emitted; callers may store both directions if they want
    bidirectional lookups.
    """
    if source.name == target.name:
        return []
    results: list[RelationshipEntry] = []
    target_fields = {f.name: f for f in target.fields}
    for source_field in source.fields:
        if not _field_is_identifier(source_field):
            continue
        target_field = target_fields.get(source_field.name)
        if target_field is None:
            continue
        if not _types_compatible(source_field.type, target_field.type):
            continue
        # Compute value overlap between the two samples.
        source_values = _extract_values(source.samples, source_field.name)
        target_values = _extract_values(target.samples, target_field.name)
        if not source_values or not target_values:
            # No samples on at least one side — emit a low-confidence shared_key
            # so the catalog still surfaces the field-name match.
            confidence = 0.4
            overlap = 0.0
        else:
            intersection = source_values & target_values
            denom = min(len(source_values), len(target_values))
            overlap = len(intersection) / denom if denom else 0.0
            if overlap < min_overlap_ratio:
                continue
            confidence = min(0.95, 0.55 + overlap / 2.0)
        rel_type = "shared_key"
        if _is_primary_identifier(target, source_field.name):
            rel_type = "foreign_reference"
        rationale = (
            f"Field '{source_field.name}' matches in both topics with type "
            f"{source_field.type}; sample overlap ratio {overlap:.2f}."
        )
        results.append(
            RelationshipEntry(
                source_topic=source.name,
                target_topic=target.name,
                relationship_type=rel_type,  # type: ignore[arg-type]
                source_field=source_field.name,
                target_field=target_field.name,
                confidence=confidence,
                rationale=rationale,
            )
        )
    return results


def _llm_cache_key(source: TopicEntry, target: TopicEntry) -> str:
    payload = {
        "kind": "relationship",
        "a": {
            "name": source.name,
            "fp": source.schema_fingerprint,
            "desc": source.description,
        },
        "b": {
            "name": target.name,
            "fp": target.schema_fingerprint,
            "desc": target.description,
        },
    }
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return "rel:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


_LLM_SYSTEM_PROMPT = (
    "You are a data catalog assistant. Given two Kafka topics with their "
    "schemas and natural-language descriptions, decide whether they are "
    "related and, if so, classify the relationship. Reply with only the JSON "
    "object the user requests."
)


def _build_llm_prompt(source: TopicEntry, target: TopicEntry) -> str:
    payload = {
        "topic_a": {
            "name": source.name,
            "description": source.description,
            "fields": [{"name": f.name, "type": f.type} for f in source.fields],
        },
        "topic_b": {
            "name": target.name,
            "description": target.description,
            "fields": [{"name": f.name, "type": f.type} for f in target.fields],
        },
    }
    instruction = (
        "Decide whether topic_a and topic_b are semantically related. Reply "
        "with a JSON object: {related: bool, relationship_type: one of "
        "['shared_key','foreign_reference','event_chain','semantic'], "
        "confidence: number in [0,1], rationale: one short sentence}. If "
        "they are unrelated, set related=false and confidence to a value <= "
        "0.3.\n\n"
    )
    return instruction + json.dumps(payload, ensure_ascii=False)


class RelationshipDetector:
    """Combines heuristic detection with an optional LLM polish step.

    Pass `inference=None` to disable the LLM layer entirely (heuristics still
    run). The detector reads from and writes to the catalog store directly.
    """

    def __init__(
        self,
        *,
        store: CatalogStore,
        inference: InferenceEngine | None = None,
        min_overlap_ratio: float = 0.2,
        llm_confidence_threshold: float = 0.6,
    ) -> None:
        self._store = store
        self._inference = inference
        self._min_overlap = min_overlap_ratio
        self._llm_threshold = llm_confidence_threshold

    async def detect_pair(
        self,
        source: TopicEntry,
        target: TopicEntry,
        *,
        already_related: bool,
    ) -> list[RelationshipEntry]:
        relationships = detect_pair_heuristic(
            source, target, min_overlap_ratio=self._min_overlap
        )
        if relationships:
            return relationships

        if already_related:
            return []

        llm_relationship = await self._maybe_llm_relationship(source, target)
        if llm_relationship is None:
            return []
        return [llm_relationship]

    async def detect_all(
        self, topics: list[TopicEntry]
    ) -> dict[str, list[RelationshipEntry]]:
        now_ms = int(time.time() * 1000)
        by_source: dict[str, list[RelationshipEntry]] = {t.name: [] for t in topics}
        # Track which unordered pairs already produced a relationship so the
        # LLM is only asked once per pair (and never asked when the heuristic
        # already produced output for either direction).
        heuristic_pairs: set[tuple[str, str]] = set()

        for source in topics:
            for target in topics:
                if source.name == target.name:
                    continue
                rels = detect_pair_heuristic(
                    source, target, min_overlap_ratio=self._min_overlap
                )
                for r in rels:
                    r.last_refresh_ms = now_ms
                    by_source[source.name].append(r)
                if rels:
                    key = tuple(sorted((source.name, target.name)))
                    heuristic_pairs.add(key)

        if self._inference is not None and self._inference.enabled:
            seen_pairs: set[tuple[str, str]] = set()
            for source in topics:
                for target in topics:
                    if source.name == target.name:
                        continue
                    key = tuple(sorted((source.name, target.name)))
                    if key in heuristic_pairs or key in seen_pairs:
                        continue
                    seen_pairs.add(key)
                    rel = await self._maybe_llm_relationship(source, target)
                    if rel is None:
                        continue
                    rel.last_refresh_ms = now_ms
                    by_source[rel.source_topic].append(rel)
        return by_source

    async def refresh_all(self, topic_names: list[str] | None = None) -> int:
        """Detect relationships across the catalog and persist them.

        Returns the total number of relationships written.
        """
        names = topic_names or self._store.list_topic_names()
        topics: list[TopicEntry] = []
        for name in names:
            entry = self._store.get_topic(name)
            if entry is not None:
                topics.append(entry)
        by_source = await self.detect_all(topics)
        total = 0
        for source_name, rels in by_source.items():
            self._store.replace_relationships(source_name, rels)
            total += len(rels)
        log.info("catalog.relationships.refreshed", topics=len(topics), total=total)
        return total

    async def _maybe_llm_relationship(
        self, source: TopicEntry, target: TopicEntry
    ) -> RelationshipEntry | None:
        if self._inference is None or not self._inference.enabled:
            return None
        cache_key = _llm_cache_key(source, target)
        cached = self._store.get_inference_cache(cache_key)
        text: str | None = None
        cost: float = 0.0
        if cached is not None:
            text = cached.get("text") if isinstance(cached, dict) else None
        if text is None:
            spent = self._store.get_spend_today(self._inference._provider.name)  # type: ignore[union-attr]
            ceiling = self._inference._config.daily_llm_spend_ceiling_usd  # type: ignore[union-attr]
            if spent >= ceiling:
                log.warning(
                    "catalog.relationships.ceiling_exceeded",
                    a=source.name,
                    b=target.name,
                    spent_usd=round(spent, 4),
                    ceiling_usd=ceiling,
                )
                return None
            prompt = _build_llm_prompt(source, target)
            try:
                text, cost = await self._inference._provider.complete(  # type: ignore[union-attr]
                    system=_LLM_SYSTEM_PROMPT,
                    prompt=prompt,
                    max_output_tokens=200,
                )
            except Exception as exc:
                log.warning(
                    "catalog.relationships.llm_failed",
                    a=source.name,
                    b=target.name,
                    error=str(exc),
                )
                return None
            self._store.record_spend(self._inference._provider.name, cost)  # type: ignore[union-attr]
            self._store.put_inference_cache(cache_key, {"text": text, "cost": cost})

        try:
            parsed = _safe_json(text)
        except ValueError:
            log.warning("catalog.relationships.parse_failed", preview=text[:160])
            return None

        if not parsed.get("related"):
            return None
        confidence = float(parsed.get("confidence") or 0.0)
        if confidence < self._llm_threshold:
            return None
        rtype = parsed.get("relationship_type") or "semantic"
        if rtype not in ("shared_key", "foreign_reference", "event_chain", "semantic"):
            rtype = "semantic"
        return RelationshipEntry(
            source_topic=source.name,
            target_topic=target.name,
            relationship_type=rtype,  # type: ignore[arg-type]
            source_field=None,
            target_field=None,
            confidence=min(1.0, max(0.0, confidence)),
            rationale=str(parsed.get("rationale") or "")[:240] or None,
        )


__all__ = ["RelationshipDetector", "detect_pair_heuristic"]
