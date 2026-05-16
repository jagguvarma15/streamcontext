"""Semantic search and metadata over the streamcontext vector store.

Pure search logic, deliberately decoupled from the MCP transport so it can be
unit-tested with fakes. The MCP server in `mcp_server.py` wraps this engine
and exposes it as tools.
"""

from __future__ import annotations

import json
import time
from typing import Any, Protocol

from qdrant_client.http import models as rest

from streamcontext.errors import StreamcontextError
from streamcontext.logging import get_logger
from streamcontext.mcp_catalog import CatalogReader
from streamcontext.mcp_models import (
    EventCoord,
    EventResult,
    FieldExplanation,
    FilterClause,
    FindTopicsResponse,
    RelationshipInfo,
    RelationshipsResponse,
    SchemaField,
    SchemaSummary,
    SearchResponse,
    TopicDescription,
    TopicInfo,
    TopicsResponse,
)
from streamcontext.sink import stable_uuid

log = get_logger("streamcontext.mcp.search")


# Fields that live at the top level of the Qdrant payload (set by the pipeline).
# Anything else is treated as a value-level field and prefixed with "value.".
_CORE_PAYLOAD_FIELDS: frozenset[str] = frozenset(
    {"topic", "partition", "offset", "timestamp_ms", "key"}
)


def _normalize_field(field: str) -> str:
    """Map a user-facing field name to its Qdrant payload path."""
    if field in _CORE_PAYLOAD_FIELDS:
        return field
    if field.startswith("value.") or "." in field:
        # Caller already passed an explicit path; trust them.
        return field
    return f"value.{field}"


def _clause_to_qdrant(clause: FilterClause) -> rest.FieldCondition:
    key = _normalize_field(clause.field)
    if clause.eq is not None:
        return rest.FieldCondition(key=key, match=rest.MatchValue(value=clause.eq))
    if clause.in_values is not None:
        return rest.FieldCondition(key=key, match=rest.MatchAny(any=list(clause.in_values)))
    if clause.gte is not None or clause.lte is not None:
        return rest.FieldCondition(
            key=key, range=rest.Range(gte=clause.gte, lte=clause.lte)
        )
    raise ValueError(
        f"FilterClause for field {clause.field!r} must set one of eq, in_values, or gte/lte."
    )


class EventNotFoundError(StreamcontextError):
    """Raised when a referenced Kafka coordinate is not present in the store."""


class _EmbedderLike(Protocol):
    dim: int

    async def embed(self, texts: list[str]) -> list[list[float]]: ...


class _QdrantLike(Protocol):
    async def search(
        self,
        collection_name: str,
        query_vector: list[float],
        limit: int,
        query_filter: Any | None = None,
        with_payload: bool = True,
        score_threshold: float | None = None,
    ) -> list[Any]: ...

    async def count(
        self,
        collection_name: str,
        count_filter: Any | None = None,
        exact: bool = False,
    ) -> Any: ...

    async def scroll(
        self,
        collection_name: str,
        scroll_filter: Any | None = None,
        limit: int = 10,
        with_payload: bool = True,
        with_vectors: bool = False,
        order_by: Any | None = None,
    ) -> Any: ...

    async def retrieve(
        self,
        collection_name: str,
        ids: list[Any],
        with_payload: bool = True,
        with_vectors: bool = False,
    ) -> list[Any]: ...

    async def close(self) -> None: ...


class _SchemaRegistryLike(Protocol):
    """Subset of confluent_kafka.schema_registry.SchemaRegistryClient we use."""

    def get_latest_version(self, subject_name: str) -> Any: ...


class SearchEngine:
    """Embeds queries, builds Qdrant filters, returns structured results.

    Enforces server-side caps on `limit` and `time_range_minutes`, and the
    topic allowlist. Inputs that exceed caps are clamped (not rejected) so
    agents get a useful response with `truncated=True`.
    """

    def __init__(
        self,
        embedder: _EmbedderLike,
        client: _QdrantLike,
        collection: str,
        topic_allowlist: frozenset[str] = frozenset(),
        max_results: int = 100,
        max_time_range_minutes: int = 10_080,
        max_value_bytes: int = 8192,
        schema_registry: _SchemaRegistryLike | None = None,
        catalog: CatalogReader | None = None,
    ) -> None:
        self._embedder = embedder
        self._client = client
        self._collection = collection
        self._topic_allowlist = topic_allowlist
        self._max_results = max_results
        self._max_time_range_minutes = max_time_range_minutes
        self._max_value_bytes = max_value_bytes
        self._schema_registry = schema_registry
        self._catalog = catalog

    def _build_filter(
        self,
        topic: str | None,
        time_range_minutes: int | None,
        filters: list[FilterClause] | None = None,
    ) -> rest.Filter | None:
        clauses: list[rest.FieldCondition] = []

        # Topic filtering: an explicit topic argument must be in the allowlist
        # (if one is configured); otherwise restrict to the allowlist itself.
        if topic is not None:
            if self._topic_allowlist and topic not in self._topic_allowlist:
                # Force a no-match filter rather than leaking the fact that the
                # topic exists outside the allowlist.
                clauses.append(
                    rest.FieldCondition(key="topic", match=rest.MatchValue(value="__denied__"))
                )
            else:
                clauses.append(
                    rest.FieldCondition(key="topic", match=rest.MatchValue(value=topic))
                )
        elif self._topic_allowlist:
            clauses.append(
                rest.FieldCondition(
                    key="topic", match=rest.MatchAny(any=sorted(self._topic_allowlist))
                )
            )

        if time_range_minutes is not None:
            clamped = max(1, min(time_range_minutes, self._max_time_range_minutes))
            cutoff_ms = int((time.time() - clamped * 60) * 1000)
            clauses.append(
                rest.FieldCondition(key="timestamp_ms", range=rest.Range(gte=cutoff_ms))
            )

        if filters:
            for clause in filters:
                clauses.append(_clause_to_qdrant(clause))

        if not clauses:
            return None
        return rest.Filter(must=clauses)

    async def search_events(
        self,
        query: str,
        limit: int = 10,
        topic: str | None = None,
        time_range_minutes: int | None = None,
        score_threshold: float | None = None,
        filters: list[FilterClause] | None = None,
        diverse: bool = False,
    ) -> SearchResponse:
        if not query or not query.strip():
            return SearchResponse(query=query, total=0, results=[])

        requested_limit = limit
        clamped_limit = max(1, min(limit, self._max_results))
        truncated = clamped_limit != requested_limit

        [vector] = await self._embedder.embed([query])
        flt = self._build_filter(topic, time_range_minutes, filters=filters)

        # For MMR we pull a larger candidate pool so the rerank has room to
        # work. 3x is the standard rule-of-thumb in the literature.
        fetch_limit = clamped_limit * 3 if diverse else clamped_limit

        kwargs: dict[str, Any] = {
            "collection_name": self._collection,
            "query_vector": vector,
            "limit": fetch_limit,
            "query_filter": flt,
            "with_payload": True,
        }
        if diverse:
            kwargs["with_vectors"] = True
        if score_threshold is not None:
            kwargs["score_threshold"] = score_threshold

        hits = await self._client.search(**kwargs)
        if diverse and hits:
            ordered = _mmr_rerank(query_vector=vector, hits=hits, k=clamped_limit)
            results = [self._apply_value_cap(_hit_to_result(h)) for h in ordered]
        else:
            results = [
                self._apply_value_cap(_hit_to_result(h)) for h in hits[:clamped_limit]
            ]

        log.info(
            "mcp.search_events",
            query_len=len(query),
            limit=clamped_limit,
            topic=topic,
            time_range_minutes=time_range_minutes,
            score_threshold=score_threshold,
            n_filters=len(filters) if filters else 0,
            diverse=diverse,
            n_candidates=len(hits),
            n_results=len(results),
            truncated=truncated,
        )
        return SearchResponse(
            query=query, total=len(results), truncated=truncated, results=results
        )


    def _topic_is_allowed(self, name: str) -> bool:
        return not self._topic_allowlist or name in self._topic_allowlist

    def _apply_value_cap(self, result: EventResult) -> EventResult:
        new_value, truncated = _maybe_truncate_value(result.value, self._max_value_bytes)
        if not truncated:
            return result
        return result.model_copy(update={"value": new_value, "value_truncated": True})

    async def _topic_count(self, name: str) -> int:
        flt = rest.Filter(
            must=[rest.FieldCondition(key="topic", match=rest.MatchValue(value=name))]
        )
        res = await self._client.count(
            collection_name=self._collection, count_filter=flt, exact=False
        )
        return int(getattr(res, "count", 0))

    async def _topic_extreme_ts(self, name: str, *, newest: bool) -> int | None:
        """Use a 1-row ordered scroll to fetch oldest (asc) or newest (desc)."""
        flt = rest.Filter(
            must=[rest.FieldCondition(key="topic", match=rest.MatchValue(value=name))]
        )
        direction = rest.Direction.DESC if newest else rest.Direction.ASC
        try:
            res = await self._client.scroll(
                collection_name=self._collection,
                scroll_filter=flt,
                limit=1,
                with_payload=True,
                with_vectors=False,
                order_by=rest.OrderBy(key="timestamp_ms", direction=direction),
            )
        except Exception as exc:
            # Order-by needs a payload index; if it isn't there yet, log and
            # return None rather than failing the whole tool call.
            log.debug("mcp.topic_extreme_ts.failed", topic=name, newest=newest, error=str(exc))
            return None
        points, _next = res if isinstance(res, tuple) else (getattr(res, "points", []), None)
        if not points:
            return None
        payload = getattr(points[0], "payload", None) or {}
        ts = payload.get("timestamp_ms")
        return int(ts) if ts is not None else None

    async def _discover_topic_names(self, sample_limit: int = 1000) -> list[str]:
        """Best-effort discovery of distinct topic names by scrolling a sample."""
        try:
            res = await self._client.scroll(
                collection_name=self._collection,
                scroll_filter=None,
                limit=sample_limit,
                with_payload=True,
                with_vectors=False,
                order_by=None,
            )
        except Exception as exc:
            log.warning("mcp.topic_discovery.failed", error=str(exc))
            return []
        points, _next = res if isinstance(res, tuple) else (getattr(res, "points", []), None)
        names: set[str] = set()
        for p in points:
            payload = getattr(p, "payload", None) or {}
            t = payload.get("topic")
            if isinstance(t, str) and t:
                names.add(t)
        return sorted(names)

    async def list_topics(self) -> TopicsResponse:
        if self._topic_allowlist:
            names = sorted(self._topic_allowlist)
        else:
            names = await self._discover_topic_names()
        infos: list[TopicInfo] = []
        for name in names:
            count = await self._topic_count(name)
            catalog_entry = (
                self._catalog.get_topic(name) if self._catalog is not None else None
            )
            description = catalog_entry.description if catalog_entry else None
            description_conf = (
                catalog_entry.description_confidence if catalog_entry else None
            )
            if count == 0 and self._topic_allowlist:
                infos.append(
                    TopicInfo(
                        name=name,
                        count=0,
                        description=description,
                        description_confidence=description_conf,
                    )
                )
                continue
            if count == 0 and catalog_entry is None:
                continue
            oldest = await self._topic_extreme_ts(name, newest=False) if count else None
            newest = await self._topic_extreme_ts(name, newest=True) if count else None
            infos.append(
                TopicInfo(
                    name=name,
                    count=count,
                    oldest_timestamp_ms=oldest,
                    newest_timestamp_ms=newest,
                    description=description,
                    description_confidence=description_conf,
                )
            )
        log.info("mcp.list_topics", n=len(infos))
        return TopicsResponse(topics=infos)

    def _fetch_schema(self, topic: str) -> SchemaSummary | None:
        if self._schema_registry is None:
            return None
        subject = f"{topic}-value"
        try:
            latest = self._schema_registry.get_latest_version(subject)
        except Exception as exc:
            log.debug("mcp.schema_fetch.failed", subject=subject, error=str(exc))
            return None
        return _summarize_schema(subject, latest)

    async def _topic_samples(self, topic: str, n: int) -> list[EventResult]:
        flt = rest.Filter(
            must=[rest.FieldCondition(key="topic", match=rest.MatchValue(value=topic))]
        )
        try:
            res = await self._client.scroll(
                collection_name=self._collection,
                scroll_filter=flt,
                limit=n,
                with_payload=True,
                with_vectors=False,
                order_by=rest.OrderBy(key="timestamp_ms", direction=rest.Direction.DESC),
            )
        except Exception:
            res = await self._client.scroll(
                collection_name=self._collection,
                scroll_filter=flt,
                limit=n,
                with_payload=True,
                with_vectors=False,
                order_by=None,
            )
        points, _next = res if isinstance(res, tuple) else (getattr(res, "points", []), None)
        return [
            self._apply_value_cap(_point_to_result(p, fallback_score=0.0)) for p in points
        ]

    async def describe_topic(self, name: str, sample_size: int = 5) -> TopicDescription:
        if not self._topic_is_allowed(name):
            # Don't reveal whether the topic exists.
            return TopicDescription(name=name, count=0)
        count = await self._topic_count(name)
        oldest = await self._topic_extreme_ts(name, newest=False) if count else None
        newest = await self._topic_extreme_ts(name, newest=True) if count else None
        samples = await self._topic_samples(name, sample_size) if count else []
        schema = self._fetch_schema(name)
        catalog_entry = (
            self._catalog.get_topic(name) if self._catalog is not None else None
        )
        if catalog_entry is not None:
            schema = _merge_catalog_into_schema(schema, catalog_entry, subject_fallback=name)
        description = catalog_entry.description if catalog_entry else None
        description_conf = (
            catalog_entry.description_confidence if catalog_entry else None
        )
        inference_status = (
            catalog_entry.inference_status if catalog_entry else None
        )
        log.info(
            "mcp.describe_topic",
            topic=name,
            count=count,
            samples=len(samples),
            schema=bool(schema),
            from_catalog=catalog_entry is not None,
        )
        return TopicDescription(
            name=name,
            count=count,
            oldest_timestamp_ms=oldest,
            newest_timestamp_ms=newest,
            schema_summary=schema,
            samples=samples,
            description=description,
            description_confidence=description_conf,
            inference_status=inference_status,
        )

    async def find_topics_by_purpose(
        self, *, description: str, limit: int = 5
    ) -> FindTopicsResponse:
        clamped = max(1, min(limit, self._max_results))
        if self._catalog is None or not description.strip():
            return FindTopicsResponse(query=description, total=0, matches=[])
        matches = await self._catalog.find_topics_by_purpose(
            embedder=self._embedder,
            description=description,
            limit=clamped,
        )
        log.info(
            "mcp.find_topics_by_purpose",
            query_len=len(description),
            limit=clamped,
            n_matches=len(matches),
        )
        return FindTopicsResponse(
            query=description, total=len(matches), matches=matches
        )

    async def get_topic_relationships(
        self, *, topic: str
    ) -> RelationshipsResponse:
        if self._catalog is None or not self._topic_is_allowed(topic):
            return RelationshipsResponse(topic=topic, total=0, relationships=[])
        rels = self._catalog.get_relationships(topic)
        log.info("mcp.get_topic_relationships", topic=topic, n=len(rels))
        return RelationshipsResponse(topic=topic, total=len(rels), relationships=rels)

    async def explain_field(
        self, *, topic: str, field: str
    ) -> FieldExplanation | None:
        if self._catalog is None or not self._topic_is_allowed(topic):
            return None
        return self._catalog.explain_field(topic=topic, field=field)

    async def find_similar_events(
        self,
        reference_id: str,
        limit: int = 10,
    ) -> SearchResponse:
        coord = _parse_reference_id(reference_id)
        if not self._topic_is_allowed(coord.topic):
            raise EventNotFoundError(reference_id)

        point_uuid = stable_uuid(coord.stable_id)
        retrieved = await self._client.retrieve(
            collection_name=self._collection,
            ids=[point_uuid],
            with_payload=True,
            with_vectors=True,
        )
        if not retrieved:
            raise EventNotFoundError(reference_id)
        ref_point = retrieved[0]
        ref_vector = getattr(ref_point, "vector", None)
        if ref_vector is None:
            raise EventNotFoundError(reference_id)

        clamped_limit = max(1, min(limit, self._max_results))
        truncated = clamped_limit != limit

        # Pull one extra so we can drop the reference itself if it comes back.
        flt = self._build_filter(topic=None, time_range_minutes=None)
        hits = await self._client.search(
            collection_name=self._collection,
            query_vector=list(ref_vector),
            limit=clamped_limit + 1,
            query_filter=flt,
            with_payload=True,
        )
        results: list[EventResult] = []
        for h in hits:
            r = _hit_to_result(h)
            if r.coord.stable_id == coord.stable_id:
                continue
            results.append(self._apply_value_cap(r))
            if len(results) >= clamped_limit:
                break

        log.info(
            "mcp.find_similar_events",
            reference=coord.stable_id,
            n_results=len(results),
            truncated=truncated,
        )
        return SearchResponse(
            query=f"similar:{coord.stable_id}",
            total=len(results),
            truncated=truncated,
            results=results,
        )


def _parse_reference_id(reference_id: str) -> EventCoord:
    """Parse 'topic:partition:offset' into an EventCoord."""
    if not reference_id or reference_id.count(":") < 2:
        raise EventNotFoundError(f"reference_id must be 'topic:partition:offset', got {reference_id!r}")
    topic, part_s, off_s = reference_id.rsplit(":", 2)
    if not topic:
        raise EventNotFoundError(f"empty topic in reference_id {reference_id!r}")
    try:
        partition = int(part_s)
        offset = int(off_s)
    except ValueError as exc:
        raise EventNotFoundError(f"non-integer partition/offset in {reference_id!r}") from exc
    if partition < 0 or offset < 0:
        raise EventNotFoundError(f"negative partition/offset in {reference_id!r}")
    return EventCoord(topic=topic, partition=partition, offset=offset, timestamp_ms=0)


def _merge_catalog_into_schema(
    schema: SchemaSummary | None,
    catalog_entry: Any,
    *,
    subject_fallback: str,
) -> SchemaSummary:
    """Overlay catalog-inferred field annotations onto a SchemaSummary.

    When the registry-derived schema is missing (SR unreachable), the
    catalog's flattened fields are used as the schema instead so callers see
    a coherent shape.
    """
    if schema is None:
        fields = [
            SchemaField(
                name=f.name,
                type=f.type,
                doc=f.doc,
                nullable=f.nullable,
                inferred_meaning=f.inferred_meaning,
                inferred_confidence=f.inferred_confidence,
            )
            for f in catalog_entry.fields
        ]
        return SchemaSummary(
            subject=catalog_entry.schema_subject or f"{subject_fallback}-value",
            version=catalog_entry.schema_version,
            schema_id=catalog_entry.schema_id,
            fields=fields,
        )
    by_name = {f.name: f for f in catalog_entry.fields}
    merged_fields: list[SchemaField] = []
    for f in schema.fields:
        catalog_field = by_name.get(f.name)
        if catalog_field is None:
            merged_fields.append(f)
            continue
        merged_fields.append(
            f.model_copy(
                update={
                    "inferred_meaning": catalog_field.inferred_meaning,
                    "inferred_confidence": catalog_field.inferred_confidence,
                    "nullable": catalog_field.nullable or f.nullable,
                    "doc": f.doc or catalog_field.doc,
                }
            )
        )
    return schema.model_copy(update={"fields": merged_fields})


def _summarize_schema(subject: str, latest: Any) -> SchemaSummary:
    """Best-effort flattening of a confluent SR RegisteredSchema to our model."""
    import json as _json

    raw = getattr(getattr(latest, "schema", None), "schema_str", None)
    fields: list[SchemaField] = []
    if isinstance(raw, str):
        try:
            parsed = _json.loads(raw)
        except _json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            for f in parsed.get("fields", []) or []:
                if not isinstance(f, dict):
                    continue
                fields.append(
                    SchemaField(
                        name=str(f.get("name", "")),
                        type=str(f.get("type", "")),
                        doc=f.get("doc"),
                    )
                )
    return SchemaSummary(
        subject=subject,
        version=getattr(latest, "version", None),
        schema_id=getattr(latest, "schema_id", None),
        fields=fields,
    )


def _point_to_result(point: Any, *, fallback_score: float) -> EventResult:
    """Convert a `Record` (from scroll) to an `EventResult`."""
    payload = getattr(point, "payload", None) or {}
    coord = EventCoord(
        topic=str(payload.get("topic", "")),
        partition=int(payload.get("partition", 0)),
        offset=int(payload.get("offset", 0)),
        timestamp_ms=int(payload.get("timestamp_ms", 0)),
    )
    value = payload.get("value")
    if not isinstance(value, dict):
        value = {}
    key = payload.get("key")
    if key is not None:
        key = str(key)
    return EventResult(coord=coord, score=fallback_score, key=key, value=value)


def _maybe_truncate_value(
    value: dict[str, Any], max_bytes: int
) -> tuple[dict[str, Any], bool]:
    """Return (value_or_stub, was_truncated). Pure; never mutates input."""
    if max_bytes <= 0:
        return value, False
    try:
        raw = json.dumps(value, default=str, ensure_ascii=False)
    except (TypeError, ValueError):
        # Non-serializable contents - keep the dict but flag the size as
        # unknown so the agent knows not to trust raw bytes.
        return value, False
    if len(raw) <= max_bytes:
        return value, False
    preview_chars = max(0, max_bytes - 128)
    return (
        {
            "_truncated": True,
            "_size_bytes": len(raw),
            "_preview": raw[:preview_chars],
        },
        True,
    )


def _cosine(a: list[float], b: list[float]) -> float:
    """Cosine similarity. Assumes inputs are non-empty and same length."""
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
    return dot / ((na**0.5) * (nb**0.5))


def _mmr_rerank(
    query_vector: list[float],
    hits: list[Any],
    k: int,
    lambda_: float = 0.7,
) -> list[Any]:
    """Maximal Marginal Relevance rerank.

    Picks `k` hits balancing relevance to the query against diversity from
    already-selected hits. Falls back to raw order if a hit's vector is
    missing (Qdrant didn't return it). `lambda_` close to 1 favors relevance;
    closer to 0 favors diversity.
    """
    if k <= 0:
        return []
    candidates = list(hits)
    if not candidates:
        return []
    selected: list[Any] = []
    selected_vectors: list[list[float]] = []

    while candidates and len(selected) < k:
        best_idx = -1
        best_score = float("-inf")
        for i, h in enumerate(candidates):
            relevance = float(getattr(h, "score", 0.0))
            vec = getattr(h, "vector", None)
            if vec is None:
                # No vector returned - fall back to relevance-only ranking
                # for this candidate.
                mmr_score = relevance
            else:
                if selected_vectors:
                    max_sim = max(_cosine(vec, sv) for sv in selected_vectors)
                else:
                    max_sim = 0.0
                mmr_score = lambda_ * relevance - (1.0 - lambda_) * max_sim
            if mmr_score > best_score:
                best_score = mmr_score
                best_idx = i
        picked = candidates.pop(best_idx)
        selected.append(picked)
        v = getattr(picked, "vector", None)
        if v is not None:
            selected_vectors.append(list(v))
    return selected


def _hit_to_result(hit: Any) -> EventResult:
    payload = hit.payload or {}
    coord = EventCoord(
        topic=str(payload.get("topic", "")),
        partition=int(payload.get("partition", 0)),
        offset=int(payload.get("offset", 0)),
        timestamp_ms=int(payload.get("timestamp_ms", 0)),
    )
    value = payload.get("value")
    if not isinstance(value, dict):
        value = {}
    key = payload.get("key")
    if key is not None:
        key = str(key)
    return EventResult(coord=coord, score=float(hit.score), key=key, value=value)
