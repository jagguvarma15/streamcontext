"""Semantic search over the streamcontext vector store.

Pure search logic, deliberately decoupled from the MCP transport so it can be
unit-tested with fakes. The MCP server in `mcp_server.py` wraps this engine
and exposes it as tools.
"""

from __future__ import annotations

import time
from typing import Any, Protocol

from qdrant_client.http import models as rest

from streamcontext.logging import get_logger
from streamcontext.mcp_models import EventCoord, EventResult, SearchResponse

log = get_logger("streamcontext.mcp.search")


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
    ) -> list[Any]: ...

    async def close(self) -> None: ...


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
    ) -> None:
        self._embedder = embedder
        self._client = client
        self._collection = collection
        self._topic_allowlist = topic_allowlist
        self._max_results = max_results
        self._max_time_range_minutes = max_time_range_minutes

    def _build_filter(
        self, topic: str | None, time_range_minutes: int | None
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
    ) -> SearchResponse:
        if not query or not query.strip():
            return SearchResponse(query=query, total=0, results=[])

        requested_limit = limit
        clamped_limit = max(1, min(limit, self._max_results))
        truncated = clamped_limit != requested_limit

        [vector] = await self._embedder.embed([query])
        flt = self._build_filter(topic, time_range_minutes)

        kwargs: dict[str, Any] = {
            "collection_name": self._collection,
            "query_vector": vector,
            "limit": clamped_limit,
            "query_filter": flt,
            "with_payload": True,
        }
        if score_threshold is not None:
            kwargs["score_threshold"] = score_threshold

        hits = await self._client.search(**kwargs)
        results = [_hit_to_result(h) for h in hits]

        log.info(
            "mcp.search_events",
            query_len=len(query),
            limit=clamped_limit,
            topic=topic,
            time_range_minutes=time_range_minutes,
            score_threshold=score_threshold,
            n_results=len(results),
            truncated=truncated,
        )
        return SearchResponse(
            query=query, total=len(results), truncated=truncated, results=results
        )


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
