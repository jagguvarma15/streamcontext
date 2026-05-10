"""Tests for the MCP search engine.

Pure unit tests with fakes — no Qdrant, no fastmcp, no network.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import pytest
from qdrant_client.http import models as rest

from streamcontext.mcp_search import SearchEngine


class FakeEmbedder:
    dim = 4

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    async def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        return [[1.0, 0.0, 0.0, 0.0] for _ in texts]


@dataclass
class FakeHit:
    score: float
    payload: dict[str, Any]


class CapturingClient:
    """Captures the kwargs passed to `search` and returns canned hits."""

    def __init__(self, hits: list[FakeHit] | None = None) -> None:
        self.last_kwargs: dict[str, Any] | None = None
        self._hits = hits or []
        self.closed = False

    async def search(self, **kwargs: Any) -> list[FakeHit]:
        self.last_kwargs = kwargs
        return list(self._hits)

    async def close(self) -> None:
        self.closed = True


def _hit(topic: str, partition: int, offset: int, ts_ms: int, value: dict, score: float = 0.9):
    return FakeHit(
        score=score,
        payload={
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "timestamp_ms": ts_ms,
            "key": f"k-{offset}",
            "value": value,
        },
    )


@pytest.mark.asyncio
async def test_blank_query_short_circuits() -> None:
    embedder = FakeEmbedder()
    client = CapturingClient()
    engine = SearchEngine(embedder, client, collection="c")
    resp = await engine.search_events(query="   ")
    assert resp.total == 0
    assert resp.results == []
    # Should not have hit the embedder or the client.
    assert embedder.calls == []
    assert client.last_kwargs is None


@pytest.mark.asyncio
async def test_results_are_well_formed() -> None:
    client = CapturingClient(
        hits=[
            _hit("orders", 0, 1, 1_700_000_000_000, {"order_id": "a", "total": 99}),
            _hit("orders", 0, 2, 1_700_000_001_000, {"order_id": "b", "total": 5}, score=0.7),
        ]
    )
    engine = SearchEngine(FakeEmbedder(), client, collection="c")
    resp = await engine.search_events(query="recent orders", limit=5)
    assert resp.total == 2
    assert resp.truncated is False
    r0 = resp.results[0]
    assert r0.coord.topic == "orders"
    assert r0.coord.stable_id == "orders:0:1"
    assert r0.value["order_id"] == "a"
    assert r0.score == pytest.approx(0.9)
    assert r0.key == "k-1"


@pytest.mark.asyncio
async def test_limit_is_clamped_and_truncated_flagged() -> None:
    client = CapturingClient(hits=[])
    engine = SearchEngine(FakeEmbedder(), client, collection="c", max_results=10)
    resp = await engine.search_events(query="anything", limit=50)
    assert resp.truncated is True
    assert client.last_kwargs is not None
    assert client.last_kwargs["limit"] == 10


@pytest.mark.asyncio
async def test_topic_allowlist_filters_query_when_no_explicit_topic() -> None:
    client = CapturingClient(hits=[])
    engine = SearchEngine(
        FakeEmbedder(),
        client,
        collection="c",
        topic_allowlist=frozenset({"orders", "clicks"}),
    )
    await engine.search_events(query="x")
    assert client.last_kwargs is not None
    flt = client.last_kwargs["query_filter"]
    assert isinstance(flt, rest.Filter)
    # One topic clause restricting to the allowlist.
    topic_clauses = [c for c in flt.must if getattr(c, "key", None) == "topic"]
    assert len(topic_clauses) == 1
    match = topic_clauses[0].match
    assert isinstance(match, rest.MatchAny)
    assert sorted(match.any) == ["clicks", "orders"]


@pytest.mark.asyncio
async def test_explicit_topic_outside_allowlist_returns_nothing_safely() -> None:
    """An off-allowlist topic must not leak that the topic exists.

    The engine rewrites the filter to a sentinel that won't match anything.
    Verified by checking the constructed filter, not by depending on Qdrant.
    """
    client = CapturingClient(hits=[])
    engine = SearchEngine(
        FakeEmbedder(),
        client,
        collection="c",
        topic_allowlist=frozenset({"orders"}),
    )
    await engine.search_events(query="x", topic="customers")
    flt = client.last_kwargs["query_filter"]
    topic_clauses = [c for c in flt.must if getattr(c, "key", None) == "topic"]
    assert len(topic_clauses) == 1
    match = topic_clauses[0].match
    assert isinstance(match, rest.MatchValue)
    assert match.value == "__denied__"


@pytest.mark.asyncio
async def test_explicit_topic_inside_allowlist_passes_through() -> None:
    client = CapturingClient(hits=[])
    engine = SearchEngine(
        FakeEmbedder(),
        client,
        collection="c",
        topic_allowlist=frozenset({"orders"}),
    )
    await engine.search_events(query="x", topic="orders")
    flt = client.last_kwargs["query_filter"]
    topic_clauses = [c for c in flt.must if getattr(c, "key", None) == "topic"]
    assert len(topic_clauses) == 1
    match = topic_clauses[0].match
    assert isinstance(match, rest.MatchValue)
    assert match.value == "orders"


@pytest.mark.asyncio
async def test_time_range_translates_to_timestamp_filter() -> None:
    client = CapturingClient(hits=[])
    engine = SearchEngine(FakeEmbedder(), client, collection="c")
    before = int(time.time() * 1000)
    await engine.search_events(query="x", time_range_minutes=60)
    after = int(time.time() * 1000)
    flt = client.last_kwargs["query_filter"]
    ts_clauses = [c for c in flt.must if getattr(c, "key", None) == "timestamp_ms"]
    assert len(ts_clauses) == 1
    rng = ts_clauses[0].range
    assert isinstance(rng, rest.Range)
    # gte is roughly (now - 60min) in ms.
    sixty_min_ms = 60 * 60 * 1000
    assert before - sixty_min_ms - 50 <= rng.gte <= after - sixty_min_ms + 50


@pytest.mark.asyncio
async def test_time_range_is_capped_to_max() -> None:
    client = CapturingClient(hits=[])
    engine = SearchEngine(
        FakeEmbedder(), client, collection="c", max_time_range_minutes=120
    )
    await engine.search_events(query="x", time_range_minutes=10_000)
    flt = client.last_kwargs["query_filter"]
    ts_clauses = [c for c in flt.must if getattr(c, "key", None) == "timestamp_ms"]
    rng = ts_clauses[0].range
    # Should reflect the 120min cap, not the 10_000min request.
    now_ms = int(time.time() * 1000)
    cap_ms = 120 * 60 * 1000
    assert (now_ms - cap_ms) - 200 <= rng.gte <= (now_ms - cap_ms) + 200


@pytest.mark.asyncio
async def test_score_threshold_passed_through() -> None:
    client = CapturingClient(hits=[])
    engine = SearchEngine(FakeEmbedder(), client, collection="c")
    await engine.search_events(query="x", score_threshold=0.5)
    assert client.last_kwargs["score_threshold"] == 0.5


@pytest.mark.asyncio
async def test_no_filter_when_no_constraints() -> None:
    client = CapturingClient(hits=[])
    engine = SearchEngine(FakeEmbedder(), client, collection="c")
    await engine.search_events(query="x")
    assert client.last_kwargs["query_filter"] is None
