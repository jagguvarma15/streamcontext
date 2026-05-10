"""Tests for the MCP search engine.

Pure unit tests with fakes — no Qdrant, no fastmcp, no network.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import pytest
from qdrant_client.http import models as rest

from streamcontext.mcp_search import (
    EventNotFoundError,
    SearchEngine,
    _parse_reference_id,
)
from streamcontext.sink import stable_uuid


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
        # Optional behaviors used by metadata-tool tests.
        self.count_results: dict[str, int] = {}
        self.scroll_results: list[FakeHit] = []
        self.scroll_by_topic: dict[str, list[FakeHit]] = {}
        self.scroll_calls: list[dict[str, Any]] = []
        self.retrieve_results: list[Any] = []
        self.search_history: list[dict[str, Any]] = []

    async def search(self, **kwargs: Any) -> list[FakeHit]:
        self.last_kwargs = kwargs
        self.search_history.append(kwargs)
        return list(self._hits)

    async def count(self, collection_name: str, count_filter=None, exact: bool = False):
        # If a topic filter is present, return the configured count for it.
        topic = None
        if count_filter is not None:
            for clause in getattr(count_filter, "must", []) or []:
                if getattr(clause, "key", None) == "topic":
                    match = getattr(clause, "match", None)
                    if match is not None:
                        topic = getattr(match, "value", None)
        n = self.count_results.get(topic, 0) if topic else sum(self.count_results.values())

        class _R:
            count = n

        return _R()

    async def scroll(
        self,
        collection_name: str,
        scroll_filter=None,
        limit: int = 10,
        with_payload: bool = True,
        with_vectors: bool = False,
        order_by=None,
    ):
        self.scroll_calls.append(
            {
                "filter": scroll_filter,
                "limit": limit,
                "order_by": order_by,
            }
        )
        topic = None
        if scroll_filter is not None:
            for clause in getattr(scroll_filter, "must", []) or []:
                if getattr(clause, "key", None) == "topic":
                    match = getattr(clause, "match", None)
                    if match is not None:
                        topic = getattr(match, "value", None)
        if topic and topic in self.scroll_by_topic:
            points = list(self.scroll_by_topic[topic])
        else:
            points = list(self.scroll_results)
        return (points[:limit], None)

    async def retrieve(
        self,
        collection_name: str,
        ids,
        with_payload: bool = True,
        with_vectors: bool = False,
    ):
        return list(self.retrieve_results)

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


# ---------- list_topics / describe_topic / find_similar_events ----------


@dataclass
class FakePoint:
    payload: dict[str, Any]
    vector: list[float] | None = None


def test_parse_reference_id_round_trip() -> None:
    coord = _parse_reference_id("orders:0:42")
    assert coord.topic == "orders"
    assert coord.partition == 0
    assert coord.offset == 42
    # Topic names with hyphens are common.
    assert _parse_reference_id("payments-v2:7:99").topic == "payments-v2"


def test_parse_reference_id_rejects_garbage() -> None:
    with pytest.raises(EventNotFoundError):
        _parse_reference_id("not-a-ref")
    with pytest.raises(EventNotFoundError):
        _parse_reference_id("orders:abc:1")
    with pytest.raises(EventNotFoundError):
        _parse_reference_id("orders:-1:0")
    with pytest.raises(EventNotFoundError):
        _parse_reference_id(":0:1")


@pytest.mark.asyncio
async def test_list_topics_uses_allowlist_when_set() -> None:
    client = CapturingClient()
    client.count_results = {"orders": 12, "clicks": 0}
    client.scroll_by_topic = {
        "orders": [
            FakePoint(payload={"timestamp_ms": 100}),
            FakePoint(payload={"timestamp_ms": 999}),
        ]
    }
    engine = SearchEngine(
        FakeEmbedder(),
        client,
        collection="c",
        topic_allowlist=frozenset({"orders", "clicks"}),
    )
    resp = await engine.list_topics()
    names = {t.name: t for t in resp.topics}
    assert set(names) == {"orders", "clicks"}
    assert names["orders"].count == 12
    # Empty allowlisted topics surface with count=0 so the agent knows they exist.
    assert names["clicks"].count == 0
    assert names["clicks"].oldest_timestamp_ms is None


@pytest.mark.asyncio
async def test_list_topics_discovers_when_no_allowlist() -> None:
    client = CapturingClient()
    client.scroll_results = [
        FakePoint(payload={"topic": "orders"}),
        FakePoint(payload={"topic": "orders"}),
        FakePoint(payload={"topic": "clicks"}),
    ]
    client.count_results = {"orders": 2, "clicks": 1}
    client.scroll_by_topic = {
        "orders": [FakePoint(payload={"timestamp_ms": 1})],
        "clicks": [FakePoint(payload={"timestamp_ms": 2})],
    }
    engine = SearchEngine(FakeEmbedder(), client, collection="c")
    resp = await engine.list_topics()
    names = sorted(t.name for t in resp.topics)
    assert names == ["clicks", "orders"]


@pytest.mark.asyncio
async def test_describe_topic_off_allowlist_returns_empty_without_leak() -> None:
    client = CapturingClient()
    # If the engine ever called count() with the off-allowlist topic, the
    # response would still be 0; but we want the engine to short-circuit
    # without even touching the client. Verify by checking call counts.
    client.count_results = {"customers": 999}  # would otherwise leak
    engine = SearchEngine(
        FakeEmbedder(),
        client,
        collection="c",
        topic_allowlist=frozenset({"orders"}),
    )
    desc = await engine.describe_topic(name="customers")
    assert desc.count == 0
    assert desc.samples == []
    assert desc.schema_summary is None


@pytest.mark.asyncio
async def test_describe_topic_returns_samples_and_window() -> None:
    client = CapturingClient()
    client.count_results = {"orders": 5}
    client.scroll_by_topic = {
        "orders": [
            FakePoint(
                payload={
                    "topic": "orders",
                    "partition": 0,
                    "offset": 1,
                    "timestamp_ms": 1000,
                    "value": {"id": "a"},
                }
            ),
            FakePoint(
                payload={
                    "topic": "orders",
                    "partition": 0,
                    "offset": 2,
                    "timestamp_ms": 2000,
                    "value": {"id": "b"},
                }
            ),
        ]
    }
    engine = SearchEngine(FakeEmbedder(), client, collection="c")
    desc = await engine.describe_topic(name="orders", sample_size=2)
    assert desc.count == 5
    assert len(desc.samples) == 2
    assert desc.samples[0].coord.topic == "orders"
    assert desc.samples[0].value == {"id": "a"}


@pytest.mark.asyncio
async def test_describe_topic_uses_schema_registry_when_provided() -> None:
    """Schema flattening: pretend SR returns a RegisteredSchema with an avsc."""

    class FakeRegisteredSchema:
        version = 3
        schema_id = 42

        class _S:
            schema_str = (
                '{"type":"record","name":"Order","fields":['
                '{"name":"order_id","type":"string","doc":"UUID"},'
                '{"name":"total","type":"double"}]}'
            )

        schema = _S()

    class FakeSR:
        def get_latest_version(self, subject_name: str):
            assert subject_name == "orders-value"
            return FakeRegisteredSchema()

    client = CapturingClient()
    client.count_results = {"orders": 1}
    client.scroll_by_topic = {"orders": [FakePoint(payload={"timestamp_ms": 1, "topic": "orders"})]}
    engine = SearchEngine(
        FakeEmbedder(), client, collection="c", schema_registry=FakeSR()
    )
    desc = await engine.describe_topic(name="orders", sample_size=1)
    assert desc.schema_summary is not None
    assert desc.schema_summary.subject == "orders-value"
    assert desc.schema_summary.version == 3
    assert {f.name for f in desc.schema_summary.fields} == {"order_id", "total"}


@pytest.mark.asyncio
async def test_find_similar_events_reuses_reference_vector_and_excludes_self() -> None:
    ref_uuid = stable_uuid("orders:0:1")
    client = CapturingClient()
    # retrieve returns the reference point with its vector
    client.retrieve_results = [
        FakePoint(
            payload={
                "topic": "orders",
                "partition": 0,
                "offset": 1,
                "timestamp_ms": 100,
                "value": {"id": "a"},
            },
            vector=[0.1, 0.2, 0.3, 0.4],
        )
    ]
    # search returns three hits including the reference itself
    client._hits = [
        _hit("orders", 0, 1, 100, {"id": "a"}, score=0.99),  # reference, must drop
        _hit("orders", 0, 2, 200, {"id": "b"}, score=0.95),
        _hit("orders", 0, 3, 300, {"id": "c"}, score=0.90),
    ]
    engine = SearchEngine(FakeEmbedder(), client, collection="c")
    resp = await engine.find_similar_events(reference_id="orders:0:1", limit=2)

    # The reference must be excluded; we should get the next two.
    coords = [r.coord.stable_id for r in resp.results]
    assert coords == ["orders:0:2", "orders:0:3"]
    # And the vector used was the one from retrieve, not a fresh embedding.
    assert client.search_history[-1]["query_vector"] == [0.1, 0.2, 0.3, 0.4]
    # Engine asked for limit+1 to leave room to drop the self-hit.
    assert client.search_history[-1]["limit"] == 3
    # Note the UUID we retrieved by:
    assert ref_uuid is not None  # sanity — only used to confirm the helper imports


@pytest.mark.asyncio
async def test_find_similar_events_raises_when_reference_missing() -> None:
    client = CapturingClient()
    client.retrieve_results = []  # simulates "point not found"
    engine = SearchEngine(FakeEmbedder(), client, collection="c")
    with pytest.raises(EventNotFoundError):
        await engine.find_similar_events(reference_id="orders:0:1")


@pytest.mark.asyncio
async def test_find_similar_events_off_allowlist_is_not_found() -> None:
    """Querying a similar-to on a topic the agent isn't allowed to see must
    look identical to the case where the reference doesn't exist."""
    client = CapturingClient()
    # retrieve_results is non-empty but we should never get there.
    client.retrieve_results = [FakePoint(payload={"topic": "secrets"}, vector=[0.0])]
    engine = SearchEngine(
        FakeEmbedder(),
        client,
        collection="c",
        topic_allowlist=frozenset({"orders"}),
    )
    with pytest.raises(EventNotFoundError):
        await engine.find_similar_events(reference_id="secrets:0:1")
