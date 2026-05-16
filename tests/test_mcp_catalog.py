"""Tests for the catalog-backed MCP tools.

Exercises the SearchEngine + CatalogReader integration end-to-end with the
real SQLite-backed store. Embedder and Qdrant client are fakes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from streamcontext.catalog.models import (
    FieldEntry,
    RelationshipEntry,
    SampleMessage,
    TopicEntry,
)
from streamcontext.catalog.store import CatalogStore
from streamcontext.mcp_catalog import CatalogReader, synthesize_description
from streamcontext.mcp_search import SearchEngine


class FakeEmbedder:
    dim = 3

    def __init__(self) -> None:
        self._vectors = {
            "billing data": [1.0, 0.0, 0.0],
            # Topic descriptions:
            "Customer billing and payment records.": [0.95, 0.05, 0.05],
            "Click events from the web frontend.": [0.05, 0.95, 0.05],
            "Kafka topic 'unknown_topic' with fields: a, b.": [0.05, 0.05, 0.95],
        }

    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._vectors.get(t, [0.5, 0.5, 0.5]) for t in texts]


@dataclass
class _Count:
    count: int


@dataclass
class _Point:
    payload: dict[str, Any]


class FakeQdrant:
    def __init__(self, *, counts: dict[str, int] | None = None) -> None:
        self.counts = counts or {}
        self.scroll_calls: list[dict[str, Any]] = []

    async def count(self, *, collection_name: str, count_filter, exact: bool = False):
        # Read topic name out of the filter.
        topic = "?"
        try:
            cond = count_filter.must[0]
            topic = cond.match.value
        except Exception:
            pass
        return _Count(count=self.counts.get(topic, 0))

    async def scroll(self, **kwargs):
        self.scroll_calls.append(kwargs)
        return ([], None)

    async def search(self, **kwargs):
        return []

    async def retrieve(self, **kwargs):
        return []

    async def close(self) -> None:
        return None


def _seed_catalog(tmp_path):
    store = CatalogStore(tmp_path / "catalog.sqlite")
    billing = TopicEntry(
        name="billing_events",
        schema_subject="billing_events-value",
        schema_id=1,
        schema_version=1,
        schema_fingerprint="fp-billing",
        description="Customer billing and payment records.",
        description_confidence=0.9,
        inference_status="inferred",
    )
    clicks = TopicEntry(
        name="clicks",
        schema_fingerprint="fp-clicks",
        description="Click events from the web frontend.",
        description_confidence=0.7,
        inference_status="inferred",
    )
    store.upsert_topic(billing)
    store.replace_fields(
        "billing_events",
        [
            FieldEntry(
                name="customer_id",
                type="string",
                doc="primary key",
                inferred_meaning="opaque identifier for the customer",
                inferred_confidence=0.92,
            ),
            FieldEntry(
                name="total_cents",
                type="long",
                inferred_meaning="invoice total in USD cents",
                inferred_confidence=0.85,
            ),
        ],
    )
    store.replace_samples(
        "billing_events",
        [
            SampleMessage(
                partition=0,
                offset=1,
                timestamp_ms=1_700_000_000_000,
                key="c1",
                value={"customer_id": "c1", "total_cents": 1500},
            ),
            SampleMessage(
                partition=0,
                offset=2,
                timestamp_ms=1_700_000_000_001,
                key="c2",
                value={"customer_id": "c2", "total_cents": 9999},
            ),
        ],
    )
    store.upsert_topic(clicks)
    store.replace_fields(
        "clicks",
        [
            FieldEntry(name="customer_id", type="string"),
            FieldEntry(name="url", type="string"),
        ],
    )
    store.replace_relationships(
        "billing_events",
        [
            RelationshipEntry(
                source_topic="billing_events",
                target_topic="customers",
                relationship_type="foreign_reference",
                source_field="customer_id",
                target_field="customer_id",
                confidence=0.9,
                rationale="shared identifier in both samples",
            )
        ],
    )
    return store


@pytest.mark.asyncio
async def test_list_topics_includes_catalog_description(tmp_path):
    store = _seed_catalog(tmp_path)
    reader = CatalogReader(store=store)
    qdrant = FakeQdrant(counts={"billing_events": 10, "clicks": 0})
    engine = SearchEngine(
        embedder=FakeEmbedder(),
        client=qdrant,
        collection="streamcontext",
        topic_allowlist=frozenset({"billing_events", "clicks"}),
        catalog=reader,
    )
    response = await engine.list_topics()
    names = [t.name for t in response.topics]
    assert "billing_events" in names
    billing = next(t for t in response.topics if t.name == "billing_events")
    assert billing.description == "Customer billing and payment records."
    assert billing.description_confidence == pytest.approx(0.9)


@pytest.mark.asyncio
async def test_describe_topic_merges_inferred_field_meanings(tmp_path):
    store = _seed_catalog(tmp_path)
    reader = CatalogReader(store=store)
    qdrant = FakeQdrant(counts={"billing_events": 2})
    engine = SearchEngine(
        embedder=FakeEmbedder(),
        client=qdrant,
        collection="streamcontext",
        topic_allowlist=frozenset({"billing_events"}),
        catalog=reader,
    )
    desc = await engine.describe_topic(name="billing_events", sample_size=0)
    assert desc.description == "Customer billing and payment records."
    assert desc.inference_status == "inferred"
    assert desc.schema_summary is not None
    fields_by_name = {f.name: f for f in desc.schema_summary.fields}
    assert fields_by_name["customer_id"].inferred_meaning.startswith("opaque")
    assert fields_by_name["customer_id"].inferred_confidence == pytest.approx(0.92)
    assert fields_by_name["total_cents"].inferred_meaning.startswith("invoice")


@pytest.mark.asyncio
async def test_find_topics_by_purpose_ranks_by_cosine(tmp_path):
    store = _seed_catalog(tmp_path)
    reader = CatalogReader(store=store)
    qdrant = FakeQdrant()
    engine = SearchEngine(
        embedder=FakeEmbedder(),
        client=qdrant,
        collection="streamcontext",
        topic_allowlist=frozenset({"billing_events", "clicks"}),
        catalog=reader,
    )
    response = await engine.find_topics_by_purpose(description="billing data", limit=2)
    assert response.total == 2
    assert response.matches[0].name == "billing_events"
    assert response.matches[0].score > response.matches[1].score
    assert response.matches[0].description_source == "inferred"


@pytest.mark.asyncio
async def test_find_topics_by_purpose_synthesizes_when_no_description(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    store.upsert_topic(TopicEntry(name="unknown_topic", schema_fingerprint="fp"))
    store.replace_fields(
        "unknown_topic",
        [FieldEntry(name="a", type="string"), FieldEntry(name="b", type="string")],
    )
    reader = CatalogReader(store=store)
    engine = SearchEngine(
        embedder=FakeEmbedder(),
        client=FakeQdrant(),
        collection="streamcontext",
        catalog=reader,
    )
    response = await engine.find_topics_by_purpose(description="billing data", limit=5)
    assert response.matches[0].description_source == "synthesized"


@pytest.mark.asyncio
async def test_get_topic_relationships(tmp_path):
    store = _seed_catalog(tmp_path)
    reader = CatalogReader(store=store)
    engine = SearchEngine(
        embedder=FakeEmbedder(),
        client=FakeQdrant(),
        collection="streamcontext",
        topic_allowlist=frozenset({"billing_events", "customers"}),
        catalog=reader,
    )
    response = await engine.get_topic_relationships(topic="billing_events")
    assert response.total == 1
    rel = response.relationships[0]
    assert rel.target_topic == "customers"
    assert rel.relationship_type == "foreign_reference"


@pytest.mark.asyncio
async def test_get_topic_relationships_hides_when_outside_allowlist(tmp_path):
    store = _seed_catalog(tmp_path)
    # Allowlist excludes 'customers'; relationship to it must be filtered out.
    reader = CatalogReader(store=store, allowlist=frozenset({"billing_events"}))
    engine = SearchEngine(
        embedder=FakeEmbedder(),
        client=FakeQdrant(),
        collection="streamcontext",
        topic_allowlist=frozenset({"billing_events"}),
        catalog=reader,
    )
    response = await engine.get_topic_relationships(topic="billing_events")
    assert response.total == 0


@pytest.mark.asyncio
async def test_explain_field_returns_meaning_and_examples(tmp_path):
    store = _seed_catalog(tmp_path)
    reader = CatalogReader(store=store)
    engine = SearchEngine(
        embedder=FakeEmbedder(),
        client=FakeQdrant(),
        collection="streamcontext",
        topic_allowlist=frozenset({"billing_events"}),
        catalog=reader,
    )
    result = await engine.explain_field(topic="billing_events", field="customer_id")
    assert result is not None
    assert result.inferred_meaning.startswith("opaque")
    assert result.inferred_confidence == pytest.approx(0.92)
    assert set(result.example_values) == {"c1", "c2"}


@pytest.mark.asyncio
async def test_explain_field_missing_field_returns_none(tmp_path):
    store = _seed_catalog(tmp_path)
    reader = CatalogReader(store=store)
    engine = SearchEngine(
        embedder=FakeEmbedder(),
        client=FakeQdrant(),
        collection="streamcontext",
        topic_allowlist=frozenset({"billing_events"}),
        catalog=reader,
    )
    result = await engine.explain_field(topic="billing_events", field="nope")
    assert result is None


@pytest.mark.asyncio
async def test_catalog_tools_no_op_when_catalog_absent(tmp_path):
    engine = SearchEngine(
        embedder=FakeEmbedder(),
        client=FakeQdrant(),
        collection="streamcontext",
        catalog=None,
    )
    response = await engine.find_topics_by_purpose(description="anything", limit=3)
    assert response.total == 0
    rels = await engine.get_topic_relationships(topic="billing_events")
    assert rels.total == 0
    field = await engine.explain_field(topic="billing_events", field="customer_id")
    assert field is None


def test_synthesize_description_includes_field_names():
    entry = TopicEntry(
        name="orders",
        fields=[FieldEntry(name="id", type="string"), FieldEntry(name="total", type="double")],
    )
    text = synthesize_description(entry)
    assert "orders" in text
    assert "id" in text
    assert "total" in text
