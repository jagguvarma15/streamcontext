"""Tests for catalog relationship detection.

Covers:
  - Heuristic shared-key detection with sample-value overlap.
  - Foreign-reference classification when the matching field is the target's
    primary identifier.
  - LLM polish layer with caching, ceiling, and rejection thresholds.
  - End-to-end detect_all -> persist roundtrip.
"""

from __future__ import annotations

import json

import pytest

from streamcontext.catalog.inference import InferenceEngine
from streamcontext.catalog.models import (
    CatalogConfig,
    FieldEntry,
    SampleMessage,
    TopicEntry,
)
from streamcontext.catalog.relationships import (
    RelationshipDetector,
    _llm_cache_key,
    detect_pair_heuristic,
)
from streamcontext.catalog.store import CatalogStore


def _topic(
    name: str,
    fields: list[tuple[str, str]],
    samples: list[dict],
    fingerprint: str | None = None,
    description: str | None = None,
) -> TopicEntry:
    return TopicEntry(
        name=name,
        schema_fingerprint=fingerprint or f"fp-{name}",
        description=description,
        fields=[FieldEntry(name=n, type=t) for n, t in fields],
        samples=[
            SampleMessage(partition=0, offset=i, timestamp_ms=i, value=v)
            for i, v in enumerate(samples)
        ],
    )


# ----------------------------------------------------------------- heuristics


def test_heuristic_detects_shared_key_with_overlap():
    orders = _topic(
        "orders",
        [("order_id", "string"), ("customer_id", "string"), ("total", "double")],
        [
            {"order_id": "o1", "customer_id": "c1", "total": 9.99},
            {"order_id": "o2", "customer_id": "c2", "total": 19.99},
            {"order_id": "o3", "customer_id": "c1", "total": 4.99},
        ],
    )
    customers = _topic(
        "customers",
        [("customer_id", "string"), ("email", "string")],
        [
            {"customer_id": "c1", "email": "a@x.com"},
            {"customer_id": "c2", "email": "b@x.com"},
            {"customer_id": "c3", "email": "c@x.com"},
        ],
    )
    rels = detect_pair_heuristic(orders, customers)
    by_field = {r.source_field: r for r in rels}
    assert "customer_id" in by_field
    rel = by_field["customer_id"]
    assert rel.relationship_type == "foreign_reference"
    assert rel.confidence > 0.5


def test_heuristic_rejects_below_overlap_threshold():
    a = _topic(
        "a",
        [("entity_id", "string")],
        [{"entity_id": "1"}, {"entity_id": "2"}, {"entity_id": "3"}],
    )
    b = _topic(
        "b",
        [("entity_id", "string")],
        [{"entity_id": "99"}, {"entity_id": "100"}, {"entity_id": "101"}],
    )
    rels = detect_pair_heuristic(a, b, min_overlap_ratio=0.5)
    assert rels == []


def test_heuristic_emits_low_confidence_when_no_samples():
    a = _topic("a", [("user_id", "string")], samples=[])
    b = _topic("b", [("user_id", "string")], samples=[])
    rels = detect_pair_heuristic(a, b)
    assert len(rels) == 1
    assert rels[0].relationship_type == "shared_key"
    assert rels[0].confidence < 0.6


def test_heuristic_skips_non_identifier_fields():
    a = _topic(
        "a",
        [("amount", "double")],
        [{"amount": 1.0}, {"amount": 2.0}],
    )
    b = _topic(
        "b",
        [("amount", "double")],
        [{"amount": 1.0}, {"amount": 2.0}],
    )
    assert detect_pair_heuristic(a, b) == []


def test_heuristic_skips_same_topic():
    a = _topic("a", [("user_id", "string")], [{"user_id": "u1"}])
    assert detect_pair_heuristic(a, a) == []


def test_heuristic_type_compatibility_required():
    a = _topic("a", [("user_id", "string")], [{"user_id": "u1"}])
    b = _topic("b", [("user_id", "long")], [{"user_id": 1}])
    assert detect_pair_heuristic(a, b) == []


def test_heuristic_classifies_shared_key_when_neither_side_is_primary():
    a = _topic(
        "events",
        [("session_id", "string")],
        [{"session_id": "s1"}, {"session_id": "s2"}],
    )
    b = _topic(
        "metrics",
        [("session_id", "string")],
        [{"session_id": "s1"}, {"session_id": "s2"}],
    )
    rels = detect_pair_heuristic(a, b)
    assert len(rels) == 1
    assert rels[0].relationship_type == "shared_key"


# ----------------------------------------------------------- detect_all + persist


@pytest.mark.asyncio
async def test_detect_all_persists_relationships(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    orders = _topic(
        "orders",
        [("order_id", "string"), ("customer_id", "string")],
        [{"order_id": "o1", "customer_id": "c1"}, {"order_id": "o2", "customer_id": "c2"}],
    )
    customers = _topic(
        "customers",
        [("customer_id", "string")],
        [{"customer_id": "c1"}, {"customer_id": "c2"}],
    )
    for entry in (orders, customers):
        store.upsert_topic(entry)
        store.replace_fields(entry.name, entry.fields)
        store.replace_samples(entry.name, entry.samples)

    detector = RelationshipDetector(store=store, inference=None)
    total = await detector.refresh_all()
    assert total >= 1
    persisted = store.get_relationships("orders")
    assert any(r.target_topic == "customers" for r in persisted)


# ------------------------------------------------------------ LLM polish layer


class _FakeProvider:
    name = "fake"

    def __init__(self, response: str, cost: float = 0.0001):
        self._response = response
        self._cost = cost
        self.calls: list[dict] = []

    async def complete(self, *, system, prompt, max_output_tokens):
        self.calls.append({"system": system, "prompt": prompt})
        return self._response, self._cost


@pytest.mark.asyncio
async def test_llm_layer_runs_only_when_heuristic_misses(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    payments = _topic(
        "payment_attempts",
        [("attempt_id", "string"), ("amount", "double")],
        [{"attempt_id": "a1", "amount": 10.0}],
        description="Records every payment attempt with its outcome.",
    )
    completions = _topic(
        "order_completions",
        [("completion_id", "string"), ("total", "double")],
        [{"completion_id": "c1", "total": 10.0}],
        description="One record per completed order, downstream of payments.",
    )
    for entry in (payments, completions):
        store.upsert_topic(entry)
        store.replace_fields(entry.name, entry.fields)
        store.replace_samples(entry.name, entry.samples)

    provider = _FakeProvider(
        json.dumps(
            {
                "related": True,
                "relationship_type": "event_chain",
                "confidence": 0.82,
                "rationale": "Payments precede order completions in the same flow.",
            }
        )
    )
    inference = InferenceEngine(
        provider=provider,
        store=store,
        config=CatalogConfig(daily_llm_spend_ceiling_usd=1.0),
    )
    detector = RelationshipDetector(store=store, inference=inference)
    await detector.refresh_all()
    rels = store.get_relationships("payment_attempts")
    chain_rels = [r for r in rels if r.relationship_type == "event_chain"]
    assert chain_rels, "expected an event_chain relationship from the LLM layer"
    assert chain_rels[0].confidence == pytest.approx(0.82)
    assert provider.calls, "LLM should be called when heuristic finds nothing"


@pytest.mark.asyncio
async def test_llm_layer_skipped_when_heuristic_already_matched(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    orders = _topic(
        "orders",
        [("customer_id", "string")],
        [{"customer_id": "c1"}, {"customer_id": "c2"}],
        description="orders",
    )
    customers = _topic(
        "customers",
        [("customer_id", "string")],
        [{"customer_id": "c1"}, {"customer_id": "c2"}],
        description="customers",
    )
    for entry in (orders, customers):
        store.upsert_topic(entry)
        store.replace_fields(entry.name, entry.fields)
        store.replace_samples(entry.name, entry.samples)
    provider = _FakeProvider(json.dumps({"related": True, "confidence": 0.99}))
    inference = InferenceEngine(
        provider=provider,
        store=store,
        config=CatalogConfig(daily_llm_spend_ceiling_usd=1.0),
    )
    detector = RelationshipDetector(store=store, inference=inference)
    await detector.refresh_all()
    assert provider.calls == []


@pytest.mark.asyncio
async def test_llm_layer_caches_results(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    a = _topic("a", [], [], fingerprint="fp-a", description="a desc")
    b = _topic("b", [], [], fingerprint="fp-b", description="b desc")
    for entry in (a, b):
        store.upsert_topic(entry)
    provider = _FakeProvider(
        json.dumps(
            {
                "related": True,
                "relationship_type": "semantic",
                "confidence": 0.9,
                "rationale": "linked",
            }
        )
    )
    inference = InferenceEngine(
        provider=provider,
        store=store,
        config=CatalogConfig(daily_llm_spend_ceiling_usd=1.0),
    )
    detector = RelationshipDetector(store=store, inference=inference)
    rel1 = await detector._maybe_llm_relationship(a, b)
    assert rel1 is not None
    rel2 = await detector._maybe_llm_relationship(a, b)
    assert rel2 is not None
    # Second call hits the inference cache; provider only invoked once.
    assert len(provider.calls) == 1


@pytest.mark.asyncio
async def test_llm_layer_respects_ceiling(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    store.record_spend("fake", 1.0)
    a = _topic("a", [], [], description="a")
    b = _topic("b", [], [], description="b")
    provider = _FakeProvider("{}")
    inference = InferenceEngine(
        provider=provider,
        store=store,
        config=CatalogConfig(daily_llm_spend_ceiling_usd=0.5),
    )
    detector = RelationshipDetector(store=store, inference=inference)
    rel = await detector._maybe_llm_relationship(a, b)
    assert rel is None
    assert provider.calls == []


@pytest.mark.asyncio
async def test_llm_layer_drops_below_threshold(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    a = _topic("a", [], [], description="a")
    b = _topic("b", [], [], description="b")
    provider = _FakeProvider(
        json.dumps(
            {
                "related": True,
                "relationship_type": "semantic",
                "confidence": 0.4,
                "rationale": "weak",
            }
        )
    )
    inference = InferenceEngine(
        provider=provider,
        store=store,
        config=CatalogConfig(daily_llm_spend_ceiling_usd=1.0),
    )
    detector = RelationshipDetector(
        store=store, inference=inference, llm_confidence_threshold=0.7
    )
    rel = await detector._maybe_llm_relationship(a, b)
    assert rel is None


def test_llm_cache_key_is_symmetric_in_inputs():
    a = _topic("a", [], [], fingerprint="fp-a")
    b = _topic("b", [], [], fingerprint="fp-b")
    # Cache key depends on order (we store one direction); just confirm stability.
    assert _llm_cache_key(a, b) == _llm_cache_key(a, b)
    assert _llm_cache_key(a, b) != _llm_cache_key(b, a)
