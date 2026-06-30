"""Catalog tests: schema flattening, SQLite store, builder orchestration.

No real Schema Registry or Kafka or Qdrant — all collaborators are fakes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest

from streamcontext.catalog.activity import ActivityProfiler
from streamcontext.catalog.builder import CatalogBuilder, _is_stale
from streamcontext.catalog.introspect import SchemaIntrospector, flatten_avro_schema
from streamcontext.catalog.models import (
    ActivityStats,
    CatalogConfig,
    FieldEntry,
    SampleMessage,
    TopicEntry,
)
from streamcontext.catalog.store import CatalogStore

# --------------------------------------------------------- schema introspection


def test_flatten_simple_record():
    schema = {
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "id", "type": "string", "doc": "order id"},
            {"name": "total", "type": "double"},
            {"name": "currency", "type": ["null", "string"], "default": None},
        ],
    }
    fields = flatten_avro_schema(schema)
    by_name = {f.name: f for f in fields}
    assert by_name["id"].type == "string"
    assert by_name["id"].doc == "order id"
    assert by_name["total"].type == "double"
    assert by_name["currency"].nullable is True
    assert by_name["currency"].type == "string"


def test_flatten_nested_records_and_arrays():
    schema = {
        "type": "record",
        "name": "Customer",
        "fields": [
            {
                "name": "address",
                "type": {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                        {"name": "city", "type": "string"},
                        {"name": "zip", "type": "string"},
                    ],
                },
            },
            {
                "name": "tags",
                "type": {"type": "array", "items": "string"},
            },
            {
                "name": "orders",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "OrderRef",
                        "fields": [
                            {"name": "id", "type": "string"},
                            {"name": "amount", "type": "double"},
                        ],
                    },
                },
            },
        ],
    }
    fields = flatten_avro_schema(schema)
    names = {f.name for f in fields}
    assert "address" in names
    assert "address.city" in names
    assert "address.zip" in names
    assert "tags" in names
    assert "orders" in names
    assert "orders[].id" in names
    assert "orders[].amount" in names


class FakeSchemaRegistry:
    def __init__(self, schemas: dict[str, dict[str, Any]]):
        self._schemas = schemas

    def get_latest_version(self, subject_name: str):
        if subject_name not in self._schemas:
            raise KeyError(subject_name)
        s = self._schemas[subject_name]

        class _Schema:
            schema_str = json.dumps(s["schema"])

        class _Reg:
            schema = _Schema()
            schema_id = s.get("id", 1)
            version = s.get("version", 1)

        return _Reg()


def test_introspector_returns_fingerprint_and_fields():
    schemas = {
        "orders-value": {
            "schema": {
                "type": "record",
                "name": "Order",
                "fields": [
                    {"name": "order_id", "type": "string"},
                    {"name": "total_cents", "type": "long"},
                ],
            },
            "id": 7,
            "version": 3,
        }
    }
    introspector = SchemaIntrospector(FakeSchemaRegistry(schemas))
    subject, schema_id, version, fingerprint, raw, fields = introspector.introspect(
        "orders"
    )
    assert subject == "orders-value"
    assert schema_id == 7
    assert version == 3
    assert fingerprint is not None and len(fingerprint) == 64
    assert raw is not None
    field_names = {f.name for f in fields}
    assert field_names == {"order_id", "total_cents"}


def test_introspector_handles_missing_subject():
    introspector = SchemaIntrospector(FakeSchemaRegistry({}))
    subject, schema_id, _version, fingerprint, _raw, fields = introspector.introspect(
        "missing"
    )
    assert subject is None
    assert schema_id is None
    assert fingerprint is None
    assert fields == []


def test_introspector_handles_no_registry():
    introspector = SchemaIntrospector(None)
    out = introspector.introspect("anything")
    assert out == (None, None, None, None, None, [])


# ------------------------------------------------------------------------ store


def test_store_roundtrip_topic_fields_samples_activity(tmp_path):
    store = CatalogStore(tmp_path / "catalog.sqlite")
    entry = TopicEntry(
        name="orders",
        schema_subject="orders-value",
        schema_id=1,
        schema_version=2,
        schema_fingerprint="a" * 64,
        inference_status="pending",
    )
    store.upsert_topic(entry, raw_schema_json='{"type": "record"}')
    store.replace_fields(
        "orders",
        [
            FieldEntry(name="order_id", type="string"),
            FieldEntry(name="total_cents", type="long", doc="amount in cents"),
        ],
    )
    store.replace_samples(
        "orders",
        [
            SampleMessage(
                partition=0,
                offset=100,
                timestamp_ms=1_700_000_000_000,
                key="K1",
                value={"order_id": "o1", "total_cents": 1500},
            )
        ],
    )
    store.upsert_activity(
        "orders",
        ActivityStats(
            messages_last_hour=10,
            messages_last_day=100,
            rate_per_minute_last_hour=0.16,
            last_observed_ts_ms=1_700_000_000_000,
        ),
    )

    fetched = store.get_topic("orders")
    assert fetched is not None
    assert fetched.schema_fingerprint == "a" * 64
    assert {f.name for f in fetched.fields} == {"order_id", "total_cents"}
    assert len(fetched.samples) == 1
    assert fetched.samples[0].value == {"order_id": "o1", "total_cents": 1500}
    assert fetched.activity.messages_last_hour == 10
    assert store.list_topic_names() == ["orders"]


def test_store_retain_samples_false_does_not_persist(tmp_path):
    store = CatalogStore(tmp_path / "catalog.sqlite")
    store.upsert_topic(TopicEntry(name="orders"))
    store.replace_samples(
        "orders",
        [
            SampleMessage(
                partition=0,
                offset=1,
                timestamp_ms=1,
                key="k",
                value={"hello": "world"},
            )
        ],
        retain=False,
    )
    fetched = store.get_topic("orders")
    assert fetched is not None
    assert fetched.samples == []


def test_store_inference_cache_roundtrip(tmp_path):
    store = CatalogStore(tmp_path / "catalog.sqlite")
    assert store.get_inference_cache("missing") is None
    store.put_inference_cache("k1", {"answer": "yes"})
    assert store.get_inference_cache("k1") == {"answer": "yes"}


def test_store_spend_ledger_accumulates(tmp_path):
    store = CatalogStore(tmp_path / "catalog.sqlite")
    total = store.record_spend("anthropic", 0.01, day="2030-01-01")
    assert total == pytest.approx(0.01)
    total = store.record_spend("anthropic", 0.02, day="2030-01-01")
    assert total == pytest.approx(0.03)
    assert store.get_spend_today("anthropic", day="2030-01-01") == pytest.approx(0.03)
    assert store.get_spend_today("anthropic", day="2030-01-02") == 0.0


# -------------------------------------------------------------------- staleness


def test_is_stale_first_time_is_stale():
    assert _is_stale(None, 60, 1_000_000_000) is True


def test_is_stale_within_ttl_is_fresh():
    now = 1_700_000_000_000
    assert _is_stale(now - 30_000, 60, now) is False


def test_is_stale_past_ttl_is_stale():
    now = 1_700_000_000_000
    assert _is_stale(now - 120_000, 60, now) is True


# ---------------------------------------------------------------- builder e2e


@dataclass
class FakeCountResult:
    count: int


@dataclass
class FakePoint:
    payload: dict[str, Any]


class FakeQdrant:
    """Just enough surface for ActivityProfiler tests."""

    def __init__(self, *, hour_count: int, day_count: int, latest_ts: int | None):
        self._hour_count = hour_count
        self._day_count = day_count
        self._latest_ts = latest_ts
        self.scroll_calls = 0
        self.count_calls = 0

    async def count(self, **kwargs):
        self.count_calls += 1
        # Distinguish hour vs day by alternating; tests pass either way.
        c = self._hour_count if self.count_calls == 1 else self._day_count
        return FakeCountResult(count=c)

    async def scroll(self, **kwargs):
        self.scroll_calls += 1
        if self._latest_ts is None:
            return ([], None)
        return ([FakePoint(payload={"timestamp_ms": self._latest_ts})], None)


class FakeSampler:
    def __init__(self, samples: list[SampleMessage]):
        self._samples = samples
        self.calls: list[tuple[str, int]] = []

    async def sample(self, topic: str, count: int = 10) -> list[SampleMessage]:
        self.calls.append((topic, count))
        return list(self._samples[:count])


@pytest.mark.asyncio
async def test_builder_refreshes_all_aspects(tmp_path):
    store = CatalogStore(tmp_path / "catalog.sqlite")
    schemas = {
        "orders-value": {
            "schema": {
                "type": "record",
                "name": "Order",
                "fields": [
                    {"name": "order_id", "type": "string"},
                    {"name": "total", "type": "double"},
                ],
            }
        }
    }
    introspector = SchemaIntrospector(FakeSchemaRegistry(schemas))
    qdrant = FakeQdrant(hour_count=5, day_count=50, latest_ts=1_700_000_000_000)
    profiler = ActivityProfiler(qdrant, "streamcontext")
    sampler = FakeSampler(
        [
            SampleMessage(
                partition=0,
                offset=42,
                timestamp_ms=1_700_000_000_000,
                key="k",
                value={"order_id": "o1", "total": 9.99},
            )
        ]
    )
    builder = CatalogBuilder(
        store=store,
        introspector=introspector,
        sampler=sampler,  # type: ignore[arg-type]
        profiler=profiler,
        config=CatalogConfig(sample_count=3),
    )

    entry = await builder.refresh_topic("orders")
    assert entry.schema_subject == "orders-value"
    assert entry.schema_fingerprint is not None
    assert {f.name for f in entry.fields} == {"order_id", "total"}
    assert len(entry.samples) == 1
    assert entry.activity.messages_last_hour == 5
    assert entry.activity.messages_last_day == 50
    assert entry.last_schema_refresh_ms is not None

    persisted = store.get_topic("orders")
    assert persisted is not None
    assert persisted.activity.last_observed_ts_ms == 1_700_000_000_000
    assert sampler.calls == [("orders", 3)]


@pytest.mark.asyncio
async def test_builder_ensure_fresh_skips_when_within_ttl(tmp_path):
    store = CatalogStore(tmp_path / "catalog.sqlite")
    introspector = SchemaIntrospector(FakeSchemaRegistry({}))
    qdrant = FakeQdrant(hour_count=0, day_count=0, latest_ts=None)
    profiler = ActivityProfiler(qdrant, "streamcontext")
    sampler = FakeSampler([])
    builder = CatalogBuilder(
        store=store,
        introspector=introspector,
        sampler=sampler,  # type: ignore[arg-type]
        profiler=profiler,
        config=CatalogConfig(
            schema_refresh_sec=3600,
            sample_refresh_sec=3600,
            stats_refresh_sec=3600,
        ),
    )
    await builder.refresh_topic("orders")
    before_calls = qdrant.count_calls
    # Second ensure_fresh should not trigger another refresh because nothing
    # is stale yet.
    await builder.ensure_fresh("orders")
    assert qdrant.count_calls == before_calls
