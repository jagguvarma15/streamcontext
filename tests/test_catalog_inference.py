"""Tests for the catalog inference engine.

All collaborators are fakes — no network calls. Covers prompt construction
(redaction, truncation), JSON parsing, the spend ceiling, and the per-input
cache key.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest

from streamcontext.catalog.activity import ActivityProfiler
from streamcontext.catalog.builder import CatalogBuilder
from streamcontext.catalog.inference import (
    SYSTEM_PROMPT,
    InferenceEngine,
    _safe_json,
    build_prompt,
    compile_patterns,
    redact_value,
)
from streamcontext.catalog.introspect import SchemaIntrospector
from streamcontext.catalog.models import (
    CatalogConfig,
    FieldEntry,
    SampleMessage,
    TopicEntry,
)
from streamcontext.catalog.store import CatalogStore

# ---------------------------------------------------------------- redaction


def test_redact_value_drops_field_names():
    patterns = compile_patterns()
    redacted = redact_value(
        {"email": "a@b.com", "name": "Ada", "nested": {"phone": "+1 415 555 9999"}},
        drop_fields=frozenset({"email", "phone"}),
        patterns=patterns,
    )
    assert "email" not in redacted
    assert "phone" not in redacted["nested"]
    assert redacted["name"] == "Ada"


def test_redact_value_masks_default_patterns():
    patterns = compile_patterns()
    redacted = redact_value(
        {"note": "contact me at ada@example.com or 415-555-9999"},
        drop_fields=frozenset(),
        patterns=patterns,
    )
    assert "ada@example.com" not in redacted["note"]
    assert "415-555-9999" not in redacted["note"]
    assert "[redacted]" in redacted["note"]


def test_redact_value_handles_lists():
    patterns = compile_patterns()
    redacted = redact_value(
        {"contacts": [{"email": "a@b.com"}, {"email": "b@c.com"}]},
        drop_fields=frozenset({"email"}),
        patterns=patterns,
    )
    assert redacted == {"contacts": [{}, {}]}


# ------------------------------------------------------------ prompt build


def _sample_fields() -> list[FieldEntry]:
    return [
        FieldEntry(name="order_id", type="string", doc="primary key"),
        FieldEntry(name="total_cents", type="long"),
        FieldEntry(name="email", type="string", nullable=True),
    ]


def _sample_messages(n: int = 4, big: bool = False) -> list[SampleMessage]:
    out: list[SampleMessage] = []
    payload_filler = "x" * (1500 if big else 8)
    for i in range(n):
        out.append(
            SampleMessage(
                partition=0,
                offset=100 + i,
                timestamp_ms=1_700_000_000_000 + i,
                key=f"k{i}",
                value={
                    "order_id": f"o-{i}",
                    "total_cents": 1500 + i,
                    "email": "ada@example.com",
                    "blob": payload_filler,
                },
            )
        )
    return out


def test_build_prompt_includes_schema_and_redacts_email():
    patterns = compile_patterns()
    prompt = build_prompt(
        topic="orders",
        fields=_sample_fields(),
        samples=_sample_messages(),
        drop_fields=frozenset({"email"}),
        patterns=patterns,
        max_samples=3,
    )
    assert "orders" in prompt
    assert "order_id" in prompt
    assert "ada@example.com" not in prompt
    assert "email" in prompt  # field-name still appears in schema
    parsed_body = json.loads(prompt.split("\n\n", 1)[1])
    assert len(parsed_body["samples"]) == 3
    for sample in parsed_body["samples"]:
        assert "email" not in sample


def test_build_prompt_respects_max_chars():
    patterns = compile_patterns()
    prompt = build_prompt(
        topic="orders",
        fields=_sample_fields(),
        samples=_sample_messages(n=5, big=True),
        drop_fields=frozenset(),
        patterns=patterns,
        max_samples=5,
        max_bytes_per_sample=400,
        max_chars=2000,
    )
    assert len(prompt) <= 2000


# ----------------------------------------------------------------- _safe_json


def test_safe_json_plain_object():
    assert _safe_json('{"a": 1}') == {"a": 1}


def test_safe_json_fenced_block():
    text = "```json\n{\n  \"a\": 1\n}\n```"
    assert _safe_json(text) == {"a": 1}


def test_safe_json_embedded_object():
    text = "Sure thing. {\"a\": 1} hope that helps."
    assert _safe_json(text) == {"a": 1}


def test_safe_json_raises_on_no_object():
    with pytest.raises(ValueError):
        _safe_json("nope")


# ------------------------------------------------------------ engine basics


class FakeProvider:
    name = "fake"

    def __init__(self, response: str, cost: float = 0.0005) -> None:
        self._response = response
        self._cost = cost
        self.calls: list[dict[str, Any]] = []

    async def complete(self, *, system: str, prompt: str, max_output_tokens: int):
        self.calls.append({"system": system, "prompt": prompt, "max": max_output_tokens})
        return self._response, self._cost


def _topic_entry() -> TopicEntry:
    return TopicEntry(
        name="orders",
        schema_fingerprint="abc123",
        fields=_sample_fields(),
        samples=_sample_messages(),
    )


@pytest.mark.asyncio
async def test_engine_inserts_into_cache_and_reads_it(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    cfg = CatalogConfig(daily_llm_spend_ceiling_usd=1.0)
    provider = FakeProvider(
        json.dumps(
            {
                "description": "Customer order events.",
                "description_confidence": 0.85,
                "field_annotations": {
                    "order_id": {"meaning": "primary key", "confidence": 0.95},
                    "total_cents": {"meaning": "total in cents", "confidence": 0.9},
                    "email": {"meaning": "unknown", "confidence": 0.1},
                },
            }
        )
    )
    engine = InferenceEngine(provider=provider, store=store, config=cfg)
    entry = _topic_entry()
    status, desc, conf, annotations = await engine.infer(entry)
    assert status == "inferred"
    assert desc == "Customer order events."
    assert conf == 0.85
    assert annotations["order_id"][0] == "primary key"
    assert annotations["order_id"][1] == 0.95
    assert len(provider.calls) == 1

    status2, desc2, _conf2, annotations2 = await engine.infer(entry)
    assert status2 == "inferred"
    assert desc2 == desc
    assert annotations2["order_id"] == annotations["order_id"]
    # No new provider call: cache hit.
    assert len(provider.calls) == 1


@pytest.mark.asyncio
async def test_engine_returns_disabled_when_no_provider(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    cfg = CatalogConfig()
    engine = InferenceEngine(provider=None, store=store, config=cfg)
    status, desc, conf, annotations = await engine.infer(_topic_entry())
    assert status == "disabled"
    assert desc is None and conf is None and annotations == {}


@pytest.mark.asyncio
async def test_engine_disabled_after_ceiling_exceeded(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    cfg = CatalogConfig(daily_llm_spend_ceiling_usd=0.001)
    provider = FakeProvider(
        json.dumps(
            {
                "description": "Customer order events.",
                "description_confidence": 0.8,
                "field_annotations": {},
            }
        ),
        cost=0.002,
    )
    engine = InferenceEngine(provider=provider, store=store, config=cfg)
    entry = _topic_entry()
    # First call goes through (ceiling not yet exceeded), then records spend.
    status, _, _, _ = await engine.infer(entry)
    assert status == "inferred"
    # Mutate samples so cache key changes; otherwise the second call hits cache.
    entry.samples = _sample_messages(n=2)
    status2, _, _, _ = await engine.infer(entry)
    assert status2 == "disabled"
    assert len(provider.calls) == 1


@pytest.mark.asyncio
async def test_engine_returns_failed_on_provider_exception(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    cfg = CatalogConfig()

    class Boom:
        name = "boom"

        async def complete(self, **kwargs):
            raise RuntimeError("boom")

    engine = InferenceEngine(provider=Boom(), store=store, config=cfg)
    status, desc, _conf, annotations = await engine.infer(_topic_entry())
    assert status == "failed"
    assert desc is None
    assert annotations == {}


def test_cache_key_order_independent():
    samples_a = [
        SampleMessage(partition=0, offset=1, timestamp_ms=1, value={"a": 1}),
        SampleMessage(partition=0, offset=2, timestamp_ms=2, value={"a": 2}),
    ]
    samples_b = list(reversed(samples_a))
    key_a = InferenceEngine.cache_key(schema_fingerprint="abc", samples=samples_a)
    key_b = InferenceEngine.cache_key(schema_fingerprint="abc", samples=samples_b)
    assert key_a == key_b


def test_cache_key_changes_with_schema():
    samples = [SampleMessage(partition=0, offset=1, timestamp_ms=1, value={"a": 1})]
    k1 = InferenceEngine.cache_key(schema_fingerprint="abc", samples=samples)
    k2 = InferenceEngine.cache_key(schema_fingerprint="xyz", samples=samples)
    assert k1 != k2


# ------------------------------------------------------ builder integration


class FakeSchemaRegistry:
    def __init__(self, schemas: dict[str, dict[str, Any]]):
        self._schemas = schemas

    def get_latest_version(self, subject_name: str):
        s = self._schemas[subject_name]

        class _Schema:
            schema_str = json.dumps(s["schema"])

        class _Reg:
            schema = _Schema()
            schema_id = s.get("id", 1)
            version = s.get("version", 1)

        return _Reg()


@dataclass
class _CountResult:
    count: int


@dataclass
class _Point:
    payload: dict[str, Any]


class _FakeQdrant:
    async def count(self, **kwargs):
        return _CountResult(count=0)

    async def scroll(self, **kwargs):
        return ([], None)


class _NoopSampler:
    async def sample(self, topic: str, count: int = 10):
        return [
            SampleMessage(
                partition=0,
                offset=1,
                timestamp_ms=1,
                key="k",
                value={"order_id": "o", "total_cents": 100, "email": "a@b.com"},
            )
        ]


@pytest.mark.asyncio
async def test_builder_applies_inference_results(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    introspector = SchemaIntrospector(
        FakeSchemaRegistry(
            {
                "orders-value": {
                    "schema": {
                        "type": "record",
                        "name": "Order",
                        "fields": [
                            {"name": "order_id", "type": "string"},
                            {"name": "total_cents", "type": "long"},
                            {"name": "email", "type": ["null", "string"]},
                        ],
                    }
                }
            }
        )
    )
    qdrant = _FakeQdrant()
    profiler = ActivityProfiler(qdrant, "streamcontext")
    sampler = _NoopSampler()
    cfg = CatalogConfig(
        pii_redact_fields=["email"], daily_llm_spend_ceiling_usd=1.0
    )
    provider = FakeProvider(
        json.dumps(
            {
                "description": "Customer order events.",
                "description_confidence": 0.9,
                "field_annotations": {
                    "order_id": {"meaning": "primary key", "confidence": 0.9},
                },
            }
        )
    )
    engine = InferenceEngine(provider=provider, store=store, config=cfg)
    builder = CatalogBuilder(
        store=store,
        introspector=introspector,
        sampler=sampler,  # type: ignore[arg-type]
        profiler=profiler,
        config=cfg,
        inference=engine,
    )
    entry = await builder.refresh_topic("orders")
    assert entry.inference_status == "inferred"
    assert entry.description == "Customer order events."
    annotations = {f.name: f.inferred_meaning for f in entry.fields}
    assert annotations["order_id"] == "primary key"
    # Email field-name was redacted from samples but is still in schema; no
    # annotation for it because the LLM did not return one.
    assert annotations.get("email") in (None, "unknown")

    # Second refresh should hit the inference cache and not call the provider.
    calls_before = len(provider.calls)
    entry2 = await builder.refresh_topic("orders")
    assert entry2.inference_status == "inferred"
    assert len(provider.calls) == calls_before


def test_system_prompt_is_constant():
    # Just a guard so we notice if the system prompt drifts unintentionally.
    assert "data catalog assistant" in SYSTEM_PROMPT
