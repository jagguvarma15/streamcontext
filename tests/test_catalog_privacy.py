"""Privacy hardening tests for the catalog.

Covers:
  - PII redaction (field-name + regex) runs before samples are persisted.
  - retain_samples=False prevents persistence but still lets inference see
    samples in-memory.
  - Schema fingerprint is a SHA-256 of canonical JSON (collision-resistant).
  - Daily LLM spend ceiling actually disables inference and recovers.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import pytest

from streamcontext.catalog.activity import ActivityProfiler
from streamcontext.catalog.builder import CatalogBuilder
from streamcontext.catalog.inference import InferenceEngine
from streamcontext.catalog.introspect import SchemaIntrospector
from streamcontext.catalog.models import (
    CatalogConfig,
    SampleMessage,
    TopicEntry,
)
from streamcontext.catalog.privacy import (
    REDACTED_TOKEN,
    compile_patterns,
    redact_value,
)
from streamcontext.catalog.store import CatalogStore

# ------------------------------------------------------------ pure redactor


def test_redact_drops_listed_fields_recursively():
    patterns = compile_patterns()
    out = redact_value(
        {
            "id": "1",
            "email": "a@b.com",
            "profile": {"phone": "415-555-9999", "name": "Ada"},
            "history": [{"card_number": "4111111111111111"}],
        },
        drop_fields=frozenset({"email", "phone", "card_number"}),
        patterns=patterns,
    )
    assert "email" not in out
    assert "phone" not in out["profile"]
    assert out["history"][0] == {}


def test_regex_redaction_masks_common_shapes():
    patterns = compile_patterns()
    out = redact_value(
        {"note": "reach me at ada@example.com or 415-555-9999, card 4111 1111 1111 1111"},
        drop_fields=frozenset(),
        patterns=patterns,
    )
    text = out["note"]
    assert "ada@example.com" not in text
    assert "415-555-9999" not in text
    assert REDACTED_TOKEN in text


def test_custom_patterns_merge_with_defaults():
    patterns = compile_patterns([r"INTERNAL-\d+"])
    out = redact_value(
        {"note": "ticket INTERNAL-12345 raised by ada@example.com"},
        drop_fields=frozenset(),
        patterns=patterns,
    )
    assert "INTERNAL-12345" not in out["note"]
    assert "ada@example.com" not in out["note"]


# -------------------------------------------------------------- fingerprint


def test_schema_fingerprint_is_sha256_of_canonical_json():
    class FakeRegistry:
        def get_latest_version(self, subject_name: str):
            schema_dict = {
                "type": "record",
                "name": "Order",
                "fields": [{"name": "id", "type": "string"}],
            }

            class _Schema:
                schema_str = json.dumps(schema_dict)

            class _Reg:
                schema = _Schema()
                schema_id = 1
                version = 1

            return _Reg()

    introspector = SchemaIntrospector(FakeRegistry())
    _, _, _, fingerprint, raw, _ = introspector.introspect("orders")
    canonical = json.dumps(json.loads(raw), sort_keys=True, separators=(",", ":"))
    assert fingerprint == hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    assert len(fingerprint) == 64


# ---------------------------------------------------- builder + persistence


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


class PIIIncomingSampler:
    """Returns samples containing email + phone + card values."""

    def __init__(self) -> None:
        self.last_returned: list[SampleMessage] = []

    async def sample(self, topic: str, count: int = 10) -> list[SampleMessage]:
        self.last_returned = [
            SampleMessage(
                partition=0,
                offset=1,
                timestamp_ms=1,
                key="k1",
                value={
                    "order_id": "o1",
                    "email": "ada@example.com",
                    "note": "call 415-555-9999",
                    "card": "4111111111111111",
                },
            )
        ]
        return list(self.last_returned)


@dataclass
class _Count:
    count: int


class _FakeQdrant:
    async def count(self, **kwargs):
        return _Count(count=0)

    async def scroll(self, **kwargs):
        return ([], None)


@pytest.mark.asyncio
async def test_samples_are_redacted_before_persistence(tmp_path):
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
                            {"name": "email", "type": ["null", "string"]},
                            {"name": "note", "type": "string"},
                            {"name": "card", "type": "string"},
                        ],
                    }
                }
            }
        )
    )
    builder = CatalogBuilder(
        store=store,
        introspector=introspector,
        sampler=PIIIncomingSampler(),  # type: ignore[arg-type]
        profiler=ActivityProfiler(_FakeQdrant(), "streamcontext"),
        config=CatalogConfig(
            pii_redact_fields=["email"],
            pii_redact_patterns=[],  # rely on defaults for phone/card
        ),
    )
    entry = await builder.refresh_topic("orders")
    persisted = store.get_topic("orders")
    assert persisted is not None
    assert persisted.samples, "expected one persisted sample"
    sample = persisted.samples[0]
    # Field-name redaction.
    assert "email" not in sample.value
    # Regex redaction inside string values.
    assert "415-555-9999" not in sample.value["note"]
    assert REDACTED_TOKEN in sample.value["note"]
    # Card-number redaction at the value level (the digits are inside a string).
    assert "4111111111111111" not in sample.value["card"]

    # The in-memory entry returned by refresh_topic is the same redacted shape.
    assert "email" not in entry.samples[0].value


@pytest.mark.asyncio
async def test_retain_samples_false_keeps_metadata_only(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    introspector = SchemaIntrospector(
        FakeSchemaRegistry(
            {
                "orders-value": {
                    "schema": {
                        "type": "record",
                        "name": "Order",
                        "fields": [{"name": "order_id", "type": "string"}],
                    }
                }
            }
        )
    )
    sampler = PIIIncomingSampler()
    builder = CatalogBuilder(
        store=store,
        introspector=introspector,
        sampler=sampler,  # type: ignore[arg-type]
        profiler=ActivityProfiler(_FakeQdrant(), "streamcontext"),
        config=CatalogConfig(retain_samples=False),
    )
    entry = await builder.refresh_topic("orders")
    # In-memory samples still present for inference.
    assert entry.samples
    # Persisted samples are not.
    persisted = store.get_topic("orders")
    assert persisted is not None
    assert persisted.samples == []


# ------------------------------------------------------- cost ceiling end-to-end


class _CostyProvider:
    """Provider whose first call costs above the configured ceiling."""

    name = "costy"

    def __init__(self, cost: float) -> None:
        self._cost = cost
        self.calls = 0

    async def complete(self, *, system, prompt, max_output_tokens):
        self.calls += 1
        return (
            json.dumps(
                {
                    "description": "Inferred topic.",
                    "description_confidence": 0.8,
                    "field_annotations": {},
                }
            ),
            self._cost,
        )


@pytest.mark.asyncio
async def test_inference_disables_after_ceiling_and_recovers(tmp_path):
    store = CatalogStore(tmp_path / "c.sqlite")
    cfg = CatalogConfig(daily_llm_spend_ceiling_usd=0.01)
    provider = _CostyProvider(cost=0.02)
    engine = InferenceEngine(provider=provider, store=store, config=cfg)
    entry = TopicEntry(name="orders", schema_fingerprint="fp1")
    # First call succeeds and spends $0.02 (already over the $0.01 ceiling).
    status, _, _, _ = await engine.infer(entry)
    assert status == "inferred"
    assert provider.calls == 1
    # Second call with a different fingerprint must be disabled — ceiling tripped.
    entry2 = TopicEntry(name="orders", schema_fingerprint="fp2")
    status2, desc2, _conf2, annotations2 = await engine.infer(entry2)
    assert status2 == "disabled"
    assert desc2 is None
    assert annotations2 == {}
    assert provider.calls == 1  # provider not contacted

    # Simulate the next day rolling over by raising the ceiling.
    cfg.daily_llm_spend_ceiling_usd = 1.0
    entry3 = TopicEntry(name="orders", schema_fingerprint="fp3")
    status3, _, _, _ = await engine.infer(entry3)
    assert status3 == "inferred"
    assert provider.calls == 2
