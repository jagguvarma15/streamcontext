"""Tests for streamcontext.

Two layers:
  - Pure unit tests (no Docker) — run by default.
  - Integration test against real Kafka + Qdrant via testcontainers,
    skipped unless `RUN_INTEGRATION=1` and Docker is available.

Run integration locally with:
    RUN_INTEGRATION=1 pytest -q tests/test_pipeline.py
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from dataclasses import dataclass

import pytest

from streamcontext.config import Settings
from streamcontext.embedder import message_to_text
from streamcontext.pipeline import Pipeline, _build_record, _max_offsets
from streamcontext.sink import stable_uuid
from streamcontext.types import KafkaMessage, VectorRecord


# ---------- Pure unit tests ----------


def test_settings_defaults_are_sane() -> None:
    s = Settings()
    assert s.batch_size > 0
    assert s.batch_flush_interval_sec > 0
    assert s.qdrant_vector_dim == 384  # matches all-MiniLM-L6-v2
    assert "orders" in s.topics_list


def test_topics_list_parses_csv() -> None:
    s = Settings(kafka_topics="orders, clicks ,signups")
    assert s.topics_list == ["orders", "clicks", "signups"]


def _msg(topic: str, partition: int, offset: int, value: dict) -> KafkaMessage:
    return KafkaMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp_ms=int(time.time() * 1000),
        key=None,
        headers={},
        value=value,
    )


def test_message_to_text_is_canonical() -> None:
    a = _msg("orders", 0, 1, {"b": 2, "a": 1})
    b = _msg("orders", 0, 1, {"a": 1, "b": 2})
    assert message_to_text(a) == message_to_text(b)


def test_stable_id_and_uuid_are_deterministic() -> None:
    m1 = _msg("orders", 2, 99, {"x": 1})
    m2 = _msg("orders", 2, 99, {"x": 999})  # different value, same coords
    assert m1.stable_id == m2.stable_id
    assert stable_uuid(m1.stable_id) == stable_uuid(m2.stable_id)
    # And different coords produce different UUIDs
    other = _msg("orders", 2, 100, {"x": 1})
    assert stable_uuid(m1.stable_id) != stable_uuid(other.stable_id)


def test_max_offsets_picks_highest_per_partition() -> None:
    msgs = [
        _msg("orders", 0, 5, {}),
        _msg("orders", 0, 7, {}),
        _msg("orders", 1, 3, {}),
        _msg("clicks", 0, 100, {}),
    ]
    offsets = _max_offsets(msgs)
    # next-offset (commit point) = max + 1
    keys = {(tp.topic, tp.partition): off for tp, off in offsets.items()}
    assert keys[("orders", 0)] == 8
    assert keys[("orders", 1)] == 4
    assert keys[("clicks", 0)] == 101


def test_build_record_preserves_metadata() -> None:
    m = _msg("orders", 1, 42, {"order_id": "abc", "total": 99.5})
    rec = _build_record(m, [0.1, 0.2, 0.3])
    assert rec.id == "orders:1:42"
    assert rec.vector == [0.1, 0.2, 0.3]
    assert rec.payload["topic"] == "orders"
    assert rec.payload["partition"] == 1
    assert rec.payload["offset"] == 42
    assert rec.payload["value"]["order_id"] == "abc"


# ---------- Pipeline behavior with fakes ----------


class FakeEmbedder:
    dim = 4

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    async def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        return [[float(len(t)), 1.0, 2.0, 3.0] for t in texts]


class FakeSink:
    def __init__(self) -> None:
        self.records: list[VectorRecord] = []
        self.ready = False

    async def ensure_ready(self) -> None:
        self.ready = True

    async def upsert(self, records: list[VectorRecord]) -> None:
        self.records.extend(records)

    async def close(self) -> None:
        pass


@dataclass
class FakeConsumer:
    """Yields a fixed list of messages then stops, like an exhausted async iter."""

    msgs: list[KafkaMessage]
    started: bool = False
    stopped: bool = False
    committed: list[dict] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        self.committed = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def commit(self, offsets) -> None:
        self.committed.append(dict(offsets))

    async def messages(self):  # pragma: no cover - generator
        for m in self.msgs:
            yield m
        # natural stop


@pytest.mark.asyncio
async def test_pipeline_batches_and_commits_with_fakes() -> None:
    msgs = [_msg("orders", 0, i, {"i": i}) for i in range(5)]
    consumer = FakeConsumer(msgs)
    embedder = FakeEmbedder()
    sink = FakeSink()
    p = Pipeline(consumer, embedder, sink, batch_size=3, flush_interval_sec=0.05)
    await p.run()

    # All 5 messages embedded and stored
    assert sum(len(b) for b in embedder.calls) == 5
    assert len(sink.records) == 5
    # Commit happened at least once
    assert consumer.committed
    # Sink readiness was checked before any upsert
    assert sink.ready


# ---------- Integration test (opt-in) ----------


INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"


@pytest.mark.skipif(not INTEGRATION, reason="Set RUN_INTEGRATION=1 to run.")
@pytest.mark.asyncio
async def test_end_to_end_kafka_to_qdrant() -> None:
    """Spin up Kafka + Qdrant in containers, push a JSON message through the pipeline.

    Avoids Schema Registry to keep the container set small — uses a JSON-only
    consumer adapter built ad-hoc here. The real gateway uses Avro; this test
    validates the orchestration plumbing only.
    """
    pytest.importorskip("testcontainers.kafka")
    pytest.importorskip("aiokafka")

    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    from qdrant_client import AsyncQdrantClient
    from qdrant_client.http import models as rest
    from testcontainers.core.container import DockerContainer
    from testcontainers.kafka import KafkaContainer

    topic = f"test-{uuid.uuid4().hex[:8]}"
    collection = f"test-{uuid.uuid4().hex[:8]}"

    with KafkaContainer() as kc, DockerContainer("qdrant/qdrant:v1.10.1").with_exposed_ports(6333) as qc:
        bootstrap = kc.get_bootstrap_server()
        qdrant_host = qc.get_container_host_ip()
        qdrant_port = qc.get_exposed_port(6333)
        qdrant_url = f"http://{qdrant_host}:{qdrant_port}"

        # Wait for Qdrant readiness (rough)
        await asyncio.sleep(2)

        # Produce a few JSON messages
        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            for i in range(4):
                await producer.send_and_wait(
                    topic,
                    json.dumps({"i": i, "note": f"row {i}"}).encode("utf-8"),
                    key=f"k{i}".encode(),
                )
        finally:
            await producer.stop()

        # Build a tiny consumer adapter (matches AvroKafkaConsumer surface)
        kc_consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            group_id="it",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await kc_consumer.start()

        class JSONAdapter:
            async def start(self) -> None: ...
            async def stop(self) -> None:
                await kc_consumer.stop()

            async def commit(self, offsets) -> None:
                await kc_consumer.commit(offsets=offsets)

            async def messages(self):
                count = 0
                async for r in kc_consumer:
                    yield KafkaMessage(
                        topic=r.topic,
                        partition=r.partition,
                        offset=r.offset,
                        timestamp_ms=r.timestamp,
                        key=r.key.decode() if r.key else None,
                        headers={},
                        value=json.loads(r.value),
                    )
                    count += 1
                    if count >= 4:
                        return

        from streamcontext.sink import QdrantSink

        embedder = FakeEmbedder()  # 4-dim deterministic vectors
        sink = QdrantSink(url=qdrant_url, collection=collection, vector_dim=embedder.dim)
        pipeline = Pipeline(JSONAdapter(), embedder, sink, batch_size=2, flush_interval_sec=0.5)
        await pipeline.run()

        client = AsyncQdrantClient(url=qdrant_url)
        try:
            info = await client.get_collection(collection)
            assert info.points_count == 4
            res = await client.search(
                collection_name=collection,
                query_vector=[1.0, 1.0, 2.0, 3.0],
                limit=4,
            )
            assert len(res) == 4
        finally:
            await client.close()
