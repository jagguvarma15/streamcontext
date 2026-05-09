"""Kafka consumer with Avro deserialization via Schema Registry.

Yields `KafkaMessage` objects with all coordinates preserved. Manual offset
commits — the pipeline only commits after a batch is durably written to the
sink (at-least-once semantics).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from streamcontext.config import Settings
from streamcontext.logging import get_logger
from streamcontext.types import KafkaMessage

log = get_logger("streamcontext.consumer")


class AvroKafkaConsumer:
    """Async wrapper around aiokafka that decodes Avro values via SR."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._consumer: AIOKafkaConsumer | None = None
        sr = SchemaRegistryClient({"url": settings.schema_registry_url})
        # Reader schema is None → use the writer schema embedded in each message
        # via the magic-byte / schema-id prefix. This is what we want for a
        # generic ingester.
        self._deserializer = AvroDeserializer(sr)

    async def start(self) -> None:
        topics = self._settings.topics_list
        if not topics:
            raise ValueError("SC_KAFKA_TOPICS must list at least one topic.")
        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            group_id=self._settings.kafka_group_id,
            auto_offset_reset=self._settings.kafka_auto_offset_reset,
            enable_auto_commit=False,
            # Reasonable defaults for a streaming workload. Override via env if
            # you really want to tune.
            max_poll_records=500,
            session_timeout_ms=30_000,
            heartbeat_interval_ms=10_000,
        )
        await self._consumer.start()
        log.info("consumer.started", topics=topics, group=self._settings.kafka_group_id)

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            log.info("consumer.stopped")
            self._consumer = None

    async def commit(self, offsets: dict[TopicPartition, int]) -> None:
        """Commit explicit (partition → next offset) positions."""
        assert self._consumer is not None, "consumer.start() must be called first"
        # aiokafka expects offsets keyed by TopicPartition, value = next offset
        await self._consumer.commit(offsets=offsets)

    def _decode_value(self, topic: str, raw: bytes | None) -> dict[str, Any] | None:
        if raw is None:
            return None
        ctx = SerializationContext(topic, MessageField.VALUE)
        return self._deserializer(raw, ctx)

    @staticmethod
    def _decode_headers(headers: list[tuple[str, bytes]] | None) -> dict[str, str]:
        if not headers:
            return {}
        out: dict[str, str] = {}
        for k, v in headers:
            try:
                out[k] = v.decode("utf-8") if isinstance(v, bytes) else str(v)
            except UnicodeDecodeError:
                out[k] = repr(v)
        return out

    async def messages(self) -> AsyncIterator[KafkaMessage]:
        """Yield decoded messages until the consumer is stopped."""
        assert self._consumer is not None, "consumer.start() must be called first"
        try:
            async for record in self._consumer:
                try:
                    value = self._decode_value(record.topic, record.value)
                except Exception as exc:
                    # Dead-letter logging for now; DLQ topic lands in v0.2.
                    log.warning(
                        "consumer.deserialize_failed",
                        topic=record.topic,
                        partition=record.partition,
                        offset=record.offset,
                        error=str(exc),
                    )
                    continue

                if value is None:
                    # Tombstone / null value — skip but don't fail.
                    continue

                key: str | None
                if record.key is None:
                    key = None
                elif isinstance(record.key, bytes):
                    try:
                        key = record.key.decode("utf-8")
                    except UnicodeDecodeError:
                        key = record.key.hex()
                else:
                    key = str(record.key)

                yield KafkaMessage(
                    topic=record.topic,
                    partition=record.partition,
                    offset=record.offset,
                    timestamp_ms=record.timestamp,
                    key=key,
                    headers=self._decode_headers(record.headers),
                    value=value,
                )
        except asyncio.CancelledError:
            log.info("consumer.cancelled")
            raise


async def smoke_test(limit: int = 20) -> None:  # pragma: no cover - manual aid
    """Run as `python -m streamcontext.consumer` to sanity-check the setup."""
    from streamcontext.config import load_settings
    from streamcontext.logging import configure_logging

    settings = load_settings()
    configure_logging(level=settings.log_level, json=False)

    consumer = AvroKafkaConsumer(settings)
    await consumer.start()
    seen = 0
    try:
        async for msg in consumer.messages():
            log.info(
                "consumer.message",
                id=msg.stable_id,
                key=msg.key,
                ts_ms=msg.timestamp_ms,
                value_keys=list(msg.value.keys()) if isinstance(msg.value, dict) else None,
            )
            seen += 1
            if seen >= limit:
                break
    finally:
        await consumer.stop()
        log.info("consumer.smoke_test.done", consumed=seen)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(smoke_test())
