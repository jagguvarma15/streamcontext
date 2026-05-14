"""Deterministic schema and message introspection.

Pulls Avro schema definitions from Schema Registry and walks them into flat
`FieldEntry` records the catalog (and LLM prompts) can reason over. Also
contains a sampling helper that opens a short-lived Kafka consumer per topic.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, Protocol

from streamcontext.catalog.models import FieldEntry, SampleMessage
from streamcontext.logging import get_logger

log = get_logger("streamcontext.catalog.introspect")


class _SchemaRegistryLike(Protocol):
    def get_latest_version(self, subject_name: str) -> Any: ...


class SchemaIntrospector:
    """Reads schemas from SR and flattens them to `FieldEntry` records.

    Nested records produce dotted paths (e.g. `address.city`). Arrays of
    records flatten to `field[].subfield`. Union types collapse to the
    non-null branch's type with `nullable=True`.
    """

    def __init__(self, schema_registry: _SchemaRegistryLike | None) -> None:
        self._sr = schema_registry

    def introspect(self, topic: str) -> tuple[
        str | None, int | None, int | None, str | None, str | None, list[FieldEntry]
    ]:
        """Return (subject, schema_id, version, fingerprint, raw_json, fields).

        Any of the leading fields may be None when Schema Registry is
        unreachable or the subject does not exist. Fields are always returned
        (empty list when no schema is available).
        """
        if self._sr is None:
            return None, None, None, None, None, []
        subject = f"{topic}-value"
        try:
            latest = self._sr.get_latest_version(subject)
        except Exception as exc:
            log.debug("catalog.schema_fetch.failed", topic=topic, error=str(exc))
            return None, None, None, None, None, []

        raw_json = getattr(getattr(latest, "schema", None), "schema_str", None)
        schema_id = getattr(latest, "schema_id", None)
        version = getattr(latest, "version", None)

        parsed: dict[str, Any] | None = None
        fingerprint: str | None = None
        if isinstance(raw_json, str):
            try:
                parsed = json.loads(raw_json)
                canonical = json.dumps(parsed, sort_keys=True, separators=(",", ":"))
                fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            except json.JSONDecodeError:
                parsed = None

        fields: list[FieldEntry] = []
        if isinstance(parsed, dict):
            fields = list(flatten_avro_schema(parsed))
        return subject, schema_id, version, fingerprint, raw_json, fields


def flatten_avro_schema(schema: dict[str, Any]) -> list[FieldEntry]:
    """Walk an Avro record schema into flat `FieldEntry` records."""
    out: list[FieldEntry] = []
    if schema.get("type") != "record":
        return out
    _walk_record(schema, prefix="", out=out)
    return out


def _walk_record(
    record: dict[str, Any], *, prefix: str, out: list[FieldEntry]
) -> None:
    for field in record.get("fields", []) or []:
        if not isinstance(field, dict):
            continue
        name = str(field.get("name", ""))
        if not name:
            continue
        path = f"{prefix}{name}" if not prefix else f"{prefix}.{name}"
        doc = field.get("doc") if isinstance(field.get("doc"), str) else None
        type_descriptor = field.get("type")
        type_label, nullable, nested_record, nested_record_array = _resolve_type(
            type_descriptor
        )
        default = field.get("default") if "default" in field else None
        out.append(
            FieldEntry(
                name=path,
                type=type_label,
                nullable=nullable,
                default=default,
                doc=doc,
            )
        )
        if nested_record is not None:
            _walk_record(nested_record, prefix=path, out=out)
        if nested_record_array is not None:
            _walk_record(nested_record_array, prefix=f"{path}[]", out=out)


def _resolve_type(
    descriptor: Any,
) -> tuple[str, bool, dict[str, Any] | None, dict[str, Any] | None]:
    """Return (type_label, nullable, nested_record, nested_record_array).

    Strips union-with-null to derive nullability. Returns the inner record
    descriptor when the field is a record (for further recursion).
    """
    nullable = False
    if isinstance(descriptor, list):
        non_null = [d for d in descriptor if d != "null"]
        nullable = len(non_null) != len(descriptor)
        if len(non_null) == 1:
            descriptor = non_null[0]
        else:
            labels = [_descriptor_label(d) for d in non_null]
            return f"union<{','.join(labels)}>", nullable, None, None

    if isinstance(descriptor, dict):
        kind = descriptor.get("type")
        if kind == "record":
            return f"record<{descriptor.get('name','anonymous')}>", nullable, descriptor, None
        if kind == "array":
            items = descriptor.get("items")
            if isinstance(items, dict) and items.get("type") == "record":
                return f"array<record<{items.get('name','anonymous')}>>", nullable, None, items
            return f"array<{_descriptor_label(items)}>", nullable, None, None
        if kind == "map":
            return f"map<{_descriptor_label(descriptor.get('values'))}>", nullable, None, None
        if kind == "enum":
            symbols = descriptor.get("symbols") or []
            return f"enum<{','.join(symbols)}>", nullable, None, None
        if kind == "fixed":
            return f"fixed<{descriptor.get('size', 0)}>", nullable, None, None
        if isinstance(kind, str):
            return kind, nullable, None, None
        return "unknown", nullable, None, None

    if isinstance(descriptor, str):
        return descriptor, nullable, None, None

    return "unknown", nullable, None, None


def _descriptor_label(descriptor: Any) -> str:
    if isinstance(descriptor, str):
        return descriptor
    if isinstance(descriptor, dict):
        return str(descriptor.get("type", "unknown"))
    return "unknown"


class MessageSampler:
    """Reads a small number of recent messages from a topic.

    Uses a short-lived `aiokafka` consumer with a fresh group id so the sampler
    never advances the production gateway's offsets. Decoding uses the same
    Schema Registry the gateway already trusts.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        schema_registry_url: str,
        group_id_prefix: str = "streamcontext-catalog-sampler",
        timeout_sec: float = 5.0,
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._sr_url = schema_registry_url
        self._group_prefix = group_id_prefix
        self._timeout = timeout_sec

    async def sample(self, topic: str, count: int = 10) -> list[SampleMessage]:
        if count <= 0:
            return []
        try:
            from aiokafka import AIOKafkaConsumer, TopicPartition
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroDeserializer
            from confluent_kafka.serialization import (
                MessageField,
                SerializationContext,
            )
        except ImportError as exc:
            log.warning("catalog.sampler.unavailable", error=str(exc))
            return []

        deserializer = AvroDeserializer(SchemaRegistryClient({"url": self._sr_url}))
        group_id = f"{self._group_prefix}-{topic}-{int(asyncio.get_event_loop().time()*1000)}"
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self._bootstrap,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            max_poll_records=count,
        )
        await consumer.start()
        samples: list[SampleMessage] = []
        try:
            assignments = consumer.assignment()
            tries = 0
            while not assignments and tries < 10:
                await asyncio.sleep(0.1)
                assignments = consumer.assignment()
                tries += 1
            if not assignments:
                return []

            # Rewind by `count` per partition so we can read up to `count`
            # messages without waiting for new traffic.
            for tp in assignments:
                end = await consumer.end_offsets([tp])
                end_offset = end[tp]
                start_offset = max(0, end_offset - count)
                consumer.seek(tp, start_offset)

            deadline = asyncio.get_event_loop().time() + self._timeout
            while len(samples) < count:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                try:
                    record = await asyncio.wait_for(
                        consumer.getone(), timeout=remaining
                    )
                except asyncio.TimeoutError:
                    break
                try:
                    ctx = SerializationContext(record.topic, MessageField.VALUE)
                    value = deserializer(record.value, ctx) if record.value else None
                except Exception as exc:
                    log.debug(
                        "catalog.sampler.decode_failed",
                        topic=topic,
                        offset=record.offset,
                        error=str(exc),
                    )
                    continue
                if not isinstance(value, dict):
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
                samples.append(
                    SampleMessage(
                        partition=record.partition,
                        offset=record.offset,
                        timestamp_ms=record.timestamp,
                        key=key,
                        value=value,
                    )
                )
        finally:
            await consumer.stop()
        return samples


__all__ = [
    "SchemaIntrospector",
    "MessageSampler",
    "flatten_avro_schema",
]
