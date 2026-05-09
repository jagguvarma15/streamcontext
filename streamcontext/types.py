"""Shared dataclasses for messages flowing through the pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class KafkaMessage:
    """One deserialized Kafka record with full metadata preserved.

    The pipeline carries this all the way to the sink so the vector store can
    be queried by topic/partition/offset/timestamp without round-tripping back
    to Kafka.
    """

    topic: str
    partition: int
    offset: int
    timestamp_ms: int
    key: str | None
    headers: dict[str, str]
    value: dict[str, Any]

    @property
    def stable_id(self) -> str:
        """Deterministic ID derived from coordinates — safe for upsert dedup."""
        return f"{self.topic}:{self.partition}:{self.offset}"


@dataclass(slots=True)
class VectorRecord:
    """An embedded message ready to upsert into the vector store."""

    id: str
    vector: list[float]
    payload: dict[str, Any] = field(default_factory=dict)
