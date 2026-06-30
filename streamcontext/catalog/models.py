"""Pydantic models for catalog entries.

These types travel through the catalog store, the inference layer, and the
MCP tool responses. Field names should read well when an LLM sees them in
JSON form.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

InferenceStatus = Literal["pending", "inferred", "disabled", "failed"]
RelationshipType = Literal["shared_key", "foreign_reference", "event_chain", "semantic"]


class FieldEntry(BaseModel):
    """One field on a topic, including inferred annotations when available."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(description="Dotted field path (e.g. 'order.customer_id').")
    type: str = Field(description="Avro/JSON type tag, flattened for readability.")
    nullable: bool = False
    default: Any | None = None
    doc: str | None = Field(
        default=None, description="Doc string from Schema Registry, if present."
    )
    inferred_meaning: str | None = Field(
        default=None,
        description=(
            "LLM-inferred natural-language meaning of this field. 'unknown' "
            "when the LLM could not determine the meaning."
        ),
    )
    inferred_confidence: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Self-reported confidence in the inferred meaning, 0..1.",
    )


class SampleMessage(BaseModel):
    """One sampled message from a topic, used for both inference and display."""

    model_config = ConfigDict(extra="forbid")

    partition: int = Field(ge=0)
    offset: int = Field(ge=0)
    timestamp_ms: int = Field(ge=0)
    key: str | None = None
    value: dict[str, Any] = Field(default_factory=dict)


class ActivityStats(BaseModel):
    """Rolling activity counters for a topic."""

    model_config = ConfigDict(extra="forbid")

    messages_last_hour: int = 0
    messages_last_day: int = 0
    rate_per_minute_last_hour: float = 0.0
    observed_schema_versions: list[int] = Field(default_factory=list)
    last_observed_ts_ms: int | None = None


class TopicEntry(BaseModel):
    """The catalog entry for a single Kafka topic."""

    model_config = ConfigDict(extra="forbid")

    name: str
    schema_subject: str | None = None
    schema_id: int | None = None
    schema_version: int | None = None
    schema_fingerprint: str | None = Field(
        default=None,
        description=(
            "SHA-256 fingerprint of the canonical schema JSON. Used as a cache "
            "key for inference. Stable across processes."
        ),
    )
    fields: list[FieldEntry] = Field(default_factory=list)
    samples: list[SampleMessage] = Field(default_factory=list)
    activity: ActivityStats = Field(default_factory=ActivityStats)
    description: str | None = Field(
        default=None, description="LLM-inferred natural-language summary of the topic."
    )
    description_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    inference_status: InferenceStatus = "pending"
    last_schema_refresh_ms: int | None = None
    last_sample_refresh_ms: int | None = None
    last_stats_refresh_ms: int | None = None
    last_inference_refresh_ms: int | None = None


class RelationshipEntry(BaseModel):
    """A detected relationship between two topics.

    The relationship is directional from `source_topic` to `target_topic`; for
    symmetric relationships (shared_key) both directions are stored.
    """

    model_config = ConfigDict(extra="forbid")

    source_topic: str
    target_topic: str
    relationship_type: RelationshipType
    source_field: str | None = None
    target_field: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str | None = None
    last_refresh_ms: int | None = None


class CatalogConfig(BaseModel):
    """Refresh cadences and runtime knobs for the catalog."""

    model_config = ConfigDict(extra="forbid")

    schema_refresh_sec: int = 300
    sample_refresh_sec: int = 900
    stats_refresh_sec: int = 60
    inference_refresh_sec: int = 3600
    sample_count: int = 10
    retain_samples: bool = True
    daily_llm_spend_ceiling_usd: float = 1.0
    pii_redact_patterns: list[str] = Field(default_factory=list)
    pii_redact_fields: list[str] = Field(default_factory=list)
