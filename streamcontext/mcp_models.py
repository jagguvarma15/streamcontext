"""Pydantic response models for MCP tools.

Returning structured models (rather than raw dicts) gives agents predictable
field names and lets fastmcp emit a JSON Schema for each tool. This is what
makes tool calls reliable.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class EventCoord(BaseModel):
    """Kafka coordinates for a single record."""

    topic: str
    partition: int = Field(ge=0)
    offset: int = Field(ge=0)
    timestamp_ms: int = Field(ge=0, description="Kafka record timestamp in ms since epoch.")

    @property
    def stable_id(self) -> str:
        return f"{self.topic}:{self.partition}:{self.offset}"


class EventResult(BaseModel):
    """One semantic-search hit with its Kafka coordinates and stored payload."""

    model_config = ConfigDict(extra="forbid")

    coord: EventCoord
    score: float = Field(description="Cosine similarity in [-1, 1]; higher is more relevant.")
    key: str | None = None
    value: dict[str, Any] = Field(
        default_factory=dict,
        description="The stored Kafka record value (already redacted by the gateway).",
    )
    value_truncated: bool = Field(
        default=False,
        description=(
            "True if `value` was too large to return in full and has been "
            "replaced with a truncated stub. Use `find_similar_events` or the "
            "Kafka coordinate to fetch the full record from upstream."
        ),
    )


class FilterClause(BaseModel):
    """One field-level filter applied alongside the semantic match.

    Field names refer to keys inside the message value (e.g. "status",
    "region"). Core Kafka coordinates (`topic`, `partition`, `offset`,
    `timestamp_ms`, `key`) may also be filtered here, though `topic` and
    `time_range_minutes` have dedicated arguments on the tool.

    Exactly one of `eq`, `in_values`, or a range (`gte`/`lte`) should be set.
    """

    model_config = ConfigDict(extra="forbid")

    field: str = Field(min_length=1, max_length=100)
    eq: str | int | float | bool | None = None
    in_values: list[str | int | float | bool] | None = Field(
        default=None,
        description="Match if the field equals any of these values.",
    )
    gte: float | None = Field(default=None, description="Inclusive lower bound (numeric).")
    lte: float | None = Field(default=None, description="Inclusive upper bound (numeric).")


class SearchResponse(BaseModel):
    """Top-level response for `search_events`."""

    model_config = ConfigDict(extra="forbid")

    query: str
    total: int
    truncated: bool = Field(
        default=False,
        description="True if the requested limit exceeded the server-side cap and was clamped.",
    )
    results: list[EventResult]


class TopicInfo(BaseModel):
    """Coarse stats for one ingested topic, with optional catalog enrichment."""

    model_config = ConfigDict(extra="forbid")

    name: str
    count: int = Field(ge=0, description="Approximate number of records in the vector store.")
    oldest_timestamp_ms: int | None = Field(
        default=None, description="Earliest record timestamp in this topic, if known."
    )
    newest_timestamp_ms: int | None = Field(
        default=None, description="Most recent record timestamp in this topic, if known."
    )
    description: str | None = Field(
        default=None,
        description=(
            "One- or two-sentence inferred description of the topic from the "
            "semantic catalog. None if inference is disabled or has not run."
        ),
    )
    description_confidence: float | None = Field(
        default=None, ge=0.0, le=1.0,
        description="Self-reported confidence of the inferred description.",
    )


class TopicsResponse(BaseModel):
    """Top-level response for `list_topics`."""

    model_config = ConfigDict(extra="forbid")

    topics: list[TopicInfo]


class TopicMatch(BaseModel):
    """One ranked match returned by `find_topics_by_purpose`."""

    model_config = ConfigDict(extra="forbid")

    name: str
    score: float = Field(
        description=(
            "Cosine similarity between the query embedding and the topic "
            "description embedding. Higher is more relevant."
        ),
    )
    description: str | None = None
    description_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    description_source: str = Field(
        default="inferred",
        description=(
            "Where the embedded description came from. 'inferred' = catalog "
            "LLM annotation; 'synthesized' = built from topic name + field "
            "names when no inferred description is available."
        ),
    )


class FindTopicsResponse(BaseModel):
    """Top-level response for `find_topics_by_purpose`."""

    model_config = ConfigDict(extra="forbid")

    query: str
    total: int
    matches: list[TopicMatch]


class RelationshipInfo(BaseModel):
    """One detected relationship between two topics."""

    model_config = ConfigDict(extra="forbid")

    source_topic: str
    target_topic: str
    relationship_type: str = Field(
        description=(
            "One of 'shared_key', 'foreign_reference', 'event_chain', 'semantic'."
        ),
    )
    source_field: str | None = None
    target_field: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str | None = None


class RelationshipsResponse(BaseModel):
    """Top-level response for `get_topic_relationships`."""

    model_config = ConfigDict(extra="forbid")

    topic: str
    total: int
    relationships: list[RelationshipInfo]


class FieldExplanation(BaseModel):
    """Top-level response for `explain_field`."""

    model_config = ConfigDict(extra="forbid")

    topic: str
    field: str
    type: str
    nullable: bool = False
    doc: str | None = Field(
        default=None, description="Doc string from Schema Registry, if present."
    )
    inferred_meaning: str | None = Field(
        default=None,
        description=(
            "LLM-inferred meaning of the field. 'unknown' if the model could "
            "not determine the meaning from the schema and samples."
        ),
    )
    inferred_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    example_values: list[Any] = Field(
        default_factory=list,
        description=(
            "Up to 5 representative values drawn from recent sample messages."
        ),
    )


class SchemaField(BaseModel):
    """One field in an Avro record schema, flattened for agent consumption."""

    model_config = ConfigDict(extra="forbid")

    name: str
    type: str
    doc: str | None = None
    nullable: bool = False
    inferred_meaning: str | None = Field(
        default=None,
        description=(
            "Catalog-inferred meaning of this field, or None when the catalog "
            "has not annotated it."
        ),
    )
    inferred_confidence: float | None = Field(default=None, ge=0.0, le=1.0)


class SchemaSummary(BaseModel):
    """The latest registered Avro value-schema for a topic, summarized."""

    model_config = ConfigDict(extra="forbid")

    subject: str
    version: int | None = None
    schema_id: int | None = None
    fields: list[SchemaField]


class TopicDescription(BaseModel):
    """Top-level response for `describe_topic`."""

    model_config = ConfigDict(extra="forbid")

    name: str
    count: int = Field(ge=0)
    oldest_timestamp_ms: int | None = None
    newest_timestamp_ms: int | None = None
    schema_summary: SchemaSummary | None = Field(
        default=None,
        description=(
            "Latest registered value-schema for this topic. None if Schema "
            "Registry is unreachable or no schema is registered."
        ),
    )
    samples: list[EventResult] = Field(default_factory=list)
    description: str | None = Field(
        default=None,
        description=(
            "Catalog-inferred natural-language summary of the topic, when "
            "available. None when inference is disabled or has not run."
        ),
    )
    description_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    inference_status: str | None = Field(
        default=None,
        description=(
            "Catalog inference status: 'pending', 'inferred', 'disabled', "
            "or 'failed'. None when the catalog has no record of this topic."
        ),
    )


class ToolError(BaseModel):
    """Structured error returned by tools instead of raising.

    Agents handle structured errors better than exceptions; the assistant gets
    a clear `code` it can branch on.
    """

    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
