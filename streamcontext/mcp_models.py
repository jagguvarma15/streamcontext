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
    """Coarse stats for one ingested topic."""

    model_config = ConfigDict(extra="forbid")

    name: str
    count: int = Field(ge=0, description="Approximate number of records in the vector store.")
    oldest_timestamp_ms: int | None = Field(
        default=None, description="Earliest record timestamp in this topic, if known."
    )
    newest_timestamp_ms: int | None = Field(
        default=None, description="Most recent record timestamp in this topic, if known."
    )


class TopicsResponse(BaseModel):
    """Top-level response for `list_topics`."""

    model_config = ConfigDict(extra="forbid")

    topics: list[TopicInfo]


class SchemaField(BaseModel):
    """One field in an Avro record schema, flattened for agent consumption."""

    model_config = ConfigDict(extra="forbid")

    name: str
    type: str
    doc: str | None = None


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


class ToolError(BaseModel):
    """Structured error returned by tools instead of raising.

    Agents handle structured errors better than exceptions; the assistant gets
    a clear `code` it can branch on.
    """

    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
