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


class ToolError(BaseModel):
    """Structured error returned by tools instead of raising.

    Agents handle structured errors better than exceptions; the assistant gets
    a clear `code` it can branch on.
    """

    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
