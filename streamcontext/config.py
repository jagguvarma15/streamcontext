"""Typed, env-driven configuration for the streamcontext gateway."""

from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration for the gateway.

    Loaded from environment variables (prefixed `SC_`) and an optional `.env`
    file. See `.env.example` for the full list.
    """

    model_config = SettingsConfigDict(
        env_prefix="SC_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Kafka ---
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topics: str = "orders"
    kafka_group_id: str = "streamcontext-gateway"
    kafka_auto_offset_reset: Literal["earliest", "latest"] = "earliest"

    # --- Schema Registry ---
    schema_registry_url: str = "http://localhost:8081"

    # --- Embedder ---
    embedder_provider: Literal["local", "openai"] = "local"
    embedder_model: str = "all-MiniLM-L6-v2"

    # --- Vector Sink ---
    sink_provider: Literal["qdrant"] = "qdrant"
    qdrant_url: str = "http://localhost:6333"
    qdrant_collection: str = "streamcontext"
    qdrant_vector_dim: int = 384

    # --- Pipeline ---
    batch_size: int = Field(default=32, ge=1, le=1024)
    batch_flush_interval_sec: float = Field(default=1.0, gt=0)

    # --- Payload redaction (defense in depth) ---
    # Comma-separated list of field names to drop from the message value before
    # it is written to the vector store payload. Field matching is case-sensitive
    # and applies recursively to nested dicts and to dicts inside lists.
    payload_redact_fields: str = ""
    # Whether to copy Kafka headers into the vector store payload. Off by default
    # because headers commonly carry auth tokens or trace context.
    payload_include_headers: bool = False
    # Comma-separated value-level fields to keyword-index in the vector store.
    # Names refer to keys inside the message value (e.g. "status,region"); the
    # sink prefixes them with "value." automatically when creating the index.
    # Without an index, filters still work but run a full-collection scan.
    payload_index_fields: str = ""

    # --- MCP server (v0.2) ---
    # Comma-separated allowlist of topic names the MCP server is permitted to
    # search. If empty, the server logs a startup warning and applies no topic
    # restriction. Operators are expected to set this to the subset of topics
    # they want exposed to agents — usually the ingested topics minus anything
    # sensitive.
    mcp_topic_allowlist: str = ""
    # Hard cap on the `limit` argument any MCP search tool will accept.
    mcp_max_results: int = Field(default=100, ge=1, le=1000)
    # Hard cap on time-range filters in minutes (default: 7 days).
    mcp_max_time_range_minutes: int = Field(default=10_080, ge=1)
    # Hard cap on per-tool wall time. Tools time out and return a structured
    # error rather than hanging the agent.
    mcp_tool_timeout_sec: float = Field(default=5.0, gt=0)
    # Per-tool token-bucket rate limit, in invocations per minute. Each tool
    # name gets its own bucket. Set to 0 to disable.
    mcp_rate_limit_per_minute: int = Field(default=120, ge=0)
    # LRU size for the embedder query cache on the MCP path. 0 disables.
    mcp_embed_cache_size: int = Field(default=256, ge=0)
    # Approximate per-record JSON byte cap on the `value` field returned by
    # search results. Anything larger is replaced with a truncated stub
    # carrying the original size. Keeps a single huge record from blowing out
    # the agent's context window.
    mcp_max_value_bytes: int = Field(default=8192, ge=256)

    # --- Observability ---
    log_level: str = "INFO"
    log_json: bool = True

    @property
    def topics_list(self) -> list[str]:
        return [t.strip() for t in self.kafka_topics.split(",") if t.strip()]

    @property
    def redact_fields_set(self) -> frozenset[str]:
        return frozenset(f.strip() for f in self.payload_redact_fields.split(",") if f.strip())

    @property
    def index_fields_list(self) -> list[str]:
        return [f.strip() for f in self.payload_index_fields.split(",") if f.strip()]

    @property
    def mcp_topic_allowlist_set(self) -> frozenset[str]:
        return frozenset(t.strip() for t in self.mcp_topic_allowlist.split(",") if t.strip())


def load_settings() -> Settings:
    return Settings()
