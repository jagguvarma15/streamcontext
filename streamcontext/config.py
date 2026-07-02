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
    # Dead-letter topic for messages that fail Avro deserialization. Empty
    # disables the DLQ (failures are logged only). When set, the raw message is
    # republished to this topic with sc_origin_* and sc_error headers.
    kafka_dlq_topic: str = ""
    # Kafka client security. PLAINTEXT (default) needs no credentials; the other
    # protocols enable SASL and/or TLS. Applied to the gateway consumer, the DLQ
    # producer, and the catalog sampler.
    kafka_security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    kafka_ssl_cafile: str = ""
    kafka_ssl_certfile: str = ""
    kafka_ssl_keyfile: str = ""

    # --- Schema Registry ---
    schema_registry_url: str = "http://localhost:8081"
    # Basic-auth credentials for a secured Schema Registry (maps to
    # basic.auth.user.info). TLS CA via schema_registry_ssl_cafile.
    schema_registry_user: str = ""
    schema_registry_password: str = ""
    schema_registry_ssl_cafile: str = ""

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
    # Max concurrent in-flight calls per tool. 0 disables (unbounded). Bounds
    # simultaneous slow queries and embed calls independent of the rate limit.
    mcp_max_concurrent_calls: int = Field(default=0, ge=0)
    # Bearer token required on the SSE (HTTP) transport. Empty leaves SSE
    # unauthenticated; stdio is local by construction and unaffected.
    mcp_sse_auth_token: str = ""

    # --- Catalog (v0.3) ---
    # Path to the SQLite file that backs the semantic catalog. The refresher
    # process writes it; the MCP server reads from it. Place on a Docker volume
    # if you want it to survive restarts.
    catalog_db_path: str = "/var/lib/streamcontext/catalog.sqlite"
    # Comma-separated list of topics to maintain in the catalog. If empty, the
    # refresher falls back to `SC_KAFKA_TOPICS`.
    catalog_topics: str = ""
    # Refresh cadences (seconds).
    catalog_schema_refresh_sec: int = Field(default=300, ge=10)
    catalog_sample_refresh_sec: int = Field(default=900, ge=10)
    catalog_stats_refresh_sec: int = Field(default=60, ge=5)
    catalog_inference_refresh_sec: int = Field(default=3600, ge=60)
    # How many sample messages to keep per topic.
    catalog_sample_count: int = Field(default=10, ge=1, le=200)
    # Per-call wall budget for the message sampler.
    catalog_sample_timeout_sec: float = Field(default=5.0, gt=0)
    # When false, samples are gathered for inference but never persisted.
    catalog_retain_samples: bool = True
    # When false, the refresher skips the Kafka-side sampler entirely (useful
    # for development environments without a broker).
    catalog_enable_sampling: bool = True
    # Default daily ceiling on LLM spend for catalog inference, USD.
    catalog_llm_daily_ceiling_usd: float = Field(default=1.0, ge=0.0)
    # Comma-separated regex patterns applied to all sampled message text before
    # persistence or LLM submission.
    catalog_pii_patterns: str = ""
    # Comma-separated literal field names always dropped from samples.
    catalog_pii_fields: str = ""
    # LLM provider for inference: 'anthropic', 'openai', 'local', or 'disabled'.
    catalog_llm_provider: Literal["anthropic", "openai", "local", "disabled"] = "disabled"
    catalog_llm_model: str = "claude-haiku-4-5-20251001"
    # Cap on prompt input tokens per inference call.
    catalog_llm_max_input_tokens: int = Field(default=5000, ge=256)
    # Minimum sample-value overlap before a shared-key relationship is emitted.
    catalog_relationship_min_overlap: float = Field(default=0.2, ge=0.0, le=1.0)
    # Minimum LLM-reported confidence before a semantic relationship is kept.
    catalog_relationship_llm_threshold: float = Field(default=0.6, ge=0.0, le=1.0)

    # --- Observability ---
    log_level: str = "INFO"
    log_json: bool = True
    # Prometheus /metrics + /health server on the gateway and catalog refresher.
    # Binds 127.0.0.1 by default; set 0.0.0.0 to scrape from another host (and
    # control exposure at the network layer). Co-located processes need distinct
    # ports. A bind failure is logged, not fatal.
    metrics_enabled: bool = True
    metrics_host: str = "127.0.0.1"
    metrics_port: int = Field(default=9108, ge=1, le=65535)

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

    @property
    def catalog_topics_list(self) -> list[str]:
        return [t.strip() for t in self.catalog_topics.split(",") if t.strip()]

    @property
    def catalog_pii_patterns_list(self) -> list[str]:
        return [p for p in self.catalog_pii_patterns.split(",") if p.strip()]

    @property
    def catalog_pii_fields_list(self) -> list[str]:
        return [f.strip() for f in self.catalog_pii_fields.split(",") if f.strip()]


def load_settings() -> Settings:
    return Settings()
