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

    # --- Observability ---
    log_level: str = "INFO"
    log_json: bool = True

    @property
    def topics_list(self) -> list[str]:
        return [t.strip() for t in self.kafka_topics.split(",") if t.strip()]

    @property
    def redact_fields_set(self) -> frozenset[str]:
        return frozenset(f.strip() for f in self.payload_redact_fields.split(",") if f.strip())


def load_settings() -> Settings:
    return Settings()
