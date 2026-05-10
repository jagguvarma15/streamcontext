# Changelog

All notable changes to streamcontext are documented here. Format roughly follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.1] - Pre-MCP audit cut

Driven by `docs/audit-v0.1.md` ahead of v0.2's MCP work. Four block-Week-2 issues fixed; others tracked.

### Added
- `streamcontext.errors` module with `ConfigurationError` and `PipelineFatalError`.
- `streamcontext.redaction.redact()` recursive helper.
- `SC_PAYLOAD_REDACT_FIELDS` (csv) and `SC_PAYLOAD_INCLUDE_HEADERS` (bool, default false) settings. Headers no longer flow into the vector store payload by default.
- Startup validation that the embedder's actual output dim matches `SC_QDRANT_VECTOR_DIM`. Mismatch now raises `ConfigurationError` and exits 78 (sysexits `EX_CONFIG`) instead of crashing mid-batch.
- Process-alive healthcheck for the gateway service in `docker-compose.yml`. Runtime image installs `procps` for `pgrep`.
- Tests for redaction (nested dicts and lists), header inclusion toggle, dim validation, and the new fatal-halt path.
- `docs/audit-v0.1.md` covering security and gap findings, with categorization (block / fix in v0.2 / defer / resolved).

### Changed
- Pipeline now raises `PipelineFatalError` and halts on persistent embed/sink/commit failure instead of silently dropping the batch and advancing past it on the next successful batch.
- `_build_record` signature now requires the redaction set and a header-inclusion flag.

### Fixed
- Silent batch loss when retries are exhausted (S2 in `audit-v0.1.md`).

## [0.1.0] - Week 1 cut

First public alpha. Streaming sink only — MCP server lands in v0.2.

### Added
- Async Kafka consumer (`aiokafka`) with Avro deserialization via Confluent Schema Registry.
- Pluggable `Embedder` protocol with two implementations:
  - `LocalEmbedder` — sentence-transformers (default `all-MiniLM-L6-v2`, 384-dim).
  - `OpenAIEmbedder` — opt-in via `pip install streamcontext[openai]`.
- Pluggable `VectorSink` protocol with a `QdrantSink` implementation. Deterministic UUID5 point IDs derived from `(topic, partition, offset)` for safe replay/upsert.
- Batched pipeline orchestrator: flushes on `batch_size` OR `batch_flush_interval_sec`. Manual offset commit only after a batch is durably upserted (at-least-once).
- Retry-with-exponential-backoff on embedder and sink failures (`tenacity`).
- Graceful shutdown on SIGINT/SIGTERM with final-batch flush.
- Structured JSON logging (`structlog`) with throughput counters every 10s.
- `examples/producer.py` — synthetic Avro-encoded e-commerce order generator.
- `examples/query.py` — natural-language search over the vector store with optional topic filter.
- `docker-compose.yml` — Kafka (KRaft, no Zookeeper), Schema Registry, Qdrant, and the gateway image.
- `Dockerfile` — multi-stage, non-root, lean runtime image.
- Unit tests for canonical text serialization, deterministic IDs, offset accounting, and pipeline batching with fakes. Opt-in (`RUN_INTEGRATION=1`) integration test with `testcontainers` spinning real Kafka + Qdrant.
- Architecture docs (`docs/architecture.md`) covering text extraction, batching, offsets, and deferred layers.

### Known limitations
- Text extraction is whole-record JSON dump. Field selection / Jinja templates land in v0.2.
- Single-broker, single-partition demo defaults. Multi-broker production tuning is not yet documented.
- `OpenAIEmbedder` is wired but untested in CI.
- No DLQ topic yet — deserialization failures are dead-letter-logged only.
- No Prometheus `/metrics` endpoint.
