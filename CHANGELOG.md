# Changelog

All notable changes to streamcontext are documented here. Format roughly follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.2.0a4] - MCP layer hardening and v0.2 audit

Day 5 of the Week 2 plan. Makes the MCP server safe to expose to a real agent in a real environment.

### Added
- `streamcontext.rate_limit.TokenBucket` and `ToolRateLimiter`: per-tool token-bucket rate limiting with smooth refill. Configured via `SC_MCP_RATE_LIMIT_PER_MINUTE` (default 120/min, 0 disables). Denied calls return `ToolError(code="rate_limited")` with retry-after in seconds.
- `streamcontext.embedder.CachedEmbedder`: LRU wrapper around the `Embedder` protocol keyed on the exact query string. Configured via `SC_MCP_EMBED_CACHE_SIZE` (default 256, 0 disables). The MCP server wraps its embedder in this automatically; the ingestion gateway does not.
- Per-record value-size cap. Search results whose `value` JSON exceeds `SC_MCP_MAX_VALUE_BYTES` (default 8192) are replaced with a `_truncated` stub carrying `_size_bytes` and `_preview`. `EventResult.value_truncated` is the structured signal for agents.
- `docs/audit-v0.2.md`: second-pass security and gap audit covering the MCP layer. Twelve security findings (nine resolved in v0.2.0a*, two tracked for v0.2.x, one deferred) plus an adversarial-pattern checklist.
- Tests in `tests/test_hardening.py` for token bucket (initial burst, refill, per-tool isolation, disabled mode), cached embedder (hits, mixed batch, LRU eviction, disabled mode), value truncation (small pass-through, oversize stub, disabled mode). Integration coverage in `tests/test_mcp_search.py::test_search_events_truncates_oversize_value`.

### Changed
- `SearchEngine` takes a `max_value_bytes` parameter and applies truncation to every result it returns (`search_events`, `find_similar_events`, `describe_topic` samples).
- `mcp_main.py` wraps the embedder in `CachedEmbedder` before passing it to the engine; startup log line now reports cache size, rate limit, value cap, and SR availability.

## [0.2.0a3] - Structured filters, value-level indexes, MMR rerank

Day 4 of the Week 2 plan. Makes search results good, not just present.

### Added
- `FilterClause` Pydantic model (`field`, plus exactly one of `eq`, `in_values`, or `gte`/`lte`).
- `search_events` accepts a `filters` argument (max 10 clauses) and a `diverse` boolean. Filters AND together with the existing topic / time-range constraints.
- Field-name normalization: user-facing field names like `status` or `region` are auto-prefixed with `value.` so agents do not need to know the payload layout. Core Kafka coordinates and explicit dotted paths pass through unchanged.
- `SC_PAYLOAD_INDEX_FIELDS` config (csv). The sink creates idempotent `value.<field>` keyword indexes at startup so filter+vector queries run at index speed.
- Maximal-marginal-relevance rerank (`_mmr_rerank`). When `diverse=true`, the engine pulls 3x candidates with vectors and selects the top-K balancing relevance against per-result novelty (lambda=0.7).
- `docs/example-conversations.md` walking three adversarial query patterns plus a follow-up similar-events flow.
- Tests for field normalization, the three FilterClause shapes (`eq`, `in_values`, `gte`/`lte`), filter-clause rejection on empty inputs, structured filters AND'd with time-range, MMR diversity vs. duplicate fallback, MMR's relevance-only fallback when vectors are missing.

### Notes
- An empty `SC_PAYLOAD_INDEX_FIELDS` still works; filters fall back to full-collection scans. The MCP tool logs `n_filters` and `diverse` so slow queries are easy to spot.

## [0.2.0a2] - Expanded MCP tool surface

Three more agent-callable tools and the Qdrant indexes that make them fast.

### Added
- `list_topics` MCP tool. Returns one `TopicInfo` per allowlisted topic (or per discovered topic when no allowlist is set), with approximate count and oldest/newest timestamps.
- `describe_topic` MCP tool. Returns count, time window, sample records, and a flattened Avro schema summary fetched from Schema Registry when reachable.
- `find_similar_events` MCP tool. Given a `topic:partition:offset` reference, retrieves the stored vector and runs a similarity search, excluding the reference itself. Cross-allowlist queries return `not_found` rather than leaking topic existence.
- `EventNotFoundError` and `_parse_reference_id` in `mcp_search.py`.
- `TopicInfo`, `TopicsResponse`, `SchemaField`, `SchemaSummary`, `TopicDescription` Pydantic models.
- Optional `SchemaRegistryClient` dependency on `SearchEngine`. `mcp_main.py` probes Schema Registry at startup; failures degrade `describe_topic.schema_summary` to `null` rather than failing the tool.
- `QdrantSink._ensure_core_indexes` creates idempotent payload indexes for `topic`, `partition`, and `timestamp_ms` so chronological scrolls and per-topic counts run at index speed.
- Tests for reference-id parsing, allowlisted vs discovered topic listing, off-allowlist describe (no leak), schema flattening, similar-events self-exclusion, similar-events off-allowlist behaviour.

### Changed
- Server `instructions` now describe a four-tool workflow (`list_topics` -> `describe_topic` -> `search_events` / `find_similar_events`) so tool-using agents pick the right entry point.

## [0.2.0a1] - MCP server foundation

First slice of v0.2: a separate MCP-server process that exposes the streamcontext vector store as agent-callable tools.

### Added
- `streamcontext.mcp_search.SearchEngine` - pure search logic decoupled from the MCP transport so it is unit-testable with fakes.
- `streamcontext.mcp_server.build_server` - FastMCP wrapper exposing `search_events(query, limit?, topic?, time_range_minutes?, score_threshold?)`. Returns `SearchResponse` or structured `ToolError`. Per-tool wall-clock timeout configurable via `SC_MCP_TOOL_TIMEOUT_SEC`.
- `streamcontext.mcp_main` - process entrypoint. Stdio transport by default (Claude Desktop / Cursor / Cline); `--transport sse --host --port` for HTTP hosts.
- `streamcontext.mcp_models` - Pydantic response schemas (`EventCoord`, `EventResult`, `SearchResponse`, `ToolError`).
- Topic allowlist enforcement at the engine layer (`SC_MCP_TOPIC_ALLOWLIST`). Off-allowlist topics are rewritten to a sentinel filter that cannot match anything, so responses cannot leak that the topic exists.
- Server-side caps: `SC_MCP_MAX_RESULTS`, `SC_MCP_MAX_TIME_RANGE_MINUTES`. Inputs above the cap are clamped (not rejected) and `truncated=true` is set on the response.
- `docs/mcp-setup.md` with the `claude_desktop_config.json` block, debugging notes, and SSE instructions.
- Unit tests for engine input clamping, topic-allowlist enforcement, time-range translation, score threshold pass-through, blank-query short-circuit.
- `streamcontext-mcp` console script.

### Changed
- `docs/architecture.md` rewritten around the two-process picture (ingestion + MCP), with the topic-allowlist and payload-redaction reasoning called out.

### Notes
- v0.2 is alpha. Day 3 expands the tool surface (`list_topics`, `describe_topic`, `find_similar_events`). Day 4 adds payload indexes for fast filter+vector search and MMR. Day 5 adds rate limiting and the embedding LRU. Day 6 ships docs and `docs/security.md`. The full plan lives in the Week 2 document.

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
