# Changelog

All notable changes to streamcontext are documented here. Format roughly follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.3.0] - The semantic catalog

streamcontext is now three cooperating processes: the v0.2 ingestion gateway and MCP server, plus a new catalog refresher that turns every topic into something an agent can reason about. Agents stop guessing at field names and start querying a catalog that knows the shape, the meaning, and the relationships of every topic in the cluster.

### Now possible
- Multi-topic agent questions ("find all failed payments and the orders they're attached to") — the catalog encodes the join graph once so the agent does not have to read three Avro schemas to find it.
- Field-level reasoning ("what does `risk_score` actually mean?") — every field carries an LLM-inferred meaning and a confidence in [0, 1], plus a handful of real example values drawn from samples.
- Purpose-based discovery ("where is the billing data?") — `find_topics_by_purpose` embeds the user's intent and ranks topics by similarity to their inferred descriptions, with a synthesized fallback when inference is disabled.
- Automatic relationship discovery — shared keys, foreign references, and (optionally) LLM-detected semantic links like `payment_attempts ↔ order_completions` even when no field names overlap.

### Added — semantic catalog (`streamcontext.catalog`)
- `CatalogStore`: SQLite-backed persistence (WAL mode, one file shared between the refresher and the MCP server). Tables for topics, fields, samples, activity, relationships, an inference cache, and a daily LLM-spend ledger.
- `SchemaIntrospector`: walks Schema Registry, flattens Avro records (nested fields, arrays-of-records, unions-with-null) into dotted-path `FieldEntry` rows, and computes a SHA-256 fingerprint over the canonical schema JSON.
- `MessageSampler`: short-lived `aiokafka` consumer with a fresh group id and `auto_offset_reset=latest`, so the catalog never disturbs production offsets.
- `ActivityProfiler`: rolling counters (last hour, last day) derived from the Qdrant payload — no additional Kafka load.
- `InferenceEngine`: LLM-powered topic descriptions and per-field meanings. Cached by `(schema_fingerprint, sample_hash)` so identical inputs never bill twice. Bounded prompt size; structured JSON output with confidence scores. Providers: `AnthropicProvider` (default, Claude Haiku), `OpenAIProvider`, `LocalLLMProvider` (Ollama-compatible).
- `RelationshipDetector`: heuristic shared-key + sample-value overlap, plus an optional LLM polish layer for semantic links the heuristic cannot see. Results stored with type (`shared_key`, `foreign_reference`, `event_chain`, `semantic`) and confidence.
- `CatalogBuilder`: per-topic refresh orchestrator with per-aspect TTLs (`schema`, `samples`, `stats`, `inference` each track their own freshness).
- `streamcontext.catalog.refresher`: `python -m streamcontext.catalog.refresher` entrypoint, one-shot or `--loop`.

### Added — MCP layer
- New tools: `find_topics_by_purpose(description, limit?)`, `get_topic_relationships(topic)`, `explain_field(topic, field)`. Catalog-aware enrichment of `list_topics` (descriptions surface on every entry) and `describe_topic` (inferred meanings overlaid on the Schema Registry view, inference status reported).
- `CatalogReader` (`streamcontext.mcp_catalog`): read-only wrapper around `CatalogStore` for the MCP server. Honours the same `SC_MCP_TOPIC_ALLOWLIST`; relationships pointing to off-allowlist topics are filtered out so the catalog cannot leak topic names.
- `streamcontext.mcp_server.build_server(authorize=...)`: optional async hook called before every tool. Composed with the rate limiter via the testable `make_gate` helper. Default no-op preserves v0.2 behaviour; downstream deployments needing per-caller auth can plug a real check in without forking.
- New response models: `TopicMatch`, `FindTopicsResponse`, `RelationshipInfo`, `RelationshipsResponse`, `FieldExplanation`. `SchemaField` and `TopicDescription` gained inferred-meaning/confidence/status fields.

### Added — privacy and cost controls
- `streamcontext.catalog.privacy`: centralized PII redaction (field-name drop + regex masking). Built-in patterns for emails, phone numbers, 13–19 digit card numbers, and SSNs. Operator-supplied patterns via `SC_CATALOG_PII_PATTERNS`; field allowlist via `SC_CATALOG_PII_FIELDS`.
- Samples are redacted **before** they land in SQLite — not just before LLM submission. The inference layer reapplies the same patterns defensively.
- `SC_CATALOG_RETAIN_SAMPLES=false` keeps metadata only: samples flow through in-memory for inference and are discarded.
- `SC_CATALOG_LLM_DAILY_CEILING_USD` enforces a daily spend cap per provider, persisted in the catalog's SQLite ledger. Once tripped, inference returns `disabled` status and agents see schema-only entries until UTC rollover.
- `docs/data-handling.md` documents what data each surface holds and the default retention policies for the supported LLM providers.

### Added — documentation
- `docs/catalog.md`: catalog deep-dive (process model, aspects, configuration, PII, cost ceiling, staleness contract, operational notes).
- `docs/audit-v0.3.md`: third-pass audit covering cost, privacy, correctness, and operational findings. All v0.3 blockers resolved; deferred items tracked.
- `docs/architecture.md`: rewritten around the three-process picture.
- `docs/example-conversations.md`: three new conversation patterns showing catalog-driven discovery, multi-topic joins, and field explanation.
- `docs/blog/why-kafka-needs-a-semantic-catalog.md`: positioning piece on how this differs from DataHub / Atlan / Alation.
- `docs/demo-script.md`: three-minute walkthrough storyboard for the release video.
- README rewritten around the self-describing-streams framing; semantic-catalog config table; updated tool table; v0.3.0 status badge.

### Configuration (new env vars)
- `SC_CATALOG_DB_PATH`, `SC_CATALOG_TOPICS`, `SC_CATALOG_SCHEMA_REFRESH_SEC`, `SC_CATALOG_SAMPLE_REFRESH_SEC`, `SC_CATALOG_STATS_REFRESH_SEC`, `SC_CATALOG_INFERENCE_REFRESH_SEC`.
- `SC_CATALOG_SAMPLE_COUNT`, `SC_CATALOG_SAMPLE_TIMEOUT_SEC`, `SC_CATALOG_RETAIN_SAMPLES`, `SC_CATALOG_ENABLE_SAMPLING`.
- `SC_CATALOG_LLM_PROVIDER`, `SC_CATALOG_LLM_MODEL`, `SC_CATALOG_LLM_DAILY_CEILING_USD`, `SC_CATALOG_LLM_MAX_INPUT_TOKENS`.
- `SC_CATALOG_PII_FIELDS`, `SC_CATALOG_PII_PATTERNS`.
- `SC_CATALOG_RELATIONSHIP_MIN_OVERLAP`, `SC_CATALOG_RELATIONSHIP_LLM_THRESHOLD`.

### Testing
- 60+ new unit tests across `tests/test_catalog.py`, `test_catalog_inference.py`, `test_catalog_relationships.py`, `test_catalog_privacy.py`, `test_mcp_catalog.py`, `test_mcp_authorization.py`. End-to-end cost-ceiling recovery test (`test_inference_disables_after_ceiling_and_recovers`) verifies the engine flips to `disabled` once spend crosses the cap and re-enables when the cap is lifted. All collaborators (Schema Registry, Kafka sampler, Qdrant, LLM providers) faked — no network in CI.
- Full suite: 109 passing (ignoring `test_pipeline.py`, which depends on `aiokafka` and is unchanged from v0.2).

### Notes
- v0.3 keeps the trusted-host threat model. The new `authorize` hook is the seam for per-caller auth in a downstream deployment; no auth is shipped in-tree.
- The catalog is **off by default**: with `SC_CATALOG_LLM_PROVIDER=disabled` (default), only deterministic catalog features (schema introspection, sample storage, activity stats, heuristic relationships) run. Enable a provider deliberately and pick one whose data-retention policy matches your requirements.
- Practical day-zero cost on a 10-topic cluster with Claude Haiku: well under $0.05 once the cache is warm.

## [0.2.0] - Week 2 cut: MCP server, audited and hardened

Consolidates the four `0.2.0a*` previews into a single release. streamcontext is now two processes - the ingestion gateway from v0.1 plus a new MCP server - sharing a Qdrant collection. MCP-compatible agents (Claude Desktop, Cursor, Cline, custom) query the vector store as a tool.

### Added (highlights, see prior 0.2.0a entries for the running log)
- MCP server (`streamcontext.mcp_main`) over stdio (default) or SSE. Four agent-callable tools: `list_topics`, `describe_topic`, `search_events`, `find_similar_events`.
- `SearchEngine` with topic allowlist enforcement, server-side caps (`limit`, `time_range_minutes`), structured `FilterClause` translation, automatic `value.` field-name normalization, MMR rerank for diverse results, optional Schema Registry integration, per-record value-size truncation.
- Defense-in-depth controls: per-tool token-bucket rate limiter (`SC_MCP_RATE_LIMIT_PER_MINUTE`), LRU embed cache (`SC_MCP_EMBED_CACHE_SIZE`), per-tool wall-clock timeouts (`SC_MCP_TOOL_TIMEOUT_SEC`), `EventResult.value_truncated` signal (`SC_MCP_MAX_VALUE_BYTES`).
- Sink-level payload indexes (`SC_PAYLOAD_INDEX_FIELDS`) for fast filter-plus-vector queries.
- `docs/architecture.md` rewritten around the two-process picture, `docs/mcp-setup.md`, `docs/example-conversations.md`, `docs/audit-v0.2.md`, `docs/security.md`.
- 30+ new unit tests covering the engine, filter translation, MMR, rate limiter, embed cache, and value truncation. Existing integration test still gated on `RUN_INTEGRATION=1`.

### Audit summary
- `docs/audit-v0.1.md` (gateway): twelve security findings, nine resolved in v0.1.1, three tracked for v0.2.x (Kafka SASL, SR auth, `/metrics`).
- `docs/audit-v0.2.md` (MCP layer): twelve security findings and four functional findings. Nine of the security findings are resolved in v0.2.0; two tracked for v0.2.x (concurrency semaphore, SSE auth); one deferred (per-tool rate-limit knobs).

### Notes
- Single bundled release rather than four separate cut tags, on user direction. The `0.2.0a*` entries below preserve the per-day running log for anyone walking the history.

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
