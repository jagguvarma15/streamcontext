# streamcontext

> **The semantic gateway between Kafka and AI agents.**
> Self-describing event streams: every topic, field, and relationship explained in natural language, queryable by agents — not just by humans reading Avro schemas.

[![ci](https://github.com/jagguvarma15/streamcontext/actions/workflows/ci.yml/badge.svg)](https://github.com/jagguvarma15/streamcontext/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![python](https://img.shields.io/badge/python-3.11%2B-blue)](pyproject.toml)
[![docker](https://img.shields.io/badge/docker-compose-2496ED?logo=docker)](docker-compose.yml)

> A short demo video lives at `docs/demo.mp4` when present. Architecture diagram is in [`docs/architecture.md`](docs/architecture.md); catalog deep-dive in [`docs/catalog.md`](docs/catalog.md).

---

## Why this exists

LLM agents are great at reasoning, terrible at remembering. RAG fixes the memory problem for static knowledge - docs, wikis, codebases. But the most operationally interesting data in any company isn't static: it is flowing through Kafka right now. Orders, clicks, alerts, sensor readings, deploys.

streamcontext is the missing intermediary. Three cooperating processes:

- The **ingestion gateway** continuously embeds Kafka messages into a vector store, preserving full Kafka metadata (topic, partition, offset, timestamp, headers, key) as filterable payload.
- The **catalog refresher** keeps a semantic catalog of every topic — schema, sample messages, activity stats, inferred descriptions, per-field meanings, and detected relationships — in a SQLite file that the MCP server reads.
- The **MCP server** runs alongside your agent host (Claude Desktop, Cursor, Cline, custom) and exposes the data through MCP tools. Agents query the stream like any other knowledge base — except this one is always current and self-describing.

Diagram in [`docs/architecture.md`](docs/architecture.md). Catalog details in [`docs/catalog.md`](docs/catalog.md).

## Quickstart (10 minutes)

Requirements: Docker, Python 3.11+, [`uv`](https://github.com/astral-sh/uv) (or pip).

```bash
git clone https://github.com/jagguvarma15/streamcontext.git
cd streamcontext
cp .env.example .env

# 1. Spin up Kafka + Schema Registry + Qdrant + the ingestion gateway
docker compose up -d

# 2. Local virtualenv for the producer/query scripts and the MCP server
uv venv && source .venv/bin/activate
uv pip install -e .

# 3. Generate synthetic e-commerce orders
python examples/producer.py --rate 5

# 4. Confirm retrieval works without an agent (sanity check)
python examples/query.py "high-value orders from California"

# 5. Populate the semantic catalog (one pass; --loop runs continuously)
python -m streamcontext.catalog.refresher

# 6. Wire up your agent (see "Use with Claude Desktop" below) and start asking questions
```

Step 5 is optional — the deterministic tools work without it — but it unlocks the catalog-backed tools (`find_topics_by_purpose`, `get_topic_relationships`, `explain_field`, plus inferred descriptions on `list_topics`/`describe_topic`). Set `SC_CATALOG_LLM_PROVIDER=anthropic` (or `openai`, or `local`) before running the refresher if you want LLM-derived descriptions; the default is `disabled`, which keeps the catalog strictly deterministic.

Within seconds of step 3 you should see top-K matching orders, each annotated with its Kafka coordinates. The Qdrant dashboard at <http://localhost:6333/dashboard> visualizes the points as they arrive.

### Cleanup

```bash
docker compose down            # stop services, keep volumes
docker compose down -v         # also delete Kafka/Qdrant data
```

## Use with Claude Desktop

Add the following to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%/Claude/claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "streamcontext": {
      "command": "python",
      "args": ["-m", "streamcontext.mcp_main"],
      "env": {
        "SC_QDRANT_URL": "http://localhost:6333",
        "SC_QDRANT_COLLECTION": "streamcontext",
        "SC_QDRANT_VECTOR_DIM": "384",
        "SC_EMBEDDER_PROVIDER": "local",
        "SC_EMBEDDER_MODEL": "all-MiniLM-L6-v2",
        "SC_MCP_TOPIC_ALLOWLIST": "orders",
        "SC_MCP_MAX_RESULTS": "50",
        "SC_MCP_TOOL_TIMEOUT_SEC": "5",
        "SC_LOG_JSON": "false"
      }
    }
  }
}
```

Restart Claude Desktop. The `streamcontext` server appears in the tools panel. Same shape works for Cursor and Cline - see [`docs/mcp-setup.md`](docs/mcp-setup.md) for IDE-specific notes and the SSE transport.

## Available MCP tools

| Tool | What it does |
|---|---|
| `list_topics` | List ingested topics with approximate counts, time windows, and the catalog-inferred description for each. Call this first when the user references unfamiliar data. |
| `describe_topic` | Schema (from Schema Registry when reachable), counts, time window, sample records, the catalog's inferred topic description, and per-field meanings with confidences. |
| `find_topics_by_purpose` | "Where would I find billing data?" Embeds the input and ranks topics by similarity to their catalog descriptions. Falls back to a synthesized description from field names when inference has not run. |
| `get_topic_relationships` | Detected relationships between topics (`shared_key`, `foreign_reference`, `event_chain`, `semantic`) with confidence scores. Use before constructing multi-topic queries. |
| `explain_field` | Inferred meaning of a single field plus example values from samples. Use before writing filter predicates on an unfamiliar field. |
| `search_events` | Semantic search ranked by similarity to a natural-language query. Supports structured `filters`, `topic`, `time_range_minutes`, `score_threshold`, and an MMR-based `diverse` mode for deduping near-identical hits. |
| `find_similar_events` | "More like this one" given a `topic:partition:offset` reference. Useful for incident investigation. |

Every result carries the Kafka coordinate (`coord.topic:coord.partition:coord.offset:coord.timestamp_ms`) so the agent can cite exactly where the record came from.

## Example agent conversations

Patterns the tools are tuned for:

1. **"Find high-value orders from California in the last hour"** — `search_events` with `topic="orders"`, `time_range_minutes=60`, and structured filters `region=US_WEST`, `total>=200`.
2. **"What failed transactions did we see today?"** — `describe_topic` to learn that `status` is an enum, then `search_events` with `filters=[{field: status, in_values: [cancelled, refunded]}]`.
3. **"Anything weird in the last 5 minutes?"** — embed the intent ("unusual, anomalous, or unexpected event"), set `diverse=true`, restrict the time window.
4. **"What kinds of customer data do we have flowing?"** — `list_topics` to see inferred descriptions, then `find_topics_by_purpose` to rank by relevance.
5. **"Find all failed payments and the orders they're attached to"** — `get_topic_relationships` to discover the join field, then two `search_events` calls.
6. **"What does the `risk_score` field actually mean?"** — `explain_field` returns the inferred meaning and a handful of example values.

Walked end-to-end with the tool-call shapes in [`docs/example-conversations.md`](docs/example-conversations.md). Tuning checklist at the bottom of that file.

## How it fits together

```
   ingestion process            MCP process             catalog refresher
   -----------------            -----------             -----------------
   Kafka -> consumer            FastMCP tools ── reads ── CatalogStore
            -> embedder              ^                   (SQLite, WAL)
            -> Qdrant ── shared ────/                    ^
                         state                          writes
                                                          │
                                              SchemaIntrospector,
                                              MessageSampler,
                                              ActivityProfiler,
                                              InferenceEngine,
                                              RelationshipDetector
```

Full design notes — text-extraction strategy, batching, offset semantics, deterministic point IDs, topic allowlist, payload redaction, the catalog process — in [`docs/architecture.md`](docs/architecture.md). Catalog deep-dive in [`docs/catalog.md`](docs/catalog.md). Data handling and provider data policies in [`docs/data-handling.md`](docs/data-handling.md). Security posture in [`docs/security.md`](docs/security.md).

## Configuration

Everything is env-driven. The most useful knobs are below; see [`.env.example`](.env.example) for the full list.

### Ingestion gateway

| Variable | Default | Notes |
|---|---|---|
| `SC_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `SC_KAFKA_TOPICS` | `orders` | Comma-separated. |
| `SC_KAFKA_DLQ_TOPIC` | (empty) | Dead-letter topic for undeserializable messages. Empty = log-and-count only. |
| `SC_SCHEMA_REGISTRY_URL` | `http://localhost:8081` | |
| `SC_EMBEDDER_PROVIDER` | `local` | `local` (sentence-transformers) or `openai`. |
| `SC_EMBEDDER_MODEL` | `all-MiniLM-L6-v2` | Change in lockstep with `SC_QDRANT_VECTOR_DIM`. |
| `SC_QDRANT_VECTOR_DIM` | `384` | Must match the embedder. Validated at startup. |
| `SC_PAYLOAD_REDACT_FIELDS` | (empty) | csv of value-field names to strip recursively before storage. |
| `SC_PAYLOAD_INCLUDE_HEADERS` | `false` | Headers commonly carry auth tokens. |
| `SC_PAYLOAD_INDEX_FIELDS` | (empty) | csv of value-field names to keyword-index in Qdrant. |
| `SC_BATCH_SIZE` | `32` | Messages per embed/upsert call. |

### MCP server

| Variable | Default | Notes |
|---|---|---|
| `SC_MCP_TOPIC_ALLOWLIST` | (empty) | csv of topics agents may query. Empty logs a warning and applies no filter. |
| `SC_MCP_MAX_RESULTS` | `100` | Per-call result cap; over-limit requests are clamped (not rejected). |
| `SC_MCP_MAX_TIME_RANGE_MINUTES` | `10080` | Hard cap on `time_range_minutes` (default 7 days). |
| `SC_MCP_TOOL_TIMEOUT_SEC` | `5.0` | Per-tool wall-clock timeout. |
| `SC_MCP_RATE_LIMIT_PER_MINUTE` | `120` | Per-tool token bucket. `0` disables. |
| `SC_MCP_EMBED_CACHE_SIZE` | `256` | LRU size for the embedder query cache. `0` disables. |
| `SC_MCP_MAX_VALUE_BYTES` | `8192` | Per-result `value` JSON cap. Larger values are replaced with a truncated stub. |

### Semantic catalog

| Variable | Default | Notes |
|---|---|---|
| `SC_CATALOG_DB_PATH` | `/var/lib/streamcontext/catalog.sqlite` | SQLite file shared between the refresher and the MCP server. |
| `SC_CATALOG_TOPICS` | (empty → `SC_KAFKA_TOPICS`) | Comma-separated topics the catalog manages. |
| `SC_CATALOG_SCHEMA_REFRESH_SEC` | `300` | Schema TTL. |
| `SC_CATALOG_SAMPLE_REFRESH_SEC` | `900` | Sample TTL. |
| `SC_CATALOG_STATS_REFRESH_SEC` | `60` | Stats TTL (also the outer `--loop` interval). |
| `SC_CATALOG_INFERENCE_REFRESH_SEC` | `3600` | LLM-inference TTL. |
| `SC_CATALOG_SAMPLE_COUNT` | `10` | Recent messages kept per topic. |
| `SC_CATALOG_RETAIN_SAMPLES` | `true` | Set `false` for metadata-only persistence. |
| `SC_CATALOG_LLM_PROVIDER` | `disabled` | `disabled`, `anthropic`, `openai`, or `local` (Ollama). |
| `SC_CATALOG_LLM_MODEL` | `claude-haiku-4-5-20251001` | Pick the smallest model that does the job well. |
| `SC_CATALOG_LLM_DAILY_CEILING_USD` | `1.0` | Hard daily spend cap; once tripped, inference is disabled until UTC rollover. |
| `SC_CATALOG_PII_FIELDS` | (empty) | csv of keys to drop from samples (e.g. `email,phone`). |
| `SC_CATALOG_PII_PATTERNS` | (empty) | csv of regexes to mask inside string values. |

### Observability

The ingestion gateway and the catalog refresher each expose `/health` and a
Prometheus `/metrics` endpoint. Full metric list, health semantics, and PromQL
examples are in [`docs/observability.md`](docs/observability.md).

| Variable | Default | Notes |
|---|---|---|
| `SC_METRICS_ENABLED` | `true` | Disable the metrics/health server entirely. |
| `SC_METRICS_HOST` | `127.0.0.1` | Bind address. `0.0.0.0` to scrape from another host. |
| `SC_METRICS_PORT` | `9108` | Co-located processes need distinct ports. |

## Security

Three audit documents drive the security posture:

- [`docs/audit-v0.1.md`](docs/audit-v0.1.md) — ingestion gateway, before MCP work began.
- [`docs/audit-v0.2.md`](docs/audit-v0.2.md) — MCP layer, before v0.2.0 cut.
- [`docs/audit-v0.3.md`](docs/audit-v0.3.md) — semantic catalog, before v0.3.0 cut.

Threat model and recommended production-adjacent settings in [`docs/security.md`](docs/security.md). Data surfaces and provider data policies in [`docs/data-handling.md`](docs/data-handling.md). Headline: v0.3 still assumes local/trusted-host deployment, but the MCP server now ships an `authorize` hook so a downstream consumer can plug in real per-caller auth without forking the server.

## Roadmap

- **v0.1 — streaming sink.** Kafka, embed, Qdrant. Batched, redacted, deterministic upsert, halt-on-failure semantics. Done.
- **v0.2 — MCP server.** `list_topics`, `describe_topic`, `search_events`, `find_similar_events`. Topic allowlist, rate limit, embed cache, value truncation. Done.
- **v0.3 — semantic catalog.** Schema introspection, sample-backed inferred descriptions, per-field meanings, relationship detection, daily LLM spend ceiling, PII redaction at persistence. Three new MCP tools: `find_topics_by_purpose`, `get_topic_relationships`, `explain_field`. This release.
- **Later.** Bidirectional flow (agents producing into Kafka with schema validation). Kafka Connect packaging. Multi-sink (Pinecone, Weaviate, pgvector). Jinja text-extraction templates. SASL/SSL Kafka, SR auth, SSE auth.

## Development

```bash
uv pip install -e '.[dev]'
pytest -q                      # unit tests (no Docker required)
RUN_INTEGRATION=1 pytest -q    # integration test (spins up Kafka + Qdrant via testcontainers)
ruff check .
```

## Contributing

The codebase is intentionally small - read it in 30 minutes, contribute on day one. Issues and PRs welcome, especially:

- New `VectorSink` implementations (Pinecone, pgvector, Weaviate).
- Better text-extraction strategies (Jinja templates, field selection).
- New language clients for the producer demo.
- More MCP tools - filter primitives, aggregations, anomaly heuristics.

## License

MIT - see [LICENSE](LICENSE).
