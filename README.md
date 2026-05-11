# streamcontext

> **The semantic gateway between Kafka and AI agents.**
> Stream Kafka events into a vector store and let MCP-compatible agents query your real-time data by meaning - with full Kafka coordinates on every result.

[![status](https://img.shields.io/badge/status-v0.2--alpha-orange)](#roadmap)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![python](https://img.shields.io/badge/python-3.11%2B-blue)](pyproject.toml)
[![docker](https://img.shields.io/badge/docker-compose-2496ED?logo=docker)](docker-compose.yml)

> A short demo video lives at `docs/demo.mp4` when present. Architecture diagram is in [`docs/architecture.md`](docs/architecture.md).

---

## Why this exists

LLM agents are great at reasoning, terrible at remembering. RAG fixes the memory problem for static knowledge - docs, wikis, codebases. But the most operationally interesting data in any company isn't static: it is flowing through Kafka right now. Orders, clicks, alerts, sensor readings, deploys.

streamcontext is the missing intermediary. Point the ingestion gateway at a Kafka topic and it continuously embeds messages into a vector store, preserving full Kafka metadata (topic, partition, offset, timestamp, headers, key) as filterable payload. Run the MCP server alongside your agent host (Claude Desktop, Cursor, Cline, custom) and your agents query the stream like any other knowledge base - except this one is always current.

Two cooperating processes, one shared Qdrant collection. See [`docs/architecture.md`](docs/architecture.md) for the two-process diagram.

## Quickstart (10 minutes)

Requirements: Docker, Python 3.11+, [`uv`](https://github.com/astral-sh/uv) (or pip).

```bash
git clone https://github.com/jagadeshvarma/streamcontext.git
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

# 5. Wire up your agent (see "Use with Claude Desktop" below) and start asking questions
```

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
| `list_topics` | List ingested topics with approximate counts and time windows. Call this first when the user references unfamiliar data. |
| `describe_topic` | Schema (from Schema Registry when reachable), counts, time window, and a few sample records for one topic. Ground the agent in the schema before constructing filters. |
| `search_events` | Semantic search ranked by similarity to a natural-language query. Supports structured `filters`, `topic`, `time_range_minutes`, `score_threshold`, and an MMR-based `diverse` mode for deduping near-identical hits. |
| `find_similar_events` | "More like this one" given a `topic:partition:offset` reference. Useful for incident investigation. |

Every result carries the Kafka coordinate (`coord.topic:coord.partition:coord.offset:coord.timestamp_ms`) so the agent can cite exactly where the record came from.

## Example agent conversations

Three patterns the v0.2 tools are tuned for:

1. **"Find high-value orders from California in the last hour"** - the agent uses `search_events` with `topic="orders"`, `time_range_minutes=60`, and structured filters `region=US_WEST`, `total>=200`.
2. **"What failed transactions did we see today?"** - the agent first calls `describe_topic` to learn that `status` is an enum with `cancelled` and `refunded` values, then `search_events` with `filters=[{field: status, in_values: [cancelled, refunded]}]`.
3. **"Anything weird in the last 5 minutes?"** - the agent embeds the intent ("unusual, anomalous, or unexpected event"), sets `diverse=true` so MMR drops near-duplicates, restricts the time window, and returns ten qualitatively different events.

Walked end-to-end with the tool-call shapes in [`docs/example-conversations.md`](docs/example-conversations.md). Tuning checklist at the bottom of that file.

## How it fits together

```
   ingestion process                       MCP process
   -----------------                       -----------

   Kafka  ->  consumer  ->  pipeline  -.            ,-  SearchEngine  ->  FastMCP  ->  stdio / SSE
              (avro/SR)    (batched,    \          /     (filters,        (tools,         |
                            redacted)    \        /       MMR,             timeout,       v
                                          \      /        truncation)      rate limit)   agent
                                           v    ^                                       (Claude
                                          Qdrant collection                              Desktop,
                                         (shared substrate)                              Cursor, ...)
```

Full design notes - text-extraction strategy, batching, offset semantics, deterministic point IDs, topic allowlist, payload redaction - in [`docs/architecture.md`](docs/architecture.md). Security posture in [`docs/security.md`](docs/security.md).

## Configuration

Everything is env-driven. The most useful knobs are below; see [`.env.example`](.env.example) for the full list.

### Ingestion gateway

| Variable | Default | Notes |
|---|---|---|
| `SC_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `SC_KAFKA_TOPICS` | `orders` | Comma-separated. |
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

## Security

Two audit documents drive the security posture:

- [`docs/audit-v0.1.md`](docs/audit-v0.1.md) - ingestion gateway, before MCP work began.
- [`docs/audit-v0.2.md`](docs/audit-v0.2.md) - MCP layer, before v0.2.0 cut.

Threat model and recommended production-adjacent settings in [`docs/security.md`](docs/security.md). Headline: v0.2 assumes local/trusted-host deployment. Multi-tenant exposure and authenticated SSE are v1.0 work.

## Roadmap

- **v0.1 - streaming sink.** Kafka, embed, Qdrant. Batched, redacted, deterministic upsert, halt-on-failure semantics. Done.
- **v0.2 - MCP server.** `list_topics`, `describe_topic`, `search_events`, `find_similar_events`. Topic allowlist, rate limit, embed cache, value truncation. This release.
- **v0.3 - semantic catalog.** Auto-discover topics, fetch schemas, surface them as MCP resources so agents can ask which topic to query before constructing one.
- **Later.** Multi-sink (Pinecone, Weaviate, pgvector). Jinja text-extraction templates. Kafka Connect packaging. Prometheus `/metrics`. SASL/SSL Kafka, SR auth, SSE auth.

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
