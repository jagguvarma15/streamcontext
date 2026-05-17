# Architecture

streamcontext runs as three cooperating processes that share state through a vector store and a SQLite catalog file. They scale, fail, and deploy independently.

## Three-process picture

```
   ──────── ingestion process (streamcontext.main) ────────

   Kafka topic(s)
        |
        v
   +--------------------------+
   |  consumer.py             |   aiokafka, Avro via Schema Registry
   |  yields KafkaMessage     |   carries (topic, partition, offset, ts, key, headers, value)
   +-------------+------------+
                 |
                 v
   +--------------------------+
   |  pipeline.py             |   buffers a batch (size N or T seconds)
   |  batches messages        |   redacts payload, drops headers by default
   +-------------+------------+
                 |
                 v
   +--------------------------+
   |  embedder.py             |   Embedder protocol; LocalEmbedder default
   |  texts -> vectors        |   (sentence-transformers, all-MiniLM-L6-v2 = 384 dims)
   +-------------+------------+
                 |
                 v
   +--------------------------+
   |  sink.py                 |   VectorSink protocol; QdrantSink default
   |  vectors + payload       |   payload = Kafka metadata + redacted value
   +-------------+------------+
                 |
                 v
        +---------------+
        |   Qdrant      |  <----+  shared substrate
        |  collection   |       |
        +---------------+       |
              ^                 |
              | reads stats     |
              |                 |
   ──── catalog refresher (streamcontext.catalog.refresher) ────
              |                 |
   +--------------------------+ |
   |  introspect.py           | |  Schema Registry walk, Avro flatten,
   |  SchemaIntrospector      | |  SHA-256 fingerprint
   +-------------+------------+ |
                 |              |
   +--------------------------+ |
   |  introspect.py           | |  Short-lived aiokafka consumer
   |  MessageSampler          | |  (fresh group id, latest, redacted)
   +-------------+------------+ |
                 |              |
   +--------------------------+ |
   |  activity.py             | |  Rolling counters from Qdrant payload
   |  ActivityProfiler        |-+
   +-------------+------------+
                 |
   +--------------------------+   Topic descriptions + field meanings,
   |  inference.py            |   cached by (fingerprint, sample_hash)
   |  InferenceEngine         |   daily LLM spend ceiling enforced
   +-------------+------------+
                 |
   +--------------------------+   Heuristic shared-key + sample overlap;
   |  relationships.py        |   optional LLM polish for semantic links
   |  RelationshipDetector    |
   +-------------+------------+
                 |
                 v
        +-----------------------+
        |  CatalogStore         |  SQLite (WAL). One file, three readers.
        |  (catalog.sqlite)     |
        +-----------+-----------+
                    ^
                    |
   ──────── MCP process (streamcontext.mcp_main) ────────
                    |
   +--------------------------+ +
   |  mcp_search.py           |-+ reads catalog
   |  SearchEngine            |   embeds queries, builds Qdrant filters,
   |                          |   enforces topic allowlist + caps
   +-------------+------------+
                 |
                 v
   +--------------------------+
   |  mcp_server.py           |   FastMCP wrapper, per-tool timeout,
   |  tools: search_events,   |   authorize hook, rate limit
   |   list_topics,           |
   |   describe_topic,        |
   |   find_topics_by_purpose,|
   |   get_topic_relationships,|
   |   explain_field,         |
   |   find_similar_events    |
   +-------------+------------+
                 |
        stdio / SSE transport
                 |
                 v
       MCP-compatible agent
       (Claude Desktop, Cursor, Cline, custom)
```

### Why three processes

- Different scaling profiles. Ingestion is throughput-bound and lives near Kafka. The MCP server is latency-bound and lives near the agent (often on the user's laptop). The catalog refresher is bursty — it does periodic heavy work then sleeps — and is naturally a job, not a service.
- Different failure modes. A wedged embedder model in ingestion shouldn't take down the agent's read path; a leaking MCP transport shouldn't lose Kafka offsets; a misbehaving LLM provider in the catalog shouldn't break either of the other two. The catalog can be entirely absent and the v0.2 tools still work.
- Different deploy patterns. Ingestion runs in a server process you supervise (compose, k8s, systemd). The MCP server is typically launched per-agent-session by the host application (Claude Desktop spawns it on demand). The catalog refresher is a cron-style job (`--loop` or scheduled).

The only couplings are the Qdrant collection name plus the catalog SQLite file path. The embedder model and vector dim are shared between ingestion and the MCP server only; the catalog never touches them.

## Design decisions

### Text extraction (v0.1): JSON-dump the whole record

When a message arrives as a structured Avro record we serialize it to canonical JSON and embed that string. This is not optimal — embedding noisy field names dilutes the signal — but it is a defensible default that requires zero schema knowledge. A future release will let users override with a Jinja template (`{{ description }}: ${{ price }}`) or a list of fields to concatenate.

### Batching: required, not optional

Calling the embedder one message at a time is roughly fifty times slower than batching 32 at a time. The pipeline always batches; the only knobs are `batch_size` and `batch_flush_interval_sec` (whichever fires first).

### Sink protocol: pluggable from day one

`VectorSink` is a `Protocol` with one method (`upsert`). Qdrant ships first because its Docker story is the cleanest, but the codebase is structured so adding Pinecone or pgvector is a single new file plus a config switch.

### Offsets: commit after sink success, halt on persistent failure

The pipeline commits offsets only after a batch is durably upserted into the sink. If retries are exhausted, the pipeline raises `PipelineFatalError` and exits non-zero rather than skipping the batch — silent data loss is the worst failure mode for an agent RAG source. See `docs/audit-v0.1.md` finding S2 for the rationale.

### Payload redaction

The Qdrant payload is what the MCP server returns to agents verbatim. By default headers are dropped (they often carry auth tokens or trace context), and `SC_PAYLOAD_REDACT_FIELDS` strips named fields from `value` recursively. Configured at the gateway, enforced before any vector is written.

### MCP topic allowlist

The MCP server enforces `SC_MCP_TOPIC_ALLOWLIST` at the engine layer: an explicit `topic` argument outside the allowlist is rewritten to a sentinel value that cannot match any record, so the response cannot leak the existence of restricted topics. An empty allowlist means "no restriction" but logs a warning at startup.

### Schema Registry: read-only

The gateway only reads schemas to deserialize messages. It never registers new ones. The producer (in `examples/`) is the only thing that writes to SR, and that is purely for the demo.

### Semantic catalog (v0.3)

The third process turns the cluster into something an agent can reason
about, not just search. For every configured topic it captures:

- The Avro schema flattened to dotted-path `FieldEntry` records, with a SHA-256 fingerprint over the canonical JSON used as a cache key.
- A bounded window of recent sample messages, redacted before persistence (see `streamcontext/catalog/privacy.py`).
- Rolling activity counters derived from the Qdrant payload (no extra Kafka load).
- LLM-inferred natural-language description and per-field meanings, cached by `(schema_fingerprint, sample_hash)` so identical inputs cost zero on the second pass.
- Heuristic relationships across topics (shared keys, foreign references, sample-value overlap) plus an optional LLM polish for semantic links the heuristic cannot see.

All of it lands in `SC_CATALOG_DB_PATH` (SQLite, WAL mode). The MCP server reads from the same file to surface the data through `list_topics`, `describe_topic`, `find_topics_by_purpose`, `get_topic_relationships`, and `explain_field`. The refresher writes; the MCP server only reads. Full design in [`catalog.md`](catalog.md), data-handling and provider policies in [`data-handling.md`](data-handling.md), audit in [`audit-v0.3.md`](audit-v0.3.md).

## Future layers (not yet shipped)

- Bidirectional flow. Agents producing back into Kafka with schema validation. Hardest safety problem in the project; deferred until the catalog is rock-solid.
- Kafka Connect packaging. Repackage the ingestion pipeline as a proper Kafka Connect sink connector. Dramatically expands the production audience.
- Production deployment guides. Helm chart, Terraform module, observability dashboards.
