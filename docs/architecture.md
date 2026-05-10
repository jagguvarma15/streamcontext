# Architecture

streamcontext runs as two cooperating processes that share state through a vector store. They scale, fail, and deploy independently.

## Two-process picture

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
                                |
                                |
   ──────── MCP process (streamcontext.mcp_main) ────────
                                |
   +--------------------------+ |
   |  mcp_search.py           |-+
   |  SearchEngine            |   embeds queries, builds Qdrant filters,
   |                          |   enforces topic allowlist + caps
   +-------------+------------+
                 |
                 v
   +--------------------------+
   |  mcp_server.py           |   FastMCP wrapper, per-tool timeout
   |  tools: search_events    |   structured ToolError on failure
   +-------------+------------+
                 |
        stdio / SSE transport
                 |
                 v
       MCP-compatible agent
       (Claude Desktop, Cursor, Cline, custom)
```

### Why two processes

- Different scaling profiles. Ingestion is throughput-bound and lives near Kafka. The MCP server is latency-bound and lives near the agent (often on the user's laptop).
- Different failure modes. A wedged embedder model in ingestion shouldn't take down the agent's read path; a leaking MCP transport shouldn't lose Kafka offsets.
- Different deploy patterns. Ingestion runs in a server process you supervise (compose, k8s, systemd). The MCP server is typically launched per-agent-session by the host application (Claude Desktop spawns it on demand).

The only coupling is the Qdrant collection name, the embedder model, and the vector dim. All three are env-driven and validated at startup of each process.

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

## Future layers (not yet shipped)

- v0.3 semantic catalog. Auto-discovers topics, fetches schemas from SR, indexes field names plus descriptions, exposes them as MCP resources so agents can ask "which topic carries customer events?" before querying.
- v0.2.x hardening. Per-tool rate limits, query LRU on the embedder, payload-field indexes in Qdrant for fast filter+vector search. Tracked in the Day 5 plan.
