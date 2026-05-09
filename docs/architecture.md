# Architecture

## The pipeline

```
   Kafka topic(s)
        │
        ▼
   ┌──────────────────────────┐
   │  consumer.py             │   aiokafka, Avro deserialization via Schema Registry
   │  yields KafkaMessage     │   carries (topic, partition, offset, ts, key, headers, value)
   └──────────┬───────────────┘
              │
              ▼
   ┌──────────────────────────┐
   │  pipeline.py             │   buffers a batch (size N or T seconds)
   │  batches messages        │
   └──────────┬───────────────┘
              │
              ▼
   ┌──────────────────────────┐
   │  embedder.py             │   `Embedder` protocol; LocalEmbedder default
   │  texts → vectors         │   (sentence-transformers, all-MiniLM-L6-v2 = 384 dims)
   └──────────┬───────────────┘
              │
              ▼
   ┌──────────────────────────┐
   │  sink.py                 │   `VectorSink` protocol; QdrantSink default
   │  vectors + payload       │   payload = full Kafka metadata + raw value
   └──────────┬───────────────┘
              │
              ▼
        Qdrant collection
```

## Design decisions

### Text extraction (v0.1): JSON-dump the whole record

When a message arrives as a structured Avro record we serialize it to canonical JSON and embed that string. This is **not optimal** — embedding noisy field names dilutes the signal — but it's a defensible default that requires zero schema knowledge. v0.2 will let users override with a Jinja template (`{{ description }}: ${{ price }}`) or a list of fields to concatenate.

### Batching: required, not optional

Calling the embedder one message at a time is ~50x slower than batching 32 at a time. The pipeline always batches; the only knobs are `batch_size` and `batch_flush_interval_sec` (whichever fires first).

### Sink protocol: pluggable from day one

`VectorSink` is a `Protocol` with one method (`upsert`). Qdrant ships first because its Docker story is the cleanest, but the codebase is structured so adding Pinecone or pgvector is a single new file plus a config switch — no refactors.

### Offsets: commit after sink success

We use manual offset commits and only commit after a batch is durably upserted into the sink. A crash mid-batch means at-least-once delivery; the vector store will see duplicates, which Qdrant handles via stable point IDs derived from `(topic, partition, offset)`.

### Schema Registry: read-only

The gateway only *reads* schemas to deserialize messages. It never registers new ones. The producer (in `examples/`) is the only thing that writes to SR, and that's purely for the demo.

## Future layers (not yet shipped)

- **MCP server (v0.2).** A FastMCP-based server exposing `search_stream(query, topic?, since?)` as an MCP tool, so any MCP-compatible agent can query the gateway with no glue code.
- **Semantic catalog (v0.3).** Auto-discovers topics, fetches schemas from SR, indexes field names + descriptions, exposes them as MCP resources so agents can ask "which topic carries customer events?" before querying.
