# streamcontext v0.1 audit

A structured walk-through of v0.1 against the security and gap checklists, before adding the MCP layer in v0.2. Every finding is categorized:

- `Block` — must be fixed before MCP work begins (otherwise the bug becomes agent-visible).
- `Fix in v0.2` — touched naturally while building MCP.
- `Defer` — tracked here, addressed in a later release.

Findings the audit confirmed are not a problem are listed at the bottom for the record.

---

## Security findings

### S1 (Block) Embedding dimension mismatch is unrecoverable mid-batch

Where: `streamcontext/embedder.py` (`LocalEmbedder.dim`), `streamcontext/config.py` (`qdrant_vector_dim`), `streamcontext/sink.py` (`QdrantSink.ensure_ready`).

The Qdrant collection is created with `settings.qdrant_vector_dim` (env-configured, default 384). The actual embedder dim is whatever `LocalEmbedder._model.get_sentence_embedding_dimension()` returns after lazy load. If a user sets `SC_EMBEDDER_MODEL=all-mpnet-base-v2` (dim 768) without also setting `SC_QDRANT_VECTOR_DIM=768`, the collection is created at 384 and the first upsert fails far from the source of the misconfiguration. The pipeline retries three times then drops the batch (see S2).

Fix today: validate at startup that `settings.qdrant_vector_dim` matches `embedder.dim` after the embedder has loaded; raise a clear `ConfigurationError` if not.

### S2 (Block) Silent batch loss on persistent flush failure

Where: `streamcontext/pipeline.py` (`_flush`).

If embedding or sink retries are exhausted, `_flush` logs and returns. Offsets for that batch are not committed, but the loop continues consuming. The next successful batch commits an offset higher than the failed batch's last offset, so the failed batch is effectively lost — the consumer group will never re-read it. For an ingestion gateway about to feed an agent's RAG, silent data loss is the worst possible failure mode.

Fix today: on persistent flush failure, request pipeline shutdown rather than skipping the batch. Operator can investigate, fix the root cause, and restart. At-least-once semantics are preserved because offsets stayed unadvanced.

### S3 (Block) Vector store payload contains every Kafka field, no redaction

Where: `streamcontext/pipeline.py` (`_build_record` writes `value` and `headers` whole into Qdrant payload).

The Qdrant payload is the data the v0.2 MCP server will return to agents verbatim. If a producer puts PII (emails, phone numbers, card PANs), session tokens in headers, or any sensitive field in a message, it lands in the vector store and then in agent contexts. The reviewer can configure their producer to omit those fields, but the gateway should give them a redaction knob too as defense in depth.

Fix today: add `SC_PAYLOAD_REDACT_FIELDS` (csv) and `SC_PAYLOAD_INCLUDE_HEADERS` (bool, default false) config. Apply redaction in the pipeline before constructing the `VectorRecord`.

### S4 (Fix in v0.2) No SASL/SSL support in the Kafka client config

Where: `streamcontext/config.py`, `streamcontext/consumer.py` (`start`).

`AIOKafkaConsumer` is constructed with bootstrap, group, offset-reset, and timeouts only. Every production Kafka cluster requires `security_protocol=SASL_SSL` plus credentials. Currently impossible to configure without code changes.

Plan: add SASL/SSL knobs in v0.2.x. Documented in `docs/security.md` (Day 6). Not a blocker because v0.1 explicitly targets local-dev and trusted-network deployments.

### S5 (Fix in v0.2) Schema Registry HTTP only, no auth

Where: `streamcontext/consumer.py:32`.

`SchemaRegistryClient({"url": ...})` only takes the URL. Confluent's client supports `basic.auth.user.info` and TLS — needs to be exposed in config.

Plan: add `SC_SCHEMA_REGISTRY_*` auth/TLS knobs alongside SASL work. Same release.

### S6 (Resolved) Compose ports default to loopback

Where: `docker-compose.yml`. Already addressed in commit `db09836`: every host port mapping binds to `${SC_HOST_BIND:-127.0.0.1}`. Listed for the record.

### S7 (Resolved) Docker image runs non-root, multi-stage

Where: `Dockerfile`. Already correct. Listed for the record.

### S8 (Resolved) Stable point IDs use SHA-256

Already addressed in commit `8399b77`. Listed for the record.

### S9 (Resolved) `.env.example` contains no real values

Verified manually. Listed for the record.

---

## Gap findings (functional)

### G1 (Block) Gateway service has no healthcheck in compose

Where: `docker-compose.yml` (`gateway`).

Kafka, Schema Registry, and Qdrant all have healthchecks; the gateway has none. `restart: unless-stopped` flaps without operator visibility into why. Once the MCP server lands as a second process, this becomes more important.

Fix today: add a process-alive healthcheck (`pgrep -f streamcontext.main`).

### G2 (Fix in v0.2) No `/health` or `/metrics` HTTP endpoint

Where: entire gateway. Day 5 of the Week 2 plan adds these.

### G3 (Fix in v0.2) No query cache on the embedder

Where: `streamcontext/embedder.py` (`OpenAIEmbedder`).

When the MCP server's `search_events` tool calls `embed()` for every agent query, OpenAI bills are unbounded. Day 5 of the Week 2 plan adds a query LRU.

### G4 (Fix in v0.2) Filterable payload fields not indexed in Qdrant

Where: `streamcontext/sink.py` (`QdrantSink.ensure_ready`).

Qdrant supports payload indexes for filterable fields (`status`, `region`, etc.). Without them, MCP filter queries do full-collection scans. Day 4 of the Week 2 plan adds these.

### G5 (Defer) Backfill mode for existing topics

Current `auto_offset_reset=earliest` covers the cold-start case adequately. A dedicated backfill mode (read-to-tip-then-go-live, separate metric exposed) is a v0.3 concern.

### G6 (Defer) Schema evolution testing

Manual smoke test passes (added Avro fields are exposed in `value`, removed fields disappear). No structural code change needed; documented in `docs/architecture.md` after v0.2 cut.

### G7 (Defer) Bounded queue between consumer and embedder

The pipeline is sequential — `await msg_iter.__anext__()` blocks until the previous batch is flushed. Backpressure naturally propagates to aiokafka's internal fetch buffer. No additional queue needed for v0.1.x throughput targets.

### G8 (Resolved) Consumer group naming

Already configurable (`SC_KAFKA_GROUP_ID`, default `streamcontext-gateway`). Defaults documented in `.env.example`. Listed for the record.

### G9 (Resolved) Configuration documentation

Every env var is documented in `.env.example` and `README.md`. Listed for the record.

---

## Summary

- Fixed in this commit: S1, S2, S3, G1.
- Tracked for Week 2: S4, S5, G2, G3, G4 — addressed across Days 4-6 of the Week 2 plan.
- Deferred (rationale above): G5, G6, G7.
