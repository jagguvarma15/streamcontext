# Observability

The two long-running processes — the ingestion gateway and the catalog
refresher — each expose a small HTTP server with two endpoints:

- `GET /health` (also `/healthz`, `/livez`, `/readyz`) — liveness/readiness.
  Returns `200` with a small JSON body when the process is healthy, `503`
  otherwise.
- `GET /metrics` — Prometheus metrics in the standard text exposition format.

The MCP server runs over stdio, is launched per agent session, and does not
expose this server.

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `SC_METRICS_ENABLED` | `true` | Set `false` to disable the server entirely. |
| `SC_METRICS_HOST` | `127.0.0.1` | Bind address. Use `0.0.0.0` to scrape from another host, and control exposure at the network layer — metric labels include topic names. |
| `SC_METRICS_PORT` | `9108` | Co-located processes need distinct ports. |

The server is best-effort: if the port cannot be bound (for example, the
gateway and refresher are co-located and share a port) the failure is logged
(`metrics.server.bind_failed`) and the process continues without metrics. It
never takes down ingestion or the refresher.

In Docker Compose the gateway sets `SC_METRICS_HOST=0.0.0.0`, publishes `9108`
to the host on `${SC_HOST_BIND:-127.0.0.1}`, and its healthcheck curls
`/health`.

## Health semantics

- **Gateway** — healthy once the pipeline is running and not halted. A
  persistent embed/sink/commit failure halts the pipeline
  (`PipelineFatalError`); `/health` then returns `503` with
  `{"halted": true, "error": "..."}` until a supervisor restarts the process.
- **Refresher** — healthy once it has completed at least one refresh cycle. The
  body reports the cycle count and the unix time of the last cycle.

## Metrics

### Gateway (`streamcontext.main`)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `sc_gateway_messages_ingested_total` | counter | | Messages embedded and upserted. |
| `sc_gateway_batches_flushed_total` | counter | | Batches committed. |
| `sc_gateway_embed_seconds` | histogram | | Per-batch embed time. |
| `sc_gateway_sink_seconds` | histogram | | Per-batch sink upsert time. |
| `sc_gateway_committed_offset` | gauge | `topic`, `partition` | Last committed next-offset. |
| `sc_gateway_deserialize_failures_total` | counter | `topic` | Messages that failed Avro decode. |
| `sc_gateway_dlq_produced_total` | counter | `topic` | Failed messages republished to the DLQ. |
| `sc_gateway_up` | gauge | | 1 while running and not halted. |

### Catalog refresher (`streamcontext.catalog.refresher`)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `sc_catalog_refresh_total` | counter | `aspect`, `result` | Refreshes by aspect (`topic`, `relationships`) and `ok`/`error`. |
| `sc_catalog_refresh_seconds` | histogram | `aspect` | Refresh time per aspect. |
| `sc_catalog_llm_spend_usd_today` | gauge | `provider` | LLM inference spend so far in the UTC day. |
| `sc_catalog_last_cycle_timestamp_seconds` | gauge | | Unix time of the last completed cycle. |
| `sc_catalog_up` | gauge | | 1 once a cycle has completed. |

Both processes also export the prometheus_client default process and Python
metrics (`process_*`, `python_*`).

## Dead-letter queue

A message that fails Avro deserialization is counted
(`sc_gateway_deserialize_failures_total`) and logged
(`consumer.deserialize_failed`). If `SC_KAFKA_DLQ_TOPIC` is set, the raw message
is also republished to that topic with headers:

- `sc_origin_topic`, `sc_origin_partition`, `sc_origin_offset` — where it came
  from.
- `sc_error` — the deserialization error (truncated to 500 bytes).

DLQ produce failures are logged (`consumer.dlq_produce_failed`) and never block
ingestion. With `SC_KAFKA_DLQ_TOPIC` empty (the default) the behaviour is
unchanged: failures are logged and counted only.

## Scraping with Prometheus

```yaml
scrape_configs:
  - job_name: streamcontext-gateway
    static_configs:
      - targets: ["localhost:9108"]
```

Useful queries:

- Ingestion throughput: `rate(sc_gateway_messages_ingested_total[1m])`
- p95 embed latency: `histogram_quantile(0.95, rate(sc_gateway_embed_seconds_bucket[5m]))`
- Deserialization failure rate: `rate(sc_gateway_deserialize_failures_total[5m])`
- Refresher stalled (no cycle in 10 minutes): `time() - sc_catalog_last_cycle_timestamp_seconds > 600`
- Gateway halted: `sc_gateway_up == 0`
