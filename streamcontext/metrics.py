"""Prometheus metrics for the ingestion gateway (pipeline + consumer).

Defined in their own module so importing the gateway does not register the
catalog's metrics and vice versa — each process's /metrics stays scoped to what
it actually does. See `streamcontext.observability` for the HTTP server.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

MESSAGES_INGESTED = Counter(
    "sc_gateway_messages_ingested_total",
    "Messages embedded and durably upserted into the vector store.",
)
BATCHES_FLUSHED = Counter(
    "sc_gateway_batches_flushed_total",
    "Batches durably upserted into the sink and committed.",
)
EMBED_SECONDS = Histogram(
    "sc_gateway_embed_seconds",
    "Wall time to embed one batch.",
)
SINK_SECONDS = Histogram(
    "sc_gateway_sink_seconds",
    "Wall time to upsert one batch into the sink.",
)
COMMITTED_OFFSET = Gauge(
    "sc_gateway_committed_offset",
    "Last committed next-offset, per topic and partition.",
    ["topic", "partition"],
)
DESERIALIZE_FAILURES = Counter(
    "sc_gateway_deserialize_failures_total",
    "Messages that failed Avro deserialization.",
    ["topic"],
)
DLQ_PRODUCED = Counter(
    "sc_gateway_dlq_produced_total",
    "Undeserializable messages republished to the dead-letter topic.",
    ["topic"],
)
UP = Gauge(
    "sc_gateway_up",
    "1 while the pipeline is running and not halted, 0 otherwise.",
)
