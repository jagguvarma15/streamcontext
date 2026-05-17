# Security

This page is for someone deciding whether streamcontext is safe to point at their data. It covers the threat model the project is designed against, what the three processes do and do not protect, and where the audit trail lives.

## Threat model in plain language

streamcontext v0.3 assumes:

- The ingestion gateway runs on infrastructure the operator controls, on a network where the Kafka cluster and Qdrant are reachable but not directly exposed to the internet.
- The MCP server runs alongside the operator's own agent host (Claude Desktop, Cursor, Cline, or a custom local agent). It is launched and torn down by that host. The connection between agent and MCP server is stdio (default) or SSE on loopback (`127.0.0.1`).
- The catalog refresher runs as a job on the same trust boundary as the gateway. It needs read access to Kafka, Schema Registry, and Qdrant, plus write access to the catalog SQLite file the MCP server reads.
- The operator is the one configuring which Kafka topics the gateway ingests and which the MCP server exposes. They are not multi-tenant; one set of topics, one agent surface.
- Producers writing to Kafka are inside the operator's trust boundary in the sense that they could already corrupt the stream. streamcontext does not attempt to detect or quarantine malicious producers - it does protect downstream agents from the fallout (see Payload redaction below).

**Out of scope for v0.3:** multi-tenant gateway deployments, authenticated SSE transport, internet-exposed MCP servers, Kafka auth (SASL/SCRAM/mTLS), Schema Registry auth, fine-grained per-agent access control. The MCP server now ships an `authorize` hook (`build_server(authorize=...)`) so a downstream deployment that needs per-caller auth can plug a real check in without forking — but no auth is shipped in-tree. All of these remain tracked as v1.0 work in `audit-v0.1.md`, `audit-v0.2.md`, and `audit-v0.3.md`.

## What the gateway protects against

Specifically, in v0.2:

1. **Silent data loss in the ingestion path.** If embedding or Qdrant upsert fails persistently, the pipeline raises `PipelineFatalError` and exits non-zero. Offsets stay unadvanced; a supervisor (compose `restart`, systemd, k8s) catches the exit and a clean restart resumes from the last durable batch. See `audit-v0.1.md` S2.
2. **Vector-store dimension drift.** The embedder is the source of truth for vector dim; the gateway raises `ConfigurationError` (exit 78) at startup if `SC_QDRANT_VECTOR_DIM` does not match. No mid-batch crashes. See `audit-v0.1.md` S1.
3. **Sensitive fields reaching the agent.** `SC_PAYLOAD_REDACT_FIELDS` strips named fields recursively from every record value before it is written to Qdrant; `SC_PAYLOAD_INCLUDE_HEADERS=false` (default) prevents headers (which routinely carry auth tokens and trace context) from being stored at all. See `audit-v0.1.md` S3.

## What the MCP server protects against

Layered on top, in v0.2:

1. **Cross-topic data leak.** `SC_MCP_TOPIC_ALLOWLIST` is enforced at three points: explicit off-allowlist `topic` arguments are rewritten to a sentinel filter that cannot match; off-allowlist `find_similar_events` references return `not_found` indistinguishable from a real miss; `describe_topic` on a restricted name returns an empty zero-count description without ever hitting the store. See `audit-v0.2.md` M1.
2. **Injection into Qdrant filters.** Every filter clause is a typed `FieldCondition`; the `FilterClause` Pydantic model has `extra="forbid"` and bounded value types. No string interpolation reaches Qdrant. See `audit-v0.2.md` M2.
3. **Excessive embedding spend and runaway agent loops.** Per-tool token-bucket rate limiting (`SC_MCP_RATE_LIMIT_PER_MINUTE`, default 120) combined with an LRU query cache (`SC_MCP_EMBED_CACHE_SIZE`, default 256). Worst-case per-minute embedding calls are bounded by `rate_limit * (1 - cache_hit_rate)`. See `audit-v0.2.md` M4, M5.
4. **Hung tools.** Every tool body is wrapped in `asyncio.wait_for(..., timeout=SC_MCP_TOOL_TIMEOUT_SEC)` (default 5s). Timeouts return `ToolError(code="timeout")` rather than hanging the agent UI. See `audit-v0.2.md` M3.
5. **Context-window blowout from a single record.** Search results whose `value` JSON exceeds `SC_MCP_MAX_VALUE_BYTES` (default 8192) are replaced with a `_truncated` stub. `EventResult.value_truncated` is the structured signal for the agent. See `audit-v0.2.md` M6.
6. **Auditability.** Every tool invocation, every rate-limit denial, every timeout, and every internal error is logged with structured fields (tool name, key arguments, result count). An operator can replay an agent session entirely from the log stream. See `audit-v0.2.md` M9.

## Recommended configuration for production-adjacent use

The defaults are tuned for the laptop/local-stack case. For a deployment with real data:

```
# Gateway (ingestion process)
SC_PAYLOAD_REDACT_FIELDS=email,phone,card_number,ssn,authorization
SC_PAYLOAD_INCLUDE_HEADERS=false
SC_PAYLOAD_INDEX_FIELDS=status,region,channel        # whatever your queries actually filter on
SC_HOST_BIND=127.0.0.1                                # default; only override with explicit reason

# MCP server (read process)
SC_MCP_TOPIC_ALLOWLIST=orders,clicks                  # explicit subset, never empty in production
SC_MCP_MAX_RESULTS=50                                  # tighten from default 100
SC_MCP_MAX_TIME_RANGE_MINUTES=1440                     # 24h is usually plenty for ad-hoc queries
SC_MCP_TOOL_TIMEOUT_SEC=5
SC_MCP_RATE_LIMIT_PER_MINUTE=60                        # per tool; tighten further for paid embedders
SC_MCP_EMBED_CACHE_SIZE=512                            # higher for chat-style usage
SC_MCP_MAX_VALUE_BYTES=4096

# Catalog refresher (third process)
SC_CATALOG_DB_PATH=/var/lib/streamcontext/catalog.sqlite
SC_CATALOG_LLM_PROVIDER=anthropic                      # or 'openai' / 'local'; 'disabled' to skip LLM inference entirely
SC_CATALOG_LLM_MODEL=claude-haiku-4-5-20251001
SC_CATALOG_LLM_DAILY_CEILING_USD=1.0                   # hard daily cap, persisted in SQLite ledger
SC_CATALOG_PII_FIELDS=email,phone,card_number,ssn,authorization
SC_CATALOG_RETAIN_SAMPLES=true                          # set false to keep only metadata in SQLite
```

If you are using a paid embedding provider (OpenAI, Cohere, Voyage) the rate limit and the cache are how you bound your bill. The rate limit gives you the worst case; the cache gives you the typical case. Catalog LLM spend is bounded separately by `SC_CATALOG_LLM_DAILY_CEILING_USD`.

## Audit documents

The full audit trail lives in:

- `docs/audit-v0.1.md` - gateway audit before MCP work began. Twelve security findings, nine resolved; three deferred to v0.2.x (SASL/SSL, Schema Registry auth, HTTP `/metrics`).
- `docs/audit-v0.2.md` - MCP-layer audit before the v0.2.0 cut. Twelve security findings and four functional findings; nine of the security findings are fixed in v0.2.0, two tracked for v0.2.x (concurrency semaphore, SSE auth), one deferred (per-tool rate-limit knobs).
- `docs/audit-v0.3.md` - semantic-catalog audit before the v0.3.0 cut. Cost, privacy, correctness, and operational findings; all v0.3 blockers resolved, with deferred items tracked.

All three follow the same Block / Fix in next release / Defer / Resolved categorization. Each finding cites the file and code construct that addresses (or fails to address) it.

## Reporting a vulnerability

If you find a security issue, please file a private GitHub security advisory rather than opening a public issue. Public issues for non-sensitive bugs and feature requests are welcome as usual.
