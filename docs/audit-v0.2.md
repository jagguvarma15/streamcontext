# streamcontext v0.2 audit (MCP layer)

Second-pass audit of the MCP server added in v0.2. Same categorization scheme as `audit-v0.1.md`:

- `Block` - must be fixed before v0.2.0 cuts.
- `Fix in v0.2.x` - addressed in a follow-up patch.
- `Defer` - tracked here, addressed later.
- `Resolved` - already in v0.2.0a*; listed for the record.

The threat model for v0.2 is "running on the operator's laptop or in a trusted environment, alongside one or more locally-launched agents." Multi-tenant exposure, authenticated SSE, and remote-attestation concerns are explicitly out of scope and tracked in `docs/security.md` for v1.0.

---

## Security findings

### M1 (Resolved) Topic allowlist enforced at the engine layer

Where: `streamcontext/mcp_search.py` (`SearchEngine._build_filter`, `find_similar_events`), `mcp_models.py` (`TopicDescription`).

`SC_MCP_TOPIC_ALLOWLIST` is enforced in two places:

1. `search_events`: an explicit off-allowlist `topic` arg is rewritten to a sentinel filter (`topic=="__denied__"`) that cannot match any record. Default (no `topic` arg) restricts to the allowlist via a `MatchAny` clause.
2. `find_similar_events`: off-allowlist references raise `EventNotFoundError`, which the tool wrapper translates to `ToolError(code="not_found")`. Indistinguishable from a genuine miss, so cross-allowlist probing is not possible.
3. `describe_topic` for an off-allowlist topic returns a zero-count description and never calls `count()` or `scroll()`. The agent cannot tell whether the topic exists, is empty, or is restricted.

Resolved in 0.2.0a2.

### M2 (Resolved) Tool inputs are Pydantic-typed with hard bounds

Where: `streamcontext/mcp_server.py`, `streamcontext/mcp_models.py`.

Every tool argument carries a Pydantic `Field(...)` with min/max bounds. FastMCP validates before the engine sees anything:

- `query`: `min_length=1, max_length=2000`
- `limit`: `ge=1, le=settings.mcp_max_results`
- `topic`: `max_length=200`
- `time_range_minutes`: `ge=1, le=settings.mcp_max_time_range_minutes`
- `score_threshold`: `ge=-1.0, le=1.0`
- `reference_id`: `min_length=5, max_length=300`
- `filters`: `max_length=10` clauses; each `FilterClause` has `extra="forbid"` and `field` length-capped

No string interpolation reaches Qdrant; all filters are typed `FieldCondition` objects. Injection into Qdrant is structurally impossible.

### M3 (Resolved) Per-tool wall-clock timeout

Where: `streamcontext/mcp_server.py`.

Every tool body is wrapped in `asyncio.wait_for(..., timeout=settings.mcp_tool_timeout_sec)`. On timeout the tool returns `ToolError(code="timeout")` rather than hanging the agent's UI. Default 5 seconds; configurable via `SC_MCP_TOOL_TIMEOUT_SEC`.

### M4 (Resolved) Per-tool token-bucket rate limit

Where: `streamcontext/rate_limit.py`, `streamcontext/mcp_server.py`.

Each tool gets its own `TokenBucket` sized at `SC_MCP_RATE_LIMIT_PER_MINUTE` (default 120/min). Buckets refill smoothly at `per_minute/60` tokens per second. Exceeded buckets return `ToolError(code="rate_limited")` with the retry-after in seconds. Setting the env to `0` disables the limit (operator opt-out).

This is the primary guard against a runaway agent hammering Qdrant or burning the embedding API.

### M5 (Resolved) Embedding query LRU cache

Where: `streamcontext/embedder.py` (`CachedEmbedder`).

The MCP server wraps its embedder in a `CachedEmbedder` keyed on the exact query string. Sized via `SC_MCP_EMBED_CACHE_SIZE` (default 256). Cuts both cost (for paid providers) and latency (for repeats from the same conversation). `0` disables.

Combined with M4, the worst-case embedding spend per minute is bounded by the rate limit times the per-tool count.

### M6 (Resolved) Per-record value size cap

Where: `streamcontext/mcp_search.py` (`_maybe_truncate_value`, `_apply_value_cap`), `streamcontext/mcp_models.py` (`EventResult.value_truncated`).

Search results whose `value` JSON exceeds `SC_MCP_MAX_VALUE_BYTES` (default 8192) are replaced with a stub carrying `_truncated`, `_size_bytes`, and a `_preview`. `EventResult.value_truncated` is set so the agent has a structured signal. Stops one 5MB row from blowing out the agent's context.

### M7 (Resolved) Payload redaction inherited from v0.1.1

Where: `streamcontext/pipeline.py` (`_build_record`), `streamcontext/redaction.py`.

The Qdrant payload is what the MCP server returns to agents verbatim. Operators set `SC_PAYLOAD_REDACT_FIELDS` and `SC_PAYLOAD_INCLUDE_HEADERS=false` (default) on the gateway side. The MCP server has no way to surface redacted fields because they were never written.

### M8 (Resolved) Schema Registry is optional and best-effort

Where: `streamcontext/mcp_main.py` (`_try_schema_registry`), `streamcontext/mcp_search.py` (`_fetch_schema`).

`describe_topic.schema_summary` is `None` when SR is unreachable, rather than crashing the tool. SR connect failures are warning-logged at startup. This avoids a deployment where the MCP process is on the user's laptop and SR lives in another network getting stuck on every `describe_topic` call.

### M9 (Resolved) Tool-call audit logging

Where: every tool function in `streamcontext/mcp_server.py`, every engine method in `streamcontext/mcp_search.py`.

Every tool invocation logs the tool name, key arguments (query length, limit, topic, time range, filter count, diverse), and the result count or `truncated` flag. Rate-limit denials, timeouts, and tool errors get their own log events. Operators can replay an agent session from the log stream.

### M10 (Fix in v0.2.x) Bound on simultaneous tool concurrency

Where: `streamcontext/mcp_server.py`.

The token bucket caps invocations per minute but doesn't cap concurrent in-flight calls. A pathological agent could fire ten parallel `search_events` and starve the embedder. Not exploitable in practice (Claude Desktop serializes tool calls), but worth a `Semaphore` wrapper for SSE deployments.

Tracked for v0.2.x.

### M11 (Fix in v0.2.x) SSE transport authentication

Where: `streamcontext/mcp_main.py`.

The SSE transport binds to `127.0.0.1` by default but offers no auth on the port. Operators running on shared hosts must front it with an authenticated reverse proxy. Documented in `docs/mcp-setup.md`; in-server auth is a v1.0 concern (mirrors v0.1 SASL/SSL story).

### M12 (Defer) Per-tool rate limits configurable per tool

Right now `SC_MCP_RATE_LIMIT_PER_MINUTE` applies the same cap to every tool. `list_topics` is much cheaper than `search_events`; an operator might want to allow more of the former. Defer until someone asks.

---

## Functional findings

### F1 (Resolved) Payload indexes for filterable fields

Where: `streamcontext/sink.py` (`_ensure_core_indexes`).

Core indexes (`topic`, `partition`, `timestamp_ms`) are created automatically; user-declared fields via `SC_PAYLOAD_INDEX_FIELDS` are added under `value.<field>`. Without these the MCP filter tools still work but fall back to full-collection scans.

### F2 (Resolved) Field-name normalization for agents

Where: `streamcontext/mcp_search.py` (`_normalize_field`).

Agents pass `status`, not `value.status`. The engine normalizes automatically; core Kafka coordinates pass through unchanged. Avoids forcing the agent to know the payload layout.

### F3 (Fix in v0.2.x) `/health` and `/metrics` HTTP endpoints

Where: entire MCP server.

The v0.2 documentation cut ships this for the gateway. The MCP server runs over stdio in the common case, where HTTP endpoints don't apply. For SSE deployments, a `/health` route is a v0.2.x addition.

### F4 (Defer) Per-tool concurrency semaphore

See M10. Same fix, same tracking.

---

## Adversarial sanity check

Reviewed each tool against the canonical attack patterns from the prompt:

| Attack | Result |
|---|---|
| Qdrant filter injection via crafted `filters` | Not possible: `FilterClause.field` is bounded to 100 chars, value types are scalar Python primitives, no string interpolation; Qdrant gets typed `FieldCondition` objects. |
| Excessive embedding spend | Bounded by `SC_MCP_RATE_LIMIT_PER_MINUTE` and the LRU cache. Worst-case per minute is `rate_limit * (1 - cache_hit_rate)` embedding calls. |
| Cross-topic data leak | Blocked at three points: search filter rewrite, similar-events `not_found`, describe-topic empty response. None reveal whether the topic exists. |
| Pydantic-malformed input crash | FastMCP validates before the engine; invalid inputs return a structured FastMCP error. The engine itself defensively checks shape (e.g. `_parse_reference_id` rejects garbage). |
| Hung tool (slow Qdrant, slow embedder) | Bounded by `SC_MCP_TOOL_TIMEOUT_SEC` (default 5s); tool returns `ToolError(code="timeout")`. |
| Memory blowout from a huge record | `SC_MCP_MAX_VALUE_BYTES` truncates per-record `value` to a stub. `SC_MCP_MAX_RESULTS` caps result count. |

---

## Summary

- Fixed before v0.2.0: M1-M9, F1-F2.
- Tracked for v0.2.x: M10, M11, F3.
- Deferred: M12, F4.

The threat model assumed by v0.2 is local/trusted-host. The block-Week-2 items from `audit-v0.1.md` (silent data loss, dim mismatch, payload redaction, gateway healthcheck) all remained fixed through the Week 2 changes.
