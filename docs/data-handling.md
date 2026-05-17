# Data handling

This document covers what data streamcontext stores, what data it sends to
third parties, and the controls you have over both.

## Three data surfaces

| Surface | What lands here | Storage location | Operator control |
|---|---|---|---|
| Vector store (Qdrant) | Embedded record values, Kafka coordinates, and the redacted record payload set by `SC_PAYLOAD_REDACT_FIELDS`. | Local Qdrant container by default; configurable with `SC_QDRANT_URL`. | `SC_PAYLOAD_REDACT_FIELDS`, `SC_PAYLOAD_INDEX_FIELDS`, `SC_PAYLOAD_INCLUDE_HEADERS`. |
| Semantic catalog (SQLite) | Topic schemas, recent sample messages, inferred descriptions and field annotations, detected relationships, daily LLM spend ledger. | `SC_CATALOG_DB_PATH` (default `/var/lib/streamcontext/catalog.sqlite`). | `SC_CATALOG_*` variables. |
| LLM provider (Anthropic / OpenAI / local) | The catalog inference prompt: condensed schema, redacted sample messages, the topic name. | Provider-hosted (Anthropic, OpenAI) or local (Ollama). | `SC_CATALOG_LLM_PROVIDER`, `SC_CATALOG_LLM_DAILY_CEILING_USD`. |

## PII redaction

Two redaction stages, both configurable per-deployment:

1. **Ingestion gateway.** `SC_PAYLOAD_REDACT_FIELDS=email,phone,ssn,...` drops
   matching fields from the record value before it is embedded or written to
   the vector store. Applied recursively to nested dicts and lists. Default
   is empty — set it explicitly for any topic that may contain sensitive
   fields.

2. **Catalog refresher.** `SC_CATALOG_PII_FIELDS` and
   `SC_CATALOG_PII_PATTERNS` are applied to every sampled message *before it
   lands in SQLite*. Field-name matches drop the field entirely; regex
   matches replace the matched substring with `[redacted]`. The same
   redaction is reapplied defensively when the inference layer builds an LLM
   prompt, so a misconfigured builder cannot leak through.

Built-in patterns cover the obvious shapes:

- email addresses (`local@host.tld`)
- phone numbers (including international prefixes)
- 13-19 digit card numbers
- US-formatted SSNs (`123-45-6789`)

Add more with `SC_CATALOG_PII_PATTERNS` (comma-separated regexes,
case-insensitive). Patterns operate on string values only — they do not
recurse into keys.

## Sample retention

By default, the catalog stores up to `SC_CATALOG_SAMPLE_COUNT` recent
messages per topic so the `describe_topic` and `explain_field` tools can
show real examples. If you do not want any sample payloads persisted, set:

```
SC_CATALOG_RETAIN_SAMPLES=false
```

The refresher still pulls samples in-memory to feed the inference layer,
but they are discarded once inference is done. The catalog keeps only
metadata (description, field annotations, relationships, activity stats).

## LLM provider data policies

| Provider | Default behaviour |
|---|---|
| Anthropic | API calls are not used to train Anthropic models by default. Inputs may be retained for up to 30 days for abuse monitoring; see Anthropic's commercial terms. |
| OpenAI | API calls are not used to train OpenAI models by default. Data may be retained for up to 30 days for abuse monitoring; an Enterprise contract can reduce or remove retention. |
| Local (Ollama) | All data stays on the host running Ollama. No network egress. |

Catalog inference is **off by default** (`SC_CATALOG_LLM_PROVIDER=disabled`).
Enable it deliberately and pick the provider whose policy matches your
requirements. If your organisation has tighter rules than the defaults,
configure your account-level data controls *before* enabling inference.

## Cost controls

The catalog enforces a daily LLM spend ceiling per provider. Once the day's
ledger crosses `SC_CATALOG_LLM_DAILY_CEILING_USD`, the inference engine
returns `inference_status="disabled"` and the catalog falls back to
schema-only entries. The next UTC day resets the ledger.

The spend ledger is stored in the same SQLite file as the catalog; you can
audit it directly:

```
sqlite3 $SC_CATALOG_DB_PATH 'SELECT * FROM llm_spend_ledger ORDER BY day DESC;'
```

Per-call caching means identical (schema, samples) inputs never cost
twice. Practical day-zero cost on a 10-topic cluster with Claude Haiku is
well under $0.05 once the cache is warm.

## Authorization

Both processes (ingestion gateway, MCP server) listen without
authentication today. Threat model: anyone who can reach the MCP port
can call any tool. The MCP server exposes an `authorize` hook so a
deployment that needs auth can plug it in; see `build_server(authorize=)`
in `streamcontext/mcp_server.py`. Multi-tenant production deployment is
explicitly out of scope.

## Auditing what flows

- **Vector store contents**: `qdrant-client` against `SC_QDRANT_COLLECTION`.
- **Catalog contents**: `sqlite3 $SC_CATALOG_DB_PATH '.tables'` and inspect.
- **LLM spend by day**: see the `llm_spend_ledger` table.
- **What the inference layer asked**: each prompt is built deterministically
  from the catalog entry; `streamcontext/catalog/inference.py::build_prompt`
  is the canonical implementation and is unit-tested for redaction.
