# The semantic catalog

streamcontext's catalog is the layer that makes your Kafka cluster
*legible* — to agents, not just to humans. For each topic it stores the
schema, a few recent sample messages, basic activity stats, an inferred
natural-language description, per-field meanings, and relationships to
other topics. Agents reach this metadata through MCP tools; operators
manage it through environment variables.

## What the catalog answers

Without a catalog, an agent that doesn't already know your topic layout
either guesses field names or gives up. With a catalog, it can answer
questions like:

- *What kinds of data flow through this system?*
- *Where would I find billing events?*
- *What does the `risk_score` field actually mean?*
- *Which topics share a `customer_id`?*

The catalog encodes these answers as structured data once, so a human
does not have to maintain a glossary by hand and an agent does not have
to re-derive them on every query.

## Process model

The catalog refresher is a **third** process, sitting alongside the
ingestion gateway and the MCP server:

```
   ingestion process            MCP process              catalog refresher
   ─────────────────            ──────────────           ───────────────────

   Kafka -> consumer            FastMCP tools <─── reads ─── CatalogStore
            -> embedder              ^                      (SQLite, WAL)
            -> Qdrant ─── shared ───/                       ▲
                          state                            writes
                                                            │
                                                  SchemaIntrospector
                                                  MessageSampler
                                                  ActivityProfiler
                                                  InferenceEngine
                                                  RelationshipDetector
```

All three processes share state through two files only: the Qdrant
collection and the catalog's SQLite database. Failures are isolated —
a wedged refresher does not stop ingest or search, and the catalog
contents simply go stale until it recovers.

Run the refresher with:

```bash
python -m streamcontext.catalog.refresher          # one-shot pass
python -m streamcontext.catalog.refresher --loop   # continuous, on the configured cadence
```

The loop respects per-aspect TTLs. Schema, samples, stats, and inference
each have their own refresh interval, so a fast `stats_refresh_sec` does
not force a slow `inference_refresh_sec` to re-run.

## Aspects of a topic

A `TopicEntry` has six independent aspects:

| Aspect | Where it comes from | Refresh knob |
|---|---|---|
| Schema | Confluent Schema Registry. Walked into a flat list of `FieldEntry` records with dotted paths for nested fields. | `SC_CATALOG_SCHEMA_REFRESH_SEC` (default 300s) |
| Samples | A short-lived `aiokafka` consumer with a fresh group id and `auto_offset_reset=latest`. Pulled per refresh, redacted, then optionally persisted. | `SC_CATALOG_SAMPLE_REFRESH_SEC` (default 900s) |
| Activity stats | Counted from the Qdrant payload (the gateway already wrote it). Avoids touching Kafka. | `SC_CATALOG_STATS_REFRESH_SEC` (default 60s) |
| Description | LLM-generated from the condensed schema + redacted samples. Cached by `(schema_fingerprint, sample_hash)` — identical inputs never cost twice. | `SC_CATALOG_INFERENCE_REFRESH_SEC` (default 3600s) |
| Field annotations | Same LLM pass that produced the description; each field gets a meaning string and a confidence in [0, 1]. | Same as Description. |
| Relationships | A heuristic pass over every pair of topics (shared keys, foreign references, sample-value overlap) followed by an optional LLM polish for semantic links the heuristic cannot see. | Same as Description. |

## Configuration

All catalog knobs are prefixed `SC_CATALOG_*`. Defaults are tuned for a
small development cluster.

| Variable | Default | Purpose |
|---|---|---|
| `SC_CATALOG_DB_PATH` | `/var/lib/streamcontext/catalog.sqlite` | Where the SQLite file lives. Put on a Docker volume to survive restarts. |
| `SC_CATALOG_TOPICS` | (empty → falls back to `SC_KAFKA_TOPICS`) | Comma-separated topics the refresher will manage. |
| `SC_CATALOG_SCHEMA_REFRESH_SEC` | 300 | Schema TTL. |
| `SC_CATALOG_SAMPLE_REFRESH_SEC` | 900 | Sample TTL. |
| `SC_CATALOG_STATS_REFRESH_SEC` | 60 | Stats TTL — also the outer loop interval in `--loop` mode. |
| `SC_CATALOG_INFERENCE_REFRESH_SEC` | 3600 | Inference TTL. |
| `SC_CATALOG_SAMPLE_COUNT` | 10 | Recent messages kept per topic. |
| `SC_CATALOG_SAMPLE_TIMEOUT_SEC` | 5.0 | Per-call wall-clock budget for the Kafka sampler. |
| `SC_CATALOG_RETAIN_SAMPLES` | `true` | Set `false` to keep metadata only — samples are pulled in-memory for inference, then discarded. |
| `SC_CATALOG_ENABLE_SAMPLING` | `true` | Set `false` in environments without a broker (e.g. tests). |
| `SC_CATALOG_LLM_PROVIDER` | `disabled` | One of `disabled`, `anthropic`, `openai`, `local`. |
| `SC_CATALOG_LLM_MODEL` | `claude-haiku-4-5-20251001` | Pick the smallest model that does the job well. Haiku is excellent at this. |
| `SC_CATALOG_LLM_DAILY_CEILING_USD` | `1.0` | Hard daily spend cap per provider. Once tripped, inference returns `disabled` until UTC rollover. |
| `SC_CATALOG_LLM_MAX_INPUT_TOKENS` | 5000 | Prompt input cap; samples are truncated to fit. |
| `SC_CATALOG_PII_FIELDS` | (empty) | Comma-separated key names dropped from samples before persistence and before any LLM submission. |
| `SC_CATALOG_PII_PATTERNS` | (empty) | Comma-separated regexes; matched substrings in string values are replaced with `[redacted]`. |
| `SC_CATALOG_RELATIONSHIP_MIN_OVERLAP` | 0.2 | Minimum sample-value overlap before a shared-key relationship is emitted. |
| `SC_CATALOG_RELATIONSHIP_LLM_THRESHOLD` | 0.6 | Minimum LLM-reported confidence to keep a semantic relationship. |

## PII redaction

Two strategies, combined:

- **Field-name redaction.** Keys listed in `SC_CATALOG_PII_FIELDS` are
  dropped recursively. Use this when you know a field carries sensitive
  data — `email`, `phone`, `ssn`, `card_number`.
- **Regex redaction.** Built-in patterns mask emails, phone numbers,
  13–19 digit card numbers, and SSNs in string values. Add more with
  `SC_CATALOG_PII_PATTERNS`.

Both run **before** anything else touches a sample — samples land in
SQLite already-redacted. The inference layer applies the same patterns
defensively, so a misconfigured builder cannot leak through.

See [`data-handling.md`](data-handling.md) for the full data surfaces
and provider data-retention policies.

## Cost ceiling

`SC_CATALOG_LLM_DAILY_CEILING_USD` is enforced before every LLM call.
Once the day's ledger meets the cap the engine returns
`inference_status="disabled"` and the catalog falls back to schema-only
entries — agents see the topic, schema, samples, and stats; only the
LLM-derived bits (description, field meanings, semantic relationships)
are missing. The next UTC day resets the ledger.

Inspect the ledger directly:

```bash
sqlite3 $SC_CATALOG_DB_PATH 'SELECT day, provider, spend_usd FROM llm_spend_ledger ORDER BY day DESC;'
```

The (`schema_fingerprint`, `sample_hash`) cache means identical inputs
never cost twice. Practical day-zero cost on a 10-topic cluster with
Claude Haiku is well under $0.05 once the cache is warm.

## MCP tools that use the catalog

| Tool | Catalog contribution |
|---|---|
| `list_topics` | Each entry now carries the inferred description and confidence. |
| `describe_topic` | Returns the inferred description plus per-field meanings overlaid on the Schema Registry view. |
| `find_topics_by_purpose` | Embeds the query against each topic's inferred description (or a synthesized fallback) and returns ranked matches. |
| `get_topic_relationships` | Returns detected relationships (`shared_key`, `foreign_reference`, `event_chain`, `semantic`) with confidence. |
| `explain_field` | Returns the field's inferred meaning plus example values from samples. |

Inputs that name an off-allowlist topic return zero matches without
revealing whether the topic exists. Allowlist gating is enforced by the
`CatalogReader` for every catalog read.

## Staleness contract

`describe_topic` does not refresh on every call — that would hammer
Schema Registry and Kafka. The catalog is **eventually fresh**, bounded
by the configured TTLs:

- Stats: ≤ 60 seconds stale.
- Schema: ≤ 5 minutes stale.
- Samples: ≤ 15 minutes stale.
- Inferred description / annotations / relationships: ≤ 1 hour stale.

If you need a guaranteed fresh view, run the refresher once on demand
(`python -m streamcontext.catalog.refresher` without `--loop`) before
querying.

## Operational notes

- The refresher and the MCP server can run on different hosts as long as
  both can mount the SQLite file (e.g. via a Docker volume). SQLite WAL
  mode is enabled, so concurrent reads are safe.
- A fresh deployment with no inferred descriptions still serves all the
  deterministic tools (`describe_topic`, `list_topics`,
  `find_similar_events`, `search_events`). The catalog degrades
  gracefully — schema-only entries are still useful.
- Bad-pattern regexes are logged and skipped, not fatal. Inspect the
  refresher logs for `catalog.pii.bad_pattern` events when adding new
  patterns.
- The catalog never writes back to Kafka or Schema Registry. It is a
  read-only consumer of both.
