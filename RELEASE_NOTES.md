# streamcontext v0.3.0 — the semantic catalog

**Self-describing event streams: every topic, field, and relationship explained in natural language, queryable by agents.**

v0.3 turns your Kafka cluster from "a list of topics" into "a system an
agent can reason about." A third process — the catalog refresher — sits
alongside the v0.2 ingestion gateway and MCP server. It walks each
topic's schema, samples recent messages, profiles activity, and (when
you enable an LLM provider) generates a natural-language description
and per-field meanings. It also detects relationships between topics
heuristically, with an optional LLM polish for semantic links that
share no key.

## What's now possible

- **"Where would I find billing data?"** Agents call
  `find_topics_by_purpose`, which embeds the question and ranks topics
  by similarity to their catalog descriptions.
- **"Find all failed payments and the orders they're attached to."**
  Agents call `get_topic_relationships` to discover the join field,
  then issue correctly-joined searches without reading any Avro by
  hand.
- **"What does the `risk_score` field actually mean?"** Agents call
  `explain_field` and get the inferred meaning, a confidence score,
  and a handful of example values from samples.
- **Multi-topic reasoning.** `list_topics` now carries inferred
  descriptions on every entry; `describe_topic` overlays per-field
  meanings on the Schema Registry view.

## Three cooperating processes

| Process | Role |
|---|---|
| Ingestion gateway (`streamcontext.main`) | Kafka → embed → Qdrant. Unchanged from v0.2. |
| Catalog refresher (`streamcontext.catalog.refresher`) | Walks schemas, samples messages, profiles activity, runs inference, detects relationships. SQLite-backed. |
| MCP server (`streamcontext.mcp_main`) | Reads catalog + Qdrant, serves MCP-compatible agents. New tools: `find_topics_by_purpose`, `get_topic_relationships`, `explain_field`. |

The three processes share state through two files: the Qdrant
collection and the catalog's SQLite database. Failures are isolated —
a wedged refresher does not affect ingestion or search; the catalog
simply goes stale until it recovers.

## Cost discipline

- Every inference call is cached on `(schema_fingerprint, sample_hash)`
  — identical inputs never cost twice.
- A daily LLM spend ceiling per provider is enforced before each call.
  Once tripped, the catalog falls back to schema-only entries with a
  clear `inference_status="disabled"` flag.
- Defaults pick the smallest capable model (Claude Haiku) and bound
  prompt size.

Practical day-zero cost on a 10-topic cluster: under $0.05 once the
cache is warm.

## Privacy

- PII redaction runs **before** samples land in SQLite — not just
  before LLM submission. Field-name redaction
  (`SC_CATALOG_PII_FIELDS`) plus regex masking (built-in patterns for
  email, phone, card, SSN, plus operator-supplied via
  `SC_CATALOG_PII_PATTERNS`).
- `SC_CATALOG_RETAIN_SAMPLES=false` keeps metadata only — samples are
  pulled in-memory for inference and discarded.
- The catalog is **off by default**. With `SC_CATALOG_LLM_PROVIDER=disabled`
  only the deterministic features run; enable a provider deliberately and
  pick one whose data policy matches your requirements.

See [`docs/data-handling.md`](docs/data-handling.md) for the full data
surfaces and provider policies, [`docs/audit-v0.3.md`](docs/audit-v0.3.md)
for the security and cost audit.

## Upgrading from v0.2

1. Pull the v0.3 image / update the package.
2. Existing v0.2 deployments keep working unchanged — the v0.2 tools
   (`list_topics`, `describe_topic`, `search_events`,
   `find_similar_events`) all still work without a catalog. The new
   catalog-backed tools simply return empty results until the catalog
   is populated.
3. Optional: start the refresher.

   ```bash
   python -m streamcontext.catalog.refresher --loop
   ```

4. Optional: enable inference. Set `SC_CATALOG_LLM_PROVIDER=anthropic`
   (or `openai`, or `local`) and provide the API key. Default daily
   spend cap is $1.

No breaking changes. No data migrations.

## What changed under the hood

- New module `streamcontext.catalog` (models, store, introspect,
  activity, inference, relationships, builder, refresher, privacy).
- New module `streamcontext.mcp_catalog` (catalog-backed read helpers
  for the MCP server).
- `streamcontext.mcp_server.build_server(authorize=...)` ships an
  optional async auth hook composed via the testable `make_gate`
  helper. Default behavior unchanged.
- 60+ new unit tests; full suite 109 passing.

Full notes in [`CHANGELOG.md`](CHANGELOG.md).

## Acknowledgements

Built openly; design notes and audit documents live in
[`docs/`](docs/). Issues and PRs welcome — especially around new
relationship heuristics, alternative inference providers, and
production deployment recipes.
