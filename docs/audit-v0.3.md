# streamcontext v0.3 audit (semantic catalog)

Third-pass audit, focused on the semantic catalog that landed in v0.3. The
catalog is a new data surface (sample messages persisted in SQLite, prompts
sent to LLM providers) and a new failure surface (inference can be wrong,
or expensive). This document tracks what changed, what is acceptable
risk, and what is deferred.

Same categorization scheme as v0.1 and v0.2:

- `Block` — must be fixed before v0.3.0 cuts.
- `Fix in v0.3.x` — addressed in a follow-up patch.
- `Defer` — tracked here, addressed later.
- `Resolved` — already in v0.3.0a/b/rc; listed for the record.

The threat model for v0.3 is unchanged: "running on the operator's laptop
or in a trusted environment, alongside one or more locally-launched
agents." Multi-tenant exposure and per-caller auth remain explicitly out
of scope — but the MCP server now ships an `authorize` hook so a
downstream consumer can plug in a real check without forking the server.

---

## Cost findings

### C1 (Resolved) Daily LLM spend ceiling enforced per provider

Where: `streamcontext/catalog/inference.py::InferenceEngine.infer`,
`streamcontext/catalog/store.py::record_spend`,
`streamcontext/catalog/store.py::get_spend_today`.

Every inference call is preceded by a `get_spend_today` lookup against the
configured provider. If the day's spend already meets or exceeds
`SC_CATALOG_LLM_DAILY_CEILING_USD`, the call returns
`inference_status="disabled"` without contacting the provider. The
catalog surfaces this status on the topic entry and on the
`describe_topic` MCP response so agents know they are getting
schema-only results.

The ledger is per-provider, per-UTC-day, persisted in the same SQLite
file as the rest of the catalog. A misconfigured deployment cannot exceed
the ceiling by more than the single in-flight call that crossed it (the
spend is recorded after the call returns).

### C2 (Resolved) Cache by (schema_fingerprint, sample_hash)

Where: `streamcontext/catalog/inference.py::InferenceEngine.cache_key`.

A SHA-256 fingerprint of the canonical schema JSON plus a SHA-256 of the
sorted sample payload form the cache key. Identical inputs never spend a
second time. The sample-sort step makes the key order-independent, so a
different arrival order does not invalidate the cache.

For the relationship layer, the same SQLite cache holds a key derived
from `(fingerprint_a, fingerprint_b, description_a, description_b)`. Pair
checks cost at most once per (schema, description) combination.

### C3 (Resolved) Prompt input is bounded

Where: `streamcontext/catalog/inference.py::build_prompt`.

The prompt is built with three hard caps:

- `max_samples=5` — only the first N samples after sorting.
- `max_bytes_per_sample=800` — each sample is tail-truncated to fit.
- `max_chars=16000` — the rendered prompt is final-truncated to fit.

`SC_CATALOG_LLM_MAX_INPUT_TOKENS` is the operator-visible knob (the
prompt is sized to leave headroom under it). A single chatty topic
cannot blow the input budget regardless of its sample-payload size.

### C4 (Defer) Daily ceiling is not aware of monthly drift

Cumulative monthly spend has no ceiling today. If the daily limit is set
to $1 and a deployment runs every day, the maximum monthly spend is $30.
A monthly cap is reasonable to add but does not change the worst-case
risk profile — defer to v0.3.x if operators ask for it.

---

## Privacy findings

### P1 (Resolved) Sample redaction runs before persistence

Where: `streamcontext/catalog/builder.py::_redact_sample`,
`streamcontext/catalog/privacy.py::redact_value`.

Every sampled message passes through `_redact_sample` before it lands in
SQLite or feeds the inference layer. Two strategies combined:

- Field-name redaction (`SC_CATALOG_PII_FIELDS`) drops the configured
  keys recursively.
- Regex redaction (built-in + `SC_CATALOG_PII_PATTERNS`) masks email,
  phone, card, SSN, and operator-supplied shapes inside string values.

The inference layer applies the same patterns defensively, so a
misconfigured builder cannot leak through. Tests in
`tests/test_catalog_privacy.py` verify that emails, phones, and card
numbers in inputs do not appear in persisted samples.

### P2 (Resolved) Sample retention is operator-controlled

Where: `streamcontext/catalog/builder.py`,
`streamcontext/catalog/store.py::replace_samples`.

`SC_CATALOG_RETAIN_SAMPLES=false` causes the refresher to keep samples
in-memory only — long enough to feed the inference layer, then
discarded. The catalog row keeps the description, field annotations,
relationships, and activity stats; the `samples` table is left empty.
Operators who do not want any sample payloads on disk can flip this
flag without losing inference quality.

### P3 (Resolved) Schema fingerprint is collision-resistant

Where: `streamcontext/catalog/introspect.py::SchemaIntrospector.introspect`.

Fingerprints are SHA-256 over the canonical JSON form of the schema
(`sort_keys=True`, no whitespace). Python's `hash()` is not used. The
fingerprint feeds the inference cache key, so a stable fingerprint is
load-bearing for cost control.

### P4 (Resolved) LLM provider data policies are documented

Where: `docs/data-handling.md`.

The data-handling doc lists what data each provider sees and what each
provider does with it by default. Catalog inference defaults to
`disabled` — operators have to opt in deliberately. Local Ollama is
documented as the no-egress option.

### P5 (Defer) Sample contents are not encrypted at rest

The SQLite file holding samples is not encrypted. If samples are
sensitive and the host disk is not encrypted, an attacker with disk
access can read them. Two acceptable mitigations today: rely on
host-level disk encryption, or set `SC_CATALOG_RETAIN_SAMPLES=false`.
Application-level encryption is deferred — it would conflict with the
SQLite-as-shared-medium architecture and is not free.

---

## Correctness findings

### K1 (Resolved) Confidence values flow through to agents

Where: `streamcontext/catalog/models.py::FieldEntry`,
`streamcontext/mcp_models.py::SchemaField,FieldExplanation`.

Every LLM-derived annotation carries a self-reported confidence in
[0, 1]. `describe_topic`, `explain_field`, and `find_topics_by_purpose`
all surface that confidence. Agents can decide whether to trust a
low-confidence annotation before they construct a filter on it.

### K2 (Resolved) Stale-aspect refresh is per-aspect

Where: `streamcontext/catalog/builder.py::CatalogBuilder.stale_aspects`.

Schema, samples, stats, and inference each have their own TTL. The
refresher loop touches only the aspects that are past their TTL, so a
fast-moving stats refresh does not invalidate a perfectly fresh
inference run.

### K3 (Fix in v0.3.x) Inference responses are parsed loosely

Where: `streamcontext/catalog/inference.py::_safe_json`.

The parser tolerates fenced JSON blocks and embedded objects in
conversational responses. This makes inference more reliable across
provider quirks but means a malformed response can produce a
`failed` status without a clear hint to the operator. Acceptable today;
revisit if false negatives become common.

### K4 (Defer) No feedback loop or fine-tuning

The catalog never learns from operator corrections to inferred
descriptions or annotations. Operators can edit the SQLite rows by
hand, but the change is overwritten on the next refresh. A
"keep this inferred meaning" override is reasonable future work.

---

## Operational findings

### O1 (Resolved) Refresher is a separate process

Where: `streamcontext/catalog/refresher.py`, `docs/architecture.md`.

The refresher runs as `python -m streamcontext.catalog.refresher`,
independent of the ingestion gateway and the MCP server. The three
processes share state only through the SQLite catalog and the Qdrant
collection. A refresher crash does not affect ingest or search.

### O2 (Resolved) Per-tool authorization hook

Where: `streamcontext/mcp_server.py::build_server`.

`build_server(authorize=...)` accepts an async hook called before every
tool. The default is no-op, preserving v0.2 behaviour. A deployment that
needs per-caller auth can supply its own check (JWT, mTLS subject, etc.)
without forking the server. The hook runs before the rate limiter so
denied calls do not consume token-bucket capacity.

### O3 (Resolved) Allowlist gates the catalog reads

Where: `streamcontext/mcp_catalog.py::CatalogReader`.

The `CatalogReader` honours the same `SC_MCP_TOPIC_ALLOWLIST` the search
engine does. `find_topics_by_purpose` only ranks allowlisted topics;
`get_topic_relationships` filters out edges that point to off-allowlist
targets so agents cannot enumerate restricted topics via the catalog.

### O4 (Defer) No background metrics export

The catalog logs structured events but does not export Prometheus
metrics today. Recommend `audit-v1.0.md` track this alongside the
broader observability story.
