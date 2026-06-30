# Why your Kafka cluster needs a semantic catalog (and why your data catalog tool isn't it)

When a new engineer joins your team and asks "where are the payment
events?", what do they do? They Slack a teammate. They open Confluence.
They grep for `topic =` in three repos. Eventually they find an Avro
schema in a `*.avsc` file and reverse-engineer the rest from a sample
message someone pasted into a thread two years ago.

Now imagine that engineer is an LLM agent. It does not have a teammate
to Slack. It does not have a Confluence search. The grep works, but the
LLM has no way to know that the field called `cust_xid` in one topic is
the same identifier as `customer_id` in another. It has no idea that
`status` in `orders` is an enum with five specific values, or that
`amount` in `transactions` is in cents and not dollars.

So the agent guesses. And the guesses are usually wrong.

This is the gap a semantic catalog fills.

## What "semantic catalog" means in a streaming context

A semantic catalog is a queryable layer that captures, for every topic
in your cluster:

- **What this topic is** — a one- or two-sentence natural-language
  description.
- **What each field means** — not just the Avro type, but the actual
  business meaning. `total_cents: monetary value, units appear to be
  USD cents`.
- **How topics relate** — `orders.customer_id` is a foreign reference
  to `customers.customer_id`; `payment_attempts` and `order_completions`
  are the same flow at different stages.
- **Activity context** — what's been flowing in the last hour, last
  day, schema-version history.
- **Real examples** — sample messages, redacted, so an agent can see
  the *shape* of the data before it writes a filter predicate.

The key word is *queryable*. The catalog isn't a wiki page someone
maintains. It's structured data your agents pull through tool calls.

## Why your data catalog tool isn't this

DataHub. Atlan. Alation. Collibra. These are real, useful products. They
are also fundamentally **batch, manual, and built for humans**.

- **Batch.** Most data catalogs scan source systems on a schedule —
  daily, sometimes hourly. That's fine for a warehouse table that
  changes shape twice a year. It's pointless for a Kafka topic where
  the schema can roll forward in the middle of a deploy.
- **Manual.** The descriptions, ownership tags, glossary entries — most
  of them are entered by humans. They drift. The owner left. The field
  meaning changed. The catalog says "this is the order amount in
  dollars" and the producer has been emitting cents for the last six
  months.
- **Built for humans.** The UI is a web page. The "API" is an
  afterthought, often optimized for ingestion (write metadata in) not
  for retrieval (have a smart consumer pull metadata out). An agent
  trying to use these via API works in spite of the design, not because
  of it.

None of these are mistakes. They are appropriate design choices for a
data catalog that serves a data engineering team. They are the wrong
choices for a catalog that serves an agent making 50 tool calls a
minute.

## What a catalog built for agents looks like

Three differences matter.

### 1. It is continuous, not batch

Schemas, samples, and activity stats refresh on TTLs measured in
**minutes**, not days. Inferred descriptions and relationships refresh
on TTLs measured in **hours**, gated by a daily cost ceiling so a
runaway can't bill you $5,000 overnight. The catalog is *eventually
fresh* the same way DNS is — there is a known staleness bound, agents
know what to expect, and the system degrades gracefully when inference
is disabled.

### 2. It is mostly inferred, not curated

Schemas come from Schema Registry. Samples come from a short-lived
consumer that reads the last N messages with a fresh group id (so it
doesn't disturb your real consumers' offsets). Descriptions and field
meanings come from an LLM that sees the schema plus a handful of
redacted samples and produces a structured JSON response. Relationships
come from heuristic shared-key detection with sample-value overlap,
polished by the same LLM for cases like `payment_attempts ↔
order_completions` that share no key but are obviously the same flow.

Humans don't have to write any of it. They can override anything they
disagree with. The system gets useful immediately, not after six months
of glossary curation.

### 3. It is consumed via tool calls, not URLs

The catalog exposes itself through MCP tools shaped for agents:

- `list_topics` — every topic, every inferred description.
- `find_topics_by_purpose("billing data")` — embeds the query, ranks
  topics by cosine similarity to their inferred descriptions.
- `get_topic_relationships("orders")` — returns the join graph.
- `explain_field("transactions", "amount")` — returns the inferred
  meaning plus a handful of example values.

An agent answering "find all failed payments and the orders they're
attached to" calls `get_topic_relationships`, sees the
`payment_attempts → orders` foreign-reference edge, and constructs two
correctly-joined `search_events` calls. Without the catalog, it would
either guess or give up.

## What changes when you have one

The single biggest shift: agents stop *searching* and start *reasoning
about data*.

Before:

> *"I'll try searching for failed payments. No results. Let me try
> 'declined transactions'. A few results, none look right. Let me try
> 'unsuccessful charges'..."*

After:

> *"I see `payment_attempts` in the catalog with an inferred
> description mentioning `status` field with values
> `succeeded/failed/pending`. The relationship graph shows it links to
> `orders` via `order_id`. Let me search `payment_attempts` filtered to
> `status=failed`, then fetch the corresponding orders."*

The first conversation produces noise. The second produces an answer.

## The cost question

A reasonable objection: "an LLM-powered catalog sounds expensive." It
isn't, if you build it right.

- **Cache by (schema_fingerprint, sample_hash).** Identical inputs
  never cost twice. A topic that has the same schema and similar samples
  to yesterday gets free inference.
- **Use the smallest capable model.** Claude Haiku is excellent at this
  kind of structured extraction at $1/M input tokens. Don't pay for
  Opus when Haiku does the job.
- **Bound the prompt.** Cap the per-sample byte budget and the
  per-topic token budget. A chatty topic with a 5 MB sample message
  doesn't blow the input budget.
- **Daily spend ceiling per provider.** Once tripped, inference falls
  back to schema-only entries with a clear flag. Agents see the topic,
  schema, samples, and stats; only the LLM-derived bits are missing.

Practical day-zero cost on a 10-topic cluster with Haiku: well under
$0.05/day once the cache is warm.

## Where this fits in the stack

This is not a replacement for DataHub. If your job is to track lineage
back to the warehouse, manage business glossaries, or run governance
workflows for your analytics team, DataHub is still the right tool.

The semantic catalog is a different layer, with a different consumer:

- **Data catalog (DataHub, Atlan, …):** for humans. Glossary,
  ownership, lineage, governance. Updated daily.
- **Semantic catalog:** for agents. Inferred descriptions, field
  meanings, relationships. Updated continuously.

You can run both. They answer different questions.

## What about hand-rolling this?

You can. The pieces aren't exotic: Schema Registry walk, sample
consumer, LLM call, SQLite cache. The tricky parts are the ones nobody
talks about:

- Cache key design so identical inputs don't bill twice.
- PII redaction *before* persistence, not just before LLM submission.
- Daily spend ceiling that actually works under concurrent calls.
- Graceful degradation to schema-only when inference is disabled.
- Sample retention semantics that let you keep metadata only.
- Allowlist gating so a multi-topic catalog doesn't leak topic names
  through the relationship graph.

These are the ergonomic edges that take a weekend prototype and turn it
into something you'd point at a real cluster.

## What this is, concretely

[streamcontext](https://github.com/jagguvarma15/streamcontext) is one
implementation of this idea. Three cooperating processes: an ingestion
gateway that embeds Kafka messages into Qdrant, an MCP server that
serves agents, and a catalog refresher that maintains the semantic
catalog described above. SQLite holds the catalog; Qdrant holds the
vectors; everything else is stateless.

It's not the only way to build this. It is, as far as I can tell, the
first one wired end-to-end through MCP — which is what makes it useful
to an agent today rather than after a year of integration work.

The interesting question is not whether you should build a semantic
catalog. The interesting question is what your agents will be able to
do *next* once they stop guessing about your data.
