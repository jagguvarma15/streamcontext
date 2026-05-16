# Example agent conversations

Three real conversation patterns the v0.2 MCP tools are tuned for, with the tool calls a well-prompted agent should make and notes on what to set on the gateway to make each fast.

For every example below, assume the gateway has been running with:

```
SC_KAFKA_TOPICS=orders,clicks
SC_MCP_TOPIC_ALLOWLIST=orders,clicks
SC_PAYLOAD_INDEX_FIELDS=status,region,channel,currency
```

and the synthetic producer from `examples/producer.py` has been running for a few minutes.

---

## 1. "Find high-value orders from California in the last hour"

The query has three intents the agent needs to separate:

- topic: `orders`
- time range: the last 60 minutes
- structured filter: `region` equals a US_WEST-ish value, `total` is large

Good agent behavior:

```text
search_events(
    query="high-value orders",
    topic="orders",
    time_range_minutes=60,
    filters=[
        {"field": "region", "eq": "US_WEST"},
        {"field": "total", "gte": 200.0}
    ],
    limit=10
)
```

Notes:

- `region` and `total` are filtered structurally, not embedded into the query string. Embedding "California orders over $200" works, but is much less reliable than a hard filter when the data has the field.
- Because `region` is in `SC_PAYLOAD_INDEX_FIELDS`, this filter runs at index speed.
- `total` is not a keyword field. The range filter still works, just without an index.

## 2. "What failed transactions did we see today?"

Here the agent should ground in the schema before guessing field names:

```text
list_topics()
# -> sees "orders" and "clicks"; user said "transactions" so "orders" is the match

describe_topic(name="orders")
# -> sees fields including status with values like
#    pending, paid, shipped, delivered, cancelled, refunded
#    and customer-facing copy from sample records

search_events(
    query="failed transactions",
    topic="orders",
    time_range_minutes=1440,
    filters=[{"field": "status", "in_values": ["cancelled", "refunded"]}],
    limit=20
)
```

Notes:

- The agent uses `describe_topic` to discover that the status field exists and what values it takes. Without that, it might guess `status=failed` (which doesn't exist) and return zero results.
- `in_values` is the right move when the user's word ("failed") maps to multiple enum values.

## 3. "Anything weird in the last 5 minutes?"

No specific topic, no obvious filter - the agent should embed the abstract intent and add diversity so the result set isn't ten variations of the same noisy event:

```text
search_events(
    query="unusual, anomalous, or unexpected event",
    time_range_minutes=5,
    diverse=true,
    limit=10
)
```

Notes:

- `diverse=true` triggers MMR reranking. The engine pulls 3x candidates, then balances cosine similarity against per-result novelty so near-duplicates are dropped from the top-K.
- A score_threshold can be added when the user wants high confidence: `score_threshold=0.5` drops anything below that cosine.

## 4. Follow-up: "Find more like this one"

The user picks a specific result and wants neighbors:

```text
find_similar_events(reference_id="orders:2:14732", limit=5)
```

Notes:

- The reference comes from the `coord.stable_id` field on a previous `search_events` result.
- The engine retrieves the stored vector for that record and does a similarity search using it directly - no re-embedding, no re-tokenization.
- The reference itself is excluded from results.
- If the topic in the reference is outside `SC_MCP_TOPIC_ALLOWLIST`, the tool returns a structured `not_found` error indistinguishable from a genuine miss.

---

## 5. "What kinds of customer data do we have flowing?"

When the user asks an open-ended discovery question, the agent should start with the catalog rather than guessing topic names.

```text
list_topics()
# -> each entry now carries an inferred description, e.g.
#    {"name": "billing_events", "description": "Customer billing and payment records.", ...}

find_topics_by_purpose(description="customer-related data", limit=5)
# -> ranks topics by cosine similarity to the description.
#    Top result: billing_events (inferred description matches strongly).
#    Second: customers (synthesized description from field names).
```

Notes:

- `find_topics_by_purpose` is the right entry point when the user describes a goal but does not name a topic.
- If a topic has no catalog-inferred description, the tool falls back to a synthesized one from the topic name and its first few field names. The response sets `description_source="synthesized"` so the agent knows the match is weaker.

## 6. "Find all failed payments and the orders they're attached to"

A multi-topic question. The agent should use `get_topic_relationships` to discover the join field before doing two searches.

```text
get_topic_relationships(topic="payment_attempts")
# -> {"source_topic": "payment_attempts", "target_topic": "orders",
#     "relationship_type": "foreign_reference",
#     "source_field": "order_id", "target_field": "order_id",
#     "confidence": 0.91}

search_events(
    query="failed payment",
    topic="payment_attempts",
    filters=[{"field": "status", "eq": "failed"}],
    limit=20,
)
# -> use the order_id values from each result to fetch the matching orders:
search_events(
    query="order details",
    topic="orders",
    filters=[{"field": "order_id", "in_values": [...]}],
    limit=20,
)
```

Notes:

- The catalog encodes the relationship once. Without it, the agent has to read two Avro schemas and infer the join by hand — slow and error-prone.
- `relationship_type` matters: `foreign_reference` means the value on the source side is the primary identifier on the target side, so a `customer_id` filter is safe.

## 7. "What does the `risk_score` field actually mean?"

Use `explain_field` before writing predicates on an unfamiliar field.

```text
explain_field(topic="fraud_decisions", field="risk_score")
# -> {"type": "double", "inferred_meaning": "model-derived likelihood that the
#     transaction is fraudulent, range 0..1",
#     "inferred_confidence": 0.85,
#     "example_values": [0.02, 0.18, 0.74, 0.91, 0.55]}
```

Notes:

- The example values let the agent pick a reasonable threshold (`gte=0.5` rather than guessing).
- A low `inferred_confidence` is a signal that the agent should either ask the user to confirm the semantics or fall back to a fuzzier semantic search instead of a hard predicate.

---

## Tuning checklist

If results feel poor, walk this list:

1. Are the filter fields in `SC_PAYLOAD_INDEX_FIELDS`? Without indexes, filters work but each query does a full-collection scan.
2. Is the embedder model the same on the ingestion gateway and the MCP process? Mismatched models silently produce useless similarity scores.
3. Is the agent calling `describe_topic` (or `find_topics_by_purpose`) before constructing structured filters? Without that grounding it guesses field names.
4. Are the user's queries hitting near-duplicates (e.g. when one customer fires many similar events)? Set `diverse=true`.
5. Is the catalog refresher running? `list_topics` without descriptions, or `find_topics_by_purpose` returning only `description_source="synthesized"` results, means the catalog has not been populated. Start it with `python -m streamcontext.catalog.refresher --loop`.
6. Is `SC_CATALOG_LLM_PROVIDER` set? With `disabled`, the catalog still indexes schemas and samples, but it cannot generate descriptions or field meanings.
