# Demo script

A three-minute walkthrough storyboard for the release video. The goal is to show,
end to end, that a Kafka stream becomes something an AI agent can discover,
understand, and query in natural language without anyone hand-writing
documentation for it.

Recording target: about three minutes. Screen capture at 1080p, terminal and
Claude Desktop side by side where possible. Keep the cursor calm; let each
command's output settle before moving on.

## Scene 1 — The problem (0:00-0:25)

On screen: the raw Avro schema (`schemas/order.avsc`) next to a Kafka topic
scrolling messages in a console consumer.

Narration: "This is an order stream. Every company has dozens of these. The data
is current and operationally interesting, but to an AI agent it is opaque. The
agent sees a topic name and a wall of fields. It does not know what `risk_score`
means, or that orders join to payments on `customer_id`."

## Scene 2 — Stand up the stack (0:25-0:50)

On screen, type:

    docker compose up -d
    python examples/producer.py --rate 5

Narration: "streamcontext runs three small processes. The ingestion gateway
embeds every Kafka message into a vector store, keeping the full Kafka coordinate
on each record. We start it, and a synthetic order stream, with two commands."

Let the Qdrant dashboard (http://localhost:6333/dashboard) show points arriving.

## Scene 3 — Build the catalog (0:50-1:20)

On screen, type:

    python -m streamcontext.catalog.refresher

Narration: "The catalog refresher reads each topic's schema, samples a few recent
messages, and asks a small model, bounded by a hard daily spend ceiling, to
describe the topic and every field in plain language. It also detects how topics
relate. All of it lands in a single SQLite file the agent's tools read."

Show a log line confirming the topic was described, or a quick read of the
catalog row.

## Scene 4 — Wire up the agent (1:20-1:40)

On screen: the `claude_desktop_config.json` block from the README, then Claude
Desktop restarting and the `streamcontext` server appearing in the tools panel.

Narration: "The MCP server runs next to the agent. Seven tools. No custom glue."

## Scene 5 — Ask in natural language (1:40-2:40)

In Claude Desktop, type three prompts and let the tool calls show:

1. "What kinds of data are flowing through Kafka right now?"
   Triggers `list_topics` / `find_topics_by_purpose`; returns topics with their
   inferred descriptions.
2. "What does the risk_score field on orders mean, and show me a few values."
   Triggers `explain_field`; returns the inferred meaning, a confidence, and
   example values drawn from samples.
3. "Find high-value orders from California in the last hour."
   Triggers `search_events` with a time window and structured filters; each hit
   cites its `topic:partition:offset`.

Narration: "The agent discovers the stream, understands a field it has never
seen, and runs a precise semantic query, citing exactly where each record came
from."

## Scene 6 — Close (2:40-3:00)

On screen: the architecture diagram from `docs/architecture.md`.

Narration: "Three processes, two shared files, no schema spelunking. Your event
streams are now self-describing, to agents, not just to humans. It is MIT
licensed and small enough to read in an afternoon."

End card: the repository URL, github.com/jagguvarma15/streamcontext.

## Notes for the recorder

- Run the producer for a minute before recording Scene 5 so the time-window query
  has data to return.
- If inference is disabled (no provider key set), Scenes 3 and 5 still work:
  topics fall back to schema-derived descriptions. Either mention this on camera
  or set `SC_CATALOG_LLM_PROVIDER` beforehand for the richer narration.
- Keep secrets out of frame. The config block references environment variables,
  not literal keys.
