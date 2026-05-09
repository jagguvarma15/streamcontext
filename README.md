# streamcontext

> **The semantic gateway between Kafka and AI agents.**
> Stream events into vector context, expose your cluster to LLMs via MCP (coming in v0.2), and let agents query your real-time data like a queryable substrate.

[![status](https://img.shields.io/badge/status-alpha--v0.1-orange)](#roadmap)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![python](https://img.shields.io/badge/python-3.11%2B-blue)](pyproject.toml)

---

## Why this exists

LLM agents are great at reasoning, terrible at remembering. RAG fixes the memory problem for *static* knowledge — docs, wikis, codebases. But the most operationally interesting data in any company isn't static: it's flowing through Kafka right now. Orders, clicks, alerts, sensor readings, deploys.

`streamcontext` is the missing intermediary. Point it at a Kafka topic and it continuously embeds messages into a vector store, preserving full Kafka metadata (topic, partition, offset, timestamp, headers, key) as filterable payload. Your agents query the stream like any other knowledge base — except this one is always current.

In v0.2 the same gateway will expose an MCP server so any MCP-compatible agent (Claude Desktop, Cursor, Cline, your own) can query the stream as a tool, with no glue code.

## Quickstart (5 minutes)

> **Coming together day-by-day this week.** This section will be the runnable demo by Day 3.

```bash
git clone https://github.com/jagadeshvarma/streamcontext.git
cd streamcontext
cp .env.example .env

# Spin up Kafka + Schema Registry + Qdrant + the gateway
docker compose up -d

# In another terminal, generate synthetic e-commerce events
python examples/producer.py

# Verify retrieval works
python examples/query.py "high-value orders from California"
```

You should see the top-5 matching orders within seconds, each annotated with its Kafka topic/partition/offset.

## Architecture

```
   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
   │  Kafka   │ →  │ consumer │ →  │ embedder │ →  │  sink    │ →  Qdrant
   │  topics  │    │ (avro)   │    │ (st/oai) │    │ (batched)│
   └──────────┘    └──────────┘    └──────────┘    └──────────┘
                                                         │
                                                         ▼
                                            ┌────────────────────────┐
                                            │  MCP server (v0.2)     │  → Claude / Cursor / agents
                                            │  Semantic catalog (v0.3)│
                                            └────────────────────────┘
```

See [docs/architecture.md](docs/architecture.md) for design rationale.

## Roadmap

- **v0.1 — streaming sink (this week).** Kafka → embed → Qdrant, with batching, retries, and graceful shutdown.
- **v0.2 — MCP server.** Expose the vector store as MCP tools so agents can query the stream natively.
- **v0.3 — semantic catalog.** Auto-discover topics, infer schemas, surface them as MCP resources.
- **Later.** Multi-sink (Pinecone, Weaviate, pgvector). Selective field embedding. Kafka Connect packaging. Prometheus metrics.

## Contributing

Issues and PRs welcome. The codebase is intentionally small in v0.1 — read it in 30 minutes, contribute on day one.

## License

MIT — see [LICENSE](LICENSE).
