# streamcontext

> **The semantic gateway between Kafka and AI agents.**
> Stream Kafka events into a vector store and let agents query your real-time data like a queryable substrate. MCP server lands in v0.2.

[![status](https://img.shields.io/badge/status-alpha--v0.1-orange)](#roadmap)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![python](https://img.shields.io/badge/python-3.11%2B-blue)](pyproject.toml)
[![docker](https://img.shields.io/badge/docker-compose-2496ED?logo=docker)](docker-compose.yml)

> 📽 _A 30-second demo GIF will live here once recorded — see `docs/architecture.md` for the diagram in the meantime._

---

## Why this exists

LLM agents are great at reasoning, terrible at remembering. RAG fixes the memory problem for *static* knowledge — docs, wikis, codebases. But the most operationally interesting data in any company isn't static: it's flowing through Kafka right now. Orders, clicks, alerts, sensor readings, deploys.

`streamcontext` is the missing intermediary. Point it at a Kafka topic and it continuously embeds messages into a vector store, preserving full Kafka metadata (topic, partition, offset, timestamp, headers, key) as filterable payload. Your agents query the stream like any other knowledge base — except this one is always current.

In v0.2 the same gateway will expose an MCP server so any MCP-compatible agent (Claude Desktop, Cursor, Cline, your own) can query the stream natively, with no glue code.

## Quickstart (5 minutes)

Requirements: Docker, Python 3.11+, [`uv`](https://github.com/astral-sh/uv) (or pip).

```bash
git clone https://github.com/jagadeshvarma/streamcontext.git
cd streamcontext
cp .env.example .env

# 1. Spin up Kafka + Schema Registry + Qdrant + the gateway
docker compose up -d

# 2. Create a local virtualenv for the producer/query scripts
uv venv && source .venv/bin/activate
uv pip install -e .

# 3. Generate synthetic e-commerce orders into the `orders` topic
python examples/producer.py --rate 5

# 4. In another terminal: query the stream by meaning
python examples/query.py "high-value orders from California"
python examples/query.py "refunded apparel" --topk 3
```

Within seconds you should see top-K matching orders, each annotated with its Kafka coordinates (`topic:partition:offset`). The Qdrant dashboard at <http://localhost:6333/dashboard> visualizes the points as they arrive.

### Cleanup

```bash
docker compose down            # stop services, keep volumes
docker compose down -v         # also delete Kafka/Qdrant data
```

## How it fits together

```
   Kafka topic(s)  ─►  consumer  ─►  embedder  ─►  batched sink  ─►  Qdrant
                       (avro/SR)     (ST/OAI)      (offsets         (queryable
                                                    committed         vector
                                                    after upsert)     store)
```

Full design notes — text-extraction strategy, batching, offset semantics, deterministic point IDs — in [docs/architecture.md](docs/architecture.md).

## Configuration

Everything is env-driven. See [`.env.example`](.env.example) for the full list. Most useful knobs:

| Variable | Default | Notes |
|---|---|---|
| `SC_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `SC_KAFKA_TOPICS` | `orders` | Comma-separated. |
| `SC_SCHEMA_REGISTRY_URL` | `http://localhost:8081` | |
| `SC_EMBEDDER_PROVIDER` | `local` | `local` (sentence-transformers) or `openai`. |
| `SC_EMBEDDER_MODEL` | `all-MiniLM-L6-v2` | If you change models, update `SC_QDRANT_VECTOR_DIM`. |
| `SC_QDRANT_COLLECTION` | `streamcontext` | |
| `SC_QDRANT_VECTOR_DIM` | `384` | Must match the embedder. |
| `SC_BATCH_SIZE` | `32` | Messages per embed/upsert call. |
| `SC_BATCH_FLUSH_INTERVAL_SEC` | `1.0` | Time-based flush trigger. |

## Roadmap

- **v0.1 — streaming sink (this week).** Kafka → embed → Qdrant, with batching, retries, deterministic upserts, and graceful shutdown. ✅
- **v0.2 — MCP server.** Expose the vector store as MCP tools so agents query the stream natively.
- **v0.3 — semantic catalog.** Auto-discover topics, infer schemas from SR, surface them as MCP resources so agents can ask *which* topic to query.
- **Later.** Multi-sink (Pinecone, Weaviate, pgvector). Selective field embedding & Jinja text templates. Kafka Connect packaging. Prometheus `/metrics`.

## Development

```bash
uv pip install -e '.[dev]'
pytest -q                      # unit tests (no Docker required)
RUN_INTEGRATION=1 pytest -q    # integration test (spins up Kafka + Qdrant via testcontainers)
ruff check .
```

## Contributing

The codebase is intentionally small in v0.1 — read it in 30 minutes, contribute on day one. Issues and PRs welcome, especially:

- New `VectorSink` implementations (Pinecone, pgvector, Weaviate)
- Better text-extraction strategies (Jinja templates, field selection)
- New language clients for the producer demo

## License

MIT — see [LICENSE](LICENSE).
