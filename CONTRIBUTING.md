# Contributing to streamcontext

Thanks for considering a contribution! The codebase is small on purpose — you should be able to read it end-to-end in 30 minutes.

## Setup

```bash
git clone https://github.com/jagadeshvarma/streamcontext.git
cd streamcontext
uv venv && source .venv/bin/activate
uv pip install -e '.[dev]'
```

## Running the stack

```bash
docker compose up -d                # Kafka + SR + Qdrant + gateway
python examples/producer.py         # synthetic data
python examples/query.py "your nl query"
```

## Running tests

```bash
pytest -q                           # fast, no Docker
RUN_INTEGRATION=1 pytest -q         # spins up Kafka + Qdrant via testcontainers
ruff check .
```

## What we want

- New `VectorSink` implementations (Pinecone, Weaviate, pgvector, Milvus). They should be a single new file plus a `build_sink` switch.
- Better text-extraction strategies — Jinja templates, field selection, multi-field concatenation.
- Producer demos in other languages (Java, Go) so non-Python shops can take it for a spin.
- Bug reports with a minimal reproduction (a topic schema + producer snippet that triggers the issue).

## What we'll defer

- Anything that requires building the MCP server right now — that's v0.2 and lives on its own branch.
- Multi-tenancy / RBAC. Not until v1.0.
- A web UI. The Qdrant dashboard is the UI for now.

## Style

- Type hints on public APIs.
- `ruff` clean — `ruff check .` and `ruff format .`.
- Docstrings on public functions when the *why* isn't obvious from the code.
- Keep modules under ~250 lines when reasonable. Split before they grow.
