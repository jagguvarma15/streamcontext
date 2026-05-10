# Using streamcontext with MCP-compatible agents

The streamcontext MCP server is a separate process from the ingestion gateway. It reads from the same Qdrant collection the gateway writes to, exposes one or more MCP tools, and runs over stdio (Claude Desktop, Cursor, Cline) or SSE (HTTP-based hosts).

This is the v0.2 layer. v0.1 only had the ingestion sink — see `docs/architecture.md` for the two-process picture.

## Tools available in v0.2.0-alpha

| Tool | Purpose |
|---|---|
| `search_events` | Semantic search over the vector store. Returns Kafka records ranked by relevance, with topic/partition/offset/timestamp coordinates. Supports `topic`, `time_range_minutes`, and `score_threshold` filters. |

More tools (`list_topics`, `describe_topic`, `find_similar_events`) land on Day 3.

## Prerequisites

1. The ingestion gateway has been running long enough to have populated some vectors. If you cloned today, run `docker compose up -d` and `python examples/producer.py` for a minute first.
2. Python 3.11+ available on PATH for the agent host (the agent host is what launches the MCP process).
3. `pip install -e .` (or `pip install streamcontext`) in an environment the agent host can reach.

## Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%/Claude/claude_desktop_config.json` (Windows). Add a `streamcontext` entry under `mcpServers`:

```json
{
  "mcpServers": {
    "streamcontext": {
      "command": "python",
      "args": ["-m", "streamcontext.mcp_main"],
      "env": {
        "SC_QDRANT_URL": "http://localhost:6333",
        "SC_QDRANT_COLLECTION": "streamcontext",
        "SC_QDRANT_VECTOR_DIM": "384",
        "SC_EMBEDDER_PROVIDER": "local",
        "SC_EMBEDDER_MODEL": "all-MiniLM-L6-v2",
        "SC_MCP_TOPIC_ALLOWLIST": "orders",
        "SC_MCP_MAX_RESULTS": "50",
        "SC_MCP_TOOL_TIMEOUT_SEC": "5",
        "SC_LOG_JSON": "false"
      }
    }
  }
}
```

Notes:

- `SC_MCP_TOPIC_ALLOWLIST` is the security fence between the agent and your data. Set it to the comma-separated subset of topics you want the agent to see. If empty, the server logs a warning and applies no restriction.
- `SC_QDRANT_VECTOR_DIM` and `SC_EMBEDDER_MODEL` must match the values the ingestion gateway used when it created the collection — otherwise the embedder produces vectors of the wrong dimension and `search_events` will fail at startup with a `ConfigurationError`.
- Restart Claude Desktop after editing the config.

Once Claude Desktop reconnects, you should see a `streamcontext` server listed in the tools panel. Try a prompt like "search the streamcontext events for high-value orders from California."

## Cursor / Cline

Same shape: register a stdio MCP server with `command: python`, `args: ["-m", "streamcontext.mcp_main"]`, and the same env block. Refer to your IDE's MCP settings UI.

## SSE transport (custom hosts)

For HTTP-based hosts that prefer SSE:

```bash
streamcontext-mcp --transport sse --host 127.0.0.1 --port 8765
```

Bind to `127.0.0.1` unless you have an authenticated reverse proxy in front — the MCP layer itself does not implement auth in v0.2 (see `docs/security.md`, threat model).

## Debugging the connection

- Run the MCP process by hand first: `streamcontext-mcp` and watch stderr for the `mcp.start` log line. If it doesn't appear, the embedder probe failed — usually a Qdrant URL or vector-dim mismatch.
- Set `SC_LOG_JSON=false` in the env block while debugging — easier to read than JSON in agent logs.
- Tool invocations are logged at INFO with the call parameters and result count, so you can correlate "agent asked for X" with "engine searched for Y."
