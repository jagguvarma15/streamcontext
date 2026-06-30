"""Entrypoint for the streamcontext MCP server.

Runs as a separate process from the ingestion gateway. Reads embedder and
Qdrant config from the same env vars (`SC_*`); ignores Kafka config since the
MCP server never talks to Kafka directly.

Default transport is stdio (Claude Desktop, Cursor, Cline). Override with
`--transport sse` for HTTP-based hosts.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from fastmcp import FastMCP
from qdrant_client import AsyncQdrantClient

from streamcontext.catalog.store import CatalogStore
from streamcontext.config import load_settings
from streamcontext.embedder import CachedEmbedder, build_embedder
from streamcontext.errors import ConfigurationError
from streamcontext.logging import configure_logging, get_logger
from streamcontext.mcp_catalog import CatalogReader
from streamcontext.mcp_search import SearchEngine, _SchemaRegistryLike
from streamcontext.mcp_server import build_server, warn_if_allowlist_empty


def _try_schema_registry(url: str) -> _SchemaRegistryLike | None:
    """Best-effort SR client construction. None on import or connect failure."""
    log = get_logger("streamcontext.mcp")
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient
    except ImportError:
        log.debug("mcp.schema_registry.unavailable", reason="confluent_kafka not installed")
        return None
    try:
        client = SchemaRegistryClient({"url": url})
        # No-op probe: list subjects. If the host is unreachable this raises.
        client.get_subjects()
        log.info("mcp.schema_registry.connected", url=url)
        return client
    except Exception as exc:
        log.warning("mcp.schema_registry.unreachable", url=url, error=str(exc))
        return None


async def _prepare() -> tuple[FastMCP, AsyncQdrantClient]:
    """Load config, build embedder + engine + server. Returns (mcp, client) so
    we can close the client cleanly on shutdown."""
    settings = load_settings()
    # Force JSON logging off when stdio transport is in use, otherwise the
    # MCP host can mistake log lines for protocol frames. Keep stderr-only.
    configure_logging(level=settings.log_level, json=settings.log_json)
    log = get_logger("streamcontext.mcp")

    inner_embedder = build_embedder(settings)
    # Probe to lock the dim before serving the first query.
    await inner_embedder.embed(["__startup_dim_probe__"])
    if inner_embedder.dim != settings.qdrant_vector_dim:
        raise ConfigurationError(
            f"embedder produces dim {inner_embedder.dim} but SC_QDRANT_VECTOR_DIM="
            f"{settings.qdrant_vector_dim}. Set them to match."
        )
    embedder = CachedEmbedder(inner_embedder, max_size=settings.mcp_embed_cache_size)

    client = AsyncQdrantClient(url=settings.qdrant_url)
    sr_client = _try_schema_registry(settings.schema_registry_url)
    catalog_reader: CatalogReader | None = None
    try:
        catalog_store = CatalogStore(settings.catalog_db_path)
        catalog_reader = CatalogReader(
            store=catalog_store,
            allowlist=settings.mcp_topic_allowlist_set,
        )
        log.info("mcp.catalog.attached", db_path=str(catalog_store.path))
    except Exception as exc:
        log.warning("mcp.catalog.unavailable", error=str(exc))
    engine = SearchEngine(
        embedder=embedder,
        client=client,
        collection=settings.qdrant_collection,
        topic_allowlist=settings.mcp_topic_allowlist_set,
        max_results=settings.mcp_max_results,
        max_time_range_minutes=settings.mcp_max_time_range_minutes,
        max_value_bytes=settings.mcp_max_value_bytes,
        schema_registry=sr_client,
        catalog=catalog_reader,
    )
    warn_if_allowlist_empty(settings)
    log.info(
        "mcp.start",
        collection=settings.qdrant_collection,
        embedder=settings.embedder_provider,
        embedder_model=settings.embedder_model,
        embedder_dim=embedder.dim,
        topic_allowlist=sorted(settings.mcp_topic_allowlist_set),
        max_results=settings.mcp_max_results,
        max_time_range_minutes=settings.mcp_max_time_range_minutes,
        max_value_bytes=settings.mcp_max_value_bytes,
        tool_timeout_sec=settings.mcp_tool_timeout_sec,
        rate_limit_per_minute=settings.mcp_rate_limit_per_minute,
        embed_cache_size=settings.mcp_embed_cache_size,
        schema_registry=bool(sr_client),
    )
    mcp = build_server(engine, settings)
    return mcp, client


def run() -> None:
    parser = argparse.ArgumentParser(description="streamcontext MCP server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
        help="MCP transport. stdio for Claude Desktop / Cursor / Cline; sse for HTTP hosts.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind host for sse transport.")
    parser.add_argument("--port", type=int, default=8765, help="Bind port for sse transport.")
    args = parser.parse_args()

    try:
        mcp, client = asyncio.run(_prepare())
    except ConfigurationError as exc:
        print(f"streamcontext-mcp: configuration error: {exc}", file=sys.stderr)
        sys.exit(78)

    try:
        if args.transport == "stdio":
            mcp.run()  # blocks; FastMCP handles its own loop
        else:
            mcp.run(transport="sse", host=args.host, port=args.port)
    finally:
        # Best-effort cleanup of the Qdrant client on exit.
        try:
            asyncio.run(client.close())
        except Exception:
            pass


if __name__ == "__main__":
    run()
