"""FastMCP server wrapper around the streamcontext search engine.

Exposes one tool in v0.2.0-alpha (`search_events`). More tools land on Day 3
(list_topics, describe_topic, find_similar_events).

The server is a separate process from the ingestion pipeline. They share state
through Qdrant only — no in-process coupling. See `docs/architecture.md`.
"""

from __future__ import annotations

import asyncio
from typing import Annotated, Any

from fastmcp import FastMCP
from pydantic import Field

from streamcontext.config import Settings
from streamcontext.logging import get_logger
from streamcontext.mcp_models import SearchResponse, ToolError
from streamcontext.mcp_search import SearchEngine

log = get_logger("streamcontext.mcp.server")

SERVER_NAME = "streamcontext"
SERVER_INSTRUCTIONS = (
    "Search a Kafka-derived vector store. Use `search_events` to find Kafka "
    "records by meaning. Each result carries the Kafka topic, partition, "
    "offset, and timestamp so you can cite it precisely. Restrict scope with "
    "the `topic` and `time_range_minutes` arguments when the user mentions "
    "a particular stream or recent activity."
)


def build_server(engine: SearchEngine, settings: Settings) -> FastMCP:
    """Construct a FastMCP server with all v0.2 tools registered.

    The engine is injected so unit tests can build a server against fakes.
    """
    mcp = FastMCP(name=SERVER_NAME, instructions=SERVER_INSTRUCTIONS)
    timeout = settings.mcp_tool_timeout_sec

    @mcp.tool()
    async def search_events(
        query: Annotated[
            str,
            Field(
                min_length=1,
                max_length=2000,
                description="Natural-language description of what to find.",
            ),
        ],
        limit: Annotated[
            int,
            Field(
                ge=1,
                le=settings.mcp_max_results,
                description="Maximum number of results to return.",
            ),
        ] = 10,
        topic: Annotated[
            str | None,
            Field(
                default=None,
                max_length=200,
                description="Restrict search to a single Kafka topic.",
            ),
        ] = None,
        time_range_minutes: Annotated[
            int | None,
            Field(
                default=None,
                ge=1,
                le=settings.mcp_max_time_range_minutes,
                description="Only return records produced within the last N minutes.",
            ),
        ] = None,
        score_threshold: Annotated[
            float | None,
            Field(
                default=None,
                ge=-1.0,
                le=1.0,
                description="Drop results whose cosine similarity is below this value.",
            ),
        ] = None,
    ) -> SearchResponse | ToolError:
        """Semantic search over the streamcontext vector store.

        Returns Kafka records ranked by similarity to `query`, optionally
        filtered by `topic` and `time_range_minutes`. Each result includes
        the original record value plus its Kafka coordinates.
        """
        try:
            return await asyncio.wait_for(
                engine.search_events(
                    query=query,
                    limit=limit,
                    topic=topic,
                    time_range_minutes=time_range_minutes,
                    score_threshold=score_threshold,
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            log.warning("mcp.search_events.timeout", timeout_sec=timeout)
            return ToolError(
                code="timeout",
                message=f"search_events exceeded {timeout:.1f}s server-side timeout.",
            )
        except Exception as exc:
            log.exception("mcp.search_events.error")
            return ToolError(code="internal_error", message=str(exc))

    return mcp


def warn_if_allowlist_empty(settings: Settings) -> None:
    if not settings.mcp_topic_allowlist_set:
        log.warning(
            "mcp.topic_allowlist.empty",
            note=(
                "SC_MCP_TOPIC_ALLOWLIST is empty; the MCP server will search "
                "every topic present in the vector store. Set the allowlist "
                "to restrict agent visibility."
            ),
        )


__all__ = ["build_server", "warn_if_allowlist_empty", "SERVER_NAME"]
