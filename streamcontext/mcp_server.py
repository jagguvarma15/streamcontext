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
from streamcontext.mcp_models import (
    FilterClause,
    SearchResponse,
    TopicDescription,
    TopicsResponse,
    ToolError,
)
from streamcontext.mcp_search import EventNotFoundError, SearchEngine
from streamcontext.rate_limit import ToolRateLimiter

log = get_logger("streamcontext.mcp.server")

SERVER_NAME = "streamcontext"
SERVER_INSTRUCTIONS = (
    "Search a Kafka-derived vector store. Workflow:\n"
    "1. Call `list_topics` first when the user references unfamiliar data, to "
    "see which streams are available.\n"
    "2. Call `describe_topic` to see the schema and a few sample records "
    "before constructing complex queries.\n"
    "3. Use `search_events` to find records by meaning. Restrict scope with "
    "`topic` and `time_range_minutes` when the user mentions a particular "
    "stream or recent activity.\n"
    "4. Use `find_similar_events` for incident-style follow-ups: 'find more "
    "events like this one' given a `topic:partition:offset` reference.\n"
    "Every result carries Kafka coordinates so you can cite it precisely."
)


def build_server(engine: SearchEngine, settings: Settings) -> FastMCP:
    """Construct a FastMCP server with all v0.2 tools registered.

    The engine is injected so unit tests can build a server against fakes.
    """
    mcp = FastMCP(name=SERVER_NAME, instructions=SERVER_INSTRUCTIONS)
    timeout = settings.mcp_tool_timeout_sec
    limiter = ToolRateLimiter(settings.mcp_rate_limit_per_minute)

    def _rate_limit(tool: str) -> ToolError | None:
        ok, retry = limiter.check(tool)
        if ok:
            return None
        log.warning("mcp.rate_limited", tool=tool, retry_after_sec=round(retry, 2))
        return ToolError(
            code="rate_limited",
            message=f"{tool} rate-limited; retry in {retry:.1f}s.",
        )

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
        filters: Annotated[
            list[FilterClause] | None,
            Field(
                default=None,
                max_length=10,
                description=(
                    "Structured filters applied alongside the semantic match. "
                    "Field names refer to keys inside the message value (e.g. "
                    "'status', 'region'); 'topic' and 'timestamp_ms' work too. "
                    "Each clause uses exactly one of eq, in_values, or gte/lte. "
                    "Add the field to SC_PAYLOAD_INDEX_FIELDS on the gateway for "
                    "fast filtering."
                ),
            ),
        ] = None,
        diverse: Annotated[
            bool,
            Field(
                default=False,
                description=(
                    "Apply maximal-marginal-relevance reranking so the result "
                    "set is less repetitive. Useful when raw similarity returns "
                    "many near-duplicates."
                ),
            ),
        ] = False,
    ) -> SearchResponse | ToolError:
        """Semantic search over the streamcontext vector store.

        Returns Kafka records ranked by similarity to `query`, optionally
        filtered by `topic`, `time_range_minutes`, and structured `filters`.
        Each result includes the original record value plus its Kafka
        coordinates. Set `diverse=true` to dedupe near-identical results.
        """
        denied = _rate_limit("search_events")
        if denied is not None:
            return denied
        try:
            return await asyncio.wait_for(
                engine.search_events(
                    query=query,
                    limit=limit,
                    topic=topic,
                    time_range_minutes=time_range_minutes,
                    score_threshold=score_threshold,
                    filters=filters,
                    diverse=diverse,
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

    @mcp.tool()
    async def list_topics() -> TopicsResponse | ToolError:
        """List Kafka topics available in the vector store with coarse stats.

        Returns one entry per topic the gateway has ingested (and that the
        operator has allowlisted to the MCP layer), with approximate count and
        the oldest and newest record timestamps when known. Call this before
        `search_events` if the user names a topic you have not seen yet.
        """
        denied = _rate_limit("list_topics")
        if denied is not None:
            return denied
        try:
            return await asyncio.wait_for(engine.list_topics(), timeout=timeout)
        except asyncio.TimeoutError:
            log.warning("mcp.list_topics.timeout", timeout_sec=timeout)
            return ToolError(
                code="timeout",
                message=f"list_topics exceeded {timeout:.1f}s server-side timeout.",
            )
        except Exception as exc:
            log.exception("mcp.list_topics.error")
            return ToolError(code="internal_error", message=str(exc))

    @mcp.tool()
    async def describe_topic(
        name: Annotated[
            str,
            Field(
                min_length=1,
                max_length=200,
                description="Kafka topic name to describe.",
            ),
        ],
        sample_size: Annotated[
            int,
            Field(
                ge=0,
                le=20,
                description="Number of recent records to include as samples.",
            ),
        ] = 5,
    ) -> TopicDescription | ToolError:
        """Describe a Kafka topic: schema, count, time window, and samples.

        Use this before constructing complex queries so you know which fields
        exist on records in this topic. If Schema Registry is unreachable the
        `schema_summary` field will be null.
        """
        denied = _rate_limit("describe_topic")
        if denied is not None:
            return denied
        try:
            return await asyncio.wait_for(
                engine.describe_topic(name=name, sample_size=sample_size),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            log.warning("mcp.describe_topic.timeout", topic=name, timeout_sec=timeout)
            return ToolError(
                code="timeout",
                message=f"describe_topic exceeded {timeout:.1f}s server-side timeout.",
            )
        except Exception as exc:
            log.exception("mcp.describe_topic.error", topic=name)
            return ToolError(code="internal_error", message=str(exc))

    @mcp.tool()
    async def find_similar_events(
        reference_id: Annotated[
            str,
            Field(
                min_length=5,
                max_length=300,
                description=(
                    "Kafka coordinate of the reference record, formatted "
                    "'topic:partition:offset' (e.g. 'orders:0:42')."
                ),
            ),
        ],
        limit: Annotated[
            int,
            Field(
                ge=1,
                le=settings.mcp_max_results,
                description="Maximum number of similar records to return.",
            ),
        ] = 10,
    ) -> SearchResponse | ToolError:
        """Find events semantically similar to a reference record.

        Useful for incident-style investigation: given a record the user is
        looking at, retrieve more like it. The reference itself is excluded
        from the results.
        """
        denied = _rate_limit("find_similar_events")
        if denied is not None:
            return denied
        try:
            return await asyncio.wait_for(
                engine.find_similar_events(reference_id=reference_id, limit=limit),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            log.warning("mcp.find_similar_events.timeout", ref=reference_id, timeout_sec=timeout)
            return ToolError(
                code="timeout",
                message=f"find_similar_events exceeded {timeout:.1f}s server-side timeout.",
            )
        except EventNotFoundError as exc:
            return ToolError(code="not_found", message=str(exc))
        except Exception as exc:
            log.exception("mcp.find_similar_events.error", ref=reference_id)
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
