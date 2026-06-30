"""FastMCP server wrapper around the streamcontext search engine.

Exposes seven tools: search_events, list_topics, describe_topic,
find_topics_by_purpose, get_topic_relationships, explain_field, and
find_similar_events. Search runs against the Qdrant vector store; the
catalog-backed tools read the semantic-catalog SQLite file.

The server is a separate process from the ingestion pipeline and the catalog
refresher; they share state through Qdrant and the catalog file only, with no
in-process coupling. See `docs/architecture.md`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from streamcontext.config import Settings
from streamcontext.logging import get_logger
from streamcontext.mcp_models import (
    FieldExplanation,
    FilterClause,
    FindTopicsResponse,
    RelationshipsResponse,
    SearchResponse,
    ToolError,
    TopicDescription,
    TopicsResponse,
)
from streamcontext.mcp_search import EventNotFoundError, SearchEngine
from streamcontext.rate_limit import ToolRateLimiter

log = get_logger("streamcontext.mcp.server")

# Type alias for the authorization hook. Implementations get the tool name and
# return None to allow the call, or a ToolError to deny it. The hook runs
# *before* the rate limiter so denied calls do not consume tokens.
AuthorizationHook = Callable[[str], Awaitable["ToolError | None"]]


def make_gate(
    *,
    authorize: AuthorizationHook | None,
    limiter: ToolRateLimiter,
) -> Callable[[str], Awaitable[ToolError | None]]:
    """Compose the per-tool authorization + rate-limit check.

    Returned as a standalone async callable so it is testable without
    standing up the FastMCP transport.
    """

    async def gate(tool: str) -> ToolError | None:
        if authorize is not None:
            denied = await authorize(tool)
            if denied is not None:
                log.info("mcp.unauthorized", tool=tool, code=denied.code)
                return denied
        ok, retry = limiter.check(tool)
        if ok:
            return None
        log.warning("mcp.rate_limited", tool=tool, retry_after_sec=round(retry, 2))
        return ToolError(
            code="rate_limited",
            message=f"{tool} rate-limited; retry in {retry:.1f}s.",
        )

    return gate


SERVER_NAME = "streamcontext"
SERVER_INSTRUCTIONS = (
    "Search a Kafka-derived vector store with a semantic catalog. Workflow:\n"
    "1. Call `list_topics` first when the user references unfamiliar data, to "
    "see which streams are available and their inferred descriptions.\n"
    "2. Call `find_topics_by_purpose` when the user describes a goal but does "
    "not name a topic: 'find me billing data', 'where are payment events?'.\n"
    "3. Call `describe_topic` to see the schema, sample records, and inferred "
    "field meanings before constructing complex queries.\n"
    "4. Call `get_topic_relationships` when answering multi-topic questions: "
    "the catalog knows which topics share keys or describe the same flow.\n"
    "5. Call `explain_field` when you need the meaning of a specific field or "
    "example values before writing a filter predicate.\n"
    "6. Use `search_events` to find records by meaning. Restrict scope with "
    "`topic` and `time_range_minutes` when the user mentions a particular "
    "stream or recent activity.\n"
    "7. Use `find_similar_events` for incident-style follow-ups: 'find more "
    "events like this one' given a `topic:partition:offset` reference.\n"
    "Every result carries Kafka coordinates so you can cite it precisely."
)


def build_server(
    engine: SearchEngine,
    settings: Settings,
    *,
    authorize: AuthorizationHook | None = None,
) -> FastMCP:
    """Construct a FastMCP server with all tools registered.

    The engine is injected so unit tests can build a server against fakes.

    `authorize` is an optional hook called before every tool runs. The
    default is no-op (anyone reachable on the transport may call any tool);
    in a multi-tenant deployment, plug in a real check here. The hook
    receives the tool name and returns either None (allow) or a ToolError
    (deny). The error code should be `not_authorized` so agents can
    distinguish missing permissions from other failures.
    """
    mcp = FastMCP(name=SERVER_NAME, instructions=SERVER_INSTRUCTIONS)
    timeout = settings.mcp_tool_timeout_sec
    limiter = ToolRateLimiter(settings.mcp_rate_limit_per_minute)
    _gate = make_gate(authorize=authorize, limiter=limiter)

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
        denied = await _gate("search_events")
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
        except TimeoutError:
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
        denied = await _gate("list_topics")
        if denied is not None:
            return denied
        try:
            return await asyncio.wait_for(engine.list_topics(), timeout=timeout)
        except TimeoutError:
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
        denied = await _gate("describe_topic")
        if denied is not None:
            return denied
        try:
            return await asyncio.wait_for(
                engine.describe_topic(name=name, sample_size=sample_size),
                timeout=timeout,
            )
        except TimeoutError:
            log.warning("mcp.describe_topic.timeout", topic=name, timeout_sec=timeout)
            return ToolError(
                code="timeout",
                message=f"describe_topic exceeded {timeout:.1f}s server-side timeout.",
            )
        except Exception as exc:
            log.exception("mcp.describe_topic.error", topic=name)
            return ToolError(code="internal_error", message=str(exc))

    @mcp.tool()
    async def find_topics_by_purpose(
        description: Annotated[
            str,
            Field(
                min_length=1,
                max_length=1000,
                description=(
                    "Natural-language description of the data you are looking "
                    "for, e.g. 'billing data', 'failed payment attempts'."
                ),
            ),
        ],
        limit: Annotated[
            int,
            Field(
                ge=1,
                le=settings.mcp_max_results,
                description="Maximum number of ranked topics to return.",
            ),
        ] = 5,
    ) -> FindTopicsResponse | ToolError:
        """Rank catalog topics by how well they match a purpose description.

        Embeds `description` and compares it to each topic's catalog
        description. Useful when the user describes a goal but does not name
        a topic. Requires the semantic catalog to be enabled.
        """
        denied = await _gate("find_topics_by_purpose")
        if denied is not None:
            return denied
        try:
            return await asyncio.wait_for(
                engine.find_topics_by_purpose(description=description, limit=limit),
                timeout=timeout,
            )
        except TimeoutError:
            log.warning("mcp.find_topics_by_purpose.timeout", timeout_sec=timeout)
            return ToolError(
                code="timeout",
                message=f"find_topics_by_purpose exceeded {timeout:.1f}s server-side timeout.",
            )
        except Exception as exc:
            log.exception("mcp.find_topics_by_purpose.error")
            return ToolError(code="internal_error", message=str(exc))

    @mcp.tool()
    async def get_topic_relationships(
        topic: Annotated[
            str,
            Field(
                min_length=1,
                max_length=200,
                description="Kafka topic whose relationships to retrieve.",
            ),
        ],
    ) -> RelationshipsResponse | ToolError:
        """Return relationships the catalog has detected for `topic`.

        Includes both heuristic matches (shared keys, foreign references) and
        semantic matches inferred by the catalog's LLM layer. Use this when
        the user asks a question that crosses topic boundaries so you can
        identify the joining field before searching.
        """
        denied = await _gate("get_topic_relationships")
        if denied is not None:
            return denied
        try:
            return await asyncio.wait_for(
                engine.get_topic_relationships(topic=topic), timeout=timeout
            )
        except TimeoutError:
            log.warning("mcp.get_topic_relationships.timeout", topic=topic, timeout_sec=timeout)
            return ToolError(
                code="timeout",
                message=f"get_topic_relationships exceeded {timeout:.1f}s server-side timeout.",
            )
        except Exception as exc:
            log.exception("mcp.get_topic_relationships.error", topic=topic)
            return ToolError(code="internal_error", message=str(exc))

    @mcp.tool()
    async def explain_field(
        topic: Annotated[
            str,
            Field(
                min_length=1,
                max_length=200,
                description="Kafka topic the field belongs to.",
            ),
        ],
        field: Annotated[
            str,
            Field(
                min_length=1,
                max_length=200,
                description=(
                    "Dotted field path, e.g. 'customer_id' or "
                    "'shipping_address.zip'."
                ),
            ),
        ],
    ) -> FieldExplanation | ToolError:
        """Return what the catalog knows about a single field.

        Includes type, doc string, the catalog's inferred meaning and
        confidence, plus a handful of example values pulled from recent
        samples so the agent can write accurate filter predicates.
        """
        denied = await _gate("explain_field")
        if denied is not None:
            return denied
        try:
            result = await asyncio.wait_for(
                engine.explain_field(topic=topic, field=field), timeout=timeout
            )
        except TimeoutError:
            log.warning("mcp.explain_field.timeout", topic=topic, field=field, timeout_sec=timeout)
            return ToolError(
                code="timeout",
                message=f"explain_field exceeded {timeout:.1f}s server-side timeout.",
            )
        except Exception as exc:
            log.exception("mcp.explain_field.error", topic=topic, field=field)
            return ToolError(code="internal_error", message=str(exc))
        if result is None:
            return ToolError(
                code="not_found",
                message=f"No catalog entry for topic {topic!r} and field {field!r}.",
            )
        return result

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
        denied = await _gate("find_similar_events")
        if denied is not None:
            return denied
        try:
            return await asyncio.wait_for(
                engine.find_similar_events(reference_id=reference_id, limit=limit),
                timeout=timeout,
            )
        except TimeoutError:
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


__all__ = ["SERVER_NAME", "build_server", "warn_if_allowlist_empty"]
