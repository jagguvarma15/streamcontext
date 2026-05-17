"""Per-tool authorization gate tests.

We test `make_gate` directly rather than the fastmcp-decorated handlers so
the test is independent of FastMCP version and Python's lazy-annotation
quirks. The gate is what every tool calls, so verifying its behavior
verifies the authorization contract.
"""

from __future__ import annotations

import pytest

from streamcontext.mcp_models import ToolError
from streamcontext.mcp_server import make_gate
from streamcontext.rate_limit import ToolRateLimiter


@pytest.mark.asyncio
async def test_default_gate_allows_when_no_hook_and_unlimited():
    gate = make_gate(authorize=None, limiter=ToolRateLimiter(per_minute=0))
    assert await gate("search_events") is None
    assert await gate("explain_field") is None


@pytest.mark.asyncio
async def test_gate_denies_when_authorize_returns_tool_error():
    async def deny(tool: str) -> ToolError | None:
        if tool in ("search_events", "find_topics_by_purpose"):
            return ToolError(code="not_authorized", message=f"{tool} denied")
        return None

    gate = make_gate(authorize=deny, limiter=ToolRateLimiter(per_minute=0))
    denied = await gate("search_events")
    assert isinstance(denied, ToolError) and denied.code == "not_authorized"
    # Untouched tools fall through.
    assert await gate("list_topics") is None


@pytest.mark.asyncio
async def test_gate_runs_authorization_before_rate_limit():
    """A denied call must not consume a token-bucket slot."""
    limiter = ToolRateLimiter(per_minute=1)

    async def deny_all(tool: str) -> ToolError | None:
        return ToolError(code="not_authorized", message="denied")

    gate = make_gate(authorize=deny_all, limiter=limiter)
    # Hit the gate many times; the limiter must not deplete because every
    # call is rejected at the authorization stage first.
    for _ in range(10):
        denied = await gate("search_events")
        assert isinstance(denied, ToolError)
        assert denied.code == "not_authorized"

    # Once the hook stops denying, the very first call is still allowed —
    # proving the limiter has its full budget intact.
    permissive_gate = make_gate(authorize=None, limiter=limiter)
    assert await permissive_gate("search_events") is None
    # Second call within the minute trips the rate limit, confirming the
    # bucket size really was 1.
    second = await permissive_gate("search_events")
    assert isinstance(second, ToolError) and second.code == "rate_limited"


@pytest.mark.asyncio
async def test_gate_rate_limits_when_authorization_allows():
    async def allow_all(tool: str) -> ToolError | None:
        return None

    gate = make_gate(authorize=allow_all, limiter=ToolRateLimiter(per_minute=2))
    assert await gate("describe_topic") is None
    assert await gate("describe_topic") is None
    denied = await gate("describe_topic")
    assert isinstance(denied, ToolError) and denied.code == "rate_limited"
