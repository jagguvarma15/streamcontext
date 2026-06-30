"""Tests for the v0.2 hardening pass: rate limit, embed cache, value-size cap."""

from __future__ import annotations

import pytest

from streamcontext.embedder import CachedEmbedder
from streamcontext.mcp_search import _maybe_truncate_value
from streamcontext.rate_limit import TokenBucket, ToolRateLimiter

# ---------- TokenBucket / ToolRateLimiter ----------


def test_token_bucket_initial_capacity_allows_burst() -> None:
    b = TokenBucket.new(capacity=5, refill_per_sec=1.0)
    for _ in range(5):
        assert b.try_take(1.0) is True
    assert b.try_take(1.0) is False


def test_token_bucket_refills_over_time() -> None:
    b = TokenBucket.new(capacity=2, refill_per_sec=10.0)
    assert b.try_take(2.0)
    assert not b.try_take(1.0)
    # Backdate last_refill so the next call computes a deterministic refill.
    b.last_refill -= 0.5  # 0.5s * 10/s = 5 tokens (clamped to capacity=2)
    assert b.try_take(1.0)
    assert b.try_take(1.0)
    assert not b.try_take(1.0)


def test_tool_rate_limiter_per_tool_buckets_independent() -> None:
    rl = ToolRateLimiter(per_minute=2)
    assert rl.check("search_events") == (True, 0.0)
    assert rl.check("search_events")[0] is True
    ok, retry = rl.check("search_events")
    assert ok is False and retry > 0
    # A second tool has its own bucket and is still allowed.
    assert rl.check("list_topics")[0] is True


def test_tool_rate_limiter_disabled_when_zero() -> None:
    rl = ToolRateLimiter(per_minute=0)
    assert rl.enabled is False
    for _ in range(1000):
        ok, retry = rl.check("any")
        assert ok is True and retry == 0.0


# ---------- CachedEmbedder ----------


class CountingEmbedder:
    dim = 4

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    async def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        return [[float(len(t)), 0.0, 0.0, 0.0] for t in texts]


@pytest.mark.asyncio
async def test_cached_embedder_hits_and_misses() -> None:
    inner = CountingEmbedder()
    c = CachedEmbedder(inner, max_size=3)

    v1 = await c.embed(["alpha"])
    v2 = await c.embed(["alpha"])
    assert v1 == v2
    assert c.hits == 1
    assert c.misses == 1
    # Inner only saw the miss.
    assert inner.calls == [["alpha"]]


@pytest.mark.asyncio
async def test_cached_embedder_mixed_batch_passes_only_misses_through() -> None:
    inner = CountingEmbedder()
    c = CachedEmbedder(inner, max_size=10)
    # Pre-warm a couple.
    await c.embed(["a", "b"])
    inner.calls.clear()

    out = await c.embed(["a", "c", "b", "d"])
    # Order is preserved.
    assert [v[0] for v in out] == [len("a"), len("c"), len("b"), len("d")]
    # Inner only saw the new misses.
    assert inner.calls == [["c", "d"]]
    assert c.hits == 2  # a, b


@pytest.mark.asyncio
async def test_cached_embedder_lru_evicts_oldest() -> None:
    inner = CountingEmbedder()
    c = CachedEmbedder(inner, max_size=2)
    await c.embed(["a"])
    await c.embed(["b"])
    await c.embed(["c"])  # evicts "a"
    inner.calls.clear()
    await c.embed(["a"])  # miss again - "a" was evicted
    assert inner.calls == [["a"]]


@pytest.mark.asyncio
async def test_cached_embedder_disabled_when_size_zero() -> None:
    inner = CountingEmbedder()
    c = CachedEmbedder(inner, max_size=0)
    await c.embed(["x"])
    await c.embed(["x"])
    assert inner.calls == [["x"], ["x"]]
    assert c.hits == 0


# ---------- value truncation ----------


def test_value_truncation_passes_small_payloads_through() -> None:
    v = {"order_id": "abc", "total": 9.5}
    out, truncated = _maybe_truncate_value(v, max_bytes=4096)
    assert out is v
    assert truncated is False


def test_value_truncation_replaces_oversize_payloads_with_stub() -> None:
    big = {"text": "x" * 20_000}
    out, truncated = _maybe_truncate_value(big, max_bytes=1024)
    assert truncated is True
    assert out["_truncated"] is True
    assert out["_size_bytes"] >= 20_000
    assert isinstance(out["_preview"], str)
    assert len(out["_preview"]) <= 1024


def test_value_truncation_disabled_when_max_zero() -> None:
    big = {"text": "x" * 1024}
    out, truncated = _maybe_truncate_value(big, max_bytes=0)
    assert truncated is False
    assert out is big
