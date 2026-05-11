"""Per-tool token-bucket rate limiter for the MCP layer.

The MCP server is single-asyncio-process and serves one agent client at a
time, so in-memory state is sufficient. The limiter is sync (no awaits) and
relies on asyncio's single-threadedness for safe access.
"""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class TokenBucket:
    """Classic token bucket. `capacity` tokens, refills at `refill_per_sec`."""

    capacity: float
    refill_per_sec: float
    tokens: float
    last_refill: float

    @classmethod
    def new(cls, capacity: int, refill_per_sec: float) -> "TokenBucket":
        return cls(
            capacity=float(capacity),
            refill_per_sec=float(refill_per_sec),
            tokens=float(capacity),
            last_refill=time.monotonic(),
        )

    def try_take(self, n: float = 1.0) -> bool:
        now = time.monotonic()
        elapsed = max(0.0, now - self.last_refill)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_sec)
        self.last_refill = now
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False

    def retry_after_sec(self, n: float = 1.0) -> float:
        deficit = max(0.0, n - self.tokens)
        if self.refill_per_sec <= 0:
            return float("inf")
        return deficit / self.refill_per_sec


class ToolRateLimiter:
    """Per-tool rate limit using token buckets.

    Each tool name gets its own bucket sized at `per_minute` tokens, refilling
    smoothly at `per_minute / 60` tokens per second. Disabled (always allow)
    when `per_minute` is zero.
    """

    def __init__(self, per_minute: int) -> None:
        self._capacity = int(per_minute)
        self._refill_per_sec = per_minute / 60.0 if per_minute > 0 else 0.0
        self._buckets: dict[str, TokenBucket] = {}

    @property
    def enabled(self) -> bool:
        return self._capacity > 0

    def _bucket(self, tool: str) -> TokenBucket:
        b = self._buckets.get(tool)
        if b is None:
            b = TokenBucket.new(self._capacity, self._refill_per_sec)
            self._buckets[tool] = b
        return b

    def check(self, tool: str) -> tuple[bool, float]:
        """Return (allowed, retry_after_sec_if_denied)."""
        if not self.enabled:
            return True, 0.0
        b = self._bucket(tool)
        if b.try_take(1.0):
            return True, 0.0
        return False, b.retry_after_sec(1.0)
