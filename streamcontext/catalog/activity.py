"""Activity stats derived from the vector store.

We compute message-rate, rolling counters, and observed schema versions from
the Qdrant payload (which the gateway has already written) rather than going
back to Kafka. This keeps the cost cheap and avoids consuming offsets we
don't own.
"""

from __future__ import annotations

import time
from typing import Any, Protocol

from qdrant_client.http import models as rest

from streamcontext.catalog.models import ActivityStats
from streamcontext.logging import get_logger

log = get_logger("streamcontext.catalog.activity")


class _QdrantLike(Protocol):
    async def count(
        self,
        collection_name: str,
        count_filter: Any | None = None,
        exact: bool = False,
    ) -> Any: ...

    async def scroll(
        self,
        collection_name: str,
        scroll_filter: Any | None = None,
        limit: int = 10,
        with_payload: bool = True,
        with_vectors: bool = False,
        order_by: Any | None = None,
    ) -> Any: ...


class ActivityProfiler:
    """Computes activity stats for one topic using Qdrant payloads."""

    def __init__(self, client: _QdrantLike, collection: str) -> None:
        self._client = client
        self._collection = collection

    async def profile(self, topic: str) -> ActivityStats:
        now_ms = int(time.time() * 1000)
        hour_ago = now_ms - 3_600_000
        day_ago = now_ms - 86_400_000
        last_hour = await self._count_since(topic, hour_ago)
        last_day = await self._count_since(topic, day_ago)
        rate_per_minute = last_hour / 60.0
        latest_ts = await self._latest_timestamp(topic)
        versions = await self._observed_versions(topic)
        return ActivityStats(
            messages_last_hour=last_hour,
            messages_last_day=last_day,
            rate_per_minute_last_hour=rate_per_minute,
            observed_schema_versions=versions,
            last_observed_ts_ms=latest_ts,
        )

    async def _count_since(self, topic: str, since_ms: int) -> int:
        flt = rest.Filter(
            must=[
                rest.FieldCondition(key="topic", match=rest.MatchValue(value=topic)),
                rest.FieldCondition(
                    key="timestamp_ms", range=rest.Range(gte=since_ms)
                ),
            ]
        )
        try:
            res = await self._client.count(
                collection_name=self._collection, count_filter=flt, exact=False
            )
        except Exception as exc:
            log.debug("catalog.activity.count_failed", topic=topic, error=str(exc))
            return 0
        return int(getattr(res, "count", 0))

    async def _latest_timestamp(self, topic: str) -> int | None:
        flt = rest.Filter(
            must=[rest.FieldCondition(key="topic", match=rest.MatchValue(value=topic))]
        )
        try:
            res = await self._client.scroll(
                collection_name=self._collection,
                scroll_filter=flt,
                limit=1,
                with_payload=True,
                with_vectors=False,
                order_by=rest.OrderBy(
                    key="timestamp_ms", direction=rest.Direction.DESC
                ),
            )
        except Exception as exc:
            log.debug("catalog.activity.latest_failed", topic=topic, error=str(exc))
            return None
        points, _ = res if isinstance(res, tuple) else (getattr(res, "points", []), None)
        if not points:
            return None
        payload = getattr(points[0], "payload", None) or {}
        ts = payload.get("timestamp_ms")
        return int(ts) if isinstance(ts, int) else None

    async def _observed_versions(self, topic: str) -> list[int]:
        """Best-effort: scan a sample of records for schema version headers.

        The gateway does not write schema version into the payload today, so
        this returns an empty list unless an operator adds the field. The
        column exists so version-aware tooling can populate it later without a
        migration.
        """
        return []


__all__ = ["ActivityProfiler"]
