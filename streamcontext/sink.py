"""Vector sink protocol and Qdrant implementation.

The sink owns the vector store wire protocol so the rest of the pipeline can
remain transport-agnostic. Adding Pinecone or pgvector is a single new class.
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Protocol, runtime_checkable

from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models as rest

from streamcontext.config import Settings
from streamcontext.logging import get_logger
from streamcontext.types import VectorRecord

log = get_logger("streamcontext.sink")


@runtime_checkable
class VectorSink(Protocol):
    async def ensure_ready(self) -> None: ...
    async def upsert(self, records: list[VectorRecord]) -> None: ...
    async def close(self) -> None: ...


def stable_uuid(stable_id: str) -> str:
    """Deterministic UUID5 from a stable string id (e.g. topic:part:offset).

    Qdrant point IDs must be unsigned int or UUID. Hashing into UUID5 lets the
    same Kafka record always map to the same point — replays / restarts upsert
    in place instead of duplicating.
    """
    digest = hashlib.sha256(stable_id.encode("utf-8")).hexdigest()
    return str(uuid.UUID(digest[:32]))


class QdrantSink:
    def __init__(
        self,
        url: str,
        collection: str,
        vector_dim: int,
        distance: rest.Distance = rest.Distance.COSINE,
    ) -> None:
        self._url = url
        self._collection = collection
        self._dim = vector_dim
        self._distance = distance
        self._client = AsyncQdrantClient(url=url)
        self._ready = False

    async def ensure_ready(self) -> None:
        if self._ready:
            return
        existing = await self._client.collection_exists(self._collection)
        if not existing:
            log.info("sink.qdrant.creating_collection", collection=self._collection, dim=self._dim)
            await self._client.create_collection(
                collection_name=self._collection,
                vectors_config=rest.VectorParams(size=self._dim, distance=self._distance),
            )
        else:
            log.info("sink.qdrant.collection_ready", collection=self._collection)
        self._ready = True

    async def upsert(self, records: list[VectorRecord]) -> None:
        if not records:
            return
        points = [
            rest.PointStruct(
                id=stable_uuid(r.id),
                vector=r.vector,
                payload=r.payload,
            )
            for r in records
        ]
        await self._client.upsert(collection_name=self._collection, points=points, wait=False)
        log.debug("sink.qdrant.upsert", n=len(points))

    async def close(self) -> None:
        await self._client.close()


def build_sink(settings: Settings) -> VectorSink:
    if settings.sink_provider == "qdrant":
        return QdrantSink(
            url=settings.qdrant_url,
            collection=settings.qdrant_collection,
            vector_dim=settings.qdrant_vector_dim,
        )
    raise ValueError(f"Unknown sink provider: {settings.sink_provider!r}")
