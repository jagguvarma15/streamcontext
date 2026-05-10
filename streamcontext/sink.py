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

    # Core payload fields the MCP tools always filter or order by. Created
    # idempotently at startup so the read path doesn't pay a full-collection
    # scan for topic counts or chronological scrolls. User-configured field
    # indexes (status, region, etc.) land in v0.2.x.
    _CORE_INDEXES: tuple[tuple[str, rest.PayloadSchemaType], ...] = (
        ("topic", rest.PayloadSchemaType.KEYWORD),
        ("partition", rest.PayloadSchemaType.INTEGER),
        ("timestamp_ms", rest.PayloadSchemaType.INTEGER),
    )

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
        await self._ensure_core_indexes()
        self._ready = True

    async def _ensure_core_indexes(self) -> None:
        for field, schema in self._CORE_INDEXES:
            try:
                await self._client.create_payload_index(
                    collection_name=self._collection,
                    field_name=field,
                    field_schema=schema,
                )
                log.info("sink.qdrant.index_created", field=field)
            except Exception as exc:
                # Qdrant returns a non-fatal error when the index already
                # exists; treat anything here as best-effort.
                log.debug("sink.qdrant.index_skip", field=field, error=str(exc))

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
