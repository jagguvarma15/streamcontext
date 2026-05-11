"""Embedder protocol and built-in implementations.

`Embedder` is a Protocol so adding a new provider is a single new class — no
inheritance hierarchy to fight. The pipeline depends on the protocol, never on
a concrete class.

v0.1 ships:
  - LocalEmbedder: sentence-transformers, runs on CPU/GPU, no API key needed.
  - OpenAIEmbedder: stub — wired up but raises until you `pip install
    streamcontext[openai]` and set OPENAI_API_KEY.

Text extraction is intentionally simple in v0.1: serialize the structured
record to canonical JSON. See docs/architecture.md for the rationale.
"""

from __future__ import annotations

import asyncio
import json
from collections import OrderedDict
from typing import Any, Protocol, runtime_checkable

from streamcontext.config import Settings
from streamcontext.logging import get_logger
from streamcontext.types import KafkaMessage

log = get_logger("streamcontext.embedder")


@runtime_checkable
class Embedder(Protocol):
    """Anything that turns a list of strings into a list of vectors."""

    dim: int

    async def embed(self, texts: list[str]) -> list[list[float]]:
        ...


def message_to_text(msg: KafkaMessage) -> str:
    """v0.1 strategy: canonical JSON of the message value.

    Sorted keys + default=str so embeddings are stable across runs and don't
    blow up on datetimes / Decimals coming out of Avro.
    """
    return json.dumps(msg.value, sort_keys=True, default=str, ensure_ascii=False)


class LocalEmbedder:
    """sentence-transformers wrapper. Lazily loads the model on first use."""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        self._model_name = model_name
        self._model: Any | None = None
        # Most popular MiniLM is 384; we patch the actual dim after load.
        self.dim: int = 384

    def _ensure_loaded(self) -> None:
        if self._model is not None:
            return
        # Imported lazily — sentence-transformers pulls in torch which is heavy.
        from sentence_transformers import SentenceTransformer

        log.info("embedder.local.loading", model=self._model_name)
        self._model = SentenceTransformer(self._model_name)
        self.dim = int(self._model.get_sentence_embedding_dimension())
        log.info("embedder.local.loaded", model=self._model_name, dim=self.dim)

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        self._ensure_loaded()
        # SentenceTransformer.encode is CPU-bound — push it to a thread so we
        # don't stall the event loop.
        vectors = await asyncio.to_thread(
            self._model.encode,  # type: ignore[union-attr]
            texts,
            batch_size=min(64, len(texts)),
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return [v.tolist() for v in vectors]


class OpenAIEmbedder:
    """OpenAI embeddings. Optional dep: `pip install streamcontext[openai]`."""

    # Common output dims for OpenAI embedding models — used to set self.dim
    # without a network call. Override via constructor if you use a different one.
    _MODEL_DIMS = {
        "text-embedding-3-small": 1536,
        "text-embedding-3-large": 3072,
        "text-embedding-ada-002": 1536,
    }

    def __init__(self, model_name: str = "text-embedding-3-small", dim: int | None = None) -> None:
        self._model_name = model_name
        self.dim = dim if dim is not None else self._MODEL_DIMS.get(model_name, 1536)
        try:
            from openai import AsyncOpenAI  # noqa: F401
        except ImportError as e:
            raise NotImplementedError(
                "OpenAIEmbedder requires the 'openai' extra. "
                "Install with: pip install streamcontext[openai]"
            ) from e
        from openai import AsyncOpenAI

        self._client = AsyncOpenAI()

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        resp = await self._client.embeddings.create(model=self._model_name, input=texts)
        return [d.embedding for d in resp.data]


class CachedEmbedder:
    """LRU wrapper around another Embedder, deduplicating identical inputs.

    Useful on the MCP-server path where many agent queries are repeats or
    near-repeats from the same conversation. For paid providers (OpenAI,
    Cohere, etc.) this is real money saved; for local models it is mostly
    a latency win. Disabled when `max_size <= 0`.
    """

    def __init__(self, inner: "Embedder", max_size: int = 256) -> None:
        self._inner = inner
        self._cache: OrderedDict[str, list[float]] = OrderedDict()
        self._max_size = max_size
        self.hits = 0
        self.misses = 0

    @property
    def dim(self) -> int:
        return self._inner.dim

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        if self._max_size <= 0:
            self.misses += len(texts)
            return await self._inner.embed(texts)

        out: list[list[float] | None] = [None] * len(texts)
        miss_indices: list[int] = []
        miss_texts: list[str] = []
        for i, t in enumerate(texts):
            if t in self._cache:
                vec = self._cache.pop(t)
                self._cache[t] = vec  # move-to-end for LRU
                out[i] = vec
                self.hits += 1
            else:
                miss_indices.append(i)
                miss_texts.append(t)
                self.misses += 1

        if miss_texts:
            new_vecs = await self._inner.embed(miss_texts)
            for idx, text, vec in zip(miss_indices, miss_texts, new_vecs, strict=True):
                out[idx] = vec
                self._cache[text] = vec
                while len(self._cache) > self._max_size:
                    self._cache.popitem(last=False)

        # All positions are filled because we either pulled from cache or
        # populated from the inner call.
        return [v for v in out if v is not None]


def build_embedder(settings: Settings) -> Embedder:
    if settings.embedder_provider == "local":
        return LocalEmbedder(settings.embedder_model)
    if settings.embedder_provider == "openai":
        return OpenAIEmbedder(settings.embedder_model)
    raise ValueError(f"Unknown embedder provider: {settings.embedder_provider!r}")


async def smoke_test() -> None:  # pragma: no cover - manual aid
    """Embed a few strings and print the first 8 dims of vector 0."""
    from streamcontext.config import load_settings
    from streamcontext.logging import configure_logging

    settings = load_settings()
    configure_logging(level=settings.log_level, json=False)
    embedder = build_embedder(settings)
    samples = [
        "The customer ordered a cast-iron skillet and a cutting board.",
        "Refunded order for noise-cancelling headphones, EU region.",
        "VIP customer in California — gift wrap requested.",
    ]
    vectors = await embedder.embed(samples)
    log.info(
        "embedder.smoke_test.done",
        n=len(vectors),
        dim=embedder.dim,
        first_8=vectors[0][:8] if vectors else None,
    )


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(smoke_test())
