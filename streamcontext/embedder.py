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
