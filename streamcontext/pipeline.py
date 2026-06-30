"""Pipeline orchestrator: consumer -> batched embedder -> sink.

Batching is not optional: per-message embedding is roughly fifty times slower
than batched. A batch flushes when it reaches `batch_size` OR when
`batch_flush_interval_sec` elapses since the first message in the batch
arrived (whichever first).

Offsets are committed only after a batch is durably upserted into the sink.
On persistent failure (after retries) the pipeline halts rather than skipping
the failed batch — silent data loss is the worst failure mode for an agent
RAG source.
"""

from __future__ import annotations

import asyncio
import signal
import time
from collections.abc import Iterable
from contextlib import suppress

from aiokafka import TopicPartition
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential

from streamcontext.config import Settings
from streamcontext.consumer import AvroKafkaConsumer
from streamcontext.embedder import Embedder, build_embedder, message_to_text
from streamcontext.errors import ConfigurationError, PipelineFatalError
from streamcontext.logging import get_logger
from streamcontext.redaction import redact
from streamcontext.sink import VectorSink, build_sink
from streamcontext.types import KafkaMessage, VectorRecord

__all__ = ["Pipeline", "build_and_run"]

log = get_logger("streamcontext.pipeline")


def _build_record(
    msg: KafkaMessage,
    vector: list[float],
    redact_fields: frozenset[str],
    include_headers: bool,
) -> VectorRecord:
    payload = {
        "topic": msg.topic,
        "partition": msg.partition,
        "offset": msg.offset,
        "timestamp_ms": msg.timestamp_ms,
        "key": msg.key,
        "value": redact(msg.value, redact_fields),
    }
    if include_headers:
        payload["headers"] = redact(msg.headers, redact_fields)
    return VectorRecord(id=msg.stable_id, vector=vector, payload=payload)


def _max_offsets(messages: Iterable[KafkaMessage]) -> dict[TopicPartition, int]:
    """For each (topic, partition) seen, return the next offset to commit."""
    out: dict[TopicPartition, int] = {}
    for m in messages:
        tp = TopicPartition(m.topic, m.partition)
        next_off = m.offset + 1
        if next_off > out.get(tp, -1):
            out[tp] = next_off
    return out


class Pipeline:
    def __init__(
        self,
        consumer: AvroKafkaConsumer,
        embedder: Embedder,
        sink: VectorSink,
        batch_size: int,
        flush_interval_sec: float,
        redact_fields: frozenset[str] = frozenset(),
        include_headers: bool = False,
    ) -> None:
        self._consumer = consumer
        self._embedder = embedder
        self._sink = sink
        self._batch_size = batch_size
        self._flush_interval = flush_interval_sec
        self._redact_fields = redact_fields
        self._include_headers = include_headers
        self._stop = asyncio.Event()
        self._fatal: PipelineFatalError | None = None
        # Throughput counters reset between log lines.
        self._counter_messages = 0
        self._counter_batches = 0

    def request_stop(self) -> None:
        if not self._stop.is_set():
            log.info("pipeline.stop_requested")
            self._stop.set()

    async def _flush(self, batch: list[KafkaMessage]) -> None:
        """Embed, upsert, commit — or raise PipelineFatalError on persistent failure."""
        if not batch:
            return
        t0 = time.perf_counter()
        texts = [message_to_text(m) for m in batch]
        try:
            vectors = await self._retrying(self._embedder.embed, texts)
        except Exception as exc:
            log.exception("pipeline.embed_failed_giving_up", batch_size=len(batch))
            raise PipelineFatalError(
                f"embedder failed for batch of {len(batch)} after retries: {exc}"
            ) from exc
        embed_ms = (time.perf_counter() - t0) * 1000

        records = [
            _build_record(m, v, self._redact_fields, self._include_headers)
            for m, v in zip(batch, vectors, strict=True)
        ]

        t1 = time.perf_counter()
        try:
            await self._retrying(self._sink.upsert, records)
        except Exception as exc:
            log.exception("pipeline.sink_failed_giving_up", batch_size=len(batch))
            raise PipelineFatalError(
                f"sink upsert failed for batch of {len(batch)} after retries: {exc}"
            ) from exc
        sink_ms = (time.perf_counter() - t1) * 1000

        offsets = _max_offsets(batch)
        try:
            await self._consumer.commit(offsets)
        except Exception as exc:
            log.exception("pipeline.commit_failed", offsets={str(k): v for k, v in offsets.items()})
            raise PipelineFatalError(f"offset commit failed: {exc}") from exc

        self._counter_messages += len(batch)
        self._counter_batches += 1
        log.info(
            "pipeline.batch_flushed",
            n=len(batch),
            embed_ms=round(embed_ms, 1),
            sink_ms=round(sink_ms, 1),
        )

    @staticmethod
    async def _retrying(coro_func, *args, **kwargs):
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
            reraise=True,
        ):
            with attempt:
                return await coro_func(*args, **kwargs)

    async def run(self) -> None:
        await self._sink.ensure_ready()
        await self._consumer.start()

        batch: list[KafkaMessage] = []
        batch_started_at: float | None = None
        report_task = asyncio.create_task(self._throughput_reporter())

        async def _flush_due() -> None:
            nonlocal batch, batch_started_at
            if batch:
                to_flush, batch = batch, []
                batch_started_at = None
                await self._flush(to_flush)

        try:
            msg_iter = self._consumer.messages().__aiter__()
            while not self._stop.is_set():
                try:
                    timeout = self._flush_interval
                    if batch_started_at is not None:
                        elapsed = time.monotonic() - batch_started_at
                        timeout = max(0.0, self._flush_interval - elapsed)
                    msg = await asyncio.wait_for(msg_iter.__anext__(), timeout=timeout)
                except TimeoutError:
                    await _flush_due()
                    continue
                except StopAsyncIteration:
                    break

                if not batch:
                    batch_started_at = time.monotonic()
                batch.append(msg)
                if len(batch) >= self._batch_size:
                    await _flush_due()

            # Final flush on graceful shutdown.
            await _flush_due()
        except PipelineFatalError as exc:
            self._fatal = exc
            log.error("pipeline.fatal", error=str(exc))
            self.request_stop()
        finally:
            report_task.cancel()
            with suppress(asyncio.CancelledError):
                await report_task
            await self._consumer.stop()
            await self._sink.close()
            if self._fatal is not None:
                # Re-raise so the entrypoint can exit non-zero and surface it
                # to whatever supervises the process.
                raise self._fatal

    async def _throughput_reporter(self) -> None:
        try:
            while True:
                await asyncio.sleep(10)
                if self._counter_messages or self._counter_batches:
                    log.info(
                        "pipeline.throughput",
                        msgs_per_10s=self._counter_messages,
                        batches=self._counter_batches,
                    )
                    self._counter_messages = 0
                    self._counter_batches = 0
        except asyncio.CancelledError:
            return


def _validate_dim(embedder_dim: int, settings_dim: int) -> None:
    if embedder_dim != settings_dim:
        raise ConfigurationError(
            f"embedder produces vectors of dim {embedder_dim} but "
            f"SC_QDRANT_VECTOR_DIM={settings_dim}. Set SC_QDRANT_VECTOR_DIM={embedder_dim} "
            f"(and recreate the Qdrant collection if it was already created at the wrong size)."
        )


async def build_and_run(settings: Settings) -> None:
    consumer = AvroKafkaConsumer(settings)
    embedder = build_embedder(settings)
    # Force lazy load now so we can validate dim at startup, not mid-batch.
    await embedder.embed(["__startup_dim_probe__"])
    _validate_dim(embedder.dim, settings.qdrant_vector_dim)

    sink = build_sink(settings)
    pipeline = Pipeline(
        consumer=consumer,
        embedder=embedder,
        sink=sink,
        batch_size=settings.batch_size,
        flush_interval_sec=settings.batch_flush_interval_sec,
        redact_fields=settings.redact_fields_set,
        include_headers=settings.payload_include_headers,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, pipeline.request_stop)

    await pipeline.run()
