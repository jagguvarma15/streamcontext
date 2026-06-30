"""Catalog refresher entrypoint.

Run as `python -m streamcontext.catalog.refresher` to refresh all configured
topics once, or with `--loop` to run on the configured cadence forever.

The refresher process is intentionally separate from both the ingestion
gateway and the MCP server. They share state via the SQLite catalog file
and the Qdrant collection only.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from collections.abc import Callable

from qdrant_client import AsyncQdrantClient

from streamcontext.catalog import metrics as cat
from streamcontext.catalog.activity import ActivityProfiler
from streamcontext.catalog.builder import CatalogBuilder
from streamcontext.catalog.inference import (
    InferenceEngine,
    LLMUnavailableError,
    build_llm_provider,
)
from streamcontext.catalog.introspect import MessageSampler, SchemaIntrospector
from streamcontext.catalog.models import CatalogConfig
from streamcontext.catalog.relationships import RelationshipDetector
from streamcontext.catalog.store import CatalogStore
from streamcontext.config import Settings, load_settings
from streamcontext.logging import configure_logging, get_logger
from streamcontext.observability import start_metrics_server

log = get_logger("streamcontext.catalog.refresher")


def _try_schema_registry(url: str):
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient
    except ImportError:
        return None
    try:
        client = SchemaRegistryClient({"url": url})
        client.get_subjects()
        return client
    except Exception as exc:
        log.warning("catalog.sr.unreachable", url=url, error=str(exc))
        return None


def build_catalog_config(settings: Settings) -> CatalogConfig:
    return CatalogConfig(
        schema_refresh_sec=settings.catalog_schema_refresh_sec,
        sample_refresh_sec=settings.catalog_sample_refresh_sec,
        stats_refresh_sec=settings.catalog_stats_refresh_sec,
        inference_refresh_sec=settings.catalog_inference_refresh_sec,
        sample_count=settings.catalog_sample_count,
        retain_samples=settings.catalog_retain_samples,
        daily_llm_spend_ceiling_usd=settings.catalog_llm_daily_ceiling_usd,
        pii_redact_patterns=settings.catalog_pii_patterns_list,
        pii_redact_fields=settings.catalog_pii_fields_list,
    )


def build_builder(
    settings: Settings,
) -> tuple[CatalogBuilder, RelationshipDetector, AsyncQdrantClient]:
    store = CatalogStore(settings.catalog_db_path)
    sr_client = _try_schema_registry(settings.schema_registry_url)
    introspector = SchemaIntrospector(sr_client)
    qdrant = AsyncQdrantClient(url=settings.qdrant_url)
    profiler = ActivityProfiler(qdrant, settings.qdrant_collection)
    sampler: MessageSampler | None = None
    if settings.catalog_enable_sampling:
        sampler = MessageSampler(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            schema_registry_url=settings.schema_registry_url,
            timeout_sec=settings.catalog_sample_timeout_sec,
        )
    catalog_config = build_catalog_config(settings)
    inference: InferenceEngine | None = None
    if settings.catalog_llm_provider != "disabled":
        try:
            provider = build_llm_provider(
                provider=settings.catalog_llm_provider,
                model=settings.catalog_llm_model,
            )
            inference = InferenceEngine(
                provider=provider,
                store=store,
                config=catalog_config,
            )
            log.info(
                "catalog.inference.enabled",
                provider=settings.catalog_llm_provider,
                model=settings.catalog_llm_model,
                ceiling_usd=catalog_config.daily_llm_spend_ceiling_usd,
            )
        except LLMUnavailableError as exc:
            log.warning(
                "catalog.inference.unavailable",
                provider=settings.catalog_llm_provider,
                error=str(exc),
            )
    builder = CatalogBuilder(
        store=store,
        introspector=introspector,
        sampler=sampler,
        profiler=profiler,
        config=catalog_config,
        inference=inference,
    )
    detector = RelationshipDetector(
        store=store,
        inference=inference,
        min_overlap_ratio=settings.catalog_relationship_min_overlap,
        llm_confidence_threshold=settings.catalog_relationship_llm_threshold,
    )
    return builder, detector, qdrant


async def refresh_once(
    builder: CatalogBuilder,
    detector: RelationshipDetector,
    topics: list[str],
) -> None:
    for topic in topics:
        try:
            with cat.track("topic"):
                await builder.refresh_topic(topic)
        except Exception:
            log.exception("catalog.refresh.failed", topic=topic)
    try:
        with cat.track("relationships"):
            await detector.refresh_all(topics)
    except Exception:
        log.exception("catalog.relationships.refresh_failed")


async def refresh_loop(
    builder: CatalogBuilder,
    detector: RelationshipDetector,
    topics: list[str],
    interval_sec: int,
    *,
    on_cycle: Callable[[], None] | None = None,
) -> None:
    while True:
        await refresh_once(builder, detector, topics)
        if on_cycle is not None:
            on_cycle()
        await asyncio.sleep(interval_sec)


class _RefresherHealth:
    """Mutable health state read by the /health endpoint."""

    def __init__(self) -> None:
        self._cycles = 0
        self._last_ts = 0.0

    def mark_cycle(self) -> None:
        self._cycles += 1
        self._last_ts = time.time()

    def check(self) -> tuple[bool, dict]:
        return (
            self._cycles > 0,
            {"cycles": self._cycles, "last_cycle_unix": round(self._last_ts, 1)},
        )


def _record_cycle(settings: Settings, builder: CatalogBuilder, health: _RefresherHealth) -> None:
    cat.LAST_CYCLE_TIMESTAMP.set(time.time())
    cat.UP.set(1)
    if settings.catalog_llm_provider != "disabled":
        spend = builder.store.get_spend_today(settings.catalog_llm_provider)
        cat.LLM_SPEND_TODAY.labels(provider=settings.catalog_llm_provider).set(spend)
    health.mark_cycle()


async def _async_main(loop: bool) -> int:
    settings = load_settings()
    configure_logging(level=settings.log_level, json=settings.log_json)
    builder, detector, qdrant = build_builder(settings)
    topics = settings.catalog_topics_list or settings.topics_list
    if not topics:
        log.error("catalog.no_topics_configured")
        return 78
    health = _RefresherHealth()
    metrics_server = start_metrics_server(settings, health_check=health.check)
    log.info(
        "catalog.refresher.start",
        topics=topics,
        db=str(builder.store.path),
        loop=loop,
    )
    try:
        if loop:
            await refresh_loop(
                builder,
                detector,
                topics,
                settings.catalog_stats_refresh_sec,
                on_cycle=lambda: _record_cycle(settings, builder, health),
            )
        else:
            await refresh_once(builder, detector, topics)
            _record_cycle(settings, builder, health)
    finally:
        if metrics_server is not None:
            metrics_server.stop()
        await qdrant.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="streamcontext catalog refresher")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously, refreshing on the configured cadence.",
    )
    args = parser.parse_args()
    code = asyncio.run(_async_main(loop=args.loop))
    if code != 0:
        sys.exit(code)


if __name__ == "__main__":
    main()
